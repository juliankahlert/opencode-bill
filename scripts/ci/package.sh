#!/bin/sh
set -eu

PROGRAM=${0##*/}

log_info() {
    printf '%s\n' "info: $*" >&2
}

log_warning() {
    printf '%s\n' "warning: $*" >&2
}

log_error() {
    printf '%s\n' "error: $*" >&2
}

die() {
    log_error "$*"
    exit 1
}

usage() {
    printf '%s\n' "usage: $PROGRAM {install-system-tools|install-cargo-tools|build|package|verify}" >&2
}

require_env() {
    for NAME do
        eval "VALUE=\${$NAME-}"
        [ -n "$VALUE" ] || die "$NAME must be set"
    done
}

install_system_tools() {
    log_info "installing system packaging tools"
    sudo apt-get update
    sudo apt-get install --yes binutils cpio rpm
}

install_cargo_tools() {
    log_info "installing Cargo packaging tools"
    cargo install --locked cargo-zigbuild@0.23.0
    cargo install --locked cargo-deb@3.7.0
    cargo install --locked cargo-generate-rpm@0.21.0
}

build_binary() {
    require_env BINARY ELF_MACHINE TARGET

    log_info "building $TARGET static binary"
    cargo zigbuild --locked --release --target "$TARGET"
    [ -x "$BINARY" ] || die "built binary is missing or not executable: $BINARY"

    if ! readelf -hW "$BINARY" | grep -q "Machine:.*$ELF_MACHINE"; then
        die "binary machine does not match $ELF_MACHINE"
    fi

    if readelf -lW "$BINARY" | grep -q INTERP; then
        die "binary has a dynamic interpreter"
    fi

    if readelf -dW "$BINARY" | grep -q '(NEEDED)'; then
        die "binary has a shared-library dependency"
    fi
}

build_packages() {
    require_env RPM_ARCH TARGET

    log_info "building $TARGET packages"
    mkdir -p dist
    cargo deb --locked --no-build --no-strip --target "$TARGET" --output dist
    # shellcheck disable=SC2153 # Set by the GitHub Actions job environment.
    cargo generate-rpm --target "$TARGET" --arch "$RPM_ARCH" --auto-req disabled --output dist
}

one_package() {
    KIND=$1

    case $KIND in
        DEB)
            set -- dist/*.deb
            ;;
        RPM)
            set -- dist/*.rpm
            ;;
        *)
            die "unsupported package kind: $KIND"
            ;;
    esac

    [ "$#" -eq 1 ] || die "expected one $KIND package, found $#"
    [ -f "$1" ] || die "expected $KIND package is missing: $1"
    printf '%s\n' "$1"
}

verify_packages() {
    require_env BINARY DEB_ARCH RPM_ARCH

    deb=$(one_package DEB)
    rpm=$(one_package RPM)
    log_info "reading package architectures"
    deb_arch=$(dpkg-deb --field "$deb" Architecture)
    rpm_arch=$(rpm --query --package --queryformat '%{ARCH}' "$rpm")

    log_info "verifying $deb ($deb_arch) and $rpm ($rpm_arch)"
    # shellcheck disable=SC2153 # Set by the GitHub Actions job environment.
    [ "$deb_arch" = "$DEB_ARCH" ] || die "DEB architecture is $deb_arch, expected $DEB_ARCH"
    [ "$rpm_arch" = "$RPM_ARCH" ] || die "RPM architecture is $rpm_arch, expected $RPM_ARCH"

    verify_dir=$(mktemp -d)
    trap 'rm -rf "$verify_dir"' 0 HUP INT TERM
    mkdir -p "$verify_dir/deb-root" "$verify_dir/rpm-root"
    log_info "extracting DEB package"
    dpkg-deb --extract "$deb" "$verify_dir/deb-root"
    log_info "extracting RPM package"
    if ! rpm2cpio "$rpm" > "$verify_dir/package.cpio"; then
        if ! cpio --quiet -it < "$verify_dir/package.cpio" >/dev/null; then
            die "rpm2cpio failed without producing a valid CPIO archive"
        fi

        log_warning "rpm2cpio exited nonzero after producing a valid CPIO archive"
    fi
    (
        cd "$verify_dir/rpm-root"
        cpio --quiet -id < "$verify_dir/package.cpio"
    )

    log_info "comparing DEB binary with $BINARY"
    cmp "$BINARY" "$verify_dir/deb-root/usr/bin/opencode-bill" || die "DEB binary differs from $BINARY"
    log_info "comparing RPM binary with $BINARY"
    cmp "$BINARY" "$verify_dir/rpm-root/usr/bin/opencode-bill" || die "RPM binary differs from $BINARY"
    log_info "package verification passed"
}

[ "$#" -eq 1 ] || {
    usage
    exit 2
}

case $1 in
    install-system-tools)
        install_system_tools
        ;;
    install-cargo-tools)
        install_cargo_tools
        ;;
    build)
        build_binary
        ;;
    package)
        build_packages
        ;;
    verify)
        verify_packages
        ;;
    *)
        usage
        exit 2
        ;;
esac
