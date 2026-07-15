set fallback := true

# Detect the host distro family: "deb" or "rpm"
distro := `if [ -f /etc/debian_version ]; then \
    echo "deb"; \
elif [ -f /etc/redhat-release ] || \
     [ -f /etc/fedora-release ] || \
     grep -Eq '^ID(_LIKE)?=.*(rhel|fedora|centos|alma|rocky)' /etc/os-release 2>/dev/null; then \
    echo "rpm"; \
else \
    echo "unknown"; \
fi`

# Validate distro detection
[private]
check-distro:
    @if [ "{{distro}}" = "unknown" ]; then \
        echo "ERROR: cannot detect distro type (deb or rpm)"; \
        echo "Set DISTRO=deb or DISTRO=rpm manually"; \
        exit 1; \
    fi

# Build the release binary
build:
    cargo build --release --locked

# Build a .deb package (requires cargo-deb)
build-deb: build
    cargo deb --locked --no-build --no-strip

# Build a .rpm package (requires cargo-generate-rpm)
build-rpm: build
    cargo generate-rpm

# Install a .deb package offline (dpkg only, no apt)
install-deb:
    @deb=$(ls target/debian/*.deb 2>/dev/null | head -1); \
    if [ -z "$deb" ]; then \
        echo "No .deb found. Run: just build-deb"; \
        exit 1; \
    fi; \
    sudo dpkg -i "$deb"

# Install a .rpm package offline (rpm only, no dnf/yum refresh)
install-rpm:
    @rpm=$(ls target/generate-rpm/*.rpm 2>/dev/null | head -1); \
    if [ -z "$rpm" ]; then \
        echo "No .rpm found. Run: just build-rpm"; \
        exit 1; \
    fi; \
    sudo rpm -U "$rpm"

# Uninstall the package
uninstall:
    sudo dpkg -r opencode-bill 2>/dev/null || sudo rpm -e opencode-bill 2>/dev/null || \
        echo "opencode-bill is not installed"

# Build, package, and install -- works on any deb or rpm distro
doit: check-distro
    @echo "Detected distro: {{distro}}"
    @just build-{{distro}}
    @just install-{{distro}}
    @echo "opencode-bill installed."

# Print the detected distro
which-distro:
    @echo "{{distro}}"
