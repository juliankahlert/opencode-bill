opencode-bill
============

Generate a plain-text bill for an OpenCode AI coding session.

Reads token usage from OpenCode's SQLite session database (or legacy
JSON storage), looks up pricing from the GitHub Copilot model pricing
page, and prints a formatted bill to stdout.

Sections in the bill include:

  * Session ID, title, and message count
  * Usage per provider / model (tokens and cost)
  * Usage per agent (tokens and cost)
  * Pricing table for each model used
  * Totals

Install
-------

Requirements: Rust toolchain, cargo-deb (deb distros) or
cargo-generate-rpm (rpm distros).

Install the Cargo helper you need:

    # For deb-based distros (Debian, Ubuntu, etc.)
    cargo install --locked cargo-deb

    # For rpm-based distros (Fedora, RHEL, CentOS, etc.)
    cargo install --locked cargo-generate-rpm

Then run:

    just doit

This detects your distro type, builds the release binary, packages it,
and installs the package with sudo (offline -- no repository refresh).

Or do it manually:

    cargo build --release
    cargo deb --no-build --no-strip      # for deb distros
    cargo generate-rpm                   # for rpm distros
    sudo dpkg -i target/debian/*.deb     # install deb
    sudo rpm -U target/generate-rpm/*.rpm  # install rpm

Usage
-----

    opencode-bill <session-id-or-prefix> [--data-dir PATH]

  session-id-or-prefix   Full OpenCode session ID or a unique prefix
                         (minimum 6 characters).

  --data-dir PATH        Path to the OpenCode data directory.
                         Defaults to the platform data directory
                         (e.g. ~/.local/share/opencode on Linux).

Examples:

    opencode-bill abc123def456
    opencode-bill abc123 --data-dir /tmp/opencode-data

Build from source
-----------------

    cargo build --release

The binary lands at target/release/opencode-bill.

License
-------

MIT -- see the LICENSE file.
