opencode-bill
============

Generate a plain-text bill for an OpenCode AI coding session.

Reads token usage and timing from OpenCode's SQLite session database (or
legacy JSON storage), looks up pricing from the GitHub Copilot model pricing
page, and prints a formatted bill to stdout.

Sections in the bill include:

  * Session ID, title, and message count
  * Usage per provider / model (tokens, cost, API time, wall time)
  * Usage per agent (tokens, cost, API time, wall time)
  * Pricing table for each model used
  * Totals, including total API time, session start and end, and wall time

Timing is derived from the `time.created` / `time.completed` timestamps that
OpenCode records per message, and the `state.time.start` / `state.time.end`
timestamps it records per tool part. API time is the summed duration of every
assistant (model) call, minus the time those calls spent waiting on tools:
only time spent waiting on the model endpoint is counted. Tool calls that run
in parallel are merged before being subtracted, and a tool that outlives its
message is clamped to it. Wall time in the TOTAL section is the span from the
first to the last message of the session, including user messages. Wall time
in the per-model and per-agent rows is the span from the first to the last
assistant call of that model or agent class; because sub-agents run in
parallel these spans overlap, so they do not add up to the session wall time.

The session start and end timestamps are printed in the local time zone of the
machine rendering the bill, with the UTC offset appended. Systems without a
usable time zone fall back to UTC.

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
