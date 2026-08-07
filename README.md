opencode-bill
============

Generate a plain-text bill for an OpenCode AI coding session.

Reads token usage and timing from OpenCode's SQLite session database (or
legacy JSON storage), looks up pricing from a local models.json catalog or
the GitHub Copilot model pricing page, falls back to a built-in generic
models.dev-style price list for well-known models when no API pricing is
available, and prints a formatted bill to stdout.

Pricing is looked up in this order: the local models.json catalog, the
GitHub Copilot pricing page (for github-copilot provider usage), the
built-in generic models.dev-style price list, and finally the pricing page
again by model name regardless of provider. Usage whose recorded cost is
zero - typical for subscription models, which carry no per-call price - is
billed at the best known rate for its provider/model pair, so subscription
usage shows an estimated cost instead of a zero row. Provider corrections
with negative costs are clamped to zero and never re-priced.

Sections in the bill include:

  * Session ID, title, and message count
  * Usage per provider / model (tokens, cost, API time)
  * Usage per agent (tokens, cost, API time)
  * Pricing table for each model used
  * Totals, including total API time and cost

Timing is derived from the `time.created` / `time.completed` timestamps that
OpenCode records per message, and the `state.time.start` / `state.time.end`
timestamps it records per tool part. API time is the summed duration of every
assistant (model) call, minus the time those calls spent waiting on tools:
only time spent waiting on the model endpoint is counted. Tool calls that run
in parallel are merged before being subtracted, and a tool that outlives its
message is clamped to it. Wall time appears only in the TOTAL section of
session bills: the span from the first to the last message of the session,
including user messages. Month bills skip Started, Ended, and Wall time
because they span many unrelated sessions.

In session bills, the session start and end timestamps are printed in the
local time zone of the machine rendering the bill, with the UTC offset
appended. Systems without a usable time zone fall back to UTC.

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

    opencode-bill [<session-id-or-prefix>] [--data-dir PATH]
    opencode-bill [--month [MONTH]] [--data-dir PATH]

  session-id-or-prefix   Full OpenCode session ID or a unique prefix
                         (minimum 6 characters). With no session argument
                         and no --month, the current month is billed.

  --month [MONTH]        Bill the calendar month (1-12) of the current
                         year instead of one session and its sub-agents.
                         With no value the current month is billed, so
                         every session with messages in that window is
                         included; no session argument is required. This
                         is also what a bare `opencode-bill` (no arguments)
                         does.

  --provider PROVIDER    Only include usage from this provider; repeatable.
                         When at least one is given, the usage, agent,
                         pricing, and total sections cover only the given
                         providers' requests, tokens, costs, and times, and
                         agent classes without usage from them are omitted;
                         the billed period itself is unchanged. Names match
                         the stored provider IDs exactly, case-sensitive
                         (e.g. openai, anthropic, github-copilot).

  --data-dir PATH        Path to the OpenCode data directory.
                         Defaults to the platform data directory
                         (e.g. ~/.local/share/opencode on Linux).

The month window is the calendar month in UTC (start of the first day to
start of the next month, in the timestamps OpenCode stores). Provider
corrections with negative token or cost values are clamped to zero, so a
monthly total can exceed what a provider's own invoice shows when such
corrections occurred.

Examples:

    opencode-bill
    opencode-bill abc123def456
    opencode-bill abc123 --data-dir /tmp/opencode-data
    opencode-bill --month
    opencode-bill --month 2
    opencode-bill abc123 --provider openai
    opencode-bill --month --provider openai --provider anthropic

When stdout is a terminal the report is shown with a loading indicator
that lists the current step (session load, aggregation, pricing lookup)
until the report is ready; the loading viewport is then erased and the
styled report table is printed over it from the top, right below your
shell prompt, so the report replaces the spinner without any blank
padding, and the app exits. The indicator renders inline in the
terminal's main buffer instead of an alternate (exclusive) screen: it
appends below your existing shell output and refreshes in place while it
runs. Press `q` or Ctrl-C to cancel while it is loading. When stdout is
piped or redirected the plain-text bill is printed as before.

Build from source
-----------------

    cargo build --release

The binary lands at target/release/opencode-bill.

License
-------

MIT -- see the LICENSE file.
