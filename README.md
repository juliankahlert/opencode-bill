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

Estimated figures are marked so they can be told apart from historic
recorded prices: in plain-text bills every cost that includes any estimated
amount (the COST column and the Amount due total) carries an asterisk, and
every MODEL PRICING row carries one, because model rates always come from
external sources rather than from the bill data. A footnote under the
pricing table explains the marker. In the TUI the same figures render in
italic.

Sections in the bill include:

  * Session ID, title, creation time, and message count
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
including user messages. Month and day bills skip Started, Ended, and Wall
time because they span many unrelated sessions.

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
    opencode-bill [--year [YEAR]] [--data-dir PATH]
    opencode-bill [--week [WEEK]] [--data-dir PATH]
    opencode-bill [--today] [--data-dir PATH]

  session-id-or-prefix   Full OpenCode session ID or a unique prefix
                         (minimum 6 characters). With no session argument,
                         no --month, no --year, no --week, and no --today,
                         a terminal shows an interactive picker with
                         session, week, month, and year tabs; without a
                         terminal the current month is billed.

  --month [MONTH]        Bill the calendar month (1-12) of the current
                         year instead of one session and its sub-agents.
                         With no value the current month is billed, so
                         every session with messages in that window is
                         included; no session argument is required. This
                         is also what a bare `opencode-bill` (no arguments)
                         does when stdout is piped or redirected (on a
                         terminal it opens the session picker instead). The
                         report replaces the TOTAL table with one bar graph
                         per total metric: each graph has a vertical bar
                         for every calendar week the month spans (four to
                         six) plus the month total as a value (the total is
                         not a bar).

  --year [YEAR]          Bill the calendar year instead of one session
                         and its sub-agents. With no value the current
                         year is billed, so every session with messages in
                         that window is included; no session argument is
                         required. The report carries the same usage,
                         agent, and pricing tables as a `--month` bill for
                         the whole year, and replaces the TOTAL table with
                         one bar graph per total metric: each graph has a
                         vertical bar for every month of the year plus the
                         year total as a value (the total is not a bar).

  --week [WEEK]          Bill the ISO calendar week (1-53) of the current
                         year instead of one session and its sub-agents.
                         With no value the current week is billed, so
                         every session with messages in that window is
                         included; no session argument is required. The
                         report replaces the TOTAL table with one bar
                         graph per total metric: each graph has a vertical
                         bar for every weekday (Monday to Sunday) plus the
                         week total as a value (the total is not a bar).

  --today                Bill the current calendar day instead of one
                         session and its sub-agents. Every session with
                         messages in that window is included; no session
                         argument is required. A single-day slice of the
                         month bill: the same data a `--month` bill covers,
                         restricted to messages recorded today.

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
start of the next month, in the timestamps OpenCode stores); the day window
is one calendar day in UTC (start of the day to start of the next day); the
year window is one calendar year in UTC (start of January 1 to start of
January 1 of the next year); the week window is one ISO calendar week in UTC
(start of Monday to start of the next Monday), so week 1 of a year can start
in the previous calendar year. The labeled day is the local calendar date;
the window is that date in UTC. Year-graph bars bucket each call by the
calendar month of its creation time, month-graph bars by the ISO calendar
week of its creation time, and week-graph bars by the weekday of its
creation time. Calls without a creation time count in the totals but belong
to no bar. Provider corrections with negative token or cost values are
clamped to zero, so a monthly total can exceed what a provider's own invoice
shows when such corrections occurred.

Examples:

    opencode-bill
    opencode-bill abc123def456
    opencode-bill abc123 --data-dir /tmp/opencode-data
    opencode-bill --month
    opencode-bill --month 2
    opencode-bill --year
    opencode-bill --year 2025
    opencode-bill --week
    opencode-bill --week 32
    opencode-bill --today
    opencode-bill abc123 --provider openai
    opencode-bill --month --provider openai --provider anthropic
    opencode-bill --year --provider openai
    opencode-bill --week --provider openai
    opencode-bill --today --provider openai

When stdout is a terminal the report is shown with a loading indicator
that lists the current step (session load, message parsing, pricing
lookup, aggregation, report build) with live loaded/parsed/aggregated
row counts and the elapsed time until the report is ready; the loading
viewport is then erased and the styled report table is printed over it
from the top, right below your shell prompt, so the report replaces the
spinner without any blank padding, and the app exits. The indicator
renders inline in the terminal's main buffer instead of an alternate
(exclusive) screen: it appends below your existing shell output and
refreshes in place while it runs. Press `q` or Ctrl-C to cancel while it
is loading. When stdout is piped or redirected the plain-text bill is
printed as before.

A bare `opencode-bill` on a terminal opens an interactive picker with four
tabs — session, week, month, and year — and the session tab active. Each
tab lists its eight most recent options, one per row: the session tab the
eight most recent root sessions (sessions without a parent) with the
creation time first and the title after, the week tab the last eight ISO
calendar weeks, the month tab the last eight calendar months, and the year
tab the last eight calendar years, each labeled like the bill it starts
(e.g. `Week 33, 2026`, `August 2026`, `2026`). The selected row is
highlighted. Press `↑`/`↓` or `j`/`k` to move within a tab, `←`/`→`, `h`/`l`,
or Tab/Shift-Tab to switch tabs, `Enter` or a digit `1`-`8` to bill the
selected option, and `q`, `Q`, `Esc`, or Ctrl-C to quit without billing
anything. After the pick, the loading box and the report replace the menu
in place, and the report table gains a session title row plus an
id/creation-time row at the top when a session was picked, or the window
label when a week, month, or year was picked. Quitting the picker prints
nothing and exits cleanly.

Build from source
-----------------

    cargo build --release

The binary lands at target/release/opencode-bill.

License
-------

MIT -- see the LICENSE file.
