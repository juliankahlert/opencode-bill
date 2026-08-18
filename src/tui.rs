//! TUI frontend: a loading indicator, then the styled report table printed
//! into the terminal's scrollback.
//!
//! The UI renders inline in the terminal's main buffer instead of the
//! alternate (exclusive) screen: a loading box right below the shell prompt
//! refreshes in place while the report is generated, and when it is ready
//! the loading viewport is erased and the report table is printed over it
//! from the top, so the table replaces the spinner without any blank
//! padding in the terminal's scrollback history. The app then exits; the
//! terminal's own scrollbar provides scrolling.

use std::io::{self, Write};
use std::time::{Duration, Instant};

use chrono::{Datelike, Duration as ChronoDuration, NaiveDate};
use crossterm::cursor::{MoveTo, Show};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::style::Print;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, size};
use opencode_bill::{
    BarGraph, BillError, BillHeader, BillProgress, BillReport, ESTIMATED_NOTE, RecentRoot,
    StorageContext, format_local_iso, graph_body_lines, load_day_report_with_progress_counted,
    load_month_report_with_progress_counted, load_report_with_progress_counted,
    load_week_report_with_progress_counted, load_year_report_with_progress_counted, recent_roots,
};
use ratatui::backend::CrosstermBackend;
use ratatui::buffer::Buffer;
use ratatui::layout::{Alignment, Constraint, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Cell, Clear, Paragraph, Row, StatefulWidget, Table, TableState};
use ratatui::{Frame, Terminal, TerminalOptions, Viewport};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;

/// Spin frames for the loading indicator.
const SPINNER_FRAMES: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
/// How often the spinner advances.
const SPINNER_TICK: Duration = Duration::from_millis(100);
/// Maximum event-poll wait; keeps the spinner animating.
const EVENT_TIMEOUT: Duration = Duration::from_millis(100);
/// Height of the inline viewport: exactly the spinner box, so no empty rows
/// pad it above or below while the report is generated.
const VIEWPORT_HEIGHT: u16 = 6;
/// How many options each picker tab lists.
const PICKER_LIMIT: usize = 8;
/// Title of the picker block.
const PICKER_TITLE: &str = "Select a bill";
/// Key hint shown at the bottom of the picker.
const PICKER_HINT: &str = "↑/↓ or j/k move · ←/→ or h/l tab · Enter or 1-8 select · q quit";
/// Unified column count shared by every report table.
const COLUMNS: usize = 11;
/// Natural width of every report table column.
///
/// Every section (usage, agent, pricing) shares one 11-column grid, so each
/// width must fit the longest header that lands in that column: col2 carries
/// "CACHE READ" in the pricing table, col3 carries "CACHE WRITE" in pricing,
/// col5 carries "CACHE WRITE" in the agent table.
///
/// The agent table fills all 11 columns, so the grid has no spare slot left:
/// another column means widening [`COLUMNS`] and re-checking every width.
const COLUMN_WIDTHS: [u16; COLUMNS] = [40, 14, 10, 11, 11, 11, 10, 10, 12, 13, 11];
/// Spaces between adjacent table columns.
const COLUMN_SPACING: u16 = 2;
/// Horizontal gap between adjacent graph blocks in the year grid.
const GRAPH_SPACING: u16 = 2;

/// One message from the background worker to the UI task.
enum Progress {
    /// A work phase is about to start.
    Task(String),
    /// The report is ready, or the failure that stopped it.
    Done(Box<Result<BillReport, String>>),
}

/// Runs the worker that loads the report in the background.
///
/// The worker is a tokio task on the runtime; the unbounded channel lets the
/// sync `on_task` callback send progress without awaiting.  The shared
/// `counters` handle carries the pipeline's live counts back to the UI task.
async fn worker(
    context: StorageContext,
    request: crate::Request,
    providers: Vec<String>,
    counters: BillProgress,
    sender: mpsc::UnboundedSender<Progress>,
) {
    let result = match request {
        crate::Request::Session(reference) => load_report_with_progress_counted(
            &context,
            &reference,
            &providers,
            |task| {
                let _ = sender.send(Progress::Task(task.to_owned()));
            },
            &counters,
        ),
        crate::Request::Month(year, month) => load_month_report_with_progress_counted(
            &context,
            year,
            month,
            &providers,
            |task| {
                let _ = sender.send(Progress::Task(task.to_owned()));
            },
            &counters,
        ),
        crate::Request::Day(year, month, day) => load_day_report_with_progress_counted(
            &context,
            year,
            month,
            day,
            &providers,
            |task| {
                let _ = sender.send(Progress::Task(task.to_owned()));
            },
            &counters,
        ),
        crate::Request::Year(year) => load_year_report_with_progress_counted(
            &context,
            year,
            &providers,
            |task| {
                let _ = sender.send(Progress::Task(task.to_owned()));
            },
            &counters,
        ),
        crate::Request::Week(year, week) => load_week_report_with_progress_counted(
            &context,
            year,
            week,
            &providers,
            |task| {
                let _ = sender.send(Progress::Task(task.to_owned()));
            },
            &counters,
        ),
        // Unreachable: run() resolves a pick before spawning the worker.
        crate::Request::Pick => Err(BillError::InvalidSessionReference(
            "the session picker is resolved before billing".to_owned(),
        )),
    };

    let _ = sender.send(Progress::Done(Box::new(
        result.map_err(|failure| failure.to_string()),
    )));
}

/// What the UI is showing right now.
enum View {
    /// Report generation is still running.
    Loading { task: String },
    /// The report is ready and will be printed as a styled table.
    Ready { report: Box<BillReport> },
    /// Report generation failed.
    Failed(String),
}

/// UI state that survives from frame to frame.
struct App {
    /// The currently visible view.
    view: View,
    /// Live counts of the running pipeline, read every frame.
    counters: BillProgress,
    /// When report generation started.
    started: Instant,
    /// Spinner frame counter.
    frame: usize,
    /// When the spinner last advanced.
    last_tick: Instant,
}

/// What the TUI session ended with.
pub enum TuiOutcome {
    /// The bill was printed (or the user quit before it finished).
    Success,
    /// The report failed and the failure message that was shown.
    Failed(String),
}

/// Runs the TUI and returns an error message when something fails.
///
/// Must be called from a tokio runtime: the worker runs as a spawned task.
pub async fn run(
    context: StorageContext,
    request: crate::Request,
    providers: Vec<String>,
) -> Result<TuiOutcome, String> {
    let mut guard = TerminalGuard::enter()?;
    // Start on a fresh row so the viewport does not overwrite the shell prompt line.
    execute!(io::stdout(), Print("\n")).map_err(|failure| failure.to_string())?;

    // Optional phase: the no-argument picker.  The picker owns the terminal
    // when an option is chosen, so the loading box and the bill replace its
    // menu in the same viewport instead of leaving a second box in the
    // scrollback.
    let mut terminal;
    let request = match request {
        crate::Request::Pick => {
            let roots =
                recent_roots(&context, PICKER_LIMIT).map_err(|failure| failure.to_string())?;
            let tabs = build_tabs(roots, chrono::Local::now().date_naive());
            let option_count = tabs.iter().map(|tab| tab.options.len()).max().unwrap_or(0);
            let (_, terminal_height) = size().unwrap_or((0, 24));
            let height = picker_height(option_count, terminal_height);
            let mut picker = Terminal::with_options(
                CrosstermBackend::new(io::stdout()),
                TerminalOptions {
                    viewport: Viewport::Inline(height),
                },
            )
            .map_err(|failure| failure.to_string())?;

            {
                let frame = picker.get_frame();

                guard.park_below(frame.area().bottom().saturating_sub(1));
            }
            match run_picker(&mut picker, &tabs)? {
                Some(request) => {
                    terminal = picker;
                    request
                }
                None => return Ok(TuiOutcome::Success),
            }
        }
        other => {
            terminal = Terminal::with_options(
                CrosstermBackend::new(io::stdout()),
                TerminalOptions {
                    viewport: Viewport::Inline(VIEWPORT_HEIGHT),
                },
            )
            .map_err(|failure| failure.to_string())?;
            let viewport_bottom = {
                let frame = terminal.get_frame();

                frame.area().bottom().saturating_sub(1)
            };

            guard.park_below(viewport_bottom);
            other
        }
    };

    let (sender, mut receiver) = mpsc::unbounded_channel();
    let counters = BillProgress::default();
    let worker_counters = counters.clone();

    let worker_task = tokio::spawn(worker(context, request, providers, worker_counters, sender));

    let mut app = App {
        view: View::Loading {
            task: "Starting".to_owned(),
        },
        counters,
        started: Instant::now(),
        frame: 0,
        last_tick: Instant::now(),
    };

    // Spin the loading box until the user quits or the report is ready.
    loop {
        if app.handle_events(&mut receiver)? {
            break;
        }
        if matches!(&app.view, View::Ready { .. }) {
            break;
        }
        app.tick();
        terminal
            .draw(|frame| app.draw(frame))
            .map_err(|failure| failure.to_string())?;
    }

    // The report is ready: erase the whole loading viewport and print the
    // styled report table from its top, so neither the spinner nor its blank
    // surround ever reaches the terminal history, then exit.
    if let View::Ready { report } = &app.view {
        // Re-capture the viewport: a mid-loading resize moves it.
        let viewport = {
            let frame = terminal.get_frame();

            frame.area()
        };
        guard.park_at(viewport.y + 1);
        drop(terminal);

        // The table starts right below the shell prompt, so blank every
        // viewport row first; the table then writes over the cleared area.
        // Still in raw mode here (the guard parks the cursor when it drops).
        {
            let mut stdout = io::stdout().lock();

            (viewport.y + 1..=viewport.y + viewport.height).try_for_each(|row| {
                write!(stdout, "\x1b[{row};1H\x1b[K").map_err(|failure| failure.to_string())
            })?;
            stdout.flush().map_err(|failure| failure.to_string())?;
        }
        // The guard parks the cursor on the viewport's top row and restores the
        // terminal; the table then starts exactly where the spinner was.
        drop(guard);
        let mut stdout = io::stdout().lock();

        // Render the whole report into one table at its natural width, then
        // print each row with its style; the terminal's own scrollbar provides
        // vertical navigation.
        let rows = build_rows(report);
        let natural_width = table_natural_width();
        let row_count = u16::try_from(rows.len()).map_err(|_| "report has too many rows")?;
        let mut table_buffer = Buffer::empty(Rect::new(0, 0, natural_width, row_count));
        let table = Table::new(rows, column_widths()).column_spacing(COLUMN_SPACING);
        let mut table_state = TableState::new().with_offset(0);

        table.render(
            Rect::new(0, 0, natural_width, row_count),
            &mut table_buffer,
            &mut table_state,
        );

        // Print the table; the visible width is the natural width (the table
        // is wider than most terminals, so the terminal wraps it).
        (0..row_count).try_for_each(|y| {
            stdout
                .write_all(render_row(&table_buffer, y, natural_width).as_bytes())
                .map_err(|failure| failure.to_string())
        })?;

        // Bar graphs (year: one per month; month: one per calendar week)
        // render as bordered blocks in a grid that fits the terminal width;
        // they print after the table, outside its 11-column grid.
        if !report.graphs.is_empty() {
            let (terminal_width, _) = size().map_err(|failure| failure.to_string())?;
            stdout
                .write_all(b"\n")
                .map_err(|failure| failure.to_string())?;

            render_graph_grid(&report.graphs, terminal_width)
                .into_iter()
                .try_for_each(|line| {
                    stdout
                        .write_all(line.as_bytes())
                        .map_err(|failure| failure.to_string())
                })?;
        }
        stdout.flush().map_err(|failure| failure.to_string())?;
        return Ok(TuiOutcome::Success);
    }

    // The user quit before the report was ready: cancel the worker.  The
    // pipeline runs as blocking code inside the task, so it finishes its
    // current phase before the cancellation lands; runtime shutdown then
    // waits only for that bounded work instead of the full pipeline.
    worker_task.abort();

    // Re-capture after the loop: a mid-session resize moves the inline viewport.
    let viewport_bottom = {
        let frame = terminal.get_frame();

        frame.area().bottom().saturating_sub(1)
    };
    guard.park_below(viewport_bottom);

    Ok(match app.view {
        View::Failed(failure) => TuiOutcome::Failed(failure),
        _ => TuiOutcome::Success,
    })
}

/// Where the cursor parks when the TUI exits.
enum Park {
    /// The row below the inline viewport (the shell prompt lands under it).
    Below(u16),
    /// The top row of the loading viewport; the report table starts there.
    At(u16),
    /// Leave the cursor where it is.
    None,
}

/// Enters raw mode without the alternate screen, restoring the terminal on
/// drop.
struct TerminalGuard {
    /// Where the cursor parks when this guard drops.
    park: Park,
}

impl TerminalGuard {
    fn enter() -> Result<Self, String> {
        enable_raw_mode().map_err(|failure| failure.to_string())?;

        Ok(TerminalGuard { park: Park::None })
    }

    fn park_below(&mut self, viewport_bottom: u16) {
        self.park = Park::Below(viewport_bottom);
    }

    fn park_at(&mut self, row: u16) {
        self.park = Park::At(row);
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let mut stdout = io::stdout();

        // MoveTo must come first: raw mode disables OPOST, so "\n" is a bare
        // LF that preserves the column. With the cursor at column 0 the LF
        // parks the shell prompt on the row below the viewport.
        match self.park {
            Park::Below(bottom) => {
                let _ = execute!(stdout, MoveTo(0, bottom), Print("\n"));
            }
            Park::At(row) => {
                let _ = execute!(stdout, MoveTo(0, row.saturating_sub(1)));
            }
            Park::None => {}
        }
        let _ = execute!(stdout, Show);
        let _ = disable_raw_mode();
    }
}

impl App {
    /// Polls input and worker messages; returns `true` when the user quits.
    fn handle_events(
        &mut self,
        receiver: &mut mpsc::UnboundedReceiver<Progress>,
    ) -> Result<bool, String> {
        self.drain_progress(receiver);

        if !event::poll(EVENT_TIMEOUT).map_err(|failure| failure.to_string())? {
            return Ok(false);
        }

        let event = event::read().map_err(|failure| failure.to_string())?;
        match event {
            Event::Key(key)
                if key.kind == KeyEventKind::Press || key.kind == KeyEventKind::Repeat =>
            {
                self.handle_key(key)
            }
            Event::Mouse(_) => Ok(false),
            _ => Ok(false),
        }
    }

    /// Applies every pending worker message to the current view.
    fn drain_progress(&mut self, receiver: &mut mpsc::UnboundedReceiver<Progress>) {
        loop {
            match receiver.try_recv() {
                Ok(Progress::Task(task)) => self.view = View::Loading { task },
                Ok(Progress::Done(result)) => match *result {
                    Ok(report) => {
                        self.view = View::Ready {
                            report: Box::new(report),
                        }
                    }
                    Err(failure) => self.view = View::Failed(failure),
                },
                Err(TryRecvError::Empty) => return,
                Err(TryRecvError::Disconnected) if matches!(self.view, View::Loading { .. }) => {
                    self.view = View::Failed("report generation stopped unexpectedly".to_owned());
                    return;
                }
                Err(TryRecvError::Disconnected) => return,
            }
        }
    }

    /// Handles one key press; returns `true` when the user quits.
    fn handle_key(&mut self, key: KeyEvent) -> Result<bool, String> {
        match key.code {
            KeyCode::Char('q') | KeyCode::Char('Q') | KeyCode::Esc => Ok(true),
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => Ok(true),
            _ => Ok(false),
        }
    }

    /// Advances the spinner clock.
    fn tick(&mut self) {
        if self.last_tick.elapsed() >= SPINNER_TICK {
            self.frame = self.frame.wrapping_add(1);
            self.last_tick = Instant::now();
        }
    }

    /// Renders the current view.
    fn draw(&self, frame: &mut Frame) {
        match &self.view {
            View::Loading { task } => draw_loading(frame, self, task),
            // Never reached: run() prints the table and exits the moment the
            // report is ready, before the next frame is drawn.
            View::Ready { .. } => {}
            View::Failed(failure) => draw_failure(frame, failure),
        }
    }
}

/// Draws the loading indicator with the current task, live counts, and
/// elapsed time.
fn draw_loading(frame: &mut Frame, app: &App, task: &str) {
    let area = frame.area();
    let spinner = SPINNER_FRAMES[app.frame % SPINNER_FRAMES.len()];
    let elapsed = format_elapsed(app.started.elapsed());
    let counts = counts_label(
        app.counters.loaded_rows(),
        app.counters.parsed_rows(),
        app.counters.folded_rows(),
    );
    let lines = vec![
        Line::from(Span::styled(
            "opencode-bill",
            Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        )),
        Line::from(vec![
            Span::styled(spinner, Style::new().fg(Color::Yellow)),
            Span::raw(format!(" {task}")),
        ]),
        Line::from(Span::styled(elapsed, Style::new().fg(Color::DarkGray))),
        Line::from(Span::styled(counts, Style::new().fg(Color::DarkGray))),
    ];
    let paragraph = Paragraph::new(lines)
        .alignment(Alignment::Center)
        .block(Block::bordered().title("Generating bill"));

    frame.render_widget(Clear, area);
    frame.render_widget(
        paragraph,
        Rect::new(area.x, area.y, 60.min(area.width), area.height),
    );
}

/// Formats an elapsed duration as seconds, or minutes and seconds once a
/// bill runs past one minute.
fn format_elapsed(elapsed: Duration) -> String {
    let total = elapsed.as_secs();

    if total >= 60 {
        format!("{}m {}s elapsed", total / 60, total % 60)
    } else {
        format!("{total}s elapsed")
    }
}

/// The live-counts line: storage rows loaded, parsed, and aggregated.
fn counts_label(loaded: u64, parsed: u64, folded: u64) -> String {
    format!(
        "{} loaded · {} parsed · {} aggregated",
        grouped(loaded),
        grouped(parsed),
        grouped(folded),
    )
}

/// Groups a count with thousands separators for readability.
fn grouped(count: u64) -> String {
    let digits = count.to_string();
    let length = digits.len();

    digits
        .char_indices()
        .map(|(index, digit)| {
            if index > 0 && (length - index).is_multiple_of(3) {
                format!(",{digit}")
            } else {
                digit.to_string()
            }
        })
        .collect()
}

/// Draws the failure message in the viewport.
fn draw_failure(frame: &mut Frame, failure: &str) {
    let area = frame.area();
    let lines = vec![
        Line::from(Span::styled(
            "Could not generate bill",
            Style::new().fg(Color::Red).add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::raw(failure.to_owned())),
        Line::from(Span::styled(
            "Press q to quit",
            Style::new().fg(Color::DarkGray),
        )),
    ];
    let paragraph = Paragraph::new(lines)
        .alignment(Alignment::Center)
        .block(Block::bordered().title("Error"));

    frame.render_widget(Clear, area);
    frame.render_widget(
        paragraph,
        Rect::new(
            area.x,
            area.y,
            80.min(area.width),
            VIEWPORT_HEIGHT.min(area.height),
        ),
    );
}

/// Height of the picker viewport: the block borders, the tab line, the
/// option rows, and the hint line, clamped to the terminal height so inline
/// rows never wrap.
fn picker_height(count: usize, terminal_height: u16) -> u16 {
    let needed = (5 + count as u16).max(6);

    needed.min(terminal_height.saturating_sub(1).max(5))
}

/// One selectable picker option, resolved into a bill request.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Pick {
    /// One root session.
    Session(RecentRoot),
    /// One ISO calendar week of a year.
    Week { year: u32, week: u32 },
    /// One calendar month of a year.
    Month { year: u32, month: u32 },
    /// One calendar year.
    Year { year: u32 },
}

impl Pick {
    /// The row text of the option, without the selection marker.
    fn label(&self) -> String {
        match self {
            Pick::Session(root) => {
                let created = root
                    .created_ms
                    .map_or_else(|| "-".to_owned(), format_local_iso);

                format!("{created}  {}", root.title)
            }
            Pick::Week { year, week } => format!("Week {week}, {year}"),
            Pick::Month { year, month } => {
                let date = NaiveDate::from_ymd_opt(*year as i32, *month, 1);

                date.map_or_else(
                    || format!("{year}-{month:02}"),
                    |first| first.format("%B %Y").to_string(),
                )
            }
            Pick::Year { year } => format!("{year}"),
        }
    }
}

/// One picker tab: its title and the options it lists.
struct Tab {
    /// Tab label, e.g. "session".
    title: &'static str,
    /// The tab's options, newest first.
    options: Vec<Pick>,
}

/// Builds the four picker tabs: session, week, month, and year.
///
/// The session tab lists the most recent root sessions; the week, month, and
/// year tabs list the last [`PICKER_LIMIT`] windows counting back from
/// `today`.  The session tab opens first.
fn build_tabs(roots: Vec<RecentRoot>, today: NaiveDate) -> Vec<Tab> {
    vec![
        Tab {
            title: "session",
            options: roots.into_iter().map(Pick::Session).collect(),
        },
        Tab {
            title: "week",
            options: last_weeks(today),
        },
        Tab {
            title: "month",
            options: last_months(today),
        },
        Tab {
            title: "year",
            options: last_years(today),
        },
    ]
}

/// The last [`PICKER_LIMIT`] ISO weeks, newest first, counting back from
/// `today`.  Each window is its ISO week number and ISO year, so a window
/// near a year boundary carries the previous ISO year.
fn last_weeks(today: NaiveDate) -> Vec<Pick> {
    (0..PICKER_LIMIT)
        .map(|index| {
            let date = today - ChronoDuration::days(7 * index as i64);
            let iso = date.iso_week();

            Pick::Week {
                year: iso.year() as u32,
                week: iso.week(),
            }
        })
        .collect()
}

/// The last [`PICKER_LIMIT`] calendar months, newest first, counting back
/// from `today`.  Each window is a month number of its year, so months
/// before January carry the previous year.
fn last_months(today: NaiveDate) -> Vec<Pick> {
    (0..PICKER_LIMIT)
        .map(|index| {
            let offset = today.month() as i32 - index as i32 - 1;

            Pick::Month {
                year: (today.year() + offset.div_euclid(12)) as u32,
                month: offset.rem_euclid(12) as u32 + 1,
            }
        })
        .collect()
}

/// The last [`PICKER_LIMIT`] calendar years, newest first, counting back
/// from `today`.
fn last_years(today: NaiveDate) -> Vec<Pick> {
    (0..PICKER_LIMIT)
        .map(|index| Pick::Year {
            year: (today.year() - index as i32) as u32,
        })
        .collect()
}

/// Resolves one picker option into the bill request it bills.
fn request_for(option: &Pick) -> crate::Request {
    match option {
        Pick::Session(root) => crate::Request::Session(root.id.clone()),
        Pick::Week { year, week } => crate::Request::Week(*year, *week),
        Pick::Month { year, month } => crate::Request::Month(*year, *month),
        Pick::Year { year } => crate::Request::Year(*year),
    }
}

/// Runs the picker loop until the user selects an option or quits.
///
/// Returns the resolved bill request, or `None` when the user quit the
/// picker without choosing.
fn run_picker(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    tabs: &[Tab],
) -> Result<Option<crate::Request>, String> {
    debug_assert!(!tabs.is_empty(), "the picker always has tabs");
    let mut tab = 0usize;
    let mut selected = 0usize;

    loop {
        if event::poll(EVENT_TIMEOUT).map_err(|failure| failure.to_string())? {
            let event = event::read().map_err(|failure| failure.to_string())?;

            if let Event::Key(key) = event
                && (key.kind == KeyEventKind::Press || key.kind == KeyEventKind::Repeat)
            {
                match pick_key(key, selected, tabs[tab].options.len(), tabs.len()) {
                    PickAction::Quit => return Ok(None),
                    PickAction::Select(index) => {
                        return Ok(Some(request_for(&tabs[tab].options[index])));
                    }
                    PickAction::MoveUp => selected = selected.saturating_sub(1),
                    PickAction::MoveDown => {
                        selected = (selected + 1).min(tabs[tab].options.len().saturating_sub(1));
                    }
                    PickAction::NextTab => {
                        tab = (tab + 1) % tabs.len();
                        selected = 0;
                    }
                    PickAction::PrevTab => {
                        tab = (tab + tabs.len() - 1) % tabs.len();
                        selected = 0;
                    }
                    PickAction::Nothing => {}
                }
            }
        }
        terminal
            .draw(|frame| draw_picker(frame, tabs, tab, selected))
            .map_err(|failure| failure.to_string())?;
    }
}

/// One key press of the picker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PickAction {
    /// Move the selection up one row.
    MoveUp,
    /// Move the selection down one row.
    MoveDown,
    /// Bill the option at this index of the active tab.
    Select(usize),
    /// Switch to the next tab.
    NextTab,
    /// Switch to the previous tab.
    PrevTab,
    /// Quit without billing anything.
    Quit,
    /// The key did not map to an action.
    Nothing,
}

/// Maps one picker key event to an action.
///
/// A pure function of the key and the selection state, so it is unit-testable
/// without a terminal.
fn pick_key(key: KeyEvent, selected: usize, count: usize, tab_count: usize) -> PickAction {
    match key.code {
        KeyCode::Up | KeyCode::Char('k') => PickAction::MoveUp,
        KeyCode::Down | KeyCode::Char('j') => PickAction::MoveDown,
        KeyCode::Enter if count > 0 => PickAction::Select(selected.min(count - 1)),
        KeyCode::Char(ch) if ch.is_ascii_digit() => {
            let index = ch
                .to_digit(10)
                .and_then(|value| usize::try_from(value).ok())
                .and_then(|value| value.checked_sub(1));

            match index {
                Some(index) if index < count => PickAction::Select(index),
                _ => PickAction::Nothing,
            }
        }
        KeyCode::Tab | KeyCode::Right | KeyCode::Char('l') if tab_count > 0 => PickAction::NextTab,
        KeyCode::BackTab | KeyCode::Left | KeyCode::Char('h') if tab_count > 0 => {
            PickAction::PrevTab
        }
        KeyCode::Char('q') | KeyCode::Char('Q') | KeyCode::Esc => PickAction::Quit,
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => PickAction::Quit,
        _ => PickAction::Nothing,
    }
}

/// Draws the picker menu in the current frame.
fn draw_picker(frame: &mut Frame, tabs: &[Tab], tab: usize, selected: usize) {
    let area = frame.area();
    let paragraph = Paragraph::new(picker_lines(tabs, tab, selected))
        .block(Block::bordered().title(PICKER_TITLE));

    frame.render_widget(Clear, area);
    frame.render_widget(paragraph, area);
}

/// One line of tab labels; the active tab is highlighted.
fn tab_line(tabs: &[Tab], tab: usize) -> Line<'static> {
    let spans = tabs
        .iter()
        .enumerate()
        .map(|(index, item)| {
            let style = if index == tab {
                Style::new().fg(Color::Yellow).add_modifier(Modifier::BOLD)
            } else {
                Style::new().fg(Color::DarkGray)
            };
            let prefix = if index == 0 { "" } else { "  " };

            Span::styled(format!("{prefix}[ {} ]", item.title), style)
        })
        .collect::<Vec<_>>();

    Line::from(spans)
}

/// One line per option of the active tab, plus the tab line and the hint.
///
/// Session rows put the creation date first, the title after; window rows
/// carry the same label as their bill.  The selected row is highlighted.
/// Lines longer than the viewport are clipped, never wrapped.
fn picker_lines(tabs: &[Tab], tab: usize, selected: usize) -> Vec<Line<'static>> {
    let mut lines = vec![tab_line(tabs, tab)];

    lines.extend(tabs[tab].options.iter().enumerate().map(|(index, option)| {
        let style = if index == selected {
            Style::new().fg(Color::Yellow).add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        };

        Line::from(Span::styled(format!("▶ {}", option.label()), style))
    }));
    lines.push(Line::from(Span::styled(
        PICKER_HINT,
        Style::new().fg(Color::DarkGray),
    )));
    lines
}

/// Builds every report row as one 11-column table, padded to a shared grid.
fn build_rows(report: &BillReport) -> Vec<Row<'static>> {
    debug_assert_eq!(report.usage_rows.len(), report.usage_cost_estimated.len());
    debug_assert_eq!(report.agent_rows.len(), report.agent_cost_estimated.len());

    let mut rows = Vec::new();

    // Month, day, week, and year bills carry their label; session bills
    // carry the title plus the root id and its creation time.
    match &report.header {
        BillHeader::Session {
            id,
            title,
            created_ms,
        } => {
            rows.push(title_row(title));
            rows.push(identity_row(id, *created_ms));
        }
        BillHeader::Month { label }
        | BillHeader::Day { label }
        | BillHeader::Week { label }
        | BillHeader::Year { label } => rows.push(title_row(label)),
    }

    rows.push(header_row(&[
        "PROVIDER / MODEL",
        "REQUESTS",
        "INPUT",
        "CACHE READ",
        "CACHE WRITE",
        "HIT",
        "OUTPUT",
        "REASONING",
        "API TIME",
        "COST (USD)",
    ]));
    rows.extend(data_rows(&report.usage_rows, &report.usage_cost_estimated));
    rows.push(blank_row());

    rows.push(title_row("USAGE BY AGENT CLASS"));
    rows.push(header_row(&[
        "AGENT",
        "SESSIONS",
        "REQUESTS",
        "INPUT",
        "CACHE READ",
        "CACHE WRITE",
        "OUTPUT",
        "REASONING",
        "API TIME",
        "COST (USD)",
    ]));
    rows.extend(data_rows(&report.agent_rows, &report.agent_cost_estimated));
    rows.push(blank_row());

    if !report.pricing_rows.is_empty() {
        rows.push(title_row("MODEL PRICING (USD / 1M TOKENS)"));
        rows.push(header_row(&[
            "PROVIDER / MODEL",
            "INPUT",
            "CACHE READ",
            "CACHE WRITE",
            "OUTPUT",
        ]));
        rows.extend(pricing_data_rows(&report.pricing_rows));
        rows.push(note_row(ESTIMATED_NOTE));
        rows.push(blank_row());
    }

    // The year bill replaces the TOTAL table with full-width bar graphs,
    // printed after the table rows.
    if report.graphs.is_empty() {
        rows.push(title_row("TOTAL"));
        report.totals.iter().for_each(|(label, value)| {
            rows.push(totals_row(
                label,
                value,
                report.amount_due_estimated && label == "Amount due:",
            ));
        });
    }

    rows
}

/// Column constraints of the unified report table.
fn column_widths() -> [Constraint; COLUMNS] {
    COLUMN_WIDTHS.map(Constraint::Length)
}

/// Full width of the unified report table in columns.
fn table_natural_width() -> u16 {
    COLUMN_WIDTHS.iter().sum::<u16>() + COLUMN_SPACING * (COLUMNS as u16 - 1)
}

/// Renders the year bill's bar graphs as bordered blocks in a grid.
///
/// The terminal width decides how many blocks sit next to each other: every
/// block shares the widest graph's content width plus borders, and columns
/// is the largest count whose blocks plus gaps still fit.  Blocks in one
/// row share the row's tallest height; shorter graphs pad with blank lines.
/// Each returned line is one styled terminal row of the finished grid.
fn render_graph_grid(graphs: &[BarGraph], terminal_width: u16) -> Vec<String> {
    let blocks = graphs.iter().map(graph_body_lines).collect::<Vec<_>>();

    if blocks.is_empty() {
        return Vec::new();
    }

    // The label sits in the block's top border, so it counts toward the
    // width just like the graph's own lines.
    let content_width = blocks
        .iter()
        .flat_map(|lines| lines.iter().map(|line| line.chars().count()))
        .chain(graphs.iter().map(|graph| graph.label.chars().count()))
        .max()
        .unwrap_or(1)
        .max(1)
        .min(usize::from(u16::MAX) - 4);
    let block_width = content_width + 2;
    let columns = (usize::from(terminal_width) + usize::from(GRAPH_SPACING))
        / (block_width + usize::from(GRAPH_SPACING));
    let columns = columns.max(1);
    let row_heights = blocks
        .chunks(columns)
        .map(|chunk| chunk.iter().map(Vec::len).max().unwrap_or(1))
        .collect::<Vec<_>>();
    let buffer_width = columns * block_width + (columns - 1) * usize::from(GRAPH_SPACING);
    let buffer_height = row_heights.iter().map(|height| *height + 2).sum::<usize>();
    let mut buffer = Buffer::empty(Rect::new(
        0,
        0,
        u16::try_from(buffer_width).unwrap_or(u16::MAX),
        u16::try_from(buffer_height).unwrap_or(u16::MAX),
    ));
    let mut y_offset = 0u16;

    blocks
        .chunks(columns)
        .enumerate()
        .for_each(|(row_index, chunk)| {
            let row_height = row_heights[row_index] as u16;

            chunk.iter().enumerate().for_each(|(column, lines)| {
                let x = column as u16 * (block_width as u16 + GRAPH_SPACING);
                let block_area = Rect::new(x, y_offset, block_width as u16, row_height + 2);
                let content_area =
                    Rect::new(x + 1, y_offset + 1, block_width as u16 - 2, row_height);

                let title = Span::styled(
                    graphs[row_index * columns + column].label,
                    Style::new().add_modifier(Modifier::BOLD),
                );

                ratatui::widgets::Widget::render(
                    Block::bordered().title(title),
                    block_area,
                    &mut buffer,
                );
                ratatui::widgets::Widget::render(
                    Paragraph::new(styled_graph_lines(lines, row_height)),
                    content_area,
                    &mut buffer,
                );
            });
            y_offset = y_offset.saturating_add(row_height + 2);
        });

    (0..buffer_height)
        .map(|y| render_row(&buffer, y as u16, buffer_width as u16))
        .collect()
}

/// Pads a graph's lines to a row height and styles them: the total line
/// bold, the month labels dim.
fn styled_graph_lines(lines: &[String], row_height: u16) -> Vec<Line<'static>> {
    let padded_len = usize::from(row_height);

    (0..padded_len)
        .map(|index| {
            let text = lines.get(index).cloned().unwrap_or_default();
            let modifier = if index == 0 {
                Modifier::BOLD
            } else if index + 1 == padded_len {
                Modifier::DIM
            } else {
                Modifier::empty()
            };

            Line::from(Span::styled(text, Style::new().add_modifier(modifier)))
        })
        .collect()
}

/// An empty row that separates the report tables.
fn blank_row() -> Row<'static> {
    Row::new(vec![Cell::new("")])
}

/// A section title row that spans the first column.
fn title_row(title: &str) -> Row<'static> {
    padded_row(
        vec![Cell::from(title.to_owned())],
        Style::new().add_modifier(Modifier::BOLD),
    )
}

/// A bold header row with numeric columns aligned right.
fn header_row(headers: &[&str]) -> Row<'static> {
    let cells = headers
        .iter()
        .enumerate()
        .map(|(column, header)| {
            aligned_cell(
                Line::from((*header).to_owned()),
                if column == 0 {
                    Alignment::Left
                } else {
                    Alignment::Right
                },
            )
        })
        .collect();

    padded_row(
        cells,
        Style::new().add_modifier(Modifier::BOLD | Modifier::UNDERLINED),
    )
}

/// Data rows with numeric columns aligned right.
///
/// The last column of a marked row is the cost cell: rows whose figure
/// includes any estimated amount render it in italic.
fn data_rows(rows: &[Vec<String>], estimated: &[bool]) -> Vec<Row<'static>> {
    rows.iter()
        .enumerate()
        .map(|(index, row)| {
            let cells = row
                .iter()
                .enumerate()
                .map(|(column, cell)| {
                    let mut line = Line::from(cell.clone());

                    if estimated.get(index).copied().unwrap_or(false) && column + 1 == row.len() {
                        line = line.style(Style::new().add_modifier(Modifier::ITALIC));
                    }

                    aligned_cell(
                        line,
                        if column == 0 {
                            Alignment::Left
                        } else {
                            Alignment::Right
                        },
                    )
                })
                .collect();

            padded_row(cells, Style::default())
        })
        .collect()
}

/// MODEL PRICING rows: every rate comes from an external source, so the
/// whole row renders in italic to match the asterisk of the plain-text bill.
fn pricing_data_rows(rows: &[Vec<String>]) -> Vec<Row<'static>> {
    rows.iter()
        .map(|row| {
            let cells = row
                .iter()
                .enumerate()
                .map(|(column, cell)| {
                    aligned_cell(
                        Line::from(cell.clone()).style(Style::new().add_modifier(Modifier::ITALIC)),
                        if column == 0 {
                            Alignment::Left
                        } else {
                            Alignment::Right
                        },
                    )
                })
                .collect();

            padded_row(cells, Style::default())
        })
        .collect()
}

/// One dimmed TOTAL row; the amount-due value turns italic when it includes
/// any estimated cost.
fn totals_row(label: &str, value: &str, estimated: bool) -> Row<'static> {
    let mut value_style = Style::default();

    if estimated {
        value_style = value_style.add_modifier(Modifier::ITALIC);
    }

    let cells = vec![
        Cell::from(label.to_owned()),
        aligned_cell(
            Line::from(value.to_owned()).style(value_style),
            Alignment::Right,
        ),
    ];

    padded_row(cells, Style::new().fg(Color::DarkGray))
}

/// One dimmed footnote row explaining the estimated marker.
fn note_row(note: &str) -> Row<'static> {
    padded_row(
        vec![Cell::from(note.to_owned())],
        Style::new().fg(Color::DarkGray),
    )
}

/// One dimmed session-identity row: the root id and its creation time.
fn identity_row(id: &str, created_ms: Option<i64>) -> Row<'static> {
    let identity = created_ms.map_or_else(
        || id.to_owned(),
        |millis| format!("{id} · {}", format_local_iso(millis)),
    );

    padded_row(vec![Cell::from(identity)], Style::new().fg(Color::DarkGray))
}

/// A cell whose content is aligned within its column.
fn aligned_cell(content: Line<'static>, alignment: Alignment) -> Cell<'static> {
    Cell::new(content.alignment(alignment))
}

/// Pads a row out to the shared column count.
fn padded_row(cells: Vec<Cell<'static>>, style: Style) -> Row<'static> {
    Row::new(padded_cells(cells)).style(style)
}

/// Pads a cell list out to the shared column count.
fn padded_cells(mut cells: Vec<Cell<'static>>) -> Vec<Cell<'static>> {
    cells.resize(COLUMNS, Cell::new(""));
    cells
}

/// Renders one table row as styled terminal output.
///
/// The table is rendered into a full-width buffer; the terminal wraps long
/// lines itself, so every column keeps its natural width and alignment.
fn render_row(buffer: &Buffer, y: u16, width: u16) -> String {
    let mut output = String::new();
    let mut active_style = Style::default();

    (0..width).for_each(|x| {
        let cell = &buffer[(x, y)];

        if cell.style() != active_style {
            // Reset first: a style change to plain text must not leak the
            // previous attributes to the end of the line.
            output.push('\x1b');
            output.push_str("[0m");
            output.push_str(&ansi_style(cell.style()));
            active_style = cell.style();
        }
        output.push_str(cell.symbol());
    });
    output.push_str("\x1b[0m\n");
    output
}

/// Converts a ratatui style into ANSI SGR escape codes.
fn ansi_style(style: Style) -> String {
    let mut codes: Vec<String> = Vec::new();

    match style.fg {
        Some(Color::Black) => codes.push("30".to_owned()),
        Some(Color::Red) => codes.push("31".to_owned()),
        Some(Color::Green) => codes.push("32".to_owned()),
        Some(Color::Yellow) => codes.push("33".to_owned()),
        Some(Color::Blue) => codes.push("34".to_owned()),
        Some(Color::Magenta) => codes.push("35".to_owned()),
        Some(Color::Cyan) => codes.push("36".to_owned()),
        Some(Color::Gray) => codes.push("37".to_owned()),
        Some(Color::DarkGray) => codes.push("90".to_owned()),
        Some(Color::LightRed) => codes.push("91".to_owned()),
        Some(Color::LightGreen) => codes.push("92".to_owned()),
        Some(Color::LightYellow) => codes.push("93".to_owned()),
        Some(Color::LightBlue) => codes.push("94".to_owned()),
        Some(Color::LightMagenta) => codes.push("95".to_owned()),
        Some(Color::LightCyan) => codes.push("96".to_owned()),
        Some(Color::White) => codes.push("97".to_owned()),
        Some(Color::Rgb(red, green, blue)) => {
            codes.push(format!("38;2;{red};{green};{blue}"));
        }
        Some(Color::Indexed(index)) => {
            codes.push(format!("38;5;{index}"));
        }
        Some(Color::Reset) | None => {}
    }

    let modifiers = style.add_modifier;

    if modifiers.contains(Modifier::BOLD) {
        codes.push("1".to_owned());
    }
    if modifiers.contains(Modifier::DIM) {
        codes.push("2".to_owned());
    }
    if modifiers.contains(Modifier::ITALIC) {
        codes.push("3".to_owned());
    }
    if modifiers.contains(Modifier::UNDERLINED) {
        codes.push("4".to_owned());
    }
    if modifiers.contains(Modifier::SLOW_BLINK) {
        codes.push("5".to_owned());
    }
    if modifiers.contains(Modifier::RAPID_BLINK) {
        codes.push("6".to_owned());
    }
    if modifiers.contains(Modifier::REVERSED) {
        codes.push("7".to_owned());
    }
    if modifiers.contains(Modifier::HIDDEN) {
        codes.push("8".to_owned());
    }
    if modifiers.contains(Modifier::CROSSED_OUT) {
        codes.push("9".to_owned());
    }

    if codes.is_empty() {
        return String::new();
    }

    format!("\x1b[{}m", codes.join(";"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_report() -> BillReport {
        BillReport {
            header: BillHeader::Session {
                id: "ses_abcdef123456".to_owned(),
                title: "Test session".to_owned(),
                created_ms: None,
            },
            session_count: 1,
            usage_rows: vec![vec![
                "openai / gpt-test".to_owned(),
                "1".to_owned(),
                "10".to_owned(),
                "6".to_owned(),
                "1".to_owned(),
                "4".to_owned(),
                "2".to_owned(),
                "0ms".to_owned(),
                "$0.125000".to_owned(),
            ]],
            agent_rows: vec![vec![
                "unknown".to_owned(),
                "1".to_owned(),
                "1".to_owned(),
                "10".to_owned(),
                "6".to_owned(),
                "1".to_owned(),
                "4".to_owned(),
                "2".to_owned(),
                "0ms".to_owned(),
                "$0.125000".to_owned(),
            ]],
            pricing_rows: Vec::new(),
            usage_cost_estimated: vec![false],
            agent_cost_estimated: vec![false],
            amount_due_estimated: false,
            totals: vec![
                ("Requests:".to_owned(), "1".to_owned()),
                ("Amount due:".to_owned(), "$0.125000 USD".to_owned()),
            ],
            graphs: Vec::new(),
        }
    }

    #[test]
    fn builds_unified_report_table() {
        let rows = build_rows(&fixture_report());

        // title + id rows, usage header + row + blank, agent title + header +
        // row + blank, total title + 2 rows.
        assert_eq!(rows.len(), 12);
    }

    #[test]
    fn italicizes_estimated_cost_cells() {
        let mut report = fixture_report();
        report.usage_cost_estimated = vec![true];
        let buffer = render_rows(build_rows(&report));

        // The usage data row is the fourth table row; the cost column is the
        // ninth of the 11-column grid.
        assert!(
            buffer[(cost_column_x(), 3)]
                .style()
                .add_modifier
                .contains(Modifier::ITALIC),
            "cost cell of an estimated row should be italic"
        );
        assert!(
            !buffer[(0, 3)]
                .style()
                .add_modifier
                .contains(Modifier::ITALIC),
            "model cell of an estimated row stays plain"
        );

        report.usage_cost_estimated = vec![false];
        let buffer = render_rows(build_rows(&report));

        assert!(
            !buffer[(cost_column_x(), 3)]
                .style()
                .add_modifier
                .contains(Modifier::ITALIC),
            "cost cell of a historic-only row stays plain"
        );
    }

    #[test]
    fn italicizes_whole_pricing_rows() {
        let mut report = fixture_report();

        report.pricing_rows = vec![vec![
            "openai / gpt-test*".to_owned(),
            "$1.250".to_owned(),
            "$0.125".to_owned(),
            "-".to_owned(),
            "$5.000".to_owned(),
        ]];
        let buffer = render_rows(build_rows(&report));

        // The pricing data row is the 12th table row; the model cell sits at
        // column zero, the first rate cell at the second column.
        let pricing_y = 11;
        assert!(
            buffer[(0, pricing_y)]
                .style()
                .add_modifier
                .contains(Modifier::ITALIC)
                && buffer[(model_rate_x(), pricing_y)]
                    .style()
                    .add_modifier
                    .contains(Modifier::ITALIC),
            "every cell of a pricing row should be italic"
        );
    }

    #[test]
    fn italicizes_estimated_amount_due() {
        let mut report = fixture_report();

        report.amount_due_estimated = true;
        let buffer = render_rows(build_rows(&report));

        // The amount-due row is the last row; its value sits in the second
        // column of the 11-column grid.
        assert!(
            buffer[(model_rate_x(), 11)]
                .style()
                .add_modifier
                .contains(Modifier::ITALIC),
            "the amount-due value of an estimated bill should be italic"
        );
    }

    #[test]
    fn adds_estimated_note_after_pricing_table() {
        let mut report = fixture_report();

        report.pricing_rows = vec![vec![
            "openai / gpt-test*".to_owned(),
            "$1.250".to_owned(),
            "$0.125".to_owned(),
            "-".to_owned(),
            "$5.000".to_owned(),
        ]];
        let buffer = render_rows(build_rows(&report));

        // The note row comes right after the pricing data row.
        let note_line = render_row(&buffer, 12, table_natural_width());

        assert!(
            note_line.contains(ESTIMATED_NOTE),
            "the estimated note should appear after the pricing table: {note_line}"
        );
    }

    /// Renders report rows into a buffer of the natural table width.
    fn render_rows(rows: Vec<Row<'static>>) -> Buffer {
        let height = u16::try_from(rows.len()).unwrap_or(u16::MAX);
        let table = Table::new(rows, column_widths()).column_spacing(COLUMN_SPACING);
        let mut table_state = TableState::new().with_offset(0);
        let mut buffer = Buffer::empty(Rect::new(0, 0, table_natural_width(), height));

        table.render(
            Rect::new(0, 0, table_natural_width(), height),
            &mut buffer,
            &mut table_state,
        );
        buffer
    }

    /// X offset of the cost column (8) in the unified grid.
    fn cost_column_x() -> u16 {
        COLUMN_WIDTHS[..8].iter().sum::<u16>() + COLUMN_SPACING * 8
    }

    /// X offset of the first numeric column (1) in the unified grid.
    fn model_rate_x() -> u16 {
        COLUMN_WIDTHS[..1].iter().sum::<u16>() + COLUMN_SPACING
    }

    #[test]
    fn prepends_month_label_row() {
        let mut report = fixture_report();

        report.header = BillHeader::Month {
            label: "February 2026".to_owned(),
        };
        let rows = build_rows(&report);
        let table = Table::new(rows, column_widths()).column_spacing(COLUMN_SPACING);
        let mut table_state = TableState::new().with_offset(0);
        let mut table_buffer = Buffer::empty(Rect::new(0, 0, table_natural_width(), 11));

        table.render(
            Rect::new(0, 0, table_natural_width(), 11),
            &mut table_buffer,
            &mut table_state,
        );

        let label = render_row(&table_buffer, 0, table_natural_width());

        assert!(label.contains("February 2026"), "label: {label}");
    }

    #[test]
    fn prepends_day_label_row() {
        let mut report = fixture_report();

        report.header = BillHeader::Day {
            label: "August 8 2026".to_owned(),
        };
        let rows = build_rows(&report);
        let table = Table::new(rows, column_widths()).column_spacing(COLUMN_SPACING);
        let mut table_state = TableState::new().with_offset(0);
        let mut table_buffer = Buffer::empty(Rect::new(0, 0, table_natural_width(), 11));

        table.render(
            Rect::new(0, 0, table_natural_width(), 11),
            &mut table_buffer,
            &mut table_state,
        );

        let label = render_row(&table_buffer, 0, table_natural_width());

        assert!(label.contains("August 8 2026"), "label: {label}");
    }

    #[test]
    fn prepends_week_label_row() {
        let mut report = fixture_report();

        report.header = BillHeader::Week {
            label: "Week 32, 2026".to_owned(),
        };
        let rows = build_rows(&report);
        let table = Table::new(rows, column_widths()).column_spacing(COLUMN_SPACING);
        let mut table_state = TableState::new().with_offset(0);
        let mut table_buffer = Buffer::empty(Rect::new(0, 0, table_natural_width(), 11));

        table.render(
            Rect::new(0, 0, table_natural_width(), 11),
            &mut table_buffer,
            &mut table_state,
        );

        let label = render_row(&table_buffer, 0, table_natural_width());

        assert!(label.contains("Week 32, 2026"), "label: {label}");
    }

    #[test]
    fn year_report_skips_the_total_table_for_graphs() {
        let mut report = fixture_report();

        report.header = BillHeader::Year {
            label: "2026".to_owned(),
        };
        report.graphs.push(opencode_bill::BarGraph {
            label: "REQUESTS",
            bars: vec![1; 12],
            labels: month_labels(),
            total: 12,
            kind: opencode_bill::MetricKind::Count,
        });

        let rows = build_rows(&report);
        let table = Table::new(rows, column_widths()).column_spacing(COLUMN_SPACING);
        let mut table_state = TableState::new().with_offset(0);
        let mut table_buffer = Buffer::empty(Rect::new(0, 0, table_natural_width(), 12));

        table.render(
            Rect::new(0, 0, table_natural_width(), 12),
            &mut table_buffer,
            &mut table_state,
        );

        let mut text = String::new();

        (0..12).for_each(|y| {
            (0..table_natural_width()).for_each(|x| {
                text.push_str(table_buffer[(x, y)].symbol());
            });
            text.push('\n');
        });

        assert!(text.contains("2026"), "text: {text}");
        assert!(
            !text.contains("TOTAL"),
            "year report must skip TOTAL: {text}"
        );
    }

    /// One label per month, January first; the year graphs' bar labels.
    fn month_labels() -> Vec<String> {
        [
            "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    }

    /// The eight year graphs mirroring the month TOTAL rows.
    fn year_graphs() -> Vec<BarGraph> {
        [
            "REQUESTS",
            "INPUT",
            "CACHE READ",
            "CACHE WRITE",
            "OUTPUT",
            "REASONING",
            "API TIME",
            "AMOUNT DUE",
        ]
        .into_iter()
        .map(|label| BarGraph {
            label,
            bars: vec![1; 12],
            labels: month_labels(),
            total: 12,
            kind: opencode_bill::MetricKind::Count,
        })
        .collect()
    }

    /// The eight month graphs mirroring the day TOTAL rows, with bars for
    /// the six calendar weeks of August 2026.
    fn month_graphs() -> Vec<BarGraph> {
        [
            "REQUESTS",
            "INPUT",
            "CACHE READ",
            "CACHE WRITE",
            "OUTPUT",
            "REASONING",
            "API TIME",
            "AMOUNT DUE",
        ]
        .into_iter()
        .map(|label| BarGraph {
            label,
            bars: vec![1; 6],
            labels: vec!["31", "32", "33", "34", "35", "36"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            total: 6,
            kind: opencode_bill::MetricKind::Count,
        })
        .collect()
    }

    #[test]
    fn grids_year_graphs_into_blocks_that_fit_the_terminal_width() {
        let grid = render_graph_grid(&year_graphs(), 200);
        let first = &grid[0];

        // 50-column blocks plus 2-column gaps: three fit in 200 columns, so
        // eight graphs need three rows of 12 lines.
        assert_eq!(grid.len(), 36);
        assert_eq!(first.matches('┌').count(), 3);
        assert!(grid[0].contains("REQUESTS"), "row: {}", grid[0]);
    }

    #[test]
    fn stacks_year_graphs_into_one_column_on_narrow_terminals() {
        let grid = render_graph_grid(&year_graphs(), 80);

        assert_eq!(grid.len(), 96);
        assert_eq!(grid[0].matches('┌').count(), 1);
    }

    #[test]
    fn fits_every_year_graph_on_one_row_when_the_terminal_is_wide() {
        let grid = render_graph_grid(&year_graphs(), 1000);

        assert_eq!(grid.len(), 12);
        assert_eq!(grid[0].matches('┌').count(), 8);
        assert!(grid[0].contains("AMOUNT DUE"), "row: {}", grid[0]);
    }

    #[test]
    fn pads_short_year_graphs_to_their_row_height() {
        let mut graphs = year_graphs();

        graphs[7].bars = vec![0; 12];
        graphs[7].total = 0;
        let grid = render_graph_grid(&graphs, 1000);

        assert_eq!(grid.len(), 12);
        // The zero-usage graph keeps its bordered block across the full row.
        assert!(grid[11].contains("└"), "row: {}", grid[11]);
    }

    #[test]
    fn grids_month_graphs_into_blocks_that_fit_the_terminal_width() {
        let grid = render_graph_grid(&month_graphs(), 200);

        // The week graphs are narrower than the year graphs (their widest
        // line is the title, not the labels), so seven blocks fit per row:
        // two rows of 12 lines.
        assert_eq!(grid.len(), 24);
        assert_eq!(grid[0].matches('┌').count(), 7);
        assert!(grid[0].contains("REQUESTS"), "row: {}", grid[0]);
        assert!(
            grid[10].contains("31  32  33  34  35  36"),
            "row: {}",
            grid[10]
        );
    }

    #[test]
    fn renders_an_empty_graph_grid() {
        assert!(render_graph_grid(&[], 200).is_empty());
    }

    #[test]
    fn pads_rows_to_the_shared_column_count() {
        let cells = padded_cells(vec![Cell::from("x"), Cell::from("y")]);

        assert_eq!(cells.len(), COLUMNS);
    }

    #[test]
    fn computes_natural_table_width() {
        assert_eq!(table_natural_width(), 173);
    }

    #[test]
    fn renders_table_at_natural_width() {
        let rows = build_rows(&fixture_report());
        let table = Table::new(rows, column_widths()).column_spacing(COLUMN_SPACING);
        let mut table_state = TableState::new().with_offset(0);
        let mut table_buffer = Buffer::empty(Rect::new(0, 0, table_natural_width(), 12));

        table.render(
            Rect::new(0, 0, table_natural_width(), 12),
            &mut table_buffer,
            &mut table_state,
        );

        let mut header = String::new();

        (0..table_natural_width()).for_each(|x| {
            header.push_str(table_buffer[(x, 2)].symbol());
        });

        assert!(header.contains("PROVIDER / MODEL"), "header: {header}");
        assert!(header.contains("REQUESTS"), "header: {header}");
    }

    #[test]
    fn renders_session_identity_rows() {
        let rows = build_rows(&fixture_report());
        let table = Table::new(rows, column_widths()).column_spacing(COLUMN_SPACING);
        let mut table_state = TableState::new().with_offset(0);
        let mut table_buffer = Buffer::empty(Rect::new(0, 0, table_natural_width(), 12));

        table.render(
            Rect::new(0, 0, table_natural_width(), 12),
            &mut table_buffer,
            &mut table_state,
        );

        let title = render_row(&table_buffer, 0, table_natural_width());
        let identity = render_row(&table_buffer, 1, table_natural_width());

        assert!(title.contains("Test session"), "title: {title}");
        assert!(
            identity.contains("ses_abcdef123456"),
            "identity: {identity}"
        );
    }

    #[test]
    fn renders_session_creation_time_in_identity_row() {
        let mut report = fixture_report();

        report.header = BillHeader::Session {
            id: "ses_abcdef123456".to_owned(),
            title: "Test session".to_owned(),
            created_ms: Some(1_786_315_623_640),
        };
        let rows = build_rows(&report);
        let table = Table::new(rows, column_widths()).column_spacing(COLUMN_SPACING);
        let mut table_state = TableState::new().with_offset(0);
        let mut table_buffer = Buffer::empty(Rect::new(0, 0, table_natural_width(), 12));

        table.render(
            Rect::new(0, 0, table_natural_width(), 12),
            &mut table_buffer,
            &mut table_state,
        );

        let identity = render_row(&table_buffer, 1, table_natural_width());

        assert!(
            identity.contains("ses_abcdef123456 · "),
            "identity: {identity}"
        );
    }

    #[test]
    fn renders_styled_header_row() {
        let rows = build_rows(&fixture_report());
        let table = Table::new(rows, column_widths()).column_spacing(COLUMN_SPACING);
        let mut table_state = TableState::new().with_offset(0);
        let mut table_buffer = Buffer::empty(Rect::new(0, 0, table_natural_width(), 12));

        table.render(
            Rect::new(0, 0, table_natural_width(), 12),
            &mut table_buffer,
            &mut table_state,
        );

        let line = render_row(&table_buffer, 2, table_natural_width());

        assert!(line.starts_with("\x1b[0m\x1b[1;4m"), "line: {line}");
        assert!(line.contains("PROVIDER / MODEL"), "line: {line}");
        assert!(line.ends_with("\x1b[0m\n"), "line: {line}");
    }

    #[test]
    fn renders_agent_header_without_truncation() {
        let rows = build_rows(&fixture_report());
        let table = Table::new(rows, column_widths()).column_spacing(COLUMN_SPACING);
        let mut table_state = TableState::new().with_offset(0);
        let mut table_buffer = Buffer::empty(Rect::new(0, 0, table_natural_width(), 12));

        table.render(
            Rect::new(0, 0, table_natural_width(), 12),
            &mut table_buffer,
            &mut table_state,
        );

        // Row 6 is the agent header; REQUESTS and CACHE WRITE once fit only
        // truncated (QUESTS / E WRITE) because they landed in narrow columns.
        let line = render_row(&table_buffer, 6, table_natural_width());

        assert!(line.contains("AGENT"), "line: {line}");
        assert!(line.contains("SESSIONS"), "line: {line}");
        assert!(line.contains("REQUESTS"), "line: {line}");
        assert!(line.contains("CACHE READ"), "line: {line}");
        assert!(line.contains("CACHE WRITE"), "line: {line}");
        assert!(line.contains("COST (USD)"), "line: {line}");

        // Row 2 is the provider/model header; it carries the cache-hit rate.
        let usage = render_row(&table_buffer, 2, table_natural_width());

        assert!(usage.contains("HIT"), "line: {usage}");
    }

    #[test]
    fn renders_dimmed_total_row() {
        let rows = build_rows(&fixture_report());
        let table = Table::new(rows, column_widths()).column_spacing(COLUMN_SPACING);
        let mut table_state = TableState::new().with_offset(0);
        let mut table_buffer = Buffer::empty(Rect::new(0, 0, table_natural_width(), 12));

        table.render(
            Rect::new(0, 0, table_natural_width(), 12),
            &mut table_buffer,
            &mut table_state,
        );

        // Row 11 is the last TOTAL row ("Amount due: ...").
        let line = render_row(&table_buffer, 11, table_natural_width());

        assert!(line.contains("\x1b[0m\x1b[90m"), "line: {line}");
        assert!(line.contains("Amount due"), "line: {line}");
    }

    #[test]
    fn quits_on_q_esc_or_ctrl_c() {
        let mut app = App {
            view: View::Loading {
                task: "Starting".to_owned(),
            },
            counters: BillProgress::default(),
            started: Instant::now(),
            frame: 0,
            last_tick: Instant::now(),
        };
        let mut quits = |code: KeyCode, modifiers: KeyModifiers| {
            app.handle_key(KeyEvent::new(code, modifiers)).unwrap()
        };

        assert!(quits(KeyCode::Char('q'), KeyModifiers::empty()));
        assert!(quits(KeyCode::Char('Q'), KeyModifiers::empty()));
        assert!(quits(KeyCode::Esc, KeyModifiers::empty()));
        assert!(quits(KeyCode::Char('c'), KeyModifiers::CONTROL));
        assert!(!quits(KeyCode::Char('h'), KeyModifiers::empty()));
        assert!(!quits(KeyCode::Char('x'), KeyModifiers::empty()));
    }

    #[test]
    fn formats_elapsed_seconds() {
        assert_eq!(format_elapsed(Duration::from_secs(0)), "0s elapsed");
        assert_eq!(format_elapsed(Duration::from_secs(5)), "5s elapsed");
        assert_eq!(format_elapsed(Duration::from_secs(59)), "59s elapsed");
    }

    #[test]
    fn formats_elapsed_minutes_and_seconds() {
        assert_eq!(format_elapsed(Duration::from_secs(60)), "1m 0s elapsed");
        assert_eq!(format_elapsed(Duration::from_secs(65)), "1m 5s elapsed");
        assert_eq!(format_elapsed(Duration::from_secs(125)), "2m 5s elapsed");
    }

    #[test]
    fn groups_counts_with_thousands_separators() {
        assert_eq!(grouped(0), "0");
        assert_eq!(grouped(999), "999");
        assert_eq!(grouped(1_000), "1,000");
        assert_eq!(grouped(12_345), "12,345");
        assert_eq!(grouped(1_234_567), "1,234,567");
    }

    #[test]
    fn labels_live_counts() {
        assert_eq!(counts_label(0, 0, 0), "0 loaded · 0 parsed · 0 aggregated");
        assert_eq!(
            counts_label(12_345, 12_345, 6_789),
            "12,345 loaded · 12,345 parsed · 6,789 aggregated"
        );
    }

    #[test]
    fn picker_selects_with_arrows_and_letters() {
        let key = |code| KeyEvent::new(code, KeyModifiers::empty());

        assert_eq!(pick_key(key(KeyCode::Down), 0, 8, 4), PickAction::MoveDown);
        assert_eq!(
            pick_key(key(KeyCode::Char('j')), 0, 8, 4),
            PickAction::MoveDown
        );
        assert_eq!(pick_key(key(KeyCode::Up), 1, 8, 4), PickAction::MoveUp);
        assert_eq!(
            pick_key(key(KeyCode::Char('k')), 1, 8, 4),
            PickAction::MoveUp
        );
        assert_eq!(
            pick_key(key(KeyCode::Enter), 3, 8, 4),
            PickAction::Select(3)
        );
        assert_eq!(
            pick_key(key(KeyCode::Char('7')), 0, 8, 4),
            PickAction::Select(6)
        );
        assert_eq!(
            pick_key(key(KeyCode::Char('9')), 0, 8, 4),
            PickAction::Nothing
        );
        assert_eq!(
            pick_key(key(KeyCode::Char('3')), 0, 2, 4),
            PickAction::Nothing
        );
    }

    #[test]
    fn picker_switches_tabs_with_tab_arrows_and_letters() {
        let key = |code| KeyEvent::new(code, KeyModifiers::empty());

        assert_eq!(pick_key(key(KeyCode::Tab), 0, 8, 4), PickAction::NextTab);
        assert_eq!(pick_key(key(KeyCode::Right), 0, 8, 4), PickAction::NextTab);
        assert_eq!(
            pick_key(key(KeyCode::Char('l')), 0, 8, 4),
            PickAction::NextTab
        );
        assert_eq!(
            pick_key(key(KeyCode::BackTab), 0, 8, 4),
            PickAction::PrevTab
        );
        assert_eq!(pick_key(key(KeyCode::Left), 0, 8, 4), PickAction::PrevTab);
        assert_eq!(
            pick_key(key(KeyCode::Char('h')), 0, 8, 4),
            PickAction::PrevTab
        );
    }

    #[test]
    fn picker_quits_on_q_esc_or_ctrl_c() {
        let key = |code, modifiers| KeyEvent::new(code, modifiers);

        assert_eq!(
            pick_key(key(KeyCode::Char('q'), KeyModifiers::empty()), 0, 8, 4),
            PickAction::Quit
        );
        assert_eq!(
            pick_key(key(KeyCode::Char('Q'), KeyModifiers::empty()), 0, 8, 4),
            PickAction::Quit
        );
        assert_eq!(
            pick_key(key(KeyCode::Esc, KeyModifiers::empty()), 0, 8, 4),
            PickAction::Quit
        );
        assert_eq!(
            pick_key(key(KeyCode::Char('c'), KeyModifiers::CONTROL), 0, 8, 4),
            PickAction::Quit
        );
    }

    #[test]
    fn picker_ignores_moves_and_selects_on_an_empty_list() {
        let key = |code| KeyEvent::new(code, KeyModifiers::empty());

        assert_eq!(pick_key(key(KeyCode::Enter), 0, 0, 4), PickAction::Nothing);
        assert_eq!(pick_key(key(KeyCode::Down), 0, 0, 4), PickAction::MoveDown);
    }

    #[test]
    fn picker_ignores_the_zero_key() {
        let key = |code| KeyEvent::new(code, KeyModifiers::empty());

        assert_eq!(
            pick_key(key(KeyCode::Char('0')), 0, 8, 4),
            PickAction::Nothing
        );
    }

    #[test]
    fn picker_clamps_enter_to_the_last_row() {
        let key = |code| KeyEvent::new(code, KeyModifiers::empty());

        assert_eq!(
            pick_key(key(KeyCode::Enter), 9, 3, 4),
            PickAction::Select(2)
        );
    }

    #[test]
    fn picker_rows_put_the_date_before_the_title() {
        let root = RecentRoot {
            id: "ses_a".to_owned(),
            title: "A root session".to_owned(),
            created_ms: Some(1_786_315_623_640),
        };
        let tabs = build_tabs(vec![root], NaiveDate::from_ymd_opt(2026, 8, 11).unwrap());
        let lines = picker_lines(&tabs, 0, 0);
        let rendered = lines[1].to_string();
        let title_at = rendered.find("A root session");
        let time_at = rendered.find('T');

        assert!(rendered.starts_with("▶ "), "line: {rendered}");
        assert!(
            title_at
                .zip(time_at)
                .is_some_and(|(title, time)| title > time),
            "the date must come before the title: {rendered}"
        );
        assert!(
            lines[2].to_string().contains(PICKER_HINT),
            "line: {}",
            lines[2]
        );
    }

    #[test]
    fn picker_opens_on_the_session_tab_first() {
        let tabs = build_tabs(Vec::new(), NaiveDate::from_ymd_opt(2026, 8, 11).unwrap());

        assert_eq!(tabs.len(), 4);
        assert_eq!(tabs[0].title, "session");
        assert_eq!(tabs[1].title, "week");
        assert_eq!(tabs[2].title, "month");
        assert_eq!(tabs[3].title, "year");
    }

    #[test]
    fn picker_draws_a_tab_line_above_the_options() {
        let tabs = build_tabs(Vec::new(), NaiveDate::from_ymd_opt(2026, 8, 11).unwrap());
        let lines = picker_lines(&tabs, 1, 0);
        let tab_line = lines[0].to_string();

        assert!(tab_line.contains("[ session ]"), "line: {tab_line}");
        assert!(tab_line.contains("[ week ]"), "line: {tab_line}");
        assert!(tab_line.contains("[ month ]"), "line: {tab_line}");
        assert!(tab_line.contains("[ year ]"), "line: {tab_line}");
        assert!(lines[1].to_string().contains("Week "), "line: {}", lines[1]);
    }

    #[test]
    fn picker_lists_the_last_eight_weeks_across_year_boundaries() {
        let weeks = last_weeks(NaiveDate::from_ymd_opt(2027, 1, 1).unwrap());

        assert_eq!(weeks.len(), 8);
        // 2027-01-01 is a Friday in ISO week 53 of 2026.
        assert_eq!(
            weeks[0],
            Pick::Week {
                year: 2026,
                week: 53
            }
        );
        assert_eq!(
            weeks[7],
            Pick::Week {
                year: 2026,
                week: 46
            }
        );
    }

    #[test]
    fn picker_lists_the_last_eight_months() {
        let months = last_months(NaiveDate::from_ymd_opt(2026, 8, 11).unwrap());

        assert_eq!(months.len(), 8);
        assert_eq!(
            months[0],
            Pick::Month {
                year: 2026,
                month: 8
            }
        );
        assert_eq!(
            months[7],
            Pick::Month {
                year: 2026,
                month: 1
            }
        );
    }

    #[test]
    fn picker_months_cross_the_year_boundary() {
        let months = last_months(NaiveDate::from_ymd_opt(2026, 3, 11).unwrap());

        assert_eq!(months.len(), 8);
        assert_eq!(
            months[5],
            Pick::Month {
                year: 2025,
                month: 10
            }
        );
        assert_eq!(
            months[7],
            Pick::Month {
                year: 2025,
                month: 8
            }
        );
    }

    #[test]
    fn picker_lists_the_last_eight_years() {
        let years = last_years(NaiveDate::from_ymd_opt(2026, 8, 11).unwrap());

        assert_eq!(years.len(), 8);
        assert_eq!(years[0], Pick::Year { year: 2026 });
        assert_eq!(years[7], Pick::Year { year: 2019 });
    }

    #[test]
    fn window_options_carry_their_bill_labels() {
        assert_eq!(
            Pick::Week {
                year: 2026,
                week: 32
            }
            .label(),
            "Week 32, 2026"
        );
        assert_eq!(
            Pick::Month {
                year: 2026,
                month: 2
            }
            .label(),
            "February 2026"
        );
        assert_eq!(Pick::Year { year: 2025 }.label(), "2025");
    }

    #[test]
    fn picker_options_resolve_into_bill_requests() {
        let root = RecentRoot {
            id: "ses_a".to_owned(),
            title: "A root session".to_owned(),
            created_ms: None,
        };

        assert_eq!(
            request_for(&Pick::Session(root.clone())),
            crate::Request::Session("ses_a".to_owned())
        );
        assert_eq!(
            request_for(&Pick::Week {
                year: 2026,
                week: 32
            }),
            crate::Request::Week(2026, 32)
        );
        assert_eq!(
            request_for(&Pick::Month {
                year: 2026,
                month: 8
            }),
            crate::Request::Month(2026, 8)
        );
        assert_eq!(
            request_for(&Pick::Year { year: 2025 }),
            crate::Request::Year(2025)
        );
    }

    #[test]
    fn picker_height_grows_with_the_row_count_when_the_terminal_allows() {
        assert_eq!(picker_height(0, 24), 6);
        assert_eq!(picker_height(3, 24), 8);
        assert_eq!(picker_height(8, 24), 13);
    }

    #[test]
    fn picker_height_clamps_to_the_terminal_height() {
        assert_eq!(picker_height(8, 10), 9);
        assert_eq!(picker_height(8, 4), 5);
    }
}
