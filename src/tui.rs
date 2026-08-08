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

use crossterm::cursor::{MoveTo, Show};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::style::Print;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use opencode_bill::{
    BillHeader, BillProgress, BillReport, StorageContext, graph_lines,
    load_month_report_with_progress_counted, load_report_with_progress_counted,
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
/// Unified column count shared by every report table.
const COLUMNS: usize = 11;
/// Natural width of every report table column.
///
/// Every section (usage, agent, pricing) shares one 11-column grid, so each
/// width must fit the longest header that lands in that column: col2 carries
/// "CACHE READ" in the pricing table, col3 carries "CACHE WRITE" in pricing,
/// col5 carries "CACHE WRITE" in the agent table.
const COLUMN_WIDTHS: [u16; COLUMNS] = [40, 14, 10, 11, 11, 11, 10, 10, 10, 11, 11];
/// Spaces between adjacent table columns.
const COLUMN_SPACING: u16 = 2;

/// One message from the background worker to the UI task.
enum Progress {
    /// A work phase is about to start.
    Task(String),
    /// The report is ready, or the failure that stopped it.
    Done(Result<BillReport, String>),
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
    };

    let _ = sender.send(Progress::Done(
        result.map_err(|failure| failure.to_string()),
    ));
}

/// What the UI is showing right now.
enum View {
    /// Report generation is still running.
    Loading { task: String },
    /// The report is ready and will be printed as a styled table.
    Ready { report: BillReport },
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
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let counters = BillProgress::default();
    let worker_counters = counters.clone();

    let worker_task = tokio::spawn(worker(context, request, providers, worker_counters, sender));

    let mut guard = TerminalGuard::enter()?;
    // Start on a fresh row so the viewport does not overwrite the shell prompt line.
    execute!(io::stdout(), Print("\n")).map_err(|failure| failure.to_string())?;
    let mut terminal = Terminal::with_options(
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

            for row in viewport.y + 1..=viewport.y + viewport.height {
                write!(stdout, "\x1b[{row};1H\x1b[K").map_err(|failure| failure.to_string())?;
            }
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
        for y in 0..row_count {
            stdout
                .write_all(render_row(&table_buffer, y, natural_width).as_bytes())
                .map_err(|failure| failure.to_string())?;
        }

        // The year bill's bar graphs are full-width lines, so they print
        // after the table instead of inside its 11-column grid.
        if let BillHeader::Year { .. } = &report.header {
            for graph in &report.graphs {
                stdout
                    .write_all(b"\n")
                    .map_err(|failure| failure.to_string())?;

                for line in graph_lines(graph) {
                    stdout
                        .write_all(line.as_bytes())
                        .map_err(|failure| failure.to_string())?;
                    stdout
                        .write_all(b"\n")
                        .map_err(|failure| failure.to_string())?;
                }
            }
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
                Ok(Progress::Done(Ok(report))) => self.view = View::Ready { report },
                Ok(Progress::Done(Err(failure))) => self.view = View::Failed(failure),
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
        Rect::new(
            area.x,
            area.y,
            60.min(area.width),
            VIEWPORT_HEIGHT.min(area.height),
        ),
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

/// Builds every report row as one 11-column table, padded to a shared grid.
fn build_rows(report: &BillReport) -> Vec<Row<'static>> {
    let mut rows = Vec::new();

    // Month, day, and year bills carry their label; session bills have no
    // header row here.
    if let BillHeader::Month { label } | BillHeader::Day { label } | BillHeader::Year { label } =
        &report.header
    {
        rows.push(title_row(label));
    }

    rows.push(header_row(&[
        "PROVIDER / MODEL",
        "REQUESTS",
        "INPUT",
        "CACHE READ",
        "CACHE WRITE",
        "OUTPUT",
        "REASONING",
        "API TIME",
        "COST (USD)",
    ]));
    rows.extend(data_rows(&report.usage_rows));
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
    rows.extend(data_rows(&report.agent_rows));
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
        rows.extend(data_rows(&report.pricing_rows));
        rows.push(blank_row());
    }

    // The year bill replaces the TOTAL table with full-width bar graphs,
    // printed after the table rows.
    if report.graphs.is_empty() {
        rows.push(title_row("TOTAL"));
        for (label, value) in &report.totals {
            rows.push(totals_row(label, value));
        }
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
fn data_rows(rows: &[Vec<String>]) -> Vec<Row<'static>> {
    rows.iter()
        .map(|row| {
            let cells = row
                .iter()
                .enumerate()
                .map(|(column, cell)| {
                    aligned_cell(
                        Line::from(cell.clone()),
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

/// One dimmed TOTAL row.
fn totals_row(label: &str, value: &str) -> Row<'static> {
    let cells = vec![
        Cell::from(label.to_owned()),
        aligned_cell(Line::from(value.to_owned()), Alignment::Right),
    ];

    padded_row(cells, Style::new().fg(Color::DarkGray))
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

    for x in 0..width {
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
    }
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

        // usage header + row + blank, agent title + header + row + blank,
        // total title + 2 rows.
        assert_eq!(rows.len(), 10);
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
    fn year_report_skips_the_total_table_for_graphs() {
        let mut report = fixture_report();

        report.header = BillHeader::Year {
            label: "2026".to_owned(),
        };
        report.graphs.push(opencode_bill::BarGraph {
            label: "REQUESTS",
            bars: [1; 12],
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

        for y in 0..12 {
            for x in 0..table_natural_width() {
                text.push_str(table_buffer[(x, y)].symbol());
            }
            text.push('\n');
        }

        assert!(text.contains("2026"), "text: {text}");
        assert!(
            !text.contains("TOTAL"),
            "year report must skip TOTAL: {text}"
        );
    }

    #[test]
    fn pads_rows_to_the_shared_column_count() {
        let cells = padded_cells(vec![Cell::from("x"), Cell::from("y")]);

        assert_eq!(cells.len(), COLUMNS);
    }

    #[test]
    fn computes_natural_table_width() {
        assert_eq!(table_natural_width(), 169);
    }

    #[test]
    fn renders_table_at_natural_width() {
        let rows = build_rows(&fixture_report());
        let table = Table::new(rows, column_widths()).column_spacing(COLUMN_SPACING);
        let mut table_state = TableState::new().with_offset(0);
        let mut table_buffer = Buffer::empty(Rect::new(0, 0, table_natural_width(), 10));

        table.render(
            Rect::new(0, 0, table_natural_width(), 10),
            &mut table_buffer,
            &mut table_state,
        );

        let mut header = String::new();

        for x in 0..table_natural_width() {
            header.push_str(table_buffer[(x, 0)].symbol());
        }

        assert!(header.contains("PROVIDER / MODEL"), "header: {header}");
        assert!(header.contains("REQUESTS"), "header: {header}");
    }

    #[test]
    fn renders_styled_header_row() {
        let rows = build_rows(&fixture_report());
        let table = Table::new(rows, column_widths()).column_spacing(COLUMN_SPACING);
        let mut table_state = TableState::new().with_offset(0);
        let mut table_buffer = Buffer::empty(Rect::new(0, 0, table_natural_width(), 10));

        table.render(
            Rect::new(0, 0, table_natural_width(), 10),
            &mut table_buffer,
            &mut table_state,
        );

        let line = render_row(&table_buffer, 0, table_natural_width());

        assert!(line.starts_with("\x1b[0m\x1b[1;4m"), "line: {line}");
        assert!(line.contains("PROVIDER / MODEL"), "line: {line}");
        assert!(line.ends_with("\x1b[0m\n"), "line: {line}");
    }

    #[test]
    fn renders_agent_header_without_truncation() {
        let rows = build_rows(&fixture_report());
        let table = Table::new(rows, column_widths()).column_spacing(COLUMN_SPACING);
        let mut table_state = TableState::new().with_offset(0);
        let mut table_buffer = Buffer::empty(Rect::new(0, 0, table_natural_width(), 10));

        table.render(
            Rect::new(0, 0, table_natural_width(), 10),
            &mut table_buffer,
            &mut table_state,
        );

        // Row 4 is the agent header; REQUESTS and CACHE WRITE once fit only
        // truncated (QUESTS / E WRITE) because they landed in narrow columns.
        let line = render_row(&table_buffer, 4, table_natural_width());

        assert!(line.contains("AGENT"), "line: {line}");
        assert!(line.contains("SESSIONS"), "line: {line}");
        assert!(line.contains("REQUESTS"), "line: {line}");
        assert!(line.contains("CACHE READ"), "line: {line}");
        assert!(line.contains("CACHE WRITE"), "line: {line}");
        assert!(line.contains("COST (USD)"), "line: {line}");
    }

    #[test]
    fn renders_dimmed_total_row() {
        let rows = build_rows(&fixture_report());
        let table = Table::new(rows, column_widths()).column_spacing(COLUMN_SPACING);
        let mut table_state = TableState::new().with_offset(0);
        let mut table_buffer = Buffer::empty(Rect::new(0, 0, table_natural_width(), 10));

        table.render(
            Rect::new(0, 0, table_natural_width(), 10),
            &mut table_buffer,
            &mut table_state,
        );

        // Row 9 is the last TOTAL row ("Amount due: ...").
        let line = render_row(&table_buffer, 9, table_natural_width());

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
}
