//! TUI frontend: a loading indicator, then the scrollable ratatui report.

use std::io;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyEventKind,
    KeyModifiers, MouseEventKind,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use opencode_bill::{BillReport, StorageContext, load_report_with_progress};
use ratatui::backend::CrosstermBackend;
use ratatui::buffer::Buffer;
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, Cell, Clear, Paragraph, Row, Scrollbar, ScrollbarOrientation, ScrollbarState,
    StatefulWidget, Table, TableState,
};
use ratatui::{Frame, Terminal};

/// Spin frames for the loading indicator.
const SPINNER_FRAMES: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
/// How often the spinner advances.
const SPINNER_TICK: Duration = Duration::from_millis(100);
/// Maximum event-poll wait; keeps the spinner animating.
const EVENT_TIMEOUT: Duration = Duration::from_millis(100);
/// Unified column count shared by every report table.
const COLUMNS: usize = 11;
/// Lines scrolled per wheel or page input.
const SCROLL_STEP: isize = 3;
/// Natural width of every report table column.
const COLUMN_WIDTHS: [u16; COLUMNS] = [40, 14, 6, 10, 11, 7, 10, 10, 10, 11, 11];
/// Spaces between adjacent table columns.
const COLUMN_SPACING: u16 = 2;

/// One message from the background worker to the UI thread.
enum Progress {
    /// A work phase is about to start.
    Task(String),
    /// The report is ready, or the failure that stopped it.
    Done(Result<BillReport, String>),
}

/// Runs the worker that loads the report in the background.
fn worker(context: StorageContext, reference: String, sender: mpsc::Sender<Progress>) {
    let result = load_report_with_progress(&context, &reference, |task| {
        let _ = sender.send(Progress::Task(task.to_owned()));
    });

    let _ = sender.send(Progress::Done(
        result.map_err(|failure| failure.to_string()),
    ));
}

/// What the UI is showing right now.
enum View {
    /// Report generation is still running.
    Loading { task: String },
    /// The report is ready and can be scrolled.
    Ready {
        report: BillReport,
        rows: Vec<Row<'static>>,
        offset: usize,
        h_offset: usize,
    },
    /// Report generation failed.
    Failed(String),
}

/// UI state that survives from frame to frame.
struct App {
    /// The currently visible view.
    view: View,
    /// When report generation started.
    started: Instant,
    /// Spinner frame counter.
    frame: usize,
    /// When the spinner last advanced.
    last_tick: Instant,
}

/// What the TUI session ended with.
pub enum TuiOutcome {
    /// The report was shown (or the user quit before it finished).
    Success,
    /// The report failed and the failure message that was shown.
    Failed(String),
}

/// Runs the TUI and returns an error message when something fails.
pub fn run(context: StorageContext, reference: String) -> Result<TuiOutcome, String> {
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || worker(context, reference, sender));

    let _guard = TerminalGuard::enter()?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))
        .map_err(|failure| failure.to_string())?;
    terminal.clear().map_err(|failure| failure.to_string())?;

    let mut app = App {
        view: View::Loading {
            task: "Starting".to_owned(),
        },
        started: Instant::now(),
        frame: 0,
        last_tick: Instant::now(),
    };

    while !app.handle_events(&receiver)? {
        app.tick();
        terminal
            .draw(|frame| app.draw(frame))
            .map_err(|failure| failure.to_string())?;
    }

    Ok(match app.view {
        View::Failed(failure) => TuiOutcome::Failed(failure),
        _ => TuiOutcome::Success,
    })
}

/// Enters raw mode and the alternate screen, restoring them on drop.
struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> Result<Self, String> {
        enable_raw_mode().map_err(|failure| failure.to_string())?;
        let guard = TerminalGuard;
        let mut stdout = io::stdout();

        execute!(stdout, EnterAlternateScreen).map_err(|failure| failure.to_string())?;
        execute!(stdout, EnableMouseCapture).map_err(|failure| failure.to_string())?;
        Ok(guard)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let mut stdout = io::stdout();
        let _ = execute!(stdout, DisableMouseCapture);
        let _ = execute!(stdout, LeaveAlternateScreen);
        let _ = disable_raw_mode();
    }
}

impl App {
    /// Polls input and worker messages; returns `true` when the user quits.
    fn handle_events(&mut self, receiver: &mpsc::Receiver<Progress>) -> Result<bool, String> {
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
            Event::Mouse(mouse) => self.handle_mouse(mouse.kind),
            _ => Ok(false),
        }
    }

    /// Applies every pending worker message to the current view.
    fn drain_progress(&mut self, receiver: &mpsc::Receiver<Progress>) {
        loop {
            match receiver.try_recv() {
                Ok(Progress::Task(task)) => {
                    self.view = View::Loading { task };
                }
                Ok(Progress::Done(Ok(report))) => {
                    self.view = View::Ready {
                        rows: build_rows(&report),
                        report,
                        offset: 0,
                        h_offset: 0,
                    };
                }
                Ok(Progress::Done(Err(failure))) => {
                    self.view = View::Failed(failure);
                }
                Err(mpsc::TryRecvError::Empty) => return,
                Err(mpsc::TryRecvError::Disconnected) => {
                    if matches!(self.view, View::Loading { .. }) {
                        self.view =
                            View::Failed("report generation stopped unexpectedly".to_owned());
                    }

                    return;
                }
            }
        }
    }

    /// Handles one key press; returns `true` when the user quits.
    fn handle_key(&mut self, key: KeyEvent) -> Result<bool, String> {
        match key.code {
            KeyCode::Char('q') | KeyCode::Char('Q') | KeyCode::Esc => Ok(true),
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => Ok(true),
            KeyCode::Up | KeyCode::Char('k') => {
                self.scroll(-1);
                Ok(false)
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.scroll(1);
                Ok(false)
            }
            KeyCode::PageUp => {
                self.scroll(-SCROLL_STEP * 4);
                Ok(false)
            }
            KeyCode::PageDown => {
                self.scroll(SCROLL_STEP * 4);
                Ok(false)
            }
            KeyCode::Home => {
                self.scroll_to(0);
                Ok(false)
            }
            KeyCode::End => {
                self.scroll_to(usize::MAX);
                Ok(false)
            }
            KeyCode::Left | KeyCode::Char('h') => {
                self.scroll_horizontal(-SCROLL_STEP);
                Ok(false)
            }
            KeyCode::Right | KeyCode::Char('l') => {
                self.scroll_horizontal(SCROLL_STEP);
                Ok(false)
            }
            _ => Ok(false),
        }
    }

    /// Handles one mouse event; returns `true` when the user quits.
    fn handle_mouse(&mut self, kind: MouseEventKind) -> Result<bool, String> {
        match kind {
            MouseEventKind::ScrollUp => {
                self.scroll(-SCROLL_STEP);
                Ok(false)
            }
            MouseEventKind::ScrollDown => {
                self.scroll(SCROLL_STEP);
                Ok(false)
            }
            _ => Ok(false),
        }
    }

    /// Moves the report scroll offset by a signed step.
    fn scroll(&mut self, delta: isize) {
        if let View::Ready { offset, rows, .. } = &mut self.view {
            let max = rows.len().saturating_sub(1);
            let next = (*offset as isize + delta).clamp(0, max as isize);

            *offset = next as usize;
        }
    }

    /// Jumps the report scroll offset to a content row.
    fn scroll_to(&mut self, target: usize) {
        if let View::Ready { offset, rows, .. } = &mut self.view {
            let max = rows.len().saturating_sub(1);

            *offset = target.min(max);
        }
    }

    /// Moves the report horizontal offset by a signed step.
    fn scroll_horizontal(&mut self, delta: isize) {
        if let View::Ready { h_offset, .. } = &mut self.view {
            *h_offset = (*h_offset as isize + delta * SCROLL_STEP).max(0) as usize;
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
            View::Ready {
                report,
                rows,
                offset,
                h_offset,
            } => draw_report(frame, report, rows, *offset, *h_offset),
            View::Failed(failure) => draw_failure(frame, failure),
        }
    }
}

/// Draws the loading indicator with the current task and elapsed time.
fn draw_loading(frame: &mut Frame, app: &App, task: &str) {
    let area = frame.area();
    let spinner = SPINNER_FRAMES[app.frame % SPINNER_FRAMES.len()];
    let elapsed = app.started.elapsed().as_secs();
    let lines = vec![
        Line::from(Span::styled(
            "opencode-bill",
            Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        )),
        Line::from(vec![
            Span::styled(spinner, Style::new().fg(Color::Yellow)),
            Span::raw(format!(" {task}")),
        ]),
        Line::from(Span::styled(
            format!("{elapsed}s elapsed"),
            Style::new().fg(Color::DarkGray),
        )),
    ];
    let paragraph = Paragraph::new(lines)
        .alignment(Alignment::Center)
        .block(Block::bordered().title("Generating bill"));

    frame.render_widget(Clear, area);
    frame.render_widget(paragraph, centered_rect(60, 5, area));
}

/// Draws the finished report as one scrollable table.
fn draw_report(
    frame: &mut Frame,
    report: &BillReport,
    rows: &[Row<'static>],
    offset: usize,
    h_offset: usize,
) {
    let area = frame.area();
    let chunks = Layout::vertical([
        Constraint::Length(3),
        Constraint::Fill(1),
        Constraint::Length(1),
    ])
    .split(area);
    let table_area = chunks[1];
    // The rightmost column is reserved for the vertical scrollbar.
    let content_area = Rect::new(
        table_area.x,
        table_area.y,
        table_area.width.saturating_sub(1),
        table_area.height,
    );
    let header = Paragraph::new(vec![
        Line::from(Span::styled(
            "OPENCODE SESSION BILL",
            Style::new().add_modifier(Modifier::BOLD),
        )),
        Line::from(format!(
            "Session: {}   Title: {}   Sessions: {} (root + descendants)",
            report.session_id, report.session_title, report.session_count
        )),
    ]);
    let footer = Paragraph::new(Line::from(Span::styled(
        "↑/↓ or j/k scroll · ←/→ or h/l horizontal · q quit",
        Style::new().fg(Color::DarkGray),
    )));

    frame.render_widget(Clear, chunks[0]);
    frame.render_widget(header, chunks[0]);

    let row_count = rows.len();
    let natural_width = table_natural_width();
    let max_h = natural_width.saturating_sub(content_area.width);
    let h = h_offset.min(max_h as usize) as u16;
    // Never scroll past the last full screenful of rows.
    let offset = offset.min(row_count.saturating_sub(content_area.height as usize));
    let table = Table::new(rows.to_vec(), column_widths()).column_spacing(COLUMN_SPACING);
    let mut table_state = TableState::new().with_offset(offset);
    let mut table_buffer = Buffer::empty(Rect::new(0, 0, natural_width, content_area.height));

    table.render(
        Rect::new(0, 0, natural_width, content_area.height),
        &mut table_buffer,
        &mut table_state,
    );

    // Blit the visible horizontal window of the table into the frame.
    {
        let frame_buffer = frame.buffer_mut();

        for y in 0..content_area.height {
            for x in 0..content_area.width {
                let source_x = h + x;

                if source_x < natural_width {
                    let cell = table_buffer[(source_x, y)].clone();
                    frame_buffer[(content_area.x + x, content_area.y + y)] = cell;
                } else {
                    frame_buffer[(content_area.x + x, content_area.y + y)].reset();
                }
            }
        }
    }

    if row_count > content_area.height as usize {
        let mut scrollbar_state = ScrollbarState::new(row_count)
            .position(offset)
            .viewport_content_length(content_area.height as usize);
        frame.render_stateful_widget(
            Scrollbar::default().orientation(ScrollbarOrientation::VerticalRight),
            table_area,
            &mut scrollbar_state,
        );
    }

    frame.render_widget(Clear, chunks[2]);
    frame.render_widget(footer, chunks[2]);
}

/// Draws a centered failure message.
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
    frame.render_widget(paragraph, centered_rect(80, 5, area));
}

/// Builds every report row as one 11-column table, padded to a shared grid.
fn build_rows(report: &BillReport) -> Vec<Row<'static>> {
    let mut rows = Vec::new();

    rows.push(header_row(&[
        "PROVIDER / MODEL",
        "REQUESTS",
        "INPUT",
        "CACHE READ",
        "CACHE WRITE",
        "OUTPUT",
        "REASONING",
        "API TIME",
        "WALL TIME",
        "COST (USD)",
    ]));
    rows.extend(data_rows(&report.usage_rows));

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
        "WALL TIME",
        "COST (USD)",
    ]));
    rows.extend(data_rows(&report.agent_rows));

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
    }

    rows.push(title_row("TOTAL"));
    for (label, value) in &report.totals {
        rows.push(totals_row(label, value));
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

/// A rect centered on `area`, clamped to its size.
fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let x = area.x + (area.width.saturating_sub(width)) / 2;
    let y = area.y + (area.height.saturating_sub(height)) / 2;

    Rect::new(x, y, width.min(area.width), height.min(area.height))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_report() -> BillReport {
        BillReport {
            session_id: "ses_abcdef123456".to_owned(),
            session_title: "Test session".to_owned(),
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
                "0ms".to_owned(),
                "$0.125000".to_owned(),
            ]],
            pricing_rows: Vec::new(),
            totals: vec![
                ("Requests:".to_owned(), "1".to_owned()),
                ("Amount due:".to_owned(), "$0.125000 USD".to_owned()),
            ],
        }
    }

    #[test]
    fn builds_unified_report_table() {
        let rows = build_rows(&fixture_report());

        // usage header + row, agent title + header + row, total title + 2 rows.
        assert_eq!(rows.len(), 8);
    }

    #[test]
    fn pads_rows_to_the_shared_column_count() {
        let cells = padded_cells(vec![Cell::from("x"), Cell::from("y")]);

        assert_eq!(cells.len(), COLUMNS);
    }

    #[test]
    fn scrolls_only_within_the_report() {
        let mut app = App {
            view: View::Ready {
                rows: build_rows(&fixture_report()),
                report: fixture_report(),
                offset: 0,
                h_offset: 0,
            },
            started: Instant::now(),
            frame: 0,
            last_tick: Instant::now(),
        };

        app.scroll(-5);
        assert!(matches!(&app.view, View::Ready { offset: 0, .. }));

        app.scroll(1000);
        assert!(matches!(&app.view, View::Ready { offset: 7, .. }));

        app.scroll_to(usize::MAX);
        assert!(matches!(&app.view, View::Ready { offset: 7, .. }));
    }

    #[test]
    fn scrolls_horizontally_without_going_negative() {
        let mut app = App {
            view: View::Ready {
                rows: build_rows(&fixture_report()),
                report: fixture_report(),
                offset: 0,
                h_offset: 0,
            },
            started: Instant::now(),
            frame: 0,
            last_tick: Instant::now(),
        };

        app.scroll_horizontal(-10);
        assert!(matches!(&app.view, View::Ready { h_offset: 0, .. }));

        app.scroll_horizontal(5);
        assert!(matches!(&app.view, View::Ready { h_offset: 15, .. }));
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
    fn computes_natural_table_width() {
        assert_eq!(table_natural_width(), 160);
    }
}
