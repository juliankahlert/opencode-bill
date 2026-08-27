//! Orchestration: load a session or month, summarize it, build the report.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::aggregate::{StreamedAggregates, aggregate, aggregate_agents, month_weeks};
use crate::context::{StorageContext, Store};
use crate::data::{
    AgentId, AgentUsage, BillProgress, ModelKey, ModelRates, SessionId, Totals, UsageRow,
    UsageStore, World,
};
use crate::error::BillError;
use crate::parse::{parse_message_batch, parse_metadata, parse_system};
use crate::pricing;
use crate::render::render_bill;
use crate::report::{BarGraph, BillReport, build_report, month_graphs, week_graphs, year_graphs};
use crate::store::{
    CalendarWindow, RawWindowMeta, RecentRoot, SessionStore, StreamBatch, validate_reference,
};
use crate::timing::{earliest, latest};

/// Default monthly spending limit in micro-dollars.
pub const DEFAULT_SPENDING_LIMIT_MICROS: u64 = 250_000_000;

/// Lockless progress counters of one bill pipeline run.
///
/// Every counter is an atomic updated from the parallel workers, so no lock
/// guards them.  The phase counter marks the pipeline stage; the row
/// counters feed the TUI loading view and are asserted by tests.  Systems
/// stay pure: progress is external observation, not state the systems read
/// to decide anything.  The row counters live in the shared
/// [`BillProgress`] handle so an observing UI sees them live.
#[derive(Default)]
pub(crate) struct PipelineProgress {
    /// Pipeline phase: 0 load, 1 parse, 2 price, 3 aggregate, 4 render.
    phase: AtomicUsize,
    /// Rows read, parsed, and folded, shared with the observing UI.
    counters: BillProgress,
}

impl PipelineProgress {
    /// Wraps shared counters so the pipeline and an observer see one count.
    pub(crate) fn with_counters(counters: BillProgress) -> Self {
        PipelineProgress {
            phase: AtomicUsize::new(0),
            counters,
        }
    }

    /// The shared counters, for stores that read rows on another thread.
    pub(crate) fn counters(&self) -> &BillProgress {
        &self.counters
    }

    /// Marks the pipeline phase that is about to run.
    pub(crate) fn note_phase(&self, phase: usize) {
        self.phase.store(phase, Ordering::Relaxed);
    }

    /// Counts one storage row processed by the parse phase.
    pub(crate) fn parsed_row(&self) {
        self.counters.parsed_row();
    }

    /// Counts one folded row from an aggregate worker.
    pub(crate) fn folded_row(&self) {
        self.counters.folded_row();
    }
}

/// Runs a future on the current tokio runtime, creating one when absent.
///
/// The public entry points stay synchronous; the pipeline internals run on
/// tokio so the store read can run on the blocking pool.  The GitHub pricing
/// fetch runs inline in the pricing phase, gated on the bill using
/// `github-copilot`.  Inside a runtime the calling thread parks with
/// `block_in_place`, so even a single-core multi-thread runtime cannot
/// deadlock waiting for a spawned task.  A `current_thread` runtime polls
/// spawned tasks from the same `block_on`, so it needs no parking.
///
/// Contract: when a runtime exists it must be entered on its own thread — a
/// `current_thread` handle entered from a foreign thread panics inside
/// tokio.  All in-crate callers (CLI and TUI tasks on the `#[tokio::main]`
/// runtime) satisfy this; external users should call the public functions
/// from plain synchronous code.  Blind spot: a foreign runtime configured
/// with `max_blocking_threads(1)` can deadlock with `block_in_place` +
/// `spawn_blocking` on a single task; crate-created runtimes use tokio
/// defaults.  The streaming window load parks two blocking-pool threads
/// (pump and parse) against each other over a bounded channel, so a foreign
/// runtime with fewer than two blocking threads can starve it the same way.
fn block_on<F: Future>(future: F) -> Result<F::Output, BillError> {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::CurrentThread => Ok(handle.block_on(future)),
            _ => Ok(tokio::task::block_in_place(|| handle.block_on(future))),
        },
        Err(_) => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(BillError::Runtime)?;

            Ok(runtime.block_on(future))
        }
    }
}

/// Generates a deterministic plain-text bill for one OpenCode session.
///
/// When `providers` is non-empty, only usage from those providers is included.
pub fn generate_bill(
    context: &StorageContext,
    reference: &str,
    providers: &[String],
) -> Result<String, BillError> {
    let report = load_report_with_progress(context, reference, providers, |_| {})?;

    render_bill(&report)
}

/// Generates a deterministic plain-text bill for one calendar month.
///
/// The month is a month number 1..=12 of the given year; every session whose
/// messages were recorded in that window is included. When `providers` is
/// non-empty, only usage from those providers is included.
pub fn generate_monthly_bill(
    context: &StorageContext,
    year: u32,
    month: u32,
    providers: &[String],
) -> Result<String, BillError> {
    generate_monthly_bill_with_limit(
        context,
        year,
        month,
        providers,
        DEFAULT_SPENDING_LIMIT_MICROS,
    )
}

/// Generates a deterministic plain-text bill for one calendar month with a
/// configurable cumulative weekly spending limit graph.
pub fn generate_monthly_bill_with_limit(
    context: &StorageContext,
    year: u32,
    month: u32,
    providers: &[String],
    limit_micros: u64,
) -> Result<String, BillError> {
    let report = load_month_report_with_progress_counted_with_limit(
        context,
        year,
        month,
        providers,
        |_| {},
        &BillProgress::default(),
        limit_micros,
    )?;

    render_bill(&report)
}

/// Generates a deterministic plain-text bill for one calendar day.
///
/// The day is a day number of the given year and month; every session whose
/// messages were recorded in that window is included. When `providers` is
/// non-empty, only usage from those providers is included.
pub fn generate_daily_bill(
    context: &StorageContext,
    year: u32,
    month: u32,
    day: u32,
    providers: &[String],
) -> Result<String, BillError> {
    let report = load_day_report_with_progress(context, year, month, day, providers, |_| {})?;

    render_bill(&report)
}

/// Generates a deterministic plain-text bill for one calendar year.
///
/// The year is a year number; every session whose messages were recorded in
/// that window is included.  The report carries the full usage tables plus
/// one bar graph per TOTAL metric of a month bill, with a vertical bar per
/// month and the year total shown as a value. When `providers` is non-empty,
/// only usage from those providers is included.
pub fn generate_yearly_bill(
    context: &StorageContext,
    year: u32,
    providers: &[String],
) -> Result<String, BillError> {
    let report = load_year_report_with_progress(context, year, providers, |_| {})?;

    render_bill(&report)
}

/// Generates a deterministic plain-text bill for one calendar week.
///
/// The week is an ISO week number 1..=53 of the given year; every session
/// whose messages were recorded in that window is included.  The report
/// carries the full usage tables plus one bar graph per TOTAL metric of a
/// day bill, with a vertical bar per weekday and the week total shown as a
/// value. When `providers` is non-empty, only usage from those providers is
/// included.
pub fn generate_weekly_bill(
    context: &StorageContext,
    year: u32,
    week: u32,
    providers: &[String],
) -> Result<String, BillError> {
    let report = load_week_report_with_progress(context, year, week, providers, |_| {})?;

    render_bill(&report)
}

/// Lists the most recent root sessions, newest first, up to a limit.
///
/// A root session is a session without a parent (in SQLite, `parent_id IS
/// NULL`; in legacy JSON, a file without a `parentID`).  The interactive
/// no-argument picker uses this list.  The query never counts rows into a
/// progress handle, and the creation time is `None` when the store does not
/// record one.
pub fn recent_roots(context: &StorageContext, limit: usize) -> Result<Vec<RecentRoot>, BillError> {
    let data_dir = context.data_dir.clone();

    block_on(blocking_load(move || {
        Store::select(&data_dir).recent_roots(limit)
    }))?
}

/// Loads a session and builds the report data, reporting each work step.
///
/// `on_task` runs before every phase (loading, parsing, pricing, aggregation,
/// report building) so a UI can show which step is currently in progress.
/// When `providers` is non-empty, only usage from those providers is included.
///
/// The pipeline runs on tokio internally (store read on the blocking pool,
/// gated GitHub pricing fetch in the pricing phase); the function itself
/// stays synchronous.
pub fn load_report_with_progress(
    context: &StorageContext,
    reference: &str,
    providers: &[String],
    on_task: impl FnMut(&str),
) -> Result<BillReport, BillError> {
    let counters = BillProgress::default();

    load_report_with_progress_counted(context, reference, providers, on_task, &counters)
}

/// Loads a session and builds the report data, reporting each work step and
/// counting live progress.
///
/// Like [`load_report_with_progress`], but the pipeline also writes the
/// loaded, parsed, and folded row counts into the shared `counters` handle
/// while it runs, so a UI on another task can show live numbers.
pub fn load_report_with_progress_counted(
    context: &StorageContext,
    reference: &str,
    providers: &[String],
    on_task: impl FnMut(&str),
    counters: &BillProgress,
) -> Result<BillReport, BillError> {
    let mut on_task = on_task;
    let progress = PipelineProgress::with_counters(counters.clone());

    block_on(load_report_async(
        context,
        reference,
        providers,
        &mut on_task,
        &progress,
    ))?
}

/// Loads one calendar month and builds the report data, reporting each step.
///
/// `on_task` runs before every phase (loading, parsing, pricing, aggregation,
/// report building) so a UI can show which step is currently in progress.
/// When `providers` is non-empty, only usage from those providers is included.
///
/// The pipeline runs on tokio internally (store read on the blocking pool,
/// gated GitHub pricing fetch in the pricing phase); the function itself
/// stays synchronous.
pub fn load_month_report_with_progress(
    context: &StorageContext,
    year: u32,
    month: u32,
    providers: &[String],
    on_task: impl FnMut(&str),
) -> Result<BillReport, BillError> {
    let counters = BillProgress::default();

    load_month_report_with_progress_counted_with_limit(
        context,
        year,
        month,
        providers,
        on_task,
        &counters,
        DEFAULT_SPENDING_LIMIT_MICROS,
    )
}

/// Loads one calendar month and builds the report data, reporting each work
/// step and counting live progress.
///
/// Like [`load_month_report_with_progress`], but the pipeline also writes the
/// loaded, parsed, and folded row counts into the shared `counters` handle
/// while it runs, so a UI on another task can show live numbers.
pub fn load_month_report_with_progress_counted(
    context: &StorageContext,
    year: u32,
    month: u32,
    providers: &[String],
    on_task: impl FnMut(&str),
    counters: &BillProgress,
) -> Result<BillReport, BillError> {
    load_month_report_with_progress_counted_with_limit(
        context,
        year,
        month,
        providers,
        on_task,
        counters,
        DEFAULT_SPENDING_LIMIT_MICROS,
    )
}

/// Loads one calendar month and builds the report data with a spending limit.
pub fn load_month_report_with_progress_counted_with_limit(
    context: &StorageContext,
    year: u32,
    month: u32,
    providers: &[String],
    on_task: impl FnMut(&str),
    counters: &BillProgress,
    limit_micros: u64,
) -> Result<BillReport, BillError> {
    let mut on_task = on_task;
    let progress = PipelineProgress::with_counters(counters.clone());

    block_on(load_month_report_async(
        context,
        year,
        month,
        providers,
        limit_micros,
        &mut on_task,
        &progress,
    ))?
}

/// Loads one calendar day and builds the report data, reporting each step.
///
/// `on_task` runs before every phase (loading, parsing, pricing, aggregation,
/// report building) so a UI can show which step is currently in progress.
/// When `providers` is non-empty, only usage from those providers is included.
///
/// The pipeline runs on tokio internally (store read on the blocking pool,
/// gated GitHub pricing fetch in the pricing phase); the function itself
/// stays synchronous.
pub fn load_day_report_with_progress(
    context: &StorageContext,
    year: u32,
    month: u32,
    day: u32,
    providers: &[String],
    on_task: impl FnMut(&str),
) -> Result<BillReport, BillError> {
    let counters = BillProgress::default();

    load_day_report_with_progress_counted(context, year, month, day, providers, on_task, &counters)
}

/// Loads one calendar day and builds the report data, reporting each work
/// step and counting live progress.
///
/// Like [`load_day_report_with_progress`], but the pipeline also writes the
/// loaded, parsed, and folded row counts into the shared `counters` handle
/// while it runs, so a UI on another task can show live numbers.
pub fn load_day_report_with_progress_counted(
    context: &StorageContext,
    year: u32,
    month: u32,
    day: u32,
    providers: &[String],
    on_task: impl FnMut(&str),
    counters: &BillProgress,
) -> Result<BillReport, BillError> {
    let mut on_task = on_task;
    let progress = PipelineProgress::with_counters(counters.clone());

    block_on(load_day_report_async(
        context,
        year,
        month,
        day,
        providers,
        &mut on_task,
        &progress,
    ))?
}

/// Loads one calendar year and builds the report data, reporting each step.
///
/// `on_task` runs before every phase (loading, parsing, pricing, aggregation,
/// report building) so a UI can show which step is currently in progress.
/// When `providers` is non-empty, only usage from those providers is included.
///
/// The pipeline runs on tokio internally (store read on the blocking pool,
/// gated GitHub pricing fetch in the pricing phase); the function itself
/// stays synchronous.
pub fn load_year_report_with_progress(
    context: &StorageContext,
    year: u32,
    providers: &[String],
    on_task: impl FnMut(&str),
) -> Result<BillReport, BillError> {
    let counters = BillProgress::default();

    load_year_report_with_progress_counted(context, year, providers, on_task, &counters)
}

/// Loads one calendar year and builds the report data, reporting each work
/// step and counting live progress.
///
/// Like [`load_year_report_with_progress`], but the pipeline also writes the
/// loaded, parsed, and folded row counts into the shared `counters` handle
/// while it runs, so a UI on another task can show live numbers.
pub fn load_year_report_with_progress_counted(
    context: &StorageContext,
    year: u32,
    providers: &[String],
    on_task: impl FnMut(&str),
    counters: &BillProgress,
) -> Result<BillReport, BillError> {
    let mut on_task = on_task;
    let progress = PipelineProgress::with_counters(counters.clone());

    block_on(load_year_report_async(
        context,
        year,
        providers,
        &mut on_task,
        &progress,
    ))?
}

/// Loads one calendar week and builds the report data, reporting each step.
///
/// `on_task` runs before every phase (loading, parsing, pricing, aggregation,
/// report building) so a UI can show which step is currently in progress.
/// When `providers` is non-empty, only usage from those providers is included.
///
/// The pipeline runs on tokio internally (store read on the blocking pool,
/// gated GitHub pricing fetch in the pricing phase); the function itself
/// stays synchronous.
pub fn load_week_report_with_progress(
    context: &StorageContext,
    year: u32,
    week: u32,
    providers: &[String],
    on_task: impl FnMut(&str),
) -> Result<BillReport, BillError> {
    let counters = BillProgress::default();

    load_week_report_with_progress_counted(context, year, week, providers, on_task, &counters)
}

/// Loads one calendar week and builds the report data, reporting each work
/// step and counting live progress.
///
/// Like [`load_week_report_with_progress`], but the pipeline also writes the
/// loaded, parsed, and folded row counts into the shared `counters` handle
/// while it runs, so a UI on another task can show live numbers.
pub fn load_week_report_with_progress_counted(
    context: &StorageContext,
    year: u32,
    week: u32,
    providers: &[String],
    on_task: impl FnMut(&str),
    counters: &BillProgress,
) -> Result<BillReport, BillError> {
    let mut on_task = on_task;
    let progress = PipelineProgress::with_counters(counters.clone());

    block_on(load_week_report_async(
        context,
        year,
        week,
        providers,
        &mut on_task,
        &progress,
    ))?
}

/// The async session pipeline: loads the store on the blocking pool, then
/// parses, prices, aggregates, and renders on the calling thread.  The
/// GitHub pricing page is fetched only when the bill uses `github-copilot`.
async fn load_report_async(
    context: &StorageContext,
    reference: &str,
    providers: &[String],
    on_task: &mut impl FnMut(&str),
    progress: &PipelineProgress,
) -> Result<BillReport, BillError> {
    on_task("Loading session data");
    progress.note_phase(0);
    let world = load_system(context, reference, progress, on_task).await?;

    summarize_async(context, world, providers, on_task, progress).await
}

/// The async month pipeline: streams the store on the blocking pool one day
/// at a time while a second blocking task parses and folds each day's batch,
/// then renders with the per-calendar-week bar graphs.  The GitHub pricing
/// page is fetched only when the bill uses `github-copilot`.
async fn load_month_report_async(
    context: &StorageContext,
    year: u32,
    month: u32,
    providers: &[String],
    limit_micros: u64,
    on_task: &mut impl FnMut(&str),
    progress: &PipelineProgress,
) -> Result<BillReport, BillError> {
    on_task("Loading month data");
    progress.note_phase(0);
    let window = CalendarWindow::month(year, month)?;
    let weeks = month_weeks(year as i32, month).unwrap_or_default();
    let today = chrono::Utc::now().date_naive();
    let streamed = load_streaming_system(
        context,
        &window,
        GraphMode::Month {
            year: year as i32,
            month,
            limit_micros,
            today,
        },
        weeks,
        providers,
        progress,
        on_task,
    )
    .await?;

    summarize_streamed_async(
        streamed,
        providers,
        GraphMode::Month {
            year: year as i32,
            month,
            limit_micros,
            today,
        },
        on_task,
        progress,
    )
}

/// The async day pipeline: streams the store on the blocking pool (a day
/// window is one day chunk) while a second blocking task parses and folds
/// each batch, then renders.  The GitHub pricing page is fetched only when
/// the bill uses `github-copilot`.
async fn load_day_report_async(
    context: &StorageContext,
    year: u32,
    month: u32,
    day: u32,
    providers: &[String],
    on_task: &mut impl FnMut(&str),
    progress: &PipelineProgress,
) -> Result<BillReport, BillError> {
    on_task("Loading day data");
    progress.note_phase(0);
    let window = CalendarWindow::day(year, month, day)?;
    let streamed = load_streaming_system(
        context,
        &window,
        GraphMode::None,
        Vec::new(),
        providers,
        progress,
        on_task,
    )
    .await?;

    summarize_streamed_async(streamed, providers, GraphMode::None, on_task, progress)
}

/// The async year pipeline: streams the store on the blocking pool one day
/// at a time while a second blocking task parses and folds each day's batch,
/// then renders with the month bar graphs.  The GitHub pricing page is
/// fetched only when the bill uses `github-copilot`.
async fn load_year_report_async(
    context: &StorageContext,
    year: u32,
    providers: &[String],
    on_task: &mut impl FnMut(&str),
    progress: &PipelineProgress,
) -> Result<BillReport, BillError> {
    on_task("Loading year data");
    progress.note_phase(0);
    let window = CalendarWindow::year(year)?;
    let streamed = load_streaming_system(
        context,
        &window,
        GraphMode::Year,
        Vec::new(),
        providers,
        progress,
        on_task,
    )
    .await?;

    summarize_streamed_async(streamed, providers, GraphMode::Year, on_task, progress)
}

/// The async week pipeline: streams the store on the blocking pool one day
/// at a time while a second blocking task parses and folds each day's batch,
/// then renders with the per-weekday bar graphs.  The GitHub pricing page is
/// fetched only when the bill uses `github-copilot`.
async fn load_week_report_async(
    context: &StorageContext,
    year: u32,
    week: u32,
    providers: &[String],
    on_task: &mut impl FnMut(&str),
    progress: &PipelineProgress,
) -> Result<BillReport, BillError> {
    on_task("Loading week data");
    progress.note_phase(0);
    let window = CalendarWindow::week(year, week)?;
    let streamed = load_streaming_system(
        context,
        &window,
        GraphMode::Week,
        Vec::new(),
        providers,
        progress,
        on_task,
    )
    .await?;

    summarize_streamed_async(streamed, providers, GraphMode::Week, on_task, progress)
}

/// Which bar graphs a streamed window report carries.
///
/// Year bills carry one graph per month, month bills one graph per calendar
/// week, week bills one graph per weekday; day and session bills carry none
/// and render the plain TOTAL table.  The mode also tells the streaming
/// pipeline which period buckets to fold.
#[derive(Clone, Copy)]
enum GraphMode {
    /// No graphs: the TOTAL table.
    None,
    /// One graph per calendar week of the billed month.
    Month {
        year: i32,
        month: u32,
        limit_micros: u64,
        today: chrono::NaiveDate,
    },
    /// One graph per calendar day of the billed week.
    Week,
    /// One graph per calendar month of the billed year.
    Year,
}

/// The result of one streamed window load: the parse world (without the
/// entity store) and the aggregates every row folded into while it streamed.
struct StreamedBill {
    /// Parse world: header, sessions, interner, wall bounds.
    world: World,
    /// The summaries the rows folded into, row by row.
    aggregates: StreamedAggregates,
    /// Rates resolved lazily per model key, in key order.
    pricing: BTreeMap<ModelKey, ModelRates>,
}

/// Streams one calendar window through the whole pipeline: the metadata
/// first, then the messages one day at a time.  Each day's batch loads,
/// parses, prices, and folds into the summaries in one pass, and the rows
/// are dropped — the memory peak is the summaries plus the in-flight chunks,
/// never the whole window.
///
/// Every window (month, day, or year) loads through this pipeline.  Two
/// tasks on the tokio blocking pool run back to back over a bounded channel:
/// the pump task reads the store and sends one day's raw rows per message,
/// the parse task parses each received day while the pump already reads the
/// next day — the next chunk loads concurrently with the current chunk's
/// parse and fold.  The pump's batches and the parse buffers are reused
/// across the whole stream (the callback returns its drained batch, the
/// decode and row buffers live in the parse task), so dynamic allocation is
/// the exception, not the rule.  The channel capacity bounds the in-flight
/// raw JSON.
///
/// The GitHub pricing page is fetched by the pump before the first batch is
/// sent whenever the window metadata reports a `github-copilot` message, so
/// every row folds with the final rates.
async fn load_streaming_system(
    context: &StorageContext,
    window: &CalendarWindow,
    mode: GraphMode,
    weeks: Vec<u32>,
    providers: &[String],
    progress: &PipelineProgress,
    on_task: &mut impl FnMut(&str),
) -> Result<StreamedBill, BillError> {
    let data_dir = context.data_dir.clone();
    let models_file = context.models_file.clone();
    let counters = progress.counters().clone();
    let window = window.clone();
    let providers = providers.to_vec();

    on_task("Parsing messages");
    progress.note_phase(1);

    // One item per store visit: the window metadata first, then one day's
    // bundle per message.  The pump task produces, the parse task consumes;
    // when the parse side fails it drops the receiver and the pump's next
    // send fails, which stops the stream.  The recycle channel hands the
    // drained bundles back to the pump, so no row buffer is ever
    // reallocated.
    let (sender, receiver) = tokio::sync::mpsc::channel::<StreamItem>(2);
    let (recycle_sender, mut recycle_receiver) = tokio::sync::mpsc::channel::<StreamBatch>(2);
    let cancelled = Arc::new(AtomicBool::new(false));
    // When this future is dropped (ctrl+C or TUI quit mid-stream), the
    // guard flags the tasks so they stop at the next batch instead of
    // streaming the whole window on a dead runtime.
    let _abort_guard = AbortGuard(cancelled.clone());
    let github = Arc::new(pricing::GithubSource::default());
    let pump_counters = counters.clone();
    let pump_github = github.clone();

    let pump = {
        let cancelled = cancelled.clone();
        tokio::task::spawn_blocking(move || {
            let store = Store::select(&data_dir);
            let pipeline = PipelineProgress::with_counters(pump_counters);

            let meta = store.load_window_meta(&window, pipeline.counters())?;

            sender
                .blocking_send(StreamItem::Meta(meta))
                .map_err(|_| BillError::Runtime(std::io::Error::other("parse stopped early")))?;

            // The GitHub pricing page is fetched at most once, when the
            // first github-copilot batch arrives — the payloads are already
            // in hand, so no whole-window pre-scan is needed, and the fetch
            // completes before that batch is sent, so every github row folds
            // with the final rates.  A failed attempt is remembered by the
            // source and never retried.
            let mut github_fetched = false;

            store.stream_window_messages(&window, pipeline.counters(), &mut |batch| {
                if cancelled.load(Ordering::Relaxed) {
                    return Err(BillError::Runtime(std::io::Error::other(
                        "pipeline aborted",
                    )));
                }

                if !github_fetched
                    && batch
                        .messages
                        .iter()
                        .any(|message| message.json.contains("github-copilot"))
                {
                    let _ = blocking_block_on(pump_github.ensure_fetched())?;
                    github_fetched = true;
                }

                sender
                    .blocking_send(StreamItem::Batch(batch))
                    .map_err(|_| {
                        cancelled.store(true, Ordering::Relaxed);

                        BillError::Runtime(std::io::Error::other(
                            "message stream stopped by the parse side",
                        ))
                    })?;

                // The parse side drained the bundle; hand its buffers back
                // (or fresh ones) so the next day reuses the same slots.
                Ok(recycle_receiver.try_recv().unwrap_or(StreamBatch {
                    messages: Vec::new(),
                    parts: Vec::new(),
                }))
            })
        })
    };
    let parse_cancelled = cancelled.clone();
    let parse = tokio::task::spawn_blocking(move || {
        let mut receiver = receiver;
        let pipeline = PipelineProgress::with_counters(counters);

        let StreamItem::Meta(meta) = receiver.blocking_recv().ok_or_else(|| {
            BillError::Runtime(std::io::Error::other("stream ended before metadata"))
        })?
        else {
            return Err(BillError::Runtime(std::io::Error::other(
                "expected window metadata first",
            )));
        };
        let (mut world, parse) = parse_metadata(meta, &pipeline)?;
        let mut aggregates = StreamedAggregates::default();

        aggregates.seed_agents(&world.sessions);
        match mode {
            GraphMode::None | GraphMode::Year => {}
            GraphMode::Month { .. } => aggregates.set_weeks(weeks),
            GraphMode::Week => aggregates.set_weekday_fold(),
        }

        // Pricing sources resolve lazily per key; the catalog and the GitHub
        // page (already fetched by the pump when needed) are shared across
        // the whole stream.
        let catalog = pricing::CatalogSource::new(models_file.as_deref())?;
        let mut pricing = BTreeMap::<ModelKey, ModelRates>::new();
        let wanted = providers
            .iter()
            .map(|provider| world.interner.intern_provider(provider))
            .collect::<BTreeSet<_>>();

        // One decode buffer and one row buffer for the whole stream.
        let mut results = Vec::new();
        let mut rows = Vec::new();

        while let Some(item) = receiver.blocking_recv() {
            if parse_cancelled.load(Ordering::Relaxed) {
                return Err(BillError::Runtime(std::io::Error::other(
                    "pipeline aborted",
                )));
            }

            let StreamItem::Batch(mut batch) = item else {
                return Err(BillError::Runtime(std::io::Error::other(
                    "unexpected item in the message stream",
                )));
            };

            parse_message_batch(
                &mut world,
                &parse,
                &mut batch.parts,
                &mut batch.messages,
                &mut results,
                &mut rows,
                &pipeline,
            )?;

            // Pre-filter: rows outside the requested providers die here,
            // before pricing and folding.
            rows.drain(..)
                .filter(|row| wanted.is_empty() || wanted.contains(&row.provider))
                // Zero-cost estimation folds in the estimated cost when a
                // rate is known; otherwise the recorded (zero) cost stays.
                .map(|row| estimate_streamed_cost(row, &mut pricing, &catalog, &github, &world))
                .for_each(|row| aggregates.fold(&row, &pipeline));

            // The bundle is drained; hand its buffers back to the pump.
            batch.messages.clear();
            let _ = recycle_sender.try_send(batch);
        }

        // The pricing map must cover every key of the bill, not only the
        // rows whose cost needed estimation: resolve the remaining model
        // keys so the MODEL PRICING table matches a whole-store bill.
        let missing = aggregates
            .by_model
            .keys()
            .filter(|key| !pricing.contains_key(key))
            .copied()
            .collect::<Vec<_>>();

        missing
            .into_iter()
            .filter_map(|key| {
                pricing::rates_for_key(&key, &world.interner, &catalog, &github)
                    .map(|rates| (key, rates))
            })
            .for_each(|(key, rates)| {
                pricing.insert(key, rates);
            });

        Ok(StreamedBill {
            world,
            aggregates,
            pricing,
        })
    });

    let pump_result = pump
        .await
        .map_err(|failure| BillError::Runtime(std::io::Error::other(failure.to_string())))?;
    let parse_result = parse
        .await
        .map_err(|failure| BillError::Runtime(std::io::Error::other(failure.to_string())))?;

    if cancelled.load(Ordering::Relaxed) {
        // The parse side failed first and stopped the stream; its error is
        // the root cause, the pump's cancellation is not.
        return parse_result;
    }

    match (pump_result, parse_result) {
        (Err(pump_error), _) => Err(pump_error),
        (Ok(()), Err(parse_error)) => Err(parse_error),
        (Ok(()), Ok(streamed)) => Ok(streamed),
    }
}

/// One item of the streaming pipeline: the window metadata, then one bundle
/// of raw rows per day.
enum StreamItem {
    /// Window metadata (sessions), always first.
    Meta(RawWindowMeta),
    /// One day's raw messages and their tool parts, in window order.
    Batch(StreamBatch),
}

/// Sets the stream's cancellation flag when the pipeline future is dropped.
///
/// Ctrl+C in the TUI drops the runtime mid-stream; the guard flags the pump
/// and parse tasks so they stop at the next batch boundary, and quitting
/// never waits for the whole window to stream.
struct AbortGuard(Arc<AtomicBool>);

impl Drop for AbortGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// Runs an async future to completion from a blocking-pool thread.
///
/// The store pump fetches the GitHub pricing page before the first batch is
/// sent; `Handle::block_on` parks the blocking thread and polls the future
/// on the runtime workers.  The crate's runtimes are multi-thread, so this
/// is safe — a `current_thread` runtime has no workers to park on, the same
/// blind spot [`block_on`] documents.
fn blocking_block_on<F: Future>(future: F) -> Result<F::Output, BillError> {
    let handle = tokio::runtime::Handle::try_current()
        .map_err(|failure| BillError::Runtime(std::io::Error::other(failure.to_string())))?;

    Ok(handle.block_on(future))
}

/// Folds one row with the estimated cost of its unknown-price call.
///
/// Rates resolve lazily per model key, exactly like a whole-window
/// [`pricing::load_pricing`] lookup: catalog, GitHub, generic fallback, then
/// the GitHub page as last resort.  The resolved rates join the pricing map,
/// so the report's MODEL PRICING table covers every key of the bill.
fn estimate_streamed_cost(
    row: UsageRow,
    pricing: &mut BTreeMap<ModelKey, ModelRates>,
    catalog: &pricing::CatalogSource,
    github: &pricing::GithubSource,
    world: &World,
) -> UsageRow {
    if !row.cost_unknown {
        return row;
    }

    let key = (row.provider, row.model);
    let rates = match pricing.get(&key) {
        Some(rates) => Some(*rates),
        None => {
            let rates = pricing::rates_for_key(&key, &world.interner, catalog, github);

            if let Some(rates) = &rates {
                pricing.insert(key, *rates);
            }

            rates
        }
    };

    match rates {
        Some(rates) => UsageRow {
            cost_micros: pricing::estimated_cost_micros(&row.tokens, &rates),
            ..row
        },
        None => row,
    }
}

/// Renders a streamed window bill: scopes the agent summary and wall bounds
/// under a provider filter, merges the bar graphs, and builds the report.
fn summarize_streamed_async(
    mut streamed: StreamedBill,
    providers: &[String],
    mode: GraphMode,
    on_task: &mut impl FnMut(&str),
    progress: &PipelineProgress,
) -> Result<BillReport, BillError> {
    if !providers.is_empty() {
        // Agent classes without kept usage die, and session counts scope to
        // the sessions that produced the kept usage; the wall bounds cover
        // only the kept calls.
        streamed.aggregates.by_agent = keep_agents_with_usage(
            streamed.aggregates.by_agent,
            providers,
            &streamed.aggregates.kept_sessions,
        );
        streamed.world.wall_start = streamed.aggregates.wall_start;
        streamed.world.wall_end = streamed.aggregates.wall_end;
    }

    let total = streamed.aggregates.total();
    let graphs = match mode {
        GraphMode::None => Vec::new(),
        GraphMode::Month {
            year,
            month,
            limit_micros,
            today,
        } => month_graphs(
            year,
            month,
            &streamed.aggregates.week_totals,
            &streamed.aggregates.weeks,
            &total,
            limit_micros,
            today,
        ),
        GraphMode::Week => week_graphs(streamed.aggregates.weekdays, &total),
        GraphMode::Year => year_graphs(streamed.aggregates.months, &total),
    };

    progress.note_phase(4);
    on_task("Building report");
    Ok(build_report(
        &streamed.world,
        &streamed.aggregates.by_model,
        &streamed.aggregates.by_agent,
        &total,
        &streamed.pricing,
        &graphs,
        &streamed.world.interner,
    ))
}

/// Loads a session tree from the store and parses it into a world.
///
/// The store read is I/O bound, so it runs on the tokio blocking pool and
/// keeps the runtime worker threads free for other async I/O; the parse
/// (compute) runs on the calling thread after the join.
async fn load_system(
    context: &StorageContext,
    reference: &str,
    progress: &PipelineProgress,
    on_task: &mut impl FnMut(&str),
) -> Result<World, BillError> {
    validate_reference(reference)?;

    let data_dir = context.data_dir.clone();
    let reference = reference.to_owned();
    let counters = progress.counters().clone();
    let raw = blocking_load(move || Store::select(&data_dir).load(&reference, &counters)).await?;

    on_task("Parsing messages");
    parse_system(raw, progress)
}

/// Runs an I/O-bound store read on the tokio blocking pool.
///
/// A failed join means the blocking task itself failed (only possible when
/// it panicked — the crate has no panic paths), mapped to `BillError::Runtime`
/// so every failure stays a `Result`.
async fn blocking_load<F, T>(load: F) -> Result<T, BillError>
where
    F: FnOnce() -> Result<T, BillError> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(load)
        .await
        .map_err(|failure| BillError::Runtime(std::io::Error::other(failure.to_string())))?
}

/// Runs the pricing, aggregation, and report-building systems.
///
/// The GitHub pricing fetch (spawned only for bills that use `github-copilot`)
/// is awaited before the pricing phase; the compute phases stay synchronous.
async fn summarize_async(
    context: &StorageContext,
    world: World,
    providers: &[String],
    on_task: &mut impl FnMut(&str),
    progress: &PipelineProgress,
) -> Result<BillReport, BillError> {
    let mut world = filter_providers(world, providers);

    progress.note_phase(2);
    let pricing = price_system_async(context, &mut world, on_task).await?;

    on_task("Aggregating usage");
    progress.note_phase(3);
    let (by_model, by_agent, total) = aggregate_system(&world, providers, progress);

    progress.note_phase(4);
    on_task("Building report");
    Ok(render_system(
        &world,
        &by_model,
        &by_agent,
        &total,
        &pricing,
        &[],
    ))
}

/// Resolves model rates and fills the estimated cost component lane.
///
/// Fetches the GitHub pricing page only when the bill actually uses
/// `github-copilot`, so catalog-only and offline bills never touch the
/// network (matching the pre-migration lazy fetch).
async fn price_system_async(
    context: &StorageContext,
    world: &mut World,
    on_task: &mut impl FnMut(&str),
) -> Result<BTreeMap<ModelKey, ModelRates>, BillError> {
    let keys = world
        .entities
        .rows()
        .map(|row| (row.provider, row.model))
        .collect::<BTreeSet<_>>();
    let needs_github = keys
        .iter()
        .any(|key| world.interner.resolve_provider(key.0) == "github-copilot");

    // The never-fetched default source answers every lookup with `None`, so
    // the pricing chain is unchanged for bills without github-copilot rows.
    let github = pricing::GithubSource::default();

    if needs_github {
        // The network fetch can stall seconds offline; announce it so the
        // loading view does not sit on the catalog message.
        on_task("Fetching GitHub pricing data");
        let _ = github.ensure_fetched().await;
        on_task("Loading model pricing");
    } else {
        on_task("Loading model pricing");
    }
    let pricing = pricing::load_pricing(
        context.models_file.as_deref(),
        keys.iter(),
        &world.interner,
        &github,
    )?;

    estimate_zero_costs(&mut world.entities, &pricing);
    Ok(pricing)
}

/// Folds the entity components into per-model and per-agent totals.
fn aggregate_system(
    world: &World,
    providers: &[String],
    progress: &PipelineProgress,
) -> (
    BTreeMap<ModelKey, Totals>,
    BTreeMap<AgentId, AgentUsage>,
    Totals,
) {
    let by_model = aggregate(&world.entities, progress);
    let by_agent = aggregate_agents(&world.entities, &world.sessions, progress);
    let kept_sessions = world
        .entities
        .rows()
        .map(|row| row.session_id)
        .collect::<BTreeSet<_>>();
    let by_agent = keep_agents_with_usage(by_agent, providers, &kept_sessions);
    let total = by_model.values().fold(Totals::default(), |mut sum, row| {
        sum.merge(row);
        sum
    });

    (by_model, by_agent, total)
}

/// Produces the report data from the world and its aggregated components.
///
/// `graphs` carries the year bill's per-month bar graphs; other bills pass
/// an empty slice and render the plain TOTAL section instead.
fn render_system(
    world: &World,
    by_model: &BTreeMap<ModelKey, Totals>,
    by_agent: &BTreeMap<AgentId, AgentUsage>,
    total: &Totals,
    pricing: &BTreeMap<ModelKey, ModelRates>,
    graphs: &[BarGraph],
) -> BillReport {
    build_report(
        world,
        by_model,
        by_agent,
        total,
        pricing,
        graphs,
        &world.interner,
    )
}

/// Fills in an estimated cost for calls whose recorded cost is unknown.
///
/// Subscription usage often has no per-call price recorded; when a rate is
/// known for the provider/model pair, the call is billed at that rate
/// instead of leaving a zero row.  Provider corrections are never re-priced.
fn estimate_zero_costs(store: &mut UsageStore, pricing: &BTreeMap<ModelKey, ModelRates>) {
    // The store is borrowed read-only while its rows enumerate, so the cost
    // writes are deferred until after the walk: only the rows that actually
    // need a price are kept, never a full store copy.
    let writes = store
        .entities()
        .filter(|(entity, row)| {
            // Every lookup is gated through validate: stale or foreign
            // handles are never written, even though this store's own
            // enumeration can only produce valid handles.
            store.validate(*entity) && row.cost_unknown
        })
        .filter_map(|(entity, row)| {
            pricing
                .get(&(row.provider, row.model))
                .map(|rates| (entity, pricing::estimated_cost_micros(&row.tokens, rates)))
        })
        .collect::<Vec<_>>();

    writes.into_iter().for_each(|(entity, micros)| {
        // The entity came from this store's own lanes, so the checked
        // write cannot miss; ignore the defensive `false`.
        let _ = store.set_cost_micros(entity, micros);
    });
}

/// Restricts the bill to the given providers, keeping the full period when
/// no provider is requested.
///
/// Wall bounds are recomputed from the kept assistant calls, so the reported
/// times cover only the requested providers.
fn filter_providers(world: World, providers: &[String]) -> World {
    if providers.is_empty() {
        return world;
    }

    let World {
        header,
        session_count,
        sessions,
        entities,
        wall_start: _,
        wall_end: _,
        mut interner,
    } = world;
    let wanted = providers
        .iter()
        .map(|provider| interner.intern_provider(provider))
        .collect::<BTreeSet<_>>();
    let entities = entities
        .rows()
        .filter(|row| wanted.contains(&row.provider))
        .collect::<UsageStore>();
    let wall_start = entities
        .rows()
        .fold(None, |bound, row| earliest(bound, row.created));
    let wall_end = entities.rows().fold(None, |bound, row| {
        latest(bound, row.completed.or(row.created))
    });

    World {
        header,
        session_count,
        sessions,
        entities,
        wall_start,
        wall_end,
        interner,
    }
}

/// Drops agent classes without usage once a provider filter is active.
///
/// Agent classes are seeded from the billed sessions even when none of their
/// messages match; an unfiltered bill shows those as zero-usage rows, but a
/// provider-filtered bill should list only agent classes that used the
/// requested providers. Session counts are also scoped to the sessions that
/// produced the kept usage, so a filtered row's SESSIONS column does not
/// overcount sessions whose usage was filtered out.
fn keep_agents_with_usage(
    by_agent: BTreeMap<AgentId, AgentUsage>,
    providers: &[String],
    kept_sessions: &BTreeSet<SessionId>,
) -> BTreeMap<AgentId, AgentUsage> {
    if providers.is_empty() {
        return by_agent;
    }

    by_agent
        .into_iter()
        .filter(|(_, usage)| usage.totals.requests > 0)
        .map(|(agent, mut usage)| {
            usage
                .sessions
                .retain(|session| kept_sessions.contains(session));

            (agent, usage)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::DATABASE_FILE;
    use crate::database::SqliteStore;
    use crate::legacy::LegacyStore;
    use crate::store::MESSAGE_BATCH_LIMIT;
    use rusqlite::{Connection, params};
    use std::fs;
    use tempfile::TempDir;

    fn fixture() -> Result<(TempDir, StorageContext), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let database = temporary.path().join(DATABASE_FILE);
        let connection = Connection::open(&database)?;

        connection.execute_batch(
            "CREATE TABLE session (id TEXT PRIMARY KEY, title TEXT NOT NULL, parent_id TEXT);\
             CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT NOT NULL,\
             time_created INTEGER NOT NULL, data TEXT NOT NULL);\
             CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT NOT NULL,\
             session_id TEXT NOT NULL, data TEXT NOT NULL);",
        )?;
        connection.execute(
            "INSERT INTO session (id, title) VALUES (?1, ?2)",
            params!["ses_abcdef123456", "Test session"],
        )?;
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_1",
                "ses_abcdef123456",
                1,
                r#"{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.125,"tokens":{"input":10,"output":4,"reasoning":2,"cache":{"read":6,"write":1}}}"#
            ],
        )?;
        drop(connection);

        let context = StorageContext::builder()
            .data_dir(temporary.path())
            .build()?;
        Ok((temporary, context))
    }

    fn has_table_row(output: &str, expected: &[&str]) -> bool {
        output
            .lines()
            .any(|line| line.split('|').map(str::trim).eq(expected.iter().copied()))
    }

    #[test]
    fn generates_bill_from_unique_prefix() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;

        let bill = generate_bill(&context, "ses_abcdef", &[])?;

        assert!(bill.contains("Session: ses_abcdef123456"));
        assert!(bill.contains("Sessions: 1 (root + descendants)"));
        assert!(has_table_row(
            &bill,
            &[
                "openai / gpt-test",
                "1",
                "10",
                "6",
                "1",
                "35.29%",
                "4",
                "2",
                "0ms",
                "$0.125000"
            ]
        ));
        assert!(bill.contains("Amount due:  $0.125000 USD"));
        Ok(())
    }

    #[test]
    fn reports_progress_steps_and_report_data() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let mut tasks = Vec::new();

        let report = load_report_with_progress(&context, "ses_abcdef", &[], |task| {
            tasks.push(task.to_owned());
        })?;

        assert_eq!(
            tasks,
            [
                "Loading session data",
                "Parsing messages",
                "Loading model pricing",
                "Aggregating usage",
                "Building report",
            ]
        );
        match &report.header {
            crate::report::BillHeader::Session { id, title, .. } => {
                assert_eq!(id, "ses_abcdef123456");
                assert_eq!(title, "Test session");
            }
            crate::report::BillHeader::Month { .. }
            | crate::report::BillHeader::Day { .. }
            | crate::report::BillHeader::Year { .. }
            | crate::report::BillHeader::Week { .. } => {
                panic!("expected a session header");
            }
        }
        assert_eq!(report.session_count, 1);
        assert_eq!(report.usage_rows.len(), 1);
        assert_eq!(report.agent_rows.len(), 1);
        assert!(report.pricing_rows.is_empty());
        assert_eq!(report.totals.len(), 11);
        Ok(())
    }

    #[test]
    fn counted_progress_shares_live_counters() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let counters = BillProgress::default();
        let mut noop = |_: &str| {};

        load_report_with_progress_counted(&context, "ses_abcdef", &[], &mut noop, &counters)?;

        // The fixture stores one session and one message, and the parse
        // phase re-counts the same universe; aggregation folds the one
        // usage row in both aggregate passes.
        assert_eq!(counters.loaded_rows(), 2);
        assert_eq!(counters.parsed_rows(), 2);
        assert_eq!(counters.folded_rows(), 2);
        Ok(())
    }

    #[test]
    fn database_recent_roots_list_newest_first_without_children()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let database = temporary.path().join(DATABASE_FILE);
        let connection = Connection::open(&database)?;

        connection.execute_batch(
            "CREATE TABLE session (id TEXT PRIMARY KEY, title TEXT NOT NULL, parent_id TEXT,\
             time_created INTEGER);",
        )?;
        connection.execute(
            "INSERT INTO session (id, title, parent_id, time_created) VALUES (?1, ?2, ?3, ?4)",
            params!["ses_old", "Old root", None::<String>, 1000_i64],
        )?;
        connection.execute(
            "INSERT INTO session (id, title, parent_id, time_created) VALUES (?1, ?2, ?3, ?4)",
            params!["ses_new", "New root", None::<String>, 3000_i64],
        )?;
        connection.execute(
            "INSERT INTO session (id, title, parent_id, time_created) VALUES (?1, ?2, ?3, ?4)",
            params!["ses_child", "Child", "ses_new", 2000_i64],
        )?;
        drop(connection);

        let store = SqliteStore::new(database);
        let roots = store.recent_roots(8)?;

        assert_eq!(roots.len(), 2, "children are not roots");
        assert_eq!(roots[0].id, "ses_new");
        assert_eq!(roots[0].title, "New root");
        assert_eq!(roots[0].created_ms, Some(3000));
        assert_eq!(roots[1].id, "ses_old");
        assert_eq!(roots[1].created_ms, Some(1000));
        Ok(())
    }

    #[test]
    fn database_recent_roots_respect_the_limit() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let database = temporary.path().join(DATABASE_FILE);
        let connection = Connection::open(&database)?;

        connection.execute_batch(
            "CREATE TABLE session (id TEXT PRIMARY KEY, title TEXT NOT NULL, parent_id TEXT,\
             time_created INTEGER);",
        )?;
        ["ses_a", "ses_b", "ses_c"]
            .into_iter()
            .enumerate()
            .try_for_each(|(index, id)| -> Result<(), rusqlite::Error> {
                connection.execute(
                    "INSERT INTO session (id, title, parent_id, time_created) VALUES (?1, ?2, ?3, ?4)",
                    params![id, id, None::<String>, index as i64],
                )?;
                Ok(())
            })?;
        drop(connection);

        let store = SqliteStore::new(database);
        let roots = store.recent_roots(2)?;

        assert_eq!(roots.len(), 2);
        assert_eq!(roots[0].id, "ses_c", "newest first");
        assert_eq!(roots[1].id, "ses_b");
        Ok(())
    }

    #[test]
    fn database_recent_roots_without_created_column_report_unknown_times()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        connection.execute(
            "INSERT INTO session (id, title) VALUES (?1, ?2)",
            params!["ses_aaa", "Older by id"],
        )?;
        drop(connection);

        let store = SqliteStore::new(context.data_dir.join(DATABASE_FILE));
        let roots = store.recent_roots(8)?;

        assert_eq!(roots.len(), 2);
        assert_eq!(roots[0].id, "ses_aaa", "fallback orders by id");
        assert!(roots.iter().all(|root| root.created_ms.is_none()));
        Ok(())
    }

    #[test]
    fn legacy_recent_roots_skip_children_and_sort_newest_first()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let session_root = temporary.path().join("storage/session/project");

        fs::create_dir_all(&session_root)?;
        fs::write(
            session_root.join("ses_old.json"),
            r#"{"id":"ses_old","title":"Old root","time":{"created":1000}}"#,
        )?;
        fs::write(
            session_root.join("ses_child.json"),
            r#"{"id":"ses_child","title":"Child","parentID":"ses_new","time":{"created":2000}}"#,
        )?;
        fs::write(
            session_root.join("ses_new.json"),
            r#"{"id":"ses_new","title":"New root","time":{"created":3000}}"#,
        )?;
        fs::write(
            session_root.join("ses_unknown.json"),
            r#"{"id":"ses_unknown","title":"No time"}"#,
        )?;

        let store = LegacyStore::new(temporary.path().join("storage"));
        let roots = store.recent_roots(8)?;

        assert_eq!(roots.len(), 3, "the child is not a root");
        assert_eq!(roots[0].id, "ses_new");
        assert_eq!(roots[1].id, "ses_old");
        assert_eq!(roots[2].id, "ses_unknown", "unknown times sort last");
        Ok(())
    }

    #[test]
    fn legacy_recent_roots_respect_the_limit() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let session_root = temporary.path().join("storage/session/project");

        fs::create_dir_all(&session_root)?;
        ["ses_a", "ses_b", "ses_c"]
            .into_iter()
            .enumerate()
            .try_for_each(|(index, id)| -> Result<(), std::io::Error> {
                fs::write(
                    session_root.join(format!("{id}.json")),
                    format!(r#"{{"id":"{id}","title":"{id}","time":{{"created":{index}}}}}"#),
                )?;
                Ok(())
            })?;

        let store = LegacyStore::new(temporary.path().join("storage"));
        let roots = store.recent_roots(2)?;

        assert_eq!(roots.len(), 2);
        assert_eq!(roots[0].id, "ses_c", "newest first");
        assert_eq!(roots[1].id, "ses_b");
        Ok(())
    }

    #[test]
    fn matches_session_reference_literally() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        // 'X' differs from '_' only under wildcard matching; a literal prefix
        // lookup must not treat this as a match for "ses_abcdef".
        connection.execute(
            "INSERT INTO session (id, title) VALUES (?1, ?2)",
            params!["sesXabcdef999999", "Other session"],
        )?;
        drop(connection);

        let bill = generate_bill(&context, "ses_abcdef", &[])?;

        assert!(bill.contains("Session: ses_abcdef123456"));
        Ok(())
    }

    #[test]
    fn renders_complete_bill_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;

        let bill = generate_bill(&context, "ses_abcdef", &[])?;

        assert_eq!(bill, golden_bill());
        Ok(())
    }

    #[test]
    fn renders_complete_priced_bill_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let (temporary, _) = fixture()?;
        let models_file = temporary.path().join("models.json");

        fs::write(
            &models_file,
            r#"{"openai":{"models":{"gpt-test":{"cost":{"input":1.25,"output":5,"cache_read":0.125}}}}}"#,
        )?;

        let context = StorageContext::builder()
            .data_dir(temporary.path())
            .models_file(&models_file)
            .build()?;
        let bill = generate_bill(&context, "ses_abcdef", &[])?;

        assert_eq!(bill, golden_priced_bill());
        Ok(())
    }

    fn golden_bill() -> String {
        r#"OPENCODE SESSION BILL
Session: ses_abcdef123456
Title:   Test session
Sessions: 1 (root + descendants)

PROVIDER / MODEL  | REQUESTS | INPUT | CACHE READ | CACHE WRITE |    HIT | OUTPUT | REASONING | API TIME | COST (USD)
------------------+----------+-------+------------+-------------+--------+--------+-----------+----------+-----------
openai / gpt-test |        1 |    10 |          6 |           1 | 35.29% |      4 |         2 |      0ms |  $0.125000

USAGE BY AGENT CLASS
AGENT   | SESSIONS | REQUESTS | INPUT | CACHE READ | CACHE WRITE | OUTPUT | REASONING | API TIME | COST (USD)
--------+----------+----------+-------+------------+-------------+--------+-----------+----------+-----------
unknown |        1 |        1 |    10 |          6 |           1 |      4 |         2 |      0ms |  $0.125000

TOTAL
Requests:    1
Input:       10
Cache read:  6
Cache write: 1
Output:      4
Reasoning:   2
Started:     -
Ended:       -
API time:    0ms
Wall time:   0ms
Amount due:  $0.125000 USD
"#
        .to_owned()
    }

    fn golden_priced_bill() -> String {
        r#"OPENCODE SESSION BILL
Session: ses_abcdef123456
Title:   Test session
Sessions: 1 (root + descendants)

PROVIDER / MODEL  | REQUESTS | INPUT | CACHE READ | CACHE WRITE |    HIT | OUTPUT | REASONING | API TIME | COST (USD)
------------------+----------+-------+------------+-------------+--------+--------+-----------+----------+-----------
openai / gpt-test |        1 |    10 |          6 |           1 | 35.29% |      4 |         2 |      0ms |  $0.125000

USAGE BY AGENT CLASS
AGENT   | SESSIONS | REQUESTS | INPUT | CACHE READ | CACHE WRITE | OUTPUT | REASONING | API TIME | COST (USD)
--------+----------+----------+-------+------------+-------------+--------+-----------+----------+-----------
unknown |        1 |        1 |    10 |          6 |           1 |      4 |         2 |      0ms |  $0.125000

MODEL PRICING (USD / 1M TOKENS)
PROVIDER / MODEL   |  INPUT | CACHE READ | CACHE WRITE | OUTPUT
-------------------+--------+------------+-------------+-------
openai / gpt-test* | $1.250 |     $0.125 |           - | $5.000
* = estimated from external list prices

TOTAL
Requests:    1
Input:       10
Cache read:  6
Cache write: 1
Output:      4
Reasoning:   2
Started:     -
Ended:       -
API time:    0ms
Wall time:   0ms
Amount due:  $0.125000 USD
"#
        .to_owned()
    }

    #[test]
    fn reports_api_and_wall_time() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        connection.execute("ALTER TABLE session ADD COLUMN agent TEXT", [])?;
        connection.execute(
            "UPDATE session SET agent = ?1 WHERE id = ?2",
            params!["build", "ses_abcdef123456"],
        )?;
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_2",
                "ses_abcdef123456",
                2,
                r#"{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.125,"time":{"created":2000,"completed":5000},"tokens":{"input":10,"output":4,"reasoning":2,"cache":{"read":6,"write":1}}}"#
            ],
        )?;
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_3",
                "ses_abcdef123456",
                3,
                r#"{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.125,"time":{"created":3000,"completed":7000},"tokens":{"input":10,"output":4,"reasoning":2,"cache":{"read":6,"write":1}}}"#
            ],
        )?;
        drop(connection);

        let bill = generate_bill(&context, "ses_abcdef", &[])?;

        assert!(has_table_row(
            &bill,
            &[
                "openai / gpt-test",
                "3",
                "30",
                "18",
                "3",
                "35.29%",
                "12",
                "6",
                "7s",
                "$0.375000"
            ]
        ));
        assert!(has_table_row(
            &bill,
            &[
                "build",
                "1",
                "3",
                "30",
                "18",
                "3",
                "12",
                "6",
                "7s",
                "$0.375000"
            ]
        ));
        assert!(bill.contains("API time:    7s"));
        assert!(bill.contains("Wall time:   5s"));
        Ok(())
    }

    #[test]
    fn excludes_tool_calls_from_api_time() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        connection.execute(
            "UPDATE message SET data = ?1 WHERE id = ?2",
            params![
                r#"{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.125,"time":{"created":1000,"completed":11000},"tokens":{"input":10,"output":4,"reasoning":2,"cache":{"read":6,"write":1}}}"#,
                "msg_1"
            ],
        )?;

        // Two overlapping tool calls spanning 2000..6000 plus one from 8000..9000
        // leave 10s - 5s = 5s of API time.
        [
            (
                "prt_1",
                r#"{"type":"tool","state":{"status":"completed","time":{"start":2000,"end":5000}}}"#,
            ),
            (
                "prt_2",
                r#"{"type":"tool","state":{"status":"completed","time":{"start":3000,"end":6000}}}"#,
            ),
            (
                "prt_3",
                r#"{"type":"tool","state":{"status":"completed","time":{"start":8000,"end":9000}}}"#,
            ),
            ("prt_4", r#"{"type":"text","text":"not a tool call"}"#),
        ]
        .into_iter()
        .try_for_each(|(id, data)| -> Result<(), rusqlite::Error> {
            connection.execute(
                "INSERT INTO part (id, message_id, session_id, data) VALUES (?1, ?2, ?3, ?4)",
                params![id, "msg_1", "ses_abcdef123456", data],
            )?;
            Ok(())
        })?;
        drop(connection);

        let bill = generate_bill(&context, "ses_abcdef", &[])?;

        assert!(bill.contains("API time:    5s"));
        assert!(bill.contains("Wall time:   10s"));
        Ok(())
    }

    #[test]
    fn includes_user_messages_in_wall_time() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_user",
                "ses_abcdef123456",
                0,
                r#"{"role":"user","time":{"created":500}}"#
            ],
        )?;
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_2",
                "ses_abcdef123456",
                2,
                r#"{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.125,"time":{"created":2000,"completed":5000},"tokens":{"input":10,"output":4,"reasoning":2,"cache":{"read":6,"write":1}}}"#
            ],
        )?;
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_3",
                "ses_abcdef123456",
                3,
                r#"{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.125,"time":{"created":3000,"completed":7000},"tokens":{"input":10,"output":4,"reasoning":2,"cache":{"read":6,"write":1}}}"#
            ],
        )?;
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_user_end",
                "ses_abcdef123456",
                4,
                r#"{"role":"user","time":{"created":9000}}"#
            ],
        )?;
        drop(connection);

        let bill = generate_bill(&context, "ses_abcdef", &[])?;

        assert!(has_table_row(
            &bill,
            &[
                "openai / gpt-test",
                "3",
                "30",
                "18",
                "3",
                "35.29%",
                "12",
                "6",
                "7s",
                "$0.375000"
            ]
        ));
        assert!(bill.contains("API time:    7s"));
        assert!(bill.contains("Wall time:   8s"));
        Ok(())
    }

    #[test]
    fn reports_time_from_legacy_json() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let session_id = "ses_legacy123456";
        let session_dir = temporary.path().join("storage/session/project");
        let message_dir = temporary.path().join("storage/message").join(session_id);

        fs::create_dir_all(&session_dir)?;
        fs::create_dir_all(&message_dir)?;
        fs::write(
            session_dir.join(format!("{session_id}.json")),
            format!(r#"{{"id":"{session_id}","title":"Legacy session"}}"#),
        )?;
        fs::write(
            message_dir.join("msg_1.json"),
            r#"{"role":"assistant","agent":"legacy-agent","providerID":"anthropic","modelID":"legacy","cost":0.25,"time":{"created":1000,"completed":4000},"tokens":{"input":20,"output":8,"reasoning":1,"cache":{"read":4,"write":2}}}"#,
        )?;

        let context = StorageContext::builder()
            .data_dir(temporary.path())
            .build()?;
        let bill = generate_bill(&context, "ses_legacy", &[])?;

        assert!(has_table_row(
            &bill,
            &[
                "anthropic / legacy",
                "1",
                "20",
                "4",
                "2",
                "15.38%",
                "8",
                "1",
                "3s",
                "$0.250000"
            ]
        ));
        assert!(bill.contains("API time:    3s"));
        assert!(bill.contains("Wall time:   3s"));
        Ok(())
    }

    #[test]
    fn renders_known_model_pricing() -> Result<(), Box<dyn std::error::Error>> {
        let (temporary, _) = fixture()?;
        let models_file = temporary.path().join("models.json");

        fs::write(
            &models_file,
            r#"{"openai":{"models":{"gpt-test":{"cost":{"input":1.25,"output":5,"cache_read":0.125}}}}}"#,
        )?;

        let context = StorageContext::builder()
            .data_dir(temporary.path())
            .models_file(&models_file)
            .build()?;
        let bill = generate_bill(&context, "ses_abcdef", &[])?;

        assert!(bill.contains("MODEL PRICING (USD / 1M TOKENS)"));
        assert!(has_table_row(
            &bill,
            &["openai / gpt-test*", "$1.250", "$0.125", "-", "$5.000"]
        ));
        Ok(())
    }

    #[test]
    fn includes_all_sqlite_descendant_sessions() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        connection.execute("ALTER TABLE session ADD COLUMN agent TEXT", [])?;
        connection.execute(
            "UPDATE session SET agent = ?1 WHERE id = ?2",
            params!["build", "ses_abcdef123456"],
        )?;
        connection.execute(
            "INSERT INTO session (id, title, parent_id, agent) VALUES (?1, ?2, ?3, ?4)",
            params!["ses_child123456", "Child", "ses_abcdef123456", "explore"],
        )?;
        connection.execute(
            "INSERT INTO session (id, title, parent_id, agent) VALUES (?1, ?2, ?3, ?4)",
            params![
                "ses_grandchild1",
                "Grandchild",
                "ses_child123456",
                "explore"
            ],
        )?;
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_child",
                "ses_child123456",
                2,
                r#"{"role":"assistant","providerID":"anthropic","modelID":"child-model","cost":0.25,"tokens":{"input":20,"output":8,"reasoning":1,"cache":{"read":4,"write":2}}}"#
            ],
        )?;
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_grandchild",
                "ses_grandchild1",
                3,
                r#"{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.5,"tokens":{"input":30,"output":12,"reasoning":3,"cache":{"read":7,"write":0}}}"#
            ],
        )?;
        drop(connection);

        let bill = generate_bill(&context, "ses_abcdef123456", &[])?;

        assert!(bill.contains("Sessions: 3 (root + descendants)"));
        assert!(has_table_row(
            &bill,
            &[
                "anthropic / child-model",
                "1",
                "20",
                "4",
                "2",
                "15.38%",
                "8",
                "1",
                "0ms",
                "$0.250000"
            ]
        ));
        assert!(has_table_row(
            &bill,
            &[
                "openai / gpt-test",
                "2",
                "40",
                "13",
                "1",
                "24.07%",
                "16",
                "5",
                "0ms",
                "$0.625000"
            ]
        ));
        assert!(bill.contains("Requests:    3"));
        assert!(bill.contains("Amount due:  $0.875000 USD"));
        assert!(bill.contains("USAGE BY AGENT CLASS"));
        assert!(has_table_row(
            &bill,
            &[
                "build",
                "1",
                "1",
                "10",
                "6",
                "1",
                "4",
                "2",
                "0ms",
                "$0.125000"
            ]
        ));
        assert!(has_table_row(
            &bill,
            &[
                "explore",
                "2",
                "2",
                "50",
                "11",
                "2",
                "20",
                "4",
                "0ms",
                "$0.750000"
            ]
        ));
        Ok(())
    }

    #[test]
    fn rejects_unsafe_reference() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;

        let failure = generate_bill(&context, "ses_%", &[])
            .err()
            .ok_or("expected an error")?;

        assert!(matches!(failure, BillError::InvalidSessionReference(_)));
        Ok(())
    }

    #[test]
    fn generates_monthly_bill_from_database() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;

        // The fixture message sits at 1ms, inside January 1970.
        let bill = generate_monthly_bill(&context, 1970, 1, &[])?;

        assert!(bill.contains("OPENCODE MONTHLY BILL"));
        assert!(bill.contains("Month:   January 1970"));
        assert!(bill.contains("Sessions: 1"));
        assert!(has_table_row(
            &bill,
            &[
                "openai / gpt-test",
                "1",
                "10",
                "6",
                "1",
                "35.29%",
                "4",
                "2",
                "0ms",
                "$0.125000"
            ]
        ));
        // The bar graphs replace the TOTAL table.
        assert!(bill.contains("REQUESTS    Total: 1"));
        assert!(bill.contains("AMOUNT DUE  Total: $0.125000 USD"));
        // Wall-clock bounds belong to session bills only.
        assert!(!bill.contains("WALL TIME"));
        assert!(!bill.contains("Started:"));
        assert!(!bill.contains("Ended:"));
        assert!(!bill.contains("Wall time:"));
        Ok(())
    }

    #[test]
    fn monthly_bill_excludes_out_of_range_messages() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        connection.execute(
            "INSERT INTO session (id, title) VALUES (?1, ?2)",
            params!["ses_other123456", "Other session"],
        )?;
        // 2001-09-09 01:46:40 UTC, outside January 1970.
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_far",
                "ses_other123456",
                1_000_000_000_000_i64,
                r#"{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.5,"tokens":{"input":30,"output":12,"reasoning":3,"cache":{"read":7,"write":2}}}"#
            ],
        )?;
        drop(connection);

        let january = generate_monthly_bill(&context, 1970, 1, &[])?;

        assert!(january.contains("Sessions: 1"));
        assert!(has_table_row(
            &january,
            &[
                "openai / gpt-test",
                "1",
                "10",
                "6",
                "1",
                "35.29%",
                "4",
                "2",
                "0ms",
                "$0.125000"
            ]
        ));
        assert!(january.contains("REQUESTS    Total: 1"));

        let september = generate_monthly_bill(&context, 2001, 9, &[])?;

        assert!(september.contains("Sessions: 1"));
        assert!(september.contains("REQUESTS    Total: 1"));
        assert!(september.contains("AMOUNT DUE  Total: $0.500000 USD"));

        let february = generate_monthly_bill(&context, 1970, 2, &[])?;

        assert!(february.contains("Sessions: 0"));
        assert!(february.contains("REQUESTS    Total: 0"));
        Ok(())
    }

    #[test]
    fn generates_yearly_bill_with_month_bars() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;
        let june = chrono::DateTime::parse_from_rfc3339("1970-06-15T12:00:00Z")?.timestamp_millis();
        let outside = 1_000_000_000_000_i64;

        // A June 1970 call (same year as the fixture's January window), and
        // a 2001 call that must stay outside the 1970 window.
        connection.execute(
            "INSERT INTO session (id, title) VALUES (?1, ?2)",
            params!["ses_june123456", "June session"],
        )?;
        connection.execute(
            "INSERT INTO session (id, title) VALUES (?1, ?2)",
            params!["ses_far1234567", "Far session"],
        )?;
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_june",
                "ses_june123456",
                june,
                format!(
                    r#"{{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.5,"time":{{"created":{june}}},"tokens":{{"input":30,"output":12,"reasoning":3,"cache":{{"read":7,"write":2}}}}}}"#
                )
            ],
        )?;
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_far",
                "ses_far1234567",
                outside,
                r#"{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.75,"tokens":{"input":50,"output":10,"reasoning":2,"cache":{"read":8,"write":1}}}"#
            ],
        )?;
        drop(connection);

        let bill = generate_yearly_bill(&context, 1970, &[])?;

        assert!(bill.contains("OPENCODE YEARLY BILL"));
        assert!(bill.contains("Year:    1970"));
        assert!(bill.contains("Sessions: 2"));
        assert!(has_table_row(
            &bill,
            &[
                "openai / gpt-test",
                "2",
                "40",
                "13",
                "3",
                "23.21%",
                "16",
                "5",
                "0ms",
                "$0.625000"
            ]
        ));
        // The bar graphs replace the TOTAL table.
        assert!(bill.contains("REQUESTS    Total: 2"));
        assert!(bill.contains("AMOUNT DUE  Total: $0.625000 USD"));
        assert!(!bill.contains("Amount due:"));
        assert!(!bill.contains("TOTAL\n"));
        // The fixture message has no creation time, so only June gets a bar;
        // its values still count in the year totals.
        assert!(bill.contains("Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec "));
        Ok(())
    }

    #[test]
    fn generates_monthly_bill_with_week_bars() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;
        let week_one =
            chrono::DateTime::parse_from_rfc3339("1970-01-01T12:00:00Z")?.timestamp_millis();
        let week_three =
            chrono::DateTime::parse_from_rfc3339("1970-01-12T12:00:00Z")?.timestamp_millis();

        // January 1970 spans ISO weeks 1..=5: the first call lands in week 1,
        // the second (January 12, a Monday) in week 3.
        connection.execute(
            "INSERT INTO session (id, title) VALUES (?1, ?2)",
            params!["ses_week1234567", "Week session"],
        )?;
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_w1",
                "ses_week1234567",
                week_one,
                format!(
                    r#"{{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.25,"time":{{"created":{week_one}}},"tokens":{{"input":20,"output":5,"reasoning":1,"cache":{{"read":2,"write":0}}}}}}"#
                )
            ],
        )?;
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_w3",
                "ses_week1234567",
                week_three,
                format!(
                    r#"{{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.25,"time":{{"created":{week_three}}},"tokens":{{"input":20,"output":5,"reasoning":1,"cache":{{"read":2,"write":0}}}}}}"#
                )
            ],
        )?;
        drop(connection);

        let bill = generate_monthly_bill(&context, 1970, 1, &[])?;

        assert!(bill.contains("OPENCODE MONTHLY BILL"));
        assert!(bill.contains("Month:   January 1970"));
        // The bar graphs replace the TOTAL table; the fixture's timestamp-less
        // message counts in the month total but belongs to no bar.
        assert!(bill.contains("REQUESTS    Total: 3"));
        assert!(bill.contains("AMOUNT DUE  Total: $0.625000 USD"));
        assert!(bill.contains("LIMIT       Remaining: $249.375000"));
        assert!(bill.contains("1   2   3   4   5"));
        assert!(!bill.contains("Amount due:"));
        assert!(!bill.contains("TOTAL\n"));
        Ok(())
    }

    #[test]
    fn monthly_limit_graph_uses_custom_limit_and_cumulative_cost()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;

        let bill = generate_monthly_bill_with_limit(&context, 1970, 1, &[], 1_000_000)?;

        assert!(bill.contains("LIMIT       Remaining: $0.875000"));
        assert!(bill.contains("-----"));
        Ok(())
    }

    #[test]
    fn generates_weekly_bill_with_weekday_bars() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;
        let monday =
            chrono::DateTime::parse_from_rfc3339("1970-01-05T12:00:00Z")?.timestamp_millis();
        let thursday =
            chrono::DateTime::parse_from_rfc3339("1970-01-08T12:00:00Z")?.timestamp_millis();
        let next_week =
            chrono::DateTime::parse_from_rfc3339("1970-01-12T12:00:00Z")?.timestamp_millis();

        // Week 2 of 1970 runs Monday January 5 to Sunday January 11: the
        // first call lands on Monday, the second on Thursday; the third
        // (next Monday) stays outside the window, like the fixture's
        // January 1 call in week 1.
        connection.execute(
            "INSERT INTO session (id, title) VALUES (?1, ?2)",
            params!["ses_week1234567", "Week session"],
        )?;
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_mon",
                "ses_week1234567",
                monday,
                format!(
                    r#"{{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.25,"time":{{"created":{monday}}},"tokens":{{"input":20,"output":5,"reasoning":1,"cache":{{"read":2,"write":0}}}}}}"#
                )
            ],
        )?;
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_thu",
                "ses_week1234567",
                thursday,
                format!(
                    r#"{{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.25,"time":{{"created":{thursday}}},"tokens":{{"input":20,"output":5,"reasoning":1,"cache":{{"read":2,"write":0}}}}}}"#
                )
            ],
        )?;
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_out",
                "ses_week1234567",
                next_week,
                format!(
                    r#"{{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.5,"time":{{"created":{next_week}}},"tokens":{{"input":30,"output":12,"reasoning":3,"cache":{{"read":7,"write":2}}}}}}"#
                )
            ],
        )?;
        drop(connection);

        let bill = generate_weekly_bill(&context, 1970, 2, &[])?;

        assert!(bill.contains("OPENCODE WEEKLY BILL"));
        assert!(bill.contains("Week:    Week 2, 1970"));
        assert!(bill.contains("Sessions: 1"));
        assert!(has_table_row(
            &bill,
            &[
                "openai / gpt-test",
                "2",
                "40",
                "4",
                "0",
                "9.09%",
                "10",
                "2",
                "0ms",
                "$0.500000"
            ]
        ));
        // The bar graphs replace the TOTAL table; calls outside the window
        // (the fixture's week-1 message and the next-week message) are absent.
        assert!(bill.contains("REQUESTS    Total: 2"));
        assert!(bill.contains("AMOUNT DUE  Total: $0.500000 USD"));
        assert!(bill.contains("Mon Tue Wed Thu Fri Sat Sun"));
        assert!(!bill.contains("Amount due:"));
        assert!(!bill.contains("TOTAL\n"));
        Ok(())
    }

    #[test]
    fn rejects_invalid_week() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;

        let before_epoch = generate_weekly_bill(&context, 1969, 1, &[])
            .err()
            .ok_or("expected an error")?;
        let zero = generate_weekly_bill(&context, 2026, 0, &[])
            .err()
            .ok_or("expected an error")?;
        let beyond_range = generate_weekly_bill(&context, 2026, 54, &[])
            .err()
            .ok_or("expected an error")?;
        let short_year = generate_weekly_bill(&context, 2025, 53, &[])
            .err()
            .ok_or("expected an error")?;
        let beyond_chrono = generate_weekly_bill(&context, 262_143, 1, &[])
            .err()
            .ok_or("expected an error")?;

        assert!(matches!(before_epoch, BillError::InvalidWeek(_)));
        assert!(matches!(zero, BillError::InvalidWeek(_)));
        assert!(matches!(beyond_range, BillError::InvalidWeek(_)));
        assert!(matches!(short_year, BillError::InvalidWeek(_)));
        assert!(matches!(beyond_chrono, BillError::InvalidWeek(_)));
        Ok(())
    }

    #[test]
    fn year_streaming_pumps_one_batch_per_day() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        // A second message on the following day (one UTC day later than the
        // fixture's day-0 message): the streamer must cut a batch boundary
        // exactly there.
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_day2",
                "ses_abcdef123456",
                90_000_000_i64,
                r#"{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.25,"time":{"created":90000000},"tokens":{"input":5,"output":2,"reasoning":0,"cache":{"read":1,"write":0}}}"#
            ],
        )?;
        drop(connection);

        let store = SqliteStore::new(context.data_dir.join(DATABASE_FILE));
        let window = CalendarWindow::year(1970)?;
        let progress = BillProgress::default();
        let mut batches = Vec::new();

        store.stream_window_messages(&window, &progress, &mut |mut batch| {
            batches.push(
                batch
                    .messages
                    .iter()
                    .map(|message| message.id.clone())
                    .collect::<Vec<_>>(),
            );
            batch.messages.clear();

            Ok(batch)
        })?;

        assert_eq!(
            batches,
            [vec!["msg_1".to_owned()], vec!["msg_day2".to_owned()]],
            "one batch per calendar day, in window order"
        );
        assert_eq!(progress.loaded_rows(), 2);
        Ok(())
    }

    #[test]
    fn month_streaming_pumps_one_batch_per_day() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        // Same two-day fixture: the month window must stream the same way
        // as the year window, with a batch boundary at the day change.
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_day2",
                "ses_abcdef123456",
                90_000_000_i64,
                r#"{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.25,"time":{"created":90000000},"tokens":{"input":5,"output":2,"reasoning":0,"cache":{"read":1,"write":0}}}"#
            ],
        )?;
        drop(connection);

        let store = SqliteStore::new(context.data_dir.join(DATABASE_FILE));
        let window = CalendarWindow::month(1970, 1)?;
        let progress = BillProgress::default();
        let mut batches = Vec::new();

        store.stream_window_messages(&window, &progress, &mut |mut batch| {
            batches.push(
                batch
                    .messages
                    .iter()
                    .map(|message| message.id.clone())
                    .collect::<Vec<_>>(),
            );
            batch.messages.clear();

            Ok(batch)
        })?;

        assert_eq!(
            batches,
            [vec!["msg_1".to_owned()], vec!["msg_day2".to_owned()]],
            "month windows stream one batch per calendar day too"
        );
        Ok(())
    }

    #[test]
    fn day_batches_never_exceed_the_message_limit() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        // The fixture's day-0 message plus one batch's worth plus one more:
        // the day must be split into a full batch and the remainder, both
        // at or below the pipeline's message limit.
        (0..MESSAGE_BATCH_LIMIT + 1).try_for_each(|index| -> Result<(), rusqlite::Error> {
            connection.execute(
                "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
                params![
                    format!("msg_bulk_{index:05}"),
                    "ses_abcdef123456",
                    1000 + index as i64,
                    r#"{"role":"user","time":{"created":1000}}"#
                ],
            )?;
            Ok(())
        })?;
        drop(connection);

        let store = SqliteStore::new(context.data_dir.join(DATABASE_FILE));
        let window = CalendarWindow::year(1970)?;
        let progress = BillProgress::default();
        let mut sizes = Vec::new();

        store.stream_window_messages(&window, &progress, &mut |mut batch| {
            sizes.push(batch.messages.len());
            batch.messages.clear();

            Ok(batch)
        })?;

        assert_eq!(
            sizes,
            [MESSAGE_BATCH_LIMIT, 2],
            "the fixture message plus the bulk rows split into at most {} rows per batch",
            MESSAGE_BATCH_LIMIT
        );
        assert_eq!(progress.loaded_rows(), MESSAGE_BATCH_LIMIT as u64 + 2);
        Ok(())
    }

    #[test]
    fn github_copilot_bill_runs_without_network() -> Result<(), Box<dyn std::error::Error>> {
        // The pump detects github-copilot payloads in the stream and fetches
        // the pricing page once; in tests the fetch is disabled and fails
        // fast, and the bill still completes with the catalog rates.
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_github",
                "ses_abcdef123456",
                90_000_000_i64,
                r#"{"role":"assistant","providerID":"github-copilot","modelID":"gpt-4o","cost":0,"tokens":{"input":1,"output":1,"reasoning":0,"cache":{"read":0,"write":0}}}"#
            ],
        )?;
        drop(connection);

        let counters = BillProgress::default();
        let mut noop = |_: &str| {};

        load_year_report_with_progress_counted(&context, 1970, &[], &mut noop, &counters)?;

        // Two assistant messages streamed: the fixture's and the github one.
        assert_eq!(counters.loaded_rows(), 2);
        assert_eq!(counters.parsed_rows(), 2);
        assert_eq!(counters.folded_rows(), 6);
        Ok(())
    }

    #[test]
    fn stream_parse_error_surfaces_as_the_root_cause() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        // The second day carries an invalid assistant message: the parse
        // side must fail, stop the pump, and its data error must surface
        // instead of the pump's cancellation error.
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_bad",
                "ses_abcdef123456",
                90_000_000_i64,
                r#"{"role":"assistant"}"#
            ],
        )?;
        drop(connection);

        let counters = BillProgress::default();
        let mut noop = |_: &str| {};

        let failure =
            load_year_report_with_progress_counted(&context, 1970, &[], &mut noop, &counters)
                .err()
                .ok_or("expected an error")?;

        assert!(
            matches!(failure, BillError::InvalidData { .. }),
            "the parse error is the root cause: {failure:?}"
        );
        Ok(())
    }

    #[test]
    fn year_streaming_counts_loaded_and_parsed_rows() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_day2",
                "ses_abcdef123456",
                90_000_000_i64,
                r#"{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.25,"time":{"created":90000000},"tokens":{"input":5,"output":2,"reasoning":0,"cache":{"read":1,"write":0}}}"#
            ],
        )?;
        drop(connection);

        let counters = BillProgress::default();
        let mut noop = |_: &str| {};

        load_year_report_with_progress_counted(&context, 1970, &[], &mut noop, &counters)?;

        // Two assistant messages (no session agent column, no parts), so the
        // streaming parse counts exactly what the store streamed, and the
        // three aggregate passes (model, agent, month bars) fold the two
        // usage rows.
        assert_eq!(counters.loaded_rows(), 2);
        assert_eq!(counters.parsed_rows(), 2);
        assert_eq!(counters.folded_rows(), 6);
        Ok(())
    }

    #[test]
    fn generates_yearly_bill_from_legacy_json() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let session_id = "ses_legacy123456";
        let message_dir = temporary.path().join("storage/message").join(session_id);

        fs::create_dir_all(&message_dir)?;
        fs::write(
            message_dir.join("msg_1.json"),
            r#"{"role":"assistant","providerID":"anthropic","modelID":"legacy","cost":0.25,"time":{"created":1000,"completed":4000},"tokens":{"input":20,"output":8,"reasoning":1,"cache":{"read":4,"write":2}}}"#,
        )?;
        // A second call on the following day of the same year.
        fs::write(
            message_dir.join("msg_2.json"),
            r#"{"role":"assistant","providerID":"anthropic","modelID":"legacy","cost":0.75,"time":{"created":90000000},"tokens":{"input":50,"output":10,"reasoning":2,"cache":{"read":8,"write":1}}}"#,
        )?;

        let context = StorageContext::builder()
            .data_dir(temporary.path())
            .build()?;
        let bill = generate_yearly_bill(&context, 1970, &[])?;

        assert!(bill.contains("OPENCODE YEARLY BILL"));
        assert!(bill.contains("Year:    1970"));
        assert!(bill.contains("Sessions: 1"));
        assert!(has_table_row(
            &bill,
            &[
                "anthropic / legacy",
                "2",
                "70",
                "12",
                "3",
                "14.11%",
                "18",
                "3",
                "3s",
                "$1.000000"
            ]
        ));
        assert!(bill.contains("REQUESTS    Total: 2"));

        // Legacy counting: one kept session + two messages, no parts, in
        // both the load and the parse universe; the three aggregate passes
        // fold the two usage rows.
        let counters = BillProgress::default();
        let mut noop = |_: &str| {};

        load_year_report_with_progress_counted(&context, 1970, &[], &mut noop, &counters)?;

        assert_eq!(counters.loaded_rows(), 3);
        assert_eq!(counters.parsed_rows(), 3);
        assert_eq!(counters.folded_rows(), 6);
        Ok(())
    }

    #[test]
    fn rejects_invalid_year() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;

        let before_epoch = generate_yearly_bill(&context, 1969, &[])
            .err()
            .ok_or("expected an error")?;
        let beyond_range = generate_yearly_bill(&context, 2_147_483_648, &[])
            .err()
            .ok_or("expected an error")?;
        let beyond_chrono = generate_yearly_bill(&context, 262_143, &[])
            .err()
            .ok_or("expected an error")?;

        assert!(matches!(before_epoch, BillError::InvalidYear(_)));
        assert!(matches!(beyond_range, BillError::InvalidYear(_)));
        assert!(matches!(beyond_chrono, BillError::InvalidYear(_)));
        Ok(())
    }

    #[test]
    fn generates_monthly_bill_from_legacy_json() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let session_id = "ses_legacy123456";
        let message_dir = temporary.path().join("storage/message").join(session_id);
        let outside_dir = temporary
            .path()
            .join("storage/message")
            .join("ses_outside12345");

        fs::create_dir_all(&message_dir)?;
        fs::create_dir_all(&outside_dir)?;
        fs::write(
            message_dir.join("msg_1.json"),
            r#"{"role":"assistant","providerID":"anthropic","modelID":"legacy","cost":0.25,"time":{"created":1000,"completed":4000},"tokens":{"input":20,"output":8,"reasoning":1,"cache":{"read":4,"write":2}}}"#,
        )?;
        fs::write(
            outside_dir.join("msg_far.json"),
            r#"{"role":"assistant","providerID":"anthropic","modelID":"legacy","cost":0.75,"time":{"created":1000000000000},"tokens":{"input":50,"output":10,"reasoning":2,"cache":{"read":8,"write":1}}}"#,
        )?;

        let context = StorageContext::builder()
            .data_dir(temporary.path())
            .build()?;
        let bill = generate_monthly_bill(&context, 1970, 1, &[])?;

        assert!(bill.contains("Sessions: 1"));
        assert!(has_table_row(
            &bill,
            &[
                "anthropic / legacy",
                "1",
                "20",
                "4",
                "2",
                "15.38%",
                "8",
                "1",
                "3s",
                "$0.250000"
            ]
        ));
        assert!(bill.contains("AMOUNT DUE  Total: $0.250000 USD"));
        Ok(())
    }

    #[test]
    fn rejects_invalid_month() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;

        let failure = generate_monthly_bill(&context, 1970, 13, &[])
            .err()
            .ok_or("expected an error")?;

        assert!(matches!(failure, BillError::InvalidMonth(_)));
        Ok(())
    }

    #[test]
    fn generates_daily_bill_from_database() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;

        // The fixture message sits at 1ms, inside January 1 1970.
        let bill = generate_daily_bill(&context, 1970, 1, 1, &[])?;

        assert!(bill.contains("OPENCODE DAILY BILL"));
        assert!(bill.contains("Day:     January 1 1970"));
        assert!(bill.contains("Sessions: 1"));
        assert!(has_table_row(
            &bill,
            &[
                "openai / gpt-test",
                "1",
                "10",
                "6",
                "1",
                "35.29%",
                "4",
                "2",
                "0ms",
                "$0.125000"
            ]
        ));
        assert!(bill.contains("Amount due:  $0.125000 USD"));
        // Wall-clock bounds belong to session bills only.
        assert!(!bill.contains("Started:"));
        assert!(!bill.contains("Ended:"));
        assert!(!bill.contains("Wall time:"));
        Ok(())
    }

    #[test]
    fn daily_bill_excludes_out_of_range_messages() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        connection.execute(
            "INSERT INTO session (id, title) VALUES (?1, ?2)",
            params!["ses_other123456", "Other session"],
        )?;
        // 2001-09-09 01:46:40 UTC, outside January 1 1970.
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_far",
                "ses_other123456",
                1_000_000_000_000_i64,
                r#"{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.5,"tokens":{"input":30,"output":12,"reasoning":3,"cache":{"read":7,"write":2}}}"#
            ],
        )?;
        drop(connection);

        let first = generate_daily_bill(&context, 1970, 1, 1, &[])?;

        assert!(first.contains("Sessions: 1"));
        assert!(first.contains("Requests:    1"));
        assert!(first.contains("Amount due:  $0.125000 USD"));

        let second = generate_daily_bill(&context, 1970, 1, 2, &[])?;

        assert!(second.contains("Sessions: 0"));
        assert!(second.contains("Requests:    0"));
        Ok(())
    }

    #[test]
    fn generates_daily_bill_from_legacy_json() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let session_id = "ses_legacy123456";
        let message_dir = temporary.path().join("storage/message").join(session_id);
        let outside_dir = temporary
            .path()
            .join("storage/message")
            .join("ses_outside12345");

        fs::create_dir_all(&message_dir)?;
        fs::create_dir_all(&outside_dir)?;
        fs::write(
            message_dir.join("msg_1.json"),
            r#"{"role":"assistant","providerID":"anthropic","modelID":"legacy","cost":0.25,"time":{"created":1000,"completed":4000},"tokens":{"input":20,"output":8,"reasoning":1,"cache":{"read":4,"write":2}}}"#,
        )?;
        fs::write(
            outside_dir.join("msg_far.json"),
            r#"{"role":"assistant","providerID":"anthropic","modelID":"legacy","cost":0.75,"time":{"created":1000000000000},"tokens":{"input":50,"output":10,"reasoning":2,"cache":{"read":8,"write":1}}}"#,
        )?;

        let context = StorageContext::builder()
            .data_dir(temporary.path())
            .build()?;
        let bill = generate_daily_bill(&context, 1970, 1, 1, &[])?;

        assert!(bill.contains("Sessions: 1"));
        assert!(has_table_row(
            &bill,
            &[
                "anthropic / legacy",
                "1",
                "20",
                "4",
                "2",
                "15.38%",
                "8",
                "1",
                "3s",
                "$0.250000"
            ]
        ));
        assert!(bill.contains("Amount due:  $0.250000 USD"));
        Ok(())
    }

    #[test]
    fn rejects_invalid_day() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;

        let failure = generate_daily_bill(&context, 1970, 1, 32, &[])
            .err()
            .ok_or("expected an error")?;

        assert!(matches!(failure, BillError::InvalidDay(_)));
        Ok(())
    }

    #[test]
    fn clamps_negative_token_and_cost_corrections() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        connection.execute(
            "UPDATE message SET data = ?1 WHERE id = ?2",
            params![
                r#"{"role":"assistant","providerID":"openrouter","modelID":"grok-test","cost":-0.005,"tokens":{"input":-39407,"output":1884,"reasoning":0,"cache":{"read":0,"write":0}}}"#,
                "msg_1"
            ],
        )?;
        drop(connection);

        let bill = generate_bill(&context, "ses_abcdef", &[])?;

        assert!(has_table_row(
            &bill,
            &[
                "openrouter / grok-test",
                "1",
                "0",
                "0",
                "0",
                "-",
                "1.9k",
                "0",
                "0ms",
                "$0.000000"
            ]
        ));
        assert!(bill.contains("Amount due:  $0.000000 USD"));
        Ok(())
    }

    #[test]
    fn estimates_zero_cost_usage_from_known_rates() -> Result<(), Box<dyn std::error::Error>> {
        let (temporary, _) = fixture()?;
        let models_file = temporary.path().join("models.json");

        fs::write(
            &models_file,
            r#"{"openai":{"models":{"gpt-test":{"cost":{"input":1.25,"output":5,"cache_read":0.125}}}}}"#,
        )?;

        let connection = Connection::open(temporary.path().join(DATABASE_FILE))?;
        connection.execute(
            "UPDATE message SET data = ?1 WHERE id = ?2",
            params![
                r#"{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0,"tokens":{"input":10,"output":4,"reasoning":2,"cache":{"read":6,"write":1}}}"#,
                "msg_1"
            ],
        )?;
        drop(connection);

        let context = StorageContext::builder()
            .data_dir(temporary.path())
            .models_file(&models_file)
            .build()?;
        let bill = generate_bill(&context, "ses_abcdef", &[])?;

        // 10 * 1.25 + 6 * 0.125 + (4 + 2) * 5 = 43.25 per million tokens.
        assert!(has_table_row(
            &bill,
            &[
                "openai / gpt-test",
                "1",
                "10",
                "6",
                "1",
                "35.29%",
                "4",
                "2",
                "0ms",
                "$0.000043*"
            ]
        ));
        assert!(bill.contains("Amount due:  $0.000043* USD"));
        Ok(())
    }

    #[test]
    fn never_prices_negative_corrections() -> Result<(), Box<dyn std::error::Error>> {
        let (temporary, _) = fixture()?;
        let models_file = temporary.path().join("models.json");

        fs::write(
            &models_file,
            r#"{"openrouter":{"models":{"grok-test":{"cost":{"input":1,"output":1}}}}}"#,
        )?;

        let connection = Connection::open(temporary.path().join(DATABASE_FILE))?;
        connection.execute(
            "UPDATE message SET data = ?1 WHERE id = ?2",
            params![
                r#"{"role":"assistant","providerID":"openrouter","modelID":"grok-test","cost":-0.005,"tokens":{"input":1000,"output":1884,"reasoning":0,"cache":{"read":0,"write":0}}}"#,
                "msg_1"
            ],
        )?;
        drop(connection);

        let context = StorageContext::builder()
            .data_dir(temporary.path())
            .models_file(&models_file)
            .build()?;
        let bill = generate_bill(&context, "ses_abcdef", &[])?;

        // The clamped correction stays zero even though a rate is known.
        assert!(has_table_row(
            &bill,
            &[
                "openrouter / grok-test",
                "1",
                "1.0k",
                "0",
                "0",
                "0.00%",
                "1.9k",
                "0",
                "0ms",
                "$0.000000"
            ]
        ));
        assert!(bill.contains("Amount due:  $0.000000 USD"));
        Ok(())
    }

    #[test]
    fn leaves_zero_cost_unknown_model_unpriced() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        connection.execute(
            "UPDATE message SET data = ?1 WHERE id = ?2",
            params![
                r#"{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0,"tokens":{"input":10,"output":4,"reasoning":2,"cache":{"read":6,"write":1}}}"#,
                "msg_1"
            ],
        )?;
        drop(connection);

        let bill = generate_bill(&context, "ses_abcdef", &[])?;

        // gpt-test has no rate anywhere: the row stays at zero.
        assert!(has_table_row(
            &bill,
            &[
                "openai / gpt-test",
                "1",
                "10",
                "6",
                "1",
                "35.29%",
                "4",
                "2",
                "0ms",
                "$0.000000"
            ]
        ));
        assert!(bill.contains("Amount due:  $0.000000 USD"));
        Ok(())
    }

    #[test]
    fn filters_usage_to_requested_providers() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_anthropic",
                "ses_abcdef123456",
                2,
                r#"{"role":"assistant","providerID":"anthropic","modelID":"claude-test","cost":0.25,"tokens":{"input":20,"output":8,"reasoning":1,"cache":{"read":4,"write":2}}}"#
            ],
        )?;
        drop(connection);

        let anthropic_only = generate_bill(&context, "ses_abcdef", &["anthropic".to_owned()])?;

        assert!(has_table_row(
            &anthropic_only,
            &[
                "anthropic / claude-test",
                "1",
                "20",
                "4",
                "2",
                "15.38%",
                "8",
                "1",
                "0ms",
                "$0.250000"
            ]
        ));
        assert!(!anthropic_only.contains("openai / gpt-test"));
        assert!(anthropic_only.contains("Requests:    1"));
        assert!(anthropic_only.contains("Amount due:  $0.250000 USD"));

        let both = generate_bill(
            &context,
            "ses_abcdef",
            &["openai".to_owned(), "anthropic".to_owned()],
        )?;

        assert!(has_table_row(
            &both,
            &[
                "openai / gpt-test",
                "1",
                "10",
                "6",
                "1",
                "35.29%",
                "4",
                "2",
                "0ms",
                "$0.125000"
            ]
        ));
        assert!(has_table_row(
            &both,
            &[
                "anthropic / claude-test",
                "1",
                "20",
                "4",
                "2",
                "15.38%",
                "8",
                "1",
                "0ms",
                "$0.250000"
            ]
        ));
        assert!(both.contains("Requests:    2"));
        assert!(both.contains("Amount due:  $0.375000 USD"));
        Ok(())
    }

    #[test]
    fn filters_monthly_bill_to_requested_providers() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_anthropic",
                "ses_abcdef123456",
                2,
                r#"{"role":"assistant","providerID":"anthropic","modelID":"claude-test","cost":0.25,"tokens":{"input":20,"output":8,"reasoning":1,"cache":{"read":4,"write":2}}}"#
            ],
        )?;
        drop(connection);

        let openai_only = generate_monthly_bill(&context, 1970, 1, &["openai".to_owned()])?;

        assert!(openai_only.contains("Sessions: 1"));
        assert!(has_table_row(
            &openai_only,
            &[
                "openai / gpt-test",
                "1",
                "10",
                "6",
                "1",
                "35.29%",
                "4",
                "2",
                "0ms",
                "$0.125000"
            ]
        ));
        assert!(!openai_only.contains("anthropic / claude-test"));
        assert!(openai_only.contains("REQUESTS    Total: 1"));
        assert!(openai_only.contains("AMOUNT DUE  Total: $0.125000 USD"));
        Ok(())
    }

    #[test]
    fn filters_wall_time_to_requested_providers() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        connection.execute(
            "UPDATE message SET data = ?1 WHERE id = ?2",
            params![
                r#"{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.125,"time":{"created":2000,"completed":5000},"tokens":{"input":10,"output":4,"reasoning":2,"cache":{"read":6,"write":1}}}"#,
                "msg_1"
            ],
        )?;
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_anthropic",
                "ses_abcdef123456",
                4,
                r#"{"role":"assistant","providerID":"anthropic","modelID":"claude-test","cost":0.25,"time":{"created":10000,"completed":15000},"tokens":{"input":20,"output":8,"reasoning":1,"cache":{"read":4,"write":2}}}"#
            ],
        )?;
        drop(connection);

        let openai_only = generate_bill(&context, "ses_abcdef", &["openai".to_owned()])?;

        assert!(has_table_row(
            &openai_only,
            &[
                "openai / gpt-test",
                "1",
                "10",
                "6",
                "1",
                "35.29%",
                "4",
                "2",
                "3s",
                "$0.125000"
            ]
        ));
        assert!(!openai_only.contains("anthropic / claude-test"));
        assert!(openai_only.contains("API time:    3s"));
        assert!(openai_only.contains("Wall time:   3s"));
        assert!(openai_only.contains("Requests:    1"));
        Ok(())
    }

    #[test]
    fn filters_pricing_to_requested_providers() -> Result<(), Box<dyn std::error::Error>> {
        let (temporary, _) = fixture()?;
        let models_file = temporary.path().join("models.json");

        fs::write(
            &models_file,
            r#"{"openai":{"models":{"gpt-test":{"cost":{"input":1.25,"output":5,"cache_read":0.125}}}},"anthropic":{"models":{"claude-test":{"cost":{"input":3,"output":15,"cache_read":1}}}}}"#,
        )?;

        let connection = Connection::open(temporary.path().join(DATABASE_FILE))?;

        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_anthropic",
                "ses_abcdef123456",
                2,
                r#"{"role":"assistant","providerID":"anthropic","modelID":"claude-test","cost":0.25,"tokens":{"input":20,"output":8,"reasoning":1,"cache":{"read":4,"write":2}}}"#
            ],
        )?;
        drop(connection);

        let context = StorageContext::builder()
            .data_dir(temporary.path())
            .models_file(&models_file)
            .build()?;
        let bill = generate_bill(&context, "ses_abcdef", &["openai".to_owned()])?;

        assert!(bill.contains("MODEL PRICING (USD / 1M TOKENS)"));
        assert!(has_table_row(
            &bill,
            &["openai / gpt-test*", "$1.250", "$0.125", "-", "$5.000"]
        ));
        assert!(!bill.contains("claude-test"));
        Ok(())
    }

    #[test]
    fn streamed_bill_shows_pricing_for_every_model() -> Result<(), Box<dyn std::error::Error>> {
        let (temporary, _) = fixture()?;
        let models_file = temporary.path().join("models.json");

        fs::write(
            &models_file,
            r#"{"openai":{"models":{"gpt-test":{"cost":{"input":1.25,"output":5,"cache_read":0.125}}}}}"#,
        )?;

        let context = StorageContext::builder()
            .data_dir(temporary.path())
            .models_file(&models_file)
            .build()?;

        // The fixture's message records a real cost, so no row needs
        // zero-cost estimation — the pricing table must still cover the
        // model, exactly like a whole-store bill.
        let bill = generate_monthly_bill(&context, 1970, 1, &[])?;

        assert!(bill.contains("MODEL PRICING (USD / 1M TOKENS)"));
        assert!(has_table_row(
            &bill,
            &["openai / gpt-test*", "$1.250", "$0.125", "-", "$5.000"]
        ));
        Ok(())
    }

    #[test]
    fn filters_agent_rows_to_provider_usage() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        connection.execute("ALTER TABLE session ADD COLUMN agent TEXT", [])?;
        connection.execute(
            "UPDATE session SET agent = ?1 WHERE id = ?2",
            params!["build", "ses_abcdef123456"],
        )?;
        connection.execute(
            "INSERT INTO session (id, title, parent_id, agent) VALUES (?1, ?2, ?3, ?4)",
            params!["ses_child123456", "Child", "ses_abcdef123456", "explore"],
        )?;
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_child",
                "ses_child123456",
                2,
                r#"{"role":"assistant","providerID":"anthropic","modelID":"claude-test","cost":0.25,"tokens":{"input":20,"output":8,"reasoning":1,"cache":{"read":4,"write":2}}}"#
            ],
        )?;
        // A second build-class session whose message is also from anthropic:
        // its SESSIONS count must exclude the root, whose openai usage was
        // filtered out.
        connection.execute(
            "INSERT INTO session (id, title, parent_id, agent) VALUES (?1, ?2, ?3, ?4)",
            params!["ses_child2", "Child 2", "ses_abcdef123456", "build"],
        )?;
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_child2",
                "ses_child2",
                3,
                r#"{"role":"assistant","providerID":"anthropic","modelID":"claude-test","cost":0.25,"tokens":{"input":20,"output":8,"reasoning":1,"cache":{"read":4,"write":2}}}"#
            ],
        )?;
        drop(connection);

        let anthropic_only = generate_bill(&context, "ses_abcdef", &["anthropic".to_owned()])?;

        // The root's build row would otherwise claim SESSIONS 2 and $0.000000.
        assert!(has_table_row(
            &anthropic_only,
            &[
                "build",
                "1",
                "1",
                "20",
                "4",
                "2",
                "8",
                "1",
                "0ms",
                "$0.250000"
            ]
        ));
        assert!(has_table_row(
            &anthropic_only,
            &[
                "explore",
                "1",
                "1",
                "20",
                "4",
                "2",
                "8",
                "1",
                "0ms",
                "$0.250000"
            ]
        ));
        assert!(anthropic_only.contains("Amount due:  $0.500000 USD"));
        Ok(())
    }

    #[test]
    fn reports_missing_session() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;

        let failure = generate_bill(&context, "ses_missing", &[])
            .err()
            .ok_or("expected an error")?;

        assert!(matches!(failure, BillError::SessionNotFound(_)));
        Ok(())
    }

    #[test]
    fn rejects_ambiguous_prefix() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        connection.execute(
            "INSERT INTO session (id, title) VALUES (?1, ?2)",
            params!["ses_abcdef999999", "Other session"],
        )?;
        drop(connection);

        let failure = generate_bill(&context, "ses_abcdef", &[])
            .err()
            .ok_or("expected an error")?;

        assert!(matches!(failure, BillError::AmbiguousSession { .. }));
        Ok(())
    }

    #[test]
    fn generates_bill_from_legacy_json() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let session_id = "ses_legacy123456";
        let session_dir = temporary.path().join("storage/session/project");
        let message_dir = temporary.path().join("storage/message").join(session_id);

        fs::create_dir_all(&session_dir)?;
        fs::create_dir_all(&message_dir)?;
        fs::write(
            session_dir.join(format!("{session_id}.json")),
            format!(r#"{{"id":"{session_id}","title":"Legacy session"}}"#),
        )?;
        fs::write(
            message_dir.join("msg_1.json"),
            r#"{"role":"assistant","agent":"legacy-agent","providerID":"anthropic","modelID":"legacy","cost":0.25,"tokens":{"input":20,"output":8,"reasoning":1,"cache":{"read":4,"write":2}}}"#,
        )?;

        let context = StorageContext::builder()
            .data_dir(temporary.path())
            .build()?;
        let bill = generate_bill(&context, "ses_legacy", &[])?;

        assert!(has_table_row(
            &bill,
            &[
                "anthropic / legacy",
                "1",
                "20",
                "4",
                "2",
                "15.38%",
                "8",
                "1",
                "0ms",
                "$0.250000"
            ]
        ));
        assert!(has_table_row(
            &bill,
            &[
                "legacy-agent",
                "1",
                "1",
                "20",
                "4",
                "2",
                "8",
                "1",
                "0ms",
                "$0.250000"
            ]
        ));
        Ok(())
    }

    #[test]
    fn includes_legacy_child_sessions() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let root_id = "ses_legacyroot12";
        let child_id = "ses_legacychild1";
        let session_dir = temporary.path().join("storage/session/project");
        let message_dir = temporary.path().join("storage/message").join(child_id);

        fs::create_dir_all(&session_dir)?;
        fs::create_dir_all(&message_dir)?;
        fs::write(
            session_dir.join(format!("{root_id}.json")),
            format!(r#"{{"id":"{root_id}","title":"Legacy root"}}"#),
        )?;
        fs::write(
            session_dir.join(format!("{child_id}.json")),
            format!(r#"{{"id":"{child_id}","title":"Child","parentID":"{root_id}"}}"#),
        )?;
        fs::write(
            message_dir.join("msg_child.json"),
            r#"{"role":"assistant","providerID":"anthropic","modelID":"legacy-child","cost":0.75,"tokens":{"input":50,"output":10,"reasoning":2,"cache":{"read":8,"write":1}}}"#,
        )?;

        let context = StorageContext::builder()
            .data_dir(temporary.path())
            .build()?;
        let bill = generate_bill(&context, root_id, &[])?;

        assert!(bill.contains("Sessions: 2 (root + descendants)"));
        assert!(has_table_row(
            &bill,
            &[
                "anthropic / legacy-child",
                "1",
                "50",
                "8",
                "1",
                "13.55%",
                "10",
                "2",
                "0ms",
                "$0.750000"
            ]
        ));
        Ok(())
    }

    /// Number of synthetic providers in the profile fixture.
    const PROFILE_PROVIDERS: u32 = 5;
    /// Number of synthetic models per provider in the profile fixture.
    const PROFILE_MODELS: u32 = 10;

    /// Builds a SQLite fixture with `message_count` assistant messages (one
    /// tool part each) plus a catalog that prices every synthetic
    /// provider/model pair, so pricing never touches the network.
    fn profile_fixture(
        message_count: u32,
    ) -> Result<(TempDir, StorageContext), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let database = temporary.path().join(DATABASE_FILE);
        let connection = Connection::open(&database)?;

        connection.execute_batch(
            "CREATE TABLE session (id TEXT PRIMARY KEY, title TEXT NOT NULL, parent_id TEXT, agent TEXT);\
             CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT NOT NULL,\
             time_created INTEGER NOT NULL, data TEXT NOT NULL);\
             CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT NOT NULL,\
             session_id TEXT NOT NULL, data TEXT NOT NULL);",
        )?;
        connection.execute(
            "INSERT INTO session (id, title, agent) VALUES (?1, ?2, ?3)",
            params!["ses_profile_000000", "Profile session", "build"],
        )?;

        let mut insert_message = connection.prepare(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
        )?;
        let mut insert_part = connection.prepare(
            "INSERT INTO part (id, message_id, session_id, data) VALUES (?1, ?2, ?3, ?4)",
        )?;

        (0..message_count).try_for_each(|index| -> Result<(), rusqlite::Error> {
            let provider = index % PROFILE_PROVIDERS;
            let model = index % PROFILE_MODELS;
            let created = 1_000 + u64::from(index) * 10;
            let completed = created + 5_000;

            insert_message.execute(params![
                format!("msg_{index:06}"),
                "ses_profile_000000",
                created as i64,
                format!(
                    r#"{{"role":"assistant","providerID":"prov{provider}","modelID":"model{model}","cost":0.125,"time":{{"created":{created},"completed":{completed}}},"tokens":{{"input":10,"output":4,"reasoning":2,"cache":{{"read":6,"write":1}}}}}}"#
                ),
            ])?;
            insert_part.execute(params![
                format!("prt_{index:06}"),
                format!("msg_{index:06}"),
                "ses_profile_000000",
                format!(
                    r#"{{"type":"tool","state":{{"status":"completed","time":{{"start":{created},"end":{completed}}}}}}}"#
                ),
            ])?;
            Ok(())
        })?;
        drop(insert_message);
        drop(insert_part);
        drop(connection);

        let models_file = temporary.path().join("models.json");

        fs::write(&models_file, profile_catalog())?;
        let context = StorageContext::builder()
            .data_dir(temporary.path())
            .models_file(&models_file)
            .build()?;
        Ok((temporary, context))
    }

    /// Catalog JSON pricing every synthetic provider/model pair.
    fn profile_catalog() -> String {
        let mut catalog = String::from("{");

        (0..PROFILE_PROVIDERS).for_each(|provider| {
            catalog.push_str(&format!(r#""prov{provider}":{{"models":{{"#));

            (0..PROFILE_MODELS).for_each(|model| {
                catalog.push_str(&format!(
                    r#""model{model}":{{"cost":{{"input":1.25,"output":5,"cache_read":0.125,"cache_write":1}}}}"#
                ));

                if model + 1 < PROFILE_MODELS {
                    catalog.push(',');
                }
            });

            catalog.push_str("}}");

            if provider + 1 < PROFILE_PROVIDERS {
                catalog.push(',');
            }
        });

        catalog.push('}');
        catalog
    }

    /// Measures the time share of every pipeline phase on a synthetic load.
    ///
    /// Prints the per-phase durations and shares; the output is captured by
    /// the test runner unless `--nocapture` is passed.  Run with
    /// `cargo test --release profiles_pipeline_time_share -- --nocapture`
    /// for meaningful numbers.
    #[tokio::test]
    async fn profiles_pipeline_time_share() -> Result<(), Box<dyn std::error::Error>> {
        const MESSAGES: u32 = 10_000;

        let (_temporary, context) = profile_fixture(MESSAGES)?;
        let progress = PipelineProgress::default();

        progress.note_phase(0);
        let (raw, load) = {
            let now = std::time::Instant::now();
            let raw = Store::select(&context.data_dir).load("ses_profile", progress.counters())?;

            (raw, now.elapsed())
        };
        let (mut world, parse) = {
            let now = std::time::Instant::now();
            let world = parse_system(raw, &progress)?;

            (world, now.elapsed())
        };
        let mut noop = |_: &str| {};
        progress.note_phase(2);
        let (pricing, price) = {
            let now = std::time::Instant::now();
            let pricing = price_system_async(&context, &mut world, &mut noop).await?;

            (pricing, now.elapsed())
        };
        progress.note_phase(3);
        let (aggregates, aggregate) = {
            let now = std::time::Instant::now();
            let aggregates = aggregate_system(&world, &[], &progress);

            (aggregates, now.elapsed())
        };
        progress.note_phase(4);
        let (bill, render) = {
            let now = std::time::Instant::now();
            let report = render_system(
                &world,
                &aggregates.0,
                &aggregates.1,
                &aggregates.2,
                &pricing,
                &[],
            );
            let bill = render_bill(&report)?;

            (bill, now.elapsed())
        };

        let shares = [
            ("load", load),
            ("parse", parse),
            ("price", price),
            ("aggregate", aggregate),
            ("render", render),
        ];
        let total = shares
            .iter()
            .map(|(_, duration)| duration.as_secs_f64())
            .sum::<f64>();

        println!("profile: {MESSAGES} messages");
        shares.into_iter().for_each(|(name, duration)| {
            let share = duration.as_secs_f64() / total * 100.0;

            println!("  {name:<9} {duration:>8?}  {share:5.1}%");
        });
        println!("  total     {total:>8.1?}");

        // The lockless counters counted every row as the store read it and
        // as the parse phase processed it (one session, one part, and one
        // message per synthetic message), and every row in both aggregate
        // folds.
        assert_eq!(progress.counters.loaded_rows(), u64::from(MESSAGES) * 2 + 1);
        assert_eq!(progress.counters.parsed_rows(), u64::from(MESSAGES) * 2 + 1);
        assert_eq!(progress.counters.folded_rows(), 2 * u64::from(MESSAGES));
        assert_eq!(progress.phase.load(Ordering::Relaxed), 4);
        assert!(!bill.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn pipeline_progress_tracks_phases_and_counts() -> Result<(), Box<dyn std::error::Error>>
    {
        let (_temporary, context) = fixture()?;
        let progress = PipelineProgress::default();
        let mut noop = |_: &str| {};

        let world = load_system(&context, "ses_abcdef", &progress, &mut noop).await?;

        summarize_async(&context, world, &[], &mut noop, &progress).await?;

        assert_eq!(progress.phase.load(Ordering::Relaxed), 4);
        assert_eq!(progress.counters.loaded_rows(), 2);
        assert_eq!(progress.counters.parsed_rows(), 2);
        assert_eq!(progress.counters.folded_rows(), 2);
        Ok(())
    }
}
