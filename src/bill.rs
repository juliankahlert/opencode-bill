//! Orchestration: load a session or month, summarize it, build the report.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::aggregate::{aggregate, aggregate_agents};
use crate::context::{StorageContext, Store};
use crate::data::{
    AgentId, AgentUsage, BillProgress, ModelKey, ModelRates, Totals, UsageStore, World,
};
use crate::error::BillError;
use crate::parse::parse_system;
use crate::pricing;
use crate::render::render_bill;
use crate::report::{BillReport, build_report};
use crate::store::{CalendarMonth, RawLoad, SessionStore, validate_reference};
use crate::timing::{earliest, latest};

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
/// defaults.
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
    let report = load_month_report_with_progress(context, year, month, providers, |_| {})?;

    render_bill(&report)
}

/// Loads a session and builds the report data, reporting each work step.
///
/// `on_task` runs before every phase (loading, aggregation, pricing, report
/// building) so a UI can show which step is currently in progress. When
/// `providers` is non-empty, only usage from those providers is included.
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
/// `on_task` runs before every phase (loading, aggregation, pricing, report
/// building) so a UI can show which step is currently in progress. When
/// `providers` is non-empty, only usage from those providers is included.
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

    load_month_report_with_progress_counted(context, year, month, providers, on_task, &counters)
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
    let mut on_task = on_task;
    let progress = PipelineProgress::with_counters(counters.clone());

    block_on(load_month_report_async(
        context,
        year,
        month,
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
    let world = load_system(context, reference, progress).await?;

    summarize_async(context, world, providers, on_task, progress).await
}

/// The async month pipeline: loads the store on the blocking pool, then
/// parses, prices, aggregates, and renders on the calling thread.  The
/// GitHub pricing page is fetched only when the bill uses `github-copilot`.
async fn load_month_report_async(
    context: &StorageContext,
    year: u32,
    month: u32,
    providers: &[String],
    on_task: &mut impl FnMut(&str),
    progress: &PipelineProgress,
) -> Result<BillReport, BillError> {
    on_task("Loading month data");
    progress.note_phase(0);
    let world = load_month_system(context, year, month, progress).await?;

    summarize_async(context, world, providers, on_task, progress).await
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
) -> Result<World, BillError> {
    validate_reference(reference)?;

    let data_dir = context.data_dir.clone();
    let reference = reference.to_owned();
    let counters = progress.counters().clone();
    let raw = blocking_load(move || Store::select(&data_dir).load(&reference, &counters)).await?;

    parse_system(raw, progress)
}

/// Loads one calendar month from the store and parses it into a world.
///
/// The store read is I/O bound, so it runs on the tokio blocking pool; the
/// month window is built inside the task because the store owns it only
/// during the read.
async fn load_month_system(
    context: &StorageContext,
    year: u32,
    month: u32,
    progress: &PipelineProgress,
) -> Result<World, BillError> {
    let data_dir = context.data_dir.clone();
    let counters = progress.counters().clone();
    let raw = blocking_load(move || {
        Store::select(&data_dir).load_month(&CalendarMonth::new(year, month)?, &counters)
    })
    .await?;

    parse_system(raw, progress)
}

/// Runs an I/O-bound store read on the tokio blocking pool.
///
/// A failed join means the blocking task itself failed (only possible when
/// it panicked — the crate has no panic paths), mapped to `BillError::Runtime`
/// so every failure stays a `Result`.
async fn blocking_load<F>(load: F) -> Result<RawLoad, BillError>
where
    F: FnOnce() -> Result<RawLoad, BillError> + Send + 'static,
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

    progress.note_phase(3);
    let (by_model, by_agent, total) = aggregate_system(&world, providers, progress);

    progress.note_phase(4);
    on_task("Building report");
    Ok(render_system(
        &world, &by_model, &by_agent, &total, &pricing,
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
    on_task("Loading model pricing");
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
        let _ = github.ensure_fetched().await;
    }
    let pricing = pricing::load_pricing(
        context.models_file.as_deref(),
        keys.iter(),
        &world.interner,
        &github,
    )?;

    on_task("Aggregating usage");
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
    let by_agent = keep_agents_with_usage(by_agent, providers, &world.entities);
    let total = by_model.values().fold(Totals::default(), |mut sum, row| {
        sum.merge(row);
        sum
    });

    (by_model, by_agent, total)
}

/// Produces the report data from the world and its aggregated components.
fn render_system(
    world: &World,
    by_model: &BTreeMap<ModelKey, Totals>,
    by_agent: &BTreeMap<AgentId, AgentUsage>,
    total: &Totals,
    pricing: &BTreeMap<ModelKey, ModelRates>,
) -> BillReport {
    build_report(world, by_model, by_agent, total, pricing, &world.interner)
}

/// Fills in an estimated cost for calls whose recorded cost is unknown.
///
/// Subscription usage often has no per-call price recorded; when a rate is
/// known for the provider/model pair, the call is billed at that rate
/// instead of leaving a zero row.  Provider corrections are never re-priced.
fn estimate_zero_costs(store: &mut UsageStore, pricing: &BTreeMap<ModelKey, ModelRates>) {
    let entities = store.entities().collect::<Vec<_>>();

    for (entity, row) in entities {
        if row.cost_unknown
            && let Some(rates) = pricing.get(&(row.provider, row.model))
        {
            // The entity came from this store's own lanes, so the checked
            // write cannot miss; ignore the defensive `false`.
            let _ =
                store.set_cost_micros(entity, pricing::estimated_cost_micros(&row.tokens, rates));
        }
    }
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
    entities: &UsageStore,
) -> BTreeMap<AgentId, AgentUsage> {
    if providers.is_empty() {
        return by_agent;
    }

    let kept_sessions = entities
        .rows()
        .map(|row| row.session_id)
        .collect::<BTreeSet<_>>();

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
                "Loading model pricing",
                "Aggregating usage",
                "Building report",
            ]
        );
        match &report.header {
            crate::report::BillHeader::Session { id, title } => {
                assert_eq!(id, "ses_abcdef123456");
                assert_eq!(title, "Test session");
            }
            crate::report::BillHeader::Month { .. } => {
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

PROVIDER / MODEL  | REQUESTS | INPUT | CACHE READ | CACHE WRITE | OUTPUT | REASONING | API TIME | COST (USD)
------------------+----------+-------+------------+-------------+--------+-----------+----------+-----------
openai / gpt-test |        1 |    10 |          6 |           1 |      4 |         2 |      0ms |  $0.125000

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

PROVIDER / MODEL  | REQUESTS | INPUT | CACHE READ | CACHE WRITE | OUTPUT | REASONING | API TIME | COST (USD)
------------------+----------+-------+------------+-------------+--------+-----------+----------+-----------
openai / gpt-test |        1 |    10 |          6 |           1 |      4 |         2 |      0ms |  $0.125000

USAGE BY AGENT CLASS
AGENT   | SESSIONS | REQUESTS | INPUT | CACHE READ | CACHE WRITE | OUTPUT | REASONING | API TIME | COST (USD)
--------+----------+----------+-------+------------+-------------+--------+-----------+----------+-----------
unknown |        1 |        1 |    10 |          6 |           1 |      4 |         2 |      0ms |  $0.125000

MODEL PRICING (USD / 1M TOKENS)
PROVIDER / MODEL  |  INPUT | CACHE READ | CACHE WRITE | OUTPUT
------------------+--------+------------+-------------+-------
openai / gpt-test | $1.250 |     $0.125 |           - | $5.000

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
        for (id, data) in [
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
        ] {
            connection.execute(
                "INSERT INTO part (id, message_id, session_id, data) VALUES (?1, ?2, ?3, ?4)",
                params![id, "msg_1", "ses_abcdef123456", data],
            )?;
        }
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
            &["openai / gpt-test", "$1.250", "$0.125", "-", "$5.000"]
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
                "4",
                "2",
                "0ms",
                "$0.125000"
            ]
        ));
        assert!(bill.contains("Amount due:  $0.125000 USD"));
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
                "4",
                "2",
                "0ms",
                "$0.125000"
            ]
        ));
        assert!(january.contains("Requests:    1"));

        let september = generate_monthly_bill(&context, 2001, 9, &[])?;

        assert!(september.contains("Sessions: 1"));
        assert!(september.contains("Requests:    1"));
        assert!(september.contains("Amount due:  $0.500000 USD"));

        let february = generate_monthly_bill(&context, 1970, 2, &[])?;

        assert!(february.contains("Sessions: 0"));
        assert!(february.contains("Requests:    0"));
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
    fn rejects_invalid_month() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;

        let failure = generate_monthly_bill(&context, 1970, 13, &[])
            .err()
            .ok_or("expected an error")?;

        assert!(matches!(failure, BillError::InvalidMonth(_)));
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
                "4",
                "2",
                "0ms",
                "$0.000043"
            ]
        ));
        assert!(bill.contains("Amount due:  $0.000043 USD"));
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
                "4",
                "2",
                "0ms",
                "$0.125000"
            ]
        ));
        assert!(!openai_only.contains("anthropic / claude-test"));
        assert!(openai_only.contains("Requests:    1"));
        assert!(openai_only.contains("Amount due:  $0.125000 USD"));
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
            &["openai / gpt-test", "$1.250", "$0.125", "-", "$5.000"]
        ));
        assert!(!bill.contains("claude-test"));
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

        for index in 0..message_count {
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
        }
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

        for provider in 0..PROFILE_PROVIDERS {
            catalog.push_str(&format!(r#""prov{provider}":{{"models":{{"#));

            for model in 0..PROFILE_MODELS {
                catalog.push_str(&format!(
                    r#""model{model}":{{"cost":{{"input":1.25,"output":5,"cache_read":0.125,"cache_write":1}}}}"#
                ));

                if model + 1 < PROFILE_MODELS {
                    catalog.push(',');
                }
            }

            catalog.push_str("}}");

            if provider + 1 < PROFILE_PROVIDERS {
                catalog.push(',');
            }
        }

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
        for (name, duration) in shares {
            let share = duration.as_secs_f64() / total * 100.0;

            println!("  {name:<9} {duration:>8?}  {share:5.1}%");
        }
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

        let world = load_system(&context, "ses_abcdef", &progress).await?;

        summarize_async(&context, world, &[], &mut |_| {}, &progress).await?;

        assert_eq!(progress.phase.load(Ordering::Relaxed), 4);
        assert_eq!(progress.counters.loaded_rows(), 2);
        assert_eq!(progress.counters.parsed_rows(), 2);
        assert_eq!(progress.counters.folded_rows(), 2);
        Ok(())
    }
}
