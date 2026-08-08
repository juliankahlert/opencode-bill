//! Summaries over the session's assistant records.

use std::collections::{BTreeMap, BTreeSet};

use chrono::Datelike;

use crate::bill::PipelineProgress;
use crate::data::{
    AgentId, AgentUsage, ModelKey, SessionId, SessionTree, Totals, UsageRow, UsageStore,
};
use crate::timing::{earliest, latest};

/// Totals per provider/model pair, in deterministic key order.
///
/// Folds the store's rows directly in row order: no full row copy is
/// materialized, so a year-scale store never doubles its memory in the
/// aggregate phase.  A single sequential fold is trivially order-equivalent
/// to itself, so the saturating cost sums stay deterministic by
/// construction.
pub(crate) fn aggregate(
    store: &UsageStore,
    progress: &PipelineProgress,
) -> BTreeMap<ModelKey, Totals> {
    store
        .rows()
        .fold(BTreeMap::<ModelKey, Totals>::new(), |mut usage, row| {
            progress.folded_row();

            usage
                .entry((row.provider, row.model))
                .or_default()
                .accumulate(&row);

            usage
        })
}

/// Calendar month (1..=12) of a Unix-millisecond timestamp, if it parses.
///
/// Timestamps beyond the `i64` millisecond range wrap when cast, so they are
/// rejected instead of landing in a wrong month.
fn month_of(created: Option<u64>) -> Option<usize> {
    let created = created?;
    let millis = i64::try_from(created).ok()?;
    let time = chrono::DateTime::from_timestamp_millis(millis)?;

    Some(time.month() as usize)
}

/// Totals per agent class, seeded with every session's declared agent.
pub(crate) fn aggregate_agents(
    store: &UsageStore,
    sessions: &SessionTree,
    progress: &PipelineProgress,
) -> BTreeMap<AgentId, AgentUsage> {
    let usage = sessions
        .iter()
        .filter_map(|(session_id, node)| node.agent.map(|agent| (*session_id, agent)))
        .fold(
            BTreeMap::<AgentId, AgentUsage>::new(),
            |mut usage, (session_id, agent)| {
                usage.entry(agent).or_default().sessions.insert(session_id);

                usage
            },
        );

    store.rows().fold(usage, |mut usage, row| {
        progress.folded_row();

        let entry = usage.entry(row.agent).or_default();

        entry.sessions.insert(row.session_id);
        entry.totals.accumulate(&row);

        usage
    })
}

/// Streaming aggregate state of the window pipeline.
///
/// Every kept usage row folds into all three summaries at once — per model,
/// per agent, and per month — and is then dropped: the stream never holds
/// more than the current chunk next to these maps.  The session set and the
/// wall bounds of the kept rows feed the provider-filtered report exactly
/// like the old whole-store filter did.
#[derive(Default)]
pub(crate) struct StreamedAggregates {
    /// Totals per provider/model pair, in deterministic key order.
    pub(crate) by_model: BTreeMap<ModelKey, Totals>,
    /// Totals per agent class, seeded with every session's declared agent.
    pub(crate) by_agent: BTreeMap<AgentId, AgentUsage>,
    /// Per-month totals, January first, for the year bars.
    pub(crate) months: [Totals; 12],
    /// Sessions that produced at least one kept usage row.
    pub(crate) kept_sessions: BTreeSet<SessionId>,
    /// Earliest kept call timestamp (Unix millis).
    pub(crate) wall_start: Option<u64>,
    /// Latest kept call end timestamp (Unix millis).
    pub(crate) wall_end: Option<u64>,
}

impl StreamedAggregates {
    /// Seeds the agent summary from the billed session tree, so sessions
    /// without usage still appear as zero-usage agent classes.
    pub(crate) fn seed_agents(&mut self, sessions: &SessionTree) {
        for (session_id, node) in sessions {
            if let Some(agent) = node.agent {
                self.by_agent
                    .entry(agent)
                    .or_default()
                    .sessions
                    .insert(*session_id);
            }
        }
    }

    /// Folds one kept usage row into every summary.
    ///
    /// The three folds mirror the three whole-store passes, so the folded
    /// counters (and the deterministic saturating sums) match them exactly;
    /// the row is dropped when the fold returns.
    pub(crate) fn fold(&mut self, row: &UsageRow, progress: &PipelineProgress) {
        progress.folded_row();
        self.by_model
            .entry((row.provider, row.model))
            .or_default()
            .accumulate(row);

        progress.folded_row();
        let agent = self.by_agent.entry(row.agent).or_default();

        agent.sessions.insert(row.session_id);
        agent.totals.accumulate(row);

        progress.folded_row();
        if let Some(month) = month_of(row.created)
            && let Some(total) = self.months.get_mut(month - 1)
        {
            total.accumulate(row);
        }

        self.kept_sessions.insert(row.session_id);
        self.wall_start = earliest(self.wall_start, row.created);
        self.wall_end = latest(self.wall_end, row.completed.or(row.created));
    }

    /// The whole-window totals: the per-model rows merged in key order.
    pub(crate) fn total(&self) -> Totals {
        self.by_model
            .values()
            .fold(Totals::default(), |mut sum, row| {
                sum.merge(row);
                sum
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::{Interner, Tokens, UsageRow, UsageStore};

    fn extreme_row(interner: &mut Interner, api_ms: u64, max_tokens: bool) -> UsageRow {
        UsageRow {
            session_id: interner.intern_session("ses"),
            agent: interner.intern_agent("build"),
            provider: interner.intern_provider("openai"),
            model: interner.intern_model("gpt-test"),
            tokens: if max_tokens {
                Tokens {
                    input: u64::MAX,
                    output: u64::MAX,
                    reasoning: u64::MAX,
                    cache_read: u64::MAX,
                    cache_write: u64::MAX,
                }
            } else {
                Tokens::default()
            },
            cost_micros: 0,
            cost_unknown: false,
            api_ms,
            created: Some(0),
            completed: Some(u64::MAX),
        }
    }

    #[test]
    fn saturates_api_time_on_extreme_spans() {
        let mut interner = Interner::default();
        let row = extreme_row(&mut interner, u64::MAX, false);
        let mut store = UsageStore::default();

        store.push(row);
        store.push(row);

        let usage = aggregate(&store, &PipelineProgress::default());
        let total = usage.values().fold(Totals::default(), |mut sum, row| {
            sum.merge(row);
            sum
        });

        assert_eq!(total.api_ms, u64::MAX);
    }

    #[test]
    fn saturates_token_sums_on_extreme_usage() {
        let mut interner = Interner::default();
        let row = extreme_row(&mut interner, 0, true);
        let mut store = UsageStore::default();

        store.push(row);
        store.push(row);

        let usage = aggregate(&store, &PipelineProgress::default());
        let total = usage.values().fold(Totals::default(), |mut sum, row| {
            sum.merge(row);
            sum
        });

        assert_eq!(total.tokens.input, u64::MAX);
        assert_eq!(total.tokens.output, u64::MAX);
        assert_eq!(total.tokens.reasoning, u64::MAX);
        assert_eq!(total.tokens.cache_read, u64::MAX);
        assert_eq!(total.tokens.cache_write, u64::MAX);
    }

    #[test]
    fn saturates_token_sums_across_many_rows() {
        let mut interner = Interner::default();
        let mut store = UsageStore::default();

        // A store far larger than the old chunk size: every row saturates
        // its token lanes, and the sequential fold must keep them saturated
        // instead of wrapping.
        for _ in 0..8192 {
            store.push(extreme_row(&mut interner, 0, true));
        }

        let usage = aggregate(&store, &PipelineProgress::default());
        let total = usage.values().fold(Totals::default(), |mut sum, row| {
            sum.merge(row);
            sum
        });

        assert_eq!(total.tokens.input, u64::MAX);
        assert_eq!(total.tokens.output, u64::MAX);
        assert_eq!(total.tokens.reasoning, u64::MAX);
        assert_eq!(total.tokens.cache_read, u64::MAX);
        assert_eq!(total.tokens.cache_write, u64::MAX);
    }

    #[test]
    fn buckets_rows_into_their_creation_month() {
        let mut interner = Interner::default();
        let mut store = UsageStore::default();
        let mut row = |created| UsageRow {
            session_id: interner.intern_session("ses"),
            agent: interner.intern_agent("build"),
            provider: interner.intern_provider("openai"),
            model: interner.intern_model("gpt-test"),
            tokens: Tokens::default(),
            cost_micros: 0,
            cost_unknown: false,
            api_ms: 0,
            created,
            completed: None,
        };

        let january = chrono::DateTime::parse_from_rfc3339("2026-01-10T12:00:00Z")
            .expect("valid timestamp")
            .timestamp_millis() as u64;
        let december = chrono::DateTime::parse_from_rfc3339("2026-12-20T12:00:00Z")
            .expect("valid timestamp")
            .timestamp_millis() as u64;

        store.push(row(Some(january)));
        store.push(row(Some(december)));
        store.push(row(None));

        let mut aggregates = StreamedAggregates::default();
        let progress = PipelineProgress::default();

        for row in store.rows() {
            aggregates.fold(&row, &progress);
        }

        let months = &aggregates.months;

        assert_eq!(months[0].requests, 1);
        assert_eq!(months[11].requests, 1);
        assert_eq!(months[1].requests, 0);
        assert_eq!(
            months.iter().map(|month| month.requests).sum::<u64>(),
            2,
            "the timestamp-less row belongs to no bar"
        );
    }

    #[test]
    fn month_of_parses_only_valid_timestamps() {
        assert_eq!(month_of(None), None);
        assert_eq!(month_of(Some(u64::MAX)), None);
        assert_eq!(month_of(Some(1_000)), Some(1), "1970-01-01 is January");
        assert_eq!(
            month_of(Some(1_000_000_000_000)),
            Some(9),
            "2001-09-09 is September"
        );
    }
}
