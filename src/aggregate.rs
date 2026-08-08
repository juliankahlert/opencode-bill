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

/// ISO week number (1..=53) of a Unix-millisecond timestamp, if it parses.
///
/// The same timestamp guard as [`month_of`]: out-of-range millis are
/// rejected instead of wrapping into a wrong week.
fn week_of(created: Option<u64>) -> Option<u32> {
    let created = created?;
    let millis = i64::try_from(created).ok()?;
    let time = chrono::DateTime::from_timestamp_millis(millis)?;

    Some(time.iso_week().week())
}

/// Index of a row's ISO week inside a month's week list, if the week is
/// listed.  Rows without a parseable timestamp belong to no bar, exactly
/// like the year bill's month bucketing.
fn week_index_of(created: Option<u64>, weeks: &[u32]) -> Option<usize> {
    let week = week_of(created)?;

    weeks.iter().position(|listed| *listed == week)
}

/// Weekday index (0 = Monday .. 6 = Sunday) of a Unix-millisecond timestamp,
/// if it parses.  The same timestamp guard as [`week_of`].
fn weekday_index_of(created: Option<u64>) -> Option<usize> {
    let created = created?;
    let millis = i64::try_from(created).ok()?;
    let time = chrono::DateTime::from_timestamp_millis(millis)?;

    Some(time.weekday().num_days_from_monday() as usize)
}

/// One month's calendar weeks as ISO week numbers, in day order.
///
/// A month spans four to six weeks; a January can start in ISO week 53 of
/// the previous year, so the list is ordered by calendar days, never by
/// week number.  Months outside the chrono date range yield no weeks.
pub(crate) fn month_weeks(year: i32, month: u32) -> Option<Vec<u32>> {
    let first = chrono::NaiveDate::from_ymd_opt(year, month, 1)?;
    let days = first.num_days_in_month();

    Some(
        (1..=days)
            .filter_map(|day| {
                first
                    .with_day(day.into())
                    .map(|date| date.iso_week().week())
            })
            .fold(Vec::new(), |mut weeks, week| {
                if weeks.last() != Some(&week) {
                    weeks.push(week);
                }

                weeks
            }),
    )
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
    /// The billed month's calendar weeks in day order; empty for bills
    /// without week bars.
    pub(crate) weeks: Vec<u32>,
    /// Per-week totals of the billed month, aligned with [`Self::weeks`].
    pub(crate) week_totals: Vec<Totals>,
    /// Whether the rows fold into per-weekday bars (week bills).
    pub(crate) weekday_fold: bool,
    /// Per-weekday totals, Monday first, for the week bars.
    pub(crate) weekdays: [Totals; 7],
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

    /// Sets the billed month's calendar weeks; the fold then buckets rows
    /// into the matching [`Self::week_totals`] bars.  Other windows leave
    /// the list empty and fold no week bars.
    pub(crate) fn set_weeks(&mut self, weeks: Vec<u32>) {
        self.week_totals = vec![Totals::default(); weeks.len()];
        self.weeks = weeks;
    }

    /// Turns on per-weekday folding (week bills); the fold then buckets
    /// rows into [`Self::weekdays`] bars, Monday first.
    pub(crate) fn set_weekday_fold(&mut self) {
        self.weekday_fold = true;
    }

    /// Folds one kept usage row into every summary.
    ///
    /// The folds mirror the whole-store passes, so the folded counters (and
    /// the deterministic saturating sums) match them exactly; the row is
    /// dropped when the fold returns.
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

        if !self.weeks.is_empty() {
            progress.folded_row();
            if let Some(index) = week_index_of(row.created, &self.weeks)
                && let Some(total) = self.week_totals.get_mut(index)
            {
                total.accumulate(row);
            }
        }

        if self.weekday_fold {
            progress.folded_row();
            if let Some(index) = weekday_index_of(row.created)
                && let Some(total) = self.weekdays.get_mut(index)
            {
                total.accumulate(row);
            }
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

    #[test]
    fn buckets_rows_into_their_creation_week() {
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

        // August 2026 spans weeks 31..=36; August 10 is in week 33 and
        // August 17 in week 34.
        let week_33 = chrono::DateTime::parse_from_rfc3339("2026-08-10T12:00:00Z")
            .expect("valid timestamp")
            .timestamp_millis() as u64;
        let week_34 = chrono::DateTime::parse_from_rfc3339("2026-08-17T12:00:00Z")
            .expect("valid timestamp")
            .timestamp_millis() as u64;

        store.push(row(Some(week_33)));
        store.push(row(Some(week_33)));
        store.push(row(Some(week_34)));
        store.push(row(None));

        let mut aggregates = StreamedAggregates::default();

        aggregates.set_weeks(vec![31, 32, 33, 34, 35, 36]);
        let progress = PipelineProgress::default();

        for row in store.rows() {
            aggregates.fold(&row, &progress);
        }

        assert_eq!(aggregates.weeks, [31, 32, 33, 34, 35, 36]);
        assert_eq!(
            aggregates
                .week_totals
                .iter()
                .map(|week| week.requests)
                .collect::<Vec<_>>(),
            [0, 0, 2, 1, 0, 0],
            "the timestamp-less row belongs to no bar"
        );
    }

    #[test]
    fn week_helpers_parse_and_index() {
        assert_eq!(week_of(None), None);
        assert_eq!(week_of(Some(u64::MAX)), None);
        assert_eq!(week_of(Some(1_000)), Some(1), "1970-01-01 is week 1");
        let weeks = vec![31, 32, 33, 34, 35, 36];

        assert_eq!(week_index_of(Some(1_000), &weeks), None);
        assert_eq!(week_index_of(None, &weeks), None);
        assert_eq!(
            week_index_of(
                Some(
                    chrono::DateTime::parse_from_rfc3339("2026-08-17T12:00:00Z")
                        .expect("valid timestamp")
                        .timestamp_millis() as u64
                ),
                &weeks
            ),
            Some(3)
        );
    }

    #[test]
    fn buckets_rows_into_their_creation_weekday() {
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

        // August 3 2026 is a Monday (weekday 0); August 7 is a Friday (4).
        let monday = chrono::DateTime::parse_from_rfc3339("2026-08-03T12:00:00Z")
            .expect("valid timestamp")
            .timestamp_millis() as u64;
        let friday = chrono::DateTime::parse_from_rfc3339("2026-08-07T12:00:00Z")
            .expect("valid timestamp")
            .timestamp_millis() as u64;

        store.push(row(Some(monday)));
        store.push(row(Some(monday)));
        store.push(row(Some(friday)));
        store.push(row(None));

        let mut aggregates = StreamedAggregates::default();

        aggregates.set_weekday_fold();
        let progress = PipelineProgress::default();

        for row in store.rows() {
            aggregates.fold(&row, &progress);
        }

        assert_eq!(
            aggregates
                .weekdays
                .iter()
                .map(|day| day.requests)
                .collect::<Vec<_>>(),
            [2, 0, 0, 0, 1, 0, 0],
            "the timestamp-less row belongs to no bar"
        );
    }

    #[test]
    fn weekday_index_of_parses_only_valid_timestamps() {
        assert_eq!(weekday_index_of(None), None);
        assert_eq!(weekday_index_of(Some(u64::MAX)), None);
        // 1970-01-01 is a Thursday (weekday 3).
        assert_eq!(weekday_index_of(Some(1_000)), Some(3));
        assert_eq!(
            weekday_index_of(Some(
                chrono::DateTime::parse_from_rfc3339("2026-08-03T12:00:00Z")
                    .expect("valid timestamp")
                    .timestamp_millis() as u64
            )),
            Some(0),
            "2026-08-03 is a Monday"
        );
    }

    #[test]
    fn month_weeks_list_calendar_weeks_in_day_order() {
        assert_eq!(month_weeks(2026, 8), Some(vec![31, 32, 33, 34, 35, 36]));
        assert_eq!(month_weeks(2026, 2), Some(vec![5, 6, 7, 8, 9]));
        // January 2021 starts in ISO week 53 of 2020: the list must keep
        // the calendar order, not the numeric one.
        assert_eq!(month_weeks(2021, 1), Some(vec![53, 1, 2, 3, 4]));
        // Beyond the chrono date range: no weeks, no bars.
        assert_eq!(month_weeks(262_143, 1), None);
    }
}
