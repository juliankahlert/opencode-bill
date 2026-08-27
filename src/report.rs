//! Structured report data shared by every renderer (plain text and TUI).

use std::collections::BTreeMap;

use chrono::{Datelike, Duration, NaiveDate};

use crate::data::{
    AgentId, AgentUsage, Interner, MICRO_PER_DOLLAR, ModelKey, ModelRates, Tokens, Totals, World,
    cost_to_micros,
};
use crate::timing::{format_duration, format_timestamp, span_ms};

const MILLION: u64 = 1_000_000;
const THOUSAND: u64 = 1_000;

/// Footnote for every value marked with an asterisk in a bill.
///
/// The marker means the figure was estimated from external list prices, not
/// read from the recorded bill data: cost cells carry it when their total
/// includes any estimated amount, and every MODEL PRICING row carries it
/// because rates always come from external sources.  Short enough to fit
/// the TUI's first table column.
pub const ESTIMATED_NOTE: &str = "* = estimated from external list prices";

/// Identifies the period a bill covers.
#[derive(Clone)]
pub enum BillHeader {
    /// One session tree, identified by its root session.
    Session {
        /// Root session ID.
        id: String,
        /// Root session title.
        title: String,
        /// Root session creation time (Unix milliseconds), when the store
        /// records one.
        created_ms: Option<i64>,
    },
    /// One calendar month, identified by its label.
    Month {
        /// Label like "February 2026".
        label: String,
    },
    /// One calendar day, identified by its label.
    Day {
        /// Label like "August 8 2026".
        label: String,
    },
    /// One calendar year, identified by its label.
    Year {
        /// Label like "2026".
        label: String,
    },
    /// One ISO calendar week, identified by its label.
    Week {
        /// Label like "Week 32, 2026".
        label: String,
    },
}

/// How a [`BarGraph`] total is formatted: counts, durations, or money.
#[derive(Clone, Copy)]
pub enum MetricKind {
    /// Plain count (requests, tokens).
    Count,
    /// A millisecond duration (API time).
    Duration,
    /// Micro-dollars (amount due).
    Cost,
    /// Micro-dollars in a cumulative spending-limit graph.
    Limit,
}

/// One metric of a window bill: a vertical bar per month or per calendar
/// week, plus the window total.
///
/// The bars hold the metric's value per period, in calendar order; `total`
/// is the whole-window value and is shown as text, never as a bar.
#[derive(Clone)]
pub struct BarGraph {
    /// Uppercase metric label, e.g. "REQUESTS" or "AMOUNT DUE".
    pub label: &'static str,
    /// One value per period, in calendar order; the bars of the graph.
    pub bars: Vec<u64>,
    /// One label per bar, e.g. "Jan" for a year bill or "31" for a month
    /// bill's calendar weeks.
    pub labels: Vec<String>,
    /// The whole-window total of the metric, displayed as a value.
    pub total: u64,
    /// How the total is formatted for display.
    pub kind: MetricKind,
    /// Whether each bar represents a calendar period that has not started.
    ///
    /// This is populated for the month LIMIT graph and empty for all other
    /// graphs. Renderers use it to mute future-week bars without changing the
    /// cumulative values.
    pub muted_bars: Vec<bool>,
}

/// Everything a renderer needs to show one bill.
pub struct BillReport {
    /// Identifies the billed period.
    pub header: BillHeader,
    /// Number of sessions that produced the billed usage.
    pub session_count: usize,
    /// One usage row per provider/model pair.
    pub usage_rows: Vec<Vec<String>>,
    /// One usage row per agent class.
    pub agent_rows: Vec<Vec<String>>,
    /// One pricing row per priced model.
    pub pricing_rows: Vec<Vec<String>>,
    /// Whether each usage row's cost cell includes an estimated amount.
    ///
    /// Parallel to [`Self::usage_rows`]; the renderers use it to emphasize
    /// the cell (italic in the TUI).
    pub usage_cost_estimated: Vec<bool>,
    /// Whether each agent row's cost cell includes an estimated amount.
    ///
    /// Parallel to [`Self::agent_rows`].
    pub agent_cost_estimated: Vec<bool>,
    /// Whether the Amount due figure includes any estimated amount.
    pub amount_due_estimated: bool,
    /// Label/value pairs of the TOTAL section.
    pub totals: Vec<(String, String)>,
    /// One bar graph per total metric; the year bill's and the month bill's
    /// TOTAL replacement.
    ///
    /// Year bills carry one graph per month, month bills one graph per
    /// calendar week; session and day bills leave this empty.
    pub graphs: Vec<BarGraph>,
}

/// Builds the report data from aggregated usage.
pub(crate) fn build_report(
    world: &World,
    by_model: &BTreeMap<ModelKey, Totals>,
    by_agent: &BTreeMap<AgentId, AgentUsage>,
    total: &Totals,
    pricing: &BTreeMap<ModelKey, ModelRates>,
    graphs: &[BarGraph],
    interner: &Interner,
) -> BillReport {
    let usage = sorted_usage(by_model, interner);
    let agents = sorted_agents(by_agent, interner);
    let usage_estimated = usage
        .iter()
        .map(|(_, totals)| has_estimated_cost(totals))
        .collect::<Vec<_>>();
    let agent_estimated = agents
        .iter()
        .map(|(_, usage)| has_estimated_cost(&usage.totals))
        .collect::<Vec<_>>();

    debug_assert_eq!(usage.len(), usage_estimated.len());
    debug_assert_eq!(agents.len(), agent_estimated.len());

    let mut usage_rows = usage
        .iter()
        .map(|entry| usage_row(*entry, interner))
        .collect::<Vec<_>>();
    let mut agent_rows = agents
        .iter()
        .map(|entry| agent_row(*entry, interner))
        .collect::<Vec<_>>();
    align_estimated_columns(&mut usage_rows);
    align_estimated_columns(&mut agent_rows);

    BillReport {
        header: world.header.clone(),
        session_count: world.session_count,
        usage_rows,
        agent_rows,
        pricing_rows: sorted_usage(pricing, interner)
            .into_iter()
            .map(|entry| pricing_row(entry, interner))
            .collect(),
        usage_cost_estimated: usage_estimated,
        agent_cost_estimated: agent_estimated,
        amount_due_estimated: has_estimated_cost(total),
        totals: total_rows(world, total),
        graphs: graphs.to_vec(),
    }
}

/// Whether a total includes any estimated (externally priced) cost.
fn has_estimated_cost(totals: &Totals) -> bool {
    cost_to_micros(totals.estimated_cost) > 0
}

/// One label per month, January first; the year graphs' bar labels.
const MONTH_LABELS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// One label per weekday, Monday first; the week graphs' bar labels.
const WEEKDAY_LABELS: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

/// The year bill's bar graphs: one per TOTAL metric of a month bill.
///
/// `months` holds the per-month totals, January first; `total` is the whole
/// year total, which the graphs show as a value instead of a bar.
pub(crate) fn year_graphs(months: [Totals; 12], total: &Totals) -> Vec<BarGraph> {
    let labels = MONTH_LABELS.map(str::to_owned).to_vec();

    vec![
        BarGraph {
            label: "REQUESTS",
            bars: months.map(|month| month.requests).to_vec(),
            labels: labels.clone(),
            total: total.requests,
            kind: MetricKind::Count,
            muted_bars: Vec::new(),
        },
        BarGraph {
            label: "INPUT",
            bars: months.map(|month| month.tokens.input).to_vec(),
            labels: labels.clone(),
            total: total.tokens.input,
            kind: MetricKind::Count,
            muted_bars: Vec::new(),
        },
        BarGraph {
            label: "CACHE READ",
            bars: months.map(|month| month.tokens.cache_read).to_vec(),
            labels: labels.clone(),
            total: total.tokens.cache_read,
            kind: MetricKind::Count,
            muted_bars: Vec::new(),
        },
        BarGraph {
            label: "CACHE WRITE",
            bars: months.map(|month| month.tokens.cache_write).to_vec(),
            labels: labels.clone(),
            total: total.tokens.cache_write,
            kind: MetricKind::Count,
            muted_bars: Vec::new(),
        },
        BarGraph {
            label: "OUTPUT",
            bars: months.map(|month| month.tokens.output).to_vec(),
            labels: labels.clone(),
            total: total.tokens.output,
            kind: MetricKind::Count,
            muted_bars: Vec::new(),
        },
        BarGraph {
            label: "REASONING",
            bars: months.map(|month| month.tokens.reasoning).to_vec(),
            labels: labels.clone(),
            total: total.tokens.reasoning,
            kind: MetricKind::Count,
            muted_bars: Vec::new(),
        },
        BarGraph {
            label: "API TIME",
            bars: months.map(|month| month.api_ms).to_vec(),
            labels: labels.clone(),
            total: total.api_ms,
            kind: MetricKind::Duration,
            muted_bars: Vec::new(),
        },
        BarGraph {
            label: "AMOUNT DUE",
            bars: months
                .map(|month| cost_to_micros(month.cost).max(0) as u64)
                .to_vec(),
            labels: labels.clone(),
            total: cost_to_micros(total.cost).max(0) as u64,
            kind: MetricKind::Cost,
            muted_bars: Vec::new(),
        },
    ]
}

/// The month bill's bar graphs: one per TOTAL metric of a day bill plus LIMIT.
///
/// `weeks` holds the per-calendar-week totals in calendar order and
/// `week_numbers` the matching ISO week numbers, which name the bars; `total`
/// is the whole-month total, shown as a value instead of a bar. The LIMIT
/// graph contains cumulative weekly costs and uses `limit_micros` as its
/// displayed limit. An empty week list yields no graphs, so an out-of-range
/// month falls back to the plain TOTAL table.
pub(crate) fn month_graphs(
    year: i32,
    month: u32,
    weeks: &[Totals],
    week_numbers: &[u32],
    total: &Totals,
    limit_micros: u64,
    today: NaiveDate,
) -> Vec<BarGraph> {
    if week_numbers.is_empty() {
        return Vec::new();
    }

    let labels = week_numbers
        .iter()
        .map(|week| week.to_string())
        .collect::<Vec<_>>();

    let cumulative_costs = weeks
        .iter()
        .scan(0_u64, |cumulative, week| {
            *cumulative = cumulative.saturating_add(cost_to_micros(week.cost).max(0) as u64);
            Some(*cumulative)
        })
        .collect::<Vec<_>>();
    let first = NaiveDate::from_ymd_opt(year, month, 1);
    let muted_bars = week_numbers
        .iter()
        .map(|week| {
            first
                .and_then(|first| week_start_in_month(first, *week))
                .is_some_and(|start| start > today)
        })
        .collect::<Vec<_>>();

    let mut graphs = vec![
        BarGraph {
            label: "REQUESTS",
            bars: weeks.iter().map(|week| week.requests).collect(),
            labels: labels.clone(),
            total: total.requests,
            kind: MetricKind::Count,
            muted_bars: Vec::new(),
        },
        BarGraph {
            label: "INPUT",
            bars: weeks.iter().map(|week| week.tokens.input).collect(),
            labels: labels.clone(),
            total: total.tokens.input,
            kind: MetricKind::Count,
            muted_bars: Vec::new(),
        },
        BarGraph {
            label: "CACHE READ",
            bars: weeks.iter().map(|week| week.tokens.cache_read).collect(),
            labels: labels.clone(),
            total: total.tokens.cache_read,
            kind: MetricKind::Count,
            muted_bars: Vec::new(),
        },
        BarGraph {
            label: "CACHE WRITE",
            bars: weeks.iter().map(|week| week.tokens.cache_write).collect(),
            labels: labels.clone(),
            total: total.tokens.cache_write,
            kind: MetricKind::Count,
            muted_bars: Vec::new(),
        },
        BarGraph {
            label: "OUTPUT",
            bars: weeks.iter().map(|week| week.tokens.output).collect(),
            labels: labels.clone(),
            total: total.tokens.output,
            kind: MetricKind::Count,
            muted_bars: Vec::new(),
        },
        BarGraph {
            label: "REASONING",
            bars: weeks.iter().map(|week| week.tokens.reasoning).collect(),
            labels: labels.clone(),
            total: total.tokens.reasoning,
            kind: MetricKind::Count,
            muted_bars: Vec::new(),
        },
        BarGraph {
            label: "API TIME",
            bars: weeks.iter().map(|week| week.api_ms).collect(),
            labels: labels.clone(),
            total: total.api_ms,
            kind: MetricKind::Duration,
            muted_bars: Vec::new(),
        },
        BarGraph {
            label: "AMOUNT DUE",
            bars: weeks
                .iter()
                .map(|week| cost_to_micros(week.cost).max(0) as u64)
                .collect(),
            labels: labels.clone(),
            total: cost_to_micros(total.cost).max(0) as u64,
            kind: MetricKind::Cost,
            muted_bars: Vec::new(),
        },
    ];
    graphs.push(BarGraph {
        label: "LIMIT",
        bars: cumulative_costs,
        labels,
        total: limit_micros,
        kind: MetricKind::Limit,
        muted_bars,
    });

    graphs
}

/// Finds the Monday starting one of a month's ISO calendar weeks.
fn week_start_in_month(first: NaiveDate, week_number: u32) -> Option<NaiveDate> {
    let monday = first.checked_sub_signed(Duration::days(i64::from(
        first.weekday().num_days_from_monday(),
    )))?;

    std::iter::successors(Some(monday), |date| {
        date.checked_add_signed(Duration::days(7))
    })
    .take(6)
    .find(|date| date.iso_week().week() == week_number)
}

/// The week bill's bar graphs: one per TOTAL metric of a day bill.
///
/// `weekdays` holds the per-weekday totals, Monday first; `total` is the
/// whole-week total, shown as a value instead of a bar.
pub(crate) fn week_graphs(weekdays: [Totals; 7], total: &Totals) -> Vec<BarGraph> {
    let labels = WEEKDAY_LABELS.map(str::to_owned).to_vec();

    vec![
        BarGraph {
            label: "REQUESTS",
            bars: weekdays.map(|day| day.requests).to_vec(),
            labels: labels.clone(),
            total: total.requests,
            kind: MetricKind::Count,
            muted_bars: Vec::new(),
        },
        BarGraph {
            label: "INPUT",
            bars: weekdays.map(|day| day.tokens.input).to_vec(),
            labels: labels.clone(),
            total: total.tokens.input,
            kind: MetricKind::Count,
            muted_bars: Vec::new(),
        },
        BarGraph {
            label: "CACHE READ",
            bars: weekdays.map(|day| day.tokens.cache_read).to_vec(),
            labels: labels.clone(),
            total: total.tokens.cache_read,
            kind: MetricKind::Count,
            muted_bars: Vec::new(),
        },
        BarGraph {
            label: "CACHE WRITE",
            bars: weekdays.map(|day| day.tokens.cache_write).to_vec(),
            labels: labels.clone(),
            total: total.tokens.cache_write,
            kind: MetricKind::Count,
            muted_bars: Vec::new(),
        },
        BarGraph {
            label: "OUTPUT",
            bars: weekdays.map(|day| day.tokens.output).to_vec(),
            labels: labels.clone(),
            total: total.tokens.output,
            kind: MetricKind::Count,
            muted_bars: Vec::new(),
        },
        BarGraph {
            label: "REASONING",
            bars: weekdays.map(|day| day.tokens.reasoning).to_vec(),
            labels: labels.clone(),
            total: total.tokens.reasoning,
            kind: MetricKind::Count,
            muted_bars: Vec::new(),
        },
        BarGraph {
            label: "API TIME",
            bars: weekdays.map(|day| day.api_ms).to_vec(),
            labels: labels.clone(),
            total: total.api_ms,
            kind: MetricKind::Duration,
            muted_bars: Vec::new(),
        },
        BarGraph {
            label: "AMOUNT DUE",
            bars: weekdays
                .map(|day| cost_to_micros(day.cost).max(0) as u64)
                .to_vec(),
            labels,
            total: cost_to_micros(total.cost).max(0) as u64,
            kind: MetricKind::Cost,
            muted_bars: Vec::new(),
        },
    ]
}

/// Model entries in lexicographic (provider, model) string order.
///
/// The aggregate folds key rows by compact handle pairs; the pre-migration
/// bill iterated `BTreeMap<(String, String), _>` in lexicographic order, so
/// the report re-sorts by resolved strings to stay byte-identical.
fn sorted_usage<'a, V>(
    map: &'a BTreeMap<ModelKey, V>,
    interner: &Interner,
) -> Vec<(&'a ModelKey, &'a V)> {
    let mut entries = map.iter().collect::<Vec<_>>();

    entries.sort_by(|(left, _), (right, _)| {
        (
            interner.resolve_provider(left.0),
            interner.resolve_model(left.1),
        )
            .cmp(&(
                interner.resolve_provider(right.0),
                interner.resolve_model(right.1),
            ))
    });

    entries
}

/// Agent entries in lexicographic agent-name order (pre-migration order).
fn sorted_agents<'a>(
    map: &'a BTreeMap<AgentId, AgentUsage>,
    interner: &Interner,
) -> Vec<(&'a AgentId, &'a AgentUsage)> {
    let mut entries = map.iter().collect::<Vec<_>>();

    entries.sort_by(|(left, _), (right, _)| {
        interner
            .resolve_agent(**left)
            .cmp(interner.resolve_agent(**right))
    });

    entries
}

/// One provider/model usage row.
fn usage_row(entry: (&ModelKey, &Totals), interner: &Interner) -> Vec<String> {
    let ((provider, model), totals) = entry;

    vec![
        format!(
            "{} / {}",
            interner.resolve_provider(*provider),
            interner.resolve_model(*model)
        ),
        compact_count(totals.requests),
        compact_count(totals.tokens.input),
        compact_count(totals.tokens.cache_read),
        compact_count(totals.tokens.cache_write),
        hit_rate(&totals.tokens),
        compact_count(totals.tokens.output),
        compact_count(totals.tokens.reasoning),
        format_duration(totals.api_ms),
        marked_cost(cost_to_micros(totals.cost), has_estimated_cost(totals)),
    ]
}

/// One agent-class usage row.
fn agent_row(entry: (&AgentId, &AgentUsage), interner: &Interner) -> Vec<String> {
    let (agent, usage) = entry;
    let totals = &usage.totals;

    vec![
        interner.resolve_agent(*agent).to_owned(),
        compact_count(usage.sessions.len() as u64),
        compact_count(totals.requests),
        compact_count(totals.tokens.input),
        compact_count(totals.tokens.cache_read),
        compact_count(totals.tokens.cache_write),
        compact_count(totals.tokens.output),
        compact_count(totals.tokens.reasoning),
        format_duration(totals.api_ms),
        marked_cost(cost_to_micros(totals.cost), has_estimated_cost(totals)),
    ]
}

/// Cache-hit share of one provider/model's prompt tokens, as "xx.xx%".
///
/// Prompt tokens are the uncached input plus the cache reads plus the cache
/// writes; only the reads were served from cache, so writes count as misses.
/// The ratio is computed in basis points so the text is bit-identical on
/// every platform, with no float rounding in the rendered bill.  A row
/// with no prompt tokens has no rate to show and renders "-".
fn hit_rate(tokens: &Tokens) -> String {
    let prompt = tokens
        .input
        .saturating_add(tokens.cache_read)
        .saturating_add(tokens.cache_write);

    if prompt == 0 {
        return "-".to_owned();
    }

    let basis = u128::from(tokens.cache_read) * 10_000 / u128::from(prompt);

    format!("{}.{:02}%", basis / 100, basis % 100)
}

/// One model pricing row.
fn pricing_row(entry: (&ModelKey, &ModelRates), interner: &Interner) -> Vec<String> {
    let ((provider, model), rates) = entry;

    vec![
        // Every rate comes from an external source, never from the parsed
        // bill data, so the whole row carries the estimated marker.
        format!(
            "{} / {}*",
            interner.resolve_provider(*provider),
            interner.resolve_model(*model)
        ),
        format_rate(rates.input),
        format_rate(rates.cache_read),
        format_rate(rates.cache_write),
        format_rate(rates.output),
    ]
}

/// A cost figure with the estimated marker appended when it includes any
/// externally priced amount.
fn marked_cost(cost_micros: i64, estimated: bool) -> String {
    let mut cost = format_cost(cost_micros);

    if estimated {
        cost.push('*');
    }

    cost
}

/// Pads unmarked cost cells with a trailing space when a table's cost
/// column mixes estimated and historic figures.
///
/// The estimated marker sits after the last digit, so under right
/// alignment an unmarked cell ("$3.000000") ends one column earlier than
/// a marked one ("$5.000000*") and its decimal point does not line up.
/// One trailing space on every unmarked cell of the column keeps the
/// decimal points aligned in both the plain-text and the TUI tables.
fn align_estimated_columns(rows: &mut [Vec<String>]) {
    let has_marked = rows
        .iter()
        .filter_map(|row| row.last())
        .any(|cell| cell.ends_with('*'));
    let has_unmarked = rows
        .iter()
        .filter_map(|row| row.last())
        .any(|cell| !cell.ends_with('*'));

    if !(has_marked && has_unmarked) {
        return;
    }

    rows.iter_mut()
        .filter_map(|row| row.last_mut())
        .filter(|cell| !cell.ends_with('*'))
        .for_each(|cell| cell.push(' '));
}

/// Label/value pairs of the TOTAL section.
///
/// Session bills carry the wall-clock bounds (Started, Ended, Wall time);
/// month and day bills skip them because they span many unrelated sessions.
fn total_rows(world: &World, total: &Totals) -> Vec<(String, String)> {
    let is_session = matches!(world.header, BillHeader::Session { .. });

    let mut rows = vec![
        ("Requests:".to_owned(), compact_count(total.requests)),
        ("Input:".to_owned(), compact_count(total.tokens.input)),
        (
            "Cache read:".to_owned(),
            compact_count(total.tokens.cache_read),
        ),
        (
            "Cache write:".to_owned(),
            compact_count(total.tokens.cache_write),
        ),
        ("Output:".to_owned(), compact_count(total.tokens.output)),
        (
            "Reasoning:".to_owned(),
            compact_count(total.tokens.reasoning),
        ),
    ];

    if is_session {
        rows.push(("Started:".to_owned(), format_timestamp(world.wall_start)));
        rows.push(("Ended:".to_owned(), format_timestamp(world.wall_end)));
    }
    rows.push(("API time:".to_owned(), format_duration(total.api_ms)));

    if is_session {
        rows.push((
            "Wall time:".to_owned(),
            format_duration(span_ms(world.wall_start, world.wall_end)),
        ));
    }
    rows.push((
        "Amount due:".to_owned(),
        format!(
            "{} USD",
            marked_cost(cost_to_micros(total.cost), has_estimated_cost(total))
        ),
    ));

    rows
}

/// Renders a micro-dollar cost as dollars with exactly six decimals.
pub(crate) fn format_cost(cost_micros: i64) -> String {
    let sign = if cost_micros < 0 { "-" } else { "" };
    let absolute = cost_micros.unsigned_abs();

    format!(
        "{sign}${}.{:06}",
        absolute / MICRO_PER_DOLLAR as u64,
        absolute % MICRO_PER_DOLLAR as u64
    )
}

/// Renders a rate as dollars, or `-` when unknown.
fn format_rate(rate: Option<f64>) -> String {
    rate.map_or_else(|| "-".to_owned(), |value| format!("${value:.3}"))
}

/// Compacts a large token or request count to a short human string.
pub(crate) fn compact_count(value: u64) -> String {
    if value >= MILLION {
        return format!("{:.1}M", value as f64 / MILLION as f64);
    }

    if value >= THOUSAND {
        return format!("{:.1}k", value as f64 / THOUSAND as f64);
    }

    value.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::{SessionTree, UsageStore};

    #[test]
    fn compacts_large_usage_counts() {
        assert_eq!(compact_count(999), "999");
        assert_eq!(compact_count(1_000), "1.0k");
        assert_eq!(compact_count(48_579), "48.6k");
        assert_eq!(compact_count(1_000_000), "1.0M");
        assert_eq!(compact_count(1_958_181), "2.0M");
    }

    #[test]
    fn builds_total_rows() {
        let world = World {
            header: BillHeader::Session {
                id: "ses".to_owned(),
                title: "Test".to_owned(),
                created_ms: None,
            },
            session_count: 1,
            sessions: SessionTree::new(),
            entities: UsageStore::default(),
            wall_start: None,
            wall_end: None,
            interner: Interner::default(),
        };
        let rows = total_rows(&world, &Totals::default());

        assert_eq!(rows.len(), 11);
        assert_eq!(rows[0], ("Requests:".to_owned(), "0".to_owned()));
        assert_eq!(rows[6], ("Started:".to_owned(), "-".to_owned()));
        assert_eq!(
            rows[10],
            ("Amount due:".to_owned(), "$0.000000 USD".to_owned())
        );
    }

    #[test]
    fn omits_wall_bounds_from_month_total() {
        let world = World {
            header: BillHeader::Month {
                label: "February 2026".to_owned(),
            },
            session_count: 1,
            sessions: SessionTree::new(),
            entities: UsageStore::default(),
            wall_start: None,
            wall_end: None,
            interner: Interner::default(),
        };
        let rows = total_rows(&world, &Totals::default());

        assert_eq!(rows.len(), 8);
        assert!(
            rows.iter().all(|(label, _)| {
                !matches!(label.as_str(), "Started:" | "Ended:" | "Wall time:")
            })
        );
        assert_eq!(rows[6], ("API time:".to_owned(), "0ms".to_owned()));
        assert_eq!(
            rows[7],
            ("Amount due:".to_owned(), "$0.000000 USD".to_owned())
        );
    }

    #[test]
    fn omits_wall_bounds_from_day_total() {
        let world = World {
            header: BillHeader::Day {
                label: "August 8 2026".to_owned(),
            },
            session_count: 1,
            sessions: SessionTree::new(),
            entities: UsageStore::default(),
            wall_start: None,
            wall_end: None,
            interner: Interner::default(),
        };
        let rows = total_rows(&world, &Totals::default());

        assert_eq!(rows.len(), 8);
        assert!(
            rows.iter().all(|(label, _)| {
                !matches!(label.as_str(), "Started:" | "Ended:" | "Wall time:")
            })
        );
        assert_eq!(rows[6], ("API time:".to_owned(), "0ms".to_owned()));
        assert_eq!(
            rows[7],
            ("Amount due:".to_owned(), "$0.000000 USD".to_owned())
        );
    }

    #[test]
    fn year_graphs_carry_bars_and_year_total() {
        use crate::data::{Tokens, cost_from_micros};

        let mut months = [Totals::default(); 12];

        months[0].requests = 5;
        months[1].requests = 7;
        months[1].tokens.output = 10;
        months[1].api_ms = 2;
        months[1].cost = cost_from_micros(25);
        let total = Totals {
            requests: 12,
            tokens: Tokens {
                output: 10,
                ..Tokens::default()
            },
            api_ms: 2,
            cost: cost_from_micros(25),
            ..Totals::default()
        };
        let graphs = year_graphs(months, &total);

        assert_eq!(graphs.len(), 8);
        assert_eq!(graphs[0].label, "REQUESTS");
        assert_eq!(graphs[0].bars[0], 5);
        assert_eq!(graphs[0].bars[1], 7);
        assert_eq!(graphs[0].total, 12);
        assert_eq!(graphs[0].labels[0], "Jan");
        assert_eq!(graphs[0].labels[11], "Dec");
        assert_eq!(graphs[6].label, "API TIME");
        assert_eq!(graphs[6].bars[1], 2);
        assert_eq!(graphs[7].label, "AMOUNT DUE");
        assert_eq!(graphs[7].bars[1], 25);
        assert_eq!(graphs[7].total, 25);
    }

    #[test]
    fn month_graphs_carry_bars_and_week_labels() {
        use crate::data::{Tokens, cost_from_micros};

        let mut weeks = [Totals::default(); 3];

        weeks[0].requests = 5;
        weeks[0].cost = cost_from_micros(5);
        weeks[1].requests = 7;
        weeks[1].tokens.output = 10;
        weeks[1].api_ms = 2;
        weeks[1].cost = cost_from_micros(25);
        let total = Totals {
            requests: 12,
            tokens: Tokens {
                output: 10,
                ..Tokens::default()
            },
            api_ms: 2,
            cost: cost_from_micros(30),
            ..Totals::default()
        };
        let graphs = month_graphs(
            2026,
            8,
            &weeks,
            &[31, 32, 33],
            &total,
            250_000_000,
            NaiveDate::from_ymd_opt(2026, 8, 7).unwrap(),
        );

        assert_eq!(graphs.len(), 9);
        assert_eq!(graphs[0].label, "REQUESTS");
        assert_eq!(graphs[0].bars, [5, 7, 0]);
        assert_eq!(graphs[0].labels, ["31", "32", "33"]);
        assert_eq!(graphs[0].total, 12);
        assert_eq!(graphs[6].label, "API TIME");
        assert_eq!(graphs[6].bars[1], 2);
        assert_eq!(graphs[7].label, "AMOUNT DUE");
        assert_eq!(graphs[7].bars, [5, 25, 0]);
        assert_eq!(graphs[7].total, 30);
        assert_eq!(graphs[8].label, "LIMIT");
        assert_eq!(graphs[8].bars, [5, 30, 30]);
        assert_eq!(graphs[8].total, 250_000_000);
        assert_eq!(graphs[8].muted_bars, vec![false, false, true]);
    }

    #[test]
    fn month_graphs_without_weeks_fall_back_to_no_graphs() {
        assert!(
            month_graphs(
                2026,
                8,
                &[],
                &[],
                &Totals::default(),
                250_000_000,
                NaiveDate::from_ymd_opt(2026, 8, 7).unwrap(),
            )
            .is_empty()
        );
    }

    #[test]
    fn month_graphs_mute_weeks_after_the_current_week_starts() {
        let weeks = vec![Totals::default(); 6];
        let total = Totals::default();
        let week_numbers = [53, 1, 2, 3, 4, 5];

        let before_week_one = month_graphs(
            2021,
            1,
            &weeks,
            &week_numbers,
            &total,
            250_000_000,
            NaiveDate::from_ymd_opt(2021, 1, 3).unwrap(),
        );
        assert_eq!(
            before_week_one[8].muted_bars,
            vec![false, true, true, true, true, true]
        );

        let on_week_one_monday = month_graphs(
            2021,
            1,
            &weeks,
            &week_numbers,
            &total,
            250_000_000,
            NaiveDate::from_ymd_opt(2021, 1, 4).unwrap(),
        );
        assert_eq!(
            on_week_one_monday[8].muted_bars,
            vec![false, false, true, true, true, true]
        );
    }

    #[test]
    fn week_graphs_carry_bars_and_weekday_labels() {
        use crate::data::{Tokens, cost_from_micros};

        let mut weekdays = [Totals::default(); 7];

        weekdays[0].requests = 5;
        weekdays[1].requests = 7;
        weekdays[1].tokens.output = 10;
        weekdays[1].api_ms = 2;
        weekdays[1].cost = cost_from_micros(25);
        let total = Totals {
            requests: 12,
            tokens: Tokens {
                output: 10,
                ..Tokens::default()
            },
            api_ms: 2,
            cost: cost_from_micros(25),
            ..Totals::default()
        };
        let graphs = week_graphs(weekdays, &total);

        assert_eq!(graphs.len(), 8);
        assert_eq!(graphs[0].label, "REQUESTS");
        assert_eq!(graphs[0].bars, [5, 7, 0, 0, 0, 0, 0]);
        assert_eq!(
            graphs[0].labels,
            ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        );
        assert_eq!(graphs[0].total, 12);
        assert_eq!(graphs[6].label, "API TIME");
        assert_eq!(graphs[6].bars[1], 2);
        assert_eq!(graphs[7].label, "AMOUNT DUE");
        assert_eq!(graphs[7].bars[1], 25);
        assert_eq!(graphs[7].total, 25);
    }

    #[test]
    fn report_rows_follow_lexicographic_string_order() {
        use crate::data::{AgentUsage, ModelRates};

        let mut interner = Interner::default();
        // Interning order is the reverse of lexicographic: zzz handles are
        // assigned before aaa, so handle-sorted maps would print zzz first.
        let zzz_provider = interner.intern_provider("zzz-provider");
        let zzz_model = interner.intern_model("zzz-model");
        let aaa_provider = interner.intern_provider("aaa-provider");
        let aaa_model = interner.intern_model("aaa-model");
        let zzz_agent = interner.intern_agent("zzz-agent");
        let aaa_agent = interner.intern_agent("aaa-agent");

        let mut by_model = BTreeMap::new();
        by_model.insert(
            (zzz_provider, zzz_model),
            Totals {
                requests: 1,
                ..Totals::default()
            },
        );
        by_model.insert(
            (aaa_provider, aaa_model),
            Totals {
                requests: 2,
                ..Totals::default()
            },
        );

        let mut by_agent = BTreeMap::new();
        by_agent.insert(
            zzz_agent,
            AgentUsage {
                sessions: Default::default(),
                totals: Totals::default(),
            },
        );
        by_agent.insert(
            aaa_agent,
            AgentUsage {
                sessions: Default::default(),
                totals: Totals::default(),
            },
        );

        let mut pricing = BTreeMap::new();
        pricing.insert(
            (zzz_provider, zzz_model),
            ModelRates {
                input: None,
                cache_read: None,
                cache_write: None,
                output: Some(1.0),
            },
        );
        pricing.insert(
            (aaa_provider, aaa_model),
            ModelRates {
                input: None,
                cache_read: None,
                cache_write: None,
                output: Some(1.0),
            },
        );

        let world = World {
            header: BillHeader::Session {
                id: "ses".to_owned(),
                title: "Test".to_owned(),
                created_ms: None,
            },
            session_count: 0,
            sessions: SessionTree::new(),
            entities: UsageStore::default(),
            wall_start: None,
            wall_end: None,
            interner: Interner::default(),
        };
        let report = build_report(
            &world,
            &by_model,
            &by_agent,
            &Totals::default(),
            &pricing,
            &[],
            &interner,
        );

        assert_eq!(report.usage_rows[0][0], "aaa-provider / aaa-model");
        assert_eq!(report.usage_rows[1][0], "zzz-provider / zzz-model");
        assert_eq!(report.agent_rows[0][0], "aaa-agent");
        assert_eq!(report.agent_rows[1][0], "zzz-agent");
        assert_eq!(report.pricing_rows[0][0], "aaa-provider / aaa-model*");
        assert_eq!(report.pricing_rows[1][0], "zzz-provider / zzz-model*");
    }

    #[test]
    fn marks_estimated_cost_cells_and_amount_due() {
        use crate::data::cost_from_micros;

        let mut interner = Interner::default();
        let provider = interner.intern_provider("openai");
        let model = interner.intern_model("gpt-test");
        let agent = interner.intern_agent("build");

        // The model row and the total mix historic and estimated cost; the
        // agent row is purely historic.
        let estimated = Totals {
            cost: cost_from_micros(5_000_000),
            estimated_cost: cost_from_micros(2_000_000),
            ..Totals::default()
        };
        let historic = Totals {
            cost: cost_from_micros(3_000_000),
            ..Totals::default()
        };
        let total = Totals {
            cost: cost_from_micros(8_000_000),
            estimated_cost: cost_from_micros(2_000_000),
            ..Totals::default()
        };

        let mut by_model = BTreeMap::new();
        by_model.insert((provider, model), estimated);
        let mut by_agent = BTreeMap::new();
        by_agent.insert(
            agent,
            AgentUsage {
                sessions: Default::default(),
                totals: historic,
            },
        );
        let mut pricing = BTreeMap::new();
        pricing.insert(
            (provider, model),
            ModelRates {
                input: Some(1.0),
                cache_read: None,
                cache_write: None,
                output: Some(2.0),
            },
        );

        let world = World {
            header: BillHeader::Session {
                id: "ses".to_owned(),
                title: "Test".to_owned(),
                created_ms: None,
            },
            session_count: 1,
            sessions: SessionTree::new(),
            entities: UsageStore::default(),
            wall_start: None,
            wall_end: None,
            interner: Interner::default(),
        };
        let report = build_report(
            &world,
            &by_model,
            &by_agent,
            &total,
            &pricing,
            &[],
            &interner,
        );

        assert_eq!(report.usage_cost_estimated, [true]);
        assert_eq!(report.usage_rows[0][9], "$5.000000*");
        assert_eq!(report.agent_cost_estimated, [false]);
        assert_eq!(report.agent_rows[0][9], "$3.000000");
        assert!(report.amount_due_estimated);
        let amount_due = &report.totals.last().expect("amount due row").1;
        assert_eq!(amount_due, "$8.000000* USD");
        assert_eq!(report.pricing_rows[0][0], "openai / gpt-test*");
    }

    #[test]
    fn pads_unmarked_cost_cells_when_column_mixes_marked_and_plain() {
        let mut rows = vec![
            vec!["m / a".to_owned(), "$5.000000*".to_owned()],
            vec!["m / b".to_owned(), "$3.000000".to_owned()],
            vec!["m / c".to_owned(), "$2.000000*".to_owned()],
        ];

        align_estimated_columns(&mut rows);

        assert_eq!(rows[0][1], "$5.000000*");
        assert_eq!(rows[1][1], "$3.000000 ");
        assert_eq!(rows[2][1], "$2.000000*");
    }

    #[test]
    fn leaves_uniform_cost_columns_untouched() {
        let mut marked = vec![
            vec!["m / a".to_owned(), "$5.000000*".to_owned()],
            vec!["m / b".to_owned(), "$2.000000*".to_owned()],
        ];
        align_estimated_columns(&mut marked);

        assert_eq!(marked[0][1], "$5.000000*");
        assert_eq!(marked[1][1], "$2.000000*");

        let mut plain = vec![vec!["m / a".to_owned(), "$5.000000".to_owned()]];
        align_estimated_columns(&mut plain);

        assert_eq!(plain[0][1], "$5.000000");
    }

    #[test]
    fn build_report_pads_cost_columns_of_mixed_tables() {
        use crate::data::cost_from_micros;

        let mut interner = Interner::default();
        let provider = interner.intern_provider("openai");
        let estimated_model = interner.intern_model("gpt-estimated");
        let historic_model = interner.intern_model("gpt-historic");
        let estimated_agent = interner.intern_agent("est");
        let historic_agent = interner.intern_agent("his");

        let estimated = Totals {
            cost: cost_from_micros(5_000_000),
            estimated_cost: cost_from_micros(2_000_000),
            ..Totals::default()
        };
        let historic = Totals {
            cost: cost_from_micros(3_000_000),
            ..Totals::default()
        };

        let mut by_model = BTreeMap::new();
        by_model.insert((provider, estimated_model), estimated);
        by_model.insert((provider, historic_model), historic);
        let mut by_agent = BTreeMap::new();
        by_agent.insert(
            estimated_agent,
            AgentUsage {
                sessions: Default::default(),
                totals: estimated,
            },
        );
        by_agent.insert(
            historic_agent,
            AgentUsage {
                sessions: Default::default(),
                totals: historic,
            },
        );

        let world = World {
            header: BillHeader::Session {
                id: "ses".to_owned(),
                title: "Test".to_owned(),
                created_ms: None,
            },
            session_count: 2,
            sessions: SessionTree::new(),
            entities: UsageStore::default(),
            wall_start: None,
            wall_end: None,
            interner: Interner::default(),
        };
        let report = build_report(
            &world,
            &by_model,
            &by_agent,
            &Totals::default(),
            &BTreeMap::new(),
            &[],
            &interner,
        );

        let usage_cost = report
            .usage_rows
            .iter()
            .map(|row| row[9].as_str())
            .collect::<Vec<_>>();
        let agent_cost = report
            .agent_rows
            .iter()
            .map(|row| row[9].as_str())
            .collect::<Vec<_>>();
        // The estimated cells keep their marker; the historic cells gain
        // one trailing space so every cost cell has the same width.
        assert!(usage_cost.contains(&"$5.000000*"));
        assert!(usage_cost.contains(&"$3.000000 "));
        assert!(agent_cost.contains(&"$5.000000*"));
        assert!(agent_cost.contains(&"$3.000000 "));
    }

    #[test]
    fn hit_rate_divides_cache_reads_by_prompt_tokens() {
        let tokens = Tokens {
            input: 10,
            cache_read: 6,
            cache_write: 1,
            ..Tokens::default()
        };

        // 6 of 17 prompt tokens came from cache; writes count as misses.
        assert_eq!(hit_rate(&tokens), "35.29%");
    }

    #[test]
    fn hit_rate_renders_a_dash_without_prompt_tokens() {
        let tokens = Tokens {
            output: 4,
            ..Tokens::default()
        };

        assert_eq!(hit_rate(&tokens), "-");
    }

    #[test]
    fn hit_rate_reports_the_full_and_the_empty_cache() {
        let all = Tokens {
            cache_read: 40,
            ..Tokens::default()
        };
        let none = Tokens {
            input: 40,
            ..Tokens::default()
        };

        assert_eq!(hit_rate(&all), "100.00%");
        assert_eq!(hit_rate(&none), "0.00%");
    }

    #[test]
    fn leaves_historic_only_bills_unmarked() {
        use crate::data::cost_from_micros;

        let mut interner = Interner::default();
        let provider = interner.intern_provider("openai");
        let model = interner.intern_model("gpt-test");
        let agent = interner.intern_agent("build");
        let historic = Totals {
            cost: cost_from_micros(3_000_000),
            ..Totals::default()
        };

        let mut by_model = BTreeMap::new();
        by_model.insert((provider, model), historic);
        let mut by_agent = BTreeMap::new();
        by_agent.insert(
            agent,
            AgentUsage {
                sessions: Default::default(),
                totals: historic,
            },
        );
        let mut pricing = BTreeMap::new();
        pricing.insert(
            (provider, model),
            ModelRates {
                input: Some(1.0),
                cache_read: None,
                cache_write: None,
                output: Some(2.0),
            },
        );

        let world = World {
            header: BillHeader::Session {
                id: "ses".to_owned(),
                title: "Test".to_owned(),
                created_ms: None,
            },
            session_count: 1,
            sessions: SessionTree::new(),
            entities: UsageStore::default(),
            wall_start: None,
            wall_end: None,
            interner: Interner::default(),
        };
        let report = build_report(
            &world,
            &by_model,
            &by_agent,
            &historic,
            &pricing,
            &[],
            &interner,
        );

        assert_eq!(report.usage_cost_estimated, [false]);
        assert_eq!(report.usage_rows[0][9], "$3.000000");
        assert_eq!(report.agent_cost_estimated, [false]);
        assert!(!report.amount_due_estimated);
        let amount_due = &report.totals.last().expect("amount due row").1;
        assert_eq!(amount_due, "$3.000000 USD");
    }
}
