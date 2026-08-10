//! Structured report data shared by every renderer (plain text and TUI).

use std::collections::BTreeMap;

use crate::data::{
    AgentId, AgentUsage, Interner, MICRO_PER_DOLLAR, ModelKey, ModelRates, Totals, World,
    cost_to_micros,
};
use crate::timing::{format_duration, format_timestamp, span_ms};

const MILLION: u64 = 1_000_000;
const THOUSAND: u64 = 1_000;

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
    /// What each bar stands for, e.g. "MONTH" or "WEEK"; the title is
    /// `"{label} BY {unit}"`.
    pub unit: &'static str,
    /// What the total is, e.g. "Year total" or "Month total".
    pub total_label: &'static str,
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
    BillReport {
        header: world.header.clone(),
        session_count: world.session_count,
        usage_rows: sorted_usage(by_model, interner)
            .into_iter()
            .map(|entry| usage_row(entry, interner))
            .collect(),
        agent_rows: sorted_agents(by_agent, interner)
            .into_iter()
            .map(|entry| agent_row(entry, interner))
            .collect(),
        pricing_rows: sorted_usage(pricing, interner)
            .into_iter()
            .map(|entry| pricing_row(entry, interner))
            .collect(),
        totals: total_rows(world, total),
        graphs: graphs.to_vec(),
    }
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
            unit: "MONTH",
            total_label: "Year total",
        },
        BarGraph {
            label: "INPUT",
            bars: months.map(|month| month.tokens.input).to_vec(),
            labels: labels.clone(),
            total: total.tokens.input,
            kind: MetricKind::Count,
            unit: "MONTH",
            total_label: "Year total",
        },
        BarGraph {
            label: "CACHE READ",
            bars: months.map(|month| month.tokens.cache_read).to_vec(),
            labels: labels.clone(),
            total: total.tokens.cache_read,
            kind: MetricKind::Count,
            unit: "MONTH",
            total_label: "Year total",
        },
        BarGraph {
            label: "CACHE WRITE",
            bars: months.map(|month| month.tokens.cache_write).to_vec(),
            labels: labels.clone(),
            total: total.tokens.cache_write,
            kind: MetricKind::Count,
            unit: "MONTH",
            total_label: "Year total",
        },
        BarGraph {
            label: "OUTPUT",
            bars: months.map(|month| month.tokens.output).to_vec(),
            labels: labels.clone(),
            total: total.tokens.output,
            kind: MetricKind::Count,
            unit: "MONTH",
            total_label: "Year total",
        },
        BarGraph {
            label: "REASONING",
            bars: months.map(|month| month.tokens.reasoning).to_vec(),
            labels: labels.clone(),
            total: total.tokens.reasoning,
            kind: MetricKind::Count,
            unit: "MONTH",
            total_label: "Year total",
        },
        BarGraph {
            label: "API TIME",
            bars: months.map(|month| month.api_ms).to_vec(),
            labels: labels.clone(),
            total: total.api_ms,
            kind: MetricKind::Duration,
            unit: "MONTH",
            total_label: "Year total",
        },
        BarGraph {
            label: "AMOUNT DUE",
            bars: months
                .map(|month| cost_to_micros(month.cost).max(0) as u64)
                .to_vec(),
            labels,
            total: cost_to_micros(total.cost).max(0) as u64,
            kind: MetricKind::Cost,
            unit: "MONTH",
            total_label: "Year total",
        },
    ]
}

/// The month bill's bar graphs: one per TOTAL metric of a day bill.
///
/// `weeks` holds the per-calendar-week totals in calendar order and
/// `week_numbers` the matching ISO week numbers, which name the bars; `total`
/// is the whole-month total, shown as a value instead of a bar.  An empty
/// week list yields no graphs, so an out-of-range month falls back to the
/// plain TOTAL table.
pub(crate) fn month_graphs(
    weeks: &[Totals],
    week_numbers: &[u32],
    total: &Totals,
) -> Vec<BarGraph> {
    if week_numbers.is_empty() {
        return Vec::new();
    }

    let labels = week_numbers
        .iter()
        .map(|week| week.to_string())
        .collect::<Vec<_>>();

    vec![
        BarGraph {
            label: "REQUESTS",
            bars: weeks.iter().map(|week| week.requests).collect(),
            labels: labels.clone(),
            total: total.requests,
            kind: MetricKind::Count,
            unit: "WEEK",
            total_label: "Month total",
        },
        BarGraph {
            label: "INPUT",
            bars: weeks.iter().map(|week| week.tokens.input).collect(),
            labels: labels.clone(),
            total: total.tokens.input,
            kind: MetricKind::Count,
            unit: "WEEK",
            total_label: "Month total",
        },
        BarGraph {
            label: "CACHE READ",
            bars: weeks.iter().map(|week| week.tokens.cache_read).collect(),
            labels: labels.clone(),
            total: total.tokens.cache_read,
            kind: MetricKind::Count,
            unit: "WEEK",
            total_label: "Month total",
        },
        BarGraph {
            label: "CACHE WRITE",
            bars: weeks.iter().map(|week| week.tokens.cache_write).collect(),
            labels: labels.clone(),
            total: total.tokens.cache_write,
            kind: MetricKind::Count,
            unit: "WEEK",
            total_label: "Month total",
        },
        BarGraph {
            label: "OUTPUT",
            bars: weeks.iter().map(|week| week.tokens.output).collect(),
            labels: labels.clone(),
            total: total.tokens.output,
            kind: MetricKind::Count,
            unit: "WEEK",
            total_label: "Month total",
        },
        BarGraph {
            label: "REASONING",
            bars: weeks.iter().map(|week| week.tokens.reasoning).collect(),
            labels: labels.clone(),
            total: total.tokens.reasoning,
            kind: MetricKind::Count,
            unit: "WEEK",
            total_label: "Month total",
        },
        BarGraph {
            label: "API TIME",
            bars: weeks.iter().map(|week| week.api_ms).collect(),
            labels: labels.clone(),
            total: total.api_ms,
            kind: MetricKind::Duration,
            unit: "WEEK",
            total_label: "Month total",
        },
        BarGraph {
            label: "AMOUNT DUE",
            bars: weeks
                .iter()
                .map(|week| cost_to_micros(week.cost).max(0) as u64)
                .collect(),
            labels,
            total: cost_to_micros(total.cost).max(0) as u64,
            kind: MetricKind::Cost,
            unit: "WEEK",
            total_label: "Month total",
        },
    ]
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
            unit: "DAY",
            total_label: "Week total",
        },
        BarGraph {
            label: "INPUT",
            bars: weekdays.map(|day| day.tokens.input).to_vec(),
            labels: labels.clone(),
            total: total.tokens.input,
            kind: MetricKind::Count,
            unit: "DAY",
            total_label: "Week total",
        },
        BarGraph {
            label: "CACHE READ",
            bars: weekdays.map(|day| day.tokens.cache_read).to_vec(),
            labels: labels.clone(),
            total: total.tokens.cache_read,
            kind: MetricKind::Count,
            unit: "DAY",
            total_label: "Week total",
        },
        BarGraph {
            label: "CACHE WRITE",
            bars: weekdays.map(|day| day.tokens.cache_write).to_vec(),
            labels: labels.clone(),
            total: total.tokens.cache_write,
            kind: MetricKind::Count,
            unit: "DAY",
            total_label: "Week total",
        },
        BarGraph {
            label: "OUTPUT",
            bars: weekdays.map(|day| day.tokens.output).to_vec(),
            labels: labels.clone(),
            total: total.tokens.output,
            kind: MetricKind::Count,
            unit: "DAY",
            total_label: "Week total",
        },
        BarGraph {
            label: "REASONING",
            bars: weekdays.map(|day| day.tokens.reasoning).to_vec(),
            labels: labels.clone(),
            total: total.tokens.reasoning,
            kind: MetricKind::Count,
            unit: "DAY",
            total_label: "Week total",
        },
        BarGraph {
            label: "API TIME",
            bars: weekdays.map(|day| day.api_ms).to_vec(),
            labels: labels.clone(),
            total: total.api_ms,
            kind: MetricKind::Duration,
            unit: "DAY",
            total_label: "Week total",
        },
        BarGraph {
            label: "AMOUNT DUE",
            bars: weekdays
                .map(|day| cost_to_micros(day.cost).max(0) as u64)
                .to_vec(),
            labels,
            total: cost_to_micros(total.cost).max(0) as u64,
            kind: MetricKind::Cost,
            unit: "DAY",
            total_label: "Week total",
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
        compact_count(totals.tokens.output),
        compact_count(totals.tokens.reasoning),
        format_duration(totals.api_ms),
        format_cost(cost_to_micros(totals.cost)),
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
        format_cost(cost_to_micros(totals.cost)),
    ]
}

/// One model pricing row.
fn pricing_row(entry: (&ModelKey, &ModelRates), interner: &Interner) -> Vec<String> {
    let ((provider, model), rates) = entry;

    vec![
        format!(
            "{} / {}",
            interner.resolve_provider(*provider),
            interner.resolve_model(*model)
        ),
        format_rate(rates.input),
        format_rate(rates.cache_read),
        format_rate(rates.cache_write),
        format_rate(rates.output),
    ]
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
        format!("{} USD", format_cost(cost_to_micros(total.cost))),
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
        assert_eq!(graphs[0].unit, "MONTH");
        assert_eq!(graphs[0].total_label, "Year total");
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
            cost: cost_from_micros(25),
            ..Totals::default()
        };
        let graphs = month_graphs(&weeks, &[31, 32, 33], &total);

        assert_eq!(graphs.len(), 8);
        assert_eq!(graphs[0].label, "REQUESTS");
        assert_eq!(graphs[0].bars, [5, 7, 0]);
        assert_eq!(graphs[0].labels, ["31", "32", "33"]);
        assert_eq!(graphs[0].total, 12);
        assert_eq!(graphs[0].unit, "WEEK");
        assert_eq!(graphs[0].total_label, "Month total");
        assert_eq!(graphs[6].label, "API TIME");
        assert_eq!(graphs[6].bars[1], 2);
        assert_eq!(graphs[7].label, "AMOUNT DUE");
        assert_eq!(graphs[7].bars[1], 25);
        assert_eq!(graphs[7].total, 25);
    }

    #[test]
    fn month_graphs_without_weeks_fall_back_to_no_graphs() {
        assert!(month_graphs(&[], &[], &Totals::default()).is_empty());
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
        assert_eq!(graphs[0].unit, "DAY");
        assert_eq!(graphs[0].total_label, "Week total");
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
        assert_eq!(report.pricing_rows[0][0], "aaa-provider / aaa-model");
        assert_eq!(report.pricing_rows[1][0], "zzz-provider / zzz-model");
    }
}
