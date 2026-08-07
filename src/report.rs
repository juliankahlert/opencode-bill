//! Structured report data shared by every renderer (plain text and TUI).

use std::collections::BTreeMap;

use crate::data::{AgentUsage, ModelKey, ModelRates, Totals};
use crate::store::BillInput;
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
    },
    /// One calendar month, identified by its label.
    Month {
        /// Label like "February 2026".
        label: String,
    },
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
}

/// Builds the report data from aggregated usage.
pub(crate) fn build_report(
    input: &BillInput,
    by_model: &BTreeMap<ModelKey, Totals>,
    by_agent: &BTreeMap<String, AgentUsage>,
    total: &Totals,
    pricing: &BTreeMap<ModelKey, ModelRates>,
) -> BillReport {
    BillReport {
        header: input.header.clone(),
        session_count: input.session_count,
        usage_rows: by_model.iter().map(usage_row).collect(),
        agent_rows: by_agent.iter().map(agent_row).collect(),
        pricing_rows: pricing.iter().map(pricing_row).collect(),
        totals: total_rows(input, total),
    }
}

/// One provider/model usage row.
fn usage_row(entry: (&ModelKey, &Totals)) -> Vec<String> {
    let ((provider, model), totals) = entry;

    vec![
        format!("{provider} / {model}"),
        compact_count(totals.requests),
        compact_count(totals.tokens.input),
        compact_count(totals.tokens.cache_read),
        compact_count(totals.tokens.cache_write),
        compact_count(totals.tokens.output),
        compact_count(totals.tokens.reasoning),
        format_duration(totals.api_ms),
        format!("${:.6}", totals.cost),
    ]
}

/// One agent-class usage row.
fn agent_row(entry: (&String, &AgentUsage)) -> Vec<String> {
    let (agent, usage) = entry;
    let totals = &usage.totals;

    vec![
        agent.clone(),
        compact_count(usage.sessions.len() as u64),
        compact_count(totals.requests),
        compact_count(totals.tokens.input),
        compact_count(totals.tokens.cache_read),
        compact_count(totals.tokens.cache_write),
        compact_count(totals.tokens.output),
        compact_count(totals.tokens.reasoning),
        format_duration(totals.api_ms),
        format!("${:.6}", totals.cost),
    ]
}

/// One model pricing row.
fn pricing_row(entry: (&ModelKey, &ModelRates)) -> Vec<String> {
    let ((provider, model), rates) = entry;

    vec![
        format!("{provider} / {model}"),
        format_rate(rates.input),
        format_rate(rates.cache_read),
        format_rate(rates.cache_write),
        format_rate(rates.output),
    ]
}

/// Label/value pairs of the TOTAL section.
///
/// Session bills carry the wall-clock bounds (Started, Ended, Wall time);
/// month bills skip them because they span many unrelated sessions.
fn total_rows(input: &BillInput, total: &Totals) -> Vec<(String, String)> {
    let is_session = matches!(input.header, BillHeader::Session { .. });

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
        rows.push(("Started:".to_owned(), format_timestamp(input.wall_start)));
        rows.push(("Ended:".to_owned(), format_timestamp(input.wall_end)));
    }
    rows.push(("API time:".to_owned(), format_duration(total.api_ms)));

    if is_session {
        rows.push((
            "Wall time:".to_owned(),
            format_duration(span_ms(input.wall_start, input.wall_end)),
        ));
    }
    rows.push(("Amount due:".to_owned(), format!("${:.6} USD", total.cost)));

    rows
}

/// Renders a rate as dollars, or `-` when unknown.
fn format_rate(rate: Option<f64>) -> String {
    rate.map_or_else(|| "-".to_owned(), |value| format!("${value:.3}"))
}

/// Compacts a large token or request count to a short human string.
fn compact_count(value: u64) -> String {
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
        let input = BillInput {
            header: BillHeader::Session {
                id: "ses".to_owned(),
                title: "Test".to_owned(),
            },
            session_count: 1,
            session_agents: BTreeMap::new(),
            messages: Vec::new(),
            wall_start: None,
            wall_end: None,
        };
        let rows = total_rows(&input, &Totals::default());

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
        let input = BillInput {
            header: BillHeader::Month {
                label: "February 2026".to_owned(),
            },
            session_count: 1,
            session_agents: BTreeMap::new(),
            messages: Vec::new(),
            wall_start: None,
            wall_end: None,
        };
        let rows = total_rows(&input, &Totals::default());

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
}
