//! Plain-text bill rendering from already-aggregated data.

use std::collections::BTreeMap;

use crate::data::{AgentUsage, ModelKey, ModelRates, Totals};
use crate::error::BillError;
use crate::store::Session;
use crate::timing::{format_duration, format_timestamp, span_ms};

const MILLION: u64 = 1_000_000;
const THOUSAND: u64 = 1_000;

/// Renders the full bill for one session.
pub(crate) fn render_bill(
    session: &Session,
    by_model: &BTreeMap<ModelKey, Totals>,
    by_agent: &BTreeMap<String, AgentUsage>,
    total: &Totals,
    pricing: &BTreeMap<ModelKey, ModelRates>,
) -> Result<String, BillError> {
    let usage_rows = by_model.iter().map(usage_row).collect::<Vec<_>>();
    let agent_rows = by_agent.iter().map(agent_row).collect::<Vec<_>>();
    let mut output = String::new();

    output.push_str("OPENCODE SESSION BILL\n");
    output.push_str(&format!("Session: {}\n", session.id));
    output.push_str(&format!("Title:   {}\n", session.title));
    output.push_str(&format!(
        "Sessions: {} (root + descendants)\n\n",
        session.session_count
    ));
    output.push_str(&render_table(
        &[
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
        ],
        &usage_rows,
    )?);

    output.push_str("\nUSAGE BY AGENT CLASS\n");
    output.push_str(&render_table(
        &[
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
        ],
        &agent_rows,
    )?);

    if !pricing.is_empty() {
        let pricing_rows = pricing.iter().map(pricing_row).collect::<Vec<_>>();

        output.push_str("\nMODEL PRICING (USD / 1M TOKENS)\n");
        output.push_str(&render_table(
            &[
                "PROVIDER / MODEL",
                "INPUT",
                "CACHE READ",
                "CACHE WRITE",
                "OUTPUT",
            ],
            &pricing_rows,
        )?);
    }

    output.push_str("\nTOTAL\n");
    output.push_str(&format!("Requests:    {}\n", compact_count(total.requests)));
    output.push_str(&format!(
        "Input:       {}\n",
        compact_count(total.tokens.input)
    ));
    output.push_str(&format!(
        "Cache read:  {}\n",
        compact_count(total.tokens.cache_read)
    ));
    output.push_str(&format!(
        "Cache write: {}\n",
        compact_count(total.tokens.cache_write)
    ));
    output.push_str(&format!(
        "Output:      {}\n",
        compact_count(total.tokens.output)
    ));
    output.push_str(&format!(
        "Reasoning:   {}\n",
        compact_count(total.tokens.reasoning)
    ));
    output.push_str(&format!(
        "Started:     {}\n",
        format_timestamp(session.wall_start)
    ));
    output.push_str(&format!(
        "Ended:       {}\n",
        format_timestamp(session.wall_end)
    ));
    output.push_str(&format!("API time:    {}\n", format_duration(total.api_ms)));
    output.push_str(&format!(
        "Wall time:   {}\n",
        format_duration(span_ms(session.wall_start, session.wall_end))
    ));
    output.push_str(&format!("Amount due:  ${:.6} USD\n", total.cost));
    Ok(output)
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
        format_duration(span_ms(totals.wall_start, totals.wall_end)),
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
        format_duration(span_ms(totals.wall_start, totals.wall_end)),
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

/// Renders a rate as dollars, or `-` when unknown.
fn format_rate(rate: Option<f64>) -> String {
    rate.map_or_else(|| "-".to_owned(), |value| format!("${value:.3}"))
}

/// Renders a plain-text table with left-aligned text and right-aligned numbers.
fn render_table(headers: &[&str], rows: &[Vec<String>]) -> Result<String, BillError> {
    if headers.is_empty() {
        return Err(render_error("table has no columns"));
    }

    if rows.iter().any(|row| row.len() != headers.len()) {
        return Err(render_error("table row has the wrong column count"));
    }

    let mut widths = headers
        .iter()
        .map(|header| header.len())
        .collect::<Vec<_>>();

    for row in rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.len());
        }
    }

    let mut output = String::new();
    let header_cells = headers
        .iter()
        .map(|value| (*value).to_owned())
        .collect::<Vec<_>>();

    push_table_row(&mut output, &header_cells, &widths);
    output.push_str(
        &widths
            .iter()
            .map(|width| "-".repeat(*width))
            .collect::<Vec<_>>()
            .join("-+-"),
    );
    output.push('\n');

    for row in rows {
        push_table_row(&mut output, row, &widths);
    }

    Ok(output)
}

/// Appends one aligned row to the table output.
fn push_table_row(output: &mut String, cells: &[String], widths: &[usize]) {
    let rendered = cells
        .iter()
        .zip(widths)
        .enumerate()
        .map(|(column, (cell, width))| {
            if column == 0 {
                return format!("{cell:<width$}");
            }

            format!("{cell:>width$}")
        })
        .collect::<Vec<_>>()
        .join(" | ");

    output.push_str(&rendered);
    output.push('\n');
}

/// Builds the error for an invalid render request.
fn render_error(detail: &str) -> BillError {
    BillError::InvalidData {
        location: "bill renderer".to_owned(),
        detail: detail.to_owned(),
    }
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
    fn aligns_text_left_and_numbers_right() -> Result<(), Box<dyn std::error::Error>> {
        let table = render_table(
            &["MODEL", "REQUESTS", "COST"],
            &[
                vec!["short".to_owned(), "2".to_owned(), "$1.00".to_owned()],
                vec![
                    "long model".to_owned(),
                    "10".to_owned(),
                    "$12.00".to_owned(),
                ],
            ],
        )?;

        assert_eq!(
            table,
            "MODEL      | REQUESTS |   COST\n-----------+----------+-------\nshort      |        2 |  $1.00\nlong model |       10 | $12.00\n"
        );
        Ok(())
    }

    #[test]
    fn compacts_large_usage_counts() {
        assert_eq!(compact_count(999), "999");
        assert_eq!(compact_count(1_000), "1.0k");
        assert_eq!(compact_count(48_579), "48.6k");
        assert_eq!(compact_count(1_000_000), "1.0M");
        assert_eq!(compact_count(1_958_181), "2.0M");
    }
}
