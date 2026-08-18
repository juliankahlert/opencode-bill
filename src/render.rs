//! Plain-text bill rendering from the structured report data.

use crate::error::BillError;
use crate::report::{
    BarGraph, BillHeader, BillReport, ESTIMATED_NOTE, MetricKind, compact_count, format_cost,
};
use crate::timing::format_duration;

/// Height of a bar graph in rows, one bar row per character.
const GRAPH_HEIGHT: u64 = 8;
/// One bar segment: a double-width block plus a gap to the next bar.
const GRAPH_BAR: &str = "██  ";
/// Empty bar-column space, width-matched to [`GRAPH_BAR`].
const GRAPH_GAP: &str = "    ";
/// Title padding, one column wider than the longest graph title so the
/// window total always starts with a space.
const GRAPH_TITLE_WIDTH: usize = 12;

/// Renders the full bill for one session, calendar month, or calendar day.
pub fn render_bill(report: &BillReport) -> Result<String, BillError> {
    let mut output = String::new();

    match &report.header {
        BillHeader::Session {
            id,
            title,
            created_ms,
        } => {
            output.push_str("OPENCODE SESSION BILL\n");
            output.push_str(&format!("Session: {id}\n"));
            output.push_str(&format!("Title:   {title}\n"));
            if let Some(created_ms) = created_ms {
                output.push_str(&format!(
                    "Created: {}\n",
                    crate::timing::format_local_iso(*created_ms)
                ));
            }
            output.push_str(&format!(
                "Sessions: {} (root + descendants)\n\n",
                report.session_count
            ));
        }
        BillHeader::Month { label } => {
            output.push_str("OPENCODE MONTHLY BILL\n");
            output.push_str(&format!("Month:   {label}\n"));
            output.push_str(&format!("Sessions: {}\n\n", report.session_count));
        }
        BillHeader::Day { label } => {
            output.push_str("OPENCODE DAILY BILL\n");
            output.push_str(&format!("Day:     {label}\n"));
            output.push_str(&format!("Sessions: {}\n\n", report.session_count));
        }
        BillHeader::Year { label } => {
            output.push_str("OPENCODE YEARLY BILL\n");
            output.push_str(&format!("Year:    {label}\n"));
            output.push_str(&format!("Sessions: {}\n\n", report.session_count));
        }
        BillHeader::Week { label } => {
            output.push_str("OPENCODE WEEKLY BILL\n");
            output.push_str(&format!("Week:    {label}\n"));
            output.push_str(&format!("Sessions: {}\n\n", report.session_count));
        }
    }
    output.push_str(&render_table(
        &[
            "PROVIDER / MODEL",
            "REQUESTS",
            "INPUT",
            "CACHE READ",
            "CACHE WRITE",
            "HIT",
            "OUTPUT",
            "REASONING",
            "API TIME",
            "COST (USD)",
        ],
        &report.usage_rows,
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
            "COST (USD)",
        ],
        &report.agent_rows,
    )?);

    if !report.pricing_rows.is_empty() {
        output.push_str("\nMODEL PRICING (USD / 1M TOKENS)\n");
        output.push_str(&render_table(
            &[
                "PROVIDER / MODEL",
                "INPUT",
                "CACHE READ",
                "CACHE WRITE",
                "OUTPUT",
            ],
            &report.pricing_rows,
        )?);
        output.push_str(ESTIMATED_NOTE);
        output.push('\n');
    }

    if report.graphs.is_empty() {
        output.push_str("\nTOTAL\n");
        report.totals.iter().for_each(|(label, value)| {
            // Labels are left-padded to 13 columns; keep every label short enough.
            debug_assert!(label.len() <= 13, "total label too wide: {label}");
            output.push_str(&format!("{label:<13}{value}\n"));
        });
        return Ok(output);
    }

    report.graphs.iter().for_each(|graph| {
        output.push('\n');
        output.push_str(&graph_lines(graph).join("\n"));
        output.push('\n');
    });
    Ok(output)
}

/// Lines of one bar graph: a title with the window total, the vertical
/// bars, and the period labels (months or calendar weeks).
///
/// The overall total is a value in the title, never a bar.  A metric with no
/// usage in the window renders only the title line.
pub fn graph_lines(graph: &BarGraph) -> Vec<String> {
    let mut lines = vec![format!(
        "{:<GRAPH_TITLE_WIDTH$}Total: {}",
        graph.label,
        graph_total(graph)
    )];
    let max = graph.bars.iter().copied().max().unwrap_or(0);

    if max > 0 {
        let heights = graph
            .bars
            .iter()
            .map(|value| bar_height(*value, max))
            .collect::<Vec<_>>();

        (0..GRAPH_HEIGHT).rev().for_each(|row| {
            lines.push(
                heights
                    .iter()
                    .map(|height| if *height > row { GRAPH_BAR } else { GRAPH_GAP })
                    .collect(),
            );
        });
        lines.push(
            graph
                .labels
                .iter()
                .map(|label| format!("{label:<4}"))
                .collect(),
        );
    }

    lines
}

/// Bar height of one month's value, scaled to [`GRAPH_HEIGHT`] rows.
///
/// Any non-zero value renders at least one row, so small months still show a
/// visible bar; the tallest month fills the full height.
fn bar_height(value: u64, max: u64) -> u64 {
    if value == 0 {
        return 0;
    }

    let scaled = (u128::from(value) * u128::from(GRAPH_HEIGHT)) / u128::from(max);

    u64::try_from(scaled.max(1)).unwrap_or(GRAPH_HEIGHT)
}

/// Formats a graph's window total per its metric kind.
fn graph_total(graph: &BarGraph) -> String {
    match graph.kind {
        MetricKind::Count => compact_count(graph.total),
        MetricKind::Duration => format_duration(graph.total),
        MetricKind::Cost => format!("{} USD", format_cost(graph.total as i64)),
    }
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

    rows.iter().for_each(|row| {
        widths
            .iter_mut()
            .zip(row)
            .for_each(|(width, cell)| *width = (*width).max(cell.len()));
    });

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

    rows.iter()
        .for_each(|row| push_table_row(&mut output, row, &widths));

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
    fn renders_vertical_bars_per_month() {
        let graph = BarGraph {
            label: "REQUESTS",
            bars: vec![10, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 1],
            labels: [
                "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
            total: 16,
            kind: MetricKind::Count,
        };
        let lines = graph_lines(&graph);

        assert_eq!(lines.len(), 10);
        assert_eq!(lines[0], "REQUESTS    Total: 16");
        // The tallest month fills the top row; the 1-row month stays at the
        // bottom, so the top row ends with an empty gap.
        assert!(lines[1].starts_with(GRAPH_BAR));
        assert!(lines[1].ends_with(GRAPH_GAP));
        assert!(lines[8].ends_with(GRAPH_BAR));
        assert!(lines[9].starts_with("Jan Feb Mar"));
        assert!(lines[9].ends_with("Dec "));
    }

    #[test]
    fn renders_vertical_bars_per_calendar_week() {
        let graph = BarGraph {
            label: "REQUESTS",
            bars: vec![0, 10, 5, 1, 0, 0],
            labels: vec!["31", "32", "33", "34", "35", "36"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            total: 16,
            kind: MetricKind::Count,
        };
        let lines = graph_lines(&graph);

        assert_eq!(lines.len(), 10);
        assert_eq!(lines[0], "REQUESTS    Total: 16");
        assert!(lines[9].starts_with("31  32  33"));
        assert!(lines[9].ends_with("36  "));
    }

    #[test]
    fn skips_bars_when_no_month_has_usage() {
        let graph = BarGraph {
            label: "AMOUNT DUE",
            bars: vec![0; 12],
            labels: vec!["Jan".to_owned()],
            total: 0,
            kind: MetricKind::Cost,
        };

        assert_eq!(
            graph_lines(&graph),
            vec!["AMOUNT DUE  Total: $0.000000 USD"]
        );
    }

    #[test]
    fn formats_graph_total_per_kind() {
        let graph = BarGraph {
            label: "API TIME",
            bars: vec![0; 12],
            labels: vec!["Jan".to_owned()],
            total: 3_600_000,
            kind: MetricKind::Duration,
        };

        assert_eq!(graph_lines(&graph), vec!["API TIME    Total: 1h 0m"]);
    }
}
