//! Plain-text bill rendering from the structured report data.

use crate::error::BillError;
use crate::report::BillReport;

/// Renders the full bill for one session.
pub(crate) fn render_bill(report: &BillReport) -> Result<String, BillError> {
    let mut output = String::new();

    output.push_str("OPENCODE SESSION BILL\n");
    output.push_str(&format!("Session: {}\n", report.session_id));
    output.push_str(&format!("Title:   {}\n", report.session_title));
    output.push_str(&format!(
        "Sessions: {} (root + descendants)\n\n",
        report.session_count
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
            "WALL TIME",
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
    }

    output.push_str("\nTOTAL\n");
    for (label, value) in &report.totals {
        // Labels are left-padded to 13 columns; keep every label short enough.
        debug_assert!(label.len() <= 13, "total label too wide: {label}");
        output.push_str(&format!("{label:<13}{value}\n"));
    }
    Ok(output)
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
}
