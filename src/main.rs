use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use chrono::{Datelike, NaiveDate};
use clap::Parser;
use log::debug;
use opencode_bill::{StorageContext, generate_bill, generate_monthly_bill};

mod tui;

/// Command-line arguments for one bill request.
#[derive(Parser)]
#[command(
    name = "opencode-bill",
    version,
    about = "Generate a bill for an OpenCode session or calendar month"
)]
struct Arguments {
    /// Full OpenCode session ID or a unique ID prefix; with no session and
    /// no --month the current month is billed
    #[arg(conflicts_with = "month")]
    session: Option<String>,

    /// OpenCode data directory (defaults to the platform data directory)
    #[arg(long, value_name = "PATH")]
    data_dir: Option<PathBuf>,

    /// Bill the calendar month (1-12) of the current year; no value means
    /// the current month
    #[arg(
        long,
        value_name = "MONTH",
        num_args = 0..=1,
        default_missing_value = "0",
        value_parser = clap::value_parser!(u8).range(0..=12)
    )]
    month: Option<u8>,

    /// Only include usage from this provider; repeatable
    #[arg(long = "provider", value_name = "PROVIDER")]
    providers: Vec<String>,
}

/// What the user asked to bill.
#[derive(Debug, PartialEq)]
enum Request {
    /// One session tree, by ID or prefix.
    Session(String),
    /// One calendar month of the current year.
    Month(u32, u32),
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = Arguments::parse();
    let data_dir = arguments.data_dir.as_ref();
    let context = match data_dir {
        Some(path) => StorageContext::builder().data_dir(path).build()?,
        None => StorageContext::builder().platform_data_dir()?.build()?,
    };
    let today = chrono::Local::now().date_naive();
    let request = resolve_request(arguments.month, arguments.session, today);

    if io::stdout().is_terminal() {
        return match tui::run(context, request, arguments.providers).await? {
            tui::TuiOutcome::Success => Ok(()),
            tui::TuiOutcome::Failed(failure) => Err(failure.into()),
        };
    }

    let providers = &arguments.providers;
    let bill = match &request {
        Request::Session(reference) => generate_bill(&context, reference, providers)?,
        Request::Month(year, month) => generate_monthly_bill(&context, *year, *month, providers)?,
    };
    let mut stdout = io::stdout().lock();

    stdout.write_all(bill.as_bytes())?;
    stdout.flush()?;
    Ok(())
}

/// Map the CLI arguments to one bill request.
///
/// `--month` without a value (clap delivers `Some(0)`) and no arguments at
/// all both mean the current calendar month.
fn resolve_request(month: Option<u8>, session: Option<String>, today: NaiveDate) -> Request {
    match (month, session) {
        (Some(0), _) | (None, None) => Request::Month(today.year() as u32, today.month()),
        (Some(month), _) => Request::Month(today.year() as u32, month as u32),
        (None, Some(session)) => Request::Session(session),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    env_logger::init();

    if let Err(failure) = run().await {
        debug!("command failed: {failure}");
        eprintln!("opencode-bill: {failure}");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_arguments_bills_current_month() {
        let today = NaiveDate::from_ymd_opt(2026, 8, 7).unwrap();

        assert_eq!(resolve_request(None, None, today), Request::Month(2026, 8));
    }

    #[test]
    fn month_without_value_bills_current_month() {
        let today = NaiveDate::from_ymd_opt(2026, 8, 7).unwrap();

        assert_eq!(
            resolve_request(Some(0), None, today),
            Request::Month(2026, 8)
        );
    }

    #[test]
    fn month_with_value_bills_that_month_of_current_year() {
        let today = NaiveDate::from_ymd_opt(2026, 8, 7).unwrap();

        assert_eq!(
            resolve_request(Some(2), None, today),
            Request::Month(2026, 2)
        );
    }

    #[test]
    fn session_argument_bills_session() {
        let today = NaiveDate::from_ymd_opt(2026, 8, 7).unwrap();

        assert_eq!(
            resolve_request(None, Some("abc123".to_string()), today),
            Request::Session("abc123".to_string())
        );
    }
}
