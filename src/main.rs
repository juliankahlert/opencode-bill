use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use chrono::{Datelike, NaiveDate};
use clap::Parser;
use log::debug;
use opencode_bill::{
    StorageContext, generate_bill, generate_daily_bill, generate_monthly_bill,
    generate_weekly_bill, generate_yearly_bill,
};

mod tui;

/// Command-line arguments for one bill request.
#[derive(Parser)]
#[command(
    name = "opencode-bill",
    version,
    about = "Generate a bill for an OpenCode session, calendar month, calendar week, calendar year, or calendar day"
)]
struct Arguments {
    /// Full OpenCode session ID or a unique prefix; with no session, no
    /// --month, no --year, no --week, and no --today the current month is
    /// billed
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

    /// Bill the calendar year; no value means the current year
    #[arg(
        long,
        value_name = "YEAR",
        num_args = 0..=1,
        default_missing_value = "0",
        value_parser = clap::value_parser!(u32).range(0..=i64::from(i32::MAX)),
        conflicts_with_all = ["month", "today", "session"]
    )]
    year: Option<u32>,

    /// Bill the current calendar day instead of one session and its
    /// sub-agents
    #[arg(long, conflicts_with_all = ["month", "session"])]
    today: bool,

    /// Bill the ISO calendar week (1-53) of the current year; no value means
    /// the current week
    #[arg(
        long,
        value_name = "WEEK",
        num_args = 0..=1,
        default_missing_value = "0",
        value_parser = clap::value_parser!(u8).range(0..=53),
        conflicts_with_all = ["month", "year", "today", "session"]
    )]
    week: Option<u8>,

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
    /// One calendar year.
    Year(u32),
    /// One calendar day of the current year.
    Day(u32, u32, u32),
    /// One ISO calendar week of the current year.
    Week(u32, u32),
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = Arguments::parse();
    let data_dir = arguments.data_dir.as_ref();
    let context = match data_dir {
        Some(path) => StorageContext::builder().data_dir(path).build()?,
        None => StorageContext::builder().platform_data_dir()?.build()?,
    };
    let today = chrono::Local::now().date_naive();
    let request = resolve_request(
        arguments.month,
        arguments.year,
        arguments.today,
        arguments.week,
        arguments.session,
        today,
    );

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
        Request::Year(year) => generate_yearly_bill(&context, *year, providers)?,
        Request::Day(year, month, day) => {
            generate_daily_bill(&context, *year, *month, *day, providers)?
        }
        Request::Week(year, week) => generate_weekly_bill(&context, *year, *week, providers)?,
    };
    let mut stdout = io::stdout().lock();

    stdout.write_all(bill.as_bytes())?;
    stdout.flush()?;
    Ok(())
}

/// Map the CLI arguments to one bill request.
///
/// `--month` without a value (clap delivers `Some(0)`) and no arguments at
/// all both mean the current calendar month; `--year` without a value means
/// the current calendar year; `--week` without a value means the current ISO
/// calendar week; `--today` means the current calendar day.  clap rejects
/// `--week` combined with `--month`, `--year`, `--today`, or a session,
/// `--today` combined with `--month`, `--year`, or a session, and `--year`
/// combined with `--month` or a session, so those arms are unreachable; the
/// month value wins, then the year value, then the week value, then the
/// today flag, then the session.
fn resolve_request(
    month: Option<u8>,
    year: Option<u32>,
    today: bool,
    week: Option<u8>,
    session: Option<String>,
    today_date: NaiveDate,
) -> Request {
    match (month, year, today, week, session) {
        (Some(0), _, _, _, _) | (None, None, false, None, None) => {
            Request::Month(today_date.year() as u32, today_date.month())
        }
        (Some(month), _, _, _, _) => Request::Month(today_date.year() as u32, month as u32),
        (None, Some(0), _, _, _) => Request::Year(today_date.year() as u32),
        (None, Some(year), _, _, _) => Request::Year(year),
        (None, None, true, _, _) => Request::Day(
            today_date.year() as u32,
            today_date.month(),
            today_date.day(),
        ),
        (None, None, false, Some(0), _) => {
            Request::Week(today_date.year() as u32, today_date.iso_week().week())
        }
        (None, None, false, Some(week), _) => Request::Week(today_date.year() as u32, week as u32),
        (None, None, false, None, Some(session)) => Request::Session(session),
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

        assert_eq!(
            resolve_request(None, None, false, None, None, today),
            Request::Month(2026, 8)
        );
    }

    #[test]
    fn month_without_value_bills_current_month() {
        let today = NaiveDate::from_ymd_opt(2026, 8, 7).unwrap();

        assert_eq!(
            resolve_request(Some(0), None, false, None, None, today),
            Request::Month(2026, 8)
        );
    }

    #[test]
    fn month_with_value_bills_that_month_of_current_year() {
        let today = NaiveDate::from_ymd_opt(2026, 8, 7).unwrap();

        assert_eq!(
            resolve_request(Some(2), None, false, None, None, today),
            Request::Month(2026, 2)
        );
    }

    #[test]
    fn year_without_value_bills_current_year() {
        let today = NaiveDate::from_ymd_opt(2026, 8, 7).unwrap();

        assert_eq!(
            resolve_request(None, Some(0), false, None, None, today),
            Request::Year(2026)
        );
    }

    #[test]
    fn year_with_value_bills_that_year() {
        let today = NaiveDate::from_ymd_opt(2026, 8, 7).unwrap();

        assert_eq!(
            resolve_request(None, Some(2025), false, None, None, today),
            Request::Year(2025)
        );
    }

    #[test]
    fn today_flag_bills_current_day() {
        let today = NaiveDate::from_ymd_opt(2026, 8, 7).unwrap();

        assert_eq!(
            resolve_request(None, None, true, None, None, today),
            Request::Day(2026, 8, 7)
        );
    }

    #[test]
    fn session_argument_bills_session() {
        let today = NaiveDate::from_ymd_opt(2026, 8, 7).unwrap();

        assert_eq!(
            resolve_request(None, None, false, None, Some("abc123".to_string()), today),
            Request::Session("abc123".to_string())
        );
    }

    #[test]
    fn week_without_value_bills_current_iso_week() {
        let today = NaiveDate::from_ymd_opt(2026, 8, 7).unwrap();

        // 2026-08-07 is a Friday in ISO week 32.
        assert_eq!(
            resolve_request(None, None, false, Some(0), None, today),
            Request::Week(2026, 32)
        );
    }

    #[test]
    fn week_with_value_bills_that_week_of_current_year() {
        let today = NaiveDate::from_ymd_opt(2026, 8, 7).unwrap();

        assert_eq!(
            resolve_request(None, None, false, Some(5), None, today),
            Request::Week(2026, 5)
        );
    }

    #[test]
    fn today_conflicts_with_month_year_week_and_session() {
        assert!(Arguments::try_parse_from(["opencode-bill", "--today", "--month"]).is_err());
        assert!(Arguments::try_parse_from(["opencode-bill", "--today", "--year"]).is_err());
        assert!(Arguments::try_parse_from(["opencode-bill", "--today", "--week"]).is_err());
        assert!(Arguments::try_parse_from(["opencode-bill", "--today", "abc123"]).is_err());
        assert!(Arguments::try_parse_from(["opencode-bill", "--today"]).is_ok());
    }

    #[test]
    fn year_conflicts_with_month_week_and_session() {
        assert!(Arguments::try_parse_from(["opencode-bill", "--year", "--month"]).is_err());
        assert!(Arguments::try_parse_from(["opencode-bill", "--year", "--week"]).is_err());
        assert!(Arguments::try_parse_from(["opencode-bill", "--year", "abc123"]).is_err());
        assert!(Arguments::try_parse_from(["opencode-bill", "--year"]).is_ok());
        assert!(Arguments::try_parse_from(["opencode-bill", "--year", "2025"]).is_ok());
    }

    #[test]
    fn week_conflicts_with_month_year_today_and_session() {
        assert!(Arguments::try_parse_from(["opencode-bill", "--week", "--month"]).is_err());
        assert!(Arguments::try_parse_from(["opencode-bill", "--week", "--year"]).is_err());
        assert!(Arguments::try_parse_from(["opencode-bill", "--week", "--today"]).is_err());
        assert!(Arguments::try_parse_from(["opencode-bill", "--week", "abc123"]).is_err());
        assert!(Arguments::try_parse_from(["opencode-bill", "--week"]).is_ok());
        assert!(Arguments::try_parse_from(["opencode-bill", "--week", "32"]).is_ok());
        assert!(Arguments::try_parse_from(["opencode-bill", "--week", "54"]).is_err());
    }
}
