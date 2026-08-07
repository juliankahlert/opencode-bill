use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use chrono::Datelike;
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
    /// Full OpenCode session ID or a unique ID prefix
    #[arg(required_unless_present = "month", conflicts_with = "month")]
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
enum Request {
    /// One session tree, by ID or prefix.
    Session(String),
    /// One calendar month of the current year.
    Month(u32, u32),
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = Arguments::parse();
    let data_dir = arguments.data_dir.as_ref();
    let context = match data_dir {
        Some(path) => StorageContext::builder().data_dir(path).build()?,
        None => StorageContext::builder().platform_data_dir()?.build()?,
    };
    let request = match arguments.month {
        Some(0) => {
            let today = chrono::Local::now().date_naive();

            Request::Month(today.year() as u32, today.month())
        }
        Some(month) => Request::Month(current_year(), month as u32),
        None => Request::Session(
            arguments
                .session
                .ok_or("clap did not provide the required session argument")?,
        ),
    };

    if io::stdout().is_terminal() {
        return match tui::run(context, request, arguments.providers)? {
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

/// Year of the current local date.
fn current_year() -> u32 {
    chrono::Local::now().date_naive().year() as u32
}

fn main() -> ExitCode {
    env_logger::init();

    if let Err(failure) = run() {
        debug!("command failed: {failure}");
        eprintln!("opencode-bill: {failure}");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}
