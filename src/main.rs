use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Arg, Command, value_parser};
use log::debug;
use opencode_bill::{StorageContext, generate_bill};

fn command() -> Command {
    Command::new("opencode-bill")
        .version(env!("CARGO_PKG_VERSION"))
        .about("Generate a bill for an OpenCode session")
        .arg(
            Arg::new("session")
                .help("Full OpenCode session ID or a unique ID prefix")
                .required(true),
        )
        .arg(
            Arg::new("data-dir")
                .long("data-dir")
                .help("OpenCode data directory (defaults to the platform data directory)")
                .value_name("PATH")
                .value_parser(value_parser!(PathBuf)),
        )
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = command().get_matches();
    let data_dir = arguments.get_one::<PathBuf>("data-dir");
    let session = arguments
        .get_one::<String>("session")
        .ok_or("clap did not provide the required session argument")?;
    let context = match data_dir {
        Some(path) => StorageContext::builder().data_dir(path).build()?,
        None => StorageContext::builder().platform_data_dir()?.build()?,
    };
    let bill = generate_bill(&context, session)?;
    let mut stdout = io::stdout().lock();

    stdout.write_all(bill.as_bytes())?;
    stdout.flush()?;
    Ok(())
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
