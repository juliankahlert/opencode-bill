//! Errors returned while locating, reading, or rendering an OpenCode session.

use std::path::PathBuf;

use thiserror::Error;

/// Errors returned while locating, reading, or rendering an OpenCode session.
#[derive(Debug, Error)]
pub enum BillError {
    /// No platform data directory is available.
    #[error("the platform does not provide a local data directory")]
    MissingDataDirectory,

    /// The supplied session reference is invalid.
    #[error("invalid session reference: {0}")]
    InvalidSessionReference(String),

    /// The month window does not exist in the calendar.
    #[error("invalid month: {0}")]
    InvalidMonth(String),

    /// The day window does not exist in the calendar.
    #[error("invalid day: {0}")]
    InvalidDay(String),

    /// The week window does not exist in the calendar.
    #[error("invalid week: {0}")]
    InvalidWeek(String),

    /// The year window does not exist in the calendar.
    #[error("invalid year: {0}")]
    InvalidYear(String),

    /// No session matched the supplied reference.
    #[error("session '{0}' was not found")]
    SessionNotFound(String),

    /// More than one session matched the supplied reference.
    #[error("session reference '{reference}' is ambiguous; matches: {matches}")]
    AmbiguousSession {
        /// The user-supplied reference.
        reference: String,
        /// A comma-separated match list.
        matches: String,
    },

    /// OpenCode's SQLite database could not be queried.
    #[error("could not query OpenCode database at {path}: {source}")]
    Database {
        /// Database path.
        path: PathBuf,
        /// SQLite error.
        #[source]
        source: rusqlite::Error,
    },

    /// A storage file could not be read.
    #[error("could not read {path}: {source}")]
    Read {
        /// File or directory path.
        path: PathBuf,
        /// I/O error.
        #[source]
        source: std::io::Error,
    },

    /// Stored JSON is malformed or has an invalid field.
    #[error("invalid OpenCode data in {location}: {detail}")]
    InvalidData {
        /// Database or file location.
        location: String,
        /// Validation detail.
        detail: String,
    },

    /// OpenCode's SQLite database matches neither known storage layout.
    #[error(
        "unsupported OpenCode database layout in {0}: found neither v2 \
         ('session_v2' + 'session_message') nor v1 ('session') tables"
    )]
    UnsupportedLayout(PathBuf),

    /// The tokio runtime could not be started, or a runtime task failed.
    #[error("async runtime failure: {0}")]
    Runtime(#[source] std::io::Error),
}
