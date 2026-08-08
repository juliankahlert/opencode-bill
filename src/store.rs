//! Session model and the store abstraction behind it.

use std::path::PathBuf;

use chrono::NaiveDate;

use crate::error::BillError;
use crate::report::BillHeader;

/// A session reference must contain at least this many characters.
const MINIMUM_REFERENCE_LENGTH: usize = 6;

/// One calendar month of one year, as a billing window.
pub(crate) struct CalendarMonth {
    /// Human label, e.g. "February 2026".
    pub(crate) label: String,
    /// First millisecond of the month (Unix time).
    pub(crate) start_ms: u64,
    /// First millisecond after the month (Unix time).
    pub(crate) end_ms: u64,
}

impl CalendarMonth {
    /// Builds the calendar month with its exact millisecond window.
    pub(crate) fn new(year: u32, month: u32) -> Result<Self, BillError> {
        let first = first_day(year, month)?;
        let (next_year, next_month_number) = if month == 12 {
            (year + 1, 1)
        } else {
            (year, month + 1)
        };
        let next_month = first_day(next_year, next_month_number)?;
        let start_ms = month_midnight(first, year, month)?;
        let end_ms = month_midnight(next_month, next_year, next_month_number)?;

        Ok(Self {
            label: first.format("%B %Y").to_string(),
            start_ms,
            end_ms,
        })
    }
}

/// First day of a month, or an error when the month does not exist.
fn first_day(year: u32, month: u32) -> Result<NaiveDate, BillError> {
    NaiveDate::from_ymd_opt(year as i32, month, 1)
        .ok_or_else(|| BillError::InvalidMonth(format!("{year}-{month}")))
}

/// Midnight of a day as Unix milliseconds.
fn month_midnight(date: NaiveDate, year: u32, month: u32) -> Result<u64, BillError> {
    date.and_hms_opt(0, 0, 0)
        .map(|time| time.and_utc().timestamp_millis() as u64)
        .ok_or_else(|| BillError::InvalidMonth(format!("{year}-{month}")))
}

/// Wherever session data lives (SQLite or legacy JSON), it loads the same way.
///
/// Stores are stateless readers: they return raw storage rows and never
/// intern, parse, or build worlds.  `parse_system` turns raw rows into a
/// `World`.
pub(crate) trait SessionStore {
    /// Resolves a session reference and loads its raw storage rows.
    fn load(&self, reference: &str) -> Result<RawLoad, BillError>;
    /// Loads every raw storage row recorded in one calendar month.
    fn load_month(&self, month: &CalendarMonth) -> Result<RawLoad, BillError>;
}

/// Raw storage rows of one load: unparsed messages plus session components.
///
/// Rows keep the exact order the store produced them in, so the interning
/// order of `parse_system` — and with it the byte-identical bill output — is
/// preserved.
pub(crate) struct RawLoad {
    pub(crate) header: BillHeader,
    pub(crate) session_count: usize,
    pub(crate) sessions: Vec<RawSession>,
    pub(crate) messages: Vec<RawMessage>,
    pub(crate) parts: Vec<RawPart>,
}

/// One session row: identity plus the agent and parent-session components.
pub(crate) struct RawSession {
    pub(crate) id: String,
    pub(crate) agent: Option<String>,
    pub(crate) parent: Option<String>,
}

/// One message row: raw JSON payload plus its storage location.
pub(crate) struct RawMessage {
    pub(crate) id: String,
    pub(crate) session_id: String,
    pub(crate) location: String,
    pub(crate) json: String,
}

/// One tool part row: raw JSON payload plus its storage location.
pub(crate) struct RawPart {
    pub(crate) message_id: String,
    pub(crate) location: String,
    pub(crate) json: String,
}

/// A candidate session match that can display itself in ambiguity errors.
pub(crate) trait MatchName {
    /// Human-readable identity used in error messages.
    fn match_name(&self) -> String;
}

impl MatchName for String {
    fn match_name(&self) -> String {
        self.clone()
    }
}

impl MatchName for PathBuf {
    fn match_name(&self) -> String {
        self.file_stem()
            .and_then(|name| name.to_str())
            .unwrap_or("<invalid session filename>")
            .to_owned()
    }
}

/// Picks the single match, or a precise error when none or many exist.
pub(crate) fn unique_match<T>(reference: &str, matches: Vec<T>) -> Result<T, BillError>
where
    T: MatchName,
{
    if matches.is_empty() {
        return Err(BillError::SessionNotFound(reference.to_owned()));
    }

    if matches.len() > 1 {
        let names = matches
            .iter()
            .take(10)
            .map(MatchName::match_name)
            .collect::<Vec<_>>()
            .join(", ");

        return Err(BillError::AmbiguousSession {
            reference: reference.to_owned(),
            matches: names,
        });
    }

    matches
        .into_iter()
        .next()
        .ok_or_else(|| BillError::SessionNotFound(reference.to_owned()))
}

/// Rejects references that could not be a safe session prefix.
pub(crate) fn validate_reference(reference: &str) -> Result<(), BillError> {
    if reference.len() < MINIMUM_REFERENCE_LENGTH {
        return Err(BillError::InvalidSessionReference(format!(
            "must contain at least {MINIMUM_REFERENCE_LENGTH} characters"
        )));
    }

    if !reference
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    {
        return Err(BillError::InvalidSessionReference(
            "only ASCII letters, digits, '_' and '-' are allowed".to_owned(),
        ));
    }

    Ok(())
}
