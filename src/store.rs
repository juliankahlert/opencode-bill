//! Session model and the store abstraction behind it.

use std::path::PathBuf;

use chrono::{Datelike, Days, NaiveDate, Weekday};

use crate::data::BillProgress;
use crate::error::BillError;
use crate::report::BillHeader;

/// A session reference must contain at least this many characters.
const MINIMUM_REFERENCE_LENGTH: usize = 6;

/// One calendar period (a month, a day, a year, or an ISO week) as a billing
/// window.
#[derive(Clone)]
pub(crate) struct CalendarWindow {
    /// Human label, e.g. "February 2026" or "August 8 2026".
    pub(crate) label: String,
    /// First millisecond of the window (Unix time).
    pub(crate) start_ms: u64,
    /// First millisecond after the window (Unix time).
    pub(crate) end_ms: u64,
    /// The header identifying the billed period.
    pub(crate) header: BillHeader,
}

impl CalendarWindow {
    /// Builds one calendar month with its exact millisecond window.
    pub(crate) fn month(year: u32, month: u32) -> Result<Self, BillError> {
        let detail = format!("{year}-{month}");
        if !(1970..=i32::MAX as u32).contains(&year) {
            return Err(BillError::InvalidMonth(detail.clone()));
        }
        let first = first_day(year, month)?;
        let (next_year, next_month_number) = if month == 12 {
            (year + 1, 1)
        } else {
            (year, month + 1)
        };
        let next_month = first_day(next_year, next_month_number)?;
        let label = first.format("%B %Y").to_string();

        Ok(Self {
            label: label.clone(),
            start_ms: midnight_ms(first).ok_or_else(|| BillError::InvalidMonth(detail.clone()))?,
            end_ms: midnight_ms(next_month).ok_or_else(|| BillError::InvalidMonth(detail))?,
            header: BillHeader::Month { label },
        })
    }

    /// Builds one calendar day with its exact millisecond window.
    pub(crate) fn day(year: u32, month: u32, day: u32) -> Result<Self, BillError> {
        let detail = format!("{year}-{month}-{day}");
        if !(1970..=i32::MAX as u32).contains(&year) {
            return Err(BillError::InvalidDay(detail.clone()));
        }
        let date = NaiveDate::from_ymd_opt(year as i32, month, day)
            .ok_or_else(|| BillError::InvalidDay(detail.clone()))?;
        let next = date
            .succ_opt()
            .ok_or_else(|| BillError::InvalidDay(detail.clone()))?;
        let label = format!("{} {} {}", date.format("%B"), date.day(), date.format("%Y"));

        Ok(Self {
            label: label.clone(),
            start_ms: midnight_ms(date).ok_or_else(|| BillError::InvalidDay(detail.clone()))?,
            end_ms: midnight_ms(next).ok_or_else(|| BillError::InvalidDay(detail))?,
            header: BillHeader::Day { label },
        })
    }

    /// Builds one calendar year with its exact millisecond window.
    pub(crate) fn year(year: u32) -> Result<Self, BillError> {
        let detail = format!("{year}");
        if !(1970..=i32::MAX as u32).contains(&year) {
            return Err(BillError::InvalidYear(detail.clone()));
        }
        let first = first_day(year, 1).map_err(|_| BillError::InvalidYear(detail.clone()))?;
        let next_year =
            first_day(year + 1, 1).map_err(|_| BillError::InvalidYear(detail.clone()))?;
        let label = first.format("%Y").to_string();

        Ok(Self {
            label: label.clone(),
            start_ms: midnight_ms(first).ok_or_else(|| BillError::InvalidYear(detail.clone()))?,
            end_ms: midnight_ms(next_year).ok_or_else(|| BillError::InvalidYear(detail))?,
            header: BillHeader::Year { label },
        })
    }

    /// Builds one ISO calendar week with its exact millisecond window.
    ///
    /// The week is the ISO week number 1..=53 of the week-year `year`; the
    /// window runs Monday 00:00 UTC to the next Monday 00:00 UTC, so a week
    /// can cross the calendar year boundary (week 1 of 2026 starts on
    /// December 29, 2025).  Weeks the ISO calendar does not contain (week 0,
    /// week 54, or week 53 of a 52-week year) are rejected.
    pub(crate) fn week(year: u32, week: u32) -> Result<Self, BillError> {
        let detail = format!("{year}-{week}");
        if !(1970..=i32::MAX as u32).contains(&year) {
            return Err(BillError::InvalidWeek(detail.clone()));
        }
        let monday = NaiveDate::from_isoywd_opt(year as i32, week, Weekday::Mon)
            .ok_or_else(|| BillError::InvalidWeek(detail.clone()))?;
        let next = monday
            .checked_add_days(Days::new(7))
            .ok_or_else(|| BillError::InvalidWeek(detail.clone()))?;
        let label = format!("Week {week}, {year}");

        Ok(Self {
            label: label.clone(),
            start_ms: midnight_ms(monday).ok_or_else(|| BillError::InvalidWeek(detail.clone()))?,
            end_ms: midnight_ms(next).ok_or_else(|| BillError::InvalidWeek(detail))?,
            header: BillHeader::Week { label },
        })
    }
}

/// First day of a month, or an error when the month does not exist.
fn first_day(year: u32, month: u32) -> Result<NaiveDate, BillError> {
    NaiveDate::from_ymd_opt(year as i32, month, 1)
        .ok_or_else(|| BillError::InvalidMonth(format!("{year}-{month}")))
}

/// Midnight of a day as Unix milliseconds.
fn midnight_ms(date: NaiveDate) -> Option<u64> {
    date.and_hms_opt(0, 0, 0)
        .map(|time| time.and_utc().timestamp_millis() as u64)
}

/// Length of one UTC day in milliseconds.
///
/// Window timestamps are UTC millis, so a day is always exactly this long:
/// no time zone, no daylight saving.  The streaming pipeline cuts its
/// per-day batches at this stride from the window start.
pub(crate) const DAY_MILLIS: u64 = 24 * 60 * 60 * 1000;

/// Maximum raw messages per streaming batch.
///
/// The pipeline stages (store pump, bounded channel, batch parse) hold one
/// batch at a time, so this cap bounds the raw JSON in flight no matter how
/// large a single day is: a day with more messages is split into several
/// batches of at most this size, in window order.
pub(crate) const MESSAGE_BATCH_LIMIT: usize = 10_000;

/// Maximum accumulated payload bytes per streaming batch.
///
/// A day of heavy usage can reach the row cap with gigabytes of payload
/// (tool parts included), so the pump also flushes on bytes: real batches
/// stay around this budget, and the in-flight copies (channel capacity plus
/// pump and parse) stay bounded regardless of how much one day holds.
pub(crate) const MESSAGE_BATCH_BYTES: usize = 32 * 1024 * 1024;

/// Rows fetched per payload query slice.
///
/// Payloads (message data and tool parts) are fetched by primary key in
/// slices of this size, so the `IN (...)` lists stay small and every query
/// returns a bounded result.
pub(crate) const DATA_SLICE: usize = 512;

/// Whether a streamed batch must flush: its row count or accumulated
/// payload reached the budget, or the calendar day changed.
pub(crate) fn batch_full(rows: usize, bytes: usize, day_changed: bool) -> bool {
    rows >= MESSAGE_BATCH_LIMIT || bytes >= MESSAGE_BATCH_BYTES || day_changed
}

/// Wherever session data lives (SQLite or legacy JSON), it loads the same way.
///
/// Stores are stateless readers: they return raw storage rows and never
/// intern, parse, or build worlds.  `parse_system` turns raw rows into a
/// `World`.
pub(crate) trait SessionStore {
    /// Resolves a session reference and loads its raw storage rows,
    /// counting every row read into the shared progress handle.
    fn load(&self, reference: &str, progress: &BillProgress) -> Result<RawLoad, BillError>;
    /// Loads one window's session and tool-part rows without the message
    /// payloads, counting every row read into the shared progress handle.
    ///
    /// The streaming pipeline loads this small metadata first (sessions and
    /// parts), then streams the messages in per-day batches.
    fn load_window_meta(
        &self,
        window: &CalendarWindow,
        progress: &BillProgress,
    ) -> Result<RawWindowMeta, BillError>;
    /// Streams one window's raw messages and tool parts in per-day batches,
    /// in window order, invoking `on_batch` once per day (or once per split
    /// batch when a day exceeds the limits).
    /// Only one day's rows are materialized at a time — tool parts can be
    /// as large as the messages, so they stream alongside instead of
    /// loading whole-window — and a callback error stops the stream and
    /// becomes the load error.  The SQLite store cuts one batch per calendar
    /// day in wall-clock order; the legacy file store cuts one batch per
    /// (session, day).  No batch exceeds [`MESSAGE_BATCH_LIMIT`] messages: a
    /// larger day is split into several batches in row order (its parts
    /// split with them).  The bill output stays byte-identical to a
    /// whole-window load either way.
    ///
    /// The callback returns the batch it consumed, drained of its rows, so
    /// the store reuses the same row buffers across the whole stream instead
    /// of allocating fresh storage per day.
    fn stream_window_messages(
        &self,
        window: &CalendarWindow,
        progress: &BillProgress,
        on_batch: &mut impl FnMut(StreamBatch) -> Result<StreamBatch, BillError>,
    ) -> Result<(), BillError>;
    /// Lists the most recent root sessions, newest first, up to `limit`.
    ///
    /// A root session is a session without a parent.  This query powers the
    /// no-argument interactive picker, so it never counts rows into a
    /// [`BillProgress`] handle: it is separate from the
    /// loaded == parsed invariant of the billing pipeline.
    fn recent_roots(&self, limit: usize) -> Result<Vec<RecentRoot>, BillError>;
}

/// One day's raw window rows: the messages and their tool parts.
///
/// The bundle flows through the streaming pipeline as one unit, so a day's
/// tool spans parse before its messages; the drained bundle comes back for
/// reuse.
pub(crate) struct StreamBatch {
    /// The day's raw messages, in window order.
    pub(crate) messages: Vec<RawMessage>,
    /// The raw tool part rows of the day's messages.
    pub(crate) parts: Vec<RawPart>,
}

/// Raw window rows without the message payloads.
///
/// The message and part rows are streamed separately, per day; the header,
/// the session count, and the session tree are small enough to stay in
/// memory for the whole window.
pub(crate) struct RawWindowMeta {
    pub(crate) header: BillHeader,
    pub(crate) session_count: usize,
    pub(crate) sessions: Vec<RawSession>,
}

/// One root session's identity for the interactive picker.
///
/// A root session is a session without a parent; the picker offers the most
/// recent roots by creation time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecentRoot {
    /// Root session ID.
    pub id: String,
    /// Root session title.
    pub title: String,
    /// Root session creation time (Unix milliseconds), when the store knows it.
    pub created_ms: Option<i64>,
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

/// One session row: identity plus its agent component.
pub(crate) struct RawSession {
    pub(crate) id: String,
    pub(crate) agent: Option<String>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_full_respects_rows_bytes_and_day() {
        assert!(batch_full(MESSAGE_BATCH_LIMIT, 0, false));
        assert!(batch_full(0, MESSAGE_BATCH_BYTES, false));
        assert!(batch_full(0, 0, true));
        assert!(!batch_full(0, 0, false));
        assert!(!batch_full(
            MESSAGE_BATCH_LIMIT - 1,
            MESSAGE_BATCH_BYTES - 1,
            false
        ));
    }
}
