//! Session model and the store abstraction behind it.

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::data::UsageRecord;
use crate::error::BillError;

/// A session reference must contain at least this many characters.
const MINIMUM_REFERENCE_LENGTH: usize = 6;

/// One OpenCode session tree with every assistant message it contains.
pub(crate) struct Session {
    /// Root session ID.
    pub(crate) id: String,
    /// Root session title.
    pub(crate) title: String,
    /// Root plus descendant session count.
    pub(crate) session_count: usize,
    /// Agent class declared by each descendant session.
    pub(crate) session_agents: BTreeMap<String, String>,
    /// Every assistant call in the session tree.
    pub(crate) messages: Vec<UsageRecord>,
    /// Earliest message timestamp (Unix millis).
    pub(crate) wall_start: Option<u64>,
    /// Latest message timestamp (Unix millis).
    pub(crate) wall_end: Option<u64>,
}

/// Wherever session data lives (SQLite or legacy JSON), it loads the same way.
pub(crate) trait SessionStore {
    /// Resolves a session reference and loads the whole session tree.
    fn load(&self, reference: &str) -> Result<Session, BillError>;
}

/// Assistant messages plus the wall-clock bounds of every message read.
pub(crate) struct MessageLoad {
    pub(crate) messages: Vec<UsageRecord>,
    pub(crate) wall_start: Option<u64>,
    pub(crate) wall_end: Option<u64>,
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
