//! Legacy JSON-file session store.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use log::debug;

use crate::error::BillError;
use crate::parse::{message_time, parse_json, read_error, read_json, required_string};
use crate::report::BillHeader;
use crate::store::{
    CalendarMonth, RawLoad, RawMessage, RawPart, RawSession, SessionStore, unique_match,
};

/// Sessions stored in OpenCode's legacy JSON directory layout.
pub(crate) struct LegacyStore {
    storage: PathBuf,
}

impl LegacyStore {
    pub(crate) fn new(storage: PathBuf) -> Self {
        Self { storage }
    }
}

impl SessionStore for LegacyStore {
    fn load(&self, reference: &str) -> Result<RawLoad, BillError> {
        let session_root = self.storage.join("session");
        let matches = legacy_matches(&session_root, reference)?;
        let session_path = unique_match(reference, matches)?;
        let session_json = read_json(&session_path)?;
        let id = required_string(&session_json, "id", &session_path.display().to_string())?;
        let title = required_string(&session_json, "title", &session_path.display().to_string())?;
        let tree = legacy_session_tree(&session_root, &id)?;
        let session_count = tree.len();
        let mut sessions = Vec::new();
        let mut messages = Vec::new();
        let mut parts = Vec::new();

        for (session_id, parent) in &tree {
            sessions.push(RawSession {
                id: session_id.clone(),
                agent: None,
                parent: parent.clone(),
            });
            let (loaded, loaded_parts) = legacy_messages(&self.storage, session_id)?;

            messages.extend(loaded);
            parts.extend(loaded_parts);
        }

        debug!(
            "loaded {} raw messages from {} legacy JSON sessions",
            messages.len(),
            session_count
        );
        Ok(RawLoad {
            header: BillHeader::Session { id, title },
            session_count,
            sessions,
            messages,
            parts,
        })
    }

    fn load_month(&self, month: &CalendarMonth) -> Result<RawLoad, BillError> {
        let message_root = self.storage.join("message");
        let mut sessions = Vec::new();
        let mut messages = Vec::new();
        let mut parts = Vec::new();

        if message_root.is_dir() {
            let session_rows = session_directories(&message_root)?
                .into_iter()
                .filter_map(|session_path| {
                    let session_id = session_path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or_default();

                    month_session_rows(&self.storage, &session_path, session_id, month).transpose()
                })
                .collect::<Result<Vec<_>, BillError>>()?;

            for (session, kept_messages, kept_parts) in session_rows {
                sessions.push(session);
                messages.extend(kept_messages);
                parts.extend(kept_parts);
            }
        }

        debug!(
            "loaded {} raw messages from {} legacy JSON sessions in {}",
            messages.len(),
            sessions.len(),
            month.label
        );
        Ok(RawLoad {
            header: BillHeader::Month {
                label: month.label.clone(),
            },
            session_count: sessions.len(),
            sessions,
            messages,
            parts,
        })
    }
}

/// Rows of one kept legacy session: the session itself, its kept messages
/// and their raw part rows.
type KeptSessionRows = (RawSession, Vec<RawMessage>, Vec<RawPart>);

/// The rows of one legacy session that fall inside the month window.
///
/// `None` when no message of the session is kept; the session itself is only
/// billed when it contributed at least one message.
fn month_session_rows(
    storage: &Path,
    session_path: &Path,
    session_id: &str,
    month: &CalendarMonth,
) -> Result<Option<KeptSessionRows>, BillError> {
    let rows = read_directory(session_path)?
        .map(|entry| entry.map_err(|source| read_error(session_path, source)))
        .collect::<Result<Vec<_>, BillError>>()?
        .into_iter()
        .map(|entry| entry.path())
        .filter_map(|path| month_message_row(storage, &path, session_id, month).transpose())
        .collect::<Result<Vec<_>, BillError>>()?;

    if rows.is_empty() {
        return Ok(None);
    }

    let session = RawSession {
        id: session_id.to_owned(),
        agent: None,
        parent: None,
    };
    let mut messages = Vec::with_capacity(rows.len());
    let mut parts = Vec::new();

    for (message, message_parts) in rows {
        messages.push(message);
        parts.extend(message_parts);
    }

    Ok(Some((session, messages, parts)))
}

/// One kept legacy JSON message of a session, plus its raw part rows.
///
/// `None` for non-JSON files and messages outside the month window.
fn month_message_row(
    storage: &Path,
    path: &Path,
    session_id: &str,
    month: &CalendarMonth,
) -> Result<Option<(RawMessage, Vec<RawPart>)>, BillError> {
    if path.extension().and_then(|value| value.to_str()) != Some("json") {
        return Ok(None);
    }

    // The window filter needs the created time, so the payload is parsed
    // here once; `parse_system` re-parses the raw string of every kept
    // message.
    let json = fs::read_to_string(path).map_err(|source| read_error(path, source))?;
    let value = parse_json(&json, &path.display().to_string())?;

    if message_time(&value)
        .0
        .is_none_or(|created| created < month.start_ms || created >= month.end_ms)
    {
        return Ok(None);
    }

    let message_id = path
        .file_stem()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    let message = RawMessage {
        id: message_id.to_owned(),
        session_id: session_id.to_owned(),
        location: path.display().to_string(),
        json,
    };
    let parts = legacy_part_rows(storage, message_id)?;

    Ok(Some((message, parts)))
}

/// Session JSON paths whose file stem starts with the reference.
fn legacy_matches(root: &Path, reference: &str) -> Result<Vec<PathBuf>, BillError> {
    let mut matches = legacy_session_paths(root)?;

    matches.retain(|path| {
        path.file_stem()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(reference))
    });
    Ok(matches)
}

/// Every session JSON path under the storage root, sorted.
fn legacy_session_paths(root: &Path) -> Result<Vec<PathBuf>, BillError> {
    let mut paths: Vec<PathBuf> = session_directories(root)?
        .into_iter()
        .map(|dir| json_file_paths(&dir))
        .collect::<Result<Vec<_>, BillError>>()?
        .into_iter()
        .flatten()
        .collect();

    paths.sort();
    Ok(paths)
}

/// The sub-directories of one storage root, in directory order.
fn session_directories(root: &Path) -> Result<Vec<PathBuf>, BillError> {
    Ok(read_directory(root)?
        .map(|entry| entry.map_err(|source| read_error(root, source)))
        .collect::<Result<Vec<_>, BillError>>()?
        .into_iter()
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect())
}

/// JSON file paths directly inside one directory, in directory order.
fn json_file_paths(dir: &Path) -> Result<Vec<PathBuf>, BillError> {
    Ok(read_directory(dir)?
        .map(|entry| entry.map_err(|source| read_error(dir, source)))
        .collect::<Result<Vec<_>, BillError>>()?
        .into_iter()
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("json"))
        .collect())
}

/// The session tree of one root: every descendant id mapped to its parent id.
///
/// String-keyed so callers can intern and process sessions in deterministic
/// string order.  Legacy session JSON carries no agent component.
fn legacy_session_tree(
    root: &Path,
    root_id: &str,
) -> Result<BTreeMap<String, Option<String>>, BillError> {
    let mut parents = BTreeMap::new();

    for path in legacy_session_paths(root)? {
        let value = read_json(&path)?;
        let location = path.display().to_string();
        let id = required_string(&value, "id", &location)?;
        let parent = value
            .get("parentID")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);

        parents.insert(id, parent);
    }

    let root_parent = parents.get(root_id).and_then(Clone::clone);
    let mut descendants = BTreeMap::from([(root_id.to_owned(), root_parent)]);
    let mut changed = true;

    while changed {
        changed = false;

        for (id, parent) in &parents {
            if parent
                .as_ref()
                .is_some_and(|value| descendants.contains_key(value))
                && descendants.insert(id.clone(), parent.clone()).is_none()
            {
                changed = true;
            }
        }
    }

    Ok(descendants)
}

/// Every raw message row of one legacy session, plus its raw part rows.
fn legacy_messages(
    storage: &Path,
    session_id: &str,
) -> Result<(Vec<RawMessage>, Vec<RawPart>), BillError> {
    let root = storage.join("message").join(session_id);

    if !root.is_dir() {
        return Ok((Vec::new(), Vec::new()));
    }

    let rows = json_file_paths(&root)?
        .into_iter()
        .map(|path| {
            let message_id = path
                .file_stem()
                .and_then(|name| name.to_str())
                .unwrap_or_default();
            let json = fs::read_to_string(&path).map_err(|source| read_error(&path, source))?;

            Ok((
                RawMessage {
                    id: message_id.to_owned(),
                    session_id: session_id.to_owned(),
                    location: path.display().to_string(),
                    json,
                },
                legacy_part_rows(storage, message_id)?,
            ))
        })
        .collect::<Result<Vec<_>, BillError>>()?;

    let mut messages = Vec::with_capacity(rows.len());
    let mut parts = Vec::new();

    for (message, message_parts) in rows {
        messages.push(message);
        parts.extend(message_parts);
    }

    Ok((messages, parts))
}

/// Raw tool part rows stored alongside one legacy JSON message.
fn legacy_part_rows(storage: &Path, message_id: &str) -> Result<Vec<RawPart>, BillError> {
    let root = storage.join("part").join(message_id);

    if !root.is_dir() {
        return Ok(Vec::new());
    }

    json_file_paths(&root)?
        .into_iter()
        .map(|path| {
            let json = fs::read_to_string(&path).map_err(|source| read_error(&path, source))?;

            Ok(RawPart {
                message_id: message_id.to_owned(),
                location: path.display().to_string(),
                json,
            })
        })
        .collect()
}

/// Opens a directory, mapping failures onto the bill error type.
fn read_directory(path: &Path) -> Result<fs::ReadDir, BillError> {
    fs::read_dir(path).map_err(|source| crate::parse::read_error(path, source))
}
