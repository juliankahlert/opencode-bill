//! Legacy JSON-file session store.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use log::debug;

use crate::error::BillError;
use crate::parse::{message_time, parse_assistant, read_json, required_string, tool_span};
use crate::report::BillHeader;
use crate::store::{BillInput, CalendarMonth, MessageLoad, SessionStore, unique_match};
use crate::timing::{earliest, latest};

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
    fn load(&self, reference: &str) -> Result<BillInput, BillError> {
        let session_root = self.storage.join("session");
        let matches = legacy_matches(&session_root, reference)?;
        let session_path = unique_match(reference, matches)?;
        let session_json = read_json(&session_path)?;
        let id = required_string(&session_json, "id", &session_path.display().to_string())?;
        let title = required_string(&session_json, "title", &session_path.display().to_string())?;
        let session_ids = legacy_descendant_ids(&session_root, &id)?;
        let session_agents = BTreeMap::new();
        let mut messages = Vec::new();
        let mut wall_start = None;
        let mut wall_end = None;

        for session_id in &session_ids {
            let MessageLoad {
                messages: loaded,
                wall_start: start,
                wall_end: end,
            } = legacy_messages(&self.storage, session_id)?;

            messages.extend(loaded);
            wall_start = earliest(wall_start, start);
            wall_end = latest(wall_end, end);
        }

        debug!(
            "loaded {} assistant messages from {} legacy JSON sessions",
            messages.len(),
            session_ids.len()
        );
        Ok(BillInput {
            header: BillHeader::Session { id, title },
            session_count: session_ids.len(),
            session_agents,
            messages,
            wall_start,
            wall_end,
        })
    }

    fn load_month(&self, month: &CalendarMonth) -> Result<BillInput, BillError> {
        let message_root = self.storage.join("message");
        let mut messages = Vec::new();
        let mut sessions = BTreeSet::new();
        let mut wall_start = None;
        let mut wall_end = None;

        if message_root.is_dir() {
            for entry in read_directory(&message_root)? {
                let entry =
                    entry.map_err(|source| crate::parse::read_error(&message_root, source))?;
                let session_path = entry.path();

                if !session_path.is_dir() {
                    continue;
                }

                let session_id = session_path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or_default();
                let mut found = false;

                for message in read_directory(&session_path)? {
                    let message = message
                        .map_err(|source| crate::parse::read_error(&session_path, source))?;
                    let path = message.path();

                    if path.extension().and_then(|value| value.to_str()) != Some("json") {
                        continue;
                    }

                    let value = read_json(&path)?;
                    let (created, completed) = message_time(&value);
                    let created = match created {
                        Some(created) if created >= month.start_ms && created < month.end_ms => {
                            created
                        }
                        _ => continue,
                    };

                    found = true;
                    wall_start = earliest(wall_start, Some(created));
                    wall_end = latest(wall_end, completed.or(Some(created)));

                    let message_id = path
                        .file_stem()
                        .and_then(|name| name.to_str())
                        .unwrap_or_default();
                    let tools = legacy_tool_spans(&self.storage, message_id)?;

                    if let Some(message) = parse_assistant(
                        &value,
                        &path.display().to_string(),
                        session_id,
                        None,
                        &tools,
                    )? {
                        messages.push(message);
                    }
                }

                if found {
                    sessions.insert(session_id.to_owned());
                }
            }
        }

        debug!(
            "loaded {} assistant messages from {} legacy JSON sessions in {}",
            messages.len(),
            sessions.len(),
            month.label
        );
        Ok(BillInput {
            header: BillHeader::Month {
                label: month.label.clone(),
            },
            session_count: sessions.len(),
            session_agents: BTreeMap::new(),
            messages,
            wall_start,
            wall_end,
        })
    }
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
    let mut paths = Vec::new();

    for project in read_directory(root)? {
        let project = project.map_err(|source| crate::parse::read_error(root, source))?;

        if !project.path().is_dir() {
            continue;
        }

        for session in read_directory(&project.path())? {
            let session =
                session.map_err(|source| crate::parse::read_error(&project.path(), source))?;
            let path = session.path();

            if path.extension().and_then(|value| value.to_str()) == Some("json") {
                paths.push(path);
            }
        }
    }

    paths.sort();
    Ok(paths)
}

/// IDs of the root session and every session that descends from it.
fn legacy_descendant_ids(root: &Path, root_id: &str) -> Result<BTreeSet<String>, BillError> {
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

    let mut descendants = BTreeSet::from([root_id.to_owned()]);
    let mut changed = true;

    while changed {
        changed = false;

        for (id, parent) in &parents {
            if parent
                .as_ref()
                .is_some_and(|value| descendants.contains(value))
                && descendants.insert(id.clone())
            {
                changed = true;
            }
        }
    }

    Ok(descendants)
}

/// Every assistant message of one legacy session, plus its wall bounds.
fn legacy_messages(storage: &Path, session_id: &str) -> Result<MessageLoad, BillError> {
    let root = storage.join("message").join(session_id);
    let mut messages = Vec::new();
    let mut wall_start = None;
    let mut wall_end = None;

    if !root.is_dir() {
        return Ok(MessageLoad {
            messages,
            wall_start,
            wall_end,
        });
    }

    for entry in read_directory(&root)? {
        let entry = entry.map_err(|source| crate::parse::read_error(&root, source))?;
        let path = entry.path();

        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }

        let value = read_json(&path)?;
        let (created, completed) = message_time(&value);

        wall_start = earliest(wall_start, created);
        wall_end = latest(wall_end, completed.or(created));

        let message_id = path
            .file_stem()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        let tools = legacy_tool_spans(storage, message_id)?;

        if let Some(message) = parse_assistant(
            &value,
            &path.display().to_string(),
            session_id,
            None,
            &tools,
        )? {
            messages.push(message);
        }
    }

    Ok(MessageLoad {
        messages,
        wall_start,
        wall_end,
    })
}

/// Collects the tool-call spans stored alongside one legacy JSON message.
fn legacy_tool_spans(storage: &Path, message_id: &str) -> Result<Vec<(u64, u64)>, BillError> {
    let root = storage.join("part").join(message_id);
    let mut spans = Vec::new();

    if !root.is_dir() {
        return Ok(spans);
    }

    for entry in read_directory(&root)? {
        let entry = entry.map_err(|source| crate::parse::read_error(&root, source))?;
        let path = entry.path();

        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }

        if let Some(span) = tool_span(&read_json(&path)?) {
            spans.push(span);
        }
    }

    Ok(spans)
}

/// Opens a directory, mapping failures onto the bill error type.
fn read_directory(path: &Path) -> Result<fs::ReadDir, BillError> {
    fs::read_dir(path).map_err(|source| crate::parse::read_error(path, source))
}
