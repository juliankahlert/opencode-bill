//! Legacy JSON-file session store.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use log::debug;

use crate::data::BillProgress;
use crate::error::BillError;
use crate::parse::{
    extract_message_fields, extract_part_fields, message_time, parse_json, read_error, read_json,
    required_string,
};
use crate::report::BillHeader;
use crate::store::{
    CalendarWindow, DAY_MILLIS, MESSAGE_BATCH_BYTES, MESSAGE_BATCH_LIMIT, RawLoad, RawMessage,
    RawPart, RawSession, RawWindowMeta, RecentRoot, SessionStore, StreamBatch, batch_full,
    unique_match,
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
    fn load(&self, reference: &str, progress: &BillProgress) -> Result<RawLoad, BillError> {
        let session_root = self.storage.join("session");
        let matches = legacy_matches(&session_root, reference)?;
        let session_path = unique_match(reference, matches)?;
        let session_json = read_json(&session_path)?;
        let id = required_string(&session_json, "id", &session_path.display().to_string())?;
        let title = required_string(&session_json, "title", &session_path.display().to_string())?;
        let created_ms = session_json
            .get("time")
            .and_then(|time| time.get("created"))
            .and_then(serde_json::Value::as_i64);
        let tree = legacy_session_tree(&session_root, &id)?;
        let session_count = tree.len();
        let (sessions, messages, parts) = tree.iter().try_fold(
            (Vec::new(), Vec::new(), Vec::new()),
            |(mut sessions, mut messages, mut parts), session_id| {
                progress.loaded_row();
                sessions.push(RawSession {
                    id: session_id.clone(),
                    agent: None,
                });
                let (loaded, loaded_parts) = legacy_messages(&self.storage, session_id, progress)?;

                messages.extend(loaded);
                parts.extend(loaded_parts);

                Ok((sessions, messages, parts))
            },
        )?;

        debug!(
            "loaded {} raw messages from {} legacy JSON sessions",
            messages.len(),
            session_count
        );
        Ok(RawLoad {
            header: BillHeader::Session {
                id,
                title,
                created_ms,
            },
            session_count,
            sessions,
            messages,
            parts,
        })
    }

    fn load_window_meta(
        &self,
        window: &CalendarWindow,
        progress: &BillProgress,
    ) -> Result<RawWindowMeta, BillError> {
        let message_root = self.storage.join("message");
        let mut sessions = Vec::new();

        if message_root.is_dir() {
            let meta = session_directories(&message_root)?
                .into_iter()
                .filter_map(|session_path| {
                    let session_id = session_path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or_default();

                    window_session_meta(&session_path, session_id, window, progress).transpose()
                })
                .collect::<Result<Vec<_>, BillError>>()?;

            sessions.extend(meta);
        }

        debug!("loaded {} raw sessions in {}", sessions.len(), window.label);
        Ok(RawWindowMeta {
            header: window.header.clone(),
            session_count: sessions.len(),
            sessions,
        })
    }

    fn stream_window_messages(
        &self,
        window: &CalendarWindow,
        progress: &BillProgress,
        on_batch: &mut impl FnMut(StreamBatch) -> Result<StreamBatch, BillError>,
    ) -> Result<(), BillError> {
        let message_root = self.storage.join("message");

        if !message_root.is_dir() {
            return Ok(());
        }

        // One batch per (session, day): the session visit order of
        // `load_window` is preserved, and each session's kept rows are cut
        // at day boundaries, so no chunk grows with the year.  Messages
        // without a creation time share one leading bucket.  Each message
        // keeps its own part rows, so a split can never separate a message
        // from its parts.
        session_directories(&message_root)?
            .into_iter()
            .try_for_each(|session_path| {
                let session_id = session_path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or_default();
                let chunks = read_directory(&session_path)?
                    .map(|entry| entry.map_err(|source| read_error(&session_path, source)))
                    .collect::<Result<Vec<_>, BillError>>()?
                    .into_iter()
                    .map(|entry| entry.path())
                    .filter_map(|path| window_message(&path, session_id, window).transpose())
                    .collect::<Result<Vec<_>, BillError>>()?
                    .into_iter()
                    .fold(
                        BTreeMap::<Option<usize>, Vec<(RawMessage, Vec<RawPart>)>>::new(),
                        |mut chunks, (message, created)| {
                            // Each kept message counts here, exactly once: the
                            // metadata pass never counts messages, so loaded
                            // stays equal to parsed.
                            progress.loaded_row();
                            let part_rows = legacy_part_rows(&self.storage, &message.id, progress)
                                .unwrap_or_default();
                            chunks
                                .entry(day_bucket(created, window.start_ms))
                                .or_default()
                                .push((message, part_rows));

                            chunks
                        },
                    );

                chunks.into_iter().try_for_each(|(_, rows)| {
                    let mut rows = rows;

                    // No pipeline batch may exceed the message limit or the
                    // payload budget; a session day larger than one batch is
                    // split in row order, message and parts together.
                    let mut bytes = rows.iter().map(row_bytes).sum::<usize>();

                    while batch_full(rows.len(), bytes, false) {
                        let cut = if rows.len() > MESSAGE_BATCH_LIMIT {
                            MESSAGE_BATCH_LIMIT
                        } else {
                            // The first cut that fits the payload budget.
                            rows.iter()
                                .scan(0usize, |budget, row| {
                                    *budget += row_bytes(row);
                                    Some(*budget)
                                })
                                .position(|budget| budget > MESSAGE_BATCH_BYTES)
                                .map_or(1, |index| index + 1)
                        };
                        let remainder = rows.split_off(cut);
                        let (messages, parts) = unzip_rows(rows);

                        let _ = on_batch(StreamBatch { messages, parts })?;
                        rows = remainder;
                        bytes = rows.iter().map(row_bytes).sum::<usize>();
                    }

                    let (messages, parts) = unzip_rows(rows);
                    let _ = on_batch(StreamBatch { messages, parts })?;
                    Ok(())
                })
            })?;

        Ok(())
    }

    fn recent_roots(&self, limit: usize) -> Result<Vec<RecentRoot>, BillError> {
        let session_root = self.storage.join("session");
        let mut roots = legacy_session_paths(&session_root)?
            .into_iter()
            .map(|path| {
                let value = read_json(&path)?;
                let location = path.display().to_string();

                // A root session is a JSON without a parent; skip children.
                if value
                    .get("parentID")
                    .and_then(serde_json::Value::as_str)
                    .is_some()
                {
                    return Ok(None);
                }
                let id = required_string(&value, "id", &location)?;
                let title = required_string(&value, "title", &location)?;
                let created_ms = value
                    .get("time")
                    .and_then(|time| time.get("created"))
                    .and_then(serde_json::Value::as_i64);

                Ok(Some(RecentRoot {
                    id,
                    title,
                    created_ms,
                }))
            })
            .collect::<Result<Vec<_>, BillError>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        roots.sort_by(|left, right| right.created_ms.cmp(&left.created_ms));
        roots.truncate(limit);
        Ok(roots)
    }
}

/// Payload bytes of one kept message and its parts.
fn row_bytes(row: &(RawMessage, Vec<RawPart>)) -> usize {
    row.0
        .json
        .len()
        .saturating_add(row.1.iter().map(|part| part.json.len()).sum::<usize>())
}

/// Splits one session day into the message and part lanes of a batch.
fn unzip_rows(rows: Vec<(RawMessage, Vec<RawPart>)>) -> (Vec<RawMessage>, Vec<RawPart>) {
    let message_capacity = rows.len();

    rows.into_iter().fold(
        (Vec::with_capacity(message_capacity), Vec::new()),
        |(mut messages, mut parts), (message, message_parts)| {
            messages.push(message);
            parts.extend(message_parts);

            (messages, parts)
        },
    )
}

/// Day bucket of one kept message: the day index within the window, or
/// `None` for a message without a creation time.
///
/// The window filter guarantees a timestamped message falls inside the
/// window, so the day difference cannot underflow and stays in range.
fn day_bucket(created: Option<u64>, window_start_ms: u64) -> Option<usize> {
    created.map(|created| ((created - window_start_ms) / DAY_MILLIS) as usize)
}

/// Metadata rows of one kept legacy session.
///
/// `None` when no message of the session is kept; the session itself is only
/// billed when it contributed at least one message.  The message payloads
/// and part rows are not retained here — the streaming message pump re-reads
/// them.
fn window_session_meta(
    session_path: &Path,
    session_id: &str,
    window: &CalendarWindow,
    progress: &BillProgress,
) -> Result<Option<RawSession>, BillError> {
    let rows = read_directory(session_path)?
        .map(|entry| entry.map_err(|source| read_error(session_path, source)))
        .collect::<Result<Vec<_>, BillError>>()?
        .into_iter()
        .map(|entry| entry.path())
        .filter_map(|path| window_message(&path, session_id, window).transpose())
        .collect::<Result<Vec<_>, BillError>>()?;

    if rows.is_empty() {
        return Ok(None);
    }

    progress.loaded_row();
    let session = RawSession {
        id: session_id.to_owned(),
        agent: None,
    };
    Ok(Some(session))
}

/// One kept legacy JSON message of a session, without its part rows.
///
/// `None` for non-JSON files and messages outside the window.  The creation
/// time is returned alongside so the streaming pump can bucket the message
/// into its day without re-parsing.
fn window_message(
    path: &Path,
    session_id: &str,
    window: &CalendarWindow,
) -> Result<Option<(RawMessage, Option<u64>)>, BillError> {
    if path.extension().and_then(|value| value.to_str()) != Some("json") {
        return Ok(None);
    }

    // The window filter needs the created time, so the payload is parsed
    // here once; the streaming pipeline re-parses the raw string of every
    // kept message.
    let json = fs::read_to_string(path).map_err(|source| read_error(path, source))?;
    let value = parse_json(&json, &path.display().to_string())?;
    let created = message_time(&value).0;

    if created.is_none_or(|created| created < window.start_ms || created >= window.end_ms) {
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
        json: extract_message_fields(&value),
    };
    Ok(Some((message, created)))
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

/// The session tree of one root: every descendant session id.
///
/// String-keyed so callers can intern and process sessions in deterministic
/// string order.  Legacy session JSON carries no agent component.
fn legacy_session_tree(root: &Path, root_id: &str) -> Result<BTreeSet<String>, BillError> {
    let parents = legacy_session_paths(root)?.into_iter().try_fold(
        BTreeMap::new(),
        |mut parents, path| {
            let value = read_json(&path)?;
            let location = path.display().to_string();
            let id = required_string(&value, "id", &location)?;
            let parent = value
                .get("parentID")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);

            parents.insert(id, parent);
            Ok(parents)
        },
    )?;

    let mut descendants = BTreeSet::from([root_id.to_owned()]);
    let mut changed = true;

    while changed {
        changed = parents.iter().fold(false, |mut changed, (id, parent)| {
            if parent
                .as_ref()
                .is_some_and(|value| descendants.contains(value))
                && descendants.insert(id.clone())
            {
                changed = true;
            }

            changed
        });
    }

    Ok(descendants)
}

/// Every raw message row of one legacy session, plus its raw part rows.
fn legacy_messages(
    storage: &Path,
    session_id: &str,
    progress: &BillProgress,
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
            let value = parse_json(&json, &path.display().to_string())?;
            let fields = extract_message_fields(&value);

            progress.loaded_row();
            Ok((
                RawMessage {
                    id: message_id.to_owned(),
                    session_id: session_id.to_owned(),
                    location: path.display().to_string(),
                    json: fields,
                },
                legacy_part_rows(storage, message_id, progress)?,
            ))
        })
        .collect::<Result<Vec<_>, BillError>>()?;

    let (messages, parts) = unzip_rows(rows);

    Ok((messages, parts))
}

/// Raw tool part rows stored alongside one legacy JSON message.
fn legacy_part_rows(
    storage: &Path,
    message_id: &str,
    progress: &BillProgress,
) -> Result<Vec<RawPart>, BillError> {
    let root = storage.join("part").join(message_id);

    if !root.is_dir() {
        return Ok(Vec::new());
    }

    json_file_paths(&root)?
        .into_iter()
        .map(|path| {
            let json = fs::read_to_string(&path).map_err(|source| read_error(&path, source))?;
            let value = parse_json(&json, &path.display().to_string())?;
            let fields = extract_part_fields(&value);

            progress.loaded_row();
            Ok(RawPart {
                message_id: message_id.to_owned(),
                location: path.display().to_string(),
                json: fields,
            })
        })
        .collect()
}

/// Opens a directory, mapping failures onto the bill error type.
fn read_directory(path: &Path) -> Result<fs::ReadDir, BillError> {
    fs::read_dir(path).map_err(|source| crate::parse::read_error(path, source))
}
