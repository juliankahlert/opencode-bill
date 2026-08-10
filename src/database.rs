//! SQLite-backed session store.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use log::{debug, warn};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params, params_from_iter};

use crate::data::BillProgress;
use crate::error::BillError;
use crate::report::BillHeader;
use crate::store::{
    CalendarWindow, DATA_SLICE, DAY_MILLIS, MESSAGE_BATCH_LIMIT, RawLoad, RawMessage, RawPart,
    RawSession, RawWindowMeta, RecentRoot, SessionStore, StreamBatch, batch_full, unique_match,
};

/// Sessions stored in OpenCode's SQLite database.
pub(crate) struct SqliteStore {
    path: PathBuf,
}

impl SqliteStore {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl SessionStore for SqliteStore {
    fn load(&self, reference: &str, progress: &BillProgress) -> Result<RawLoad, BillError> {
        let connection = open_database(&self.path)?;
        let matches = database_matches(&connection, &self.path, reference)?;
        let id = unique_match(reference, matches)?;
        let title = database_title(&connection, &self.path, &id)?;
        let created_ms = database_created_ms(&connection, &self.path, &id)?;
        let sessions = database_session_rows(&connection, &self.path, &id, progress)?;
        let session_count = sessions.len();
        let parts = database_tool_parts(&connection, &self.path, &id, progress)?;
        let messages = database_messages(&connection, &self.path, &id, progress)?;

        debug!(
            "loaded {} raw messages from {session_count} SQLite sessions",
            messages.len()
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
        let connection = open_database(&self.path)?;
        let sessions = database_window_session_rows(&connection, &self.path, window, progress)?;
        let session_count = database_window_session_count(&connection, &self.path, window)?;

        debug!("loaded {} raw sessions in {}", sessions.len(), window.label);
        Ok(RawWindowMeta {
            header: window.header.clone(),
            session_count,
            sessions,
        })
    }

    fn stream_window_messages(
        &self,
        window: &CalendarWindow,
        progress: &BillProgress,
        on_batch: &mut impl FnMut(StreamBatch) -> Result<StreamBatch, BillError>,
    ) -> Result<(), BillError> {
        // Two-phase streaming over keyset chunks: the window's (id, time)
        // keys are walked in ordered chunks of MESSAGE_BATCH_LIMIT rows —
        // a leaf-only scan that never touches the payload pages — and the
        // payloads (message data and tool parts) are fetched by primary key
        // in slices of DATA_SLICE ids.  Every row is read exactly once, and
        // no single query materializes more than the pipeline consumes at
        // once.  The whole stream runs in one read snapshot, so the keys
        // and their payloads always agree even while OpenCode writes; the
        // long-lived read pins WAL frames while OpenCode keeps writing, so
        // a year bill against a busy live database can grow the WAL.
        let connection = open_database(&self.path)?;
        let transaction = connection
            .unchecked_transaction()
            .map_err(|source| database_error(&self.path, source))?;
        let location = self.path.display().to_string();
        let has_parts = if database_has_table(&transaction, &self.path, "part")? {
            true
        } else {
            warn!("no 'part' table: API time will still include tool calls");
            false
        };

        let mut batch = Vec::new();
        let mut parts = Vec::new();
        let mut batch_bytes = 0usize;
        let mut batch_day: Option<usize> = None;
        let mut last_created = 0i64;
        let mut last_id = String::new();
        let mut streamed_messages = 0usize;
        let mut streamed_parts = 0usize;
        let mut streamed_bytes = 0usize;

        let mut flush = |messages: &mut Vec<RawMessage>,
                         parts: &mut Vec<RawPart>,
                         bytes: &mut usize,
                         day: &mut Option<usize>|
         -> Result<(), BillError> {
            let returned = on_batch(StreamBatch {
                messages: std::mem::take(messages),
                parts: std::mem::take(parts),
            })?;

            *messages = returned.messages;
            *parts = returned.parts;
            *bytes = 0;
            *day = None;

            Ok(())
        };

        let mut chunk =
            database_window_message_keys(&transaction, &self.path, window, None, progress)?;

        while !chunk.is_empty() {
            let chunk_len = chunk.len();
            let mut index = 0usize;

            while index < chunk.len() {
                let end = (index + DATA_SLICE).min(chunk.len());
                let slice = &chunk[index..end];

                let data = database_batch_message_fields(&transaction, &self.path, slice)?;
                let mut by_id = data.into_iter().collect::<HashMap<_, _>>();

                for (created, message_id, session_id) in slice {
                    let next_day = ((*created as u64 - window.start_ms) / DAY_MILLIS) as usize;

                    if batch_day.is_none() {
                        batch_day = Some(next_day);
                    } else if batch_day.is_some_and(|day| day != next_day) {
                        flush(&mut batch, &mut parts, &mut batch_bytes, &mut batch_day)?;
                        batch_day = Some(next_day);
                    }

                    last_created = *created;
                    last_id = message_id.clone();
                    let json = by_id
                        .remove(message_id)
                        .ok_or_else(|| BillError::Database {
                            path: self.path.clone(),
                            source: rusqlite::Error::QueryReturnedNoRows,
                        })?;

                    // A giant payload (a tool result embedded in the
                    // message) can dwarf the byte budget on its own: flush
                    // before it joins, so no batch ever stacks more than one
                    // such row at a time.
                    if !batch.is_empty()
                        && batch_full(batch.len(), batch_bytes.saturating_add(json.len()), false)
                    {
                        flush(&mut batch, &mut parts, &mut batch_bytes, &mut batch_day)?;
                        batch_day = Some(next_day);
                    }

                    batch_bytes += json.len();
                    streamed_bytes += json.len();
                    streamed_messages += 1;
                    batch.push(RawMessage {
                        id: message_id.clone(),
                        session_id: session_id.clone(),
                        location: location.clone(),
                        json,
                    });
                }

                if has_parts {
                    let ids = slice
                        .iter()
                        .map(|(_, id, _)| id.clone())
                        .collect::<Vec<_>>();
                    let parts_start = parts.len();

                    database_batch_parts(
                        &transaction,
                        &self.path,
                        &ids,
                        &mut parts,
                        &location,
                        progress,
                    )?;

                    // Only the newly appended parts count toward this
                    // batch's budget and totals.
                    let added = parts[parts_start..]
                        .iter()
                        .map(|part| part.json.len())
                        .sum::<usize>();

                    batch_bytes += added;
                    streamed_parts += parts.len() - parts_start;
                    streamed_bytes += added;
                }

                if batch_full(batch.len(), batch_bytes, false) {
                    flush(&mut batch, &mut parts, &mut batch_bytes, &mut batch_day)?;
                }

                index = end;
            }

            if chunk_len < MESSAGE_BATCH_LIMIT {
                break;
            }

            chunk = database_window_message_keys(
                &transaction,
                &self.path,
                window,
                Some((last_created, last_id.clone())),
                progress,
            )?;
        }

        if !batch.is_empty() {
            flush(&mut batch, &mut parts, &mut batch_bytes, &mut batch_day)?;
        }

        debug!(
            "streamed {streamed_messages} messages and {streamed_parts} parts ({streamed_bytes} payload bytes) in {}",
            window.label
        );
        Ok(())
    }

    fn recent_roots(&self, limit: usize) -> Result<Vec<RecentRoot>, BillError> {
        let connection = open_database(&self.path)?;
        let has_created = database_has_column(&connection, &self.path, "session", "time_created")?;
        let (created_column, order) = if has_created {
            (", time_created", "time_created DESC, id")
        } else {
            // Test fixture schemas predate the column; order by id and leave
            // the creation time unknown instead of failing the query.
            (", NULL", "id")
        };
        let mut statement = connection
            .prepare(&format!(
                "SELECT id, title{created_column} FROM session \
                 WHERE parent_id IS NULL ORDER BY {order} LIMIT ?1"
            ))
            .map_err(|source| database_error(&self.path, source))?;
        let rows = statement
            .query_map(params![limit as i64], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                ))
            })
            .map_err(|source| database_error(&self.path, source))?;
        let mut roots = Vec::new();

        for row in rows {
            let (id, title, created_ms) =
                row.map_err(|source| database_error(&self.path, source))?;

            roots.push(RecentRoot {
                id,
                title,
                created_ms,
            });
        }

        Ok(roots)
    }
}

/// Opens the database read-only and without mutex coordination.
fn open_database(path: &Path) -> Result<Connection, BillError> {
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;

    Connection::open_with_flags(path, flags).map_err(|source| BillError::Database {
        path: path.to_path_buf(),
        source,
    })
}

/// Session IDs whose literal prefix matches the reference, ordered by ID.
///
/// Uses `substr` comparison instead of `LIKE` so the `_` and `%` characters
/// allowed in references cannot act as SQL wildcards.
fn database_matches(
    connection: &Connection,
    path: &Path,
    reference: &str,
) -> Result<Vec<String>, BillError> {
    let mut statement = connection
        .prepare("SELECT id FROM session WHERE substr(id, 1, length(?1)) = ?1 ORDER BY id LIMIT 11")
        .map_err(|source| database_error(path, source))?;
    let rows = statement
        .query_map(params![reference], |row| row.get::<_, String>(0))
        .map_err(|source| database_error(path, source))?;
    let mut matches = Vec::new();

    for row in rows {
        matches.push(row.map_err(|source| database_error(path, source))?);
    }

    Ok(matches)
}

/// Title of one session row.
fn database_title(connection: &Connection, path: &Path, id: &str) -> Result<String, BillError> {
    connection
        .query_row(
            "SELECT title FROM session WHERE id = ?1",
            params![id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|source| database_error(path, source))?
        .ok_or_else(|| BillError::SessionNotFound(id.to_owned()))
}

/// Creation time of one session row, when its table stores it.
///
/// Schemas that predate the `time_created` column yield `None` instead of a
/// broken query.
fn database_created_ms(
    connection: &Connection,
    path: &Path,
    id: &str,
) -> Result<Option<i64>, BillError> {
    if !database_has_column(connection, path, "session", "time_created")? {
        return Ok(None);
    }

    connection
        .query_row(
            "SELECT time_created FROM session WHERE id = ?1",
            params![id],
            |row| row.get::<_, Option<i64>>(0),
        )
        .optional()
        .map_err(|source| database_error(path, source))
        .map(|created| created.flatten())
}

/// Raw session rows of one root: every descendant with its agent component,
/// ordered by session id.
fn database_session_rows(
    connection: &Connection,
    path: &Path,
    id: &str,
    progress: &BillProgress,
) -> Result<Vec<RawSession>, BillError> {
    let has_agent = database_has_column(connection, path, "session", "agent")?;
    let agent_column = if has_agent { ", session.agent" } else { "" };
    let agent_index = if has_agent { Some(1usize) } else { None };
    let mut statement = connection
        .prepare(&format!(
            r#"WITH RECURSIVE descendants(id) AS (
                   SELECT id FROM session WHERE id = ?1
                   UNION
                   SELECT session.id FROM session
                   JOIN descendants ON session.parent_id = descendants.id
               )
               SELECT session.id{agent_column} FROM session
               JOIN descendants ON session.id = descendants.id
               ORDER BY session.id"#
        ))
        .map_err(|source| database_error(path, source))?;
    let rows = statement
        .query_map(params![id], |row| {
            let session_id = row.get::<_, String>(0)?;
            let agent = match agent_index {
                Some(index) => row.get::<_, Option<String>>(index)?,
                None => None,
            };

            Ok((session_id, agent))
        })
        .map_err(|source| database_error(path, source))?;
    let mut sessions = Vec::new();

    for row in rows {
        let (session_id, agent) = row.map_err(|source| database_error(path, source))?;

        progress.loaded_row();
        sessions.push(RawSession {
            id: session_id,
            agent,
        });
    }

    Ok(sessions)
}

/// Raw agent-component rows of every session with a message in the window,
/// ordered by session id.  Sessions without an agent component are not part
/// of the window's agent tree.
fn database_window_session_rows(
    connection: &Connection,
    path: &Path,
    window: &CalendarWindow,
    progress: &BillProgress,
) -> Result<Vec<RawSession>, BillError> {
    if !database_has_column(connection, path, "session", "agent")? {
        return Ok(Vec::new());
    }

    let mut statement = connection
        .prepare(
            r#"SELECT DISTINCT session.id, session.agent FROM session
               JOIN message ON message.session_id = session.id
               WHERE message.time_created >= ?1 AND message.time_created < ?2
               ORDER BY session.id"#,
        )
        .map_err(|source| database_error(path, source))?;
    let rows = statement
        .query_map(params![window.start_ms, window.end_ms], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })
        .map_err(|source| database_error(path, source))?;
    let sessions = rows
        .map(|row| {
            let (session_id, agent) = row.map_err(|source| database_error(path, source))?;

            Ok((session_id, agent))
        })
        .collect::<Result<Vec<_>, BillError>>()?
        .into_iter()
        .filter_map(|(session_id, agent)| {
            let agent = agent.filter(|agent| !agent.is_empty())?;

            progress.loaded_row();
            Some(RawSession {
                id: session_id,
                agent: Some(agent),
            })
        })
        .collect::<Vec<_>>();

    Ok(sessions)
}

/// Every raw message row in the session tree, in wall-clock order.
fn database_messages(
    connection: &Connection,
    path: &Path,
    id: &str,
    progress: &BillProgress,
) -> Result<Vec<RawMessage>, BillError> {
    let mut statement = connection
        .prepare(
            r#"WITH RECURSIVE descendants(id) AS (
                   SELECT id FROM session WHERE id = ?1
                   UNION
                   SELECT session.id FROM session
                   JOIN descendants ON session.parent_id = descendants.id
               )
               SELECT message.id, message.session_id,
                      json_extract(message.data, '$.role','$.providerID','$.modelID','$.cost',
                                   '$.time.created','$.time.completed','$.tokens.input',
                                   '$.tokens.output','$.tokens.reasoning','$.tokens.cache.read',
                                   '$.tokens.cache.write','$.agent','$.mode') FROM message
               JOIN descendants ON message.session_id = descendants.id
               ORDER BY message.time_created, message.id"#,
        )
        .map_err(|source| database_error(path, source))?;
    let rows = statement
        .query_map(params![id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(|source| database_error(path, source))?;
    let location = path.display().to_string();
    let mut messages = Vec::new();

    for row in rows {
        let (message_id, session_id, json) = row.map_err(|source| database_error(path, source))?;

        progress.loaded_row();
        messages.push(RawMessage {
            id: message_id,
            session_id,
            location: location.clone(),
            json,
        });
    }

    Ok(messages)
}

/// One keyset chunk of the window's message keys, ordered by wall-clock
/// time.
///
/// Every chunk holds at most [`MESSAGE_BATCH_LIMIT`] rows and selects only
/// the leaf columns — the payload pages are never touched here, so the scan
/// stays cheap even without a plain `time_created` index; `after` continues
/// the walk past that (time_created, id) key.  Each key counts as one loaded
/// row; the payload fetch never counts again.
fn database_window_message_keys(
    connection: &Connection,
    path: &Path,
    window: &CalendarWindow,
    after: Option<(i64, String)>,
    progress: &BillProgress,
) -> Result<Vec<(i64, String, String)>, BillError> {
    let mut statement = connection
        .prepare(
            r#"SELECT message.id, message.session_id, message.time_created FROM message
               WHERE message.time_created >= ?1 AND message.time_created < ?2
                 AND (?3 IS NULL OR message.time_created > ?3
                      OR (message.time_created = ?3 AND message.id > ?4))
               ORDER BY message.time_created, message.id
               LIMIT ?5"#,
        )
        .map_err(|source| database_error(path, source))?;
    let (after_created, after_id) = match after {
        Some((created, id)) => (Some(created), Some(id)),
        None => (None, None),
    };
    let rows = statement
        .query_map(
            params![
                window.start_ms,
                window.end_ms,
                after_created,
                after_id,
                MESSAGE_BATCH_LIMIT as i64
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .map_err(|source| database_error(path, source))?;
    let mut chunk = Vec::new();

    for row in rows {
        let (id, session_id, created) = row.map_err(|source| database_error(path, source))?;

        progress.loaded_row();
        chunk.push((created, id, session_id));
    }

    Ok(chunk)
}

/// Extracted fields of one key slice, by primary key.
///
/// The `IN (...)` lookup returns rows in an arbitrary order; the caller
/// reorders them against the slice's keyset order.  Only the internable
/// fields are extracted here — a message whose payload is hundreds of
/// megabytes (a tool result embedded in the data) never crosses into the
/// pipeline.  No row is counted here — the keys scan already counted it.
fn database_batch_message_fields(
    connection: &Connection,
    path: &Path,
    slice: &[(i64, String, String)],
) -> Result<Vec<(String, String)>, BillError> {
    let placeholders = vec!["?"; slice.len()].join(",");
    let mut statement = connection
        .prepare(&format!(
            "SELECT message.id, \
             CASE WHEN LENGTH(message.data) <= 1048576 THEN \
               json_extract(message.data, \
                 '$.role','$.providerID','$.modelID','$.cost',\
                 '$.time.created','$.time.completed','$.tokens.input',\
                 '$.tokens.output','$.tokens.reasoning','$.tokens.cache.read',\
                 '$.tokens.cache.write','$.agent','$.mode') END, \
             CASE WHEN LENGTH(message.data) > 1048576 THEN message.data END \
             FROM message WHERE message.id IN ({placeholders})"
        ))
        .map_err(|source| database_error(path, source))?;
    let rows = statement
        .query_map(
            params_from_iter(slice.iter().map(|(_, id, _)| id.as_str())),
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .map_err(|source| database_error(path, source))?;
    let location = path.display().to_string();
    let mut data = Vec::new();

    for row in rows {
        let (id, fields, raw) = row.map_err(|source| database_error(path, source))?;
        // Small payloads arrive already extracted; oversized ones are
        // stream-parsed here (serde skips the unknown fields, so no tree is
        // built for the giant tool result).
        let fields = match (fields, raw) {
            (Some(fields), _) => fields,
            (None, Some(raw)) => crate::parse::extract_giant_message_fields(&raw, &location)?,
            (None, None) => {
                return Err(BillError::InvalidData {
                    location,
                    detail: "unparseable message payload".to_owned(),
                });
            }
        };

        data.push((id, fields));
    }

    Ok(data)
}

/// Extracted tool part fields of one key slice, into a reusable buffer.
///
/// The `message_id IN (...)` lookup rides the part table's message index
/// (present in the real OpenCode database), so a million-part table is
/// never walked once per calendar day: the parts of a batch are fetched by
/// their messages' ids instead.  Only the span fields are extracted, so a
/// part with a giant tool result never crosses into the pipeline.
fn database_batch_parts(
    connection: &Connection,
    path: &Path,
    ids: &[String],
    parts: &mut Vec<RawPart>,
    location: &str,
    progress: &BillProgress,
) -> Result<(), BillError> {
    let placeholders = vec!["?"; ids.len()].join(",");
    let mut statement = connection
        .prepare(&format!(
            "SELECT part.message_id, json_extract(part.data, \
             '$.type','$.state.time.start','$.state.time.end') \
             FROM part WHERE part.message_id IN ({placeholders})"
        ))
        .map_err(|source| database_error(path, source))?;
    let rows = statement
        .query_map(params_from_iter(ids.iter().map(String::as_str)), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|source| database_error(path, source))?;

    for row in rows {
        let (message_id, json) = row.map_err(|source| database_error(path, source))?;

        progress.loaded_row();
        parts.push(RawPart {
            message_id,
            location: location.to_owned(),
            json,
        });
    }

    Ok(())
}

/// Number of distinct sessions with at least one message in the window.
fn database_window_session_count(
    connection: &Connection,
    path: &Path,
    window: &CalendarWindow,
) -> Result<usize, BillError> {
    connection
        .query_row(
            "SELECT COUNT(DISTINCT session_id) FROM message \
             WHERE time_created >= ?1 AND time_created < ?2",
            params![window.start_ms, window.end_ms],
            |row| row.get(0),
        )
        .map_err(|source| database_error(path, source))
}

/// Raw tool part rows of every message in the session tree.
fn database_tool_parts(
    connection: &Connection,
    path: &Path,
    id: &str,
    progress: &BillProgress,
) -> Result<Vec<RawPart>, BillError> {
    if !database_has_table(connection, path, "part")? {
        warn!("no 'part' table: API time will still include tool calls");
        return Ok(Vec::new());
    }

    let mut statement = connection
        .prepare(
            r#"WITH RECURSIVE descendants(id) AS (
                   SELECT id FROM session WHERE id = ?1
                   UNION
                   SELECT session.id FROM session
                   JOIN descendants ON session.parent_id = descendants.id
               )
               SELECT part.message_id, json_extract(part.data,
                      '$.type','$.state.time.start','$.state.time.end') FROM part
               JOIN descendants ON part.session_id = descendants.id"#,
        )
        .map_err(|source| database_error(path, source))?;
    let rows = statement
        .query_map(params![id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|source| database_error(path, source))?;
    let location = path.display().to_string();
    let mut parts = Vec::new();

    for row in rows {
        let (message_id, json) = row.map_err(|source| database_error(path, source))?;

        progress.loaded_row();
        parts.push(RawPart {
            message_id,
            location: location.clone(),
            json,
        });
    }

    Ok(parts)
}

/// Whether the database has a table with the given name.
fn database_has_table(
    connection: &Connection,
    path: &Path,
    table: &str,
) -> Result<bool, BillError> {
    connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            params![table],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map(|found| found.is_some())
        .map_err(|source| database_error(path, source))
}

/// Whether a table has a column with the given name.
fn database_has_column(
    connection: &Connection,
    path: &Path,
    table: &str,
    column: &str,
) -> Result<bool, BillError> {
    let mut statement = connection
        .prepare(&format!("PRAGMA table_info({table})"))
        .map_err(|source| database_error(path, source))?;
    let names = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|source| database_error(path, source))?;

    for name in names {
        if name.map_err(|source| database_error(path, source))? == column {
            return Ok(true);
        }
    }

    Ok(false)
}

/// Maps a SQLite failure onto the bill error type.
fn database_error(path: &Path, source: rusqlite::Error) -> BillError {
    BillError::Database {
        path: path.to_path_buf(),
        source,
    }
}
