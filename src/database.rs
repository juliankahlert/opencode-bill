//! SQLite-backed session store.

use std::path::{Path, PathBuf};

use log::{debug, warn};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};

use crate::data::BillProgress;
use crate::error::BillError;
use crate::report::BillHeader;
use crate::store::{
    CalendarMonth, RawLoad, RawMessage, RawPart, RawSession, SessionStore, unique_match,
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
        let sessions = database_session_rows(&connection, &self.path, &id, progress)?;
        let session_count = sessions.len();
        let parts = database_tool_parts(&connection, &self.path, &id, progress)?;
        let messages = database_messages(&connection, &self.path, &id, progress)?;

        debug!(
            "loaded {} raw messages from {session_count} SQLite sessions",
            messages.len()
        );
        Ok(RawLoad {
            header: BillHeader::Session { id, title },
            session_count,
            sessions,
            messages,
            parts,
        })
    }

    fn load_month(
        &self,
        month: &CalendarMonth,
        progress: &BillProgress,
    ) -> Result<RawLoad, BillError> {
        let connection = open_database(&self.path)?;
        let sessions = database_month_session_rows(&connection, &self.path, month, progress)?;
        let parts = database_month_tool_parts(&connection, &self.path, month, progress)?;
        let messages = database_month_messages(&connection, &self.path, month, progress)?;
        let session_count = database_month_session_count(&connection, &self.path, month)?;

        debug!(
            "loaded {} raw messages from {session_count} SQLite sessions in {}",
            messages.len(),
            month.label
        );
        Ok(RawLoad {
            header: BillHeader::Month {
                label: month.label.clone(),
            },
            session_count,
            sessions,
            messages,
            parts,
        })
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

/// Raw agent-component rows of every session with a message in the month
/// window, ordered by session id.  Sessions without an agent component are
/// not part of the month's agent tree.
fn database_month_session_rows(
    connection: &Connection,
    path: &Path,
    month: &CalendarMonth,
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
        .query_map(params![month.start_ms, month.end_ms], |row| {
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

/// Every raw message row recorded in one calendar month, in wall-clock order.
fn database_month_messages(
    connection: &Connection,
    path: &Path,
    month: &CalendarMonth,
    progress: &BillProgress,
) -> Result<Vec<RawMessage>, BillError> {
    let mut statement = connection
        .prepare(
            r#"SELECT message.id, message.session_id,
                      json_extract(message.data, '$.role','$.providerID','$.modelID','$.cost',
                                   '$.time.created','$.time.completed','$.tokens.input',
                                   '$.tokens.output','$.tokens.reasoning','$.tokens.cache.read',
                                   '$.tokens.cache.write','$.agent','$.mode') FROM message
               WHERE message.time_created >= ?1 AND message.time_created < ?2
               ORDER BY message.time_created, message.id"#,
        )
        .map_err(|source| database_error(path, source))?;
    let rows = statement
        .query_map(params![month.start_ms, month.end_ms], |row| {
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

/// Number of distinct sessions with at least one message in the month window.
fn database_month_session_count(
    connection: &Connection,
    path: &Path,
    month: &CalendarMonth,
) -> Result<usize, BillError> {
    connection
        .query_row(
            "SELECT COUNT(DISTINCT session_id) FROM message \
             WHERE time_created >= ?1 AND time_created < ?2",
            params![month.start_ms, month.end_ms],
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

/// Raw tool part rows of every message recorded in one calendar month.
fn database_month_tool_parts(
    connection: &Connection,
    path: &Path,
    month: &CalendarMonth,
    progress: &BillProgress,
) -> Result<Vec<RawPart>, BillError> {
    if !database_has_table(connection, path, "part")? {
        warn!("no 'part' table: API time will still include tool calls");
        return Ok(Vec::new());
    }

    let mut statement = connection
        .prepare(
            r#"SELECT part.message_id, json_extract(part.data,
                      '$.type','$.state.time.start','$.state.time.end') FROM part
               JOIN message ON part.message_id = message.id
               WHERE message.time_created >= ?1 AND message.time_created < ?2"#,
        )
        .map_err(|source| database_error(path, source))?;
    let rows = statement
        .query_map(params![month.start_ms, month.end_ms], |row| {
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
