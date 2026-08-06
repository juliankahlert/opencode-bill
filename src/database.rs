//! SQLite-backed session store.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use log::{debug, warn};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};

use crate::data::ToolSpans;
use crate::error::BillError;
use crate::parse::{message_time, parse_assistant, parse_json, tool_span};
use crate::store::{MessageLoad, Session, SessionStore, unique_match};
use crate::timing::{earliest, latest};

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
    fn load(&self, reference: &str) -> Result<Session, BillError> {
        let connection = open_database(&self.path)?;
        let matches = database_matches(&connection, &self.path, reference)?;
        let id = unique_match(reference, matches)?;
        let title = database_title(&connection, &self.path, &id)?;
        let session_count = database_session_count(&connection, &self.path, &id)?;
        let session_agents = database_session_agents(&connection, &self.path, &id)?;
        let MessageLoad {
            messages,
            wall_start,
            wall_end,
        } = database_messages(&connection, &self.path, &id, &session_agents)?;

        debug!(
            "loaded {} assistant messages from {session_count} SQLite sessions",
            messages.len()
        );
        Ok(Session {
            id,
            title,
            session_count,
            session_agents,
            messages,
            wall_start,
            wall_end,
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

/// Count of the session and all its descendants.
fn database_session_count(
    connection: &Connection,
    path: &Path,
    id: &str,
) -> Result<usize, BillError> {
    connection
        .query_row(
            r#"WITH RECURSIVE descendants(id) AS (
                   SELECT id FROM session WHERE id = ?1
                   UNION
                   SELECT session.id FROM session
                   JOIN descendants ON session.parent_id = descendants.id
               )
               SELECT COUNT(*) FROM descendants"#,
            params![id],
            |row| row.get(0),
        )
        .map_err(|source| database_error(path, source))
}

/// Agent class of every descendant session, keyed by session ID.
fn database_session_agents(
    connection: &Connection,
    path: &Path,
    id: &str,
) -> Result<BTreeMap<String, String>, BillError> {
    let mut columns = connection
        .prepare("PRAGMA table_info(session)")
        .map_err(|source| database_error(path, source))?;
    let names = columns
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|source| database_error(path, source))?;
    let mut has_agent = false;

    for name in names {
        if name.map_err(|source| database_error(path, source))? == "agent" {
            has_agent = true;
            break;
        }
    }

    if !has_agent {
        return Ok(BTreeMap::new());
    }

    let mut statement = connection
        .prepare(
            r#"WITH RECURSIVE descendants(id) AS (
                   SELECT id FROM session WHERE id = ?1
                   UNION
                   SELECT session.id FROM session
                   JOIN descendants ON session.parent_id = descendants.id
               )
               SELECT session.id, session.agent FROM session
               JOIN descendants ON session.id = descendants.id
               ORDER BY session.id"#,
        )
        .map_err(|source| database_error(path, source))?;
    let rows = statement
        .query_map(params![id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })
        .map_err(|source| database_error(path, source))?;
    let mut agents = BTreeMap::new();

    for row in rows {
        let (session_id, agent) = row.map_err(|source| database_error(path, source))?;

        if let Some(agent) = agent.filter(|agent| !agent.is_empty()) {
            agents.insert(session_id, agent);
        }
    }

    Ok(agents)
}

/// Every assistant message in the session tree, plus the wall-clock bounds.
fn database_messages(
    connection: &Connection,
    path: &Path,
    id: &str,
    session_agents: &BTreeMap<String, String>,
) -> Result<MessageLoad, BillError> {
    let mut statement = connection
        .prepare(
            r#"WITH RECURSIVE descendants(id) AS (
                   SELECT id FROM session WHERE id = ?1
                   UNION
                   SELECT session.id FROM session
                   JOIN descendants ON session.parent_id = descendants.id
               )
               SELECT message.id, message.session_id, message.data FROM message
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
    let tools = database_tool_spans(connection, path, id)?;
    let mut messages = Vec::new();
    let mut wall_start = None;
    let mut wall_end = None;

    for row in rows {
        let (message_id, session_id, json) = row.map_err(|source| database_error(path, source))?;
        let value = parse_json(&json, &location)?;
        let (created, completed) = message_time(&value);

        wall_start = earliest(wall_start, created);
        wall_end = latest(wall_end, completed.or(created));

        if let Some(message) = parse_assistant(
            &value,
            &location,
            &session_id,
            session_agents.get(&session_id).map(String::as_str),
            tools.get(&message_id).map_or(&[][..], Vec::as_slice),
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

/// Collects the tool-call spans of every message in the session tree.
fn database_tool_spans(
    connection: &Connection,
    path: &Path,
    id: &str,
) -> Result<ToolSpans, BillError> {
    if !database_has_table(connection, path, "part")? {
        warn!("no 'part' table: API time will still include tool calls");
        return Ok(ToolSpans::new());
    }

    let mut statement = connection
        .prepare(
            r#"WITH RECURSIVE descendants(id) AS (
                   SELECT id FROM session WHERE id = ?1
                   UNION
                   SELECT session.id FROM session
                   JOIN descendants ON session.parent_id = descendants.id
               )
               SELECT part.message_id, part.data FROM part
               JOIN descendants ON part.session_id = descendants.id"#,
        )
        .map_err(|source| database_error(path, source))?;
    let rows = statement
        .query_map(params![id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|source| database_error(path, source))?;
    let location = path.display().to_string();
    let mut spans = ToolSpans::new();

    for row in rows {
        let (message_id, json) = row.map_err(|source| database_error(path, source))?;
        let value = parse_json(&json, &location)?;

        if let Some(span) = tool_span(&value) {
            spans.entry(message_id).or_default().push(span);
        }
    }

    Ok(spans)
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

/// Maps a SQLite failure onto the bill error type.
fn database_error(path: &Path, source: rusqlite::Error) -> BillError {
    BillError::Database {
        path: path.to_path_buf(),
        source,
    }
}
