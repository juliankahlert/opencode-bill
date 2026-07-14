//! OpenCode session bill generation.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::time::Duration;

use directories::BaseDirs;
use log::{debug, warn};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde_json::Value;
use thiserror::Error;

const DATABASE_FILE: &str = "opencode.db";
const GITHUB_COPILOT_PRICING_URL: &str = "https://docs.github.com/api/article/body?pathname=/en/copilot/reference/copilot-billing/models-and-pricing";
const HTTP_TIMEOUT_SECONDS: u64 = 5;
const MINIMUM_REFERENCE_LENGTH: usize = 6;
const MODELS_FILE: &str = "models.json";
const USER_AGENT: &str = concat!("opencode-bill/", env!("CARGO_PKG_VERSION"));
const THOUSAND: u64 = 1_000;
const MILLION: u64 = 1_000_000;

type ModelKey = (String, String);
type ModelRates = [Option<f64>; 4];

/// Errors returned while locating, reading, or rendering an OpenCode session.
#[derive(Debug, Error)]
pub enum BillError {
    /// No platform data directory is available.
    #[error("the platform does not provide a local data directory")]
    MissingDataDirectory,

    /// The supplied session reference is invalid.
    #[error("invalid session reference: {0}")]
    InvalidSessionReference(String),

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
}

/// Marker for a context builder without a data directory.
pub struct MissingDataDirectory;

/// Marker for a context builder with a data directory.
pub struct DataDirectorySet;

/// Builder that requires an OpenCode data directory before construction.
pub struct StorageContextBuilder<State> {
    data_dir: Option<PathBuf>,
    models_file: Option<PathBuf>,
    state: PhantomData<State>,
}

/// Explicit filesystem state used to locate OpenCode session data.
pub struct StorageContext {
    data_dir: PathBuf,
    models_file: Option<PathBuf>,
}

impl StorageContext {
    /// Starts a context builder with its required field unset.
    #[must_use]
    pub fn builder() -> StorageContextBuilder<MissingDataDirectory> {
        StorageContextBuilder {
            data_dir: None,
            models_file: None,
            state: PhantomData,
        }
    }
}

impl StorageContextBuilder<MissingDataDirectory> {
    /// Uses an explicit OpenCode data directory.
    pub fn data_dir(self, path: impl AsRef<Path>) -> StorageContextBuilder<DataDirectorySet> {
        StorageContextBuilder {
            data_dir: Some(path.as_ref().to_path_buf()),
            models_file: self.models_file,
            state: PhantomData,
        }
    }

    /// Uses `$XDG_DATA_HOME/opencode`, or the platform-equivalent location.
    pub fn platform_data_dir(self) -> Result<StorageContextBuilder<DataDirectorySet>, BillError> {
        let base = BaseDirs::new().ok_or(BillError::MissingDataDirectory)?;

        Ok(StorageContextBuilder {
            data_dir: Some(base.data_local_dir().join("opencode")),
            models_file: Some(base.cache_dir().join("opencode").join(MODELS_FILE)),
            state: PhantomData,
        })
    }
}

impl StorageContextBuilder<DataDirectorySet> {
    /// Uses an explicit OpenCode model catalog instead of the platform cache.
    #[must_use]
    pub fn models_file(mut self, path: impl AsRef<Path>) -> Self {
        self.models_file = Some(path.as_ref().to_path_buf());
        self
    }

    /// Builds a valid storage context.
    pub fn build(self) -> Result<StorageContext, BillError> {
        let data_dir = self.data_dir.ok_or(BillError::MissingDataDirectory)?;

        if data_dir.as_os_str().is_empty() {
            return Err(BillError::MissingDataDirectory);
        }

        Ok(StorageContext {
            data_dir,
            models_file: self.models_file,
        })
    }
}

enum SessionData {
    Found {
        id: String,
        title: String,
        session_count: usize,
        session_agents: BTreeMap<String, String>,
        messages: Vec<MessageUsage>,
    },
}

enum MessageUsage {
    Assistant {
        session_id: String,
        agent: String,
        provider: String,
        model: String,
        input: u64,
        output: u64,
        reasoning: u64,
        cache_read: u64,
        cache_write: u64,
        cost: f64,
    },
}

#[derive(Clone, Copy)]
struct Totals {
    requests: u64,
    input: u64,
    output: u64,
    reasoning: u64,
    cache_read: u64,
    cache_write: u64,
    cost: f64,
}

enum MissingTotals {}

enum TotalsSet {}

struct TotalsBuilder<State> {
    requests: u64,
    input: u64,
    output: u64,
    reasoning: u64,
    cache_read: u64,
    cache_write: u64,
    cost: f64,
    state: PhantomData<State>,
}

struct AgentUsage {
    sessions: BTreeSet<String>,
    totals: Totals,
}

impl Totals {
    fn builder() -> TotalsBuilder<MissingTotals> {
        TotalsBuilder {
            requests: 0,
            input: 0,
            output: 0,
            reasoning: 0,
            cache_read: 0,
            cache_write: 0,
            cost: 0.0,
            state: PhantomData,
        }
    }
}

impl TotalsBuilder<MissingTotals> {
    fn zeroed(self) -> TotalsBuilder<TotalsSet> {
        TotalsBuilder {
            requests: self.requests,
            input: self.input,
            output: self.output,
            reasoning: self.reasoning,
            cache_read: self.cache_read,
            cache_write: self.cache_write,
            cost: self.cost,
            state: PhantomData,
        }
    }
}

impl TotalsBuilder<TotalsSet> {
    fn build(self) -> Totals {
        Totals {
            requests: self.requests,
            input: self.input,
            output: self.output,
            reasoning: self.reasoning,
            cache_read: self.cache_read,
            cache_write: self.cache_write,
            cost: self.cost,
        }
    }
}

/// Generates a deterministic plain-text bill for one OpenCode session.
pub fn generate_bill(context: &StorageContext, reference: &str) -> Result<String, BillError> {
    validate_reference(reference)?;

    let session = load_session(context, reference)?;
    render_bill(context, session)
}

fn validate_reference(reference: &str) -> Result<(), BillError> {
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

fn load_session(context: &StorageContext, reference: &str) -> Result<SessionData, BillError> {
    let database = context.data_dir.join(DATABASE_FILE);

    if database.is_file() {
        return load_database_session(&database, reference);
    }

    load_legacy_session(&context.data_dir.join("storage"), reference)
}

fn open_database(path: &Path) -> Result<Connection, BillError> {
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;

    Connection::open_with_flags(path, flags).map_err(|source| BillError::Database {
        path: path.to_path_buf(),
        source,
    })
}

fn load_database_session(path: &Path, reference: &str) -> Result<SessionData, BillError> {
    let connection = open_database(path)?;
    let matches = database_matches(&connection, path, reference)?;
    let id = unique_match(reference, matches)?;
    let title = database_title(&connection, path, &id)?;
    let session_count = database_session_count(&connection, path, &id)?;
    let session_agents = database_session_agents(&connection, path, &id)?;
    let messages = database_messages(&connection, path, &id, &session_agents)?;

    debug!(
        "loaded {} assistant messages from {session_count} SQLite sessions",
        messages.len()
    );
    Ok(SessionData::Found {
        id,
        title,
        session_count,
        session_agents,
        messages,
    })
}

fn database_matches(
    connection: &Connection,
    path: &Path,
    reference: &str,
) -> Result<Vec<String>, BillError> {
    let pattern = format!("{reference}%");
    let mut statement = connection
        .prepare("SELECT id FROM session WHERE id LIKE ?1 ORDER BY id LIMIT 11")
        .map_err(|source| database_error(path, source))?;
    let rows = statement
        .query_map(params![pattern], |row| row.get::<_, String>(0))
        .map_err(|source| database_error(path, source))?;
    let mut matches = Vec::new();

    for row in rows {
        matches.push(row.map_err(|source| database_error(path, source))?);
    }

    Ok(matches)
}

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

fn database_messages(
    connection: &Connection,
    path: &Path,
    id: &str,
    session_agents: &BTreeMap<String, String>,
) -> Result<Vec<MessageUsage>, BillError> {
    let mut statement = connection
        .prepare(
            r#"WITH RECURSIVE descendants(id) AS (
                   SELECT id FROM session WHERE id = ?1
                   UNION
                   SELECT session.id FROM session
                   JOIN descendants ON session.parent_id = descendants.id
               )
               SELECT message.session_id, message.data FROM message
               JOIN descendants ON message.session_id = descendants.id
               ORDER BY message.time_created, message.id"#,
        )
        .map_err(|source| database_error(path, source))?;
    let rows = statement
        .query_map(params![id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|source| database_error(path, source))?;
    let location = path.display().to_string();
    let mut messages = Vec::new();

    for row in rows {
        let (session_id, json) = row.map_err(|source| database_error(path, source))?;

        if let Some(message) = parse_message(
            &json,
            &location,
            &session_id,
            session_agents.get(&session_id).map(String::as_str),
        )? {
            messages.push(message);
        }
    }

    Ok(messages)
}

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

fn database_error(path: &Path, source: rusqlite::Error) -> BillError {
    BillError::Database {
        path: path.to_path_buf(),
        source,
    }
}

fn load_legacy_session(storage: &Path, reference: &str) -> Result<SessionData, BillError> {
    let session_root = storage.join("session");
    let matches = legacy_matches(&session_root, reference)?;
    let session_path = unique_match(reference, matches)?;
    let session_json = read_json(&session_path)?;
    let id = required_string(&session_json, "id", &session_path.display().to_string())?;
    let title = required_string(&session_json, "title", &session_path.display().to_string())?;
    let session_ids = legacy_descendant_ids(&session_root, &id)?;
    let session_agents = BTreeMap::new();
    let mut messages = Vec::new();

    for session_id in &session_ids {
        messages.extend(legacy_messages(storage, session_id)?);
    }

    debug!(
        "loaded {} assistant messages from {} legacy JSON sessions",
        messages.len(),
        session_ids.len()
    );
    Ok(SessionData::Found {
        id,
        title,
        session_count: session_ids.len(),
        session_agents,
        messages,
    })
}

fn legacy_matches(root: &Path, reference: &str) -> Result<Vec<PathBuf>, BillError> {
    let mut matches = legacy_session_paths(root)?;

    matches.retain(|path| {
        path.file_stem()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(reference))
    });
    Ok(matches)
}

fn legacy_session_paths(root: &Path) -> Result<Vec<PathBuf>, BillError> {
    let mut paths = Vec::new();

    for project in read_directory(root)? {
        let project = project.map_err(|source| read_error(root, source))?;

        if !project.path().is_dir() {
            continue;
        }

        for session in read_directory(&project.path())? {
            let session = session.map_err(|source| read_error(&project.path(), source))?;
            let path = session.path();

            if path.extension().and_then(|value| value.to_str()) == Some("json") {
                paths.push(path);
            }
        }
    }

    paths.sort();
    Ok(paths)
}

fn legacy_descendant_ids(root: &Path, root_id: &str) -> Result<BTreeSet<String>, BillError> {
    let mut parents = BTreeMap::new();

    for path in legacy_session_paths(root)? {
        let value = read_json(&path)?;
        let location = path.display().to_string();
        let id = required_string(&value, "id", &location)?;
        let parent = value
            .get("parentID")
            .and_then(Value::as_str)
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

fn legacy_messages(storage: &Path, session_id: &str) -> Result<Vec<MessageUsage>, BillError> {
    let root = storage.join("message").join(session_id);
    let mut messages = Vec::new();

    if !root.is_dir() {
        return Ok(messages);
    }

    for entry in read_directory(&root)? {
        let entry = entry.map_err(|source| read_error(&root, source))?;
        let path = entry.path();

        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }

        let json = fs::read_to_string(&path).map_err(|source| read_error(&path, source))?;

        if let Some(message) = parse_message(&json, &path.display().to_string(), session_id, None)?
        {
            messages.push(message);
        }
    }

    Ok(messages)
}

fn read_directory(path: &Path) -> Result<fs::ReadDir, BillError> {
    fs::read_dir(path).map_err(|source| read_error(path, source))
}

fn read_json(path: &Path) -> Result<Value, BillError> {
    let json = fs::read_to_string(path).map_err(|source| read_error(path, source))?;

    serde_json::from_str(&json).map_err(|failure| BillError::InvalidData {
        location: path.display().to_string(),
        detail: failure.to_string(),
    })
}

fn read_error(path: &Path, source: std::io::Error) -> BillError {
    BillError::Read {
        path: path.to_path_buf(),
        source,
    }
}

fn unique_match<T>(reference: &str, matches: Vec<T>) -> Result<T, BillError>
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

trait MatchName {
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

fn parse_message(
    json: &str,
    location: &str,
    session_id: &str,
    session_agent: Option<&str>,
) -> Result<Option<MessageUsage>, BillError> {
    let value: Value = serde_json::from_str(json).map_err(|failure| BillError::InvalidData {
        location: location.to_owned(),
        detail: failure.to_string(),
    })?;

    if value.get("role").and_then(Value::as_str) != Some("assistant") {
        return Ok(None);
    }

    let tokens = value
        .get("tokens")
        .ok_or_else(|| invalid_field(location, "tokens"))?;
    let cache = tokens
        .get("cache")
        .ok_or_else(|| invalid_field(location, "tokens.cache"))?;
    let provider = required_string(&value, "providerID", location)?;
    let model = required_string(&value, "modelID", location)?;
    let cost = required_f64(&value, "cost", location)?;
    let input = required_u64(tokens, "input", location)?;
    let output = required_u64(tokens, "output", location)?;
    let reasoning = required_u64(tokens, "reasoning", location)?;
    let cache_read = required_u64(cache, "read", location)?;
    let cache_write = required_u64(cache, "write", location)?;
    let agent = session_agent
        .or_else(|| value.get("agent").and_then(Value::as_str))
        .or_else(|| value.get("mode").and_then(Value::as_str))
        .filter(|agent| !agent.is_empty())
        .unwrap_or("unknown")
        .to_owned();

    if !cost.is_finite() || cost < 0.0 {
        return Err(invalid_field(
            location,
            "cost must be finite and non-negative",
        ));
    }

    Ok(Some(MessageUsage::Assistant {
        session_id: session_id.to_owned(),
        agent,
        provider,
        model,
        input,
        output,
        reasoning,
        cache_read,
        cache_write,
        cost,
    }))
}

fn required_string(value: &Value, field: &str, location: &str) -> Result<String, BillError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| invalid_field(location, field))
}

fn required_u64(value: &Value, field: &str, location: &str) -> Result<u64, BillError> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| invalid_field(location, field))
}

fn required_f64(value: &Value, field: &str, location: &str) -> Result<f64, BillError> {
    value
        .get(field)
        .and_then(Value::as_f64)
        .ok_or_else(|| invalid_field(location, field))
}

fn invalid_field(location: &str, field: &str) -> BillError {
    BillError::InvalidData {
        location: location.to_owned(),
        detail: format!("missing or invalid '{field}'"),
    }
}

fn load_pricing<'a>(
    context: &StorageContext,
    models: impl Iterator<Item = &'a ModelKey>,
) -> Result<BTreeMap<ModelKey, ModelRates>, BillError> {
    let requested = models.cloned().collect::<BTreeSet<_>>();
    let mut pricing = load_local_pricing(context, &requested)?;
    let missing_copilot = requested
        .iter()
        .filter(|key| key.0 == "github-copilot" && !pricing.contains_key(*key))
        .cloned()
        .collect::<Vec<_>>();

    if missing_copilot.is_empty() {
        return Ok(pricing);
    }

    let markdown = match fetch_github_pricing() {
        Ok(markdown) => markdown,
        Err(failure) => {
            warn!("could not fetch GitHub Copilot pricing: {failure}");
            return Ok(pricing);
        }
    };

    for key in missing_copilot {
        if let Some(rates) = github_model_pricing(&markdown, &key.1) {
            pricing.insert(key, rates);
        }
    }

    Ok(pricing)
}

fn load_local_pricing(
    context: &StorageContext,
    requested: &BTreeSet<ModelKey>,
) -> Result<BTreeMap<ModelKey, ModelRates>, BillError> {
    let Some(path) = context.models_file.as_deref() else {
        return Ok(BTreeMap::new());
    };

    if !path.is_file() {
        return Ok(BTreeMap::new());
    }

    let catalog = read_json(path)?;
    let mut pricing = BTreeMap::new();

    for (provider, model) in requested {
        let cost = catalog
            .get(provider)
            .and_then(|value| value.get("models"))
            .and_then(|value| value.get(model))
            .and_then(|value| value.get("cost"));

        if let Some(rates) = cost.and_then(model_rates) {
            pricing.insert((provider.clone(), model.clone()), rates);
        }
    }

    Ok(pricing)
}

fn model_rates(cost: &Value) -> Option<ModelRates> {
    let input = cost.get("input").and_then(valid_rate);
    let output = cost.get("output").and_then(valid_rate);
    let cache_read = cost.get("cache_read").and_then(valid_rate);
    let cache_write = cost.get("cache_write").and_then(valid_rate);

    if input.is_none() && output.is_none() && cache_read.is_none() && cache_write.is_none() {
        return None;
    }

    Some([input, cache_read, cache_write, output])
}

fn valid_rate(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .filter(|rate| rate.is_finite() && *rate >= 0.0)
}

fn fetch_github_pricing() -> Result<String, reqwest::Error> {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(HTTP_TIMEOUT_SECONDS))
        .user_agent(USER_AGENT)
        .build()?
        .get(GITHUB_COPILOT_PRICING_URL)
        .send()?
        .error_for_status()?
        .text()
}

fn github_model_pricing(markdown: &str, model: &str) -> Option<ModelRates> {
    let wanted = normalized_model_id(model);
    let mut headers = Vec::new();

    for line in markdown.lines() {
        let cells = markdown_cells(line);

        if cells.iter().any(|cell| cell == "Model") && cells.iter().any(|cell| cell == "Input") {
            headers = cells;
            continue;
        }

        if headers.is_empty() || cells.len() != headers.len() {
            continue;
        }

        let fields = headers
            .iter()
            .map(String::as_str)
            .zip(cells.iter().map(String::as_str))
            .collect::<BTreeMap<_, _>>();
        let name = fields.get("Model")?;

        if normalize_model_name(name) != wanted || !is_default_tier(&fields) {
            continue;
        }

        let input = markdown_rate(&fields, "Input");
        let output = markdown_rate(&fields, "Output");
        let cache_read = markdown_rate(&fields, "Cached input");
        let cache_write = markdown_rate(&fields, "Cache write");

        if input.is_some() && output.is_some() {
            return Some([input, cache_read, cache_write, output]);
        }
    }

    None
}

fn markdown_cells(line: &str) -> Vec<String> {
    let trimmed = line.trim();

    if !trimmed.starts_with('|') || !trimmed.ends_with('|') {
        return Vec::new();
    }

    trimmed
        .trim_matches('|')
        .split('|')
        .map(|cell| cell.trim().to_owned())
        .collect()
}

fn markdown_rate(fields: &BTreeMap<&str, &str>, name: &str) -> Option<f64> {
    fields
        .get(name)
        .and_then(|value| value.trim().trim_start_matches('$').parse().ok())
        .filter(|rate: &f64| rate.is_finite() && *rate >= 0.0)
}

fn is_default_tier(fields: &BTreeMap<&str, &str>) -> bool {
    fields
        .get("Tier")
        .is_none_or(|tier| tier.is_empty() || *tier == "Default")
}

fn normalized_model_id(model: &str) -> String {
    const EFFORT_SUFFIXES: [&str; 5] = ["-xhigh", "-high", "-medium", "-low", "-none"];
    let base = EFFORT_SUFFIXES
        .iter()
        .find_map(|suffix| model.strip_suffix(suffix))
        .unwrap_or(model);

    normalize_model_name(base)
}

fn normalize_model_name(name: &str) -> String {
    let mut normalized = String::new();
    let mut separator = false;

    for character in name.chars().flat_map(char::to_lowercase) {
        if character.is_ascii_alphanumeric() {
            normalized.push(character);
            separator = false;
        } else if !separator && !normalized.is_empty() {
            normalized.push('-');
            separator = true;
        }
    }

    normalized.trim_end_matches('-').to_owned()
}

fn render_bill(context: &StorageContext, session: SessionData) -> Result<String, BillError> {
    let SessionData::Found {
        id,
        title,
        session_count,
        session_agents,
        messages,
    } = session;
    let agent_usage = aggregate_agents(&messages, &session_agents);
    let usage = aggregate(messages);
    let total = usage
        .values()
        .fold(Totals::builder().zeroed().build(), add_totals);
    let pricing = load_pricing(context, usage.keys())?;
    let usage_rows = usage.iter().map(usage_row).collect::<Vec<_>>();
    let mut output = String::new();

    output.push_str("OPENCODE SESSION BILL\n");
    output.push_str(&format!("Session: {id}\n"));
    output.push_str(&format!("Title:   {title}\n"));
    output.push_str(&format!(
        "Sessions: {session_count} (root + descendants)\n\n"
    ));
    output.push_str(&render_table(
        &[
            "PROVIDER / MODEL",
            "REQUESTS",
            "INPUT",
            "CACHE READ",
            "CACHE WRITE",
            "OUTPUT",
            "REASONING",
            "COST (USD)",
        ],
        &usage_rows,
    )?);

    let agent_rows = agent_usage.iter().map(agent_row).collect::<Vec<_>>();

    output.push_str("\nUSAGE BY AGENT CLASS\n");
    output.push_str(&render_table(
        &[
            "AGENT",
            "SESSIONS",
            "REQUESTS",
            "INPUT",
            "CACHE READ",
            "CACHE WRITE",
            "OUTPUT",
            "REASONING",
            "COST (USD)",
        ],
        &agent_rows,
    )?);

    if !pricing.is_empty() {
        let pricing_rows = pricing.iter().map(pricing_row).collect::<Vec<_>>();

        output.push_str("\nMODEL PRICING (USD / 1M TOKENS)\n");
        output.push_str(&render_table(
            &[
                "PROVIDER / MODEL",
                "INPUT",
                "CACHE READ",
                "CACHE WRITE",
                "OUTPUT",
            ],
            &pricing_rows,
        )?);
    }

    output.push_str("\nTOTAL\n");
    output.push_str(&format!("Requests:    {}\n", compact_count(total.requests)));
    output.push_str(&format!("Input:       {}\n", compact_count(total.input)));
    output.push_str(&format!(
        "Cache read:  {}\n",
        compact_count(total.cache_read)
    ));
    output.push_str(&format!(
        "Cache write: {}\n",
        compact_count(total.cache_write)
    ));
    output.push_str(&format!("Output:      {}\n", compact_count(total.output)));
    output.push_str(&format!(
        "Reasoning:   {}\n",
        compact_count(total.reasoning)
    ));
    output.push_str(&format!("Amount due:  ${:.6} USD\n", total.cost));
    Ok(output)
}

fn usage_row(entry: (&ModelKey, &Totals)) -> Vec<String> {
    let ((provider, model), totals) = entry;

    vec![
        format!("{provider} / {model}"),
        compact_count(totals.requests),
        compact_count(totals.input),
        compact_count(totals.cache_read),
        compact_count(totals.cache_write),
        compact_count(totals.output),
        compact_count(totals.reasoning),
        format!("${:.6}", totals.cost),
    ]
}

fn agent_row(entry: (&String, &AgentUsage)) -> Vec<String> {
    let (agent, usage) = entry;
    let totals = &usage.totals;

    vec![
        agent.clone(),
        compact_count(usage.sessions.len() as u64),
        compact_count(totals.requests),
        compact_count(totals.input),
        compact_count(totals.cache_read),
        compact_count(totals.cache_write),
        compact_count(totals.output),
        compact_count(totals.reasoning),
        format!("${:.6}", totals.cost),
    ]
}

fn pricing_row(entry: (&ModelKey, &ModelRates)) -> Vec<String> {
    let ((provider, model), [input, cache_read, cache_write, output]) = entry;

    vec![
        format!("{provider} / {model}"),
        format_rate(*input),
        format_rate(*cache_read),
        format_rate(*cache_write),
        format_rate(*output),
    ]
}

fn format_rate(rate: Option<f64>) -> String {
    rate.map_or_else(|| "-".to_owned(), |value| format!("${value:.3}"))
}

fn render_table(headers: &[&str], rows: &[Vec<String>]) -> Result<String, BillError> {
    if headers.is_empty() {
        return Err(render_error("table has no columns"));
    }

    if rows.iter().any(|row| row.len() != headers.len()) {
        return Err(render_error("table row has the wrong column count"));
    }

    let mut widths = headers
        .iter()
        .map(|header| header.len())
        .collect::<Vec<_>>();

    for row in rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.len());
        }
    }

    let mut output = String::new();
    let header_cells = headers
        .iter()
        .map(|value| (*value).to_owned())
        .collect::<Vec<_>>();

    push_table_row(&mut output, &header_cells, &widths);
    output.push_str(
        &widths
            .iter()
            .map(|width| "-".repeat(*width))
            .collect::<Vec<_>>()
            .join("-+-"),
    );
    output.push('\n');

    for row in rows {
        push_table_row(&mut output, row, &widths);
    }

    Ok(output)
}

fn push_table_row(output: &mut String, cells: &[String], widths: &[usize]) {
    let rendered = cells
        .iter()
        .zip(widths)
        .enumerate()
        .map(|(column, (cell, width))| {
            if column == 0 {
                return format!("{cell:<width$}");
            }

            format!("{cell:>width$}")
        })
        .collect::<Vec<_>>()
        .join(" | ");

    output.push_str(&rendered);
    output.push('\n');
}

fn render_error(detail: &str) -> BillError {
    BillError::InvalidData {
        location: "bill renderer".to_owned(),
        detail: detail.to_owned(),
    }
}

fn compact_count(value: u64) -> String {
    if value >= MILLION {
        return format!("{:.1}M", value as f64 / MILLION as f64);
    }

    if value >= THOUSAND {
        return format!("{:.1}k", value as f64 / THOUSAND as f64);
    }

    value.to_string()
}

fn aggregate(messages: Vec<MessageUsage>) -> BTreeMap<ModelKey, Totals> {
    let mut usage = BTreeMap::new();

    for message in messages {
        let MessageUsage::Assistant {
            session_id: _,
            agent: _,
            provider,
            model,
            input,
            output,
            reasoning,
            cache_read,
            cache_write,
            cost,
        } = message;
        let totals = usage
            .entry((provider, model))
            .or_insert_with(|| Totals::builder().zeroed().build());

        totals.requests += 1;
        totals.input += input;
        totals.output += output;
        totals.reasoning += reasoning;
        totals.cache_read += cache_read;
        totals.cache_write += cache_write;
        totals.cost += cost;
    }

    usage
}

fn aggregate_agents(
    messages: &[MessageUsage],
    session_agents: &BTreeMap<String, String>,
) -> BTreeMap<String, AgentUsage> {
    let mut usage = BTreeMap::new();

    for (session_id, agent) in session_agents {
        agent_usage_entry(&mut usage, agent)
            .sessions
            .insert(session_id.clone());
    }

    for message in messages {
        let MessageUsage::Assistant {
            session_id,
            agent,
            input,
            output,
            reasoning,
            cache_read,
            cache_write,
            cost,
            ..
        } = message;
        let entry = agent_usage_entry(&mut usage, agent);

        entry.sessions.insert(session_id.clone());
        entry.totals.requests += 1;
        entry.totals.input += input;
        entry.totals.output += output;
        entry.totals.reasoning += reasoning;
        entry.totals.cache_read += cache_read;
        entry.totals.cache_write += cache_write;
        entry.totals.cost += cost;
    }

    usage
}

fn agent_usage_entry<'a>(
    usage: &'a mut BTreeMap<String, AgentUsage>,
    agent: &str,
) -> &'a mut AgentUsage {
    usage.entry(agent.to_owned()).or_insert_with(|| AgentUsage {
        sessions: BTreeSet::new(),
        totals: Totals::builder().zeroed().build(),
    })
}

fn add_totals(mut sum: Totals, value: &Totals) -> Totals {
    sum.requests += value.requests;
    sum.input += value.input;
    sum.output += value.output;
    sum.reasoning += value.reasoning;
    sum.cache_read += value.cache_read;
    sum.cache_write += value.cache_write;
    sum.cost += value.cost;
    sum
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use tempfile::TempDir;

    fn fixture() -> Result<(TempDir, StorageContext), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let database = temporary.path().join(DATABASE_FILE);
        let connection = Connection::open(&database)?;

        connection.execute_batch(
            "CREATE TABLE session (id TEXT PRIMARY KEY, title TEXT NOT NULL, parent_id TEXT);\
             CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT NOT NULL,\
             time_created INTEGER NOT NULL, data TEXT NOT NULL);",
        )?;
        connection.execute(
            "INSERT INTO session (id, title) VALUES (?1, ?2)",
            params!["ses_abcdef123456", "Test session"],
        )?;
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_1",
                "ses_abcdef123456",
                1,
                r#"{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.125,"tokens":{"input":10,"output":4,"reasoning":2,"cache":{"read":6,"write":1}}}"#
            ],
        )?;
        drop(connection);

        let context = StorageContext::builder()
            .data_dir(temporary.path())
            .build()?;
        Ok((temporary, context))
    }

    fn has_table_row(output: &str, expected: &[&str]) -> bool {
        output
            .lines()
            .any(|line| line.split('|').map(str::trim).eq(expected.iter().copied()))
    }

    #[test]
    fn generates_bill_from_unique_prefix() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;

        let bill = generate_bill(&context, "ses_abcdef")?;

        assert!(bill.contains("Session: ses_abcdef123456"));
        assert!(bill.contains("Sessions: 1 (root + descendants)"));
        assert!(has_table_row(
            &bill,
            &[
                "openai / gpt-test",
                "1",
                "10",
                "6",
                "1",
                "4",
                "2",
                "$0.125000"
            ]
        ));
        assert!(bill.contains("Amount due:  $0.125000 USD"));
        Ok(())
    }

    #[test]
    fn compacts_large_usage_counts() {
        assert_eq!(compact_count(999), "999");
        assert_eq!(compact_count(1_000), "1.0k");
        assert_eq!(compact_count(48_579), "48.6k");
        assert_eq!(compact_count(1_000_000), "1.0M");
        assert_eq!(compact_count(1_958_181), "2.0M");
    }

    #[test]
    fn aligns_text_left_and_numbers_right() -> Result<(), Box<dyn std::error::Error>> {
        let table = render_table(
            &["MODEL", "REQUESTS", "COST"],
            &[
                vec!["short".to_owned(), "2".to_owned(), "$1.00".to_owned()],
                vec![
                    "long model".to_owned(),
                    "10".to_owned(),
                    "$12.00".to_owned(),
                ],
            ],
        )?;

        assert_eq!(
            table,
            "MODEL      | REQUESTS |   COST\n-----------+----------+-------\nshort      |        2 |  $1.00\nlong model |       10 | $12.00\n"
        );
        Ok(())
    }

    #[test]
    fn renders_known_model_pricing() -> Result<(), Box<dyn std::error::Error>> {
        let (temporary, _) = fixture()?;
        let models_file = temporary.path().join("models.json");

        fs::write(
            &models_file,
            r#"{"openai":{"models":{"gpt-test":{"cost":{"input":1.25,"output":5,"cache_read":0.125}}}}}"#,
        )?;

        let context = StorageContext::builder()
            .data_dir(temporary.path())
            .models_file(&models_file)
            .build()?;
        let bill = generate_bill(&context, "ses_abcdef")?;

        assert!(bill.contains("MODEL PRICING (USD / 1M TOKENS)"));
        assert!(has_table_row(
            &bill,
            &["openai / gpt-test", "$1.250", "$0.125", "-", "$5.000"]
        ));
        Ok(())
    }

    #[test]
    fn parses_github_pricing_for_effort_variant() {
        let markdown = r#"
| Model         | Release status | Category | Tier         | Threshold | Input | Cached input | Output |
| ------------- | -------------- | -------- | ------------ | --------- | ----: | -----------: | -----: |
| GPT-5.6 Sol   | GA             | Powerful | Default      | <= 272K   | $5.00 |        $0.50 | $30.00 |
| GPT-5.6 Sol   | GA             | Powerful | Long context | > 272K    | $10.00 |        $1.00 | $45.00 |
"#;

        assert_eq!(
            github_model_pricing(markdown, "gpt-5.6-sol-low"),
            Some([Some(5.0), Some(0.5), None, Some(30.0)])
        );
    }

    #[test]
    fn includes_all_sqlite_descendant_sessions() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        connection.execute("ALTER TABLE session ADD COLUMN agent TEXT", [])?;
        connection.execute(
            "UPDATE session SET agent = ?1 WHERE id = ?2",
            params!["build", "ses_abcdef123456"],
        )?;
        connection.execute(
            "INSERT INTO session (id, title, parent_id, agent) VALUES (?1, ?2, ?3, ?4)",
            params!["ses_child123456", "Child", "ses_abcdef123456", "explore"],
        )?;
        connection.execute(
            "INSERT INTO session (id, title, parent_id, agent) VALUES (?1, ?2, ?3, ?4)",
            params![
                "ses_grandchild1",
                "Grandchild",
                "ses_child123456",
                "explore"
            ],
        )?;
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_child",
                "ses_child123456",
                2,
                r#"{"role":"assistant","providerID":"anthropic","modelID":"child-model","cost":0.25,"tokens":{"input":20,"output":8,"reasoning":1,"cache":{"read":4,"write":2}}}"#
            ],
        )?;
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_grandchild",
                "ses_grandchild1",
                3,
                r#"{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.5,"tokens":{"input":30,"output":12,"reasoning":3,"cache":{"read":7,"write":0}}}"#
            ],
        )?;
        drop(connection);

        let bill = generate_bill(&context, "ses_abcdef123456")?;

        assert!(bill.contains("Sessions: 3 (root + descendants)"));
        assert!(has_table_row(
            &bill,
            &[
                "anthropic / child-model",
                "1",
                "20",
                "4",
                "2",
                "8",
                "1",
                "$0.250000"
            ]
        ));
        assert!(has_table_row(
            &bill,
            &[
                "openai / gpt-test",
                "2",
                "40",
                "13",
                "1",
                "16",
                "5",
                "$0.625000"
            ]
        ));
        assert!(bill.contains("Requests:    3"));
        assert!(bill.contains("Amount due:  $0.875000 USD"));
        assert!(bill.contains("USAGE BY AGENT CLASS"));
        assert!(has_table_row(
            &bill,
            &["build", "1", "1", "10", "6", "1", "4", "2", "$0.125000"]
        ));
        assert!(has_table_row(
            &bill,
            &["explore", "2", "2", "50", "11", "2", "20", "4", "$0.750000"]
        ));
        Ok(())
    }

    #[test]
    fn rejects_unsafe_reference() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;

        let failure = generate_bill(&context, "ses_%")
            .err()
            .ok_or("expected an error")?;

        assert!(matches!(failure, BillError::InvalidSessionReference(_)));
        Ok(())
    }

    #[test]
    fn reports_missing_session() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;

        let failure = generate_bill(&context, "ses_missing")
            .err()
            .ok_or("expected an error")?;

        assert!(matches!(failure, BillError::SessionNotFound(_)));
        Ok(())
    }

    #[test]
    fn rejects_ambiguous_prefix() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        connection.execute(
            "INSERT INTO session (id, title) VALUES (?1, ?2)",
            params!["ses_abcdef999999", "Other session"],
        )?;
        drop(connection);

        let failure = generate_bill(&context, "ses_abcdef")
            .err()
            .ok_or("expected an error")?;

        assert!(matches!(failure, BillError::AmbiguousSession { .. }));
        Ok(())
    }

    #[test]
    fn generates_bill_from_legacy_json() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let session_id = "ses_legacy123456";
        let session_dir = temporary.path().join("storage/session/project");
        let message_dir = temporary.path().join("storage/message").join(session_id);

        fs::create_dir_all(&session_dir)?;
        fs::create_dir_all(&message_dir)?;
        fs::write(
            session_dir.join(format!("{session_id}.json")),
            format!(r#"{{"id":"{session_id}","title":"Legacy session"}}"#),
        )?;
        fs::write(
            message_dir.join("msg_1.json"),
            r#"{"role":"assistant","agent":"legacy-agent","providerID":"anthropic","modelID":"legacy","cost":0.25,"tokens":{"input":20,"output":8,"reasoning":1,"cache":{"read":4,"write":2}}}"#,
        )?;

        let context = StorageContext::builder()
            .data_dir(temporary.path())
            .build()?;
        let bill = generate_bill(&context, "ses_legacy")?;

        assert!(has_table_row(
            &bill,
            &[
                "anthropic / legacy",
                "1",
                "20",
                "4",
                "2",
                "8",
                "1",
                "$0.250000"
            ]
        ));
        assert!(has_table_row(
            &bill,
            &[
                "legacy-agent",
                "1",
                "1",
                "20",
                "4",
                "2",
                "8",
                "1",
                "$0.250000"
            ]
        ));
        Ok(())
    }

    #[test]
    fn includes_legacy_child_sessions() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let root_id = "ses_legacyroot12";
        let child_id = "ses_legacychild1";
        let session_dir = temporary.path().join("storage/session/project");
        let message_dir = temporary.path().join("storage/message").join(child_id);

        fs::create_dir_all(&session_dir)?;
        fs::create_dir_all(&message_dir)?;
        fs::write(
            session_dir.join(format!("{root_id}.json")),
            format!(r#"{{"id":"{root_id}","title":"Legacy root"}}"#),
        )?;
        fs::write(
            session_dir.join(format!("{child_id}.json")),
            format!(r#"{{"id":"{child_id}","title":"Child","parentID":"{root_id}"}}"#),
        )?;
        fs::write(
            message_dir.join("msg_child.json"),
            r#"{"role":"assistant","providerID":"anthropic","modelID":"legacy-child","cost":0.75,"tokens":{"input":50,"output":10,"reasoning":2,"cache":{"read":8,"write":1}}}"#,
        )?;

        let context = StorageContext::builder()
            .data_dir(temporary.path())
            .build()?;
        let bill = generate_bill(&context, root_id)?;

        assert!(bill.contains("Sessions: 2 (root + descendants)"));
        assert!(has_table_row(
            &bill,
            &[
                "anthropic / legacy-child",
                "1",
                "50",
                "8",
                "1",
                "10",
                "2",
                "$0.750000"
            ]
        ));
        Ok(())
    }
}
