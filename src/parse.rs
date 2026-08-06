//! Extraction of typed records from OpenCode's JSON message data.

use std::fs;
use std::path::Path;

use serde_json::Value;

use crate::data::{Tokens, UsageRecord};
use crate::error::BillError;
use crate::timing::message_api_ms;

/// Parses a JSON string, tagging failures with its location.
pub(crate) fn parse_json(json: &str, location: &str) -> Result<Value, BillError> {
    serde_json::from_str(json).map_err(|failure| BillError::InvalidData {
        location: location.to_owned(),
        detail: failure.to_string(),
    })
}

/// Reads and parses a JSON file.
pub(crate) fn read_json(path: &Path) -> Result<Value, BillError> {
    let json = fs::read_to_string(path).map_err(|source| read_error(path, source))?;

    serde_json::from_str(&json).map_err(|failure| BillError::InvalidData {
        location: path.display().to_string(),
        detail: failure.to_string(),
    })
}

/// Maps a file I/O failure onto the bill error type.
pub(crate) fn read_error(path: &Path, source: std::io::Error) -> BillError {
    BillError::Read {
        path: path.to_path_buf(),
        source,
    }
}

/// The `time.created` and `time.completed` fields of a message.
pub(crate) fn message_time(value: &Value) -> (Option<u64>, Option<u64>) {
    let time = value.get("time");

    (
        time.and_then(|value| value.get("created"))
            .and_then(Value::as_u64),
        time.and_then(|value| value.get("completed"))
            .and_then(Value::as_u64),
    )
}

/// Returns the busy span of a tool part, if it ran to completion.
pub(crate) fn tool_span(value: &Value) -> Option<(u64, u64)> {
    if value.get("type").and_then(Value::as_str) != Some("tool") {
        return None;
    }

    let time = value.get("state").and_then(|state| state.get("time"))?;
    let start = time.get("start").and_then(Value::as_u64)?;
    let end = time.get("end").and_then(Value::as_u64)?;

    (end > start).then_some((start, end))
}

/// Parses an assistant message into a usage record, or `None` for other roles.
pub(crate) fn parse_assistant(
    value: &Value,
    location: &str,
    session_id: &str,
    session_agent: Option<&str>,
    tools: &[(u64, u64)],
) -> Result<Option<UsageRecord>, BillError> {
    if value.get("role").and_then(Value::as_str) != Some("assistant") {
        return Ok(None);
    }

    let tokens = value
        .get("tokens")
        .ok_or_else(|| invalid_field(location, "tokens"))?;
    let cache = tokens
        .get("cache")
        .ok_or_else(|| invalid_field(location, "tokens.cache"))?;
    let provider = required_string(value, "providerID", location)?;
    let model = required_string(value, "modelID", location)?;
    let cost = required_f64(value, "cost", location)?;
    let usage = Tokens {
        input: required_u64(tokens, "input", location)?,
        output: required_u64(tokens, "output", location)?,
        reasoning: required_u64(tokens, "reasoning", location)?,
        cache_read: required_u64(cache, "read", location)?,
        cache_write: required_u64(cache, "write", location)?,
    };
    let agent = session_agent
        .or_else(|| value.get("agent").and_then(Value::as_str))
        .or_else(|| value.get("mode").and_then(Value::as_str))
        .filter(|agent| !agent.is_empty())
        .unwrap_or("unknown")
        .to_owned();
    let (created, completed) = message_time(value);

    if !cost.is_finite() || cost < 0.0 {
        return Err(invalid_field(
            location,
            "cost must be finite and non-negative",
        ));
    }

    Ok(Some(UsageRecord {
        session_id: session_id.to_owned(),
        agent,
        provider,
        model,
        tokens: usage,
        cost,
        api_ms: message_api_ms(created, completed, tools),
        created,
        completed,
    }))
}

/// Reads a required non-empty string field.
pub(crate) fn required_string(
    value: &Value,
    field: &str,
    location: &str,
) -> Result<String, BillError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| invalid_field(location, field))
}

/// Reads a required integer field.
fn required_u64(value: &Value, field: &str, location: &str) -> Result<u64, BillError> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| invalid_field(location, field))
}

/// Reads a required floating-point field.
fn required_f64(value: &Value, field: &str, location: &str) -> Result<f64, BillError> {
    value
        .get(field)
        .and_then(Value::as_f64)
        .ok_or_else(|| invalid_field(location, field))
}

/// Builds the error for a missing or invalid field.
fn invalid_field(location: &str, field: &str) -> BillError {
    BillError::InvalidData {
        location: location.to_owned(),
        detail: format!("missing or invalid '{field}'"),
    }
}
