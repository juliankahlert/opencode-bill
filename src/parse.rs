//! Extraction of typed records from OpenCode's JSON message data.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use rayon::prelude::*;
use serde_json::Value;

use crate::bill::PipelineProgress;
use crate::data::{
    AgentId, Interner, SessionNode, SessionTree, Tokens, ToolSpans, UsageRow, UsageStore, World,
    usd_to_micros,
};
use crate::error::BillError;
use crate::store::{RawLoad, RawMessage, RawPart};
use crate::timing::{earliest, latest, message_api_ms};

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

/// One parsed message: its wall-clock bounds plus an internable usage row.
///
/// Parallel workers parse JSON and extract raw strings; interning happens
/// later in one ordered pass, so handle assignment — and with it the
/// byte-identical bill output — stays deterministic.
struct ParsedMessage {
    /// Session id of the message, for ordered interning.
    session_id: String,
    /// Message creation timestamp (Unix millis).
    created: Option<u64>,
    /// Message completion timestamp (Unix millis).
    completed: Option<u64>,
    /// The internable usage row of an assistant message.
    row: Option<ParsedRow>,
}

/// An internable assistant row: raw strings plus the pre-resolved session
/// agent handle, which the session pass already interned in a fixed order.
struct ParsedRow {
    /// Declared agent of the message's session, already interned.
    session_agent: Option<AgentId>,
    /// Message-level agent fallback, when the session declares none.
    message_agent: Option<String>,
    /// Provider id string, interned in the ordered merge pass.
    provider: String,
    /// Model id string, interned in the ordered merge pass.
    model: String,
    /// Token usage of the call.
    tokens: Tokens,
    /// Cost of the call in micro-dollars.
    cost_micros: i64,
    /// Whether the recorded cost is zero because no price was known.
    cost_unknown: bool,
    /// Time the model endpoint was busy, in milliseconds.
    api_ms: u64,
    /// Message creation timestamp (Unix millis).
    created: Option<u64>,
    /// Message completion timestamp (Unix millis).
    completed: Option<u64>,
}

/// Parses one message row without interning anything.
///
/// This is the parallel work unit of `parse_system`: pure JSON decoding and
/// string extraction, sharing only the read-only session-agent map and tool
/// spans.  Every string identity is returned raw; the ordered merge pass in
/// `parse_system` interns it, preserving the interning order.
fn parse_message(
    message: &RawMessage,
    session_agents: &BTreeMap<String, Option<AgentId>>,
    tools: &ToolSpans,
) -> Result<ParsedMessage, BillError> {
    let value = parse_json(&message.json, &message.location)?;
    let (created, completed) = message_time(&value);
    let session_agent = session_agents.get(&message.session_id).copied().flatten();

    let row = if value.get("role").and_then(Value::as_str) == Some("assistant") {
        Some(parse_assistant_row(
            &value,
            &message.location,
            session_agent,
            tools.get(&message.id).map_or(&[][..], Vec::as_slice),
        )?)
    } else {
        None
    };

    Ok(ParsedMessage {
        session_id: message.session_id.clone(),
        created,
        completed,
        row,
    })
}

/// Extracts the internable fields of one assistant message.
///
/// Mirrors the pre-Phase-5 `parse_assistant` exactly, except that nothing is
/// interned: the message-level agent fallback is only extracted when the
/// session declares no agent, so the interning order stays byte-identical.
fn parse_assistant_row(
    value: &Value,
    location: &str,
    session_agent: Option<AgentId>,
    tools: &[(u64, u64)],
) -> Result<ParsedRow, BillError> {
    let tokens = value
        .get("tokens")
        .ok_or_else(|| invalid_field(location, "tokens"))?;
    let cache = tokens
        .get("cache")
        .ok_or_else(|| invalid_field(location, "tokens.cache"))?;
    let provider = required_string(value, "providerID", location)?;
    let model = required_string(value, "modelID", location)?;
    let usage = Tokens {
        input: required_u64(tokens, "input", location)?,
        output: required_u64(tokens, "output", location)?,
        reasoning: required_u64(tokens, "reasoning", location)?,
        cache_read: required_u64(cache, "read", location)?,
        cache_write: required_u64(cache, "write", location)?,
    };
    let message_agent = if session_agent.is_some() {
        None
    } else {
        Some(
            value
                .get("agent")
                .and_then(Value::as_str)
                .or_else(|| value.get("mode").and_then(Value::as_str))
                .filter(|agent| !agent.is_empty())
                .unwrap_or("unknown")
                .to_owned(),
        )
    };
    let (created, completed) = message_time(value);
    let cost = required_f64(value, "cost", location)?;

    if !cost.is_finite() {
        return Err(invalid_field(location, "cost must be finite"));
    }

    // A zero cost usually means the client had no price for this model (for
    // example subscription usage) and the call can be billed at a known
    // rate; a negative cost is a provider correction, which is clamped to
    // zero and must never be re-priced.
    let cost_unknown = cost == 0.0;

    // Providers correct over-counted usage with a negative charge; a bill
    // cannot show negative usage, so clamp the charge to zero.
    let cost_micros = usd_to_micros(cost.max(0.0));

    Ok(ParsedRow {
        session_agent,
        message_agent,
        provider,
        model,
        tokens: usage,
        cost_micros,
        cost_unknown,
        api_ms: message_api_ms(created, completed, tools),
        created,
        completed,
    })
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

/// Reads a required integer field, clamping negative corrections to zero.
fn required_u64(value: &Value, field: &str, location: &str) -> Result<u64, BillError> {
    value
        .get(field)
        .and_then(|count| {
            count
                .as_u64()
                .or_else(|| count.as_i64().filter(|count| *count < 0).map(|_| 0))
        })
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

/// Parses raw storage rows into a world: entities, components, interner.
///
/// The system preserves the row order the store produced, so the interning
/// order of session/agent/provider/model handles — and with it the
/// byte-identical bill output — stays exactly as before.
///
/// Message parsing is the parallel work unit: a rayon `par_iter` decodes
/// every message JSON in parallel and extracts raw strings, while the merge
/// pass below interns in message order.  Interning stays single-threaded and
/// ordered, so handle assignment is deterministic regardless of thread
/// scheduling.  The first failing message in store order stops the pipeline.
pub(crate) fn parse_system(raw: RawLoad, progress: &PipelineProgress) -> Result<World, BillError> {
    progress.note_phase(1);
    let mut interner = Interner::default();
    let mut sessions = SessionTree::new();
    let mut session_agents = BTreeMap::new();

    for session in raw.sessions {
        progress.parsed_row();
        let id = interner.intern_session(&session.id);
        let agent = session
            .agent
            .filter(|agent| !agent.is_empty())
            .map(|agent| interner.intern_agent(&agent));

        sessions.insert(id, SessionNode { agent });
        session_agents.entry(session.id).or_insert(agent);
    }

    let tools = parse_tool_spans(raw.parts, progress)?;

    let results = raw
        .messages
        .par_iter()
        .map(|message| {
            progress.parsed_row();

            parse_message(message, &session_agents, &tools)
        })
        .collect::<Vec<_>>();

    let mut entities = UsageStore::default();
    let mut wall_start = None;
    let mut wall_end = None;

    for result in results {
        let message = result?;
        let session_handle = interner.intern_session(&message.session_id);

        wall_start = earliest(wall_start, message.created);
        wall_end = latest(wall_end, message.completed.or(message.created));

        let Some(row) = message.row else {
            continue;
        };
        let agent = match row.session_agent {
            Some(agent) => agent,
            None => interner.intern_agent(row.message_agent.as_deref().unwrap_or("unknown")),
        };

        entities.push(UsageRow {
            session_id: session_handle,
            agent,
            provider: interner.intern_provider(&row.provider),
            model: interner.intern_model(&row.model),
            tokens: row.tokens,
            cost_micros: row.cost_micros,
            cost_unknown: row.cost_unknown,
            api_ms: row.api_ms,
            created: row.created,
            completed: row.completed,
        });
    }

    Ok(World {
        header: raw.header,
        session_count: raw.session_count,
        sessions,
        entities,
        wall_start,
        wall_end,
        interner,
    })
}

/// Groups the raw tool part rows into per-message busy spans.
fn parse_tool_spans(
    parts: Vec<RawPart>,
    progress: &PipelineProgress,
) -> Result<ToolSpans, BillError> {
    let mut spans = ToolSpans::new();

    for part in parts {
        progress.parsed_row();
        let value = parse_json(&part.json, &part.location)?;

        if let Some(span) = tool_span(&value) {
            spans.entry(part.message_id).or_default().push(span);
        }
    }

    Ok(spans)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::BillHeader;
    use crate::store::{RawLoad, RawMessage, RawPart, RawSession};

    #[test]
    fn parse_system_builds_world_from_raw_rows() -> Result<(), Box<dyn std::error::Error>> {
        let raw = RawLoad {
            header: BillHeader::Session {
                id: "ses_abc123456".to_owned(),
                title: "Test session".to_owned(),
            },
            session_count: 1,
            sessions: vec![RawSession {
                id: "ses_abc123456".to_owned(),
                agent: Some("build".to_owned()),
            }],
            messages: vec![
                RawMessage {
                    id: "msg_1".to_owned(),
                    session_id: "ses_abc123456".to_owned(),
                    location: "fixture".to_owned(),
                    json: r#"{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.125,"time":{"created":1000,"completed":3000},"tokens":{"input":10,"output":4,"reasoning":2,"cache":{"read":6,"write":1}}}"#
                        .to_owned(),
                },
                RawMessage {
                    id: "msg_user".to_owned(),
                    session_id: "ses_abc123456".to_owned(),
                    location: "fixture".to_owned(),
                    json: r#"{"role":"user","time":{"created":500}}"#.to_owned(),
                },
            ],
            parts: vec![RawPart {
                message_id: "msg_1".to_owned(),
                location: "fixture".to_owned(),
                json: r#"{"type":"tool","state":{"status":"completed","time":{"start":1000,"end":2500}}}"#
                    .to_owned(),
            }],
        };

        let world = parse_system(raw, &PipelineProgress::default())?;

        assert_eq!(world.session_count, 1);
        assert_eq!(world.sessions.len(), 1);
        assert_eq!(world.entities.len(), 1);

        let row = world
            .entities
            .rows()
            .next()
            .ok_or("expected one usage row")?;

        assert_eq!(world.interner.resolve_agent(row.agent), "build");
        assert_eq!(world.interner.resolve_provider(row.provider), "openai");
        assert_eq!(world.interner.resolve_model(row.model), "gpt-test");
        // 3000 - 1000 message span minus the 2500 - 1000 tool span.
        assert_eq!(row.api_ms, 500);
        // The user message sets the earliest wall bound.
        assert_eq!(world.wall_start, Some(500));
        assert_eq!(world.wall_end, Some(3000));
        Ok(())
    }

    #[test]
    fn parse_system_interns_sessions_in_row_order() -> Result<(), Box<dyn std::error::Error>> {
        let raw = RawLoad {
            header: BillHeader::Month {
                label: "January 1970".to_owned(),
            },
            session_count: 2,
            sessions: vec![
                RawSession {
                    id: "ses_first".to_owned(),
                    agent: None,
                },
                RawSession {
                    id: "ses_second".to_owned(),
                    agent: None,
                },
            ],
            messages: Vec::new(),
            parts: Vec::new(),
        };

        let world = parse_system(raw, &PipelineProgress::default())?;

        assert_eq!(world.sessions.len(), 2);
        assert_eq!(
            world.sessions.keys().copied().collect::<Vec<_>>(),
            session_handles(&["ses_first", "ses_second"])
        );
        Ok(())
    }

    /// Handles a fresh interner gives the listed ids, in that order.
    ///
    /// A fresh interner is deterministic: both here and in `parse_system`
    /// the same first-interned ids receive the same handle values.
    fn session_handles(ids: &[&str]) -> Vec<crate::data::SessionId> {
        let mut interner = Interner::default();

        ids.iter().map(|id| interner.intern_session(id)).collect()
    }

    #[test]
    fn interns_fallback_agents_in_message_order() -> Result<(), Box<dyn std::error::Error>> {
        let raw = RawLoad {
            header: BillHeader::Month {
                label: "January 1970".to_owned(),
            },
            session_count: 2,
            sessions: vec![
                RawSession {
                    id: "ses_first".to_owned(),
                    agent: None,
                },
                RawSession {
                    id: "ses_second".to_owned(),
                    agent: None,
                },
            ],
            messages: vec![
                RawMessage {
                    id: "msg_1".to_owned(),
                    session_id: "ses_first".to_owned(),
                    location: "fixture".to_owned(),
                    json: assistant_json("agent_beta"),
                },
                RawMessage {
                    id: "msg_2".to_owned(),
                    session_id: "ses_second".to_owned(),
                    location: "fixture".to_owned(),
                    json: assistant_json("agent_alpha"),
                },
            ],
            parts: Vec::new(),
        };

        let world = parse_system(raw, &PipelineProgress::default())?;

        // No session declares an agent, so the message-level fallbacks are
        // interned in message order: agent_beta gets handle 0, agent_alpha
        // handle 1 — exactly as the old sequential parser produced.
        let rows = world.entities.rows().collect::<Vec<_>>();

        assert_eq!(world.interner.resolve_agent(rows[0].agent), "agent_beta");
        assert_eq!(world.interner.resolve_agent(rows[1].agent), "agent_alpha");
        // Interned first = lower handle: message order, not name order.
        assert!(rows[0].agent < rows[1].agent);
        Ok(())
    }

    /// Assistant message JSON whose agent comes only from the message.
    fn assistant_json(agent: &str) -> String {
        format!(
            r#"{{"role":"assistant","agent":"{agent}","providerID":"prov","modelID":"model","cost":0.1,"time":{{"created":1000,"completed":2000}},"tokens":{{"input":1,"output":1,"reasoning":0,"cache":{{"read":0,"write":0}}}}}}"#,
        )
    }
}
