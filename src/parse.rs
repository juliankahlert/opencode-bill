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
use crate::store::{RawLoad, RawMessage, RawPart, RawWindowMeta};
use crate::timing::{earliest, latest, message_api_ms};

/// Parses a JSON string, tagging failures with its location.
pub(crate) fn parse_json(json: &str, location: &str) -> Result<Value, BillError> {
    serde_json::from_str(json).map_err(|failure| BillError::InvalidData {
        location: location.to_owned(),
        detail: failure.to_string(),
    })
}

/// The internable fields of one message, in a fixed extraction order.
///
/// The stores extract these fields (SQLite via `json_extract`, legacy from
/// its parsed file JSON) so the raw payload — which can be hundreds of
/// megabytes when a tool result is embedded — never crosses into the
/// pipeline.  The fields travel as a positional JSON array text.
#[allow(non_snake_case)]
struct MessageFields {
    role: Option<String>,
    providerID: Option<String>,
    modelID: Option<String>,
    cost: Option<f64>,
    time_created: Option<i64>,
    time_completed: Option<i64>,
    tokens_input: Option<i64>,
    tokens_output: Option<i64>,
    tokens_reasoning: Option<i64>,
    cache_read: Option<i64>,
    cache_write: Option<i64>,
    agent: Option<String>,
    mode: Option<String>,
}

impl MessageFields {
    /// Builds the fields from the positional extraction array.
    fn from_array(values: &[Option<serde_json::Value>]) -> MessageFields {
        MessageFields {
            role: string_at(values, 0),
            providerID: string_at(values, 1),
            modelID: string_at(values, 2),
            cost: values
                .get(3)
                .and_then(|value| value.as_ref().and_then(Value::as_f64)),
            time_created: int_at(values, 4),
            time_completed: int_at(values, 5),
            tokens_input: int_at(values, 6),
            tokens_output: int_at(values, 7),
            tokens_reasoning: int_at(values, 8),
            cache_read: int_at(values, 9),
            cache_write: int_at(values, 10),
            agent: string_at(values, 11),
            mode: string_at(values, 12),
        }
    }
}

/// One tool part's extraction: type plus the busy span bounds.
struct PartFields {
    r#type: Option<String>,
    start: Option<i64>,
    end: Option<i64>,
}

impl PartFields {
    /// Builds the fields from the positional extraction array.
    fn from_array(values: &[Option<serde_json::Value>]) -> PartFields {
        PartFields {
            r#type: string_at(values, 0),
            start: int_at(values, 1),
            end: int_at(values, 2),
        }
    }
}

/// A string field of the positional extraction array.
fn string_at(values: &[Option<serde_json::Value>], index: usize) -> Option<String> {
    values
        .get(index)
        .and_then(|value| value.as_ref().and_then(Value::as_str))
        .map(str::to_owned)
}

/// An integer field of the positional extraction array.
fn int_at(values: &[Option<serde_json::Value>], index: usize) -> Option<i64> {
    values
        .get(index)
        .and_then(|value| value.as_ref().and_then(Value::as_i64))
}

/// Parses a message's extracted fields text.
fn parse_message_fields(json: &str, location: &str) -> Result<MessageFields, BillError> {
    let values = serde_json::from_str::<Vec<Option<Value>>>(json).map_err(|failure| {
        BillError::InvalidData {
            location: location.to_owned(),
            detail: failure.to_string(),
        }
    })?;

    Ok(MessageFields::from_array(&values))
}

/// Streaming envelope for oversized payloads.
///
/// Messages over the extraction threshold are parsed here instead of by
/// `json_extract`: serde skips the unknown fields (the giant tool result)
/// without building them, so a hundreds-of-megabytes payload costs a linear
/// scan instead of SQLite's internal JSON tree.
#[allow(non_snake_case)]
#[derive(serde::Deserialize)]
struct GiantEnvelope {
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    providerID: Option<String>,
    #[serde(default)]
    modelID: Option<String>,
    #[serde(default)]
    cost: Option<f64>,
    #[serde(default)]
    time: Option<GiantTime>,
    #[serde(default)]
    tokens: Option<GiantTokens>,
    #[serde(default)]
    agent: Option<String>,
    #[serde(default)]
    mode: Option<String>,
}

#[derive(serde::Deserialize)]
struct GiantTime {
    #[serde(default)]
    created: Option<i64>,
    #[serde(default)]
    completed: Option<i64>,
}

#[derive(serde::Deserialize)]
struct GiantTokens {
    #[serde(default)]
    input: Option<i64>,
    #[serde(default)]
    output: Option<i64>,
    #[serde(default)]
    reasoning: Option<i64>,
    #[serde(default)]
    cache: Option<GiantCache>,
}

#[derive(serde::Deserialize)]
struct GiantCache {
    #[serde(default)]
    read: Option<i64>,
    #[serde(default)]
    write: Option<i64>,
}

/// Extracts one oversized message's internable fields, as the positional
/// array text the pipeline parses.
pub(crate) fn extract_giant_message_fields(
    json: &str,
    location: &str,
) -> Result<String, BillError> {
    let envelope: GiantEnvelope =
        serde_json::from_str(json).map_err(|failure| BillError::InvalidData {
            location: location.to_owned(),
            detail: failure.to_string(),
        })?;
    let time = envelope.time.as_ref();
    let tokens = envelope.tokens.as_ref();
    let cache = tokens.and_then(|tokens| tokens.cache.as_ref());

    Ok(serde_json::json!([
        envelope.role.as_deref(),
        envelope.providerID.as_deref(),
        envelope.modelID.as_deref(),
        envelope.cost,
        time.and_then(|time| time.created),
        time.and_then(|time| time.completed),
        tokens.and_then(|tokens| tokens.input),
        tokens.and_then(|tokens| tokens.output),
        tokens.and_then(|tokens| tokens.reasoning),
        cache.and_then(|cache| cache.read),
        cache.and_then(|cache| cache.write),
        envelope.agent.as_deref(),
        envelope.mode.as_deref(),
    ])
    .to_string())
}

/// Extracts one message's internable fields from its parsed JSON, as the
/// positional array text the pipeline parses.
///
/// The legacy store extracts here (its files are small); the SQLite store
/// extracts with `json_extract` in the same fixed order.
pub(crate) fn extract_message_fields(value: &Value) -> String {
    serde_json::json!([
        value.get("role").and_then(Value::as_str),
        value.get("providerID").and_then(Value::as_str),
        value.get("modelID").and_then(Value::as_str),
        value.get("cost").and_then(Value::as_f64),
        value
            .get("time")
            .and_then(|time| time.get("created"))
            .and_then(Value::as_i64),
        value
            .get("time")
            .and_then(|time| time.get("completed"))
            .and_then(Value::as_i64),
        value
            .get("tokens")
            .and_then(|tokens| tokens.get("input"))
            .and_then(Value::as_i64),
        value
            .get("tokens")
            .and_then(|tokens| tokens.get("output"))
            .and_then(Value::as_i64),
        value
            .get("tokens")
            .and_then(|tokens| tokens.get("reasoning"))
            .and_then(Value::as_i64),
        value
            .get("tokens")
            .and_then(|tokens| tokens.get("cache"))
            .and_then(|cache| cache.get("read"))
            .and_then(Value::as_i64),
        value
            .get("tokens")
            .and_then(|tokens| tokens.get("cache"))
            .and_then(|cache| cache.get("write"))
            .and_then(Value::as_i64),
        value.get("agent").and_then(Value::as_str),
        value.get("mode").and_then(Value::as_str),
    ])
    .to_string()
}

/// Extracts one tool part's span fields from its parsed JSON, as the
/// positional array text the pipeline parses.
pub(crate) fn extract_part_fields(value: &Value) -> String {
    serde_json::json!([
        value.get("type").and_then(Value::as_str),
        value
            .get("state")
            .and_then(|state| state.get("time"))
            .and_then(|time| time.get("start"))
            .and_then(Value::as_i64),
        value
            .get("state")
            .and_then(|state| state.get("time"))
            .and_then(|time| time.get("end"))
            .and_then(Value::as_i64),
    ])
    .to_string()
}

/// Parses a tool part's extracted fields text.
fn parse_part_fields(json: &str, location: &str) -> Result<PartFields, BillError> {
    let values = serde_json::from_str::<Vec<Option<Value>>>(json).map_err(|failure| {
        BillError::InvalidData {
            location: location.to_owned(),
            detail: failure.to_string(),
        }
    })?;

    Ok(PartFields::from_array(&values))
}

/// A timestamp as unsigned millis, `None` when negative or missing.
fn non_negative(value: Option<i64>) -> Option<u64> {
    value.and_then(|value| u64::try_from(value).ok())
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

/// One parsed message: its wall-clock bounds plus an internable usage row.
///
/// Parallel workers parse JSON and extract raw strings; interning happens
/// later in one ordered pass, so handle assignment — and with it the
/// byte-identical bill output — stays deterministic.
pub(crate) struct ParsedMessage {
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
    let fields = parse_message_fields(&message.json, &message.location)?;
    let (created, completed) = (
        non_negative(fields.time_created),
        non_negative(fields.time_completed),
    );
    let session_agent = session_agents.get(&message.session_id).copied().flatten();

    let row = if fields.role.as_deref() == Some("assistant") {
        Some(parse_assistant_row(
            &fields,
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
    fields: &MessageFields,
    location: &str,
    session_agent: Option<AgentId>,
    tools: &[(u64, u64)],
) -> Result<ParsedRow, BillError> {
    let provider = required_envelope_string(&fields.providerID, location, "providerID")?;
    let model = required_envelope_string(&fields.modelID, location, "modelID")?;
    let usage = Tokens {
        input: required_envelope_u64(fields.tokens_input, location, "tokens.input")?,
        output: required_envelope_u64(fields.tokens_output, location, "tokens.output")?,
        reasoning: required_envelope_u64(fields.tokens_reasoning, location, "tokens.reasoning")?,
        cache_read: required_envelope_u64(fields.cache_read, location, "tokens.cache.read")?,
        cache_write: required_envelope_u64(fields.cache_write, location, "tokens.cache.write")?,
    };
    let message_agent = if session_agent.is_some() {
        None
    } else {
        Some(
            fields
                .agent
                .as_deref()
                .or(fields.mode.as_deref())
                .filter(|agent| !agent.is_empty())
                .unwrap_or("unknown")
                .to_owned(),
        )
    };
    let (created, completed) = (
        non_negative(fields.time_created),
        non_negative(fields.time_completed),
    );
    let cost = fields.cost.ok_or_else(|| invalid_field(location, "cost"))?;

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

/// Reads a required non-empty typed string field.
fn required_envelope_string(
    field: &Option<String>,
    location: &str,
    name: &str,
) -> Result<String, BillError> {
    field
        .as_deref()
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| invalid_field(location, name))
}

/// Reads a required typed integer field, clamping negative corrections to
/// zero.
fn required_envelope_u64(field: Option<i64>, location: &str, name: &str) -> Result<u64, BillError> {
    field
        .map(|count| u64::try_from(count.max(0)).unwrap_or(0))
        .ok_or_else(|| invalid_field(location, name))
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
/// order of agent/provider/model handles — and with it the byte-identical
/// bill output — stays exactly as before.  Session handles intern in session
/// row order; no component renders session handle order.
///
/// The pipeline shares its machinery with the streaming year load: the
/// window metadata parses first ([`parse_metadata`]), then the messages
/// parse in batches ([`parse_message_batch`]).  Message parsing is the
/// parallel work unit: a rayon `par_iter` decodes every message JSON in
/// parallel and extracts raw strings, while the merge pass interns in
/// message order.  Interning stays single-threaded and ordered, so handle
/// assignment is deterministic regardless of thread scheduling.  The first
/// failing message in store order stops the pipeline.
pub(crate) fn parse_system(raw: RawLoad, progress: &PipelineProgress) -> Result<World, BillError> {
    let RawLoad {
        header,
        session_count,
        sessions,
        messages,
        parts,
    } = raw;
    let meta = RawWindowMeta {
        header,
        session_count,
        sessions,
    };
    let (mut world, parse) = parse_metadata(meta, progress)?;
    let mut messages = messages;
    let mut parts = parts;
    let mut results = Vec::new();
    let mut rows = Vec::new();

    parse_message_batch(
        &mut world,
        &parse,
        &mut parts,
        &mut messages,
        &mut results,
        &mut rows,
        progress,
    )?;

    for row in rows.drain(..) {
        world.entities.push(row);
    }

    Ok(world)
}

/// Window-wide parse state shared by every message batch of one window.
///
/// Session agents are window metadata, built once by [`parse_metadata`];
/// tool spans stay batch-local: a day's parts parse with its messages, and
/// the spans map never grows with the window.
pub(crate) struct ParseContext {
    /// Declared agent of every session, for message rows that need it.
    session_agents: BTreeMap<String, Option<AgentId>>,
}

/// Parses one window's metadata (sessions) into a world with an empty entity
/// store, plus the shared batch parse context.
///
/// Session handles intern here, in session row order; message handles intern
/// later, batch by batch, in message order.
pub(crate) fn parse_metadata(
    meta: RawWindowMeta,
    progress: &PipelineProgress,
) -> Result<(World, ParseContext), BillError> {
    progress.note_phase(1);
    let mut interner = Interner::default();
    let mut sessions = SessionTree::new();
    let mut session_agents = BTreeMap::new();

    for session in meta.sessions {
        progress.parsed_row();
        let id = interner.intern_session(&session.id);
        let agent = session
            .agent
            .filter(|agent| !agent.is_empty())
            .map(|agent| interner.intern_agent(&agent));

        sessions.insert(id, SessionNode { agent });
        session_agents.entry(session.id).or_insert(agent);
    }

    Ok((
        World {
            header: meta.header,
            session_count: meta.session_count,
            sessions,
            entities: UsageStore::default(),
            wall_start: None,
            wall_end: None,
            interner,
        },
        ParseContext { session_agents },
    ))
}

/// Parses one batch of raw tool parts and messages into usage rows, in
/// batch order.
///
/// The day's tool parts parse first into a batch-local span map — every
/// message only ever looks up its own parts, so the map never outlives the
/// batch and the window's span memory stays flat.  The rayon `par_iter`
/// decodes the batch's JSON in parallel while the merge pass interns in
/// message order; batches arrive in window order, so handle assignment stays
/// deterministic across the whole window.  The part, message, decode-result,
/// and row buffers belong to the caller and are reused for every batch of
/// the stream — the message, decode-result, and row containers are reused
/// (the parts container is consumed by the span parse).  The rows are
/// drained by the caller (the session pipeline pushes them into the entity
/// store, the streaming pipeline folds them into the aggregates and drops
/// them).  Wall bounds cover every message, so a timestamp-less user
/// message still sets the earliest bound.
pub(crate) fn parse_message_batch(
    world: &mut World,
    context: &ParseContext,
    parts: &mut Vec<RawPart>,
    messages: &mut Vec<RawMessage>,
    results: &mut Vec<Result<ParsedMessage, BillError>>,
    rows: &mut Vec<UsageRow>,
    progress: &PipelineProgress,
) -> Result<(), BillError> {
    let tools = parse_tool_spans(std::mem::take(parts), progress)?;

    results.clear();
    messages
        .par_iter()
        .map(|message| {
            progress.parsed_row();

            parse_message(message, &context.session_agents, &tools)
        })
        .collect_into_vec(results);

    rows.clear();
    for result in results.drain(..) {
        let message = result?;
        let session_handle = world.interner.intern_session(&message.session_id);

        world.wall_start = earliest(world.wall_start, message.created);
        world.wall_end = latest(world.wall_end, message.completed.or(message.created));

        let Some(row) = message.row else {
            continue;
        };
        let agent = match row.session_agent {
            Some(agent) => agent,
            None => world
                .interner
                .intern_agent(row.message_agent.as_deref().unwrap_or("unknown")),
        };

        rows.push(UsageRow {
            session_id: session_handle,
            agent,
            provider: world.interner.intern_provider(&row.provider),
            model: world.interner.intern_model(&row.model),
            tokens: row.tokens,
            cost_micros: row.cost_micros,
            cost_unknown: row.cost_unknown,
            api_ms: row.api_ms,
            created: row.created,
            completed: row.completed,
        });
    }

    Ok(())
}

/// Groups the raw tool part rows into per-message busy spans.
fn parse_tool_spans(
    parts: Vec<RawPart>,
    progress: &PipelineProgress,
) -> Result<ToolSpans, BillError> {
    let mut spans = ToolSpans::new();

    for part in parts {
        progress.parsed_row();
        let fields = parse_part_fields(&part.json, &part.location)?;

        if let Some(span) = part_span(&fields) {
            spans.entry(part.message_id).or_default().push(span);
        }
    }

    Ok(spans)
}

/// Returns the busy span of an extracted tool part, if it ran to completion.
fn part_span(fields: &PartFields) -> Option<(u64, u64)> {
    if fields.r#type.as_deref() != Some("tool") {
        return None;
    }

    let start = non_negative(fields.start)?;
    let end = non_negative(fields.end)?;

    (end > start).then_some((start, end))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::BillHeader;
    use crate::store::{RawLoad, RawMessage, RawPart, RawSession, RawWindowMeta};

    /// Raw rows of one session: two assistant calls plus a user message.
    fn raw_fixture() -> RawLoad {
        RawLoad {
            header: BillHeader::Session {
                id: "ses_abc123456".to_owned(),
                title: "Test session".to_owned(),
                created_ms: None,
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
                    json: r#"["assistant","openai","gpt-test",0.125,1000,3000,10,4,2,6,1,"build","primary"]"#
                        .to_owned(),
                },
                RawMessage {
                    id: "msg_2".to_owned(),
                    session_id: "ses_abc123456".to_owned(),
                    location: "fixture".to_owned(),
                    json: r#"["assistant","anthropic","claude-test",0.5,2000,2500,30,8,1,2,0,null,null]"#
                        .to_owned(),
                },
                RawMessage {
                    id: "msg_user".to_owned(),
                    session_id: "ses_abc123456".to_owned(),
                    location: "fixture".to_owned(),
                    json: r#"["user",null,null,null,500,null,null,null,null,null,null,null,null]"#.to_owned(),
                },
            ],
            parts: vec![RawPart {
                message_id: "msg_1".to_owned(),
                location: "fixture".to_owned(),
                json: r#"["tool",1000,2500]"#
                    .to_owned(),
            }],
        }
    }

    #[test]
    fn batched_parsing_matches_single_pass() -> Result<(), Box<dyn std::error::Error>> {
        let single = parse_system(raw_fixture(), &PipelineProgress::default())?;
        let raw = raw_fixture();
        let RawLoad {
            header,
            session_count,
            sessions,
            messages,
            parts,
        } = raw;
        let meta = RawWindowMeta {
            header,
            session_count,
            sessions,
        };
        let (mut world, parse) = parse_metadata(meta, &PipelineProgress::default())?;

        // Stream the parts and messages in two batches: the first two rows,
        // then the rest — the interning order must match the single pass
        // exactly.  The part, decode, and row buffers are reused across both
        // batches, as the streaming pipeline does.
        let mut messages = messages.into_iter();
        let mut first_parts = parts;
        let mut first = messages.by_ref().take(2).collect::<Vec<_>>();
        let mut results = Vec::new();
        let mut rows = Vec::new();

        parse_message_batch(
            &mut world,
            &parse,
            &mut first_parts,
            &mut first,
            &mut results,
            &mut rows,
            &PipelineProgress::default(),
        )?;
        for row in rows.drain(..) {
            world.entities.push(row);
        }

        let mut second_parts = Vec::new();
        let mut second = messages.collect::<Vec<_>>();
        parse_message_batch(
            &mut world,
            &parse,
            &mut second_parts,
            &mut second,
            &mut results,
            &mut rows,
            &PipelineProgress::default(),
        )?;
        for row in rows.drain(..) {
            world.entities.push(row);
        }

        assert_eq!(world.session_count, single.session_count);
        assert_eq!(world.entities.len(), single.entities.len());
        assert_eq!(world.wall_start, single.wall_start);
        assert_eq!(world.wall_end, single.wall_end);

        for (batched, whole) in world.entities.rows().zip(single.entities.rows()) {
            // Handles are equal only when interning happened in the same
            // order: the batch stream must assign identical ids.
            assert_eq!(batched.session_id, whole.session_id);
            assert_eq!(batched.agent, whole.agent);
            assert_eq!(batched.provider, whole.provider);
            assert_eq!(batched.model, whole.model);
            assert_eq!(batched.tokens.input, whole.tokens.input);
            assert_eq!(batched.tokens.output, whole.tokens.output);
            assert_eq!(batched.tokens.reasoning, whole.tokens.reasoning);
            assert_eq!(batched.tokens.cache_read, whole.tokens.cache_read);
            assert_eq!(batched.tokens.cache_write, whole.tokens.cache_write);
            assert_eq!(batched.cost_micros, whole.cost_micros);
            assert_eq!(batched.cost_unknown, whole.cost_unknown);
            assert_eq!(batched.api_ms, whole.api_ms);
            assert_eq!(batched.created, whole.created);
            assert_eq!(batched.completed, whole.completed);
        }

        Ok(())
    }

    #[test]
    fn parse_system_builds_world_from_raw_rows() -> Result<(), Box<dyn std::error::Error>> {
        let raw = RawLoad {
            header: BillHeader::Session {
                id: "ses_abc123456".to_owned(),
                title: "Test session".to_owned(),
                created_ms: None,
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
                    json: r#"["assistant","openai","gpt-test",0.125,1000,3000,10,4,2,6,1,"build","primary"]"#
                        .to_owned(),
                },
                RawMessage {
                    id: "msg_user".to_owned(),
                    session_id: "ses_abc123456".to_owned(),
                    location: "fixture".to_owned(),
                    json: r#"["user",null,null,null,500,null,null,null,null,null,null,null,null]"#.to_owned(),
                },
            ],
            parts: vec![RawPart {
                message_id: "msg_1".to_owned(),
                location: "fixture".to_owned(),
                json: r#"["tool",1000,2500]"#
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

    /// Assistant message fields whose agent comes only from the message.
    fn assistant_json(agent: &str) -> String {
        format!(r#"["assistant","prov","model",0.1,1000,2000,1,1,0,0,0,"{agent}",null]"#,)
    }
}
