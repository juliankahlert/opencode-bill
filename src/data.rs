//! Plain records that flow through bill generation.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::AddAssign;

use crate::timing::{earliest, latest};

/// (provider, model) identity of one model row.
pub(crate) type ModelKey = (String, String);

/// Token counters reported by one assistant call.
///
/// Sums saturate at `u64::MAX` instead of wrapping or panicking; extreme
/// usage cannot produce a wrong (or crashed) bill.
#[derive(Clone, Copy, Default)]
pub(crate) struct Tokens {
    /// Input tokens.
    pub(crate) input: u64,
    /// Output tokens.
    pub(crate) output: u64,
    /// Reasoning tokens.
    pub(crate) reasoning: u64,
    /// Cached-input tokens.
    pub(crate) cache_read: u64,
    /// Cache-write tokens.
    pub(crate) cache_write: u64,
}

impl AddAssign<&Tokens> for Tokens {
    fn add_assign(&mut self, other: &Tokens) {
        self.input = self.input.saturating_add(other.input);
        self.output = self.output.saturating_add(other.output);
        self.reasoning = self.reasoning.saturating_add(other.reasoning);
        self.cache_read = self.cache_read.saturating_add(other.cache_read);
        self.cache_write = self.cache_write.saturating_add(other.cache_write);
    }
}

/// One assistant (model) call in a session tree.
#[derive(Clone)]
pub(crate) struct UsageRecord {
    /// Session that produced the call.
    pub(crate) session_id: String,
    /// Agent class of the session or message.
    pub(crate) agent: String,
    /// Model provider.
    pub(crate) provider: String,
    /// Model identifier.
    pub(crate) model: String,
    /// Token usage of the call.
    pub(crate) tokens: Tokens,
    /// Cost of the call in USD.
    pub(crate) cost: f64,
    /// Time the model endpoint was busy, in milliseconds.
    pub(crate) api_ms: u64,
    /// Message creation timestamp (Unix millis).
    pub(crate) created: Option<u64>,
    /// Message completion timestamp (Unix millis).
    pub(crate) completed: Option<u64>,
}

/// Accumulated usage of one model row or agent class.
///
/// Token and API-time sums saturate at `u64::MAX`; wall bounds widen in the
/// message-order-independent way used across every fold in the crate.
#[derive(Clone, Copy, Default)]
pub(crate) struct Totals {
    /// Number of assistant calls.
    pub(crate) requests: u64,
    /// Summed token usage.
    pub(crate) tokens: Tokens,
    /// Summed cost in USD.
    pub(crate) cost: f64,
    /// Summed endpoint-busy time in milliseconds.
    pub(crate) api_ms: u64,
    /// Earliest wall-clock start (Unix millis).
    pub(crate) wall_start: Option<u64>,
    /// Latest wall-clock end (Unix millis).
    pub(crate) wall_end: Option<u64>,
}

impl Totals {
    /// Folds one assistant call into this running total.
    pub(crate) fn accumulate(&mut self, record: &UsageRecord) {
        self.requests += 1;
        self.tokens += &record.tokens;
        self.cost += record.cost;
        self.api_ms = self.api_ms.saturating_add(record.api_ms);
        self.wall_start = earliest(self.wall_start, record.created);
        self.wall_end = latest(self.wall_end, record.completed.or(record.created));
    }

    /// Merges another running total into this one.
    pub(crate) fn merge(&mut self, other: &Totals) {
        self.requests += other.requests;
        self.tokens += &other.tokens;
        self.cost += other.cost;
        self.api_ms = self.api_ms.saturating_add(other.api_ms);
        self.wall_start = earliest(self.wall_start, other.wall_start);
        self.wall_end = latest(self.wall_end, other.wall_end);
    }
}

/// Per-agent-class totals plus the sessions that produced them.
#[derive(Default)]
pub(crate) struct AgentUsage {
    /// Session IDs attributed to the agent class.
    pub(crate) sessions: BTreeSet<String>,
    /// Summed usage of the agent class.
    pub(crate) totals: Totals,
}

/// Model pricing per million tokens.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct ModelRates {
    /// Input token rate in USD per million.
    pub(crate) input: Option<f64>,
    /// Cached-input token rate in USD per million.
    pub(crate) cache_read: Option<f64>,
    /// Cache-write token rate in USD per million.
    pub(crate) cache_write: Option<f64>,
    /// Output token rate in USD per million.
    pub(crate) output: Option<f64>,
}

/// Millisecond spans of the tool calls made from one assistant message.
pub(crate) type ToolSpans = BTreeMap<String, Vec<(u64, u64)>>;
