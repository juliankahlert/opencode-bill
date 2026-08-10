//! Plain records that flow through bill generation.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::AddAssign;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rusty_money::{FormattableCurrency, Locale, Money};

use crate::report::BillHeader;
use crate::timing::{earliest, latest};

/// Live counters of one bill pipeline run, shared with an observing UI.
///
/// The pipeline writes the atomics as its parallel workers progress; a UI
/// reads them from another task while the pipeline runs, then drops its
/// handle.  Pure external observation, matching the data-oriented design:
/// systems never read the counters to decide anything, and no lock guards
/// them.  Every counter counts storage rows: the load phase counts rows as
/// the store reads them, the parse phase counts the same universe as it
/// processes it, and the aggregate phase counts rows folded per fold.
#[derive(Clone, Default)]
pub struct BillProgress {
    /// Rows read from storage (sessions, tool parts, messages).
    loaded: Arc<AtomicU64>,
    /// Rows processed by the parse phase (sessions, tool parts, messages).
    parsed: Arc<AtomicU64>,
    /// Rows folded during aggregation, counted in both aggregate folds.
    folded: Arc<AtomicU64>,
}

impl BillProgress {
    /// Storage rows read so far by the running pipeline.
    pub fn loaded_rows(&self) -> u64 {
        self.loaded.load(Ordering::Relaxed)
    }

    /// Storage rows processed by the parse phase so far.
    pub fn parsed_rows(&self) -> u64 {
        self.parsed.load(Ordering::Relaxed)
    }

    /// Rows folded during aggregation so far.
    pub fn folded_rows(&self) -> u64 {
        self.folded.load(Ordering::Relaxed)
    }

    /// Counts one storage row read by a store.
    pub(crate) fn loaded_row(&self) {
        self.loaded.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts one storage row processed by the parse phase.
    pub(crate) fn parsed_row(&self) {
        self.parsed.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts one folded row from an aggregate worker.
    pub(crate) fn folded_row(&self) {
        self.folded.fetch_add(1, Ordering::Relaxed);
    }
}

/// Handle of one session, interned once per bill input.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SessionId(u32);

/// Handle of one agent class, interned once per bill input.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct AgentId(u32);

/// Handle of one provider, interned once per bill input.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ProviderId(u32);

/// Handle of one model, interned once per bill input.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ModelId(u32);

/// (provider, model) identity of one model row, as interned handles.
pub(crate) type ModelKey = (ProviderId, ModelId);

/// Identity of one message entity: its lane index and generation.
///
/// Every assistant call of the billed period is an entity; the components of
/// entity `n` are the lanes at index `n`.  Ids are consecutive from zero.
/// The generation validates that a handle still names its original row: a
/// removed-and-replaced entity keeps a fresh generation, so stale handles
/// fail [`UsageStore::validate`] instead of aliasing the moved row.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct Entity {
    id: u32,
    generation: u32,
}

impl Entity {
    /// Builds an entity handle for lane `id` of the given generation.
    pub(crate) fn new(id: u32, generation: u32) -> Self {
        Entity { id, generation }
    }

    /// The lane index of this entity in its store.
    pub(crate) fn id(self) -> u32 {
        self.id
    }

    /// The generation this handle must match to stay valid.
    pub(crate) fn generation(self) -> u32 {
        self.generation
    }
}

/// One session of the billed session tree.
///
/// A session entity carries its agent class component, when declared.  The
/// session tree itself is resolved by the stores during load.
#[derive(Clone, Copy)]
pub(crate) struct SessionNode {
    /// Agent class declared by the session, when known.
    pub(crate) agent: Option<AgentId>,
}

/// The billed session tree: every session keyed by id, with its components.
pub(crate) type SessionTree = BTreeMap<SessionId, SessionNode>;

/// The ECS world: entity storage, component arrays, and the interner.
///
/// Every assistant call of the billed period is one message entity stored in
/// [`World::entities`]; its components (session, agent, provider, model,
/// tokens, cost, api time, wall bounds) are the [`UsageStore`] lanes indexed
/// by [`Entity`].  Sessions form a tree; stores resolve descendant
/// membership during load.  Systems are free functions over this world; no
/// struct method hides mutation.
pub(crate) struct World {
    /// Identifies the billed period (session tree, calendar month, or day).
    pub(crate) header: BillHeader,
    /// Number of sessions that produced the billed usage.
    pub(crate) session_count: usize,
    /// The session tree, as session entities with their components.
    pub(crate) sessions: SessionTree,
    /// Every assistant call in the billed period, as message entities.
    pub(crate) entities: UsageStore,
    /// Earliest message timestamp (Unix millis).
    pub(crate) wall_start: Option<u64>,
    /// Latest message timestamp (Unix millis).
    pub(crate) wall_end: Option<u64>,
    /// Interning tables that own every handle in this world.
    pub(crate) interner: Interner,
}

/// Interns strings into stable per-kind handles, once per bill world.
///
/// Every handle kind resolves into its own table, so a `SessionId` can never
/// resolve into an agent name.  One instance lives per `World`; dropping
/// the world frees every table.
#[derive(Default)]
pub(crate) struct Interner {
    sessions: Vec<String>,
    agents: Vec<String>,
    providers: Vec<String>,
    models: Vec<String>,
    session_ids: HashMap<String, u32>,
    agent_ids: HashMap<String, u32>,
    provider_ids: HashMap<String, u32>,
    model_ids: HashMap<String, u32>,
}

impl Interner {
    /// Returns the existing handle for a session id, interning it on first use.
    pub(crate) fn intern_session(&mut self, text: &str) -> SessionId {
        SessionId(intern(&mut self.session_ids, &mut self.sessions, text))
    }

    /// Returns the existing handle for an agent class, interning on first use.
    pub(crate) fn intern_agent(&mut self, text: &str) -> AgentId {
        AgentId(intern(&mut self.agent_ids, &mut self.agents, text))
    }

    /// Returns the existing handle for a provider, interning on first use.
    pub(crate) fn intern_provider(&mut self, text: &str) -> ProviderId {
        ProviderId(intern(&mut self.provider_ids, &mut self.providers, text))
    }

    /// Returns the existing handle for a model, interning on first use.
    pub(crate) fn intern_model(&mut self, text: &str) -> ModelId {
        ModelId(intern(&mut self.model_ids, &mut self.models, text))
    }

    /// Resolves an agent handle back to its string.
    pub(crate) fn resolve_agent(&self, id: AgentId) -> &str {
        self.agents
            .get(id.0 as usize)
            .map(String::as_str)
            .unwrap_or("<unknown agent>")
    }

    /// Resolves a provider handle back to its string.
    pub(crate) fn resolve_provider(&self, id: ProviderId) -> &str {
        self.providers
            .get(id.0 as usize)
            .map(String::as_str)
            .unwrap_or("<unknown provider>")
    }

    /// Resolves a model handle back to its string.
    pub(crate) fn resolve_model(&self, id: ModelId) -> &str {
        self.models
            .get(id.0 as usize)
            .map(String::as_str)
            .unwrap_or("<unknown model>")
    }
}

/// Shared interning core: existing id, or the next free slot.
fn intern(ids: &mut HashMap<String, u32>, table: &mut Vec<String>, text: &str) -> u32 {
    if let Some(&id) = ids.get(text) {
        return id;
    }

    let id = table.len() as u32;

    table.push(text.to_owned());
    ids.insert(text.to_owned(), id);
    id
}

/// One micro-dollar (`1/1_000_000` of one USD), the deterministic cost unit.
pub(crate) const MICRO_PER_DOLLAR: i64 = 1_000_000;

/// Currency whose minor unit is one micro-dollar.
///
/// The ISO `USD` currency counts cents (exponent 2); the storage lanes and
/// the report both count micro-dollars (exponent 6), so every cost
/// calculation uses this currency and `rusty_money`'s checked arithmetic
/// instead of raw `i64` adds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MicroUsd;

impl FormattableCurrency for MicroUsd {
    fn to_string(&self) -> String {
        "USDM".to_owned()
    }

    fn exponent(&self) -> u32 {
        6
    }

    fn code(&self) -> &'static str {
        "USDM"
    }

    fn locale(&self) -> Locale {
        Locale::EnUs
    }

    fn symbol(&self) -> &'static str {
        ""
    }

    fn symbol_first(&self) -> bool {
        true
    }
}

/// The single micro-dollar currency instance.
pub(crate) const MICRO_USD: MicroUsd = MicroUsd;

/// A cost amount in micro-dollars, with checked `rusty_money` arithmetic.
pub(crate) type Cost = Money<'static, MicroUsd>;

/// Wraps a micro-dollar integer as a [`Cost`].
pub(crate) fn cost_from_micros(micros: i64) -> Cost {
    Money::from_minor(micros, &MICRO_USD)
}

/// Adds two costs, saturating at the `i64` micro-dollar range.
///
/// `rusty_money`'s `Money::add` falls through to `rust_decimal`'s `Add`,
/// which panics on decimal overflow instead of returning an error, so the
/// addition runs on `Decimal::checked_add` and clamps on overflow —
/// keeping the saturating-sum contract that every fold path relies on.
pub(crate) fn cost_add(left: Cost, right: Cost) -> Cost {
    match left.amount().checked_add(*right.amount()) {
        Some(amount) => Money::from_decimal(amount, &MICRO_USD),
        None if left.is_negative() || right.is_negative() => cost_from_micros(i64::MIN),
        None => cost_from_micros(i64::MAX),
    }
}

/// Micro-dollar integer of a [`Cost`], truncated toward zero and clamped to
/// the `i64` range.
///
/// Totals may legally exceed `i64::MAX` in the decimal domain; the report
/// clamps here, mirroring the previous saturating sums.
pub(crate) fn cost_to_micros(cost: Cost) -> i64 {
    let saturated = if cost.is_negative() {
        i64::MIN
    } else {
        i64::MAX
    };
    let Some(scaled) = cost
        .amount()
        .checked_mul(Decimal::from(MICRO_PER_DOLLAR))
        .map(|scaled| scaled.trunc())
    else {
        return saturated;
    };

    if scaled >= Decimal::from(i64::MAX) {
        i64::MAX
    } else if scaled <= Decimal::from(i64::MIN) {
        i64::MIN
    } else {
        scaled.to_i64().unwrap_or(0)
    }
}

/// Converts a USD amount to micro-dollars with display-equivalent rounding.
///
/// Rounds ties to even, matching `format!("{:.6}")` on the same amount, so a
/// micro-dollar bill prints byte-identically to the old `f64` bill.
pub(crate) fn usd_to_micros(usd: f64) -> i64 {
    (usd * MICRO_PER_DOLLAR as f64).round_ties_even() as i64
}
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

/// One assistant (model) call in a session tree, as a copyable row of values.
///
/// Rows are transient views over a [`UsageStore`]; storage itself is
/// structure-of-arrays so every component lane stays cache-friendly.
#[derive(Clone, Copy)]
pub(crate) struct UsageRow {
    /// Session that produced the call.
    pub(crate) session_id: SessionId,
    /// Agent class of the session or message.
    pub(crate) agent: AgentId,
    /// Model provider.
    pub(crate) provider: ProviderId,
    /// Model identifier.
    pub(crate) model: ModelId,
    /// Token usage of the call.
    pub(crate) tokens: Tokens,
    /// Cost of the call in micro-dollars.
    pub(crate) cost_micros: i64,
    /// Whether the recorded cost is zero because no price was known.
    ///
    /// Subscription usage usually carries no per-call price; such calls can
    /// be billed at a known rate instead of showing a zero row.  Negative
    /// provider corrections also clamp to zero but are never re-priced.
    pub(crate) cost_unknown: bool,
    /// Time the model endpoint was busy, in milliseconds.
    pub(crate) api_ms: u64,
    /// Message creation timestamp (Unix millis).
    pub(crate) created: Option<u64>,
    /// Message completion timestamp (Unix millis).
    pub(crate) completed: Option<u64>,
}

/// Structure-of-arrays storage for every assistant call of one bill.
///
/// Each component is one parallel array; lane `i` of every array belongs to
/// the same call.  The parallel-array invariant (all lanes have equal length)
/// is maintained by the single `push` path and asserted in `rows`.
#[derive(Default)]
pub(crate) struct UsageStore {
    /// Session component lane.
    pub(crate) sessions: Vec<SessionId>,
    /// Agent component lane.
    pub(crate) agents: Vec<AgentId>,
    /// Provider component lane.
    pub(crate) providers: Vec<ProviderId>,
    /// Model component lane.
    pub(crate) models: Vec<ModelId>,
    /// Token component lane.
    pub(crate) tokens: Vec<Tokens>,
    /// Cost component lane, in micro-dollars.
    pub(crate) costs_micros: Vec<i64>,
    /// Unknown-cost flag lane (zero-cost calls that may be re-priced).
    pub(crate) cost_unknown: Vec<bool>,
    /// Endpoint-busy time component lane, in milliseconds.
    pub(crate) api_ms: Vec<u64>,
    /// Created timestamps of every call with one recorded.
    pub(crate) created: Vec<u64>,
    /// Presence bitmask for the created lane.
    pub(crate) created_mask: Vec<u64>,
    /// Completed timestamps of every call with one recorded.
    pub(crate) completed: Vec<u64>,
    /// Presence bitmask for the completed lane.
    pub(crate) completed_mask: Vec<u64>,
    /// Generation component lane: bumping a slot invalidates stale handles.
    pub(crate) generations: Vec<u32>,
}

impl UsageStore {
    /// Number of assistant calls stored.
    pub(crate) fn len(&self) -> usize {
        self.sessions.len()
    }

    /// Appends one call, keeping every lane aligned.
    pub(crate) fn push(&mut self, row: UsageRow) {
        self.sessions.push(row.session_id);
        self.agents.push(row.agent);
        self.providers.push(row.provider);
        self.models.push(row.model);
        self.tokens.push(row.tokens);
        self.costs_micros.push(row.cost_micros);
        self.cost_unknown.push(row.cost_unknown);
        self.api_ms.push(row.api_ms);
        self.created.push(row.created.unwrap_or(0));
        self.created_mask.push(present_bit(row.created.is_some()));
        self.completed.push(row.completed.unwrap_or(0));
        self.completed_mask
            .push(present_bit(row.completed.is_some()));
        self.generations.push(0);
    }

    /// Whether an entity handle still names its original row.
    ///
    /// `false` for out-of-range ids and for handles whose generation does
    /// not match the lane: a stale or foreign handle never aliases another
    /// row.  Every entity lookup is gated through this routine.
    pub(crate) fn validate(&self, entity: Entity) -> bool {
        self.generations
            .get(entity.id() as usize)
            .is_some_and(|generation| *generation == entity.generation())
    }

    /// Overwrites one call's cost lane slot; `false` when the entity is
    /// stale or out of range (checked access, never a panic).
    pub(crate) fn set_cost_micros(&mut self, entity: Entity, micros: i64) -> bool {
        if !self.validate(entity) {
            return false;
        }
        let Some(slot) = self.costs_micros.get_mut(entity.id() as usize) else {
            return false;
        };

        *slot = micros;
        true
    }

    /// Every stored call as a copyable row, in lane order.
    pub(crate) fn rows(&self) -> impl Iterator<Item = UsageRow> + '_ {
        debug_assert_eq!(self.agents.len(), self.len(), "agent lane length");
        debug_assert_eq!(self.providers.len(), self.len(), "provider lane length");
        debug_assert_eq!(self.models.len(), self.len(), "model lane length");
        debug_assert_eq!(self.tokens.len(), self.len(), "tokens lane length");
        debug_assert_eq!(self.costs_micros.len(), self.len(), "cost lane length");
        debug_assert_eq!(
            self.cost_unknown.len(),
            self.len(),
            "unknown-cost lane length"
        );
        debug_assert_eq!(self.api_ms.len(), self.len(), "api-time lane length");
        debug_assert_eq!(self.created.len(), self.len(), "created lane length");
        debug_assert_eq!(
            self.created_mask.len(),
            self.len(),
            "created-mask lane length"
        );
        debug_assert_eq!(self.completed.len(), self.len(), "completed lane length");
        debug_assert_eq!(
            self.completed_mask.len(),
            self.len(),
            "completed-mask lane length"
        );
        debug_assert_eq!(self.generations.len(), self.len(), "generation lane length");

        self.sessions
            .iter()
            .copied()
            .zip(self.agents.iter().copied())
            .zip(self.providers.iter().copied())
            .zip(self.models.iter().copied())
            .zip(self.tokens.iter().copied())
            .zip(self.costs_micros.iter().copied())
            .zip(self.cost_unknown.iter().copied())
            .zip(self.api_ms.iter().copied())
            .zip(self.created.iter().copied())
            .zip(self.created_mask.iter().copied())
            .zip(self.completed.iter().copied())
            .zip(self.completed_mask.iter().copied())
            .map(|row| {
                let (row, completed_mask) = row;
                let (row, completed) = row;
                let (row, created_mask) = row;
                let (row, created) = row;
                let (row, api_ms) = row;
                let (row, cost_unknown) = row;
                let (row, cost_micros) = row;
                let (row, tokens) = row;
                let (row, model) = row;
                let (row, provider) = row;
                let (session_id, agent) = row;

                UsageRow {
                    session_id,
                    agent,
                    provider,
                    model,
                    tokens,
                    cost_micros,
                    cost_unknown,
                    api_ms,
                    created: presence_bit(created_mask).then_some(created),
                    completed: presence_bit(completed_mask).then_some(completed),
                }
            })
    }

    /// Every entity of this store: its handle and the components of one call.
    ///
    /// Handles carry the lane's current generation; ids are consecutive from
    /// zero, matching the lane index.  Handles yielded here always validate
    /// against this store.
    pub(crate) fn entities(&self) -> impl Iterator<Item = (Entity, UsageRow)> + '_ {
        self.rows()
            .zip(self.generations.iter().copied())
            .enumerate()
            .map(|(entity_id, (row, generation))| (Entity::new(entity_id as u32, generation), row))
    }
}

impl FromIterator<UsageRow> for UsageStore {
    fn from_iter<T: IntoIterator<Item = UsageRow>>(iter: T) -> Self {
        let mut store = UsageStore::default();

        for row in iter {
            store.push(row);
        }

        store
    }
}

/// Whether the presence bit of one lane word is set.
fn presence_bit(mask: u64) -> bool {
    mask != 0
}

/// Presence word for one lane: bit 0 set when the timestamp is present.
fn present_bit(present: bool) -> u64 {
    u64::from(present)
}

/// Accumulated usage of one model row or agent class.
///
/// Token and API-time sums saturate at `u64::MAX`; cost sums saturate in
/// micro-dollars; wall bounds widen in the message-order-independent way used
/// across every fold in the crate.
#[derive(Clone, Copy)]
pub(crate) struct Totals {
    /// Number of assistant calls.
    pub(crate) requests: u64,
    /// Summed token usage.
    pub(crate) tokens: Tokens,
    /// Summed cost in micro-dollars.
    pub(crate) cost: Cost,
    /// Summed portion of `cost` estimated from external list prices.
    ///
    /// Only zero-cost calls with a known rate contribute; the rest of
    /// `cost` is historic per-message pricing recorded in the bill data.
    pub(crate) estimated_cost: Cost,
    /// Summed endpoint-busy time in milliseconds.
    pub(crate) api_ms: u64,
    /// Earliest wall-clock start (Unix millis).
    pub(crate) wall_start: Option<u64>,
    /// Latest wall-clock end (Unix millis).
    pub(crate) wall_end: Option<u64>,
}

impl Default for Totals {
    fn default() -> Self {
        Totals {
            requests: 0,
            tokens: Tokens::default(),
            cost: cost_from_micros(0),
            estimated_cost: cost_from_micros(0),
            api_ms: 0,
            wall_start: None,
            wall_end: None,
        }
    }
}

impl Totals {
    /// Folds one assistant call into this running total.
    pub(crate) fn accumulate(&mut self, row: &UsageRow) {
        self.requests += 1;
        self.tokens += &row.tokens;
        self.cost = cost_add(self.cost, cost_from_micros(row.cost_micros));
        if row.cost_unknown && row.cost_micros > 0 {
            self.estimated_cost = cost_add(self.estimated_cost, cost_from_micros(row.cost_micros));
        }
        self.api_ms = self.api_ms.saturating_add(row.api_ms);
        self.wall_start = earliest(self.wall_start, row.created);
        self.wall_end = latest(self.wall_end, row.completed.or(row.created));
    }

    /// Merges another running total into this one.
    pub(crate) fn merge(&mut self, other: &Totals) {
        self.requests += other.requests;
        self.tokens += &other.tokens;
        self.cost = cost_add(self.cost, other.cost);
        self.estimated_cost = cost_add(self.estimated_cost, other.estimated_cost);
        self.api_ms = self.api_ms.saturating_add(other.api_ms);
        self.wall_start = earliest(self.wall_start, other.wall_start);
        self.wall_end = latest(self.wall_end, other.wall_end);
    }
}

/// Per-agent-class totals plus the sessions that produced them.
#[derive(Default)]
pub(crate) struct AgentUsage {
    /// Session IDs attributed to the agent class.
    pub(crate) sessions: BTreeSet<SessionId>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interns_stable_handles_per_kind() {
        let mut interner = Interner::default();
        let agent = interner.intern_agent("build");
        let provider = interner.intern_provider("openai");
        let model = interner.intern_model("gpt-4o");

        assert_eq!(interner.resolve_agent(agent), "build");
        assert_eq!(interner.resolve_provider(provider), "openai");
        assert_eq!(interner.resolve_model(model), "gpt-4o");
    }

    #[test]
    fn interns_identical_strings_to_same_handle() {
        let mut interner = Interner::default();
        let first = interner.intern_provider("openai");
        let second = interner.intern_provider("openai");

        assert_eq!(first, second);
    }

    #[test]
    fn resolves_missing_handle_to_marker() {
        let interner = Interner::default();

        assert_eq!(
            interner.resolve_provider(ProviderId(0)),
            "<unknown provider>"
        );
    }

    #[test]
    fn usage_store_keeps_parallel_lanes_aligned() {
        let mut interner = Interner::default();
        let mut store = UsageStore::default();
        let row = UsageRow {
            session_id: interner.intern_session("ses_1"),
            agent: interner.intern_agent("build"),
            provider: interner.intern_provider("openai"),
            model: interner.intern_model("gpt-test"),
            tokens: Tokens::default(),
            cost_micros: 125_000,
            cost_unknown: false,
            api_ms: 0,
            created: Some(1),
            completed: None,
        };

        store.push(row);
        store.push(row);

        assert_eq!(store.len(), 2);
        assert_eq!(store.sessions.len(), 2);
        assert_eq!(store.agents.len(), 2);
        assert_eq!(store.providers.len(), 2);
        assert_eq!(store.models.len(), 2);
        assert_eq!(store.tokens.len(), 2);
        assert_eq!(store.costs_micros.len(), 2);
        assert_eq!(store.cost_unknown.len(), 2);
        assert_eq!(store.api_ms.len(), 2);
        assert_eq!(store.created.len(), 2);
        assert_eq!(store.created_mask.len(), 2);
        assert_eq!(store.completed.len(), 2);
        assert_eq!(store.completed_mask.len(), 2);
        assert_eq!(store.generations.len(), 2);

        let rows = store.rows().collect::<Vec<_>>();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].created, Some(1));
        assert_eq!(rows[1].completed, None);
        assert_eq!(rows[0].session_id, rows[1].session_id);
    }

    #[test]
    fn usage_store_yields_consecutive_entity_ids() {
        let mut interner = Interner::default();
        let mut store = UsageStore::default();
        let row = UsageRow {
            session_id: interner.intern_session("ses_1"),
            agent: interner.intern_agent("build"),
            provider: interner.intern_provider("openai"),
            model: interner.intern_model("gpt-test"),
            ..usage_row_default()
        };

        store.push(row);
        store.push(row);

        let entities = store
            .entities()
            .map(|(entity, _)| entity)
            .collect::<Vec<Entity>>();
        assert_eq!(entities, [Entity::new(0, 0), Entity::new(1, 0)]);
    }

    #[test]
    fn validate_rejects_out_of_range_entities() {
        let mut store = UsageStore::default();
        store.push(usage_row_default());

        assert!(store.validate(Entity::new(0, 0)));
        assert!(!store.validate(Entity::new(1, 0)));
        assert!(!store.validate(Entity::new(10, 0)));
    }

    #[test]
    fn validate_rejects_stale_generation() {
        let mut store = UsageStore::default();
        store.push(usage_row_default());

        assert!(!store.validate(Entity::new(0, 1)));
    }

    #[test]
    fn set_cost_micros_refuses_stale_or_foreign_entities() {
        let mut store = UsageStore::default();
        store.push(usage_row_default());

        assert!(!store.set_cost_micros(Entity::new(0, 1), 7));
        assert!(!store.set_cost_micros(Entity::new(1, 0), 7));
        assert_eq!(store.costs_micros[0], 0);

        assert!(store.set_cost_micros(Entity::new(0, 0), 7));
        assert_eq!(store.costs_micros[0], 7);
    }

    #[test]
    fn entities_and_validate_follow_the_generation_lane() {
        let mut store = UsageStore::default();
        store.push(usage_row_default());
        store.generations[0] = 5;

        let entities = store
            .entities()
            .map(|(entity, _)| entity)
            .collect::<Vec<_>>();
        assert_eq!(entities, [Entity::new(0, 5)]);
        assert!(store.validate(Entity::new(0, 5)));
        assert!(!store.validate(Entity::new(0, 0)));

        assert!(store.set_cost_micros(Entity::new(0, 5), 9));
        assert_eq!(store.costs_micros[0], 9);
        assert!(!store.set_cost_micros(Entity::new(0, 0), 1));
        assert_eq!(store.costs_micros[0], 9);
    }

    #[test]
    fn world_models_session_tree_with_agent_component() {
        let mut interner = Interner::default();
        let root = interner.intern_session("ses_root");
        let child = interner.intern_session("ses_child");
        let build = interner.intern_agent("build");
        let sessions = SessionTree::from([
            (root, SessionNode { agent: None }),
            (child, SessionNode { agent: Some(build) }),
        ]);
        let mut entities = UsageStore::default();
        entities.push(usage_row_default());

        let world = World {
            header: BillHeader::Session {
                id: "ses_root".to_owned(),
                title: "Test".to_owned(),
                created_ms: None,
            },
            session_count: 2,
            sessions,
            entities,
            wall_start: None,
            wall_end: None,
            interner,
        };

        assert_eq!(world.sessions[&root].agent, None);
        assert_eq!(
            world
                .interner
                .resolve_agent(world.sessions[&child].agent.unwrap()),
            "build"
        );
        assert_eq!(
            world
                .entities
                .entities()
                .map(|(entity, _)| entity)
                .collect::<Vec<_>>(),
            [Entity::new(0, 0)]
        );
    }

    #[test]
    fn converts_usd_to_micro_dollars() {
        assert_eq!(usd_to_micros(0.125), 125_000);
        assert_eq!(usd_to_micros(0.0), 0);
        assert_eq!(usd_to_micros(7.5), 7_500_000);
        assert_eq!(usd_to_micros(0.00004325), 43);
    }

    #[test]
    fn totals_saturate_cost_in_micro_dollars() {
        let mut total = Totals {
            cost: cost_from_micros(i64::MAX),
            ..Totals::default()
        };
        let row = UsageRow {
            cost_micros: 1,
            created: None,
            completed: None,
            ..usage_row_default()
        };

        total.accumulate(&row);

        assert_eq!(cost_to_micros(total.cost), i64::MAX);
    }

    #[test]
    fn cost_add_saturates_on_decimal_overflow() {
        let huge = Money::from_decimal(Decimal::MAX, &MICRO_USD);

        assert_eq!(cost_to_micros(cost_add(huge, huge)), i64::MAX);
        assert_eq!(cost_to_micros(cost_add(-huge, -huge)), i64::MIN);
    }

    fn usage_row_default() -> UsageRow {
        UsageRow {
            session_id: SessionId(0),
            agent: AgentId(0),
            provider: ProviderId(0),
            model: ModelId(0),
            tokens: Tokens::default(),
            cost_micros: 0,
            cost_unknown: false,
            api_ms: 0,
            created: None,
            completed: None,
        }
    }
}
