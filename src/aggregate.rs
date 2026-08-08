//! Summaries over the session's assistant records.

use std::collections::BTreeMap;

use crate::bill::PipelineProgress;
use crate::data::{AgentId, AgentUsage, ModelKey, SessionTree, Totals, UsageStore};

/// Totals per provider/model pair, in deterministic key order.
///
/// Folds the store's rows directly in row order: no full row copy is
/// materialized, so a year-scale store never doubles its memory in the
/// aggregate phase.  A single sequential fold is trivially order-equivalent
/// to itself, so the saturating cost sums stay deterministic by
/// construction.
pub(crate) fn aggregate(
    store: &UsageStore,
    progress: &PipelineProgress,
) -> BTreeMap<ModelKey, Totals> {
    store
        .rows()
        .fold(BTreeMap::<ModelKey, Totals>::new(), |mut usage, row| {
            progress.folded_row();

            usage
                .entry((row.provider, row.model))
                .or_default()
                .accumulate(&row);

            usage
        })
}

/// Totals per agent class, seeded with every session's declared agent.
pub(crate) fn aggregate_agents(
    store: &UsageStore,
    sessions: &SessionTree,
    progress: &PipelineProgress,
) -> BTreeMap<AgentId, AgentUsage> {
    let usage = sessions
        .iter()
        .filter_map(|(session_id, node)| node.agent.map(|agent| (*session_id, agent)))
        .fold(
            BTreeMap::<AgentId, AgentUsage>::new(),
            |mut usage, (session_id, agent)| {
                usage.entry(agent).or_default().sessions.insert(session_id);

                usage
            },
        );

    store.rows().fold(usage, |mut usage, row| {
        progress.folded_row();

        let entry = usage.entry(row.agent).or_default();

        entry.sessions.insert(row.session_id);
        entry.totals.accumulate(&row);

        usage
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::{Interner, Tokens, UsageRow, UsageStore};

    fn extreme_row(interner: &mut Interner, api_ms: u64, max_tokens: bool) -> UsageRow {
        UsageRow {
            session_id: interner.intern_session("ses"),
            agent: interner.intern_agent("build"),
            provider: interner.intern_provider("openai"),
            model: interner.intern_model("gpt-test"),
            tokens: if max_tokens {
                Tokens {
                    input: u64::MAX,
                    output: u64::MAX,
                    reasoning: u64::MAX,
                    cache_read: u64::MAX,
                    cache_write: u64::MAX,
                }
            } else {
                Tokens::default()
            },
            cost_micros: 0,
            cost_unknown: false,
            api_ms,
            created: Some(0),
            completed: Some(u64::MAX),
        }
    }

    #[test]
    fn saturates_api_time_on_extreme_spans() {
        let mut interner = Interner::default();
        let row = extreme_row(&mut interner, u64::MAX, false);
        let mut store = UsageStore::default();

        store.push(row);
        store.push(row);

        let usage = aggregate(&store, &PipelineProgress::default());
        let total = usage.values().fold(Totals::default(), |mut sum, row| {
            sum.merge(row);
            sum
        });

        assert_eq!(total.api_ms, u64::MAX);
    }

    #[test]
    fn saturates_token_sums_on_extreme_usage() {
        let mut interner = Interner::default();
        let row = extreme_row(&mut interner, 0, true);
        let mut store = UsageStore::default();

        store.push(row);
        store.push(row);

        let usage = aggregate(&store, &PipelineProgress::default());
        let total = usage.values().fold(Totals::default(), |mut sum, row| {
            sum.merge(row);
            sum
        });

        assert_eq!(total.tokens.input, u64::MAX);
        assert_eq!(total.tokens.output, u64::MAX);
        assert_eq!(total.tokens.reasoning, u64::MAX);
        assert_eq!(total.tokens.cache_read, u64::MAX);
        assert_eq!(total.tokens.cache_write, u64::MAX);
    }

    #[test]
    fn saturates_token_sums_across_many_rows() {
        let mut interner = Interner::default();
        let mut store = UsageStore::default();

        // A store far larger than the old chunk size: every row saturates
        // its token lanes, and the sequential fold must keep them saturated
        // instead of wrapping.
        for _ in 0..8192 {
            store.push(extreme_row(&mut interner, 0, true));
        }

        let usage = aggregate(&store, &PipelineProgress::default());
        let total = usage.values().fold(Totals::default(), |mut sum, row| {
            sum.merge(row);
            sum
        });

        assert_eq!(total.tokens.input, u64::MAX);
        assert_eq!(total.tokens.output, u64::MAX);
        assert_eq!(total.tokens.reasoning, u64::MAX);
        assert_eq!(total.tokens.cache_read, u64::MAX);
        assert_eq!(total.tokens.cache_write, u64::MAX);
    }
}
