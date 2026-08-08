//! Summaries over the session's assistant records.

use std::collections::BTreeMap;

use rayon::prelude::*;

use crate::bill::PipelineProgress;
use crate::data::{AgentId, AgentUsage, ModelKey, SessionTree, Totals, UsageStore};

/// Chunk size of the parallel aggregate folds.
///
/// Each chunk folds sequentially into its own map; the per-chunk maps are
/// then merged in chunk order, so the result equals a single sequential fold
/// in row order — deterministic even for saturating cost sums.
const AGGREGATE_CHUNK: usize = 4096;

/// Totals per provider/model pair, in deterministic key order.
pub(crate) fn aggregate(
    store: &UsageStore,
    progress: &PipelineProgress,
) -> BTreeMap<ModelKey, Totals> {
    let rows = store.rows().collect::<Vec<_>>();
    let chunk_maps = rows
        .par_chunks(AGGREGATE_CHUNK)
        .map(|chunk| {
            let mut local = BTreeMap::<ModelKey, Totals>::new();

            for row in chunk {
                progress.folded_row();

                local
                    .entry((row.provider, row.model))
                    .or_default()
                    .accumulate(row);
            }

            local
        })
        .collect::<Vec<_>>();

    chunk_maps
        .into_iter()
        .fold(BTreeMap::new(), |mut usage, local| {
            for (key, totals) in local {
                usage.entry(key).or_default().merge(&totals);
            }

            usage
        })
}

/// Totals per agent class, seeded with every session's declared agent.
pub(crate) fn aggregate_agents(
    store: &UsageStore,
    sessions: &SessionTree,
    progress: &PipelineProgress,
) -> BTreeMap<AgentId, AgentUsage> {
    let mut usage = sessions
        .iter()
        .filter_map(|(session_id, node)| node.agent.map(|agent| (*session_id, agent)))
        .fold(
            BTreeMap::<AgentId, AgentUsage>::new(),
            |mut usage, (session_id, agent)| {
                usage.entry(agent).or_default().sessions.insert(session_id);

                usage
            },
        );

    let rows = store.rows().collect::<Vec<_>>();
    let chunk_maps = rows
        .par_chunks(AGGREGATE_CHUNK)
        .map(|chunk| {
            let mut local = BTreeMap::<AgentId, AgentUsage>::new();

            for row in chunk {
                progress.folded_row();

                let entry = local.entry(row.agent).or_default();

                entry.sessions.insert(row.session_id);
                entry.totals.accumulate(row);
            }

            local
        })
        .collect::<Vec<_>>();

    for local in chunk_maps {
        for (agent, entry) in local {
            let target = usage.entry(agent).or_default();

            target.sessions.extend(entry.sessions);
            target.totals.merge(&entry.totals);
        }
    }

    usage
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::{Interner, SessionNode, Tokens, UsageRow, UsageStore};

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
    fn parallel_aggregate_matches_sequential_fold() {
        let mut interner = Interner::default();
        let mut store = UsageStore::default();
        let mut sessions = SessionTree::new();

        sessions.insert(
            interner.intern_session("ses_0000"),
            SessionNode {
                agent: Some(interner.intern_agent("agent_0000")),
                parent: None,
            },
        );

        // Enough rows to span several aggregate chunks, with every lane
        // variety: two providers, three models, four agent classes.
        for index in 0..(AGGREGATE_CHUNK as u64 * 2 + 13) {
            store.push(UsageRow {
                session_id: interner.intern_session(&format!("ses_{:04}", index % 7)),
                agent: interner.intern_agent(&format!("agent_{:04}", index % 4)),
                provider: interner.intern_provider(&format!("prov_{:04}", index % 2)),
                model: interner.intern_model(&format!("model_{:04}", index % 3)),
                tokens: Tokens {
                    input: index,
                    output: index.wrapping_mul(3),
                    reasoning: 1,
                    cache_read: index / 2,
                    cache_write: index % 5,
                },
                cost_micros: index as i64,
                cost_unknown: false,
                api_ms: index,
                created: Some(index),
                completed: Some(index.wrapping_add(10)),
            });
        }

        let progress = PipelineProgress::default();
        let parallel = aggregate(&store, &progress);
        let parallel_agents = aggregate_agents(&store, &sessions, &progress);
        let sequential =
            store
                .rows()
                .fold(BTreeMap::<ModelKey, Totals>::new(), |mut usage, row| {
                    usage
                        .entry((row.provider, row.model))
                        .or_default()
                        .accumulate(&row);
                    usage
                });

        assert_eq!(parallel.len(), sequential.len());

        for (key, totals) in &sequential {
            let parallel_totals = parallel.get(key).expect("key present in parallel map");

            assert_eq!(parallel_totals.requests, totals.requests);
            assert_eq!(parallel_totals.tokens.input, totals.tokens.input);
            assert_eq!(parallel_totals.tokens.output, totals.tokens.output);
            assert_eq!(parallel_totals.tokens.reasoning, totals.tokens.reasoning);
            assert_eq!(parallel_totals.tokens.cache_read, totals.tokens.cache_read);
            assert_eq!(
                parallel_totals.tokens.cache_write,
                totals.tokens.cache_write
            );
            assert_eq!(parallel_totals.cost, totals.cost);
            assert_eq!(parallel_totals.api_ms, totals.api_ms);
            assert_eq!(parallel_totals.wall_start, totals.wall_start);
            assert_eq!(parallel_totals.wall_end, totals.wall_end);
        }

        // The agent fold must match a sequential fold as well: every seeded
        // session plus every row, accumulated in row order.
        let sequential_agents = {
            let mut usage = sessions
                .iter()
                .filter_map(|(session_id, node)| node.agent.map(|agent| (*session_id, agent)))
                .fold(
                    BTreeMap::<AgentId, AgentUsage>::new(),
                    |mut usage, (session_id, agent)| {
                        usage.entry(agent).or_default().sessions.insert(session_id);

                        usage
                    },
                );

            for row in store.rows() {
                let entry = usage.entry(row.agent).or_default();

                entry.sessions.insert(row.session_id);
                entry.totals.accumulate(&row);
            }

            usage
        };

        assert_eq!(parallel_agents.len(), sequential_agents.len());

        for (agent, expected) in &sequential_agents {
            let actual = parallel_agents
                .get(agent)
                .expect("agent present in parallel map");

            assert_eq!(actual.sessions, expected.sessions);
            assert_eq!(actual.totals.requests, expected.totals.requests);
            assert_eq!(actual.totals.tokens.input, expected.totals.tokens.input);
            assert_eq!(actual.totals.tokens.output, expected.totals.tokens.output);
            assert_eq!(
                actual.totals.tokens.reasoning,
                expected.totals.tokens.reasoning
            );
            assert_eq!(
                actual.totals.tokens.cache_read,
                expected.totals.tokens.cache_read
            );
            assert_eq!(
                actual.totals.tokens.cache_write,
                expected.totals.tokens.cache_write
            );
            assert_eq!(actual.totals.cost, expected.totals.cost);
            assert_eq!(actual.totals.api_ms, expected.totals.api_ms);
            assert_eq!(actual.totals.wall_start, expected.totals.wall_start);
            assert_eq!(actual.totals.wall_end, expected.totals.wall_end);
        }
    }

    #[test]
    fn chunked_aggregate_saturates_across_chunk_boundaries() {
        let mut interner = Interner::default();
        let mut store = UsageStore::default();

        // A store whose rows straddle a chunk boundary: more saturated rows
        // than one chunk holds, so the boundary falls inside the saturated
        // group and both sides must still saturate to u64::MAX in the merge.
        for _ in 0..(AGGREGATE_CHUNK + 1) {
            store.push(extreme_row(&mut interner, 0, true));
        }

        let aggregate_chunked = aggregate(&store, &PipelineProgress::default());
        let total_chunked = aggregate_chunked
            .values()
            .fold(Totals::default(), |mut sum, row| {
                sum.merge(row);
                sum
            });

        assert_eq!(total_chunked.tokens.input, u64::MAX);
        assert_eq!(total_chunked.tokens.output, u64::MAX);
        assert_eq!(total_chunked.tokens.reasoning, u64::MAX);
        assert_eq!(total_chunked.tokens.cache_read, u64::MAX);
        assert_eq!(total_chunked.tokens.cache_write, u64::MAX);
    }
}
