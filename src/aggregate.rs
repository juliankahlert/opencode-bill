//! Summaries over the session's assistant records.

use std::collections::BTreeMap;

use crate::data::{AgentUsage, ModelKey, Totals, UsageRecord};

/// Totals per provider/model pair, in deterministic key order.
pub(crate) fn aggregate(records: &[UsageRecord]) -> BTreeMap<ModelKey, Totals> {
    records.iter().fold(BTreeMap::new(), |mut usage, record| {
        usage
            .entry((record.provider.clone(), record.model.clone()))
            .or_default()
            .accumulate(record);
        usage
    })
}

/// Totals per agent class, seeded with every session's declared agent.
pub(crate) fn aggregate_agents(
    records: &[UsageRecord],
    session_agents: &BTreeMap<String, String>,
) -> BTreeMap<String, AgentUsage> {
    let mut usage = session_agents.iter().fold(
        BTreeMap::<String, AgentUsage>::new(),
        |mut usage, (session_id, agent)| {
            usage
                .entry(agent.clone())
                .or_default()
                .sessions
                .insert(session_id.clone());
            usage
        },
    );

    records.iter().fold(&mut usage, |usage, record| {
        let entry = usage.entry(record.agent.clone()).or_default();

        entry.sessions.insert(record.session_id.clone());
        entry.totals.accumulate(record);
        usage
    });

    usage
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::Tokens;

    #[test]
    fn saturates_api_time_on_extreme_spans() {
        let message = UsageRecord {
            session_id: "ses".to_owned(),
            agent: "build".to_owned(),
            provider: "openai".to_owned(),
            model: "gpt-test".to_owned(),
            tokens: Tokens::default(),
            cost: 0.0,
            cost_unknown: false,
            api_ms: u64::MAX,
            created: Some(0),
            completed: Some(u64::MAX),
        };

        let usage = aggregate(&[message.clone(), message]);
        let total = usage.values().fold(Totals::default(), |mut sum, row| {
            sum.merge(row);
            sum
        });

        assert_eq!(total.api_ms, u64::MAX);
    }

    #[test]
    fn saturates_token_sums_on_extreme_usage() {
        let message = UsageRecord {
            session_id: "ses".to_owned(),
            agent: "build".to_owned(),
            provider: "openai".to_owned(),
            model: "gpt-test".to_owned(),
            tokens: Tokens {
                input: u64::MAX,
                output: u64::MAX,
                reasoning: u64::MAX,
                cache_read: u64::MAX,
                cache_write: u64::MAX,
            },
            cost: 0.0,
            cost_unknown: false,
            api_ms: 0,
            created: None,
            completed: None,
        };

        let usage = aggregate(&[message.clone(), message]);
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
