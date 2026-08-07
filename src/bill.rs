//! Orchestration: load a session, summarize it, and build the report.

use crate::aggregate::{aggregate, aggregate_agents};
use crate::context::StorageContext;
use crate::data::Totals;
use crate::error::BillError;
use crate::pricing;
use crate::render::render_bill;
use crate::report::{BillReport, build_report};
use crate::store::{SessionStore, validate_reference};

/// Generates a deterministic plain-text bill for one OpenCode session.
pub fn generate_bill(context: &StorageContext, reference: &str) -> Result<String, BillError> {
    let report = load_report_with_progress(context, reference, |_| {})?;

    render_bill(&report)
}

/// Loads a session and builds the report data, reporting each work step.
///
/// `on_task` runs before every phase (loading, aggregation, pricing, report
/// building) so a UI can show which step is currently in progress.
pub fn load_report_with_progress(
    context: &StorageContext,
    reference: &str,
    mut on_task: impl FnMut(&str),
) -> Result<BillReport, BillError> {
    validate_reference(reference)?;

    on_task("Loading session data");
    let session = context.store().load(reference)?;

    on_task("Aggregating usage");
    let by_model = aggregate(&session.messages);
    let by_agent = aggregate_agents(&session.messages, &session.session_agents);
    let total = by_model.values().fold(Totals::default(), |mut sum, row| {
        sum.merge(row);
        sum
    });

    on_task("Loading model pricing");
    let pricing = pricing::load_pricing(context.models_file.as_deref(), by_model.keys())?;

    on_task("Building report");
    Ok(build_report(
        &session, &by_model, &by_agent, &total, &pricing,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::DATABASE_FILE;
    use rusqlite::{Connection, params};
    use std::fs;
    use tempfile::TempDir;

    fn fixture() -> Result<(TempDir, StorageContext), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let database = temporary.path().join(DATABASE_FILE);
        let connection = Connection::open(&database)?;

        connection.execute_batch(
            "CREATE TABLE session (id TEXT PRIMARY KEY, title TEXT NOT NULL, parent_id TEXT);\
             CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT NOT NULL,\
             time_created INTEGER NOT NULL, data TEXT NOT NULL);\
             CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT NOT NULL,\
             session_id TEXT NOT NULL, data TEXT NOT NULL);",
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
                "0ms",
                "0ms",
                "$0.125000"
            ]
        ));
        assert!(bill.contains("Amount due:  $0.125000 USD"));
        Ok(())
    }

    #[test]
    fn reports_progress_steps_and_report_data() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let mut tasks = Vec::new();

        let report = load_report_with_progress(&context, "ses_abcdef", |task| {
            tasks.push(task.to_owned());
        })?;

        assert_eq!(
            tasks,
            [
                "Loading session data",
                "Aggregating usage",
                "Loading model pricing",
                "Building report",
            ]
        );
        assert_eq!(report.session_id, "ses_abcdef123456");
        assert_eq!(report.session_title, "Test session");
        assert_eq!(report.session_count, 1);
        assert_eq!(report.usage_rows.len(), 1);
        assert_eq!(report.agent_rows.len(), 1);
        assert!(report.pricing_rows.is_empty());
        assert_eq!(report.totals.len(), 11);
        Ok(())
    }

    #[test]
    fn matches_session_reference_literally() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        // 'X' differs from '_' only under wildcard matching; a literal prefix
        // lookup must not treat this as a match for "ses_abcdef".
        connection.execute(
            "INSERT INTO session (id, title) VALUES (?1, ?2)",
            params!["sesXabcdef999999", "Other session"],
        )?;
        drop(connection);

        let bill = generate_bill(&context, "ses_abcdef")?;

        assert!(bill.contains("Session: ses_abcdef123456"));
        Ok(())
    }

    #[test]
    fn renders_complete_bill_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;

        let bill = generate_bill(&context, "ses_abcdef")?;

        assert_eq!(bill, golden_bill());
        Ok(())
    }

    #[test]
    fn renders_complete_priced_bill_bytes() -> Result<(), Box<dyn std::error::Error>> {
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

        assert_eq!(bill, golden_priced_bill());
        Ok(())
    }

    fn golden_bill() -> String {
        r#"OPENCODE SESSION BILL
Session: ses_abcdef123456
Title:   Test session
Sessions: 1 (root + descendants)

PROVIDER / MODEL  | REQUESTS | INPUT | CACHE READ | CACHE WRITE | OUTPUT | REASONING | API TIME | WALL TIME | COST (USD)
------------------+----------+-------+------------+-------------+--------+-----------+----------+-----------+-----------
openai / gpt-test |        1 |    10 |          6 |           1 |      4 |         2 |      0ms |       0ms |  $0.125000

USAGE BY AGENT CLASS
AGENT   | SESSIONS | REQUESTS | INPUT | CACHE READ | CACHE WRITE | OUTPUT | REASONING | API TIME | WALL TIME | COST (USD)
--------+----------+----------+-------+------------+-------------+--------+-----------+----------+-----------+-----------
unknown |        1 |        1 |    10 |          6 |           1 |      4 |         2 |      0ms |       0ms |  $0.125000

TOTAL
Requests:    1
Input:       10
Cache read:  6
Cache write: 1
Output:      4
Reasoning:   2
Started:     -
Ended:       -
API time:    0ms
Wall time:   0ms
Amount due:  $0.125000 USD
"#
        .to_owned()
    }

    fn golden_priced_bill() -> String {
        r#"OPENCODE SESSION BILL
Session: ses_abcdef123456
Title:   Test session
Sessions: 1 (root + descendants)

PROVIDER / MODEL  | REQUESTS | INPUT | CACHE READ | CACHE WRITE | OUTPUT | REASONING | API TIME | WALL TIME | COST (USD)
------------------+----------+-------+------------+-------------+--------+-----------+----------+-----------+-----------
openai / gpt-test |        1 |    10 |          6 |           1 |      4 |         2 |      0ms |       0ms |  $0.125000

USAGE BY AGENT CLASS
AGENT   | SESSIONS | REQUESTS | INPUT | CACHE READ | CACHE WRITE | OUTPUT | REASONING | API TIME | WALL TIME | COST (USD)
--------+----------+----------+-------+------------+-------------+--------+-----------+----------+-----------+-----------
unknown |        1 |        1 |    10 |          6 |           1 |      4 |         2 |      0ms |       0ms |  $0.125000

MODEL PRICING (USD / 1M TOKENS)
PROVIDER / MODEL  |  INPUT | CACHE READ | CACHE WRITE | OUTPUT
------------------+--------+------------+-------------+-------
openai / gpt-test | $1.250 |     $0.125 |           - | $5.000

TOTAL
Requests:    1
Input:       10
Cache read:  6
Cache write: 1
Output:      4
Reasoning:   2
Started:     -
Ended:       -
API time:    0ms
Wall time:   0ms
Amount due:  $0.125000 USD
"#
        .to_owned()
    }

    #[test]
    fn reports_api_and_wall_time() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        connection.execute("ALTER TABLE session ADD COLUMN agent TEXT", [])?;
        connection.execute(
            "UPDATE session SET agent = ?1 WHERE id = ?2",
            params!["build", "ses_abcdef123456"],
        )?;
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_2",
                "ses_abcdef123456",
                2,
                r#"{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.125,"time":{"created":2000,"completed":5000},"tokens":{"input":10,"output":4,"reasoning":2,"cache":{"read":6,"write":1}}}"#
            ],
        )?;
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_3",
                "ses_abcdef123456",
                3,
                r#"{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.125,"time":{"created":3000,"completed":7000},"tokens":{"input":10,"output":4,"reasoning":2,"cache":{"read":6,"write":1}}}"#
            ],
        )?;
        drop(connection);

        let bill = generate_bill(&context, "ses_abcdef")?;

        assert!(has_table_row(
            &bill,
            &[
                "openai / gpt-test",
                "3",
                "30",
                "18",
                "3",
                "12",
                "6",
                "7s",
                "5s",
                "$0.375000"
            ]
        ));
        assert!(has_table_row(
            &bill,
            &[
                "build",
                "1",
                "3",
                "30",
                "18",
                "3",
                "12",
                "6",
                "7s",
                "5s",
                "$0.375000"
            ]
        ));
        assert!(bill.contains("API time:    7s"));
        assert!(bill.contains("Wall time:   5s"));
        Ok(())
    }

    #[test]
    fn excludes_tool_calls_from_api_time() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        connection.execute(
            "UPDATE message SET data = ?1 WHERE id = ?2",
            params![
                r#"{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.125,"time":{"created":1000,"completed":11000},"tokens":{"input":10,"output":4,"reasoning":2,"cache":{"read":6,"write":1}}}"#,
                "msg_1"
            ],
        )?;

        // Two overlapping tool calls spanning 2000..6000 plus one from 8000..9000
        // leave 10s - 5s = 5s of API time.
        for (id, data) in [
            (
                "prt_1",
                r#"{"type":"tool","state":{"status":"completed","time":{"start":2000,"end":5000}}}"#,
            ),
            (
                "prt_2",
                r#"{"type":"tool","state":{"status":"completed","time":{"start":3000,"end":6000}}}"#,
            ),
            (
                "prt_3",
                r#"{"type":"tool","state":{"status":"completed","time":{"start":8000,"end":9000}}}"#,
            ),
            ("prt_4", r#"{"type":"text","text":"not a tool call"}"#),
        ] {
            connection.execute(
                "INSERT INTO part (id, message_id, session_id, data) VALUES (?1, ?2, ?3, ?4)",
                params![id, "msg_1", "ses_abcdef123456", data],
            )?;
        }
        drop(connection);

        let bill = generate_bill(&context, "ses_abcdef")?;

        assert!(bill.contains("API time:    5s"));
        assert!(bill.contains("Wall time:   10s"));
        Ok(())
    }

    #[test]
    fn includes_user_messages_in_wall_time() -> Result<(), Box<dyn std::error::Error>> {
        let (_temporary, context) = fixture()?;
        let connection = Connection::open(context.data_dir.join(DATABASE_FILE))?;

        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_user",
                "ses_abcdef123456",
                0,
                r#"{"role":"user","time":{"created":500}}"#
            ],
        )?;
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_2",
                "ses_abcdef123456",
                2,
                r#"{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.125,"time":{"created":2000,"completed":5000},"tokens":{"input":10,"output":4,"reasoning":2,"cache":{"read":6,"write":1}}}"#
            ],
        )?;
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_3",
                "ses_abcdef123456",
                3,
                r#"{"role":"assistant","providerID":"openai","modelID":"gpt-test","cost":0.125,"time":{"created":3000,"completed":7000},"tokens":{"input":10,"output":4,"reasoning":2,"cache":{"read":6,"write":1}}}"#
            ],
        )?;
        connection.execute(
            "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
            params![
                "msg_user_end",
                "ses_abcdef123456",
                4,
                r#"{"role":"user","time":{"created":9000}}"#
            ],
        )?;
        drop(connection);

        let bill = generate_bill(&context, "ses_abcdef")?;

        assert!(has_table_row(
            &bill,
            &[
                "openai / gpt-test",
                "3",
                "30",
                "18",
                "3",
                "12",
                "6",
                "7s",
                "5s",
                "$0.375000"
            ]
        ));
        assert!(bill.contains("API time:    7s"));
        assert!(bill.contains("Wall time:   8s"));
        Ok(())
    }

    #[test]
    fn reports_time_from_legacy_json() -> Result<(), Box<dyn std::error::Error>> {
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
            r#"{"role":"assistant","agent":"legacy-agent","providerID":"anthropic","modelID":"legacy","cost":0.25,"time":{"created":1000,"completed":4000},"tokens":{"input":20,"output":8,"reasoning":1,"cache":{"read":4,"write":2}}}"#,
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
                "3s",
                "3s",
                "$0.250000"
            ]
        ));
        assert!(bill.contains("API time:    3s"));
        assert!(bill.contains("Wall time:   3s"));
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
                "0ms",
                "0ms",
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
                "0ms",
                "0ms",
                "$0.625000"
            ]
        ));
        assert!(bill.contains("Requests:    3"));
        assert!(bill.contains("Amount due:  $0.875000 USD"));
        assert!(bill.contains("USAGE BY AGENT CLASS"));
        assert!(has_table_row(
            &bill,
            &[
                "build",
                "1",
                "1",
                "10",
                "6",
                "1",
                "4",
                "2",
                "0ms",
                "0ms",
                "$0.125000"
            ]
        ));
        assert!(has_table_row(
            &bill,
            &[
                "explore",
                "2",
                "2",
                "50",
                "11",
                "2",
                "20",
                "4",
                "0ms",
                "0ms",
                "$0.750000"
            ]
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
                "0ms",
                "0ms",
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
                "0ms",
                "0ms",
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
                "0ms",
                "0ms",
                "$0.750000"
            ]
        ));
        Ok(())
    }
}
