//! Fault probes for the isolated test harness, absent from ordinary builds.
//! The Worker authenticates the internal request before reaching this module.
//! Operations are finite and fixed; this is not an arbitrary-SQL endpoint.

use serde_json::{json, Value};
use worker::*;

#[derive(serde::Deserialize, distributed::CommandInput)]
pub struct BatchInput {
    pub todo_id: String,
    pub title: String,
}

#[derive(serde::Serialize, distributed::CommandOutput)]
pub struct BatchPayload {
    pub title: String,
}

async fn handle_batch(
    ctx: &distributed::microsvc::CausalCommandContext<'_, todo_domain::Todo>,
    input: BatchInput,
) -> std::result::Result<
    distributed::command::PreparedCommand<distributed::command::Eventual<BatchPayload>>,
    distributed::microsvc::HandlerError,
> {
    use distributed::microsvc::HandlerError;
    let principal = ctx.user_id()?;
    let repo = ctx.repo();
    let mut todo = repo
        .get(&input.todo_id)
        .await?
        .ok_or_else(|| HandlerError::Rejected("missing batch fixture".into()))?;
    for index in 0..32 {
        todo.rename(principal, &format!("{} {index}", input.title))
            .map_err(|error| HandlerError::Rejected(error.to_string()))?;
    }
    let title = todo_domain::TodoState::from(&*todo).title;
    repo.publish_events()
        .commit(todo)?
        .eventual(BatchPayload { title })
}

distributed::portable_command! {
    name: "todo.test_batch",
    transition: todo_domain::domain_commands::Rename,
    aggregate: todo_domain::Todo,
    input: BatchInput,
    outcome: distributed::command::Eventual<BatchPayload>,
    shard: |input| input.todo_id.clone(),
    roles: ["user"],
    field: "test_batch",
    handle: handle_batch,
}

pub async fn handle(sql: &SqlStorage, request: &mut Request) -> Result<Response> {
    if request.method() != Method::Post {
        return Response::error("method not allowed", 405);
    }
    let body: Value = request.json().await?;
    match body.get("operation").and_then(Value::as_str) {
        Some("inspect") => {
            let rows: Vec<Value> = sql.exec(
                "SELECT
                  (SELECT COUNT(*) FROM aggregate_events) AS events,
                  (SELECT COALESCE(SUM(length(payload)), 0) FROM aggregate_events) AS eventBytes,
                  (SELECT COALESCE(MAX(sequence), 0) FROM aggregate_events) AS version,
                  (SELECT COUNT(*) FROM aggregate_snapshots) AS snapshots,
                  (SELECT COALESCE(MAX(version), 0) FROM aggregate_snapshots) AS snapshotVersion,
                  (SELECT COUNT(*) FROM command_ledger) AS receipts,
                  (SELECT COUNT(*) FROM command_ledger WHERE completed_at IS NOT NULL) AS completed,
                  (SELECT COUNT(*) FROM outbox_messages) AS outbox,
                  (SELECT COUNT(*) FROM sqlite_master WHERE name = 'cell_state') AS wholeStateTables",
                None,
            )?.to_array()?;
            let outbox: Vec<Value> = sql
                .exec(
                    "SELECT message_id, status, attempts FROM outbox_messages ORDER BY message_id",
                    None,
                )?
                .to_array()?;
            Response::from_json(&json!({ "counts": rows[0], "outbox": outbox }))
        }
        Some("fail-completion") => {
            sql.exec(
                "CREATE TRIGGER test_fail_completion BEFORE UPDATE ON command_ledger
                 WHEN NEW.state = 'succeeded'
                 BEGIN SELECT RAISE(ABORT, 'test receipt write failure'); END",
                None,
            )?;
            Response::ok("armed")
        }
        Some("expire-at-commit") => {
            sql.exec(
                "CREATE TRIGGER test_expire_attempt AFTER INSERT ON aggregate_events
                 BEGIN UPDATE command_ledger SET lease_expires_at = 0 WHERE state = 'in_progress'; END",
                None,
            )?;
            Response::ok("armed")
        }
        Some("fail-settlement") => {
            sql.exec(
                "CREATE TRIGGER test_fail_settlement BEFORE DELETE ON outbox_messages
                 BEGIN SELECT RAISE(ABORT, 'test settlement failure'); END",
                None,
            )?;
            Response::ok("armed")
        }
        Some("fail-claim") => {
            sql.exec(
                "CREATE TRIGGER test_fail_claim BEFORE UPDATE ON outbox_messages
                 WHEN NEW.status = 'in_flight'
                 BEGIN SELECT RAISE(ABORT, 'test claim failure'); END",
                None,
            )?;
            Response::ok("armed")
        }
        Some("clear-faults") => {
            reset_faults_on_activation(sql)?;
            Response::ok("cleared")
        }
        _ => Response::error("unknown probe", 400),
    }
}

// celld intentionally forbids TEMP schema objects. These fixed test triggers
// are activation-scoped by explicit cleanup on the next test Worker activation.
pub fn reset_faults_on_activation(sql: &SqlStorage) -> Result<()> {
    sql.exec(
        "DROP TRIGGER IF EXISTS test_fail_completion;
        DROP TRIGGER IF EXISTS test_expire_attempt;
        DROP TRIGGER IF EXISTS test_fail_settlement;
        DROP TRIGGER IF EXISTS test_fail_claim;",
        None,
    )?;
    Ok(())
}
