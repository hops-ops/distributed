//! Postgres-dialect checks. Behaviour every repository shares lives in the
//! repository conformance suite (`common::repository`).

use common::ids::unique_id;
use common::postgres::PostgresTestSchema;
use common::todo::Todo;
use distributed::{
    Aggregate, AggregateBuilder, GetStream, PostgresRepository, RepositoryError, StreamIdentity,
};

async fn repository() -> Option<(PostgresTestSchema, PostgresRepository)> {
    let schema =
        PostgresTestSchema::create_from_env("postgres_repo", "Postgres integration test").await?;
    let repo = schema.repository().await;
    Some((schema, repo))
}

#[tokio::test]
async fn migration_is_idempotent_and_uses_postgres_column_types() {
    let Some((schema, repo)) = repository().await else {
        return;
    };
    repo.migrate().await.unwrap();

    let rows = sqlx::query(
        r#"
        SELECT column_name, udt_name
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name = 'aggregate_events'
          AND column_name IN ('payload', 'metadata', 'recorded_at')
        "#,
    )
    .bind(schema.schema_name())
    .fetch_all(repo.pool())
    .await
    .unwrap();

    let mut columns = rows
        .into_iter()
        .map(|row| {
            (
                sqlx::Row::try_get::<String, _>(&row, "column_name").unwrap(),
                sqlx::Row::try_get::<String, _>(&row, "udt_name").unwrap(),
            )
        })
        .collect::<Vec<_>>();
    columns.sort();

    assert_eq!(
        columns,
        vec![
            ("metadata".into(), "jsonb".into()),
            ("payload".into(), "bytea".into()),
            ("recorded_at".into(), "timestamptz".into()),
        ]
    );

    let document_table: Option<String> = sqlx::query_scalar(
        r#"
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = $1
          AND table_name = 'transactional_read_models'
        "#,
    )
    .bind(schema.schema_name())
    .fetch_optional(repo.pool())
    .await
    .unwrap();
    assert!(document_table.is_none());

    let processed_messages_table: Option<String> = sqlx::query_scalar(
        r#"
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = $1
          AND table_name = 'read_model_processed_messages'
        "#,
    )
    .bind(schema.schema_name())
    .fetch_optional(repo.pool())
    .await
    .unwrap();
    assert!(processed_messages_table.is_none());
}

#[tokio::test]
async fn backend_termination_mid_commit_rolls_back_and_nothing_persists() {
    // Fault injection: a commit_batch is killed mid-transaction (the backend is
    // terminated while the commit waits on a table lock). The write must roll
    // back completely and surface as a storage error rather than hanging or
    // partially persisting.
    let Some((schema, repo)) = repository().await else {
        return;
    };
    let database_url = std::env::var("DATABASE_URL").expect("repository() checked DATABASE_URL");
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .expect("admin pool connects");

    // Take an ACCESS EXCLUSIVE lock on the events table from a second
    // connection so the commit blocks mid-transaction at its first table touch.
    let mut locker = admin.acquire().await.expect("locker connection");
    let locker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *locker)
        .await
        .expect("locker pid");
    sqlx::query("BEGIN")
        .execute(&mut *locker)
        .await
        .expect("begin lock tx");
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "LOCK TABLE \"{}\".aggregate_events IN ACCESS EXCLUSIVE MODE",
        schema.schema_name().replace('"', "\"\"")
    )))
    .execute(&mut *locker)
    .await
    .expect("lock aggregate_events");

    // The commit now parks on the lock inside its transaction.
    let id = unique_id("terminated");
    let commit = {
        let repo = repo.clone();
        let id = id.clone();
        tokio::spawn(async move {
            let mut todo = Todo::new();
            todo.initialize(id, "user-1".into(), "terminated".into())
                .unwrap();
            repo.aggregate::<Todo>().commit(&mut todo).await
        })
    };

    // Find the backend our lock is blocking (precisely: blocked BY locker_pid),
    // then terminate it mid-commit.
    let mut victim_pid: Option<i32> = None;
    for _ in 0..100 {
        let blocked: Option<i32> = sqlx::query_scalar(
            "SELECT pid FROM pg_stat_activity WHERE pg_blocking_pids(pid) @> ARRAY[$1] LIMIT 1",
        )
        .bind(locker_pid)
        .fetch_optional(&admin)
        .await
        .expect("pg_stat_activity lookup");
        if let Some(pid) = blocked {
            victim_pid = Some(pid);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let victim_pid = victim_pid.expect("the commit should block on the table lock");
    let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(victim_pid)
        .fetch_one(&admin)
        .await
        .expect("pg_terminate_backend");
    assert!(terminated, "the blocked commit backend was terminated");

    let err = commit
        .await
        .expect("commit task joins")
        .expect_err("a terminated backend must fail the commit");
    assert!(
        matches!(&err, RepositoryError::Storage { .. }),
        "the terminated commit surfaces as a storage error, got {err:?}"
    );
    // The termination surfaces as SQLSTATE 57P01; `is_sqlx_transient` now
    // classifies 57P01/57P02/57P03 and class-08 connection failures as
    // transient (losing the connection is an infrastructure hiccup), so the
    // error is retryable.
    assert!(
        err.is_retryable(),
        "57P01 (admin shutdown) must be retryable; got {err:?}"
    );

    // Release the lock and prove the whole transaction rolled back.
    sqlx::query("ROLLBACK")
        .execute(&mut *locker)
        .await
        .expect("release lock");
    let identity = StreamIdentity::new(Todo::aggregate_type(), &id).unwrap();
    assert!(
        repo.get_stream(&identity).await.unwrap().is_none(),
        "nothing from the killed commit may persist"
    );
}
