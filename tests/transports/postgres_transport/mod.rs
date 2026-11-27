//! Postgres transports: the bus contract and the SQL bus contract over
//! `PostgresBus`, and `OutboxSource<PostgresOutboxStore>` (claim with
//! `FOR UPDATE SKIP LOCKED` under a lease, dispatch, settle by row status).
//! Skips when `DATABASE_URL` is unset.

use common::outbox as outbox_support;
use common::postgres;
use common::transport::bus::{BusCapabilities, BusHarness, KindCapabilities};

use std::sync::{Arc, Mutex};
use std::time::Duration;

use distributed::bus::{
    run_source, MessageSource, PostgresBus, ReceivedMessage, RunOptions, TransportError,
};
use distributed::microsvc::{Context, Routes, Service};
use distributed::outbox::{OutboxMessage, OutboxMessageStatus};
use distributed::outbox_worker::OutboxSource;
use distributed::projection_protocol::ProjectionEpoch;
use distributed::{CommitBatch, PostgresOutboxStore, PostgresRepository, TransactionalCommit};
use serde_json::json;

use crate::sql_bus_conformance::{sql_bus_conformance_tests, SqlBackend};

const SKIP: &str = "postgres transport test";

async fn enqueue(repo: &PostgresRepository, id: &str, name: &str) {
    let mut batch = CommitBatch::empty();
    batch
        .outbox_messages
        .push(OutboxMessage::create(id, name, b"{}".to_vec()).unwrap());
    repo.commit_batch(batch)
        .await
        .expect("outbox row should commit");
}

async fn status(store: &PostgresOutboxStore, id: &str) -> Option<OutboxMessageStatus> {
    outbox_support::outbox_status_by_id(store, id).await
}

fn recording_service(handled: Arc<Mutex<Vec<String>>>) -> Arc<Service> {
    Arc::new(
        Service::new().routes(
            Routes::new()
                .with_dependencies(())
                .event("order.initialized")
                .handle(move |ctx: &Context<()>| {
                    handled
                        .lock()
                        .unwrap()
                        .push(ctx.message().id().unwrap_or_default().to_string());
                    async move { Ok(json!({})) }
                }),
        ),
    )
}

#[tokio::test]
async fn outbox_source_run_drains_and_completes() {
    let Some(schema) = postgres::PostgresTestSchema::create_from_env("pg_tx_drain", SKIP).await
    else {
        return;
    };
    let repo = schema.repository().await;
    enqueue(&repo, "m1", "order.initialized").await;
    enqueue(&repo, "m2", "order.initialized").await;
    let store = Arc::new(repo.outbox_store());

    let handled = Arc::new(Mutex::new(Vec::new()));
    let service = recording_service(handled.clone());
    run_source(
        service,
        OutboxSource::new(store.clone(), "pg-drain"),
        RunOptions::until_idle(),
    )
    .await
    .unwrap();

    let mut ids = handled.lock().unwrap().clone();
    ids.sort();
    assert_eq!(ids, vec!["m1".to_string(), "m2".to_string()]);
    assert_eq!(status(&store, "m1").await, None);
    assert_eq!(status(&store, "m2").await, None);
}

#[tokio::test]
async fn concurrent_sources_process_each_row_once() {
    let Some(schema) =
        postgres::PostgresTestSchema::create_from_env("pg_tx_concurrent", SKIP).await
    else {
        return;
    };
    let repo = schema.repository().await;
    let ids: Vec<String> = (0..20).map(|i| format!("c{i}")).collect();
    for id in &ids {
        enqueue(&repo, id, "order.initialized").await;
    }
    let store = Arc::new(repo.outbox_store());

    let handled = Arc::new(Mutex::new(Vec::new()));
    let run = |worker: &'static str| {
        run_source(
            recording_service(handled.clone()),
            OutboxSource::new(store.clone(), worker),
            RunOptions::until_idle(),
        )
    };
    // Two competing consumers drain concurrently; SKIP LOCKED guarantees each
    // row is claimed (and handled) exactly once.
    let (a, b) = tokio::join!(run("worker-a"), run("worker-b"));
    a.unwrap();
    b.unwrap();

    let mut got = handled.lock().unwrap().clone();
    got.sort();
    let unique = {
        let mut u = got.clone();
        u.dedup();
        u
    };
    assert_eq!(got, unique, "no row handled more than once");
    assert_eq!(unique.len(), ids.len(), "every row handled");
}

#[tokio::test]
async fn nack_releases_then_a_later_claim_completes() {
    let Some(schema) = postgres::PostgresTestSchema::create_from_env("pg_tx_retry", SKIP).await
    else {
        return;
    };
    let repo = schema.repository().await;
    enqueue(&repo, "m1", "order.initialized").await;
    let store = Arc::new(repo.outbox_store());

    // First claim, nack -> released to pending (attempts incremented).
    let mut source = OutboxSource::new(store.clone(), "pg-retry");
    let received = source.recv().await.unwrap().expect("a claimable row");
    received.nack(std::time::Duration::ZERO).await.unwrap();
    assert_eq!(
        status(&store, "m1").await,
        Some(OutboxMessageStatus::Pending)
    );

    // A later claim completes it.
    let mut source2 = OutboxSource::new(store.clone(), "pg-retry-2");
    let received2 = source2.recv().await.unwrap().expect("a re-claimable row");
    received2.ack().await.unwrap();
    assert_eq!(status(&store, "m1").await, None);
}

// ---- PostgresBus ----

/// One schema per test; every bus shares its pool.
struct Postgres {
    _schema: postgres::PostgresTestSchema,
    pool: sqlx::PgPool,
}

impl BusHarness for Postgres {
    type Bus = PostgresBus;

    const CAPABILITIES: BusCapabilities = BusCapabilities {
        // The queue keeps every send; the log stores a stable id once.
        commands: KindCapabilities {
            dedupes_ids: false,
            ..KindCapabilities::LOG
        },
        events: KindCapabilities::LOG,
        rebalance_may_redeliver: false,
        added_metadata: &[],
    };

    async fn start() -> Option<Self> {
        let postgres = Self::start_empty().await?;
        PostgresBus::new(postgres.pool.clone())
            .ensure_tables()
            .await
            .expect("ensure tables");
        Some(postgres)
    }

    async fn bus(&self) -> PostgresBus {
        PostgresBus::new(self.pool.clone())
    }
}

impl SqlBackend for Postgres {
    const PERMISSIVE_QUEUE: &'static [&'static str] = &[
        "DROP TABLE IF EXISTS bus_queue",
        "CREATE TABLE bus_queue (
            seq          BIGSERIAL PRIMARY KEY,
            claim_token  TEXT,
            name         TEXT,
            message_id   TEXT,
            kind         TEXT NOT NULL,
            payload      BYTEA NOT NULL,
            content_type TEXT NOT NULL DEFAULT 'application/json',
            metadata     TEXT NOT NULL DEFAULT '[]',
            available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            locked_until TIMESTAMPTZ,
            attempts     INTEGER NOT NULL DEFAULT 0
        )",
        "CREATE INDEX bus_queue_claim_idx ON bus_queue (name, available_at, locked_until, seq)",
    ];
    const PERMISSIVE_LOG: &'static [&'static str] = &[
        "DROP TABLE IF EXISTS bus_log",
        "CREATE TABLE bus_log (
            seq          BIGSERIAL PRIMARY KEY,
            name         TEXT,
            message_id   TEXT,
            kind         TEXT NOT NULL,
            payload      BYTEA NOT NULL,
            content_type TEXT DEFAULT 'application/json',
            metadata     TEXT NOT NULL DEFAULT '[]',
            appended_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
        "CREATE INDEX bus_log_name_seq_idx ON bus_log (name, seq)",
        "CREATE UNIQUE INDEX bus_log_message_id_unique_idx \
         ON bus_log (message_id) WHERE message_id IS NOT NULL",
    ];
    const PAYLOAD: &'static str = "'\\x7b7d'::bytea";

    async fn start_empty() -> Option<Self> {
        let schema = postgres::PostgresTestSchema::create_from_env("sql_bus", SKIP).await?;
        let pool = schema.repository().await.pool().clone();
        Some(Self {
            _schema: schema,
            pool,
        })
    }

    async fn execute(&self, sql: &str) -> Result<(), String> {
        sqlx::raw_sql(sqlx::AssertSqlSafe(sql.to_owned()))
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    async fn ensure_tables(bus: &PostgresBus) -> Result<(), TransportError> {
        bus.ensure_tables().await
    }

    fn with_source_epoch(bus: PostgresBus, epoch: ProjectionEpoch) -> PostgresBus {
        bus.with_source_epoch(epoch)
    }

    fn with_lease(bus: PostgresBus, lease: Duration) -> PostgresBus {
        bus.with_lease(lease)
    }

    async fn reset_ordered_log(
        bus: &PostgresBus,
        expected: &ProjectionEpoch,
        next: &ProjectionEpoch,
    ) -> Result<(), TransportError> {
        bus.reset_ordered_log(expected, next).await
    }
}

common::bus_conformance_tests!(Postgres);
sql_bus_conformance_tests!(Postgres);

/// Starting a consumer does not wait behind a write in flight. Every
/// `listen` and `subscribe` ensures the bus tables; `CREATE INDEX IF NOT
/// EXISTS` locks the table even when the index exists, so that wait would
/// also hold up every claim, lease renewal and ack queued behind it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn starting_a_consumer_does_not_wait_behind_a_write() {
    use distributed::bus::{Bus, BusConsumer, Handlers, Message, MessageKind};

    let Some(postgres) = Postgres::start().await else {
        return;
    };
    let bus = postgres.bus().await;
    bus.send_message(Message::new(
        "sql.held",
        MessageKind::Command,
        b"{}".to_vec(),
    ))
    .await
    .unwrap();
    let writer = tokio::spawn({
        let pool = postgres.pool.clone();
        async move {
            sqlx::raw_sql(
                "WITH held AS (UPDATE bus_queue SET attempts = attempts RETURNING seq) \
                 SELECT pg_sleep(60) FROM held",
            )
            .execute(&pool)
            .await
        }
    });
    // Until the writer holds its lock on the table.
    while !writer.is_finished()
        && postgres
            .execute(
                "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_locks \
             WHERE relation = 'bus_queue'::regclass AND mode = 'RowExclusiveLock' \
             AND pid <> pg_backend_pid()) THEN RAISE EXCEPTION 'not yet'; END IF; END $$",
            )
            .await
            .is_err()
    {
        tokio::task::yield_now().await;
    }
    if writer.is_finished() {
        panic!(
            "the writer stopped before holding its lock: {:?}",
            writer.await
        );
    }

    let handlers = Arc::new(
        Handlers::new()
            .named("starting")
            .on_command("sql.other", |_: &Message| async { Ok(()) }),
    );
    let started = tokio::time::timeout(
        Duration::from_secs(20),
        bus.listen(handlers, RunOptions::until_idle()),
    )
    .await;

    postgres
        .execute(
            "SELECT pg_cancel_backend(pid) FROM pg_stat_activity \
             WHERE query LIKE 'WITH held AS%' \
             AND pid <> pg_backend_pid()",
        )
        .await
        .unwrap();
    writer.await.unwrap().unwrap_err();
    started
        .expect("a consumer starts while a write is in flight")
        .expect("the consumer drains");
}
