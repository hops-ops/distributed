//! The persistent-repository contract: one case list, run on every
//! repository by [`repository_conformance_tests!`](crate::repository_conformance_tests).

pub mod checkout;
pub mod checkout_saga;
pub mod inbox;
pub mod outbox;
pub mod read_models;
pub mod scenario;
pub mod seat;
pub mod snapshots;
pub mod storage;

use distributed::{
    InMemoryOutboxStore, InMemoryRepository, OutboxMessage, OutboxStore, Queueable,
    QueuedRepository,
};

/// A repository under test. `open` returns `None` to skip when the backing
/// service is absent and not required (see [`crate::env::require_service`]);
/// the backend value holds whatever the test needs alive (a temp database, a
/// Postgres schema) and drops it when the test ends.
///
/// A SQL backend bootstraps [`read_models::conformance_table_registry`] in
/// `open`.
pub trait RepositoryBackend: Sized {
    type Repository: Clone + Send + Sync + 'static;
    type Outbox: OutboxStore + Send + Sync;

    async fn open(case: &str) -> Option<Self>;
    fn repository(&self) -> Self::Repository;
    fn outbox_store(repository: &Self::Repository) -> Self::Outbox;

    fn outbox(&self) -> Self::Outbox {
        Self::outbox_store(&self.repository())
    }

    /// The outbox message with `id`, in any status.
    async fn outbox_message(&self, id: &str) -> Option<OutboxMessage> {
        crate::outbox::find_outbox_by_id(&Self::outbox_store(&self.repository()), id).await
    }
}

/// Capability `sql_faults`: the backend injects faults and foreign rows with
/// raw SQL in its own dialect.
pub trait SqlFaults: RepositoryBackend {
    /// Add a database constraint rejecting rows of `table` whose `column` is
    /// negative.
    async fn reject_negative(&self, table: &str, column: &str);
    /// Store an event row whose payload codec no reader supports.
    async fn insert_event_with_unsupported_codec(&self, aggregate_type: &str, aggregate_id: &str);
    /// Store a pending outbox row with correlation id `corr-column` and
    /// causation id `cause-column` in their own columns.
    async fn insert_outbox_row_with_trace_columns(&self, message_id: &str);
}

/// Capability `durable`: storage outlives the repository handles over it, so
/// a new handle plays a restarted process.
pub trait Durable: RepositoryBackend {
    /// Like [`RepositoryBackend::open`], with storage [`Durable::reopen`] can
    /// reach again.
    async fn open_durable(case: &str) -> Option<Self>;
    /// A new repository over the same storage, as a restarted process.
    async fn reopen(&self) -> Self::Repository;
}

/// The in-memory repository.
pub struct InMemory(InMemoryRepository);

impl RepositoryBackend for InMemory {
    type Repository = InMemoryRepository;
    type Outbox = InMemoryOutboxStore;

    async fn open(_case: &str) -> Option<Self> {
        Some(Self(InMemoryRepository::new()))
    }

    fn repository(&self) -> InMemoryRepository {
        self.0.clone()
    }

    fn outbox_store(repository: &InMemoryRepository) -> InMemoryOutboxStore {
        repository.outbox_store()
    }
}

/// `B` behind a [`QueuedRepository`]: the wrapper must pass the whole
/// contract of the repository it wraps, so it is run on every backend.
pub struct Queued<B>(B);

impl<B: RepositoryBackend> RepositoryBackend for Queued<B> {
    type Repository = QueuedRepository<B::Repository>;
    type Outbox = B::Outbox;

    async fn open(case: &str) -> Option<Self> {
        B::open(case).await.map(Self)
    }

    fn repository(&self) -> Self::Repository {
        self.0.repository().queued()
    }

    fn outbox_store(repository: &Self::Repository) -> B::Outbox {
        B::outbox_store(repository.inner())
    }
}

impl<B: SqlFaults> SqlFaults for Queued<B> {
    async fn reject_negative(&self, table: &str, column: &str) {
        self.0.reject_negative(table, column).await
    }

    async fn insert_event_with_unsupported_codec(&self, aggregate_type: &str, aggregate_id: &str) {
        self.0
            .insert_event_with_unsupported_codec(aggregate_type, aggregate_id)
            .await
    }

    async fn insert_outbox_row_with_trace_columns(&self, message_id: &str) {
        self.0
            .insert_outbox_row_with_trace_columns(message_id)
            .await
    }
}

impl<B: Durable> Durable for Queued<B> {
    async fn open_durable(case: &str) -> Option<Self> {
        B::open_durable(case).await.map(Self)
    }

    async fn reopen(&self) -> Self::Repository {
        self.0.reopen().await.queued()
    }
}

#[cfg(feature = "sqlite")]
pub use self::sqlite::Sqlite;

#[cfg(feature = "sqlite")]
mod sqlite {
    use distributed::{SqliteOutboxStore, SqliteRepository};

    use super::{read_models, Durable, RepositoryBackend, SqlFaults};
    use crate::sqlite::TempDb;

    /// SQLite: an in-memory database, or a temp file for [`Durable`].
    pub struct Sqlite {
        repository: SqliteRepository,
        file: Option<TempDb>,
    }

    async fn bootstrapped(repository: SqliteRepository) -> SqliteRepository {
        repository
            .bootstrap_table_schema_for_dev(&read_models::conformance_table_registry())
            .await
            .expect("conformance tables should bootstrap");
        repository
    }

    async fn execute(repository: &SqliteRepository, sql: String) {
        sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
            .execute(repository.pool())
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }

    impl RepositoryBackend for Sqlite {
        type Repository = SqliteRepository;
        type Outbox = SqliteOutboxStore;

        async fn open(_case: &str) -> Option<Self> {
            let repository = SqliteRepository::connect_and_migrate("sqlite::memory:")
                .await
                .expect("sqlite repository should migrate");
            Some(Self {
                repository: bootstrapped(repository).await,
                file: None,
            })
        }

        fn repository(&self) -> SqliteRepository {
            self.repository.clone()
        }

        fn outbox_store(repository: &SqliteRepository) -> SqliteOutboxStore {
            repository.outbox_store()
        }
    }

    impl Durable for Sqlite {
        async fn open_durable(case: &str) -> Option<Self> {
            let file = TempDb::new(&format!("conformance_{case}"));
            let repository = SqliteRepository::new(file.pool().await);
            repository
                .migrate()
                .await
                .expect("sqlite repository should migrate");
            Some(Self {
                repository: bootstrapped(repository).await,
                file: Some(file),
            })
        }

        async fn reopen(&self) -> SqliteRepository {
            let file = self
                .file
                .as_ref()
                .expect("reopen needs a backend from `open_durable`");
            SqliteRepository::new(file.pool().await)
        }
    }

    impl SqlFaults for Sqlite {
        async fn reject_negative(&self, table: &str, column: &str) {
            // SQLite cannot add a CHECK to an existing table; a trigger the
            // engine runs inside the transaction is the same fault.
            execute(
                &self.repository,
                format!(
                    "CREATE TRIGGER reject_negative_{table}_{column} BEFORE INSERT ON \"{table}\" \
                     WHEN NEW.\"{column}\" < 0 \
                     BEGIN SELECT RAISE(ABORT, '{column} must be non-negative'); END"
                ),
            )
            .await;
        }

        async fn insert_event_with_unsupported_codec(&self, aggregate_type: &str, id: &str) {
            sqlx::query(
                "INSERT INTO aggregate_events (aggregate_type, aggregate_id, sequence, event_name, \
                 event_version, payload, payload_codec, payload_codec_version, metadata, \
                 recorded_at) \
                 VALUES (?, ?, 1, 'BadEvent', 1, x'00', 'json', 1, '{}', 0)",
            )
            .bind(aggregate_type)
            .bind(id)
            .execute(self.repository.pool())
            .await
            .unwrap();
        }

        async fn insert_outbox_row_with_trace_columns(&self, message_id: &str) {
            sqlx::query(
                "INSERT INTO outbox_messages (message_id, event_type, payload, payload_codec, \
                 payload_codec_version, metadata, status, created_at, next_available_at, \
                 attempts, correlation_id, causation_id) \
                 VALUES (?, 'OutboxColumns', x'00', 'bytes', 1, '{}', 'pending', \
                 0, 0, 0, 'corr-column', 'cause-column')",
            )
            .bind(message_id)
            .execute(self.repository.pool())
            .await
            .unwrap();
        }
    }
}

#[cfg(feature = "postgres")]
pub use self::postgres::Postgres;

#[cfg(feature = "postgres")]
mod postgres {
    use distributed::{PostgresOutboxStore, PostgresRepository};

    use super::{read_models, Durable, RepositoryBackend, SqlFaults};
    use crate::postgres::PostgresTestSchema;

    /// Postgres: a fresh schema of `DATABASE_URL` per test.
    pub struct Postgres {
        repository: PostgresRepository,
        schema: PostgresTestSchema,
    }

    impl RepositoryBackend for Postgres {
        type Repository = PostgresRepository;
        type Outbox = PostgresOutboxStore;

        async fn open(case: &str) -> Option<Self> {
            let schema = PostgresTestSchema::create_from_env("conformance", case).await?;
            let repository = schema.repository().await;
            repository
                .bootstrap_table_schema_for_dev(&read_models::conformance_table_registry())
                .await
                .expect("conformance tables should bootstrap");
            Some(Self { repository, schema })
        }

        fn repository(&self) -> PostgresRepository {
            self.repository.clone()
        }

        fn outbox_store(repository: &PostgresRepository) -> PostgresOutboxStore {
            repository.outbox_store()
        }
    }

    impl Durable for Postgres {
        async fn open_durable(case: &str) -> Option<Self> {
            Self::open(case).await
        }

        async fn reopen(&self) -> PostgresRepository {
            self.schema.repository().await
        }
    }

    impl SqlFaults for Postgres {
        async fn reject_negative(&self, table: &str, column: &str) {
            let sql = format!(
                "ALTER TABLE \"{table}\" ADD CONSTRAINT reject_negative_{column} \
                 CHECK (\"{column}\" >= 0)"
            );
            sqlx::query(sqlx::AssertSqlSafe(sql))
                .execute(self.repository.pool())
                .await
                .unwrap();
        }

        async fn insert_event_with_unsupported_codec(&self, aggregate_type: &str, id: &str) {
            sqlx::query(
                "INSERT INTO aggregate_events (aggregate_type, aggregate_id, sequence, event_name, \
                 event_version, payload, payload_codec, payload_codec_version, metadata, \
                 recorded_at) \
                 VALUES ($1, $2, 1, 'BadEvent', 1, $3, 'json', 1, '{}'::jsonb, 0)",
            )
            .bind(aggregate_type)
            .bind(id)
            .bind(vec![0_u8])
            .execute(self.repository.pool())
            .await
            .unwrap();
        }

        async fn insert_outbox_row_with_trace_columns(&self, message_id: &str) {
            sqlx::query(
                "INSERT INTO outbox_messages (message_id, event_type, payload, payload_codec, \
                 payload_codec_version, metadata, status, created_at, next_available_at, \
                 attempts, correlation_id, causation_id) \
                 VALUES ($1, 'OutboxColumns', decode('00', 'hex'), 'bytes', 1, '{}'::jsonb, \
                 'pending', 0, 0, 0, 'corr-column', 'cause-column')",
            )
            .bind(message_id)
            .execute(self.repository.pool())
            .await
            .unwrap();
        }
    }
}

/// Expands every repository conformance case as a `#[tokio::test]` for
/// `$backend`, a [`RepositoryBackend`]. Each capability must be stated; a
/// `false` capability leaves out the cases that need it. `queued: true`
/// states that `$backend`'s repository is a [`QueuedRepository`], whose
/// locking reads serialize racers instead of letting them conflict, so it
/// runs the queued cases in place of the optimistic race.
///
/// ```ignore
/// common::repository_conformance_tests!(common::repository::Sqlite, sql_faults: true, durable: true, queued: false);
/// ```
#[macro_export]
macro_rules! repository_conformance_tests {
    ($backend:ty, sql_faults: $sql_faults:tt, durable: $durable:tt, queued: $queued:tt $(,)?) => {
        $crate::conformance_cases! { $crate::repository::RepositoryBackend, $backend, open;
            #[tokio::test]
            snapshotted_reads_hydrate_snapshot_then_tail(b) =>
                $crate::repository::snapshots::snapshotted_reads_hydrate_snapshot_then_tail(b.repository());
            #[tokio::test]
            stream_tail_of_missing_stream_is_none_and_past_bound_clamps(b) =>
                $crate::repository::snapshots::stream_tail_of_missing_stream_is_none_and_past_bound_clamps(b.repository());
            #[tokio::test]
            aggregate_checkout_flow_persists_reloaded_state(b) =>
                $crate::repository::scenario::aggregate_checkout_flow_persists_reloaded_state(b.repository());
            #[tokio::test]
            get_all_and_commit_all_round_trip(b) =>
                $crate::repository::scenario::get_all_and_commit_all_round_trip(b.repository());
            #[tokio::test]
            multi_stream_conflict_rolls_back_other_stream_and_snapshot(b) =>
                $crate::repository::scenario::multi_stream_conflict_rolls_back_other_stream_and_snapshot(b.repository());
            #[tokio::test]
            duplicate_stream_identity_is_rejected_before_write(b) =>
                $crate::repository::scenario::duplicate_stream_identity_is_rejected_before_write(b.repository());
            #[tokio::test]
            metadata_round_trips(b) =>
                $crate::repository::scenario::metadata_round_trips(b.repository());
            #[tokio::test]
            unsupported_codec_is_rejected_on_write(b) =>
                $crate::repository::scenario::unsupported_codec_is_rejected_on_write(b.repository());
            #[tokio::test]
            snapshots_use_full_stream_identity(b) =>
                $crate::repository::scenario::snapshots_use_full_stream_identity(b.repository());
            #[tokio::test]
            aggregate_stream_round_trips_with_metadata(b) =>
                $crate::repository::storage::aggregate_stream_round_trips_with_metadata(b.repository());
            #[tokio::test]
            stream_identity_separates_same_id_across_types(b) =>
                $crate::repository::storage::stream_identity_separates_same_id_across_types(b.repository());
            #[tokio::test]
            commit_batch_with_1000_events_round_trips(b) =>
                $crate::repository::storage::commit_batch_with_1000_events_round_trips(b.repository());
            #[tokio::test]
            standalone_relational_write_plan_persists_row(b) =>
                $crate::repository::read_models::standalone_relational_write_plan_persists_row(b.repository());
            #[tokio::test]
            write_plan_lowers_relational_read_model_into_registered_table(b) =>
                $crate::repository::storage::write_plan_lowers_relational_read_model_into_registered_table(b.repository());
            #[tokio::test]
            read_model_plan_patches_and_deletes_relational_rows(b) =>
                $crate::repository::storage::read_model_plan_patches_and_deletes_relational_rows(b.repository());
            #[tokio::test]
            high_level_outbox_commit_persists_row_without_stream(b) =>
                $crate::repository::outbox::high_level_outbox_commit_persists_row_without_stream(b.repository(), b.outbox());
            #[tokio::test]
            duplicate_outbox_insert_rolls_back_aggregate(b) =>
                $crate::repository::outbox::duplicate_outbox_insert_rolls_back_aggregate(b.repository());
            #[tokio::test]
            aggregate_conflict_rolls_back_outbox(b) =>
                $crate::repository::outbox::aggregate_conflict_rolls_back_outbox(b.repository(), b.outbox());
            #[tokio::test]
            worker_claim_complete_and_retry_lifecycle(b) =>
                $crate::repository::outbox::worker_claim_complete_and_retry_lifecycle(b.repository(), b.outbox());
            #[tokio::test]
            worker_claim_by_ids_claims_only_requested(b) =>
                $crate::repository::outbox::worker_claim_by_ids_claims_only_requested(b.repository(), b.outbox());
            #[tokio::test]
            expired_outbox_lease_is_reclaimed_by_second_worker(b) =>
                $crate::repository::outbox::expired_outbox_lease_is_reclaimed_by_second_worker(b.repository(), b.outbox());
            #[tokio::test]
            publish_failure_after_commit_retains_outbox_row_until_delivered(b) =>
                $crate::repository::outbox::publish_failure_after_commit_retains_outbox_row_until_delivered(b.repository(), b.outbox());
            #[tokio::test]
            broker_outage_keeps_the_backlog_pending_with_growing_delay(b) =>
                $crate::repository::outbox::broker_outage_keeps_the_backlog_pending_with_growing_delay(b.repository(), b.outbox());
            #[tokio::test]
            failed_row_is_requeued_and_delivered(b) =>
                $crate::repository::outbox::failed_row_is_requeued_and_delivered(b.repository(), b.outbox());
            #[tokio::test]
            worker_completes_claims_in_one_batch(b) =>
                $crate::repository::outbox::worker_completes_claims_in_one_batch(b.repository(), b.outbox());
            #[tokio::test]
            outbox_backlog_stats_report_the_oldest_pending_message(b) =>
                $crate::repository::storage::outbox_backlog_stats_report_the_oldest_pending_message(b.repository(), b.outbox());
            #[tokio::test]
            consumer_inbox_records_dedupes_and_fences_with_real_effects(b) =>
                $crate::repository::inbox::inbox_records_dedupes_and_fences_with_real_effects(b.repository(), b.outbox());
            #[tokio::test]
            consumer_inbox_rejects_empty_receipt(b) =>
                $crate::repository::inbox::inbox_rejects_empty_receipt(b.repository());
        }
        $crate::conformance_cases! { @if $sql_faults $crate::repository::RepositoryBackend, $backend, open;
            #[tokio::test]
            read_model_failure_mid_plan_rolls_back_the_whole_plan(b) =>
                $crate::repository::storage::read_model_failure_mid_plan_rolls_back_the_whole_plan(&b);
            #[tokio::test]
            unsupported_codec_rows_fail_on_read(b) =>
                $crate::repository::storage::unsupported_codec_rows_fail_on_read(&b);
            #[tokio::test]
            outbox_trace_columns_round_trip_into_message_metadata(b) =>
                $crate::repository::storage::outbox_trace_columns_round_trip_into_message_metadata(&b);
        }
        $crate::conformance_cases! { @unless $queued $crate::repository::RepositoryBackend, $backend, open;
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            racing_commits_one_wins_one_conflicts(b) =>
                $crate::repository::scenario::racing_commits_one_wins_one_conflicts(b.repository(), 8);
        }
        $crate::conformance_cases! { @if $queued $crate::repository::RepositoryBackend, $backend, open;
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            queued_racing_commits_take_turns(b) =>
                $crate::repository::scenario::queued_racing_commits_take_turns(b.repository(), 8);
            #[tokio::test]
            queued_snapshotted_reads_hydrate_snapshot_then_tail(b) =>
                $crate::repository::snapshots::queued_snapshotted_reads_hydrate_snapshot_then_tail(b.repository());
        }
        $crate::conformance_cases! { @if $durable $crate::repository::Durable, $backend, open_durable;
            #[tokio::test]
            released_row_keeps_its_backoff_across_restart(b) =>
                $crate::repository::storage::released_row_keeps_its_backoff_across_restart(&b);
        }
    };
}
