use super::*;
use std::time::{Duration, SystemTime};

use uuid::{Uuid, Variant};

use super::ids::COMMAND_REPLAY_VERSION;
use crate::entity::Entity;
use crate::microsvc::HasOutboxStore;
use crate::outbox::{OutboxMessage, OutboxMessageStatus};
use crate::outbox_worker::OutboxStore;
use crate::projection_protocol::{
    ProjectionChange, ProjectionChangeCursor, ProjectionChangeKind, ProjectionEpoch,
    ProjectionObservation, ProjectionObservationKind, ProjectionPartition,
    ProjectionRecordMetadata, ProjectionRecordScope, ProjectorTopologyId, RecordRevision,
    ResolvedProjectionKey, ResolvedProjectionKeyField, ResolvedProjectionObligation,
    SameTransactionProjectionEvidence,
};
use crate::read_model::{ReadModelWritePlanBuilder, RelationalReadModel};
use crate::repository::{
    CommitBatch, GetStream, InboxReceipt, InboxStore, RelationalReadModelQueryStore, SnapshotStore,
    SnapshotWrite, StreamIdentity, StreamWrite,
};
use crate::snapshot::SnapshotRecord;
use crate::table::{
    ColumnType, PrimaryKey, RowKey, RowValue, RowValues, TableColumn, TableKind, TableSchema,
    TableSchemaRegistry, TableStoreError,
};

#[derive(Clone, Debug)]
struct LedgerConformanceView {
    id: String,
    marker: String,
}

impl RelationalReadModel for LedgerConformanceView {
    fn schema() -> &'static TableSchema {
        static SCHEMA: std::sync::LazyLock<TableSchema> =
            std::sync::LazyLock::new(|| TableSchema {
                model_name: "LedgerConformanceView".into(),
                table_name: "command_ledger_conformance_views".into(),
                columns: vec![
                    TableColumn {
                        primary_key: true,
                        ..TableColumn::new("id", "id", ColumnType::Text)
                    },
                    TableColumn::new("marker", "marker", ColumnType::Text),
                ],
                primary_key: PrimaryKey::new(["id"]),
                version_column: Some(crate::table::DEFAULT_TABLE_VERSION_COLUMN.into()),
                foreign_keys: Vec::new(),
                indexes: Vec::new(),
                relationships: Vec::new(),
                kind: TableKind::ReadModel,
            });
        &SCHEMA
    }

    fn primary_key(&self) -> Result<RowKey, TableStoreError> {
        Ok(RowKey::new([("id", RowValue::String(self.id.clone()))]))
    }

    fn to_row(&self) -> Result<RowValues, TableStoreError> {
        let mut row = RowValues::new();
        row.insert("id", RowValue::String(self.id.clone()));
        row.insert("marker", RowValue::String(self.marker.clone()));
        Ok(row)
    }

    fn from_row(row: RowValues) -> Result<Self, TableStoreError> {
        Ok(Self {
            id: row.get_serde("id")?,
            marker: row.get_serde("marker")?,
        })
    }
}

fn conformance_table_registry() -> TableSchemaRegistry {
    let mut registry = TableSchemaRegistry::new();
    registry
        .register::<LedgerConformanceView>()
        .expect("ledger conformance schema should register");
    registry
}

fn resolved_obligation(marker: &str) -> ResolvedProjectionObligation {
    let projector = format!("projector-{marker}");
    let topology =
        crate::projection_protocol::ProjectorTopologyId::new(1, &projector, [7; 32]).unwrap();
    let partition =
        crate::projection_protocol::ProjectionPartition::new(b"test-partition".to_vec()).unwrap();
    let scope = crate::projection_protocol::ProjectionRecordScope::new(
        topology,
        partition,
        "LedgerConformanceView",
        format!("key-{marker}").into_bytes(),
    )
    .unwrap();
    ResolvedProjectionObligation {
        projector,
        model: "LedgerConformanceView".into(),
        key: ResolvedProjectionKey {
            fields: vec![ResolvedProjectionKeyField {
                field: "id".into(),
                value: serde_json::json!({"wire": marker, "wide": "18446744073709551615"}),
            }],
        },
        partition: Some(serde_json::Value::Null),
        scope,
    }
}

fn direct_projection_evidence(marker: &str) -> SameTransactionProjectionEvidence {
    let topology = ProjectorTopologyId::new(1, "ledger-direct-projector", [0x42; 32]).unwrap();
    let partition = ProjectionPartition::new(format!("partition:{marker}")).unwrap();
    let epoch = ProjectionEpoch::new("ledger-direct-v1").unwrap();
    let scope = ProjectionRecordScope::new(
        topology.clone(),
        partition.clone(),
        "LedgerConformanceView",
        format!("key:{marker}"),
    )
    .unwrap();
    let revision = RecordRevision::new(scope.clone(), 1, 1).unwrap();
    let cursor = ProjectionChangeCursor::new(topology, partition, epoch, 1).unwrap();
    let record = ProjectionRecordMetadata {
        source_snapshot: None,
        revision: revision.clone(),
        tombstone: false,
        change: cursor.clone(),
    };
    let change = ProjectionChange {
        cursor: cursor.clone(),
        kind: ProjectionChangeKind::RecordUpsert,
        causation_id: format!("cause:{marker}"),
        observation_kind: None,
        scope: Some(scope.clone()),
        revision: Some(revision.clone()),
        failure_id: None,
    };
    let observation = ProjectionObservation {
        causation_id: format!("cause:{marker}"),
        kind: ProjectionObservationKind::Record,
        revision: Some(revision),
        scope,
        change: cursor,
    };
    SameTransactionProjectionEvidence {
        records: vec![record],
        changes: vec![change],
        observations: vec![observation],
    }
}

fn attach_test_direct_projection(
    mut completion: CommandCompletion,
    marker: &str,
) -> CommandCompletion {
    completion
        .attach_direct_projection(&direct_projection_evidence(marker))
        .unwrap();
    completion
}

fn fresh_attempt() -> CommandAttempt {
    let request = reservation(&Uuid::now_v7().to_string(), 1, 2).unwrap();
    CommandLedgerRecord::initial(&request, SystemTime::now())
        .unwrap()
        .acquired_attempt()
        .unwrap()
}

fn completed_replay_record(
    state: CommandLedgerState,
    obligations: Vec<ResolvedProjectionObligation>,
) -> CommandLedgerRecord {
    let request = reservation(&Uuid::now_v7().to_string(), 1, 2).unwrap();
    let started = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    let mut row = CommandLedgerRecord::initial(&request, started).unwrap();
    row.state = state;
    row.attempt_token = None;
    row.lease_expires_at = None;
    row.outcome_json = Some(
        serde_json::json!({
            "version": COMMAND_REPLAY_VERSION,
            "outcome": {"ok": true},
            "projection_obligations": obligations,
        })
        .to_string(),
    );
    row.updated_at = started + Duration::from_secs(1);
    row.completed_at = Some(started + Duration::from_secs(1));
    row
}

fn reservation_for_partition(
    command_id: &str,
    principal_partition: &str,
    command_name: &str,
    contract: u8,
    input: u8,
) -> Result<CommandReservation, CommandLedgerError> {
    reservation_for_partition_with_policy(
        command_id,
        principal_partition,
        command_name,
        contract,
        input,
        Duration::from_secs(30),
        Duration::from_secs(300),
    )
}

fn reservation_for_partition_with_policy(
    command_id: &str,
    principal_partition: &str,
    command_name: &str,
    contract: u8,
    input: u8,
    lease: Duration,
    retention: Duration,
) -> Result<CommandReservation, CommandLedgerError> {
    CommandReservation::new(
        CommandLedgerKey::new(
            "orders",
            PrincipalPartitionId::new(principal_partition)?,
            CommandId::parse(command_id)?,
        )?,
        command_name,
        CommandContractFingerprint::new([contract; 32]),
        CanonicalInputHash::new([input; 32]),
        lease,
        retention,
    )
}

fn reservation(
    command_id: &str,
    contract: u8,
    input: u8,
) -> Result<CommandReservation, CommandLedgerError> {
    reservation_for_partition(
        command_id,
        "v1:sha256:principal",
        "order.create",
        contract,
        input,
    )
}

trait CommandLedgerAdapterConformance:
    CommandLedgerStore
    + CausalTransactionalCommit
    + GetStream
    + SnapshotStore
    + InboxStore
    + RelationalReadModelQueryStore
    + HasOutboxStore
{
}

impl<T> CommandLedgerAdapterConformance for T where
    T: CommandLedgerStore
        + CausalTransactionalCommit
        + GetStream
        + SnapshotStore
        + InboxStore
        + RelationalReadModelQueryStore
        + HasOutboxStore
{
}

async fn acquire<R>(repo: &R, request: CommandReservation) -> CommandAttempt
where
    R: CommandLedgerStore,
{
    match repo.reserve_command(request).await.unwrap() {
        ReservationOutcome::Acquired(attempt) => attempt,
        other => panic!("expected acquired command attempt, got {other:?}"),
    }
}

async fn same_input_retries_and_identity_conflicts_conform<R>(repo: &R)
where
    R: CommandLedgerAdapterConformance,
{
    let id = Uuid::now_v7().to_string();
    let request = reservation(&id, 11, 12).unwrap();
    let key = request.key().clone();
    let attempt = acquire(repo, request).await;
    let causation = attempt.causation_id().clone();

    match repo
        .reserve_command(reservation(&id, 11, 12).unwrap())
        .await
        .unwrap()
    {
        ReservationOutcome::InProgress { causation_id } => {
            assert_eq!(causation_id, causation)
        }
        other => panic!("same-input retry should remain in progress, got {other:?}"),
    }
    assert!(matches!(
        repo.reserve_command(reservation(&id, 11, 99).unwrap())
            .await
            .unwrap(),
        ReservationOutcome::Conflict
    ));
    assert!(matches!(
        repo.reserve_command(
            reservation_for_partition(&id, "v1:sha256:principal", "order.cancel", 11, 12,).unwrap(),
        )
        .await
        .unwrap(),
        ReservationOutcome::Conflict
    ));

    let expected_outcome = serde_json::json!({"order_id": "same-input"});
    let completion = attempt
        .complete(
            TerminalCommandState::Succeeded,
            expected_outcome.clone(),
            Duration::from_secs(300),
        )
        .unwrap();
    repo.commit_causal_batch(CausalCommitBatch::new(CommitBatch::empty(), completion))
        .await
        .unwrap();

    let reserved_replay = match repo
        .reserve_command(reservation(&id, 11, 12).unwrap())
        .await
        .unwrap()
    {
        ReservationOutcome::Replay(replay) => replay,
        other => panic!("completed same-input retry should replay, got {other:?}"),
    };
    assert_eq!(
        repo.lookup_command(&key, CommandLookupScope::CommandName("order.cancel"))
            .await
            .unwrap(),
        CommandLookup::Unknown,
        "status lookup must not disclose a command owned by a different route"
    );
    let wrong_contract = [99; SHA256_BYTES];
    assert_eq!(
        repo.lookup_command(
            &key,
            CommandLookupScope::CommandContract {
                command_name: "order.create",
                contract_fingerprint: &wrong_contract,
            },
        )
        .await
        .unwrap(),
        CommandLookup::Unknown,
        "status lookup must not disclose a command owned by a drifted contract"
    );
    let current_contract = [11; SHA256_BYTES];
    let contract_lookup = repo
        .lookup_command(
            &key,
            CommandLookupScope::CommandContract {
                command_name: "order.create",
                contract_fingerprint: &current_contract,
            },
        )
        .await
        .unwrap();
    let lookup_replay = match repo
        .lookup_command(&key, CommandLookupScope::CommandName("order.create"))
        .await
        .unwrap()
    {
        CommandLookup::Replay(replay) => replay,
        other => panic!("completed command lookup should replay, got {other:?}"),
    };
    assert_eq!(
        contract_lookup,
        CommandLookup::Replay(lookup_replay.clone())
    );
    assert_eq!(reserved_replay, lookup_replay);
    assert_eq!(reserved_replay.state, CommandLedgerState::Succeeded);
    assert_eq!(reserved_replay.causation_id, causation);
    assert_eq!(reserved_replay.outcome, expected_outcome);
    assert!(reserved_replay.projection_obligations.is_empty());
}

async fn concurrent_reservations_have_one_winner_and_one_causation<R>(repo: &R)
where
    R: CommandLedgerAdapterConformance,
{
    let id = Uuid::now_v7().to_string();
    let left = reservation(&id, 16, 17).unwrap();
    let right = reservation(&id, 16, 17).unwrap();
    let (left, right) = tokio::join!(repo.reserve_command(left), repo.reserve_command(right));
    let outcomes = (left.unwrap(), right.unwrap());
    let (winner, observed_causation) = match outcomes {
        (ReservationOutcome::Acquired(winner), ReservationOutcome::InProgress { causation_id })
        | (ReservationOutcome::InProgress { causation_id }, ReservationOutcome::Acquired(winner)) => {
            (winner, causation_id)
        }
        other => panic!(
            "concurrent reservations should have one winner and one in-progress observer, got {other:?}"
        ),
    };
    assert_eq!(winner.causation_id(), &observed_causation);

    let completion = winner
        .complete(
            TerminalCommandState::Succeeded,
            serde_json::json!({"concurrent_winner": true}),
            Duration::from_secs(300),
        )
        .unwrap();
    repo.commit_causal_batch(CausalCommitBatch::new(CommitBatch::empty(), completion))
        .await
        .unwrap();
}

async fn expired_lease_reclaims_through_the_adapter_clock<R>(repo: &R)
where
    R: CommandLedgerAdapterConformance,
{
    let id = Uuid::now_v7().to_string();
    let short_lease = reservation_for_partition_with_policy(
        &id,
        "v1:sha256:principal",
        "order.create",
        18,
        19,
        Duration::from_millis(100),
        Duration::from_secs(300),
    )
    .unwrap();
    let first = acquire(repo, short_lease).await;
    let causation = first.causation_id().clone();
    let first_token = first.attempt_token().as_str().to_string();

    tokio::time::sleep(Duration::from_millis(300)).await;
    let second = acquire(repo, reservation(&id, 18, 19).unwrap()).await;
    assert_eq!(second.causation_id(), &causation);
    assert_eq!(second.attempt_number(), 2);
    assert_ne!(second.attempt_token().as_str(), first_token);

    let completion = second
        .complete(
            TerminalCommandState::Succeeded,
            serde_json::json!({"lease_reclaimed": true}),
            Duration::from_secs(300),
        )
        .unwrap();
    repo.commit_causal_batch(CausalCommitBatch::new(CommitBatch::empty(), completion))
        .await
        .unwrap();
}

async fn terminal_replays_are_deterministic<R>(repo: &R)
where
    R: CommandLedgerAdapterConformance,
{
    let terminal_states = [
        (
            TerminalCommandState::Succeeded,
            CommandLedgerState::Succeeded,
        ),
        (
            TerminalCommandState::SucceededPendingProjection,
            CommandLedgerState::SucceededPendingProjection,
        ),
        (TerminalCommandState::Atomic, CommandLedgerState::Atomic),
        (TerminalCommandState::Rejected, CommandLedgerState::Rejected),
    ];

    for (index, (terminal_state, ledger_state)) in terminal_states.into_iter().enumerate() {
        let id = Uuid::now_v7().to_string();
        let request = reservation(&id, 21, index as u8 + 1).unwrap();
        let key = request.key().clone();
        let attempt = acquire(repo, request).await;
        let causation = attempt.causation_id().clone();
        let expected_outcome = serde_json::json!({
            "terminal": ledger_state.as_str(),
            "index": index,
        });
        let expected_obligations =
            if terminal_state == TerminalCommandState::SucceededPendingProjection {
                vec![resolved_obligation(&format!("terminal-{index}"))]
            } else {
                Vec::new()
            };
        let mut completion = attempt
            .complete_with_obligations(
                terminal_state,
                expected_outcome.clone(),
                expected_obligations.clone(),
                Duration::from_secs(300),
            )
            .unwrap();
        let expected_direct_projection =
            (terminal_state == TerminalCommandState::Atomic).then(|| {
                let evidence = direct_projection_evidence(&format!("terminal-{index}"));
                completion.attach_direct_projection(&evidence).unwrap();
                evidence.replay_value()
            });
        repo.commit_causal_batch(CausalCommitBatch::new(CommitBatch::empty(), completion))
            .await
            .unwrap();

        let first = match repo
            .lookup_command(&key, CommandLookupScope::CommandName("order.create"))
            .await
            .unwrap()
        {
            CommandLookup::Replay(replay) => replay,
            other => panic!("terminal lookup should replay, got {other:?}"),
        };
        let second = match repo
            .reserve_command(reservation(&id, 21, index as u8 + 1).unwrap())
            .await
            .unwrap()
        {
            ReservationOutcome::Replay(replay) => replay,
            other => panic!("terminal reservation should replay, got {other:?}"),
        };
        assert_eq!(first, second);
        assert_eq!(first.state, ledger_state);
        assert_eq!(first.causation_id, causation);
        assert_eq!(first.outcome, expected_outcome);
        assert_eq!(first.projection_obligations, expected_obligations);
        assert_eq!(first.direct_projection, expected_direct_projection);
    }
}

async fn response_loss_replays_outcome_and_projection_obligations<R>(repo: &R)
where
    R: CommandLedgerAdapterConformance,
{
    let id = Uuid::now_v7().to_string();
    let attempt = acquire(repo, reservation(&id, 31, 32).unwrap()).await;
    let causation = attempt.causation_id().clone();
    let expected_outcome = serde_json::json!({"order_id": "response-lost"});
    let expected_obligations = vec![resolved_obligation("response-loss")];
    let completion = attempt
        .complete_with_obligations(
            TerminalCommandState::SucceededPendingProjection,
            expected_outcome.clone(),
            expected_obligations.clone(),
            Duration::from_secs(300),
        )
        .unwrap();

    // Model a committed transaction whose HTTP/GraphQL acknowledgement was
    // lost: the caller knows only that it must retry the same command ID.
    repo.commit_causal_batch(CausalCommitBatch::new(CommitBatch::empty(), completion))
        .await
        .unwrap();

    match repo
        .reserve_command(reservation(&id, 31, 32).unwrap())
        .await
        .unwrap()
    {
        ReservationOutcome::Replay(replay) => {
            assert_eq!(replay.state, CommandLedgerState::SucceededPendingProjection);
            assert_eq!(replay.causation_id, causation);
            assert_eq!(replay.outcome, expected_outcome);
            assert_eq!(replay.projection_obligations, expected_obligations);
        }
        other => panic!("retry after response loss should replay, got {other:?}"),
    }
}

async fn retryable_unknown_reclaims_with_stable_causation<R>(repo: &R)
where
    R: CommandLedgerAdapterConformance,
{
    let id = Uuid::now_v7().to_string();
    let request = reservation(&id, 41, 42).unwrap();
    let key = request.key().clone();
    let first = acquire(repo, request).await;
    let causation = first.causation_id().clone();
    let first_token = first.attempt_token().as_str().to_string();
    repo.mark_retryable_unknown(first.fence()).await.unwrap();

    match repo
        .lookup_command(&key, CommandLookupScope::CommandName("order.create"))
        .await
        .unwrap()
    {
        CommandLookup::RetryableUnknown { causation_id } => {
            assert_eq!(causation_id, causation)
        }
        other => panic!("abandoned attempt should be retryable-unknown, got {other:?}"),
    }

    let second = acquire(repo, reservation(&id, 41, 42).unwrap()).await;
    assert_eq!(second.causation_id(), &causation);
    assert_eq!(second.attempt_number(), 2);
    assert_ne!(second.attempt_token().as_str(), first_token);
    let completion = second
        .complete(
            TerminalCommandState::Succeeded,
            serde_json::json!({"reclaimed": true}),
            Duration::from_secs(300),
        )
        .unwrap();
    repo.commit_causal_batch(CausalCommitBatch::new(CommitBatch::empty(), completion))
        .await
        .unwrap();
}

async fn principal_partitions_are_isolated<R>(repo: &R)
where
    R: CommandLedgerAdapterConformance,
{
    let id = Uuid::now_v7().to_string();
    let first_request =
        reservation_for_partition(&id, "v1:sha256:principal-a", "order.create", 51, 52).unwrap();
    let first_key = first_request.key().clone();
    let first = acquire(repo, first_request).await;
    let first_causation = first.causation_id().clone();
    let first_outcome = serde_json::json!({"partition": "a"});
    let completion = first
        .complete(
            TerminalCommandState::Succeeded,
            first_outcome.clone(),
            Duration::from_secs(300),
        )
        .unwrap();
    repo.commit_causal_batch(CausalCommitBatch::new(CommitBatch::empty(), completion))
        .await
        .unwrap();

    let second_request =
        reservation_for_partition(&id, "v1:sha256:principal-b", "order.create", 51, 52).unwrap();
    let second_key = second_request.key().clone();
    let second = acquire(repo, second_request).await;
    let second_causation = second.causation_id().clone();
    assert_ne!(second_causation, first_causation);
    let second_outcome = serde_json::json!({"partition": "b"});
    let completion = second
        .complete(
            TerminalCommandState::Rejected,
            second_outcome.clone(),
            Duration::from_secs(300),
        )
        .unwrap();
    repo.commit_causal_batch(CausalCommitBatch::new(CommitBatch::empty(), completion))
        .await
        .unwrap();

    let first_replay = match repo
        .lookup_command(&first_key, CommandLookupScope::CommandName("order.create"))
        .await
        .unwrap()
    {
        CommandLookup::Replay(replay) => replay,
        other => panic!("first principal should retain its replay, got {other:?}"),
    };
    let second_replay = match repo
        .lookup_command(&second_key, CommandLookupScope::CommandName("order.create"))
        .await
        .unwrap()
    {
        CommandLookup::Replay(replay) => replay,
        other => panic!("second principal should retain its replay, got {other:?}"),
    };
    assert_eq!(first_replay.state, CommandLedgerState::Succeeded);
    assert_eq!(first_replay.outcome, first_outcome);
    assert_eq!(first_replay.causation_id, first_causation);
    assert_eq!(second_replay.state, CommandLedgerState::Rejected);
    assert_eq!(second_replay.outcome, second_outcome);
    assert_eq!(second_replay.causation_id, second_causation);
}

async fn committed_events_and_outbox_round_trip_ledger_causation<R>(repo: &R)
where
    R: CommandLedgerAdapterConformance,
{
    let id = Uuid::now_v7().to_string();
    let request = reservation(&id, 56, 57).unwrap();
    let key = request.key().clone();
    let attempt = acquire(repo, request).await;
    let causation = attempt.causation_id().as_str().to_string();

    let aggregate_id = format!("ledger-causation-stream-{}", Uuid::now_v7());
    let identity = StreamIdentity::new("command-ledger-conformance", &aggregate_id).unwrap();
    let mut entity = Entity::with_id(&aggregate_id);
    entity.set_causation_id("handler-event-causation-must-be-replaced");
    entity.digest_empty("CommandLedgerCausationEvent").unwrap();

    let outbox_id = format!("ledger-causation-outbox-{}", Uuid::now_v7());
    let mut message = OutboxMessage::create(
        outbox_id.clone(),
        "CommandLedgerCausationFact",
        b"{}".to_vec(),
    )
    .unwrap();
    message.set_causation_id("handler-outbox-causation-must-be-replaced");

    let completion = attempt
        .complete(
            TerminalCommandState::Succeeded,
            serde_json::json!({"causation": "committed"}),
            Duration::from_secs(300),
        )
        .unwrap();
    let mut domain = CommitBatch::new(vec![StreamWrite::new(identity.clone(), &mut entity)]);
    domain.outbox_messages.push(message);
    repo.commit_causal_batch(CausalCommitBatch::new(domain, completion))
        .await
        .unwrap();

    let stored_stream = repo
        .get_stream(&identity)
        .await
        .unwrap()
        .expect("causal stream should persist");
    assert_eq!(stored_stream.events().len(), 1);
    assert_eq!(
        stored_stream.events()[0].causation_id(),
        Some(causation.as_str())
    );

    let stored_message = repo
        .outbox_store()
        .messages_by_status(OutboxMessageStatus::Pending, 1_000)
        .await
        .unwrap()
        .into_iter()
        .find(|message| message.id() == outbox_id)
        .expect("causal outbox message should persist");
    assert_eq!(stored_message.causation_id(), Some(causation.as_str()));
    match repo
        .lookup_command(&key, CommandLookupScope::CommandName("order.create"))
        .await
        .unwrap()
    {
        CommandLookup::Replay(replay) => {
            assert_eq!(replay.causation_id.as_str(), causation)
        }
        other => panic!("causal command should replay after commit, got {other:?}"),
    }
}

async fn stale_fence_rolls_back_every_commit_participant<R>(repo: &R)
where
    R: CommandLedgerAdapterConformance,
{
    let id = Uuid::now_v7().to_string();
    let key = reservation(&id, 61, 62).unwrap().key().clone();
    let first = acquire(repo, reservation(&id, 61, 62).unwrap()).await;
    repo.mark_retryable_unknown(first.fence()).await.unwrap();
    let second = acquire(repo, reservation(&id, 61, 62).unwrap()).await;
    let live_causation = second.causation_id().clone();

    let aggregate_id = format!("ledger-stale-stream-{}", Uuid::now_v7());
    let identity = StreamIdentity::new("command-ledger-conformance", &aggregate_id).unwrap();
    let mut entity = Entity::with_id(&aggregate_id);
    entity
        .digest_empty("CommandLedgerConformanceEvent")
        .unwrap();

    let outbox_id = format!("ledger-stale-outbox-{}", Uuid::now_v7());
    let outbox_message = OutboxMessage::create(
        outbox_id.clone(),
        "CommandLedgerConformanceFact",
        b"{}".to_vec(),
    )
    .unwrap();

    let read_model_id = format!("ledger-stale-view-{}", Uuid::now_v7());
    let view = LedgerConformanceView {
        id: read_model_id.clone(),
        marker: "must-roll-back".into(),
    };
    let mut read_models = ReadModelWritePlanBuilder::new();
    read_models.upsert(&view).unwrap();

    let inbox_consumer = format!("ledger-consumer-{}", Uuid::now_v7());
    let inbox_message_id = format!("ledger-message-{}", Uuid::now_v7());
    let snapshot = SnapshotRecord::new(
        identity.aggregate_type(),
        identity.aggregate_id(),
        1,
        1,
        vec![1, 2, 3],
    );

    let stale_completion = first
        .complete(
            TerminalCommandState::Succeeded,
            serde_json::json!({"winner": false}),
            Duration::from_secs(300),
        )
        .unwrap();
    let mut domain = CommitBatch::new(vec![StreamWrite::new(identity.clone(), &mut entity)]);
    domain.outbox_messages.push(outbox_message);
    domain
        .read_model_plans
        .push(read_models.into_write_plan().unwrap());
    domain.snapshots.push(SnapshotWrite::Save {
        identity: identity.clone(),
        record: snapshot,
    });
    domain.inbox_receipts.push(InboxReceipt::new(
        inbox_consumer.clone(),
        inbox_message_id.clone(),
    ));

    let stale_result = repo
        .commit_causal_batch(CausalCommitBatch::new(domain, stale_completion))
        .await;
    assert!(
        matches!(stale_result, Err(CommandLedgerError::AttemptFenced { .. })),
        "stale causal commit should be fenced after every participant is staged, got {stale_result:?}"
    );

    assert!(repo.get_stream(&identity).await.unwrap().is_none());
    let pending = repo
        .outbox_store()
        .messages_by_status(OutboxMessageStatus::Pending, 1_000)
        .await
        .unwrap();
    assert!(pending.iter().all(|message| message.id() != outbox_id));
    let load = ReadModelWritePlanBuilder::new()
        .load::<LedgerConformanceView>(RowKey::new([("id", RowValue::String(read_model_id))]))
        .unwrap();
    assert!(repo.load_graph(load).await.unwrap().root.is_none());
    assert!(repo.get_snapshot(&identity).await.unwrap().is_none());
    assert!(!repo
        .inbox_contains(&inbox_consumer, &inbox_message_id)
        .await
        .unwrap());
    match repo
        .lookup_command(&key, CommandLookupScope::CommandName("order.create"))
        .await
        .unwrap()
    {
        CommandLookup::InProgress { causation_id } => {
            assert_eq!(causation_id, live_causation)
        }
        other => panic!("stale commit must leave live attempt untouched, got {other:?}"),
    }

    let live_completion = second
        .complete(
            TerminalCommandState::Succeeded,
            serde_json::json!({"winner": true}),
            Duration::from_secs(300),
        )
        .unwrap();
    repo.commit_causal_batch(CausalCommitBatch::new(
        CommitBatch::empty(),
        live_completion,
    ))
    .await
    .unwrap();
}

async fn compacted_expiry_is_a_permanent_tombstone<R>(repo: &R)
where
    R: CommandLedgerAdapterConformance,
{
    let id = Uuid::now_v7().to_string();
    let request = reservation(&id, 71, 72).unwrap();
    let key = request.key().clone();
    let attempt = acquire(repo, request).await;
    let completion = attempt
        .complete(
            TerminalCommandState::Succeeded,
            serde_json::json!({"short_lived": true}),
            Duration::from_millis(100),
        )
        .unwrap();
    repo.commit_causal_batch(CausalCommitBatch::new(CommitBatch::empty(), completion))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(repo.compact_expired_commands(1_000).await.unwrap() >= 1);
    assert_eq!(
        repo.lookup_command(&key, CommandLookupScope::CommandName("order.create"))
            .await
            .unwrap(),
        CommandLookup::Expired
    );
    assert!(matches!(
        repo.reserve_command(reservation(&id, 71, 72).unwrap())
            .await
            .unwrap(),
        ReservationOutcome::Expired
    ));
    assert!(matches!(
        repo.reserve_command(reservation(&id, 99, 99).unwrap())
            .await
            .unwrap(),
        ReservationOutcome::Expired
    ));
}

async fn expired_modeled_metadata_deadline_cannot_commit<R>(repo: &R)
where
    R: CommandLedgerAdapterConformance,
{
    let id = Uuid::now_v7().to_string();
    let request = reservation(&id, 81, 82).unwrap();
    let key = request.key().clone();
    let attempt = acquire(repo, request).await;
    let causation_id = attempt.causation_id().clone();
    let completion = attempt
        .complete_with_projection_metadata_until(
            TerminalCommandState::SucceededPendingProjection,
            serde_json::json!({"ok": true}),
            br#"{"modeled":true}"#.to_vec(),
            Duration::from_secs(300),
            SystemTime::UNIX_EPOCH + Duration::from_secs(1),
        )
        .unwrap();
    assert!(repo
        .commit_causal_batch(CausalCommitBatch::new(CommitBatch::empty(), completion))
        .await
        .is_err());
    assert_eq!(
        repo.lookup_command(&key, CommandLookupScope::CommandName("order.create"))
            .await
            .unwrap(),
        CommandLookup::InProgress { causation_id }
    );
}

async fn run_command_ledger_adapter_conformance<R>(repo: &R)
where
    R: CommandLedgerAdapterConformance,
{
    same_input_retries_and_identity_conflicts_conform(repo).await;
    concurrent_reservations_have_one_winner_and_one_causation(repo).await;
    expired_lease_reclaims_through_the_adapter_clock(repo).await;
    terminal_replays_are_deterministic(repo).await;
    response_loss_replays_outcome_and_projection_obligations(repo).await;
    retryable_unknown_reclaims_with_stable_causation(repo).await;
    principal_partitions_are_isolated(repo).await;
    committed_events_and_outbox_round_trip_ledger_causation(repo).await;
    stale_fence_rolls_back_every_commit_participant(repo).await;
    compacted_expiry_is_a_permanent_tombstone(repo).await;
    expired_modeled_metadata_deadline_cannot_commit(repo).await;
}

#[test]
fn command_id_requires_uuid_v7_and_canonicalizes() {
    let id = Uuid::now_v7().simple().to_string().to_uppercase();
    let parsed = CommandId::parse(id).unwrap();
    assert_eq!(
        Uuid::parse_str(parsed.as_str()).unwrap().get_version_num(),
        7
    );
    assert!(CommandId::parse("67e55044-10b1-426f-9247-bb680e5fe0c8").is_err());
    assert!(CommandId::parse("not-a-uuid").is_err());
}

#[test]
fn uuid_v7_identities_require_the_rfc4122_variant() {
    let mut bytes = *Uuid::now_v7().as_bytes();
    bytes[8] &= 0x7f;
    let ncs_variant_v7 = Uuid::from_bytes(bytes);
    assert_eq!(ncs_variant_v7.get_version_num(), 7);
    assert_eq!(ncs_variant_v7.get_variant(), Variant::NCS);
    let spelling = ncs_variant_v7.hyphenated().to_string();

    assert!(matches!(
        CommandId::parse(&spelling),
        Err(CommandLedgerError::Invalid(_))
    ));
    assert!(matches!(
        CausationId::parse_stored(spelling.clone()),
        Err(CommandLedgerError::Corrupt(_))
    ));
    assert!(matches!(
        AttemptToken::parse_stored(spelling),
        Err(CommandLedgerError::Corrupt(_))
    ));

    let valid = Uuid::now_v7().to_string();
    assert!(CommandId::parse(&valid).is_ok());
    assert!(CausationId::parse_stored(valid.clone()).is_ok());
    assert!(AttemptToken::parse_stored(valid).is_ok());
}

#[test]
fn prefixed_sha256_parser_is_checked_and_canonical() {
    let encoded = format!("sha256:{}", "ab".repeat(32));
    assert_eq!(
        CanonicalInputHash::parse_sha256(&encoded)
            .unwrap()
            .as_bytes(),
        &[0xab; 32]
    );
    assert!(CanonicalInputHash::parse_sha256(&"ab".repeat(32)).is_err());
    assert!(
        CommandContractFingerprint::parse_sha256(&format!("sha256:{}", "AB".repeat(32))).is_err()
    );
    assert!(CommandContractFingerprint::parse_sha256("sha256:00").is_err());
}

#[test]
fn contract_and_input_hashes_are_distinct_identity_components() {
    let id = Uuid::now_v7().to_string();
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    let original = reservation(&id, 1, 2).unwrap();
    let row = CommandLedgerRecord::initial(&original, now).unwrap();

    let contract_drift = reservation(&id, 9, 2).unwrap();
    assert_eq!(
        row.classify_reservation(&contract_drift, now).unwrap(),
        ReservationDecision::Conflict
    );
    let input_drift = reservation(&id, 1, 9).unwrap();
    assert_eq!(
        row.classify_reservation(&input_drift, now).unwrap(),
        ReservationDecision::Conflict
    );
}

#[test]
fn reclaim_preserves_causation_and_rotates_attempt_fence() {
    let id = Uuid::now_v7().to_string();
    let initial = reservation(&id, 1, 2).unwrap();
    let started = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    let mut row = CommandLedgerRecord::initial(&initial, started).unwrap();
    let first_cause = row.causation_id.clone();
    let first_token = row.attempt_token.as_ref().unwrap().0.clone();

    let retry = reservation(&id, 1, 2).unwrap();
    let after_lease = started + Duration::from_secs(31);
    assert_eq!(
        row.classify_reservation(&retry, after_lease).unwrap(),
        ReservationDecision::Reclaim
    );
    row.reclaim(&retry, after_lease).unwrap();

    assert_eq!(row.causation_id, first_cause);
    assert_ne!(row.attempt_token.as_ref().unwrap().0, first_token);
    assert_eq!(row.attempt_number, 2);
}

#[test]
fn stale_attempt_cannot_complete_after_reclaim() {
    let id = Uuid::now_v7().to_string();
    let initial = reservation(&id, 1, 2).unwrap();
    let started = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    let mut row = CommandLedgerRecord::initial(&initial, started).unwrap();
    let stale = row.acquired_attempt().unwrap();

    let retry = reservation(&id, 1, 2).unwrap();
    row.reclaim(&retry, started + Duration::from_secs(31))
        .unwrap();
    let completion = stale
        .complete(
            TerminalCommandState::Succeeded,
            serde_json::json!({"ok": true}),
            Duration::from_secs(300),
        )
        .unwrap();

    assert!(matches!(
        row.complete(&completion, started + Duration::from_secs(32)),
        Err(CommandLedgerError::AttemptFenced { .. })
    ));
}

#[test]
fn completion_rejects_inconsistent_projection_obligation_states() {
    for state in [
        TerminalCommandState::Succeeded,
        TerminalCommandState::Atomic,
        TerminalCommandState::Rejected,
    ] {
        assert!(matches!(
            fresh_attempt().complete_with_obligations(
                state,
                serde_json::json!({"ok": true}),
                vec![resolved_obligation("unexpected")],
                Duration::from_secs(300),
            ),
            Err(CommandLedgerError::Invalid(_))
        ));
    }

    assert!(matches!(
        fresh_attempt().complete_with_obligations(
            TerminalCommandState::SucceededPendingProjection,
            serde_json::json!({"ok": true}),
            Vec::new(),
            Duration::from_secs(300),
        ),
        Err(CommandLedgerError::Invalid(_))
    ));

    for state in [
        TerminalCommandState::Succeeded,
        TerminalCommandState::Atomic,
        TerminalCommandState::Rejected,
    ] {
        assert!(fresh_attempt()
            .complete(
                state,
                serde_json::json!({"ok": true}),
                Duration::from_secs(300),
            )
            .is_ok());
    }
    assert!(fresh_attempt()
        .complete_with_obligations(
            TerminalCommandState::SucceededPendingProjection,
            serde_json::json!({"ok": true}),
            vec![resolved_obligation("pending")],
            Duration::from_secs(300),
        )
        .is_ok());
}

#[test]
fn completion_rejects_malformed_projection_obligations() {
    let mut blank_projector = resolved_obligation("blank-projector");
    blank_projector.projector = " \t".into();
    let mut blank_model = resolved_obligation("blank-model");
    blank_model.model = "\n".into();
    let mut empty_key = resolved_obligation("empty-key");
    empty_key.key.fields.clear();
    let mut blank_field = resolved_obligation("blank-field");
    blank_field.key.fields[0].field = "  ".into();
    let mut duplicate_field = resolved_obligation("duplicate-field");
    duplicate_field
        .key
        .fields
        .push(duplicate_field.key.fields[0].clone());

    for malformed in [
        blank_projector,
        blank_model,
        empty_key,
        blank_field,
        duplicate_field,
    ] {
        assert!(matches!(
            fresh_attempt().complete_with_obligations(
                TerminalCommandState::SucceededPendingProjection,
                serde_json::json!({"ok": true}),
                vec![malformed],
                Duration::from_secs(300),
            ),
            Err(CommandLedgerError::Invalid(_))
        ));
    }
}

#[test]
fn replay_rejects_inconsistent_projection_obligation_states() {
    for state in [
        CommandLedgerState::Succeeded,
        CommandLedgerState::Atomic,
        CommandLedgerState::Rejected,
    ] {
        let row = completed_replay_record(state, vec![resolved_obligation("unexpected")]);
        assert!(row.validate_stored_shape().is_ok());
        assert!(matches!(row.replay(), Err(CommandLedgerError::Corrupt(_))));
    }

    for state in [
        CommandLedgerState::SucceededPendingProjection,
        CommandLedgerState::ProjectionFailed,
    ] {
        let row = completed_replay_record(state, Vec::new());
        assert!(row.validate_stored_shape().is_ok());
        assert!(matches!(row.replay(), Err(CommandLedgerError::Corrupt(_))));
    }

    let projection_failed = completed_replay_record(
        CommandLedgerState::ProjectionFailed,
        vec![resolved_obligation("failed")],
    );
    let replay = projection_failed.replay().unwrap();
    assert_eq!(replay.state, CommandLedgerState::ProjectionFailed);
    assert_eq!(replay.projection_obligations.len(), 1);
}

#[test]
fn replay_rejects_malformed_projection_obligations() {
    let mut blank_projector = resolved_obligation("blank-projector");
    blank_projector.projector = " ".into();
    let mut blank_model = resolved_obligation("blank-model");
    blank_model.model.clear();
    let mut empty_key = resolved_obligation("empty-key");
    empty_key.key.fields.clear();
    let mut blank_field = resolved_obligation("blank-field");
    blank_field.key.fields[0].field = "\r\n".into();
    let mut duplicate_field = resolved_obligation("duplicate-field");
    duplicate_field
        .key
        .fields
        .push(duplicate_field.key.fields[0].clone());

    for malformed in [
        blank_projector,
        blank_model,
        empty_key,
        blank_field,
        duplicate_field,
    ] {
        let row = completed_replay_record(
            CommandLedgerState::SucceededPendingProjection,
            vec![malformed],
        );
        assert!(row.validate_stored_shape().is_ok());
        assert!(matches!(row.replay(), Err(CommandLedgerError::Corrupt(_))));
    }
}

#[test]
fn replay_validates_envelope_and_returns_only_the_outcome() {
    let id = Uuid::now_v7().to_string();
    let reservation = reservation(&id, 1, 2).unwrap();
    let started = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    let mut row = CommandLedgerRecord::initial(&reservation, started).unwrap();
    let obligation = resolved_obligation("round-trip");
    let completion = row
        .acquired_attempt()
        .unwrap()
        .complete_with_obligations(
            TerminalCommandState::SucceededPendingProjection,
            serde_json::json!({"order_id": "o-1"}),
            vec![obligation.clone()],
            Duration::from_secs(300),
        )
        .unwrap();
    row.complete(&completion, started + Duration::from_secs(1))
        .unwrap();
    let replay = row.replay().unwrap();
    assert_eq!(replay.outcome, serde_json::json!({"order_id": "o-1"}));
    assert_eq!(replay.projection_obligations, vec![obligation.clone()]);

    row.outcome_json = Some(r#"{"version":1,"outcome":null}"#.into());
    assert!(matches!(row.replay(), Err(CommandLedgerError::Corrupt(_))));

    let mut obligation_with_unknown_field = serde_json::to_value(obligation).unwrap();
    obligation_with_unknown_field
        .as_object_mut()
        .unwrap()
        .insert("unknown".into(), serde_json::json!(true));
    row.outcome_json = Some(
        serde_json::json!({
            "version": 1,
            "outcome": null,
            "projection_obligations": [obligation_with_unknown_field],
        })
        .to_string(),
    );
    assert!(matches!(row.replay(), Err(CommandLedgerError::Corrupt(_))));

    row.outcome_json = Some(r#"{"version":2,"outcome":null,"projection_obligations":[]}"#.into());
    assert!(matches!(row.replay(), Err(CommandLedgerError::Corrupt(_))));
}

#[test]
fn modeled_projection_metadata_replays_exact_bytes_without_plaintext_scope_material() {
    let id = Uuid::now_v7().to_string();
    let reservation = reservation(&id, 11, 12).unwrap();
    let started = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    let mut row = CommandLedgerRecord::initial(&reservation, started).unwrap();
    let metadata =
        br#"{"role_safe":true,"opaque_scope":"v1.projection-obligation.token"}"#.to_vec();
    let completion = row
        .acquired_attempt()
        .unwrap()
        .complete_with_projection_metadata(
            TerminalCommandState::SucceededPendingProjection,
            serde_json::json!({"todo_id": "todo-1"}),
            metadata.clone(),
            Duration::from_secs(300),
        )
        .unwrap();
    row.complete(&completion, started + Duration::from_secs(1))
        .unwrap();

    let stored = row.outcome_json.as_deref().unwrap();
    assert!(!stored.contains("opaque_scope"));
    let first = row.replay().unwrap();
    let second = row.replay().unwrap();
    assert_eq!(
        first.projection_metadata.as_deref(),
        Some(metadata.as_slice())
    );
    assert_eq!(first.projection_metadata, second.projection_metadata);
    assert!(first.projection_obligations.is_empty());

    row.outcome_json = Some(
        r#"{"version":2,"outcome":null,"projection_obligations":[],"projection_metadata":"not/canonical"}"#
            .into(),
    );
    assert!(matches!(row.replay(), Err(CommandLedgerError::Corrupt(_))));
}

#[test]
fn modeled_projection_metadata_and_ledger_share_one_absolute_retention_deadline() {
    let request = reservation(&Uuid::now_v7().to_string(), 11, 12).unwrap();
    let started = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    let deadline = started + Duration::from_secs(301);
    let mut row = CommandLedgerRecord::initial(&request, started).unwrap();
    let completion = row
        .acquired_attempt()
        .unwrap()
        .complete_with_projection_metadata_until(
            TerminalCommandState::SucceededPendingProjection,
            serde_json::json!({"todo_id": "todo-deadline"}),
            br#"{"modeled":true}"#.to_vec(),
            Duration::from_secs(300),
            deadline,
        )
        .unwrap();

    // A delayed final commit must retain the exact metadata boundary rather
    // than extending the ledger beyond it with commit-now + retention.
    row.complete(&completion, started + Duration::from_secs(10))
        .unwrap();
    assert_eq!(row.retention_expires_at, deadline);
    assert!(row.replay().is_ok());
}

#[test]
fn modeled_projection_metadata_bounds_fail_before_completion() {
    for metadata in [
        Vec::new(),
        vec![b'x'; crate::MAX_DOMAIN_EVENT_BODY_BYTES + 1],
    ] {
        assert!(matches!(
            fresh_attempt().complete_with_projection_metadata(
                TerminalCommandState::Succeeded,
                serde_json::json!({"ok": true}),
                metadata,
                Duration::from_secs(300),
            ),
            Err(CommandLedgerError::Invalid(_))
        ));
    }
    assert!(matches!(
        fresh_attempt().complete_with_projection_metadata(
            TerminalCommandState::Atomic,
            serde_json::json!({"ok": true}),
            b"{}".to_vec(),
            Duration::from_secs(300),
        ),
        Err(CommandLedgerError::Invalid(_))
    ));
}

#[test]
fn causal_batch_applies_the_authoritative_stamp_at_the_final_boundary() {
    use crate::outbox::OutboxMessage;
    use crate::repository::StreamWrite;

    let id = Uuid::now_v7().to_string();
    let reservation = reservation(&id, 1, 2).unwrap();
    let row = CommandLedgerRecord::initial(&reservation, SystemTime::now()).unwrap();
    let attempt = row.acquired_attempt().unwrap();
    let causation = attempt.causation_id().as_str().to_string();
    let completion = attempt
        .complete(
            TerminalCommandState::Succeeded,
            serde_json::json!({"ok": true}),
            Duration::from_secs(300),
        )
        .unwrap();

    let mut entity = Entity::with_id("order-1");
    entity.set_causation_id("handler-event-cause");
    entity.digest_empty("OrderCreated").unwrap();
    let mut message = OutboxMessage::create("fact-1", "OrderCreated", vec![]).unwrap();
    message.set_causation_id("handler-fact-cause");
    let mut domain = CommitBatch::new(vec![StreamWrite::new(
        StreamIdentity::new("order", "order-1").unwrap(),
        &mut entity,
    )]);
    domain.outbox_messages.push(message);

    let causal = CausalCommitBatch::new(domain, completion);
    assert_eq!(
        causal.domain.streams[0].entity.new_events()[0].causation_id(),
        Some(causation.as_str())
    );
    assert_eq!(
        causal.domain.outbox_messages[0].causation_id(),
        Some(causation.as_str())
    );
}

#[tokio::test]
async fn in_memory_command_ledger_adapter_conformance() {
    use crate::in_memory_repo::InMemoryRepository;

    let repo = InMemoryRepository::new();
    repo.model_store()
        .register_schema::<LedgerConformanceView>()
        .unwrap();
    run_command_ledger_adapter_conformance(&repo).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_command_ledger_adapter_conformance() {
    use crate::SqliteRepository;

    let repo = SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .unwrap();
    repo.bootstrap_table_schema_for_dev(&conformance_table_registry())
        .await
        .unwrap();
    run_command_ledger_adapter_conformance(&repo).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_terminal_replay_survives_pool_drop_and_reopen() {
    use std::ffi::OsString;
    use std::path::PathBuf;

    use crate::SqliteRepository;

    struct TempSqliteDatabase {
        directory: PathBuf,
        database: PathBuf,
    }

    impl TempSqliteDatabase {
        fn new() -> Self {
            let directory = std::env::temp_dir().join(format!(
                "distributed-command-ledger-restart-{}",
                Uuid::now_v7()
            ));
            std::fs::create_dir(&directory).unwrap();
            let database = directory.join("ledger.sqlite3");
            Self {
                directory,
                database,
            }
        }

        fn url(&self) -> String {
            format!(
                "sqlite://{}?mode=rwc",
                self.database
                    .to_str()
                    .expect("temporary SQLite path must be valid UTF-8")
            )
        }
    }

    impl Drop for TempSqliteDatabase {
        fn drop(&mut self) {
            for suffix in ["", "-shm", "-wal", "-journal"] {
                let mut path = OsString::from(self.database.as_os_str());
                path.push(suffix);
                let _ = std::fs::remove_file(PathBuf::from(path));
            }
            let _ = std::fs::remove_dir(&self.directory);
        }
    }

    let database = TempSqliteDatabase::new();
    let database_url = database.url();
    let repo = SqliteRepository::connect_and_migrate(&database_url)
        .await
        .unwrap();

    let command_id = Uuid::now_v7().to_string();
    let request = reservation(&command_id, 81, 82).unwrap();
    let key = request.key().clone();
    let attempt = acquire(&repo, request).await;
    let expected_causation = attempt.causation_id().clone();
    let expected_outcome = serde_json::json!({"order_id": "restart-order"});
    let expected_obligations = vec![resolved_obligation("sqlite-restart")];

    let aggregate_id = format!("sqlite-restart-stream-{}", Uuid::now_v7());
    let identity = StreamIdentity::new("command-ledger-restart", &aggregate_id).unwrap();
    let mut entity = Entity::with_id(&aggregate_id);
    entity.digest_empty("SqliteRestartCommitted").unwrap();
    let outbox_id = format!("sqlite-restart-outbox-{}", Uuid::now_v7());
    let outbox_message =
        OutboxMessage::create(&outbox_id, "SqliteRestartCommitted", b"{}".to_vec()).unwrap();

    let completion = attempt
        .complete_with_obligations(
            TerminalCommandState::SucceededPendingProjection,
            expected_outcome.clone(),
            expected_obligations.clone(),
            Duration::from_secs(300),
        )
        .unwrap();
    let mut domain = CommitBatch::new(vec![StreamWrite::new(identity.clone(), &mut entity)]);
    domain.outbox_messages.push(outbox_message);
    repo.commit_causal_batch(CausalCommitBatch::new(domain, completion))
        .await
        .unwrap();

    repo.pool().close().await;
    drop(repo);

    let reopened = SqliteRepository::connect_and_migrate(&database_url)
        .await
        .unwrap();
    let replay = match reopened
        .lookup_command(&key, CommandLookupScope::CommandName("order.create"))
        .await
        .unwrap()
    {
        CommandLookup::Replay(replay) => replay,
        other => panic!("reopened SQLite ledger should replay, got {other:?}"),
    };
    assert_eq!(replay.state, CommandLedgerState::SucceededPendingProjection);
    assert_eq!(replay.causation_id, expected_causation);
    assert_eq!(replay.outcome, expected_outcome);
    assert_eq!(replay.projection_obligations, expected_obligations);

    let stored_stream = reopened
        .get_stream(&identity)
        .await
        .unwrap()
        .expect("reopened SQLite repository should retain the causal event stream");
    assert_eq!(stored_stream.events().len(), 1);
    assert_eq!(
        stored_stream.events()[0].causation_id(),
        Some(expected_causation.as_str())
    );
    let stored_outbox = reopened
        .outbox_store()
        .messages_by_status(OutboxMessageStatus::Pending, 1_000)
        .await
        .unwrap()
        .into_iter()
        .find(|message| message.id() == outbox_id)
        .expect("reopened SQLite repository should retain the causal outbox fact");
    assert_eq!(
        stored_outbox.causation_id(),
        Some(expected_causation.as_str())
    );

    reopened.pool().close().await;
    drop(reopened);
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_command_ledger_adapter_conformance_typechecks() {
    fn assert_conformance<R: CommandLedgerAdapterConformance>() {}
    assert_conformance::<crate::PostgresRepository>();
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_command_ledger_adapter_conformance_when_database_available() {
    use crate::PostgresRepository;

    let Ok(database_url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping Postgres command-ledger conformance test without DATABASE_URL");
        return;
    };
    let repo = PostgresRepository::connect_and_migrate(&database_url)
        .await
        .unwrap();
    repo.bootstrap_table_schema_for_dev(&conformance_table_registry())
        .await
        .unwrap();
    run_command_ledger_adapter_conformance(&repo).await;
}

#[tokio::test]
async fn in_memory_adapter_reclaims_and_replays_with_a_stable_causation() {
    use crate::in_memory_repo::InMemoryRepository;

    let repo = InMemoryRepository::new();
    assert_eq!(
        repo.causal_storage_identity(),
        repo.clone().causal_storage_identity()
    );
    assert_ne!(
        repo.causal_storage_identity(),
        InMemoryRepository::new().causal_storage_identity()
    );

    let id = Uuid::now_v7().to_string();
    let first = match repo
        .reserve_command(reservation(&id, 1, 2).unwrap())
        .await
        .unwrap()
    {
        ReservationOutcome::Acquired(attempt) => attempt,
        other => panic!("expected acquired attempt, got {other:?}"),
    };
    let cause = first.causation_id().clone();
    let first_token = first.attempt_token().as_str().to_string();
    repo.mark_retryable_unknown(first.fence()).await.unwrap();

    let second = match repo
        .reserve_command(reservation(&id, 1, 2).unwrap())
        .await
        .unwrap()
    {
        ReservationOutcome::Acquired(attempt) => attempt,
        other => panic!("expected reclaimed attempt, got {other:?}"),
    };
    assert_eq!(second.causation_id(), &cause);
    assert_ne!(second.attempt_token().as_str(), first_token);
    assert_eq!(second.attempt_number(), 2);

    let completion = second
        .complete(
            TerminalCommandState::Succeeded,
            serde_json::json!({"ok": true}),
            Duration::from_secs(300),
        )
        .unwrap();
    repo.commit_causal_batch(CausalCommitBatch::new(CommitBatch::empty(), completion))
        .await
        .unwrap();

    let key = reservation(&id, 1, 2).unwrap().key().clone();
    match repo
        .lookup_command(&key, CommandLookupScope::CommandName("order.create"))
        .await
        .unwrap()
    {
        CommandLookup::Replay(replay) => {
            assert_eq!(replay.causation_id, cause);
            assert_eq!(replay.outcome, serde_json::json!({"ok": true}));
        }
        other => panic!("expected replay, got {other:?}"),
    }
}

#[tokio::test]
async fn concurrent_in_memory_reservations_have_one_winner_and_one_cause() {
    use crate::in_memory_repo::InMemoryRepository;

    let repo = InMemoryRepository::new();
    let id = Uuid::now_v7().to_string();
    let left = reservation(&id, 5, 6).unwrap();
    let right = reservation(&id, 5, 6).unwrap();
    let (left, right) = tokio::join!(repo.reserve_command(left), repo.reserve_command(right));
    let outcomes = [left.unwrap(), right.unwrap()];
    let acquired = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, ReservationOutcome::Acquired(_)))
        .count();
    assert_eq!(acquired, 1);
    let causes = outcomes
        .iter()
        .map(|outcome| match outcome {
            ReservationOutcome::Acquired(attempt) => attempt.causation_id().as_str(),
            ReservationOutcome::InProgress { causation_id } => causation_id.as_str(),
            other => panic!("unexpected concurrent reservation outcome: {other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(causes[0], causes[1]);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_adapter_enforces_attempt_fence_and_replays() {
    use crate::outbox::{OutboxMessage, OutboxMessageStatus};
    use crate::outbox_worker::OutboxStore;
    use crate::SqliteRepository;

    let repo = SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .unwrap();
    let id = Uuid::now_v7().to_string();
    let first = match repo
        .reserve_command(reservation(&id, 3, 4).unwrap())
        .await
        .unwrap()
    {
        ReservationOutcome::Acquired(attempt) => attempt,
        other => panic!("expected acquired attempt, got {other:?}"),
    };
    let cause = first.causation_id().clone();
    repo.mark_retryable_unknown(first.fence()).await.unwrap();
    let second = match repo
        .reserve_command(reservation(&id, 3, 4).unwrap())
        .await
        .unwrap()
    {
        ReservationOutcome::Acquired(attempt) => attempt,
        other => panic!("expected reclaimed attempt, got {other:?}"),
    };
    assert_eq!(second.causation_id(), &cause);

    let stale = first
        .complete(
            TerminalCommandState::Succeeded,
            serde_json::json!({"winner": false}),
            Duration::from_secs(300),
        )
        .unwrap();
    let mut stale_domain = CommitBatch::empty();
    stale_domain
        .outbox_messages
        .push(OutboxMessage::create("stale-effect", "ShouldRollback", vec![]).unwrap());
    assert!(matches!(
        repo.commit_causal_batch(CausalCommitBatch::new(stale_domain, stale))
            .await,
        Err(CommandLedgerError::AttemptFenced { .. })
    ));
    assert!(repo
        .outbox_store()
        .messages_by_status(OutboxMessageStatus::Pending, 10)
        .await
        .unwrap()
        .is_empty());

    let completion = second
        .complete(
            TerminalCommandState::Atomic,
            serde_json::json!({"winner": true}),
            Duration::from_secs(300),
        )
        .unwrap();
    let completion = attach_test_direct_projection(completion, "sqlite-winner");
    repo.commit_causal_batch(CausalCommitBatch::new(CommitBatch::empty(), completion))
        .await
        .unwrap();
    let key = reservation(&id, 3, 4).unwrap().key().clone();
    match repo
        .lookup_command(&key, CommandLookupScope::CommandName("order.create"))
        .await
        .unwrap()
    {
        CommandLookup::Replay(replay) => {
            assert_eq!(replay.state, CommandLedgerState::Atomic);
            assert_eq!(replay.causation_id, cause);
            assert_eq!(replay.outcome, serde_json::json!({"winner": true}));
        }
        other => panic!("expected replay, got {other:?}"),
    }
}

#[test]
fn expiry_is_a_permanent_compact_tombstone() {
    let id = Uuid::now_v7().to_string();
    let original = reservation(&id, 1, 2).unwrap();
    let started = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    let mut row = CommandLedgerRecord::initial(&original, started).unwrap();
    row.expire(started + Duration::from_secs(301));

    let different = reservation(&id, 9, 9).unwrap();
    assert_eq!(
        row.classify_reservation(&different, started + Duration::from_secs(302))
            .unwrap(),
        ReservationDecision::Expire
    );
    assert!(row.outcome_json.is_none());
    assert!(row.attempt_token.is_none());
    assert_eq!(row.lookup().unwrap(), CommandLookup::Expired);
}

#[test]
fn durable_success_states_use_only_the_succeeded_vocabulary() {
    let cases = [
        (CommandLedgerState::Succeeded, "succeeded"),
        (
            CommandLedgerState::SucceededPendingProjection,
            "succeeded_pending_projection",
        ),
        (CommandLedgerState::Atomic, "atomic"),
    ];

    for (state, encoded) in cases {
        assert_eq!(state.as_str(), encoded);
        assert_eq!(CommandLedgerState::parse(encoded).unwrap(), state);
    }
    assert!(CommandLedgerState::parse("accepted").is_err());
    assert!(CommandLedgerState::parse("accepted_pending_projection").is_err());
}
