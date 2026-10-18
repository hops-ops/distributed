//! Native durable cells (`cell-sqlite`): restart, replay, outbox and
//! ownership conformance against real SQLite files.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};

use super::super::{
    AggregateCell, CellCommandIdentity, CellDispatchError, CellSqlitePath, CellStreamStore,
};
use super::{owner_session, CellItem, Complete, Create, Publish};
use crate::bus::{Message, MessagePublisher, TransportError};
use crate::entity::{Entity, EventRecord};
use crate::microsvc::HasOutboxStore;
use crate::outbox::{OutboxMessage, OutboxMessageStatus};
use crate::outbox_worker::{ClaimOutboxMessages, OutboxClaimRef, OutboxStore};
use crate::repository::{
    CommitBatch, GetStream, PreparedEventAppend, RepositoryError, SnapshotStore, StreamIdentity,
    StreamWrite, TransactionalCommit,
};
use crate::snapshot::SnapshotRecord;

struct TempRoot(PathBuf);

impl TempRoot {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("distributed-cell-sqlite-{}", uuid::Uuid::now_v7())))
    }

    fn location(&self) -> CellSqlitePath {
        CellSqlitePath::root(&self.0)
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Debug, PartialEq)]
struct CellDump {
    events: Vec<EventRecord>,
    snapshot: Option<SnapshotRecord>,
    outbox: Vec<OutboxMessage>,
    sealed: Option<Value>,
}

/// Read every durable concern of one exclusive cell through a fresh handle,
/// which is dropped again before returning.
async fn dump(location: &CellSqlitePath, shard: &str) -> CellDump {
    let identity = StreamIdentity::new("CellItem", shard).unwrap();
    let store = CellStreamStore::open_sqlite(location, identity.clone()).unwrap();
    assert!(store.is_sqlite());
    let events = GetStream::get_stream(&store, &identity)
        .await
        .unwrap()
        .map(|entity| entity.events().to_vec())
        .unwrap_or_default();
    let snapshot = SnapshotStore::get_snapshot(&store, &identity)
        .await
        .unwrap();
    let outbox_store = store.outbox_store();
    let mut outbox = Vec::new();
    for status in [
        OutboxMessageStatus::Pending,
        OutboxMessageStatus::InFlight,
        OutboxMessageStatus::Failed,
    ] {
        outbox.extend(outbox_store.messages_by_status(status, 100).await.unwrap());
    }
    CellDump {
        events,
        snapshot,
        outbox,
        sealed: store.sealed_row().unwrap(),
    }
}

fn count_events(
    current: Option<&Value>,
    appends: &[PreparedEventAppend<'_>],
) -> Result<Option<Value>, RepositoryError> {
    let mut count = current.and_then(|row| row["events"].as_u64()).unwrap_or(0);
    let mut last = current
        .map(|row| row["last"].clone())
        .unwrap_or(Value::Null);
    for append in appends {
        for event in append.events {
            count += 1;
            last = Value::String(event.event_name.clone());
        }
    }
    Ok(Some(json!({ "events": count, "last": last })))
}

fn identity(command_id: &str) -> CellCommandIdentity {
    CellCommandIdentity::new("cell-test-service", "principal-alice", command_id).unwrap()
}

fn sqlite_cell(location: &CellSqlitePath, shard: &str) -> AggregateCell<CellItem> {
    let store =
        CellStreamStore::open_sqlite(location, StreamIdentity::new("CellItem", shard).unwrap())
            .unwrap()
            .with_sealed_row_reducer(count_events);
    AggregateCell::<CellItem>::from_store_with_snapshots(store, 1)
        .unwrap()
        .mount(Create)
        .mount(Complete)
        .mount(Publish)
}

#[tokio::test]
async fn sqlite_cell_reopens_identically_and_replays_recorded_outcomes() {
    let root = TempRoot::new();
    let location = root.location();
    let create_id = identity("0190a000-0000-7000-8000-000000000501");
    let complete_id = identity("0190a000-0000-7000-8000-000000000502");
    let create_input = json!({ "id": "item-1", "title": "ship" });

    let cell = sqlite_cell(&location, "item-1");
    let created = cell
        .dispatch_idempotent(
            "cell_item.create",
            &create_id,
            create_input.clone(),
            owner_session(),
        )
        .await
        .unwrap();
    let completed = cell
        .dispatch_idempotent(
            "cell_item.complete",
            &complete_id,
            json!({ "id": "item-1" }),
            owner_session(),
        )
        .await
        .unwrap();
    assert!(!created.replayed() && !completed.replayed());
    // Whole-state export is a reference-host API; SQLite cells persist rows.
    assert!(cell.durable_state().is_err());

    let before = dump(&location, "item-1").await;
    assert_eq!(before.events.len(), 2);
    assert_eq!(
        before.snapshot.as_ref().map(|record| record.version),
        Some(2)
    );
    assert_eq!(
        before.sealed,
        Some(json!({ "events": 2, "last": "cell_item.completed" }))
    );
    assert!(before.outbox.is_empty(), "create/complete publish nothing");

    // Drop every handle: nothing survives except the cell's SQLite file.
    drop(cell);
    assert!(location.resolve("CellItem:item-1").is_file());

    let after = dump(&location, "item-1").await;
    assert_eq!(after, before, "reopen must restore identical cell state");

    let reopened = AggregateCell::<CellItem>::open_sqlite_with_snapshots(&location, "item-1", 1)
        .unwrap()
        .mount(Create)
        .mount(Complete);
    let loaded = reopened.load().await.unwrap().expect("durable aggregate");
    assert_eq!(loaded.title, "ship");
    assert!(loaded.done);
    assert_eq!(loaded.entity.snapshot_version(), 2);
    assert_eq!(
        reopened.sealed_row().unwrap(),
        Some(json!({ "events": 2, "last": "cell_item.completed" }))
    );

    // Create would be rejected ("already exists") if it ran again; the durable
    // ledger must return the recorded outcome without invoking the handler.
    let replay = reopened
        .dispatch_idempotent(
            "cell_item.create",
            &create_id,
            create_input,
            owner_session(),
        )
        .await
        .expect("durable replay after reopen");
    assert!(replay.replayed());
    assert_eq!(replay.payload(), created.payload());
    assert_eq!(replay.causation_id(), created.causation_id());
    assert_eq!(replay.projection_events(), created.projection_events());

    let reuse = reopened
        .dispatch_idempotent(
            "cell_item.create",
            &create_id,
            json!({ "id": "item-1", "title": "different" }),
            owner_session(),
        )
        .await
        .unwrap_err();
    assert!(matches!(reuse, CellDispatchError::CommandIdReuse));

    drop(reopened);
    assert_eq!(dump(&location, "item-1").await, before, "replay re-applied");
}

#[tokio::test]
async fn sqlite_cell_history_reads_every_event_after_reopen() {
    let root = TempRoot::new();
    let location = root.location();
    let cell = sqlite_cell(&location, "item-h");
    assert!(cell.history().await.unwrap().is_empty());
    assert!(
        cell.durable_events().is_err(),
        "whole-state export stays a reference-host API"
    );
    cell.dispatch_idempotent(
        "cell_item.create",
        &identity("0190a000-0000-7000-8000-000000000601"),
        json!({ "id": "item-h", "title": "ship" }),
        owner_session(),
    )
    .await
    .unwrap();
    cell.dispatch_idempotent(
        "cell_item.complete",
        &identity("0190a000-0000-7000-8000-000000000602"),
        json!({ "id": "item-h" }),
        owner_session(),
    )
    .await
    .unwrap();
    let history = cell.history().await.unwrap();
    let names: Vec<_> = history.iter().map(|e| e.event_name.as_str()).collect();
    assert_eq!(names.len(), 2, "{names:?}");
    drop(cell);

    // A fresh handle (process restart) reads the same history, snapshots
    // notwithstanding (snapshot every event here).
    let reopened = sqlite_cell(&location, "item-h");
    assert_eq!(reopened.history().await.unwrap(), history);
    let own = StreamIdentity::new("CellItem", "item-h").unwrap();
    assert_eq!(reopened.stream_events(&own).await.unwrap(), history);
    assert_eq!(dump(&location, "item-h").await.events, history);
}

#[tokio::test]
async fn sealed_row_reducer_failure_rolls_back_the_whole_command() {
    let root = TempRoot::new();
    let location = root.location();
    let store = CellStreamStore::open_sqlite(
        &location,
        StreamIdentity::new("CellItem", "item-r").unwrap(),
    )
    .unwrap()
    .with_sealed_row_reducer(|current, appends| {
        if appends
            .iter()
            .flat_map(|append| append.events)
            .any(|event| event.event_name == "cell_item.completed")
        {
            return Err(RepositoryError::Model("sealed row rejected".into()));
        }
        count_events(current, appends)
    });
    let cell = AggregateCell::<CellItem>::from_store_with_snapshots(store, 1)
        .unwrap()
        .mount(Create)
        .mount(Complete);
    cell.dispatch_idempotent(
        "cell_item.create",
        &identity("0190a000-0000-7000-8000-000000000511"),
        json!({ "id": "item-r", "title": "keep" }),
        owner_session(),
    )
    .await
    .unwrap();
    let before = dump(&location, "item-r").await;

    assert!(cell
        .dispatch_idempotent(
            "cell_item.complete",
            &identity("0190a000-0000-7000-8000-000000000512"),
            json!({ "id": "item-r" }),
            owner_session(),
        )
        .await
        .is_err());
    drop(cell);

    let after = dump(&location, "item-r").await;
    assert_eq!(after, before, "events, snapshot and sealed row roll back");
    assert_eq!(after.events.len(), 1);
    assert_eq!(after.snapshot.map(|record| record.version), Some(1));
}

#[derive(Clone, Default)]
struct RecordingPublisher {
    fail: Arc<AtomicBool>,
    published: Arc<Mutex<Vec<Option<String>>>>,
}

impl MessagePublisher for RecordingPublisher {
    async fn publish(&self, message: Message) -> Result<(), TransportError> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(TransportError::retryable("transport down"));
        }
        self.published.lock().unwrap().push(message.id);
        Ok(())
    }
}

#[tokio::test]
async fn outbox_rows_survive_reopen_and_drain_at_least_once() {
    let root = TempRoot::new();
    let location = root.location();
    let cell = sqlite_cell(&location, "pub-1");
    cell.dispatch_idempotent(
        "cell_item.publish",
        &identity("0190a000-0000-7000-8000-000000000521"),
        json!({ "id": "pub-1", "title": "news" }),
        owner_session(),
    )
    .await
    .unwrap();

    // A drain that fails to publish releases the row for a later pass.
    let publisher = RecordingPublisher::default();
    publisher.fail.store(true, Ordering::SeqCst);
    let outcome = cell
        .drain_outbox_with(publisher.clone(), Duration::from_secs(30), 10, 10)
        .await
        .unwrap();
    assert_eq!((outcome.claimed, outcome.released), (1, 1));
    drop(cell);

    let pending = dump(&location, "pub-1").await.outbox;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].status, OutboxMessageStatus::Pending);
    let message_id = pending[0].id.clone();

    // Simulate a crash mid-drain: claimed with a short lease, never settled.
    let store =
        CellStreamStore::open_sqlite(&location, StreamIdentity::new("CellItem", "pub-1").unwrap())
            .unwrap();
    let claimed = store
        .outbox_store()
        .claim(ClaimOutboxMessages::new(
            "crashed-worker",
            10,
            Duration::from_millis(200),
        ))
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    let stale = OutboxClaimRef::from_message(&claimed[0]).unwrap();
    drop(store);

    // Restart. The row survives as in-flight under the dead worker's lease.
    let reopened = sqlite_cell(&location, "pub-1");
    let outbox = dump(&location, "pub-1").await.outbox;
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].status, OutboxMessageStatus::InFlight);
    publisher.fail.store(false, Ordering::SeqCst);
    let fenced = reopened
        .drain_outbox_with(publisher.clone(), Duration::from_secs(30), 10, 10)
        .await
        .unwrap();
    assert_eq!(fenced.claimed, 0, "a live lease is not stolen");

    tokio::time::sleep(Duration::from_millis(250)).await;
    let store =
        CellStreamStore::open_sqlite(&location, StreamIdentity::new("CellItem", "pub-1").unwrap())
            .unwrap();
    assert!(
        store.outbox_store().complete(&stale).await.is_err(),
        "an expired lease cannot delete the row"
    );
    drop(store);

    let drained = reopened
        .drain_outbox_with(publisher.clone(), Duration::from_secs(30), 10, 10)
        .await
        .unwrap();
    assert_eq!((drained.claimed, drained.published), (1, 1));
    assert_eq!(
        publisher.published.lock().unwrap().as_slice(),
        &[Some(message_id)]
    );
    drop(reopened);

    let after = dump(&location, "pub-1").await;
    assert!(after.outbox.is_empty(), "published rows are deleted");
    assert_eq!(after.events.len(), 1);
}

#[tokio::test]
async fn sqlite_cells_enforce_ownership_and_file_binding() {
    let root = TempRoot::new();
    let location = root.location();

    let exclusive = CellStreamStore::open_sqlite(
        &location,
        StreamIdentity::new("CellItem", "item-1").unwrap(),
    )
    .unwrap();
    let mut foreign = Entity::with_id("item-2");
    foreign.digest_empty("created").unwrap();
    let error = TransactionalCommit::commit_batch(
        &exclusive,
        CommitBatch::new(vec![StreamWrite::new(
            StreamIdentity::new("CellItem", "item-2").unwrap(),
            &mut foreign,
        )]),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, RepositoryError::Model(message) if message.contains("cannot access stream"))
    );

    let owns = |game: &'static str| {
        move |identity: &StreamIdentity| identity.aggregate_id().starts_with(game)
    };
    let game_1 = CellStreamStore::open_sqlite_parent(&location, "game", "g1", owns("g1:")).unwrap();
    let mut player = Entity::with_id("g1:player:1");
    player.digest_empty("joined").unwrap();
    let mut bomb = Entity::with_id("g1:bomb:1");
    bomb.digest_empty("placed").unwrap();
    let player_id = StreamIdentity::new("Player", "g1:player:1").unwrap();
    let bomb_id = StreamIdentity::new("Bomb", "g1:bomb:1").unwrap();
    TransactionalCommit::commit_batch(
        &game_1,
        CommitBatch::new(vec![
            StreamWrite::new(player_id.clone(), &mut player),
            StreamWrite::new(bomb_id.clone(), &mut bomb),
        ]),
    )
    .await
    .unwrap();
    drop(game_1);

    let game_1 = CellStreamStore::open_sqlite_parent(&location, "game", "g1", owns("g1:")).unwrap();
    assert!(GetStream::get_stream(&game_1, &player_id)
        .await
        .unwrap()
        .is_some());
    assert!(GetStream::get_stream(&game_1, &bomb_id)
        .await
        .unwrap()
        .is_some());
    let game_2 = CellStreamStore::open_sqlite_parent(&location, "game", "g2", owns("g2:")).unwrap();
    assert!(GetStream::get_stream(&game_2, &player_id).await.is_err());
    assert_ne!(
        location.resolve("game:g1"),
        location.resolve("game:g2"),
        "one SQLite file per cell"
    );

    // An explicit file is bound to the first cell that opens it.
    let file = CellSqlitePath::file(root.0.join("explicit.db"));
    CellStreamStore::open_sqlite(&file, StreamIdentity::new("CellItem", "a").unwrap()).unwrap();
    let error = CellStreamStore::open_sqlite(&file, StreamIdentity::new("CellItem", "b").unwrap())
        .err()
        .expect("file bound to CellItem:a");
    assert!(error.to_string().contains("belongs to"), "{error}");
    CellStreamStore::open_sqlite(&file, StreamIdentity::new("CellItem", "a").unwrap()).unwrap();
}
