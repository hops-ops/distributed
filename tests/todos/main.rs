mod aggregate;

use aggregate::{Todo, TodoSnapshot};
use distributed::bus::{Message, MessagePublisher, TransportError};
use distributed::{
    AggregateBuilder, CommitBuilderExt, InMemoryOutboxStore, InMemoryRepository, Lock, LockManager,
    OutboxDispatcher, OutboxMessage, OutboxMessageStatus, OutboxStore, Queueable, RepositoryError,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn next_id() -> String {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    format!("todo-{}", id)
}

fn initialized_todo(user_id: &str, task: &str) -> (Todo, String) {
    let mut todo = Todo::new();
    let id = next_id();
    todo.initialize(id.clone(), user_id.to_string(), task.to_string())
        .unwrap();
    (todo, id)
}

fn todo_outbox_message(
    id: &str,
    suffix: &str,
    event_type: &str,
    snapshot: &TodoSnapshot,
) -> OutboxMessage {
    OutboxMessage::encode(format!("{id}:{suffix}"), event_type, snapshot).unwrap()
}

/// Publisher that records every published message so tests can assert on what
/// the outbox dispatcher actually sent through the transport seam.
#[derive(Default)]
struct RecordingPublisher {
    published: Mutex<Vec<Message>>,
}

impl RecordingPublisher {
    fn messages(&self) -> Vec<Message> {
        self.published.lock().unwrap().clone()
    }
}

impl MessagePublisher for RecordingPublisher {
    async fn publish(&self, message: Message) -> Result<(), TransportError> {
        self.published.lock().unwrap().push(message);
        Ok(())
    }
}

/// Production drain path over the in-memory store: claim → publish → complete.
fn dispatcher(
    repo: &InMemoryRepository,
) -> OutboxDispatcher<InMemoryOutboxStore, RecordingPublisher> {
    OutboxDispatcher::new(
        repo.outbox_store(),
        RecordingPublisher::default(),
        "todos-worker",
        Duration::from_secs(30),
        3,
    )
}

async fn load_outbox_message(repo: &InMemoryRepository, id: &str) -> Option<OutboxMessage> {
    let store = repo.outbox_store();
    for status in [
        OutboxMessageStatus::Pending,
        OutboxMessageStatus::InFlight,
        OutboxMessageStatus::Published,
        OutboxMessageStatus::Failed,
    ] {
        if let Some(message) = store
            .messages_by_status(status, usize::MAX)
            .await
            .unwrap()
            .into_iter()
            .find(|message| message.id() == id)
        {
            return Some(message);
        }
    }
    None
}

#[tokio::test]
async fn todos() {
    let repo = InMemoryRepository::new().queued().aggregate::<Todo>();

    let (mut todo, id1) = initialized_todo("user1", "Buy groceries");
    let init_message = todo_outbox_message(&id1, "init", "todo.initialized", &todo.snapshot());

    // Commit the Todo + Outbox message to the repository
    repo.outbox(init_message)
        .commit(&mut todo)
        .await
        .expect("initial todo outbox commit should succeed");

    // Verify the outbox event was captured
    {
        let pending = repo
            .repo()
            .inner()
            .outbox_store()
            .pending(usize::MAX)
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].event_type, "todo.initialized");
    }

    // Retrieve the Todo from the repository and complete it, then commit again
    let mut retrieved_todo = repo.get(&id1).await.unwrap().expect("Todo not found");
    retrieved_todo.complete().unwrap();
    let complete_message = todo_outbox_message(
        &id1,
        "complete",
        "todo.completed",
        &retrieved_todo.snapshot(),
    );

    repo.outbox(complete_message)
        .commit(&mut retrieved_todo)
        .await
        .expect("completed todo outbox commit should succeed");

    {
        let pending = repo
            .repo()
            .inner()
            .outbox_store()
            .pending(usize::MAX)
            .await
            .unwrap();
        assert_eq!(pending.len(), 2);
        assert!(pending
            .iter()
            .any(|msg| msg.event_type == "todo.initialized"));
        assert!(pending.iter().any(|msg| msg.event_type == "todo.completed"));
    }

    let completed_todo = repo
        .get(&id1)
        .await
        .unwrap()
        .expect("Updated Todo not found");
    assert!(completed_todo.snapshot().id == id1);
    assert!(completed_todo.snapshot().user_id == "user1");
    assert!(completed_todo.snapshot().task == "Buy groceries");
    assert!(completed_todo.snapshot().completed);
    repo.abort(&completed_todo).await.unwrap();

    let (mut todo2, id2) = initialized_todo("user1", "Buy Sauna");
    let (mut todo3, id3) = initialized_todo("user2", "Chew bubblegum");

    // Commit multiple Todos to the repository
    repo.commit_all(&mut [&mut todo2, &mut todo3])
        .await
        .expect("bulk commit should succeed");

    // get all the todos from the repository
    let all_todos = repo.peek_all(&[&id1, &id2, &id3]).await.unwrap();
    assert_eq!(
        all_todos.len(),
        3,
        "expected all committed todos to be present"
    );
}

#[tokio::test]
async fn get_commit_roundtrip() {
    let repo = InMemoryRepository::new().queued().aggregate::<Todo>();
    let (mut todo, id) = initialized_todo("user1", "Roundtrip");

    repo.commit(&mut todo).await.unwrap();

    let retrieved = repo.peek(&id).await.unwrap().expect("Todo not found");
    assert_eq!(retrieved.snapshot().id, id);
    assert_eq!(retrieved.snapshot().user_id, "user1");
    assert_eq!(retrieved.snapshot().task, "Roundtrip");
    assert!(!retrieved.snapshot().completed);
}

#[tokio::test]
async fn get_all_commit_all_roundtrip() {
    let repo = InMemoryRepository::new().queued().aggregate::<Todo>();

    let (mut todo1, id1) = initialized_todo("user1", "todo.first_recorded");
    let (mut todo2, id2) = initialized_todo("user2", "todo.second_recorded");

    repo.commit_all(&mut [&mut todo1, &mut todo2])
        .await
        .unwrap();

    let todos = repo.get_all(&[&id1, &id2]).await.unwrap();
    assert_eq!(todos.len(), 2);
    assert_eq!(todos[0].snapshot().id, id1);
    assert!(!todos[0].snapshot().completed);
    assert_eq!(todos[1].snapshot().id, id2);
    assert!(!todos[1].snapshot().completed);

    let mut iter = todos.into_iter();
    let mut todo1v2 = iter.next().unwrap();
    let mut todo2v2 = iter.next().unwrap();

    todo1v2.complete().unwrap();
    todo2v2.complete().unwrap();

    repo.commit_all(&mut [&mut todo1v2, &mut todo2v2])
        .await
        .unwrap();

    let v2_todos = repo.peek_all(&[&id1, &id2]).await.unwrap();

    assert_eq!(v2_todos.len(), 2);
    assert_eq!(v2_todos[0].snapshot().id, id1);
    assert!(v2_todos[0].snapshot().completed);
    assert_eq!(v2_todos[1].snapshot().id, id2);
    assert!(v2_todos[1].snapshot().completed);
}

#[tokio::test]
async fn outbox_records_persisted() {
    let repo = InMemoryRepository::new();
    let (mut todo, id) = initialized_todo("user1", "Outbox demo");
    let snapshot = todo.snapshot();
    let message = todo_outbox_message(&id, "init", "todo.initialized", &snapshot);

    repo.outbox(message).commit(&mut todo).await.unwrap();

    // Check pending outbox messages
    let pending = repo.outbox_store().pending(usize::MAX).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].event_type, "todo.initialized");

    let published: TodoSnapshot = bitcode::deserialize(&pending[0].payload).unwrap();
    assert_eq!(published.id, snapshot.id);
    assert_eq!(published.user_id, snapshot.user_id);
    assert_eq!(published.task, snapshot.task);
    assert_eq!(published.completed, snapshot.completed);
}

#[tokio::test]
async fn outbox_dispatch_publishes_and_completes_committed_row() {
    let repo = InMemoryRepository::new();
    let (mut todo, id) = initialized_todo("user1", "Outbox dispatch");
    let message = todo_outbox_message(&id, "init", "todo.initialized", &todo.snapshot());
    let message_id = message.id().to_string();
    repo.outbox(message).commit(&mut todo).await.unwrap();

    let dispatcher = dispatcher(&repo);
    let outcome = dispatcher.dispatch_batch(10).await.unwrap();
    assert_eq!(outcome.claimed, 1);
    assert_eq!(outcome.published, 1);

    let sent = dispatcher.publisher().messages();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].name(), "todo.initialized");

    // Delivered work is removed; publication evidence belongs to the bus.
    assert!(load_outbox_message(&repo, &message_id).await.is_none());
}

#[tokio::test]
async fn abort_releases_lock_after_get() {
    let repo = Arc::new(InMemoryRepository::new().queued().aggregate::<Todo>());
    let (mut todo, id) = initialized_todo("user1", "Abort get");
    repo.commit(&mut todo).await.unwrap();

    let locked = repo.get(&id).await.unwrap().unwrap();

    let (tx_started, rx_started) = mpsc::channel();
    let (tx_got, rx_got) = mpsc::channel();
    let repo_other = Arc::clone(&repo);
    let id_other = id.clone();
    thread::spawn(move || {
        tx_started.send(()).unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _ = rt.block_on(repo_other.get(&id_other)).unwrap();
        tx_got.send(()).unwrap();
    });

    rx_started.recv().unwrap();
    assert!(rx_got.recv_timeout(Duration::from_millis(200)).is_err());

    repo.abort(&locked).await.unwrap();
    assert!(rx_got.recv_timeout(Duration::from_millis(500)).is_ok());
}

#[tokio::test]
async fn abort_releases_lock_after_get_all() {
    let repo = Arc::new(InMemoryRepository::new().queued().aggregate::<Todo>());
    let (mut todo1, id1) = initialized_todo("user1", "Abort get_all 1");
    repo.commit(&mut todo1).await.unwrap();

    let (mut todo2, id2) = initialized_todo("user2", "Abort get_all 2");
    repo.commit(&mut todo2).await.unwrap();

    let locked = repo.get_all(&[&id1, &id2]).await.unwrap();

    let (tx_started, rx_started) = mpsc::channel();
    let (tx_got, rx_got) = mpsc::channel();
    let repo_other = Arc::clone(&repo);
    let id_other = id1.clone();
    thread::spawn(move || {
        tx_started.send(()).unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _ = rt.block_on(repo_other.get(&id_other)).unwrap();
        tx_got.send(()).unwrap();
    });

    rx_started.recv().unwrap();
    assert!(rx_got.recv_timeout(Duration::from_millis(200)).is_err());

    for todo in &locked {
        repo.abort(todo).await.unwrap();
    }

    assert!(rx_got.recv_timeout(Duration::from_millis(500)).is_ok());
}

#[tokio::test]
async fn queued_repo_blocks_get_until_commit() {
    let repo = Arc::new(InMemoryRepository::new().queued().aggregate::<Todo>());
    let (mut todo, id) = initialized_todo("user1", "Queue test");
    repo.commit(&mut todo).await.unwrap();

    let (mut other_todo, other_id) = initialized_todo("user2", "Independent queue");
    repo.commit(&mut other_todo).await.unwrap();

    let (tx_started, rx_started) = mpsc::channel();
    let (tx_release, rx_release) = mpsc::channel();
    let (tx_committed, rx_committed) = mpsc::channel();

    let repo_a = Arc::clone(&repo);
    let id_a = id.clone();
    thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut todo = rt.block_on(repo_a.get(&id_a)).unwrap().unwrap();
        tx_started.send(()).unwrap();
        rx_release.recv().unwrap();
        let _ = rt.block_on(repo_a.commit(&mut todo));
        tx_committed.send(()).unwrap();
    });

    rx_started.recv().unwrap();

    let (tx_other_done, rx_other_done) = mpsc::channel();
    let repo_other = Arc::clone(&repo);
    let other_id_clone = other_id.clone();
    thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let todo = rt
            .block_on(repo_other.get(&other_id_clone))
            .unwrap()
            .unwrap();
        rt.block_on(repo_other.abort(&todo)).unwrap();
        tx_other_done.send(()).unwrap();
    });

    let (tx_peek_done, rx_peek_done) = mpsc::channel();
    let repo_peek = Arc::clone(&repo);
    let id_peek = id.clone();
    thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _ = rt.block_on(repo_peek.peek(&id_peek)).unwrap();
        tx_peek_done.send(()).unwrap();
    });

    let (tx_peek_all_done, rx_peek_all_done) = mpsc::channel();
    let repo_peek_all = Arc::clone(&repo);
    let id_peek_all = id.clone();
    thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ids = [id_peek_all.as_str()];
        let _ = rt.block_on(repo_peek_all.peek_all(&ids)).unwrap();
        tx_peek_all_done.send(()).unwrap();
    });

    assert!(rx_peek_done
        .recv_timeout(Duration::from_millis(200))
        .is_ok());
    assert!(rx_peek_all_done
        .recv_timeout(Duration::from_millis(200))
        .is_ok());
    assert!(rx_other_done
        .recv_timeout(Duration::from_millis(200))
        .is_ok());

    let (tx_done, rx_done) = mpsc::channel();
    let repo_b = Arc::clone(&repo);
    let id_b = id.clone();
    thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut todo = rt.block_on(repo_b.get(&id_b)).unwrap().unwrap();
        let _ = rt.block_on(repo_b.commit(&mut todo));
        tx_done.send(()).unwrap();
    });

    assert!(rx_done.recv_timeout(Duration::from_millis(200)).is_err());
    tx_release.send(()).unwrap();
    rx_committed.recv().unwrap();
    assert!(rx_done.recv_timeout(Duration::from_millis(500)).is_ok());
}

#[tokio::test]
async fn manual_lock_reports_failure_when_already_held() {
    let repo = InMemoryRepository::new().queued();
    let id = next_id();

    let lock = repo.lock_manager().get_lock(&id).unwrap();

    // First acquisition succeeds.
    assert!(
        lock.try_lock().await.unwrap(),
        "first manual lock should be acquired"
    );

    // Second acquisition reports the lock is already held.
    let second = repo.lock_manager().get_lock(&id).unwrap();
    assert!(
        !second.try_lock().await.unwrap(),
        "second manual lock should report already held"
    );

    lock.unlock().await.unwrap();
}

#[tokio::test]
async fn commit_failure_keeps_lock_until_abort() {
    let repo = Arc::new(InMemoryRepository::new().queued().aggregate::<Todo>());
    let (mut todo, id) = initialized_todo("user1", "Commit failure lock");
    repo.commit(&mut todo).await.unwrap();

    let mut locked = repo.get(&id).await.unwrap().unwrap();

    // Simulate a concurrent writer that bumps the version out from under the
    // locked aggregate. The inner repository is unlocked (the queue lock lives in
    // the `QueuedRepository` wrapper held by the main thread), and a cheap clone
    // shares the same `Arc`-backed store, so this writes to the same namespaced
    // stream without contending on the lock.
    let inner = repo.repo().inner().clone().aggregate::<Todo>();
    let mut concurrent = inner.get(&id).await.unwrap().unwrap();
    concurrent.complete().unwrap();
    inner.commit(&mut concurrent).await.unwrap();

    locked.complete().unwrap();
    let err = repo
        .commit(&mut locked)
        .await
        .expect_err("stale locked aggregate should fail optimistic commit");
    assert!(
        matches!(err, RepositoryError::ConcurrentWrite { .. }),
        "unexpected error: {err}"
    );

    let (tx_started, rx_started) = mpsc::channel();
    let (tx_got, rx_got) = mpsc::channel();
    let repo_other = Arc::clone(&repo);
    let id_other = id.clone();
    thread::spawn(move || {
        tx_started.send(()).unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let todo = rt.block_on(repo_other.get(&id_other)).unwrap().unwrap();
        rt.block_on(repo_other.abort(&todo)).unwrap();
        tx_got.send(()).unwrap();
    });

    rx_started.recv().unwrap();
    assert!(rx_got.recv_timeout(Duration::from_millis(200)).is_err());

    repo.abort(&locked).await.unwrap();
    assert!(rx_got.recv_timeout(Duration::from_millis(500)).is_ok());
}

#[tokio::test]
async fn outbox_dispatch_drains_one_row_at_a_time() {
    let repo = InMemoryRepository::new();
    let (mut todo, id) = initialized_todo("user1", "Process next test");
    let snapshot = todo.snapshot();

    // Queue 3 messages
    let message1 = todo_outbox_message(&id, "1", "todo.first_recorded", &snapshot);
    let message2 = todo_outbox_message(&id, "2", "todo.second_recorded", &snapshot);
    let message3 = todo_outbox_message(&id, "3", "todo.third_recorded", &snapshot);

    let message_ids = vec![
        message1.id().to_string(),
        message2.id().to_string(),
        message3.id().to_string(),
    ];

    repo.outbox(message1)
        .outbox(message2)
        .outbox(message3)
        .commit(&mut todo)
        .await
        .unwrap();

    // Drain one row per pass: each pass claims, publishes, and settles a single
    // message through the shared dispatch path.
    let dispatcher = dispatcher(&repo);
    let mut processed = 0;

    loop {
        let outcome = dispatcher.dispatch_batch(1).await.unwrap();
        if outcome.claimed == 0 {
            break;
        }
        processed += outcome.published + outcome.released + outcome.failed;
    }

    assert_eq!(processed, 3);
    for id in &message_ids {
        assert!(load_outbox_message(&repo, id).await.is_none());
    }
    assert_eq!(
        repo.outbox_store().pending(usize::MAX).await.unwrap().len(),
        0
    );
    assert_eq!(dispatcher.publisher().messages().len(), 3);
}

/// Full metadata chain: Entity → EventRecord → OutboxMessage → OutboxWorker → publisher
#[tokio::test]
async fn metadata_flows_from_entity_through_outbox_to_publisher() {
    let repo = InMemoryRepository::new();

    // 1. Create a todo with metadata on the entity
    let mut todo = Todo::new();
    let id = next_id();
    todo.entity.set_correlation_id("req-abc-123");
    todo.entity.set_causation_id("cmd-create-todo");
    todo.entity.set_meta("user_id", "u-42");
    todo.initialize(id.clone(), "user1".to_string(), "Metadata test".to_string())
        .unwrap();

    // 2. Verify metadata propagated to event records
    let new_events = todo.entity.new_events();
    assert_eq!(new_events[0].correlation_id(), Some("req-abc-123"));
    assert_eq!(new_events[0].causation_id(), Some("cmd-create-todo"));
    assert_eq!(new_events[0].meta("user_id"), Some("u-42"));

    // 3. Create outbox message — metadata propagates automatically from entity
    let snapshot = todo.snapshot();
    let message = OutboxMessage::encode_for_entity(
        format!("{}:init", id),
        "todo.initialized",
        &snapshot,
        &todo.entity,
    )
    .unwrap();
    assert_eq!(message.correlation_id(), Some("req-abc-123"));
    assert_eq!(message.causation_id(), Some("cmd-create-todo"));

    // 4. Commit both using outbox commit builder
    let repo = repo.aggregate::<Todo>();
    repo.outbox(message).commit(&mut todo).await.unwrap();

    // 5. Dispatch through the production drain path, verify metadata reaches
    //    the publisher on the canonical transport message
    let dispatcher = dispatcher(repo.repo());
    let outcome = dispatcher.dispatch_batch(10).await.unwrap();
    assert_eq!(outcome.published, 1);

    // 6. Verify the publisher received metadata
    let sent = dispatcher.publisher().messages();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].name(), "todo.initialized");
    assert_eq!(sent[0].correlation_id(), Some("req-abc-123"));
    assert_eq!(sent[0].causation_id(), Some("cmd-create-todo"));
    assert_eq!(sent[0].metadata("user_id"), Some("u-42"));
}
