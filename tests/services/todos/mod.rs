use common::todo::{Todo, TodoState};
use distributed::bus::{Message, MessagePublisher, TransportError};
use distributed::outbox::{OutboxMessage, OutboxMessageStatus};
use distributed::outbox_worker::{OutboxDispatcher, OutboxStore};
use distributed::{AggregateBuilder, InMemoryOutboxStore, InMemoryRepository, Queueable};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
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
    snapshot: &TodoState,
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
    let init_message = todo_outbox_message(&id1, "init", "todo.initialized", &todo.state());

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
    let complete_message =
        todo_outbox_message(&id1, "complete", "todo.completed", &retrieved_todo.state());

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
    assert!(completed_todo.state().id == id1);
    assert!(completed_todo.state().user_id == "user1");
    assert!(completed_todo.state().task == "Buy groceries");
    assert!(completed_todo.state().completed);
    drop(completed_todo);

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
    assert_eq!(retrieved.state().id, id);
    assert_eq!(retrieved.state().user_id, "user1");
    assert_eq!(retrieved.state().task, "Roundtrip");
    assert!(!retrieved.state().completed);
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
    assert_eq!(todos[0].state().id, id1);
    assert!(!todos[0].state().completed);
    assert_eq!(todos[1].state().id, id2);
    assert!(!todos[1].state().completed);

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
    assert_eq!(v2_todos[0].state().id, id1);
    assert!(v2_todos[0].state().completed);
    assert_eq!(v2_todos[1].state().id, id2);
    assert!(v2_todos[1].state().completed);
}

#[tokio::test]
async fn outbox_records_persisted() {
    let repo = InMemoryRepository::new();
    let (mut todo, id) = initialized_todo("user1", "Outbox demo");
    let snapshot = todo.state();
    let message = todo_outbox_message(&id, "init", "todo.initialized", &snapshot);

    repo.clone()
        .aggregate::<Todo>()
        .outbox(message)
        .commit(&mut todo)
        .await
        .unwrap();

    // Check pending outbox messages
    let pending = repo.outbox_store().pending(usize::MAX).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].event_type, "todo.initialized");

    let published: TodoState = bitcode::deserialize(&pending[0].payload).unwrap();
    assert_eq!(published.id, snapshot.id);
    assert_eq!(published.user_id, snapshot.user_id);
    assert_eq!(published.task, snapshot.task);
    assert_eq!(published.completed, snapshot.completed);
}

#[tokio::test]
async fn outbox_dispatch_publishes_and_completes_committed_row() {
    let repo = InMemoryRepository::new();
    let (mut todo, id) = initialized_todo("user1", "Outbox dispatch");
    let message = todo_outbox_message(&id, "init", "todo.initialized", &todo.state());
    let message_id = message.id().to_string();
    repo.clone()
        .aggregate::<Todo>()
        .outbox(message)
        .commit(&mut todo)
        .await
        .unwrap();

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
async fn outbox_dispatch_drains_one_row_at_a_time() {
    let repo = InMemoryRepository::new();
    let (mut todo, id) = initialized_todo("user1", "Process next test");
    let snapshot = todo.state();

    // Queue 3 messages
    let message1 = todo_outbox_message(&id, "1", "todo.first_recorded", &snapshot);
    let message2 = todo_outbox_message(&id, "2", "todo.second_recorded", &snapshot);
    let message3 = todo_outbox_message(&id, "3", "todo.third_recorded", &snapshot);

    let message_ids = vec![
        message1.id().to_string(),
        message2.id().to_string(),
        message3.id().to_string(),
    ];

    repo.clone()
        .aggregate::<Todo>()
        .outbox(message1)
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
    let snapshot = todo.state();
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
