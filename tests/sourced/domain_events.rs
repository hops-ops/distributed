use std::cell::Cell;

use distributed::{
    domain_event::{DomainEventBodyContract, DomainEventContract},
    AggregateBuilder, DomainDeletion, DomainEvent, DomainEventBodyKind, DomainState, Entity,
    InMemoryRepository,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, distributed_macros::DomainState)]
#[domain_state(version = 3)]
struct DomainTodoState {
    todo_id: String,
    title: String,
    completed: bool,
}

#[derive(Default)]
struct DomainTodo {
    entity: Entity,
    title: String,
    completed: bool,
    label: String,
    purged: bool,
    adapter_calls: Cell<usize>,
}

impl From<&DomainTodo> for DomainTodoState {
    fn from(todo: &DomainTodo) -> Self {
        Self {
            todo_id: todo.entity.id().to_owned(),
            title: todo.title.clone(),
            completed: todo.completed,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, distributed_macros::DomainEvent)]
#[domain_event(name = "todo.labelled", version = 1)]
struct TodoLabelled {
    todo_id: String,
    label: String,
    completed: bool,
}

impl TodoLabelled {
    fn capture_after(todo: &DomainTodo, replay: &DomainTodoReplayEvent) -> Self {
        todo.adapter_calls.set(todo.adapter_calls.get() + 1);
        let DomainTodoReplayEvent::Labelled { label } = replay else {
            unreachable!("the adapter is generated only for the labelled transition")
        };
        Self {
            todo_id: todo.entity.id().to_owned(),
            label: label.clone(),
            completed: todo.completed,
        }
    }
}

#[distributed::sourced(
    entity,
    events = "DomainTodoReplayEvent",
    aggregate_type = "todo",
    domain_state = DomainTodoState,
)]
impl DomainTodo {
    pub fn label_via_helper(&mut self, label: String) -> distributed::SourcedResult {
        self.label_helper(label)
    }

    fn label_helper(&mut self, label: String) -> distributed::SourcedResult {
        self.label(label)?;
        Ok(())
    }

    #[event("todo.created", version = 1, domain)]
    fn create(&mut self, todo_id: String, title: String) {
        self.entity.set_id(todo_id);
        self.title = title;
    }

    #[event("todo.completed", version = 1, when = !self.completed, domain = state)]
    fn complete(&mut self) {
        self.completed = true;
    }

    #[event("todo.renamed", version = 2, domain = event)]
    fn rename(&mut self, title: String) {
        self.title = title;
    }

    #[event(
        "todo.labelled",
        version = 1,
        domain = with(TodoLabelled, TodoLabelled::capture_after)
    )]
    fn label(&mut self, label: String) {
        self.label = label;
    }

    #[event("todo.purged", version = 1, domain = deleted)]
    fn purge(&mut self) {
        self.purged = true;
    }

    #[event("todo.audit-touched")]
    fn touch_audit(&mut self) {}
}

#[test]
fn sourced_transitions_export_exact_non_publishable_event_contract_markers() {
    fn assert_state<C>()
    where
        C: DomainEventBodyContract<DomainTodoState>,
    {
    }
    fn assert_deletion<C>()
    where
        C: DomainEventBodyContract<DomainDeletion<DomainTodoDomainIdentity>>,
    {
    }

    assert_state::<DomainTodoCreatedDomainEvent>();
    assert_state::<DomainTodoCompletedDomainEvent>();
    assert_deletion::<DomainTodoPurgedDomainEvent>();

    let completed = DomainTodoCompletedDomainEvent::descriptor();
    assert_eq!(completed.name, "todo.completed");
    assert_eq!(completed.version, 1);
    assert_eq!(completed.body.kind, DomainEventBodyKind::State);

    let purged = DomainTodoPurgedDomainEvent::descriptor();
    assert_eq!(purged.name, "todo.purged");
    assert_eq!(purged.version, 1);
    assert_eq!(purged.body.kind, DomainEventBodyKind::Deletion);
}

#[test]
fn state_mode_captures_each_exact_post_transition_state() {
    let mut todo = DomainTodo::default();

    todo.create("todo-1".into(), "First".into()).unwrap();
    todo.complete().unwrap();

    let states = todo
        .entity
        .pending_domain_events()
        .iter()
        .map(|occurrence| occurrence.decode_body::<DomainTodoState>().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        states,
        vec![
            DomainTodoState {
                todo_id: "todo-1".into(),
                title: "First".into(),
                completed: false,
            },
            DomainTodoState {
                todo_id: "todo-1".into(),
                title: "First".into(),
                completed: true,
            },
        ]
    );
}

#[test]
fn false_guard_does_not_change_occurrences_or_poison() {
    let mut todo = DomainTodo::default();
    todo.create("todo-1".into(), "First".into()).unwrap();
    todo.complete().unwrap();
    let before = todo.entity.pending_domain_events().to_vec();

    todo.complete().unwrap();

    assert_eq!(todo.entity.pending_domain_events(), before);
    assert!(todo.entity.domain_event_poison().is_none());
}

#[test]
fn identity_mode_reencodes_typed_fields_instead_of_reusing_replay_bytes() {
    let mut todo = DomainTodo::default();
    todo.create("todo-1".into(), "First".into()).unwrap();

    todo.rename("Renamed".into()).unwrap();

    let replay = todo.entity.events().last().unwrap();
    let occurrence = todo.entity.pending_domain_events().last().unwrap();
    let body = occurrence
        .decode_body::<DomainTodoRenamedDomainEvent>()
        .unwrap();
    assert_eq!(body.title, "Renamed");
    assert_ne!(replay.payload_bytes(), occurrence.body_bytes());
    assert_eq!(occurrence.descriptor().version, 2);
    assert_eq!(
        occurrence.descriptor().body.kind,
        DomainEventBodyKind::Event
    );
}

#[test]
fn custom_adapter_observes_the_successful_post_transition_state() {
    use distributed::command::CommandEventSet;

    assert_eq!(
        domain_commands::LabelViaHelper::command_event_set(),
        <TodoLabelled as CommandEventSet>::command_event_set(),
    );
    let mut todo = DomainTodo::default();
    todo.create("todo-1".into(), "First".into()).unwrap();
    todo.complete().unwrap();

    todo.label_via_helper("urgent".into()).unwrap();

    let occurrence = todo.entity.pending_domain_events().last().unwrap();
    let labelled = occurrence.decode_body::<TodoLabelled>().unwrap();
    assert_eq!(
        labelled,
        TodoLabelled {
            todo_id: "todo-1".into(),
            label: "urgent".into(),
            completed: true,
        }
    );
    assert_eq!(occurrence.descriptor(), &TodoLabelled::DESCRIPTOR);
    assert_eq!(todo.adapter_calls.get(), 1);
}

#[test]
fn deletion_mode_carries_typed_identity_and_causing_sequence_incarnation() {
    let mut todo = DomainTodo::default();
    todo.create("todo-1".into(), "First".into()).unwrap();

    todo.purge().unwrap();

    let occurrence = todo.entity.pending_domain_events().last().unwrap();
    let deletion = occurrence
        .decode_body::<DomainDeletion<DomainTodoDomainIdentity>>()
        .unwrap();
    assert_eq!(deletion.key.aggregate_id, "todo-1");
    assert_eq!(deletion.incarnation, occurrence.aggregate_sequence());
    assert_eq!(
        occurrence.descriptor().body.kind,
        DomainEventBodyKind::Deletion
    );
}

#[test]
fn unmarked_aggregate_event_does_not_capture_a_domain_occurrence() {
    let mut todo = DomainTodo::default();
    todo.create("todo-1".into(), "First".into()).unwrap();
    let before = todo.entity.pending_domain_events().to_vec();

    todo.touch_audit().unwrap();

    assert_eq!(todo.entity.pending_domain_events(), before);
    assert!(todo.entity.domain_event_poison().is_none());
}

#[tokio::test]
async fn replay_leaves_pending_occurrences_and_poison_empty() {
    let repository = InMemoryRepository::new().aggregate::<DomainTodo>();
    let mut todo = DomainTodo::default();
    todo.create("todo-1".into(), "First".into()).unwrap();
    todo.complete().unwrap();
    todo.label("replayed".into()).unwrap();
    repository.commit(&mut todo).await.unwrap();

    let loaded = repository.get("todo-1").await.unwrap().unwrap();

    assert!(loaded.entity.pending_domain_events().is_empty());
    assert!(loaded.entity.domain_event_poison().is_none());
    assert_eq!(loaded.adapter_calls.get(), 0);
    assert!(loaded.completed);
}

#[test]
fn derive_descriptors_match_frozen_schema_vectors() {
    assert_eq!(
        DomainTodoState::DESCRIPTOR.fingerprint,
        "sha256:06c09de7db0ebfc76732c6316b4f80f627d96b58718540c38b2324a0cfd6ad76"
    );
    assert_eq!(
        TodoLabelled::DESCRIPTOR.body.fingerprint,
        "sha256:86a3954f27edecb9cc030479bd03e8d79e99d6c2c379dc961f12ca84ea4e1994"
    );
}
