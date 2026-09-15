use distributed::{sourced, Entity, Snapshot};
use serde::{Deserialize, Serialize};

use super::{TodoError, TodoState, TodoStatus};

#[derive(Debug, Clone, Default, Serialize, Deserialize, Snapshot)]
#[snapshot(id = "todo_id")]
pub struct Todo {
    #[serde(skip, default)]
    pub entity: Entity,
    pub todo_id: String,
    pub owner_id: String,
    pub title: String,
    pub status: TodoStatus,
    pub assignee_id: Option<String>,
    #[serde(default)]
    purged: bool,
    #[serde(default)]
    snapshot_generation: u64,
}

impl Todo {
    pub fn is_created(&self) -> bool {
        !self.todo_id.is_empty() && !self.purged
    }

    pub fn is_purged(&self) -> bool {
        self.purged
    }

    pub fn ensure_owner(&self, owner_id: &str) -> Result<(), TodoError> {
        if !self.is_created() {
            return Err(TodoError::NotCreated);
        }
        if self.owner_id != owner_id {
            return Err(TodoError::NotOwner);
        }
        Ok(())
    }

    fn require_mutable(&self) -> Result<(), TodoError> {
        if !self.is_created() {
            return Err(TodoError::NotCreated);
        }
        if matches!(self.status, TodoStatus::Archived) {
            return Err(TodoError::Archived);
        }
        Ok(())
    }

    fn advance_snapshot_generation(&mut self) {
        self.snapshot_generation = self.snapshot_generation.saturating_add(1);
    }
}

#[sourced(
    entity,
    events = "TodoEvent",
    aggregate_type = "todo",
    domain_state = TodoState,
)]
impl Todo {
    /// Create a new open todo. `owner_id` is the authenticated user (never trusted from peers).
    pub fn create(
        &mut self,
        todo_id: impl Into<String>,
        owner_id: impl Into<String>,
        title: impl Into<String>,
    ) -> Result<(), TodoError> {
        if self.purged || self.is_created() {
            return Err(TodoError::AlreadyExists);
        }
        let todo_id = todo_id.into();
        let owner_id = owner_id.into();
        let title = title.into();
        if todo_id.trim().is_empty() {
            return Err(TodoError::EmptyId);
        }
        if owner_id.trim().is_empty() {
            return Err(TodoError::EmptyOwner);
        }
        let title = title.trim();
        if title.is_empty() {
            return Err(TodoError::EmptyTitle);
        }
        self.record_created(todo_id, owner_id, title.to_string())?;
        Ok(())
    }

    #[event("todo.created", version = 1, domain)]
    fn record_created(&mut self, todo_id: String, owner_id: String, title: String) {
        self.entity.set_id(&todo_id);
        self.todo_id = todo_id;
        self.owner_id = owner_id;
        self.title = title;
        self.status = TodoStatus::Open;
        self.assignee_id = None;
        self.purged = false;
        self.advance_snapshot_generation();
    }

    pub fn rename(&mut self, owner_id: &str, title: impl Into<String>) -> Result<(), TodoError> {
        self.ensure_owner(owner_id)?;
        self.require_mutable()?;
        let title = title.into();
        let title = title.trim();
        if title.is_empty() {
            return Err(TodoError::EmptyTitle);
        }
        if title == self.title {
            return Ok(()); // no-op: same title
        }
        self.record_renamed(title.to_string())?;
        Ok(())
    }

    #[event("todo.renamed", version = 1, domain)]
    fn record_renamed(&mut self, title: String) {
        self.title = title;
        self.advance_snapshot_generation();
    }

    pub fn complete(&mut self, owner_id: &str) -> Result<(), TodoError> {
        self.ensure_owner(owner_id)?;
        self.require_mutable()?;
        if matches!(self.status, TodoStatus::Completed) {
            return Err(TodoError::AlreadyCompleted);
        }
        self.record_completed()?;
        Ok(())
    }

    #[event("todo.completed", version = 1, domain)]
    fn record_completed(&mut self) {
        self.status = TodoStatus::Completed;
        self.advance_snapshot_generation();
    }

    pub fn reopen(&mut self, owner_id: &str) -> Result<(), TodoError> {
        self.ensure_owner(owner_id)?;
        self.require_mutable()?;
        if !matches!(self.status, TodoStatus::Completed) {
            return Err(TodoError::NotCompleted);
        }
        self.record_reopened()?;
        Ok(())
    }

    #[event("todo.reopened", version = 1, domain)]
    fn record_reopened(&mut self) {
        self.status = TodoStatus::Open;
        self.advance_snapshot_generation();
    }

    pub fn reassign(
        &mut self,
        owner_id: &str,
        assignee_id: impl Into<String>,
    ) -> Result<(), TodoError> {
        self.ensure_owner(owner_id)?;
        self.require_mutable()?;
        let assignee_id = assignee_id.into();
        let assignee_id = assignee_id.trim();
        if assignee_id.is_empty() {
            return Err(TodoError::EmptyOwner);
        }
        if self.assignee_id.as_deref() == Some(assignee_id) {
            return Ok(());
        }
        self.record_reassigned(assignee_id.to_owned())?;
        Ok(())
    }

    #[event("todo.reassigned", version = 1, domain)]
    fn record_reassigned(&mut self, assignee_id: String) {
        self.assignee_id = Some(assignee_id);
        self.advance_snapshot_generation();
    }

    pub fn archive(&mut self, owner_id: &str) -> Result<(), TodoError> {
        self.ensure_owner(owner_id)?;
        if !self.is_created() {
            return Err(TodoError::NotCreated);
        }
        if matches!(self.status, TodoStatus::Archived) {
            return Ok(()); // idempotent archive
        }
        self.record_archived()?;
        Ok(())
    }

    #[event("todo.archived", version = 1, domain)]
    fn record_archived(&mut self) {
        self.status = TodoStatus::Archived;
        self.advance_snapshot_generation();
    }

    /// Record an administrator intervention separately from owner archival.
    pub fn force_archive(&mut self) -> Result<(), TodoError> {
        if !self.is_created() {
            return Err(TodoError::NotCreated);
        }
        self.record_force_archived()?;
        Ok(())
    }

    #[event("todo.force_archived", version = 1, domain)]
    fn record_force_archived(&mut self) {
        self.status = TodoStatus::Archived;
        self.advance_snapshot_generation();
    }

    /// Physically remove the projected Todo through an explicit deletion event.
    pub fn purge(&mut self, owner_id: &str) -> Result<(), TodoError> {
        if self.purged {
            return Ok(());
        }
        self.ensure_owner(owner_id)?;
        self.record_purged()?;
        Ok(())
    }

    #[event("todo.purged", version = 1, domain = deleted)]
    fn record_purged(&mut self) {
        self.purged = true;
        self.advance_snapshot_generation();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_todo() -> Todo {
        let mut t = Todo::default();
        t.create("t1", "alice", "Buy milk").unwrap();
        t
    }

    #[test]
    fn create_sets_owner_and_open_status() {
        let t = open_todo();
        assert!(t.is_created());
        assert_eq!(t.owner_id, "alice");
        assert_eq!(t.title, "Buy milk");
        assert_eq!(t.status, TodoStatus::Open);
        assert_eq!(t.entity.id(), "t1");
        assert_eq!(t.entity.version(), 1);
        assert_eq!(t.entity.events()[0].event_name, "todo.created");
        assert_eq!(
            t.entity.pending_domain_events()[0].descriptor().name,
            "todo.created"
        );
    }

    #[test]
    fn domain_commands_create_matches_created_event_contract() {
        use distributed::command::CommandEventSet;
        use distributed::domain_event::DomainEventContract;

        let from_transition = domain_commands::Create::command_event_set();
        let from_event = distributed::events![TodoCreatedDomainEvent];
        assert_eq!(
            from_transition, from_event,
            "Create transition must materialize the same emit set as TodoCreatedDomainEvent"
        );
        assert_eq!(TodoCreatedDomainEvent::EVENT_NAME, "todo.created");
    }

    #[test]
    fn create_after_purge_is_rejected_without_emitting_events() {
        let mut todo = open_todo();
        todo.purge("alice").unwrap();
        let events_before = (
            todo.entity.version(),
            todo.entity.events().len(),
            todo.entity.pending_domain_events().len(),
        );

        let error = todo
            .create("t1", "alice", "Recreated without an explicit command")
            .unwrap_err();

        assert_eq!(error, TodoError::AlreadyExists);
        assert_eq!(
            (
                todo.entity.version(),
                todo.entity.events().len(),
                todo.entity.pending_domain_events().len(),
            ),
            events_before
        );
    }

    #[test]
    fn hydrated_create_after_purge_is_rejected_without_emitting_events() {
        let mut todo = open_todo();
        todo.purge("alice").unwrap();
        todo.entity.mark_committed();
        todo.entity.mark_domain_events_committed().unwrap();

        let mut hydrated: Todo = distributed::hydrate(todo.entity.clone()).unwrap();
        let error = hydrated
            .create("t1", "alice", "Recreated after hydration")
            .unwrap_err();

        assert_eq!(
            (
                error,
                hydrated.is_purged(),
                hydrated.entity.version(),
                hydrated.entity.events().len(),
                hydrated.entity.pending_domain_events().len(),
            ),
            (TodoError::AlreadyExists, true, 2, 2, 0)
        );
    }

    #[test]
    fn rejects_empty_title_and_double_create() {
        let mut t = Todo::default();
        assert_eq!(
            t.create("t1", "alice", "  ").unwrap_err(),
            TodoError::EmptyTitle
        );
        t.create("t1", "alice", "ok").unwrap();
        assert_eq!(
            t.create("t1", "alice", "again").unwrap_err(),
            TodoError::AlreadyExists
        );
    }

    #[test]
    fn only_owner_can_mutate() {
        let mut t = open_todo();
        assert_eq!(t.complete("bob").unwrap_err(), TodoError::NotOwner);
        t.complete("alice").unwrap();
        assert_eq!(t.status, TodoStatus::Completed);
    }

    #[test]
    fn complete_reopen_cycle() {
        let mut t = open_todo();
        t.complete("alice").unwrap();
        assert_eq!(
            t.complete("alice").unwrap_err(),
            TodoError::AlreadyCompleted
        );
        t.reopen("alice").unwrap();
        assert_eq!(t.status, TodoStatus::Open);
        assert_eq!(t.reopen("alice").unwrap_err(), TodoError::NotCompleted);
    }

    #[test]
    fn rename_trims_and_rejects_empty() {
        let mut t = open_todo();
        t.rename("alice", "  Eggs  ").unwrap();
        assert_eq!(t.title, "Eggs");
        assert_eq!(t.rename("alice", "   ").unwrap_err(), TodoError::EmptyTitle);
        // A same-title decision emits neither replay nor outward occurrences.
        let v = t.entity.version();
        let replay_events = t.entity.events().len();
        let domain_events = t.entity.pending_domain_events().len();
        t.rename("alice", "Eggs").unwrap();
        assert_eq!(t.entity.version(), v);
        assert_eq!(t.entity.events().len(), replay_events);
        assert_eq!(t.entity.pending_domain_events().len(), domain_events);
    }

    #[test]
    fn archive_is_terminal_for_mutations() {
        let mut t = open_todo();
        t.archive("alice").unwrap();
        assert_eq!(t.status, TodoStatus::Archived);
        assert_eq!(t.complete("alice").unwrap_err(), TodoError::Archived);
        assert_eq!(t.rename("alice", "x").unwrap_err(), TodoError::Archived);
        // A repeated archive is idempotent and emits no replay or outward occurrence.
        let replay_events = t.entity.events().len();
        let domain_events = t.entity.pending_domain_events().len();
        t.archive("alice").unwrap();
        assert_eq!(t.status, TodoStatus::Archived);
        assert_eq!(t.entity.events().len(), replay_events);
        assert_eq!(t.entity.pending_domain_events().len(), domain_events);
    }

    #[test]
    fn state_events_capture_each_post_transition_without_snapshot_only_fields() {
        let mut todo = Todo::default();
        todo.create("t1", "alice", "First").unwrap();
        todo.rename("alice", "Second").unwrap();
        todo.reassign("alice", "bob").unwrap();
        todo.complete("alice").unwrap();

        let states = todo
            .entity
            .pending_domain_events()
            .iter()
            .map(|event| event.decode_body::<TodoState>().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(states.len(), 4);
        assert!(todo.entity.pending_domain_events().iter().all(|event| event
            .descriptor()
            .body
            .version
            == 1));
        assert_eq!(states[0].title, "First");
        assert_eq!(states[1].title, "Second");
        assert_eq!(states[2].assignee_id.as_deref(), Some("bob"));
        assert_eq!(states[3].status, "completed");

        let snapshot = serde_json::to_value(&todo).unwrap();
        let public = serde_json::to_value(TodoState::from(&todo)).unwrap();
        assert!(snapshot.get("snapshot_generation").is_some());
        assert!(public.get("snapshot_generation").is_none());
        assert!(public.get("purged").is_none());
    }

    #[test]
    fn force_archive_and_purge_are_distinct_state_and_deletion_occurrences() {
        use distributed::DomainEventBodyKind;

        let mut todo = open_todo();
        todo.force_archive().unwrap();
        let forced = todo.entity.pending_domain_events().last().unwrap();
        assert_eq!(forced.descriptor().name, "todo.force_archived");
        assert_eq!(
            forced.decode_body::<TodoState>().unwrap().status,
            "archived"
        );

        todo.purge("alice").unwrap();
        let purged = todo.entity.pending_domain_events().last().unwrap();
        assert_eq!(purged.descriptor().name, "todo.purged");
        assert_eq!(purged.descriptor().body.kind, DomainEventBodyKind::Deletion);
        assert!(todo.is_purged());
    }

    #[test]
    fn retry_reads_identical_canonical_occurrences_until_commit_succeeds() {
        let todo = open_todo();
        let first = todo
            .entity
            .pending_domain_events_for_commit()
            .unwrap()
            .iter()
            .map(|event| event.canonical_bytes().unwrap())
            .collect::<Vec<_>>();
        let retry = todo
            .entity
            .pending_domain_events_for_commit()
            .unwrap()
            .iter()
            .map(|event| event.canonical_bytes().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(first, retry);
    }

    #[test]
    fn replay_suppresses_domain_event_recapture() {
        let mut todo = open_todo();
        todo.complete("alice").unwrap();
        todo.entity.mark_committed();
        todo.entity.mark_domain_events_committed().unwrap();

        let replayed: Todo = distributed::hydrate(todo.entity.clone()).unwrap();
        assert!(replayed.entity.pending_domain_events().is_empty());
        assert_eq!(replayed.status, TodoStatus::Completed);
    }
}
