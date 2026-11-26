use distributed::domain_event::DomainState;
use serde::{Deserialize, Serialize};

use super::{Todo, TodoStatus};

/// Stable public post-transition state carried by Todo domain events.
///
/// This contract is versioned independently from the aggregate replay event
/// and snapshot schemas.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, DomainState)]
#[domain_state(version = 1)]
pub struct TodoState {
    pub todo_id: String,
    pub owner_id: String,
    pub title: String,
    /// `open` | `completed` | `archived`
    pub status: String,
    pub assignee_id: Option<String>,
}

impl TodoState {
    pub fn from_todo(todo: &Todo) -> Self {
        Self::from(todo)
    }
}

impl From<&Todo> for TodoState {
    fn from(todo: &Todo) -> Self {
        Self {
            todo_id: todo.todo_id.clone(),
            owner_id: todo.owner_id.clone(),
            title: todo.title.clone(),
            status: match todo.status {
                TodoStatus::Open => "open".into(),
                TodoStatus::Completed => "completed".into(),
                TodoStatus::Archived => "archived".into(),
            },
            assignee_id: todo.assignee_id.clone(),
        }
    }
}
