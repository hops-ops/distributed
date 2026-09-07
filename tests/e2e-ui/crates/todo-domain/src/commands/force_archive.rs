use distributed::command::{Eventual, PreparedCommand};
use distributed::microsvc::{CausalCommandContext, HandlerError};
use distributed::portable_command;
use serde::{Deserialize, Serialize};

use super::support::{admin_user, principal, rejected};
use crate::{domain_commands, Todo, TodoState};

#[derive(Debug, Deserialize, distributed::CommandInput)]
pub struct TodoForceArchiveInput {
    pub todo_id: String,
}

#[derive(Debug, Serialize, distributed::CommandOutput)]
pub struct TodoForceArchivePayload {
    pub todo_id: String,
    pub owner_id: String,
    pub status: String,
    pub archived_by: String,
}

pub async fn handle_force_archive(
    ctx: &CausalCommandContext<'_, Todo>,
    input: TodoForceArchiveInput,
) -> Result<PreparedCommand<Eventual<TodoForceArchivePayload>>, HandlerError> {
    let admin = principal(ctx)?;
    let repo = ctx.repo();
    let mut todo = repo
        .get(&input.todo_id)
        .await?
        .ok_or_else(|| HandlerError::NotFound(input.todo_id.clone()))?;
    todo.force_archive().map_err(rejected)?;
    let state = TodoState::from(&*todo);
    repo.publish_events()
        .commit(todo)?
        .eventual(TodoForceArchivePayload {
            todo_id: state.todo_id,
            owner_id: state.owner_id,
            status: state.status,
            archived_by: admin,
        })
}

portable_command! {
    name: "todo.force_archive",
    transition: domain_commands::ForceArchive,
    aggregate: Todo,
    input: TodoForceArchiveInput,
    outcome: Eventual<TodoForceArchivePayload>,
    shard: |input| input.todo_id.clone(),
    roles: ["admin"],
    field: "todos_force_archive",
    guard: admin_user,
    handle: handle_force_archive,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn force_archive_uses_handle_escape_hatch() {
        assert_eq!(ForceArchive::COMMAND, "todo.force_archive");
        let _ = handle_force_archive;
    }

    #[test]
    fn shard_is_todo_id() {
        let input = TodoForceArchiveInput {
            todo_id: "todo-1".into(),
        };
        assert_eq!(ForceArchive::shard(&input), "todo-1");
    }
}
