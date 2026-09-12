use distributed::command::{Eventual, PreparedCommand};
use distributed::command_input_defaults;
use distributed::microsvc::{CausalCommandContext, HandlerError};
use distributed::portable_command;
use serde::{Deserialize, Serialize};

use super::support::{authenticated_user, principal, rejected};
use crate::{domain_commands, Todo, TodoState};

#[derive(Debug, Deserialize, distributed::CommandInput)]
pub struct TodoCreateInput {
    pub todo_id: String,
    pub title: String,
}

#[derive(Debug, Serialize, distributed::CommandOutput)]
pub struct TodoCreatePayload {
    pub todo_id: String,
    pub owner_id: String,
    pub title: String,
    pub status: String,
}

pub async fn handle_create(
    ctx: &CausalCommandContext<'_, Todo>,
    input: TodoCreateInput,
) -> Result<PreparedCommand<Eventual<TodoCreatePayload>>, HandlerError> {
    let owner = principal(ctx)?;
    let repo = ctx.repo();
    if repo.get(&input.todo_id).await?.is_some() {
        return Err(HandlerError::Rejected(format!(
            "todo {} already exists",
            input.todo_id
        )));
    }
    let mut todo = repo.create();
    todo.create(&input.todo_id, &owner, &input.title)
        .map_err(rejected)?;
    let state = TodoState::from(&*todo);
    repo.publish_events()
        .commit(todo)?
        .eventual(TodoCreatePayload {
            todo_id: state.todo_id,
            owner_id: state.owner_id,
            title: state.title,
            status: state.status,
        })
}

portable_command! {
    name: "todo.create",
    transition: domain_commands::Create,
    aggregate: Todo,
    input: TodoCreateInput,
    outcome: Eventual<TodoCreatePayload>,
    shard: |input| input.todo_id.clone(),
    roles: ["user", "admin"],
    field: "todos_create",
    guard: authenticated_user,
    handle: handle_create,
    defaults: command_input_defaults! {
        input: TodoCreateInput;
        default input.todo_id = uuid_v7();
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_uses_handle_escape_hatch() {
        assert_eq!(Create::COMMAND, "todo.create");
        let _ = handle_create;
    }

    #[test]
    fn shard_is_todo_id() {
        let input = TodoCreateInput {
            todo_id: "todo-1".into(),
            title: "one".into(),
        };
        assert_eq!(Create::shard(&input), "todo-1");
    }
}
