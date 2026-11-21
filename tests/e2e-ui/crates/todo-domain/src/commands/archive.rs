use distributed::command;
use distributed::command::Eventual;
use serde::Deserialize;

use super::TodoStatusPayload;
use crate::{domain_commands, Todo};

#[derive(Debug, Deserialize, distributed::CommandInput)]
pub struct TodoArchiveInput {
    pub todo_id: String,
}

pub type TodoArchivePayload = TodoStatusPayload;

command! {
    name: "todo.archive",
    transition: domain_commands::Archive,
    aggregate: Todo,
    input: TodoArchiveInput,
    outcome: Eventual<TodoArchivePayload>,
    shard: |input| input.todo_id.clone(),
    load: required,
    roles: ["user", "admin"],
    field: "todos_archive",
    invoke: |todo, _input, principal| todo.archive(principal),
    payload: |todo, _principal| TodoStatusPayload::from_todo(&**todo),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_is_todo_id() {
        let input = TodoArchiveInput {
            todo_id: "todo-1".into(),
        };
        assert_eq!(Archive::shard(&input), "todo-1");
    }
}
