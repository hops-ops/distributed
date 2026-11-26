//! Portable Todo command declarations.
//!
//! Hosts mount these values without depending on sqlx, celld, or a concrete
//! repository. Each command and its GraphQL types live in one module.

mod archive;
mod complete;
mod create;
mod force_archive;
mod purge;
mod rename;
mod reopen;

pub use archive::{archive, Archive, TodoArchiveInput, TodoArchivePayload};
pub use complete::{complete, Complete, TodoCompleteInput, TodoStatusPayload};
pub use create::{create, Create, TodoCreateInput, TodoCreatePayload};
pub use force_archive::{
    force_archive, ForceArchive, TodoForceArchiveInput, TodoForceArchivePayload,
};
pub use purge::{purge, Purge, TodoPurgeInput, TodoPurgePayload};
pub use rename::{rename, Rename, TodoRenameInput, TodoRenamePayload};
pub use reopen::{reopen, Reopen, TodoReopenInput, TodoReopenPayload};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Todo;
    use distributed::microsvc::Routes;
    use distributed::{AggregateBuilder, InMemoryRepository};

    fn mounted_specs() -> Vec<String> {
        let repository = InMemoryRepository::new();
        Routes::new()
            .with_repo(repository.aggregate::<Todo>())
            .mount(create())
            .mount(rename())
            .mount(complete())
            .mount(reopen())
            .mount(archive())
            .mount(force_archive())
            .mount(purge())
            .command_specs()
            .expect("todo command declarations compile")
            .into_iter()
            .map(|spec| spec.id)
            .collect()
    }

    #[test]
    fn domain_declarations_mount_without_sqlx_or_celld() {
        let ids = mounted_specs();
        for command in [
            "todo.create",
            "todo.rename",
            "todo.complete",
            "todo.reopen",
            "todo.archive",
            "todo.force_archive",
            "todo.purge",
        ] {
            assert!(ids.iter().any(|id| id == command), "missing {command}");
        }
    }

    #[test]
    fn complete_is_thin_shard_invoke_eventual() {
        let complete_spec = Routes::new()
            .with_repo(InMemoryRepository::new().aggregate::<Todo>())
            .mount(complete())
            .command_specs()
            .expect("complete spec")
            .into_iter()
            .find(|spec| spec.id == "todo.complete")
            .expect("todo.complete");
        assert_eq!(complete_spec.field_name, "todos_complete");
    }

    #[tokio::test]
    async fn cell_host_dispatches_complete_with_the_same_handle_as_soa() {
        use distributed::microsvc::cell_host::{AggregateCell, CellCommandIdentity};
        use distributed::microsvc::{Session, USER_ID_KEY};

        let cell = AggregateCell::<Todo>::new("todo-1")
            .expect("cell identity")
            .mount(create())
            .mount(complete());
        assert_eq!(cell.instance_name(), "todo:todo-1");
        assert!(cell.is_command_only());
        assert!(cell
            .command_names()
            .iter()
            .any(|name| name == "todo.complete"));

        let mut session = Session::new();
        session.set(USER_ID_KEY, "owner-1");
        session.set("x-roles", "user");

        let identity = |command_id| {
            CellCommandIdentity::new("todo", "owner-1", command_id).expect("command identity")
        };
        cell.dispatch_idempotent(
            "todo.create",
            &identity("0190a000-0000-7000-8000-000000000501"),
            serde_json::json!({
                "todo_id": "todo-1",
                "title": "cell complete",
            }),
            session.clone(),
        )
        .await
        .expect("todo.create on cell");

        let completed = cell
            .dispatch_idempotent(
                "todo.complete",
                &identity("0190a000-0000-7000-8000-000000000502"),
                serde_json::json!({ "todo_id": "todo-1" }),
                session,
            )
            .await
            .expect("todo.complete on cell");
        assert_eq!(completed.payload()["todo_id"], "todo-1");
        assert_eq!(completed.payload()["status"], "completed");
    }
}
