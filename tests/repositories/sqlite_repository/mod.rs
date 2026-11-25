//! SQLite-dialect checks. Behaviour every repository shares lives in the
//! repository conformance suite (`common::repository`).

use common::todo::Todo;
use distributed::table::TableSchemaRegistry;
use distributed::{AggregateBuilder, SqliteRepository, OUTBOX_MESSAGES_TABLE};

#[tokio::test]
async fn migration_is_idempotent() {
    let repo = SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .unwrap();
    repo.migrate().await.unwrap();

    let todos = repo.aggregate::<Todo>();
    let mut todo = Todo::new();
    todo.initialize("todo-1".into(), "user-1".into(), "migrate".into())
        .unwrap();
    todos.commit(&mut todo).await.unwrap();
    assert_eq!(todos.get("todo-1").await.unwrap().unwrap().task, "migrate");
}

#[tokio::test]
async fn dev_bootstrap_applies_registered_table_schemas() {
    let repo = SqliteRepository::connect("sqlite::memory:").await.unwrap();
    let mut registry = TableSchemaRegistry::new();
    registry
        .register_schema(distributed::outbox_message_schema().clone())
        .unwrap();

    let artifacts = repo.generate_table_migration_artifacts(&registry).unwrap();
    assert!(artifacts[0]
        .statements
        .iter()
        .any(|statement| statement.contains("CREATE TABLE IF NOT EXISTS \"outbox_messages\"")));

    let bootstrap = repo
        .bootstrap_table_schema_for_dev(&registry)
        .await
        .unwrap();
    assert_eq!(
        bootstrap.bootstrapped_tables,
        vec![OUTBOX_MESSAGES_TABLE.to_string()]
    );

    let row = sqlx::query("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?")
        .bind(OUTBOX_MESSAGES_TABLE)
        .fetch_one(repo.pool())
        .await
        .unwrap();
    let table_name: String = sqlx::Row::try_get(&row, "name").unwrap();
    assert_eq!(table_name, OUTBOX_MESSAGES_TABLE);
}
