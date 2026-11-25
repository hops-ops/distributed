//! Declared `ReadModelId` is independent of the Rust type and table.

use distributed::{ReadModel, RelationalReadModel};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize, ReadModel, Serialize)]
#[readmodel(name = "operational.todos", table = "operational_todos")]
struct OperationalTodos {
    #[id]
    todo_id: String,
    title: String,
}

#[derive(Clone, Debug, Default, Deserialize, ReadModel, Serialize)]
#[readmodel(name = "analytics.todos", table = "todo_throughput")]
struct TodoThroughput {
    #[id]
    todo_id: String,
    completions: i32,
}

#[test]
fn declared_name_is_not_the_rust_type_or_table() {
    assert_eq!(OperationalTodos::read_model_id(), "operational.todos");
    assert_eq!(OperationalTodos::schema().model_name, "operational.todos");
    assert_eq!(OperationalTodos::schema().table_name, "operational_todos");
    assert_ne!(
        OperationalTodos::read_model_id(),
        std::any::type_name::<OperationalTodos>()
            .rsplit("::")
            .next()
            .unwrap()
    );
    assert_eq!(TodoThroughput::read_model_id(), "analytics.todos");
    assert_ne!(
        OperationalTodos::read_model_id(),
        TodoThroughput::read_model_id()
    );
}

#[test]
fn rust_type_default_id_is_the_type_name() {
    #[derive(Clone, Debug, Default, Deserialize, ReadModel, Serialize)]
    #[table("legacy_todos")]
    struct LegacyTodos {
        #[id]
        todo_id: String,
    }

    assert_eq!(LegacyTodos::read_model_id(), "LegacyTodos");
    assert_eq!(LegacyTodos::schema().table_name, "legacy_todos");
}
