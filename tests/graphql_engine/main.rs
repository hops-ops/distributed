//! GraphqlEngine builder validation tests.

#![cfg(all(feature = "graphql", feature = "sqlite"))]

use distributed::{
    graphql::GraphqlEngine, ColumnType, PrimaryKey, RelationshipDef, RelationshipKind, TableColumn,
    TableKind, TableSchema,
};
use sqlx::sqlite::SqlitePoolOptions;

fn simple_schema(model: &str, table: &str) -> TableSchema {
    TableSchema {
        model_name: model.into(),
        table_name: table.into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                ..TableColumn::new("id", "id", ColumnType::Text)
            },
            TableColumn::new("owner_id", "owner_id", ColumnType::Text),
        ],
        primary_key: PrimaryKey::new(["id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    }
}

fn bidirectional_parent_schema() -> TableSchema {
    TableSchema {
        model_name: "Parent".into(),
        table_name: "parents".into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                ..TableColumn::new("id", "id", ColumnType::Text)
            },
            TableColumn::new("name", "name", ColumnType::Text),
        ],
        primary_key: PrimaryKey::new(["id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: vec![RelationshipDef {
            references: None,
            field_name: "children".into(),
            kind: RelationshipKind::HasMany,
            target_model: "Child".into(),
            foreign_key: Some("parent_id".into()),
            through: None,
            target_foreign_key: None,
        }],
        kind: TableKind::ReadModel,
    }
}

fn bidirectional_child_schema() -> TableSchema {
    TableSchema {
        model_name: "Child".into(),
        table_name: "children".into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                ..TableColumn::new("id", "id", ColumnType::Text)
            },
            TableColumn::new("parent_id", "parent_id", ColumnType::Text),
            TableColumn::new("name", "name", ColumnType::Text),
        ],
        primary_key: PrimaryKey::new(["id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: vec![RelationshipDef {
            references: None,
            field_name: "parent".into(),
            kind: RelationshipKind::BelongsTo,
            target_model: "Parent".into(),
            foreign_key: Some("parent_id".into()),
            through: None,
            target_foreign_key: None,
        }],
        kind: TableKind::ReadModel,
    }
}

async fn pool() -> sqlx::SqlitePool {
    SqlitePoolOptions::new()
        .connect("sqlite::memory:")
        .await
        .unwrap()
}

#[tokio::test]
async fn isolated_multi_column_primary_key_builds_with_full_by_pk_tuple() {
    let schema = TableSchema {
        model_name: "Composite".into(),
        table_name: "composites".into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                ..TableColumn::new("a", "a", ColumnType::Text)
            },
            TableColumn {
                primary_key: true,
                ..TableColumn::new("b", "b", ColumnType::Text)
            },
        ],
        primary_key: PrimaryKey::new(["a", "b"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    };
    let manifest = distributed::ReadModelCatalog::new("t").table_schema(schema);
    let engine = GraphqlEngine::from_schema_catalog(&manifest, pool().await)
        .unwrap()
        .roles(&["user"])
        .grant_all("user")
        .build()
        .expect("isolated composite-key roots are supported");
    let sdl = engine.sdl_for_role("user").unwrap();
    assert!(
        sdl.contains("composites_by_pk(a: String!, b: String!): Composite"),
        "composite by-PK root must require the complete key tuple:\n{sdl}"
    );
}

#[tokio::test]
async fn grant_all_builds_and_sdl_for_role() {
    let schema = simple_schema("Item", "items");
    let manifest = distributed::ReadModelCatalog::new("t").table_schema(schema);
    let engine = GraphqlEngine::from_schema_catalog(&manifest, pool().await)
        .unwrap()
        .roles(&["user", "anonymous"])
        .grant_all("user")
        .build()
        .unwrap();
    let sdl = engine.sdl_for_role("user").expect("role sdl");
    assert!(sdl.contains("items") || sdl.contains("Item"));
}

#[tokio::test]
async fn duplicate_table_name_errors() {
    let a = simple_schema("A", "items");
    let b = simple_schema("B", "items");
    let result = GraphqlEngine::builder(pool().await)
        .table_schema(a)
        .table_schema(b)
        .build();
    let err = match result {
        Ok(_) => panic!("expected duplicate table_name error"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("duplicate table_name") || err.to_string().contains("items"),
        "{err}"
    );
}

#[tokio::test]
async fn unknown_column_in_permission_via_grant_ok() {
    // grant_all uses all_columns — valid
    let schema = simple_schema("Item", "items");
    let manifest = distributed::ReadModelCatalog::new("t").table_schema(schema);
    GraphqlEngine::from_schema_catalog(&manifest, pool().await)
        .unwrap()
        .grant_all("user")
        .build()
        .unwrap();
}

#[tokio::test]
async fn build_handles_bidirectional_relationship_schemas() {
    let manifest = distributed::ReadModelCatalog::new("t")
        .table_schema(bidirectional_parent_schema())
        .table_schema(bidirectional_child_schema());
    let engine = GraphqlEngine::from_schema_catalog(&manifest, pool().await)
        .unwrap()
        .grant_all("user")
        .build()
        .unwrap();
    let sdl = engine.sdl_for_role("user").expect("role sdl");
    assert!(sdl.contains("children"));
    assert!(sdl.contains("parent"));
}

#[tokio::test]
async fn pure_sql_compile_helper() {
    use distributed::graphql::naming::root_list_field;
    let schema = simple_schema("Item", "items");
    assert_eq!(root_list_field(&schema), "items");
    let sql = distributed::graphql::sdl::graphql_sdl_for_tables(&[schema]).unwrap();
    assert!(sql.contains("type Item"));
}
