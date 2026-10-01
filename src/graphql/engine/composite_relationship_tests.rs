use std::collections::{BTreeMap, BTreeSet};

use async_graphql::Request;

use super::*;
use crate::microsvc::Session;
use crate::table::{
    ColumnType, PrimaryKey, ReadModelCatalog, RelationshipDef, RelationshipKind, TableColumn,
    TableKind, TableSchema,
};

#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn composite_records() -> TableSchema {
    TableSchema {
        model_name: "CompositeRecord".into(),
        table_name: "composite_records".into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                ..TableColumn::new("tenant_id", "tenant_id", ColumnType::Text)
            },
            TableColumn {
                primary_key: true,
                ..TableColumn::new("record_id", "record_id", ColumnType::Text)
            },
            TableColumn::new("value", "value", ColumnType::Text),
        ],
        primary_key: PrimaryKey::new(["tenant_id", "record_id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    }
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn simple_records() -> TableSchema {
    TableSchema {
        model_name: "SimpleRecord".into(),
        table_name: "simple_records".into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                ..TableColumn::new("simple_id", "simple_id", ColumnType::Text)
            },
            TableColumn::new("tenant_id", "tenant_id", ColumnType::Text),
        ],
        primary_key: PrimaryKey::new(["simple_id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    }
}

#[cfg(feature = "sqlite")]
fn workspaces() -> TableSchema {
    TableSchema {
        model_name: "WorkspaceView".into(),
        table_name: "workspaces".into(),
        columns: vec![TableColumn {
            primary_key: true,
            ..TableColumn::new("workspace_id", "workspace_id", ColumnType::Text)
        }],
        primary_key: PrimaryKey::new(["workspace_id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: vec![RelationshipDef {
            references: None,
            field_name: "projects".into(),
            kind: RelationshipKind::HasMany,
            target_model: "ProjectView".into(),
            foreign_key: Some("workspace_id".into()),
            through: None,
            target_foreign_key: None,
        }],
        kind: TableKind::ReadModel,
    }
}

#[cfg(feature = "sqlite")]
fn projects() -> TableSchema {
    TableSchema {
        model_name: "ProjectView".into(),
        table_name: "projects".into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                ..TableColumn::new("workspace_id", "workspace_id", ColumnType::Text)
            },
            TableColumn {
                primary_key: true,
                ..TableColumn::new("path", "path", ColumnType::Text)
            },
            TableColumn::new("kind", "kind", ColumnType::Text),
        ],
        primary_key: PrimaryKey::new(["workspace_id", "path"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    }
}

#[cfg(feature = "sqlite")]
fn labels() -> TableSchema {
    TableSchema {
        model_name: "LabelView".into(),
        table_name: "labels".into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                ..TableColumn::new("label_id", "label_id", ColumnType::Text)
            },
            TableColumn::new("name", "name", ColumnType::Text),
        ],
        primary_key: PrimaryKey::new(["label_id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    }
}

#[cfg(feature = "sqlite")]
fn project_labels() -> TableSchema {
    TableSchema {
        model_name: "ProjectLabel".into(),
        table_name: "project_labels".into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                ..TableColumn::new("workspace_id", "workspace_id", ColumnType::Text)
            },
            TableColumn {
                primary_key: true,
                ..TableColumn::new("path", "path", ColumnType::Text)
            },
            TableColumn {
                primary_key: true,
                ..TableColumn::new("label_id", "label_id", ColumnType::Text)
            },
        ],
        primary_key: PrimaryKey::new(["workspace_id", "path", "label_id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    }
}

#[cfg(feature = "sqlite")]
fn projects_with_labels() -> TableSchema {
    let mut schema = projects();
    schema.relationships.push(RelationshipDef {
        references: None,
        field_name: "labels".into(),
        kind: RelationshipKind::ManyToMany,
        target_model: "LabelView".into(),
        foreign_key: None,
        through: Some("project_labels".into()),
        target_foreign_key: None,
    });
    schema
}

#[cfg(feature = "sqlite")]
fn simple_records_with_composite_fk() -> TableSchema {
    let mut schema = simple_records();
    schema
        .columns
        .push(TableColumn::new("record_id", "record_id", ColumnType::Text));
    schema.relationships.push(RelationshipDef {
        references: None,
        field_name: "composite".into(),
        kind: RelationshipKind::BelongsTo,
        target_model: "CompositeRecord".into(),
        foreign_key: Some("tenant_id,record_id".into()),
        through: None,
        target_foreign_key: None,
    });
    schema
}

#[cfg(feature = "sqlite")]
fn projects_with_files() -> TableSchema {
    let mut schema = projects();
    schema.relationships.push(RelationshipDef {
        references: None,
        field_name: "files".into(),
        kind: RelationshipKind::HasMany,
        target_model: "ProjectFileView".into(),
        foreign_key: Some("workspace_id,path".into()),
        through: None,
        target_foreign_key: None,
    });
    schema
}

#[cfg(feature = "sqlite")]
fn project_files() -> TableSchema {
    TableSchema {
        model_name: "ProjectFileView".into(),
        table_name: "project_files".into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                ..TableColumn::new("workspace_id", "workspace_id", ColumnType::Text)
            },
            TableColumn {
                primary_key: true,
                ..TableColumn::new("path", "path", ColumnType::Text)
            },
            TableColumn {
                primary_key: true,
                ..TableColumn::new("file_id", "file_id", ColumnType::Text)
            },
        ],
        primary_key: PrimaryKey::new(["workspace_id", "path", "file_id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    }
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn belongs_to_with_a_partial_composite_foreign_key_is_rejected() {
    let composite = composite_records();
    let mut simple = simple_records();
    simple.relationships.push(RelationshipDef {
        references: None,
        field_name: "composite".into(),
        kind: RelationshipKind::BelongsTo,
        target_model: "CompositeRecord".into(),
        foreign_key: Some("tenant_id".into()),
        through: None,
        target_foreign_key: None,
    });
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .connect_lazy("sqlite::memory:")
        .unwrap();
    let project = ReadModelCatalog::new("composite-service")
        .table_schema(composite)
        .table_schema(simple);
    let error = GraphqlEngine::from_schema_catalog(&project, pool)
        .unwrap()
        .roles(&["admin"])
        .grant_all("admin")
        .build()
        .err()
        .expect("partial composite foreign_key must fail");
    let message = error.to_string();
    assert!(
        message.contains("lists 1 column") && message.contains("primary key has 2"),
        "{message}"
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn unique_key_join_preserves_namespace_nulls_and_surrogate_identity() {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    unique_key_join_fixture(pool.into()).await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires dedicated DISTRIBUTED_UNIQUE_KEY_TEST_POSTGRES_URL; run explicitly in PostgreSQL CI"]
async fn unique_key_join_postgres_authorization_and_manifest() {
    let url = std::env::var("DISTRIBUTED_UNIQUE_KEY_TEST_POSTGRES_URL")
        .expect("dedicated test database URL");
    let options: sqlx::postgres::PgConnectOptions = url.parse().unwrap();
    assert!(options
        .get_database()
        .unwrap_or("")
        .starts_with("distributed_unique_key_test"));
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    unique_key_join_fixture(pool.into()).await;
}

async fn unique_key_sql(pool: &GraphqlPool, statement: &'static str) {
    match pool {
        #[cfg(feature = "sqlite")]
        GraphqlPool::Sqlite(pool) => {
            sqlx::query(statement).execute(pool).await.unwrap();
        }
        #[cfg(feature = "postgres")]
        GraphqlPool::Postgres(pool) => {
            sqlx::query(statement).execute(pool).await.unwrap();
        }
    }
}

async fn unique_key_join_fixture(pool: GraphqlPool) {
    use futures_util::StreamExt;
    for statement in [
        "CREATE TEMP TABLE composite_records (id TEXT PRIMARY KEY NOT NULL, tenant_id TEXT NOT NULL, record_id TEXT NOT NULL, value TEXT NOT NULL, UNIQUE(tenant_id, record_id))",
        "CREATE TEMP TABLE simple_records (simple_id TEXT PRIMARY KEY NOT NULL, tenant_id TEXT NOT NULL, record_id TEXT)",
        "INSERT INTO composite_records VALUES ('opaque-a','a','same','first'),('opaque-b','b','same','second')",
        "INSERT INTO simple_records VALUES ('1','a','same'),('2','b','same'),('3','a',NULL),('4','missing','same')",
    ] {
        unique_key_sql(&pool, statement).await;
    }
    let mut target = composite_records();
    for column in &mut target.columns {
        column.primary_key = false;
    }
    target.columns.push(TableColumn {
        primary_key: true,
        ..TableColumn::new("id", "id", ColumnType::Text)
    });
    target.primary_key = PrimaryKey::new(["id"]);
    target.indexes.push(crate::table::TableIndex {
        name: None,
        columns: vec!["tenant_id".into(), "record_id".into()],
        unique: true,
    });
    target.relationships.push(RelationshipDef {
        references: Some("tenant_id,record_id".into()),
        field_name: "refs".into(),
        kind: RelationshipKind::HasMany,
        target_model: "SimpleRecord".into(),
        foreign_key: Some("tenant_id,record_id".into()),
        through: None,
        target_foreign_key: None,
    });
    let mut source = simple_records();
    source.columns.push(TableColumn {
        nullable: true,
        ..TableColumn::new("record_id", "record_id", ColumnType::Text)
    });
    source.relationships.push(RelationshipDef {
        references: Some("tenant_id,record_id".into()),
        field_name: "record".into(),
        kind: RelationshipKind::BelongsTo,
        target_model: "CompositeRecord".into(),
        foreign_key: Some("tenant_id,record_id".into()),
        through: None,
        target_foreign_key: None,
    });
    let project = ReadModelCatalog::new("unique-key-test")
        .table_schema(target)
        .table_schema(source);
    let engine = GraphqlEngine::from_schema_catalog(&project, pool.clone())
        .unwrap()
        .roles(&["admin"])
        .grant_all("admin")
        .build()
        .unwrap();
    let mut session = Session::new();
    session.set(crate::microsvc::ROLE_KEY, "admin");
    let response = engine.execute(&session, Request::new(
        "{ simple_records(order_by: [{simple_id: asc}]) { simple_id record { id value } } }"
    )).await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let data = response.data.into_json().unwrap();
    assert_eq!(
        data["simple_records"],
        serde_json::json!([
            {"simple_id":"1","record":{"id":"opaque-a","value":"first"}},
            {"simple_id":"2","record":{"id":"opaque-b","value":"second"}},
            {"simple_id":"3","record":null},
            {"simple_id":"4","record":null},
        ])
    );
    let reverse = engine
        .execute(
            &session,
            Request::new("{ composite_records(order_by: [{id: asc}]) { id refs { simple_id } } }"),
        )
        .await;
    assert!(reverse.errors.is_empty(), "{:?}", reverse.errors);
    assert_eq!(
        reverse.data.into_json().unwrap()["composite_records"],
        serde_json::json!([
            {"id":"opaque-a","refs":[{"simple_id":"1"}]},
            {"id":"opaque-b","refs":[{"simple_id":"2"}]},
        ])
    );
    // Parent visibility must not confer visibility on the referenced object.
    let (changes, change_rx) = tokio::sync::broadcast::channel(8);
    let mut builder = GraphqlEngine::from_schema_catalog(&project, pool.clone())
        .unwrap()
        .roles(&["reader"])
        .grant_all("reader")
        .change_stream(change_rx);
    builder
        .permissions
        .get_mut(&("CompositeRecord".into(), "reader".into()))
        .unwrap()
        .permission
        .row_filter = Some(crate::graphql::col("value").eq("first"));
    let restricted = builder.build().unwrap();
    let manifest = restricted.client_manifest_for_role("reader").unwrap();
    let object_model = manifest
        .models
        .iter()
        .find(|model| model.source_table == "composite_records")
        .unwrap();
    let normalization = serde_json::to_value(&object_model.normalization).unwrap();
    assert_eq!(normalization["kind"], "normalized");
    assert_eq!(normalization["fields"].as_array().unwrap().len(), 1);
    assert_eq!(normalization["fields"][0]["name"], "id");
    let ref_model = manifest
        .models
        .iter()
        .find(|model| model.source_table == "simple_records")
        .unwrap();
    let mapping = &ref_model
        .relationships
        .iter()
        .find(|relation| relation.name == "record")
        .unwrap()
        .key_mapping;
    assert_eq!(
        serde_json::to_value(mapping).unwrap(),
        serde_json::json!({
            "kind":"direct", "local":["tenant_id","record_id"], "remote":["tenant_id","record_id"]
        })
    );
    session.set(crate::microsvc::ROLE_KEY, "reader");
    let query = "{ simple_records(order_by: [{simple_id: asc}]) { simple_id record { id } } }";
    let mut live = Box::pin(
        restricted.execute_stream(&session, Request::new(format!("subscription {query}"))),
    );
    let initial_live = tokio::time::timeout(std::time::Duration::from_secs(3), live.next())
        .await
        .expect("initial candidate-key subscription timed out")
        .expect("subscription ended");
    assert!(initial_live.errors.is_empty(), "{:?}", initial_live.errors);
    assert_eq!(
        initial_live.data.into_json().unwrap()["simple_records"][0]["record"]["id"],
        "opaque-a"
    );
    let response = restricted.execute(&session, Request::new(query)).await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    assert_eq!(
        response.data.into_json().unwrap()["simple_records"],
        serde_json::json!([
            {"simple_id":"1","record":{"id":"opaque-a"}},
            {"simple_id":"2","record":null},
            {"simple_id":"3","record":null},
            {"simple_id":"4","record":null},
        ])
    );
    let hidden_filter = restricted
        .execute(
            &session,
            Request::new(
                "{ simple_records(where: {record: {value: {_eq: \"second\"}}}) { simple_id } }",
            ),
        )
        .await;
    assert!(
        hidden_filter.errors.is_empty(),
        "{:?}",
        hidden_filter.errors
    );
    assert_eq!(
        hidden_filter.data.into_json().unwrap()["simple_records"],
        serde_json::json!([])
    );
    unique_key_sql(
        &pool,
        "UPDATE composite_records SET value='revoked' WHERE id='opaque-a'",
    )
    .await;
    changes
        .send(crate::read_model::ReadModelChange::new([
            "composite_records",
        ]))
        .unwrap();
    let revoked_live = tokio::time::timeout(std::time::Duration::from_secs(3), live.next())
        .await
        .expect("target-only change did not refresh candidate-key subscription")
        .expect("subscription ended");
    assert!(revoked_live.errors.is_empty(), "{:?}", revoked_live.errors);
    for row in revoked_live.data.into_json().unwrap()["simple_records"]
        .as_array()
        .unwrap()
    {
        assert!(
            row["record"].is_null(),
            "live result leaked revoked target: {row}"
        );
    }
    let revoked = restricted.execute(&session, Request::new(query)).await;
    assert!(revoked.errors.is_empty(), "{:?}", revoked.errors);
    for row in revoked.data.into_json().unwrap()["simple_records"]
        .as_array()
        .unwrap()
    {
        assert!(row["record"].is_null(), "revoked target leaked: {row}");
    }
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn belongs_to_loads_composite_target_rows() {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE composite_records (\
            tenant_id TEXT NOT NULL, \
            record_id TEXT NOT NULL, \
            value TEXT NOT NULL, \
            PRIMARY KEY (tenant_id, record_id)\
        )",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TABLE simple_records (\
            simple_id TEXT PRIMARY KEY NOT NULL, \
            tenant_id TEXT NOT NULL, \
            record_id TEXT NOT NULL\
        )",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO composite_records (tenant_id, record_id, value) VALUES \
            ('tenant-a', 'record-1', 'first'), \
            ('tenant-a', 'record-2', 'second')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO simple_records (simple_id, tenant_id, record_id) VALUES \
            ('s-1', 'tenant-a', 'record-2'), \
            ('s-2', 'tenant-a', 'record-1')",
    )
    .execute(&pool)
    .await
    .unwrap();

    let project = ReadModelCatalog::new("composite-service")
        .table_schema(composite_records())
        .table_schema(simple_records_with_composite_fk());
    let engine = GraphqlEngine::from_schema_catalog(&project, pool)
        .unwrap()
        .roles(&["admin"])
        .grant_all("admin")
        .build()
        .expect("belongs_to onto a composite primary key must build");
    let mut session = Session::new();
    session.set("x-roles", "admin");

    let nested = engine
        .execute(
            &session,
            Request::new(
                r#"{
                    simple_records {
                        simple_id
                        composite { value }
                    }
                }"#,
            ),
        )
        .await;
    assert!(nested.errors.is_empty(), "{nested:?}");
    let rows = nested.data.into_json().unwrap();
    let by_id = rows["simple_records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["simple_id"].as_str().unwrap().to_string(),
                row["composite"]["value"].as_str().unwrap().to_string(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(by_id.get("s-1").unwrap(), "second");
    assert_eq!(by_id.get("s-2").unwrap(), "first");
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn has_many_lists_child_rows_on_a_composite_parent() {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE projects (\
            workspace_id TEXT NOT NULL, \
            path TEXT NOT NULL, \
            kind TEXT NOT NULL, \
            PRIMARY KEY (workspace_id, path)\
        )",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TABLE project_files (\
            workspace_id TEXT NOT NULL, \
            path TEXT NOT NULL, \
            file_id TEXT NOT NULL, \
            PRIMARY KEY (workspace_id, path, file_id)\
        )",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO projects (workspace_id, path, kind) VALUES \
            ('acme', 'core', 'git'), \
            ('acme', 'api', 'git')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO project_files (workspace_id, path, file_id) VALUES \
            ('acme', 'core', 'readme'), \
            ('acme', 'core', 'lib.rs'), \
            ('acme', 'api', 'main.rs')",
    )
    .execute(&pool)
    .await
    .unwrap();

    let project = ReadModelCatalog::new("workspace-service")
        .table_schema(projects_with_files())
        .table_schema(project_files());
    let engine = GraphqlEngine::from_schema_catalog(&project, pool)
        .unwrap()
        .roles(&["admin"])
        .grant_all("admin")
        .build()
        .expect("has_many from a composite parent must build");
    let mut session = Session::new();
    session.set("x-roles", "admin");

    let nested = engine
        .execute(
            &session,
            Request::new(
                r#"{
                    projects {
                        path
                        files { file_id }
                    }
                }"#,
            ),
        )
        .await;
    assert!(nested.errors.is_empty(), "{nested:?}");
    let rows = nested.data.into_json().unwrap();
    let by_path = rows["projects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            let files = row["files"]
                .as_array()
                .unwrap()
                .iter()
                .map(|file| file["file_id"].as_str().unwrap().to_string())
                .collect::<BTreeSet<_>>();
            (row["path"].as_str().unwrap().to_string(), files)
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        by_path.get("core").unwrap(),
        &BTreeSet::from(["lib.rs".into(), "readme".into()])
    );
    assert_eq!(
        by_path.get("api").unwrap(),
        &BTreeSet::from(["main.rs".into()])
    );

    let filtered = engine
        .execute(
            &session,
            Request::new(
                r#"{
                    projects(where: { files: { file_id: { _eq: "main.rs" } } }) {
                        path
                    }
                }"#,
            ),
        )
        .await;
    assert!(filtered.errors.is_empty(), "{filtered:?}");
    assert_eq!(
        filtered.data.into_json().unwrap(),
        serde_json::json!({ "projects": [{ "path": "api" }] })
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn has_many_lists_composite_child_rows_on_a_single_column_parent() {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::query("CREATE TABLE workspaces (workspace_id TEXT PRIMARY KEY NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE projects (\
            workspace_id TEXT NOT NULL, \
            path TEXT NOT NULL, \
            kind TEXT NOT NULL, \
            PRIMARY KEY (workspace_id, path)\
        )",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO workspaces (workspace_id) VALUES ('acme'), ('other')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO projects (workspace_id, path, kind) VALUES \
            ('acme', 'core', 'git'), \
            ('acme', 'api', 'git'), \
            ('other', 'docs', 'git')",
    )
    .execute(&pool)
    .await
    .unwrap();

    let project = ReadModelCatalog::new("workspace-service")
        .table_schema(workspaces())
        .table_schema(projects());
    let engine = GraphqlEngine::from_schema_catalog(&project, pool)
        .unwrap()
        .roles(&["admin"])
        .grant_all("admin")
        .build()
        .expect("has_many onto composite child must build");
    let mut session = Session::new();
    session.set("x-roles", "admin");

    let nested = engine
        .execute(
            &session,
            Request::new(
                r#"{
                    workspaces {
                        workspace_id
                        projects { path kind }
                    }
                }"#,
            ),
        )
        .await;
    assert!(nested.errors.is_empty(), "{nested:?}");
    let rows = nested.data.into_json().unwrap();
    let by_workspace = rows["workspaces"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            let paths = row["projects"]
                .as_array()
                .unwrap()
                .iter()
                .map(|project| project["path"].as_str().unwrap().to_string())
                .collect::<BTreeSet<_>>();
            (row["workspace_id"].as_str().unwrap().to_string(), paths)
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        by_workspace.get("acme").unwrap(),
        &BTreeSet::from(["api".into(), "core".into()]),
        "{rows}"
    );
    assert_eq!(
        by_workspace.get("other").unwrap(),
        &BTreeSet::from(["docs".into()]),
        "{rows}"
    );

    let filtered = engine
        .execute(
            &session,
            Request::new(
                r#"{
                    workspaces(where: { projects: { path: { _eq: "core" } } }) {
                        workspace_id
                    }
                }"#,
            ),
        )
        .await;
    assert!(filtered.errors.is_empty(), "{filtered:?}");
    assert_eq!(
        filtered.data.into_json().unwrap(),
        serde_json::json!({ "workspaces": [{ "workspace_id": "acme" }] })
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn m2m_joins_composite_parent_key_through_the_join_table() {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE projects (\
            workspace_id TEXT NOT NULL, \
            path TEXT NOT NULL, \
            kind TEXT NOT NULL, \
            PRIMARY KEY (workspace_id, path)\
        )",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TABLE labels (\
            label_id TEXT PRIMARY KEY NOT NULL, \
            name TEXT NOT NULL\
        )",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TABLE project_labels (\
            workspace_id TEXT NOT NULL, \
            path TEXT NOT NULL, \
            label_id TEXT NOT NULL, \
            PRIMARY KEY (workspace_id, path, label_id)\
        )",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO projects (workspace_id, path, kind) VALUES \
            ('acme', 'core', 'git'), \
            ('acme', 'api', 'git')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO labels (label_id, name) VALUES ('rust', 'Rust'), ('svc', 'Service')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO project_labels (workspace_id, path, label_id) VALUES \
            ('acme', 'core', 'rust'), \
            ('acme', 'api', 'svc'), \
            ('acme', 'api', 'rust')",
    )
    .execute(&pool)
    .await
    .unwrap();

    let catalog = ReadModelCatalog::new("label-service")
        .table_schema(projects_with_labels())
        .table_schema(labels())
        .table_schema(project_labels());
    let engine = GraphqlEngine::from_schema_catalog(&catalog, pool)
        .unwrap()
        .roles(&["admin"])
        .grant_all("admin")
        .build()
        .expect("m2m from a composite parent must build");
    let mut session = Session::new();
    session.set("x-roles", "admin");

    let nested = engine
        .execute(
            &session,
            Request::new(
                r#"{
                    projects {
                        path
                        labels { name }
                    }
                }"#,
            ),
        )
        .await;
    assert!(nested.errors.is_empty(), "{nested:?}");
    let rows = nested.data.into_json().unwrap();
    let by_path = rows["projects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            let names = row["labels"]
                .as_array()
                .unwrap()
                .iter()
                .map(|label| label["name"].as_str().unwrap().to_string())
                .collect::<BTreeSet<_>>();
            (row["path"].as_str().unwrap().to_string(), names)
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        by_path.get("core").unwrap(),
        &BTreeSet::from(["Rust".into()])
    );
    assert_eq!(
        by_path.get("api").unwrap(),
        &BTreeSet::from(["Rust".into(), "Service".into()])
    );

    let filtered = engine
        .execute(
            &session,
            Request::new(
                r#"{
                    projects(where: { labels: { name: { _eq: "Service" } } }) {
                        path
                    }
                }"#,
            ),
        )
        .await;
    assert!(filtered.errors.is_empty(), "{filtered:?}");
    assert_eq!(
        filtered.data.into_json().unwrap(),
        serde_json::json!({ "projects": [{ "path": "api" }] })
    );
}
