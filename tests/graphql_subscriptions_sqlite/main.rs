//! Phase-4 exit: GraphQL subscription pushes exactly once per projection commit (SQLite).

#![cfg(all(feature = "graphql", feature = "sqlite"))]

use std::time::Duration;

use async_graphql::Request;
use distributed::graphql::{claim, col, read, GraphqlEngine, ModelPermissions};
use distributed::microsvc::{Session, ROLE_KEY, USER_ID_KEY};
use distributed::{
    ColumnType, ExpectedVersion, PrimaryKey, ReadModelChange, ReadModelWritePlanStore, RowKey,
    RowValue, RowValues, RowWriteMode, TableColumn, TableKind, TableMutation, TableRowMutation,
    TableSchema, TableWritePlan,
};
use futures_util::StreamExt;
use sqlx::sqlite::SqlitePoolOptions;

fn items_schema() -> TableSchema {
    TableSchema {
        model_name: "ItemView".into(),
        table_name: "items".into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                ..TableColumn::new("id", "id", ColumnType::Text)
            },
            TableColumn::new("name", "name", ColumnType::Text),
            TableColumn::new("status", "status", ColumnType::Text),
        ],
        primary_key: PrimaryKey::new(["id"]),
        version_column: Some("_sourced_version".into()),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    }
}

async fn setup_fixed() -> (
    distributed::SqliteRepository,
    GraphqlEngine,
    sqlx::SqlitePool,
) {
    let repo = distributed::SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .unwrap();
    let pool = repo.pool().clone();
    sqlx::query(
        "CREATE TABLE items (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            status TEXT NOT NULL,
            _sourced_version INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO items (id, name, status, _sourced_version) VALUES
            ('i1', 'alpha', 'open', 1),
            ('i2', 'beta', 'closed', 1);",
    )
    .execute(&pool)
    .await
    .unwrap();

    let change_rx = repo.read_model_changes();

    let manifest = distributed::ReadModelCatalog::new("items").table_schema(items_schema());
    let engine = GraphqlEngine::from_schema_catalog(&manifest, pool.clone())
        .unwrap()
        .roles(&["user"])
        .grant_all("user")
        .change_stream(change_rx)
        .build()
        .expect("build");

    (repo, engine, pool)
}

fn user_session() -> Session {
    let mut s = Session::new();
    s.set(ROLE_KEY, "user");
    s
}

fn static_schema() -> &'static TableSchema {
    Box::leak(Box::new(items_schema()))
}

async fn upsert_item(repo: &distributed::SqliteRepository, id: &str, name: &str, status: &str) {
    let schema = static_schema();
    let mut values = RowValues::new();
    values.insert("id", RowValue::String(id.into()));
    values.insert("name", RowValue::String(name.into()));
    values.insert("status", RowValue::String(status.into()));
    let plan = TableWritePlan::new(vec![TableMutation::UpsertRow(TableRowMutation {
        schema,
        key: RowKey::new([("id", RowValue::String(id.into()))]),
        values,
        expected_version: ExpectedVersion::Any,
        mode: RowWriteMode::Upsert,
    })]);
    repo.commit_write_plan(plan).await.unwrap();
}

#[tokio::test]
async fn subscription_pushes_exactly_once_per_commit() {
    let (repo, engine, _pool) = setup_fixed().await;
    let session = user_session();

    // Filtered subscription: only open items (nested selection of fields).
    let request = Request::new(
        r#"subscription { items(where: { status: { _eq: "open" } }) { id name status } }"#,
    );
    let mut stream = Box::pin(engine.execute_stream(&session, request));

    // Initial push
    let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("timeout waiting initial")
        .expect("stream ended");
    assert!(!first.is_err(), "initial errors: {:?}", first.errors);
    let data = serde_json::to_value(&first.data).unwrap();
    let items = data["items"].as_array().expect("items array");
    assert_eq!(items.len(), 1, "only i1 is open: {data}");
    assert_eq!(items[0]["id"], "i1");

    // Commit a projection that changes the filtered set (i2 becomes open).
    upsert_item(&repo, "i2", "beta", "open").await;

    // Exactly one push after debounce
    let second = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("timeout waiting push")
        .expect("stream ended");
    assert!(!second.is_err(), "push errors: {:?}", second.errors);
    let data = serde_json::to_value(&second.data).unwrap();
    let items = data["items"].as_array().unwrap();
    assert_eq!(items.len(), 2, "both open after commit: {data}");

    // No third push without another commit (debounce + idle).
    let third = tokio::time::timeout(Duration::from_millis(300), stream.next()).await;
    assert!(
        third.is_err(),
        "idle subscription must not push without a commit; got {:?}",
        third
    );
}

#[tokio::test]
async fn hash_gate_no_push_when_result_unchanged() {
    let (repo, engine, _pool) = setup_fixed().await;
    let session = user_session();

    let request = Request::new(r#"subscription { items { id name status } }"#);
    let mut stream = Box::pin(engine.execute_stream(&session, request));

    let _initial = stream.next().await.expect("initial");

    // Upsert same values for i1 — projection commit fires, result unchanged → no push.
    upsert_item(&repo, "i1", "alpha", "open").await;

    let next = tokio::time::timeout(Duration::from_millis(400), stream.next()).await;
    assert!(
        next.is_err(),
        "hash gate must suppress push when payload unchanged"
    );
}

#[tokio::test]
async fn subscription_unknown_role_returns_error_response() {
    let (_repo, engine, _pool) = setup_fixed().await;
    let mut session = user_session();
    session.set(ROLE_KEY, "ghost");

    let request = Request::new(r#"subscription { items { id name status } }"#);
    let mut stream = Box::pin(engine.execute_stream(&session, request));

    let response = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("timeout waiting error")
        .expect("stream ended");
    assert!(
        response.is_err(),
        "unknown role must return an error response"
    );
    // Set-only + surface authority: unconfigured singleton role cannot open a
    // role surface (no primary-role schema lookup fallback).
    assert_eq!(
        response.errors[0].message,
        "GraphQL execution requires a named application surface for multi-role principals, a membership-checked role surface, or an anonymous session"
    );
}

#[tokio::test]
async fn broadcast_fires_on_write_plan_commit() {
    let (repo, _engine, _pool) = setup_fixed().await;
    let mut rx = repo.read_model_changes();
    upsert_item(&repo, "i3", "gamma", "open").await;
    let change = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("timeout")
        .expect("recv");
    assert!(change.tables.contains("items"), "{change:?}");
}

#[tokio::test]
async fn zero_receiver_send_is_noop() {
    let pool = SqlitePoolOptions::new()
        .connect("sqlite::memory:")
        .await
        .unwrap();
    let repo = distributed::SqliteRepository::new(pool);
    repo.publish_read_model_change(ReadModelChange::new(["items"]));
}

#[test]
fn response_hash_stable() {
    use async_graphql::Value;
    let a = Value::from(1);
    let b = Value::from(1);
    let c = Value::from(2);
    assert_eq!(
        distributed::graphql::subscribe::response_hash(&a),
        distributed::graphql::subscribe::response_hash(&b)
    );
    assert_ne!(
        distributed::graphql::subscribe::response_hash(&a),
        distributed::graphql::subscribe::response_hash(&c)
    );
}

/// Live stream re-exec must AND claim row filters (same as query path).
/// Two tenants subscribe with the same document; each only ever sees own rows
/// on the initial push and after a cross-tenant write.
#[tokio::test]
async fn subscription_claim_isolation_across_tenants() {
    use distributed::{ReadModel, RelationalReadModel};
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
    #[table("notes")]
    struct NoteView {
        #[id("note_id")]
        note_id: String,
        owner_id: String,
        body: String,
    }

    let repo = distributed::SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .unwrap();
    let pool = repo.pool().clone();
    sqlx::query(
        "CREATE TABLE notes (
            note_id TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            body TEXT NOT NULL,
            _sourced_version INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO notes (note_id, owner_id, body, _sourced_version) VALUES
            ('n-a1', 'tenant-a', 'a1', 1),
            ('n-b1', 'tenant-b', 'b1', 1);",
    )
    .execute(&pool)
    .await
    .unwrap();

    let change_rx = repo.read_model_changes();

    let engine = GraphqlEngine::builder(pool.clone())
        .roles(&["user"])
        .model::<NoteView>(
            ModelPermissions::new().grant(
                "user",
                read()
                    .all_columns()
                    .rows(col("owner_id").eq(claim("x-user-id"))),
            ),
        )
        .change_stream(change_rx)
        .build()
        .expect("build");

    fn tenant_session(tenant: &str) -> Session {
        let mut s = Session::new();
        s.set(ROLE_KEY, "user");
        s.set(USER_ID_KEY, tenant);
        s
    }

    let sub_doc = r#"subscription { notes { note_id owner_id body } }"#;

    let mut stream_a =
        Box::pin(engine.execute_stream(&tenant_session("tenant-a"), Request::new(sub_doc)));
    let mut stream_b =
        Box::pin(engine.execute_stream(&tenant_session("tenant-b"), Request::new(sub_doc)));

    let first_a = tokio::time::timeout(Duration::from_secs(2), stream_a.next())
        .await
        .expect("timeout A")
        .expect("stream A ended");
    assert!(!first_a.is_err(), "{:?}", first_a.errors);
    let data_a = serde_json::to_value(&first_a.data).unwrap();
    let notes_a = data_a["notes"].as_array().expect("notes A");
    assert_eq!(notes_a.len(), 1, "A must only see own row: {data_a}");
    assert_eq!(notes_a[0]["owner_id"], "tenant-a");
    assert_eq!(notes_a[0]["note_id"], "n-a1");

    let first_b = tokio::time::timeout(Duration::from_secs(2), stream_b.next())
        .await
        .expect("timeout B")
        .expect("stream B ended");
    assert!(!first_b.is_err(), "{:?}", first_b.errors);
    let data_b = serde_json::to_value(&first_b.data).unwrap();
    let notes_b = data_b["notes"].as_array().expect("notes B");
    assert_eq!(notes_b.len(), 1, "B must only see own row: {data_b}");
    assert_eq!(notes_b[0]["owner_id"], "tenant-b");

    // Commit a new row for A — only stream A may receive it.
    let schema = NoteView::schema();
    let mut values = RowValues::new();
    values.insert("note_id", RowValue::String("n-a2".into()));
    values.insert("owner_id", RowValue::String("tenant-a".into()));
    values.insert("body", RowValue::String("a2".into()));
    let plan = TableWritePlan::new(vec![TableMutation::UpsertRow(TableRowMutation {
        schema,
        key: RowKey::new([("note_id", RowValue::String("n-a2".into()))]),
        values,
        expected_version: ExpectedVersion::Any,
        mode: RowWriteMode::Upsert,
    })]);
    repo.commit_write_plan(plan).await.unwrap();

    let push_a = tokio::time::timeout(Duration::from_secs(2), stream_a.next())
        .await
        .expect("timeout A push")
        .expect("stream A ended");
    assert!(!push_a.is_err(), "{:?}", push_a.errors);
    let data_a2 = serde_json::to_value(&push_a.data).unwrap();
    let notes_a2 = data_a2["notes"].as_array().unwrap();
    assert_eq!(notes_a2.len(), 2, "A sees both own notes: {data_a2}");
    assert!(
        notes_a2.iter().all(|n| n["owner_id"] == "tenant-a"),
        "A stream leaked foreign owner: {data_a2}"
    );

    // B's payload is unchanged (still one row) — hash gate may suppress a push.
    // If a push arrives, it must still be only tenant-b.
    if let Ok(Some(push_b)) =
        tokio::time::timeout(Duration::from_millis(500), stream_b.next()).await
    {
        assert!(!push_b.is_err(), "{:?}", push_b.errors);
        let data_b2 = serde_json::to_value(&push_b.data).unwrap();
        let notes_b2 = data_b2["notes"].as_array().unwrap();
        assert_eq!(notes_b2.len(), 1, "B must not see A's insert: {data_b2}");
        assert_eq!(notes_b2[0]["owner_id"], "tenant-b");
    }
}
