//! A `Json` column reaches the client as JSON, never as a string holding
//! JSON, on every read path and on both dialects. A text column whose value
//! looks like JSON stays a string.

use distributed::graphql::IdentityConfig;
use distributed::graphql::{read, GraphqlEngine};
use distributed::{
    ColumnType, PrimaryKey, ReadModel, ReadModelCatalog, RelationalReadModel, RelationshipDef,
    RelationshipKind, TableColumn, TableKind, TableSchema,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use common::graphql::{exec_json, session, Db};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("folders")]
struct Folder {
    #[id("folder_id")]
    folder_id: String,
    settings: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("docs")]
struct Doc {
    #[id("doc_id")]
    doc_id: String,
    folder_id: String,
    payload: serde_json::Value,
    /// Text that happens to parse as JSON.
    note: String,
}

fn relationship(field: &str, kind: RelationshipKind, target: &str) -> RelationshipDef {
    RelationshipDef {
        references: None,
        field_name: field.into(),
        kind,
        target_model: target.into(),
        foreign_key: Some("folder_id".into()),
        through: None,
        target_foreign_key: None,
    }
}

pub async fn json_columns_are_json_on_every_read_path(db: Db) {
    let payload = json!({ "a": 1, "tags": ["x", "y"], "nested": { "ok": true } });
    let settings = json!(["list", { "k": "v" }]);
    db.seed([Folder {
        folder_id: "f1".into(),
        settings: settings.clone(),
    }])
    .await;
    db.seed([Doc {
        doc_id: "d1".into(),
        folder_id: "f1".into(),
        payload: payload.clone(),
        note: r#"{"looks":"json"}"#.into(),
    }])
    .await;

    let mut folders = Folder::schema().clone();
    folders.relationships = vec![relationship("docs", RelationshipKind::HasMany, "Doc")];
    let mut docs = Doc::schema().clone();
    docs.relationships = vec![relationship(
        "folder",
        RelationshipKind::BelongsTo,
        "Folder",
    )];
    let catalog = ReadModelCatalog::new("json-columns")
        .table_schema(folders)
        .table_schema(docs);
    let engine = GraphqlEngine::from_schema_catalog(&catalog, db.pool())
        .unwrap()
        .identity(IdentityConfig::dev_headers())
        .roles(&["user"])
        .permission::<Folder>("user", read().all_columns().aggregations())
        .permission::<Doc>("user", read().all_columns().aggregations())
        .build()
        .unwrap();

    let doc = json!({ "payload": payload, "note": r#"{"looks":"json"}"# });
    let cases = [
        (
            "list",
            "{ docs { payload note } }",
            json!({ "docs": [doc] }),
        ),
        (
            "by-pk",
            r#"{ docs_by_pk(doc_id: "d1") { payload note } }"#,
            json!({ "docs_by_pk": doc }),
        ),
        (
            "aggregate nodes",
            "{ docs_aggregate { nodes { payload note } } }",
            json!({ "docs_aggregate": { "nodes": [doc] } }),
        ),
        (
            "has-many",
            "{ folders { settings docs { payload note } } }",
            json!({ "folders": [{ "settings": settings, "docs": [doc] }] }),
        ),
        (
            "belongs-to",
            "{ docs { folder { settings } } }",
            json!({ "docs": [{ "folder": { "settings": settings } }] }),
        ),
        (
            "relationship aggregate nodes",
            "{ folders { docs_aggregate { nodes { payload } } } }",
            json!({ "folders": [{ "docs_aggregate": { "nodes": [{ "payload": payload }] } }] }),
        ),
    ];
    for (path, query, expected) in cases {
        let data = exec_json(&engine, &session("user", "u"), query).await;
        assert_eq!(data, expected, "{path}: {query}");
    }
}

/// A row wide enough that the projection is built in several chunks keeps
/// its JSON column as JSON in the last one.
pub async fn json_column_in_a_wide_row_is_json(db: Db) {
    let mut columns = vec![TableColumn {
        primary_key: true,
        ..TableColumn::new("id", "id", ColumnType::Text)
    }];
    columns.extend((0..44).map(|n| {
        let name = format!("c{n}");
        TableColumn::new(name.clone(), name, ColumnType::Text)
    }));
    columns.push(TableColumn::new("payload", "payload", ColumnType::Json));
    let schema = TableSchema {
        model_name: "WideView".into(),
        table_name: "wide".into(),
        columns,
        primary_key: PrimaryKey::new(["id"]),
        version_column: None,
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    };
    let payload = json!({ "a": [1, { "b": null }] });
    let mut row = vec![json!("w1")];
    row.extend((0..44).map(|n| json!(format!("v{n}"))));
    row.push(payload.clone());
    db.table(&schema, json!([row])).await;

    let fields: Vec<String> = (0..44).map(|n| format!("c{n}")).collect();
    let engine = GraphqlEngine::from_schema_catalog(
        &ReadModelCatalog::new("wide").table_schema(schema),
        db.pool(),
    )
    .unwrap()
    .identity(IdentityConfig::dev_headers())
    .roles(&["user"])
    .grant_all("user")
    .build()
    .unwrap();
    let data = exec_json(
        &engine,
        &session("user", "u"),
        &format!("{{ wide {{ id {} payload }} }}", fields.join(" ")),
    )
    .await;
    assert_eq!(data["wide"][0]["payload"], payload, "{data}");
    assert_eq!(data["wide"][0]["c43"], "v43");
}
