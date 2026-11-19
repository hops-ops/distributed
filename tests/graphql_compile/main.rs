//! Production compile-path behavioral goldens (via GraphqlEngine::execute).
//! Empty placeholder removed — suite drives shipped compile_root through execute.

#![cfg(all(feature = "graphql", feature = "sqlite"))]

use async_graphql::Request;
use distributed::graphql::{read, GraphqlEngine, ModelPermissions};
use distributed::microsvc::{Session, ROLE_KEY};
use distributed::ReadModel;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[path = "../support/graphql.rs"]
mod common;
use common::{on_each_dialect, Db};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("items")]
struct ItemView {
    #[id("id")]
    id: String,
    name: String,
}

fn user() -> Session {
    let mut s = Session::new();
    s.set(ROLE_KEY, "user");
    s
}

async fn engine(db: &Db) -> GraphqlEngine {
    let item = |id: &str, name: &str| ItemView {
        id: id.into(),
        name: name.into(),
    };
    db.seed([item("1", "a"), item("2", "b"), item("3", "c")])
        .await;
    GraphqlEngine::builder(db.pool())
        .roles(&["user"])
        .model::<ItemView>(ModelPermissions::new().grant("user", read().all_columns()))
        .build()
        .unwrap()
}

pub async fn list_and_by_pk_compile_and_execute(db: Db) {
    let engine = engine(&db).await;
    let resp = engine
        .execute(&user(), Request::new("{ items { id name } }"))
        .await;
    assert!(!resp.is_err(), "{:?}", resp.errors);
    let data = serde_json::to_value(&resp.data).unwrap();
    assert_eq!(data["items"].as_array().unwrap().len(), 3);

    let resp = engine
        .execute(
            &user(),
            Request::new(r#"{ items_by_pk(id: "2") { name } }"#),
        )
        .await;
    assert!(!resp.is_err(), "{:?}", resp.errors);
    let data = serde_json::to_value(&resp.data).unwrap();
    assert_eq!(data["items_by_pk"]["name"], "b");
}

pub async fn where_eq_uses_production_compiler(db: Db) {
    let engine = engine(&db).await;
    let resp = engine
        .execute(
            &user(),
            Request::new(r#"{ items(where: { name: { _eq: "a" } }) { id } }"#),
        )
        .await;
    assert!(!resp.is_err(), "{:?}", resp.errors);
    let data = serde_json::to_value(&resp.data).unwrap();
    assert_eq!(data["items"].as_array().unwrap().len(), 1);
    assert_eq!(data["items"][0]["id"], "1");
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("client_plan_rows")]
struct ClientPlanRow {
    #[id("id")]
    id: String,
    priority: i32,
    completed: bool,
}

#[derive(Deserialize)]
struct ClientPlanCorpus {
    records: Vec<ClientPlanRow>,
    cases: Vec<ClientPlanCase>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClientPlanCase {
    name: String,
    #[serde(rename = "where")]
    where_: JsonValue,
    order_by: JsonValue,
    limit: i32,
    offset: i32,
    expected: Vec<String>,
}

fn graphql_input(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => "null".into(),
        JsonValue::Bool(value) => value.to_string(),
        JsonValue::Number(value) => value.to_string(),
        JsonValue::String(value) => serde_json::to_string(value).expect("serialize string"),
        JsonValue::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(graphql_input)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        JsonValue::Object(values) => format!(
            "{{{}}}",
            values
                .iter()
                .map(|(name, value)| format!("{name}: {}", graphql_input(value)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn graphql_order(value: &JsonValue) -> String {
    let entries = value.as_array().expect("orderBy array");
    format!(
        "[{}]",
        entries
            .iter()
            .map(|entry| {
                let object = entry.as_object().expect("orderBy object");
                let (field, direction) = object.iter().next().expect("orderBy field");
                assert_eq!(object.len(), 1, "orderBy entry must be unambiguous");
                format!(
                    "{{{field}: {}}}",
                    direction.as_str().expect("orderBy direction")
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    )
}

pub async fn portable_query_plan_corpus_matches_server_semantics(db: Db) {
    let corpus: ClientPlanCorpus =
        serde_json::from_str(include_str!("../fixtures/client-query-plan-corpus.json"))
            .expect("parse shared client query-plan corpus");
    db.seed(corpus.records).await;
    let engine = GraphqlEngine::builder(db.pool())
        .roles(&["user"])
        .model::<ClientPlanRow>(ModelPermissions::new().grant("user", read().all_columns()))
        .build()
        .unwrap();

    for case in corpus.cases {
        let query = format!(
            "{{ client_plan_rows(where: {}, order_by: {}, limit: {}, offset: {}) {{ id }} }}",
            graphql_input(&case.where_),
            graphql_order(&case.order_by),
            case.limit,
            case.offset
        );
        let response = engine.execute(&user(), Request::new(query)).await;
        assert!(!response.is_err(), "{}: {:?}", case.name, response.errors);
        let data = serde_json::to_value(response.data).unwrap();
        let actual = data["client_plan_rows"]
            .as_array()
            .expect("rows")
            .iter()
            .map(|row| row["id"].as_str().expect("string id").to_string())
            .collect::<Vec<_>>();
        assert_eq!(actual, case.expected, "{}", case.name);
    }
}

on_each_dialect! { ;
    list_and_by_pk_compile_and_execute,
    where_eq_uses_production_compiler,
    portable_query_plan_corpus_matches_server_semantics,
}
