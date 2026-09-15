//! Real-database coverage for literal search through the generated GraphQL API.
#![cfg(all(feature = "graphql", any(feature = "sqlite", feature = "postgres")))]

use async_graphql::{Request, Variables};
use distributed::{
    graphql::{claim, col, read, GraphqlEngine, ModelPermissions},
    microsvc::Session,
    ReadModel, ROLE_KEY, USER_ID_KEY,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("gql_literal_text")]
struct SearchRow {
    #[id("id")]
    id: String,
    label: Option<String>,
    owner: String,
    number: i64,
}

const DDL: &str = "CREATE TEMP TABLE gql_literal_text (id TEXT PRIMARY KEY, label TEXT, owner TEXT NOT NULL, number BIGINT NOT NULL)";
const ROWS: &[(&str, Option<&str>, &str)] = &[
    ("01", Some("A%_!\\'BC"), "alice"),
    ("02", Some("aZQ!\\'bc"), "alice"),
    ("03", None, "alice"),
    ("04", Some(""), "alice"),
    ("05", Some("A%_!\\'BC"), "bob"),
];

fn permissions() -> ModelPermissions<SearchRow> {
    ModelPermissions::new().grant(
        "user",
        read()
            .all_columns()
            .rows(col("owner").eq(claim("x-user-id"))),
    )
}

async fn verify(engine: GraphqlEngine) {
    let mut session = Session::new();
    session.set(ROLE_KEY, "user");
    session.set(USER_ID_KEY, "alice");
    for (text, expected) in [
        ("%", vec!["01"]),
        ("_", vec!["01"]),
        ("%_", vec!["01"]),
        ("!", vec!["01", "02"]),
        ("\\", vec!["01", "02"]),
        ("'", vec!["01", "02"]),
        ("bc", vec!["01", "02"]),
        ("a%_!\\'bc", vec!["01"]),
        ("' OR 1=1 --", vec![]),
        ("", vec!["01", "02", "04"]),
    ] {
        let response = engine.execute(&session, Request::new(
            "query($q: String!) { gql_literal_text(where: {label: {_icontains: $q}}, order_by: [{id: asc}], limit: 20) {id} }"
        ).variables(Variables::from_json(json!({"q": text})))).await;
        assert!(
            response.errors.is_empty(),
            "{text:?}: {:?}",
            response.errors
        );
        let data = response.data.into_json().unwrap();
        let ids: Vec<_> = data["gql_literal_text"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, expected, "operand {text:?}");
    }
    let page = engine.execute(&session, Request::new(
        "{ gql_literal_text(where: {label: {_icontains: \"bc\"}}, order_by: [{id: asc}], limit: 1, offset: 1) {id} }"
    )).await;
    assert!(page.errors.is_empty(), "{:?}", page.errors);
    assert_eq!(
        page.data.into_json().unwrap(),
        json!({"gql_literal_text":[{"id":"02"}]})
    );
    for query in [
        "{ gql_literal_text(where: {number: {_icontains: \"1\"}}) {id} }",
        "{ gql_literal_text(where: {label: {_icontains: 1}}) {id} }",
    ] {
        assert!(!engine
            .execute(&session, Request::new(query))
            .await
            .errors
            .is_empty());
    }
    // Existing wildcard semantics remain intentionally different.
    let pattern = engine.execute(&session, Request::new(
        "{ gql_literal_text(where: {label: {_ilike: \"a%bc\"}}, order_by: [{id: asc}]) {id} }"
    )).await;
    assert!(pattern.errors.is_empty(), "{:?}", pattern.errors);
    assert_eq!(
        pattern.data.into_json().unwrap(),
        json!({"gql_literal_text":[{"id":"01"},{"id":"02"}]})
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_literal_text() {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::query(DDL).execute(&pool).await.unwrap();
    for (id, label, owner) in ROWS {
        sqlx::query("INSERT INTO gql_literal_text VALUES (?, ?, ?, 1)")
            .bind(id)
            .bind(label)
            .bind(owner)
            .execute(&pool)
            .await
            .unwrap();
    }
    verify(
        GraphqlEngine::builder(pool)
            .roles(&["user"])
            .model::<SearchRow>(permissions())
            .build()
            .unwrap(),
    )
    .await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_literal_text() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skip: DATABASE_URL unset");
        return;
    };
    // Session-local table and a single connection: no retained tables changed.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    sqlx::query(DDL).execute(&pool).await.unwrap();
    for (id, label, owner) in ROWS {
        sqlx::query("INSERT INTO gql_literal_text VALUES ($1, $2, $3, 1)")
            .bind(id)
            .bind(label)
            .bind(owner)
            .execute(&pool)
            .await
            .unwrap();
    }
    verify(
        GraphqlEngine::builder(pool)
            .roles(&["user"])
            .model::<SearchRow>(permissions())
            .build()
            .unwrap(),
    )
    .await;
}
