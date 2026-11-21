//! `_like`, `_ilike` and `_icontains` mean the same thing on SQLite and
//! Postgres: `%` and `_` are wildcards, `\` escapes, `_like` compares
//! characters exactly and the other two fold ASCII letters only.

use async_graphql::{Request, Variables};
use distributed::graphql::IdentityConfig;
use distributed::graphql::{col, read, GraphqlEngine, ModelPermissions};
use distributed::ReadModel;
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::common::{extension_code, session, Db};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("words")]
struct Word {
    #[id("word")]
    word: String,
}

const WORDS: [&str; 12] = [
    "Open", "open", "OPEN", "Émile", "émile", "100%", "100x", "a_b", "axb", "a*b", "a[b", r"a\b",
];

async fn engine(db: &Db) -> GraphqlEngine {
    db.seed(WORDS.map(|word| Word { word: word.into() })).await;
    GraphqlEngine::builder(db.pool())
        .identity(IdentityConfig::dev_headers())
        .roles(&["user", "capital_o", "any_open"])
        .model::<Word>(
            ModelPermissions::new()
                .grant("user", read().all_columns())
                .grant(
                    "capital_o",
                    read().all_columns().rows(col("word").like("O%")),
                )
                .grant(
                    "any_open",
                    read().all_columns().rows(col("word").ilike("open")),
                ),
        )
        .build()
        .unwrap()
}

async fn words(engine: &GraphqlEngine, role: &str, filter: serde_json::Value) -> Vec<String> {
    let response = engine
        .execute(
            &session(role, "u"),
            Request::new(
                "query($where: words_bool_exp) { words(where: $where, order_by: [{word: asc}]) { word } }",
            )
            .variables(Variables::from_json(json!({ "where": filter }))),
        )
        .await;
    assert!(
        response.errors.is_empty(),
        "{filter}: {:?}",
        response.errors
    );
    let data = serde_json::to_value(&response.data).unwrap();
    let mut words: Vec<String> = data["words"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["word"].as_str().unwrap().to_string())
        .collect();
    words.sort();
    words
}

fn sorted(expected: &[&str]) -> Vec<String> {
    let mut expected: Vec<String> = expected.iter().map(|word| word.to_string()).collect();
    expected.sort();
    expected
}

pub async fn string_operators_match_the_same_rows_on_every_dialect(db: Db) {
    let engine = engine(&db).await;
    let cases: [(&str, &str, &[&str]); 16] = [
        // `_like` compares characters exactly.
        ("_like", "open", &["open"]),
        ("_like", "O%", &["Open", "OPEN"]),
        ("_like", "%mile", &["Émile", "émile"]),
        ("_like", "É%", &["Émile"]),
        // `%` and `_` are wildcards; `\` makes the next character literal.
        ("_like", "100%", &["100%", "100x"]),
        ("_like", r"100\%", &["100%"]),
        ("_like", "a_b", &["a_b", "axb", "a*b", "a[b", r"a\b"]),
        ("_like", r"a\_b", &["a_b"]),
        ("_like", r"a\\b", &[r"a\b"]),
        // Characters special to SQLite's GLOB are literal text.
        ("_like", "a*b", &["a*b"]),
        ("_like", "a[b", &["a[b"]),
        // `_ilike` folds ASCII letters and nothing else.
        ("_ilike", "open", &["Open", "open", "OPEN"]),
        ("_ilike", "É%", &["Émile"]),
        ("_ilike", r"100\%", &["100%"]),
        // `_icontains` takes literal text and folds the same way.
        ("_icontains", "PEN", &["Open", "open", "OPEN"]),
        ("_icontains", "%", &["100%"]),
    ];
    for (operator, pattern, expected) in cases {
        assert_eq!(
            words(&engine, "user", json!({ "word": { operator: pattern } })).await,
            sorted(expected),
            "{operator} {pattern:?}"
        );
    }
}

pub async fn row_policy_patterns_match_the_same_rows_on_every_dialect(db: Db) {
    let engine = engine(&db).await;
    assert_eq!(
        words(&engine, "capital_o", json!({})).await,
        sorted(&["Open", "OPEN"])
    );
    assert_eq!(
        words(&engine, "any_open", json!({})).await,
        sorted(&["Open", "open", "OPEN"])
    );
}

pub async fn pattern_ending_in_its_escape_is_a_bad_request(db: Db) {
    let engine = engine(&db).await;
    for operator in ["_like", "_ilike"] {
        let response = engine
            .execute(
                &session("user", "u"),
                Request::new(
                    "query($p: String!) { words(where: {word: {OP: $p}}) { word } }"
                        .replace("OP", operator),
                )
                .variables(Variables::from_json(json!({ "p": r"open\" }))),
            )
            .await;
        let code = response.errors.first().and_then(extension_code);
        assert!(
            code.as_deref()
                .is_some_and(|code| code.contains("BAD_REQUEST")),
            "{operator}: {:?}",
            response.errors
        );
    }
}
