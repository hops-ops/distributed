//! Production compile-path behavioral goldens (via GraphqlEngine::execute).
//! Empty placeholder removed — suite drives shipped compile_root through execute.

use async_graphql::Request;
use distributed::graphql::IdentityConfig;
use distributed::graphql::{read, FilterExpr, GraphqlEngine, LitValue, ModelPermissions, Truth};
use distributed::microsvc::{Session, ROLE_KEY};
use distributed::read_model::{ReadModel, RelationalReadModel};
use distributed::table::ColumnType;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use common::graphql::Db;
use common::on_each_dialect;

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
        .identity(IdentityConfig::dev_headers())
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
    status: String,
    owner: String,
    weight: f64,
    total: i64,
    note: Option<String>,
    rank: Option<i32>,
    score: Option<f64>,
    flagged: Option<bool>,
}

/// `tests/fixtures/client-query-plan-corpus.json`: the one statement of what
/// a `where`, a row policy and an `order_by` mean. This file checks it against
/// SQLite, Postgres and `FilterExpr::evaluate`;
/// `js/tests/replica-query-plan-corpus.test.mjs` checks the client replica.
#[derive(Serialize, Deserialize)]
struct ClientPlanCorpus {
    /// The server's own description of each field, as the client manifest
    /// carries it. The JS consumer builds its plan artifacts from this.
    fields: JsonValue,
    /// Session headers a row policy may read, in trusted-preset form.
    claims: Vec<CorpusClaim>,
    records: Vec<ClientPlanRow>,
    cases: Vec<ClientPlanCase>,
}

#[derive(Serialize, Deserialize)]
struct CorpusClaim {
    name: String,
    codec: String,
    value: JsonValue,
}

/// One filter (a client `where` or a row policy, never both) and one page.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ClientPlanCase {
    name: String,
    #[serde(rename = "where", default, skip_serializing_if = "Option::is_none")]
    where_: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    row_policy: Option<FilterExpr>,
    order_by: JsonValue,
    limit: i32,
    offset: i32,
    /// The page the server returns, in order.
    #[serde(default)]
    expected: Vec<String>,
    /// Every row the filter is TRUE for.
    #[serde(default)]
    matches: Vec<String>,
    /// Rows the filter is NULL for: neither it nor its negation returns them.
    #[serde(default)]
    unknown: Vec<String>,
    /// Rows SQL decides but an in-process evaluator must leave undecided.
    /// `"*"` is every row SQL decides.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    undecided: Vec<String>,
}

const CORPUS_PATH: &str = "tests/fixtures/client-query-plan-corpus.json";

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

fn corpus_engine(db: &Db, policy: Option<FilterExpr>) -> GraphqlEngine {
    let read = match policy {
        Some(policy) => read().all_columns().rows(policy),
        None => read().all_columns(),
    };
    GraphqlEngine::builder(db.pool())
        .identity(IdentityConfig::dev_headers())
        .service_id("corpus")
        .roles(&["user"])
        .model::<ClientPlanRow>(ModelPermissions::new().grant("user", read))
        .build()
        .unwrap()
}

async fn corpus_ids(engine: &GraphqlEngine, session: &Session, arguments: &str) -> Vec<String> {
    let query = format!("{{ client_plan_rows({arguments}) {{ id }} }}");
    let response = engine.execute(session, Request::new(&query)).await;
    assert!(!response.is_err(), "{query}: {:?}", response.errors);
    serde_json::to_value(response.data).unwrap()["client_plan_rows"]
        .as_array()
        .expect("rows")
        .iter()
        .map(|row| row["id"].as_str().expect("string id").to_string())
        .collect()
}

/// The manifest's description of each corpus field, in the corpus's shape.
fn manifest_fields(engine: &GraphqlEngine) -> JsonValue {
    let manifest = serde_json::to_value(engine.client_manifest_for_role("user").unwrap()).unwrap();
    let model = &manifest["models"][0];
    let operators = |name: &JsonValue| {
        model["filter_input"]["fields"]
            .as_array()
            .unwrap()
            .iter()
            .find(|field| &field["name"] == name)
            .map(|field| field["operators"].clone())
            .expect("filter field")
    };
    model["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|field| {
            serde_json::json!({
                "field": field["name"],
                "scalar": field["scalar"],
                "codec": field["codec"],
                "nullable": field["nullable"],
                "operators": operators(&field["name"]),
            })
        })
        .collect()
}

/// A claim as the literal the compiler binds for a column of `column_type`.
fn claim_literal(
    claims: &[CorpusClaim],
    header: &str,
    column_type: &ColumnType,
) -> Option<LitValue> {
    let value = &claims.iter().find(|claim| claim.name == header)?.value;
    Some(match column_type {
        ColumnType::Text => LitValue::String(value.as_str()?.to_string()),
        ColumnType::Boolean => LitValue::Bool(value.as_bool()?),
        ColumnType::Integer => LitValue::I64(value.as_i64()?),
        ColumnType::Float => LitValue::F64(value.as_f64()?),
        _ => return None,
    })
}

/// Set `DISTRIBUTED_BLESS_CORPUS=1` to rewrite each case's `expected`,
/// `matches` and `unknown` (and `fields`) from what SQLite returns. Postgres
/// and both evaluators are then held to the rewritten file, so review its diff.
pub async fn query_plan_corpus_matches_server_semantics(db: Db) {
    let mut corpus: ClientPlanCorpus =
        serde_json::from_str(&std::fs::read_to_string(CORPUS_PATH).expect("read corpus"))
            .expect("parse shared client query-plan corpus");
    let bless =
        std::env::var_os("DISTRIBUTED_BLESS_CORPUS").is_some() && matches!(db, Db::Sqlite(_));
    db.seed(corpus.records.clone()).await;

    let mut session = user();
    for claim in &corpus.claims {
        let value = match &claim.value {
            JsonValue::String(value) => value.clone(),
            other => other.to_string(),
        };
        session.set(&claim.name, value);
    }
    let unrestricted = corpus_engine(&db, None);
    let fields = manifest_fields(&unrestricted);
    if bless {
        corpus.fields = fields;
    } else {
        assert_eq!(
            corpus.fields, fields,
            "corpus fields drifted from the manifest"
        );
    }
    let all = corpus
        .records
        .iter()
        .map(|record| record.id.clone())
        .collect::<Vec<_>>();
    let sorted = |mut ids: Vec<String>| {
        ids.sort();
        ids
    };

    for case in &mut corpus.cases {
        let name = &case.name;
        let (engine, negated_engine, filter, negated_filter) =
            match (&case.where_, &case.row_policy) {
                (Some(filter), None) => {
                    let filter = graphql_input(filter);
                    let negated = format!("where: {{_not: {filter}}}, ");
                    (
                        corpus_engine(&db, None),
                        corpus_engine(&db, None),
                        format!("where: {filter}, "),
                        negated,
                    )
                }
                (None, Some(policy)) => (
                    corpus_engine(&db, Some(policy.clone())),
                    corpus_engine(&db, Some(FilterExpr::Not(Box::new(policy.clone())))),
                    String::new(),
                    String::new(),
                ),
                _ => panic!("{name}: a case has exactly one of `where` and `rowPolicy`"),
            };
        let page = corpus_ids(
            &engine,
            &session,
            &format!(
                "{filter}order_by: {}, limit: {}, offset: {}",
                graphql_order(&case.order_by),
                case.limit,
                case.offset
            ),
        )
        .await;
        let matches = sorted(corpus_ids(&engine, &session, &format!("{filter}limit: 1000")).await);
        let rejected = corpus_ids(
            &negated_engine,
            &session,
            &format!("{negated_filter}limit: 1000"),
        )
        .await;
        let unknown = sorted(
            all.iter()
                .filter(|id| !matches.contains(id) && !rejected.contains(id))
                .cloned()
                .collect(),
        );
        if bless {
            (case.expected, case.matches, case.unknown) = (page, matches, unknown);
        } else {
            assert_eq!(page, case.expected, "{name}: page");
            assert_eq!(matches, case.matches, "{name}: matches");
            assert_eq!(unknown, case.unknown, "{name}: unknown");
        }
        let undecided = |id: &String| {
            case.undecided.contains(id)
                || (case.undecided.iter().any(|id| id == "*") && !case.unknown.contains(id))
        };
        for id in &case.undecided {
            assert!(
                !case.unknown.contains(id),
                "{name}: SQL does not decide `{id}` either"
            );
        }

        let Some(policy) = &case.row_policy else {
            continue;
        };
        for record in &corpus.records {
            let JsonValue::Object(row) = serde_json::to_value(record).unwrap() else {
                unreachable!("a record is an object");
            };
            let expected = if case.unknown.contains(&record.id) || undecided(&record.id) {
                Truth::Unknown
            } else if case.matches.contains(&record.id) {
                Truth::True
            } else {
                Truth::False
            };
            let actual = policy.evaluate(ClientPlanRow::schema(), &row, &|claim, column_type| {
                claim_literal(&corpus.claims, &claim.header, column_type)
            });
            assert_eq!(
                actual, expected,
                "{name}: FilterExpr::evaluate on row `{}`",
                record.id
            );
        }
    }

    if bless {
        // One field, claim, record or case per line keeps the diff reviewable.
        fn lines<T: Serialize>(items: &[T]) -> String {
            items
                .iter()
                .map(|item| format!("    {}", serde_json::to_string(item).unwrap()))
                .collect::<Vec<_>>()
                .join(",\n")
        }
        let text = format!(
            "{{\n  \"fields\": [\n{}\n  ],\n  \"claims\": [\n{}\n  ],\n  \"records\": [\n{}\n  ],\n  \"cases\": [\n{}\n  ]\n}}\n",
            lines(corpus.fields.as_array().expect("fields")),
            lines(&corpus.claims),
            lines(&corpus.records),
            lines(&corpus.cases),
        );
        std::fs::write(CORPUS_PATH, text).expect("write blessed corpus");
    }
}

/// The keyset `where` a `@window` operation sends: rows after the cursor in
/// `priority desc, id asc` order (the server appends the primary key ASC).
const KEYSET_PAGE: &str = r#"query Page($limit: Int!, $cursor_priority: BigInt, $cursor_id: String) {
  client_plan_rows(
    where: {_or: [{priority: {_lt: $cursor_priority}}, {priority: {_eq: $cursor_priority}, id: {_gt: $cursor_id}}]}
    order_by: [{priority: desc}]
    limit: $limit
  ) { id }
}"#;

async fn keyset_ids(engine: &GraphqlEngine, variables: JsonValue) -> Vec<String> {
    let request =
        Request::new(KEYSET_PAGE).variables(async_graphql::Variables::from_json(variables));
    let response = engine.execute(&user(), request).await;
    assert!(!response.is_err(), "{:?}", response.errors);
    serde_json::to_value(response.data).unwrap()["client_plan_rows"]
        .as_array()
        .expect("rows")
        .iter()
        .map(|row| row["id"].as_str().expect("string id").to_string())
        .collect()
}

/// GraphQL resolves an omitted nullable variable to `null`; a `null`
/// comparison operand adds no predicate, so the head page (no cursor) is the
/// first page of the ordered result, as the client replica computes it.
pub async fn omitted_cursor_variables_select_the_head_page(db: Db) {
    let corpus: ClientPlanCorpus =
        serde_json::from_str(&std::fs::read_to_string(CORPUS_PATH).expect("read corpus"))
            .expect("parse corpus");
    db.seed(corpus.records).await;
    let engine = corpus_engine(&db, None);
    let head = keyset_ids(&engine, serde_json::json!({"limit": 3})).await;
    assert_eq!(head, ["2", "8", "5"]);
    let explicit_null = keyset_ids(
        &engine,
        serde_json::json!({"limit": 3, "cursor_priority": null, "cursor_id": null}),
    )
    .await;
    assert_eq!(explicit_null, head);
    // Each page continues after the previous page's last row; the tie on
    // priority 3 (ids 3 and 7) and on 5 (ids 2 and 8) is split by id.
    let mut pages = vec![head];
    loop {
        let last = pages.last().unwrap().last().cloned();
        let Some(last) = last else { break };
        let row = corpus_row(&last);
        let page = keyset_ids(
            &engine,
            serde_json::json!({"limit": 3, "cursor_priority": row.0, "cursor_id": row.1}),
        )
        .await;
        let done = page.len() < 3;
        pages.push(page);
        if done {
            break;
        }
    }
    assert_eq!(
        pages.concat(),
        ["2", "8", "5", "3", "7", "1", "6", "4", "9"],
        "keyset pages concatenate to the ordered result: {pages:?}"
    );
}

fn corpus_row(id: &str) -> (i64, String) {
    let priority = match id {
        "4" | "9" => 1,
        "1" | "6" => 2,
        "3" | "7" => 3,
        "5" => 4,
        "2" | "8" => 5,
        other => panic!("no corpus row `{other}`"),
    };
    (priority, id.to_string())
}

/// A read policy and a cursor compose: the cursor narrows the rows the
/// privilege may read and never widens them.
pub async fn keyset_cursor_composes_with_a_row_policy(db: Db) {
    let corpus: ClientPlanCorpus =
        serde_json::from_str(&std::fs::read_to_string(CORPUS_PATH).expect("read corpus"))
            .expect("parse corpus");
    db.seed(corpus.records).await;
    // Only `ana`'s rows (1, 2, 5, 8) are readable.
    let policy = distributed::graphql::col("owner").eq("ana");
    let engine = corpus_engine(&db, Some(policy));
    // `ana` in `priority desc, id asc`: 2 (5), 8 (5), 5 (4), 1 (2).
    assert_eq!(
        keyset_ids(&engine, serde_json::json!({"limit": 10})).await,
        ["2", "8", "5", "1"]
    );
    // After (5, "2"): 8 shares the priority and sorts after by id; 3 and 7
    // (priority 3, `bo`/`cy`) would follow 5 without the policy.
    assert_eq!(
        keyset_ids(
            &engine,
            serde_json::json!({"limit": 2, "cursor_priority": 5, "cursor_id": "2"})
        )
        .await,
        ["8", "5"]
    );
    assert_eq!(
        keyset_ids(
            &engine,
            serde_json::json!({"limit": 2, "cursor_priority": 4, "cursor_id": "5"})
        )
        .await,
        ["1"]
    );
}

on_each_dialect! { ;
    list_and_by_pk_compile_and_execute,
    where_eq_uses_production_compiler,
    query_plan_corpus_matches_server_semantics,
    omitted_cursor_variables_select_the_head_page,
    keyset_cursor_composes_with_a_row_policy,
}
