//! Process-plan read stores for GraphQL.
//!
//! Read **models** stay host-agnostic (`DCS-DEC-008`). The engine mounts a
//! [`ReadStore`] per model: SQL scan (default) or cell GET-by-pk.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::Value;

use crate::command_dispatch::HttpCommandHost;
use crate::microsvc::cell_host::InternalHttpSecret;

/// How one GraphQL model is served by this process.
#[derive(Clone)]
pub enum ReadStore {
    /// SQL scan: list/filter/sort/join/`@live` (playground default).
    Sql,
    /// Sealed cell row by primary key only (`DCS-REQ-009`).
    CellByKey(Arc<dyn CellByKeyGetter>),
}

impl std::fmt::Debug for ReadStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sql => f.write_str("Sql"),
            Self::CellByKey(_) => f.write_str("CellByKey"),
        }
    }
}

impl PartialEq for ReadStore {
    fn eq(&self, other: &Self) -> bool {
        matches!((self, other), (Self::Sql, Self::Sql))
            || matches!((self, other), (Self::CellByKey(_), Self::CellByKey(_)))
    }
}

/// GET the sealed JSON row for one primary key (`DCS-AC-010.1` cell GET).
#[async_trait]
pub trait CellByKeyGetter: Send + Sync {
    async fn get_sealed_row(
        &self,
        primary_key: &BTreeMap<String, String>,
    ) -> Result<Option<Value>, String>;
}

/// HTTP GET `{base}/{pk}` of the sealed row (Todo `/todo/{id}`, Blob `/blob/{game_id}`).
#[derive(Clone)]
pub struct HttpCellByKey {
    http: HttpCommandHost,
}

impl HttpCellByKey {
    pub fn new(base: impl AsRef<str>, internal_secret: InternalHttpSecret) -> Result<Self, String> {
        Ok(Self {
            http: HttpCommandHost::new_internal(base, internal_secret)
                .map_err(|error| error.to_string())?,
        })
    }
}

#[async_trait]
impl CellByKeyGetter for HttpCellByKey {
    async fn get_sealed_row(
        &self,
        primary_key: &BTreeMap<String, String>,
    ) -> Result<Option<Value>, String> {
        if primary_key.len() != 1 {
            return Err("cell-by-key HTTP GET requires exactly one primary-key field".into());
        }
        let id = primary_key.values().next().expect("length checked");
        let (status, body) = self
            .http
            .get_json(id)
            .await
            .map_err(|error| format!("cell GET failed: {error}"))?;
        if status == 404 {
            return Ok(None);
        }
        if !(200..300).contains(&status) {
            return Err(format!("cell GET returned HTTP {status}"));
        }
        Ok(Some(body))
    }
}

/// In-memory sealed rows for compiler/engine tests.
#[derive(Clone, Default)]
pub struct MapCellByKey {
    rows: Arc<Mutex<BTreeMap<String, Value>>>,
}

impl MapCellByKey {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, pk: impl Into<String>, row: Value) {
        self.rows
            .lock()
            .expect("cell map lock")
            .insert(pk.into(), row);
    }
}

#[async_trait]
impl CellByKeyGetter for MapCellByKey {
    async fn get_sealed_row(
        &self,
        primary_key: &BTreeMap<String, String>,
    ) -> Result<Option<Value>, String> {
        if primary_key.len() != 1 {
            return Err("cell-by-key map requires exactly one primary-key field".into());
        }
        let id = primary_key.values().next().expect("length checked");
        Ok(self.rows.lock().expect("cell map lock").get(id).cloned())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReadStoreKind {
    SqlScan,
    CellByKey,
}

impl ReadStore {
    pub(crate) fn kind(&self) -> ReadStoreKind {
        match self {
            Self::Sql => ReadStoreKind::SqlScan,
            Self::CellByKey(_) => ReadStoreKind::CellByKey,
        }
    }

    pub(crate) fn cell_getter(&self) -> Option<Arc<dyn CellByKeyGetter>> {
        match self {
            Self::Sql => None,
            Self::CellByKey(getter) => Some(Arc::clone(getter)),
        }
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::*;
    use crate::graphql::compile::{compile_query, QueryPlan, RootKind, SelectionNode};
    use crate::graphql::{claim, col, read, GraphqlEngine, ModelPermissions, ReadStore};
    use crate::microsvc::Session;
    use crate::ReadModel;
    use async_graphql::Request;
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    #[derive(Clone, Serialize, Deserialize, ReadModel)]
    #[readmodel(primary_key = ["id"])]
    struct Todos {
        #[readmodel(id)]
        id: String,
        title: String,
    }

    #[derive(Clone, Serialize, Deserialize, ReadModel)]
    #[readmodel(primary_key = ["user_id"])]
    struct AuthUsers {
        #[readmodel(id)]
        user_id: String,
    }

    #[derive(Clone, Serialize, Deserialize, ReadModel)]
    #[readmodel(primary_key = ["game_id"])]
    struct BlobGames {
        #[readmodel(id)]
        game_id: String,
        owner_id: String,
        score: i64,
        #[readmodel(belongs_to = "AuthUsers", foreign_key = "owner_id")]
        owner: Option<AuthUsers>,
    }

    fn pool() -> sqlx::SqlitePool {
        sqlx::SqlitePool::connect_lazy("sqlite::memory:").unwrap()
    }

    fn session_user() -> Session {
        let mut session = Session::new();
        session.set(crate::microsvc::ROLE_KEY, "user");
        session.set(crate::microsvc::USER_ID_KEY, "alice");
        session
    }

    fn blob_perms() -> ModelPermissions<BlobGames> {
        ModelPermissions::new().grant(
            "user",
            read()
                .all_columns()
                .rows(col("owner_id").eq(claim("x-user-id"))),
        )
    }

    fn todo_perms() -> ModelPermissions<Todos> {
        ModelPermissions::new().grant(
            "user",
            read().all_columns().rows(col("id").eq(claim("x-user-id"))),
        )
    }

    fn user_perms() -> ModelPermissions<AuthUsers> {
        ModelPermissions::new().grant("user", read().all_columns())
    }

    fn list_selection() -> SelectionNode {
        SelectionNode {
            response_key: "todos".into(),
            field_name: "todos".into(),
            args: BTreeMap::from([(
                "where".into(),
                async_graphql::Value::from_json(json!({"title": {"_eq": "ship"}})).unwrap(),
            )]),
            children: vec![SelectionNode {
                response_key: "id".into(),
                field_name: "id".into(),
                args: BTreeMap::new(),
                children: vec![],
            }],
        }
    }

    fn by_pk_selection(game_id: &str) -> SelectionNode {
        SelectionNode {
            response_key: "blob_games_by_pk".into(),
            field_name: "blob_games_by_pk".into(),
            args: BTreeMap::from([("game_id".into(), async_graphql::Value::from(game_id))]),
            children: vec![
                SelectionNode {
                    response_key: "game_id".into(),
                    field_name: "game_id".into(),
                    args: BTreeMap::new(),
                    children: vec![],
                },
                SelectionNode {
                    response_key: "score".into(),
                    field_name: "score".into(),
                    args: BTreeMap::new(),
                    children: vec![],
                },
            ],
        }
    }

    fn by_pk_with_owner_join(game_id: &str) -> SelectionNode {
        let mut selection = by_pk_selection(game_id);
        selection.children.push(SelectionNode {
            response_key: "owner".into(),
            field_name: "owner".into(),
            args: BTreeMap::new(),
            children: vec![SelectionNode {
                response_key: "user_id".into(),
                field_name: "user_id".into(),
                args: BTreeMap::new(),
                children: vec![],
            }],
        });
        selection
    }

    #[tokio::test]
    async fn sql_store_compiles_todos_list_filter() {
        let engine = GraphqlEngine::builder(pool())
            .identity(crate::graphql::IdentityConfig::dev_headers())
            .roles(&["user"])
            .model::<Todos>(todo_perms())
            .build()
            .unwrap();
        let plan = compile_query(
            &engine.inner,
            &session_user(),
            "user",
            "Todos",
            RootKind::List,
            &list_selection(),
        )
        .expect("SQL list/filter should compile");
        assert!(matches!(plan, QueryPlan::Sql(_)));
    }

    #[tokio::test]
    async fn same_blob_games_type_compiles_as_sql_or_cell() {
        let sql = GraphqlEngine::builder(pool())
            .identity(crate::graphql::IdentityConfig::dev_headers())
            .roles(&["user"])
            .model::<BlobGames>(blob_perms())
            .model::<AuthUsers>(user_perms())
            .read_store::<BlobGames>(ReadStore::Sql)
            .build()
            .unwrap();
        assert!(matches!(
            compile_query(
                &sql.inner,
                &session_user(),
                "user",
                "BlobGames",
                RootKind::ByPk,
                &by_pk_selection("g1"),
            )
            .unwrap(),
            QueryPlan::Sql(_)
        ));

        let cells = MapCellByKey::new();
        let cell = GraphqlEngine::builder(pool())
            .identity(crate::graphql::IdentityConfig::dev_headers())
            .roles(&["user"])
            .model::<BlobGames>(blob_perms())
            .model::<AuthUsers>(user_perms())
            .read_store::<BlobGames>(ReadStore::CellByKey(Arc::new(cells)))
            .build()
            .unwrap();
        assert!(matches!(
            compile_query(
                &cell.inner,
                &session_user(),
                "user",
                "BlobGames",
                RootKind::ByPk,
                &by_pk_selection("g1"),
            )
            .unwrap(),
            QueryPlan::CellByKey { .. }
        ));
    }

    #[tokio::test]
    async fn cell_store_rejects_list_filter_join_and_live() {
        let cells = MapCellByKey::new();
        let engine = GraphqlEngine::builder(pool())
            .identity(crate::graphql::IdentityConfig::dev_headers())
            .roles(&["user"])
            .model::<BlobGames>(blob_perms())
            .model::<AuthUsers>(user_perms())
            .read_store::<BlobGames>(ReadStore::CellByKey(Arc::new(cells)))
            .build()
            .unwrap();
        let list = compile_query(
            &engine.inner,
            &session_user(),
            "user",
            "BlobGames",
            RootKind::List,
            &list_selection(),
        )
        .unwrap_err();
        assert!(list.contains("fan out to N cells"), "{list}");

        let mut filtered = by_pk_selection("g1");
        filtered.args.insert(
            "where".into(),
            async_graphql::Value::from_json(json!({"score": {"_gt": 1}})).unwrap(),
        );
        let filter = compile_query(
            &engine.inner,
            &session_user(),
            "user",
            "BlobGames",
            RootKind::ByPk,
            &filtered,
        )
        .unwrap_err();
        assert!(filter.contains("filter"), "{filter}");

        let join = compile_query(
            &engine.inner,
            &session_user(),
            "user",
            "BlobGames",
            RootKind::ByPk,
            &by_pk_with_owner_join("g1"),
        )
        .unwrap_err();
        assert!(join.contains("join"), "{join}");
    }

    #[tokio::test]
    async fn graphql_by_id_hits_cell_get() {
        let cells = MapCellByKey::new();
        cells.insert(
            "game-1",
            json!({ "game_id": "game-1", "owner_id": "alice", "score": 9 }),
        );
        let engine = GraphqlEngine::builder(pool())
            .identity(crate::graphql::IdentityConfig::dev_headers())
            .roles(&["user"])
            .model::<BlobGames>(blob_perms())
            .model::<AuthUsers>(user_perms())
            .read_store::<BlobGames>(ReadStore::CellByKey(Arc::new(cells)))
            .build()
            .unwrap();
        let mut session = session_user();
        session.set(crate::microsvc::USER_ID_KEY, "alice");
        let response = engine
            .execute(
                &session,
                Request::new(r#"{ blob_games_by_pk(game_id: "game-1") { game_id score } }"#),
            )
            .await;
        assert!(response.errors.is_empty(), "{response:?}");
        let data = response.data.into_json().unwrap();
        assert_eq!(data["blob_games_by_pk"]["game_id"], "game-1");
        assert_eq!(data["blob_games_by_pk"]["score"], 9);
    }

    #[tokio::test]
    async fn graphql_by_id_hides_cell_rows_outside_the_role_policy() {
        let cells = MapCellByKey::new();
        cells.insert(
            "game-bob",
            json!({ "game_id": "game-bob", "owner_id": "bob", "score": 9 }),
        );
        cells.insert(
            "game-malformed",
            json!({ "game_id": "game-malformed", "score": 10 }),
        );
        let engine = GraphqlEngine::builder(pool())
            .identity(crate::graphql::IdentityConfig::dev_headers())
            .roles(&["user"])
            .model::<BlobGames>(blob_perms())
            .model::<AuthUsers>(user_perms())
            .read_store::<BlobGames>(ReadStore::CellByKey(Arc::new(cells)))
            .build()
            .unwrap();

        for game_id in ["game-bob", "game-malformed"] {
            let response = engine
                .execute(
                    &session_user(),
                    Request::new(format!(
                        "{{ blob_games_by_pk(game_id: \"{game_id}\") {{ game_id score }} }}"
                    )),
                )
                .await;
            assert!(response.errors.is_empty(), "{response:?}");
            let data = response.data.into_json().unwrap();
            assert!(data["blob_games_by_pk"].is_null(), "{data}");
        }
    }

    #[tokio::test]
    async fn graphql_owner_join_fails_on_cell_store() {
        let cells = MapCellByKey::new();
        let engine = GraphqlEngine::builder(pool())
            .identity(crate::graphql::IdentityConfig::dev_headers())
            .roles(&["user"])
            .model::<BlobGames>(blob_perms())
            .model::<AuthUsers>(user_perms())
            .read_store::<BlobGames>(ReadStore::CellByKey(Arc::new(cells)))
            .build()
            .unwrap();
        let response = engine
            .execute(
                &session_user(),
                Request::new(
                    r#"{ blob_games_by_pk(game_id: "game-1") { game_id owner { user_id } } }"#,
                ),
            )
            .await;
        assert_eq!(response.errors.len(), 1, "{response:?}");
        assert!(
            response.errors[0]
                .message
                .contains("unsupported on cell store"),
            "{response:?}"
        );
    }

    #[tokio::test]
    async fn http_cell_by_key_gets_sealed_row() {
        use axum::extract::Path;
        use axum::http::{HeaderMap, StatusCode};
        use axum::routing::get;
        use axum::{Json, Router};

        const SECRET: &str = "test-only-cell-by-key-secret-32-bytes";

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let app = Router::new().route(
                "/blob/{id}",
                get(|headers: HeaderMap, Path(id): Path<String>| async move {
                    if headers
                        .get(crate::microsvc::cell_host::CELL_INTERNAL_SECRET_HEADER)
                        .and_then(|value| value.to_str().ok())
                        != Some(SECRET)
                    {
                        return (StatusCode::UNAUTHORIZED, Json(json!({ "error": "no" })));
                    }
                    (StatusCode::OK, Json(json!({ "game_id": id, "score": 3 })))
                }),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let getter = HttpCellByKey::new(
            format!("http://{addr}/blob"),
            InternalHttpSecret::new(SECRET).unwrap(),
        )
        .unwrap();
        let mut pk = BTreeMap::new();
        pk.insert("game_id".into(), "g-http".into());
        let row = getter.get_sealed_row(&pk).await.unwrap().unwrap();
        assert_eq!(row["game_id"], "g-http");
        assert_eq!(row["score"], 3);

        pk.insert("tenant_id".into(), "tenant-1".into());
        assert!(getter.get_sealed_row(&pk).await.is_err());
    }
}
