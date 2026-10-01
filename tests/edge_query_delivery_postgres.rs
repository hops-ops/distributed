#![cfg(all(
    feature = "gateway-delivery",
    feature = "graphql",
    feature = "postgres"
))]
use async_graphql::Request;
use distributed::graphql::{read, GraphqlEngine, ModelPermissions, ReadRouting};
use distributed::microsvc::Session;
use serde_json::{json, Value};
use std::time::Duration;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, distributed::ReadModel)]
#[readmodel(table = "gateway_replica_views", primary_key = ["id"])]
struct ReplicaView {
    id: String,
    title: String,
}
const QUERY: &str = "query StaleAllowed { gateway_replica_views { id title } }";
const CURRENT: &str = "query CurrentState { gateway_replica_views { id title } }";
fn engine(primary: &sqlx::PgPool, replica: &sqlx::PgPool, namespace: &str) -> GraphqlEngine {
    GraphqlEngine::builder(primary.clone())
        .service_id("replica-fixture")
        .protocol_token_key([57; 32])
        .protocol_namespace(namespace)
        .roles(&["user"])
        .anonymous_role("user")
        .model::<ReplicaView>(ModelPermissions::new().grant("user", read().all_columns()))
        .read_routing(
            ReadRouting::new(replica.clone())
                .stale_tolerant(QUERY, Some("StaleAllowed".into()))
                .unwrap(),
        )
        .build()
        .unwrap()
}
async fn query(engine: &GraphqlEngine, document: &str, context: Option<Value>) -> Value {
    let mut request = Request::new(document).operation_name(if document == QUERY {
        "StaleAllowed"
    } else {
        "CurrentState"
    });
    if let Some(context) = context {
        request.extensions.insert(
            "gatewayFreshness".into(),
            async_graphql::Value::from_json(context).unwrap(),
        );
    }
    serde_json::to_value(engine.execute(&Session::new(), request).await).unwrap()
}
async fn wait_replayed(pool: &sqlx::PgPool, title: &str) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if sqlx::query_scalar::<_, String>(
                "SELECT title FROM gateway_replica_views WHERE id='one'",
            )
            .fetch_optional(pool)
            .await
            .ok()
            .flatten()
            .as_deref()
                == Some(title)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("standby replayed fixture write");
}

#[tokio::test]
#[ignore = "requires owned primary and paused standby; run tests/gateway-postgres/run.py"]
async fn paused_replica_never_certifies_freshness() {
    let primary = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(Duration::from_secs(3))
        .connect(
            &std::env::var("GATEWAY_TEST_PRIMARY_URL").expect("run tests/gateway-postgres/run.py"),
        )
        .await
        .unwrap();
    let replica = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(Duration::from_secs(3))
        .connect(&std::env::var("GATEWAY_TEST_REPLICA_URL").expect("owned standby URL"))
        .await
        .unwrap();
    sqlx::query("CREATE TABLE gateway_replica_views (id text PRIMARY KEY, title text NOT NULL)")
        .execute(&primary)
        .await
        .unwrap();
    sqlx::query("INSERT INTO gateway_replica_views VALUES ('one','before')")
        .execute(&primary)
        .await
        .unwrap();
    wait_replayed(&replica, "before").await;
    assert!(sqlx::query_scalar::<_, bool>("SELECT pg_is_in_recovery()")
        .fetch_one(&replica)
        .await
        .unwrap());
    sqlx::query("SELECT pg_wal_replay_pause()")
        .execute(&replica)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if sqlx::query_scalar::<_, String>("SELECT pg_get_wal_replay_pause_state()")
                .fetch_one(&replica)
                .await
                .unwrap()
                == "paused"
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    sqlx::query("UPDATE gateway_replica_views SET title='committed' WHERE id='one'")
        .execute(&primary)
        .await
        .unwrap();
    let engine = engine(&primary, &replica, "epoch-1");
    assert_eq!(
        query(&engine, QUERY, None).await["data"]["gateway_replica_views"][0]["title"],
        "before"
    );
    assert_eq!(
        query(&engine, CURRENT, None).await["data"]["gateway_replica_views"][0]["title"],
        "committed"
    );
    let identity = engine
        .delivery_identity(
            &Session::new(),
            &Request::new(QUERY).operation_name("StaleAllowed"),
        )
        .unwrap();
    let context = json!({"version":1,"schemaHash":identity.schema_hash,"protocolHash":identity.protocol_hash,"authorizationGeneration":identity.authorization_generation,"cacheScope":identity.cache_scope,"pending":[{"complete":true,"models":["ReplicaView"],"relationships":[]}],"minimum":[]});
    assert_eq!(
        query(&engine, QUERY, Some(context.clone())).await["data"]["gateway_replica_views"][0]
            ["title"],
        "committed"
    );
    let mut disjoint = context.clone();
    disjoint["pending"][0]["models"] = json!(["OtherView"]);
    assert_eq!(
        query(&engine, QUERY, Some(disjoint)).await["data"]["gateway_replica_views"][0]["title"],
        "before"
    );
    // A new backend generation cannot authenticate old-scope context.
    let next = self::engine(&replica, &primary, "epoch-2");
    assert_eq!(
        query(&next, QUERY, Some(context.clone())).await["errors"][0]["extensions"]["code"],
        "FRESHNESS_SCOPE_CHANGED"
    );
    let misbound = query(&next, CURRENT, None).await;
    assert!(
        misbound.get("errors").is_some() && misbound["data"].is_null(),
        "a standby cannot certify a current read: {misbound}"
    );
    let container = std::env::var("GATEWAY_TEST_PRIMARY_CONTAINER").unwrap();
    assert!(container.starts_with("gateway-replay-") && container.ends_with("-primary"));
    assert!(std::process::Command::new("docker")
        .args(["stop", "-t", "1", &container])
        .status()
        .unwrap()
        .success());
    let unavailable = query(&engine, QUERY, Some(context.clone())).await;
    assert!(unavailable.get("errors").is_some(), "{unavailable}");
    assert!(
        unavailable["data"].is_null(),
        "must not retry the old standby: {unavailable}"
    );
    assert!(std::process::Command::new("docker")
        .args(["start", &container])
        .status()
        .unwrap()
        .success());
    wait_replayed(&primary, "committed").await;
    assert_eq!(
        query(&engine, CURRENT, Some(context)).await["data"]["gateway_replica_views"][0]["title"],
        "committed",
        "restarted primary retains its committed data and serves a current read"
    );
    sqlx::query("SELECT pg_wal_replay_resume()")
        .execute(&replica)
        .await
        .unwrap();
    wait_replayed(&replica, "committed").await;
    assert_eq!(
        query(&engine, QUERY, None).await["data"]["gateway_replica_views"][0]["title"],
        "committed"
    );
    primary.close().await;
    replica.close().await;
}
