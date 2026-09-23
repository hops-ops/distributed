//! Env-gated Postgres coverage for the causal GraphQL query envelope.
//!
//! The test compiles on every Postgres GraphQL build and skips cleanly when
//! `DATABASE_URL` is unavailable.

#![cfg(all(feature = "graphql", feature = "postgres"))]

#[path = "../support/postgres.rs"]
mod postgres;

use std::time::Duration;

use async_graphql::Request;
use base64::Engine as _;
use distributed::bus::{Bus, InMemoryBus, Message, MessageKind, RunOptions};
use distributed::graphql::{read, GraphqlEngine, ModelPermissions, SurfaceProjector};
use distributed::microsvc::{
    CausalProjectorContext, HandlerError, Routes, Service, Session, ROLE_KEY,
};
use distributed::projection_protocol::ProjectionChangeRetention;
use distributed::{PostgresRepository, ReadModel, ReadModelCatalog};
use futures_util::{stream::BoxStream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const SERVICE_ID: &str = "postgres-query-protocol-fixture";
const FACT_NAME: &str = "postgres_query_protocol.item_changed";
const PROJECTOR_NAME: &str = "project_postgres_query_protocol_items";
const CHANGE_EPOCH: &str = "postgres-query-protocol-items-v1";
const ROW_ID: &str = "postgres-private-9007199254740993";
const TEST_PROTOCOL_TOKEN_KEY: [u8; 32] = [0x4d; 32];
const LIVE_SUBSCRIPTION: &str =
    "subscription WatchPostgresRows { postgres_protocol_views { title } }";

#[derive(Clone, Debug, Deserialize)]
struct ItemChanged {
    id: String,
    title: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[readmodel(table = "postgres_protocol_views", primary_key = ["id"])]
struct PostgresProtocolView {
    id: String,
    title: String,
}

fn projector() -> SurfaceProjector {
    SurfaceProjector::new(PROJECTOR_NAME)
        .facts([FACT_NAME])
        .models(["PostgresProtocolView"])
        .change_epoch(CHANGE_EPOCH)
}

fn user_session() -> Session {
    let mut session = Session::new();
    session.set(ROLE_KEY, "user");
    session
}

fn projection_service(repository: &PostgresRepository, bus: &InMemoryBus) -> Service {
    let routes = Routes::new()
        .with_read_model_store(repository.clone())
        .causal_projector::<ItemChanged>(projector())
        .model::<PostgresProtocolView>()
        .handle(
            |context: CausalProjectorContext, fact: ItemChanged| async move {
                context
                    .project(&PostgresProtocolView {
                        id: fact.id,
                        title: fact.title,
                    })
                    .await?;
                Ok::<(), HandlerError>(())
            },
        );
    Service::new()
        .named(SERVICE_ID)
        .routes(routes)
        .with_bus(bus.clone())
}

async fn project_item(repository: &PostgresRepository, bus: &InMemoryBus) {
    bus.publish_message(
        Message::new(
            FACT_NAME,
            MessageKind::Event,
            serde_json::to_vec(&json!({
                "id": ROW_ID,
                "title": "postgres causal row"
            }))
            .expect("fixture fact"),
        )
        .with_id("postgres-query-protocol-fact-1")
        .with_metadata(
            distributed::trace_context::CAUSATION_ID,
            "postgres-query-protocol-command-1",
        ),
    )
    .await
    .expect("publish Postgres query protocol fact");
    projection_service(repository, bus)
        .run(RunOptions::idempotent())
        .await
        .expect("project Postgres query protocol fact");
}

fn wire_response(response: async_graphql::Response) -> Value {
    assert!(
        !response.is_err(),
        "unexpected GraphQL errors: {:?}",
        response.errors
    );
    serde_json::to_value(response).expect("GraphQL wire response")
}

fn distributed_envelope(response: &Value) -> &Value {
    response
        .get("extensions")
        .and_then(|extensions| extensions.get("distributed"))
        .unwrap_or_else(|| panic!("missing extensions.distributed: {response}"))
}

fn assert_opaque_token(token: &Value, purpose: &str) {
    let token = token
        .as_str()
        .unwrap_or_else(|| panic!("missing {purpose} token: {token}"));
    let segments = token.split('.').collect::<Vec<_>>();
    assert_eq!(segments.len(), 3, "bounded opaque token: {token}");
    assert_eq!(segments[0], "v1");
    assert_eq!(segments[1], purpose);
    let mac = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(segments[2])
        .unwrap_or_else(|error| panic!("invalid opaque token MAC `{token}`: {error}"));
    assert_eq!(mac.len(), 32, "HMAC-SHA256 token: {token}");
    assert!(
        !token.contains(ROW_ID),
        "opaque token disclosed private row identity: {token}"
    );
}

async fn next_wire_frame(stream: &mut BoxStream<'static, async_graphql::Response>) -> Value {
    let response = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("timeout waiting for GraphQL subscription frame")
        .expect("GraphQL subscription ended");
    wire_response(response)
}

fn request_with_resume(cursors: Value) -> Request {
    serde_json::from_value(json!({
        "query": LIVE_SUBSCRIPTION,
        "extensions": {
            "distributed": {
                "resume": {
                    "cursors": cursors
                }
            }
        }
    }))
    .expect("valid GraphQL request with distributed resume extension")
}

#[tokio::test]
async fn postgres_emits_exact_revisions_and_accepts_an_exact_live_resume() {
    let Some(schema) = postgres::PostgresTestSchema::create_from_env(
        "gql_query_protocol",
        "skipping Postgres GraphQL query-protocol test",
    )
    .await
    else {
        return;
    };
    let repository = schema.repository().await.with_projection_change_retention(
        ProjectionChangeRetention::new(2).expect("positive retention"),
    );
    let manifest = ReadModelCatalog::new(SERVICE_ID).read_model::<PostgresProtocolView>();
    repository
        .bootstrap_table_schema_for_dev(
            &manifest
                .table_registry()
                .expect("Postgres query protocol table registry"),
        )
        .await
        .expect("Postgres query protocol read-model table");

    let bus = InMemoryBus::new();
    project_item(&repository, &bus).await;

    let engine = GraphqlEngine::builder(&repository)
        .service_id(SERVICE_ID)
        .protocol_token_key(TEST_PROTOCOL_TOKEN_KEY)
        .roles(&["user"])
        .model::<PostgresProtocolView>(ModelPermissions::new().grant("user", read().all_columns()))
        .client_projectors([projector()])
        .change_stream(repository.read_model_changes())
        .build()
        .expect("Postgres query protocol GraphQL engine");

    let query = wire_response(
        engine
            .execute(
                &user_session(),
                Request::new("query PostgresSnapshot { postgres_protocol_views { title } }"),
            )
            .await,
    );
    assert_eq!(
        query["data"],
        json!({ "postgres_protocol_views": [{ "title": "postgres causal row" }] })
    );
    assert!(
        !query.to_string().contains(ROW_ID),
        "an omitted primary key leaked through Postgres protocol metadata: {query}"
    );
    let distributed = distributed_envelope(&query);
    assert_eq!(distributed["protocolVersion"], 1);
    assert_opaque_token(&distributed["cacheScope"], "cache-scope");
    let snapshot = &distributed["snapshot"];
    assert_eq!(snapshot["recordsComplete"], true);
    assert_eq!(snapshot["indexesComparable"], true);
    assert_eq!(snapshot["records"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        snapshot["records"][0]["path"],
        json!(["postgres_protocol_views", "0"])
    );
    assert_eq!(snapshot["records"][0]["incarnation"], "1");
    assert_eq!(snapshot["records"][0]["revision"], "1");
    assert_eq!(snapshot["records"][0]["tombstone"], false);
    assert_opaque_token(&snapshot["records"][0]["scopeToken"], "record-revision");
    assert_eq!(snapshot["indexes"][0]["projection"], PROJECTOR_NAME);
    assert_eq!(snapshot["indexes"][0]["position"], "1");
    assert_opaque_token(&snapshot["indexes"][0]["scopeToken"], "query-index");

    let mut initial_stream =
        engine.execute_stream(&user_session(), Request::new(LIVE_SUBSCRIPTION));
    let initial = next_wire_frame(&mut initial_stream).await;
    let initial_live = &distributed_envelope(&initial)["live"];
    assert_eq!(initial_live["mode"], "resumable", "{initial}");
    assert_eq!(initial_live["reset"], false, "{initial}");
    let cursors = initial_live["cursors"].clone();
    assert_eq!(cursors[0]["projection"], PROJECTOR_NAME);
    assert_eq!(cursors[0]["position"], "1");
    assert_opaque_token(&cursors[0]["token"], "live-resume");
    drop(initial_stream);

    let mut resumed_stream = engine.execute_stream(&user_session(), request_with_resume(cursors));
    let resumed = next_wire_frame(&mut resumed_stream).await;
    let resumed_live = &distributed_envelope(&resumed)["live"];
    assert_eq!(resumed_live["mode"], "resumable", "{resumed}");
    assert_eq!(resumed_live["reset"], false, "{resumed}");
    assert_eq!(resumed_live["cursors"][0]["position"], "1");
}
