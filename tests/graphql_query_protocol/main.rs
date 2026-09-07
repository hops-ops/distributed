#![cfg(all(feature = "graphql", feature = "sqlite"))]

use std::sync::Arc;
use std::time::Duration;

use async_graphql::Request;
use base64::Engine as _;
use distributed::bus::{Bus, InMemoryBus, Message, MessageKind, RunOptions};
use distributed::graphql::{
    col, read, GraphqlEngine, ModelNormalization, ModelPermissions, SurfaceProjector,
};
use distributed::microsvc::{
    CausalProjectorContext, HandlerError, Routes, Service, Session, ROLE_KEY,
};
use distributed::projection_protocol::ProjectionChangeRetention;
use distributed::{ReadModel, ReadModelCatalog, RelationalReadModel, SqliteRepository};
use futures_util::{stream::BoxStream, SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;
use tokio_tungstenite::tungstenite::Message as WsMessage;

const SERVICE_ID: &str = "query-protocol-fixture";
const FACT_NAME: &str = "query_protocol.item_changed";
const PROJECTOR_NAME: &str = "project_query_protocol_items";
const CHANGE_EPOCH: &str = "query-protocol-items-v1";
const ROW_ID: &str = "row-private-9007199254740993";
const LEGACY_ROW_ID: &str = "legacy-private-9007199254740995";
const HIDDEN_ALIAS_PREFIX: &str = "0__distributed_evidence_pk_";
const TEST_PROTOCOL_TOKEN_KEY: [u8; 32] = [0x7b; 32];
const LIVE_SUBSCRIPTION: &str = "subscription WatchCausalRows { causal_query_views { title } }";
const FILTERED_LIVE_SUBSCRIPTION: &str = r#"
subscription WatchFilteredRows {
  causal_query_views(where: { title: { _eq: "causal row" } }) { title }
}
"#;
const EMBEDDED_SERVICE_ID: &str = "embedded-query-protocol-fixture";
const EMBEDDED_FACT_NAME: &str = "query_protocol.embedded_item_changed";
const EMBEDDED_PROJECTOR_NAME: &str = "project_embedded_query_protocol_items";
const EMBEDDED_CHANGE_EPOCH: &str = "embedded-query-protocol-items-v1";

#[derive(Clone, Debug, Deserialize)]
struct ItemChanged {
    id: String,
    title: String,
    #[serde(default)]
    delete: bool,
    #[serde(default)]
    recreate: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[readmodel(table = "causal_query_views", primary_key = ["id"])]
struct CausalQueryView {
    id: String,
    title: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[readmodel(table = "legacy_query_views", primary_key = ["id"])]
struct LegacyQueryView {
    id: String,
    title: String,
}

#[derive(Clone, Debug, Deserialize)]
struct EmbeddedItemChanged {
    key: i64,
    title: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[readmodel(table = "embedded_query_views", primary_key = ["key"])]
struct EmbeddedQueryView {
    id: String,
    key: i64,
    title: String,
}

fn projector() -> SurfaceProjector {
    SurfaceProjector::new(PROJECTOR_NAME)
        .facts([FACT_NAME])
        .models(["CausalQueryView"])
        .change_epoch(CHANGE_EPOCH)
}

fn embedded_projector() -> SurfaceProjector {
    SurfaceProjector::new(EMBEDDED_PROJECTOR_NAME)
        .facts([EMBEDDED_FACT_NAME])
        .models(["EmbeddedQueryView"])
        .change_epoch(EMBEDDED_CHANGE_EPOCH)
}

fn user_session() -> Session {
    let mut session = Session::new();
    session.set(ROLE_KEY, "user");
    session
}

struct ProtocolFixture {
    repository: SqliteRepository,
    bus: InMemoryBus,
    engine: GraphqlEngine,
}

fn projection_service(repository: &SqliteRepository, bus: &InMemoryBus) -> Service {
    let declaration = projector();
    let routes = Routes::new()
        .with_read_model_store(repository.clone())
        .causal_projector::<ItemChanged>(declaration)
        .model::<CausalQueryView>()
        .handle(
            |context: CausalProjectorContext, fact: ItemChanged| async move {
                let view = CausalQueryView {
                    id: fact.id,
                    title: fact.title,
                };
                if fact.recreate {
                    let tombstone = context
                        .tombstone_revision::<CausalQueryView>(view.primary_key()?)
                        .await?
                        .expect("recreate fixture requires its durable tombstone");
                    context.recreate(&view, &tombstone)?;
                } else if fact.delete {
                    let loaded = context
                        .load::<CausalQueryView>(view.primary_key()?)
                        .await?
                        .expect("delete fixture requires its projected row");
                    context
                        .delete::<CausalQueryView>(loaded.model.primary_key()?, &loaded.revision)?;
                } else {
                    context.project(&view).await?;
                }
                Ok::<(), HandlerError>(())
            },
        );
    Service::new()
        .named(SERVICE_ID)
        .routes(routes)
        .with_bus(bus.clone())
}

fn embedded_projection_service(repository: &SqliteRepository, bus: &InMemoryBus) -> Service {
    let declaration = embedded_projector();
    let routes = Routes::new()
        .with_read_model_store(repository.clone())
        .causal_projector::<EmbeddedItemChanged>(declaration)
        .model::<EmbeddedQueryView>()
        .handle(
            |context: CausalProjectorContext, fact: EmbeddedItemChanged| async move {
                context
                    .project(&EmbeddedQueryView {
                        id: format!("embedded-{}", fact.key),
                        key: fact.key,
                        title: fact.title,
                    })
                    .await?;
                Ok::<(), HandlerError>(())
            },
        );
    Service::new()
        .named(EMBEDDED_SERVICE_ID)
        .routes(routes)
        .with_bus(bus.clone())
}

async fn project_item(
    repository: &SqliteRepository,
    bus: &InMemoryBus,
    sequence: u64,
    title: &str,
) {
    project_item_with_id(repository, bus, sequence, ROW_ID, title).await;
}

async fn project_item_with_id(
    repository: &SqliteRepository,
    bus: &InMemoryBus,
    sequence: u64,
    id: &str,
    title: &str,
) {
    bus.publish_message(
        Message::new(
            FACT_NAME,
            MessageKind::Event,
            serde_json::to_vec(&json!({
                "id": id,
                "title": title
            }))
            .expect("fixture fact"),
        )
        .with_id(format!("query-protocol-fact-{sequence}"))
        .with_metadata(
            distributed::trace_context::CAUSATION_ID,
            format!("query-protocol-command-{sequence}"),
        ),
    )
    .await
    .expect("publish query protocol fact");
    projection_service(repository, bus)
        .run(RunOptions::idempotent())
        .await
        .expect("project query protocol fact");
}

async fn delete_item(repository: &SqliteRepository, bus: &InMemoryBus, sequence: u64) {
    bus.publish_message(
        Message::new(
            FACT_NAME,
            MessageKind::Event,
            serde_json::to_vec(&json!({
                "id": ROW_ID,
                "title": "deleted row",
                "delete": true
            }))
            .expect("fixture delete fact"),
        )
        .with_id(format!("query-protocol-fact-{sequence}"))
        .with_metadata(
            distributed::trace_context::CAUSATION_ID,
            format!("query-protocol-command-{sequence}"),
        ),
    )
    .await
    .expect("publish query protocol delete fact");
    projection_service(repository, bus)
        .run(RunOptions::idempotent())
        .await
        .expect("delete query protocol row");
}

async fn recreate_item(
    repository: &SqliteRepository,
    bus: &InMemoryBus,
    sequence: u64,
    title: &str,
) {
    bus.publish_message(
        Message::new(
            FACT_NAME,
            MessageKind::Event,
            serde_json::to_vec(&json!({
                "id": ROW_ID,
                "title": title,
                "recreate": true
            }))
            .expect("fixture recreate fact"),
        )
        .with_id(format!("query-protocol-fact-{sequence}"))
        .with_metadata(
            distributed::trace_context::CAUSATION_ID,
            format!("query-protocol-command-{sequence}"),
        ),
    )
    .await
    .expect("publish query protocol recreate fact");
    projection_service(repository, bus)
        .run(RunOptions::idempotent())
        .await
        .expect("recreate query protocol row");
}

async fn protocol_fixture_with_rows() -> ProtocolFixture {
    protocol_fixture_with_retention(1).await
}

async fn protocol_fixture_with_retention(retention: u64) -> ProtocolFixture {
    let repository = SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .expect("migrated SQLite repository")
        .with_projection_change_retention(
            ProjectionChangeRetention::new(retention)
                .expect("positive retained projection change count"),
        );
    let manifest = ReadModelCatalog::new(SERVICE_ID)
        .read_model::<CausalQueryView>()
        .read_model::<LegacyQueryView>();
    repository
        .bootstrap_table_schema_for_dev(
            &manifest
                .table_registry()
                .expect("query protocol table registry"),
        )
        .await
        .expect("query protocol read-model tables");

    sqlx::query("INSERT INTO legacy_query_views (id, title) VALUES (?, ?)")
        .bind(LEGACY_ROW_ID)
        .bind("legacy row")
        .execute(repository.pool())
        .await
        .expect("legacy row");

    let bus = InMemoryBus::new();
    let declaration = projector();
    project_item(&repository, &bus, 1, "causal row").await;

    let engine = GraphqlEngine::builder(&repository)
        .service_id(SERVICE_ID)
        .protocol_token_key(TEST_PROTOCOL_TOKEN_KEY)
        .roles(&["user"])
        .model::<CausalQueryView>(
            ModelPermissions::new().grant("user", read().all_columns().aggregations()),
        )
        .model::<LegacyQueryView>(ModelPermissions::new().grant("user", read().all_columns()))
        .client_projectors([declaration])
        .change_stream(repository.read_model_changes())
        .build()
        .expect("query protocol GraphQL engine");
    ProtocolFixture {
        repository,
        bus,
        engine,
    }
}

async fn protocol_engine_with_rows() -> GraphqlEngine {
    protocol_fixture_with_rows().await.engine
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
    assert_eq!(
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac),
        segments[2],
        "token MAC must use canonical unpadded base64url"
    );
    for private in [ROW_ID, LEGACY_ROW_ID, CHANGE_EPOCH] {
        assert!(
            !token.contains(private),
            "opaque token disclosed private scope material `{private}`: {token}"
        );
    }
}

async fn next_wire_frame(stream: &mut BoxStream<'static, async_graphql::Response>) -> Value {
    let response = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("timeout waiting for GraphQL subscription frame")
        .expect("GraphQL subscription ended");
    wire_response(response)
}

fn request_with_resume(document: &str, cursors: Value) -> Request {
    serde_json::from_value(json!({
        "query": document,
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

fn assert_live_frame(
    response: &Value,
    root_field: &str,
    expected_title: &str,
    expected_position: &str,
    expected_reset: bool,
) -> Value {
    assert_eq!(
        response["data"][root_field],
        json!([{ "title": expected_title }]),
        "{response}"
    );
    let distributed = distributed_envelope(response);
    let snapshot = &distributed["snapshot"];
    assert_eq!(snapshot["recordsComplete"], true, "{response}");
    assert_eq!(snapshot["indexesComparable"], true, "{response}");
    assert_eq!(snapshot["indexes"].as_array().map(Vec::len), Some(1));
    assert_eq!(snapshot["indexes"][0]["projection"], PROJECTOR_NAME);
    assert_eq!(snapshot["indexes"][0]["position"], expected_position);

    let live = &distributed["live"];
    assert_eq!(live["supported"], true, "{response}");
    assert_eq!(live["reset"], expected_reset, "{response}");
    assert_eq!(live["cursors"].as_array().map(Vec::len), Some(1));
    assert_eq!(live["cursors"][0]["projection"], PROJECTOR_NAME);
    assert_eq!(live["cursors"][0]["position"], expected_position);
    assert_eq!(
        live["cursors"][0], snapshot["indexes"][0]["resume"],
        "the live cursor and authoritative snapshot index must describe one position"
    );
    live["cursors"].clone()
}

fn tamper_first_cursor_token(mut cursors: Value) -> Value {
    let token = cursors[0]["token"]
        .as_str()
        .expect("live cursor token")
        .to_string();
    let mut bytes = token.into_bytes();
    let mac_start = bytes
        .iter()
        .rposition(|byte| *byte == b'.')
        .expect("token purpose separator")
        + 1;
    bytes[mac_start] = if bytes[mac_start] == b'A' { b'B' } else { b'A' };
    cursors[0]["token"] =
        Value::String(String::from_utf8(bytes).expect("opaque token remains ASCII"));
    cursors
}

#[tokio::test]
async fn projected_query_emits_exact_record_and_index_revisions_without_key_leaks() {
    let engine = protocol_engine_with_rows().await;
    let response = wire_response(
        engine
            .execute(
                &user_session(),
                Request::new("query QuerySnapshot { aliasedRows: causal_query_views { title } }"),
            )
            .await,
    );

    assert_eq!(
        response["data"],
        json!({ "aliasedRows": [{ "title": "causal row" }] })
    );
    let serialized = response.to_string();
    assert!(
        !serialized.contains(HIDDEN_ALIAS_PREFIX),
        "compiler evidence alias leaked into GraphQL: {response}"
    );
    assert!(
        !serialized.contains(ROW_ID),
        "an omitted primary key leaked through protocol metadata: {response}"
    );

    let distributed = distributed_envelope(&response);
    assert_eq!(distributed["protocolVersion"], 1);
    assert!(distributed.get("command").is_none());
    assert!(distributed.get("live").is_none());
    assert_opaque_token(&distributed["cacheScope"], "cache-scope");

    let snapshot = &distributed["snapshot"];
    assert_eq!(snapshot["recordsComplete"], true);
    assert_eq!(snapshot["indexesComparable"], true);
    assert!(snapshot.get("complete").is_none());
    assert_eq!(snapshot["observations"], json!([]));
    assert_opaque_token(&snapshot["scopeToken"], "query-snapshot");

    let records = snapshot["records"].as_array().expect("record revisions");
    assert_eq!(records.len(), 1, "{snapshot}");
    let record = &records[0];
    assert_eq!(record["path"], json!(["aliasedRows", "0"]));
    assert_eq!(record["model"], "CausalQueryView");
    assert_eq!(record["incarnation"], "1");
    assert_eq!(record["revision"], "1");
    assert_eq!(record["tombstone"], false);
    assert_opaque_token(&record["scopeToken"], "record-revision");

    let indexes = snapshot["indexes"].as_array().expect("index revisions");
    assert_eq!(indexes.len(), 1, "{snapshot}");
    let index = &indexes[0];
    assert_eq!(index["projection"], PROJECTOR_NAME);
    assert_eq!(index["position"], "1");
    assert_opaque_token(&index["scopeToken"], "query-index");
    assert_eq!(index["resume"]["projection"], PROJECTOR_NAME);
    assert_eq!(index["resume"]["position"], "1");
    assert_opaque_token(&index["resume"]["token"], "live-resume");

    let tokens = [
        snapshot["scopeToken"].as_str().unwrap(),
        record["scopeToken"].as_str().unwrap(),
        index["scopeToken"].as_str().unwrap(),
        index["resume"]["token"].as_str().unwrap(),
    ];
    for (offset, token) in tokens.iter().enumerate() {
        assert!(
            !tokens[..offset].contains(token),
            "purpose-separated protocol tokens must not alias: {tokens:?}"
        );
    }
}

#[tokio::test]
async fn count_only_aggregate_emits_no_unselected_node_record_evidence() {
    let engine = protocol_engine_with_rows().await;
    let response = wire_response(
        engine
            .execute(
                &user_session(),
                Request::new(
                    "query CountOnly {
                        stats: causal_query_views_aggregate {
                            __typename
                            totals: aggregate {
                                __typename
                                total: count
                            }
                        }
                    }",
                ),
            )
            .await,
    );

    assert_eq!(
        response["data"],
        json!({
            "stats": {
                "__typename": "causal_query_views_aggregate",
                "totals": {
                    "__typename": "causal_query_views_aggregate_fields",
                    "total": 1
                }
            }
        }),
        "{response}"
    );
    let snapshot = &distributed_envelope(&response)["snapshot"];
    assert_eq!(snapshot["recordsComplete"], true, "{snapshot}");
    assert_eq!(snapshot["indexesComparable"], true, "{snapshot}");
    assert_eq!(snapshot["records"], json!([]), "{snapshot}");
    assert_eq!(snapshot["indexes"].as_array().map(Vec::len), Some(1));
    assert_eq!(snapshot["indexes"][0]["projection"], PROJECTOR_NAME);
}

#[tokio::test]
async fn aggregate_node_aliases_drive_data_and_exact_record_paths() {
    let engine = protocol_engine_with_rows().await;
    let response = wire_response(
        engine
            .execute(
                &user_session(),
                Request::new(
                    "query AliasedAggregateNodes {
                        stats: causal_query_views_aggregate {
                            firstTotals: aggregate { firstCount: count }
                            secondTotals: aggregate { secondCount: count }
                            rowsAlias: nodes { heading: title }
                            otherRowsAlias: nodes { alternateHeading: title }
                        }
                    }",
                ),
            )
            .await,
    );

    assert_eq!(
        response["data"],
        json!({
            "stats": {
                "firstTotals": { "firstCount": 1 },
                "secondTotals": { "secondCount": 1 },
                "rowsAlias": [{ "heading": "causal row" }],
                "otherRowsAlias": [{ "alternateHeading": "causal row" }]
            }
        }),
        "{response}"
    );
    let snapshot = &distributed_envelope(&response)["snapshot"];
    let records = snapshot["records"].as_array().expect("record revisions");
    assert_eq!(records.len(), 2, "{snapshot}");
    assert_eq!(records[0]["path"], json!(["stats", "rowsAlias", "0"]));
    assert_eq!(records[0]["model"], "CausalQueryView");
    assert_eq!(records[1]["path"], json!(["stats", "otherRowsAlias", "0"]));
    assert_eq!(records[1]["model"], "CausalQueryView");
}

#[tokio::test]
async fn embedded_models_emit_index_evidence_without_record_evidence() {
    let repository = SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .expect("migrated embedded SQLite repository");
    let manifest = ReadModelCatalog::new(EMBEDDED_SERVICE_ID).read_model::<EmbeddedQueryView>();
    repository
        .bootstrap_table_schema_for_dev(
            &manifest
                .table_registry()
                .expect("embedded query protocol table registry"),
        )
        .await
        .expect("embedded query protocol read-model table");

    let bus = InMemoryBus::new();
    bus.publish_message(
        Message::new(
            EMBEDDED_FACT_NAME,
            MessageKind::Event,
            serde_json::to_vec(&json!({
                "key": 42,
                "title": "embedded row"
            }))
            .expect("embedded fixture fact"),
        )
        .with_id("embedded-query-protocol-fact-1")
        .with_metadata(
            distributed::trace_context::CAUSATION_ID,
            "embedded-query-protocol-command-1",
        ),
    )
    .await
    .expect("publish embedded query protocol fact");
    embedded_projection_service(&repository, &bus)
        .run(RunOptions::idempotent())
        .await
        .expect("project embedded query protocol fact");

    let engine = GraphqlEngine::builder(&repository)
        .service_id(EMBEDDED_SERVICE_ID)
        .protocol_token_key(TEST_PROTOCOL_TOKEN_KEY)
        .roles(&["user"])
        .model::<EmbeddedQueryView>(ModelPermissions::new().grant("user", read().all_columns()))
        .client_projectors([embedded_projector()])
        .change_stream(repository.read_model_changes())
        .build()
        .expect("embedded query protocol GraphQL engine");
    let client_manifest = engine
        .client_manifest_for_role("user")
        .expect("embedded client manifest");
    assert!(matches!(
        &client_manifest.models[0].normalization,
        ModelNormalization::Embedded
    ));

    let response = wire_response(
        engine
            .execute(
                &user_session(),
                Request::new(
                    "query EmbeddedSnapshot {
                        embeddedRows: embedded_query_views { heading: title }
                    }",
                ),
            )
            .await,
    );
    assert_eq!(
        response["data"],
        json!({ "embeddedRows": [{ "heading": "embedded row" }] }),
        "{response}"
    );
    let serialized = response.to_string();
    assert!(!serialized.contains(HIDDEN_ALIAS_PREFIX), "{response}");
    let snapshot = &distributed_envelope(&response)["snapshot"];
    assert_eq!(snapshot["recordsComplete"], true, "{snapshot}");
    assert_eq!(snapshot["indexesComparable"], true, "{snapshot}");
    assert_eq!(snapshot["records"], json!([]), "{snapshot}");
    assert_eq!(snapshot["indexes"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        snapshot["indexes"][0]["projection"],
        EMBEDDED_PROJECTOR_NAME
    );
    assert_eq!(snapshot["indexes"][0]["position"], "1");
}

#[tokio::test]
async fn query_evidence_chunks_129_unique_rows_inside_one_snapshot() {
    let fixture = protocol_fixture_with_retention(10).await;
    for sequence in 2_u64..=129 {
        fixture
            .bus
            .publish_message(
                Message::new(
                    FACT_NAME,
                    MessageKind::Event,
                    serde_json::to_vec(&json!({
                        "id": format!("bulk-row-{sequence:03}"),
                        "title": format!("bulk title {sequence}")
                    }))
                    .expect("bulk fixture fact"),
                )
                .with_id(format!("query-protocol-bulk-fact-{sequence}"))
                .with_metadata(
                    distributed::trace_context::CAUSATION_ID,
                    format!("query-protocol-bulk-command-{sequence}"),
                ),
            )
            .await
            .expect("publish bulk query protocol fact");
    }
    projection_service(&fixture.repository, &fixture.bus)
        .run(RunOptions::idempotent())
        .await
        .expect("project bulk query protocol facts");

    let response = wire_response(
        fixture
            .engine
            .execute(
                &user_session(),
                Request::new(
                    "query QueryEvidenceBatchBoundary { causal_query_views(limit: 129) { title } }",
                ),
            )
            .await,
    );
    assert_eq!(
        response["data"]["causal_query_views"]
            .as_array()
            .map(Vec::len),
        Some(129),
        "{response}"
    );
    let snapshot = &distributed_envelope(&response)["snapshot"];
    assert_eq!(snapshot["recordsComplete"], true, "{snapshot}");
    assert_eq!(snapshot["indexesComparable"], true, "{snapshot}");
    assert_eq!(snapshot["records"].as_array().map(Vec::len), Some(129));
}

#[tokio::test]
async fn matching_query_and_live_plans_share_snapshot_scope_not_operation_hash() {
    let fixture = protocol_fixture_with_rows().await;
    let query = wire_response(
        fixture
            .engine
            .execute(
                &user_session(),
                Request::new("query ReadCausalRows { causal_query_views { title } }"),
            )
            .await,
    );
    let mut live_stream = fixture
        .engine
        .execute_stream(&user_session(), Request::new(LIVE_SUBSCRIPTION));
    let live = next_wire_frame(&mut live_stream).await;
    let query_protocol = distributed_envelope(&query);
    let live_protocol = distributed_envelope(&live);

    assert_ne!(
        query_protocol["operation"], live_protocol["operation"],
        "transport operation hashes remain exact document drift fences"
    );
    assert_eq!(
        query_protocol["snapshot"]["scopeToken"], live_protocol["snapshot"]["scopeToken"],
        "matching compiler plans must share one comparable query/live snapshot scope"
    );
    assert_eq!(
        query_protocol["snapshot"]["indexes"], live_protocol["snapshot"]["indexes"],
        "matching plans at one head must carry identical index evidence"
    );
}

#[tokio::test]
async fn live_subscription_accepts_an_exact_resume_without_reset() {
    let fixture = protocol_fixture_with_rows().await;
    let mut initial_stream = fixture
        .engine
        .execute_stream(&user_session(), Request::new(LIVE_SUBSCRIPTION));
    let initial = next_wire_frame(&mut initial_stream).await;
    let initial_cursors =
        assert_live_frame(&initial, "causal_query_views", "causal row", "1", false);
    drop(initial_stream);

    let mut resumed_stream = fixture.engine.execute_stream(
        &user_session(),
        request_with_resume(LIVE_SUBSCRIPTION, initial_cursors.clone()),
    );
    let resumed = next_wire_frame(&mut resumed_stream).await;
    let resumed_cursors =
        assert_live_frame(&resumed, "causal_query_views", "causal row", "1", false);
    assert_eq!(
        resumed_cursors, initial_cursors,
        "an exact reconnect must remain in the same operation and projection scope"
    );
}

#[tokio::test]
async fn live_subscription_tampering_and_wrong_scope_reset_without_errors() {
    let fixture = protocol_fixture_with_rows().await;
    let mut initial_stream = fixture
        .engine
        .execute_stream(&user_session(), Request::new(LIVE_SUBSCRIPTION));
    let initial = next_wire_frame(&mut initial_stream).await;
    let initial_cursors =
        assert_live_frame(&initial, "causal_query_views", "causal row", "1", false);
    let initial_snapshot_scope = distributed_envelope(&initial)["snapshot"]["scopeToken"].clone();
    drop(initial_stream);

    let tampered_cursors = tamper_first_cursor_token(initial_cursors.clone());
    let mut tampered_stream = fixture.engine.execute_stream(
        &user_session(),
        request_with_resume(LIVE_SUBSCRIPTION, tampered_cursors.clone()),
    );
    let tampered = next_wire_frame(&mut tampered_stream).await;
    let tampered_fallback_cursors =
        assert_live_frame(&tampered, "causal_query_views", "causal row", "1", true);
    assert_eq!(
        tampered_fallback_cursors, initial_cursors,
        "fallback must issue the authoritative current cursor, not echo tampered metadata"
    );
    assert_ne!(tampered_fallback_cursors, tampered_cursors);
    drop(tampered_stream);

    let mut wrong_scope_stream = fixture.engine.execute_stream(
        &user_session(),
        request_with_resume(FILTERED_LIVE_SUBSCRIPTION, initial_cursors.clone()),
    );
    let wrong_scope = next_wire_frame(&mut wrong_scope_stream).await;
    let wrong_scope_cursors =
        assert_live_frame(&wrong_scope, "causal_query_views", "causal row", "1", true);
    assert_ne!(
        distributed_envelope(&wrong_scope)["snapshot"]["scopeToken"],
        initial_snapshot_scope,
        "a different logical filter must receive its own authoritative snapshot scope"
    );
    assert_ne!(
        wrong_scope_cursors, initial_cursors,
        "a cursor must never cross operation-instance scopes"
    );
}

#[tokio::test]
async fn live_subscription_out_of_window_resume_resets_to_current_snapshot() {
    let fixture = protocol_fixture_with_rows().await;
    let mut initial_stream = fixture
        .engine
        .execute_stream(&user_session(), Request::new(LIVE_SUBSCRIPTION));
    let initial = next_wire_frame(&mut initial_stream).await;
    let stale_cursors = assert_live_frame(&initial, "causal_query_views", "causal row", "1", false);
    drop(initial_stream);

    project_item(&fixture.repository, &fixture.bus, 2, "causal row 2").await;
    project_item(&fixture.repository, &fixture.bus, 3, "causal row 3").await;
    let compacted_through: i64 =
        sqlx::query_scalar("SELECT compacted_through FROM projection_partitions")
            .fetch_one(fixture.repository.pool())
            .await
            .expect("projection compaction watermark");
    assert_eq!(
        compacted_through, 2,
        "position 1 must be strictly outside the retained resume window"
    );

    let mut resumed_stream = fixture.engine.execute_stream(
        &user_session(),
        request_with_resume(LIVE_SUBSCRIPTION, stale_cursors.clone()),
    );
    let resumed = next_wire_frame(&mut resumed_stream).await;
    let current_cursors =
        assert_live_frame(&resumed, "causal_query_views", "causal row 3", "3", true);
    assert_ne!(
        current_cursors, stale_cursors,
        "the reset frame must carry only the current authoritative cursor"
    );
}

#[tokio::test]
async fn live_subscription_frames_keep_data_and_metadata_in_fifo_order() {
    let fixture = protocol_fixture_with_rows().await;
    let mut stream = fixture
        .engine
        .execute_stream(&user_session(), Request::new(LIVE_SUBSCRIPTION));

    let first = next_wire_frame(&mut stream).await;
    let first_cursors = assert_live_frame(&first, "causal_query_views", "causal row", "1", false);

    fixture
        .repository
        .publish_read_model_change(distributed::ReadModelChange::new(["causal_query_views"]));
    assert!(
        tokio::time::timeout(Duration::from_millis(350), stream.next())
            .await
            .is_err(),
        "a redundant invalidation must be hash-gated without queuing orphaned frame metadata"
    );

    project_item(&fixture.repository, &fixture.bus, 2, "causal row 2").await;
    let second = next_wire_frame(&mut stream).await;
    let second_cursors =
        assert_live_frame(&second, "causal_query_views", "causal row 2", "2", false);

    project_item(&fixture.repository, &fixture.bus, 3, "causal row 3").await;
    let third = next_wire_frame(&mut stream).await;
    let third_cursors = assert_live_frame(&third, "causal_query_views", "causal row 3", "3", false);

    assert_ne!(first_cursors, second_cursors);
    assert_ne!(second_cursors, third_cursors);
    let second_snapshot = &distributed_envelope(&second)["snapshot"];
    assert_eq!(
        second_snapshot["observations"],
        json!([{
            "causationId": "query-protocol-command-2",
            "projection": PROJECTOR_NAME,
            "model": "CausalQueryView",
            "scopeToken": second_snapshot["observations"][0]["scopeToken"].clone()
        }]),
        "the live suffix must carry exact causation evidence"
    );
    assert_opaque_token(
        &second_snapshot["observations"][0]["scopeToken"],
        "projection-obligation",
    );
    assert!(
        second_snapshot["records"]
            .as_array()
            .expect("second frame record revisions")
            .iter()
            .any(|record| {
                record["path"] == json!(["causal_query_views", "0"])
                    && record["incarnation"] == "1"
                    && record["revision"] == "2"
                    && record["tombstone"] == false
            }),
        "the current row path must carry the final record fence: {second}"
    );
    assert_eq!(
        distributed_envelope(&first)["snapshot"]["indexes"][0]["position"],
        "1",
        "later metadata must not bleed into the first emitted response"
    );
    assert_eq!(
        distributed_envelope(&second)["snapshot"]["indexes"][0]["position"],
        "2",
        "the second response must retain its own immutable frame metadata"
    );
}

#[tokio::test]
async fn live_subscription_replays_delete_tombstone_and_observation() {
    let fixture = protocol_fixture_with_rows().await;
    let mut stream = fixture
        .engine
        .execute_stream(&user_session(), Request::new(LIVE_SUBSCRIPTION));
    let initial = next_wire_frame(&mut stream).await;
    assert_live_frame(&initial, "causal_query_views", "causal row", "1", false);

    delete_item(&fixture.repository, &fixture.bus, 2).await;
    let deleted = next_wire_frame(&mut stream).await;
    assert_eq!(
        deleted["data"]["causal_query_views"],
        json!([]),
        "{deleted}"
    );
    let distributed = distributed_envelope(&deleted);
    assert_eq!(distributed["live"]["supported"], true);
    assert_eq!(distributed["live"]["reset"], false);
    assert_eq!(distributed["live"]["cursors"][0]["position"], "2");
    assert_eq!(distributed["snapshot"]["indexes"][0]["position"], "2");

    let tombstone = distributed["snapshot"]["records"]
        .as_array()
        .expect("delete record metadata")
        .iter()
        .find(|record| record["tombstone"] == true)
        .unwrap_or_else(|| panic!("delete frame omitted its tombstone: {deleted}"));
    assert!(tombstone.get("path").is_none(), "{tombstone}");
    assert_eq!(tombstone["model"], "CausalQueryView");
    assert_eq!(tombstone["incarnation"], "1");
    assert_eq!(tombstone["revision"], "2");
    assert_opaque_token(&tombstone["scopeToken"], "record-revision");

    let observation = &distributed["snapshot"]["observations"][0];
    assert_eq!(observation["causationId"], "query-protocol-command-2");
    assert_eq!(observation["projection"], PROJECTOR_NAME);
    assert_eq!(observation["model"], "CausalQueryView");
    assert_opaque_token(&observation["scopeToken"], "projection-obligation");
}

#[tokio::test]
async fn live_subscription_emits_pathless_fence_when_a_record_leaves_the_result() {
    let fixture = protocol_fixture_with_retention(10).await;
    let mut stream = fixture
        .engine
        .execute_stream(&user_session(), Request::new(FILTERED_LIVE_SUBSCRIPTION));
    let initial = next_wire_frame(&mut stream).await;
    assert_eq!(
        initial["data"]["causal_query_views"],
        json!([{ "title": "causal row" }])
    );

    project_item(&fixture.repository, &fixture.bus, 2, "outside filter").await;
    let excluded = next_wire_frame(&mut stream).await;
    assert_eq!(excluded["data"]["causal_query_views"], json!([]));
    let distributed = distributed_envelope(&excluded);
    assert_eq!(distributed["snapshot"]["indexes"][0]["position"], "2");
    let records = distributed["snapshot"]["records"]
        .as_array()
        .expect("pathless live fence");
    assert_eq!(records.len(), 1, "{excluded}");
    assert!(records[0].get("path").is_none(), "{excluded}");
    assert_eq!(records[0]["incarnation"], "1");
    assert_eq!(records[0]["revision"], "2");
    assert_eq!(records[0]["tombstone"], false);
    assert_opaque_token(&records[0]["scopeToken"], "record-revision");
    assert_eq!(
        distributed["snapshot"]["observations"][0]["causationId"],
        "query-protocol-command-2"
    );
}

#[tokio::test]
async fn resumed_suffix_coalesces_delete_recreate_and_repeated_upserts() {
    let fixture = protocol_fixture_with_retention(10).await;
    let mut initial_stream = fixture
        .engine
        .execute_stream(&user_session(), Request::new(LIVE_SUBSCRIPTION));
    let initial = next_wire_frame(&mut initial_stream).await;
    let cursors = assert_live_frame(&initial, "causal_query_views", "causal row", "1", false);
    drop(initial_stream);

    delete_item(&fixture.repository, &fixture.bus, 2).await;
    recreate_item(&fixture.repository, &fixture.bus, 3, "recreated").await;
    project_item(&fixture.repository, &fixture.bus, 4, "recreated 2").await;
    project_item(&fixture.repository, &fixture.bus, 5, "recreated 3").await;

    let mut resumed_stream = fixture.engine.execute_stream(
        &user_session(),
        request_with_resume(LIVE_SUBSCRIPTION, cursors),
    );
    let resumed = next_wire_frame(&mut resumed_stream).await;
    assert_live_frame(&resumed, "causal_query_views", "recreated 3", "5", false);
    let snapshot = &distributed_envelope(&resumed)["snapshot"];
    let records = snapshot["records"].as_array().expect("coalesced records");
    assert_eq!(records.len(), 1, "{resumed}");
    assert_eq!(records[0]["path"], json!(["causal_query_views", "0"]));
    assert_eq!(records[0]["incarnation"], "2");
    assert_eq!(records[0]["revision"], "3");
    assert_eq!(records[0]["tombstone"], false);

    let observations = snapshot["observations"]
        .as_array()
        .expect("causation observations");
    assert_eq!(observations.len(), 4, "{resumed}");
    assert_eq!(
        observations
            .iter()
            .map(|observation| observation["causationId"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec![
            "query-protocol-command-2",
            "query-protocol-command-3",
            "query-protocol-command-4",
            "query-protocol-command-5",
        ]
    );
}

#[tokio::test]
async fn multi_root_causal_query_fails_before_merging_independent_snapshots() {
    let engine = protocol_engine_with_rows().await;
    let response = engine
        .execute(
            &user_session(),
            Request::new(
                "query InvalidAtomicity { first: causal_query_views { title } second: causal_query_views { title } }",
            ),
        )
        .await;
    assert!(response.is_err(), "multi-root query unexpectedly succeeded");
    assert_eq!(response.errors.len(), 1);
    assert!(
        response.errors[0].message.contains("one read root"),
        "unexpected fail-closed diagnostic: {:?}",
        response.errors
    );
    assert!(
        !response.extensions.contains_key("distributed"),
        "a rejected multi-root operation must not advertise a fabricated atomic snapshot"
    );
}

#[tokio::test]
async fn row_filtered_surface_never_exposes_partition_wide_live_activity() {
    let fixture = protocol_fixture_with_rows().await;
    let engine = GraphqlEngine::builder(&fixture.repository)
        .service_id(SERVICE_ID)
        .protocol_token_key(TEST_PROTOCOL_TOKEN_KEY)
        .roles(&["user"])
        .model::<CausalQueryView>(
            ModelPermissions::new().grant("user", read().all_columns().rows(col("id").eq(ROW_ID))),
        )
        .client_projectors([projector()])
        .change_stream(fixture.repository.read_model_changes())
        .build()
        .expect("row-filtered query protocol engine");
    let mut stream = engine.execute_stream(&user_session(), Request::new(LIVE_SUBSCRIPTION));
    let initial = next_wire_frame(&mut stream).await;
    assert_eq!(
        initial["data"]["causal_query_views"],
        json!([{ "title": "causal row" }])
    );
    let envelope = distributed_envelope(&initial);
    assert_eq!(envelope["snapshot"]["recordsComplete"], true, "{initial}");
    assert_eq!(
        envelope["snapshot"]["indexesComparable"], false,
        "{initial}"
    );
    assert_eq!(envelope["snapshot"]["indexes"], json!([]), "{initial}");
    assert_eq!(envelope["snapshot"]["observations"], json!([]));
    let records = envelope["snapshot"]["records"]
        .as_array()
        .expect("authorized row record evidence");
    assert_eq!(records.len(), 1, "{initial}");
    assert_eq!(
        records[0]["path"],
        json!(["causal_query_views", "0"]),
        "{initial}"
    );
    assert_eq!(records[0]["model"], "CausalQueryView");
    assert_eq!(records[0]["tombstone"], false);
    assert_opaque_token(&records[0]["scopeToken"], "record-revision");
    assert_eq!(envelope["live"]["supported"], false, "{initial}");
    assert_eq!(envelope["live"]["reset"], true, "{initial}");
    assert_eq!(envelope["live"]["cursors"], json!([]), "{initial}");

    project_item_with_id(
        &fixture.repository,
        &fixture.bus,
        2,
        "other-principal-private-row",
        "denied row",
    )
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(350), stream.next())
            .await
            .is_err(),
        "a denied-row commit must not leak a cursor, causation, tombstone, or activity frame"
    );
}

async fn query_over_http_and_graphql_ws(
    address: std::net::SocketAddr,
    document: &str,
    identity: Option<(&str, &str)>,
) -> (Value, Value) {
    let mut http_request = reqwest::Client::new()
        .post(format!("http://{address}/graphql"))
        .json(&json!({ "query": document }));
    if let Some((user, role)) = identity {
        http_request = http_request
            .header("x-user-id", user)
            .header("x-roles", role);
    }
    let http = http_request
        .send()
        .await
        .expect("HTTP query response")
        .json()
        .await
        .expect("HTTP query JSON");

    let mut request = format!("ws://{address}/graphql/ws")
        .into_client_request()
        .expect("GraphQL WS request");
    request.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        "graphql-transport-ws"
            .parse()
            .expect("GraphQL WS protocol header"),
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(request)
        .await
        .expect("GraphQL WS connect");
    let init_payload = identity
        .map(|(user, role)| json!({ "x-user-id": user, "x-roles": role }))
        .unwrap_or_else(|| json!({}));
    socket
        .send(WsMessage::Text(
            json!({ "type": "connection_init", "payload": init_payload })
                .to_string()
                .into(),
        ))
        .await
        .expect("GraphQL WS init");
    let acknowledgement = socket
        .next()
        .await
        .expect("GraphQL WS acknowledgement")
        .expect("valid GraphQL WS acknowledgement");
    assert_eq!(
        serde_json::from_str::<Value>(acknowledgement.to_text().unwrap()).unwrap(),
        json!({ "type": "connection_ack" })
    );
    socket
        .send(WsMessage::Text(
            json!({
                "id": "query-1",
                "type": "subscribe",
                "payload": { "query": document }
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("GraphQL WS query");
    let next = socket
        .next()
        .await
        .expect("GraphQL WS next frame")
        .expect("valid GraphQL WS next frame");
    let next: Value = serde_json::from_str(next.to_text().unwrap()).unwrap();
    assert_eq!(next["type"], "next", "{next}");
    (http, next["payload"].clone())
}

#[derive(Clone, Copy)]
enum QueryGatewayMode {
    Direct,
    #[cfg(feature = "gateway-graphql-native")]
    Embedded,
    #[cfg(feature = "gateway-graphql-native")]
    Remote,
}

#[tokio::test]
async fn http_and_graphql_ws_serialize_the_same_query_revision_envelope() {
    exercise_query_transport(QueryGatewayMode::Direct).await;
}

#[cfg(feature = "gateway-graphql-native")]
#[tokio::test]
async fn gateway_embedded_query_revision_parity() {
    exercise_query_transport(QueryGatewayMode::Embedded).await;
}

#[cfg(feature = "gateway-graphql-native")]
#[tokio::test]
async fn gateway_remote_query_revision_parity() {
    exercise_query_transport(QueryGatewayMode::Remote).await;
}

async fn exercise_query_transport(mode: QueryGatewayMode) {
    let fixture = protocol_fixture_with_retention(10).await;
    let builder = GraphqlEngine::builder(&fixture.repository)
        .service_id(SERVICE_ID)
        .protocol_token_key(TEST_PROTOCOL_TOKEN_KEY)
        .roles(&["user"])
        .anonymous_role("user")
        .model::<CausalQueryView>(ModelPermissions::new().grant("user", read().all_columns()))
        .client_projectors([projector()])
        .change_stream(fixture.repository.read_model_changes());
    #[cfg(feature = "gateway-graphql-native")]
    let builder = match mode {
        QueryGatewayMode::Direct => builder,
        _ => builder.identity(distributed::graphql::IdentityConfig::oidc_bearer(
            distributed::graphql::OidcConfig::new("http://local-fixture.invalid", "query-api")
                .require_auth(false),
        )),
    };
    let engine = builder.build().expect("query transport parity engine");
    let service = Arc::new(
        Service::new()
            .named(SERVICE_ID)
            .try_with_graphql(engine)
            .expect("query transport parity service"),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("query transport listener");
    let address = listener.local_addr().expect("query transport address");
    #[allow(unused_mut)]
    let mut upstream = None::<tokio::task::JoinHandle<()>>;
    let router = match mode {
        QueryGatewayMode::Direct => distributed::microsvc::router(service),
        #[cfg(feature = "gateway-graphql-native")]
        mode => {
            use distributed::gateway::{native::*, *};
            let caps = GraphqlCapabilities {
                commands: false,
                queries: true,
                live: true,
            };
            let (executor, binding) = match mode {
                QueryGatewayMode::Embedded => (
                    GraphqlExecutor::Embedded,
                    GraphqlBinding::Embedded(
                        EmbeddedGraphql::new(service.graphql_engine().unwrap(), None, caps)
                            .unwrap(),
                    ),
                ),
                QueryGatewayMode::Remote => {
                    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let origin = format!("http://{}", listener.local_addr().unwrap());
                    upstream = Some(tokio::spawn(async move {
                        axum::serve(listener, distributed::microsvc::router(service))
                            .await
                            .unwrap()
                    }));
                    (
                        GraphqlExecutor::Remote { origin },
                        GraphqlBinding::Remote(RemoteGraphql::default()),
                    )
                }
                QueryGatewayMode::Direct => unreachable!(),
            };
            let config = GatewayConfig {
                bindings: vec![Binding::new(
                    "graphql",
                    BindingKind::Graphql {
                        executor,
                        capabilities: caps,
                        delivery: DeliveryCapabilities::default(),
                        schema_extensions: vec![],
                    },
                )],
                routes: vec![Route::new("api", RoutePath::prefix("/graphql"), "graphql")],
            }
            .build()
            .unwrap();
            NativeGateway::new(
                config,
                NativeOptions::new(format!("http://{address}")),
                [("graphql".into(), NativeBinding::Graphql(binding))],
                NativeAuth::anonymous(),
            )
            .unwrap()
            .router()
        }
    };
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("query transport server");
    });

    let document = "query QueryTransportParity { causal_query_views { title } }";
    for (case, identity) in [
        ("anonymous", None),
        ("explicit dev identity", Some(("demo", "user"))),
    ] {
        let (http, websocket) = query_over_http_and_graphql_ws(address, document, identity).await;
        assert_eq!(http["data"], websocket["data"], "{case}");
        assert_eq!(
            distributed_envelope(&http),
            distributed_envelope(&websocket),
            "{case}: HTTP and GraphQL-WS must preserve one canonical query/revision envelope"
        );
    }
    // Exercise committed projection changes through the actual socket path,
    // including redundant invalidation and immutable per-frame causal metadata.
    let mut request = format!("ws://{address}/graphql/ws")
        .into_client_request()
        .unwrap();
    request.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        "graphql-transport-ws".parse().unwrap(),
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    socket
        .send(WsMessage::Text(
            json!({"type":"connection_init"}).to_string().into(),
        ))
        .await
        .unwrap();
    let ack = socket.next().await.unwrap().unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(ack.to_text().unwrap()).unwrap()["type"],
        "connection_ack"
    );
    socket
        .send(WsMessage::Text(
            json!({"id":"live-proof", "type":"subscribe", "payload":{"query":LIVE_SUBSCRIPTION}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let mut frames = Vec::new();
    for position in 1..=3 {
        if position > 1 {
            project_item(
                &fixture.repository,
                &fixture.bus,
                position,
                &format!("causal row {position}"),
            )
            .await;
        }
        let frame = tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let frame: Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
        assert_eq!(frame["id"], "live-proof");
        assert_eq!(frame["type"], "next");
        let title = if position == 1 {
            "causal row".into()
        } else {
            format!("causal row {position}")
        };
        assert_live_frame(
            &frame["payload"],
            "causal_query_views",
            &title,
            &position.to_string(),
            false,
        );
        if position > 1 {
            assert_eq!(
                distributed_envelope(&frame["payload"])["snapshot"]["observations"][0]
                    ["causationId"],
                format!("query-protocol-command-{position}")
            );
        }
        frames.push(frame["payload"].clone());
        fixture
            .repository
            .publish_read_model_change(distributed::ReadModelChange::new(["causal_query_views"]));
        let redundant = tokio::time::timeout(Duration::from_millis(350), socket.next()).await;
        if position == 1 {
            assert!(
                redundant.is_err(),
                "identical data and metadata must be suppressed"
            );
        } else {
            // The origin emits a proof-only frame as its causation suffix moves
            // to the new head. Preserve it even though domain data is identical.
            let redundant = redundant.unwrap().unwrap().unwrap();
            let proof: Value = serde_json::from_str(redundant.to_text().unwrap()).unwrap();
            assert_eq!(proof["id"], "live-proof");
            assert_eq!(proof["payload"]["data"], frame["payload"]["data"]);
            assert_eq!(
                distributed_envelope(&proof["payload"])["snapshot"]["observations"],
                json!([])
            );
            fixture
                .repository
                .publish_read_model_change(distributed::ReadModelChange::new([
                    "causal_query_views",
                ]));
            assert!(
                tokio::time::timeout(Duration::from_millis(350), socket.next())
                    .await
                    .is_err()
            );
        }
    }
    for (i, frame) in frames.iter().enumerate() {
        assert_eq!(
            distributed_envelope(frame)["snapshot"]["indexes"][0]["position"],
            (i + 1).to_string()
        );
    }
    socket.close(None).await.unwrap();
    server.abort();
    if let Some(upstream) = upstream {
        upstream.abort();
    }
}

#[tokio::test]
async fn unowned_legacy_query_is_explicitly_incomplete_and_still_strips_keys() {
    let engine = protocol_engine_with_rows().await;
    let response = wire_response(
        engine
            .execute(
                &user_session(),
                Request::new("query LegacyFallback { legacyAlias: legacy_query_views { title } }"),
            )
            .await,
    );

    assert_eq!(
        response["data"],
        json!({ "legacyAlias": [{ "title": "legacy row" }] })
    );
    let serialized = response.to_string();
    assert!(!serialized.contains(HIDDEN_ALIAS_PREFIX), "{response}");
    assert!(
        !serialized.contains(LEGACY_ROW_ID),
        "unowned primary key leaked through fallback metadata: {response}"
    );

    let snapshot = &distributed_envelope(&response)["snapshot"];
    assert_eq!(snapshot["recordsComplete"], false);
    assert_eq!(snapshot["indexesComparable"], false);
    assert_eq!(snapshot["records"], json!([]));
    assert_eq!(snapshot["indexes"], json!([]));
    assert_eq!(snapshot["observations"], json!([]));
    assert_opaque_token(&snapshot["scopeToken"], "query-snapshot");
}

#[cfg(feature = "gateway-delivery")]
#[tokio::test]
async fn freshness_context_binds_origin_and_rejects_unproven_minima() {
    use distributed::gateway::delivery::{FreshnessContext, Minimum};
    let fixture = protocol_fixture_with_retention(10).await;
    let document = "query ReadFreshness { causal_query_views { title } }";
    let request = || Request::new(document);
    let identity = fixture
        .engine
        .delivery_identity(&user_session(), &request())
        .unwrap();
    let mut context = FreshnessContext::parse(&json!({
        "version":1, "schemaHash":identity.schema_hash, "protocolHash":identity.protocol_hash,
        "authorizationGeneration":identity.authorization_generation, "cacheScope":identity.cache_scope,
        "pending":[{"complete":true,"models":["CausalQueryView"],"relationships":[]}], "minimum":[]
    })).unwrap();
    let with_context = |context: &FreshnessContext| {
        let mut request = request();
        request.extensions.insert(
            "gatewayFreshness".into(),
            async_graphql::Value::from_json(serde_json::to_value(context).unwrap()).unwrap(),
        );
        request
    };
    let before = wire_response(
        fixture
            .engine
            .execute(&user_session(), with_context(&context))
            .await,
    );
    assert_eq!(
        before["data"]["causal_query_views"][0]["title"],
        "causal row"
    );
    assert_eq!(
        distributed_envelope(&before)["snapshot"]["observations"],
        json!([])
    );
    project_item(&fixture.repository, &fixture.bus, 2, "committed").await;
    let after = wire_response(
        fixture
            .engine
            .execute(&user_session(), with_context(&context))
            .await,
    );
    let index = &distributed_envelope(&after)["snapshot"]["indexes"][0];
    context
        .observe([Minimum::Index {
            projection: index["projection"].as_str().unwrap().into(),
            scope_token: index["scopeToken"].as_str().unwrap().into(),
            position: "2".into(),
        }])
        .unwrap();
    context.pending.clear();
    assert_eq!(
        wire_response(
            fixture
                .engine
                .execute(&user_session(), with_context(&context))
                .await
        )["data"]["causal_query_views"][0]["title"],
        "committed"
    );
    let mut future = context.clone();
    if let Minimum::Index { position, .. } = &mut future.minimum[0] {
        *position = "999".into();
    }
    let rejected = serde_json::to_value(
        fixture
            .engine
            .execute(&user_session(), with_context(&future))
            .await,
    )
    .unwrap();
    assert_eq!(
        rejected["errors"][0]["extensions"]["code"],
        "FRESHNESS_PENDING"
    );
    assert!(rejected["data"].is_null());
    let mut forged = context;
    forged.cache_scope = "another-subject".into();
    assert_eq!(
        serde_json::to_value(
            fixture
                .engine
                .execute(&user_session(), with_context(&forged))
                .await
        )
        .unwrap()["errors"][0]["extensions"]["code"],
        "FRESHNESS_SCOPE_CHANGED"
    );
}

#[cfg(feature = "gateway-delivery")]
#[tokio::test]
async fn gateway_validator_tracks_projection_commit_in_the_result_snapshot() {
    use distributed::graphql::delivery::GatewayVersionStore;
    let fixture = protocol_fixture_with_retention(10).await;
    let pool = distributed::graphql::GraphqlPool::from(fixture.repository.pool().clone());
    let store =
        GatewayVersionStore::install(&pool, "query-protocol-cache", ["causal_query_views".into()])
            .await
            .unwrap();
    let engine = GraphqlEngine::builder(&fixture.repository)
        .service_id(SERVICE_ID)
        .protocol_token_key(TEST_PROTOCOL_TOKEN_KEY)
        .roles(&["user"])
        .anonymous_role("user")
        .model::<CausalQueryView>(ModelPermissions::new().grant("user", read().all_columns()))
        .client_projectors([projector()])
        .gateway_versions(store.clone())
        .build()
        .unwrap();
    let request = |action: &str| {
        let mut request = Request::new("query CachedRows { causal_query_views { title } }");
        request.extensions.insert(
            "gatewayDelivery".into(),
            async_graphql::Value::from_json(json!({"action":action})).unwrap(),
        );
        request
    };
    let validate = wire_response(engine.execute(&user_session(), request("validate")).await);
    assert_eq!(
        validate["extensions"]["gatewayDelivery"]["eligible"], true,
        "{validate}"
    );
    let validator = validate["extensions"]["gatewayDelivery"]["admission"]["validator"].clone();
    let first = wire_response(engine.execute(&user_session(), request("snapshot")).await);
    assert_eq!(
        first["extensions"]["gatewayDelivery"]["validator"], validator,
        "{first}"
    );
    assert_eq!(
        first["data"]["causal_query_views"][0]["title"],
        "causal row"
    );
    project_item(&fixture.repository, &fixture.bus, 2, "changed").await;
    let validate = wire_response(engine.execute(&user_session(), request("validate")).await);
    assert_ne!(
        validate["extensions"]["gatewayDelivery"]["admission"]["validator"],
        validator
    );
    let second = wire_response(engine.execute(&user_session(), request("snapshot")).await);
    assert_eq!(
        second["extensions"]["gatewayDelivery"]["validator"],
        validate["extensions"]["gatewayDelivery"]["admission"]["validator"]
    );
    assert_eq!(second["data"]["causal_query_views"][0]["title"], "changed");
    assert_eq!(
        distributed_envelope(&second)["snapshot"]["indexes"][0]["position"],
        "2"
    );
    store.rotate_epoch(&pool).await.unwrap();
    let next_epoch = wire_response(engine.execute(&user_session(), request("validate")).await);
    assert_ne!(
        next_epoch["extensions"]["gatewayDelivery"]["admission"]["validator"],
        second["extensions"]["gatewayDelivery"]["validator"]
    );
    // A failed write transaction cannot advance the cache dependency version.
    let mut tx = fixture.repository.pool().begin().await.unwrap();
    sqlx::query("UPDATE causal_query_views SET title='rolled back'")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    let rolled_back = wire_response(engine.execute(&user_session(), request("validate")).await);
    assert_eq!(
        rolled_back["extensions"]["gatewayDelivery"]["admission"]["validator"],
        next_epoch["extensions"]["gatewayDelivery"]["admission"]["validator"]
    );
}

#[cfg(all(feature = "gateway-delivery", feature = "gateway-graphql-native"))]
#[tokio::test]
async fn native_snapshot_cache_revalidates_each_consumer_without_result_sql() {
    use axum::{routing::post, Router};
    use distributed::gateway::{delivery::SnapshotLimits, native::*, *};
    use distributed::graphql::delivery::GatewayVersionStore;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tower::ServiceExt;
    let fixture = protocol_fixture_with_retention(10).await;
    let pool = distributed::graphql::GraphqlPool::from(fixture.repository.pool().clone());
    let store = GatewayVersionStore::install(&pool, "native-cache", ["causal_query_views".into()])
        .await
        .unwrap();
    let engine = Arc::new(
        GraphqlEngine::builder(&fixture.repository)
            .service_id(SERVICE_ID)
            .protocol_token_key(TEST_PROTOCOL_TOKEN_KEY)
            .roles(&["user"])
            .anonymous_role("user")
            .subscriptions(false)
            .model::<CausalQueryView>(
                ModelPermissions::new().grant("user", read().all_columns().aggregations()),
            )
            .client_projectors([projector()])
            .gateway_versions(store.clone())
            .build()
            .unwrap(),
    );
    let revoked = Arc::new(AtomicBool::new(false));
    let origin = {
        let engine = engine.clone();
        let revoked = revoked.clone();
        Router::new().route(
            "/graphql",
            post(move |axum::Json(value): axum::Json<serde_json::Value>| {
                let engine = engine.clone();
                let revoked = revoked.clone();
                async move {
                    if revoked.load(Ordering::SeqCst) {
                        return (
                            axum::http::StatusCode::UNAUTHORIZED,
                            axum::Json(json!({"error":"denied"})),
                        );
                    }
                    let request: Request = serde_json::from_value(value).unwrap();
                    (
                        axum::http::StatusCode::OK,
                        axum::Json(
                            serde_json::to_value(engine.execute(&user_session(), request).await)
                                .unwrap(),
                        ),
                    )
                }
            }),
        )
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin_url = format!("http://{}", listener.local_addr().unwrap());
    let origin_task = tokio::spawn(async move { axum::serve(listener, origin).await.unwrap() });
    struct Stop(tokio::task::JoinHandle<()>);
    impl Drop for Stop {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let _origin = Stop(origin_task);
    for remote in [false, true] {
        let caps = GraphqlCapabilities {
            queries: true,
            ..Default::default()
        };
        let executor = if remote {
            GraphqlExecutor::Remote {
                origin: origin_url.clone(),
            }
        } else {
            GraphqlExecutor::Embedded
        };
        let binding = if remote {
            GraphqlBinding::Remote(RemoteGraphql {
                live_path: None,
                ..Default::default()
            })
        } else {
            GraphqlBinding::Embedded(EmbeddedGraphql::new(engine.clone(), None, caps).unwrap())
        };
        let delivery = Arc::new(NativeDelivery::snapshots(SnapshotLimits::default()).unwrap());
        let config = GatewayConfig {
            bindings: vec![Binding::new(
                "api",
                BindingKind::Graphql {
                    executor,
                    capabilities: caps,
                    delivery: DeliveryCapabilities {
                        snapshots: true,
                        ..Default::default()
                    },
                    schema_extensions: vec![],
                },
            )],
            routes: vec![Route::new("api", RoutePath::prefix("/graphql"), "api")],
        }
        .build()
        .unwrap();
        let gateway = NativeGateway::new(
            config,
            NativeOptions::new("http://public.invalid"),
            [(
                "api".into(),
                NativeBinding::GraphqlWithDelivery(binding, delivery.clone()),
            )],
            NativeAuth::anonymous(),
        )
        .unwrap()
        .router();
        let run = |document: &str| {
            let gateway = gateway.clone();
            let body = serde_json::to_vec(&json!({"query": document})).unwrap();
            async move {
                let response = gateway
                    .oneshot(
                        axum::http::Request::post("/graphql")
                            .header("content-type", "application/json")
                            .body(axum::body::Body::from(body))
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                let status = response.status();
                let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
                    .await
                    .unwrap();
                (
                    status,
                    serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
                )
            }
        };
        let document = "query Cache { causal_query_views { title } }";
        let before = store.metrics();
        let (status, first) = run(document).await;
        assert_eq!(status, axum::http::StatusCode::OK);
        assert!(first.get("errors").is_none(), "{first}");
        assert!(
            first["extensions"]["gatewayDelivery"]["validator"].is_string(),
            "{first}"
        );
        let filled = store.metrics();
        assert_eq!(filled.result_executions, before.result_executions + 1);
        assert_eq!(filled.validations, before.validations + 2);
        for _ in 0..5 {
            assert_eq!(run(document).await.1, first);
        }
        let hit = store.metrics();
        assert_eq!(
            hit.result_executions, filled.result_executions,
            "cache hit ran result SQL"
        );
        assert_eq!(
            hit.validations,
            filled.validations + 5,
            "each consumer needs fresh origin admission"
        );
        // No invalidation feed is connected: transactional dependency validation
        // must still discover a supported external SQL producer's update.
        sqlx::query("UPDATE causal_query_views SET title=title || '-external'")
            .execute(fixture.repository.pool())
            .await
            .unwrap();
        let changed = run(document).await.1;
        assert_ne!(changed["data"], first["data"]);
        assert_eq!(store.metrics().result_executions, hit.result_executions + 1);
        assert_eq!(run(document).await.1, changed);
        delivery.invalidate_all();
        assert_eq!(run(document).await.1, changed);
        assert_eq!(store.metrics().result_executions, hit.result_executions + 2);
        if remote {
            revoked.store(true, Ordering::SeqCst);
            assert_eq!(
                run(document).await.0,
                axum::http::StatusCode::UNAUTHORIZED,
                "no private stale-on-auth-error"
            );
            assert_eq!(store.metrics().result_executions, hit.result_executions + 2);
            revoked.store(false, Ordering::SeqCst);
        }
        // A projection can advance evidence while leaving selected data equal.
        let title = changed["data"]["causal_query_views"][0]["title"]
            .as_str()
            .unwrap();
        project_item(
            &fixture.repository,
            &fixture.bus,
            if remote { 3 } else { 2 },
            title,
        )
        .await;
        let proof_only = run(document).await.1;
        assert_eq!(proof_only["data"], changed["data"]);
        assert_ne!(
            proof_only["extensions"]["gatewayDelivery"]["validator"],
            changed["extensions"]["gatewayDelivery"]["validator"]
        );
        let filtered =
            "query Filtered { causal_query_views(where: {title: {_eq: \"matching\"}}) { title } }";
        let count = "query Count { causal_query_views_aggregate(where: {title: {_eq: \"matching\"}}) { aggregate { count } } }";
        assert_eq!(
            run(filtered).await.1["data"]["causal_query_views"],
            json!([])
        );
        let empty_count = run(count).await.1;
        assert!(empty_count.get("errors").is_none(), "{empty_count}");
        assert_eq!(
            empty_count["data"]["causal_query_views_aggregate"]["aggregate"]["count"],
            0
        );
        let filled = store.metrics();
        run(filtered).await;
        run(count).await;
        assert_eq!(store.metrics().result_executions, filled.result_executions);
        sqlx::query("UPDATE causal_query_views SET title='matching'")
            .execute(fixture.repository.pool())
            .await
            .unwrap();
        assert_eq!(
            run(filtered).await.1["data"]["causal_query_views"][0]["title"],
            "matching"
        );
        assert_eq!(
            run(count).await.1["data"]["causal_query_views_aggregate"]["aggregate"]["count"],
            1
        );
    }
}

#[cfg(all(feature = "gateway-delivery", feature = "gateway-graphql-native"))]
#[tokio::test]
async fn hundred_reads_one_execution_with_cache_disabled() {
    use axum::{routing::post, Router};
    use distributed::gateway::{delivery::FlightLimits, native::*, *};
    use distributed::graphql::delivery::GatewayVersionStore;
    use tower::ServiceExt;
    let fixture = protocol_fixture_with_retention(10).await;
    let store = GatewayVersionStore::install(
        &distributed::graphql::GraphqlPool::from(fixture.repository.pool().clone()),
        "flight-test",
        ["causal_query_views".into()],
    )
    .await
    .unwrap();
    let engine = Arc::new(
        GraphqlEngine::builder(&fixture.repository)
            .service_id(SERVICE_ID)
            .protocol_token_key(TEST_PROTOCOL_TOKEN_KEY)
            .roles(&["user"])
            .anonymous_role("user")
            .subscriptions(false)
            .model::<CausalQueryView>(ModelPermissions::new().grant("user", read().all_columns()))
            .client_projectors([projector()])
            .gateway_versions(store.clone())
            .build()
            .unwrap(),
    );
    let protocol_hash = engine
        .delivery_identity(
            &user_session(),
            &Request::new("query Coalesced { causal_query_views { title } }"),
        )
        .unwrap()
        .protocol_hash;
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let handler_gate = gate.clone();
    let origin = Router::new().route(
        "/graphql",
        post(move |axum::Json(value): axum::Json<serde_json::Value>| {
            let engine = engine.clone();
            let gate = handler_gate.clone();
            async move {
                if value["extensions"]["gatewayDelivery"]["action"] == "snapshot" {
                    gate.acquire().await.unwrap().forget();
                }
                axum::Json(
                    serde_json::to_value(
                        engine
                            .execute(
                                &user_session(),
                                serde_json::from_value::<Request>(value).unwrap(),
                            )
                            .await,
                    )
                    .unwrap(),
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin_url = format!("http://{}", listener.local_addr().unwrap());
    struct Stop(tokio::task::JoinHandle<()>);
    impl Drop for Stop {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let _server = Stop(tokio::spawn(async move {
        axum::serve(listener, origin).await.unwrap()
    }));
    let delivery = Arc::new(NativeDelivery::coalescing(FlightLimits::default()).unwrap());
    let config = GatewayConfig {
        bindings: vec![Binding::new(
            "api",
            BindingKind::Graphql {
                executor: GraphqlExecutor::Remote { origin: origin_url },
                capabilities: GraphqlCapabilities {
                    queries: true,
                    ..Default::default()
                },
                delivery: DeliveryCapabilities {
                    coalescing: true,
                    ..Default::default()
                },
                schema_extensions: vec![],
            },
        )],
        routes: vec![Route::new("api", RoutePath::prefix("/graphql"), "api")],
    }
    .build()
    .unwrap();
    let router = NativeGateway::new(
        config,
        NativeOptions::new("http://public.invalid"),
        [(
            "api".into(),
            NativeBinding::GraphqlWithDelivery(
                GraphqlBinding::Remote(RemoteGraphql {
                    live_path: None,
                    ..Default::default()
                }),
                delivery.clone(),
            ),
        )],
        NativeAuth::anonymous(),
    )
    .unwrap()
    .router();
    let document = "query Coalesced { causal_query_views { title } }";
    let spawn = |extension: Option<serde_json::Value>| {
        let router = router.clone();
        let mut request = json!({"query":document});
        if let Some(extension) = extension {
            request["extensions"] = json!({"gatewayFreshness":extension});
        }
        tokio::spawn(async move {
            let response = router
                .oneshot(
                    axum::http::Request::post("/graphql")
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(request.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), axum::http::StatusCode::OK);
            let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
                .await
                .unwrap();
            serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()
        })
    };
    let wait = |groups, consumers| {
        let delivery = delivery.clone();
        async move {
            tokio::time::timeout(Duration::from_secs(10), async {
                while delivery.flight_counts() != (groups, consumers) {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("all consumers admitted and joined before releasing result execution");
        }
    };
    let mut consumers = Vec::new();
    for _ in 0..100 {
        consumers.push(spawn(None));
    }
    wait(1, 100).await;
    assert_eq!(store.metrics().validations, 100);
    assert_eq!(store.metrics().result_executions, 0);
    gate.add_permits(1);
    let first = consumers.pop().unwrap().await.unwrap();
    assert!(first.get("errors").is_none(), "{first}");
    for consumer in consumers {
        assert_eq!(consumer.await.unwrap(), first);
    }
    assert_eq!(
        store.metrics().result_executions,
        1,
        "100 overlapping consumers must execute result SQL exactly once"
    );
    assert_eq!(delivery.flight_counts(), (0, 0));
    // Cache is disabled: a subsequent nonoverlapping request executes again.
    let next = spawn(None);
    wait(1, 1).await;
    gate.add_permits(1);
    assert_eq!(next.await.unwrap(), first);
    assert_eq!(store.metrics().result_executions, 2);
    // One consumer can cancel without aborting the other's execution.
    let cancelled = spawn(None);
    let survivor = spawn(None);
    wait(1, 2).await;
    cancelled.abort();
    let _ = cancelled.await;
    wait(1, 1).await;
    gate.add_permits(1);
    assert_eq!(survivor.await.unwrap(), first);
    assert_eq!(store.metrics().result_executions, 3);
    // A stronger floor must not join a flight admitted without that floor.
    let protocol = &first["extensions"]["distributed"];
    let index = &protocol["snapshot"]["indexes"][0];
    let context = json!({"version":1,"schemaHash":protocol["schemaHash"],"protocolHash":protocol_hash,
        "authorizationGeneration":protocol["authorizationGeneration"],"cacheScope":protocol["cacheScope"],"pending":[],
        "minimum":[{"kind":"index","projection":index["projection"],"scopeToken":index["scopeToken"],"position":"1"}]});
    let old = spawn(None);
    wait(1, 1).await;
    let stronger = spawn(Some(context));
    wait(2, 2).await;
    gate.add_permits(2);
    assert_eq!(old.await.unwrap(), first);
    let strong = stronger.await.unwrap();
    assert!(strong.get("errors").is_none(), "{strong}");
    assert_eq!(store.metrics().result_executions, 5);
    assert_eq!(delivery.flight_counts(), (0, 0));
}

#[cfg(all(feature = "gateway-delivery", feature = "gateway-graphql-native"))]
#[tokio::test]
async fn hundred_subscribers_one_upstream_over_embedded_and_remote_websockets() {
    use axum::Router;
    use distributed::gateway::{delivery::LiveLimits, native::*, *};
    use distributed::graphql::{delivery::GatewayVersionStore, IdentityConfig, OidcConfig};
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use rsa::{pkcs1::EncodeRsaPrivateKey, traits::PublicKeyParts, RsaPrivateKey, RsaPublicKey};
    let private = RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();
    let public = RsaPublicKey::from(&private);
    let encoding = EncodingKey::from_rsa_pem(
        private
            .to_pkcs1_pem(rsa::pkcs8::LineEnding::LF)
            .unwrap()
            .as_bytes(),
    )
    .unwrap();
    let jwks=json!({"keys":[{"kty":"RSA","kid":"live-test","alg":"RS256","use":"sig",
        "n":base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(public.n().to_bytes_be()),"e":base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(public.e().to_bytes_be())}]}).to_string();
    let token = |subject: &str| {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("live-test".into());
        encode(&header,&json!({"iss":"https://live-fixture.invalid","aud":"live-fixture","sub":subject,"iat":now-1,"nbf":now-1,"exp":now+3600,"roles":["user"]}),&encoding).unwrap()
    };
    struct Server {
        origin: String,
        task: tokio::task::JoinHandle<()>,
    }
    impl Drop for Server {
        fn drop(&mut self) {
            self.task.abort();
        }
    }
    async fn serve(router: Router) -> Server {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        Server {
            origin,
            task: tokio::spawn(async move { axum::serve(listener, router).await.unwrap() }),
        }
    }
    type Socket = tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >;
    async fn connect(origin: &str, token: &str, legacy: bool) -> Socket {
        let mut request = format!("{}/graphql/ws", origin.replace("http:", "ws:"))
            .into_client_request()
            .unwrap();
        request.headers_mut().insert(
            "sec-websocket-protocol",
            if legacy {
                "graphql-ws"
            } else {
                "graphql-transport-ws"
            }
            .parse()
            .unwrap(),
        );
        let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        socket.send(WsMessage::Text(json!({"type":"connection_init","payload":{"authorization":format!("Bearer {token}")}}).to_string().into())).await.unwrap();
        let ack = tokio::time::timeout(Duration::from_secs(10), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            matches!(ack,WsMessage::Text(ref text) if serde_json::from_str::<Value>(text).unwrap()["type"]=="connection_ack"),
            "{ack:?}"
        );
        socket
    }
    async fn next(socket: &mut Socket, id: &str, title: &str) -> Value {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let message = socket.next().await.unwrap().unwrap();
                if let WsMessage::Text(text) = message {
                    let value: Value = serde_json::from_str(&text).unwrap();
                    if matches!(value["type"].as_str(), Some("next" | "data")) {
                        assert_eq!(value["id"], id);
                        assert!(value["payload"].get("errors").is_none(), "{value}");
                        if value["payload"]["data"]["causal_query_views"][0]["title"] == title {
                            return value["payload"].clone();
                        }
                    }
                }
            }
        })
        .await
        .expect("matching full live frame")
    }
    for remote in [false, true] {
        eprintln!(
            "shared live transport: {}",
            if remote { "remote" } else { "embedded" }
        );
        let fixture = protocol_fixture_with_retention(10).await;
        let versions = GatewayVersionStore::install(
            &distributed::graphql::GraphqlPool::from(fixture.repository.pool().clone()),
            "live-fixture",
            ["causal_query_views".into()],
        )
        .await
        .unwrap();
        let oidc = OidcConfig::new("https://live-fixture.invalid", "live-fixture")
            .with_static_jwks(jwks.clone())
            .engine_roles(&["user"]);
        let engine = Arc::new(
            GraphqlEngine::builder(&fixture.repository)
                .service_id(SERVICE_ID)
                .protocol_token_key(TEST_PROTOCOL_TOKEN_KEY)
                .roles(&["user"])
                .anonymous_role("user")
                .identity(IdentityConfig::oidc_bearer(oidc))
                .model::<CausalQueryView>(
                    ModelPermissions::new().grant("user", read().all_columns()),
                )
                .client_projectors([projector()])
                .change_stream(fixture.repository.read_model_changes())
                .gateway_versions(versions.clone())
                .build()
                .unwrap(),
        );
        let caps = GraphqlCapabilities {
            queries: true,
            live: true,
            ..Default::default()
        };
        let origin = serve(distributed::graphql::graphql_router_composed(
            engine.clone(),
            None,
            None,
        ))
        .await;
        let executor = if remote {
            GraphqlExecutor::Remote {
                origin: origin.origin.clone(),
            }
        } else {
            GraphqlExecutor::Embedded
        };
        let binding = if remote {
            GraphqlBinding::Remote(RemoteGraphql::default())
        } else {
            GraphqlBinding::Embedded(EmbeddedGraphql::new(engine.clone(), None, caps).unwrap())
        };
        let delivery = Arc::new(NativeDelivery::live(LiveLimits::default()).unwrap());
        let config = GatewayConfig {
            bindings: vec![Binding::new(
                "api",
                BindingKind::Graphql {
                    executor,
                    capabilities: caps,
                    delivery: DeliveryCapabilities {
                        live_sharing: true,
                        ..Default::default()
                    },
                    schema_extensions: vec![],
                },
            )],
            routes: vec![Route::new("api", RoutePath::prefix("/graphql"), "api")],
        }
        .build()
        .unwrap();
        let gateway = serve(
            NativeGateway::new(
                config,
                NativeOptions::new("http://public.invalid"),
                [(
                    "api".into(),
                    NativeBinding::GraphqlWithDelivery(binding, delivery.clone()),
                )],
                NativeAuth::anonymous(),
            )
            .unwrap()
            .router(),
        )
        .await;
        let alice = token("alice");
        let bob = token("bob");
        let mut sockets = futures_util::future::join_all(
            (0..100).map(|_| connect(&gateway.origin, &alice, false)),
        )
        .await;
        for (n, socket) in sockets.iter_mut().enumerate() {
            socket.send(WsMessage::Text(json!({"id":format!("consumer-{n}"),"type":"subscribe","payload":{"query":LIVE_SUBSCRIPTION}}).to_string().into())).await.unwrap();
        }
        let mut first = None;
        for (n, socket) in sockets.iter_mut().enumerate() {
            let payload = next(socket, &format!("consumer-{n}"), "causal row").await;
            if let Some(first) = &first {
                assert_eq!(&payload, first);
            } else {
                first = Some(payload);
            }
        }
        assert_eq!(
            (delivery.live_counts().0, delivery.live_counts().1),
            (1, 100)
        );
        assert_eq!(engine.live_subscriber_count(), 1);
        assert_eq!(versions.metrics().validations, 100);
        assert_eq!(
            versions.metrics().result_executions,
            1,
            "100 authenticated clients should own one origin live query"
        );
        let first = first.unwrap();
        let mut bob_socket = connect(&gateway.origin, &bob, false).await;
        bob_socket
            .send(WsMessage::Text(
                json!({"id":"consumer-0","type":"subscribe","payload":{"query":LIVE_SUBSCRIPTION}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let bob_frame = next(&mut bob_socket, "consumer-0", "causal row").await;
        assert_ne!(
            first["extensions"]["distributed"]["cacheScope"],
            bob_frame["extensions"]["distributed"]["cacheScope"]
        );
        assert_eq!(
            (delivery.live_counts().0, engine.live_subscriber_count()),
            (2, 2),
            "identical roles/data must not cross subject scopes"
        );
        bob_socket
            .send(WsMessage::Text(
                json!({"id":"consumer-0","type":"complete"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let _ = bob_socket.close(None).await;
        project_item(&fixture.repository, &fixture.bus, 2, "committed-live").await;
        for (n, socket) in sockets.iter_mut().enumerate() {
            let frame = next(socket, &format!("consumer-{n}"), "committed-live").await;
            assert_eq!(
                frame["extensions"]["distributed"]["snapshot"]["indexes"][0]["position"],
                "2"
            );
        }
        let mut resumed = connect(&gateway.origin, &alice, true).await;
        resumed.send(WsMessage::Text(json!({"id":"legacy-resume","type":"start","payload":{"query":LIVE_SUBSCRIPTION,"extensions":{"distributed":{"resume":{"cursors":first["extensions"]["distributed"]["live"]["cursors"]}}}}}).to_string().into())).await.unwrap();
        let replay = next(&mut resumed, "legacy-resume", "committed-live").await;
        assert_eq!(replay["extensions"]["distributed"]["live"]["reset"], false);
        tokio::time::timeout(Duration::from_secs(5), async {
            while delivery.live_counts().0 != 1 || engine.live_subscriber_count() != 1 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("independent replay handed off and released its origin producer");
        assert!(delivery.live_counts().6 >= 1);
        for (n, socket) in sockets.iter_mut().enumerate() {
            socket
                .send(WsMessage::Text(
                    json!({"id":format!("consumer-{n}"),"type":"complete"})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            let _ = socket.close(None).await;
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while delivery.live_counts().1 != 1 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            engine.live_subscriber_count(),
            1,
            "remaining resumed client retains upstream ownership"
        );
        resumed
            .send(WsMessage::Text(
                json!({"id":"legacy-resume","type":"stop"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let _ = resumed.close(None).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while delivery.live_counts().0 != 0 || engine.live_subscriber_count() != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("last leave tears down gateway and origin live producers");
    }
}
