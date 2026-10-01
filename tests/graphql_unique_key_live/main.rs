#![cfg(all(feature = "graphql", feature = "sqlite"))]

use distributed::bus::{Bus, InMemoryBus, Message, MessageKind, RunOptions};
use distributed::graphql::{read, GraphqlEngine, ModelPermissions, SurfaceProjector};
use distributed::microsvc::{CausalProjectorContext, HandlerError, Routes, Service};
use distributed::{ReadModel, ReadModelCatalog, SqliteRepository};
use distributed_cli::{compile_client, ClientCompileInput, ClientDocument, ClientSurfaceSelector};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[readmodel(table = "revision_views", primary_key = ["id"])]
#[unique(columns = ["namespace", "revision"])]
struct RevisionView {
    id: String,
    namespace: String,
    revision: String,
    body: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[readmodel(table = "reference_views", primary_key = ["id"])]
struct ReferenceView {
    id: String,
    namespace: String,
    revision: String,
    #[readmodel(
        belongs_to = "RevisionView",
        foreign_key = "namespace,revision",
        references = "namespace,revision"
    )]
    target: Option<RevisionView>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct RevisionPublished {
    id: String,
    namespace: String,
    revision: String,
    body: String,
}

fn projector() -> SurfaceProjector {
    SurfaceProjector::new("publish_revision")
        .facts(["revision.published"])
        .models(["ReferenceView", "RevisionView"])
        .change_epoch("revision-test-v1")
}

async fn publish(repository: &SqliteRepository, bus: &InMemoryBus, revision: &str) {
    let routes = Routes::new()
        .with_read_model_store(repository.clone())
        .causal_projector::<RevisionPublished>(projector())
        .model::<ReferenceView>()
        .model::<RevisionView>()
        .handle(
            |context: CausalProjectorContext, fact: RevisionPublished| async move {
                context
                    .project(&RevisionView {
                        id: fact.id,
                        namespace: fact.namespace.clone(),
                        revision: fact.revision.clone(),
                        body: fact.body,
                    })
                    .await?;
                context
                    .project(&ReferenceView {
                        id: "reference-stable".into(),
                        namespace: fact.namespace,
                        revision: fact.revision,
                        target: None,
                    })
                    .await?;
                Ok::<(), HandlerError>(())
            },
        );
    bus.publish_message(
        Message::new(
            "revision.published",
            MessageKind::Event,
            serde_json::to_vec(&RevisionPublished {
                id: format!("opaque-{revision}"),
                namespace: "team-a".into(),
                revision: revision.into(),
                body: format!("Content {revision}"),
            })
            .unwrap(),
        )
        .with_id(format!("published-{revision}"))
        .with_metadata(
            distributed::trace_context::CAUSATION_ID,
            format!("publish-command-{revision}"),
        ),
    )
    .await
    .unwrap();
    Service::new()
        .named("unique-key-live")
        .routes(routes)
        .with_bus(bus.clone())
        .run(RunOptions::idempotent())
        .await
        .unwrap();
}

async fn fixture() -> (
    SqliteRepository,
    InMemoryBus,
    GraphqlEngine,
    distributed_cli::GeneratedClientProject,
) {
    let repository = SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .unwrap();
    let catalog = ReadModelCatalog::new("unique-key-live")
        .read_model::<ReferenceView>()
        .read_model::<RevisionView>();
    repository
        .bootstrap_table_schema_for_dev(&catalog.table_registry().unwrap())
        .await
        .unwrap();
    let bus = InMemoryBus::new();
    publish(&repository, &bus, "one").await;
    let engine = GraphqlEngine::builder(&repository)
        .service_id("unique-key-live")
        .protocol_token_key([0x37; 32])
        .roles(&["user"])
        .anonymous_role("user")
        .model::<ReferenceView>(ModelPermissions::new().grant("user", read().all_columns()))
        .model::<RevisionView>(ModelPermissions::new().grant("user", read().all_columns()))
        .client_projectors([projector()])
        .change_stream(repository.read_model_changes())
        .build()
        .unwrap();
    let generated = compile_client(ClientCompileInput::new(
        serde_json::to_value(engine.client_manifest_for_role("user").unwrap()).unwrap(),
        ClientSurfaceSelector::role("user"),
        vec![ClientDocument::new(
            "src/routes/references/+page.graphql",
            "query References @load @live { reference_views { id target { id body } } }",
        )],
    ))
    .unwrap();
    assert_eq!(generated.operations.len(), 1);
    assert!(generated.operations[0].live_operation_hash.is_some());
    (repository, bus, engine, generated)
}

#[tokio::test]
async fn projected_candidate_keys_generate_a_live_client() {
    let (repository, bus, engine, _) = fixture().await;
    let mut session = distributed::microsvc::Session::new();
    session.set(distributed::microsvc::ROLE_KEY, "user");
    let result = engine
        .execute(
            &session,
            async_graphql::Request::new("{ reference_views { id target { id body } } }"),
        )
        .await;
    assert!(result.errors.is_empty(), "{:?}", result.errors);
    assert_eq!(
        result.data.into_json().unwrap(),
        serde_json::json!({"reference_views": [
            {"id": "reference-stable", "target": {"id": "opaque-one", "body": "Content one"}}
        ]})
    );
    let mut live = Box::pin(engine.execute_stream(
        &session,
        async_graphql::Request::new("subscription { reference_views { id target { id body } } }"),
    ));
    let first = tokio::time::timeout(std::time::Duration::from_secs(3), live.next())
        .await
        .unwrap()
        .unwrap();
    assert!(first.errors.is_empty(), "{:?}", first.errors);
    publish(&repository, &bus, "two").await;
    let next = tokio::time::timeout(std::time::Duration::from_secs(3), live.next())
        .await
        .unwrap()
        .unwrap();
    assert!(next.errors.is_empty(), "{:?}", next.errors);
    assert_eq!(
        next.data.into_json().unwrap(),
        serde_json::json!({"reference_views": [
            {"id": "reference-stable", "target": {"id": "opaque-two", "body": "Content two"}}
        ]})
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires built JS and Playwright Chromium; explicitly run by browser CI"]
async fn projected_candidate_keys_reach_chromium_over_http_and_websocket() {
    use axum::{
        routing::{get, post},
        Json,
    };
    let (repository, bus, engine, generated) = fixture().await;
    let operation = &generated.operations[0];
    let module = generated
        .files
        .iter()
        .find(|file| file.path == operation.module_path)
        .unwrap()
        .contents
        .clone();
    let export_name = operation.export_name.clone();
    let app = distributed::microsvc::router(std::sync::Arc::new(
        Service::new()
            .named("unique-key-live")
            .try_with_graphql(engine)
            .unwrap(),
    ))
    .route(
        "/references",
        get(|| async { axum::response::Html("<output></output>") }),
    )
    .route(
        "/fixture",
        get(move || {
            let module = module.clone();
            let export_name = export_name.clone();
            async move { Json(serde_json::json!({"module": module, "exportName": export_name})) }
        }),
    )
    .route(
        "/publish",
        post(move || {
            let repository = repository.clone();
            let bus = bus.clone();
            async move {
                publish(&repository, &bus, "two").await;
                Json(serde_json::json!({"ok": true}))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let status = tokio::task::spawn_blocking(move || {
        std::process::Command::new("node")
            .arg("tests/e2e-ui/scripts/unique-key-connected.mjs")
            .arg(origin)
            .status()
    })
    .await
    .unwrap();
    server.abort();
    assert!(status.unwrap().success(), "connected browser proof failed");
}
