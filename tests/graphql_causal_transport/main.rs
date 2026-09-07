#![cfg(all(feature = "graphql", feature = "sqlite"))]

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use distributed::command::{typed_command, PreparedCommand, Succeeded};
use distributed::graphql::{GraphqlEngine, IdentityConfig, OidcConfig};
use distributed::microsvc::{CausalCommandContext, HandlerError, Routes, Service};
use distributed::{
    Aggregate, AggregateRepository, CommandInput, CommandOutput, Entity, EventRecord,
    InMemoryRepository,
};
use futures_util::{SinkExt, StreamExt};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rand::thread_rng;
use rsa::pkcs1::EncodeRsaPrivateKey;
use rsa::traits::PublicKeyParts;
use rsa::{RsaPrivateKey, RsaPublicKey};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

const SERVICE_ID: &str = "causal-transport";
const ISSUER: &str = "https://issuer.causal-transport.example";
const AUDIENCE: &str = SERVICE_ID;
const TEST_PROTOCOL_TOKEN_KEY: [u8; 32] = [0x5a; 32];
const TARGET_COMMAND_ID: &str = "0190a000-0000-7000-8000-000000000101";
const GUESSED_COMMAND_ID: &str = "0190a000-0000-7000-8000-000000000102";
const EXPIRED_COMMAND_ID: &str = "0190a000-0000-7000-8000-000000000103";
const MUTATION: &str = r#"
mutation CausalTransportMutation($commandId: ID!) {
  todo_create(commandId: $commandId, input: { id: "todo-1" }) {
    id
  }
}
"#;
const STATUS_QUERY: &str = r#"
query CausalTransportStatus($commandId: ID!) {
  commandStatus(commandId: $commandId) {
    state
  }
}
"#;
const RAW_QUERY: &str = "query RawQuery { __typename }";
const AMBIGUOUS_QUERY: &str = "query First { __typename } query Second { __typename }";

#[derive(Default)]
struct TransportAggregate {
    entity: Entity,
}

impl Aggregate for TransportAggregate {
    type ReplayError = String;

    fn aggregate_type() -> &'static str {
        "causal-transport-fixture"
    }

    fn entity(&self) -> &Entity {
        &self.entity
    }

    fn entity_mut(&mut self) -> &mut Entity {
        &mut self.entity
    }

    fn replay_event(&mut self, _event: &EventRecord) -> Result<(), Self::ReplayError> {
        Ok(())
    }
}

#[derive(Deserialize, CommandInput)]
struct TransportCommandInput {
    id: String,
}

#[derive(Serialize, CommandOutput)]
struct TransportCommandOutput {
    id: String,
}

async fn accept_command(
    _context: &CausalCommandContext<'_, TransportAggregate>,
    input: TransportCommandInput,
) -> Result<PreparedCommand<Succeeded<TransportCommandOutput>>, HandlerError> {
    Ok(
        PreparedCommand::<Succeeded<TransportCommandOutput>>::prepare(TransportCommandOutput {
            id: input.id,
        })
        .expect("the fixture output is JSON-serializable"),
    )
}

fn causal_service() -> Service {
    let routes: Routes<AggregateRepository<InMemoryRepository, TransportAggregate>> = Routes::new()
        .with_repo(AggregateRepository::new(InMemoryRepository::new()))
        .typed_command(
            typed_command::<TransportCommandInput, Succeeded<TransportCommandOutput>>(
                "todo.create",
            )
            .roles(["writer"]),
        )
        .handle(accept_command)
        // Keep commandStatus present on the downgraded role's schema while
        // withholding the target command's current grant.
        .typed_command(
            typed_command::<TransportCommandInput, Succeeded<TransportCommandOutput>>(
                "reader.ping",
            )
            .roles(["reader"]),
        )
        .handle(accept_command);
    Service::new().named(SERVICE_ID).routes(routes)
}

struct TestKeys {
    encoding: EncodingKey,
    jwks: String,
    kid: String,
}

impl TestKeys {
    fn new() -> Self {
        let private = RsaPrivateKey::new(&mut thread_rng(), 2048).expect("test RSA key");
        let public = RsaPublicKey::from(&private);
        let pem = private
            .to_pkcs1_pem(rsa::pkcs8::LineEnding::LF)
            .expect("test RSA PEM");
        let encoding = EncodingKey::from_rsa_pem(pem.as_bytes()).expect("test RSA encoding key");
        let kid = "causal-transport-test-key".to_string();
        let modulus =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(public.n().to_bytes_be());
        let exponent =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(public.e().to_bytes_be());
        let jwks = json!({
            "keys": [{
                "kty": "RSA",
                "kid": kid,
                "alg": "RS256",
                "use": "sig",
                "n": modulus,
                "e": exponent
            }]
        })
        .to_string();
        Self {
            encoding,
            jwks,
            kid,
        }
    }

    fn token(&self, subject: &str, role: &str) -> String {
        self.token_for_tenant(subject, role, "tenant-a")
    }

    fn token_for_tenant(&self, subject: &str, role: &str, tenant: &str) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_secs();
        let claims = json!({
            "iss": ISSUER,
            "aud": AUDIENCE,
            "sub": subject,
            "iat": now.saturating_sub(1),
            "nbf": now.saturating_sub(1),
            "exp": now + 3600,
            "roles": [role],
            "tenant": tenant
        });
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(self.kid.clone());
        encode(&header, &claims, &self.encoding).expect("sign test access token")
    }
}

struct TestServer {
    http_url: String,
    ws_url: String,
    task: JoinHandle<()>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn spawn_server(service: Arc<Service>) -> TestServer {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let address = listener.local_addr().expect("test server address");
    let app = distributed::microsvc::router(service);
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("causal transport server");
    });
    TestServer {
        http_url: format!("http://{address}/graphql"),
        ws_url: format!("ws://{address}/graphql/ws"),
        task,
    }
}

async fn http_graphql(
    server: &TestServer,
    bearer: Option<&str>,
    query: &str,
    variables: Value,
) -> Value {
    let client = reqwest::Client::new();
    let mut request = client
        .post(&server.http_url)
        .json(&json!({ "query": query, "variables": variables }));
    if let Some(token) = bearer {
        request = request.bearer_auth(token);
    }
    let response = request.send().await.expect("HTTP GraphQL response");
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "GraphQL HTTP status"
    );
    response.json().await.expect("GraphQL HTTP JSON")
}

type TestWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn next_ws_json(socket: &mut TestWebSocket) -> Value {
    loop {
        match socket
            .next()
            .await
            .expect("GraphQL WS frame")
            .expect("valid GraphQL WS frame")
        {
            Message::Text(text) => {
                return serde_json::from_str(text.as_ref()).expect("GraphQL WS JSON");
            }
            Message::Ping(payload) => {
                socket
                    .send(Message::Pong(payload))
                    .await
                    .expect("GraphQL WS pong");
            }
            Message::Close(frame) => panic!("GraphQL WS closed early: {frame:?}"),
            _ => {}
        }
    }
}

async fn ws_graphql(
    server: &TestServer,
    connection_init: Value,
    query: &str,
    variables: Value,
) -> Value {
    let mut request = server
        .ws_url
        .as_str()
        .into_client_request()
        .expect("GraphQL WS request");
    request.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        "graphql-transport-ws"
            .parse()
            .expect("GraphQL WS protocol header"),
    );
    let (mut socket, response) = tokio_tungstenite::connect_async(request)
        .await
        .expect("GraphQL WS connect");
    assert_eq!(
        response
            .headers()
            .get(SEC_WEBSOCKET_PROTOCOL)
            .and_then(|value| value.to_str().ok()),
        Some("graphql-transport-ws")
    );

    socket
        .send(Message::Text(
            json!({ "type": "connection_init", "payload": connection_init })
                .to_string()
                .into(),
        ))
        .await
        .expect("GraphQL WS connection_init");
    let acknowledgement = next_ws_json(&mut socket).await;
    assert_eq!(
        acknowledgement,
        json!({ "type": "connection_ack" }),
        "GraphQL WS acknowledgement"
    );

    socket
        .send(Message::Text(
            json!({
                "id": "operation-1",
                "type": "subscribe",
                "payload": { "query": query, "variables": variables }
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("GraphQL WS subscribe");
    let next = next_ws_json(&mut socket).await;
    assert_eq!(next["id"], "operation-1");
    assert_eq!(next["type"], "next", "GraphQL WS operation result: {next}");
    let payload = next["payload"].clone();
    let complete = next_ws_json(&mut socket).await;
    assert_eq!(
        complete,
        json!({ "id": "operation-1", "type": "complete" }),
        "GraphQL WS completion"
    );
    socket.close(None).await.expect("close GraphQL WS");
    payload
}

fn bearer_init(token: &str) -> Value {
    json!({ "authorization": format!("Bearer {token}") })
}

fn distributed_envelope(response: &Value) -> &Value {
    response
        .get("extensions")
        .and_then(|extensions| extensions.get("distributed"))
        .unwrap_or_else(|| panic!("missing extensions.distributed: {response}"))
}

fn assert_unknown_status(response: &Value, hidden_command_id: &str) {
    assert_eq!(
        response["data"],
        json!({ "commandStatus": { "state": "unknown" } })
    );
    assert!(
        distributed_envelope(response).get("command").is_none(),
        "unknown status must not fabricate or disclose receipt metadata: {response}"
    );
    assert!(
        !response.to_string().contains(hidden_command_id),
        "non-enumerating status echoed a command ID: {response}"
    );
}

async fn assert_http_ws_status_pair(
    server: &TestServer,
    token: &str,
    command_id: &str,
) -> (Value, Value) {
    let variables = json!({ "commandId": command_id });
    let http = http_graphql(server, Some(token), STATUS_QUERY, variables.clone()).await;
    let websocket = ws_graphql(server, bearer_init(token), STATUS_QUERY, variables).await;
    assert_eq!(
        distributed_envelope(&http),
        distributed_envelope(&websocket),
        "HTTP and GraphQL-WS must serialize one canonical distributed envelope"
    );
    assert_eq!(http["data"], websocket["data"]);
    (http, websocket)
}

#[tokio::test]
async fn causal_receipt_status_replay_and_nonenumeration_match_http_and_ws() {
    let keys = TestKeys::new();
    let mut oidc = OidcConfig::new(ISSUER, AUDIENCE)
        .with_static_jwks(keys.jwks.clone())
        .principal_tenant_claims(["tenant"])
        .engine_roles(&["writer", "reader"]);
    oidc.require_role = true;

    let service = causal_service();
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .connect_lazy("sqlite::memory:")
        .expect("GraphQL SQLite pool");
    let engine = GraphqlEngine::builder(pool)
        .protocol_token_key(TEST_PROTOCOL_TOKEN_KEY)
        .roles(&["writer", "reader"])
        .identity(IdentityConfig::oidc_bearer(oidc))
        .service(&service)
        .build()
        .expect("causal transport GraphQL engine");
    let service = Arc::new(
        service
            .try_with_graphql(engine)
            .expect("causal transport GraphQL attachment"),
    );
    let server = spawn_server(service).await;

    let writer_a = keys.token("subject-a", "writer");
    let writer_b = keys.token("subject-b", "writer");
    let writer_a_other_tenant = keys.token_for_tenant("subject-a", "writer", "tenant-b");
    let reader_a = keys.token("subject-a", "reader");
    let mutation_variables = json!({ "commandId": TARGET_COMMAND_ID });

    // HTTP accepts the command; GraphQL-WS repeats the exact same command ID
    // and input. The durable replay must yield byte-for-byte equivalent public
    // data and receipt metadata on both transports.
    let http_mutation = http_graphql(
        &server,
        Some(&writer_a),
        MUTATION,
        mutation_variables.clone(),
    )
    .await;
    let ws_replay = ws_graphql(
        &server,
        bearer_init(&writer_a),
        MUTATION,
        mutation_variables,
    )
    .await;
    assert_eq!(
        http_mutation["data"],
        json!({ "todo_create": { "id": "todo-1" } }),
        "domain data must not contain receipt metadata"
    );
    assert_eq!(
        http_mutation["data"], ws_replay["data"],
        "GraphQL-WS replay response: {ws_replay}"
    );
    assert_eq!(
        distributed_envelope(&http_mutation),
        distributed_envelope(&ws_replay),
        "same-ID replay must retain the exact transport-independent envelope"
    );
    let receipt = &distributed_envelope(&http_mutation)["command"];
    assert_eq!(receipt["commandId"], TARGET_COMMAND_ID);
    assert_eq!(receipt["state"], "succeeded");
    assert_eq!(receipt["consistency"], "succeeded");
    assert_eq!(receipt["expects"], json!([]));
    assert!(
        receipt["causationId"]
            .as_str()
            .is_some_and(|id| !id.is_empty()),
        "receipt must include its durable causation identity: {receipt}"
    );

    // The current principal and current writer grant see the same sanitized
    // status and receipt metadata through HTTP and GraphQL-WS.
    let (http_status, ws_status) =
        assert_http_ws_status_pair(&server, &writer_a, TARGET_COMMAND_ID).await;
    assert_eq!(
        http_status["data"],
        json!({ "commandStatus": { "state": "succeeded" } }),
        "status data stays intentionally minimal"
    );
    assert_eq!(http_status["data"], ws_status["data"]);
    assert_eq!(
        distributed_envelope(&http_status)["command"],
        distributed_envelope(&http_mutation)["command"],
        "status must expose the same durable receipt, not reconstructed state"
    );

    // A guessed ID, a different verified subject, and the same subject after a
    // role downgrade all collapse to the identical public `unknown` shape.
    let (guessed_http, guessed_ws) =
        assert_http_ws_status_pair(&server, &writer_a, GUESSED_COMMAND_ID).await;
    assert_unknown_status(&guessed_http, GUESSED_COMMAND_ID);
    assert_unknown_status(&guessed_ws, GUESSED_COMMAND_ID);

    let (wrong_principal_http, wrong_principal_ws) =
        assert_http_ws_status_pair(&server, &writer_b, TARGET_COMMAND_ID).await;
    assert_unknown_status(&wrong_principal_http, TARGET_COMMAND_ID);
    assert_unknown_status(&wrong_principal_ws, TARGET_COMMAND_ID);

    let (wrong_tenant_http, wrong_tenant_ws) =
        assert_http_ws_status_pair(&server, &writer_a_other_tenant, TARGET_COMMAND_ID).await;
    assert_unknown_status(&wrong_tenant_http, TARGET_COMMAND_ID);
    assert_unknown_status(&wrong_tenant_ws, TARGET_COMMAND_ID);

    let (downgraded_http, downgraded_ws) =
        assert_http_ws_status_pair(&server, &reader_a, TARGET_COMMAND_ID).await;
    assert_unknown_status(&downgraded_http, TARGET_COMMAND_ID);
    assert_unknown_status(&downgraded_ws, TARGET_COMMAND_ID);
}

#[tokio::test]
async fn expired_command_status_is_typed_and_transport_independent() {
    let keys = TestKeys::new();
    let mut oidc = OidcConfig::new(ISSUER, AUDIENCE)
        .with_static_jwks(keys.jwks.clone())
        .principal_tenant_claims(["tenant"])
        .engine_roles(&["writer", "reader"]);
    oidc.require_role = true;

    let service = causal_service()
        .causal_command_timing(Duration::from_millis(25), Duration::from_millis(100));
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .connect_lazy("sqlite::memory:")
        .expect("GraphQL SQLite pool");
    let engine = GraphqlEngine::builder(pool)
        .protocol_token_key(TEST_PROTOCOL_TOKEN_KEY)
        .roles(&["writer", "reader"])
        .identity(IdentityConfig::oidc_bearer(oidc))
        .service(&service)
        .build()
        .expect("expiring causal transport GraphQL engine");
    let service = Arc::new(
        service
            .try_with_graphql(engine)
            .expect("expiring causal transport GraphQL attachment"),
    );
    let server = spawn_server(service).await;
    let writer = keys.token("expiring-subject", "writer");

    let accepted = http_graphql(
        &server,
        Some(&writer),
        MUTATION,
        json!({ "commandId": EXPIRED_COMMAND_ID }),
    )
    .await;
    assert_eq!(
        distributed_envelope(&accepted)["command"]["state"],
        "succeeded"
    );

    tokio::time::sleep(Duration::from_millis(150)).await;
    let (http, websocket) = assert_http_ws_status_pair(&server, &writer, EXPIRED_COMMAND_ID).await;
    assert_eq!(
        http["data"],
        json!({ "commandStatus": { "state": "expired" } })
    );
    assert_eq!(http["data"], websocket["data"]);
    assert!(
        distributed_envelope(&http).get("command").is_none(),
        "expired status must not revive stale receipt evidence: {http}"
    );
}

#[tokio::test]
async fn query_only_keyless_service_remains_envelope_free_over_http_and_ws() {
    let service = Service::new().named("raw-query-only");
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .connect_lazy("sqlite::memory:")
        .expect("GraphQL SQLite pool");
    let engine = GraphqlEngine::builder(pool)
        .roles(&["user"])
        .service(&service)
        .build()
        .expect("keyless query-only engine");
    let service = Arc::new(
        service
            .try_with_graphql(engine)
            .expect("keyless query-only attachment"),
    );
    let server = spawn_server(service).await;

    let http = http_graphql(&server, None, RAW_QUERY, json!({})).await;
    let websocket = ws_graphql(&server, json!({}), RAW_QUERY, json!({})).await;
    assert_eq!(http["data"], json!({ "__typename": "Query" }));
    assert_eq!(http, websocket);
    assert!(
        http.get("extensions")
            .and_then(|extensions| extensions.get("distributed"))
            .is_none(),
        "keyless query-only fallback must not fabricate protocol evidence: {http}"
    );

    let ambiguous = ws_graphql(&server, json!({}), AMBIGUOUS_QUERY, json!({})).await;
    assert_eq!(ambiguous["data"], Value::Null);
    assert_eq!(
        ambiguous["errors"][0]["message"],
        "GraphQL operation name is required for multi-operation documents"
    );
}
