//! Always-on identity suite: fixtures F1–F10 against shipped identity path.
//!
//! No network / no Zitadel. Synthetic RSA JWKS + JWT via jsonwebtoken.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::{HeaderMap, HeaderValue};
use base64::Engine;
use distributed::graphql::http::{graphql_router, graphql_router_composed};
use distributed::graphql::{
    extract_bearer, map_claims_to_session, read, AuthError, ClaimMapConfig, GraphqlEngine,
    IdentityConfig, IdentityResolver, ModelPermissions, OidcConfig, OidcValidator, WebsocketLimits,
};
use distributed::microsvc::Session;
use distributed::read_model::ReadModel;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rsa::pkcs1::EncodeRsaPrivateKey;
use rsa::traits::PublicKeyParts;
use rsa::{RsaPrivateKey, RsaPublicKey};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tower::util::ServiceExt;

// ── RSA fixture helpers ─────────────────────────────────────────────────────

struct TestKeys {
    encoding: EncodingKey,
    jwks_json: String,
    #[allow(dead_code)]
    kid: String,
}

fn b64url(data: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data)
}

fn mint_keys() -> TestKeys {
    mint_keys_with_kid("test-kid-1")
}

fn mint_keys_with_kid(kid: &str) -> TestKeys {
    let mut rng = rand::thread_rng();
    let private = RsaPrivateKey::new(&mut rng, 2048).expect("rsa key");
    let public = RsaPublicKey::from(&private);
    let pem = private.to_pkcs1_pem(rsa::pkcs8::LineEnding::LF).unwrap();
    let encoding = EncodingKey::from_rsa_pem(pem.as_bytes()).unwrap();
    let kid = kid.to_string();
    let n = b64url(&public.n().to_bytes_be());
    let e = b64url(&public.e().to_bytes_be());
    let jwks_json = json!({
        "keys": [{
            "kty": "RSA",
            "kid": kid,
            "alg": "RS256",
            "use": "sig",
            "n": n,
            "e": e
        }]
    })
    .to_string();
    TestKeys {
        encoding,
        jwks_json,
        kid,
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn sign_claims(keys: &TestKeys, claims: Value) -> String {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(keys.kid.clone());
    // jsonwebtoken wants a serializable claims struct — use Map via Value
    encode(&header, &claims, &keys.encoding).expect("sign")
}

fn oidc_cfg(keys: &TestKeys) -> OidcConfig {
    OidcConfig::new("http://localhost:8080", "graphql-api")
        .with_static_jwks(&keys.jwks_json)
        .engine_roles(&["admin", "customer", "user"])
}

const ES256_PRIVATE_KEY: &str = r#"-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgWTFfCGljY6aw3Hrt
kHmPRiazukxPLb6ilpRAewjW8nihRANCAATDskChT+Altkm9X7MI69T3IUmrQU0L
950IxEzvw/x5BMEINRMrXLBJhqzO9Bm+d6JbqA21YQmd1Kt4RzLJR1W+
-----END PRIVATE KEY-----"#;

const ES256_X: &str = "w7JAoU_gJbZJvV-zCOvU9yFJq0FNC_edCMRM78P8eQQ";
const ES256_Y: &str = "wQg1EytcsEmGrM70Gb53oluoDbVhCZ3Uq3hHMslHVb4";

/// Resolve one request as a service would. Every config here uses static
/// JWKS, so nothing touches the network.
fn resolve_session_sync(
    headers: &HeaderMap,
    config: &IdentityConfig,
) -> Result<Session, AuthError> {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(IdentityResolver::new(config.clone()).resolve_session(headers))
}

fn headers_from(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut h = HeaderMap::new();
    for (k, v) in pairs {
        h.insert(
            axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
            HeaderValue::from_str(v).unwrap(),
        );
    }
    h
}

fn bearer_headers(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).expect("bearer header"),
    );
    headers
}

// ── F1 / F2 claim map (shipped map_claims_to_session) ───────────────────────

#[test]
fn f1_zitadel_project_roles_session() {
    let claims = json!({
        "iss": "http://localhost:8080",
        "aud": ["graphql-api", "123@graphql"],
        "sub": "user-a-001",
        "exp": 4102444800_i64,
        "urn:zitadel:iam:org:project:roles": {
            "customer": { "280664559058878577": "zitadel.localhost" },
            "admin": { "280664559058878577": "zitadel.localhost" }
        }
    });
    let cfg = ClaimMapConfig {
        engine_roles: vec!["admin".into(), "customer".into(), "user".into()],
        ..Default::default()
    };
    let session = map_claims_to_session(&claims, &cfg).unwrap();
    assert_eq!(session.user_id(), Some("user-a-001"));
    assert_eq!(session.get("x-roles"), Some("admin,customer"));
    assert_eq!(session.role(), None);
}

#[test]
fn f2_groups_array_session() {
    let claims = json!({
        "iss": "http://localhost:8080",
        "aud": "graphql-api",
        "sub": "user-b-002",
        "exp": 4102444800_i64,
        "groups": ["customer", "other-unmapped"]
    });
    let cfg = ClaimMapConfig {
        engine_roles: vec!["admin".into(), "customer".into(), "user".into()],
        ..Default::default()
    };
    let session = map_claims_to_session(&claims, &cfg).unwrap();
    assert_eq!(session.user_id(), Some("user-b-002"));
    assert_eq!(session.get("x-roles"), Some("customer"));
    assert_eq!(session.roles(), vec!["customer"]);
    assert_eq!(session.role(), Some("customer"));
}

#[test]
fn require_role_rejects_signed_token_without_role_claims() {
    let keys = mint_keys();
    let token = sign_claims(
        &keys,
        json!({
            "iss": "http://localhost:8080",
            "aud": "graphql-api",
            "sub": "user-without-roles",
            "exp": now() + 3600,
            "iat": now()
        }),
    );
    let mut cfg = oidc_cfg(&keys);
    cfg.require_role = true;

    let error = OidcValidator::new(cfg)
        .validate_and_map(&token)
        .expect_err("strict role mode must reject the user fallback");

    assert_eq!(error.to_string(), "require_role: no asserted engine role");
}

#[test]
fn require_role_rejects_signed_token_with_only_nonmatching_roles() {
    let keys = mint_keys();
    let token = sign_claims(
        &keys,
        json!({
            "iss": "http://localhost:8080",
            "aud": "graphql-api",
            "sub": "user-with-nonmatching-role",
            "exp": now() + 3600,
            "iat": now(),
            "groups": ["external-role"]
        }),
    );
    let mut cfg = oidc_cfg(&keys);
    cfg.require_role = true;

    let error = OidcValidator::new(cfg)
        .validate_and_map(&token)
        .expect_err("strict role mode must reject non-allowlisted claims");

    assert_eq!(error.to_string(), "require_role: no asserted engine role");
}

// ── F3–F5 JWT validation rejects ────────────────────────────────────────────

#[test]
fn f3_alg_none_rejected() {
    let keys = mint_keys();
    let validator = OidcValidator::new(oidc_cfg(&keys));
    // Compact JWT with alg=none (unsigned)
    let header = b64url(br#"{"alg":"none","typ":"JWT"}"#);
    let payload = b64url(
        br#"{"iss":"http://localhost:8080","aud":"graphql-api","sub":"x","exp":4102444800}"#,
    );
    let token = format!("{header}.{payload}.");
    let err = validator.validate_token(&token).unwrap_err();
    assert!(
        matches!(
            err,
            distributed::graphql::ValidationError::AlgNone
                | distributed::graphql::ValidationError::AlgNotAllowed
                | distributed::graphql::ValidationError::Malformed
                | distributed::graphql::ValidationError::Signature
        ),
        "expected alg reject, got {err:?}"
    );
}

#[test]
fn f4_wrong_aud_rejected() {
    let keys = mint_keys();
    let claims = json!({
        "iss": "http://localhost:8080",
        "aud": "other-api",
        "sub": "user-a-001",
        "exp": now() + 3600,
        "iat": now(),
        "groups": ["customer"]
    });
    let token = sign_claims(&keys, claims);
    let err = OidcValidator::new(oidc_cfg(&keys))
        .validate_token(&token)
        .unwrap_err();
    assert!(
        matches!(err, distributed::graphql::ValidationError::Audience)
            || err.to_string().contains("aud"),
        "got {err:?}"
    );
}

#[test]
fn f5_expired_rejected() {
    let keys = mint_keys();
    let claims = json!({
        "iss": "http://localhost:8080",
        "aud": "graphql-api",
        "sub": "user-a-001",
        "exp": 1_000_000_000_i64,
        "iat": 999_999_000_i64,
        "groups": ["customer"]
    });
    let token = sign_claims(&keys, claims);
    let err = OidcValidator::new(oidc_cfg(&keys))
        .validate_token(&token)
        .unwrap_err();
    assert!(
        matches!(err, distributed::graphql::ValidationError::Expired),
        "got {err:?}"
    );
}

#[test]
fn es256_jwks_key_validates_token_when_allowed_by_default() {
    let kid = "ec-test-kid-1";
    let jwks_json = json!({
        "keys": [{
            "kty": "EC",
            "kid": kid,
            "alg": "ES256",
            "use": "sig",
            "crv": "P-256",
            "x": ES256_X,
            "y": ES256_Y
        }]
    })
    .to_string();
    let cfg = OidcConfig::new("http://localhost:8080", "graphql-api")
        .with_static_jwks(jwks_json)
        .engine_roles(&["admin", "customer", "user"]);
    let claims = json!({
        "iss": "http://localhost:8080",
        "aud": "graphql-api",
        "sub": "user-ec-001",
        "exp": now() + 3600,
        "iat": now(),
        "groups": ["customer"]
    });
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(kid.to_string());
    let token = encode(
        &header,
        &claims,
        &EncodingKey::from_ec_pem(ES256_PRIVATE_KEY.as_bytes()).unwrap(),
    )
    .unwrap();

    let session = OidcValidator::new(cfg).validate_and_map(&token).unwrap();
    assert_eq!(session.user_id(), Some("user-ec-001"));
    assert!(
        session
            .get("x-roles")
            .map(|r| r.split(",").any(|p| p == "customer"))
            .unwrap_or(false),
        "expected role customer in x-roles: {:?}",
        session.get("x-roles")
    );
}

// ── F1 success via full validate_and_map + spoof headers ignored ────────────

#[test]
fn f1_valid_jwt_maps_session_spoof_headers_ignored() {
    let keys = mint_keys();
    let claims = json!({
        "iss": "http://localhost:8080",
        "aud": "graphql-api",
        "sub": "user-a-001",
        "exp": now() + 3600,
        "iat": now(),
        "urn:zitadel:iam:org:project:roles": {
            "customer": { "1": "x" },
            "admin": { "1": "x" }
        }
    });
    let token = sign_claims(&keys, claims);
    let cfg = IdentityConfig::oidc_bearer(oidc_cfg(&keys));
    let headers = headers_from(&[
        ("authorization", &format!("Bearer {token}")),
        ("x-user-id", "evil"),
        ("x-roles", "admin"),
    ]);
    let session = resolve_session_sync(&headers, &cfg).unwrap();
    assert_eq!(session.user_id(), Some("user-a-001"));
    assert!(
        session
            .get("x-roles")
            .map(|r| r.split(",").any(|p| p == "admin"))
            .unwrap_or(false),
        "expected role admin in x-roles: {:?}",
        session.get("x-roles")
    );
    // Client spoof must not replace sub
    assert_ne!(session.user_id(), Some("evil"));
}

// ── Gateway: headers are believed only with the shared secret ───────────────

#[test]
fn f6_gateway_with_secret_trusts_injected_headers() {
    let cfg = IdentityConfig::gateway("x-gateway-secret", "s3cret").unwrap();
    let headers = headers_from(&[
        ("x-gateway-secret", "s3cret"),
        ("x-user-id", "gateway-user-9"),
        ("x-roles", "customer"),
        ("cookie", "sid=1"),
    ]);
    let session = resolve_session_sync(&headers, &cfg).unwrap();
    assert_eq!(session.user_id(), Some("gateway-user-9"));
    assert_eq!(session.roles(), vec!["customer"]);
    for credential in ["x-gateway-secret", "cookie", "authorization"] {
        assert!(session.get(credential).is_none(), "{credential} leaked");
    }
}

#[test]
fn f6_gateway_without_secret_is_unauthorized() {
    let cfg = IdentityConfig::gateway("x-gateway-secret", "s3cret").unwrap();
    let headers = headers_from(&[("x-user-id", "attacker"), ("x-roles", "admin")]);
    assert_eq!(
        resolve_session_sync(&headers, &cfg).unwrap_err(),
        AuthError::Unauthorized
    );
}

// ── F7 invalid Bearer → 401, client headers never a fallback ────────────────

#[test]
fn f7_invalid_bearer_does_not_fall_back_to_client_headers() {
    let keys = mint_keys();
    let mut oidc = oidc_cfg(&keys);
    oidc.require_auth = false;
    let cfg = IdentityConfig::oidc_bearer(oidc);
    let header = b64url(br#"{"alg":"none"}"#);
    let payload = b64url(br#"{}"#);
    let bad = format!("{header}.{payload}.");
    let headers = headers_from(&[
        ("authorization", &format!("Bearer {bad}")),
        ("x-user-id", "gateway-user-9"),
        ("x-roles", "admin"),
    ]);
    let err = resolve_session_sync(&headers, &cfg).unwrap_err();
    assert_eq!(err, AuthError::Unauthorized);
}

// ── F8 / F9 require_auth ────────────────────────────────────────────────────

#[test]
fn f8_oidc_missing_require_auth_unauthorized() {
    let keys = mint_keys();
    let cfg = IdentityConfig::oidc_bearer(oidc_cfg(&keys)); // require_auth true
    let headers = HeaderMap::new();
    assert_eq!(
        resolve_session_sync(&headers, &cfg).unwrap_err(),
        AuthError::Unauthorized
    );
}

#[test]
fn f9_oidc_missing_require_auth_false_anonymous() {
    let keys = mint_keys();
    let mut oidc = oidc_cfg(&keys);
    oidc.require_auth = false;
    let cfg = IdentityConfig::oidc_bearer(oidc);
    let headers = HeaderMap::new();
    let session = resolve_session_sync(&headers, &cfg).unwrap();
    assert!(session.user_id().is_none());
    assert!(session.role().is_none());
}

// ── F10 client identity headers are never believed outside DevHeaders ───────

#[test]
fn f10_anonymous_oidc_ignores_client_identity_headers() {
    let keys = mint_keys();
    let mut oidc = oidc_cfg(&keys);
    oidc.require_auth = false;
    let cfg = IdentityConfig::oidc_bearer(oidc);
    let headers = headers_from(&[
        ("x-user-id", "attacker"),
        ("x-roles", "admin"),
        ("x-request-id", "req-1"),
    ]);
    let session = resolve_session_sync(&headers, &cfg).unwrap();
    assert!(session.user_id().is_none());
    assert!(session.roles().is_empty());
}

#[test]
fn extract_bearer_empty_is_invalid() {
    let headers = headers_from(&[("authorization", "Bearer ")]);
    assert_eq!(
        extract_bearer(&headers).unwrap_err(),
        AuthError::Unauthorized
    );
}

// ── HTTP 401 on real GraphQL router (OidcBearer) ─────────────────────────────

use common::graphql as graphql_support;
use graphql_support::Db;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("id_items")]
struct IdItem {
    #[id("id")]
    id: String,
    owner: String,
}

async fn engine_with_identity(identity: IdentityConfig) -> Arc<GraphqlEngine> {
    engine_with(identity, WebsocketLimits::default()).await
}

async fn engine_with(identity: IdentityConfig, limits: WebsocketLimits) -> Arc<GraphqlEngine> {
    let db = Db::sqlite().await;
    db.seed(
        [("1", "user-a-001"), ("2", "other")].map(|(id, owner)| IdItem {
            id: id.into(),
            owner: owner.into(),
        }),
    )
    .await;
    let pool = db.pool();
    let engine = GraphqlEngine::builder(pool)
        .roles(&["customer", "admin", "user"])
        .model::<IdItem>(
            ModelPermissions::new()
                .grant(
                    "customer",
                    read().all_columns().rows(
                        distributed::graphql::col("owner")
                            .eq(distributed::graphql::claim("x-user-id")),
                    ),
                )
                .grant("admin", read().all_columns())
                .grant("user", read().all_columns()),
        )
        .identity(identity)
        .websocket_limits(limits)
        .change_stream(db.read_model_changes())
        .graphiql(false)
        .build()
        .unwrap();
    Arc::new(engine)
}

#[derive(Clone)]
struct RotatingJwks {
    body: Arc<RwLock<String>>,
    fetches: Arc<AtomicUsize>,
}

/// Serves the current JWKS body; an empty body stands for an issuer outage.
async fn serve_rotating_jwks(
    axum::extract::State(state): axum::extract::State<RotatingJwks>,
) -> Result<String, axum::http::StatusCode> {
    state.fetches.fetch_add(1, Ordering::SeqCst);
    let body = state.body.read().expect("read JWKS response").clone();
    if body.is_empty() {
        Err(axum::http::StatusCode::SERVICE_UNAVAILABLE)
    } else {
        Ok(body)
    }
}

async fn authenticated_graphql_status(app: axum::Router, token: &str) -> axum::http::StatusCode {
    app.oneshot(
        axum::http::Request::builder()
            .method("POST")
            .uri("/graphql")
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {token}"))
            .body(axum::body::Body::from(r#"{"query":"{ __typename }"}"#))
            .unwrap(),
    )
    .await
    .unwrap()
    .status()
}

#[tokio::test]
async fn engine_reuses_expires_and_singleflights_rotated_jwks() {
    let first_keys = mint_keys_with_kid("rotation-kid-1");
    let rotated_keys = mint_keys_with_kid("rotation-kid-2");
    let jwks = RotatingJwks {
        body: Arc::new(RwLock::new(first_keys.jwks_json.clone())),
        fetches: Arc::new(AtomicUsize::new(0)),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind JWKS server");
    let address = listener.local_addr().expect("JWKS server address");
    let jwks_server = axum::Router::new()
        .route("/jwks", axum::routing::get(serve_rotating_jwks))
        .with_state(jwks.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, jwks_server)
            .await
            .expect("serve JWKS");
    });

    let claims = |subject: &str| {
        json!({
            "iss": "http://localhost:8080",
            "aud": "graphql-api",
            "sub": subject,
            "exp": now() + 3600,
            "iat": now(),
            "groups": ["customer"]
        })
    };
    let first_token = sign_claims(&first_keys, claims("first-key-user"));
    let rotated_token = sign_claims(&rotated_keys, claims("rotated-key-user"));
    let mut oidc =
        OidcConfig::new("http://localhost:8080", "graphql-api").engine_roles(&["customer"]);
    oidc.jwks_uri = Some(format!("http://{address}/jwks"));
    let engine = engine_with_identity(IdentityConfig::oidc_bearer(oidc)).await;
    let app = graphql_router(engine);

    assert_eq!(
        authenticated_graphql_status(app.clone(), &first_token).await,
        axum::http::StatusCode::OK
    );
    assert_eq!(
        authenticated_graphql_status(app.clone(), &first_token).await,
        axum::http::StatusCode::OK
    );
    assert_eq!(
        jwks.fetches.load(Ordering::SeqCst),
        1,
        "repeated engine requests must reuse the JWKS cache"
    );

    *jwks.body.write().expect("rotate JWKS response") = rotated_keys.jwks_json.clone();
    let (left, right) = tokio::join!(
        authenticated_graphql_status(app.clone(), &rotated_token),
        authenticated_graphql_status(app.clone(), &rotated_token),
    );
    assert_eq!(left, axum::http::StatusCode::OK);
    assert_eq!(right, axum::http::StatusCode::OK);
    assert_eq!(
        jwks.fetches.load(Ordering::SeqCst),
        2,
        "concurrent unknown-kid requests must trigger one refresh"
    );
    assert_eq!(
        authenticated_graphql_status(app, &rotated_token).await,
        axum::http::StatusCode::OK
    );
    assert_eq!(jwks.fetches.load(Ordering::SeqCst), 2);

    jwks.fetches.store(0, Ordering::SeqCst);
    let mut expiring_oidc =
        OidcConfig::new("http://localhost:8080", "graphql-api").engine_roles(&["customer"]);
    expiring_oidc.jwks_uri = Some(format!("http://{address}/jwks"));
    expiring_oidc.jwks_cache_ttl = std::time::Duration::ZERO;
    let expiring_engine = engine_with_identity(IdentityConfig::oidc_bearer(expiring_oidc)).await;
    let expiring_app = graphql_router(expiring_engine);
    assert_eq!(
        authenticated_graphql_status(expiring_app.clone(), &rotated_token).await,
        axum::http::StatusCode::OK
    );
    assert_eq!(
        authenticated_graphql_status(expiring_app, &rotated_token).await,
        axum::http::StatusCode::OK
    );
    assert_eq!(
        jwks.fetches.load(Ordering::SeqCst),
        2,
        "expired JWKS must refresh before the next authenticated request"
    );

    server.abort();
}

#[tokio::test]
async fn identity_resolver_reuses_live_jwks_for_repeated_valid_requests() {
    let keys = mint_keys_with_kid("resolver-kid");
    let jwks = RotatingJwks {
        body: Arc::new(RwLock::new(keys.jwks_json.clone())),
        fetches: Arc::new(AtomicUsize::new(0)),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind JWKS server");
    let address = listener.local_addr().expect("JWKS server address");
    let server = tokio::spawn({
        let jwks = jwks.clone();
        async move {
            axum::serve(
                listener,
                axum::Router::new()
                    .route("/jwks", axum::routing::get(serve_rotating_jwks))
                    .with_state(jwks),
            )
            .await
            .expect("serve JWKS");
        }
    });
    let token = sign_claims(
        &keys,
        json!({
            "iss": "http://localhost:8080",
            "aud": "graphql-api",
            "sub": "resolver-user",
            "exp": now() + 3600,
            "iat": now(),
            "groups": ["customer"]
        }),
    );
    let mut oidc =
        OidcConfig::new("http://localhost:8080", "graphql-api").engine_roles(&["customer"]);
    oidc.jwks_uri = Some(format!("http://{address}/jwks"));
    let resolver = IdentityResolver::new(IdentityConfig::oidc_bearer(oidc));
    let headers = bearer_headers(&token);

    resolver
        .resolve_session(&headers)
        .await
        .expect("first resolve");
    resolver
        .resolve_session(&headers)
        .await
        .expect("second resolve");

    assert_eq!(
        jwks.fetches.load(Ordering::SeqCst),
        1,
        "a reusable resolver must retain its JWKS cache"
    );
    server.abort();
}

#[tokio::test]
async fn unknown_kid_refreshes_are_cooled_down_until_rotation_can_be_retried() {
    let first_keys = mint_keys_with_kid("cooldown-current");
    let unknown_keys = mint_keys_with_kid("cooldown-unknown");
    let rotated_keys = mint_keys_with_kid("cooldown-rotated");
    let jwks = RotatingJwks {
        body: Arc::new(RwLock::new(first_keys.jwks_json.clone())),
        fetches: Arc::new(AtomicUsize::new(0)),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind JWKS server");
    let address = listener.local_addr().expect("JWKS server address");
    let server = tokio::spawn({
        let jwks = jwks.clone();
        async move {
            axum::serve(
                listener,
                axum::Router::new()
                    .route("/jwks", axum::routing::get(serve_rotating_jwks))
                    .with_state(jwks),
            )
            .await
            .expect("serve JWKS");
        }
    });
    let claims = |subject: &str| {
        json!({
            "iss": "http://localhost:8080",
            "aud": "graphql-api",
            "sub": subject,
            "exp": now() + 3600,
            "iat": now(),
            "groups": ["customer"]
        })
    };
    let first_token = sign_claims(&first_keys, claims("current-user"));
    let unknown_token = sign_claims(&unknown_keys, claims("unknown-user"));
    let rotated_token = sign_claims(&rotated_keys, claims("rotated-user"));
    let mut oidc =
        OidcConfig::new("http://localhost:8080", "graphql-api").engine_roles(&["customer"]);
    oidc.jwks_uri = Some(format!("http://{address}/jwks"));
    oidc.jwks_forced_refresh_cooldown = std::time::Duration::from_millis(200);
    let resolver = IdentityResolver::new(IdentityConfig::oidc_bearer(oidc));

    resolver
        .resolve_session(&bearer_headers(&first_token))
        .await
        .expect("load initial JWKS");
    assert_eq!(jwks.fetches.load(Ordering::SeqCst), 1);

    assert_eq!(
        resolver
            .resolve_session(&bearer_headers(&unknown_token))
            .await
            .unwrap_err(),
        AuthError::Unauthorized
    );
    assert_eq!(jwks.fetches.load(Ordering::SeqCst), 2);

    assert_eq!(
        resolver
            .resolve_session(&bearer_headers(&rotated_token))
            .await
            .unwrap_err(),
        AuthError::Unauthorized
    );
    assert_eq!(
        jwks.fetches.load(Ordering::SeqCst),
        2,
        "a distinct unknown kid must not bypass the forced-refresh cooldown"
    );

    *jwks.body.write().expect("rotate JWKS response") = rotated_keys.jwks_json.clone();
    assert_eq!(
        resolver
            .resolve_session(&bearer_headers(&rotated_token))
            .await
            .unwrap_err(),
        AuthError::Unauthorized
    );
    assert_eq!(jwks.fetches.load(Ordering::SeqCst), 2);

    tokio::time::sleep(std::time::Duration::from_millis(225)).await;
    resolver
        .resolve_session(&bearer_headers(&rotated_token))
        .await
        .expect("rotation refresh after cooldown");
    assert_eq!(jwks.fetches.load(Ordering::SeqCst), 3);

    server.abort();
}

#[tokio::test]
async fn http_oidc_missing_bearer_returns_401() {
    let keys = mint_keys();
    let engine = engine_with_identity(IdentityConfig::oidc_bearer(oidc_cfg(&keys))).await;
    let app = graphql_router(engine);
    let res = app
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/graphql")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(r#"{"query":"{ id_items { id } }"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), axum::http::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn http_oidc_valid_bearer_isolation() {
    let keys = mint_keys();
    let claims = json!({
        "iss": "http://localhost:8080",
        "aud": "graphql-api",
        "sub": "user-a-001",
        "exp": now() + 3600,
        "iat": now(),
        "groups": ["customer"]
    });
    let token = sign_claims(&keys, claims);
    let engine = engine_with_identity(IdentityConfig::oidc_bearer(oidc_cfg(&keys))).await;
    let app = graphql_router(engine);
    let res = app
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/graphql")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .header("x-user-id", "evil") // spoof ignored
                .body(axum::body::Body::from(
                    r#"{"query":"{ id_items { id owner } }"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), axum::http::StatusCode::OK);
    let bytes = axum::body::to_bytes(res.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    let rows = v["data"]["id_items"].as_array().expect("data");
    assert_eq!(rows.len(), 1, "isolation: only owner rows: {v}");
    assert_eq!(rows[0]["owner"], "user-a-001");
}

#[tokio::test]
async fn http_invalid_bearer_401() {
    let keys = mint_keys();
    let engine = engine_with_identity(IdentityConfig::oidc_bearer(oidc_cfg(&keys))).await;
    let app = graphql_router(engine);
    let res = app
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/graphql")
                .header("content-type", "application/json")
                .header("authorization", "Bearer eyJhbGciOiJub25lIn0.e30.")
                .header("x-roles", "admin")
                .body(axum::body::Body::from(r#"{"query":"{ id_items { id } }"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), axum::http::StatusCode::UNAUTHORIZED);
}

#[test]
fn public_scaffold_default_is_oidc_bearer_not_dev() {
    // Drives shipped `public_oidc_identity_from_env_vars` (same function scaffold
    // calls via `public_oidc_identity_from_env`) — D6 never DevHeaders.
    use distributed::graphql::{
        public_oidc_identity_from_env_vars, UNSET_OIDC_AUDIENCE, UNSET_OIDC_ISSUER,
    };

    let unset = public_oidc_identity_from_env_vars(None, None, None, None);
    let oidc = unset.oidc().expect("public default is OidcBearer");
    assert!(oidc.require_auth);
    assert_eq!(oidc.issuer, UNSET_OIDC_ISSUER);
    assert_eq!(oidc.audience, UNSET_OIDC_AUDIENCE);

    // Ambient headers alone must not authenticate under public default.
    let headers = headers_from(&[("x-user-id", "attacker"), ("x-roles", "admin")]);
    assert_eq!(
        resolve_session_sync(&headers, &unset).unwrap_err(),
        AuthError::Unauthorized
    );

    let configured = public_oidc_identity_from_env_vars(
        Some("http://localhost:8080"),
        Some("graphql-api"),
        None,
        None,
    );
    let oidc = configured.oidc().expect("configured is OidcBearer");
    assert!(oidc.require_auth);
    assert_eq!(oidc.issuer, "http://localhost:8080");
}

#[test]
fn gateway_secret_wrong_is_401() {
    let cfg = IdentityConfig::gateway("x-gateway-secret", "s3cret").unwrap();
    let headers = headers_from(&[
        ("x-gateway-secret", "wrong"),
        ("x-user-id", "u"),
        ("x-roles", "admin"),
    ]);
    assert_eq!(
        resolve_session_sync(&headers, &cfg).unwrap_err(),
        AuthError::Unauthorized
    );
}

#[cfg(feature = "gateway")]
#[tokio::test]
async fn gateway_oidc_context_keeps_backend_authority() {
    use distributed::gateway::{Admission, AuthProvider, BackendCredential, Credentials};
    use distributed::graphql::OidcGatewayProvider;
    let keys = mint_keys();
    let claims = json!({ "iss": "http://localhost:8080", "aud": "graphql-api", "sub": "user-a-001", "exp": now() + 3600, "iat": now(), "groups": ["customer", "unmapped"] });
    let token = sign_claims(&keys, claims.clone());
    let provider = OidcGatewayProvider::new(oidc_cfg(&keys), "oidc-v1");
    let credentials = Credentials {
        authorization: Some(format!("Bearer {token}")),
        cookie: Some("forged-session".into()),
    };
    let context = provider.authenticate(&credentials).await.unwrap();
    assert_eq!(Admission::Authenticated.check(&context, now()), Ok(()));
    assert_eq!(context.identity().unwrap().subject(), "user-a-001");
    assert_eq!(context.identity().unwrap().roles(), &["customer"]);
    let BackendCredential::Bearer(forwarded) = context.backend_credential() else {
        panic!("explicit bearer")
    };
    assert_eq!(forwarded, &token);

    // A valid gateway assertion does not suppress backend credential checks.
    let mut backend = oidc_cfg(&keys);
    backend.audience = "another-api".into();
    let app = graphql_router(engine_with_identity(IdentityConfig::oidc_bearer(backend)).await);
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/graphql")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {forwarded}"))
                .header("x-user-id", "admin")
                .body(axum::body::Body::from(r#"{"query":"{ id_items { id } }"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::UNAUTHORIZED);

    for replacement in [
        "invalid".to_string(),
        sign_claims(&keys, {
            let mut expired = claims;
            expired["exp"] = json!(now() - 3600);
            expired
        }),
    ] {
        assert!(provider
            .authenticate(&Credentials {
                authorization: Some(format!("Bearer {replacement}")),
                cookie: None
            })
            .await
            .is_err());
    }
    let anon = provider
        .authenticate(&Credentials::default())
        .await
        .unwrap();
    assert!(anon.identity().is_none());
}

// ── No default mode ─────────────────────────────────────────────────────────

#[tokio::test]
async fn engine_without_an_identity_mode_does_not_build() {
    let db = Db::sqlite().await;
    db.seed::<IdItem>([]).await;
    let error = GraphqlEngine::builder(db.pool())
        .roles(&["user"])
        .model::<IdItem>(ModelPermissions::new().grant("user", read().all_columns()))
        .build()
        .err()
        .expect("an engine with no identity mode must not build");
    assert!(error.to_string().contains("identity"), "{error}");
}

// ── JWKS: an issuer outage does not fail requests the cached keys can check ──

async fn jwks_server(jwks: RotatingJwks) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new()
                .route("/jwks", axum::routing::get(serve_rotating_jwks))
                .with_state(jwks),
        )
        .await
        .unwrap();
    });
    address
}

fn customer_token(keys: &TestKeys, expires_in: u64) -> String {
    sign_claims(
        keys,
        json!({
            "iss": "http://localhost:8080",
            "aud": "graphql-api",
            "sub": "user-a-001",
            "exp": now() + expires_in,
            "iat": now(),
            "groups": ["customer"]
        }),
    )
}

#[tokio::test]
async fn failed_jwks_refresh_serves_cached_keys_and_backs_off() {
    let keys = mint_keys();
    let jwks = RotatingJwks {
        body: Arc::new(RwLock::new(keys.jwks_json.clone())),
        fetches: Arc::new(AtomicUsize::new(0)),
    };
    let address = jwks_server(jwks.clone()).await;
    let mut oidc =
        OidcConfig::new("http://localhost:8080", "graphql-api").engine_roles(&["customer"]);
    oidc.jwks_uri = Some(format!("http://{address}/jwks"));
    // Every request finds the keys stale, so every request wants a refresh.
    oidc.jwks_cache_ttl = std::time::Duration::ZERO;
    let resolver = IdentityResolver::new(IdentityConfig::oidc_bearer(oidc.clone()));
    let headers = bearer_headers(&customer_token(&keys, 3600));

    resolver.resolve_session(&headers).await.expect("keys load");
    assert_eq!(jwks.fetches.load(Ordering::SeqCst), 1);

    jwks.body.write().unwrap().clear();
    resolver
        .resolve_session(&headers)
        .await
        .expect("a failed refresh must keep serving the keys already held");
    assert_eq!(jwks.fetches.load(Ordering::SeqCst), 2);
    for _ in 0..5 {
        resolver.resolve_session(&headers).await.expect("backoff");
    }
    assert_eq!(
        jwks.fetches.load(Ordering::SeqCst),
        2,
        "requests inside the backoff must not ask the failing issuer again"
    );

    // With no keys at all there is nothing to serve: fail, but still back off.
    jwks.fetches.store(0, Ordering::SeqCst);
    let cold = IdentityResolver::new(IdentityConfig::oidc_bearer(oidc));
    for _ in 0..3 {
        assert_eq!(
            cold.resolve_session(&headers).await.unwrap_err(),
            AuthError::Unauthorized
        );
    }
    assert_eq!(jwks.fetches.load(Ordering::SeqCst), 1);
}

// ── WebSocket: identity, lifetime, idleness, live-query cap ──────────────────

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn ws_server(engine: Arc<GraphqlEngine>) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, graphql_router_composed(engine, None, None))
            .await
            .unwrap();
    });
    address
}

/// Open `ws://{address}/graphql/ws{query}` with `headers` on the upgrade.
async fn ws_connect(
    address: std::net::SocketAddr,
    query: &str,
    headers: &[(&'static str, &str)],
) -> Result<Ws, tokio_tungstenite::tungstenite::Error> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut request = format!("ws://{address}/graphql/ws{query}")
        .into_client_request()
        .unwrap();
    request.headers_mut().insert(
        "sec-websocket-protocol",
        HeaderValue::from_static("graphql-transport-ws"),
    );
    for (name, value) in headers {
        request
            .headers_mut()
            .insert(*name, HeaderValue::from_str(value).unwrap());
    }
    tokio_tungstenite::connect_async(request)
        .await
        .map(|(socket, _)| socket)
}

async fn ws_send(socket: &mut Ws, message: Value) {
    use futures_util::SinkExt;
    socket
        .send(tokio_tungstenite::tungstenite::Message::Text(
            message.to_string().into(),
        ))
        .await
        .unwrap();
}

/// The next GraphQL message, or the close code when the server hangs up.
async fn ws_next(socket: &mut Ws) -> Result<Value, Option<u16>> {
    use futures_util::StreamExt;
    use tokio_tungstenite::tungstenite::Message;
    loop {
        let frame = tokio::time::timeout(std::time::Duration::from_secs(10), socket.next())
            .await
            .expect("the server must answer or close within 10s");
        match frame {
            Some(Ok(Message::Text(text))) => return Ok(serde_json::from_str(&text).unwrap()),
            Some(Ok(Message::Close(frame))) => return Err(frame.map(|frame| frame.code.into())),
            Some(Ok(_)) => continue,
            Some(Err(_)) | None => return Err(None),
        }
    }
}

async fn ws_subscribe(socket: &mut Ws, id: &str) {
    ws_send(
        socket,
        json!({
            "id": id,
            "type": "subscribe",
            "payload": { "query": "subscription { id_items(order_by: [{id: asc}]) { id } }" }
        }),
    )
    .await;
}

const SPOOF: &str = "?x-user-id=user-a-001&x-roles=admin";

#[tokio::test]
async fn ws_url_identity_params_are_ignored_under_oidc() {
    let keys = mint_keys();
    let mut oidc = oidc_cfg(&keys);
    oidc.require_auth = false;
    let address = ws_server(engine_with_identity(IdentityConfig::oidc_bearer(oidc)).await).await;
    let mut socket = ws_connect(address, SPOOF, &[]).await.unwrap();
    ws_send(&mut socket, json!({ "type": "connection_init" })).await;
    assert_eq!(
        ws_next(&mut socket).await.unwrap()["type"],
        "connection_ack"
    );
    ws_subscribe(&mut socket, "spoof").await;
    let answer = ws_next(&mut socket).await.unwrap();
    assert!(
        answer["payload"]["data"].is_null() && answer["payload"]["errors"].is_array(),
        "an anonymous session has no grant, whatever the URL says: {answer}"
    );

    // With authentication required the same connection is refused outright.
    let address =
        ws_server(engine_with_identity(IdentityConfig::oidc_bearer(oidc_cfg(&keys))).await).await;
    let mut socket = ws_connect(address, SPOOF, &[]).await.unwrap();
    ws_send(&mut socket, json!({ "type": "connection_init" })).await;
    assert!(ws_next(&mut socket).await.is_err(), "no bearer, no session");
}

#[tokio::test]
async fn ws_under_gateway_needs_the_secret_on_the_upgrade() {
    let identity = IdentityConfig::gateway("x-gateway-secret", "s3cret").unwrap();
    let address = ws_server(engine_with_identity(identity).await).await;

    let refused = ws_connect(address, SPOOF, &[]).await;
    assert!(
        matches!(
            &refused,
            Err(tokio_tungstenite::tungstenite::Error::Http(response)) if response.status() == 401
        ),
        "an upgrade without the secret is a 401, whatever the URL says"
    );

    // The payload is the client's, so it cannot raise the gateway's roles.
    let mut socket = ws_connect(
        address,
        SPOOF,
        &[
            ("x-gateway-secret", "s3cret"),
            ("x-user-id", "user-a-001"),
            ("x-roles", "customer"),
        ],
    )
    .await
    .unwrap();
    ws_send(
        &mut socket,
        json!({ "type": "connection_init", "payload": { "x-roles": "admin", "x-user-id": "other" } }),
    )
    .await;
    assert_eq!(
        ws_next(&mut socket).await.unwrap()["type"],
        "connection_ack"
    );
    ws_subscribe(&mut socket, "mine").await;
    let rows = ws_next(&mut socket).await.unwrap();
    assert_eq!(
        rows["payload"]["data"]["id_items"],
        json!([{ "id": "1" }]),
        "the customer row policy from the gateway's headers applies: {rows}"
    );
}

#[tokio::test]
async fn ws_is_closed_when_its_token_expires() {
    let keys = mint_keys();
    let address =
        ws_server(engine_with_identity(IdentityConfig::oidc_bearer(oidc_cfg(&keys))).await).await;
    let mut socket = ws_connect(address, "", &[]).await.unwrap();
    let token = customer_token(&keys, 2);
    ws_send(
        &mut socket,
        json!({ "type": "connection_init", "payload": { "authorization": format!("Bearer {token}") } }),
    )
    .await;
    assert_eq!(
        ws_next(&mut socket).await.unwrap()["type"],
        "connection_ack"
    );
    ws_subscribe(&mut socket, "live").await;
    assert_eq!(ws_next(&mut socket).await.unwrap()["type"], "next");
    assert_eq!(
        ws_next(&mut socket).await,
        Err(Some(4401)),
        "the live query must not outlive the credential that opened it"
    );
}

#[tokio::test]
async fn ws_lifetime_idle_timeout_and_live_query_cap_are_enforced() {
    let limits = WebsocketLimits {
        max_lifetime: std::time::Duration::from_secs(2),
        idle_timeout: std::time::Duration::from_millis(600),
        max_live_queries: 1,
    };
    let engine = engine_with(IdentityConfig::dev_headers(), limits).await;
    assert_eq!(engine.websocket_limits(), limits);
    let address = ws_server(engine).await;
    let init = json!({ "type": "connection_init", "payload": { "x-roles": "admin" } });

    let mut socket = ws_connect(address, "", &[]).await.unwrap();
    ws_send(&mut socket, init.clone()).await;
    assert_eq!(
        ws_next(&mut socket).await.unwrap()["type"],
        "connection_ack"
    );
    ws_subscribe(&mut socket, "first").await;
    assert_eq!(ws_next(&mut socket).await.unwrap()["id"], "first");
    ws_subscribe(&mut socket, "second").await;
    let refused = ws_next(&mut socket).await.unwrap();
    assert_eq!(refused["id"], "second");
    assert_eq!(
        refused["payload"]["errors"][0]["message"], "too many live queries on this connection",
        "{refused}"
    );
    assert_eq!(
        ws_next(&mut socket).await.unwrap(),
        json!({ "id": "second", "type": "complete" })
    );
    // Stopping the first frees its slot.
    ws_send(&mut socket, json!({ "id": "first", "type": "complete" })).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    ws_subscribe(&mut socket, "third").await;
    let mut third = ws_next(&mut socket).await.unwrap();
    while third["id"] == "first" {
        // Frames already queued for the stopped subscription.
        third = ws_next(&mut socket).await.unwrap();
    }
    assert_eq!(third["id"], "third");
    assert_eq!(
        third["payload"]["data"]["id_items"],
        json!([{ "id": "1" }, { "id": "2" }])
    );
    // A client that keeps reading answers pings, so only the lifetime ends it.
    let opened = std::time::Instant::now();
    assert_eq!(ws_next(&mut socket).await, Err(Some(1000)));
    assert!(opened.elapsed() >= std::time::Duration::from_millis(1200));

    // A client that stops reading never answers a ping and is dropped early.
    let mut silent = ws_connect(address, "", &[]).await.unwrap();
    ws_send(&mut silent, init).await;
    assert_eq!(
        ws_next(&mut silent).await.unwrap()["type"],
        "connection_ack"
    );
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    let mut frames = 0;
    while ws_next(&mut silent).await.is_ok() {
        frames += 1;
        assert!(frames < 10, "the idle connection should already be closed");
    }
}
