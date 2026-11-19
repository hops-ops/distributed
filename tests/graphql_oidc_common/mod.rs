//! Shared helpers for multi-provider GraphQL OIDC live e2e (E1–E8).
//! Included via `#[path = "../graphql_oidc_common/mod.rs"] mod common;`

#![allow(dead_code)]

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::{HeaderMap, HeaderValue, StatusCode};
use base64::Engine as _;
use distributed::graphql::{
    graphql_router, read, resolve_session, AuthError, GraphqlEngine, GraphqlPoolSource,
    IdentityConfig, IdentityMode,
    ModelPermissions, OidcConfig, OidcValidator, ValidationError,
};
use distributed::ReadModel;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rsa::pkcs1::EncodeRsaPrivateKey;
use rsa::traits::PublicKeyParts;
use rsa::{RsaPrivateKey, RsaPublicKey};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tower::util::ServiceExt;

#[path = "../support/graphql.rs"]
mod graphql_support;
use graphql_support::Db;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("oidc_e2e_items")]
pub struct OidcE2eItem {
    #[id("id")]
    pub id: String,
    pub owner: String,
}

/// `oidc_e2e_items` created from its read-model declaration, holding `rows`
/// of `(id, owner)`.
async fn items(rows: &[(&str, &str)]) -> GraphqlPoolSource {
    let db = Db::sqlite().await;
    db.seed::<OidcE2eItem>([]).await;
    db.seed(rows.iter().map(|(id, owner)| OidcE2eItem {
        id: (*id).into(),
        owner: (*owner).into(),
    }))
    .await;
    db.pool()
}

pub fn gate_enabled(name: &str) -> bool {
    matches!(
        std::env::var(name)
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes"
    )
}

pub async fn discovery_ready(issuer: &str) -> bool {
    let base = issuer.trim_end_matches('/');
    let url = format!("{base}/.well-known/openid-configuration");
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
    {
        Ok(c) => c,
        Err(_) => return false,
    };
    client
        .get(&url)
        .send()
        .await
        .map(|r| r.status().is_success())
        .unwrap_or(false)
}

pub async fn engine_oidc(oidc: OidcConfig) -> Arc<GraphqlEngine> {
    let pool = items(&[("1", "subject-a"), ("2", "subject-b")]).await;
    let mut oidc = oidc;
    oidc.require_auth = true;
    oidc.claim_map.engine_roles = vec!["admin".into(), "customer".into(), "user".into()];
    Arc::new(
        GraphqlEngine::builder(pool)
            .roles(&["customer", "admin", "user"])
            .model::<OidcE2eItem>(
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
            .identity(IdentityConfig::oidc_bearer(oidc))
            .graphiql(false)
            .build()
            .unwrap(),
    )
}

async fn post_graphql(
    engine: Arc<GraphqlEngine>,
    headers: HeaderMap,
    body: &str,
) -> (StatusCode, Value) {
    let app = graphql_router(engine);
    let mut req = axum::http::Request::builder()
        .method("POST")
        .uri("/graphql")
        .header("content-type", "application/json");
    for (k, v) in headers.iter() {
        req = req.header(k, v);
    }
    let res = app
        .oneshot(req.body(axum::body::Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap_or(json_null());
    (status, v)
}

fn json_null() -> Value {
    Value::Null
}

fn bearer_headers(token: &str) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
    );
    h
}

/// Shared E1–E8 against shipped GraphQL HTTP + OidcBearer.
///
/// `mint_a` / `mint_b` return access tokens for two distinct subjects (E1/E7).
/// `oidc` is issuer+audience (+ optional role claim defaults) for validation.
pub async fn run_e1_through_e8(
    oidc: OidcConfig,
    mint_a: impl std::future::Future<Output = Result<(String, String), String>>,
    mint_b: impl std::future::Future<Output = Result<(String, String), String>>,
) {
    let (token_a, sub_a) = mint_a.await.expect("mint A");
    let (token_b, sub_b) = mint_b.await.expect("mint B");
    assert_ne!(sub_a, sub_b, "E7 needs two distinct subjects");
    assert!(!token_a.is_empty() && !token_b.is_empty());

    // Seed DB owners to match minted subjects.
    let pool = items(&[("1", &sub_a), ("2", &sub_b)]).await;

    let mut oidc_cfg = oidc.clone();
    oidc_cfg.require_auth = true;
    oidc_cfg.claim_map.engine_roles = vec!["admin".into(), "customer".into(), "user".into()];
    // Prefer user role schema if IdP omits roles — grant user for isolation tests.
    let engine = Arc::new(
        GraphqlEngine::builder(pool)
            .roles(&["customer", "admin", "user"])
            .model::<OidcE2eItem>(
                ModelPermissions::new()
                    .grant(
                        "customer",
                        read().all_columns().rows(
                            distributed::graphql::col("owner")
                                .eq(distributed::graphql::claim("x-user-id")),
                        ),
                    )
                    .grant(
                        "user",
                        read().all_columns().rows(
                            distributed::graphql::col("owner")
                                .eq(distributed::graphql::claim("x-user-id")),
                        ),
                    )
                    .grant("admin", read().all_columns()),
            )
            .identity(IdentityConfig::oidc_bearer(oidc_cfg.clone()))
            .graphiql(false)
            .build()
            .unwrap(),
    );

    // E1 — valid token isolation (must map an engine role + own-row filter)
    {
        let session = OidcValidator::new(oidc_cfg.clone())
            .validate_and_map_async(&token_a)
            .await
            .expect("E1 validate");
        assert_eq!(session.user_id(), Some(sub_a.as_str()), "E1 sub");
        // Set-only identity: roles live in `x-roles` (Session::roles).
        let roles: Vec<String> = session.roles().into_iter().map(str::to_string).collect();
        assert!(
            roles
                .iter()
                .any(|r| r == "customer" || r == "user" || r == "admin"),
            "E1 token A must map an engine role (customer|user|admin), got {roles:?}. \
             Fix IdP bootstrap so access token carries roles/groups/realm_access.roles \
             (or Zitadel project roles)."
        );
        let (status, v) = post_graphql(
            Arc::clone(&engine),
            bearer_headers(&token_a),
            r#"{"query":"{ oidc_e2e_items { id owner } }"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "E1 status: {v}");
        assert!(
            v.get("errors")
                .and_then(|e| e.as_array())
                .map(|a| a.is_empty())
                .unwrap_or(true),
            "E1 GraphQL errors (role surface empty?): {v}"
        );
        let arr = v["data"]["oidc_e2e_items"]
            .as_array()
            .unwrap_or_else(|| panic!("E1 missing data.oidc_e2e_items: {v}"));
        if roles.iter().any(|r| r == "admin") {
            // Admin sees all rows; still must execute successfully.
            assert!(!arr.is_empty(), "E1 admin must see rows: {v}");
        } else {
            // customer/user: row isolation to subject A only
            assert_eq!(arr.len(), 1, "E1 isolation row count: {v}");
            assert_eq!(
                arr[0]["owner"].as_str(),
                Some(sub_a.as_str()),
                "E1 isolation owner: {v}"
            );
        }
        eprintln!("E1 ok sub={sub_a} roles={roles:?}");
    }

    // E2 — spoof headers ignored
    {
        let mut h = bearer_headers(&token_a);
        h.insert("x-user-id", HeaderValue::from_static("evil-spoof"));
        h.insert("x-roles", HeaderValue::from_static("admin"));
        let session = resolve_session(&h, engine.identity_config())
            .await
            .expect("E2 resolve");
        assert_eq!(session.user_id(), Some(sub_a.as_str()), "E2 spoof ignored");
        assert_ne!(session.user_id(), Some("evil-spoof"));
        eprintln!("E2 ok");
    }

    // E3 — missing Bearer → 401
    {
        let (status, _) = post_graphql(
            Arc::clone(&engine),
            HeaderMap::new(),
            r#"{"query":"{ oidc_e2e_items { id } }"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "E3");
        eprintln!("E3 ok");
    }

    // E4 — malformed / alg=none → 401
    {
        let mut h = HeaderMap::new();
        h.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer eyJhbGciOiJub25lIn0.e30."),
        );
        let (status, _) = post_graphql(
            Arc::clone(&engine),
            h,
            r#"{"query":"{ oidc_e2e_items { id } }"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "E4");
        eprintln!("E4 ok");
    }

    // E5 — expired access token → ValidationError::Expired (signed JWT, exp in past).
    // Uses static JWKS on the shipped validator so failure is expiry, not signature.
    {
        let keys = mint_e5_rsa_keys();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let claims = json!({
            "iss": oidc_cfg.issuer,
            "aud": oidc_cfg.audience,
            "sub": "expired-subject",
            "exp": now - 120,
            "iat": now - 3600,
            "nbf": now - 3600,
        });
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(keys.kid.clone());
        let expired_token = encode(&header, &claims, &keys.encoding).expect("E5 sign");
        let mut exp_cfg =
            OidcConfig::new(&oidc_cfg.issuer, &oidc_cfg.audience).with_static_jwks(&keys.jwks_json);
        exp_cfg.clock_skew = std::time::Duration::from_secs(0);
        let err = OidcValidator::new(exp_cfg)
            .validate_token(&expired_token)
            .expect_err("E5 must reject expired token");
        assert!(
            matches!(err, ValidationError::Expired),
            "E5 must be Expired (not signature/malformed), got {err:?}"
        );
        // HTTP path with engine wired to the same static JWKS → 401
        let pool = items(&[]).await;
        let mut http_cfg =
            OidcConfig::new(&oidc_cfg.issuer, &oidc_cfg.audience).with_static_jwks(&keys.jwks_json);
        http_cfg.require_auth = true;
        http_cfg.clock_skew = std::time::Duration::from_secs(0);
        http_cfg.claim_map.engine_roles = vec!["admin".into(), "customer".into(), "user".into()];
        let exp_engine = Arc::new(
            GraphqlEngine::builder(pool)
                .roles(&["customer", "admin", "user"])
                .model::<OidcE2eItem>(
                    ModelPermissions::new().grant("customer", read().all_columns()),
                )
                .identity(IdentityConfig::oidc_bearer(http_cfg))
                .graphiql(false)
                .build()
                .unwrap(),
        );
        let (status, _) = post_graphql(
            exp_engine,
            bearer_headers(&expired_token),
            r#"{"query":"{ oidc_e2e_items { id } }"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "E5 HTTP");
        eprintln!("E5 ok (Expired + HTTP 401)");
    }

    // E6 — wrong audience config rejects valid token
    {
        let mut bad = oidc_cfg.clone();
        bad.audience = "definitely-wrong-audience-xyz".into();
        // Multi-client configs carry extra_audiences (azp) — clear them so E6 is honest.
        bad.extra_audiences.clear();
        let err = OidcValidator::new(bad)
            .validate_and_map_async(&token_a)
            .await;
        assert!(err.is_err(), "E6 wrong aud must fail");
        assert!(
            matches!(err, Err(ValidationError::Audience))
                || err
                    .as_ref()
                    .err()
                    .map(|e| e.to_string().contains("audience"))
                    .unwrap_or(false),
            "E6 expected Audience error, got {err:?}"
        );
        eprintln!("E6 ok");
    }

    // E7 — second subject distinct session
    {
        let session_b = OidcValidator::new(oidc_cfg.clone())
            .validate_and_map_async(&token_b)
            .await
            .expect("E7 validate B");
        assert_eq!(session_b.user_id(), Some(sub_b.as_str()), "E7");
        assert_ne!(session_b.user_id(), Some(sub_a.as_str()));
        eprintln!("E7 ok sub_b={sub_b}");
    }

    // E8 — no token material in error bodies for auth failures
    {
        let mut h = HeaderMap::new();
        let secret = format!("Bearer {token_a}");
        h.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_str("Bearer not-a-jwt").unwrap(),
        );
        let (status, v) = post_graphql(
            Arc::clone(&engine),
            h,
            r#"{"query":"{ oidc_e2e_items { id } }"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "E8 status");
        let body = v.to_string();
        assert!(
            !body.contains(token_a.as_str())
                && !body.contains(secret.trim_start_matches("Bearer ")),
            "E8 must not echo tokens: {body}"
        );
        eprintln!("E8 ok");
    }

    let _ = IdentityMode::OidcBearer;
    let _ = AuthError::Unauthorized;
}

// ── E5 helpers: signed expired JWT against static JWKS (shipped validate path) ─

struct E5Keys {
    encoding: EncodingKey,
    jwks_json: String,
    kid: String,
}

fn mint_e5_rsa_keys() -> E5Keys {
    let mut rng = rand::thread_rng();
    let private = RsaPrivateKey::new(&mut rng, 2048).expect("rsa");
    let public = RsaPublicKey::from(&private);
    let pem = private.to_pkcs1_pem(rsa::pkcs8::LineEnding::LF).unwrap();
    let encoding = EncodingKey::from_rsa_pem(pem.as_bytes()).unwrap();
    let kid = "e5-expired-kid".to_string();
    let n = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(public.n().to_bytes_be());
    let e = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(public.e().to_bytes_be());
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
    E5Keys {
        encoding,
        jwks_json,
        kid,
    }
}
