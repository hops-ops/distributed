//! Shared helpers for the e2e-ui suite — GraphQL-only public API.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde_json::{json, Value};

const OFFLINE_ISSUER: &str = "https://offline-oidc.e2e.invalid";
const OFFLINE_AUDIENCE: &str = "e2e-ui-offline";
const OFFLINE_KID: &str = "e2e-ui-offline-es256";
const OFFLINE_ES256_PRIVATE_KEY: &str = r#"-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgWTFfCGljY6aw3Hrt
kHmPRiazukxPLb6ilpRAewjW8nihRANCAATDskChT+Altkm9X7MI69T3IUmrQU0L
950IxEzvw/x5BMEINRMrXLBJhqzO9Bm+d6JbqA21YQmd1Kt4RzLJR1W+
-----END PRIVATE KEY-----"#;
const OFFLINE_ES256_X: &str = "w7JAoU_gJbZJvV-zCOvU9yFJq0FNC_edCMRM78P8eQQ";
const OFFLINE_ES256_Y: &str = "wQg1EytcsEmGrM70Gb53oluoDbVhCZ3Uq3hHMslHVb4";

static OFFLINE_OIDC_ENABLED: AtomicBool = AtomicBool::new(false);

/// Configure the in-process suite with a real signature-verified OIDC identity.
///
/// Durable commands intentionally reject DevHeaders. This fixture preserves
/// that production fence while keeping the SQLite behavioral suite offline.
pub fn offline_oidc_identity() -> distributed::graphql::IdentityConfig {
    OFFLINE_OIDC_ENABLED.store(true, Ordering::Release);
    let jwks = json!({
        "keys": [{
            "kty": "EC",
            "kid": OFFLINE_KID,
            "alg": "ES256",
            "use": "sig",
            "crv": "P-256",
            "x": OFFLINE_ES256_X,
            "y": OFFLINE_ES256_Y
        }]
    })
    .to_string();
    e2e_service::oidc_bearer_config(OFFLINE_ISSUER, OFFLINE_AUDIENCE, None, Some(jwks))
}

/// Roles asserted on the access token / session for this principal.
/// Admins always also hold `user` (local IdP + offline tests).
fn identity_roles_json(role: &str) -> Value {
    match role {
        "admin" => json!(["admin", "user"]),
        _ => json!([role]),
    }
}

fn offline_bearer(subject: &str, role: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_secs();
    let roles = identity_roles_json(role);
    let claims = json!({
        "iss": OFFLINE_ISSUER,
        "aud": OFFLINE_AUDIENCE,
        "sub": subject,
        "iat": now.saturating_sub(1),
        "nbf": now.saturating_sub(1),
        "exp": now + 3600,
        "roles": roles,
        "groups": roles
    });
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(OFFLINE_KID.to_string());
    let key =
        EncodingKey::from_ec_pem(OFFLINE_ES256_PRIVATE_KEY.as_bytes()).expect("offline EC key");
    encode(&header, &claims, &key).expect("sign offline access token")
}

fn with_identity(
    request: reqwest::RequestBuilder,
    user_id: &str,
    role: &str,
) -> reqwest::RequestBuilder {
    if OFFLINE_OIDC_ENABLED.load(Ordering::Acquire) {
        request.bearer_auth(offline_bearer(user_id, role))
    } else {
        let roles = if role == "admin" { "admin,user" } else { role };
        request
            .header("x-user-id", user_id)
            .header("x-roles", roles)
    }
}

pub fn new_command_id() -> String {
    uuid::Uuid::now_v7().hyphenated().to_string()
}

pub fn base_url() -> String {
    std::env::var("E2E_BASE_URL").unwrap_or_else(|_| "http://127.0.0.1:8791".into())
}

pub async fn wait_ready(base: &str, timeout: Duration) -> bool {
    let client = reqwest::Client::new();
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        // Probe as single-role user (multi-role admin requires a named surface).
        let req = with_identity(
            client
                .post(format!("{base}/graphql"))
                .header("content-type", "application/json"),
            "probe",
            "user",
        )
        .json(&json!({"query":"{ __typename }"}));
        if let Ok(resp) = req.send().await {
            if resp.status().as_u16() < 500 {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// Default GraphQL helper: multi-role principals must name a surface.
/// - `user` → e2e-ui (eligible admin+user, schema privilege user)
/// - `admin` → e2e-ui-admin (admin privilege)
pub async fn graphql(base: &str, query: &str, user_id: &str, role: &str) -> Result<Value, String> {
    let (application, eligible_roles, schema_roles, schema_hash) =
        default_application_surface(role)?;
    graphql_for_application(
        base,
        query,
        user_id,
        role,
        application,
        &eligible_roles,
        &schema_roles,
        &schema_hash,
    )
    .await
}

fn default_application_surface(
    role: &str,
) -> Result<(&'static str, Vec<&'static str>, Vec<&'static str>, String), String> {
    use e2e_service::{
        distributed_admin_client_surface, distributed_client_surface, ADMIN_CLIENT_SURFACE,
        CLIENT_SURFACE,
    };
    let (surface, export) = match role {
        "admin" => (ADMIN_CLIENT_SURFACE, distributed_admin_client_surface()),
        _ => (CLIENT_SURFACE, distributed_client_surface()),
    };
    let manifest = export.manifest().map_err(|e| e.to_string())?;
    Ok((
        surface.name,
        surface.eligible_roles.to_vec(),
        surface.schema_roles.to_vec(),
        manifest.schema_fingerprint,
    ))
}

pub async fn graphql_for_application(
    base: &str,
    query: &str,
    user_id: &str,
    role: &str,
    application: &str,
    eligible_roles: &[&str],
    schema_roles: &[&str],
    schema_hash: &str,
) -> Result<Value, String> {
    graphql_request(
        base,
        json!({
            "query": query,
            "extensions": {
                "distributed": {
                    "client": {
                        "surface": {
                            "kind": "application",
                            "name": application,
                            "eligible_roles": eligible_roles,
                            "schema_roles": schema_roles,
                        },
                        "schemaHash": schema_hash,
                    }
                }
            }
        }),
        user_id,
        role,
    )
    .await
}

async fn graphql_request(
    base: &str,
    body: Value,
    user_id: &str,
    role: &str,
) -> Result<Value, String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| e.to_string())?;
    let resp = with_identity(
        client
            .post(format!("{base}/graphql"))
            .header("content-type", "application/json"),
        user_id,
        role,
    )
    .json(&body)
    .send()
    .await
    .map_err(|e| e.to_string())?;
    let status = resp.status();
    let v: Value = resp.json().await.map_err(|e| e.to_string())?;
    if !status.is_success() {
        return Err(format!("graphql HTTP {status}: {v}"));
    }
    Ok(v)
}

/// GraphQL document without identity headers (unauthenticated probe).
pub async fn graphql_raw(base: &str, query: &str) -> Result<(u16, Value), String> {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/graphql"))
        .header("content-type", "application/json")
        .json(&json!({ "query": query }))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = resp.status().as_u16();
    let v: Value = resp.json().await.unwrap_or(json!({}));
    Ok((status, v))
}

fn gql_errors(v: &Value) -> Option<String> {
    let errs = v.get("errors")?.as_array()?;
    if errs.is_empty() {
        return None;
    }
    Some(
        errs.iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("; "),
    )
}

/// Run a mutation and return `data.<field>` or Err with GraphQL errors.
pub async fn mutate(
    base: &str,
    field: &str,
    document: &str,
    user_id: &str,
    role: &str,
) -> Result<Value, String> {
    let v = graphql(base, document, user_id, role).await?;
    if let Some(msg) = gql_errors(&v) {
        return Err(format!("{field} errors: {msg}"));
    }
    v["data"][field]
        .as_object()
        .cloned()
        .map(Value::Object)
        .ok_or_else(|| format!("{field} missing data: {v}"))
}

pub async fn todos_create(
    base: &str,
    todo_id: &str,
    title: &str,
    user_id: &str,
    role: &str,
) -> Result<Value, String> {
    let command_id = new_command_id();
    let doc = format!(
        r#"mutation {{
          todos_create(commandId: "{command_id}", input: {{ todo_id: "{todo_id}", title: "{title}" }}) {{
            todo_id owner_id title status
          }}
        }}"#
    );
    mutate(base, "todos_create", &doc, user_id, role).await
}

pub async fn todos_complete(
    base: &str,
    todo_id: &str,
    user_id: &str,
    role: &str,
) -> Result<Value, String> {
    let command_id = new_command_id();
    let doc = format!(
        r#"mutation {{
          todos_complete(commandId: "{command_id}", input: {{ todo_id: "{todo_id}" }}) {{
            todo_id status
          }}
        }}"#
    );
    mutate(base, "todos_complete", &doc, user_id, role).await
}

pub async fn todos_archive(
    base: &str,
    todo_id: &str,
    user_id: &str,
    role: &str,
) -> Result<Value, String> {
    let command_id = new_command_id();
    let doc = format!(
        r#"mutation {{
          todos_archive(commandId: "{command_id}", input: {{ todo_id: "{todo_id}" }}) {{
            todo_id status
          }}
        }}"#
    );
    mutate(base, "todos_archive", &doc, user_id, role).await
}

/// Admin-only mutation (not present on the user role schema).
pub async fn todos_force_archive(
    base: &str,
    todo_id: &str,
    user_id: &str,
    role: &str,
) -> Result<Value, String> {
    let command_id = new_command_id();
    let doc = format!(
        r#"mutation {{
          todos_force_archive(commandId: "{command_id}", input: {{ todo_id: "{todo_id}" }}) {{
            todo_id owner_id status archived_by
          }}
        }}"#
    );
    mutate(base, "todos_force_archive", &doc, user_id, role).await
}

pub async fn todos_rename(
    base: &str,
    todo_id: &str,
    title: &str,
    user_id: &str,
    role: &str,
) -> Result<Value, String> {
    let command_id = new_command_id();
    let doc = format!(
        r#"mutation {{
          todos_rename(commandId: "{command_id}", input: {{ todo_id: "{todo_id}", title: "{title}" }}) {{
            todo_id title status
          }}
        }}"#
    );
    mutate(base, "todos_rename", &doc, user_id, role).await
}

pub async fn todos_reopen(
    base: &str,
    todo_id: &str,
    user_id: &str,
    role: &str,
) -> Result<Value, String> {
    let command_id = new_command_id();
    let doc = format!(
        r#"mutation {{
          todos_reopen(commandId: "{command_id}", input: {{ todo_id: "{todo_id}" }}) {{
            todo_id status
          }}
        }}"#
    );
    mutate(base, "todos_reopen", &doc, user_id, role).await
}

pub async fn todos_purge(
    base: &str,
    todo_id: &str,
    user_id: &str,
    role: &str,
) -> Result<Value, String> {
    let command_id = new_command_id();
    let doc = format!(
        r#"mutation {{
          todos_purge(commandId: "{command_id}", input: {{ todo_id: "{todo_id}" }}) {{
            todo_id purged
          }}
        }}"#
    );
    mutate(base, "todos_purge", &doc, user_id, role).await
}

/// Assert HTTP command routes are not mounted (GraphQL-only surface).
pub async fn assert_http_commands_disabled(base: &str) -> Result<(), String> {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/todo.create"))
        .header("content-type", "application/json")
        .header("x-user-id", "alice")
        .header("x-roles", "user")
        .json(&json!({ "todo_id": "t-should-404", "title": "nope" }))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = resp.status().as_u16();
    if status != 404 {
        return Err(format!(
            "expected unmounted POST /todo.create to return 404, got {status}"
        ));
    }
    Ok(())
}

pub mod cases {
    pub const CREATE: &str = "T1_create_todo";
    pub const OWNER_ISOLATION: &str = "T2_owner_isolation";
    pub const ADMIN_SEES_ALL: &str = "T2b_admin_sees_all_owners";
    pub const ADMIN_FORCE_ARCHIVE: &str = "T2c_admin_force_archive";
    pub const SDL_ROLE_SPLIT: &str = "T2d_sdl_role_split_force_archive";
    pub const CREATE_OWNER_SESSION: &str = "T1c_create_owner_from_session";
    pub const COMPLETE: &str = "T3_complete_todo";
    pub const NOT_OWNER: &str = "T4_not_owner_rejected";
    pub const NOT_OWNER_MUTATES: &str = "T4b_not_owner_rename_archive_reopen";
    pub const UNAUTH: &str = "T5_unauthenticated_rejected";
    pub const LIFECYCLE: &str = "T6_lifecycle_rename_archive";
    pub const HTTP_OFF: &str = "T0_http_commands_disabled";
    pub const ADMIN_QUERY_LIMIT: &str = "T2e_admin_todos_limit";
}
