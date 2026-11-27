//! Zitadel live e2e — reference provider for [[specs/query-layer/oidc-e2e]] E1–E8.
//! Live (`#[ignore = "live: ..."]`): run with `--ignored` after
//! `scripts/oidc-zitadel-up.sh`; fails without `OIDC_ISSUER`.
//! Mint: JWT-bearer (adapter [[specs/query-layer/oidc-zitadel]]).

use common::env as env_support;
use common::oidc;

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use distributed::graphql::OidcConfig;
use serde_json::{json, Value};

/// Live E1–E8 against real Zitadel issuer + shipped OidcBearer HTTP path.
#[tokio::test]
#[ignore = "live: needs a bootstrapped Zitadel (scripts/oidc-zitadel-up.sh, OIDC_ISSUER)"]
async fn e1_through_e8_live() {
    let iss = env_support::require_live("OIDC_ISSUER", "Zitadel OIDC e2e");
    assert!(
        oidc::discovery_ready(&iss).await || issuer_ready_debug(&iss).await,
        "issuer not ready: {iss}"
    );
    let audience = std::env::var("OIDC_AUDIENCE")
        .or_else(|_| std::env::var("OIDC_CLIENT_ID"))
        .expect("OIDC_AUDIENCE");

    let customer_key = std::env::var("GRAPHQL_E2E_CUSTOMER_KEY").expect("CUSTOMER_KEY");
    let customer_uid = std::env::var("GRAPHQL_E2E_CUSTOMER_USER_ID").expect("CUSTOMER_USER_ID");
    let admin_key = std::env::var("GRAPHQL_E2E_ADMIN_KEY").expect("ADMIN_KEY");
    let admin_uid = std::env::var("GRAPHQL_E2E_ADMIN_USER_ID").expect("ADMIN_USER_ID");

    let oidc = OidcConfig::new(&iss, &audience);
    oidc::run_e1_through_e8(
        oidc,
        async {
            let t = mint_jwt_bearer(&iss, &customer_key, &customer_uid).await?;
            Ok((t, customer_uid.clone()))
        },
        async {
            let t = mint_jwt_bearer(&iss, &admin_key, &admin_uid).await?;
            Ok((t, admin_uid.clone()))
        },
    )
    .await;
}

async fn issuer_ready_debug(iss: &str) -> bool {
    let url = format!("{}/debug/ready", iss.trim_end_matches('/'));
    reqwest::Client::new()
        .get(url)
        .send()
        .await
        .map(|r| r.status().is_success())
        .unwrap_or(false)
}

async fn mint_jwt_bearer(issuer: &str, key_path: &str, user_id: &str) -> Result<String, String> {
    let raw =
        std::fs::read_to_string(PathBuf::from(key_path)).map_err(|e| format!("read key: {e}"))?;
    let key_json: Value = serde_json::from_str(&raw).map_err(|e| format!("parse key: {e}"))?;
    let key_id = key_json
        .get("keyId")
        .or_else(|| key_json.get("key_id"))
        .and_then(|v| v.as_str())
        .ok_or("keyId missing")?;
    let private_pem = key_json
        .get("key")
        .and_then(|v| v.as_str())
        .ok_or("key PEM missing")?;

    let iss = issuer.trim_end_matches('/');
    let token_url = format!("{iss}/oauth/v2/token");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let assertion_claims = json!({
        "iss": user_id,
        "sub": user_id,
        "aud": [token_url.clone(), iss],
        "iat": now,
        "exp": now + 60,
    });
    let encoding = jsonwebtoken::EncodingKey::from_rsa_pem(private_pem.as_bytes())
        .map_err(|e| format!("pem: {e}"))?;
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
    header.kid = Some(key_id.to_string());
    let assertion = jsonwebtoken::encode(&header, &assertion_claims, &encoding)
        .map_err(|e| format!("sign: {e}"))?;

    let project_id = std::env::var("ZITADEL_PROJECT_ID")
        .or_else(|_| std::env::var("OIDC_AUDIENCE"))
        .unwrap_or_default();
    // `projects:roles` (plural) yields `urn:zitadel:iam:org:project:{id}:roles` on tokens.
    let scope = if project_id.is_empty() {
        "openid profile urn:zitadel:iam:org:project:roles urn:zitadel:iam:org:projects:roles"
            .to_string()
    } else {
        format!(
            "openid profile urn:zitadel:iam:org:project:id:{project_id}:aud urn:zitadel:iam:org:project:roles urn:zitadel:iam:org:projects:roles"
        )
    };

    let body = format!(
        "grant_type={}&scope={}&assertion={}",
        urlencoding("urn:ietf:params:oauth:grant-type:jwt-bearer"),
        urlencoding(&scope),
        urlencoding(&assertion),
    );
    let resp = reqwest::Client::new()
        .post(&token_url)
        .header("content-type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .map_err(|e| format!("token request: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!(
            "token endpoint error: {}",
            resp.text().await.unwrap_or_default()
        ));
    }
    let body: Value = resp.json().await.map_err(|e| format!("json: {e}"))?;
    body.get("access_token")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| "no access_token".into())
}

fn urlencoding(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}
