//! Authentik live e2e — E1–E8. Gate: `AUTHENTIK_E2E=1`.
//! Mint: client_credentials ([[specs/query-layer/oidc-authentik]]).

#![cfg(all(feature = "graphql", feature = "sqlite"))]

#[path = "../graphql_oidc_common/mod.rs"]
mod common;

use distributed::graphql::OidcConfig;
use serde_json::Value;

fn e2e_enabled() -> bool {
    common::gate_enabled("AUTHENTIK_E2E")
}

#[tokio::test]
async fn e1_through_e8_live() {
    if !e2e_enabled() {
        eprintln!("AUTHENTIK_E2E not set — skip");
        return;
    }
    let iss = std::env::var("OIDC_ISSUER").unwrap_or_default();
    if iss.is_empty() {
        panic!(
            "AUTHENTIK_E2E=1 requires OIDC_ISSUER (run ./scripts/oidc-authentik-up.sh to bootstrap)"
        );
    }
    let jwks_uri = std::env::var("OIDC_JWKS_URI").unwrap_or_default();
    // GLOBAL issuer mode puts iss at origin; discovery/JWKS live under application slug.
    let discovery_ok = common::discovery_ready(&iss).await
        || (!jwks_uri.is_empty() && jwks_reachable(&jwks_uri).await)
        || common::discovery_ready(&format!(
            "{}/application/o/graphql-e2e-customer/",
            iss.trim_end_matches('/')
        ))
        .await;
    assert!(
        discovery_ok,
        "Authentik discovery/JWKS not ready (issuer={iss}, jwks={jwks_uri})"
    );
    let audience = std::env::var("OIDC_AUDIENCE").expect("OIDC_AUDIENCE");
    let token_url = std::env::var("AUTHENTIK_TOKEN_URL").unwrap_or_else(|_| {
        let base = iss.trim_end_matches('/');
        // Token endpoint is always under /application/o/token/ for Authentik
        if base.contains("/application/o/") {
            format!("{base}/../token/").replace(
                "/application/o/graphql-e2e-customer/../token/",
                "/application/o/token/",
            )
        } else {
            format!("{base}/application/o/token/")
        }
    });
    let c_id = std::env::var("AUTHENTIK_E2E_CUSTOMER_CLIENT_ID").expect("customer client");
    let c_sec = std::env::var("AUTHENTIK_E2E_CUSTOMER_CLIENT_SECRET").expect("customer secret");
    let a_id = std::env::var("AUTHENTIK_E2E_ADMIN_CLIENT_ID").expect("admin client");
    let a_sec = std::env::var("AUTHENTIK_E2E_ADMIN_CLIENT_SECRET").expect("admin secret");

    if c_sec.is_empty() || a_sec.is_empty() {
        panic!("AUTHENTIK_E2E=1 requires client secrets from bootstrap");
    }

    let mut oidc = OidcConfig::new(&iss, &audience).with_extra_audiences([a_id.as_str()]);
    if audience != c_id {
        oidc.extra_audiences.push(c_id.clone());
    }
    if !jwks_uri.is_empty() {
        oidc.jwks_uri = Some(jwks_uri);
    }
    oidc.claim_map.role_claims = vec!["groups".into(), "roles".into()];

    common::run_e1_through_e8(
        oidc,
        async { mint_client_credentials(&token_url, &c_id, &c_sec).await },
        async { mint_client_credentials(&token_url, &a_id, &a_sec).await },
    )
    .await;
}

async fn jwks_reachable(url: &str) -> bool {
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
    {
        Ok(c) => c,
        Err(_) => return false,
    };
    match client.get(url).send().await {
        Ok(r) if r.status().is_success() => {
            let v: Value = r.json().await.unwrap_or(Value::Null);
            v.get("keys")
                .and_then(|k| k.as_array())
                .map(|a| !a.is_empty())
                .unwrap_or(false)
        }
        _ => false,
    }
}

async fn mint_client_credentials(
    token_url: &str,
    client_id: &str,
    client_secret: &str,
) -> Result<(String, String), String> {
    // Request `groups` so ScopeMapping injects customer/admin for E1 isolation.
    let body = format!(
        "grant_type=client_credentials&client_id={}&client_secret={}&scope={}",
        urlencoding(client_id),
        urlencoding(client_secret),
        urlencoding("openid groups profile")
    );
    let resp = reqwest::Client::new()
        .post(token_url)
        .header("content-type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .map_err(|e| format!("token: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!(
            "token HTTP {}: {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        ));
    }
    let v: Value = resp.json().await.map_err(|e| e.to_string())?;
    let token = v
        .get("access_token")
        .and_then(|t| t.as_str())
        .ok_or("no access_token")?
        .to_string();
    let sub = decode_sub(&token).ok_or("no sub")?;
    Ok((token, sub))
}

fn decode_sub(token: &str) -> Option<String> {
    let payload = token.split('.').nth(1)?;
    let mut s = payload.replace('-', "+").replace('_', "/");
    while s.len() % 4 != 0 {
        s.push('=');
    }
    let bytes =
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s.as_bytes()).ok()?;
    let v: Value = serde_json::from_slice(&bytes).ok()?;
    v.get("sub")?.as_str().map(|s| s.to_string())
}

fn urlencoding(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}
