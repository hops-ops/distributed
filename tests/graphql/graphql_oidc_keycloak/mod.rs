//! Keycloak live e2e — E1–E8. Gate: `KEYCLOAK_E2E=1`.
//! Mint: client_credentials ([[specs/query-layer/oidc-keycloak]]).

use common::oidc;

use distributed::graphql::OidcConfig;
use serde_json::Value;

fn e2e_enabled() -> bool {
    oidc::gate_enabled("KEYCLOAK_E2E")
}

#[tokio::test]
async fn e1_through_e8_live() {
    if !e2e_enabled() {
        eprintln!("KEYCLOAK_E2E not set — skip");
        return;
    }
    let iss = std::env::var("OIDC_ISSUER").expect("OIDC_ISSUER");
    assert!(
        oidc::discovery_ready(&iss).await,
        "Keycloak discovery not ready at {iss}"
    );
    let audience = std::env::var("OIDC_AUDIENCE").expect("OIDC_AUDIENCE");

    let cust_id = std::env::var("KEYCLOAK_E2E_CUSTOMER_CLIENT_ID").expect("customer client");
    let cust_sec = std::env::var("KEYCLOAK_E2E_CUSTOMER_CLIENT_SECRET").expect("customer secret");
    let adm_id = std::env::var("KEYCLOAK_E2E_ADMIN_CLIENT_ID").expect("admin client");
    let adm_sec = std::env::var("KEYCLOAK_E2E_ADMIN_CLIENT_SECRET").expect("admin secret");

    // Keycloak client_credentials tokens typically omit `aud` and set `azp` to the
    // client id — accept both customer and admin clients for E1–E8 multi-subject.
    let mut oidc = OidcConfig::new(&iss, &audience).with_extra_audiences([adm_id.as_str()]);
    if audience != cust_id {
        oidc.extra_audiences.push(cust_id.clone());
    }
    oidc.claim_map.role_claims = vec!["realm_access.roles".into(), "groups".into(), "roles".into()];

    oidc::run_e1_through_e8(
        oidc,
        async {
            let (t, sub) = mint_client_credentials(&iss, &cust_id, &cust_sec).await?;
            Ok((t, sub))
        },
        async {
            let (t, sub) = mint_client_credentials(&iss, &adm_id, &adm_sec).await?;
            Ok((t, sub))
        },
    )
    .await;
}

async fn mint_client_credentials(
    issuer: &str,
    client_id: &str,
    client_secret: &str,
) -> Result<(String, String), String> {
    let token_url = format!(
        "{}/protocol/openid-connect/token",
        issuer.trim_end_matches('/')
    );
    let body = format!(
        "grant_type=client_credentials&client_id={}&client_secret={}",
        urlencoding(client_id),
        urlencoding(client_secret)
    );
    let resp = reqwest::Client::new()
        .post(&token_url)
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
    let sub = decode_sub(&token).ok_or("no sub in token")?;
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
