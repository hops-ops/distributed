//! Zitadel Management API scrape: configuration, listing, mapping, and the
//! background loop that dispatches `zitadel.scrape.v1`.
//!
//! Actions cover the happy path. Scrape reconciles users we never got events for
//! (Action downtime, misconfig, historical backfill).

use std::env;
use std::sync::Arc;
use std::time::Duration;

use distributed::microsvc::Service;
use serde::Deserialize;
use serde_json::{json, Value};

use super::map::{user_profile, MappedDelivery, HUMAN_DEACTIVATED, HUMAN_UPDATED, MACHINE_CREATED};

/// Env: Management API base (no trailing slash). Falls back to `OIDC_ISSUER`.
pub const API_URL_ENV: &str = "ZITADEL_API_URL";
/// Env: PAT / service user token (same as Login V2 `ZITADEL_SERVICE_USER_TOKEN`).
pub const TOKEN_ENV: &str = "ZITADEL_SERVICE_USER_TOKEN";
/// Env: scrape interval seconds. `0` or unset with no token → disabled.
/// Default when token present: `60`.
pub const INTERVAL_ENV: &str = "ZITADEL_SCRAPE_INTERVAL_SECS";
/// Env: run one scrape immediately on process start (`1`/`true`). Default on when configured.
pub const ON_START_ENV: &str = "ZITADEL_SCRAPE_ON_START";

#[derive(Debug, Clone)]
pub struct ZitadelScrapeConfig {
    pub api_base: String,
    pub token: String,
    pub interval: Duration,
    pub on_start: bool,
    pub page_size: u32,
}

impl ZitadelScrapeConfig {
    /// Load from env. Returns `None` when token or API base is missing, or interval is 0
    /// with scrape explicitly disabled.
    pub fn from_env() -> Option<Self> {
        let token = env::var(TOKEN_ENV)
            .or_else(|_| env::var("ZITADEL_MANAGEMENT_PAT"))
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())?;

        let api_base = env::var(API_URL_ENV)
            .or_else(|_| env::var("OIDC_ISSUER"))
            .ok()
            .map(|s| s.trim().trim_end_matches('/').to_string())
            .filter(|s| !s.is_empty())?;

        let interval_secs: u64 = env::var(INTERVAL_ENV)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(60);
        if interval_secs == 0 {
            // Allow on-demand command only; no background loop.
            return Some(Self {
                api_base,
                token,
                interval: Duration::ZERO,
                on_start: false,
                page_size: 100,
            });
        }

        let on_start = !matches!(
            env::var(ON_START_ENV).ok().as_deref().map(str::trim),
            Some("0") | Some("false") | Some("FALSE") | Some("off")
        );

        Some(Self {
            api_base,
            token,
            interval: Duration::from_secs(interval_secs),
            on_start,
            page_size: 100,
        })
    }

    pub fn background_enabled(&self) -> bool {
        !self.interval.is_zero()
    }
}

/// List users from the Zitadel Management API as provider deliveries:
/// how many were listed, and the ones with a usable id.
pub async fn list_mapped_users(
    cfg: &ZitadelScrapeConfig,
) -> Result<(usize, Vec<MappedDelivery>), String> {
    let users = list_all_users(cfg).await?;
    Ok((
        users.len(),
        users.iter().filter_map(map_mgmt_user).collect(),
    ))
}

#[derive(Debug, Clone, Deserialize)]
struct SearchResponse {
    #[serde(default)]
    result: Vec<MgmtUser>,
}

#[derive(Debug, Clone, Deserialize)]
struct MgmtUser {
    id: Option<String>,
    #[serde(default, rename = "userName")]
    user_name: Option<String>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    human: Option<MgmtHuman>,
    #[serde(default)]
    machine: Option<MgmtMachine>,
    #[serde(default, rename = "changeDate")]
    change_date: Option<String>,
    #[serde(default, rename = "details")]
    details: Option<MgmtDetails>,
}

#[derive(Debug, Clone, Deserialize)]
struct MgmtDetails {
    #[serde(default, rename = "changeDate")]
    change_date: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct MgmtHuman {
    #[serde(default)]
    profile: Option<MgmtProfile>,
    #[serde(default)]
    email: Option<MgmtEmail>,
}

#[derive(Debug, Clone, Deserialize)]
struct MgmtProfile {
    #[serde(default, rename = "displayName")]
    display_name: Option<String>,
    #[serde(default, rename = "firstName")]
    first_name: Option<String>,
    #[serde(default, rename = "lastName")]
    last_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct MgmtEmail {
    #[serde(default)]
    email: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct MgmtMachine {
    #[serde(default)]
    name: Option<String>,
}

async fn list_all_users(cfg: &ZitadelScrapeConfig) -> Result<Vec<MgmtUser>, String> {
    let client = reqwest::Client::new();
    let mut offset: u64 = 0;
    let mut all = Vec::new();

    loop {
        let body = json!({
            "query": {
                "offset": offset.to_string(),
                "limit": cfg.page_size,
                "asc": true
            },
            "sortingColumn": "USER_FIELD_NAME_USER_NAME",
            "queries": []
        });
        let url = format!("{}/management/v1/users/_search", cfg.api_base);
        let resp = client
            .post(&url)
            .header("Authorization", format!("Bearer {}", cfg.token))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("zitadel search request: {e}"))?;

        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| format!("zitadel search body: {e}"))?;
        if !status.is_success() {
            return Err(format!("zitadel search HTTP {status}: {text}"));
        }
        let page: SearchResponse = serde_json::from_str(&text)
            .map_err(|e| format!("zitadel search json: {e}; body={text}"))?;
        let n = page.result.len();
        all.extend(page.result);
        if n < cfg.page_size as usize {
            break;
        }
        offset += n as u64;
        if offset > 10_000 {
            break; // safety
        }
    }
    Ok(all)
}

/// Map one Management API user row → provider bus delivery (or None if unusable).
pub fn map_management_user(raw: &Value) -> Option<MappedDelivery> {
    let user: MgmtUser = serde_json::from_value(raw.clone()).ok()?;
    map_mgmt_user(&user)
}

fn map_mgmt_user(user: &MgmtUser) -> Option<MappedDelivery> {
    let id = user.id.as_deref()?.trim();
    if id.is_empty() {
        return None;
    }
    let state = user.state.as_deref().unwrap_or("USER_STATE_ACTIVE");
    let is_machine = user.machine.is_some() && user.human.is_none();
    let deactivated = state.contains("INACTIVE")
        || state.contains("LOCKED")
        || state.contains("SUSPEND")
        || state.contains("DELETED");

    let (email, display_name, user_kind) = if is_machine {
        let name = user
            .machine
            .as_ref()
            .and_then(|m| m.name.clone())
            .or_else(|| user.user_name.clone())
            .unwrap_or_else(|| id.to_string());
        (String::new(), name, "machine".to_string())
    } else {
        let human = user.human.as_ref();
        let email = human
            .and_then(|h| h.email.as_ref())
            .and_then(|e| e.email.clone())
            .unwrap_or_default();
        let display = human
            .and_then(|h| h.profile.as_ref())
            .and_then(|p| {
                p.display_name
                    .clone()
                    .or_else(|| match (&p.first_name, &p.last_name) {
                        (Some(f), Some(l)) => Some(format!("{f} {l}")),
                        (Some(f), None) => Some(f.clone()),
                        _ => None,
                    })
            })
            .or_else(|| user.user_name.clone())
            .unwrap_or_else(|| {
                if email.is_empty() {
                    id.to_string()
                } else {
                    email.clone()
                }
            });
        (email, display, "human".to_string())
    };

    let message_name = if is_machine {
        MACHINE_CREATED
    } else if deactivated {
        HUMAN_DEACTIVATED
    } else {
        // Reconcile as update — projector upserts; works for create + change.
        HUMAN_UPDATED
    };

    let change = user
        .change_date
        .clone()
        .or_else(|| user.details.as_ref().and_then(|d| d.change_date.clone()))
        .unwrap_or_else(|| "0".into());
    // Stable while the profile is unchanged: consumers can tell a repeat.
    let fingerprint = simple_fingerprint(&[&email, &display_name, state, &user_kind, &change]);
    let delivery_id = format!("zitadel-scrape:{id}:{fingerprint}");

    let profile = user_profile(
        message_name,
        id,
        user_kind,
        email,
        Some(display_name),
        // Scrape treats every listed identity as a directory member.
        "approved".into(),
    );

    Some(MappedDelivery {
        message_name: message_name.to_string(),
        delivery_id,
        profile,
    })
}

fn simple_fingerprint(parts: &[&str]) -> String {
    // FNV-1a 64 — stable, no extra deps.
    let mut hash: u64 = 0xcbf29ce484222325;
    for p in parts {
        for b in p.as_bytes() {
            hash ^= u64::from(*b);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

/// Background loop: optional immediate scrape, then every `cfg.interval`.
/// Each scrape is one `zitadel.scrape.v1` command on `service`.
pub fn spawn_scrape_loop(service: Arc<Service>, cfg: ZitadelScrapeConfig) {
    if !cfg.background_enabled() && !cfg.on_start {
        return;
    }
    tokio::spawn(async move {
        if cfg.on_start {
            scrape_once(&service, "start").await;
        }
        if !cfg.background_enabled() {
            return;
        }
        loop {
            tokio::time::sleep(cfg.interval).await;
            scrape_once(&service, "interval").await;
        }
    });
}

async fn scrape_once(service: &Service, trigger: &str) {
    match crate::handlers::ingestors::zitadel_scrape::scrape(service, trigger).await {
        Ok(report) => eprintln!("zitadel scrape ({trigger}): {report}"),
        // The operator's log: the full cause, not the client-facing message.
        Err(error) => eprintln!("zitadel scrape ({trigger}) failed: {error:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn maps_active_human() {
        let raw = json!({
            "id": "user-1",
            "userName": "alice",
            "state": "USER_STATE_ACTIVE",
            "human": {
                "profile": { "displayName": "Alice" },
                "email": { "email": "alice@e2e.local", "isEmailVerified": true }
            },
            "changeDate": "2026-01-01T00:00:00Z"
        });
        let m = map_management_user(&raw).expect("mapped");
        assert_eq!(m.message_name, HUMAN_UPDATED);
        assert_eq!(m.profile.user_id, "user-1");
        assert_eq!(m.profile.display_name, "Alice");
        assert_eq!(m.profile.email, "alice@e2e.local");
        assert_eq!(m.profile.approval_status, "approved");
        assert!(m.delivery_id.starts_with("zitadel-scrape:user-1:"));
    }

    #[test]
    fn maps_inactive_as_deactivated() {
        let raw = json!({
            "id": "user-2",
            "state": "USER_STATE_INACTIVE",
            "human": {
                "profile": { "displayName": "Bob" },
                "email": { "email": "bob@e2e.local" }
            }
        });
        let m = map_management_user(&raw).unwrap();
        assert_eq!(m.message_name, HUMAN_DEACTIVATED);
        assert_eq!(m.profile.status, "deactivated");
    }

    #[test]
    fn maps_machine() {
        let raw = json!({
            "id": "svc-1",
            "state": "USER_STATE_ACTIVE",
            "machine": { "name": "bot" }
        });
        let m = map_management_user(&raw).unwrap();
        assert_eq!(m.message_name, MACHINE_CREATED);
        assert_eq!(m.profile.user_kind, "machine");
    }

    #[test]
    fn same_profile_same_fingerprint() {
        let raw = json!({
            "id": "user-1",
            "state": "USER_STATE_ACTIVE",
            "human": {
                "profile": { "displayName": "Alice" },
                "email": { "email": "a@x.com" }
            },
            "changeDate": "t1"
        });
        let a = map_management_user(&raw).unwrap();
        let b = map_management_user(&raw).unwrap();
        assert_eq!(a.delivery_id, b.delivery_id);
    }
}
