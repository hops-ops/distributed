//! Host-to-host admin calls (child events, transfer records, barrier
//! phases, zone messages): JSON over HTTP with `reqwest`, authenticated as
//! internal traffic ([`CELL_INTERNAL_SECRET_HEADER`] carrying the hosts'
//! shared [`InternalHttpSecret`]).

use std::sync::OnceLock;
use std::time::Duration;

use distributed::microsvc::cell_host::{InternalHttpSecret, CELL_INTERNAL_SECRET_HEADER};
use serde_json::Value;

/// POST `body` to `{admin_url}{path}`; returns the status and the JSON
/// body (`Null` when empty).
pub(crate) async fn post_json(
    admin_url: &str,
    path: &str,
    secret: &InternalHttpSecret,
    body: &Value,
    timeout: Duration,
) -> Result<(u16, Value), String> {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    let url = format!("{}{path}", admin_url.trim_end_matches('/'));
    let response = CLIENT
        .get_or_init(reqwest::Client::new)
        .post(&url)
        .header(CELL_INTERNAL_SECRET_HEADER, secret.header_value())
        .json(body)
        .timeout(timeout)
        .send()
        .await
        .map_err(|e| format!("peer {admin_url}: {e}"))?;
    let status = response.status().as_u16();
    let bytes = response
        .bytes()
        .await
        .map_err(|e| format!("peer {admin_url}: {e}"))?;
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).map_err(|e| format!("peer {admin_url}: {e}"))?
    };
    Ok((status, value))
}
