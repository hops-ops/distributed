//! Minimal HTTP/1.1 JSON client for host-to-host admin calls (child events,
//! transfer records, barrier phases). One request per connection; every
//! request carries the shared admin secret header.

use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Header carrying the shared admin secret.
pub const ADMIN_SECRET_HEADER: &str = "x-game-admin-secret";

/// Constant-time byte comparison.
pub(crate) fn secret_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// POST `body` to `{admin_url}{path}`; returns status and JSON body.
pub async fn post_json(
    admin_url: &str,
    path: &str,
    secret: &str,
    body: &Value,
    timeout: Duration,
) -> Result<(u16, Value), String> {
    tokio::time::timeout(timeout, post_inner(admin_url, path, secret, body))
        .await
        .map_err(|_| format!("peer {admin_url} timed out"))?
}

async fn post_inner(
    admin_url: &str,
    path: &str,
    secret: &str,
    body: &Value,
) -> Result<(u16, Value), String> {
    let authority = admin_url
        .strip_prefix("http://")
        .ok_or_else(|| format!("unsupported admin url {admin_url}"))?
        .trim_end_matches('/');
    let mut stream = tokio::net::TcpStream::connect(authority)
        .await
        .map_err(|e| e.to_string())?;
    let payload = body.to_string();
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {authority}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\n{ADMIN_SECRET_HEADER}: {secret}\r\nConnection: close\r\n\r\n{payload}",
        payload.len()
    );
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .map_err(|e| e.to_string())?;
    let split = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or("malformed response")?;
    let head = String::from_utf8_lossy(&response[..split]);
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or("malformed status")?;
    let body = &response[split + 4..];
    let chunked = head.lines().any(|l| {
        l.to_ascii_lowercase()
            .starts_with("transfer-encoding: chunked")
    });
    let body = if chunked {
        dechunk(body)
    } else {
        body.to_vec()
    };
    let value = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).map_err(|e| e.to_string())?
    };
    Ok((status, value))
}

fn dechunk(mut body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(line_end) = body.windows(2).position(|w| w == b"\r\n") {
        let size = std::str::from_utf8(&body[..line_end])
            .ok()
            .and_then(|s| usize::from_str_radix(s.trim(), 16).ok())
            .unwrap_or(0);
        body = &body[line_end + 2..];
        if size == 0 || body.len() < size {
            break;
        }
        out.extend_from_slice(&body[..size]);
        body = body.get(size + 2..).unwrap_or_default();
    }
    out
}
