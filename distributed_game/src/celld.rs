//! A zone server's celld clients: [`CelldFactSink`] (facts as commands to
//! the durable aggregates in celld cells, game-services decision D3: the
//! [`CommandFactSink`](crate::command_sink::CommandFactSink) over a
//! remote-only `CelldCommandHost`), [`CellZoneOwnership`] (zone leases) and
//! [`CellZoneStateStore`] (hibernation snapshots).

pub use crate::command_sink::{CelldFactSink, FactRoute, DEFAULT_CELLD_FACT_TIMEOUT};

use std::time::Duration;

use distributed::cell_host::{
    InternalHttpSecret, CELL_INTERNAL_SECRET_HEADER, CELL_PRINCIPAL_PARTITION_HEADER,
    CELL_SERVICE_ID_HEADER,
};
use distributed::USER_ID_KEY;
use serde_json::{json, Value};

use crate::facts::BoxFuture;

/// Path segments are non-empty, short, and printable.
fn valid_segment(segment: &str) -> bool {
    !segment.is_empty()
        && segment.len() <= 256
        && segment.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
}

fn encode_segment(segment: &str) -> String {
    let mut encoded = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~' | b':') {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

/// [`ZoneOwnership`](crate::world::ZoneOwnership) over celld
/// `ZoneLease:{zone_id}` cells ([`crate::world::zone_lease`]): zone servers
/// acquire, renew and release over the internal path, and the cell's alarm
/// declares expiry (`zone.lease_expired`) when a holder stops renewing, so
/// services see a crashed zone server.
///
/// Every call uses a fresh command id. A retried `acquire` by the same
/// holder returns its existing claim, so an ambiguous response is safe.
#[derive(Clone)]
pub struct CellZoneOwnership {
    client: reqwest::Client,
    base: String,
    secret: InternalHttpSecret,
    service_id: String,
    host: String,
    kind: String,
}

/// Cell kind (URL segment) of zone-lease cells.
pub const ZONE_LEASE_KIND: &str = "zone-lease";

impl CellZoneOwnership {
    /// Ownership for zone server `host` (its internal identity) against
    /// `celld_url`, under command-ledger `service_id`.
    pub fn new(
        celld_url: impl Into<String>,
        secret: InternalHttpSecret,
        service_id: impl Into<String>,
        host: impl Into<String>,
    ) -> Result<Self, String> {
        let base = celld_url.into().trim_end_matches('/').to_string();
        if !(base.starts_with("http://") || base.starts_with("https://")) {
            return Err(format!("celld URL must be http(s): {base}"));
        }
        let service_id = service_id.into();
        let host = host.into();
        if service_id.trim().is_empty() || host.trim().is_empty() {
            return Err("cell zone ownership needs a service id and a host identity".into());
        }
        let client = reqwest::Client::builder()
            .timeout(DEFAULT_CELLD_FACT_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| format!("celld ownership client: {error}"))?;
        Ok(Self {
            client,
            base,
            secret,
            service_id,
            host,
            kind: ZONE_LEASE_KIND.to_string(),
        })
    }

    async fn call(
        &self,
        zone_id: &str,
        command: &str,
        input: Value,
    ) -> Result<crate::world::zone_lease::ClaimPayload, crate::world::OwnershipError> {
        use crate::world::zone_lease::ZONE_HOST_ROLE;
        use crate::world::OwnershipError;
        if !valid_segment(zone_id) {
            return Err(OwnershipError::Backend(format!(
                "invalid zone id `{zone_id}`"
            )));
        }
        let url = format!(
            "{}/{}/{}/{}",
            self.base,
            encode_segment(&self.kind),
            encode_segment(zone_id),
            encode_segment(command)
        );
        let response = self
            .client
            .post(&url)
            .header(CELL_INTERNAL_SECRET_HEADER, self.secret.header_value())
            .header(CELL_SERVICE_ID_HEADER, &self.service_id)
            .header(CELL_PRINCIPAL_PARTITION_HEADER, &self.host)
            .header(USER_ID_KEY, &self.host)
            .header(distributed::microsvc::ROLE_KEY, ZONE_HOST_ROLE)
            .json(&json!({
                "commandId": uuid::Uuid::now_v7().hyphenated().to_string(),
                "input": input,
            }))
            .send()
            .await
            .map_err(|error| OwnershipError::Backend(format!("celld unreachable: {error}")))?;
        let status = response.status().as_u16();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        if (200..300).contains(&status) {
            return serde_json::from_value(body.get("payload").cloned().unwrap_or(Value::Null))
                .map_err(|error| OwnershipError::Backend(format!("bad claim payload: {error}")));
        }
        let message = body.get("error").and_then(Value::as_str).unwrap_or("");
        Err(match crate::world::zone_lease::ownership_error(message) {
            OwnershipError::Backend(message) => {
                OwnershipError::Backend(format!("HTTP {status}: {message}"))
            }
            error => error,
        })
    }
}

fn claim_of(payload: crate::world::zone_lease::ClaimPayload) -> crate::world::ZoneClaim {
    crate::world::ZoneClaim {
        zone_id: payload.zone_id,
        holder: payload.holder,
        token: payload.token,
        expires_at: std::time::SystemTime::UNIX_EPOCH
            + Duration::from_millis(payload.expires_at_ms),
    }
}

fn ttl_ms(ttl: Duration) -> u64 {
    u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX).max(1)
}

impl crate::world::ZoneOwnership for CellZoneOwnership {
    fn acquire<'a>(
        &'a self,
        zone_id: &'a str,
        holder: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<crate::world::ZoneClaim, crate::world::OwnershipError>> {
        Box::pin(async move {
            self.call(
                zone_id,
                crate::world::zone_lease::ACQUIRE,
                json!({ "zone_id": zone_id, "holder": holder, "ttl_ms": ttl_ms(ttl) }),
            )
            .await
            .map(claim_of)
        })
    }

    fn renew<'a>(
        &'a self,
        claim: &'a crate::world::ZoneClaim,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<crate::world::ZoneClaim, crate::world::OwnershipError>> {
        Box::pin(async move {
            self.call(
                &claim.zone_id,
                crate::world::zone_lease::RENEW,
                json!({
                    "zone_id": claim.zone_id,
                    "holder": claim.holder,
                    "token": claim.token,
                    "ttl_ms": ttl_ms(ttl),
                }),
            )
            .await
            .map(claim_of)
        })
    }

    fn release<'a>(&'a self, claim: &'a crate::world::ZoneClaim) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let _ = self
                .call(
                    &claim.zone_id,
                    crate::world::zone_lease::RELEASE,
                    json!({ "zone_id": claim.zone_id, "holder": claim.holder, "token": claim.token }),
                )
                .await;
        })
    }
}

/// [`ZoneStateStore`](crate::world::ZoneStateStore) over celld
/// `ZoneState:{zone_id}` cells ([`crate::world::zone_state`]): hibernation
/// snapshots of a zone server live in one celld cell per zone (inline below
/// [`ZONE_STATE_INLINE_MAX`](crate::world::zone_state::ZONE_STATE_INLINE_MAX),
/// in celld's object storage above it), fenced by the ownership token.
///
/// Every write uses a fresh command id: a retried save under the same token
/// is accepted again (equal tokens are allowed), so an ambiguous response is
/// safe.
#[derive(Clone)]
pub struct CellZoneStateStore {
    client: reqwest::Client,
    base: String,
    secret: InternalHttpSecret,
    service_id: String,
    host: String,
}

/// Cell kind (URL segment) of zone-state cells.
pub const ZONE_STATE_KIND: &str = "zone-state";

impl CellZoneStateStore {
    /// Store for zone server `host` against `celld_url`, under
    /// command-ledger `service_id`.
    pub fn new(
        celld_url: impl Into<String>,
        secret: InternalHttpSecret,
        service_id: impl Into<String>,
        host: impl Into<String>,
    ) -> Result<Self, String> {
        let base = celld_url.into().trim_end_matches('/').to_string();
        if !(base.starts_with("http://") || base.starts_with("https://")) {
            return Err(format!("celld URL must be http(s): {base}"));
        }
        let service_id = service_id.into();
        let host = host.into();
        if service_id.trim().is_empty() || host.trim().is_empty() {
            return Err("cell zone state needs a service id and a host identity".into());
        }
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| format!("celld zone-state client: {error}"))?;
        Ok(Self {
            client,
            base,
            secret,
            service_id,
            host,
        })
    }

    fn url(
        &self,
        zone_id: &str,
        command: Option<&str>,
    ) -> Result<String, crate::world::StateError> {
        if !valid_segment(zone_id) {
            return Err(crate::world::StateError::Backend(format!(
                "invalid zone id `{zone_id}`"
            )));
        }
        let mut url = format!(
            "{}/{}/{}",
            self.base,
            encode_segment(ZONE_STATE_KIND),
            encode_segment(zone_id)
        );
        if let Some(command) = command {
            url.push('/');
            url.push_str(&encode_segment(command));
        }
        Ok(url)
    }

    async fn command(
        &self,
        zone_id: &str,
        command: &str,
        input: Value,
    ) -> Result<(), crate::world::StateError> {
        use crate::world::zone_lease::ZONE_HOST_ROLE;
        use crate::world::StateError;
        let url = self.url(zone_id, Some(command))?;
        let token = input.get("token").and_then(Value::as_u64).unwrap_or(0);
        let response = self
            .client
            .post(&url)
            .header(CELL_INTERNAL_SECRET_HEADER, self.secret.header_value())
            .header(CELL_SERVICE_ID_HEADER, &self.service_id)
            .header(CELL_PRINCIPAL_PARTITION_HEADER, &self.host)
            .header(USER_ID_KEY, &self.host)
            .header(distributed::microsvc::ROLE_KEY, ZONE_HOST_ROLE)
            .json(&json!({
                "commandId": uuid::Uuid::now_v7().hyphenated().to_string(),
                "input": input,
            }))
            .send()
            .await
            .map_err(|error| StateError::Backend(format!("celld unreachable: {error}")))?;
        let status = response.status().as_u16();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        if (200..300).contains(&status) {
            return Ok(());
        }
        let message = body.get("error").and_then(Value::as_str).unwrap_or("");
        match crate::world::zone_state::stale_current(message) {
            Some(current) => Err(StateError::Stale {
                presented: token,
                current,
            }),
            None => Err(StateError::Backend(format!("HTTP {status}: {message}"))),
        }
    }
}

impl crate::world::ZoneStateStore for CellZoneStateStore {
    fn load<'a>(
        &'a self,
        zone_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<crate::world::StoredZoneState>, crate::world::StateError>>
    {
        use crate::world::StateError;
        Box::pin(async move {
            use base64::Engine as _;
            let url = self.url(zone_id, None)?;
            let response = self
                .client
                .get(&url)
                .header(CELL_INTERNAL_SECRET_HEADER, self.secret.header_value())
                .send()
                .await
                .map_err(|error| StateError::Backend(format!("celld unreachable: {error}")))?;
            let status = response.status().as_u16();
            let body: Value = response.json().await.unwrap_or(Value::Null);
            if !(200..300).contains(&status) {
                return Err(StateError::Backend(format!("HTTP {status}: {body}")));
            }
            let Some(state) = body.get("state").filter(|state| !state.is_null()) else {
                return Ok(None);
            };
            let field = |name: &str| {
                state
                    .get(name)
                    .ok_or_else(|| StateError::Backend(format!("zone state without `{name}`")))
            };
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(field("state_b64")?.as_str().unwrap_or_default())
                .map_err(|_| StateError::Backend("zone state is not base64".into()))?;
            Ok(Some(crate::world::StoredZoneState {
                token: field("token")?.as_u64().unwrap_or(0),
                content_version: field("content_version")?
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                state: bytes,
            }))
        })
    }

    fn save<'a>(
        &'a self,
        zone_id: &'a str,
        state: &'a crate::world::StoredZoneState,
    ) -> BoxFuture<'a, Result<(), crate::world::StateError>> {
        Box::pin(async move {
            use base64::Engine as _;
            self.command(
                zone_id,
                crate::world::zone_state::SAVE,
                json!({
                    "zone_id": zone_id,
                    "token": state.token,
                    "content_version": state.content_version,
                    "state_b64": base64::engine::general_purpose::STANDARD.encode(&state.state),
                }),
            )
            .await
        })
    }

    fn delete<'a>(
        &'a self,
        zone_id: &'a str,
        token: u64,
    ) -> BoxFuture<'a, Result<(), crate::world::StateError>> {
        Box::pin(async move {
            self.command(
                zone_id,
                crate::world::zone_state::DELETE,
                json!({ "zone_id": zone_id, "token": token }),
            )
            .await
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::facts::Fact;
    use crate::stamp::FactStamp;
    use distributed::USER_ID_KEY;
    use serde_json::json;

    #[test]
    fn the_cell_rebuilds_the_trusted_fact_session_from_the_headers() {
        let stamp = FactStamp {
            zone_id: "pit-1".into(),
            tick: 42,
            content_version: "c1".into(),
            emitted_at_ms: 1_700_000_000_000,
            activation: 3,
            config_version: "cfg-2".into(),
            seq: 5,
        };
        let fact = Fact::trusted(
            "k",
            "crawler.record_kill",
            json!({}),
            Some("alice".into()),
            stamp.clone(),
        );
        // Exactly the headers `commit` sends (besides the internal ones).
        let mut headers = std::collections::HashMap::new();
        headers.insert(USER_ID_KEY.to_string(), "alice".to_string());
        for (name, value) in fact.stamp().session_vars() {
            headers.insert(name, value);
        }
        let session =
            crate::cell::fact_session_from_headers(|name| headers.get(name).cloned()).unwrap();
        assert_eq!(session.user_id(), Some("alice"));
        assert_eq!(crate::cell::session_stamp(&session), Some(stamp));
        headers.remove("x-game-tick");
        assert!(
            crate::cell::fact_session_from_headers(|name| headers.get(name).cloned()).is_none()
        );
        headers.insert("x-game-tick".into(), "42".into());
        headers.remove(USER_ID_KEY);
        assert!(
            crate::cell::fact_session_from_headers(|name| headers.get(name).cloned()).is_none()
        );
    }
}
