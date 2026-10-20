//! [`CelldFactSink`]: a zone server's facts, sent over the network to the
//! durable aggregates in celld cells (game-services decision D3).
//!
//! One sink per zone server routes every fact by its command
//! ([`FactCommand::COMMAND`](crate::FactCommand::COMMAND)) to a registered
//! [`CelldFactRoute`]: the cell kind and a shard function of the fact, giving
//! `POST {CELLD_URL}/{kind}/{shard}/{command}` (for example
//! `Crawler:{character}` or `Match:{match_id}`). Player lifecycle facts
//! (`zone.*`, [`crate::lifecycle`]) route to the presence cell of the
//! player's account, never to a gameplay cell
//! ([`CelldFactSink::lifecycle_route`]).
//!
//! - **Exactly one commit per key.** The command id is
//!   [`command_id_for`]`(service_id, fact key)`, so a retry after a timeout
//!   replays through the cell's command ledger (`replayed` receipt) instead
//!   of committing twice.
//! - **Trusted identity over the internal path only.** Every request carries
//!   `DISTRIBUTED_INTERNAL_SECRET`; the principal (bound by the host from the
//!   verified ticket) travels as `x-user-id` and the command-ledger principal
//!   partition, and the [`FactStamp`](crate::FactStamp) as `x-game-*`
//!   headers. The cell rebuilds the session with
//!   [`fact_session_from_headers`](crate::cell::fact_session_from_headers).
//! - **No local durable fallback.** celld slow or unreachable, a 5xx, or a
//!   command still in progress is [`FactError::Retryable`]: the fact stays in
//!   the zone's [`FactQueue`](crate::FactQueue) backlog and the worker backs
//!   off (use [`RetryPolicy::until_settled`](crate::RetryPolicy::until_settled)
//!   so an outage never drops a fact); a full queue surfaces to the zone as
//!   [`EmitError::Backpressure`](crate::EmitError::Backpressure). An unrouted
//!   command and a rejected command are [`FactError::Permanent`].

use std::sync::Arc;
use std::time::Duration;

use distributed::cell_host::{
    InternalHttpSecret, CELL_INTERNAL_SECRET_HEADER, CELL_PRINCIPAL_PARTITION_HEADER,
    CELL_SERVICE_ID_HEADER,
};
use distributed::USER_ID_KEY;
use serde_json::{json, Value};

use crate::cell::command_id_for;
use crate::facts::{BoxFuture, Fact, FactCommand, FactError, FactReceipt, FactSink};
use crate::lifecycle::LIFECYCLE_COMMANDS;

/// Default per-request timeout.
pub const DEFAULT_CELLD_FACT_TIMEOUT: Duration = Duration::from_secs(10);

type ShardFn = Arc<dyn Fn(&Fact, &str) -> Option<String> + Send + Sync>;

/// Where facts of one command go: a celld cell kind and a shard function.
#[derive(Clone)]
pub struct CelldFactRoute {
    command: String,
    kind: String,
    shard: ShardFn,
}

impl CelldFactRoute {
    /// Route `command` to `{kind}/{shard(fact, principal)}`. `principal` is the
    /// fact's verified principal, or the sink's system principal for zone
    /// facts. Return `None` when the fact cannot be routed (settles
    /// `Permanent`).
    pub fn new(
        command: impl Into<String>,
        kind: impl Into<String>,
        shard: impl Fn(&Fact, &str) -> Option<String> + Send + Sync + 'static,
    ) -> Self {
        Self {
            command: command.into(),
            kind: kind.into(),
            shard: Arc::new(shard),
        }
    }

    /// Route the typed fact `C` to the cell of its principal (one cell per
    /// player: `PlayerProgress:{character}`, `Crawler:{principal}`).
    pub fn by_principal<C: FactCommand>(kind: impl Into<String>) -> Self {
        Self::new(C::COMMAND, kind, |_, principal| Some(principal.to_string()))
    }

    /// Route the typed fact `C` by a field of its input (one cell per match:
    /// `Match:{match_id}` with `field = "match_id"`).
    pub fn by_input_field<C: FactCommand>(kind: impl Into<String>, field: &'static str) -> Self {
        Self::new(C::COMMAND, kind, move |fact, _| {
            match fact.input().get(field)? {
                Value::String(value) => Some(value.clone()),
                Value::Number(value) => Some(value.to_string()),
                _ => None,
            }
        })
    }

    /// The routed command.
    pub fn command(&self) -> &str {
        &self.command
    }

    /// The cell kind (URL segment).
    pub fn kind(&self) -> &str {
        &self.kind
    }
}

/// Fact sink posting each fact to its celld cell over the internal path.
#[derive(Clone)]
pub struct CelldFactSink {
    client: reqwest::Client,
    base: String,
    secret: InternalHttpSecret,
    service_id: String,
    system_principal: String,
    routes: Vec<CelldFactRoute>,
}

impl CelldFactSink {
    /// A sink for `celld_url` under command-ledger `service_id`; zone facts
    /// without a player use `system_principal`.
    pub fn new(
        celld_url: impl Into<String>,
        secret: InternalHttpSecret,
        service_id: impl Into<String>,
        system_principal: impl Into<String>,
    ) -> Result<Self, String> {
        Self::with_timeout(
            celld_url,
            secret,
            service_id,
            system_principal,
            DEFAULT_CELLD_FACT_TIMEOUT,
        )
    }

    /// As [`new`](Self::new) with an explicit per-request timeout.
    pub fn with_timeout(
        celld_url: impl Into<String>,
        secret: InternalHttpSecret,
        service_id: impl Into<String>,
        system_principal: impl Into<String>,
        timeout: Duration,
    ) -> Result<Self, String> {
        let base = celld_url.into().trim_end_matches('/').to_string();
        if !(base.starts_with("http://") || base.starts_with("https://")) {
            return Err(format!("celld URL must be http(s): {base}"));
        }
        let service_id = service_id.into();
        let system_principal = system_principal.into();
        if service_id.trim().is_empty() || system_principal.trim().is_empty() {
            return Err("celld fact sink needs a service id and a system principal".into());
        }
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| format!("celld fact client: {error}"))?;
        Ok(Self {
            client,
            base,
            secret,
            service_id,
            system_principal,
            routes: Vec::new(),
        })
    }

    /// Add a route. A later route for the same command replaces the earlier.
    pub fn route(mut self, route: CelldFactRoute) -> Self {
        self.routes
            .retain(|existing| existing.command != route.command);
        self.routes.push(route);
        self
    }

    /// Route every player lifecycle fact (`zone.*`) to the presence cell
    /// `{kind}/{principal}` (the player's account), not to gameplay cells.
    pub fn lifecycle_route(mut self, kind: impl Into<String>) -> Self {
        let kind = kind.into();
        for command in LIFECYCLE_COMMANDS {
            self = self.route(CelldFactRoute::new(
                command,
                kind.clone(),
                |_, principal| Some(principal.to_string()),
            ));
        }
        self
    }

    /// The route a fact would take, if any.
    pub fn route_for(&self, command: &str) -> Option<&CelldFactRoute> {
        self.routes.iter().find(|route| route.command == command)
    }

    /// The request a fact becomes: `(url, command id)`.
    pub fn target(&self, fact: &Fact) -> Result<(String, String), FactError> {
        let route = self.route_for(fact.command()).ok_or_else(|| {
            FactError::Permanent(format!(
                "no celld route for fact command `{}`",
                fact.command()
            ))
        })?;
        let principal = fact.principal().unwrap_or(&self.system_principal);
        let shard = (route.shard)(fact, principal)
            .filter(|shard| valid_segment(shard))
            .ok_or_else(|| {
                FactError::Permanent(format!(
                    "fact `{}` has no valid `{}` shard",
                    fact.key(),
                    route.kind
                ))
            })?;
        Ok((
            format!(
                "{}/{}/{}/{}",
                self.base,
                encode_segment(&route.kind),
                encode_segment(&shard),
                encode_segment(fact.command())
            ),
            command_id_for(&self.service_id, fact.key()),
        ))
    }
}

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

fn classify(status: u16, body: &Value) -> FactError {
    let code = body.get("code").and_then(Value::as_str).unwrap_or("");
    let message = body
        .get("error")
        .and_then(Value::as_str)
        .unwrap_or("celld rejected the fact");
    let detail = format!("HTTP {status} {code}: {message}");
    match (status, code) {
        (_, "COMMAND_IN_PROGRESS") | (_, "INTERNAL") => FactError::Retryable(detail),
        (408 | 429, _) => FactError::Retryable(detail),
        (500..=599, _) => FactError::Retryable(detail),
        _ => FactError::Permanent(detail),
    }
}

impl FactSink for CelldFactSink {
    fn commit<'a>(&'a self, fact: &'a Fact) -> BoxFuture<'a, Result<FactReceipt, FactError>> {
        Box::pin(async move {
            let (url, command_id) = self.target(fact)?;
            let principal = fact.principal().unwrap_or(&self.system_principal);
            let mut request = self
                .client
                .post(&url)
                .header(CELL_INTERNAL_SECRET_HEADER, self.secret.header_value())
                .header(CELL_SERVICE_ID_HEADER, &self.service_id)
                .header(CELL_PRINCIPAL_PARTITION_HEADER, principal)
                .header(USER_ID_KEY, principal)
                .json(&json!({ "commandId": command_id, "input": fact.input() }));
            for (name, value) in fact.stamp().session_vars() {
                request = request.header(name, value);
            }
            let response = request.send().await.map_err(|error| {
                FactError::Retryable(format!("celld unreachable at {url}: {error}"))
            })?;
            let status = response.status().as_u16();
            let body: Value = response.json().await.unwrap_or(Value::Null);
            if !(200..300).contains(&status) {
                return Err(classify(status, &body));
            }
            let replayed = body
                .pointer("/receipt/replayed")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            Ok(FactReceipt {
                payload: body.get("payload").cloned().unwrap_or(Value::Null),
                replayed,
            })
        })
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stamp::FactStamp;

    const SECRET: &str = "test-only-internal-secret-change-me-2026";

    #[derive(serde::Serialize)]
    struct Kill {
        mob: String,
    }
    impl FactCommand for Kill {
        const COMMAND: &'static str = "crawler.record_kill";
    }
    #[derive(serde::Serialize)]
    struct Result_ {
        match_id: String,
    }
    impl FactCommand for Result_ {
        const COMMAND: &'static str = "match.record_result";
    }

    fn sink() -> CelldFactSink {
        CelldFactSink::new(
            "http://127.0.0.1:9/",
            InternalHttpSecret::new(SECRET).unwrap(),
            "zone-server",
            "zone:pit-1",
        )
        .unwrap()
        .route(CelldFactRoute::by_principal::<Kill>("crawler"))
        .route(CelldFactRoute::by_input_field::<Result_>(
            "match", "match_id",
        ))
        .lifecycle_route("presence")
    }

    fn fact(command: &str, input: Value, principal: Option<&str>) -> Fact {
        Fact::trusted(
            format!("{command}:pit-1:1:0"),
            command,
            input,
            principal.map(str::to_string),
            FactStamp::default(),
        )
    }

    #[test]
    fn routes_by_command_to_kind_and_shard_with_a_stable_command_id() {
        let sink = sink();
        let kill = fact("crawler.record_kill", json!({ "mob": "m1" }), Some("alice"));
        let (url, id) = sink.target(&kill).unwrap();
        assert_eq!(url, "http://127.0.0.1:9/crawler/alice/crawler.record_kill");
        assert_eq!(id, command_id_for("zone-server", kill.key()));
        assert_eq!(sink.target(&kill).unwrap().1, id, "stable across retries");

        let result = fact("match.record_result", json!({ "match_id": "m/7" }), None);
        let (url, _) = sink.target(&result).unwrap();
        assert_eq!(
            url, "http://127.0.0.1:9/match/m%2F7/match.record_result",
            "a shard cannot add path segments"
        );
        let spaced = fact("match.record_result", json!({ "match_id": "m 7" }), None);
        assert!(matches!(sink.target(&spaced), Err(FactError::Permanent(_))));

        for command in LIFECYCLE_COMMANDS {
            let lifecycle = fact(command, json!({ "zone": "pit-1" }), Some("alice"));
            let (url, _) = sink.target(&lifecycle).unwrap();
            assert_eq!(url, format!("http://127.0.0.1:9/presence/alice/{command}"));
        }
    }

    #[test]
    fn unknown_commands_and_unroutable_facts_are_permanent() {
        let sink = sink();
        let unknown = fact("guild.whatever", json!({}), Some("alice"));
        assert!(matches!(
            sink.target(&unknown),
            Err(FactError::Permanent(_))
        ));
        let missing = fact("match.record_result", json!({}), None);
        assert!(matches!(
            sink.target(&missing),
            Err(FactError::Permanent(_))
        ));
    }

    #[test]
    fn classifies_cell_responses() {
        assert!(matches!(
            classify(409, &json!({ "code": "COMMAND_IN_PROGRESS" })),
            FactError::Retryable(_)
        ));
        assert!(matches!(
            classify(503, &Value::Null),
            FactError::Retryable(_)
        ));
        assert!(matches!(
            classify(500, &json!({ "code": "INTERNAL" })),
            FactError::Retryable(_)
        ));
        assert!(matches!(
            classify(400, &json!({ "code": "BAD_REQUEST" })),
            FactError::Permanent(_)
        ));
        assert!(matches!(
            classify(401, &json!({ "code": "UNAUTHORIZED" })),
            FactError::Permanent(_)
        ));
        assert!(matches!(
            classify(422, &json!({ "code": "REJECTED" })),
            FactError::Permanent(_)
        ));
    }

    #[tokio::test]
    async fn an_unreachable_celld_is_retryable() {
        let sink = sink();
        let kill = fact("crawler.record_kill", json!({ "mob": "m1" }), Some("alice"));
        assert!(matches!(
            sink.commit(&kill).await,
            Err(FactError::Retryable(_))
        ));
    }

    #[test]
    fn the_cell_rebuilds_the_trusted_fact_session_from_the_headers() {
        let stamp = FactStamp {
            zone_id: "pit-1".into(),
            tick: 42,
            content_version: "c1".into(),
            emitted_at_ms: 1_700_000_000_000,
            activation: 3,
            config_version: "cfg-2".into(),
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

    #[test]
    fn rejects_bad_configuration() {
        let secret = || InternalHttpSecret::new(SECRET).unwrap();
        assert!(CelldFactSink::new("ftp://x", secret(), "s", "p").is_err());
        assert!(CelldFactSink::new("http://x", secret(), " ", "p").is_err());
    }
}
