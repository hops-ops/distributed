//! [`CommandFactSink`]: a zone's facts dispatched as commands through any
//! [`FactHost`] — the same durable aggregates whether they are reached over
//! celld's internal HTTP path ([`CelldCommandHost`], feature `command-host`,
//! `POST {celld}/{kind}/{shard}/{command}`), object-to-object from a zone
//! running as a celld Durable Object
//! ([`CelldObjectHost`](crate::celld_zone::CelldObjectHost), feature
//! `celld-zone`, wasm32), or in an ordinary service over any repository
//! ([`LocalCommandHost`], feature `command-host`).
//!
//! One sink per zone (host) routes every fact by its command
//! ([`FactCommand::COMMAND`](crate::FactCommand::COMMAND)) to a registered
//! [`FactRoute`]: a cell kind and a shard function of the fact's input and
//! trusted session (principal + [`FactStamp`](crate::FactStamp)). A host
//! that addresses cells uses both; a service routes by command and its
//! handler picks the stream from the same session or input. Player
//! lifecycle facts (`zone.*`, [`crate::lifecycle`]) route to the presence
//! aggregate of the player's account ([`CommandFactSink::lifecycle_route`]).
//!
//! - **Exactly one commit per key.** The command id is
//!   [`command_id_for`]`(service_id, fact key)`, so a retry after a timeout
//!   replays through the aggregate's command ledger (`replayed` receipt).
//! - **Trusted identity.** The principal (bound by the zone host from the
//!   verified ticket) is the session's `x-user-id`; the stamp travels as
//!   `x-game-*` session variables (forwarded to cells as trusted metadata).
//! - **No local durable fallback.** A host that is slow or unreachable, a
//!   5xx, or a command still in progress is [`FactError::Retryable`]: the fact
//!   stays in the zone's queue (use
//!   [`RetryPolicy::until_settled`](crate::RetryPolicy::until_settled)). An
//!   unrouted or rejected command is [`FactError::Permanent`].

use std::sync::Arc;
#[cfg(feature = "command-host")]
use std::time::Duration;

#[cfg(feature = "command-host")]
use distributed::cell_host::{CelldCommandHost, InternalHttpSecret};
#[cfg(feature = "command-host")]
use distributed::command_dispatch::{CommandHost, LocalCommandHost};
#[cfg(feature = "command-host")]
use distributed::graphql::identity::VerifiedPrincipal;
#[cfg(feature = "command-host")]
use distributed::microsvc::CausalDispatchError;
use distributed::microsvc::Session;
use serde_json::Value;

use crate::cell::{command_id_for, session_stamp, trusted_session};
use crate::facts::{BoxFuture, Fact, FactCommand, FactError, FactReceipt, FactSink};
use crate::lifecycle::LIFECYCLE_COMMANDS;
#[cfg(feature = "command-host")]
use crate::stamp::SESSION_PREFIX;

type ShardFn = Arc<dyn Fn(&Value, &Session) -> Option<String> + Send + Sync>;

/// Where facts of one command go: an aggregate kind and a shard function of
/// the fact's input and trusted session.
#[derive(Clone)]
pub struct FactRoute {
    command: String,
    kind: String,
    shard: ShardFn,
}

impl FactRoute {
    /// Route `command` to `{kind}/{shard(input, session)}`. The session holds
    /// the fact's principal (`user_id()`) and stamp
    /// ([`session_stamp`]). Return `None` when the fact cannot be routed.
    pub fn new(
        command: impl Into<String>,
        kind: impl Into<String>,
        shard: impl Fn(&Value, &Session) -> Option<String> + Send + Sync + 'static,
    ) -> Self {
        Self {
            command: command.into(),
            kind: kind.into(),
            shard: Arc::new(shard),
        }
    }

    /// Route the typed fact `C` to the aggregate of its principal.
    pub fn by_principal<C: FactCommand>(kind: impl Into<String>) -> Self {
        Self::new(C::COMMAND, kind, |_, session| {
            session.user_id().map(str::to_string)
        })
    }

    /// Route the typed fact `C` by a field of its input.
    pub fn by_input_field<C: FactCommand>(kind: impl Into<String>, field: &'static str) -> Self {
        Self::new(C::COMMAND, kind, move |input, _| {
            match input.get(field)? {
                Value::String(value) => Some(value.clone()),
                Value::Number(value) => Some(value.to_string()),
                _ => None,
            }
        })
    }

    /// Route the typed fact `C` to the aggregate of the zone that emitted it.
    pub fn by_zone<C: FactCommand>(kind: impl Into<String>) -> Self {
        Self::new(C::COMMAND, kind, |_, session| {
            session_stamp(session).map(|stamp| stamp.zone_id)
        })
    }

    /// The routed command.
    pub fn command(&self) -> &str {
        &self.command
    }

    /// The aggregate kind (cell URL segment).
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// The shard of a fact with `input` and `session`.
    pub fn shard(&self, input: &Value, session: &Session) -> Option<String> {
        (self.shard)(input, session)
    }
}

/// One routed fact, ready for a [`FactHost`].
#[derive(Debug, Clone)]
pub struct FactCall<'a> {
    /// Durable command name.
    pub command: &'a str,
    /// Aggregate kind of the route.
    pub kind: &'a str,
    /// Validated shard (non-empty, printable ASCII, ≤ 256 bytes).
    pub shard: String,
    /// Deterministic command id ([`command_id_for`]).
    pub command_id: String,
    /// Command input.
    pub input: &'a Value,
    /// Verified principal of the fact (or the sink's system principal).
    pub principal: String,
    /// Trusted session: `x-user-id` + the `x-game-*` stamp.
    pub session: Session,
}

/// What a [`CommandFactSink`] dispatches through: anything that runs one
/// durable command idempotently by command id. Implemented for
/// [`CelldCommandHost`] and [`LocalCommandHost`] (feature `command-host`)
/// and for the celld object host (feature `celld-zone`).
pub trait FactHost: Send + Sync + 'static {
    /// Learn `route` (a host that addresses aggregates by kind and shard
    /// records it; one that routes by command ignores it).
    fn register_fact_route(&mut self, _route: &FactRoute) {}

    /// Run `call` once per command id. Classify failures: unreachable,
    /// 5xx, in progress → [`FactError::Retryable`]; anything else →
    /// [`FactError::Permanent`].
    fn dispatch_fact<'a>(
        &'a self,
        call: FactCall<'a>,
    ) -> BoxFuture<'a, Result<FactReceipt, FactError>>;

    /// Facts this host runs per [`dispatch_fact_batch`](Self::dispatch_fact_batch)
    /// at most (default 1). A host with a batch route returns more, and
    /// the sink's [`FactSink::max_batch`] follows it.
    fn max_fact_batch(&self) -> usize {
        1
    }

    /// Run `calls` (consecutive facts for one aggregate: same kind and
    /// shard) in order, under [`FactSink::commit_batch`]'s contract: the
    /// results end at the first retryable failure. The default runs them
    /// one by one; a host with a batch route sends them in one request.
    fn dispatch_fact_batch<'a>(
        &'a self,
        calls: Vec<FactCall<'a>>,
    ) -> BoxFuture<'a, Vec<Result<FactReceipt, FactError>>> {
        Box::pin(async move {
            let mut results = Vec::with_capacity(calls.len());
            for call in calls {
                let result = self.dispatch_fact(call).await;
                let stop = matches!(result, Err(FactError::Retryable(_)));
                results.push(result);
                if stop {
                    break;
                }
            }
            results
        })
    }
}

/// See the module docs.
pub struct CommandFactSink<H> {
    host: Arc<H>,
    service_id: String,
    system_principal: String,
    routes: Vec<FactRoute>,
}

impl<H> Clone for CommandFactSink<H> {
    fn clone(&self) -> Self {
        Self {
            host: self.host.clone(),
            service_id: self.service_id.clone(),
            system_principal: self.system_principal.clone(),
            routes: self.routes.clone(),
        }
    }
}

impl<H: FactHost> CommandFactSink<H> {
    /// A sink dispatching through `host` under command-ledger `service_id`;
    /// zone facts without a player use `system_principal`.
    pub fn new(
        host: H,
        service_id: impl Into<String>,
        system_principal: impl Into<String>,
    ) -> Result<Self, String> {
        let service_id = service_id.into();
        let system_principal = system_principal.into();
        if service_id.trim().is_empty() || system_principal.trim().is_empty() {
            return Err("a fact sink needs a service id and a system principal".into());
        }
        Ok(Self {
            host: Arc::new(host),
            service_id,
            system_principal,
            routes: Vec::new(),
        })
    }

    /// Add a route (before the sink is cloned). A later route for the same
    /// command replaces the earlier.
    pub fn route(mut self, route: FactRoute) -> Self {
        Arc::get_mut(&mut self.host)
            .expect("add fact routes before cloning the sink")
            .register_fact_route(&route);
        self.routes
            .retain(|existing| existing.command != route.command);
        self.routes.push(route);
        self
    }

    /// Route every player lifecycle fact (`zone.*`) to the presence
    /// aggregate `{kind}/{principal}` (the player's account).
    pub fn lifecycle_route(mut self, kind: impl Into<String>) -> Self {
        let kind = kind.into();
        for command in LIFECYCLE_COMMANDS {
            self = self.route(FactRoute::new(command, kind.clone(), |_, session| {
                session.user_id().map(str::to_string)
            }));
        }
        self
    }
}

impl<H> CommandFactSink<H> {
    /// The host.
    pub fn host(&self) -> &H {
        &self.host
    }

    /// The command-ledger service id.
    pub fn service_id(&self) -> &str {
        &self.service_id
    }

    /// The route a fact would take, if any.
    pub fn route_for(&self, command: &str) -> Option<&FactRoute> {
        self.routes.iter().find(|route| route.command == command)
    }

    /// `(principal, trusted session, command id)` of a routed fact.
    pub fn prepare(&self, fact: &Fact) -> Result<(String, Session, String), FactError> {
        let (_, _, principal, session, command_id) = self.resolve(fact)?;
        Ok((principal, session, command_id))
    }

    #[allow(clippy::type_complexity)]
    fn resolve(
        &self,
        fact: &Fact,
    ) -> Result<(&FactRoute, String, String, Session, String), FactError> {
        let route = self.route_for(fact.command()).ok_or_else(|| {
            FactError::Permanent(format!("no route for fact command `{}`", fact.command()))
        })?;
        let principal = fact
            .principal()
            .unwrap_or(&self.system_principal)
            .to_string();
        let session = trusted_session(&principal, fact);
        let shard = route
            .shard(fact.input(), &session)
            .filter(|shard| {
                !shard.is_empty()
                    && shard.len() <= 256
                    && shard.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
            })
            .ok_or_else(|| {
                FactError::Permanent(format!(
                    "fact `{}` has no valid `{}` shard",
                    fact.key(),
                    route.kind
                ))
            })?;
        Ok((
            route,
            shard,
            principal,
            session,
            command_id_for(&self.service_id, fact.key()),
        ))
    }
}

impl<H: FactHost> CommandFactSink<H> {
    fn call_for<'a>(&'a self, fact: &'a Fact) -> Result<FactCall<'a>, FactError> {
        let (route, shard, principal, session, command_id) = self.resolve(fact)?;
        Ok(FactCall {
            command: fact.command(),
            kind: route.kind(),
            shard,
            command_id,
            input: fact.input(),
            principal,
            session,
        })
    }
}

impl<H: FactHost> FactSink for CommandFactSink<H> {
    fn commit<'a>(&'a self, fact: &'a Fact) -> BoxFuture<'a, Result<FactReceipt, FactError>> {
        Box::pin(async move { self.host.dispatch_fact(self.call_for(fact)?).await })
    }

    fn max_batch(&self) -> usize {
        self.host.max_fact_batch()
    }

    /// Consecutive facts for the same aggregate go to the host together
    /// ([`FactHost::dispatch_fact_batch`]).
    fn commit_batch<'a>(
        &'a self,
        facts: &'a [Fact],
    ) -> BoxFuture<'a, Vec<Result<FactReceipt, FactError>>> {
        Box::pin(async move {
            let mut results = Vec::with_capacity(facts.len());
            let mut i = 0;
            while i < facts.len() {
                let call = match self.call_for(&facts[i]) {
                    Ok(call) => call,
                    Err(error) => {
                        results.push(Err(error));
                        i += 1;
                        continue;
                    }
                };
                let mut run = vec![call];
                let mut next = i + 1;
                while let Some(fact) = facts.get(next) {
                    match self.call_for(fact) {
                        Ok(call) if call.kind == run[0].kind && call.shard == run[0].shard => {
                            run.push(call);
                            next += 1;
                        }
                        _ => break,
                    }
                }
                let n = run.len();
                let batch: Vec<_> = self
                    .host
                    .dispatch_fact_batch(run)
                    .await
                    .into_iter()
                    .take(n)
                    .collect();
                let complete =
                    batch.len() == n && !matches!(batch.last(), Some(Err(FactError::Retryable(_))));
                results.extend(batch);
                if !complete {
                    break;
                }
                i = next;
            }
            results
        })
    }
}

/// celld slow, down or still running the command: keep the fact queued.
#[cfg(feature = "command-host")]
fn classify(error: CausalDispatchError) -> FactError {
    let detail = format!("{}: {error:?}", error.code());
    match &error {
        CausalDispatchError::InProgress | CausalDispatchError::Internal(_) => {
            FactError::Retryable(detail)
        }
        CausalDispatchError::Rejected { status, .. }
            if *status >= 500 || matches!(*status, 408 | 429) =>
        {
            FactError::Retryable(detail)
        }
        CausalDispatchError::Handler(handler) if handler.status_code() >= 500 => {
            FactError::Retryable(detail)
        }
        _ => FactError::Permanent(detail),
    }
}

/// Any [`CommandHost`]: `invoke` with the fact's trusted session.
#[cfg(feature = "command-host")]
fn invoke_command_host<'a, H: CommandHost>(
    host: &'a H,
    call: FactCall<'a>,
) -> BoxFuture<'a, Result<FactReceipt, FactError>> {
    Box::pin(async move {
        let result = host
            .invoke(
                call.command,
                &call.command_id,
                call.input.clone(),
                call.session,
                VerifiedPrincipal::from_trusted_transport(&call.principal),
                None,
            )
            .await
            .map_err(classify)?;
        Ok(FactReceipt {
            payload: result.payload().clone(),
            replayed: result.replayed(),
        })
    })
}

/// A service routes by command: its handler selects the stream.
#[cfg(feature = "command-host")]
impl FactHost for LocalCommandHost {
    fn dispatch_fact<'a>(
        &'a self,
        call: FactCall<'a>,
    ) -> BoxFuture<'a, Result<FactReceipt, FactError>> {
        invoke_command_host(self, call)
    }
}

/// celld addresses cells: `{kind}/{shard}/{command}`.
#[cfg(feature = "command-host")]
impl FactHost for CelldCommandHost {
    fn register_fact_route(&mut self, route: &FactRoute) {
        let shard = route.shard.clone();
        self.add_cell_route(route.command(), route.kind(), move |input, session| {
            shard(input, session)
        });
    }

    fn dispatch_fact<'a>(
        &'a self,
        call: FactCall<'a>,
    ) -> BoxFuture<'a, Result<FactReceipt, FactError>> {
        invoke_command_host(self, call)
    }
}

/// Default per-request timeout of [`CelldFactSink`].
#[cfg(feature = "command-host")]
pub const DEFAULT_CELLD_FACT_TIMEOUT: Duration = Duration::from_secs(10);

/// Facts into celld cells over the internal path.
#[cfg(feature = "command-host")]
pub type CelldFactSink = CommandFactSink<CelldCommandHost>;

#[cfg(feature = "command-host")]
impl CommandFactSink<CelldCommandHost> {
    /// A sink for the cells at `celld_url` under command-ledger
    /// `service_id`: a remote-only [`CelldCommandHost`] forwarding the fact
    /// stamp (`x-game-*`).
    pub fn celld(
        celld_url: impl Into<String>,
        secret: InternalHttpSecret,
        service_id: impl Into<String>,
        system_principal: impl Into<String>,
    ) -> Result<Self, String> {
        Self::celld_with_timeout(
            celld_url,
            secret,
            service_id,
            system_principal,
            DEFAULT_CELLD_FACT_TIMEOUT,
        )
    }

    /// As [`celld`](Self::celld) with an explicit per-request timeout.
    pub fn celld_with_timeout(
        celld_url: impl Into<String>,
        secret: InternalHttpSecret,
        service_id: impl Into<String>,
        system_principal: impl Into<String>,
        timeout: Duration,
    ) -> Result<Self, String> {
        let service_id = service_id.into();
        let host = CelldCommandHost::remote(celld_url, &service_id, secret, timeout)
            .map_err(|error| format!("celld command host: {error:?}"))?
            .forward_session_prefix(SESSION_PREFIX)
            .map_err(|error| format!("celld command host: {error:?}"))?;
        Self::new(host, service_id, system_principal)
    }
}

#[cfg(all(test, feature = "command-host"))]
mod tests {
    use super::*;
    use crate::stamp::FactStamp;
    use serde_json::json;

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
        CelldFactSink::celld(
            "http://127.0.0.1:9/",
            InternalHttpSecret::new(SECRET).unwrap(),
            "zone-server",
            "zone:pit-1",
        )
        .unwrap()
        .route(FactRoute::by_principal::<Kill>("crawler"))
        .route(FactRoute::by_input_field::<Result_>("match", "match_id"))
        .lifecycle_route("presence")
    }

    fn fact(command: &str, input: Value, principal: Option<&str>) -> Fact {
        Fact::trusted(
            format!("{command}:pit-1:1:0"),
            command,
            input,
            principal.map(str::to_string),
            FactStamp {
                zone_id: "pit-1".into(),
                ..FactStamp::default()
            },
        )
    }

    fn shard(sink: &CelldFactSink, fact: &Fact) -> Option<String> {
        let (_, session, _) = sink.prepare(fact).ok()?;
        sink.route_for(fact.command())?
            .shard(fact.input(), &session)
    }

    #[test]
    fn routes_by_command_to_kind_and_shard_with_a_stable_command_id() {
        let sink = sink();
        let kill = fact("crawler.record_kill", json!({ "mob": "m1" }), Some("alice"));
        let (principal, session, id) = sink.prepare(&kill).unwrap();
        assert_eq!(principal, "alice");
        assert_eq!(session.user_id(), Some("alice"));
        assert_eq!(session_stamp(&session).unwrap().zone_id, "pit-1");
        assert_eq!(id, command_id_for("zone-server", kill.key()));
        assert_eq!(sink.prepare(&kill).unwrap().2, id, "stable across retries");
        assert_eq!(sink.route_for(kill.command()).unwrap().kind(), "crawler");
        assert_eq!(shard(&sink, &kill).as_deref(), Some("alice"));

        let result = fact("match.record_result", json!({ "match_id": "m/7" }), None);
        assert_eq!(shard(&sink, &result).as_deref(), Some("m/7"));
        assert_eq!(
            sink.prepare(&result).unwrap().0,
            "zone:pit-1",
            "zone facts use the system principal"
        );
        let spaced = fact("match.record_result", json!({ "match_id": "m 7" }), None);
        assert!(matches!(
            sink.prepare(&spaced),
            Err(FactError::Permanent(_))
        ));

        for command in LIFECYCLE_COMMANDS {
            let lifecycle = fact(command, json!({ "zone": "pit-1" }), Some("alice"));
            assert_eq!(sink.route_for(command).unwrap().kind(), "presence");
            assert_eq!(shard(&sink, &lifecycle).as_deref(), Some("alice"));
        }
        let by_zone = FactRoute::by_zone::<Kill>("zone-log");
        let (_, session, _) = sink.prepare(&kill).unwrap();
        assert_eq!(
            by_zone.shard(&json!({}), &session).as_deref(),
            Some("pit-1")
        );
    }

    #[test]
    fn unknown_commands_and_unroutable_facts_are_permanent() {
        let sink = sink();
        let unknown = fact("guild.whatever", json!({}), Some("alice"));
        assert!(matches!(
            sink.prepare(&unknown),
            Err(FactError::Permanent(_))
        ));
        let missing = fact("match.record_result", json!({}), None);
        assert!(matches!(
            sink.prepare(&missing),
            Err(FactError::Permanent(_))
        ));
    }

    #[test]
    fn classifies_dispatch_errors() {
        let retryable = |error| matches!(classify(error), FactError::Retryable(_));
        assert!(retryable(CausalDispatchError::InProgress));
        assert!(retryable(CausalDispatchError::Internal("down".into())));
        assert!(retryable(CausalDispatchError::Rejected {
            code: "REJECTED",
            status: 503,
            message: String::new()
        }));
        assert!(retryable(CausalDispatchError::Rejected {
            code: "REJECTED",
            status: 429,
            message: String::new()
        }));
        assert!(!retryable(CausalDispatchError::BadRequest("bad".into())));
        assert!(!retryable(CausalDispatchError::Forbidden));
        assert!(!retryable(CausalDispatchError::Rejected {
            code: "UNAUTHORIZED",
            status: 401,
            message: String::new()
        }));
        assert!(!retryable(CausalDispatchError::Rejected {
            code: "REJECTED",
            status: 422,
            message: String::new()
        }));
    }

    #[tokio::test]
    async fn an_unreachable_host_is_retryable() {
        let sink = sink();
        let kill = fact("crawler.record_kill", json!({ "mob": "m1" }), Some("alice"));
        assert!(matches!(
            sink.commit(&kill).await,
            Err(FactError::Retryable(_))
        ));
    }

    #[test]
    fn rejects_bad_configuration() {
        let secret = || InternalHttpSecret::new(SECRET).unwrap();
        assert!(CelldFactSink::celld("ftp://x", secret(), "s", "p").is_err());
        assert!(CelldFactSink::celld("http://x", secret(), " ", "p").is_err());
        assert!(CelldFactSink::celld("http://x", secret(), "s", " ").is_err());
    }
}

#[cfg(test)]
mod batch_tests {
    use std::sync::Mutex;

    use serde_json::json;

    use super::*;

    /// Records each batch as `(shard, commands)`; `busy` answers retryable.
    #[derive(Default)]
    struct RecordingHost {
        batches: Mutex<Vec<(String, usize)>>,
        busy: Option<String>,
    }

    impl FactHost for RecordingHost {
        fn dispatch_fact<'a>(
            &'a self,
            _: FactCall<'a>,
        ) -> BoxFuture<'a, Result<FactReceipt, FactError>> {
            unreachable!("batches only")
        }

        fn dispatch_fact_batch<'a>(
            &'a self,
            calls: Vec<FactCall<'a>>,
        ) -> BoxFuture<'a, Vec<Result<FactReceipt, FactError>>> {
            Box::pin(async move {
                let shard = calls[0].shard.clone();
                self.batches.lock().unwrap().push((shard, calls.len()));
                let mut results = Vec::new();
                for call in &calls {
                    if self.busy.as_deref() == Some(call.input["n"].as_str().unwrap()) {
                        results.push(Err(FactError::Retryable("busy".into())));
                        break;
                    }
                    results.push(Ok(FactReceipt {
                        payload: Value::Null,
                        replayed: false,
                    }));
                }
                results
            })
        }
    }

    fn sink(host: RecordingHost) -> CommandFactSink<RecordingHost> {
        CommandFactSink::new(host, "zone-test", "zone:z1")
            .unwrap()
            .route(FactRoute::new("credit", "room", |input, _| {
                input.get("room")?.as_str().map(str::to_string)
            }))
    }

    fn facts(rooms: &[&str]) -> Vec<Fact> {
        rooms
            .iter()
            .enumerate()
            .map(|(i, room)| {
                let command = if room.is_empty() {
                    "unrouted"
                } else {
                    "credit"
                };
                Fact::new(
                    format!("k{i}"),
                    command,
                    json!({ "room": room, "n": format!("n{i}") }),
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn consecutive_facts_for_one_aggregate_travel_together() {
        let sink = sink(RecordingHost::default());
        let facts = facts(&["r1", "r1", "", "r2", "r2", "r2", "r1"]);
        let results = sink.commit_batch(&facts).await;
        assert_eq!(results.len(), 7);
        assert!(matches!(results[2], Err(FactError::Permanent(_))));
        assert!(results.iter().enumerate().all(|(i, r)| i == 2 || r.is_ok()));
        assert_eq!(
            *sink.host.batches.lock().unwrap(),
            [("r1".into(), 2), ("r2".into(), 3), ("r1".into(), 1)]
        );
    }

    #[tokio::test]
    async fn a_retryable_fact_ends_the_batch() {
        let sink = sink(RecordingHost {
            busy: Some("n3".into()),
            ..RecordingHost::default()
        });
        let facts = facts(&["r1", "r1", "r2", "r2", "r2", "r3"]);
        let results = sink.commit_batch(&facts).await;
        // r1 ×2, then r2 stops at its second fact; r3 is not attempted.
        assert_eq!(results.len(), 4);
        assert!(matches!(results[3], Err(FactError::Retryable(_))));
        assert_eq!(
            *sink.host.batches.lock().unwrap(),
            [("r1".into(), 2), ("r2".into(), 3)]
        );
    }
}
