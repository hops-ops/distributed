//! Admin peer routes and the world barrier coordinator.
//!
//! # Admin routes (`ServerConfig::admin_secret`)
//!
//! `POST` with header `x-game-admin-secret: <secret>` and a JSON body:
//!
//! | Route | Body | Effect |
//! |---|---|---|
//! | `/admin/transfer` | `{op: reserve\|confirm\|release\|return, nonce}` | transfer record at the issuing host |
//! | `/admin/child-event` | `{zone, event}` | deliver a tier-up event to a local zone |
//! | `/admin/zone-message` | `{zone, kick: {principal, reason}}` or `{zone, message: {type_id, payload}}` | zone inbox ([`super::inbox`]): 404 inactive, 421 wrong owner |
//! | `/admin/barrier/prepare` | `{barrier, reason, version, notes, timeout_ms}` | freeze all zones, wait for acks (409 + `missing`) |
//! | `/admin/barrier/commit` | `{barrier, version, notes, mode: rolling\|hard}` | apply version, resume |
//! | `/admin/barrier/abort` | `{barrier, notes}` | resume on the old version |
//!
//! # Barrier
//!
//! [`WorldCoordinator::rollout`] runs a two-phase barrier over every
//! participant (local [`GameServer`]s and [`RemoteHost`]s):
//!
//! 1. **prepare**: each host freezes all its zones (`Freeze{reason,
//!    version}`) and acknowledges once every zone is frozen (a zone may
//!    delay with [`Zone::freeze_ready`]). Zones
//!    activated during the barrier start frozen.
//! 2. if every host acknowledged within the timeout, the `apply` callback
//!    installs the new content (sync, or awaited with
//!    [`WorldCoordinator::rollout_async`] — zones stay frozen meanwhile);
//!    then **commit**: every zone switches to the new version and resumes
//!    (`Resume{version, notes}`). A **rolling** commit also reloads stale
//!    clients already inside a safe-room gate ([`Zone::is_gate`] under
//!    `StalePolicy::SafeRoom`): they reached a safe point, so they reload
//!    there instead of waiting to walk into another gate.
//!
//! Any missing ack (or a failed `apply`) **aborts**: every zone resumes on
//! the old version with [`AbortNotes`] ([`WorldCoordinator::with_abort_notes`]). No zone ever simulates the new version while another
//! still simulates the old one outside the barrier: nothing resumes before
//! every zone is frozen.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde_json::{json, Value};

use super::inbox::{delivered_json, InboxError, WireInbound};
use super::peer::{post_json, secret_eq, ADMIN_SECRET_HEADER};
use super::{activate_local, transfer_op, unix_now, GameServer, Inner, TransferOp, ZoneCmd};
use crate::facts::BoxFuture;
use crate::protocol::Notice;
use crate::world::{BarrierMode, ChildEvent, HostAddr};
use crate::zone::Zone;

type Reply = (StatusCode, String);

fn reply(status: StatusCode, body: Value) -> Reply {
    (status, body.to_string())
}

fn authorize<Z: Zone>(inner: &Inner<Z>, headers: &HeaderMap, body: &[u8]) -> Result<Value, Reply> {
    let expected = inner.config.admin_secret.as_deref().unwrap_or_default();
    let presented = headers
        .get(ADMIN_SECRET_HEADER)
        .map(|v| v.as_bytes())
        .unwrap_or_default();
    if expected.is_empty() || !secret_eq(expected.as_bytes(), presented) {
        return Err(reply(
            StatusCode::UNAUTHORIZED,
            json!({"ok": false, "error": "unauthorized"}),
        ));
    }
    serde_json::from_slice(body).map_err(|e| {
        reply(
            StatusCode::BAD_REQUEST,
            json!({"ok": false, "error": e.to_string()}),
        )
    })
}

fn text(body: &Value, name: &str) -> String {
    body.get(name)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

pub(super) fn child_event_json(event: &ChildEvent) -> Value {
    json!({
        "origin_zone": event.origin_zone,
        "from_zone": event.from_zone,
        "kind": event.kind,
        "payload": URL_SAFE_NO_PAD.encode(&event.payload),
        "origin_tick": event.origin_tick,
        "content_version": event.content_version,
        "levels": event.levels,
    })
}

fn child_event_from_json(v: &Value) -> Option<ChildEvent> {
    Some(ChildEvent {
        origin_zone: v.get("origin_zone")?.as_str()?.to_string(),
        from_zone: v.get("from_zone")?.as_str()?.to_string(),
        kind: v.get("kind")?.as_str()?.to_string(),
        payload: URL_SAFE_NO_PAD.decode(v.get("payload")?.as_str()?).ok()?,
        origin_tick: v.get("origin_tick")?.as_u64()?,
        content_version: v.get("content_version")?.as_str()?.to_string(),
        levels: u8::try_from(v.get("levels")?.as_u64()?).ok()?,
    })
}

pub(super) async fn admin_transfer<Z>(
    State(inner): State<Arc<Inner<Z>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Reply
where
    Z: Zone,
    Z::Config: Send,
{
    let body = match authorize(&inner, &headers, &body) {
        Ok(body) => body,
        Err(r) => return r,
    };
    let Some(op) = TransferOp::parse(&text(&body, "op")) else {
        return reply(StatusCode::BAD_REQUEST, json!({"ok": false, "error": "op"}));
    };
    let host_id = inner.host_id.clone();
    match transfer_op(&inner, &host_id, op, &text(&body, "nonce")).await {
        Ok(()) => reply(StatusCode::OK, json!({"ok": true})),
        Err(e) => reply(StatusCode::CONFLICT, json!({"ok": false, "error": e})),
    }
}

pub(super) async fn admin_zone_message<Z>(
    State(inner): State<Arc<Inner<Z>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Reply
where
    Z: Zone,
    Z::Config: Send,
{
    let body = match authorize(&inner, &headers, &body) {
        Ok(body) => body,
        Err(r) => return r,
    };
    let (zone, inbound) = match WireInbound::from_json(&body) {
        Ok(parsed) => parsed,
        Err(e) => return reply(StatusCode::BAD_REQUEST, json!({"ok": false, "error": e})),
    };
    let server = GameServer { inner };
    match server.deliver_wire(&zone, inbound).await {
        Ok(delivered) => reply(StatusCode::OK, delivered_json(delivered)),
        Err(InboxError::WrongOwner(owner)) => reply(
            StatusCode::MISDIRECTED_REQUEST,
            json!({"ok": false, "error": "wrong_owner", "owner": owner.id, "admin_url": owner.admin_url}),
        ),
        Err(InboxError::Inactive) => reply(
            StatusCode::NOT_FOUND,
            json!({"ok": false, "error": "inactive"}),
        ),
        Err(e) => reply(
            StatusCode::BAD_REQUEST,
            json!({"ok": false, "error": e.to_string()}),
        ),
    }
}

pub(super) async fn admin_child_event<Z>(
    State(inner): State<Arc<Inner<Z>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Reply
where
    Z: Zone,
    Z::Config: Send,
{
    let body = match authorize(&inner, &headers, &body) {
        Ok(body) => body,
        Err(r) => return r,
    };
    let Some(event) = body.get("event").and_then(child_event_from_json) else {
        return reply(
            StatusCode::BAD_REQUEST,
            json!({"ok": false, "error": "event"}),
        );
    };
    // Deliver here only (never re-forward): a peer sent it because its
    // directory says we own the zone.
    match activate_local(&inner, &text(&body, "zone")) {
        Some(tx) => {
            let _ = tx.send(ZoneCmd::ChildEvent(event)).await;
            reply(StatusCode::OK, json!({"ok": true}))
        }
        None => reply(
            StatusCode::NOT_FOUND,
            json!({"ok": false, "error": "unknown zone"}),
        ),
    }
}

pub(super) async fn admin_barrier<Z>(
    State(inner): State<Arc<Inner<Z>>>,
    Path(phase): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Reply
where
    Z: Zone,
    Z::Config: Send,
{
    let body = match authorize(&inner, &headers, &body) {
        Ok(body) => body,
        Err(r) => return r,
    };
    let server = GameServer { inner };
    let barrier = body.get("barrier").and_then(Value::as_u64).unwrap_or(0);
    match phase.as_str() {
        "prepare" => {
            let notice = Notice {
                reason: text(&body, "reason"),
                version: text(&body, "version"),
                notes: text(&body, "notes"),
            };
            let timeout = Duration::from_millis(
                body.get("timeout_ms")
                    .and_then(Value::as_u64)
                    .unwrap_or(5_000),
            );
            match server.prepare_barrier(barrier, notice, timeout).await {
                Ok(()) => reply(StatusCode::OK, json!({"ok": true})),
                Err(missing) => reply(
                    StatusCode::CONFLICT,
                    json!({"ok": false, "missing": missing}),
                ),
            }
        }
        "commit" => {
            let mode = match text(&body, "mode").as_str() {
                "hard" => BarrierMode::Hard,
                _ => BarrierMode::Rolling,
            };
            server
                .commit_barrier(
                    barrier,
                    &text(&body, "version"),
                    &text(&body, "notes"),
                    mode,
                )
                .await;
            reply(StatusCode::OK, json!({"ok": true}))
        }
        "abort" => {
            server.abort_barrier(barrier, &text(&body, "notes")).await;
            reply(StatusCode::OK, json!({"ok": true}))
        }
        _ => reply(StatusCode::NOT_FOUND, json!({"ok": false})),
    }
}

/// One host taking part in a world barrier.
pub trait BarrierParticipant: Send + Sync + 'static {
    /// Display name (host id).
    fn name(&self) -> String;

    /// Freeze everything; `Err` names what did not acknowledge in time.
    fn prepare<'a>(
        &'a self,
        barrier: u64,
        notice: &'a Notice,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<(), Vec<String>>>;

    /// Switch to `version` and resume.
    fn commit<'a>(
        &'a self,
        barrier: u64,
        version: &'a str,
        notes: &'a str,
        mode: BarrierMode,
    ) -> BoxFuture<'a, Result<(), String>>;

    /// Resume on the old version.
    fn abort<'a>(&'a self, barrier: u64, notes: &'a str) -> BoxFuture<'a, Result<(), String>>;
}

impl<Z> BarrierParticipant for GameServer<Z>
where
    Z: Zone,
    Z::Config: Send,
{
    fn name(&self) -> String {
        self.inner.host_id.clone()
    }

    fn prepare<'a>(
        &'a self,
        barrier: u64,
        notice: &'a Notice,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<(), Vec<String>>> {
        Box::pin(async move {
            let host = self.inner.host_id.clone();
            self.prepare_barrier(barrier, notice.clone(), timeout)
                .await
                .map_err(|missing| missing.into_iter().map(|z| format!("{host}/{z}")).collect())
        })
    }

    fn commit<'a>(
        &'a self,
        barrier: u64,
        version: &'a str,
        notes: &'a str,
        mode: BarrierMode,
    ) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            self.commit_barrier(barrier, version, notes, mode).await;
            Ok(())
        })
    }

    fn abort<'a>(&'a self, barrier: u64, notes: &'a str) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            self.abort_barrier(barrier, notes).await;
            Ok(())
        })
    }
}

/// A host reached through its admin endpoint.
#[derive(Debug, Clone)]
pub struct RemoteHost {
    host: HostAddr,
    secret: String,
    /// Extra time allowed on top of the barrier timeout for the HTTP call.
    slack: Duration,
}

impl RemoteHost {
    /// Remote participant using the shared admin secret.
    pub fn new(host: HostAddr, secret: impl Into<String>) -> Self {
        Self {
            host,
            secret: secret.into(),
            slack: Duration::from_secs(2),
        }
    }

    async fn call(&self, path: &str, body: Value, timeout: Duration) -> Result<Value, String> {
        match post_json(&self.host.admin_url, path, &self.secret, &body, timeout).await? {
            (200, reply) => Ok(reply),
            (status, reply) => Err(format!("{} {status}: {reply}", self.host.id)),
        }
    }
}

impl BarrierParticipant for RemoteHost {
    fn name(&self) -> String {
        self.host.id.clone()
    }

    fn prepare<'a>(
        &'a self,
        barrier: u64,
        notice: &'a Notice,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<(), Vec<String>>> {
        Box::pin(async move {
            let body = json!({
                "barrier": barrier, "reason": notice.reason, "version": notice.version,
                "notes": notice.notes, "timeout_ms": timeout.as_millis() as u64,
            });
            match post_json(
                &self.host.admin_url,
                "/admin/barrier/prepare",
                &self.secret,
                &body,
                timeout + self.slack,
            )
            .await
            {
                Ok((200, _)) => Ok(()),
                Ok((_, reply)) => Err(reply
                    .get("missing")
                    .and_then(Value::as_array)
                    .map(|zones| {
                        zones
                            .iter()
                            .filter_map(Value::as_str)
                            .map(|z| format!("{}/{z}", self.host.id))
                            .collect()
                    })
                    .unwrap_or_else(|| vec![self.host.id.clone()])),
                Err(_) => Err(vec![self.host.id.clone()]),
            }
        })
    }

    fn commit<'a>(
        &'a self,
        barrier: u64,
        version: &'a str,
        notes: &'a str,
        mode: BarrierMode,
    ) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            let mode = match mode {
                BarrierMode::Rolling => "rolling",
                BarrierMode::Hard => "hard",
            };
            let body =
                json!({"barrier": barrier, "version": version, "notes": notes, "mode": mode});
            self.call("/admin/barrier/commit", body, self.slack * 2)
                .await
                .map(|_| ())
        })
    }

    fn abort<'a>(&'a self, barrier: u64, notes: &'a str) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            let body = json!({"barrier": barrier, "notes": notes});
            self.call("/admin/barrier/abort", body, self.slack * 2)
                .await
                .map(|_| ())
        })
    }
}

/// Successful rollout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RolloutReport {
    /// Barrier id.
    pub barrier: u64,
    /// Version now running everywhere.
    pub version: String,
    /// Commit calls that failed (the host must catch up out of band).
    pub commit_errors: Vec<String>,
}

/// Failed (aborted) rollout: every zone resumed on the old version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RolloutError {
    /// Zones/hosts that did not acknowledge the freeze in time.
    NotAcked(Vec<String>),
    /// The apply callback failed.
    ApplyFailed(String),
}

/// Notes sent with `Resume` when a rollout aborts
/// ([`WorldCoordinator::with_abort_notes`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbortNotes {
    /// A zone or host did not acknowledge the freeze in time.
    pub not_acked: String,
    /// The `apply` callback failed; `{error}` is replaced by its message.
    pub apply_failed: String,
}

impl Default for AbortNotes {
    fn default() -> Self {
        Self {
            not_acked: "rollout aborted: freeze not acknowledged".into(),
            apply_failed: "rollout aborted: apply failed".into(),
        }
    }
}

/// Runs content-version barriers across every participant of one world.
pub struct WorldCoordinator {
    participants: Vec<Arc<dyn BarrierParticipant>>,
    ack_timeout: Duration,
    next: AtomicU64,
    abort_notes: AbortNotes,
}

impl WorldCoordinator {
    /// Coordinator waiting `ack_timeout` for freeze acknowledgements.
    pub fn new(ack_timeout: Duration) -> Self {
        Self {
            participants: Vec::new(),
            ack_timeout,
            next: AtomicU64::new(1),
            abort_notes: AbortNotes::default(),
        }
    }

    /// Notes clients see in `Resume(content_update_aborted)` when a rollout
    /// aborts (defaults: "rollout aborted: freeze not acknowledged" /
    /// "rollout aborted: apply failed").
    pub fn with_abort_notes(mut self, notes: AbortNotes) -> Self {
        self.abort_notes = notes;
        self
    }

    /// Add a participant.
    pub fn participant(mut self, participant: Arc<dyn BarrierParticipant>) -> Self {
        self.participants.push(participant);
        self
    }

    /// Freeze everything, `apply(version)`, resume everything on `version`;
    /// abort (resume on the old version) if any zone fails to acknowledge
    /// or `apply` fails. Synchronous `apply`; see
    /// [`rollout_async`](Self::rollout_async) for one that awaits.
    pub async fn rollout(
        &self,
        version: &str,
        notes: &str,
        mode: BarrierMode,
        apply: impl FnOnce(&str) -> Result<(), String>,
    ) -> Result<RolloutReport, RolloutError> {
        self.rollout_async(version, notes, mode, |v: String| {
            let result = apply(&v);
            std::future::ready(result)
        })
        .await
    }

    /// [`rollout`](Self::rollout) with an async `apply(version)`, awaited
    /// while every zone is frozen (e.g. staging a content pack, so clients
    /// see an "applying update" pause). Nothing resumes before it returns.
    pub async fn rollout_async<F, Fut>(
        &self,
        version: &str,
        notes: &str,
        mode: BarrierMode,
        apply: F,
    ) -> Result<RolloutReport, RolloutError>
    where
        F: FnOnce(String) -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        let barrier = (unix_now() << 20) ^ self.next.fetch_add(1, Ordering::Relaxed);
        let notice = Notice {
            reason: "content_update".into(),
            version: version.to_string(),
            notes: notes.to_string(),
        };
        let mut prepares = tokio::task::JoinSet::new();
        for p in &self.participants {
            let p = p.clone();
            let notice = notice.clone();
            let timeout = self.ack_timeout;
            prepares.spawn(async move { p.prepare(barrier, &notice, timeout).await });
        }
        let mut missing = Vec::new();
        while let Some(result) = prepares.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(zones)) => missing.extend(zones),
                Err(e) => missing.push(e.to_string()),
            }
        }
        if !missing.is_empty() {
            missing.sort();
            self.abort_all(barrier, &self.abort_notes.not_acked).await;
            return Err(RolloutError::NotAcked(missing));
        }
        if let Err(e) = apply(version.to_string()).await {
            let notes = self.abort_notes.apply_failed.replace("{error}", &e);
            self.abort_all(barrier, &notes).await;
            return Err(RolloutError::ApplyFailed(e));
        }
        let mut commit_errors = Vec::new();
        for p in &self.participants {
            if let Err(e) = p.commit(barrier, version, notes, mode).await {
                commit_errors.push(e);
            }
        }
        Ok(RolloutReport {
            barrier,
            version: version.to_string(),
            commit_errors,
        })
    }

    async fn abort_all(&self, barrier: u64, notes: &str) {
        for p in &self.participants {
            let _ = p.abort(barrier, notes).await;
        }
    }
}
