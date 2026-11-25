//! One zone's task: fenced activation, the loop driving its [`ZoneHost`],
//! commands from connections and peers, and what it sends on (frames, zone
//! messages, tier-up events).

use super::*;
use crate::facts::{Fact, FactBatchSink, FactError, FactReceipt};

/// Route a tier-up event to `parent` (local activation or peer host).
pub(super) fn deliver_child<Z>(inner: &Arc<Inner<Z>>, parent: String, event: ChildEvent)
where
    Z: Zone,
    Z::Config: Send,
{
    let local = match remote_owner(inner, &parent) {
        Some(host) => Err(host),
        None => Ok(activate_local(inner, &parent)),
    };
    match local {
        Ok(Ok(tx)) => {
            if let Err(mpsc::error::TrySendError::Full(cmd)) =
                tx.try_send(ZoneCmd::ChildEvent(event))
            {
                tokio::spawn(async move {
                    let _ = tx.send(cmd).await;
                });
            }
        }
        Ok(Err(refused)) => {
            eprintln!("distributed_game: tier-up event for zone {parent} dropped: {refused}");
        }
        Err(host) => {
            let Some(secret) = inner.config.admin_secret.clone() else {
                return;
            };
            let timeout = inner.config.peer_timeout;
            let body = json!({ "zone": parent, "event": world::child_event_json(&event) });
            tokio::spawn(async move {
                let _ = peer::post_json(
                    &host.admin_url,
                    "/admin/child-event",
                    &secret,
                    &body,
                    timeout,
                )
                .await;
            });
        }
    }
}

pub(super) enum Exit {
    Hibernate(Vec<ZoneCmd>),
    Shutdown,
    Lost,
}

/// Acquire the claim, waiting up to `activation_timeout` for a held one.
/// Returns it with the instant its successful attempt began (the claim's
/// TTL runs from no earlier than that).
pub(super) async fn acquire_claim<Z: Zone>(
    inner: &Arc<Inner<Z>>,
    zone_id: &str,
    holder: &str,
) -> Result<(ZoneClaim, Instant), String> {
    let deadline = Instant::now() + inner.config.activation_timeout;
    loop {
        let began = Instant::now();
        let error = match inner
            .ownership
            .acquire(zone_id, holder, inner.config.ownership_ttl)
            .await
        {
            Ok(claim) => return Ok((claim, began)),
            Err(OwnershipError::Held { holder }) => format!("zone is owned by {holder}"),
            Err(e) => format!("{e:?}"),
        };
        if Instant::now() >= deadline {
            return Err(error);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Activation failed: answer everything queued with `code` and unregister.
pub(super) fn refuse_activation<Z: Zone>(
    inner: &Arc<Inner<Z>>,
    zone_id: &str,
    activation: u64,
    mut rx: mpsc::Receiver<ZoneCmd>,
    code: u32,
    message: &str,
) {
    {
        let mut zones = inner.zones.lock().expect("zones");
        if zones
            .get(zone_id)
            .is_some_and(|e| e.activation == activation)
        {
            zones.remove(zone_id);
        }
        rx.close();
    }
    while let Ok(cmd) = rx.try_recv() {
        match cmd {
            ZoneCmd::Join { reply, .. } | ZoneCmd::Reconnect { reply, .. } => {
                let _ = reply.send(Err(JoinError {
                    code,
                    message: message.to_string(),
                }));
            }
            // Nothing runs here, so nothing to freeze.
            ZoneCmd::Freeze { ack, .. } => {
                let _ = ack.send(());
            }
            ZoneCmd::Deliver { reply, .. } => {
                let _ = reply.send(Err(InboxError::Inactive));
            }
            _ => {}
        }
    }
}

/// How long a claim acquired or renewed at some instant stays usable:
/// `ttl - renew_every` (the margin covers the renewal's own timeout and
/// clock skew with the store), so a zone fences itself before its claim
/// can expire at the store.
pub(super) fn fence_after(config: &ServerConfig) -> Duration {
    config
        .ownership_ttl
        .saturating_sub(config.ownership_renew_every)
}

/// Keep the claim alive. Every successful renewal moves the fence (the
/// instant the zone must stop by) to when that renewal began plus
/// [`fence_after`]; failed or timed-out renewals leave it where it is.
/// `Lost` moves it to now.
pub(super) async fn renew_claim(
    ownership: Arc<dyn ZoneOwnership>,
    current: Arc<Mutex<ZoneClaim>>,
    config: (Duration, Duration, Duration),
    fence: watch::Sender<Instant>,
) {
    let (every, ttl, fence_after) = config;
    loop {
        tokio::time::sleep(every).await;
        let claim = current.lock().expect("claim").clone();
        let began = Instant::now();
        match tokio::time::timeout(every / 2, ownership.renew(&claim, ttl)).await {
            Ok(Ok(renewed)) => {
                *current.lock().expect("claim") = renewed;
                fence.send_replace(began + fence_after);
            }
            Ok(Err(OwnershipError::Backend(_))) | Err(_) => continue,
            Ok(Err(_)) => {
                fence.send_replace(Instant::now());
                return;
            }
        }
    }
}

/// Wait for the fence to move (forever once the renewer stopped).
pub(super) async fn fence_moved(fence: &mut watch::Receiver<Instant>) {
    if fence.changed().await.is_err() {
        std::future::pending::<()>().await;
    }
}

/// Run `work` while the fence holds: None, without polling it, when the
/// fence has passed already, and None, dropping it, when the fence passes
/// first. A renewal that moves the fence while `work` runs extends it.
async fn before_fence<T>(
    fence: &watch::Receiver<Instant>,
    work: impl Future<Output = T>,
) -> Option<T> {
    let mut fence = fence.clone();
    tokio::pin!(work);
    loop {
        let at = *fence.borrow_and_update();
        if Instant::now() >= at {
            return None;
        }
        tokio::select! {
            biased;
            done = &mut work => return Some(done),
            _ = fence_moved(&mut fence) => {}
            _ = tokio::time::sleep_until(at) => {}
        }
    }
}

/// One activation's fact sink, bound by its fence: a commit (or batch)
/// starts only while the fence holds and is abandoned when the fence passes
/// first, so the worker sends nothing the zone can no longer prove it owns.
/// A fenced fact settles [`FactError::Permanent`].
///
/// Facts already carry the fencing token (the stamp's `activation`), but a
/// player cell cannot check it without reading the zone's lease in another
/// cell, and that read would race the lease anyway; the zone, which holds
/// the lease, enforces it here instead. What is left is a request already
/// on the wire at the fence: it reaches the store before the claim expires
/// unless it is delayed by more than the fence's margin (`renew_every`).
pub(super) struct FencedSink<S> {
    sink: S,
    fence: watch::Receiver<Instant>,
}

impl<S: FactSink> FencedSink<S> {
    pub(super) fn new(sink: S, fence: watch::Receiver<Instant>) -> Self {
        Self { sink, fence }
    }
}

fn fenced() -> FactError {
    FactError::Permanent("zone fenced: its ownership is no longer proven".into())
}

impl<S: FactSink> FactSink for FencedSink<S> {
    fn commit<'a>(&'a self, fact: &'a Fact) -> BoxFuture<'a, Result<FactReceipt, FactError>> {
        Box::pin(async move {
            before_fence(&self.fence, self.sink.commit(fact))
                .await
                .unwrap_or_else(|| Err(fenced()))
        })
    }

    fn batches(&self) -> Option<&dyn FactBatchSink> {
        self.sink.batches().map(|_| self as &dyn FactBatchSink)
    }
}

impl<S: FactSink> FactBatchSink for FencedSink<S> {
    fn commit_batch<'a>(
        &'a self,
        facts: &'a [Fact],
    ) -> BoxFuture<'a, Vec<Result<FactReceipt, FactError>>> {
        Box::pin(async move {
            let Some(batches) = self.sink.batches() else {
                return crate::facts::commit_each(self, facts).await;
            };
            before_fence(&self.fence, batches.commit_batch(facts))
                .await
                .unwrap_or_else(|| facts.iter().map(|_| Err(fenced())).collect())
        })
    }
}

/// Stop the fact worker and wait until it is gone (an aborted commit is
/// dropped, never resumed).
async fn stop_worker(worker: JoinHandle<()>) {
    worker.abort();
    let _ = worker.await;
}

/// The queue is closed (the host was dropped): let the worker commit what
/// it still holds within `settle_timeout` (and the fence), then stop it.
async fn settle_worker(worker: Option<JoinHandle<()>>, settle_timeout: Duration) {
    let Some(mut worker) = worker else { return };
    if tokio::time::timeout(settle_timeout, &mut worker)
        .await
        .is_err()
    {
        stop_worker(worker).await;
    }
}

/// Commands a zone handles between two looks at its clock: a flood of
/// commands (inputs, focus, inbox messages) never holds a due tick back
/// longer than this many command handlers; the rest stay queued.
pub(super) const COMMANDS_PER_ITERATION: usize = 64;

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_zone<Z>(
    inner: Arc<Inner<Z>>,
    zone_id: String,
    mut rx: mpsc::Receiver<ZoneCmd>,
    activation: u64,
    world: WorldState,
) where
    Z: Zone,
    Z::Config: Send,
{
    // 1. Fenced ownership, kept alive from here on: the fence is the instant
    // the zone (and its fact worker) must stop by.
    let holder = format!("{}#{}#{activation}", inner.host_id, inner.incarnation);
    let (claim, acquired_at) = match acquire_claim(&inner, &zone_id, &holder).await {
        Ok(acquired) => acquired,
        Err(message) => {
            return refuse_activation(
                &inner,
                &zone_id,
                activation,
                rx,
                error::ZONE_UNAVAILABLE,
                &message,
            )
        }
    };
    let token = claim.token;
    let current = Arc::new(Mutex::new(claim));
    let (fence_tx, mut fence) = watch::channel(acquired_at + fence_after(&inner.config));
    let renewer = tokio::spawn(renew_claim(
        inner.ownership.clone(),
        current.clone(),
        (
            inner.config.ownership_renew_every,
            inner.config.ownership_ttl,
            fence_after(&inner.config),
        ),
        fence_tx,
    ));
    let release = |renewer: JoinHandle<()>| {
        let inner = inner.clone();
        let current = current.clone();
        async move {
            renewer.abort();
            let _ = renewer.await;
            let claim = current.lock().expect("claim").clone();
            inner.ownership.release(&claim).await;
        }
    };
    // 2. Restore the hibernated snapshot, if any.
    let saved = match &inner.state_store {
        Some(store) => match store.load(&zone_id).await {
            Ok(saved) => saved,
            _ => {
                release(renewer).await;
                return refuse_activation(
                    &inner,
                    &zone_id,
                    activation,
                    rx,
                    error::ZONE_UNAVAILABLE,
                    "zone state unavailable",
                );
            }
        },
        None => None,
    };
    // 3. The allocation record and the game's factory, bounded together.
    let created = tokio::time::timeout(inner.config.activation_timeout, async {
        let allocation = match &inner.allocations {
            Some(store) => store.load(&zone_id).await?,
            None => None,
        };
        (inner.factory.0)(ZoneActivation {
            zone_id: zone_id.clone(),
            activation: token,
            allocation,
        })
        .await
    })
    .await;
    let zone_config = match created {
        Ok(Ok(Some(config))) => config,
        refused => {
            release(renewer).await;
            let (code, message) = match refused {
                Ok(Ok(None)) => (error::NOT_FOUND, "unknown zone".to_string()),
                Ok(Err(e)) => (error::ZONE_UNAVAILABLE, e.to_string()),
                _ => (
                    error::ZONE_UNAVAILABLE,
                    "zone creation timed out".to_string(),
                ),
            };
            inner.metrics.record_activation_refused(code);
            return refuse_activation(&inner, &zone_id, activation, rx, code, &message);
        }
    };
    let mut host_config = inner.config.host.clone();
    if let Some(adjust) = &inner.host_config {
        adjust(&zone_id, &mut host_config);
    }
    host_config.content_version = world.version.clone();
    // Fencing tokens grow across activations and restarts: a restart-safe
    // epoch for fact keys (`Ctx::boot_epoch`).
    host_config.boot_epoch = token;
    let token_inner = inner.clone();
    let issuer_inner = inner.clone();
    let generation = token;
    let mut host = ZoneHost::<Z>::new(
        zone_id.clone(),
        host_config,
        inner.clock.clone(),
        zone_config,
    )
    .with_token_source(move || random_token(&token_inner, "token"))
    .with_transfer_issuer(move |request: &HandoffRequest| {
        let inner = &issuer_inner;
        let expires_at = unix_now() + inner.config.transfer_ttl.as_secs().max(1);
        let nonce = random_token(inner, "transfer");
        inner
            .transfers
            .lock()
            .expect("transfers")
            .issue(&nonce, expires_at);
        Some(inner.config.admission.key.sign_transfer(&Transfer {
            claims: TicketClaims {
                principal: request.principal.clone(),
                zone: request.target_zone.clone(),
                expires_at,
                nonce,
                role: Role::Player,
                name: request.name.clone(),
                extensions: request.extensions.clone(),
                wake: false,
            },
            source_zone: request.source_zone.clone(),
            issuer: inner.host_id.clone(),
            generation,
            carried: request.carried.clone(),
            departed_at_ms: request.departed_at_ms,
        }))
    });
    if let Some(saved) = &saved {
        host.restore(&saved.state, &saved.content_version);
    }
    // 4. The fact worker, bound by the same fence.
    let fact_worker = match inner.facts.as_ref().map(|spawn| spawn(fence.clone())) {
        Some((queue, worker)) => {
            host = host.with_facts(queue);
            Some(worker)
        }
        None => None,
    };
    if let Some((_, notice)) = world.barrier {
        host.request_freeze(notice);
    }

    let mut conns: HashMap<ClientId, mpsc::Sender<ConnOut>> = HashMap::new();
    let mut freeze_acks: Vec<oneshot::Sender<()>> = Vec::new();
    let mut dormant_since: Option<Duration> = None;
    let mut shutdown = inner.shutdown.subscribe();
    let exit = loop {
        // Fenced: someone else may own the zone by now. Not one more tick.
        let fenced_at = *fence.borrow_and_update();
        if Instant::now() >= fenced_at {
            break Exit::Lost;
        }
        let report = host.advance();
        inner.metrics.record_advance(&host, &report);
        route(&mut host, &mut conns);
        forward_upward(&inner, &zone_id, &mut host);
        send_zone_messages(&inner, &mut host);
        if host.is_frozen() {
            for ack in freeze_acks.drain(..) {
                let _ = ack.send(());
            }
        }
        let now = inner.clock.now();
        if host.tier() == Tier::Dormant {
            dormant_since.get_or_insert(now);
        } else {
            dormant_since = None;
        }
        let hibernate_at = match (inner.config.hibernate_after, dormant_since) {
            (Some(after), Some(since))
                if conns.is_empty()
                    && host.freeze_state() == FreezeState::Running
                    && host.facts_in_flight() == 0 =>
            {
                Some(since + after)
            }
            _ => None,
        };
        if hibernate_at.is_some_and(|at| now >= at) {
            if let Some(leftovers) = begin_hibernation(&inner, &zone_id, activation, &mut rx) {
                break Exit::Hibernate(leftovers);
            }
        }
        let wake = match (report.next_wake, hibernate_at) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        let sleep_for = wake.map(|at| at.saturating_sub(now));
        tokio::select! {
            biased;
            _ = shutdown.wait_for(|stop| *stop) => {
                let bye = ServerMessage::Kick { code: kick::SHUTDOWN, message: "server shutting down".into() };
                let frame = bye.encode(&()).expect("kick encodes");
                for out in conns.values() {
                    let _ = out.try_send(ConnOut::Frame(frame.clone()));
                    let _ = out.try_send(ConnOut::Close);
                }
                conns.clear();
                host.dispose();
                host.drain_outbound();
                break Exit::Shutdown;
            }
            // Checked at the top of the loop.
            _ = fence_moved(&mut fence) => {}
            _ = tokio::time::sleep_until(fenced_at) => {}
            cmd = rx.recv() => {
                let Some(cmd) = cmd else {
                    host.dispose();
                    route(&mut host, &mut conns);
                    break Exit::Shutdown;
                };
                handle_cmd(&mut host, &mut conns, &inner.metrics, &mut freeze_acks, cmd);
                // Take what else is queued, up to a budget, before ticking
                // again.
                for _ in 1..COMMANDS_PER_ITERATION {
                    let Ok(cmd) = rx.try_recv() else { break };
                    handle_cmd(&mut host, &mut conns, &inner.metrics, &mut freeze_acks, cmd);
                }
                route(&mut host, &mut conns);
            }
            _ = async {
                match sleep_for {
                    Some(d) => tokio::time::sleep(d).await,
                    None => std::future::pending().await,
                }
            } => {}
        }
    };
    // The claim stays renewed until the fact worker is gone: no fact is
    // committed after the claim is released.
    match exit {
        Exit::Hibernate(leftovers) => {
            let snapshot = host.hibernate_state();
            let content_version = host.config().content_version.clone();
            host.dispose();
            host.drain_outbound();
            forward_upward(&inner, &zone_id, &mut host);
            send_zone_messages(&inner, &mut host);
            if let (Some(state), Some(store)) = (snapshot, inner.state_store.clone()) {
                let id = zone_id.clone();
                let stored = StoredZoneState {
                    token,
                    content_version,
                    state,
                };
                // A stale token means a newer owner saved already: ours is
                // dropped, but never silently (metrics + stderr).
                let saved = store.save(&id, &stored).await;
                let outcome = match &saved {
                    Ok(()) => "saved",
                    Err(StateError::Stale { .. }) => "stale",
                    Err(_) => "error",
                };
                inner.metrics.record_hibernation_save(outcome);
                if outcome != "saved" {
                    eprintln!(
                        "distributed_game: hibernation snapshot of zone {zone_id} (token {token}) not saved: {saved:?}",
                    );
                }
            }
            drop(host);
            settle_worker(fact_worker, inner.config.settle_timeout).await;
            release(renewer).await;
            for cmd in leftovers {
                if matches!(
                    cmd,
                    ZoneCmd::Input { .. } | ZoneCmd::Disconnect { .. } | ZoneCmd::ZoneMessage(_)
                ) {
                    continue;
                }
                // Messages never activate a zone.
                if let ZoneCmd::Deliver { reply, .. } = cmd {
                    let _ = reply.send(Err(InboxError::Inactive));
                    continue;
                }
                if let Ok(tx) = activate_local(&inner, &zone_id) {
                    let _ = tx.send(cmd).await;
                }
            }
        }
        Exit::Shutdown => {
            drop(host);
            settle_worker(fact_worker, inner.config.settle_timeout).await;
            release(renewer).await;
        }
        Exit::Lost => {
            // Fenced: another host may run the zone. Nothing more is
            // persisted or committed: queued facts are dropped, the worker
            // stops (its sink already refuses past the fence) and the
            // disposal's facts go nowhere. Clients are kicked.
            renewer.abort();
            let dropped = host.abandon_facts();
            if let Some(worker) = fact_worker {
                stop_worker(worker).await;
            }
            host.dispose();
            route(&mut host, &mut conns);
            eprintln!(
                "distributed_game: zone {zone_id} (token {token}) fenced without a live claim; {dropped} unsettled facts dropped",
            );
            drop(host);
        }
    }
    let mut zones = inner.zones.lock().expect("zones");
    if zones
        .get(&zone_id)
        .is_some_and(|e| e.activation == activation)
    {
        zones.remove(&zone_id);
    }
    // Keep the series when a newer activation (after hibernation) runs.
    if !zones.contains_key(&zone_id) {
        inner.metrics.forget_zone(&zone_id);
    }
}

/// Unregister the zone and close its inbox atomically; None when commands
/// are pending (hibernation is retried later).
pub(super) fn begin_hibernation<Z: Zone>(
    inner: &Arc<Inner<Z>>,
    zone_id: &str,
    activation: u64,
    rx: &mut mpsc::Receiver<ZoneCmd>,
) -> Option<Vec<ZoneCmd>> {
    let mut zones = inner.zones.lock().expect("zones");
    if !rx.is_empty() {
        return None;
    }
    if zones
        .get(zone_id)
        .is_some_and(|e| e.activation == activation)
    {
        zones.remove(zone_id);
    }
    rx.close();
    drop(zones);
    // Commands sent between the check and the close are re-routed to the
    // next activation (which waits for our claim release).
    let mut leftovers = Vec::new();
    while let Ok(cmd) = rx.try_recv() {
        leftovers.push(cmd);
    }
    Some(leftovers)
}

pub(super) fn forward_upward<Z>(inner: &Arc<Inner<Z>>, zone_id: &str, host: &mut ZoneHost<Z>)
where
    Z: Zone,
    Z::Config: Send,
{
    let events = host.drain_upward();
    if events.is_empty() {
        return;
    }
    let Some(parent) = inner.topology.parent(zone_id) else {
        return;
    };
    for event in events {
        deliver_child(inner, parent.clone(), event);
    }
}

/// Path of the peer route that takes a zone message for a zone this host
/// runs ([`send_zone_messages`]).
pub(super) const ZONE_ENVELOPE_PATH: &str = "/admin/zone-envelope";

/// Hand the zone messages `host` sent to their targets, off the tick: a
/// zone running here gets it on its queue, a zone the directory places on
/// a peer gets it over `ZONE_ENVELOPE_PATH`. As on celld, delivery is best
/// effort and at most once (the target drops a repeated id): a message for
/// a zone that is not running is dropped (it never wakes one), a full
/// queue or failed peer call drops it, nothing is retried, and messages
/// carry no order guarantee.
pub(super) fn send_zone_messages<Z>(inner: &Arc<Inner<Z>>, host: &mut ZoneHost<Z>)
where
    Z: Zone,
    Z::Config: Send,
{
    for envelope in host.take_zone_messages() {
        if let Some(peer) = remote_owner(inner, &envelope.to) {
            let Some(secret) = inner.config.admin_secret.clone() else {
                continue;
            };
            let timeout = inner.config.peer_timeout;
            tokio::spawn(async move {
                let body = serde_json::to_value(&envelope).expect("an envelope serializes");
                match peer::post_json(&peer.admin_url, ZONE_ENVELOPE_PATH, &secret, &body, timeout)
                    .await
                {
                    Ok((204, _)) => {}
                    Ok((status, _)) => eprintln!(
                        "distributed_game: zone message {} to {} refused: {status}",
                        envelope.id, envelope.to
                    ),
                    Err(e) => {
                        eprintln!("distributed_game: zone message to {}: {e}", envelope.to)
                    }
                }
            });
        } else if let Some(tx) = running_zone(inner, &envelope.to) {
            let _ = tx.try_send(ZoneCmd::ZoneMessage(envelope));
        }
    }
}

pub(super) fn handle_cmd<Z: Zone>(
    host: &mut ZoneHost<Z>,
    conns: &mut HashMap<ClientId, mpsc::Sender<ConnOut>>,
    metrics: &GameMetrics,
    freeze_acks: &mut Vec<oneshot::Sender<()>>,
    cmd: ZoneCmd,
) {
    match cmd {
        ZoneCmd::Join {
            claims,
            transfer,
            capabilities,
            content_version,
            out,
            reply,
        } => {
            let result = match &transfer {
                Some(transfer) => host.join_transfer(transfer, capabilities, &content_version),
                None => host.join(&claims, capabilities, &content_version),
            };
            if let Ok(joined) = &result {
                conns.insert(joined.client, out);
            }
            let _ = reply.send(result);
        }
        ZoneCmd::Reconnect {
            token,
            capabilities,
            content_version,
            out,
            reply,
        } => {
            let result = host.reconnect_with_version(&token, capabilities, &content_version);
            if let Ok(joined) = &result {
                conns.insert(joined.client, out);
            }
            let _ = reply.send(result);
        }
        ZoneCmd::Input {
            client,
            seq,
            client_tick,
            payload,
        } => {
            let frozen_before = host.inputs_dropped_frozen();
            let outcome = host.push_input(client, seq, client_tick, &payload);
            if host.inputs_dropped_frozen() > frozen_before {
                metrics.record_frozen_input(host.zone_id());
            } else {
                metrics.record_input(host.zone_id(), outcome);
            }
        }
        ZoneCmd::Disconnect { client, graceful } => {
            conns.remove(&client);
            host.disconnect(client, graceful);
        }
        ZoneCmd::Content { client, version } => {
            host.content_patched(client, &version);
        }
        ZoneCmd::Focus { client, x, y } => host.observer_focus(client, x, y),
        ZoneCmd::ChildEvent(event) => host.deliver_child_event(event),
        // Repeats and misaddressed messages are dropped, as on celld.
        ZoneCmd::ZoneMessage(envelope) => {
            let _ = host.deliver_zone(envelope);
        }
        ZoneCmd::Deliver { inbound, reply } => {
            let _ = reply.send(inbound.decode::<Z::Message>().map(|m| host.deliver(m)));
        }
        ZoneCmd::Freeze { notice, ack } => {
            if host.request_freeze(notice) == FreezeState::Frozen {
                let _ = ack.send(());
            } else {
                freeze_acks.push(ack);
            }
        }
        ZoneCmd::Commit {
            version,
            notes,
            mode,
        } => {
            host.set_content_version(version.clone());
            if host.freeze_state() != FreezeState::Running {
                host.resume(Notice {
                    reason: "content_update".into(),
                    version,
                    notes,
                });
            }
            // Hard reloads every stale client; Rolling reloads the ones
            // already inside a safe-room gate (they will not walk into one).
            let gate = host.zone().is_gate()
                && matches!(host.config().stale_policy, StalePolicy::SafeRoom { .. });
            if mode == BarrierMode::Hard || gate {
                host.reload_stale_players();
            }
            freeze_acks.clear();
        }
        ZoneCmd::Abort { notice } => {
            host.cancel_freeze(notice);
            freeze_acks.clear();
        }
        ZoneCmd::Inspect(f) => f(&*host as &dyn Any),
    }
}

pub(super) fn route<Z: Zone>(
    host: &mut ZoneHost<Z>,
    conns: &mut HashMap<ClientId, mpsc::Sender<ConnOut>>,
) {
    for outbound in host.drain_outbound() {
        match outbound {
            Outbound::Frame { client, frame } => {
                let Some(out) = conns.get(&client) else {
                    continue;
                };
                if out.try_send(ConnOut::Frame(frame)).is_err() {
                    // Slow or gone: drop the connection; the player keeps its grace window.
                    let out = conns.remove(&client).expect("present");
                    let _ = out.try_send(ConnOut::Close);
                    host.disconnect(client, false);
                }
            }
            Outbound::Close { client } => {
                if let Some(out) = conns.remove(&client) {
                    let _ = out.try_send(ConnOut::Close);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Takes `delay` per commit (or batch); records what it started and
    /// what it finished.
    #[derive(Clone, Default)]
    struct Slow {
        delay: Duration,
        batching: bool,
        started: Arc<Mutex<Vec<String>>>,
        finished: Arc<Mutex<Vec<String>>>,
    }

    impl Slow {
        fn new(delay: Duration) -> Self {
            Self {
                delay,
                ..Self::default()
            }
        }

        async fn take(&self, facts: &[Fact]) -> Vec<Result<FactReceipt, FactError>> {
            let keys: Vec<String> = facts.iter().map(|f| f.key().to_string()).collect();
            self.started.lock().unwrap().extend(keys.iter().cloned());
            tokio::time::sleep(self.delay).await;
            self.finished.lock().unwrap().extend(keys);
            facts
                .iter()
                .map(|_| {
                    Ok(FactReceipt {
                        payload: serde_json::Value::Null,
                        replayed: false,
                    })
                })
                .collect()
        }

        fn started(&self) -> Vec<String> {
            self.started.lock().unwrap().clone()
        }

        fn finished(&self) -> Vec<String> {
            self.finished.lock().unwrap().clone()
        }
    }

    impl FactSink for Slow {
        fn commit<'a>(&'a self, fact: &'a Fact) -> BoxFuture<'a, Result<FactReceipt, FactError>> {
            Box::pin(async move {
                self.take(std::slice::from_ref(fact))
                    .await
                    .pop()
                    .expect("one result")
            })
        }

        fn batches(&self) -> Option<&dyn FactBatchSink> {
            self.batching.then_some(self as &dyn FactBatchSink)
        }
    }

    impl FactBatchSink for Slow {
        fn commit_batch<'a>(
            &'a self,
            facts: &'a [Fact],
        ) -> BoxFuture<'a, Vec<Result<FactReceipt, FactError>>> {
            Box::pin(self.take(facts))
        }
    }

    fn fact(key: &str) -> Fact {
        Fact::new(key, "test.fact", serde_json::Value::Null)
    }

    #[tokio::test(start_paused = true)]
    async fn nothing_starts_once_the_fence_has_passed() {
        let slow = Slow::new(Duration::from_millis(10));
        let (_tx, fence) = watch::channel(Instant::now());
        let sink = FencedSink::new(slow.clone(), fence);
        assert_eq!(sink.commit(&fact("a")).await, Err(fenced()));
        assert!(slow.started().is_empty(), "{:?}", slow.started());
    }

    #[tokio::test(start_paused = true)]
    async fn a_commit_still_running_at_the_fence_is_abandoned() {
        let slow = Slow::new(Duration::from_secs(2));
        let start = Instant::now();
        let (_tx, fence) = watch::channel(start + Duration::from_secs(1));
        let sink = FencedSink::new(slow.clone(), fence);
        assert_eq!(sink.commit(&fact("a")).await, Err(fenced()));
        assert_eq!(Instant::now() - start, Duration::from_secs(1));
        assert_eq!(slow.started(), ["a"]);
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(slow.finished().is_empty(), "dropped, never resumed");
    }

    #[tokio::test(start_paused = true)]
    async fn a_renewal_during_a_commit_lets_it_finish() {
        let slow = Slow::new(Duration::from_secs(2));
        let start = Instant::now();
        let (tx, fence) = watch::channel(start + Duration::from_secs(1));
        let sink = FencedSink::new(slow.clone(), fence);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            tx.send_replace(start + Duration::from_secs(5));
            // Keep the sender: a dropped one freezes the fence, which is
            // also fine, but this is the renewer's shape.
            std::future::pending::<()>().await;
        });
        assert!(sink.commit(&fact("a")).await.is_ok());
        assert_eq!(slow.finished(), ["a"]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_fenced_batch_fails_every_fact_in_it() {
        let mut slow = Slow::new(Duration::from_secs(2));
        slow.batching = true;
        let (_tx, fence) = watch::channel(Instant::now() + Duration::from_secs(1));
        let sink = FencedSink::new(slow.clone(), fence);
        let batches = sink.batches().expect("batching passes through");
        let results = batches.commit_batch(&[fact("a"), fact("b")]).await;
        assert_eq!(results, vec![Err(fenced()), Err(fenced())]);
        assert!(slow.finished().is_empty());
    }

    /// Through the feed: settled before the fence, abandoned at it, never
    /// started after it.
    #[tokio::test(start_paused = true)]
    async fn the_worker_commits_nothing_past_the_fence() {
        let slow = Slow::new(Duration::from_millis(1_500));
        let (_tx, fence) = watch::channel(Instant::now() + Duration::from_secs(2));
        let (mut queue, feed) = crate::facts::fact_channel(8, RetryPolicy::default());
        let worker = crate::facts::spawn_fact_worker(feed, FencedSink::new(slow.clone(), fence));
        for key in ["a", "b", "c"] {
            queue.try_send(fact(key)).unwrap();
        }
        drop(queue);
        worker.await.unwrap();
        assert_eq!(slow.finished(), ["a"]);
        assert_eq!(slow.started(), ["a", "b"], "c never started");
    }
}
