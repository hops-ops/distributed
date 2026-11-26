//! The host state hooks reach ([`Core`]) and the zone's API to it ([`Ctx`]).

use super::*;

pub(super) struct PlayerSlot<I> {
    pub(super) info: PlayerInfo,
    pub(super) inputs: InputBuffer<I>,
    pub(super) client: Option<ClientId>,
    pub(super) view: View,
    pub(super) grace_until: Option<Duration>,
    pub(super) reconnect_token: String,
    pub(super) session_id: String,
    pub(super) content_version: String,
    /// Lifecycle facts emitted for this slot (part of their keys).
    pub(super) transitions: u32,
}

pub(super) struct ObserverSlot {
    pub(super) info: ObserverInfo,
    pub(super) client: ClientId,
    pub(super) view: View,
}

pub(super) struct ClientSlot {
    pub(super) viewer: Viewer,
    pub(super) replica: ClientReplica,
}

/// Host state reachable from hooks through [`Ctx`].
pub(crate) struct Core<Z: Zone> {
    pub(super) zone_id: String,
    pub(super) config: HostConfig,
    pub(super) clock: Arc<dyn Clock>,
    /// The clock read when the current stimulus arrived
    /// ([`ZoneHost::apply`]): every time the host and the zone see while it
    /// runs, so a recording that keeps it replays exactly.
    pub(super) now: Duration,
    /// Wall time (Unix ms) read with [`now`](Self::now).
    pub(super) unix_ms: u64,
    pub(super) rng: ZoneRng,
    pub(super) timers: Timers<Z::Timer>,
    pub(super) tick: u64,
    pub(super) sim_time: Duration,
    pub(super) dt: Duration,
    /// Ticks dropped (catch-up cap) since the last simulated tick; seen by
    /// the next `on_tick` through `Ctx::dropped_ticks`, then reset.
    pub(super) dropped_ticks: u32,
    pub(super) tier: Tier,
    pub(super) players: BTreeMap<PlayerId, PlayerSlot<Z::Input>>,
    pub(super) observers: BTreeMap<ObserverId, ObserverSlot>,
    /// Observer being removed while `on_unobserve` runs.
    pub(super) departing: Option<ObserverInfo>,
    pub(super) clients: BTreeMap<ClientId, ClientSlot>,
    pub(super) next_id: u32,
    pub(super) outbound: Vec<Outbound>,
    pub(super) facts: Option<FactQueue>,
    pub(super) pending_kicks: Vec<(PlayerId, String)>,
    pub(super) rewind: Rewind,
    pub(super) colliders: Colliders,
    pub(super) transfer_issuer: Option<Box<TransferIssuer>>,
    /// Handoffs to finish after the hook: player, target zone, transfer
    /// ticket and the departure stamp time signed into the ticket.
    pub(super) pending_handoffs: Vec<(PlayerId, String, String, u64)>,
    pub(super) upward: Vec<ChildEvent>,
    pub(super) child_inbox: Vec<ChildEvent>,
    pub(super) dropped_frozen: u64,
    /// Typed facts keyed without dedupe parts this tick: `(tick, count)`.
    pub(super) fact_seq: (u64, u32),
    /// Facts stamped this activation ([`FactStamp::seq`] of the latest).
    pub(super) stamped: u64,
    /// Stamps are never earlier than this (Unix ms): raised past a handoff
    /// source's departure time when a transfer is admitted.
    pub(super) stamp_floor_ms: u64,
    /// Stamp time forced while a handoff departure runs `on_leave`, so the
    /// source's departure facts carry exactly the time in the ticket.
    pub(super) pinned_stamp_ms: Option<u64>,
    /// Bound facts waiting for queue capacity, in emission order: the
    /// lifecycle facts and [`Ctx::emit_queued`]'s.
    pub(super) backlog: VecDeque<Fact>,
    /// Zone messages sent since the driver last took them.
    pub(super) zone_out: Vec<ZoneEnvelope>,
    /// Zone messages sent this activation (their ids).
    pub(super) zone_seq: u64,
    /// Zone message ids delivered lately (repeats are dropped).
    pub(super) zone_seen: SeenIds,
}

impl<Z: Zone> Core<Z> {
    pub(super) fn next_id(&mut self) -> u32 {
        self.next_id += 1;
        self.next_id
    }

    pub(super) fn client_of(&self, viewer: Viewer) -> Option<ClientId> {
        match viewer {
            Viewer::Player(p) => self.players.get(&p).and_then(|s| s.client),
            Viewer::Observer(o) => self.observers.get(&o).map(|s| s.client),
        }
    }

    pub(super) fn send(&mut self, client: ClientId, frame: Vec<u8>) {
        self.outbound.push(Outbound::Frame { client, frame });
    }

    pub(super) fn server_message(&mut self, client: ClientId, message: &ServerMessage) {
        let frame = message.encode(&()).expect("non-delta messages encode");
        self.send(client, frame);
    }

    pub(super) fn server_time_ms(&self) -> f64 {
        self.now.as_secs_f64() * 1000.0
    }

    pub(super) fn in_flight(&self) -> usize {
        self.facts.as_ref().map_or(0, FactQueue::in_flight) + self.backlog.len()
    }

    /// Queue a lifecycle fact for `principal`'s slot `player` (transition
    /// `n`), keyed restart-safely and stamped now.
    pub(super) fn lifecycle<C: FactCommand>(
        &mut self,
        player: PlayerId,
        principal: &str,
        n: u32,
        input: C,
    ) {
        if !self.config.lifecycle_facts || self.facts.is_none() {
            return;
        }
        let key = crate::facts::fact_key(
            C::COMMAND,
            &self.zone_id,
            self.config.boot_epoch,
            &[&format_args!("p{}", player.0), &n],
        );
        let Ok(fact) = Fact::typed(key, &input) else {
            return;
        };
        let fact = fact.bind(Some(principal.to_string()), self.stamp_fact());
        self.queue(fact);
    }

    /// Queue a bound fact behind the backlog and send what fits.
    pub(super) fn queue(&mut self, fact: Fact) {
        self.backlog.push_back(fact);
        self.flush_backlog();
    }

    /// Send backlogged facts while the queue has room.
    pub(super) fn flush_backlog(&mut self) {
        let Some(queue) = self.facts.as_mut() else {
            self.backlog.clear();
            return;
        };
        while let Some(fact) = self.backlog.front() {
            match queue.try_send(fact.clone()) {
                Err(EmitError::Backpressure) => return,
                // Sent, or a duplicate already in flight: done either way.
                _ => {
                    self.backlog.pop_front();
                }
            }
        }
    }

    /// Next transition number of a slot.
    pub(super) fn transition(&mut self, player: PlayerId) -> Option<(String, u32)> {
        let slot = self.players.get_mut(&player)?;
        slot.transitions += 1;
        Some((slot.info.principal.clone(), slot.transitions))
    }

    pub(super) fn emit_lifecycle<C: FactCommand>(&mut self, player: PlayerId, input: C) {
        if !self.config.lifecycle_facts {
            return;
        }
        if let Some((principal, n)) = self.transition(player) {
            self.lifecycle(player, &principal, n, input);
        }
    }

    /// Wall time stamped on anything produced now (Unix ms).
    pub(super) fn stamp_ms(&self) -> u64 {
        self.pinned_stamp_ms
            .unwrap_or_else(|| self.unix_ms.max(self.stamp_floor_ms))
    }

    pub(super) fn stamp(&self) -> FactStamp {
        FactStamp {
            zone_id: self.zone_id.clone(),
            tick: self.tick,
            content_version: self.config.content_version.clone(),
            emitted_at_ms: self.stamp_ms(),
            activation: self.config.boot_epoch,
            config_version: self.config.config_version.clone(),
            seq: self.stamped,
        }
    }

    /// Stamp for a fact: the next emission sequence number.
    pub(super) fn stamp_fact(&mut self) -> FactStamp {
        self.stamped += 1;
        self.stamp()
    }
}

/// Hook context: everything a zone may touch besides its own state.
pub struct Ctx<'a, Z: Zone> {
    pub(super) core: &'a mut Core<Z>,
}

impl<Z: Zone> Ctx<'_, Z> {
    /// Zone id.
    pub fn zone_id(&self) -> &str {
        &self.core.zone_id
    }

    /// Ticks simulated so far (the current tick inside `on_tick`).
    pub fn tick(&self) -> u64 {
        self.core.tick
    }

    /// Step length of the current tick (Hot or Idle rate).
    pub fn dt(&self) -> Duration {
        self.core.dt
    }

    /// Ticks the host dropped right before this one: the clock ran further
    /// ahead than `max_catch_up` steps, so the rest of the backlog was not
    /// simulated (0 normally). Simulated ticks then lag wall-clock time; a
    /// zone that paces players' inputs per tick credits these so inputs
    /// sent in real time do not queue up for good.
    pub fn dropped_ticks(&self) -> u32 {
        self.core.dropped_ticks
    }

    /// Step length in seconds.
    pub fn dt_secs(&self) -> f32 {
        self.core.dt.as_secs_f32()
    }

    /// Simulation time (sum of all step lengths).
    pub fn sim_time(&self) -> Duration {
        self.core.sim_time
    }

    /// Host clock when the current stimulus arrived (the same throughout
    /// one [`ZoneHost::apply`]; recorded, so it replays).
    pub fn now(&self) -> Duration {
        self.core.now
    }

    /// Current activity tier.
    pub fn tier(&self) -> Tier {
        self.core.tier
    }

    /// Content version.
    pub fn content_version(&self) -> &str {
        &self.core.config.content_version
    }

    /// Content version `player`'s client runs (what it presented on its
    /// last Hello; differs from [`content_version`](Self::content_version)
    /// for a stale client kept under [`StalePolicy::SafeRoom`]).
    pub fn player_content_version(&self, player: PlayerId) -> Option<&str> {
        self.core
            .players
            .get(&player)
            .map(|slot| slot.content_version.as_str())
    }

    /// True when `player`'s client runs older content than the zone (it
    /// may only be handed off into a gate zone, unless the policy roams).
    pub fn is_stale(&self, player: PlayerId) -> bool {
        let expected = &self.core.config.content_version;
        !expected.is_empty()
            && self
                .player_content_version(player)
                .is_some_and(|version| version != expected)
    }

    /// The host's [`StalePolicy`].
    pub fn stale_policy(&self) -> StalePolicy {
        self.core.config.stale_policy
    }

    /// Remote config version ([`HostConfig::config_version`]).
    pub fn config_version(&self) -> &str {
        &self.core.config.config_version
    }

    /// Switch the remote config version (e.g. on a config-changed
    /// message); facts and stamps produced afterwards carry it.
    pub fn set_config_version(&mut self, version: impl Into<String>) {
        self.core.config.config_version = version.into();
    }

    /// Epoch of this zone activation ([`HostConfig::boot_epoch`]): unique
    /// per activation and increasing across restarts and hibernation.
    /// Tick counts and entity generations restart with the process, so a
    /// fact key built only from them **collides after a restart** (and the
    /// sink replays the old commit instead of recording the new outcome);
    /// include this epoch, e.g. via [`fact_key`](Self::fact_key).
    pub fn boot_epoch(&self) -> u64 {
        self.core.config.boot_epoch
    }

    /// Restart-safe fact idempotency key:
    /// `"{prefix}:{zone}:{boot_epoch}:{part}:{part}…"` (see
    /// [`fact_key`](crate::facts::fact_key)).
    pub fn fact_key(&self, prefix: &str, parts: &[&dyn std::fmt::Display]) -> String {
        crate::facts::fact_key(
            prefix,
            &self.core.zone_id,
            self.core.config.boot_epoch,
            parts,
        )
    }

    /// The zone's deterministic RNG.
    pub fn rng(&mut self) -> &mut ZoneRng {
        &mut self.core.rng
    }

    /// One-shot timer after `after` of simulation time.
    pub fn set_timeout(&mut self, after: Duration, timer: Z::Timer) -> TimerId {
        let now = self.core.sim_time;
        self.core.timers.once(now, after, timer)
    }

    /// Repeating timer every `every` of simulation time.
    pub fn set_interval(&mut self, every: Duration, timer: Z::Timer) -> TimerId {
        let now = self.core.sim_time;
        self.core.timers.interval(now, every, timer)
    }

    /// Cancel a timer.
    pub fn cancel_timer(&mut self, id: TimerId) -> bool {
        self.core.timers.cancel(id)
    }

    /// Players (connected or within the reconnect grace window).
    pub fn players(&self) -> impl Iterator<Item = &PlayerInfo> + '_ {
        self.core.players.values().map(|slot| &slot.info)
    }

    /// One player.
    pub fn player(&self, id: PlayerId) -> Option<&PlayerInfo> {
        self.core.players.get(&id).map(|slot| &slot.info)
    }

    /// Observers currently subscribed.
    pub fn observers(&self) -> impl Iterator<Item = &ObserverInfo> + '_ {
        self.core.observers.values().map(|slot| &slot.info)
    }

    /// One observer (verified principal, display name). Also answers for
    /// the observer leaving during [`Zone::on_unobserve`].
    pub fn observer(&self, id: ObserverId) -> Option<&ObserverInfo> {
        self.core.observers.get(&id).map(|slot| &slot.info).or(self
            .core
            .departing
            .as_ref()
            .filter(|info| info.id == id))
    }

    /// Observers whose verified principal is `principal` (a principal may
    /// observe through several connections).
    pub fn observers_of<'p>(&'p self, principal: &'p str) -> impl Iterator<Item = ObserverId> + 'p {
        self.observers()
            .filter(move |info| info.principal == principal)
            .map(|info| info.id)
    }

    /// Consume buffered inputs of `player`; advances its ack.
    pub fn take_inputs(
        &mut self,
        player: PlayerId,
        policy: ConsumePolicy,
    ) -> Vec<SequencedInput<Z::Input>> {
        self.core
            .players
            .get_mut(&player)
            .map(|slot| slot.inputs.take(policy))
            .unwrap_or_default()
    }

    /// Typed fact for a cell command input, keyed restart-safely:
    /// `"{C::COMMAND}:{zone}:{boot_epoch}:{parts…}"` with the input's
    /// [`dedupe_parts`](FactCommand::dedupe_parts), or `t{tick}:{n}` (the
    /// n-th such fact this tick) when it has none. Emit it with
    /// [`emit_fact_for`](Self::emit_fact_for) (e.g. to keep it in a retry
    /// backlog) or use [`emit`](Self::emit) directly.
    pub fn fact<C: FactCommand>(&mut self, input: &C) -> Result<Fact, EmitError> {
        let parts = input.dedupe_parts();
        let key = if parts.is_empty() {
            let tick = self.core.tick;
            if self.core.fact_seq.0 != tick {
                self.core.fact_seq = (tick, 0);
            }
            let n = self.core.fact_seq.1;
            self.core.fact_seq.1 += 1;
            self.fact_key(C::COMMAND, &[&format_args!("t{tick}"), &n])
        } else {
            let parts: Vec<&dyn std::fmt::Display> =
                parts.iter().map(|p| p as &dyn std::fmt::Display).collect();
            self.fact_key(C::COMMAND, &parts)
        };
        Fact::typed(key, input).map_err(|e| EmitError::Encode(e.to_string()))
    }

    /// [`fact`](Self::fact) with explicit dedupe parts (overriding the
    /// input's own).
    pub fn fact_with_key<C: FactCommand>(
        &self,
        input: &C,
        parts: &[&dyn std::fmt::Display],
    ) -> Result<Fact, EmitError> {
        Fact::typed(self.fact_key(C::COMMAND, parts), input)
            .map_err(|e| EmitError::Encode(e.to_string()))
    }

    /// Emit a typed cell command input on behalf of `player`: the principal
    /// is bound from the player's verified ticket (the input carries no
    /// identity), the key is derived as in [`fact`](Self::fact). Returns
    /// the key. Never blocks.
    pub fn emit<C: FactCommand>(
        &mut self,
        player: PlayerId,
        input: C,
    ) -> Result<String, EmitError> {
        let fact = self.fact(&input)?;
        let key = fact.key().to_string();
        self.emit_fact_for(player, fact).map(|()| key)
    }

    /// [`emit`](Self::emit) with explicit dedupe parts.
    pub fn emit_keyed<C: FactCommand>(
        &mut self,
        player: PlayerId,
        input: C,
        parts: &[&dyn std::fmt::Display],
    ) -> Result<String, EmitError> {
        let fact = self.fact_with_key(&input, parts)?;
        let key = fact.key().to_string();
        self.emit_fact_for(player, fact).map(|()| key)
    }

    /// Emit a typed zone/system fact (no principal; the sink's system
    /// principal applies). Returns the key.
    pub fn emit_zone<C: FactCommand>(&mut self, input: C) -> Result<String, EmitError> {
        let fact = self.fact(&input)?;
        let key = fact.key().to_string();
        self.emit_fact(fact).map(|()| key)
    }

    /// Emit a zone/system fact (no principal). Never blocks.
    pub fn emit_fact(&mut self, fact: Fact) -> Result<(), EmitError> {
        self.emit_bound(fact, None)
    }

    /// Emit a fact on behalf of a principal verified elsewhere: one carried
    /// by a zone message ([`ZoneSender::principal`]), e.g. the shooter of a
    /// shot that crossed into this zone. Never blocks.
    pub fn emit_fact_as(
        &mut self,
        principal: &VerifiedPrincipal,
        fact: Fact,
    ) -> Result<(), EmitError> {
        self.emit_bound(fact, Some(principal.as_str().to_string()))
    }

    /// `player`'s principal, bound from its verified ticket (for carrying
    /// in game state; facts bind it themselves).
    pub fn verified_principal(&self, player: PlayerId) -> Option<VerifiedPrincipal> {
        self.core
            .players
            .get(&player)
            .map(|slot| VerifiedPrincipal::new(slot.info.principal.clone()))
    }

    /// Send `message` to the running zone `to` ([`crate::zone_message`]):
    /// it arrives in [`Zone::on_zone_message`] there, at most once, or not
    /// at all (a zone that is not running refuses it).
    pub fn send_zone(&mut self, to: &str, message: &Z::Message) {
        self.queue_zone(to, None, message);
    }

    /// [`send_zone`](Self::send_zone) on behalf of `player`: its verified
    /// principal travels with the message (the receiver may emit facts as
    /// it). Nothing is sent for an unknown player.
    pub fn send_zone_for(&mut self, player: PlayerId, to: &str, message: &Z::Message) {
        if let Some(principal) = self.verified_principal(player) {
            self.queue_zone(to, Some(principal), message);
        }
    }

    /// Send `message` on behalf of a principal this zone was itself sent
    /// ([`ZoneSender::principal`]): e.g. passing a shot on again.
    pub fn send_zone_as(&mut self, principal: &VerifiedPrincipal, to: &str, message: &Z::Message) {
        self.queue_zone(to, Some(principal.clone()), message);
    }

    pub(super) fn queue_zone(
        &mut self,
        to: &str,
        principal: Option<VerifiedPrincipal>,
        message: &Z::Message,
    ) {
        self.core.zone_seq += 1;
        let id = format!(
            "{}/{}/{}",
            self.core.zone_id, self.core.config.boot_epoch, self.core.zone_seq
        );
        self.core.zone_out.push(ZoneEnvelope {
            id,
            from: self.core.zone_id.clone(),
            to: to.to_string(),
            principal: principal.map(|p| p.as_str().to_string()),
            type_id: message.event_id(),
            payload: message.to_payload(),
        });
    }

    /// Emit a fact on behalf of `player`; the principal is bound from the
    /// player's verified ticket. Never blocks.
    pub fn emit_fact_for(&mut self, player: PlayerId, fact: Fact) -> Result<(), EmitError> {
        let principal = self
            .core
            .players
            .get(&player)
            .map(|slot| slot.info.principal.clone())
            .ok_or(EmitError::UnknownPlayer)?;
        self.emit_bound(fact, Some(principal))
    }

    pub(super) fn emit_bound(
        &mut self,
        fact: Fact,
        principal: Option<String>,
    ) -> Result<(), EmitError> {
        let stamp = self.core.stamp_fact();
        self.core
            .facts
            .as_mut()
            .ok_or(EmitError::NoSink)?
            .try_send(fact.bind(principal, stamp))
    }

    /// [`emit`](Self::emit) that never answers [`EmitError::Backpressure`]:
    /// when the fact queue is full the fact waits in the host's backlog,
    /// in order behind facts queued earlier (the lifecycle facts use the
    /// same backlog), and is sent as capacity frees, at the start of each
    /// advance. It is stamped now and counts in
    /// [`facts_in_flight`](Self::facts_in_flight) from now on, so the zone
    /// does not go Dormant with it waiting; its settlement arrives in
    /// [`Zone::on_fact_settled`] as any other. A key already in flight is
    /// not sent twice (the waiting copy is dropped). A fact still waiting
    /// when the queue is abandoned is lost with the queue. Returns the key.
    pub fn emit_queued<C: FactCommand>(
        &mut self,
        player: PlayerId,
        input: C,
    ) -> Result<String, EmitError> {
        let fact = self.fact(&input)?;
        let key = fact.key().to_string();
        self.queue_fact_for(player, fact).map(|()| key)
    }

    /// [`emit_fact_for`](Self::emit_fact_for) through the backlog, as
    /// [`emit_queued`](Self::emit_queued).
    pub fn queue_fact_for(&mut self, player: PlayerId, fact: Fact) -> Result<(), EmitError> {
        let principal = self
            .core
            .players
            .get(&player)
            .map(|slot| slot.info.principal.clone())
            .ok_or(EmitError::UnknownPlayer)?;
        self.queue_bound(fact, Some(principal))
    }

    /// [`emit_fact_as`](Self::emit_fact_as) through the backlog, as
    /// [`emit_queued`](Self::emit_queued).
    pub fn queue_fact_as(
        &mut self,
        principal: &VerifiedPrincipal,
        fact: Fact,
    ) -> Result<(), EmitError> {
        self.queue_bound(fact, Some(principal.as_str().to_string()))
    }

    pub(super) fn queue_bound(
        &mut self,
        fact: Fact,
        principal: Option<String>,
    ) -> Result<(), EmitError> {
        if self.core.facts.is_none() {
            return Err(EmitError::NoSink);
        }
        let stamp = self.core.stamp_fact();
        self.core.queue(fact.bind(principal, stamp));
        Ok(())
    }

    /// Unsettled facts (the backlog included).
    pub fn facts_in_flight(&self) -> usize {
        self.core.in_flight()
    }

    /// Facts waiting in the backlog for queue capacity
    /// ([`emit_queued`](Self::emit_queued)).
    pub fn facts_queued(&self) -> usize {
        self.core.backlog.len()
    }

    /// Lag-compensation history (filled by [`Zone::record_rewind`]).
    pub fn rewind(&self) -> &Rewind {
        &self.core.rewind
    }

    /// Set the zone's static level geometry (replaces any before). Call it
    /// from [`Zone::on_create`], which runs on every wake.
    pub fn set_colliders(&mut self, colliders: Colliders) {
        self.core.colliders = colliders;
    }

    /// The zone's static level geometry.
    pub fn colliders(&self) -> &Colliders {
        &self.core.colliders
    }

    /// Closest hit of `ray` now: the latest recorded entities
    /// ([`Zone::record_rewind`]) and the colliders.
    pub fn raycast(&self, ray: &Ray) -> Option<RayHit> {
        let rewind = &self.core.rewind;
        match rewind.latest() {
            Some(frame) => rewind.raycast_at(frame.time(), ray, &self.core.colliders, |_| true),
            None => self.core.colliders.raycast(ray),
        }
    }

    /// Closest hit of `ray` with the entities as they were at `time` (from
    /// [`Rewind::resolve`]) and the colliders.
    pub fn raycast_at(&self, time: Duration, ray: &Ray) -> Option<RayHit> {
        self.raycast_where(time, ray, |_| true)
    }

    /// [`raycast_at`](Self::raycast_at) passing through the entities
    /// `accept` refuses (game state a layer cannot say, e.g. a mob killed
    /// since the shooter's view time).
    pub fn raycast_where(
        &self,
        time: Duration,
        ray: &Ray,
        accept: impl FnMut(u32) -> bool,
    ) -> Option<RayHit> {
        self.core
            .rewind
            .raycast_at(time, ray, &self.core.colliders, accept)
    }

    /// Lag-compensated shot: closest hit of `ray` with the entities as the
    /// shooter saw them (`timing`, clamped by [`RewindConfig::max_rewind`])
    /// and the colliders; the latest state when the view time cannot be
    /// resolved.
    pub fn raycast_shot(&self, timing: ShotTiming, ray: &Ray) -> Option<RayHit> {
        match self.core.rewind.resolve(timing) {
            Some(resolved) => self.raycast_at(resolved.time, ray),
            None => self.raycast(ray),
        }
    }

    /// Reliable event to one viewer (dropped if it has no live connection).
    pub fn send_event(&mut self, to: impl Into<Viewer>, type_id: u32, payload: Vec<u8>) {
        if let Some(client) = self.core.client_of(to.into()) {
            self.core
                .server_message(client, &ServerMessage::Event { type_id, payload });
        }
    }

    /// Reliable event to every connection.
    pub fn broadcast_event(&mut self, type_id: u32, payload: Vec<u8>) {
        let frame = ServerMessage::Event { type_id, payload }
            .encode(&())
            .expect("event encodes");
        let clients: Vec<_> = self.core.clients.keys().copied().collect();
        for client in clients {
            self.core.send(client, frame.clone());
        }
    }

    /// Typed reliable event to one viewer (dropped if it has no live
    /// connection).
    pub fn send<E: GameEvent>(&mut self, to: impl Into<Viewer>, event: &E) {
        self.send_event(to, event.event_id(), event.to_payload());
    }

    /// Typed reliable event to every connection.
    pub fn broadcast<E: GameEvent>(&mut self, event: &E) {
        self.broadcast_event(event.event_id(), event.to_payload());
    }

    /// A viewer's area of interest (None for an unknown viewer).
    pub fn view_of(&self, viewer: impl Into<Viewer>) -> Option<View> {
        match viewer.into() {
            Viewer::Player(p) => self.core.players.get(&p).map(|slot| slot.view),
            Viewer::Observer(o) => self.core.observers.get(&o).map(|slot| slot.view),
        }
    }

    /// Set a viewer's area of interest.
    pub fn set_view(&mut self, viewer: impl Into<Viewer>, view: View) {
        match viewer.into() {
            Viewer::Player(p) => {
                if let Some(slot) = self.core.players.get_mut(&p) {
                    slot.view = view;
                }
            }
            Viewer::Observer(o) => {
                if let Some(slot) = self.core.observers.get_mut(&o) {
                    slot.view = view;
                }
            }
        }
    }

    /// Remove a player after the current hook returns.
    pub fn kick(&mut self, player: PlayerId, message: impl Into<String>) {
        self.core.pending_kicks.push((player, message.into()));
    }

    /// Provenance of anything produced now (zone, activation, tick, wall
    /// time, content and config version); embed it in events so clients
    /// and logs can tell which content version produced them.
    pub fn stamp(&self) -> FactStamp {
        self.core.stamp()
    }

    /// Move `player` to `target_zone` with `carried` state (any [`Wire`]
    /// value, e.g. a `#[derive(GameRecord)]` struct; the target decodes it
    /// in [`Zone::on_handoff_join`] with
    /// [`codec::from_bytes`](crate::codec::from_bytes)). A signed transfer
    /// ticket is issued now; after the current hook returns the player is
    /// removed (`LeaveReason::Handoff`) and its client is told
    /// `Handoff(target, ticket)`. The player can never be in both zones: it
    /// leaves this one before the ticket is usable, and the ticket is
    /// single-use.
    ///
    /// **Custody.** From then on the carried state exists only inside the
    /// ticket, which the client holds: it is delivered at most once. A
    /// client that drops the ticket (or deliberately discards one carrying
    /// state it dislikes and joins afresh), a ticket that expires, or an
    /// issuing host that restarts before the target reserves it
    /// (`unknown transfer`) loses the carried state, and the player is in
    /// neither zone until it joins again. Anything that must survive that
    /// belongs in a durable record, moved by a fact from the hook (see the
    /// handoff section of the README), not in `carried`.
    pub fn handoff<C: Wire>(
        &mut self,
        player: PlayerId,
        target_zone: &str,
        carried: &C,
    ) -> Result<(), HandoffError> {
        if target_zone == self.core.zone_id {
            return Err(HandoffError::SameZone);
        }
        let carried = crate::codec::to_bytes(carried);
        if carried.len() > MAX_CARRIED_BYTES {
            return Err(HandoffError::TooLarge);
        }
        let (principal, name, extensions) = self
            .core
            .players
            .get(&player)
            .map(|slot| {
                (
                    slot.info.principal.clone(),
                    slot.info.name.clone(),
                    slot.info.extensions.clone(),
                )
            })
            .ok_or(HandoffError::UnknownPlayer)?;
        if self
            .core
            .pending_handoffs
            .iter()
            .any(|(queued, ..)| *queued == player)
        {
            return Err(HandoffError::Refused);
        }
        let departed_at_ms = self.core.stamp_ms();
        let request = HandoffRequest {
            principal,
            name,
            extensions,
            source_zone: self.core.zone_id.clone(),
            target_zone: target_zone.to_string(),
            carried,
            departed_at_ms,
        };
        let issuer = self
            .core
            .transfer_issuer
            .as_mut()
            .ok_or(HandoffError::NoIssuer)?;
        let ticket = issuer(&request).ok_or(HandoffError::Refused)?;
        self.core
            .pending_handoffs
            .push((player, request.target_zone, ticket, departed_at_ms));
        Ok(())
    }

    /// Send a tier-up event to the parent zone (and, while `levels` remain,
    /// further up). Delivered asynchronously on a later tick of the parent.
    pub fn emit_upward(&mut self, kind: impl Into<String>, payload: Vec<u8>, levels: u8) {
        if levels == 0 {
            return;
        }
        self.core.upward.push(ChildEvent {
            origin_zone: self.core.zone_id.clone(),
            from_zone: self.core.zone_id.clone(),
            kind: kind.into(),
            payload,
            origin_tick: self.core.tick,
            content_version: self.core.config.content_version.clone(),
            levels,
        });
    }
}
