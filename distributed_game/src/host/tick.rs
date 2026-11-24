//! The tick loop: tiers, fixed steps, settlements, patches and disposal.

use super::*;

impl<Z: Zone> ZoneHost<Z> {
    /// [`Stimulus::Dispose`].
    pub(super) fn dispose_zone(&mut self) {
        if self.disposed {
            return;
        }
        let bye = ServerMessage::Kick {
            code: kick::ZONE_DISPOSED,
            message: "zone disposed".into(),
        };
        let players: Vec<_> = self.core.players.keys().copied().collect();
        for player in players {
            self.remove_player(player, LeaveReason::Disposed, Some(bye.clone()));
        }
        let observers: Vec<_> = self.core.observers.keys().copied().collect();
        for observer in observers {
            self.remove_observer(observer, Some(&bye));
        }
        hook!(self.on_dispose());
        self.core.pending_kicks.clear();
        self.core.timers.clear();
        self.disposed = true;
    }

    pub(super) fn wake(&mut self) {
        let now = self.core.now;
        self.set_tier(Tier::Hot, now);
    }

    pub(super) fn set_tier(&mut self, to: Tier, now: Duration) {
        let from = self.core.tier;
        if from == to {
            return;
        }
        self.core.tier = to;
        self.hot.reset();
        if let Some(idle) = &mut self.idle {
            idle.reset();
        }
        match to {
            Tier::Hot => self.next_patch = now,
            Tier::Idle => self.idle_since = now,
            Tier::Dormant => {}
        }
        self.transitions.push((from, to));
        hook!(self.on_tier_changed(from, to));
        self.process_kicks();
    }

    pub(super) fn evaluate_tier(&mut self, now: Duration) {
        let present = !self.core.players.is_empty() || !self.core.clients.is_empty();
        if present || self.zone.has_pending_work() {
            self.set_tier(Tier::Hot, now);
            return;
        }
        match self.core.tier {
            Tier::Hot => self.set_tier(Tier::Idle, now),
            Tier::Idle => {
                if now.saturating_sub(self.idle_since) >= self.core.config.dormant_after
                    && self.core.in_flight() == 0
                    && !self.zone.wants_background()
                {
                    self.set_tier(Tier::Dormant, now);
                }
            }
            Tier::Dormant => {}
        }
    }

    pub(super) fn deliver_settlements(&mut self) {
        let tick = self.core.tick;
        let settled = match &mut self.core.facts {
            Some(queue) => queue.drain_at(tick),
            None => return,
        };
        for settlement in settled {
            self.pending.settled.push((tick, settlement.clone()));
            hook!(self.on_fact_settled(settlement));
        }
        self.process_kicks();
    }

    pub(super) fn deliver_child_events(&mut self) {
        if self.frozen || self.core.child_inbox.is_empty() {
            return;
        }
        for event in std::mem::take(&mut self.core.child_inbox) {
            hook!(self.on_child_event(&event));
            if event.levels > 1 {
                let mut forward = event;
                forward.levels -= 1;
                forward.from_zone = self.core.zone_id.clone();
                self.core.upward.push(forward);
            }
        }
        self.process_kicks();
    }

    pub(super) fn tick_once(&mut self, dt: Duration) {
        // wasm32-unknown-unknown (celld) has no `Instant`: measure with the
        // host clock there (the celld zone host).
        #[cfg(not(target_arch = "wasm32"))]
        let started = std::time::Instant::now();
        #[cfg(target_arch = "wasm32")]
        let started = self.core.clock.now();
        self.core.tick += 1;
        self.core.sim_time += dt;
        self.core.dt = dt;
        self.deliver_settlements();
        self.deliver_child_events();
        let due = self.core.timers.due(self.core.sim_time);
        for (id, timer) in due {
            hook!(self.on_timer(id, timer));
        }
        hook!(self.on_tick());
        self.core.dropped_ticks = 0;
        self.process_kicks();
        let mut frame = self.core.rewind.frame();
        self.zone.record_rewind(&mut frame);
        let entities = frame.into_entities();
        if !entities.is_empty() || !self.core.rewind.is_empty() {
            self.core
                .rewind
                .record(self.core.tick, self.core.sim_time, entities);
        }
        #[cfg(not(target_arch = "wasm32"))]
        self.pending.tick_durations.push(started.elapsed());
        #[cfg(target_arch = "wasm32")]
        self.pending
            .tick_durations
            .push(self.core.clock.now().saturating_sub(started));
        if self.hash_ticks {
            let hash = self.state_hash();
            self.pending.tick_hashes.push((self.core.tick, hash));
        }
    }

    /// This patch's replicated world: every registered entity collection's
    /// projected views ([`Zone::entities`]), then [`Zone::replicate`].
    pub fn world_view(&self) -> WorldView {
        let mut view = WorldView::default();
        self.entities.project(&self.zone, &mut view);
        self.zone.replicate(&mut view);
        view
    }

    pub(super) fn patch(&mut self) {
        let view = &mut self.view;
        view.clear();
        self.entities.project(&self.zone, view);
        self.zone.replicate(view);
        let core = &mut self.core;
        self.pending.entities = Some(view.entities().len());
        let interest = &core.config.interest;
        let mut world = match self.last_world.take() {
            Some(last) => last.next(&self.schema, view, interest),
            None => EncodedWorld::build(&self.schema, view, interest),
        };
        let tick = core.tick as u32;
        let budget = core.config.byte_budget;
        for (client, slot) in core.clients.iter_mut() {
            let (view, ack) = match slot.viewer {
                Viewer::Player(p) => match core.players.get(&p) {
                    Some(player) => (player.view, player.inputs.last_processed()),
                    None => continue,
                },
                Viewer::Observer(o) => match core.observers.get(&o) {
                    Some(observer) => (observer.view, 0),
                    None => continue,
                },
            };
            let (frame, stats) = slot.replica.build_delta(
                &self.schema,
                &mut world,
                &core.config.interest,
                &view,
                budget,
                tick,
                ack,
            );
            self.pending.frames.push(FrameStats {
                client: *client,
                bytes: frame.len(),
                stats,
            });
            core.outbound.push(Outbound::Frame {
                client: *client,
                frame,
            });
        }
        self.pending.encodes += world.encodes();
        self.last_world = Some(world);
    }

    /// [`Stimulus::Advance`].
    pub(super) fn step(&mut self) -> AdvanceReport {
        let mut report = std::mem::take(&mut self.pending);
        if self.disposed {
            return report;
        }
        let now = self.core.now;
        let elapsed = now.saturating_sub(self.last_advance);
        self.last_advance = now;
        self.core.flush_backlog();

        let expired: Vec<_> = self
            .core
            .players
            .values()
            .filter(|slot| slot.grace_until.is_some_and(|until| until <= now))
            .map(|slot| slot.info.id)
            .collect();
        for player in expired {
            self.remove_player(player, LeaveReason::Disconnected, None);
        }
        self.process_kicks();
        self.evaluate_tier(now);

        let (steps, dt) = match self.core.tier {
            Tier::Hot => (self.hot.advance(elapsed), self.hot.step()),
            Tier::Idle => match &mut self.idle {
                Some(idle) => (idle.advance(elapsed), idle.step()),
                None => Default::default(),
            },
            Tier::Dormant => Default::default(),
        };
        report.dropped = steps.dropped;
        if !dt.is_zero() {
            report.dropped_ticks = (steps.dropped.as_nanos() / dt.as_nanos()) as u32;
        }
        self.core.dropped_ticks = self.core.dropped_ticks.saturating_add(report.dropped_ticks);
        if !self.frozen {
            for _ in 0..steps.steps {
                self.tick_once(dt);
            }
            report.ticks = steps.steps;
        }
        if report.ticks == 0 {
            self.deliver_settlements();
            self.deliver_child_events();
        }
        self.try_freeze();

        let patch_interval = rate_to_step(self.core.config.patch_rate);
        if self.core.tier == Tier::Hot && !self.core.clients.is_empty() && now >= self.next_patch {
            self.patch();
            report.patched = true;
            self.next_patch = if now - self.next_patch >= patch_interval {
                now + patch_interval
            } else {
                self.next_patch + patch_interval
            };
        }

        let mut wake = match self.core.tier {
            Tier::Hot => {
                let mut at = now + self.hot.until_next();
                if !self.core.clients.is_empty() {
                    at = at.min(self.next_patch);
                }
                Some(at)
            }
            Tier::Idle => {
                let dormant_at = self.idle_since + self.core.config.dormant_after;
                let tick_at = self.idle.as_ref().map(|idle| now + idle.until_next());
                Some(tick_at.map_or(dormant_at, |t| t.min(dormant_at)).max(now))
            }
            Tier::Dormant => None,
        };
        for slot in self.core.players.values() {
            if let Some(until) = slot.grace_until {
                wake = Some(wake.map_or(until, |w| w.min(until)));
            }
        }
        report.next_wake = wake;
        report.transitions = std::mem::take(&mut self.transitions);
        let pending = std::mem::take(&mut self.pending);
        report.tick_durations.extend(pending.tick_durations);
        report.entities = pending.entities;
        report.frames.extend(pending.frames);
        report.encodes += pending.encodes;
        report.settled.extend(pending.settled);
        report.tick_hashes.extend(pending.tick_hashes);
        report
    }
}
