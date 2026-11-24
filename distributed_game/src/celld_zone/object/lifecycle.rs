//! The tick loop, persistence, dormancy and hibernation, and alarms.

use super::*;

impl<Z: CelldZone> ZoneObject<Z> {
    pub(super) async fn tick_loop(&self) {
        let (hz, persist_every, dormant_after) = {
            let mut inner = self.inner.borrow_mut();
            inner.empty_since_ms = None;
            let options = inner.options.as_ref().expect("configured");
            (
                options.tick_hz,
                options.persist_every.as_millis() as f64,
                options.dormant_after.as_millis() as f64,
            )
        };
        let mut schedule = TickSchedule::new(now_ms(), hz);
        // Crash watchdog: if this activation dies while awake, the alarm
        // wakes a fresh one that reports the room asleep. Armed once here and
        // again from `alarm` while ticking — never per snapshot: each
        // `set_alarm` leaves a wake entry celld scans when it starts.
        if let Some(watchdog) = self.watchdog_ms() {
            self.arm_later(watchdog);
        }
        loop {
            Delay::from(Duration::from_micros(
                (schedule.wait_ms(now_ms()) * 1000.0) as u64,
            ))
            .await;
            let woke = now_ms();
            let late = schedule.woke(woke);
            let (stop, persist, mail) = {
                let mut inner = self.inner.borrow_mut();
                let Some(host) = inner.host.as_mut() else {
                    inner.ticking = false;
                    return;
                };
                let report = host.advance();
                let players = host.player_count();
                inner.work.borrow_mut().record_advance(&report);
                if players < inner.players_seen {
                    inner.persist_soon = true;
                }
                inner.players_seen = players;
                inner.flush();
                let mail = inner
                    .host
                    .as_mut()
                    .map(ZoneHost::take_zone_messages)
                    .unwrap_or_default();
                let stats = &mut inner.stats;
                stats.iterations += 1;
                stats.late.push(late);
                stats.work.push(now_ms() - woke);
                stats.dropped += u64::from(report.dropped_ticks);
                if let Some(queue) = inner.activity.as_mut() {
                    queue.drain();
                }
                let stop = if inner.occupied() {
                    inner.empty_since_ms = None;
                    false
                } else {
                    woke - *inner.empty_since_ms.get_or_insert(woke) >= dormant_after
                };
                let persist =
                    stop || inner.persist_soon || woke - inner.last_persist_ms >= persist_every;
                if stop {
                    inner.ticking = false;
                }
                (stop, persist, mail)
            };
            self.send_zone_messages(mail);
            // Storage stays off the loop: the simulation keeps its pace
            // while a write waits on the bucket.
            self.spool_later();
            self.reconcile_world(false);
            self.refresh_if_due();
            if stop {
                self.persist().await;
                self.go_dormant().await;
                return;
            }
            if persist {
                self.persist_later();
            }
        }
    }

    /// Write the zone's snapshot (after the fact outbox writes queued by
    /// this tick, so a crash never keeps an outcome without its fact).
    pub(super) async fn persist(&self) {
        yield_now().await;
        let snapshot = {
            let mut inner = self.inner.borrow_mut();
            inner.last_persist_ms = now_ms();
            inner.persist_soon = false;
            inner.snapshot()
        };
        if let Some(snapshot) = snapshot {
            if let Err(e) = self.state.storage().put(SNAPSHOT_KEY, snapshot).await {
                worker::console_error!("zone snapshot write failed: {e}");
            }
        }
    }

    /// [`persist`](Self::persist) off the tick loop. One snapshot write at
    /// a time: while one is in flight a due tick leaves it to a later tick.
    /// Spawned after the tick's [`spool_later`](Self::spool_later), so the
    /// outbox write is issued before the snapshot that reflects it.
    pub(super) fn persist_later(&self) {
        {
            let mut inner = self.inner.borrow_mut();
            if inner.persisting {
                return;
            }
            inner.persisting = true;
        }
        let this = self.handle();
        wasm_bindgen_futures::spawn_local(async move {
            this.persist().await;
            this.inner.borrow_mut().persisting = false;
        });
    }

    pub(super) async fn go_dormant(&self) {
        let after = {
            let mut inner = self.inner.borrow_mut();
            if inner.occupied() {
                return;
            }
            inner.state = RoomState::Dormant { since_ms: now_ms() };
            inner.report(ACTIVITY_DORMANT);
            inner
                .options
                .as_ref()
                .map_or(10_000.0, |o| o.hibernate_after.as_millis() as f64)
        };
        self.store_status().await;
        self.arm(after).await;
    }

    pub(super) async fn hibernate(&self) {
        self.persist().await;
        {
            let mut inner = self.inner.borrow_mut();
            if inner.occupied() {
                return;
            }
            inner.report(ACTIVITY_HIBERNATED);
            // Watchers of a sleeping zone are let go (they may come back
            // and are told it is asleep).
            for (_, ws) in inner.sockets.drain() {
                let _ = ws.close(Some(1000), Some("zone asleep"));
            }
            inner.out_lines.clear();
            inner.in_lines.clear();
            inner.may_wake.clear();
            inner.near.clear();
            // Dropped, so the next join restores from storage.
            inner.host = None;
            inner.state = RoomState::Cold;
            inner.ticking = false;
        }
        self.store_status().await;
    }

    /// Write the facts emitted since the last call to the outbox.
    pub(super) async fn spool(&self) {
        self.spool_with(None).await;
    }

    /// [`spool`](Self::spool), with the zone's `status` in the same batch.
    pub(super) async fn spool_with(&self, status: Option<String>) {
        let facts = self.inner.borrow_mut().emitted();
        if facts.is_empty() && status.is_none() {
            return;
        }
        // One batched write (the storage API takes at most 128 keys).
        let mut batches: Vec<(js_sys::Object, usize)> = facts
            .chunks(128)
            .map(|chunk| {
                let batch = js_sys::Object::new();
                for fact in chunk {
                    let text = serde_json::to_string(&StoredFact::of(fact)).unwrap_or_default();
                    let _ = js_sys::Reflect::set(
                        &batch,
                        &StoredFact::storage_key(fact.key()).into(),
                        &text.into(),
                    );
                }
                (batch, chunk.len())
            })
            .collect();
        if let Some(status) = status {
            if batches.last().is_none_or(|(_, keys)| *keys >= 128) {
                batches.push((js_sys::Object::new(), 0));
            }
            if let Some((batch, keys)) = batches.last_mut() {
                let _ = js_sys::Reflect::set(batch, &STATUS_KEY.into(), &status.into());
                *keys += 1;
            }
        }
        let storage = self.state.storage();
        for (batch, _) in batches {
            if let Err(e) = storage.put_multiple_raw(batch).await {
                worker::console_error!("fact outbox write failed: {e}");
            }
        }
        if !facts.is_empty() {
            self.inner.borrow_mut().spooled += facts.len() as u64;
            self.arm_outbox().await;
        }
    }

    /// [`spool`](Self::spool) from a synchronous handler.
    pub(super) fn spool_later(&self) {
        let this = self.handle();
        wasm_bindgen_futures::spawn_local(async move { this.spool().await });
    }

    /// Spool the pending facts and write the zone's status with them when
    /// it changed (one batch; an unchanged status writes nothing).
    pub(super) async fn store_status(&self) {
        let (status, known) = {
            let inner = self.inner.borrow();
            (inner.last_status.clone(), inner.stored_status.clone())
        };
        let stored = match known {
            Some(stored) => Some(stored),
            None => self
                .state
                .storage()
                .get::<String>(STATUS_KEY)
                .await
                .ok()
                .flatten(),
        };
        let changed = !status.is_empty() && stored.as_deref() != Some(status.as_str());
        self.inner.borrow_mut().stored_status = Some(if changed {
            status.clone()
        } else {
            stored.unwrap_or_default()
        });
        self.spool_with(changed.then_some(status)).await;
    }

    /// Set the object's alarm `in_ms` from now.
    pub(super) async fn arm(&self, in_ms: f64) {
        if let Err(e) = self
            .state
            .storage()
            .set_alarm(Duration::from_millis(in_ms.max(1.0) as u64))
            .await
        {
            worker::console_error!("zone alarm: {e}");
        }
    }

    /// The crash watchdog's period while ticking (three snapshot
    /// intervals, at least a minute), when configured and ticking.
    pub(super) fn watchdog_ms(&self) -> Option<f64> {
        let inner = self.inner.borrow();
        let options = inner.options.as_ref()?;
        (inner.ticking && inner.host.is_some())
            .then(|| (options.persist_every.as_millis() as f64 * 3.0).max(WATCHDOG_MIN_MS))
    }

    /// [`arm`](Self::arm) off the caller's path.
    pub(super) fn arm_later(&self, in_ms: f64) {
        let this = self.handle();
        wasm_bindgen_futures::spawn_local(async move { this.arm(in_ms).await });
    }

    /// Keep an alarm while unsettled facts wait in the outbox.
    pub(super) async fn arm_outbox(&self) {
        let pending = {
            let mut inner = self.inner.borrow_mut();
            // Settlements only leave the queue on `drain`; a hibernated
            // room has no tick loop to drain it, and would otherwise re-arm
            // this alarm forever on a report that settled long ago.
            if let Some(queue) = inner.activity.as_mut() {
                queue.drain();
            }
            inner.activity.as_ref().map_or(0, |q| q.in_flight())
                + inner.host.as_ref().map_or(0, |h| h.facts_in_flight())
        };
        let alarm = self.state.storage().get_alarm().await.ok().flatten();
        if pending > 0 && alarm.is_none() {
            self.arm(OUTBOX_RETRY_MS).await;
        }
    }
}
