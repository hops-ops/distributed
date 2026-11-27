//! Refresh, world watches and pushes, zone messages and content rollout.

use super::*;

impl<Z: CelldZone> ZoneObject<Z> {
    /// Run [`CelldZone::refresh`] off the tick and deliver its message.
    pub(super) fn refresh(&self) {
        let (zone_id, round) = {
            let mut inner = self.inner.borrow_mut();
            inner.last_refresh_ms = now_ms();
            let round = inner.refresh_round;
            inner.refresh_round += 1;
            (inner.zone_id.clone(), round)
        };
        let Some(work) = Z::refresh(&self.env, &zone_id, round) else {
            return;
        };
        let inner = self.inner.clone();
        wasm_bindgen_futures::spawn_local(async move {
            if let Some(message) = work.await {
                let mut inner = inner.borrow_mut();
                if let Some(host) = inner.host.as_mut() {
                    host.deliver(ZoneInbound::Message(message));
                    inner.flush();
                }
            }
        });
    }

    /// [`refresh`](Self::refresh) when [`CelldZone::refresh_every`] is due.
    pub(super) fn refresh_if_due(&self) {
        let Some(every) = Z::refresh_every() else {
            return;
        };
        let due = now_ms() - self.inner.borrow().last_refresh_ms >= every.as_millis() as f64;
        if due {
            self.refresh();
        }
    }

    /// The world whose content rollout the zone follows, if any.
    pub(super) fn content_world(&self) -> Option<String> {
        let inner = self.inner.borrow();
        inner
            .options
            .as_ref()
            .and_then(|o| o.content_rollout.clone())
    }

    /// Every watched topic: the dev settings (`ZONE_DEV_SETTINGS=1`), the
    /// content rollout (`ZONE_CONTENT_ROLLOUT`) and the game's
    /// [`CelldZone::watch`].
    pub(super) fn world_topics(&self) -> Vec<String> {
        let (dev, zone_id) = {
            let inner = self.inner.borrow();
            let dev = inner.options.as_ref().is_some_and(|o| o.dev_settings);
            (dev, inner.zone_id.clone())
        };
        let watches = self
            .inner
            .borrow_mut()
            .watches
            .get_or_insert_with(|| Rc::new(Z::watch(&zone_id)))
            .clone();
        let mut topics = Vec::with_capacity(watches.len() + 2);
        if dev {
            topics.push(topic(ZONE_DEV_KIND, NETSIM_SHARD));
        }
        if let Some(world) = self.content_world() {
            topics.push(topic(CONTENT_ROLLOUT_KIND, &world));
        }
        topics.extend(watches.iter().map(|w| w.topic()));
        topics
    }

    /// Whether watched cells push to this object: it has a class binding
    /// and `ZONE_WORLD_PUSH` is not `0`.
    pub(super) fn pushed(&self) -> bool {
        !self.binding.is_empty()
            && self
                .inner
                .borrow()
                .options
                .as_ref()
                .is_some_and(|o| o.world_push)
    }

    /// Read the watched topics that are due (all of them with `all`, on
    /// wake) off the tick ([`WorldSync::due`]). When pushed, each read
    /// renews the zone's watch and the next comes ~45 s later (pushes
    /// carry the changes); otherwise the cells are polled every second.
    pub(super) fn reconcile_world(&self, all: bool) {
        let topics = self.world_topics();
        let pushed = self.pushed();
        let due = self.inner.borrow_mut().world.due(
            topics,
            now_ms() as u64,
            all,
            pushed,
            js_sys::Math::random,
        );
        for topic in due {
            let this = self.handle();
            wasm_bindgen_futures::spawn_local(async move { this.reconcile(&topic).await });
        }
    }

    /// Read watched `topic` (`kind/shard`), watching it when the object
    /// can take pushes, and apply its view.
    pub(super) async fn reconcile(&self, topic: &str) {
        let Some((kind, shard)) = topic.split_once('/') else {
            return;
        };
        let watcher = self.pushed().then(|| {
            let zone_id = self.inner.borrow().zone_id.clone();
            watcher_address(&self.binding, &zone_id)
        });
        self.inner.borrow().work.borrow_mut().world_reads += 1;
        match self
            .env
            .read_cell_watching(kind, shard, watcher.as_deref())
            .await
        {
            Ok(view) => self.apply_view(topic, view),
            Err(e) => worker::console_warn!("world view {topic}: {e}"),
        }
    }

    /// A pushed update (`POST …/__world`, internal secret): applied when
    /// the zone is in memory and watches the topic; a sleeping zone
    /// hydrates on wake instead (a push never wakes it).
    /// A message from another zone object ([`crate::zone_message`]):
    /// internal secret only; refused (409) while the zone is not running,
    /// so it never wakes one.
    pub(super) async fn take_zone_message(&self, mut req: Request) -> Result<Response> {
        if let Err(error) = super::super::cells::authenticate(&req, self.env.env()) {
            return super::super::cells::error_response(error);
        }
        let Ok(envelope) = req.json::<crate::zone_message::ZoneEnvelope>().await else {
            return Response::error("a zone message is required", 400);
        };
        let mut inner = self.inner.borrow_mut();
        let Some(host) = inner.host.as_mut() else {
            return Response::error("zone asleep", 409);
        };
        let delivered = host.deliver_zone(envelope);
        inner.flush();
        match delivered {
            Ok(()) => Response::empty().map(|r| r.with_status(204)),
            Err(crate::zone_message::ZoneMessageError::Duplicate) => {
                Response::empty().map(|r| r.with_status(204))
            }
            Err(e) => Response::error(e.to_string(), 409),
        }
    }

    /// Post the zone messages the tick sent to their target objects (same
    /// class), off the tick. Best effort: a refused or failed one is
    /// dropped (protocols that must not lose state carry their own acks).
    pub(super) fn send_zone_messages(&self, messages: Vec<crate::zone_message::ZoneEnvelope>) {
        if messages.is_empty() {
            return;
        }
        if self.binding.is_empty() {
            worker::console_error!("zone messages need the object's class binding (of_class)");
            return;
        }
        for envelope in messages {
            let env = self.env.env().clone();
            let binding = self.binding.clone();
            wasm_bindgen_futures::spawn_local(async move {
                match super::super::cells::send_zone_message(&env, &binding, &envelope).await {
                    Ok(204) => {}
                    Ok(status) => worker::console_log!(
                        "zone message {} to {} refused: {status}",
                        envelope.id,
                        envelope.to
                    ),
                    Err(e) => worker::console_error!("zone message to {}: {e}", envelope.to),
                }
            });
        }
    }

    pub(super) async fn take_push(&self, mut req: Request) -> Result<Response> {
        if let Err(error) = super::super::cells::authenticate(&req, self.env.env()) {
            return super::super::cells::error_response(error);
        }
        let Ok(update) = req.json::<WatchUpdate>().await else {
            return Response::error("a world update is required", 400);
        };
        let awake = {
            let inner = self.inner.borrow();
            inner.work.borrow_mut().world_pushes += 1;
            inner.host.is_some()
        };
        if awake && self.world_topics().contains(&update.topic) {
            self.apply_view(&update.topic, update.view);
        }
        Response::empty().map(|r| r.with_status(204))
    }

    /// Apply `view` of `topic` if it is newer than what the zone has.
    pub(super) fn apply_view(&self, topic: &str, view: Value) {
        let version = view_version(&view);
        if !self.inner.borrow_mut().world.accept(topic, version) {
            return;
        }
        let (kind, shard) = topic.split_once('/').unwrap_or((topic, ""));
        if kind == ZONE_DEV_KIND && shard == NETSIM_SHARD {
            let sim = serde_json::from_value::<NetSimJson>(view)
                .unwrap_or_default()
                .to_sim();
            self.inner.borrow_mut().net_sim = sim;
            return;
        }
        if kind == CONTENT_ROLLOUT_KIND && self.content_world().as_deref() == Some(shard) {
            let state = view
                .get("state")
                .cloned()
                .and_then(|state| serde_json::from_value::<ContentState>(state).ok());
            if let Some(state) = state {
                self.apply_content(&state);
            }
            return;
        }
        let watches = self.inner.borrow().watches.clone();
        let Some(message) = watches
            .as_ref()
            .and_then(|w| w.get(topic))
            .and_then(|watch| watch.message(view))
        else {
            return;
        };
        let mut inner = self.inner.borrow_mut();
        if let Some(host) = inner.host.as_mut() {
            host.deliver(ZoneInbound::Message(message));
            inner.flush();
        }
    }

    pub(super) fn apply_content(&self, state: &ContentState) {
        let mut inner = self.inner.borrow_mut();
        let Some(host) = inner.host.as_mut() else {
            return;
        };
        let frozen = host.freeze_state() != FreezeState::Running;
        let current = host.config().content_version.clone();
        match zone_step(state, &current, frozen) {
            ZoneStep::Stay => return,
            ZoneStep::Freeze => {
                host.request_freeze(Notice {
                    reason: "content_update".into(),
                    version: state.target.clone(),
                    notes: state.notes.clone(),
                });
            }
            ZoneStep::Apply(version) => {
                host.set_content_version(version.clone());
                if frozen {
                    host.resume(Notice {
                        reason: "content_updated".into(),
                        version,
                        notes: state.notes.clone(),
                    });
                }
                if state.hard {
                    host.reload_stale_players();
                }
            }
        }
        inner.flush();
    }
}
