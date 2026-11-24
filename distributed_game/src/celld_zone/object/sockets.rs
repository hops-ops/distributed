//! Client sockets: admission (Hello), keeping the zone awake, dropping.

use super::*;

impl<Z: CelldZone> ZoneObject<Z> {
    pub(super) async fn hello(&self, ws: &WebSocket, hello: crate::protocol::Hello) -> Result<()> {
        let now = (now_ms() / 1000.0) as u64;
        let admitted = {
            let inner = self.inner.borrow();
            let admission = inner.admission.as_ref().expect("configured");
            admit(admission, &hello, &inner.zone_id, now)
        };
        let admitted = match admitted {
            Ok(admitted) => admitted,
            Err(refusal) => {
                reject(ws, refusal.code, &refusal.message);
                return Ok(());
            }
        };
        let cold = self.inner.borrow().host.is_none();
        let mut principal = None;
        let mut wake_grant = false;
        let mut woke = false;
        let joined = match admitted {
            Admitted::Reconnect(token) => {
                // Sessions die with the activation: rejoin with a ticket
                // (which wakes the zone for a player, not for an observer).
                let mut inner = self.inner.borrow_mut();
                let Some(host) = inner.host.as_mut() else {
                    drop(inner);
                    reject(ws, error::RECONNECT_FAILED, "zone restarted; rejoin");
                    return Ok(());
                };
                host.reconnect_with_version(&token, hello.capabilities, &hello.content_version)
            }
            // A transfer's single use is recorded by the target object
            // alone: a return to the source would make it usable twice.
            Admitted::Return(_) => {
                reject(ws, error::UNAUTHORIZED, "ticket is for another zone");
                return Ok(());
            }
            admitted @ (Admitted::Join(_) | Admitted::Arrival(_)) => {
                let claims = admitted.claims().expect("a ticket").clone();
                // A passive watcher never wakes a sleeping zone; a near one
                // whose ticket grants `wake` does.
                let observer = claims.role == Role::Observer;
                wake_grant = observer && claims.wake;
                if cold && observer && !(hello.near && wake_grant) {
                    reject(ws, error::ASLEEP, "zone asleep");
                    return Ok(());
                }
                // Transfer tickets are single use across activations too.
                let arrival = matches!(admitted, Admitted::Arrival(_));
                let nonce_key = format!("{NONCE_PREFIX}{}", claims.nonce);
                let used_before = arrival
                    && self
                        .state
                        .storage()
                        .get::<u64>(&nonce_key)
                        .await
                        .ok()
                        .flatten()
                        .is_some();
                let consumed = admitted.consume(&mut self.inner.borrow_mut().nonces, now);
                if used_before || consumed.is_err() {
                    reject(ws, error::UNAUTHORIZED, "ticket already used");
                    return Ok(());
                }
                if arrival {
                    let _ = self
                        .state
                        .storage()
                        .put(&nonce_key, claims.expires_at)
                        .await;
                }
                woke = match self.wake().await {
                    Ok(woke) => woke,
                    Err(e) => {
                        reject(ws, error::NOT_FOUND, &e.to_string());
                        return Ok(());
                    }
                };
                if claims.role == Role::Player {
                    principal = Some(claims.principal.clone());
                }
                // A client on another content version than the zone: the
                // zone may be behind the world (it slept through a
                // rollout), so it learns the live version before deciding.
                let behind = {
                    let inner = self.inner.borrow();
                    inner
                        .options
                        .as_ref()
                        .is_some_and(|o| o.content_rollout.is_some())
                        && inner
                            .host
                            .as_ref()
                            .is_some_and(|h| h.config().content_version != hello.content_version)
                };
                if behind {
                    if let Some(world) = self.content_world() {
                        self.reconcile(&topic(CONTENT_ROLLOUT_KIND, &world)).await;
                    }
                }
                let mut inner = self.inner.borrow_mut();
                let Some(host) = inner.host.as_mut() else {
                    drop(inner);
                    reject(ws, error::ZONE_UNAVAILABLE, "zone asleep");
                    return Ok(());
                };
                match &admitted {
                    Admitted::Arrival(transfer) => {
                        host.join_transfer(transfer, hello.capabilities, &hello.content_version)
                    }
                    _ => host.join(&claims, hello.capabilities, &hello.content_version),
                }
            }
        };
        match joined {
            Ok(joined) => {
                let mut observer = matches!(joined.viewer, Viewer::Observer(_));
                {
                    let mut inner = self.inner.borrow_mut();
                    let activation = inner.activation.clone();
                    ws.serialize_attachment(Attachment::Joined {
                        client: joined.client.0,
                        activation,
                    })?;
                    inner.sockets.insert(joined.client.0, ws.clone());
                    if observer && wake_grant {
                        inner.may_wake.insert(joined.client.0);
                    }
                    if observer && hello.near {
                        // A near watcher (with the grant) keeps the zone
                        // awake like a player; without it, it is told so
                        // after its Welcome and stays passive.
                        inner.flush();
                        inner.set_near(joined.client.0, true);
                        observer = !wake_grant;
                    }
                    if observer && !inner.ticking {
                        // A dormant zone does not tick: greet the watcher
                        // with its current state (it stays dormant).
                        if let Some(host) = inner.host.as_mut() {
                            host.patch_now();
                        }
                    }
                    inner.flush();
                    if !observer && inner.state != RoomState::Active {
                        inner.state = RoomState::Active;
                        inner.report(ACTIVITY_ACTIVE);
                    }
                }
                if observer {
                    if woke {
                        self.store_status().await;
                    }
                    return Ok(());
                }
                let joined_at = now_ms();
                self.store_status().await;
                self.start_loop();
                {
                    let mut inner = self.inner.borrow_mut();
                    let trace = &mut inner.trace;
                    if trace.welcome_at.is_none() {
                        trace.welcome_at = Some(joined_at);
                        trace.join_ms = Some(now_ms() - joined_at);
                    }
                }
                if let Some(principal) = principal {
                    self.after_join(principal);
                }
            }
            Err(e) => {
                reject(ws, e.code, &e.message);
                if woke {
                    self.store_status().await;
                }
            }
        }
        Ok(())
    }

    /// A near watcher arrived on an awake zone that is not ticking
    /// (dormant): it is active again and ticks.
    pub(super) async fn keep_awake(&self) {
        {
            let mut inner = self.inner.borrow_mut();
            if inner.host.is_none() {
                return;
            }
            if inner.state != RoomState::Active {
                inner.state = RoomState::Active;
                inner.report(ACTIVITY_ACTIVE);
            }
        }
        self.store_status().await;
        self.start_loop();
    }

    pub(super) fn after_join(&self, principal: String) {
        let Some(work) = Z::after_join(&self.env, &principal) else {
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
                // Facts the message produced are written on the next tick.
            }
        });
    }

    pub(super) fn drop_socket(&self, ws: &WebSocket) {
        let attached: Option<Attachment> = ws.deserialize_attachment().ok().flatten();
        let mut inner = self.inner.borrow_mut();
        let client = match attached {
            Some(Attachment::Joined { client, activation }) if activation == inner.activation => {
                Some(client)
            }
            _ => None,
        };
        if let Some(client) = client {
            inner.sockets.remove(&client);
            inner.forget(client);
            if let Some(host) = inner.host.as_mut() {
                host.disconnect(ClientId(client), false);
            }
            inner.flush();
            drop(inner);
            self.spool_later();
        }
    }

    pub(super) fn start_loop(&self) {
        {
            let mut inner = self.inner.borrow_mut();
            if inner.ticking || inner.host.is_none() {
                return;
            }
            inner.ticking = true;
        }
        let this = self.handle();
        wasm_bindgen_futures::spawn_local(async move { this.tick_loop().await });
    }

    pub(super) fn handle(&self) -> Self {
        Self {
            state: self.state.clone(),
            env: self.env.clone(),
            inner: self.inner.clone(),
            binding: self.binding.clone(),
        }
    }
}
