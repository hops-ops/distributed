//! Players and observers coming and going: admission, reconnects,
//! inputs, kicks and handoffs.

use super::*;

impl<Z: Zone> ZoneHost<Z> {
    /// [`Stimulus::JoinTransfer`]: from now on this zone stamps facts later
    /// than the source stamped the departure ([`Transfer::departed_at_ms`]),
    /// whatever the two hosts' clocks say.
    pub(super) fn admit_transfer(
        &mut self,
        transfer: &Transfer,
        capabilities: u32,
        content_version: &str,
    ) -> Result<Joined, JoinError> {
        if transfer.claims.role != Role::Player {
            return Err(JoinError::new(
                error::UNAUTHORIZED,
                "transfers are for players",
            ));
        }
        self.core.stamp_floor_ms = self
            .core
            .stamp_floor_ms
            .max(transfer.departed_at_ms.saturating_add(1));
        self.admit(
            &transfer.claims,
            capabilities,
            content_version,
            Some((transfer.source_zone.as_str(), transfer.carried.clone())),
        )
    }

    pub(super) fn admit(
        &mut self,
        claims: &TicketClaims,
        capabilities: u32,
        content_version: &str,
        handoff: Option<(&str, Vec<u8>)>,
    ) -> Result<Joined, JoinError> {
        if self.disposed {
            return Err(JoinError::new(error::NOT_FOUND, "zone disposed"));
        }
        if claims.zone != self.core.zone_id {
            return Err(JoinError::new(
                error::UNAUTHORIZED,
                "ticket is for another zone",
            ));
        }
        let expected = &self.core.config.content_version;
        let mut reload = false;
        if !expected.is_empty() && content_version != expected {
            let allowed = match self.core.config.stale_policy {
                StalePolicy::Strict => false,
                StalePolicy::SafeRoom { roam } => {
                    let gate = self.zone.is_gate();
                    reload = gate;
                    handoff.is_some() && claims.role == Role::Player && (gate || roam)
                }
            };
            if !allowed {
                return Err(JoinError::new(
                    error::VERSION_MISMATCH,
                    format!("content version {content_version} != {expected}: reload"),
                ));
            }
        }
        let existing = self
            .core
            .players
            .values()
            .find(|slot| slot.info.principal == claims.principal)
            .map(|slot| slot.info.id);
        let capacity_ok = match claims.role {
            Role::Player => {
                existing.is_some() || self.core.players.len() < self.core.config.max_players
            }
            Role::Observer => self.core.observers.len() < self.core.config.max_observers,
        };
        if !capacity_ok {
            return Err(JoinError::new(error::ZONE_FULL, "zone is full"));
        }
        let request = JoinRequest {
            claims,
            capabilities,
            content_version,
        };
        if let Admission::Reject(message) = hook!(self.on_auth(&request)) {
            self.process_kicks();
            return Err(JoinError::new(error::REJECTED, message));
        }
        self.wake();
        let client = ClientId(self.core.next_id());
        let mut handoff = handoff;
        let joined = match (claims.role, existing) {
            (Role::Player, Some(player)) => {
                if let Some(slot) = self.core.players.get_mut(&player) {
                    if claims.name.is_some() {
                        slot.info.name = claims.name.clone();
                    }
                    slot.info.extensions = claims.extensions.clone();
                }
                self.bind_player(player, client, capabilities);
                let zone = self.core.zone_id.clone();
                if let Some((from, carried)) = handoff.take() {
                    self.core.emit_lifecycle(
                        player,
                        PlayerJoined {
                            zone,
                            via: JoinVia::Handoff,
                            from_zone: Some(from.to_string()),
                        },
                    );
                    hook!(self.on_handoff_join(player, from, carried));
                } else {
                    self.core.emit_lifecycle(
                        player,
                        PlayerJoined {
                            zone,
                            via: JoinVia::Rejoin,
                            from_zone: None,
                        },
                    );
                    hook!(self.on_rejoin(player));
                }
                Joined {
                    client,
                    viewer: Viewer::Player(player),
                }
            }
            (Role::Player, None) => {
                let player = PlayerId(self.core.next_id());
                let radius = self.core.config.default_view_radius;
                let capacity = self.core.config.input_capacity;
                let session_id = self.next_token();
                self.core.players.insert(
                    player,
                    PlayerSlot {
                        info: PlayerInfo {
                            id: player,
                            principal: claims.principal.clone(),
                            name: claims.name.clone(),
                            extensions: claims.extensions.clone(),
                            capabilities,
                            connected: true,
                        },
                        inputs: InputBuffer::new(capacity),
                        client: None,
                        view: View::new(0.0, 0.0, radius),
                        grace_until: None,
                        reconnect_token: String::new(),
                        session_id,
                        content_version: content_version.to_string(),
                        transitions: 0,
                    },
                );
                self.bind_player(player, client, capabilities);
                let zone = self.core.zone_id.clone();
                match handoff.take() {
                    Some((from, carried)) => {
                        self.core.emit_lifecycle(
                            player,
                            PlayerJoined {
                                zone,
                                via: JoinVia::Handoff,
                                from_zone: Some(from.to_string()),
                            },
                        );
                        hook!(self.on_handoff_join(player, from, carried));
                    }
                    None => {
                        self.core.emit_lifecycle(
                            player,
                            PlayerJoined {
                                zone,
                                via: JoinVia::Ticket,
                                from_zone: None,
                            },
                        );
                        hook!(self.on_join(player));
                    }
                }
                Joined {
                    client,
                    viewer: Viewer::Player(player),
                }
            }
            (Role::Observer, _) => {
                let observer = ObserverId(self.core.next_id());
                let session_id = self.next_token();
                self.core.observers.insert(
                    observer,
                    ObserverSlot {
                        info: ObserverInfo {
                            id: observer,
                            principal: claims.principal.clone(),
                            name: claims.name.clone(),
                            extensions: claims.extensions.clone(),
                            capabilities,
                        },
                        client,
                        view: View::new(0.0, 0.0, self.core.config.default_view_radius),
                    },
                );
                self.attach_client(client, Viewer::Observer(observer));
                self.welcome(client, session_id, String::new(), Role::Observer);
                hook!(self.on_observe(observer));
                Joined {
                    client,
                    viewer: Viewer::Observer(observer),
                }
            }
        };
        if let Viewer::Player(player) = joined.viewer {
            if let Some(slot) = self.core.players.get_mut(&player) {
                slot.content_version = content_version.to_string();
            }
            if reload {
                self.reload(player);
            }
        }
        self.process_kicks();
        Ok(joined)
    }

    pub(super) fn reconnect_as(
        &mut self,
        token: &str,
        capabilities: u32,
        content_version: Option<&str>,
    ) -> Result<Joined, JoinError> {
        let player = (!token.is_empty() && !self.disposed)
            .then(|| {
                self.core
                    .players
                    .values()
                    .find(|slot| slot.reconnect_token == token)
                    .map(|slot| slot.info.id)
            })
            .flatten()
            .ok_or_else(|| JoinError::new(error::RECONNECT_FAILED, "unknown or expired token"))?;
        let expected = self.core.config.content_version.clone();
        let mut reload = false;
        if let Some(version) = content_version {
            if !expected.is_empty() && version != expected {
                if self.core.config.stale_policy == StalePolicy::Strict {
                    return Err(JoinError::new(
                        error::VERSION_MISMATCH,
                        format!("content version {version} != {expected}: reload"),
                    ));
                }
                reload = self.zone.is_gate();
            }
        }
        self.wake();
        let client = ClientId(self.core.next_id());
        self.bind_player(player, client, capabilities);
        if let Some(version) = content_version {
            if let Some(slot) = self.core.players.get_mut(&player) {
                slot.content_version = version.to_string();
            }
        }
        let zone = self.core.zone_id.clone();
        self.core.emit_lifecycle(player, PlayerReconnected { zone });
        hook!(self.on_reconnected(player));
        if reload {
            self.reload(player);
        }
        self.process_kicks();
        Ok(Joined {
            client,
            viewer: Viewer::Player(player),
        })
    }

    /// Grace started: lifecycle fact, then [`Zone::on_connection_lost`].
    pub(super) fn connection_lost(&mut self, player: PlayerId, cause: LostCause) {
        let lost = ConnectionLost {
            zone: self.core.zone_id.clone(),
            grace_ms: self.core.config.reconnect_grace.as_millis() as u64,
            cause,
        };
        self.core.emit_lifecycle(player, lost);
        hook!(self.on_connection_lost(player));
    }

    /// [`Stimulus::Focus`].
    pub(super) fn focus_observer(&mut self, client: ClientId, x: f32, y: f32) {
        if let Some(Viewer::Observer(observer)) = self.viewer_of(client) {
            hook!(self.on_observer_focus(observer, x, y));
        }
    }

    pub(super) fn attach_client(&mut self, client: ClientId, viewer: Viewer) {
        self.core.clients.insert(
            client,
            ClientSlot {
                viewer,
                replica: ClientReplica::new(),
            },
        );
    }

    pub(super) fn bind_player(&mut self, player: PlayerId, client: ClientId, capabilities: u32) {
        let token = self.next_token();
        let slot = self.core.players.get_mut(&player).expect("player exists");
        let previous = slot.client.replace(client);
        slot.grace_until = None;
        slot.info.connected = true;
        slot.info.capabilities = capabilities;
        slot.reconnect_token = token.clone();
        // Every Welcome restarts the client's input sequence (ticket or
        // reconnect token), so forget the previous connection's.
        slot.inputs.reset_sequence();
        let session_id = slot.session_id.clone();
        if let Some(old) = previous {
            self.core.clients.remove(&old);
            self.core.server_message(
                old,
                &ServerMessage::Kick {
                    code: kick::REPLACED,
                    message: "replaced by a newer connection".into(),
                },
            );
            self.core.outbound.push(Outbound::Close { client: old });
        }
        self.attach_client(client, Viewer::Player(player));
        self.welcome(client, session_id, token, Role::Player);
    }

    pub(super) fn welcome(
        &mut self,
        client: ClientId,
        session_id: String,
        token: String,
        role: Role,
    ) {
        let welcome = ServerMessage::Welcome(Welcome {
            session_id,
            zone_id: self.core.zone_id.clone(),
            role,
            tick_rate: self.core.config.tick_rate,
            patch_rate: self.core.config.patch_rate,
            server_tick: self.core.tick as u32,
            server_time: self.core.server_time_ms(),
            reconnect_token: token,
            content_version: self.core.config.content_version.clone(),
            schema: self.schema.clone(),
            features: feature::ALL,
            origin: self.zone.origin(),
        });
        self.core.server_message(client, &welcome);
    }

    /// [`Stimulus::Disconnect`].
    pub(super) fn drop_client(&mut self, client: ClientId, graceful: bool) {
        let Some(slot) = self.core.clients.remove(&client) else {
            return;
        };
        match slot.viewer {
            Viewer::Player(player) => {
                if graceful {
                    self.remove_player(player, LeaveReason::Left, None);
                } else if let Some(p) = self.core.players.get_mut(&player) {
                    p.client = None;
                    p.info.connected = false;
                    p.inputs.clear();
                    p.grace_until = Some(self.core.now + self.core.config.reconnect_grace);
                    self.connection_lost(player, LostCause::Dropped);
                }
            }
            Viewer::Observer(observer) => self.remove_observer(observer, None),
        }
        self.process_kicks();
    }

    /// [`Stimulus::Input`]: decoded and sanitized by the zone.
    pub(super) fn offer_input(
        &mut self,
        client: ClientId,
        seq: u32,
        client_tick: u32,
        payload: &[u8],
    ) -> PushOutcome {
        let Some(Viewer::Player(player)) = self.viewer_of(client) else {
            return PushOutcome::UnknownClient;
        };
        if self.frozen {
            self.core.dropped_frozen += 1;
            return PushOutcome::Rejected;
        }
        let Some(slot) = self.core.players.get(&player) else {
            return PushOutcome::UnknownClient;
        };
        if let Some(outcome) = slot.inputs.precheck(seq) {
            return outcome;
        }
        let Some(input) = self
            .zone
            .decode_input(player, payload)
            .and_then(|input| self.zone.sanitize_input(player, input))
        else {
            return PushOutcome::Rejected;
        };
        self.wake();
        self.core
            .players
            .get_mut(&player)
            .expect("player exists")
            .inputs
            .push(seq, client_tick, input)
    }

    /// [`Stimulus::Kick`]: every connection of `principal` goes (players
    /// leave with [`LeaveReason::Kicked`]).
    pub(super) fn kick_principal(&mut self, principal: &str, reason: String) -> Delivered {
        let mut delivered = Delivered::default();
        let players: Vec<_> = self
            .core
            .players
            .values()
            .filter(|slot| slot.info.principal == principal)
            .map(|slot| slot.info.id)
            .collect();
        delivered.players_kicked = players.len();
        for player in players {
            self.core.pending_kicks.push((player, reason.clone()));
        }
        let observers: Vec<_> = self
            .core
            .observers
            .values()
            .filter(|slot| slot.info.principal == principal)
            .map(|slot| slot.info.id)
            .collect();
        delivered.observers_kicked = observers.len();
        let bye = ServerMessage::Kick {
            code: kick::BY_ZONE,
            message: reason,
        };
        for observer in observers {
            self.remove_observer(observer, Some(&bye));
        }
        self.process_kicks();
        delivered
    }

    /// [`Stimulus::Handoff`].
    pub(super) fn hand_off(&mut self, player: PlayerId, target_zone: &str, transfer_ticket: &str) {
        let message = ServerMessage::Handoff {
            target_zone: target_zone.into(),
            transfer_ticket: transfer_ticket.into(),
        };
        self.remove_player(player, LeaveReason::Handoff, Some(message));
        self.process_kicks();
    }

    pub(super) fn remove_player(
        &mut self,
        player: PlayerId,
        reason: LeaveReason,
        last: Option<ServerMessage>,
    ) {
        let Some(slot) = self.core.players.remove(&player) else {
            return;
        };
        if let Some(client) = slot.client {
            self.core.clients.remove(&client);
            if let Some(message) = last {
                self.core.server_message(client, &message);
            }
            self.core.outbound.push(Outbound::Close { client });
        }
        hook!(self.on_leave(player, reason));
        if self.core.config.lifecycle_facts {
            let zone = self.core.zone_id.clone();
            let (principal, n) = (slot.info.principal, slot.transitions + 1);
            match LeftReason::from_leave(reason) {
                Some(reason) => {
                    self.core
                        .lifecycle(player, &principal, n, PlayerLeft { zone, reason })
                }
                None => self
                    .core
                    .lifecycle(player, &principal, n, PlayerDisconnected { zone }),
            }
        }
    }

    pub(super) fn remove_observer(&mut self, observer: ObserverId, last: Option<&ServerMessage>) {
        let Some(slot) = self.core.observers.remove(&observer) else {
            return;
        };
        self.core.clients.remove(&slot.client);
        if let Some(message) = last {
            self.core.server_message(slot.client, message);
            self.core.outbound.push(Outbound::Close {
                client: slot.client,
            });
        }
        self.core.departing = Some(slot.info);
        hook!(self.on_unobserve(observer));
        self.core.departing = None;
    }

    pub(super) fn process_kicks(&mut self) {
        while let Some((player, message, departed_at_ms)) = self.core.pending_handoffs.pop() {
            self.core.pinned_stamp_ms = Some(departed_at_ms);
            self.remove_player(player, LeaveReason::Handoff, Some(message));
            self.core.pinned_stamp_ms = None;
        }
        while let Some((player, message)) = self.core.pending_kicks.pop() {
            let kick = ServerMessage::Kick {
                code: kick::BY_ZONE,
                message,
            };
            self.remove_player(player, LeaveReason::Kicked, Some(kick));
        }
    }
}
