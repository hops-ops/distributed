//! [`Stimulus`] and its dispatch ([`ZoneHost::apply`]), the recording tap
//! and the shorthands drivers call.

use super::*;

/// Something from outside the zone that changes it: the one way anything
/// does. [`ZoneHost::apply`] runs it with the clock read once on arrival;
/// the typed methods ([`join`](ZoneHost::join),
/// [`push_input`](ZoneHost::push_input), [`advance`](ZoneHost::advance), …)
/// are shorthand for it. A host with [`HostConfig::record`] keeps every
/// stimulus, with that clock reading and what it observed, in its
/// [`Recording`](crate::replay::Recording).
///
/// Game messages travel encoded (`type_id` + payload, as on the wire), so
/// a stimulus is independent of the zone type.
#[derive(Debug, Clone, PartialEq)]
pub enum Stimulus {
    /// Admit a verified ticket holder.
    Join {
        /// Verified claims.
        claims: TicketClaims,
        /// Hello capabilities.
        capabilities: u32,
        /// The client's content version.
        content_version: String,
    },
    /// Admit a player arriving with a verified transfer ticket: the zone
    /// sees [`Zone::on_handoff_join`] with the carried state (also when the
    /// principal is still present, e.g. inside its reconnect grace).
    JoinTransfer {
        /// Verified transfer.
        transfer: Transfer,
        /// Hello capabilities.
        capabilities: u32,
        /// The client's content version.
        content_version: String,
    },
    /// Resume a player's session with the token from its last Welcome.
    /// With `content_version` (the client's, possibly reloaded) a stale
    /// version is refused under [`StalePolicy::Strict`]; under `SafeRoom`
    /// it is admitted to this zone and told `Kick(RELOAD)` again if this
    /// zone is a gate.
    Reconnect {
        /// Reconnect token (in a recording: its place in the host's token
        /// sequence, `token#N`).
        token: String,
        /// Hello capabilities.
        capabilities: u32,
        /// The client's content version, when it presented one.
        content_version: Option<String>,
    },
    /// A connection ended. Graceful (Leave) removes the player now;
    /// otherwise the player stays (disconnected) for `reconnect_grace`.
    Disconnect {
        /// Connection.
        client: ClientId,
        /// Leave vs drop.
        graceful: bool,
    },
    /// A wire input from `client`, decoded and sanitized by the zone.
    Input {
        /// Connection.
        client: ClientId,
        /// Sequence.
        seq: u32,
        /// Client tick.
        client_tick: u32,
        /// Encoded input.
        payload: Vec<u8>,
    },
    /// A client patched its content live and now runs `version`
    /// (`ClientMessage::Content`): kept for its player only when it is the
    /// zone's current version (a client can catch up, never get ahead).
    Content {
        /// Connection.
        client: ClientId,
        /// Version it runs.
        version: String,
    },
    /// An observer's `Focus` ([`Zone::on_observer_focus`]); ignored for
    /// players and unknown clients.
    Focus {
        /// Connection.
        client: ClientId,
        /// Focus x.
        x: f32,
        /// Focus y.
        y: f32,
    },
    /// Service inbox ([`crate::inbox`]): remove every connection of
    /// `principal` (players leave with [`LeaveReason::Kicked`]).
    Kick {
        /// Principal kicked.
        principal: String,
        /// Reason sent to its clients.
        reason: String,
    },
    /// Service inbox: an encoded `Z::Message` for [`Zone::on_message`].
    Message {
        /// Message type id.
        type_id: u32,
        /// Encoded payload.
        payload: Vec<u8>,
    },
    /// A message from another zone ([`Zone::on_zone_message`]), unless its
    /// id was seen lately.
    ZoneMessage(ZoneEnvelope),
    /// A tier-up event from a child zone: delivered to
    /// [`Zone::on_child_event`] on a later tick (or the next advance when
    /// the zone is not ticking), never while frozen.
    ChildEvent(ChildEvent),
    /// Barrier phase 1: freeze as soon as [`Zone::freeze_ready`] allows
    /// (now, or at the end of a later advance; the zone keeps ticking until
    /// then).
    RequestFreeze(Notice),
    /// Abort a barrier: drop a pending freeze request, and resume with the
    /// notice if already frozen.
    CancelFreeze(Notice),
    /// Resume after a freeze; the frozen backlog is not simulated.
    Resume(Notice),
    /// Switch to a new content version
    /// ([`Zone::on_content_version_changed`]); facts and stamps produced
    /// afterwards carry it.
    ContentVersion(String),
    /// Switch the remote config version ([`HostConfig::config_version`]);
    /// facts and stamps produced afterwards carry it.
    ConfigVersion(String),
    /// Tell a connected player to reload its content (`Kick(RELOAD)`) and
    /// keep it in the reconnect grace window so it can reconnect with its
    /// last Welcome's token after reloading.
    Reload(PlayerId),
    /// [`Reload`](Self::Reload) every connected player whose content
    /// version differs from the zone's (hard rollout).
    ReloadStale,
    /// Kick a player now.
    KickPlayer {
        /// Player.
        player: PlayerId,
        /// Reason sent to its client.
        message: String,
    },
    /// Tell a player to move to `target_zone`, then remove it
    /// (`LeaveReason::Handoff`); the transfer is the caller's concern.
    Handoff {
        /// Player.
        player: PlayerId,
        /// Zone it moves to.
        target_zone: String,
        /// Transfer ticket it presents there.
        ticket: String,
    },
    /// Hand a hibernated snapshot to the zone ([`Zone::on_restore`]),
    /// right after construction.
    Restore {
        /// Snapshot bytes.
        state: Vec<u8>,
        /// Content version it was written under.
        saved_version: String,
    },
    /// Kick everyone (`on_leave(Disposed)` / `on_unobserve`), call
    /// `on_dispose`, clear timers. The host stops ticking afterwards.
    Dispose,
    /// Run due ticks and patches for the time elapsed on the clock.
    Advance,
}

/// What a [`Stimulus`] answered.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// Nothing to report.
    Done,
    /// A join or reconnect.
    Joined(Result<Joined, JoinError>),
    /// An input.
    Input(PushOutcome),
    /// A kick or message from the service inbox.
    Delivered(Delivered),
    /// A zone message.
    ZoneMessage(Result<(), ZoneMessageError>),
    /// A freeze request.
    Freeze(FreezeState),
    /// Players told to reload.
    Reloaded(usize),
    /// Whether a client's content patch was kept.
    Patched(bool),
    /// An advance.
    Advanced(AdvanceReport),
}

/// Recording kept by a host with [`HostConfig::record`].
pub(super) struct Tape {
    pub(super) recording: Recording,
    /// Tokens issued so far: a reconnect is recorded by its token's place
    /// in this sequence, never the secret.
    pub(super) tokens: Vec<String>,
}

impl<Z: Zone> ZoneHost<Z> {
    /// Run one [`Stimulus`]: the single way the zone changes from outside.
    /// The clock is read once here; the host and the zone see that time
    /// throughout ([`Ctx::now`], fact stamps, grace deadlines). With
    /// [`HostConfig::record`] the stimulus, that time and what it observed
    /// (join and input outcomes, settlements and per-tick state hashes) are
    /// appended to the [`recording`](Self::recording).
    pub fn apply(&mut self, stimulus: Stimulus) -> Outcome {
        // Never backwards, whatever the clock does.
        self.core.now = self.core.clock.now().max(self.core.now);
        self.core.unix_ms = self.core.clock.unix_ms().max(self.core.unix_ms);
        let recorded = self.tape.as_ref().map(|tape| tape.redact(&stimulus));
        let outcome = self.dispatch(stimulus);
        if let (Some(stimulus), Some(tape)) = (recorded, &mut self.tape) {
            tape.recording.entries.push(Entry {
                at: self.core.now,
                unix_ms: self.core.unix_ms,
                stimulus,
                observed: Observed::of(&outcome),
            });
        }
        outcome
    }

    pub(super) fn dispatch(&mut self, stimulus: Stimulus) -> Outcome {
        match stimulus {
            Stimulus::Join {
                claims,
                capabilities,
                content_version,
            } => Outcome::Joined(self.admit(&claims, capabilities, &content_version, None)),
            Stimulus::JoinTransfer {
                transfer,
                capabilities,
                content_version,
            } => Outcome::Joined(self.admit_transfer(&transfer, capabilities, &content_version)),
            Stimulus::Reconnect {
                token,
                capabilities,
                content_version,
            } => {
                Outcome::Joined(self.reconnect_as(&token, capabilities, content_version.as_deref()))
            }
            Stimulus::Disconnect { client, graceful } => {
                self.drop_client(client, graceful);
                Outcome::Done
            }
            Stimulus::Input {
                client,
                seq,
                client_tick,
                payload,
            } => Outcome::Input(self.offer_input(client, seq, client_tick, &payload)),
            Stimulus::Content { client, version } => {
                Outcome::Patched(self.patch_content(client, &version))
            }
            Stimulus::Focus { client, x, y } => {
                self.focus_observer(client, x, y);
                Outcome::Done
            }
            Stimulus::Kick { principal, reason } => {
                Outcome::Delivered(self.kick_principal(&principal, reason))
            }
            Stimulus::Message { type_id, payload } => {
                if let Ok(Some(message)) = Z::Message::decode_event(type_id, &payload) {
                    hook!(self.on_message(message));
                    self.process_kicks();
                }
                Outcome::Delivered(Delivered::default())
            }
            Stimulus::ZoneMessage(envelope) => {
                Outcome::ZoneMessage(self.deliver_envelope(envelope))
            }
            Stimulus::ChildEvent(event) => {
                self.core.child_inbox.push(event);
                Outcome::Done
            }
            Stimulus::RequestFreeze(notice) => Outcome::Freeze(self.ask_freeze(notice)),
            Stimulus::CancelFreeze(notice) => {
                self.abort_freeze(notice);
                Outcome::Done
            }
            Stimulus::Resume(notice) => {
                self.thaw(notice);
                Outcome::Done
            }
            Stimulus::ContentVersion(version) => {
                self.switch_content(version);
                Outcome::Done
            }
            Stimulus::ConfigVersion(version) => {
                self.core.config.config_version = version;
                Outcome::Done
            }
            Stimulus::Reload(player) => {
                self.reload(player);
                Outcome::Done
            }
            Stimulus::ReloadStale => Outcome::Reloaded(self.reload_stale()),
            Stimulus::KickPlayer { player, message } => {
                self.core.pending_kicks.push((player, message));
                self.process_kicks();
                Outcome::Done
            }
            Stimulus::Handoff {
                player,
                target_zone,
                ticket,
            } => {
                self.hand_off(player, &target_zone, &ticket);
                Outcome::Done
            }
            Stimulus::Restore {
                state,
                saved_version,
            } => {
                self.restore_snapshot(&state, &saved_version);
                Outcome::Done
            }
            Stimulus::Dispose => {
                self.dispose_zone();
                Outcome::Done
            }
            Stimulus::Advance => Outcome::Advanced(self.step()),
        }
    }

    /// The recording so far ([`HostConfig::record`]; `None` when off).
    pub fn recording(&self) -> Option<&Recording> {
        self.tape.as_ref().map(|tape| &tape.recording)
    }

    pub(super) fn next_token(&mut self) -> String {
        let token = (self.token_source)();
        if let Some(tape) = &mut self.tape {
            tape.tokens.push(token.clone());
        }
        token
    }

    // ---- Shorthand for `apply` ----

    /// [`Stimulus::Join`].
    pub fn join(
        &mut self,
        claims: &TicketClaims,
        capabilities: u32,
        content_version: &str,
    ) -> Result<Joined, JoinError> {
        self.apply(Stimulus::Join {
            claims: claims.clone(),
            capabilities,
            content_version: content_version.into(),
        })
        .joined()
    }

    /// [`Stimulus::JoinTransfer`].
    pub fn join_transfer(
        &mut self,
        transfer: &Transfer,
        capabilities: u32,
        content_version: &str,
    ) -> Result<Joined, JoinError> {
        self.apply(Stimulus::JoinTransfer {
            transfer: transfer.clone(),
            capabilities,
            content_version: content_version.into(),
        })
        .joined()
    }

    /// [`Stimulus::Reconnect`] without a content version.
    pub fn reconnect(&mut self, token: &str, capabilities: u32) -> Result<Joined, JoinError> {
        self.apply(Stimulus::Reconnect {
            token: token.into(),
            capabilities,
            content_version: None,
        })
        .joined()
    }

    /// [`Stimulus::Reconnect`] presenting the client's content version.
    pub fn reconnect_with_version(
        &mut self,
        token: &str,
        capabilities: u32,
        content_version: &str,
    ) -> Result<Joined, JoinError> {
        self.apply(Stimulus::Reconnect {
            token: token.into(),
            capabilities,
            content_version: Some(content_version.into()),
        })
        .joined()
    }

    /// [`Stimulus::Disconnect`].
    pub fn disconnect(&mut self, client: ClientId, graceful: bool) {
        self.apply(Stimulus::Disconnect { client, graceful });
    }

    /// [`Stimulus::Input`].
    pub fn push_input(
        &mut self,
        client: ClientId,
        seq: u32,
        client_tick: u32,
        payload: &[u8],
    ) -> PushOutcome {
        match self.apply(Stimulus::Input {
            client,
            seq,
            client_tick,
            payload: payload.to_vec(),
        }) {
            Outcome::Input(outcome) => outcome,
            other => unreachable!("input answered {other:?}"),
        }
    }

    /// [`Stimulus::Content`]: whether it was kept.
    pub fn content_patched(&mut self, client: ClientId, version: &str) -> bool {
        matches!(
            self.apply(Stimulus::Content {
                client,
                version: version.into(),
            }),
            Outcome::Patched(true)
        )
    }

    /// [`Stimulus::Focus`].
    pub fn observer_focus(&mut self, client: ClientId, x: f32, y: f32) {
        self.apply(Stimulus::Focus { client, x, y });
    }

    /// Deliver a service message ([`crate::inbox`]): [`Stimulus::Kick`] or
    /// [`Stimulus::Message`].
    pub fn deliver(&mut self, inbound: ZoneInbound<Z::Message>) -> Delivered {
        let stimulus = match inbound {
            ZoneInbound::Kick { principal, reason } => Stimulus::Kick { principal, reason },
            ZoneInbound::Message(message) => Stimulus::Message {
                type_id: message.event_id(),
                payload: message.to_payload(),
            },
        };
        match self.apply(stimulus) {
            Outcome::Delivered(delivered) => delivered,
            other => unreachable!("delivery answered {other:?}"),
        }
    }

    /// [`Stimulus::ZoneMessage`].
    pub fn deliver_zone(&mut self, envelope: ZoneEnvelope) -> Result<(), ZoneMessageError> {
        match self.apply(Stimulus::ZoneMessage(envelope)) {
            Outcome::ZoneMessage(result) => result,
            other => unreachable!("zone message answered {other:?}"),
        }
    }

    /// [`Stimulus::ChildEvent`].
    pub fn deliver_child_event(&mut self, event: ChildEvent) {
        self.apply(Stimulus::ChildEvent(event));
    }

    /// [`Stimulus::RequestFreeze`]: the resulting state.
    pub fn request_freeze(&mut self, notice: Notice) -> FreezeState {
        match self.apply(Stimulus::RequestFreeze(notice)) {
            Outcome::Freeze(state) => state,
            other => unreachable!("freeze request answered {other:?}"),
        }
    }

    /// [`Stimulus::CancelFreeze`].
    pub fn cancel_freeze(&mut self, notice: Notice) {
        self.apply(Stimulus::CancelFreeze(notice));
    }

    /// [`Stimulus::Resume`].
    pub fn resume(&mut self, notice: Notice) {
        self.apply(Stimulus::Resume(notice));
    }

    /// [`Stimulus::ContentVersion`].
    pub fn set_content_version(&mut self, version: impl Into<String>) {
        self.apply(Stimulus::ContentVersion(version.into()));
    }

    /// [`Stimulus::ConfigVersion`].
    pub fn set_config_version(&mut self, version: impl Into<String>) {
        self.apply(Stimulus::ConfigVersion(version.into()));
    }

    /// [`Stimulus::Reload`].
    pub fn reload_player(&mut self, player: PlayerId) {
        self.apply(Stimulus::Reload(player));
    }

    /// [`Stimulus::ReloadStale`]: the number reloaded.
    pub fn reload_stale_players(&mut self) -> usize {
        match self.apply(Stimulus::ReloadStale) {
            Outcome::Reloaded(n) => n,
            other => unreachable!("reload answered {other:?}"),
        }
    }

    /// [`Stimulus::KickPlayer`].
    pub fn kick(&mut self, player: PlayerId, message: impl Into<String>) {
        self.apply(Stimulus::KickPlayer {
            player,
            message: message.into(),
        });
    }

    /// [`Stimulus::Handoff`].
    pub fn handoff(&mut self, player: PlayerId, target_zone: &str, transfer_ticket: &str) {
        self.apply(Stimulus::Handoff {
            player,
            target_zone: target_zone.into(),
            ticket: transfer_ticket.into(),
        });
    }

    /// [`Stimulus::Restore`].
    pub fn restore(&mut self, state: &[u8], saved_version: &str) {
        self.apply(Stimulus::Restore {
            state: state.to_vec(),
            saved_version: saved_version.into(),
        });
    }

    /// [`Stimulus::Dispose`].
    pub fn dispose(&mut self) {
        self.apply(Stimulus::Dispose);
    }

    /// [`Stimulus::Advance`].
    pub fn advance(&mut self) -> AdvanceReport {
        match self.apply(Stimulus::Advance) {
            Outcome::Advanced(report) => report,
            other => unreachable!("advance answered {other:?}"),
        }
    }
}

impl Outcome {
    pub(super) fn joined(self) -> Result<Joined, JoinError> {
        match self {
            Self::Joined(result) => result,
            other => unreachable!("join answered {other:?}"),
        }
    }
}

impl Tape {
    /// `stimulus` as recorded: a reconnect token becomes its place in the
    /// token sequence (`token#N`; empty when this host never issued it).
    pub(super) fn redact(&self, stimulus: &Stimulus) -> Stimulus {
        match stimulus {
            Stimulus::Reconnect {
                token,
                capabilities,
                content_version,
            } => Stimulus::Reconnect {
                token: self
                    .tokens
                    .iter()
                    .position(|issued| issued == token)
                    .map_or_else(String::new, crate::replay::token_label),
                capabilities: *capabilities,
                content_version: content_version.clone(),
            },
            other => other.clone(),
        }
    }
}
