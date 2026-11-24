//! Content versions, reloads and the freeze around a content rollout.

use super::*;

impl<Z: Zone> ZoneHost<Z> {
    /// [`Stimulus::Reload`].
    pub(super) fn reload(&mut self, player: PlayerId) {
        let now = self.core.now;
        let grace = self.core.config.reconnect_grace;
        let message = ServerMessage::Kick {
            code: kick::RELOAD,
            message: format!("content {} required", self.core.config.content_version),
        };
        let Some(slot) = self.core.players.get_mut(&player) else {
            return;
        };
        let client = slot.client.take();
        slot.info.connected = false;
        slot.inputs.clear();
        slot.grace_until = Some(now + grace);
        if let Some(client) = client {
            self.core.clients.remove(&client);
            self.core.server_message(client, &message);
            self.core.outbound.push(Outbound::Close { client });
            self.connection_lost(player, LostCause::Reload);
        }
    }

    /// [`Stimulus::ReloadStale`].
    pub(super) fn reload_stale(&mut self) -> usize {
        let expected = self.core.config.content_version.clone();
        if expected.is_empty() {
            return 0;
        }
        let stale: Vec<_> = self
            .core
            .players
            .values()
            .filter(|slot| slot.client.is_some() && slot.content_version != expected)
            .map(|slot| slot.info.id)
            .collect();
        for player in &stale {
            self.reload(*player);
        }
        stale.len()
    }

    /// [`Stimulus::Content`].
    pub(super) fn patch_content(&mut self, client: ClientId, version: &str) -> bool {
        if version != self.core.config.content_version {
            return false;
        }
        let Some(Viewer::Player(player)) = self.viewer_of(client) else {
            return false;
        };
        match self.core.players.get_mut(&player) {
            Some(slot) => {
                slot.content_version = version.to_string();
                true
            }
            None => false,
        }
    }

    /// Content version a player's client reported.
    pub fn player_content_version(&self, player: PlayerId) -> Option<&str> {
        self.core
            .players
            .get(&player)
            .map(|slot| slot.content_version.as_str())
    }

    /// [`Stimulus::ContentVersion`].
    pub(super) fn switch_content(&mut self, version: String) {
        if version == self.core.config.content_version {
            return;
        }
        let previous = std::mem::replace(&mut self.core.config.content_version, version.clone());
        hook!(self.on_content_version_changed(&previous, &version));
        self.process_kicks();
    }

    /// Freeze: ticks stop and inputs are rejected; clients get Freeze.
    pub(super) fn freeze(&mut self, notice: Notice) {
        self.freeze_request = None;
        self.frozen = true;
        self.broadcast(&ServerMessage::Freeze(notice));
    }

    /// [`Stimulus::RequestFreeze`].
    pub(super) fn ask_freeze(&mut self, notice: Notice) -> FreezeState {
        if !self.frozen {
            self.freeze_request = Some(notice);
            self.try_freeze();
        }
        self.freeze_state()
    }

    /// [`Stimulus::CancelFreeze`].
    pub(super) fn abort_freeze(&mut self, notice: Notice) {
        self.freeze_request = None;
        if self.frozen {
            self.thaw(notice);
        }
    }

    /// Freeze progress.
    pub fn freeze_state(&self) -> FreezeState {
        if self.frozen {
            FreezeState::Frozen
        } else if self.freeze_request.is_some() {
            FreezeState::Freezing
        } else {
            FreezeState::Running
        }
    }

    pub(super) fn try_freeze(&mut self) {
        if self.freeze_request.is_some() && !self.disposed && self.zone.freeze_ready() {
            let notice = self.freeze_request.take().expect("checked");
            self.freeze(notice);
        }
    }

    /// [`Stimulus::Resume`]: the frozen backlog is not simulated.
    pub(super) fn thaw(&mut self, notice: Notice) {
        self.freeze_request = None;
        self.frozen = false;
        self.hot.reset();
        if let Some(idle) = &mut self.idle {
            idle.reset();
        }
        self.broadcast(&ServerMessage::Resume(notice));
    }
}
