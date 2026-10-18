//! Content-version policy shared by [`ZoneHost`](crate::ZoneHost) and the
//! world barrier (`net::WorldCoordinator`).
//!
//! A world runs one content version at a time. A rollout is a two-phase
//! barrier: every zone freezes (and acknowledges), the new version is
//! applied, every zone resumes on it. What happens to clients still running
//! the *old* client build is the [`StalePolicy`] plus the [`BarrierMode`].

/// What a host does with a client whose Hello content version differs from
/// the zone's current content version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StalePolicy {
    /// Reject every admission with a different version
    /// (`Error(VERSION_MISMATCH)`). Matches the pre-barrier behavior.
    #[default]
    Strict,
    /// "Keep playing until a safe room": a stale client already in the world
    /// keeps its current zone (reconnects are allowed) and may only be
    /// handed off into a gate zone ([`Zone::is_gate`](crate::Zone::is_gate))
    /// unless `roam` is true. On entering a gate it is admitted, then told
    /// `Kick(RELOAD)` and kept in the reconnect grace window, so the reloaded
    /// client reconnects with its token and the new version. A fresh join
    /// with a stale version is rejected (`VERSION_MISMATCH`, "reload").
    SafeRoom {
        /// Also allow stale clients to be handed off into non-gate zones.
        roam: bool,
    },
}

/// How a committed rollout treats connected clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BarrierMode {
    /// Zones resume on the new version; stale clients keep playing under the
    /// [`StalePolicy`] until they reach a gate (or reconnect).
    #[default]
    Rolling,
    /// Zones resume on the new version and every stale client is sent
    /// `Kick(RELOAD)` immediately (kept in the reconnect grace window).
    Hard,
}

/// Freeze progress of one zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FreezeState {
    /// Simulating.
    Running,
    /// A freeze was requested; the zone is not ready yet
    /// ([`Zone::freeze_ready`](crate::Zone::freeze_ready)) and still ticks.
    Freezing,
    /// Frozen: no ticks, inputs dropped and counted.
    Frozen,
}
