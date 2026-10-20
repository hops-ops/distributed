//! Sorted-set leaderboards as a rebuildable projection target.
//!
//! Leaderboards are a poor fit for the relational read-model traits
//! ([`ReadModelWritePlanStore`](crate::ReadModelWritePlanStore) and friends):
//! the hot queries are *rank of member*, *top N* and *members around a member*,
//! which a sorted set answers in `O(log n)` while a SQL table needs a window
//! function or a full index scan per request. This module is therefore a
//! dedicated, typed seam rather than another relational backend.
//!
//! # Pieces
//!
//! - [`LeaderboardStore`] — the typed store API: [`submit`](LeaderboardStore::submit)
//!   a score under a [`ScorePolicy`], [`rank`](LeaderboardStore::rank),
//!   [`score`](LeaderboardStore::score), [`top`](LeaderboardStore::top),
//!   [`around`](LeaderboardStore::around), [`reset`](LeaderboardStore::reset)
//!   and the rebuild primitives ([`Board::staging`] + [`promote`](LeaderboardStore::promote)).
//! - [`InMemoryLeaderboard`] — a dependency-free implementation with the same
//!   ordering and tie-break semantics as Redis, for tests and demos.
//! - `RedisLeaderboard` (feature `redis`) — Redis sorted sets, with every
//!   idempotent write executed by one Lua script.
//! - [`LeaderboardProjector`] — maps [`DomainEventOccurrence`](crate::DomainEventOccurrence)s (directly, or
//!   decoded from an outbox [`Message`](crate::bus::Message)) to score changes
//!   through a [`LeaderboardProjection`], applies them idempotently and
//!   rebuilds boards from an event history.
//! - `BackgroundLeaderboard` (feature `redis`) — a bounded, fire-and-forget
//!   in-process queue in front of a projector, so a Redis outage never blocks
//!   gameplay.
//!
//! # GraphQL
//!
//! With the `graphql` feature, `GraphqlEngineBuilder::leaderboard` exposes a
//! store through the generated surface (top-N, around-me and my-rank roots,
//! `@live` subscriptions, the client manifest and `leaderboards.ts`), and
//! [`LeaderboardProjector::with_change_listener`] +
//! `GraphqlEngine::leaderboard_change_listener` refresh live queries after an
//! applied change. See `docs/live-query-delivery.md`.
//!
//! # How it plugs into projections
//!
//! A leaderboard is an *eventual* projection: the source of truth stays the
//! aggregate event stream, and the board can be thrown away and rebuilt at any
//! time. There are two ways to feed it:
//!
//! 1. **Outbox / bus (durable, recommended for production).** Publish domain
//!    events through the outbox as usual and subscribe a consumer whose handler
//!    calls [`LeaderboardProjector::apply_message`]. The handler returns a
//!    classified [`TransportError`](crate::bus::TransportError): a Redis outage
//!    is retryable (the message is redelivered), an undecodable payload is
//!    permanent. Because gameplay commands only commit to the event store and
//!    outbox, a Redis blip delays the board but never blocks a command.
//! 2. **In-process (best effort).** Hand committed occurrences to a
//!    `BackgroundLeaderboard` (feature `leaderboard-background`, implied by
//!    `redis`; needs Tokio, works over any store, e.g. the in-memory one),
//!    which applies them on a background task and drops (and counts)
//!    updates when its queue is full or the store stays down. Dropped
//!    updates are healed by the next rebuild.
//!
//! # Idempotency
//!
//! At-least-once delivery and replays must not double-increment. Each change
//! applied by the projector carries an [`ApplyKey`]: the occurrence id plus the
//! change's index within that occurrence (`"<occurrence id>#<index>"`), and the
//! occurrence timestamp. A store records applied keys *per board* in a second
//! structure (in Redis, a sorted set of applied ids scored by event time) and
//! checks-and-records them atomically with the score change (in Redis, one Lua
//! script, so the check, the `ZADD`/`ZINCRBY` and the record cannot interleave
//! with another writer). A duplicate returns [`ApplyOutcome::Duplicate`] and
//! changes nothing. Projections must therefore be deterministic: the same
//! occurrence must always produce the same changes in the same order.
//!
//! The applied-key set grows with the number of applied changes. Two ways keep
//! it bounded: [`LeaderboardStore::prune_applied`] forgets keys older than a
//! cutoff (safe once redelivery older than the cutoff is impossible — e.g.
//! beyond the bus retention window), and a rebuild starts from an empty set.
//!
//! # Rebuild
//!
//! [`LeaderboardProjector::begin_rebuild`] writes into the *staging* variant
//! of each board ([`Board::staging`]) and [`LeaderboardRebuild::commit`]
//! atomically promotes staging over live, so readers never see a half-built
//! board. Live updates applied while a rebuild runs land on the old live board
//! and are discarded by the promote; replay the tail of the stream from the
//! rebuild's starting cursor after committing — the promoted applied-key set
//! makes overlapping replay safe.

mod in_memory;
mod projector;

#[cfg(feature = "leaderboard-background")]
mod background;
#[cfg(feature = "redis")]
mod redis;

use std::fmt;

use async_trait::async_trait;

pub use in_memory::InMemoryLeaderboard;
pub use projector::{
    LeaderboardChange, LeaderboardChangeListener, LeaderboardProjection, LeaderboardProjector,
    LeaderboardRebuild, ProjectionReport,
};

#[cfg(feature = "leaderboard-background")]
pub use background::{BackgroundLeaderboard, BackgroundLeaderboardOptions, BackgroundStats};
#[cfg(feature = "redis")]
pub use redis::{RedisLeaderboard, DEFAULT_REDIS_LEADERBOARD_PREFIX};

/// Maximum board name length in bytes.
pub const MAX_BOARD_NAME_LEN: usize = 200;
/// Maximum member length in bytes.
pub const MAX_MEMBER_LEN: usize = 512;

/// Which end of the sorted set ranks first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScoreOrder {
    /// Highest score is rank 0 (points, kills, gold).
    HighFirst,
    /// Lowest score is rank 0 (speedrun times, deaths).
    LowFirst,
}

/// How a submitted score combines with a member's existing score.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScorePolicy {
    /// Keep the greater of the existing and submitted score (`ZADD GT`).
    Max,
    /// Keep the lesser of the existing and submitted score (`ZADD LT`).
    Min,
    /// Overwrite the existing score.
    Replace,
    /// Add the submitted score to the existing score (`ZINCRBY`); absent
    /// members start at zero.
    Increment,
}

impl ScorePolicy {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            ScorePolicy::Max => "max",
            ScorePolicy::Min => "min",
            ScorePolicy::Replace => "replace",
            ScorePolicy::Increment => "increment",
        }
    }
}

/// A named leaderboard and its rank order.
///
/// Every board has a *live* variant (what queries read) and a *staging*
/// variant (what a rebuild writes); [`LeaderboardStore::promote`] atomically
/// replaces live with staging.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Board {
    name: String,
    order: ScoreOrder,
    staging: bool,
}

impl Board {
    /// A live board. Names are 1..=[`MAX_BOARD_NAME_LEN`] bytes, without
    /// control characters or `{`/`}` (reserved for Redis Cluster hash tags).
    pub fn new(name: impl Into<String>, order: ScoreOrder) -> Result<Self, LeaderboardError> {
        let name = name.into();
        if name.is_empty()
            || name.len() > MAX_BOARD_NAME_LEN
            || name.chars().any(|c| c.is_control() || c == '{' || c == '}')
        {
            return Err(LeaderboardError::invalid(format!(
                "invalid board name {name:?}"
            )));
        }
        Ok(Self {
            name,
            order,
            staging: false,
        })
    }

    /// A live board where the highest score ranks first.
    pub fn high_first(name: impl Into<String>) -> Result<Self, LeaderboardError> {
        Self::new(name, ScoreOrder::HighFirst)
    }

    /// A live board where the lowest score ranks first.
    pub fn low_first(name: impl Into<String>) -> Result<Self, LeaderboardError> {
        Self::new(name, ScoreOrder::LowFirst)
    }

    /// The board name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The board's rank order.
    pub fn order(&self) -> ScoreOrder {
        self.order
    }

    /// Whether this is the staging (rebuild) variant.
    pub fn is_staging(&self) -> bool {
        self.staging
    }

    /// The staging variant of this board.
    pub fn staging(&self) -> Board {
        Board {
            staging: true,
            ..self.clone()
        }
    }

    /// The live variant of this board.
    pub fn live(&self) -> Board {
        Board {
            staging: false,
            ..self.clone()
        }
    }
}

/// One score submission for one member.
#[derive(Debug, Clone, PartialEq)]
pub struct ScoreSubmission {
    /// The ranked member (player id, guild id, ...).
    pub member: String,
    /// The submitted score. Must be finite.
    pub score: f64,
    /// How the score combines with the existing one.
    pub policy: ScorePolicy,
}

impl ScoreSubmission {
    /// Build a submission.
    pub fn new(member: impl Into<String>, score: f64, policy: ScorePolicy) -> Self {
        Self {
            member: member.into(),
            score,
            policy,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), LeaderboardError> {
        validate_member(&self.member)?;
        if !self.score.is_finite() {
            return Err(LeaderboardError::invalid("score must be finite"));
        }
        Ok(())
    }
}

pub(crate) fn validate_member(member: &str) -> Result<(), LeaderboardError> {
    if member.is_empty() || member.len() > MAX_MEMBER_LEN {
        return Err(LeaderboardError::invalid(format!(
            "member must be 1..={MAX_MEMBER_LEN} bytes"
        )));
    }
    Ok(())
}

/// The deduplication identity of one applied change.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ApplyKey {
    /// Stable id; the projector uses `"<occurrence id>#<change index>"`.
    pub id: String,
    /// Event time in Unix milliseconds; [`LeaderboardStore::prune_applied`]
    /// forgets keys older than a cutoff.
    pub at_unix_ms: u64,
}

impl ApplyKey {
    /// Build an apply key.
    pub fn new(id: impl Into<String>, at_unix_ms: u64) -> Self {
        Self {
            id: id.into(),
            at_unix_ms,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), LeaderboardError> {
        if self.id.is_empty() || self.id.len() > MAX_MEMBER_LEN {
            return Err(LeaderboardError::invalid(format!(
                "apply key must be 1..={MAX_MEMBER_LEN} bytes"
            )));
        }
        Ok(())
    }
}

/// Result of an idempotent [`LeaderboardStore::apply`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ApplyOutcome {
    /// The change was applied (a `Max`/`Min` submission that did not beat the
    /// existing score still counts as applied). Carries the resulting score.
    Applied {
        /// Member score after the change.
        score: f64,
    },
    /// The key was already applied; nothing changed.
    Duplicate {
        /// Current member score, if any.
        score: Option<f64>,
    },
}

impl ApplyOutcome {
    /// Whether this was a duplicate.
    pub fn is_duplicate(&self) -> bool {
        matches!(self, ApplyOutcome::Duplicate { .. })
    }
}

/// One ranked row.
#[derive(Debug, Clone, PartialEq)]
pub struct LeaderboardEntry {
    /// Zero-based rank in the board's [`ScoreOrder`].
    pub rank: u64,
    /// The member.
    pub member: String,
    /// The member's score.
    pub score: f64,
}

/// Leaderboard failure classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaderboardErrorKind {
    /// Bad input (board name, member, non-finite score) or an undecodable
    /// event. Retrying the same input will not help.
    Invalid,
    /// The backing store is unreachable or timed out. Retry later.
    Unavailable,
}

/// A leaderboard error.
#[derive(Debug, Clone)]
pub struct LeaderboardError {
    kind: LeaderboardErrorKind,
    message: String,
}

impl LeaderboardError {
    /// An invalid-input error.
    pub fn invalid(message: impl Into<String>) -> Self {
        Self {
            kind: LeaderboardErrorKind::Invalid,
            message: message.into(),
        }
    }

    /// A store-unavailable error.
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self {
            kind: LeaderboardErrorKind::Unavailable,
            message: message.into(),
        }
    }

    /// The classification.
    pub fn kind(&self) -> LeaderboardErrorKind {
        self.kind
    }

    /// The diagnostic message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Map to a transport error: unavailable is retryable, invalid permanent.
    pub fn into_transport_error(self) -> crate::bus::TransportError {
        match self.kind {
            LeaderboardErrorKind::Unavailable => {
                crate::bus::TransportError::retryable(self.to_string())
            }
            LeaderboardErrorKind::Invalid => {
                crate::bus::TransportError::permanent(self.to_string())
            }
        }
    }
}

impl fmt::Display for LeaderboardError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self.kind {
            LeaderboardErrorKind::Invalid => "invalid",
            LeaderboardErrorKind::Unavailable => "unavailable",
        };
        write!(f, "leaderboard error ({kind}): {}", self.message)
    }
}

impl std::error::Error for LeaderboardError {}

impl From<LeaderboardError> for crate::bus::TransportError {
    fn from(err: LeaderboardError) -> Self {
        err.into_transport_error()
    }
}

/// A sorted-set leaderboard store.
///
/// Ordering contract (shared by every implementation, matching Redis): rows
/// sort by score in the board's [`ScoreOrder`]; equal scores tie-break by the
/// member's bytes — ascending for [`ScoreOrder::LowFirst`], descending for
/// [`ScoreOrder::HighFirst`] (exactly `ZRANGE` / `ZREVRANGE`). Ranks are
/// zero-based.
#[async_trait]
pub trait LeaderboardStore: Send + Sync {
    /// Submit a score without deduplication and return the resulting score.
    ///
    /// Use for direct, already-authoritative writes. Projections should use
    /// [`apply`](LeaderboardStore::apply) so redelivery is harmless.
    async fn submit(
        &self,
        board: &Board,
        submission: &ScoreSubmission,
    ) -> Result<f64, LeaderboardError>;

    /// Apply a score submission at most once per `key` and board, atomically
    /// with recording the key.
    async fn apply(
        &self,
        board: &Board,
        submission: &ScoreSubmission,
        key: &ApplyKey,
    ) -> Result<ApplyOutcome, LeaderboardError>;

    /// A member's score, if ranked.
    async fn score(&self, board: &Board, member: &str) -> Result<Option<f64>, LeaderboardError>;

    /// A member's zero-based rank, if ranked.
    async fn rank(&self, board: &Board, member: &str) -> Result<Option<u64>, LeaderboardError>;

    /// The first `n` rows.
    async fn top(&self, board: &Board, n: usize)
        -> Result<Vec<LeaderboardEntry>, LeaderboardError>;

    /// Up to `radius` rows either side of `member`, plus the member; empty if
    /// the member is not ranked. Read atomically.
    async fn around(
        &self,
        board: &Board,
        member: &str,
        radius: usize,
    ) -> Result<Vec<LeaderboardEntry>, LeaderboardError>;

    /// Number of ranked members.
    async fn len(&self, board: &Board) -> Result<u64, LeaderboardError>;

    /// Remove a member; returns whether it was ranked.
    async fn remove(&self, board: &Board, member: &str) -> Result<bool, LeaderboardError>;

    /// Delete every score and applied key of this board variant.
    async fn reset(&self, board: &Board) -> Result<(), LeaderboardError>;

    /// Atomically replace the live board (scores and applied keys) with its
    /// staging variant. An absent staging board promotes as empty.
    async fn promote(&self, board: &Board) -> Result<(), LeaderboardError>;

    /// Forget applied keys recorded with `at_unix_ms < before_unix_ms`;
    /// returns how many were forgotten.
    async fn prune_applied(
        &self,
        board: &Board,
        before_unix_ms: u64,
    ) -> Result<u64, LeaderboardError>;
}

#[async_trait]
impl<T: LeaderboardStore + ?Sized> LeaderboardStore for std::sync::Arc<T> {
    async fn submit(
        &self,
        board: &Board,
        submission: &ScoreSubmission,
    ) -> Result<f64, LeaderboardError> {
        (**self).submit(board, submission).await
    }
    async fn apply(
        &self,
        board: &Board,
        submission: &ScoreSubmission,
        key: &ApplyKey,
    ) -> Result<ApplyOutcome, LeaderboardError> {
        (**self).apply(board, submission, key).await
    }
    async fn score(&self, board: &Board, member: &str) -> Result<Option<f64>, LeaderboardError> {
        (**self).score(board, member).await
    }
    async fn rank(&self, board: &Board, member: &str) -> Result<Option<u64>, LeaderboardError> {
        (**self).rank(board, member).await
    }
    async fn top(
        &self,
        board: &Board,
        n: usize,
    ) -> Result<Vec<LeaderboardEntry>, LeaderboardError> {
        (**self).top(board, n).await
    }
    async fn around(
        &self,
        board: &Board,
        member: &str,
        radius: usize,
    ) -> Result<Vec<LeaderboardEntry>, LeaderboardError> {
        (**self).around(board, member, radius).await
    }
    async fn len(&self, board: &Board) -> Result<u64, LeaderboardError> {
        (**self).len(board).await
    }
    async fn remove(&self, board: &Board, member: &str) -> Result<bool, LeaderboardError> {
        (**self).remove(board, member).await
    }
    async fn reset(&self, board: &Board) -> Result<(), LeaderboardError> {
        (**self).reset(board).await
    }
    async fn promote(&self, board: &Board) -> Result<(), LeaderboardError> {
        (**self).promote(board).await
    }
    async fn prune_applied(
        &self,
        board: &Board,
        before_unix_ms: u64,
    ) -> Result<u64, LeaderboardError> {
        (**self).prune_applied(board, before_unix_ms).await
    }
}

/// Shared behavioural contract run against every implementation.
#[cfg(any(test, feature = "redis"))]
#[doc(hidden)]
pub mod conformance;
