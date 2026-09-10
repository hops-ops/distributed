use std::future::Future;

use crate::entity::Entity;
use crate::repository::{RepositoryError, StreamIdentity};

use super::{
    AttemptFence, CausalCommitBatch, CausalStorageIdentity, CommandLedgerError, CommandLedgerKey,
    CommandLookup, CommandLookupScope, CommandReservation, ExternalCommandCompletion,
    ReservationOutcome,
};

/// Read capability used by the causal workspace. Unlike ordinary
/// `QueuedRepository::get_stream`, wrapper implementations must not retain a
/// queue lock while user handler code awaits.
pub(crate) trait CausalGetStream: Send + Sync {
    fn get_causal_stream<'a>(
        &'a self,
        identity: &'a StreamIdentity,
    ) -> impl Future<Output = Result<Option<Entity>, RepositoryError>> + Send + 'a;

    /// Fetch only the post-snapshot tail without retaining wrapper queue locks.
    fn get_causal_stream_tail<'a>(
        &'a self,
        identity: &'a StreamIdentity,
        after_version: u64,
    ) -> impl Future<Output = Result<Option<Entity>, RepositoryError>> + Send + 'a;
}

/// Proves that command reservation, stream loading, and causal commit are all
/// routed to the same concrete leaf repository handle. Wrappers delegate the
/// opaque value; independently constructed leaves always mint a new one.
pub(crate) trait CausalRepositoryIdentity: Send + Sync {
    fn causal_storage_identity(&self) -> CausalStorageIdentity;
}

/// Short-transaction ledger operations. Lease recovery is part of `reserve` so
/// an adapter cannot expose a non-atomic read-then-steal sequence.
pub(crate) trait CommandLedgerStore: Send + Sync {
    fn reserve_command(
        &self,
        reservation: CommandReservation,
    ) -> impl Future<Output = Result<ReservationOutcome, CommandLedgerError>> + Send + '_;

    fn lookup_command<'a>(
        &'a self,
        key: &'a CommandLedgerKey,
        scope: CommandLookupScope<'a>,
    ) -> impl Future<Output = Result<CommandLookup, CommandLedgerError>> + Send + 'a;

    fn mark_retryable_unknown(
        &self,
        attempt: AttemptFence,
    ) -> impl Future<Output = Result<(), CommandLedgerError>> + Send + '_;

    /// Record a terminal receipt for a remote aggregate-cell command. This
    /// updates only the ledger row and is never a substitute for a local
    /// domain commit.
    fn complete_external_command(
        &self,
        completion: ExternalCommandCompletion,
    ) -> impl Future<Output = Result<(), CommandLedgerError>> + Send + '_;

    #[allow(dead_code)]
    fn compact_expired_commands(
        &self,
        limit: usize,
    ) -> impl Future<Output = Result<u64, CommandLedgerError>> + Send + '_;
}

/// Private transaction capability that makes terminal ledger completion an
/// inseparable participant in the domain commit.
pub(crate) trait CausalTransactionalCommit: Send + Sync {
    fn commit_causal_batch<'a>(
        &'a self,
        batch: CausalCommitBatch<'a>,
    ) -> impl Future<Output = Result<(), CommandLedgerError>> + Send + 'a;
}
