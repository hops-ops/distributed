//! Native durable cell storage (`cell-sqlite`).
//!
//! Deployment model: one SQLite file per cell instance, the native analogue of
//! one Durable Object per `{aggregate_type}:{shard}`. [`CellSqlitePath`]
//! derives the file from a root directory and the sanitized instance name, or
//! names one file explicitly. Every command commits its events, snapshots,
//! command-ledger receipt, outbox rows and (optionally) the sealed row in one
//! SQLite transaction through the store shared with Workers (`sql_store.rs`).
//!
//! The in-process reference host ([`CellStreamStore::new`] and friends) is
//! unchanged and still backs the whole-state export used by conformance
//! fixtures; the SQLite backend is selected only by the `open_sqlite*`
//! constructors.
//!
//! [`CellStreamStore::new`]: super::CellStreamStore::new

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use super::sql_store::{CellSqlOutboxStore, CellSqlRepository};
use crate::command_ledger::{
    AttemptFence, CausalCommitBatch, CausalGetStream, CausalRepositoryIdentity,
    CausalStorageIdentity, CausalTransactionalCommit, CommandLedgerError, CommandLedgerKey,
    CommandLedgerStore, CommandLookup, CommandLookupScope, CommandReservation,
    ExternalCommandCompletion, ReservationOutcome,
};
use crate::entity::Entity;
use crate::microsvc::HasOutboxStore;
use crate::outbox::{OutboxMessage, OutboxMessageStatus};
use crate::outbox_worker::{ClaimOutboxMessages, OutboxBacklogStats, OutboxClaimRef, OutboxStore};
use crate::repository::{
    CommitBatch, GetStream, RepositoryError, SnapshotStore, StreamIdentity, TransactionalCommit,
};
use crate::snapshot::SnapshotRecord;
use crate::{InMemoryOutboxStore, InMemoryRepository};

/// File extension used for cell files derived from a root directory.
pub const NATIVE_CELL_SQLITE_EXTENSION: &str = "sqlite3";

/// Longest sanitized instance name kept verbatim in a derived file name.
const MAX_FILE_STEM: usize = 120;

/// Where a native cell keeps its SQLite file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CellSqlitePath {
    /// One file per cell under this directory:
    /// `{root}/{sanitized instance name}.sqlite3`.
    Root(PathBuf),
    /// This exact file. The file is bound to the first cell that opens it.
    File(PathBuf),
}

impl CellSqlitePath {
    /// Derive one file per cell instance under `dir`.
    pub fn root(dir: impl Into<PathBuf>) -> Self {
        Self::Root(dir.into())
    }

    /// Use exactly `path` for one cell.
    pub fn file(path: impl Into<PathBuf>) -> Self {
        Self::File(path.into())
    }

    /// The database file for `instance_name` (`{aggregate_type}:{shard}`).
    pub fn resolve(&self, instance_name: &str) -> PathBuf {
        match self {
            Self::Root(root) => root.join(Self::file_name(instance_name)),
            Self::File(path) => path.clone(),
        }
    }

    /// Injective, filesystem-safe file name for an instance name.
    ///
    /// `[A-Za-z0-9_-]` and non-leading `.` are kept; every other byte is
    /// percent-encoded, so `game:a` and `game_a` never share a file. Very long
    /// names keep a readable prefix plus a SHA-256 suffix of the full name.
    pub fn file_name(instance_name: &str) -> String {
        let mut stem = String::with_capacity(instance_name.len());
        for (index, byte) in instance_name.bytes().enumerate() {
            let keep = byte.is_ascii_alphanumeric()
                || byte == b'-'
                || byte == b'_'
                || (byte == b'.' && index != 0);
            if keep {
                stem.push(byte as char);
            } else {
                stem.push_str(&format!("%{byte:02X}"));
            }
        }
        if stem.is_empty() {
            stem.push_str("%00");
        }
        if stem.len() > MAX_FILE_STEM {
            let digest = format!("{:x}", Sha256::digest(instance_name.as_bytes()));
            let mut cut = MAX_FILE_STEM - 33;
            while !stem.is_char_boundary(cut) {
                cut -= 1;
            }
            stem = format!("{}~{}", &stem[..cut], &digest[..32]);
        }
        format!("{stem}.{NATIVE_CELL_SQLITE_EXTENSION}")
    }

    pub(super) fn open(&self, instance_name: &str) -> Result<CellSqlRepository, RepositoryError> {
        CellSqlRepository::open(Path::new(&self.resolve(instance_name)), instance_name)
    }
}

/// Native cell backend: the in-process reference host or a SQLite file.
#[derive(Clone)]
pub(super) enum NativeCellRepository {
    Memory(InMemoryRepository),
    Sql(CellSqlRepository),
}

impl NativeCellRepository {
    pub(super) fn memory(&self) -> Result<&InMemoryRepository, RepositoryError> {
        match self {
            Self::Memory(memory) => Ok(memory),
            Self::Sql(_) => Err(RepositoryError::Model(
                "SQLite cells persist rows directly; whole-state export/restore is only \
                 available on the in-process reference host"
                    .into(),
            )),
        }
    }

    pub(super) fn sql(&self) -> Option<&CellSqlRepository> {
        match self {
            Self::Sql(sql) => Some(sql),
            Self::Memory(_) => None,
        }
    }

    pub(super) fn sql_mut(&mut self) -> Option<&mut CellSqlRepository> {
        match self {
            Self::Sql(sql) => Some(sql),
            Self::Memory(_) => None,
        }
    }
}

macro_rules! delegate {
    ($self:ident, $inner:ident => $call:expr) => {
        match $self {
            NativeCellRepository::Memory($inner) => $call,
            NativeCellRepository::Sql($inner) => $call,
        }
    };
}

impl GetStream for NativeCellRepository {
    async fn get_stream(
        &self,
        identity: &StreamIdentity,
    ) -> Result<Option<Entity>, RepositoryError> {
        delegate!(self, inner => GetStream::get_stream(inner, identity).await)
    }

    async fn get_stream_tail(
        &self,
        identity: &StreamIdentity,
        after_version: u64,
    ) -> Result<Option<Entity>, RepositoryError> {
        delegate!(self, inner => GetStream::get_stream_tail(inner, identity, after_version).await)
    }
}

impl CausalGetStream for NativeCellRepository {
    async fn get_causal_stream(
        &self,
        identity: &StreamIdentity,
    ) -> Result<Option<Entity>, RepositoryError> {
        delegate!(self, inner => CausalGetStream::get_causal_stream(inner, identity).await)
    }

    async fn get_causal_stream_tail(
        &self,
        identity: &StreamIdentity,
        after_version: u64,
    ) -> Result<Option<Entity>, RepositoryError> {
        delegate!(self, inner => inner.get_causal_stream_tail(identity, after_version).await)
    }
}

impl SnapshotStore for NativeCellRepository {
    async fn get_snapshot(
        &self,
        identity: &StreamIdentity,
    ) -> Result<Option<SnapshotRecord>, RepositoryError> {
        delegate!(self, inner => SnapshotStore::get_snapshot(inner, identity).await)
    }

    async fn get_snapshots(
        &self,
        identities: &[StreamIdentity],
    ) -> Result<Vec<SnapshotRecord>, RepositoryError> {
        delegate!(self, inner => SnapshotStore::get_snapshots(inner, identities).await)
    }

    async fn save_snapshot(
        &self,
        identity: &StreamIdentity,
        record: SnapshotRecord,
    ) -> Result<(), RepositoryError> {
        delegate!(self, inner => SnapshotStore::save_snapshot(inner, identity, record).await)
    }

    async fn delete_snapshot(&self, identity: &StreamIdentity) -> Result<bool, RepositoryError> {
        delegate!(self, inner => SnapshotStore::delete_snapshot(inner, identity).await)
    }
}

impl CausalRepositoryIdentity for NativeCellRepository {
    fn causal_storage_identity(&self) -> CausalStorageIdentity {
        delegate!(self, inner => CausalRepositoryIdentity::causal_storage_identity(inner))
    }
}

impl CommandLedgerStore for NativeCellRepository {
    async fn reserve_command(
        &self,
        reservation: CommandReservation,
    ) -> Result<ReservationOutcome, CommandLedgerError> {
        delegate!(self, inner => CommandLedgerStore::reserve_command(inner, reservation).await)
    }

    async fn lookup_command(
        &self,
        key: &CommandLedgerKey,
        scope: CommandLookupScope<'_>,
    ) -> Result<CommandLookup, CommandLedgerError> {
        delegate!(self, inner => CommandLedgerStore::lookup_command(inner, key, scope).await)
    }

    async fn mark_retryable_unknown(
        &self,
        attempt: AttemptFence,
    ) -> Result<(), CommandLedgerError> {
        delegate!(self, inner => CommandLedgerStore::mark_retryable_unknown(inner, attempt).await)
    }

    async fn complete_external_command(
        &self,
        completion: ExternalCommandCompletion,
    ) -> Result<(), CommandLedgerError> {
        delegate!(self, inner => CommandLedgerStore::complete_external_command(inner, completion).await)
    }

    async fn compact_expired_commands(&self, limit: usize) -> Result<u64, CommandLedgerError> {
        delegate!(self, inner => CommandLedgerStore::compact_expired_commands(inner, limit).await)
    }
}

impl TransactionalCommit for NativeCellRepository {
    async fn commit_batch(&self, batch: CommitBatch<'_>) -> Result<(), RepositoryError> {
        delegate!(self, inner => TransactionalCommit::commit_batch(inner, batch).await)
    }
}

impl CausalTransactionalCommit for NativeCellRepository {
    async fn commit_causal_batch(
        &self,
        batch: CausalCommitBatch<'_>,
    ) -> Result<(), CommandLedgerError> {
        delegate!(self, inner => CausalTransactionalCommit::commit_causal_batch(inner, batch).await)
    }
}

impl HasOutboxStore for NativeCellRepository {
    type OutboxStore = CellOutboxStore;

    fn outbox_store(&self) -> CellOutboxStore {
        match self {
            Self::Memory(inner) => CellOutboxStore::Memory(inner.outbox_store()),
            Self::Sql(inner) => CellOutboxStore::Sql(HasOutboxStore::outbox_store(inner)),
        }
    }
}

/// Outbox of a native cell: the reference host's rows or the cell's SQLite
/// `outbox_messages` table (lease-fenced claim, publish, delete).
#[derive(Clone)]
pub enum CellOutboxStore {
    Memory(InMemoryOutboxStore),
    Sql(CellSqlOutboxStore),
}

macro_rules! delegate_outbox {
    ($self:ident, $inner:ident => $call:expr) => {
        match $self {
            CellOutboxStore::Memory($inner) => $call,
            CellOutboxStore::Sql($inner) => $call,
        }
    };
}

impl OutboxStore for CellOutboxStore {
    async fn messages_by_status(
        &self,
        status: OutboxMessageStatus,
        limit: usize,
    ) -> Result<Vec<OutboxMessage>, RepositoryError> {
        delegate_outbox!(self, inner => inner.messages_by_status(status, limit).await)
    }

    async fn backlog_stats(&self) -> Result<OutboxBacklogStats, RepositoryError> {
        delegate_outbox!(self, inner => inner.backlog_stats().await)
    }

    async fn claim(
        &self,
        request: ClaimOutboxMessages,
    ) -> Result<Vec<OutboxMessage>, RepositoryError> {
        delegate_outbox!(self, inner => inner.claim(request).await)
    }

    async fn complete(&self, claim: &OutboxClaimRef) -> Result<(), RepositoryError> {
        delegate_outbox!(self, inner => inner.complete(claim).await)
    }

    async fn complete_many(&self, claims: &[OutboxClaimRef]) -> Result<(), RepositoryError> {
        delegate_outbox!(self, inner => inner.complete_many(claims).await)
    }

    async fn release(&self, claim: &OutboxClaimRef, error: &str) -> Result<(), RepositoryError> {
        delegate_outbox!(self, inner => inner.release(claim, error).await)
    }

    async fn fail(&self, claim: &OutboxClaimRef, error: &str) -> Result<(), RepositoryError> {
        delegate_outbox!(self, inner => inner.fail(claim, error).await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_names_are_safe_and_injective() {
        assert_eq!(CellSqlitePath::file_name("game:g1"), "game%3Ag1.sqlite3");
        assert_ne!(
            CellSqlitePath::file_name("game:a"),
            CellSqlitePath::file_name("game_a")
        );
        assert_eq!(CellSqlitePath::file_name(".."), "%2E..sqlite3");
        assert_eq!(CellSqlitePath::file_name("a/../b"), "a%2F..%2Fb.sqlite3");
        let long = format!("Room:{}", "x".repeat(400));
        let name = CellSqlitePath::file_name(&long);
        assert!(name.len() < 140, "{name}");
        assert_ne!(name, CellSqlitePath::file_name(&format!("{long}y")));
        assert_eq!(
            CellSqlitePath::root("/data/cells").resolve("Room:r1"),
            PathBuf::from("/data/cells/Room%3Ar1.sqlite3")
        );
        assert_eq!(
            CellSqlitePath::file("/tmp/one.db").resolve("Room:r1"),
            PathBuf::from("/tmp/one.db")
        );
    }
}
