//! Adapter-neutral durable command identity and fenced commit primitives.
//!
//! This module is deliberately crate-private. Application handlers interact
//! with the typed command API; only the framework dispatcher and repository
//! adapters may reserve attempts or attach a ledger completion to a domain
//! [`crate::repository::CommitBatch`]. Keeping the completion outside the public batch preserves
//! the existing repository API while making causal completion inseparable from
//! the backend transaction that stores the domain effects.

#![cfg_attr(not(feature = "graphql"), allow(dead_code))]

mod error;
mod ids;
mod record;
mod reservation;
mod state;
mod traits;

#[cfg(test)]
mod tests;

pub(crate) use error::CommandLedgerError;
// PrincipalPartitionId is consumed by graphql/sqlx modules; unused on default features.
#[cfg_attr(
    not(any(feature = "graphql", feature = "sqlite", feature = "postgres")),
    allow(unused_imports)
)]
pub(crate) use ids::{
    AttemptToken, CanonicalInputHash, CausalStorageIdentity, CausationId,
    CommandContractFingerprint, CommandId, CommandLedgerKey, PrincipalPartitionId, SHA256_BYTES,
};
pub(crate) use record::{CommandLedgerRecord, ReservationDecision};
pub(crate) use reservation::{
    AttemptFence, CausalCommitBatch, CommandAttempt, CommandCompletion, CommandLookup,
    CommandLookupScope, CommandReplay, CommandReservation, ExternalCommandCompletion,
    ExternalDispatchBinding, ReservationOutcome,
};
pub(crate) use state::{CommandLedgerState, TerminalCommandState};
pub(crate) use traits::{
    CausalGetStream, CausalRepositoryIdentity, CausalTransactionalCommit, CommandLedgerStore,
};
