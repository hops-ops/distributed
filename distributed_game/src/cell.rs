//! [`CellFactSink`]: commit facts as idempotent commands into a
//! `distributed` [`AggregateCell`].
//!
//! For each fact the sink:
//! 1. takes the principal **only** from [`Fact::principal`] — bound by the
//!    host from the player's verified ticket — or a configured system
//!    principal for zone facts; client payloads never supply identity;
//! 2. builds a trusted server-side [`Session`] (`x-user-id` = principal, plus
//!    the [`FactStamp`](crate::FactStamp) as `x-game-*` variables so handlers
//!    can [`stamp_metadata`](crate::stamp_metadata) their events);
//! 3. derives a deterministic UUIDv7-shaped command id from
//!    `sha256(service_id ‖ 0x00 ‖ fact key)`, so retries and re-emissions of
//!    the same key replay through the cell's command ledger instead of
//!    committing twice;
//! 4. calls [`AggregateCell::dispatch_idempotent`].
//!
//! `InProgress` and `Internal` errors are retryable; everything else is
//! permanent.

use std::sync::Arc;

use distributed::cell_host::{AggregateCell, CellCommandIdentity, CellDispatchError};
use distributed::microsvc::Session;
use distributed::{Aggregate, USER_ID_KEY};
use sha2::{Digest, Sha256};

use crate::facts::{BoxFuture, Fact, FactError, FactReceipt, FactSink};

/// Fact sink backed by an aggregate cell.
pub struct CellFactSink<A>
where
    A: Aggregate + Send + Sync + 'static,
{
    cell: Arc<AggregateCell<A>>,
    service_id: String,
    system_principal: String,
}

impl<A> CellFactSink<A>
where
    A: Aggregate + Send + Sync + 'static,
{
    /// Sink dispatching into `cell` under command-ledger `service_id`.
    /// Zone facts without a player use `system_principal`.
    pub fn new(
        cell: Arc<AggregateCell<A>>,
        service_id: impl Into<String>,
        system_principal: impl Into<String>,
    ) -> Self {
        Self {
            cell,
            service_id: service_id.into(),
            system_principal: system_principal.into(),
        }
    }
}

/// Deterministic UUIDv7-shaped command id for a fact key.
pub fn command_id_for(service_id: &str, key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(service_id.as_bytes());
    hasher.update([0u8]);
    hasher.update(key.as_bytes());
    let digest = hasher.finalize();
    let mut b = [0u8; 16];
    b.copy_from_slice(&digest[..16]);
    b[6] = (b[6] & 0x0f) | 0x70;
    b[8] = (b[8] & 0x3f) | 0x80;
    let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// Trusted session for a fact: principal + stamp.
pub fn trusted_session(principal: &str, fact: &Fact) -> Session {
    let mut session = Session::new();
    session.set(USER_ID_KEY, principal);
    for (key, value) in fact.stamp().session_vars() {
        session.set(key, value);
    }
    session
}

fn classify(error: CellDispatchError) -> FactError {
    match error {
        CellDispatchError::InProgress | CellDispatchError::Internal(_) => {
            FactError::Retryable(format!("{error:?}"))
        }
        other => FactError::Permanent(format!("{}: {other:?}", other.code())),
    }
}

impl<A> FactSink for CellFactSink<A>
where
    A: Aggregate + Send + Sync + 'static,
{
    fn commit<'a>(&'a self, fact: &'a Fact) -> BoxFuture<'a, Result<FactReceipt, FactError>> {
        Box::pin(async move {
            let principal = fact.principal().unwrap_or(&self.system_principal);
            let identity = CellCommandIdentity::new(
                self.service_id.clone(),
                principal,
                command_id_for(&self.service_id, fact.key()),
            )
            .map_err(|e| FactError::Permanent(format!("{e:?}")))?;
            let result = self
                .cell
                .dispatch_idempotent(
                    fact.command(),
                    &identity,
                    fact.input().clone(),
                    trusted_session(principal, fact),
                )
                .await
                .map_err(classify)?;
            Ok(FactReceipt {
                payload: result.payload().clone(),
                replayed: result.replayed(),
            })
        })
    }
}
