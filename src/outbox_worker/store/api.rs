use std::future::Future;
use std::time::{Duration, SystemTime};

use crate::outbox::{OutboxMessage, OutboxMessageStatus};
use crate::repository::RepositoryError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxPublishFailureAction {
    Released,
    Failed,
}

/// Lightweight outbox backlog summary for metrics and diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct OutboxBacklogStats {
    /// Number of rows currently in the pending status.
    pub pending: usize,
    /// Creation time of the oldest pending row, when any pending rows exist.
    pub oldest_created_at: Option<SystemTime>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimOutboxMessages {
    pub worker_id: String,
    pub batch_size: usize,
    pub lease: Duration,
    pub destination: Option<String>,
    /// Restrict the claim to this explicit set of message ids. `None` claims the
    /// next claimable batch in created-at order (normal worker polling); `Some`
    /// claims only the listed ids (after-commit immediate dispatch). Ids that are
    /// not currently claimable are simply skipped — a raced id is not an error.
    pub message_ids: Option<Vec<String>>,
}

impl ClaimOutboxMessages {
    pub fn new(worker_id: impl Into<String>, batch_size: usize, lease: Duration) -> Self {
        Self {
            worker_id: worker_id.into(),
            batch_size,
            lease,
            destination: None,
            message_ids: None,
        }
    }

    pub fn to_destination(mut self, destination: impl Into<String>) -> Self {
        self.destination = Some(destination.into());
        self
    }

    /// Restrict this claim to an explicit list of message ids (after-commit
    /// immediate dispatch). The batch size is bounded by the id count.
    pub fn for_ids(worker_id: impl Into<String>, ids: Vec<String>, lease: Duration) -> Self {
        Self {
            worker_id: worker_id.into(),
            batch_size: ids.len(),
            lease,
            destination: None,
            message_ids: Some(ids),
        }
    }

    /// Whether `id` is selectable under this request's id filter.
    pub(super) fn selects(&self, id: &str) -> bool {
        match &self.message_ids {
            Some(ids) => ids.iter().any(|wanted| wanted == id),
            None => true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboxClaimRef {
    pub message_id: String,
    pub worker_id: String,
    pub leased_until: SystemTime,
    pub attempt: u32,
}

impl OutboxClaimRef {
    pub fn from_message(message: &OutboxMessage) -> Result<Self, RepositoryError> {
        let worker_id = message
            .worker_id
            .clone()
            .ok_or_else(|| invalid_outbox_state(message, "outbox claim worker"))?;
        let leased_until = message
            .leased_until
            .ok_or_else(|| invalid_outbox_state(message, "outbox claim lease"))?;

        Ok(Self {
            message_id: message.id().to_string(),
            worker_id,
            leased_until,
            attempt: message.attempts,
        })
    }
}

/// Row bound for the default [`backlog_stats`] scan.
///
/// [`backlog_stats`]: OutboxStore::backlog_stats
pub const BACKLOG_STATS_SCAN_LIMIT: usize = 1000;

/// Store capability for claiming and updating durable outbox messages.
pub trait OutboxStore: Send + Sync {
    /// List up to `limit` messages with the given status, in claim order
    /// (created-at, then message id). The listing is a diagnostic/ops read, so
    /// the bound is mandatory: an outbox can grow far beyond what any caller
    /// should page into memory at once.
    fn messages_by_status(
        &self,
        status: OutboxMessageStatus,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<OutboxMessage>, RepositoryError>> + Send + '_;

    /// List up to `limit` pending messages (see [`messages_by_status`]).
    ///
    /// [`messages_by_status`]: OutboxStore::messages_by_status
    fn pending(
        &self,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<OutboxMessage>, RepositoryError>> + Send + '_ {
        async move {
            self.messages_by_status(OutboxMessageStatus::Pending, limit)
                .await
        }
    }

    /// Return pending-row count and oldest pending creation time without
    /// requiring callers to page full rows. Backends with query support should
    /// override this with `COUNT`/`MIN(created_at)` or equivalent (both
    /// in-tree stores do).
    ///
    /// The default pages up to [`BACKLOG_STATS_SCAN_LIMIT`] pending rows —
    /// never the whole outbox, per the bound contract on
    /// [`messages_by_status`]. The count saturates at that limit; the oldest
    /// creation time stays exact because pending listings are in claim
    /// (oldest-first) order.
    ///
    /// [`messages_by_status`]: OutboxStore::messages_by_status
    fn backlog_stats(
        &self,
    ) -> impl Future<Output = Result<OutboxBacklogStats, RepositoryError>> + Send + '_ {
        async move {
            let pending = self.pending(BACKLOG_STATS_SCAN_LIMIT).await?;
            let oldest_created_at = pending.first().map(|message| message.created_at);
            Ok(OutboxBacklogStats {
                pending: pending.len(),
                oldest_created_at,
            })
        }
    }

    fn claim<'a>(
        &'a self,
        request: ClaimOutboxMessages,
    ) -> impl Future<Output = Result<Vec<OutboxMessage>, RepositoryError>> + Send + 'a;

    /// Delete a delivered row while holding its active claim. The outbox is
    /// pending delivery work, not a publication-history or replay store.
    fn complete<'a>(
        &'a self,
        claim: &'a OutboxClaimRef,
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a;

    /// Complete a batch of claims after their rows were published.
    ///
    /// Equivalent to calling [`complete`] for each claim; backends override it
    /// to settle the batch in fewer round trips (one statement/transaction).
    /// Error semantics match the serial loop: a claim that is no longer
    /// completable (missing row, stale worker/attempt, expired lease) surfaces
    /// the same `NotFound`/`InvalidState` error, and other claims in the batch
    /// may already have been completed when the error is returned.
    ///
    /// [`complete`]: OutboxStore::complete
    fn complete_many<'a>(
        &'a self,
        claims: &'a [OutboxClaimRef],
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a {
        async move {
            for claim in claims {
                self.complete(claim).await?;
            }
            Ok(())
        }
    }

    fn release<'a>(
        &'a self,
        claim: &'a OutboxClaimRef,
        error: &'a str,
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a;

    fn fail<'a>(
        &'a self,
        claim: &'a OutboxClaimRef,
        error: &'a str,
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a;

    fn record_failure<'a>(
        &'a self,
        claim: &'a OutboxClaimRef,
        error: &'a str,
        max_attempts: u32,
    ) -> impl Future<Output = Result<OutboxPublishFailureAction, RepositoryError>> + Send + 'a {
        async move {
            if claim.attempt >= max_attempts {
                self.fail(claim, error).await?;
                Ok(OutboxPublishFailureAction::Failed)
            } else {
                self.release(claim, error).await?;
                Ok(OutboxPublishFailureAction::Released)
            }
        }
    }
}

fn outbox_state(message: &OutboxMessage) -> String {
    format!(
        "{:?}, worker={:?}, leased_until={:?}, attempts={}",
        message.status, message.worker_id, message.leased_until, message.attempts
    )
}

fn invalid_outbox_state(message: &OutboxMessage, expected: &'static str) -> RepositoryError {
    RepositoryError::InvalidState {
        id: message.id().to_string(),
        expected,
        actual: outbox_state(message),
    }
}

pub(crate) fn ensure_active_claim(
    message: &OutboxMessage,
    claim: Option<&OutboxClaimRef>,
    now: SystemTime,
) -> Result<(), RepositoryError> {
    if !message.is_in_flight() {
        return Err(invalid_outbox_state(message, "in-flight outbox message"));
    }

    if let Some(claim) = claim {
        if !message.is_claimed_by(&claim.worker_id) {
            return Err(invalid_outbox_state(
                message,
                "outbox claim held by requesting worker",
            ));
        }

        if message.attempts != claim.attempt {
            return Err(invalid_outbox_state(
                message,
                "outbox claim attempt held by requesting worker",
            ));
        }
    }

    if message.has_expired_lease_at(now) {
        return Err(invalid_outbox_state(message, "unexpired outbox claim"));
    }

    Ok(())
}

/// Claim order: oldest row first, message id as the deterministic tiebreaker.
/// The single definition both claim-order helpers sort by.
fn claim_order_key(message: &OutboxMessage) -> (SystemTime, &str) {
    (message.created_at, message.id())
}

pub(crate) fn sort_by_claim_order(messages: &mut [OutboxMessage]) {
    messages.sort_by(|left, right| claim_order_key(left).cmp(&claim_order_key(right)));
}

pub(crate) fn claim_order_ids<'a>(
    messages: impl Iterator<Item = &'a OutboxMessage>,
) -> Vec<String> {
    let mut messages: Vec<&OutboxMessage> = messages.collect();
    messages.sort_by(|left, right| claim_order_key(left).cmp(&claim_order_key(right)));
    messages
        .into_iter()
        .map(|message| message.id().to_string())
        .collect()
}
