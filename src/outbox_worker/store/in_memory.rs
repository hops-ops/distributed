use std::future::Future;

use crate::in_memory_repo::InMemoryOutboxStore;
use crate::outbox::{OutboxMessage, OutboxMessageStatus};
use crate::repository::RepositoryError;

use super::{
    claim_order_ids, ensure_active_claim, sort_by_claim_order, ClaimOutboxMessages,
    OutboxBacklogStats, OutboxClaimRef, OutboxStore,
};

impl InMemoryOutboxStore {
    fn update_outbox_message<T>(
        &self,
        message_id: &str,
        update: impl FnOnce(&mut OutboxMessage) -> Result<T, RepositoryError>,
    ) -> Result<T, RepositoryError> {
        let mut storage = self
            .storage
            .write()
            .map_err(|_| RepositoryError::LockPoisoned("outbox write"))?;

        let message = storage
            .get_mut(message_id)
            .ok_or_else(|| RepositoryError::NotFound {
                id: message_id.to_string(),
            })?;

        update(message)
    }
}

impl OutboxStore for InMemoryOutboxStore {
    fn messages_by_status(
        &self,
        status: OutboxMessageStatus,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<OutboxMessage>, RepositoryError>> + Send + '_ {
        async move {
            let storage = self
                .storage
                .read()
                .map_err(|_| RepositoryError::LockPoisoned("outbox read"))?;

            let mut messages = storage
                .values()
                .filter(|message| message.status == status)
                .cloned()
                .collect::<Vec<_>>();
            sort_by_claim_order(&mut messages);
            messages.truncate(limit);
            Ok(messages)
        }
    }

    fn backlog_stats(
        &self,
    ) -> impl Future<Output = Result<OutboxBacklogStats, RepositoryError>> + Send + '_ {
        async move {
            let storage = self
                .storage
                .read()
                .map_err(|_| RepositoryError::LockPoisoned("outbox read"))?;

            let mut stats = OutboxBacklogStats::default();
            for message in storage
                .values()
                .filter(|message| message.status == OutboxMessageStatus::Pending)
            {
                stats.pending += 1;
                stats.oldest_created_at = Some(
                    stats
                        .oldest_created_at
                        .map_or(message.created_at, |oldest| oldest.min(message.created_at)),
                );
            }
            Ok(stats)
        }
    }

    fn claim<'a>(
        &'a self,
        request: ClaimOutboxMessages,
    ) -> impl Future<Output = Result<Vec<OutboxMessage>, RepositoryError>> + Send + 'a {
        async move {
            let mut storage = self
                .storage
                .write()
                .map_err(|_| RepositoryError::LockPoisoned("outbox write"))?;

            if request.batch_size == 0 {
                return Ok(Vec::new());
            }

            let now = crate::time::now();
            let ids = claim_order_ids(storage.values());
            let mut claimed = Vec::new();
            for id in ids {
                if !request.selects(&id) {
                    continue;
                }

                let Some(message) = storage.get_mut(&id) else {
                    continue;
                };

                if message.is_claimable_at(now) {
                    if let Some(destination) = request.destination.as_deref() {
                        if message.destination.as_deref() != Some(destination) {
                            continue;
                        }
                    }
                    message.claim_at(&request.worker_id, request.lease, now)?;
                    claimed.push(message.clone());
                }

                if claimed.len() >= request.batch_size {
                    break;
                }
            }

            Ok(claimed)
        }
    }

    fn complete<'a>(
        &'a self,
        claim: &'a OutboxClaimRef,
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a {
        async move { self.complete_many(std::slice::from_ref(claim)).await }
    }

    /// Batched complete under a single write lock instead of one lock
    /// acquisition per claim. Same per-claim validation as [`complete`];
    /// claims before a failing one stay completed, like the serial loop.
    ///
    /// [`complete`]: OutboxStore::complete
    fn complete_many<'a>(
        &'a self,
        claims: &'a [OutboxClaimRef],
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a {
        async move {
            if claims.is_empty() {
                return Ok(());
            }
            let mut storage = self
                .storage
                .write()
                .map_err(|_| RepositoryError::LockPoisoned("outbox write"))?;
            let now = crate::time::now();
            for claim in claims {
                let message = storage.get_mut(&claim.message_id).ok_or_else(|| {
                    RepositoryError::NotFound {
                        id: claim.message_id.clone(),
                    }
                })?;
                ensure_active_claim(message, Some(claim), now)?;
                storage.remove(&claim.message_id);
            }
            Ok(())
        }
    }

    fn release<'a>(
        &'a self,
        claim: &'a OutboxClaimRef,
        error: &'a str,
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a {
        async move {
            self.update_outbox_message(&claim.message_id, |message| {
                ensure_active_claim(message, Some(claim), crate::time::now())?;
                message.release(error.to_string())?;
                Ok(())
            })
        }
    }

    fn fail<'a>(
        &'a self,
        claim: &'a OutboxClaimRef,
        error: &'a str,
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a {
        async move {
            self.update_outbox_message(&claim.message_id, |message| {
                ensure_active_claim(message, Some(claim), crate::time::now())?;
                message.fail(error.to_string())?;
                Ok(())
            })
        }
    }
}
