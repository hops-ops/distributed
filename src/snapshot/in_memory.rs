#![expect(
    clippy::manual_async_fn,
    reason = "async trait impls return impl Future + Send to preserve public Send bounds"
)]

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, RwLock};

use crate::repository::{RepositoryError, SnapshotStore, StreamIdentity};

use super::store::SnapshotRecord;

/// In-memory snapshot store backed by `Arc<RwLock<HashMap>>`.
///
/// Clone-friendly (cloning shares the same underlying storage).
/// Follows the same pattern as `InMemoryReadModelStore`.
#[derive(Clone)]
pub struct InMemorySnapshotStore {
    pub(crate) storage: Arc<RwLock<HashMap<String, SnapshotRecord>>>,
}

impl Default for InMemorySnapshotStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemorySnapshotStore {
    pub fn new() -> Self {
        Self {
            storage: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl SnapshotStore for InMemorySnapshotStore {
    fn get_snapshot<'a>(
        &'a self,
        identity: &'a StreamIdentity,
    ) -> impl Future<Output = Result<Option<SnapshotRecord>, RepositoryError>> + Send + 'a {
        async move {
            let storage = self
                .storage
                .read()
                .map_err(|_| RepositoryError::LockPoisoned("async snapshot read"))?;
            Ok(storage.get(&identity.storage_key()).cloned())
        }
    }

    fn get_snapshots<'a>(
        &'a self,
        identities: &'a [StreamIdentity],
    ) -> impl Future<Output = Result<Vec<SnapshotRecord>, RepositoryError>> + Send + 'a {
        async move {
            let storage = self
                .storage
                .read()
                .map_err(|_| RepositoryError::LockPoisoned("async snapshot read"))?;
            Ok(identities
                .iter()
                .filter_map(|identity| storage.get(&identity.storage_key()).cloned())
                .collect())
        }
    }

    fn save_snapshot<'a>(
        &'a self,
        identity: &'a StreamIdentity,
        record: SnapshotRecord,
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a {
        async move {
            record.validate_for_identity(identity)?;
            let mut storage = self
                .storage
                .write()
                .map_err(|_| RepositoryError::LockPoisoned("async snapshot write"))?;
            storage.insert(identity.storage_key(), record);
            Ok(())
        }
    }

    fn delete_snapshot<'a>(
        &'a self,
        identity: &'a StreamIdentity,
    ) -> impl Future<Output = Result<bool, RepositoryError>> + Send + 'a {
        async move {
            let mut storage = self
                .storage
                .write()
                .map_err(|_| RepositoryError::LockPoisoned("async snapshot write"))?;
            Ok(storage.remove(&identity.storage_key()).is_some())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(id: &str) -> StreamIdentity {
        StreamIdentity::new("test.aggregate", id).unwrap()
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let store = InMemorySnapshotStore::new();
        assert!(store
            .get_snapshot(&identity("missing"))
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn save_overwrites() {
        let store = InMemorySnapshotStore::new();
        store
            .save_snapshot(
                &identity("agg-1"),
                SnapshotRecord::new("test.aggregate", "agg-1", 1, 1, vec![1]),
            )
            .await
            .unwrap();
        store
            .save_snapshot(
                &identity("agg-1"),
                SnapshotRecord::new("test.aggregate", "agg-1", 5, 1, vec![5]),
            )
            .await
            .unwrap();

        let loaded = store
            .get_snapshot(&identity("agg-1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.version, 5);
        assert_eq!(loaded.payload, vec![5]);
    }

    #[tokio::test]
    async fn delete_existing() {
        let store = InMemorySnapshotStore::new();
        store
            .save_snapshot(
                &identity("agg-1"),
                SnapshotRecord::new("test.aggregate", "agg-1", 1, 1, vec![1]),
            )
            .await
            .unwrap();
        assert!(store.delete_snapshot(&identity("agg-1")).await.unwrap());
        assert!(store
            .get_snapshot(&identity("agg-1"))
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn delete_missing_returns_false() {
        let store = InMemorySnapshotStore::new();
        assert!(!store.delete_snapshot(&identity("missing")).await.unwrap());
    }

    #[tokio::test]
    async fn clone_shares_storage() {
        let store = InMemorySnapshotStore::new();
        let clone = store.clone();
        store
            .save_snapshot(
                &identity("agg-1"),
                SnapshotRecord::new("test.aggregate", "agg-1", 3, 1, vec![3]),
            )
            .await
            .unwrap();

        let loaded = clone
            .get_snapshot(&identity("agg-1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.version, 3);
    }
}
