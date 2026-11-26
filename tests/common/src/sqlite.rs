//! A temp-file SQLite database for tests that need one database shared by
//! several pools (cross-"process" visibility) — `:memory:` cannot be shared
//! across pools, so a file is required.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;

static DB_SEQ: AtomicU64 = AtomicU64::new(0);

/// A temp-file SQLite database, deleted (with its WAL/SHM sidecars) on drop.
pub struct TempDb {
    path: PathBuf,
}

impl TempDb {
    /// Create a fresh database path under the system temp dir; `prefix` names
    /// the owning suite so leftover files are attributable.
    pub fn new(prefix: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after UNIX epoch")
            .as_nanos();
        let seq = DB_SEQ.fetch_add(1, Ordering::Relaxed);
        let mut path = std::env::temp_dir();
        path.push(format!("{prefix}_{nanos}_{seq}.db"));
        Self { path }
    }

    /// A pool with a 5s busy timeout (writer collisions wait, not SQLITE_BUSY).
    pub async fn pool(&self) -> SqlitePool {
        self.pool_with_timeout(Duration::from_secs(5)).await
    }

    /// A pool with an explicit busy timeout (0 to surface SQLITE_BUSY at once).
    pub async fn pool_with_timeout(&self, busy_timeout: Duration) -> SqlitePool {
        let options = SqliteConnectOptions::new()
            .filename(&self.path)
            .create_if_missing(true)
            .busy_timeout(busy_timeout);
        SqlitePoolOptions::new()
            .max_connections(crate::POOL_CONNECTIONS)
            .connect_with(options)
            .await
            .expect("sqlite test pool")
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        // Best-effort cleanup of the db and its WAL/SHM sidecars.
        let _ = std::fs::remove_file(&self.path);
        for suffix in ["-wal", "-shm"] {
            let mut sidecar = self.path.clone();
            if let Some(file_name) = sidecar.file_name() {
                let name = format!("{}{suffix}", file_name.to_string_lossy());
                sidecar.set_file_name(name);
                let _ = std::fs::remove_file(sidecar);
            }
        }
    }
}
