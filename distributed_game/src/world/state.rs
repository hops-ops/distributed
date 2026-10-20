//! Durable zone state for hibernation.
//!
//! When a zone hibernates, the host asks it for a snapshot
//! ([`Zone::hibernate`](crate::Zone::hibernate)) and stores it here under
//! the zone's ownership token; the next activation (on any host) loads it
//! and hands it to [`Zone::on_restore`](crate::Zone::on_restore).
//!
//! Writes are **fenced**: `save` with a token older than the stored one
//! fails with [`StateError::Stale`], so a host that lost ownership while
//! paused can never overwrite the state written by its successor.
//!
//! The trait is synchronous (snapshots are small and rare); async drivers
//! call it on a blocking thread.
//!
//! Why not an `AggregateCell`: a hibernation snapshot is a whole-value
//! overwrite keyed by zone, not a stream of domain events; storing it as
//! events would grow without bound and buy nothing. Durable *outcomes* (XP,
//! loot) go through facts into cells; the snapshot only holds ephemeral
//! simulation state worth keeping across a cold start.

use std::collections::HashMap;
use std::sync::Mutex;

/// A stored snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredZoneState {
    /// Ownership token it was written under.
    pub token: u64,
    /// Content version the zone was running.
    pub content_version: String,
    /// Opaque zone bytes.
    pub state: Vec<u8>,
}

/// Store failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateError {
    /// Write fenced off: a newer owner already saved.
    Stale {
        /// Presented token.
        presented: u64,
        /// Stored token.
        current: u64,
    },
    /// Backend failure.
    Backend(String),
}

impl std::fmt::Display for StateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stale { presented, current } => {
                write!(f, "stale zone-state token {presented} (current {current})")
            }
            Self::Backend(e) => write!(f, "zone-state backend: {e}"),
        }
    }
}

impl std::error::Error for StateError {}

/// Fenced snapshot store.
pub trait ZoneStateStore: Send + Sync + 'static {
    /// Latest snapshot of `zone_id`.
    fn load(&self, zone_id: &str) -> Result<Option<StoredZoneState>, StateError>;

    /// Store a snapshot if `state.token` >= the stored token.
    fn save(&self, zone_id: &str, state: &StoredZoneState) -> Result<(), StateError>;
}

/// In-memory store (tests / single process).
#[derive(Debug, Default)]
pub struct MemoryZoneStateStore {
    states: Mutex<HashMap<String, StoredZoneState>>,
}

impl MemoryZoneStateStore {
    /// Empty.
    pub fn new() -> Self {
        Self::default()
    }
}

impl ZoneStateStore for MemoryZoneStateStore {
    fn load(&self, zone_id: &str) -> Result<Option<StoredZoneState>, StateError> {
        Ok(self.states.lock().expect("states").get(zone_id).cloned())
    }

    fn save(&self, zone_id: &str, state: &StoredZoneState) -> Result<(), StateError> {
        let mut states = self.states.lock().expect("states");
        if let Some(current) = states.get(zone_id) {
            if current.token > state.token {
                return Err(StateError::Stale {
                    presented: state.token,
                    current: current.token,
                });
            }
        }
        states.insert(zone_id.to_string(), state.clone());
        Ok(())
    }
}

/// Native SQLite store (feature `sqlite`): one table, one fenced upsert.
#[cfg(feature = "sqlite")]
pub struct SqliteZoneStateStore {
    conn: Mutex<rusqlite::Connection>,
}

#[cfg(feature = "sqlite")]
impl std::fmt::Debug for SqliteZoneStateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SqliteZoneStateStore")
    }
}

#[cfg(feature = "sqlite")]
fn backend(e: rusqlite::Error) -> StateError {
    StateError::Backend(e.to_string())
}

#[cfg(feature = "sqlite")]
impl SqliteZoneStateStore {
    /// Open (or create) the database file.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, StateError> {
        Self::from_connection(rusqlite::Connection::open(path).map_err(backend)?)
    }

    /// In-memory database.
    pub fn open_in_memory() -> Result<Self, StateError> {
        Self::from_connection(rusqlite::Connection::open_in_memory().map_err(backend)?)
    }

    fn from_connection(conn: rusqlite::Connection) -> Result<Self, StateError> {
        // Hosts sharing one file collide on writes; wait for the other writer
        // instead of failing at once with "database is locked".
        const BUSY: std::time::Duration = std::time::Duration::from_secs(5);
        conn.busy_timeout(BUSY).map_err(backend)?;
        // The WAL switch at open does not use the busy handler, so two hosts
        // opening the file together retry it until the same deadline.
        let deadline = std::time::Instant::now() + BUSY;
        loop {
            match conn.execute_batch(
                "PRAGMA journal_mode=WAL;
                 CREATE TABLE IF NOT EXISTS game_zone_state (
                   zone_id TEXT PRIMARY KEY,
                   token INTEGER NOT NULL,
                   content_version TEXT NOT NULL,
                   state BLOB NOT NULL
                 );",
            ) {
                Ok(()) => break,
                Err(rusqlite::Error::SqliteFailure(e, _))
                    if matches!(
                        e.code,
                        rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
                    ) && std::time::Instant::now() < deadline =>
                {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(e) => return Err(backend(e)),
            }
        }
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}

#[cfg(feature = "sqlite")]
impl ZoneStateStore for SqliteZoneStateStore {
    fn load(&self, zone_id: &str) -> Result<Option<StoredZoneState>, StateError> {
        use rusqlite::OptionalExtension;
        let conn = self.conn.lock().expect("sqlite");
        conn.query_row(
            "SELECT token, content_version, state FROM game_zone_state WHERE zone_id = ?1",
            [zone_id],
            |row| {
                Ok(StoredZoneState {
                    token: row.get::<_, i64>(0)? as u64,
                    content_version: row.get(1)?,
                    state: row.get(2)?,
                })
            },
        )
        .optional()
        .map_err(backend)
    }

    fn save(&self, zone_id: &str, state: &StoredZoneState) -> Result<(), StateError> {
        let conn = self.conn.lock().expect("sqlite");
        let token = i64::try_from(state.token).map_err(|_| StateError::Backend("token".into()))?;
        let changed = conn
            .execute(
                "INSERT INTO game_zone_state (zone_id, token, content_version, state)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(zone_id) DO UPDATE SET
                   token = excluded.token,
                   content_version = excluded.content_version,
                   state = excluded.state
                 WHERE excluded.token >= game_zone_state.token",
                rusqlite::params![zone_id, token, state.content_version, state.state],
            )
            .map_err(backend)?;
        if changed == 0 {
            let current: i64 = conn
                .query_row(
                    "SELECT token FROM game_zone_state WHERE zone_id = ?1",
                    [zone_id],
                    |row| row.get(0),
                )
                .map_err(backend)?;
            return Err(StateError::Stale {
                presented: state.token,
                current: current as u64,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(token: u64, bytes: &[u8]) -> StoredZoneState {
        StoredZoneState {
            token,
            content_version: "c1".into(),
            state: bytes.to_vec(),
        }
    }

    fn exercise(store: &dyn ZoneStateStore) {
        assert_eq!(store.load("z").unwrap(), None);
        store.save("z", &snap(2, b"a")).unwrap();
        store.save("z", &snap(2, b"b")).unwrap();
        assert_eq!(
            store.save("z", &snap(1, b"old")),
            Err(StateError::Stale {
                presented: 1,
                current: 2
            })
        );
        store.save("z", &snap(3, b"c")).unwrap();
        assert_eq!(store.load("z").unwrap(), Some(snap(3, b"c")));
    }

    #[test]
    fn memory_store_is_fenced() {
        exercise(&MemoryZoneStateStore::new());
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_stores_sharing_a_file_wait_for_each_other() {
        // Hosts sharing one file (the multi-host demos) open and write at the
        // same time; a colliding writer must wait, not fail "database is locked".
        let path = std::env::temp_dir().join(format!(
            "dg-zone-state-shared-{}-{}.sqlite3",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let threads: Vec<_> = (0..8u64)
            .map(|host| {
                let path = path.clone();
                std::thread::spawn(move || {
                    let store = SqliteZoneStateStore::open(&path)?;
                    for token in 1..=50 {
                        store.save(&format!("zone.{host}"), &snap(token, b"s"))?;
                    }
                    Ok::<_, StateError>(())
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap().unwrap();
        }
        let store = SqliteZoneStateStore::open(&path).unwrap();
        for host in 0..8 {
            assert_eq!(
                store.load(&format!("zone.{host}")).unwrap(),
                Some(snap(50, b"s"))
            );
        }
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_store_is_fenced_and_durable() {
        exercise(&SqliteZoneStateStore::open_in_memory().unwrap());
        let path = std::env::temp_dir().join(format!(
            "dg-zone-state-{}-{}.sqlite3",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        {
            let store = SqliteZoneStateStore::open(&path).unwrap();
            store.save("realm.east", &snap(7, b"boss:dead")).unwrap();
        }
        let reopened = SqliteZoneStateStore::open(&path).unwrap();
        assert_eq!(
            reopened.load("realm.east").unwrap(),
            Some(snap(7, b"boss:dead"))
        );
        drop(reopened);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }
}
