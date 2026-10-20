//! Validated SQL migration inventory shared by native and cell executors.

#[derive(Clone, Copy)]
pub(crate) struct EmbeddedMigration {
    pub(crate) version: i64,
    // Read by the SQLx adapters only; cell executors do not report it.
    #[cfg_attr(not(any(feature = "sqlite", feature = "postgres")), allow(dead_code))]
    pub(crate) description: &'static str,
    pub(crate) sql: &'static str,
}

include!(concat!(env!("OUT_DIR"), "/migration_inventory.rs"));

/// Cells host commands, not read-model projectors. These are the event,
/// snapshot, delivery and command-ledger migrations from the native inventory.
/// Projection-owned tables are initialized by their separate query host.
#[cfg(all(feature = "workers-rs", target_arch = "wasm32"))]
pub(crate) fn cell_migrations() -> impl Iterator<Item = &'static EmbeddedMigration> {
    SQLITE_MIGRATIONS
        .iter()
        .filter(|migration| matches!(migration.version, 1 | 2 | 4 | 8 | 11))
}
