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
