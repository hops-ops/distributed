//! Repositories and their conformance suites, the event store, the queued repository, leases, locks, schedules and the outbox.
//!
//! One test binary for the area: each former `tests/<name>` target is a
//! module, so `cargo test --test repositories <name>::` runs what `--test <name>` ran.

#[cfg(feature = "sqlite")]
mod aggregate_schedules;
mod coordination_lease;
#[cfg(feature = "sqlite")]
mod durable_enqueue_sqlite;
mod in_memory_repository_conformance;
#[cfg(feature = "postgres")]
mod postgres_repository;
#[cfg(feature = "postgres")]
mod postgres_repository_conformance;
mod queued_repo;
mod repository_api;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod schedule_worker;
#[cfg(feature = "sqlite")]
mod snapshot_sqlite_hardening;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod sql_lock_manager;
#[cfg(feature = "sqlite")]
mod sqlite_repository;
#[cfg(feature = "sqlite")]
mod sqlite_repository_conformance;
