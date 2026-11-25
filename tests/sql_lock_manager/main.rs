//! Durable SQLx lease-lock conformance for `SqliteLockManager` and
//! `PostgresLockManager` (see `common::lock`). Postgres skips without
//! `DATABASE_URL`.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

#[cfg(feature = "sqlite")]
mod sqlite {
    common::lock_conformance_tests!(common::lock::Sqlite);
}

#[cfg(feature = "postgres")]
mod postgres {
    common::lock_conformance_tests!(common::lock::Postgres);
}
