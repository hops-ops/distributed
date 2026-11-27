//! Lease + fencing-token conformance for `InMemoryLeaseStore`,
//! `SqliteLeaseStore` and `PostgresLeaseStore` (see `common::lease`).
//! Postgres skips without `DATABASE_URL`.

mod in_memory {
    common::lease_conformance_tests!(
        common::lease::InMemory,
        controlled_clock: true,
        standalone_migration: false,
    );
}

#[cfg(feature = "sqlite")]
mod sqlite {
    common::lease_conformance_tests!(
        common::lease::Sqlite,
        controlled_clock: false,
        standalone_migration: true,
    );
}

#[cfg(feature = "postgres")]
mod postgres {
    common::lease_conformance_tests!(
        common::lease::Postgres,
        controlled_clock: false,
        standalone_migration: true,
    );
}
