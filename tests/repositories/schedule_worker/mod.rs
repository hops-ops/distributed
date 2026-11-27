//! The schedule worker on the in-memory, SQLite and Postgres repositories
//! (see `common::schedule`). Postgres skips without `DATABASE_URL`.

mod in_memory {
    common::schedule_conformance_tests!(
        common::schedule::InMemory,
        settle_faults: false,
        store_clock: true,
    );
}

#[cfg(feature = "sqlite")]
mod sqlite {
    common::schedule_conformance_tests!(
        common::schedule::Sqlite,
        settle_faults: true,
        store_clock: false,
    );
}

#[cfg(feature = "postgres")]
mod postgres {
    common::schedule_conformance_tests!(
        common::schedule::Postgres,
        settle_faults: true,
        store_clock: false,
    );
}
