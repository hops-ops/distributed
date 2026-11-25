#![cfg(feature = "sqlite")]

common::repository_conformance_tests!(
    common::repository::Sqlite,
    sql_faults: true,
    durable: true,
);
