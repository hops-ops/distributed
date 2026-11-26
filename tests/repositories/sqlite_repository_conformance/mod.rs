common::repository_conformance_tests!(
    common::repository::Sqlite,
    sql_faults: true,
    durable: true,
    queued: false,
);

/// The same contract through a queued repository.
mod queued {
    common::repository_conformance_tests!(
        common::repository::Queued<common::repository::Sqlite>,
        sql_faults: true,
        durable: true,
        queued: true,
    );
}
