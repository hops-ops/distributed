common::repository_conformance_tests!(
    common::repository::Postgres,
    sql_faults: true,
    durable: true,
    queued: false,
);

/// The same contract through a queued repository.
mod queued {
    common::repository_conformance_tests!(
        common::repository::Queued<common::repository::Postgres>,
        sql_faults: true,
        durable: true,
        queued: true,
    );
}
