common::repository_conformance_tests!(
    common::repository::InMemory,
    sql_faults: false,
    durable: false,
    queued: false,
);

/// The same contract through a queued repository.
mod queued {
    common::repository_conformance_tests!(
        common::repository::Queued<common::repository::InMemory>,
        sql_faults: false,
        durable: false,
        queued: true,
    );
}
