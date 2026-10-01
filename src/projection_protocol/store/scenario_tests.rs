use std::future::Future;

use super::*;

pub(crate) trait ProjectionProtocolScenario {
    type Store: ProjectionProtocolStore;

    fn repository(&self) -> impl Future<Output = Self::Store> + Send;

    fn topology(&self) -> ProjectorTopologyId;

    fn other_topology(&self) -> ProjectorTopologyId;

    fn partition(&self) -> ProjectionPartition;

    fn source(&self, name: &str, key: &[u8]) -> ProjectionSource {
        ProjectionSource::new(name, key.to_vec()).unwrap()
    }

    fn input_cursor(&self, position: u64) -> ProjectionInputCursor {
        ProjectionInputCursor::new(
            self.topology(),
            self.partition(),
            self.source("todo_stream", b"todo-1"),
            ProjectionEpoch::new("source-v1").unwrap(),
            position,
        )
        .unwrap()
    }

    fn input(
        &self,
        position: u64,
        fingerprint: &[u8],
        message_id: &str,
        causation_id: &str,
        generation: ProjectionGeneration,
    ) -> TrustedProjectionInput {
        TrustedProjectionInput::mint(
            self.input_cursor(position),
            ProjectionInputFingerprint::from_canonical_bytes(fingerprint),
            message_id,
            causation_id,
            generation,
            true,
        )
        .unwrap()
    }

    fn input_for_partition(
        &self,
        partition: ProjectionPartition,
        position: u64,
        fingerprint: &[u8],
        message_id: &str,
        causation_id: &str,
    ) -> TrustedProjectionInput {
        TrustedProjectionInput::mint(
            ProjectionInputCursor::new(
                self.topology(),
                partition,
                self.source("todo_stream", b"todo-1"),
                ProjectionEpoch::new("source-v1").unwrap(),
                position,
            )
            .unwrap(),
            ProjectionInputFingerprint::from_canonical_bytes(fingerprint),
            message_id,
            causation_id,
            ProjectionGeneration::initial(),
            true,
        )
        .unwrap()
    }

    fn change_epoch(&self) -> ProjectionEpoch;

    fn ownership(&self) -> ProjectionModelOwnership;

    fn mutation(
        &self,
        expectation: ProjectionRecordExpectation,
        kind: ProjectionMutationKind,
    ) -> ProjectionRecordMutation;

    fn row_exists<'a>(
        &'a self,
        repository: &'a Self::Store,
    ) -> impl Future<Output = bool> + Send + 'a;

    fn batch(
        &self,
        input: TrustedProjectionInput,
        mutations: Vec<ProjectionRecordMutation>,
        observations: Vec<ProjectionObservationRequest>,
    ) -> ProjectionCommitBatch {
        ProjectionCommitBatch {
            input,
            change_epoch: self.change_epoch(),
            ownership: vec![self.ownership()],
            mutations,
            observations,
        }
    }
}

pub(crate) async fn input_disposition_is_read_only_exact_and_repair_fenced(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    let first_input = scenario.input(
        1,
        b"preflight-one",
        "preflight-message-1",
        "preflight-cause-1",
        ProjectionGeneration::initial(),
    );
    assert_eq!(
        repository
            .projection_input_disposition(&first_input)
            .await
            .unwrap(),
        ProjectionInputDisposition::Pending
    );
    assert_eq!(
        repository
            .projection_partition_runtime_state(&scenario.topology(), &scenario.partition())
            .await
            .unwrap(),
        None,
        "a preflight read must not create projection partition state"
    );

    let applied = repository
        .commit_projection(scenario.batch(first_input.clone(), Vec::new(), Vec::new()))
        .await
        .unwrap();
    assert_eq!(
        repository
            .projection_input_disposition(&first_input)
            .await
            .unwrap(),
        ProjectionInputDisposition::Duplicate(applied.checkpoint.unwrap())
    );

    let stale = scenario.input(
        0,
        b"preflight-stale",
        "preflight-message-0",
        "preflight-cause-0",
        ProjectionGeneration::initial(),
    );
    assert!(matches!(
        repository
            .projection_input_disposition(&stale)
            .await
            .unwrap(),
        ProjectionInputDisposition::Stale(checkpoint) if checkpoint.input().position() == 1
    ));

    let corrupted = scenario.input(
        1,
        b"preflight-corrupt",
        "preflight-message-1",
        "preflight-cause-1",
        ProjectionGeneration::initial(),
    );
    assert!(matches!(
        repository.projection_input_disposition(&corrupted).await,
        Err(ProjectionProtocolError::InputCorruption)
    ));

    let reused_message = scenario.input(
        2,
        b"preflight-two",
        "preflight-message-1",
        "preflight-cause-2",
        ProjectionGeneration::initial(),
    );
    assert!(matches!(
        repository.projection_input_disposition(&reused_message).await,
        Err(ProjectionProtocolError::MessageIdReuse { message_id })
            if message_id == "preflight-message-1"
    ));

    let failed_input = scenario.input(
        2,
        b"preflight-two",
        "preflight-message-2",
        "preflight-cause-2",
        ProjectionGeneration::initial(),
    );
    repository
        .record_projection_failure(
            ProjectionFailureBatch::new(
                failed_input.clone(),
                scenario.change_epoch(),
                "preflight-failure-2",
                "decode_error",
                b"bad payload".to_vec(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        repository
            .projection_input_disposition(&failed_input)
            .await,
        Err(ProjectionProtocolError::PartitionStopped { failure_id })
            if failure_id == "preflight-failure-2"
    ));

    let generation = repository
        .repair_projection(
            &scenario.topology(),
            &scenario.partition(),
            "preflight-failure-2",
        )
        .await
        .unwrap();
    let retry = scenario.input(
        2,
        b"preflight-two",
        "preflight-message-2",
        "preflight-cause-2",
        generation,
    );
    assert_eq!(
        repository
            .projection_input_disposition(&retry)
            .await
            .unwrap(),
        ProjectionInputDisposition::Pending
    );
    assert!(matches!(
        repository.projection_input_disposition(&first_input).await,
        Err(ProjectionProtocolError::GenerationFenced {
            expected: 2,
            actual: 1
        })
    ));
    assert!(matches!(
        repository
            .projection_input_disposition(&scenario.input(
                3,
                b"preflight-later",
                "preflight-message-3",
                "preflight-cause-3",
                generation,
            ))
            .await,
        Err(ProjectionProtocolError::IncomparableInput)
    ));

    let repaired = repository
        .commit_projection(scenario.batch(retry.clone(), Vec::new(), Vec::new()))
        .await
        .unwrap();
    assert_eq!(
        repository
            .projection_input_disposition(&retry)
            .await
            .unwrap(),
        ProjectionInputDisposition::Duplicate(repaired.checkpoint.unwrap())
    );
}

pub(crate) async fn identical_redelivery_advances_only_broker_checkpoint(
    scenario: impl ProjectionProtocolScenario,
) {
    let store = scenario.repository().await;
    let first = scenario.input(
        1,
        b"immutable-input",
        "message-repeat",
        "cause-repeat",
        ProjectionGeneration::initial(),
    );
    let mutation = scenario.mutation(
        ProjectionRecordExpectation::Missing,
        ProjectionMutationKind::Upsert,
    );
    let scope = mutation.scope.clone();
    store
        .commit_projection(scenario.batch(first.clone(), vec![mutation], Vec::new()))
        .await
        .unwrap();
    let original = store.projection_record(&scope).await.unwrap().unwrap();
    let repeat = scenario.input(
        2,
        b"immutable-input",
        "message-repeat",
        "cause-repeat",
        ProjectionGeneration::initial(),
    );
    assert_eq!(
        store.projection_input_disposition(&repeat).await.unwrap(),
        ProjectionInputDisposition::Redelivery
    );
    // Even a caller that supplies effects again cannot repeat them.
    let repeated = store
        .commit_projection(scenario.batch(
            repeat.clone(),
            vec![scenario.mutation(
                ProjectionRecordExpectation::Missing,
                ProjectionMutationKind::Upsert,
            )],
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(repeated.outcome, ProjectionCommitOutcome::Duplicate);
    assert_eq!(repeated.checkpoint.unwrap().input().position(), 2);
    assert_eq!(repeated.changes.len(), 1);
    assert_eq!(repeated.changes[0].kind, ProjectionChangeKind::Checkpoint);
    assert_eq!(
        store.projection_record(&scope).await.unwrap().unwrap(),
        original
    );
    assert!(matches!(
        store.projection_input_disposition(&repeat).await.unwrap(),
        ProjectionInputDisposition::Duplicate(_)
    ));
    let next = scenario.input(
        3,
        b"next-input",
        "message-next",
        "cause-next",
        ProjectionGeneration::initial(),
    );
    store
        .commit_projection(scenario.batch(next, Vec::new(), Vec::new()))
        .await
        .unwrap();
    // Every alias position permanently retains the original bytes and identity.
    let substitution = scenario.input(
        2,
        b"immutable-input",
        "substituted-message",
        "cause-repeat",
        ProjectionGeneration::initial(),
    );
    assert!(matches!(
        store.projection_input_disposition(&substitution).await,
        Err(ProjectionProtocolError::InputCorruption)
    ));
    let altered = scenario.input(
        4,
        b"changed-input",
        "message-repeat",
        "cause-repeat",
        ProjectionGeneration::initial(),
    );
    assert!(store.projection_input_disposition(&altered).await.is_err());
    let other_partition = scenario.input_for_partition(
        ProjectionPartition::new(b"other-partition".to_vec()).unwrap(),
        1,
        b"immutable-input",
        "message-repeat",
        "cause-repeat",
    );
    assert!(matches!(
        store.projection_input_disposition(&other_partition).await,
        Err(ProjectionProtocolError::MessageIdReuse { .. })
    ));

    // A repaired generation owns a fresh execution history. An old-generation
    // receipt must not suppress that generation's first actual delivery.
    let failed = scenario.input(
        4,
        b"repair-input",
        "repair-message",
        "repair-cause",
        ProjectionGeneration::initial(),
    );
    store
        .record_projection_failure(
            ProjectionFailureBatch::new(
                failed,
                scenario.change_epoch(),
                "repair-redelivery",
                "fixture",
                b"failure".to_vec(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let generation = store
        .repair_projection(
            &scenario.topology(),
            &scenario.partition(),
            "repair-redelivery",
        )
        .await
        .unwrap();
    store
        .commit_projection(scenario.batch(
            scenario.input(
                4,
                b"repair-input",
                "repair-message",
                "repair-cause",
                generation,
            ),
            Vec::new(),
            Vec::new(),
        ))
        .await
        .unwrap();
    let replay = scenario.input(
        5,
        b"immutable-input",
        "message-repeat",
        "cause-repeat",
        generation,
    );
    assert_eq!(
        store.projection_input_disposition(&replay).await.unwrap(),
        ProjectionInputDisposition::Pending
    );
    let applied = store
        .commit_projection(scenario.batch(
            replay,
            vec![scenario.mutation(
                ProjectionRecordExpectation::Exact(original.revision),
                ProjectionMutationKind::Upsert,
            )],
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(applied.outcome, ProjectionCommitOutcome::Applied);
    let repeated = scenario.input(
        6,
        b"immutable-input",
        "message-repeat",
        "cause-repeat",
        generation,
    );
    assert_eq!(
        store.projection_input_disposition(&repeated).await.unwrap(),
        ProjectionInputDisposition::Redelivery
    );
}

pub(crate) async fn message_identity_is_topology_wide_across_projection_partitions(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    repository
        .commit_projection(scenario.batch(
            scenario.input(
                1,
                b"topology-wide-message",
                "topology-wide-message",
                "topology-wide-cause",
                ProjectionGeneration::initial(),
            ),
            Vec::new(),
            Vec::new(),
        ))
        .await
        .unwrap();

    let other_partition = ProjectionScopeCodec::new(scenario.topology())
        .encode_partition(Some(&serde_json::json!("tenant-b")))
        .unwrap();
    let remapped = scenario.input_for_partition(
        other_partition,
        1,
        b"topology-wide-message",
        "topology-wide-message",
        "topology-wide-cause",
    );

    assert!(matches!(
        repository
            .commit_projection(scenario.batch(remapped, Vec::new(), Vec::new()))
            .await,
        Err(ProjectionProtocolError::MessageIdReuse { message_id })
            if message_id == "topology-wide-message"
    ));
}

pub(crate) async fn failure_recording_is_idempotent_for_exact_batch(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    let batch = ProjectionFailureBatch::new(
        scenario.input(
            1,
            b"idempotent-failure",
            "idempotent-failure-message",
            "idempotent-failure-cause",
            ProjectionGeneration::initial(),
        ),
        scenario.change_epoch(),
        "idempotent-failure",
        "decode_error",
        b"bad payload".to_vec(),
    )
    .unwrap();

    let failure = repository
        .record_projection_failure(batch.clone())
        .await
        .unwrap();
    assert_eq!(
        repository.record_projection_failure(batch).await.unwrap(),
        failure
    );
    assert_eq!(
        repository
            .projection_failure(
                &scenario.topology(),
                &scenario.partition(),
                "idempotent-failure",
            )
            .await
            .unwrap(),
        Some(failure)
    );
}

pub(crate) async fn registered_table_ownership_rejects_other_topology(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    assert!(matches!(
        repository
            .register_projection_models(&scenario.other_topology(), &[scenario.ownership()])
            .await,
        Err(ProjectionProtocolError::InvalidBatch(message))
            if message.contains("authorit")
    ));
}

pub(crate) async fn tombstone_requires_explicit_exact_recreation(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    let created = repository
        .commit_projection(scenario.batch(
            scenario.input(
                1,
                b"create",
                "message-1",
                "cause-1",
                ProjectionGeneration::initial(),
            ),
            vec![scenario.mutation(
                ProjectionRecordExpectation::Missing,
                ProjectionMutationKind::Upsert,
            )],
            Vec::new(),
        ))
        .await
        .unwrap()
        .records
        .pop()
        .unwrap();
    let deleted = repository
        .commit_projection(scenario.batch(
            scenario.input(
                2,
                b"delete",
                "message-2",
                "cause-2",
                ProjectionGeneration::initial(),
            ),
            vec![scenario.mutation(
                ProjectionRecordExpectation::Exact(created.revision),
                ProjectionMutationKind::Delete,
            )],
            Vec::new(),
        ))
        .await
        .unwrap()
        .records
        .pop()
        .unwrap();
    assert!(deleted.tombstone);
    assert!(!scenario.row_exists(&repository).await);

    assert!(matches!(
        repository
            .commit_projection(scenario.batch(
                scenario.input(
                    3,
                    b"plain-upsert",
                    "message-3",
                    "cause-3",
                    ProjectionGeneration::initial(),
                ),
                vec![scenario.mutation(
                    ProjectionRecordExpectation::Exact(deleted.revision.clone()),
                    ProjectionMutationKind::Upsert,
                )],
                Vec::new(),
            ))
            .await,
        Err(ProjectionProtocolError::RecordTombstoned { .. })
    ));

    let recreated = repository
        .commit_projection(scenario.batch(
            scenario.input(
                3,
                b"recreate",
                "message-3b",
                "cause-3",
                ProjectionGeneration::initial(),
            ),
            vec![scenario.mutation(
                ProjectionRecordExpectation::Exact(deleted.revision),
                ProjectionMutationKind::Recreate,
            )],
            Vec::new(),
        ))
        .await
        .unwrap()
        .records
        .pop()
        .unwrap();
    assert_eq!(recreated.revision.incarnation(), 2);
    assert_eq!(recreated.revision.revision(), 1);
    assert!(!recreated.tombstone);
    assert!(scenario.row_exists(&repository).await);
}
