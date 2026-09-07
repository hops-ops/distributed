use super::*;
use crate::projection_protocol::{ResolvedProjectionKey, ResolvedProjectionObligation};
use crate::table::{
    DeleteTableRowMutation, ExpectedVersion, PrimaryKey, RowKey, RowValue, TableSchema,
};

fn topology() -> ProjectorTopologyId {
    ProjectorTopologyId::new(1, "todos", [7; 32]).unwrap()
}

fn partition() -> ProjectionPartition {
    ProjectionPartition::new(b"tenant:a".to_vec()).unwrap()
}

fn scope(model: &str, key: &[u8]) -> ProjectionRecordScope {
    ProjectionRecordScope::new(topology(), partition(), model, key.to_vec()).unwrap()
}

fn schema() -> &'static TableSchema {
    Box::leak(Box::new(TableSchema {
        model_name: "TodoView".into(),
        table_name: "todo_views".into(),
        columns: Vec::new(),
        primary_key: PrimaryKey::new(["id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: Default::default(),
    }))
}

fn same_transaction_evidence() -> SameTransactionProjectionEvidence {
    let scope = scope("TodoView", b"todo-1");
    let revision = RecordRevision::new(scope.clone(), 1, 1).unwrap();
    let change = ProjectionChangeCursor::new(
        topology(),
        partition(),
        ProjectionEpoch::new("changes-v1").unwrap(),
        1,
    )
    .unwrap();
    SameTransactionProjectionEvidence {
        records: vec![ProjectionRecordMetadata {
            source_snapshot: None,
            revision: revision.clone(),
            tombstone: false,
            change: change.clone(),
        }],
        changes: vec![ProjectionChange {
            cursor: change.clone(),
            kind: ProjectionChangeKind::RecordUpsert,
            causation_id: "cause-1".into(),
            observation_kind: None,
            scope: Some(scope.clone()),
            revision: Some(revision.clone()),
            failure_id: None,
        }],
        observations: vec![ProjectionObservation {
            causation_id: "cause-1".into(),
            kind: ProjectionObservationKind::Record,
            revision: Some(revision),
            scope,
            change,
        }],
    }
}

fn failure_batch() -> ProjectionFailureBatch {
    let input = TrustedProjectionInput::mint(
        ProjectionInputCursor::new(
            topology(),
            partition(),
            ProjectionSource::new("todo_stream", b"todo-1".to_vec()).unwrap(),
            ProjectionEpoch::new("source-v1").unwrap(),
            1,
        )
        .unwrap(),
        ProjectionInputFingerprint::from_canonical_bytes(b"input-1"),
        "message-1",
        "cause-1",
        ProjectionGeneration::initial(),
        true,
    )
    .unwrap();
    ProjectionFailureBatch::new(
        input,
        ProjectionEpoch::new("changes-v1").unwrap(),
        "failure-1",
        "decode_error",
        b"bad payload".to_vec(),
    )
    .unwrap()
}

#[test]
fn change_retention_is_nonzero_portable_and_bounded_by_default() {
    assert_eq!(
        ProjectionChangeRetention::default().max_retained_changes(),
        DEFAULT_MAX_RETAINED_PROJECTION_CHANGES
    );
    assert_eq!(
        ProjectionChangeRetention::new(0),
        Err(ProjectionProtocolValidationError::Zero {
            field: "projection retained change count",
        })
    );
    assert_eq!(
        ProjectionChangeRetention::new(u64::MAX),
        Err(ProjectionProtocolValidationError::TooLarge {
            field: "projection retained change count",
            value: u64::MAX,
            max: MAX_PROJECTION_POSITION,
        })
    );
    assert_eq!(
        ProjectionChangeRetention::new(MAX_PROJECTION_POSITION)
            .unwrap()
            .max_retained_changes(),
        MAX_PROJECTION_POSITION
    );
}

#[test]
fn generations_are_nonzero_and_checked() {
    assert!(ProjectionGeneration::new(0).is_err());
    assert_eq!(ProjectionGeneration::initial().get(), 1);
    assert_eq!(
        ProjectionGeneration::new(7)
            .unwrap()
            .checked_next()
            .unwrap()
            .get(),
        8
    );
    assert_eq!(
        ProjectionGeneration::new(u64::MAX),
        Err(ProjectionProtocolValidationError::TooLarge {
            field: "projection generation",
            value: u64::MAX,
            max: MAX_PROJECTION_POSITION,
        })
    );
    assert!(matches!(
        ProjectionGeneration::new(MAX_PROJECTION_POSITION)
            .unwrap()
            .checked_next(),
        Err(ProjectionProtocolError::PositionOverflow {
            domain: "projection generation"
        })
    ));
}

#[test]
fn trusted_input_identity_is_bounded_and_deterministic() {
    let cursor = ProjectionInputCursor::new(
        topology(),
        partition(),
        super::super::ProjectionSource::new("aggregate", b"todo:1".to_vec()).unwrap(),
        ProjectionEpoch::new("source-v1").unwrap(),
        0,
    )
    .unwrap();
    let left = ProjectionInputFingerprint::from_canonical_bytes(b"same");
    let right = ProjectionInputFingerprint::from_canonical_bytes(b"same");
    assert_eq!(left, right);
    let input = TrustedProjectionInput::mint(
        cursor,
        left,
        "message-1",
        "cause-1",
        ProjectionGeneration::initial(),
        false,
    )
    .unwrap();
    assert!(input.inbox_receipt().validate().is_ok());
    assert!(input.consumer_name().starts_with("projection:v1:"));
}

#[test]
fn record_expectations_and_mutation_kinds_fail_closed() {
    let first_scope = scope("TodoView", b"1");
    let other_scope = scope("TodoView", b"2");
    let revision = RecordRevision::new(other_scope, 1, 1).unwrap();
    let delete = TableMutation::DeleteRow(DeleteTableRowMutation {
        schema: schema(),
        key: RowKey::new([("id", RowValue::String("1".into()))]),
        expected_version: ExpectedVersion::Any,
    });
    assert!(matches!(
        ProjectionRecordMutation::new(
            first_scope.clone(),
            delete.clone(),
            ProjectionRecordExpectation::Exact(revision),
            ProjectionMutationKind::Delete,
        ),
        Err(ProjectionProtocolError::ScopeMismatch { .. })
    ));
    assert!(matches!(
        ProjectionRecordMutation::new(
            first_scope,
            delete,
            ProjectionRecordExpectation::Missing,
            ProjectionMutationKind::Delete,
        ),
        Err(ProjectionProtocolError::InvalidBatch(_))
    ));
}

#[test]
fn absent_and_explicit_null_obligation_partitions_survive_round_trip() {
    let topology = ProjectorTopologyId::new(1, "todos", [9; 32]).unwrap();
    let partition = super::super::ProjectionPartition::new(b"unit".to_vec()).unwrap();
    let scope = super::super::ProjectionRecordScope::new(
        topology,
        partition,
        "TodoView",
        b"todo-1".to_vec(),
    )
    .unwrap();
    let absent = ResolvedProjectionObligation {
        projector: "todos".into(),
        model: "TodoView".into(),
        key: ResolvedProjectionKey { fields: Vec::new() },
        partition: None,
        scope,
    };
    let explicit_null = ResolvedProjectionObligation {
        partition: Some(serde_json::Value::Null),
        ..absent.clone()
    };
    let absent_json = serde_json::to_value(&absent).unwrap();
    let null_json = serde_json::to_value(&explicit_null).unwrap();
    assert!(absent_json.get("partition").is_none());
    assert_eq!(null_json.get("partition"), Some(&serde_json::Value::Null));
    assert_eq!(
        serde_json::from_value::<ResolvedProjectionObligation>(null_json)
            .unwrap()
            .partition,
        Some(serde_json::Value::Null)
    );
}

#[test]
fn same_transaction_replay_semantically_decodes_exact_typed_evidence() {
    let replay = same_transaction_evidence().replay_value();
    SameTransactionProjectionEvidence::validate_replay_value(&replay).unwrap();
}

#[test]
fn same_transaction_replay_rejects_version_and_identity_digest_tampering() {
    let valid = same_transaction_evidence().replay_value();
    let mut cases = Vec::new();

    let mut unsupported_version = valid.clone();
    unsupported_version["version"] = serde_json::json!(2);
    cases.push(("version", unsupported_version));

    let mut malformed_topology_digest = valid.clone();
    malformed_topology_digest["records"][0]["revision"]["scope"]["topology_digest"] =
        serde_json::json!("00");
    cases.push(("topology digest", malformed_topology_digest));

    let mut mismatched_topology = valid.clone();
    mismatched_topology["records"][0]["revision"]["scope"]["topology_digest"] =
        serde_json::json!("08".repeat(32));
    cases.push(("topology identity", mismatched_topology));

    let mut mismatched_partition_digest = valid.clone();
    mismatched_partition_digest["records"][0]["revision"]["scope"]["partition_digest"] =
        serde_json::json!("00".repeat(32));
    cases.push(("partition digest", mismatched_partition_digest));

    let mut mismatched_key_digest = valid.clone();
    mismatched_key_digest["records"][0]["revision"]["scope"]["key_digest"] =
        serde_json::json!("00".repeat(32));
    cases.push(("key digest", mismatched_key_digest));

    let mut noncanonical_key = valid;
    noncanonical_key["records"][0]["revision"]["scope"]["key"] = serde_json::json!("AA");
    cases.push(("canonical key", noncanonical_key));

    for (case, replay) in cases {
        assert!(
            SameTransactionProjectionEvidence::validate_replay_value(&replay).is_err(),
            "{case} tampering must be rejected"
        );
    }
}

#[test]
fn same_transaction_replay_rejects_cross_component_semantic_drift() {
    let valid = same_transaction_evidence().replay_value();
    let mut cases = Vec::new();

    let mut zero_revision = valid.clone();
    zero_revision["records"][0]["revision"]["revision"] = serde_json::json!(0);
    cases.push(("zero revision", zero_revision));

    let mut mismatched_revision = valid.clone();
    mismatched_revision["changes"][0]["revision"]["revision"] = serde_json::json!(2);
    cases.push(("mismatched revision", mismatched_revision));

    let mut mismatched_cursor = valid.clone();
    mismatched_cursor["observations"][0]["change"]["position"] = serde_json::json!(2);
    cases.push(("mismatched cursor", mismatched_cursor));

    let mut cursor_scope_drift = valid.clone();
    let other_topology_digest = serde_json::json!("08".repeat(32));
    cursor_scope_drift["records"][0]["change"]["topology_digest"] = other_topology_digest.clone();
    cursor_scope_drift["changes"][0]["cursor"]["topology_digest"] = other_topology_digest.clone();
    cursor_scope_drift["observations"][0]["change"]["topology_digest"] = other_topology_digest;
    cases.push(("cursor/scope topology drift", cursor_scope_drift));

    let mut mismatched_causation = valid.clone();
    mismatched_causation["observations"][0]["causation_id"] = serde_json::json!("other-cause");
    cases.push(("mismatched causation", mismatched_causation));

    let mut tombstone = valid.clone();
    tombstone["records"][0]["tombstone"] = serde_json::json!(true);
    cases.push(("tombstone", tombstone));

    let mut wrong_change_kind = valid.clone();
    wrong_change_kind["changes"][0]["kind"] = serde_json::json!("record_delete");
    cases.push(("wrong change kind", wrong_change_kind));

    let mut duplicate_record = valid;
    let record = duplicate_record["records"][0].clone();
    duplicate_record["records"]
        .as_array_mut()
        .unwrap()
        .push(record);
    cases.push(("duplicate record", duplicate_record));

    for (case, replay) in cases {
        assert!(
            SameTransactionProjectionEvidence::validate_replay_value(&replay).is_err(),
            "{case} must be rejected"
        );
    }
}

#[test]
fn failure_batch_validation_rechecks_all_mutable_identity_and_payload_fields() {
    let valid = failure_batch();
    valid.validate().unwrap();

    let mut cases = Vec::new();
    let mut empty_failure_id = valid.clone();
    empty_failure_id.failure_id.clear();
    cases.push(("empty failure ID", empty_failure_id));

    let mut invalid_failure_code = valid.clone();
    invalid_failure_code.failure_code = "decode error".into();
    cases.push(("invalid failure code", invalid_failure_code));

    let mut empty_details = valid.clone();
    empty_details.failure_bytes.clear();
    empty_details.failure_digest =
        ProjectionFailureBatch::fingerprint_bytes(&empty_details.failure_bytes);
    cases.push(("empty failure details", empty_details));

    let mut oversized_details = valid.clone();
    oversized_details.failure_bytes = vec![0; MAX_FAILURE_DETAIL_BYTES + 1];
    oversized_details.failure_digest =
        ProjectionFailureBatch::fingerprint_bytes(&oversized_details.failure_bytes);
    cases.push(("oversized failure details", oversized_details));

    let mut mismatched_digest = valid.clone();
    mismatched_digest.failure_bytes[0] ^= 0xff;
    cases.push(("mismatched failure digest", mismatched_digest));

    let mut empty_message = valid.clone();
    empty_message.input.message_id.clear();
    cases.push(("empty input message ID", empty_message));

    let mut invalid_causation = valid;
    invalid_causation.input.causation_id = "cause\n2".into();
    cases.push(("invalid input causation ID", invalid_causation));

    for (case, batch) in cases {
        assert!(batch.validate().is_err(), "{case} must be rejected");
    }
}
