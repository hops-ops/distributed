use super::accumulator::ProtocolAccumulatorError;
use super::*;
use crate::command::CommandConsistency;
use crate::command_ledger::CommandLedgerState;
use crate::microsvc::{
    CausalCommandProjectionObligation, CausalCommandPublicState, CausalCommandPublicStatus,
    CausalCommandReceiptSource, CausalProjectionEvidenceState,
};
use crate::projection_protocol::{
    ProjectionChange, ProjectionChangeCursor, ProjectionChangeKind, ProjectionEpoch,
    ProjectionObservation, ProjectionObservationKind, ProjectionPartition,
    ProjectionRecordMetadata, ProjectionRecordScope, ProjectorTopologyId, RecordRevision,
    SameTransactionProjectionEvidence,
};
use async_graphql::{Response, Value};

fn codec(byte: u8) -> ProtocolTokenCodec {
    ProtocolTokenCodec::new([byte; 32])
}

#[test]
fn opaque_tokens_are_deterministic_bound_and_non_disclosing() {
    let material = ("tenant-7", "todos", "private-key-42");
    let token = codec(7)
        .issue(ProtocolTokenPurpose::ProjectionObligation, &material)
        .unwrap();
    let again = codec(7)
        .issue(ProtocolTokenPurpose::ProjectionObligation, &material)
        .unwrap();
    assert_eq!(token, again);
    assert!(!token.as_str().contains("tenant"));
    assert!(!token.as_str().contains("private-key"));
    codec(7)
        .verify(
            &token,
            ProtocolTokenPurpose::ProjectionObligation,
            &material,
        )
        .unwrap();
    assert_eq!(
        codec(7).verify(&token, ProtocolTokenPurpose::CacheScope, &material),
        Err(ProtocolTokenError::Malformed)
    );
    assert_eq!(
        codec(8).verify(
            &token,
            ProtocolTokenPurpose::ProjectionObligation,
            &material
        ),
        Err(ProtocolTokenError::Mismatch)
    );
    assert_eq!(
        codec(7).verify(
            &token,
            ProtocolTokenPurpose::ProjectionObligation,
            &("tenant-7", "todos", "other")
        ),
        Err(ProtocolTokenError::Mismatch)
    );
    assert_eq!(format!("{token:?}"), "OpaqueProtocolToken([redacted])");
}

#[test]
fn malformed_or_tampered_tokens_fail_closed() {
    let material = ("scope", 9_u64);
    let token = codec(3)
        .issue(ProtocolTokenPurpose::CacheScope, &material)
        .unwrap();
    let mut changed = token.as_str().as_bytes().to_vec();
    let last = changed.last_mut().unwrap();
    *last = if *last == b'A' { b'B' } else { b'A' };
    let tampered = OpaqueProtocolToken(String::from_utf8(changed).unwrap());
    assert!(matches!(
        codec(3).verify(&tampered, ProtocolTokenPurpose::CacheScope, &material),
        Err(ProtocolTokenError::Mismatch | ProtocolTokenError::Malformed)
    ));
    let malformed = OpaqueProtocolToken("scope-is-not-a-token".into());
    assert_eq!(
        codec(3).verify(&malformed, ProtocolTokenPurpose::CacheScope, &material),
        Err(ProtocolTokenError::Malformed)
    );
}

fn command(id: &str) -> DistributedCommandMetadata {
    DistributedCommandMetadata {
        command_id: id.into(),
        causation_id: "cause-17".into(),
        state: DistributedCommandState::SucceededPendingProjection,
        consistency: DistributedCommandConsistency::Eventual,
        projection_disposition: None,
        expects: vec![DistributedProjectionExpectation {
            projection: "todos".into(),
            model: "TodoView".into(),
            scope_token: codec(9)
                .issue(ProtocolTokenPurpose::ProjectionObligation, &(id, 42_u64))
                .unwrap(),
        }],
        projection: None,
        observations: Vec::new(),
        records: Vec::new(),
    }
}

fn receipt() -> CausalCommandReceiptSource {
    let topology = ProjectorTopologyId::new(1, "todos", [17; 32]).unwrap();
    let partition =
        ProjectionPartition::new(br#"["tenant-private-900719925474099312345"]"#.to_vec()).unwrap();
    let scope = ProjectionRecordScope::new(
        topology,
        partition,
        "TodoView",
        br#"["9223372036854775807","child-private"]"#.to_vec(),
    )
    .unwrap();
    CausalCommandReceiptSource {
        command_id: "0190a000-0000-7000-8000-000000000042".into(),
        command_name: "todo.complete".into(),
        causation_id: "0190a000-0000-7000-8000-000000000017".into(),
        consistency: CommandConsistency::Eventual,
        state: CommandLedgerState::SucceededPendingProjection,
        outcome: serde_json::json!({ "accepted": true }),
        obligations: vec![CausalCommandProjectionObligation {
            projector: "todos".into(),
            model: "TodoView".into(),
            scope,
            observation_kind: ProjectionObservationKind::Record,
        }],
        projection_metadata: None,
        direct_projection: None,
    }
}

fn direct_projected_receipt() -> CausalCommandReceiptSource {
    let mut receipt = receipt();
    let scope = receipt.obligations[0].scope.clone();
    let revision = RecordRevision::new(scope.clone(), 3, 9_007_199_254_740_991).unwrap();
    let change = ProjectionChangeCursor::new(
        scope.topology().clone(),
        scope.projection_partition().clone(),
        ProjectionEpoch::new("direct-v1").unwrap(),
        17,
    )
    .unwrap();
    receipt.consistency = CommandConsistency::Atomic;
    receipt.state = CommandLedgerState::Atomic;
    receipt.obligations.clear();
    receipt.direct_projection = Some(SameTransactionProjectionEvidence {
        records: vec![ProjectionRecordMetadata {
            source_snapshot: None,
            revision: revision.clone(),
            tombstone: false,
            change: change.clone(),
        }],
        changes: vec![ProjectionChange {
            cursor: change.clone(),
            kind: ProjectionChangeKind::RecordUpsert,
            causation_id: receipt.causation_id.clone(),
            observation_kind: None,
            scope: Some(scope.clone()),
            revision: Some(revision.clone()),
            failure_id: None,
        }],
        observations: vec![ProjectionObservation {
            causation_id: receipt.causation_id.clone(),
            kind: ProjectionObservationKind::Record,
            revision: Some(revision),
            scope,
            change,
        }],
    });
    receipt
}

fn change_cursor(position: u64) -> ProjectionChangeCursor {
    ProjectionChangeCursor::new(
        ProjectorTopologyId::new(1, "todos", [17; 32]).unwrap(),
        ProjectionPartition::new(br#"["tenant-private-900719925474099312345"]"#.to_vec()).unwrap(),
        ProjectionEpoch::new("todos-v1").unwrap(),
        position,
    )
    .unwrap()
}

fn query_snapshot(
    accumulator: &ProtocolResponseAccumulator,
    operation: &str,
    position: u64,
) -> DistributedQuerySnapshot {
    let scope_token = accumulator
        .issue_query_snapshot_scope(&(operation, "canonical-window"))
        .unwrap();
    let cursor = change_cursor(position);
    DistributedQuerySnapshot {
        scope_token: scope_token.clone(),
        records_complete: true,
        indexes_comparable: true,
        records: Vec::new(),
        indexes: vec![DistributedIndexRevision {
            projection: "todos".into(),
            scope_token: accumulator
                .issue_index_scope(&scope_token, &cursor)
                .unwrap(),
            position: position.to_string(),
            resume: Some(
                accumulator
                    .issue_live_resume("todos", &scope_token, &cursor)
                    .unwrap(),
            ),
        }],
        observations: Vec::new(),
    }
}

fn accumulator(key: u8, cache_material: &str, schema: &str) -> ProtocolResponseAccumulator {
    let codec = codec(key);
    let cache_scope = codec
        .issue(ProtocolTokenPurpose::CacheScope, &cache_material)
        .unwrap();
    ProtocolResponseAccumulator::new(
        DistributedEnvelopeV1::new(schema, "sha256:test-authorization", cache_scope, None),
        codec,
    )
}

#[test]
fn accumulator_is_idempotent_and_rejects_ambiguous_receipts() {
    let scope = codec(9)
        .issue(ProtocolTokenPurpose::CacheScope, &("principal", "surface"))
        .unwrap();
    let accumulator = ProtocolResponseAccumulator::new(
        DistributedEnvelopeV1::new(
            "sha256:schema",
            "sha256:test-authorization",
            scope,
            Some("sha256:operation".into()),
        ),
        codec(9),
    );
    accumulator.claim_dispatch().unwrap();
    assert_eq!(
        accumulator.claim_dispatch(),
        Err(ProtocolAccumulatorError::MultipleCommands)
    );
    accumulator.record_command(command("cmd-1")).unwrap();
    accumulator.record_command(command("cmd-1")).unwrap();
    assert_eq!(
        accumulator.record_command(command("cmd-2")),
        Err(ProtocolAccumulatorError::MultipleCommands)
    );

    let envelope = accumulator.snapshot().unwrap();
    let json = serde_json::to_value(envelope).unwrap();
    assert_eq!(json["protocolVersion"], 1);
    assert_eq!(json["schemaHash"], "sha256:schema");
    assert_eq!(json["authorizationGeneration"], "sha256:test-authorization");
    assert_eq!(json["operation"], "sha256:operation");
    assert_eq!(json["command"]["commandId"], "cmd-1");
    assert_eq!(json["command"]["state"], "succeeded_pending_projection");
    assert_eq!(json["command"]["expects"][0]["model"], "TodoView");
    assert!(json["command"]["expects"][0]["scopeToken"]
        .as_str()
        .unwrap()
        .starts_with("v1.projection-obligation."));
}

#[test]
fn durable_receipts_issue_stable_generation_bound_non_disclosing_obligations() {
    let receipt = receipt();
    let first = accumulator(11, "principal-a", "sha256:schema-a");
    first.record_receipt(&receipt).unwrap();
    let first = serde_json::to_value(first.snapshot().unwrap()).unwrap();
    let first_token = first["command"]["expects"][0]["scopeToken"]
        .as_str()
        .unwrap();
    assert_eq!(first["command"]["state"], "succeeded_pending_projection");
    assert_eq!(first["command"]["consistency"], "eventual");
    assert!(!first_token.contains("tenant-private"));
    assert!(!first_token.contains("9223372036854775807"));
    assert!(!first_token.contains("child-private"));

    let replay = accumulator(11, "principal-a", "sha256:schema-a");
    replay.record_receipt(&receipt).unwrap();
    let replay = serde_json::to_value(replay.snapshot().unwrap()).unwrap();
    assert_eq!(
        first_token,
        replay["command"]["expects"][0]["scopeToken"]
            .as_str()
            .unwrap()
    );

    for changed in [
        accumulator(12, "principal-a", "sha256:schema-a"),
        accumulator(11, "principal-b", "sha256:schema-a"),
        accumulator(11, "principal-a", "sha256:schema-b"),
    ] {
        changed.record_receipt(&receipt).unwrap();
        let changed = serde_json::to_value(changed.snapshot().unwrap()).unwrap();
        assert_ne!(
            first_token,
            changed["command"]["expects"][0]["scopeToken"]
                .as_str()
                .unwrap()
        );
    }
}

#[test]
fn direct_projected_receipt_replays_exact_record_revision_as_decimal_strings() {
    let receipt = direct_projected_receipt();
    let first = accumulator(17, "principal-a", "sha256:schema-a");
    first.record_receipt(&receipt).unwrap();
    let command = serde_json::to_value(first.snapshot().unwrap()).unwrap()["command"].clone();
    assert_eq!(command["state"], "atomic");
    assert_eq!(command["consistency"], "atomic");
    assert_eq!(command["records"][0]["incarnation"], "3");
    assert_eq!(command["records"][0]["revision"], "9007199254740991");
    assert_eq!(command["records"][0]["tombstone"], false);
    assert!(command["records"][0].get("path").is_none());
    let token = command["records"][0]["scopeToken"].as_str().unwrap();
    assert!(token.starts_with("v1.record-revision."));
    assert!(!token.contains("tenant-private"));
    assert!(!token.contains("child-private"));

    let replay = accumulator(17, "principal-a", "sha256:schema-a");
    replay.record_receipt(&receipt).unwrap();
    let replay = serde_json::to_value(replay.snapshot().unwrap()).unwrap();
    assert_eq!(command["records"], replay["command"]["records"]);
}

#[test]
fn unknown_status_does_not_fabricate_receipt_identity() {
    let accumulator = accumulator(5, "principal-a", "sha256:schema-a");
    accumulator
        .record_status(&CausalCommandPublicStatus {
            state: CausalCommandPublicState::Unknown,
            command_id: "0190a000-0000-7000-8000-000000000099".into(),
            command_name: None,
            causation_id: None,
            consistency: None,
            outcome: None,
            obligations: Vec::new(),
            projection_metadata: None,
            projection_revalidate: false,
            evidence: Vec::new(),
            direct_projection: None,
        })
        .unwrap();
    let envelope = serde_json::to_value(accumulator.snapshot().unwrap()).unwrap();
    assert!(envelope.get("command").is_none());
}

#[test]
fn projected_status_exposes_only_matching_opaque_observations() {
    let source = receipt();
    let accumulator = accumulator(5, "principal-a", "sha256:schema-a");
    accumulator
        .record_status(&CausalCommandPublicStatus {
            state: CausalCommandPublicState::Atomic,
            command_id: source.command_id,
            command_name: Some(source.command_name),
            causation_id: Some(source.causation_id.clone()),
            consistency: Some(source.consistency),
            outcome: Some(source.outcome),
            obligations: source.obligations,
            projection_metadata: None,
            projection_revalidate: false,
            evidence: vec![crate::microsvc::CausalCommandProjectionEvidence {
                obligation_index: 0,
                state: CausalProjectionEvidenceState::Observed,
                incarnation: Some(1),
                revision: Some(7),
            }],
            direct_projection: None,
        })
        .unwrap();
    let command = serde_json::to_value(accumulator.snapshot().unwrap()).unwrap()["command"].clone();
    assert_eq!(command["state"], "atomic");
    assert_eq!(
        command["observations"][0]["causationId"],
        source.causation_id
    );
    assert_eq!(
        command["observations"][0]["scopeToken"],
        command["expects"][0]["scopeToken"]
    );
    let encoded = command.to_string();
    assert!(!encoded.contains("tenant-private"));
    assert!(!encoded.contains("9223372036854775807"));
}

#[test]
fn revision_and_resume_tokens_are_scoped_comparable_and_tamper_evident() {
    let accumulator = accumulator(31, "principal-a", "sha256:schema-a");
    let snapshot_scope = accumulator
        .issue_query_snapshot_scope(&("sha256:operation-a", "window-a"))
        .unwrap();
    let other_operation = accumulator
        .issue_query_snapshot_scope(&("sha256:operation-b", "window-a"))
        .unwrap();
    assert_ne!(snapshot_scope, other_operation);

    let receipt = receipt();
    let scope = &receipt.obligations[0].scope;
    let record_scope = accumulator.issue_record_scope(scope).unwrap();
    assert_eq!(record_scope, accumulator.issue_record_scope(scope).unwrap());
    assert!(!record_scope.as_str().contains("tenant-private"));
    assert!(!record_scope.as_str().contains("9223372036854775807"));

    let cursor = change_cursor(9_007_199_254_740_991);
    let live = accumulator
        .issue_live_resume("todos", &snapshot_scope, &cursor)
        .unwrap();
    assert_eq!(live.position, "9007199254740991");
    accumulator
        .verify_live_resume(&live, &snapshot_scope, &cursor)
        .unwrap();

    let parsed = OpaqueProtocolToken::parse(live.token.as_str()).unwrap();
    assert_eq!(parsed, live.token);
    let mut wrong_position = live.clone();
    wrong_position.position = "9007199254740990".into();
    assert_eq!(
        accumulator.verify_live_resume(&wrong_position, &snapshot_scope, &cursor),
        Err(ProtocolTokenError::Mismatch)
    );
    assert_eq!(
        accumulator.verify_live_resume(&live, &other_operation, &cursor),
        Err(ProtocolTokenError::Mismatch)
    );
    assert_eq!(
        OpaqueProtocolToken::parse("v1.live-resume.not/canonical"),
        Err(ProtocolTokenError::Malformed)
    );
}

#[test]
fn observation_tokens_exactly_match_receipt_obligations() {
    let receipt = receipt();
    let accumulator = accumulator(41, "principal-a", "sha256:schema-a");
    accumulator.record_receipt(&receipt).unwrap();
    let expectation = accumulator.snapshot().unwrap().command.unwrap().expects[0]
        .scope_token
        .clone();
    let obligation = &receipt.obligations[0];
    let observed = accumulator
        .issue_projection_obligation_scope(
            &receipt.causation_id,
            &obligation.projector,
            &obligation.model,
            obligation.observation_kind,
            &obligation.scope,
        )
        .unwrap();
    assert_eq!(expectation, observed);
}

#[test]
fn query_snapshot_merge_preserves_comparable_indexes_when_only_records_are_incomplete() {
    let accumulator = accumulator(47, "principal-a", "sha256:schema-a");
    let mut record_incomplete = query_snapshot(&accumulator, "operation-a", 1);
    record_incomplete.records_complete = false;
    let mut record_complete = query_snapshot(&accumulator, "operation-a", 2);
    record_complete.indexes.clear();

    accumulator
        .record_query_metadata(record_incomplete, None)
        .unwrap();
    accumulator
        .record_query_metadata(record_complete, None)
        .unwrap();

    let snapshot = accumulator.snapshot().unwrap().snapshot.unwrap();
    assert!(!snapshot.records_complete);
    assert!(snapshot.indexes_comparable);
    assert_eq!(snapshot.indexes.len(), 1);
    let wire = serde_json::to_value(snapshot).unwrap();
    assert_eq!(wire["recordsComplete"], false);
    assert_eq!(wire["indexesComparable"], true);
    assert!(wire.get("complete").is_none());
}

#[test]
fn query_snapshot_merge_discards_index_evidence_when_any_index_is_incomparable() {
    let accumulator = accumulator(49, "principal-a", "sha256:schema-a");
    let comparable = query_snapshot(&accumulator, "operation-a", 1);
    let mut incomparable = query_snapshot(&accumulator, "operation-a", 2);
    incomparable.indexes_comparable = false;
    incomparable.observations = vec![DistributedProjectionObservation {
        causation_id: "causation-private".into(),
        projection: "todos".into(),
        model: "TodoView".into(),
        scope_token: incomparable.indexes[0].scope_token.clone(),
    }];

    accumulator.record_query_metadata(comparable, None).unwrap();
    accumulator
        .record_query_metadata(incomparable, None)
        .unwrap();

    let snapshot = accumulator.snapshot().unwrap().snapshot.unwrap();
    assert!(snapshot.records_complete);
    assert!(!snapshot.indexes_comparable);
    assert!(snapshot.indexes.is_empty());
    assert!(snapshot.observations.is_empty());
    let wire = serde_json::to_value(snapshot).unwrap();
    assert_eq!(wire["recordsComplete"], true);
    assert_eq!(wire["indexesComparable"], false);
    assert_eq!(wire["indexes"], serde_json::json!([]));
    assert_eq!(wire["observations"], serde_json::json!([]));
}

#[test]
fn stream_frames_are_immutable_fifo_and_do_not_bleed_forward() {
    let accumulator = accumulator(51, "principal-a", "sha256:schema-a");
    accumulator.begin_stream().unwrap();
    let first = query_snapshot(&accumulator, "operation-a", 1);
    let second = query_snapshot(&accumulator, "operation-a", 2);
    accumulator
        .record_query_metadata(
            first.clone(),
            Some(DistributedLiveMetadata {
                supported: true,
                reset: true,
                cursors: vec![first.indexes[0].resume.clone().unwrap()],
            }),
        )
        .unwrap();
    accumulator
        .record_query_metadata(
            second.clone(),
            Some(DistributedLiveMetadata {
                supported: true,
                reset: false,
                cursors: vec![second.indexes[0].resume.clone().unwrap()],
            }),
        )
        .unwrap();

    let mut first_response = Response::new(Value::Null);
    let mut second_response = Response::new(Value::Null);
    let mut trailing_response = Response::new(Value::Null);
    accumulator.attach(&mut first_response).unwrap();
    accumulator.attach(&mut second_response).unwrap();
    accumulator.attach(&mut trailing_response).unwrap();

    let first_json = first_response.extensions["distributed"]
        .clone()
        .into_json()
        .unwrap();
    let second_json = second_response.extensions["distributed"]
        .clone()
        .into_json()
        .unwrap();
    let trailing_json = trailing_response.extensions["distributed"]
        .clone()
        .into_json()
        .unwrap();
    assert_eq!(first_json["snapshot"]["indexes"][0]["position"], "1");
    assert_eq!(first_json["live"]["reset"], true);
    assert_eq!(second_json["snapshot"]["indexes"][0]["position"], "2");
    assert_eq!(second_json["live"]["reset"], false);
    assert!(trailing_json.get("snapshot").is_none());
    assert!(trailing_json.get("live").is_none());
}
