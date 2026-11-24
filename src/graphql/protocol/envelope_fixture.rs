//! `tests/fixtures/protocol-envelope-v1.json`: whole GraphQL responses as this
//! crate serializes them, for the TypeScript client to parse and admit
//! (`js/tests/protocol-envelope-fixture.test.mjs`).
//!
//! Every envelope is assembled by [`ProtocolResponseAccumulator`] from the
//! same receipt, status and snapshot values the resolvers hand it, then
//! attached to an `async_graphql::Response` and serialized the way the HTTP and
//! GraphQL-WS transports do. Regenerate with
//! `UPDATE_GOLDEN=1 cargo test -p distributed --features graphql --lib envelope_fixture`.

use async_graphql::{PathSegment, Pos, Response, Value};
use serde_json::json;

use super::*;
use crate::command::CommandConsistency;
use crate::command_ledger::CommandLedgerState;
use crate::graphql::client_manifest::DISTRIBUTED_CLIENT_PROTOCOL_VERSION;
use crate::graphql::engine::live_resume::MAX_LIVE_RESUME_PROJECTION_BYTES;
use crate::graphql::query_protocol::MAX_PROTOCOL_EVIDENCE_ITEMS;
use crate::microsvc::{
    CausalCommandPublicState, CausalCommandPublicStatus, CausalCommandReceiptSource,
};
use crate::projection_protocol::{
    PartitionId, ProjectionChangeCursor, ProjectionEpoch, ProjectionObservationKind,
    ProjectionRecordMetadata, ProjectionRecordScope, ProjectorTopologyId, RecordRevision,
    SameTransactionProjectionEvidence,
};

const FIXTURE: &str = "tests/fixtures/protocol-envelope-v1.json";
const SCHEMA_HASH: &str = "sha256:5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c";
const AUTHORIZATION_GENERATION: &str =
    "sha256:a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7";
const QUERY_OPERATION: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const LIVE_OPERATION: &str =
    "sha256:2222222222222222222222222222222222222222222222222222222222222222";
const COMMAND_OPERATION: &str =
    "sha256:3333333333333333333333333333333333333333333333333333333333333333";
const STATUS_OPERATION: &str =
    "sha256:4444444444444444444444444444444444444444444444444444444444444444";
const PROJECTOR: &str = "todos-projector";
const MODEL: &str = "TodoView";
const COMMAND_NAME: &str = "todo.complete";
const COMMAND_FIELD: &str = "todo_complete";
const COMMAND_ID: &str = "0190a000-0000-7000-8000-000000000042";
const CAUSATION_ID: &str = "0190a000-0000-7000-8000-000000000017";
/// One past `Number.MAX_SAFE_INTEGER`: a position a client must not read
/// through `number`.
const UNSAFE_POSITION: u64 = 9_007_199_254_740_993;

fn accumulator(operation: &str) -> ProtocolResponseAccumulator {
    let codec = ProtocolTokenCodec::new([0x5c; 32]);
    let cache_scope = codec
        .issue(ProtocolTokenPurpose::CacheScope, &("principal", "user"))
        .unwrap();
    ProtocolResponseAccumulator::new(
        DistributedEnvelopeV1::new(
            SCHEMA_HASH,
            AUTHORIZATION_GENERATION,
            cache_scope,
            Some(operation.into()),
        ),
        codec,
    )
}

fn topology() -> ProjectorTopologyId {
    ProjectorTopologyId::new(1, PROJECTOR, [17; 32]).unwrap()
}

fn partition() -> PartitionId {
    PartitionId::new(br#"["tenant-private"]"#.to_vec()).unwrap()
}

fn record_scope(id: &str) -> ProjectionRecordScope {
    ProjectionRecordScope::new(
        topology(),
        partition(),
        MODEL,
        serde_json::to_vec(&[id]).unwrap(),
    )
    .unwrap()
}

fn cursor(position: u64) -> ProjectionChangeCursor {
    ProjectionChangeCursor::new(
        topology(),
        partition(),
        ProjectionEpoch::new("todos-v1").unwrap(),
        position,
    )
    .unwrap()
}

/// One row of `todos`: its wire data and the record it is evidence for.
struct Row {
    id: &'static str,
    title: &'static str,
    incarnation: u64,
    revision: u64,
}

fn todos(rows: &[Row]) -> serde_json::Value {
    json!({
        "todos": rows
            .iter()
            .map(|row| json!({ "id": row.id, "title": row.title }))
            .collect::<Vec<_>>()
    })
}

/// Resumable evidence for `rows` at `position`, as `wire_query_snapshot` and
/// `prepare_live_metadata` assemble it for one projector partition.
fn snapshot(
    accumulator: &ProtocolResponseAccumulator,
    rows: &[Row],
    position: u64,
    observed_causation: Option<&str>,
) -> DistributedQuerySnapshot {
    // Query and live documents of one plan share a snapshot scope.
    let scope_token = accumulator
        .issue_query_snapshot_scope(&("todos", "canonical-window"))
        .unwrap();
    let cursor = cursor(position);
    DistributedQuerySnapshot {
        scope_token: scope_token.clone(),
        records_complete: true,
        indexes_comparable: true,
        records: rows
            .iter()
            .enumerate()
            .map(|(index, row)| DistributedRecordRevision {
                path: Some(vec!["todos".into(), index.to_string()]),
                model: MODEL.into(),
                scope_token: accumulator
                    .issue_record_scope(&record_scope(row.id))
                    .unwrap(),
                incarnation: row.incarnation.to_string(),
                revision: row.revision.to_string(),
                tombstone: false,
            })
            .collect(),
        indexes: vec![DistributedIndexRevision {
            projection: PROJECTOR.into(),
            scope_token: accumulator
                .issue_index_scope(&scope_token, &cursor)
                .unwrap(),
            position: position.to_string(),
            resume: Some(
                accumulator
                    .issue_live_resume(PROJECTOR, &scope_token, &cursor)
                    .unwrap(),
            ),
        }],
        observations: observed_causation
            .into_iter()
            .map(|causation_id| DistributedProjectionObservation {
                causation_id: causation_id.into(),
                projection: PROJECTOR.into(),
                model: MODEL.into(),
                scope_token: accumulator
                    .issue_projection_obligation_scope(
                        causation_id,
                        PROJECTOR,
                        MODEL,
                        ProjectionObservationKind::Record,
                        &record_scope(rows[0].id),
                    )
                    .unwrap(),
            })
            .collect(),
    }
}

fn resumable(snapshot: &DistributedQuerySnapshot, reset: bool) -> DistributedLiveMetadata {
    DistributedLiveMetadata {
        mode: DistributedLiveMode::Resumable,
        reset,
        cursors: snapshot
            .indexes
            .iter()
            .filter_map(|index| index.resume.clone())
            .collect(),
    }
}

/// The response as a transport writes it.
fn wire(accumulator: &ProtocolResponseAccumulator, mut response: Response) -> serde_json::Value {
    accumulator.attach(&mut response).unwrap();
    serde_json::to_value(response).unwrap()
}

fn data(value: serde_json::Value) -> Response {
    Response::new(Value::from_json(value).unwrap())
}

fn error(message: &str, code: Option<(&str, u16)>, field: &str) -> Response {
    let mut error = match code {
        Some((code, status)) => {
            crate::graphql::schema::client_error_with_status(code, status, message)
        }
        None => async_graphql::Error::new(message),
    }
    .into_server_error(Pos { line: 1, column: 3 });
    error.path = vec![PathSegment::Field(field.into())];
    Response::from_errors(vec![error])
}

fn receipt(
    consistency: CommandConsistency,
    state: CommandLedgerState,
) -> CausalCommandReceiptSource {
    CausalCommandReceiptSource {
        command_id: COMMAND_ID.into(),
        command_name: COMMAND_NAME.into(),
        causation_id: CAUSATION_ID.into(),
        consistency,
        state,
        outcome: json!({ "id": "todo-1" }),
        projection_metadata: None,
        direct_projection: None,
    }
}

/// The same-transaction record an atomic command seals its row with.
fn direct_projection(revision: u64) -> SameTransactionProjectionEvidence {
    SameTransactionProjectionEvidence {
        records: vec![ProjectionRecordMetadata {
            source_snapshot: None,
            revision: RecordRevision::new(record_scope("todo-1"), 1, revision).unwrap(),
            tombstone: false,
            change: cursor(revision),
        }],
        changes: Vec::new(),
        observations: Vec::new(),
    }
}

fn dispatch(receipt: &CausalCommandReceiptSource, payload: serde_json::Value) -> serde_json::Value {
    let accumulator = accumulator(COMMAND_OPERATION);
    accumulator.claim_dispatch().unwrap();
    accumulator.record_receipt(receipt).unwrap();
    wire(&accumulator, data(json!({ COMMAND_FIELD: payload })))
}

/// `commandStatus` as `resolve_command_status` answers it.
fn status(
    state: CausalCommandPublicState,
    identity: Option<CommandConsistency>,
    direct_projection: Option<SameTransactionProjectionEvidence>,
) -> serde_json::Value {
    let status = CausalCommandPublicStatus {
        state,
        command_id: COMMAND_ID.into(),
        command_name: identity.map(|_| COMMAND_NAME.into()),
        causation_id: identity.map(|_| CAUSATION_ID.into()),
        consistency: identity,
        outcome: None,
        projection_metadata: None,
        projection_revalidate: false,
        evidence: Vec::new(),
        direct_projection,
    };
    let accumulator = accumulator(STATUS_OPERATION);
    accumulator.record_status(&status).unwrap();
    wire(
        &accumulator,
        data(json!({ "commandStatus": { "state": status.state.as_str() } })),
    )
}

fn fixture() -> serde_json::Value {
    let query = accumulator(QUERY_OPERATION);
    let first = [Row {
        id: "todo-1",
        title: "first",
        incarnation: 1,
        revision: 1,
    }];
    query
        .record_query_metadata(snapshot(&query, &first, 1, None), None)
        .unwrap();
    let scope = query.snapshot().unwrap();

    // One GraphQL-WS subscription: the opening snapshot, a delta, a reset
    // after the retained change log was outrun, then the failure frame.
    let live = accumulator(LIVE_OPERATION);
    live.begin_stream().unwrap();
    let opening = [Row {
        id: "todo-1",
        title: "second",
        incarnation: 1,
        revision: 2,
    }];
    let delta = [
        Row {
            id: "todo-1",
            title: "second",
            incarnation: 1,
            revision: 2,
        },
        Row {
            id: "todo-2",
            title: "added",
            incarnation: 1,
            revision: 3,
        },
    ];
    let reset = [Row {
        id: "todo-2",
        title: "after the gap",
        incarnation: 2,
        revision: UNSAFE_POSITION,
    }];
    for (rows, position, is_reset, observed) in [
        (&opening[..], 2, true, None),
        (&delta[..], 3, false, Some(CAUSATION_ID)),
        (&reset[..], UNSAFE_POSITION, true, None),
    ] {
        let snapshot = snapshot(&live, rows, position, observed);
        let metadata = resumable(&snapshot, is_reset);
        live.record_query_metadata(snapshot, Some(metadata))
            .unwrap();
    }
    let frames = [&opening[..], &delta[..], &reset[..]].map(|rows| wire(&live, data(todos(rows))));
    let failure = wire(
        &live,
        error("GraphQL query snapshot failed: fixture", None, "todos"),
    );

    // A row-filtered subscription: authorized data without a comparable
    // projector position, so every frame replaces the last one.
    let unresumable = accumulator(LIVE_OPERATION);
    unresumable.begin_stream().unwrap();
    let mut private = snapshot(&unresumable, &first, 1, None);
    private.indexes_comparable = false;
    unresumable
        .record_query_metadata(
            private,
            Some(DistributedLiveMetadata {
                mode: DistributedLiveMode::Snapshot,
                reset: true,
                cursors: Vec::new(),
            }),
        )
        .unwrap();

    let mut atomic = receipt(CommandConsistency::Atomic, CommandLedgerState::Atomic);
    atomic.direct_projection = Some(direct_projection(4));
    let rejection = accumulator(COMMAND_OPERATION);
    rejection.claim_dispatch().unwrap();

    use CausalCommandPublicState as State;
    use CommandConsistency::Eventual;
    json!({
        "version": 1,
        "scope": {
            "protocolVersion": DISTRIBUTED_CLIENT_PROTOCOL_VERSION,
            "schemaHash": scope.schema_hash,
            "authorizationGeneration": scope.authorization_generation,
            "cacheScope": scope.cache_scope,
        },
        "limits": {
            "evidenceItems": MAX_PROTOCOL_EVIDENCE_ITEMS,
            "liveResumeCursors": MAX_LIVE_RESUME_CURSORS,
            "opaqueTokenBytes": super::token::MAX_OPAQUE_TOKEN_BYTES,
            "liveResumeProjectionBytes": MAX_LIVE_RESUME_PROJECTION_BYTES,
            "commandProjectionObligations": MAX_COMMAND_PROJECTION_OBLIGATIONS,
        },
        "operations": {
            "query": QUERY_OPERATION,
            "live": LIVE_OPERATION,
            "command": COMMAND_OPERATION,
            "status": STATUS_OPERATION,
        },
        "query": wire(&query, data(todos(&first))),
        "live": {
            "opening": frames[0],
            "delta": frames[1],
            "reset": frames[2],
            "failure": failure,
            "unresumable": wire(&unresumable, data(todos(&first))),
        },
        "command": {
            "name": COMMAND_NAME,
            "mutationField": COMMAND_FIELD,
            "commandId": COMMAND_ID,
            "causationId": CAUSATION_ID,
            "dispatch": {
                "succeeded": dispatch(
                    &receipt(CommandConsistency::Succeeded, CommandLedgerState::Succeeded),
                    json!({ "id": "todo-1" }),
                ),
                "in_progress": dispatch(
                    &receipt(Eventual, CommandLedgerState::InProgress),
                    json!({ "id": "todo-1" }),
                ),
                "succeeded_pending_projection": dispatch(
                    &receipt(Eventual, CommandLedgerState::SucceededPendingProjection),
                    json!({ "id": "todo-1" }),
                ),
                "atomic": dispatch(&atomic, json!({ "id": "todo-1", "title": "sealed" })),
                "rejected": wire(
                    &rejection,
                    error("todo is already done", Some(("REJECTED", 422)), COMMAND_FIELD),
                ),
            },
            "status": {
                "in_progress": status(State::InProgress, Some(Eventual), None),
                "succeeded": status(State::Succeeded, Some(Eventual), None),
                "succeeded_pending_projection":
                    status(State::SucceededPendingProjection, Some(Eventual), None),
                "atomic": status(
                    State::Atomic,
                    Some(CommandConsistency::Atomic),
                    Some(direct_projection(4)),
                ),
                "rejected": status(State::Rejected, Some(Eventual), None),
                "projection_failed": status(State::ProjectionFailed, Some(Eventual), None),
                "expired": status(State::Expired, None, None),
                "unknown": status(State::Unknown, None, None),
            },
        },
    })
}

#[test]
fn protocol_envelope_fixture_is_current() {
    let actual = serde_json::to_string_pretty(&fixture()).unwrap() + "\n";
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(FIXTURE);
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, actual).unwrap();
        return;
    }
    // Not `assert_eq!`: a diff of the two documents says more than both in full.
    assert!(
        std::fs::read_to_string(&path).unwrap_or_default() == actual,
        "{FIXTURE} drifted: the response envelope changed (UPDATE_GOLDEN=1 to accept, review the diff, then run the js tests)"
    );
}
