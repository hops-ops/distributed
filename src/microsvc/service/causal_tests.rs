//! The decision tables of typed causal dispatch: the public error vocabulary,
//! stored-receipt replay, the wait-path wire, ingress admission and the
//! ledger recovery that settles an attempt this process lost track of.

use std::collections::VecDeque;
use std::sync::Mutex;

use serde_json::json;

use super::*;
use crate::command::{typed_command, CommandInputType, CommandTypeDef, CommandTypeField};
use crate::command::{CommandOutputType, Succeeded};
use crate::command_ledger::{CommandAttempt, ReservationOutcome};
use crate::microsvc::service::routes::CausalCommandPolicy;
use crate::InMemoryRepository;

#[derive(serde::Deserialize)]
struct CausalUnitInput {
    #[allow(dead_code)]
    id: String,
}

#[derive(serde::Serialize)]
struct CausalUnitOutput {
    id: String,
}

fn id_type(name: &str) -> CommandTypeDef {
    CommandTypeDef::new(
        name,
        vec![CommandTypeField {
            unsigned_integer: None,
            name: "id".into(),
            type_name: "String".into(),
            nullable: false,
            list: false,
            item_nullable: false,
            nested: None,
        }],
    )
}

impl CommandInputType for CausalUnitInput {
    fn command_type() -> CommandTypeDef {
        id_type("CausalUnitInput").with_type_id(std::any::TypeId::of::<Self>())
    }
}

impl CommandOutputType for CausalUnitOutput {
    fn command_type() -> CommandTypeDef {
        id_type("CausalUnitOutput").with_type_id(std::any::TypeId::of::<Self>())
    }
}

fn admin_contract() -> TypedCommandContract {
    typed_command::<CausalUnitInput, Succeeded<CausalUnitOutput>>("causal.unit")
        .roles(["admin"])
        .into_contract()
}

fn new_command_id() -> CommandId {
    CommandId::parse(uuid::Uuid::now_v7().hyphenated().to_string()).unwrap()
}

fn replay(state: CommandLedgerState, outcome: Value) -> CommandReplay {
    CommandReplay {
        command_id: new_command_id(),
        command_name: "causal.unit".into(),
        state,
        causation_id: CausationId::new(),
        outcome,
        projection_metadata: None,
        direct_projection: None,
    }
}

fn session(user: Option<&str>, role: Option<&str>) -> Session {
    let mut session = Session::new();
    if let Some(user) = user {
        session.set(crate::microsvc::USER_ID_KEY, user);
    }
    if let Some(role) = role {
        session.set(crate::microsvc::ROLE_KEY, role);
    }
    session
}

/// A reservation acquired from a real ledger, so the fence and attempt are
/// the ones production code receives.
async fn acquire(
    repository: &InMemoryRepository,
    binding: Option<crate::command_ledger::ExternalDispatchBinding>,
) -> CommandAttempt {
    let key = CommandLedgerKey::new(
        "causal-unit-service",
        PrincipalPartitionId::new("principal").unwrap(),
        new_command_id(),
    )
    .unwrap();
    let policy = CausalCommandPolicy::default();
    let mut reservation = CommandReservation::new(
        key,
        "causal.unit",
        CommandContractFingerprint::new([1; 32]),
        CanonicalInputHash::new([2; 32]),
        policy.attempt_lease,
        policy.replay_retention,
    )
    .unwrap();
    if let Some(binding) = binding {
        reservation = reservation.with_external_binding(binding);
    }
    match CommandLedgerStore::reserve_command(repository, reservation)
        .await
        .unwrap()
    {
        ReservationOutcome::Acquired(attempt) => attempt,
        _ => panic!("a fresh command ID is acquired"),
    }
}

#[test]
fn every_dispatch_error_has_a_stable_code_status_and_client_message() {
    struct Case {
        name: &'static str,
        error: CausalDispatchError,
        code: &'static str,
        status: u16,
        message: String,
    }
    let cases = [
        Case {
            name: "bad request keeps its message",
            error: CausalDispatchError::BadRequest("id is required".into()),
            code: "BAD_REQUEST",
            status: 400,
            message: "id is required".into(),
        },
        Case {
            name: "unauthorized",
            error: CausalDispatchError::Unauthorized,
            code: "UNAUTHORIZED",
            status: 401,
            message: "missing authenticated principal".into(),
        },
        Case {
            name: "forbidden",
            error: CausalDispatchError::Forbidden,
            code: "FORBIDDEN",
            status: 403,
            message: "command is not allowed".into(),
        },
        Case {
            name: "command ID reuse",
            error: CausalDispatchError::CommandIdReuse,
            code: "COMMAND_ID_REUSE",
            status: 409,
            message: "command ID was already used for different input".into(),
        },
        Case {
            name: "in progress",
            error: CausalDispatchError::InProgress,
            code: "COMMAND_IN_PROGRESS",
            status: 409,
            message: "command is already in progress".into(),
        },
        Case {
            name: "expired",
            error: CausalDispatchError::Expired,
            code: "COMMAND_EXPIRED",
            status: 410,
            message: "command ID has expired".into(),
        },
        Case {
            name: "a stored rejection keeps its own code and status",
            error: CausalDispatchError::Rejected {
                code: "NOT_FOUND",
                status: 404,
                message: "no such todo".into(),
            },
            code: "NOT_FOUND",
            status: 404,
            message: "no such todo".into(),
        },
        Case {
            name: "handler decode failure",
            error: HandlerError::DecodeFailed("bad json".into()).into(),
            code: "BAD_REQUEST",
            status: 400,
            message: HandlerError::DecodeFailed("bad json".into()).to_string(),
        },
        Case {
            name: "handler unauthorized",
            error: HandlerError::Unauthorized("who are you".into()).into(),
            code: "UNAUTHORIZED",
            status: 401,
            message: HandlerError::Unauthorized("who are you".into()).to_string(),
        },
        Case {
            name: "handler not found",
            error: HandlerError::NotFound("todo-1".into()).into(),
            code: "NOT_FOUND",
            status: 404,
            message: HandlerError::NotFound("todo-1".into()).to_string(),
        },
        Case {
            name: "handler rejection",
            error: HandlerError::Rejected("already done".into()).into(),
            code: "REJECTED",
            status: 422,
            message: HandlerError::Rejected("already done".into()).to_string(),
        },
        Case {
            name: "a server-side handler failure is masked",
            error: HandlerError::Other("connection to 10.0.0.7 refused".into()).into(),
            code: "INTERNAL",
            status: 500,
            message: "Internal server error".into(),
        },
        Case {
            name: "concurrent write",
            error: CausalDispatchError::ConcurrentWrite,
            code: "CONCURRENT_WRITE",
            status: 503,
            message: "command conflicted with concurrent writes; retry".into(),
        },
        Case {
            name: "internal detail never reaches the client",
            error: CausalDispatchError::Internal("ledger row 42 is corrupt".into()),
            code: "INTERNAL",
            status: 500,
            message: "internal error".into(),
        },
    ];
    for case in cases {
        assert_eq!(case.error.code(), case.code, "{}", case.name);
        assert_eq!(case.error.status_code(), case.status, "{}", case.name);
        assert_eq!(case.error.client_message(), case.message, "{}", case.name);
    }
}

#[test]
fn display_shows_internal_detail_and_only_a_handler_error_has_a_source() {
    let internal = CausalDispatchError::Internal("ledger row 42 is corrupt".into());
    assert_eq!(internal.to_string(), "ledger row 42 is corrupt");
    assert!(std::error::Error::source(&internal).is_none());

    let forbidden = CausalDispatchError::Forbidden;
    assert_eq!(forbidden.to_string(), "command is not allowed");
    assert!(std::error::Error::source(&forbidden).is_none());

    let handler = CausalDispatchError::from(HandlerError::Rejected("already done".into()));
    let source = std::error::Error::source(&handler).expect("handler error is the source");
    assert!(matches!(
        source.downcast_ref::<HandlerError>(),
        Some(HandlerError::Rejected(message)) if message == "already done"
    ));
}

#[test]
fn a_client_fault_handler_error_is_stored_under_its_rejection_code() {
    let cases = [
        (HandlerError::DecodeFailed("x".into()), "BAD_REQUEST"),
        (HandlerError::GuardRejected("x".into()), "BAD_REQUEST"),
        (HandlerError::Unauthorized("x".into()), "UNAUTHORIZED"),
        (HandlerError::NotFound("x".into()), "NOT_FOUND"),
        (HandlerError::UnknownCommand("x".into()), "NOT_FOUND"),
        (HandlerError::Rejected("x".into()), "REJECTED"),
    ];
    for (error, code) in cases {
        assert_eq!(handler_error_code(&error), code, "{error:?}");
    }
}

#[test]
fn a_terminal_replay_returns_its_stored_outcome_as_a_replay() {
    for state in [
        CommandLedgerState::Succeeded,
        CommandLedgerState::SucceededPendingProjection,
        CommandLedgerState::Atomic,
        CommandLedgerState::ProjectionFailed,
    ] {
        let stored = replay(state, json!({ "id": "todo-1" }));
        let causation = stored.causation_id.as_str().to_string();
        let result = replay_result(CommandConsistency::Succeeded, stored)
            .unwrap_or_else(|error| panic!("{}: {error}", state.as_str()));
        assert!(result.replayed(), "{}", state.as_str());
        assert_eq!(result.payload(), &json!({ "id": "todo-1" }));
        assert_eq!(result.state(), state.as_str());
        assert_eq!(result.causation_id(), causation);
    }
}

#[test]
fn a_replay_that_is_not_terminal_or_holds_a_malformed_rejection_is_refused() {
    struct Case {
        name: &'static str,
        state: CommandLedgerState,
        outcome: Value,
        /// The internal detail the refusal carries.
        detail: &'static str,
    }
    let rejection = |error: Value| json!({ "error": error });
    let cases = [
        Case {
            name: "in progress",
            state: CommandLedgerState::InProgress,
            outcome: Value::Null,
            detail: "stored replay has a non-terminal state",
        },
        Case {
            name: "retryable unknown",
            state: CommandLedgerState::RetryableUnknown,
            outcome: Value::Null,
            detail: "stored replay has a non-terminal state",
        },
        Case {
            name: "expired",
            state: CommandLedgerState::Expired,
            outcome: Value::Null,
            detail: "stored replay has a non-terminal state",
        },
        Case {
            name: "rejection without an error object",
            state: CommandLedgerState::Rejected,
            outcome: json!({ "message": "no" }),
            detail: "stored rejection is malformed",
        },
        Case {
            name: "rejection with an unknown code",
            state: CommandLedgerState::Rejected,
            outcome: rejection(json!({ "code": "TEAPOT", "status": 418, "message": "no" })),
            detail: "stored rejection code is invalid",
        },
        Case {
            name: "rejection with a server status",
            state: CommandLedgerState::Rejected,
            outcome: rejection(json!({ "code": "REJECTED", "status": 500, "message": "no" })),
            detail: "stored rejection status is invalid",
        },
        Case {
            name: "rejection with a status outside u16",
            state: CommandLedgerState::Rejected,
            outcome: rejection(json!({ "code": "REJECTED", "status": 65_936, "message": "no" })),
            detail: "stored rejection status is invalid",
        },
        Case {
            name: "rejection without a message",
            state: CommandLedgerState::Rejected,
            outcome: rejection(json!({ "code": "REJECTED", "status": 422 })),
            detail: "stored rejection message is invalid",
        },
    ];
    for case in cases {
        match replay_result(
            CommandConsistency::Succeeded,
            replay(case.state, case.outcome),
        ) {
            Err(CausalDispatchError::Internal(detail)) => {
                assert_eq!(detail, case.detail, "{}", case.name)
            }
            Err(other) => panic!("{}: expected Internal, got {other:?}", case.name),
            Ok(_) => panic!("{}: expected a refusal", case.name),
        }
    }
}

#[test]
fn a_stored_rejection_replays_as_the_rejection_it_was() {
    for code in [
        "BAD_REQUEST",
        "UNAUTHORIZED",
        "FORBIDDEN",
        "NOT_FOUND",
        "REJECTED",
    ] {
        let outcome = rejection_outcome(code, 409, "already done");
        match replay_result(
            CommandConsistency::Succeeded,
            replay(CommandLedgerState::Rejected, outcome),
        ) {
            Err(CausalDispatchError::Rejected {
                code: replayed,
                status,
                message,
            }) => {
                assert_eq!(replayed, code);
                assert_eq!(status, 409, "{code}");
                assert_eq!(message, "already done", "{code}");
            }
            other => panic!("{code}: expected the stored rejection, got {other:?}"),
        }
    }
}

#[test]
fn stored_receipt_evidence_that_does_not_decode_is_an_internal_error() {
    let mut direct = replay(CommandLedgerState::Atomic, json!({}));
    direct.direct_projection = Some(json!({ "not": "evidence" }));
    match CausalCommandReceiptSource::from_replay(CommandConsistency::Atomic, direct) {
        Err(CausalDispatchError::Internal(detail)) => assert!(
            detail.starts_with("stored direct projection evidence is invalid"),
            "{detail}"
        ),
        other => panic!("expected Internal, got {other:?}"),
    }

    #[cfg(feature = "graphql")]
    {
        let mut metadata = replay(CommandLedgerState::Succeeded, json!({}));
        metadata.projection_metadata = Some(b"not metadata".to_vec());
        match CausalCommandReceiptSource::from_replay(CommandConsistency::Succeeded, metadata) {
            Err(CausalDispatchError::Internal(detail)) => assert!(
                detail.starts_with("stored command projection metadata is invalid"),
                "{detail}"
            ),
            other => panic!("expected Internal, got {other:?}"),
        }
    }
}

fn wait_path_body(command_id: &str, causation_id: &str, state: &str) -> Value {
    json!({
        "payload": { "id": "todo-1" },
        "receipt": {
            "commandId": command_id,
            "causationId": causation_id,
            "state": state,
            "replayed": true,
        },
        "events": [],
    })
}

#[test]
fn the_wait_path_wire_round_trips_and_a_new_payload_replaces_the_old() {
    let command_id = new_command_id();
    let causation = CausationId::new();
    let decoded = CausalDispatchResult::from_wait_path_wire(wait_path_body(
        command_id.as_str(),
        causation.as_str(),
        "succeeded",
    ))
    .unwrap();
    assert_eq!(decoded.command_id(), command_id.as_str());
    assert_eq!(decoded.causation_id(), causation.as_str());
    assert_eq!(decoded.state(), "succeeded");
    assert!(decoded.replayed());
    assert_eq!(
        CausalDispatchResult::from_wait_path_wire(decoded.wait_path_wire()).unwrap(),
        decoded
    );

    let remapped = decoded.with_payload(json!({ "todoId": "todo-1" }));
    assert_eq!(remapped.payload(), &json!({ "todoId": "todo-1" }));
    assert_eq!(
        remapped.wait_path_wire()["payload"],
        json!({ "todoId": "todo-1" })
    );
}

#[test]
fn a_wait_path_response_that_breaks_the_wire_contract_is_refused() {
    let causation = CausationId::new();
    let valid = || wait_path_body(new_command_id().as_str(), causation.as_str(), "succeeded");
    let event = json!({
        "id": "event-1",
        "eventType": "todo.created",
        "payload": [],
        "payloadCodec": "json",
        "payloadCodecVersion": 1,
        "sourceAggregateType": null,
        "sourceAggregateId": null,
        "sourceSequence": null,
    });
    let cases: Vec<(&str, Value, &str)> = vec![
        (
            "an unknown top-level field",
            {
                let mut body = valid();
                body["extra"] = json!(1);
                body
            },
            "invalid wait-path response",
        ),
        (
            "an empty command ID",
            wait_path_body("  ", causation.as_str(), "succeeded"),
            "wait-path receipt contains an invalid identifier",
        ),
        (
            "a command ID over 512 bytes",
            wait_path_body(&"c".repeat(513), causation.as_str(), "succeeded"),
            "wait-path receipt contains an invalid identifier",
        ),
        (
            "a causation ID over 512 bytes",
            wait_path_body(new_command_id().as_str(), &"c".repeat(513), "succeeded"),
            "wait-path receipt contains an invalid identifier",
        ),
        (
            "an unknown ledger state",
            wait_path_body(new_command_id().as_str(), causation.as_str(), "finished"),
            "wait-path receipt state",
        ),
        (
            "two projection events with one id",
            {
                let mut body = valid();
                body["events"] = json!([event.clone(), event.clone()]);
                body
            },
            "invalid cell projection event response",
        ),
    ];
    for (name, body, detail) in cases {
        match CausalDispatchResult::from_wait_path_wire(body) {
            Err(CausalDispatchError::Internal(message)) => {
                assert!(message.starts_with(detail), "{name}: {message}")
            }
            other => panic!("{name}: expected Internal, got {other:?}"),
        }
    }
}

#[cfg(feature = "graphql")]
#[test]
fn a_wait_path_receipt_reports_its_ledger_state_and_claims_no_observation() {
    let cases = [
        ("in_progress", CausalCommandPublicState::InProgress),
        ("retryable_unknown", CausalCommandPublicState::InProgress),
        ("succeeded", CausalCommandPublicState::Succeeded),
        (
            "succeeded_pending_projection",
            CausalCommandPublicState::SucceededPendingProjection,
        ),
        ("atomic", CausalCommandPublicState::Atomic),
        ("rejected", CausalCommandPublicState::Rejected),
        (
            "projection_failed",
            CausalCommandPublicState::ProjectionFailed,
        ),
        ("expired", CausalCommandPublicState::Expired),
    ];
    for (wire_state, expected) in cases {
        let causation = CausationId::new();
        let result = CausalDispatchResult::from_wait_path_wire(wait_path_body(
            new_command_id().as_str(),
            causation.as_str(),
            wire_state,
        ))
        .unwrap();
        let status = result.public_status();
        assert_eq!(status.state, expected, "{wire_state}");
        assert_eq!(
            status.command_name, None,
            "{wire_state}: no name on the wire"
        );
        assert_eq!(status.causation_id.as_deref(), Some(causation.as_str()));
        assert!(status.evidence.is_empty(), "{wire_state}");
        assert_eq!(status.outcome, Some(json!({ "id": "todo-1" })));
    }
}

#[test]
fn a_cell_ingress_hides_why_a_caller_was_refused_and_refuses_atomic_commands() {
    let identity = crate::microsvc::cell_host::CellCommandIdentity::new(
        "causal-unit-service",
        "principal",
        new_command_id().as_str(),
    )
    .unwrap();
    let shard = StreamIdentity::new("causal-unit", "todo-1").unwrap();
    let service = CommandIngress::Service {
        service_id: "causal-unit-service",
        principal_partition: "principal".into(),
        command_id: "unused",
        policy: CausalCommandPolicy::default(),
        seal_modeled_projection: None,
    };
    let cell = CommandIngress::Cell {
        identity: &identity,
        shard: &shard,
    };
    let contract = admin_contract();
    let mut atomic = admin_contract();
    atomic.consistency = CommandConsistency::Atomic;

    let anonymous = session(None, None);
    assert!(matches!(
        service.admit(&contract, &anonymous),
        Err(CausalDispatchError::Handler(HandlerError::Unauthorized(_)))
    ));
    assert!(matches!(
        cell.admit(&contract, &anonymous),
        Err(CausalDispatchError::Unauthorized)
    ));

    let guest = session(Some("user-1"), Some("guest"));
    assert!(matches!(
        service.admit(&contract, &guest),
        Err(CausalDispatchError::Forbidden)
    ));
    assert!(matches!(
        cell.admit(&contract, &guest),
        Err(CausalDispatchError::Forbidden)
    ));

    let admin = session(Some("user-1"), Some("admin"));
    assert!(service.admit(&contract, &admin).is_ok());
    assert!(cell.admit(&contract, &admin).is_ok());
    assert!(service.admit(&atomic, &admin).is_ok());
    assert!(matches!(
        cell.admit(&atomic, &admin),
        Err(CausalDispatchError::BadRequest(_))
    ));
}

/// A ledger whose recovery answers are scripted, to drive every branch of
/// the recovery decision.
struct ScriptedLedger {
    marks: Mutex<VecDeque<Result<(), CommandLedgerError>>>,
    lookup: Mutex<Option<Result<CommandLookup, CommandLedgerError>>>,
}

impl ScriptedLedger {
    fn new(
        marks: Vec<Result<(), CommandLedgerError>>,
        lookup: Option<Result<CommandLookup, CommandLedgerError>>,
    ) -> Self {
        Self {
            marks: Mutex::new(marks.into()),
            lookup: Mutex::new(lookup),
        }
    }
}

impl CommandLedgerStore for ScriptedLedger {
    fn reserve_command(
        &self,
        _reservation: CommandReservation,
    ) -> impl std::future::Future<
        Output = Result<crate::command_ledger::ReservationOutcome, CommandLedgerError>,
    > + Send
           + '_ {
        std::future::ready(Err(CommandLedgerError::Invalid(
            "the scripted ledger does not reserve".into(),
        )))
    }

    fn lookup_command<'a>(
        &'a self,
        _key: &'a CommandLedgerKey,
        _scope: CommandLookupScope<'a>,
    ) -> impl std::future::Future<Output = Result<CommandLookup, CommandLedgerError>> + Send + 'a
    {
        let answer = self
            .lookup
            .lock()
            .unwrap()
            .take()
            .expect("the scripted lookup is asked at most once");
        std::future::ready(answer)
    }

    fn mark_retryable_unknown(
        &self,
        _attempt: AttemptFence,
    ) -> impl std::future::Future<Output = Result<(), CommandLedgerError>> + Send + '_ {
        let answer = self
            .marks
            .lock()
            .unwrap()
            .pop_front()
            .expect("every scripted mark is answered");
        std::future::ready(answer)
    }

    fn complete_external_command(
        &self,
        _completion: crate::command_ledger::ExternalCommandCompletion,
    ) -> impl std::future::Future<Output = Result<(), CommandLedgerError>> + Send + '_ {
        std::future::ready(Err(CommandLedgerError::Invalid(
            "the scripted ledger does not complete".into(),
        )))
    }

    fn compact_expired_commands(
        &self,
        _limit: usize,
    ) -> impl std::future::Future<Output = Result<u64, CommandLedgerError>> + Send + '_ {
        std::future::ready(Ok(0))
    }
}

fn fenced() -> CommandLedgerError {
    CommandLedgerError::AttemptFenced {
        command_id: "fenced".into(),
    }
}

fn broken() -> CommandLedgerError {
    CommandLedgerError::Invalid("ledger unavailable".into())
}

enum Settled {
    /// The caller is told the error it handed in.
    Original,
    /// The attempt committed after all: its receipt, as a fresh result.
    Fresh,
    Expired,
    InProgress,
    /// An internal error starting with this detail.
    Internal(&'static str),
}

fn assert_settled(
    name: &str,
    outcome: Result<CausalDispatchResult, CausalDispatchError>,
    expected: &Settled,
) {
    match (expected, outcome) {
        (Settled::Original, Err(CausalDispatchError::BadRequest(message))) => {
            assert_eq!(message, "the original error", "{name}")
        }
        (Settled::Fresh, Ok(result)) => {
            assert!(!result.replayed(), "{name}: a recovered commit is fresh");
            assert_eq!(result.payload(), &json!({ "id": "todo-1" }), "{name}");
        }
        (Settled::Expired, Err(CausalDispatchError::Expired)) => {}
        (Settled::InProgress, Err(CausalDispatchError::InProgress)) => {}
        (Settled::Internal(detail), Err(CausalDispatchError::Internal(message))) => {
            assert!(message.contains(detail), "{name}: {message}")
        }
        (_, other) => panic!("{name}: unexpected outcome {other:?}"),
    }
}

#[tokio::test]
async fn an_abandoned_attempt_is_settled_by_what_the_ledger_holds() {
    struct Case {
        name: &'static str,
        marks: Vec<Result<(), CommandLedgerError>>,
        lookup: Option<Result<CommandLookup, CommandLedgerError>>,
        expected: Settled,
    }
    let in_progress = || {
        Some(Ok(CommandLookup::InProgress {
            causation_id: CausationId::new(),
        }))
    };
    let cases = vec![
        Case {
            name: "released: a retry runs again",
            marks: vec![Ok(())],
            lookup: None,
            expected: Settled::Original,
        },
        Case {
            name: "release fails",
            marks: vec![Err(broken())],
            lookup: None,
            expected: Settled::Internal("failed to mark command retryable"),
        },
        Case {
            name: "fenced, and the attempt committed",
            marks: vec![Err(fenced())],
            lookup: Some(Ok(CommandLookup::Replay(replay(
                CommandLedgerState::Succeeded,
                json!({ "id": "todo-1" }),
            )))),
            expected: Settled::Fresh,
        },
        Case {
            name: "fenced, and the row expired",
            marks: vec![Err(fenced())],
            lookup: Some(Ok(CommandLookup::Expired)),
            expected: Settled::Expired,
        },
        Case {
            name: "fenced, and already retryable",
            marks: vec![Err(fenced())],
            lookup: Some(Ok(CommandLookup::RetryableUnknown {
                causation_id: CausationId::new(),
            })),
            expected: Settled::Original,
        },
        Case {
            name: "fenced, still in progress, released on the second try",
            marks: vec![Err(fenced()), Ok(())],
            lookup: in_progress(),
            expected: Settled::Original,
        },
        Case {
            name: "fenced, in progress under a newer attempt",
            marks: vec![Err(fenced()), Err(fenced())],
            lookup: in_progress(),
            expected: Settled::InProgress,
        },
        Case {
            name: "fenced, in progress, and the second release fails",
            marks: vec![Err(fenced()), Err(broken())],
            lookup: in_progress(),
            expected: Settled::Internal("command recovery failed"),
        },
        Case {
            name: "fenced, and the row disappeared",
            marks: vec![Err(fenced())],
            lookup: Some(Ok(CommandLookup::Unknown)),
            expected: Settled::Internal("command ledger row disappeared"),
        },
        Case {
            name: "fenced, and the lookup fails",
            marks: vec![Err(fenced())],
            lookup: Some(Err(broken())),
            expected: Settled::Internal("command outcome lookup failed"),
        },
    ];
    let repository = InMemoryRepository::new();
    let ingress = CommandIngress::Service {
        service_id: "causal-unit-service",
        principal_partition: "principal".into(),
        command_id: "unused",
        policy: CausalCommandPolicy::default(),
        seal_modeled_projection: None,
    };
    for case in cases {
        let fence = acquire(&repository, None).await.fence();
        let ledger = ScriptedLedger::new(case.marks, case.lookup);
        let outcome = abandon_attempt(
            &ledger,
            &ingress,
            CommandConsistency::Succeeded,
            fence,
            CausalDispatchError::BadRequest("the original error".into()),
        )
        .await;
        assert_settled(case.name, outcome, &case.expected);
        assert!(
            ledger.marks.lock().unwrap().is_empty(),
            "{}: every scripted release was asked for",
            case.name
        );
    }
}

#[cfg(feature = "http")]
fn external_attempt(attempt: CommandAttempt) -> ExternalCausalAttempt {
    ExternalCausalAttempt {
        command_name: "causal.unit".into(),
        output: CausalUnitOutput::command_type(),
        attempt,
        retention: CausalCommandPolicy::default().replay_retention,
    }
}

#[cfg(feature = "http")]
fn external_binding() -> crate::command_ledger::ExternalDispatchBinding {
    crate::command_ledger::ExternalDispatchBinding::new("aggregate-cell", "todo-1").unwrap()
}

#[cfg(feature = "http")]
#[tokio::test]
async fn a_cell_receipt_completes_its_attempt_only_when_it_proves_the_same_terminal_command() {
    struct Case {
        name: &'static str,
        /// Builds the cell's answer for the attempt's command and causation.
        answer: fn(&str, &str) -> Value,
        accepted: bool,
        /// For a refusal: whether it is a client fault rather than internal.
        bad_request: bool,
    }
    let cases = [
        Case {
            name: "a succeeded receipt for the same command",
            answer: |command, causation| wait_path_body(command, causation, "succeeded"),
            accepted: true,
            bad_request: false,
        },
        Case {
            name: "a pending-projection receipt without its projection metadata",
            answer: |command, causation| {
                wait_path_body(command, causation, "succeeded_pending_projection")
            },
            accepted: false,
            bad_request: false,
        },
        Case {
            name: "another command's receipt",
            answer: |_, causation| {
                wait_path_body(new_command_id().as_str(), causation, "succeeded")
            },
            accepted: false,
            bad_request: false,
        },
        Case {
            name: "another causation",
            answer: |command, _| wait_path_body(command, CausationId::new().as_str(), "succeeded"),
            accepted: false,
            bad_request: false,
        },
        Case {
            name: "a payload that is not the declared output",
            answer: |command, causation| {
                let mut body = wait_path_body(command, causation, "succeeded");
                body["payload"] = json!({ "title": "no id" });
                body
            },
            accepted: false,
            bad_request: false,
        },
        Case {
            name: "an atomic receipt",
            answer: |command, causation| wait_path_body(command, causation, "atomic"),
            accepted: false,
            bad_request: true,
        },
        Case {
            name: "a receipt still in progress",
            answer: |command, causation| wait_path_body(command, causation, "in_progress"),
            accepted: false,
            bad_request: false,
        },
    ];
    let repository = InMemoryRepository::new();
    for case in cases {
        let attempt = external_attempt(acquire(&repository, Some(external_binding())).await);
        let result = CausalDispatchResult::from_wait_path_wire((case.answer)(
            attempt.command_id(),
            attempt.causation_id(),
        ))
        .unwrap();
        let key = attempt.fence().key().clone();
        match attempt.into_external_completion(&result) {
            Ok(completion) => {
                assert!(case.accepted, "{}: expected a refusal", case.name);
                CommandLedgerStore::complete_external_command(&repository, completion)
                    .await
                    .unwrap();
                let lookup = CommandLedgerStore::lookup_command(
                    &repository,
                    &key,
                    CommandLookupScope::CommandName("causal.unit"),
                )
                .await
                .unwrap();
                assert!(
                    matches!(&lookup, CommandLookup::Replay(replay) if replay.state.as_str() == result.state()),
                    "{}: the ledger holds the cell's terminal state",
                    case.name
                );
            }
            Err(CausalDispatchError::BadRequest(_)) => {
                assert!(!case.accepted && case.bad_request, "{}", case.name)
            }
            Err(CausalDispatchError::Internal(_)) => {
                assert!(!case.accepted && !case.bad_request, "{}", case.name)
            }
            Err(other) => panic!("{}: unexpected {other:?}", case.name),
        }
    }
}

#[cfg(feature = "http")]
#[tokio::test]
async fn an_abandoned_external_attempt_reports_whether_the_cell_settled_it() {
    struct Case {
        name: &'static str,
        marks: Vec<Result<(), CommandLedgerError>>,
        lookup: Option<Result<CommandLookup, CommandLedgerError>>,
        /// `None`: settled (Ok). `Some(detail)`: Internal with this detail.
        internal: Option<&'static str>,
        expired: bool,
    }
    let cases = vec![
        Case {
            name: "released",
            marks: vec![Ok(())],
            lookup: None,
            internal: Some("cell did not answer"),
            expired: false,
        },
        Case {
            name: "release fails",
            marks: vec![Err(broken())],
            lookup: None,
            internal: Some("failed to mark external command retryable"),
            expired: false,
        },
        Case {
            name: "fenced, and the cell's receipt is stored",
            marks: vec![Err(fenced())],
            lookup: Some(Ok(CommandLookup::Replay(replay(
                CommandLedgerState::Succeeded,
                json!({ "id": "todo-1" }),
            )))),
            internal: None,
            expired: false,
        },
        Case {
            name: "fenced, and expired",
            marks: vec![Err(fenced())],
            lookup: Some(Ok(CommandLookup::Expired)),
            internal: None,
            expired: true,
        },
        Case {
            name: "fenced, and retryable",
            marks: vec![Err(fenced())],
            lookup: Some(Ok(CommandLookup::RetryableUnknown {
                causation_id: CausationId::new(),
            })),
            internal: Some("cell did not answer"),
            expired: false,
        },
        Case {
            name: "fenced, and in progress",
            marks: vec![Err(fenced())],
            lookup: Some(Ok(CommandLookup::InProgress {
                causation_id: CausationId::new(),
            })),
            internal: Some("cell did not answer"),
            expired: false,
        },
        Case {
            name: "fenced, and gone",
            marks: vec![Err(fenced())],
            lookup: Some(Ok(CommandLookup::Unknown)),
            internal: Some("cell did not answer"),
            expired: false,
        },
        Case {
            name: "fenced, and the lookup fails",
            marks: vec![Err(fenced())],
            lookup: Some(Err(broken())),
            internal: Some("external command recovery failed"),
            expired: false,
        },
    ];
    let repository = InMemoryRepository::new();
    for case in cases {
        let attempt = external_attempt(acquire(&repository, Some(external_binding())).await);
        let ledger = ScriptedLedger::new(case.marks, case.lookup);
        let outcome =
            abandon_external_attempt(&ledger, attempt, "cell did not answer".into()).await;
        match (outcome, case.internal, case.expired) {
            (Ok(()), None, false) => {}
            (Err(CausalDispatchError::Expired), None, true) => {}
            (Err(CausalDispatchError::Internal(message)), Some(detail), false) => {
                assert!(message.contains(detail), "{}: {message}", case.name)
            }
            (other, _, _) => panic!("{}: unexpected {other:?}", case.name),
        }
    }
}

#[cfg(feature = "graphql")]
#[tokio::test]
async fn command_status_without_modeled_metadata_follows_the_ledger_state() {
    let repository = InMemoryRepository::new();
    let command_id = new_command_id();
    let status = |lookup| {
        evaluate_causal_command_status(
            &repository,
            &command_id,
            CommandConsistency::Succeeded,
            lookup,
            None,
        )
    };

    assert!(status(CommandLookup::Unknown).await.unwrap().is_unknown());
    let expired = status(CommandLookup::Expired).await.unwrap();
    assert_eq!(expired.state, CausalCommandPublicState::Expired);
    assert_eq!(expired.causation_id, None);

    for lookup in [
        CommandLookup::InProgress {
            causation_id: CausationId::new(),
        },
        CommandLookup::RetryableUnknown {
            causation_id: CausationId::new(),
        },
    ] {
        let status = status(lookup).await.unwrap();
        assert_eq!(status.state, CausalCommandPublicState::InProgress);
        assert!(status.causation_id.is_some());
        assert_eq!(status.outcome, None);
    }

    for (state, expected) in [
        (
            CommandLedgerState::Succeeded,
            CausalCommandPublicState::Succeeded,
        ),
        (CommandLedgerState::Atomic, CausalCommandPublicState::Atomic),
        (
            CommandLedgerState::Rejected,
            CausalCommandPublicState::Rejected,
        ),
        (
            CommandLedgerState::ProjectionFailed,
            CausalCommandPublicState::ProjectionFailed,
        ),
    ] {
        let stored = replay(state, json!({ "id": "todo-1" }));
        let causation = stored.causation_id.as_str().to_string();
        let status = status(CommandLookup::Replay(stored)).await.unwrap();
        assert_eq!(status.state, expected, "{}", state.as_str());
        assert_eq!(status.command_name.as_deref(), Some("causal.unit"));
        assert_eq!(status.causation_id.as_deref(), Some(causation.as_str()));
        assert_eq!(status.outcome, Some(json!({ "id": "todo-1" })));
        assert!(status.evidence.is_empty(), "{}", state.as_str());
    }

    for (state, detail) in [
        (
            CommandLedgerState::SucceededPendingProjection,
            "pending projection replay has no modeled projection metadata",
        ),
        (
            CommandLedgerState::InProgress,
            "stored replay has non-terminal state `in_progress`",
        ),
        (
            CommandLedgerState::Expired,
            "stored replay has non-terminal state `expired`",
        ),
    ] {
        match status(CommandLookup::Replay(replay(state, Value::Null))).await {
            Err(CausalDispatchError::Internal(message)) => {
                assert_eq!(message, detail, "{}", state.as_str())
            }
            other => panic!("{}: expected Internal, got {other:?}", state.as_str()),
        }
    }
}
