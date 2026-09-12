//! Typed outward domain events captured separately from aggregate replay bytes.
//!
//! A [`DomainEventOccurrence`] owns canonical bytes at transition time. Aggregate
//! replay suppresses capture, failed persistence leaves pending occurrences
//! untouched, and only explicit successful persistence clears them.

mod canonical;
#[cfg(test)]
mod derived_tests;
mod descriptor;
mod occurrence;

pub use canonical::{
    DOMAIN_EVENT_BODY_CODEC, DOMAIN_EVENT_BODY_CODEC_VERSION, MAX_DOMAIN_EVENT_BODY_BYTES,
    MAX_DOMAIN_EVENT_OCCURRENCE_WIRE_BYTES,
};
pub use descriptor::{
    DomainDeletion, DomainDeletionError, DomainEvent, DomainEventBodyContract,
    DomainEventBodyDescriptor, DomainEventBodyKind, DomainEventContract, DomainEventDescriptor,
    DomainState, DomainStateDescriptor,
};
pub use occurrence::{
    DomainEventCaptureError, DomainEventCaptureOutcome, DomainEventCapturePoison,
    DomainEventCommitGuardError, DomainEventDerivation, DomainEventEnvelope, DomainEventOccurrence,
    DOMAIN_EVENT_OCCURRENCE_VERSION,
};

pub(crate) use canonical::canonical_json_bytes;
pub(crate) use occurrence::state_descriptor_matches;

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::collections::BTreeMap;
    use std::time::{Duration, UNIX_EPOCH};

    use serde::ser::Error as _;
    use serde::{Deserialize, Serialize};
    use sha2::{Digest, Sha256};

    use super::*;
    use crate::bus::{MAX_MESSAGE_NAME_LEN, MAX_STABLE_MESSAGE_ID_LEN};
    use crate::Entity;

    const STATE_FINGERPRINT: &str =
        "sha256:1111111111111111111111111111111111111111111111111111111111111111";
    const EVENT_FINGERPRINT: &str =
        "sha256:2222222222222222222222222222222222222222222222222222222222222222";
    const DELETE_FINGERPRINT: &str =
        "sha256:3333333333333333333333333333333333333333333333333333333333333333";

    #[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
    struct TodoState {
        todo_id: String,
        status: String,
    }

    impl TodoState {
        fn new(status: &str) -> Self {
            Self {
                todo_id: "todo-1".into(),
                status: status.into(),
            }
        }
    }

    impl DomainState for TodoState {
        const DESCRIPTOR: DomainStateDescriptor = DomainStateDescriptor::distributed_json(
            "TodoState",
            3,
            "todo-state-v3",
            STATE_FINGERPRINT,
        );
    }

    #[derive(Serialize)]
    struct TodoRenamed {
        title: String,
    }

    impl DomainEvent for TodoRenamed {
        const DESCRIPTOR: DomainEventDescriptor = DomainEventDescriptor {
            name: Cow::Borrowed("todo.renamed"),
            version: 2,
            body: DomainEventBodyDescriptor::distributed_json(
                DomainEventBodyKind::Event,
                "TodoRenamed",
                4,
                "todo-renamed-v4",
                EVENT_FINGERPRINT,
            ),
        };
    }

    struct FailingState;

    impl Serialize for FailingState {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(S::Error::custom("intentional domain-state failure"))
        }
    }

    impl DomainState for FailingState {
        const DESCRIPTOR: DomainStateDescriptor = DomainStateDescriptor::distributed_json(
            "FailingState",
            1,
            "failing-state-v1",
            STATE_FINGERPRINT,
        );
    }

    fn state_event(name: &'static str) -> DomainEventDescriptor {
        DomainEventDescriptor::state::<TodoState>(name, 1)
    }

    fn deletion_event(name: impl Into<Cow<'static, str>>) -> DomainEventDescriptor {
        DomainEventDescriptor {
            name: name.into(),
            version: 1,
            body: DomainEventBodyDescriptor::distributed_json(
                DomainEventBodyKind::Deletion,
                "DomainDeletion<String>",
                1,
                "domain-deletion-string-v1",
                DELETE_FINGERPRINT,
            ),
        }
    }

    fn fixed_envelope(aggregate_id: impl Into<String>) -> DomainEventEnvelope {
        DomainEventEnvelope {
            aggregate_type: "todo".into(),
            aggregate_id: aggregate_id.into(),
            aggregate_sequence: 7,
            publication_ordinal: 0,
            occurred_at: UNIX_EPOCH + Duration::from_millis(1_700_000_000_123),
            metadata: BTreeMap::from([
                ("causation_id".into(), "cmd-7".into()),
                ("correlation_id".into(), "request-4".into()),
                (
                    "traceparent".into(),
                    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into(),
                ),
            ]),
        }
    }

    #[test]
    fn transitions_retain_intermediate_and_final_state_occurrences_in_order() {
        let mut entity = Entity::with_id("todo-1");
        entity.set_correlation_id("request-1");
        entity.digest("todo.created", &()).unwrap();
        entity
            .capture_domain_state("todo", state_event("todo.created"), &TodoState::new("open"))
            .unwrap();
        entity.set_correlation_id("request-2");
        entity.digest("todo.completed", &()).unwrap();
        entity
            .capture_domain_state(
                "todo",
                state_event("todo.completed"),
                &TodoState::new("completed"),
            )
            .unwrap();

        let states = entity
            .pending_domain_events()
            .iter()
            .map(|occurrence| occurrence.decode_body::<TodoState>().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            states,
            vec![TodoState::new("open"), TodoState::new("completed")]
        );
        assert_eq!(
            entity
                .pending_domain_events()
                .iter()
                .map(DomainEventOccurrence::aggregate_sequence)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn occurrence_uses_the_causing_replay_records_timestamp_and_metadata() {
        let mut entity = Entity::with_id("todo-1");
        entity.set_correlation_id("request-1");
        entity.digest("todo.completed", &()).unwrap();
        let causing_timestamp = entity.events()[0].timestamp;
        entity.set_correlation_id("changed-after-transition");

        entity
            .capture_domain_state(
                "todo",
                state_event("todo.completed"),
                &TodoState::new("completed"),
            )
            .unwrap();

        let occurrence = &entity.pending_domain_events()[0];
        let expected_timestamp: u64 = causing_timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .try_into()
            .unwrap();
        assert_eq!(occurrence.occurred_at_unix_ms(), expected_timestamp);
        assert_eq!(occurrence.correlation_id(), Some("request-1"));
    }

    #[test]
    fn authoritative_causal_stamp_updates_replay_and_outward_occurrences_together() {
        let mut entity = Entity::with_id("todo-1");
        entity.set_causation_id("handler-value");
        entity.digest("todo.completed", &()).unwrap();
        entity
            .capture_domain_state(
                "todo",
                state_event("todo.completed"),
                &TodoState::new("completed"),
            )
            .unwrap();

        entity.overwrite_new_event_causation_id("ledger-causation");

        assert_eq!(entity.events()[0].causation_id(), Some("ledger-causation"));
        assert_eq!(
            entity.pending_domain_events()[0].causation_id(),
            Some("ledger-causation")
        );
    }

    #[test]
    fn replay_suppresses_outward_capture_without_poisoning_the_entity() {
        let mut entity = Entity::with_id("todo-1");
        entity.set_replaying(true);

        let outcome = entity
            .capture_domain_state(
                "todo",
                state_event("todo.completed"),
                &TodoState::new("completed"),
            )
            .unwrap();

        assert_eq!(outcome, DomainEventCaptureOutcome::SuppressedDuringReplay);
        assert!(entity.pending_domain_events().is_empty());
        assert!(entity.domain_event_poison().is_none());
    }

    #[test]
    fn retry_identity_is_stable_while_repeated_publication_ordinal_is_distinct() {
        fn captured() -> Entity {
            let mut entity = Entity::with_id("todo-1");
            entity.digest("todo.completed", &()).unwrap();
            entity
                .capture_domain_state(
                    "todo",
                    state_event("todo.completed"),
                    &TodoState::new("completed"),
                )
                .unwrap();
            entity
                .capture_domain_state(
                    "todo",
                    state_event("todo.completed"),
                    &TodoState::new("completed"),
                )
                .unwrap();
            entity
        }

        let first = captured();
        let retry = captured();
        assert_eq!(
            first.pending_domain_events()[0].id(),
            retry.pending_domain_events()[0].id()
        );
        assert_ne!(
            first.pending_domain_events()[0].id(),
            first.pending_domain_events()[1].id()
        );
        assert_eq!(first.pending_domain_events()[1].publication_ordinal(), 1);
    }

    #[test]
    fn serialization_failure_poison_blocks_manual_commit_guard() {
        let mut entity = Entity::with_id("todo-1");
        entity.digest("todo.failed", &()).unwrap();
        let descriptor = FailingState::DESCRIPTOR.clone().event("todo.failed", 1);

        let error = entity
            .capture_domain_state("todo", descriptor, &FailingState)
            .unwrap_err();
        let guard = entity.domain_event_commit_guard().unwrap_err();

        assert!(matches!(error, DomainEventCaptureError::BodyEncoding(_)));
        assert_eq!(guard.poison().error, error);
        assert!(entity.pending_domain_events().is_empty());
    }

    #[test]
    fn failed_persistence_retains_bytes_and_successful_persistence_clears_them() {
        fn persist_fails(_occurrences: &[DomainEventOccurrence]) -> Result<(), &'static str> {
            Err("injected persistence failure")
        }

        let mut entity = Entity::with_id("todo-1");
        entity.digest("todo.renamed", &()).unwrap();
        entity
            .capture_domain_event(
                "todo",
                &TodoRenamed {
                    title: "new".into(),
                },
            )
            .unwrap();
        let before_failure = entity.pending_domain_events_for_commit().unwrap()[0]
            .canonical_bytes()
            .unwrap();

        assert_eq!(
            persist_fails(entity.pending_domain_events_for_commit().unwrap()),
            Err("injected persistence failure")
        );
        let after_failure = entity.pending_domain_events_for_commit().unwrap()[0]
            .canonical_bytes()
            .unwrap();
        assert_eq!(after_failure, before_failure);

        entity.mark_domain_events_committed().unwrap();
        assert!(entity.pending_domain_events().is_empty());
    }

    #[test]
    fn deletion_body_requires_nonzero_incarnation_and_explicit_deletion_kind() {
        assert_eq!(
            DomainDeletion::new("todo-1", 0).unwrap_err(),
            DomainDeletionError
        );
        let deletion = DomainDeletion::new("todo-1", 3).unwrap();
        let mut entity = Entity::with_id("todo-1");
        entity.digest("todo.purged", &()).unwrap();
        entity
            .capture_domain_deletion("todo", deletion_event("todo.purged"), &deletion)
            .unwrap();

        assert_eq!(
            entity.pending_domain_events()[0].descriptor().body.kind,
            DomainEventBodyKind::Deletion
        );
    }

    #[test]
    fn message_name_and_aggregate_id_limits_accept_boundary_values() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/domain-event-occurrence-v1.json"
        ))
        .unwrap();
        let message_name_bytes = fixture["boundary_vectors"]["message_name"]["at_bytes"]
            .as_u64()
            .unwrap() as usize;
        let stable_id_bytes = fixture["boundary_vectors"]["stable_id"]["at_bytes"]
            .as_u64()
            .unwrap() as usize;
        let mut envelope = fixed_envelope("i".repeat(stable_id_bytes));
        envelope.aggregate_type = "a".repeat(message_name_bytes);
        let mut descriptor = state_event("todo.completed");
        descriptor.name = Cow::Owned("e".repeat(message_name_bytes));

        let occurrence =
            DomainEventOccurrence::capture(descriptor, envelope, &TodoState::new("open"));

        assert_eq!(message_name_bytes, MAX_MESSAGE_NAME_LEN);
        assert_eq!(stable_id_bytes, MAX_STABLE_MESSAGE_ID_LEN);
        assert_eq!(
            fixture["boundary_vectors"]["message_name"]["at_result"],
            "accepted"
        );
        assert_eq!(
            fixture["boundary_vectors"]["stable_id"]["at_result"],
            "accepted"
        );
        let occurrence = occurrence.expect("boundary values must be accepted");
        assert!(occurrence.id().len() <= MAX_STABLE_MESSAGE_ID_LEN);
    }

    #[test]
    fn message_name_and_aggregate_id_limits_reject_over_limit_values() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/domain-event-occurrence-v1.json"
        ))
        .unwrap();
        let message_name_bytes = fixture["boundary_vectors"]["message_name"]["over_bytes"]
            .as_u64()
            .unwrap() as usize;
        let stable_id_bytes = fixture["boundary_vectors"]["stable_id"]["over_bytes"]
            .as_u64()
            .unwrap() as usize;
        let descriptor = DomainEventDescriptor {
            name: Cow::Owned("e".repeat(message_name_bytes)),
            ..state_event("todo.completed")
        };
        let name_error = DomainEventOccurrence::capture(
            descriptor,
            fixed_envelope("todo-1"),
            &TodoState::new("open"),
        )
        .unwrap_err();
        let id_error = DomainEventOccurrence::capture(
            state_event("todo.completed"),
            fixed_envelope("i".repeat(stable_id_bytes)),
            &TodoState::new("open"),
        )
        .unwrap_err();

        assert_eq!(message_name_bytes, MAX_MESSAGE_NAME_LEN + 1);
        assert_eq!(stable_id_bytes, MAX_STABLE_MESSAGE_ID_LEN + 1);
        assert_eq!(
            fixture["boundary_vectors"]["message_name"]["over_result"],
            "event_name_too_long"
        );
        assert_eq!(
            fixture["boundary_vectors"]["stable_id"]["over_result"],
            "aggregate_id_too_long"
        );
        assert!(matches!(
            name_error,
            DomainEventCaptureError::EventName(crate::bus::MessageNameError::TooLong { .. })
        ));
        assert!(matches!(
            id_error,
            DomainEventCaptureError::AggregateId(crate::bus::StableMessageIdError::TooLong { .. })
        ));
    }

    #[test]
    fn canonical_body_limit_accepts_exactly_one_mib() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/domain-event-occurrence-v1.json"
        ))
        .unwrap();
        let body_bytes = fixture["boundary_vectors"]["body"]["at_bytes"]
            .as_u64()
            .unwrap() as usize;
        let body = "a".repeat(body_bytes - 2);
        let occurrence = DomainEventOccurrence::capture(
            state_event("todo.completed"),
            fixed_envelope("todo-1"),
            &body,
        )
        .unwrap();

        assert_eq!(body_bytes, MAX_DOMAIN_EVENT_BODY_BYTES);
        assert_eq!(fixture["boundary_vectors"]["body"]["at_result"], "accepted");
        assert_eq!(occurrence.body_bytes().len(), body_bytes);
    }

    #[test]
    fn canonical_body_limit_rejects_one_byte_over_one_mib() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/domain-event-occurrence-v1.json"
        ))
        .unwrap();
        let body_bytes = fixture["boundary_vectors"]["body"]["over_bytes"]
            .as_u64()
            .unwrap() as usize;
        let body = "a".repeat(body_bytes - 2);
        let error = DomainEventOccurrence::capture(
            state_event("todo.completed"),
            fixed_envelope("todo-1"),
            &body,
        )
        .unwrap_err();

        assert_eq!(
            fixture["boundary_vectors"]["body"]["over_result"],
            "body_too_large"
        );
        assert_eq!(
            error,
            DomainEventCaptureError::BodyTooLarge { len: body_bytes }
        );
    }

    #[test]
    fn canonical_fixture_round_trips_and_fingerprints_exact_wire_bytes() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/domain-event-occurrence-v1.json"
        ))
        .unwrap();
        let occurrence = DomainEventOccurrence::capture(
            state_event("todo.completed"),
            fixed_envelope("todo-1"),
            &TodoState::new("completed"),
        )
        .unwrap();
        let canonical = occurrence.canonical_bytes().unwrap();
        let actual_value: serde_json::Value = serde_json::from_slice(&canonical).unwrap();
        let digest = Sha256::digest(&canonical);
        let actual_digest = format!("sha256:{digest:x}");

        assert_eq!(actual_value, fixture["occurrence"]);
        assert_eq!(actual_digest, fixture["canonical_occurrence_fingerprint"]);
        assert_eq!(
            std::str::from_utf8(occurrence.body_bytes()).unwrap(),
            fixture["canonical_body"].as_str().unwrap()
        );
        assert_eq!(
            DomainEventOccurrence::from_canonical_bytes(&canonical).unwrap(),
            occurrence
        );
        assert_eq!(
            fixture["limits"]["body_bytes"].as_u64().unwrap() as usize,
            MAX_DOMAIN_EVENT_BODY_BYTES
        );
        assert_eq!(
            fixture["limits"]["occurrence_wire_bytes"].as_u64().unwrap() as usize,
            MAX_DOMAIN_EVENT_OCCURRENCE_WIRE_BYTES
        );
    }
}
