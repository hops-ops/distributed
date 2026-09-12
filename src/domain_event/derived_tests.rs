use super::*;
use serde::Serialize;

#[derive(Clone, serde::Serialize, serde::Deserialize, crate::ReadModel)]
#[readmodel(table = "derived_rows", primary_key = ["bytes"])]
struct DerivedRows {
    bytes: u64,
}
#[allow(non_snake_case)]
fn SaveDerived() -> crate::Mutation<()> {
    crate::mutation_file!("tests/fixtures/derived_event_save.graphql")
}
use crate::projection::lower::{EventualOnly, ProjectionDescriptor};
crate::projection! {
    const DERIVED_ROWS: ProjectionDescriptor<EventualOnly> = {
        name: "derived-rows", version: 1, epoch: "derived-v1", model: DerivedRows,
        on { events: [Indexed], mutation: SaveDerived, input: { row: body }, },
    };
}
crate::projection! {
    const SNAPSHOT_ROWS: ProjectionDescriptor<EventualOnly> = {
        name: "derived-snapshot-rejection", version: 1, epoch: "snapshot-v1", model: DerivedRows,
        source: aggregate_snapshot,
        on { events: [Indexed], mutation: SaveDerived, input: { row: body }, },
    };
}

#[derive(Serialize)]
struct Indexed {
    bytes: u64,
}
impl DomainEventContract for Indexed {
    const EVENT_NAME: &'static str = "document.indexed";
    const EVENT_VERSION: u64 = 1;
    fn descriptor() -> DomainEventDescriptor {
        <Self as DomainEvent>::DESCRIPTOR.clone()
    }
}
impl DomainEvent for Indexed {
    const DESCRIPTOR: DomainEventDescriptor = DomainEventDescriptor {
        name: std::borrow::Cow::Borrowed("document.indexed"),
        version: 1,
        body: DomainEventBodyDescriptor::distributed_json(
            DomainEventBodyKind::Event,
            "Indexed",
            1,
            "indexed-v1",
            "sha256:1111111111111111111111111111111111111111111111111111111111111111",
        ),
    };
}
fn source() -> DomainEventOccurrence {
    let mut entity = crate::Entity::with_id("document-1");
    entity.set_causation_id("command-1");
    entity.set_correlation_id("request-1");
    entity.digest("document.changed", &()).unwrap();
    entity
        .capture_domain_event("document", &Indexed { bytes: 1 })
        .unwrap();
    entity.pending_domain_events()[0].clone()
}

#[test]
fn derived_retry_round_trip_preserves_source_provenance() {
    let source = source();
    let derived = source
        .derive("indexer", "summary", &Indexed { bytes: 10 })
        .unwrap();
    let repeated = source
        .derive("indexer", "summary", &Indexed { bytes: 10 })
        .unwrap();
    assert_eq!(derived, repeated);
    assert_eq!(
        derived.canonical_bytes().unwrap(),
        repeated.canonical_bytes().unwrap()
    );
    assert_eq!(derived.aggregate_id(), source.aggregate_id());
    assert_eq!(derived.aggregate_sequence(), source.aggregate_sequence());
    assert_eq!(derived.occurred_at_unix_ms(), source.occurred_at_unix_ms());
    assert_eq!(derived.causation_id(), source.causation_id());
    assert_eq!(derived.correlation_id(), source.correlation_id());
    assert_eq!(
        derived.derivation().unwrap().source_occurrence_id,
        source.id()
    );
    assert_eq!(
        DomainEventOccurrence::from_canonical_bytes(&derived.canonical_bytes().unwrap()).unwrap(),
        derived
    );
    assert!(source.derivation().is_none());
    assert_eq!(
        DomainEventOccurrence::from_canonical_bytes(&source.canonical_bytes().unwrap()).unwrap(),
        source
    );
    let nested = derived
        .derive("renderer", "html", &Indexed { bytes: 20 })
        .unwrap();
    assert_eq!(
        nested.derivation().unwrap().source_occurrence_id,
        derived.id()
    );
    assert_eq!(nested.aggregate_id(), source.aggregate_id());
}

#[test]
fn derived_identity_binds_producer_key_body_and_parent() {
    let source = source();
    let first = source
        .derive("indexer", "one", &Indexed { bytes: 10 })
        .unwrap();
    for other in [
        source
            .derive("other", "one", &Indexed { bytes: 10 })
            .unwrap(),
        source
            .derive("indexer", "two", &Indexed { bytes: 10 })
            .unwrap(),
        source
            .derive("indexer", "one", &Indexed { bytes: 11 })
            .unwrap(),
        first
            .derive("indexer", "one", &Indexed { bytes: 10 })
            .unwrap(),
    ] {
        assert_ne!(first.id(), other.id());
    }
}

#[test]
fn derived_keys_and_canonical_tampering_fail_closed() {
    let source = source();
    for key in ["".into(), " ".into(), "a\nb".into(), "x".repeat(1025)] {
        assert!(source
            .derive("indexer", &key, &Indexed { bytes: 1 })
            .is_err());
    }
    assert!(source.derive("", "key", &Indexed { bytes: 1 }).is_err());
    let derived = source
        .derive("indexer", "one", &Indexed { bytes: 10 })
        .unwrap();
    for (pointer, value) in [
        ("/derivation/output_key", serde_json::json!("changed")),
        ("/aggregate_sequence", serde_json::json!(10)),
        ("/occurred_at_unix_ms", serde_json::json!(0)),
        ("/body", serde_json::json!("e30=")),
    ] {
        let mut wire = serde_json::to_value(&derived).unwrap();
        *wire.pointer_mut(pointer).unwrap() = value;
        assert!(
            DomainEventOccurrence::from_canonical_bytes(&canonical_json_bytes(&wire).unwrap())
                .is_err()
        );
    }
}

#[test]
fn derived_facts_are_not_aggregate_snapshots() {
    let source = source();
    let derived = source
        .derive("indexer", "one", &Indexed { bytes: 10 })
        .unwrap();
    assert!(crate::projection_protocol::SourceSnapshotVersion::from_occurrence(&source).is_ok());
    assert!(crate::projection_protocol::SourceSnapshotVersion::from_occurrence(&derived).is_err());
    assert_eq!(
        DERIVED_ROWS
            .server_executor()
            .unwrap()
            .plan(&derived)
            .unwrap()
            .write_plan
            .mutations
            .len(),
        1
    );
    assert!(SNAPSHOT_ROWS
        .server_executor()
        .unwrap()
        .plan(&derived)
        .is_err());
    assert!(SNAPSHOT_ROWS
        .server_executor()
        .unwrap()
        .plan(&source)
        .is_ok());
}
