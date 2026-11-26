//! What `#[derive(DomainState)]` and `#[derive(DomainEvent)]` generate, checked
//! on real types: the frozen descriptors (a wire contract) and the projection
//! field metadata, held against what serde actually writes.

use distributed::domain_event::DomainEventContract;
use distributed::domain_event::{
    DomainEvent, DomainEventBodyKind, DomainEventDescriptor, DomainState, DOMAIN_EVENT_BODY_CODEC,
    DOMAIN_EVENT_BODY_CODEC_VERSION,
};
use distributed::projection::lower::{ProjectionBodyMetadata, ProjectionPortableType};
use serde::Serialize;

#[derive(Serialize, distributed_macros::DomainState)]
#[domain_state(version = 3)]
struct TodoState {
    todo_id: String,
    #[serde(rename = "is_done")]
    completed: bool,
}

#[derive(Serialize, distributed_macros::DomainEvent)]
#[domain_event(name = "todo.ownership-transferred", version = 2)]
struct TodoOwnershipTransferred {
    todo_id: String,
    previous_owner_id: String,
    new_owner_id: String,
}

#[derive(Serialize, distributed_macros::DomainState)]
#[domain_state(version = 1)]
#[serde(rename_all(serialize = "camelCase", deserialize = "snake_case"))]
struct OwnedTodoState {
    todo_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner_id: Option<String>,
}

#[test]
fn domain_state_descriptor_is_frozen_and_carries_no_event_name() {
    let descriptor = TodoState::DESCRIPTOR;
    assert_eq!(descriptor.type_name, "TodoState");
    assert_eq!(descriptor.version, 3);
    assert_eq!(
        descriptor.fingerprint,
        "sha256:fcae34456c0974737ca52bb2fef970712936e913791bea977d68d82ac88173a4"
    );
    assert_eq!(descriptor.codec, DOMAIN_EVENT_BODY_CODEC);
    assert_eq!(descriptor.codec_version, DOMAIN_EVENT_BODY_CODEC_VERSION);

    // The semantic name and version come from the event that uses the state.
    let event = DomainEventDescriptor::state::<TodoState>("todo.completed", 7);
    assert_eq!(event.name, "todo.completed");
    assert_eq!(event.version, 7);
    assert_eq!(event.body.kind, DomainEventBodyKind::State);
    assert_eq!(event.body.version, 3);
    assert_eq!(event.body.fingerprint, descriptor.fingerprint);
}

#[test]
fn domain_event_descriptor_is_frozen_and_matches_its_contract() {
    let descriptor = TodoOwnershipTransferred::DESCRIPTOR;
    assert_eq!(descriptor.name, "todo.ownership-transferred");
    assert_eq!(descriptor.version, 2);
    assert_eq!(descriptor.body.kind, DomainEventBodyKind::Event);
    assert_eq!(descriptor.body.type_name, "TodoOwnershipTransferred");
    assert_eq!(descriptor.body.version, 2);
    assert_eq!(
        descriptor.body.fingerprint,
        "sha256:c3f0bc1645b8685a8c496a74650ec41c70f9a8356e81d26cd254a4bd7a8ae9ac"
    );

    assert_eq!(
        <TodoOwnershipTransferred as DomainEventContract>::EVENT_NAME,
        "todo.ownership-transferred"
    );
    assert_eq!(
        <TodoOwnershipTransferred as DomainEventContract>::EVENT_VERSION,
        2
    );
    assert_eq!(
        <TodoOwnershipTransferred as DomainEventContract>::descriptor(),
        descriptor
    );
}

#[test]
fn projection_metadata_follows_serialize_names_and_conditional_presence() {
    let fields = <OwnedTodoState as ProjectionBodyMetadata>::PROJECTION_FIELDS;
    let summary: Vec<_> = fields
        .iter()
        .map(|field| {
            (
                field.rust_name,
                field.wire_name,
                field.portable_type,
                field.nullable,
                field.present,
                field.always_present,
            )
        })
        .collect();
    assert_eq!(
        summary,
        [
            (
                "todo_id",
                "todoId",
                ProjectionPortableType::String,
                false,
                true,
                true
            ),
            (
                "owner_id",
                "ownerId",
                ProjectionPortableType::String,
                true,
                true,
                false
            ),
        ]
    );

    // The metadata describes what serde writes: serialize-direction names, and
    // `ownerId` only when it has a value.
    let absent = serde_json::to_value(OwnedTodoState {
        todo_id: "t-1".into(),
        owner_id: None,
    })
    .unwrap();
    assert_eq!(absent, serde_json::json!({ "todoId": "t-1" }));
    let present = serde_json::to_value(OwnedTodoState {
        todo_id: "t-1".into(),
        owner_id: Some("u-1".into()),
    })
    .unwrap();
    assert_eq!(
        present,
        serde_json::json!({ "todoId": "t-1", "ownerId": "u-1" })
    );
}

#[test]
fn projection_metadata_uses_field_renames() {
    let fields = <TodoState as ProjectionBodyMetadata>::PROJECTION_FIELDS;
    let wire: Vec<_> = fields.iter().map(|field| field.wire_name).collect();
    assert_eq!(wire, ["todo_id", "is_done"]);

    let body = serde_json::to_value(TodoState {
        todo_id: "t-1".into(),
        completed: true,
    })
    .unwrap();
    let mut keys: Vec<_> = body.as_object().unwrap().keys().cloned().collect();
    keys.sort();
    assert_eq!(keys, ["is_done", "todo_id"]);
}
