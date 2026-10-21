use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::authorization::{
    ProjectionPartitionAuthority, ProjectionPartitionScopeEncoder, ProjectionVisibilityEvaluator,
};
use super::canonical::{canonicalize_operations, canonicalize_recoveries};
use super::lower::ProjectionDeltaRequestAuthority;
use super::types::{
    AuthorizationTransition, ProjectionDeltaCacheScopeToken, ProjectionDeltaVisibility,
    ProjectionMutationSource, MAX_PROJECTION_DELTA_OPERATIONS,
};
use super::*;
use crate::graphql::client_manifest::{
    ClientCapabilities, ClientExecutionLimits, ClientProtocolOperations, ClientSurfaceIdentity,
    DistributedClientManifest, DISTRIBUTED_CLIENT_MANIFEST_VERSION,
    DISTRIBUTED_CLIENT_PROTOCOL_VERSION,
};
use crate::{
    ResolvedProjectionMutation, ResolvedProjectionPartition, ResolvedProjectionRelationshipEffect,
    MAX_DOMAIN_EVENT_BODY_BYTES, MAX_PROJECTION_EXPRESSION_DEPTH,
};

const CACHE_SCOPE_TOKEN: &str = "v1.cache-scope.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const PARTITION_TOKEN: &str = "v1.projection-partition.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const CAUSATION_ID: &str = "0190a000-0000-7000-8000-000000000017";
const GOLDEN_SHA256: &str =
    "sha256:7bdc06e1d3accc4c62132f967df1310d31f2f3b856fa7e97a7c5d0907a4ae17b";
const FORBIDDEN_ARTIFACT_TEXT: &[&str] = &[
    "raw_event_body",
    "owner-secret",
    "denied-value",
    "physical_table",
    "join_table",
    "memberships_physical",
    "authorization-secret",
];

#[test]
fn projection_delta_v1_matches_cross_language_golden_bytes_and_fingerprint() {
    let expected = golden_delta().canonical_bytes().unwrap();
    let fixture = include_bytes!("../../../tests/fixtures/projection-delta-v1.json");
    let fixture = fixture.strip_suffix(b"\n").unwrap_or(fixture);

    assert_eq!(fixture, expected);
    assert_eq!(
        format!("sha256:{:x}", Sha256::digest(fixture)),
        GOLDEN_SHA256
    );
    assert_eq!(ProjectionDelta::from_json(fixture).unwrap(), golden_delta());
}

#[test]
fn golden_vector_preserves_presence_masks_relationships_and_numeric_bounds() {
    let delta = golden_delta();
    let upsert = mutation(&delta, |mutation| {
        matches!(mutation, ProjectionDeltaMutation::Upsert { .. })
    });
    let ProjectionDeltaMutation::Upsert {
        scope,
        fields,
        replace,
    } = upsert
    else {
        unreachable!("selected mutation is an upsert")
    };
    assert_eq!(
        scope
            .key
            .iter()
            .map(|field| (field.ordinal, field.field.as_str()))
            .collect::<Vec<_>>(),
        vec![(0, "z_tenant_id"), (1, "a_user_id")]
    );
    assert_eq!(
        replace,
        &vec![
            "archived_at".to_owned(),
            "metrics".to_owned(),
            "nullable_note".to_owned(),
            "title".to_owned(),
        ]
    );
    assert!(fields.iter().all(|field| field.field != "archived_at"));
    assert!(fields
        .iter()
        .any(|field| field.field == "nullable_note" && field.value == DeltaValue::Null));
    assert!(fields.iter().any(|field| {
        field.field == "title" && field.value == DeltaValue::String("Résumé 🚀".to_owned())
    }));

    let patch = mutation(&delta, |mutation| {
        matches!(mutation, ProjectionDeltaMutation::Patch { .. })
    });
    let ProjectionDeltaMutation::Patch {
        set,
        unset,
        if_present,
        ..
    } = patch
    else {
        unreachable!("selected mutation is a patch")
    };
    assert!(*if_present);
    assert_eq!(unset, &vec!["legacy_title".to_owned()]);
    assert!(set.iter().all(|field| field.field != "absent"));
    assert!(delta.recoveries.iter().any(|recovery| {
        recovery.condition == ProjectionDeltaRecoveryCondition::IfRecordMissing
            && matches!(
                recovery.target,
                ProjectionDeltaRecoveryTarget::Record { .. }
            )
    }));

    assert!(delta
        .operations
        .iter()
        .any(|operation| { matches!(operation.mutation, ProjectionDeltaMutation::Delete { .. }) }));
    assert!(delta
        .operations
        .iter()
        .any(|operation| { matches!(operation.mutation, ProjectionDeltaMutation::Link { .. }) }));
    assert!(delta
        .operations
        .iter()
        .any(|operation| { matches!(operation.mutation, ProjectionDeltaMutation::Unlink { .. }) }));
    assert!(delta.operations.iter().any(|operation| {
        matches!(
            operation.mutation,
            ProjectionDeltaMutation::InvalidateModel { .. }
        )
    }));
    assert!(delta.operations.iter().any(|operation| {
        matches!(
            operation.mutation,
            ProjectionDeltaMutation::InvalidateRelationship { .. }
        )
    }));
}

#[test]
fn golden_vector_exposes_logical_fk_and_join_edges_without_server_internals() {
    let bytes = golden_delta().canonical_bytes().unwrap();
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(text.contains("\"relationship\":\"owner\""));
    assert!(text.contains("\"relationship\":\"members\""));
    for forbidden in FORBIDDEN_ARTIFACT_TEXT {
        assert!(!text.contains(forbidden), "leaked `{forbidden}`");
    }
    for forbidden_key in [
        "\"storage\"",
        "\"table\"",
        "\"event_body\"",
        "\"authorization\"",
        "\"physical_plan\"",
    ] {
        assert!(!text.contains(forbidden_key), "leaked `{forbidden_key}`");
    }
}

#[test]
fn canonical_netting_retains_ordered_occurrences_and_one_final_mutation_per_scope() {
    let scope = scope("Todos", vec![key(0, "todo_id", "todo-1")]);
    let operations = vec![
        operation(
            0,
            vec![0],
            ProjectionDeltaMutation::Upsert {
                scope: scope.clone(),
                fields: vec![
                    field("status", DeltaValue::String("open".to_owned())),
                    field("title", DeltaValue::String("first".to_owned())),
                ],
                replace: vec!["status".to_owned(), "title".to_owned()],
            },
        ),
        operation(
            1,
            vec![1],
            ProjectionDeltaMutation::Patch {
                scope: scope.clone(),
                set: vec![field("title", DeltaValue::String("second".to_owned()))],
                unset: vec!["status".to_owned()],
                if_present: true,
            },
        ),
        operation(
            2,
            vec![0],
            ProjectionDeltaMutation::Delete {
                scope: scope.clone(),
            },
        ),
        operation(
            3,
            vec![1],
            ProjectionDeltaMutation::Upsert {
                scope,
                fields: vec![field("title", DeltaValue::String("final".to_owned()))],
                replace: vec!["status".to_owned(), "title".to_owned()],
            },
        ),
    ];
    let canonical = canonicalize_operations(operations).unwrap();

    assert_eq!(canonical.len(), 1);
    assert_eq!(canonical[0].occurrence_ordinal, 3);
    assert_eq!(canonical[0].projection_refs, vec![0, 1]);
    assert!(matches!(
        &canonical[0].mutation,
        ProjectionDeltaMutation::Upsert { fields, replace, .. }
            if fields == &vec![field("title", DeltaValue::String("final".to_owned()))]
                && replace == &vec!["status".to_owned(), "title".to_owned()]
    ));
}

#[test]
fn zero_actual_occurrences_is_an_authoritative_empty_delta() {
    let delta = ProjectionDelta {
        wire_version: PROJECTION_DELTA_WIRE_VERSION,
        identity: identity(),
        projections: vec![],
        occurrences: vec![],
        operations: vec![],
        recoveries: vec![],
    };
    let bytes = delta.canonical_bytes().unwrap();

    assert_eq!(ProjectionDelta::from_json(&bytes).unwrap(), delta);
    assert_eq!(
        String::from_utf8(bytes).unwrap(),
        format!(
            "{{\"wire_version\":1,\"identity\":{},\"projections\":[],\"occurrences\":[],\"operations\":[]}}",
            serde_json::to_string(&identity()).unwrap()
        )
    );
}

#[test]
fn strict_wire_decode_rejects_unknown_versions_fields_and_duplicate_sources() {
    let golden = golden_delta();
    let mut value = serde_json::to_value(&golden).unwrap();
    value["unknown"] = json!(true);
    assert_invalid_wire(&value);

    let mut value = serde_json::to_value(&golden).unwrap();
    value["identity"]["unknown"] = json!("denied");
    assert_invalid_wire(&value);

    let mut value = serde_json::to_value(&golden).unwrap();
    value["operations"][0]["mutation"]["unknown"] = json!("physical");
    assert_invalid_wire(&value);

    let mut value = serde_json::to_value(&golden).unwrap();
    value["wire_version"] = json!(2);
    assert!(matches!(
        ProjectionDelta::from_json(&serde_json::to_vec(&value).unwrap()),
        Err(ProjectionDeltaError::UnsupportedVersion { actual: 2 })
    ));

    let mut value = serde_json::to_value(&golden).unwrap();
    value["identity"]["manifest_version"] = json!(3);
    assert!(matches!(
        ProjectionDelta::from_json(&serde_json::to_vec(&value).unwrap()),
        Err(ProjectionDeltaError::UnsupportedClientVersion {
            field: "manifest_version",
            actual: 3
        })
    ));

    let mut value = serde_json::to_value(&golden).unwrap();
    value["projections"][0]["program_ir_version"] = json!(2);
    assert!(matches!(
        ProjectionDelta::from_json(&serde_json::to_vec(&value).unwrap()),
        Err(ProjectionDeltaError::UnsupportedExecutableVersion {
            field: "program_ir_version",
            actual: 2
        })
    ));

    let mut duplicate_occurrence = golden.clone();
    duplicate_occurrence.occurrences[1].occurrence_id =
        duplicate_occurrence.occurrences[0].occurrence_id.clone();
    assert!(matches!(
        duplicate_occurrence.canonical_bytes(),
        Err(ProjectionDeltaError::InvalidOperation(
            "occurrence IDs must be unique"
        ))
    ));

    let mut duplicate_projection = golden;
    duplicate_projection.projections[1] = duplicate_projection.projections[0].clone();
    assert!(matches!(
        duplicate_projection.canonical_bytes(),
        Err(ProjectionDeltaError::NonCanonicalOrder {
            field: "projections"
        })
    ));
}

#[test]
fn strict_wire_decode_rejects_duplicate_recovery_targets_even_with_distinct_ordinals() {
    let mut delta = golden_delta();
    let duplicate = ProjectionDeltaRecovery {
        occurrence_ordinal: 5,
        projection_refs: vec![1],
        condition: ProjectionDeltaRecoveryCondition::Always,
        target: delta.recoveries[0].target.clone(),
    };
    delta.recoveries.push(duplicate);
    delta
        .recoveries
        .sort_by_key(ProjectionDeltaRecovery::canonical_order);
    let bytes = serde_json::to_vec(&delta).unwrap();

    assert!(matches!(
        ProjectionDelta::from_json(&bytes),
        Err(ProjectionDeltaError::DuplicateScope)
    ));
}

#[test]
fn replay_scope_fences_surface_cache_generation_causation_and_fingerprints() {
    let delta = golden_delta();
    let manifest = replay_manifest();
    let request = ReplayRequest::new("generation-7", CACHE_SCOPE_TOKEN, CAUSATION_ID);
    delta.validate_replay_scope(&manifest, &request).unwrap();

    let mutations = [
        ReplayMutation::Surface,
        ReplayMutation::CacheScope,
        ReplayMutation::AuthorizationGeneration,
        ReplayMutation::Causation,
        ReplayMutation::SchemaFingerprint,
        ReplayMutation::ProtocolFingerprint,
    ];
    for mutation in mutations {
        let (mut changed_delta, changed_manifest, changed_request) =
            (delta.clone(), manifest.clone(), request.clone());
        let (changed_manifest, changed_request) = match mutation {
            ReplayMutation::Surface => {
                changed_delta.identity.surface = ProjectionDeltaSurfaceIdentity::Role {
                    name: "other-role".to_owned(),
                };
                (changed_manifest, changed_request)
            }
            ReplayMutation::CacheScope => (
                changed_manifest,
                ReplayRequest::new("generation-7", alternate_cache_scope(), CAUSATION_ID),
            ),
            ReplayMutation::AuthorizationGeneration => (
                changed_manifest,
                ReplayRequest::new("generation-8", CACHE_SCOPE_TOKEN, CAUSATION_ID),
            ),
            ReplayMutation::Causation => (
                changed_manifest,
                ReplayRequest::new(
                    "generation-7",
                    CACHE_SCOPE_TOKEN,
                    "0190a000-0000-7000-8000-000000000018",
                ),
            ),
            ReplayMutation::SchemaFingerprint => {
                changed_delta.identity.schema_fingerprint = "sha256:other-schema".to_owned();
                (changed_manifest, changed_request)
            }
            ReplayMutation::ProtocolFingerprint => {
                changed_delta.identity.protocol_fingerprint = "sha256:other-protocol".to_owned();
                (changed_manifest, changed_request)
            }
        };
        assert_eq!(
            changed_delta.validate_replay_scope(&changed_manifest, &changed_request),
            Err(ProjectionDeltaError::ReplayScopeMismatch),
            "{mutation:?} must be fenced"
        );
    }
}

#[test]
fn numeric_values_accept_exact_bounds_and_reject_noncanonical_or_out_of_range_forms() {
    for value in [
        DeltaValue::I64(i64::MIN.to_string()),
        DeltaValue::I64(i64::MAX.to_string()),
        DeltaValue::U64(u64::MIN.to_string()),
        DeltaValue::U64(u64::MAX.to_string()),
        DeltaValue::F64("1.5".to_owned()),
        DeltaValue::F64("0.0".to_owned()),
        DeltaValue::F64("1.0".to_owned()),
    ] {
        value.validate().unwrap();
    }
    for value in [
        DeltaValue::I64("-9223372036854775809".to_owned()),
        DeltaValue::I64("01".to_owned()),
        DeltaValue::U64("18446744073709551616".to_owned()),
        DeltaValue::U64("+1".to_owned()),
        DeltaValue::F64("-0".to_owned()),
        DeltaValue::F64("NaN".to_owned()),
        DeltaValue::F64("inf".to_owned()),
    ] {
        assert_eq!(value.validate(), Err(ProjectionDeltaError::InvalidNumber));
    }
}

#[test]
fn operation_projection_occurrence_and_recovery_limits_accept_128_and_reject_129() {
    let projection_limit = (0..MAX_PROJECTION_DELTA_OPERATIONS)
        .map(projection_identity)
        .collect::<Vec<_>>();
    let mut delta = wire_shell();
    delta.projections = projection_limit;
    delta.canonical_bytes().unwrap();
    delta
        .projections
        .push(projection_identity(MAX_PROJECTION_DELTA_OPERATIONS));
    assert!(matches!(
        delta.canonical_bytes(),
        Err(ProjectionDeltaError::TooManyProjections { len: 129, max: 128 })
    ));

    let mut delta = wire_shell();
    delta.projections = vec![projection_identity(0)];
    delta.occurrences = (0..MAX_PROJECTION_DELTA_OPERATIONS)
        .map(occurrence)
        .collect();
    delta.canonical_bytes().unwrap();
    delta
        .occurrences
        .push(occurrence(MAX_PROJECTION_DELTA_OPERATIONS));
    assert!(matches!(
        delta.canonical_bytes(),
        Err(ProjectionDeltaError::TooManyOccurrences { len: 129, max: 128 })
    ));

    let mut delta = wire_shell();
    delta.projections = vec![projection_identity(0)];
    delta.occurrences = vec![occurrence(0)];
    delta.operations = (0..MAX_PROJECTION_DELTA_OPERATIONS)
        .map(|index| {
            operation(
                0,
                vec![0],
                ProjectionDeltaMutation::Delete {
                    scope: scope("Rows", vec![key(0, "id", &format!("record-{index:03}"))]),
                },
            )
        })
        .collect();
    delta.canonical_bytes().unwrap();
    delta.operations.push(operation(
        0,
        vec![0],
        ProjectionDeltaMutation::Delete {
            scope: scope("Rows", vec![key(0, "id", "record-128")]),
        },
    ));
    assert!(matches!(
        delta.canonical_bytes(),
        Err(ProjectionDeltaError::TooManyOperations { len: 129, max: 128 })
    ));

    let mut delta = wire_shell();
    delta.projections = vec![projection_identity(0)];
    delta.occurrences = vec![occurrence(0)];
    delta.recoveries = (0..MAX_PROJECTION_DELTA_OPERATIONS)
        .map(|index| ProjectionDeltaRecovery {
            occurrence_ordinal: 0,
            projection_refs: vec![0],
            condition: ProjectionDeltaRecoveryCondition::Always,
            target: ProjectionDeltaRecoveryTarget::Model {
                partition: None,
                model: format!("Model{index:03}"),
            },
        })
        .collect();
    delta.canonical_bytes().unwrap();
    delta.recoveries.push(ProjectionDeltaRecovery {
        occurrence_ordinal: 0,
        projection_refs: vec![0],
        condition: ProjectionDeltaRecoveryCondition::Always,
        target: ProjectionDeltaRecoveryTarget::Model {
            partition: None,
            model: "Model128".to_owned(),
        },
    });
    assert!(matches!(
        delta.canonical_bytes(),
        Err(ProjectionDeltaError::TooManyRecoveries { len: 129, max: 128 })
    ));
}

#[test]
fn wire_decode_streams_no_more_than_128_top_level_delta_items() {
    let recovery = ProjectionDeltaRecovery {
        occurrence_ordinal: 0,
        projection_refs: vec![0],
        condition: ProjectionDeltaRecoveryCondition::Always,
        target: ProjectionDeltaRecoveryTarget::Model {
            partition: None,
            model: "Rows".into(),
        },
    };
    let operation = operation(
        0,
        vec![0],
        ProjectionDeltaMutation::Delete {
            scope: scope("Rows", vec![key(0, "id", "record-1")]),
        },
    );
    for (field, item) in [
        (
            "projections",
            serde_json::to_value(projection_identity(0)).unwrap(),
        ),
        ("occurrences", serde_json::to_value(occurrence(0)).unwrap()),
        ("operations", serde_json::to_value(operation).unwrap()),
        ("recoveries", serde_json::to_value(recovery).unwrap()),
    ] {
        let mut wire = serde_json::to_value(wire_shell()).unwrap();
        wire[field] =
            Value::Array(std::iter::repeat_n(item, MAX_PROJECTION_DELTA_OPERATIONS + 1).collect());
        assert!(matches!(
            ProjectionDelta::from_json(&serde_json::to_vec(&wire).unwrap()),
            Err(ProjectionDeltaError::InvalidWire(message))
                if message.contains(field) && message.contains("more than 128")
        ));
    }

    let mut wire = serde_json::to_value(wire_shell()).unwrap();
    wire["occurrences"] = Value::Array(
        std::iter::repeat_n(serde_json::to_value(occurrence(0)).unwrap(), 4_096).collect(),
    );
    let bytes = serde_json::to_vec(&wire).unwrap();
    assert!(bytes.len() < MAX_DOMAIN_EVENT_BODY_BYTES);
    assert!(matches!(
        ProjectionDelta::from_json(&bytes),
        Err(ProjectionDeltaError::InvalidWire(message))
            if message.contains("occurrences") && message.contains("more than 128")
    ));
}

#[test]
fn depth_key_partition_and_body_boundaries_fail_closed() {
    let at_depth = nested_value(MAX_PROJECTION_EXPRESSION_DEPTH);
    at_depth.validate().unwrap();
    assert!(matches!(
        nested_value(MAX_PROJECTION_EXPRESSION_DEPTH + 1).validate(),
        Err(ProjectionDeltaError::ValueTooDeep { depth: 65, max: 64 })
    ));

    let at_key =
        key_with_exact_encoded_size(crate::projection_protocol::MAX_PROJECTION_RECORD_KEY_BYTES);
    assert_eq!(
        serde_json::to_vec(&at_key).unwrap().len(),
        crate::projection_protocol::MAX_PROJECTION_RECORD_KEY_BYTES
    );
    scope("Rows", at_key).validate().unwrap();
    let over_key = key_with_exact_encoded_size(
        crate::projection_protocol::MAX_PROJECTION_RECORD_KEY_BYTES + 1,
    );
    assert!(matches!(
        scope("Rows", over_key).validate(),
        Err(ProjectionDeltaError::KeyTooLarge { .. })
    ));

    let structurally_bounded = ProjectionDeltaPartition::Opaque {
        token: PARTITION_TOKEN.to_owned(),
    };
    structurally_bounded.validate().unwrap();
    let at_partition = ProjectionDeltaPartition::Opaque {
        token: opaque_token_with_serialized_size(
            crate::projection_protocol::MAX_PROJECTION_PARTITION_BYTES,
        ),
    };
    assert_eq!(
        serde_json::to_vec(&at_partition).unwrap().len(),
        crate::projection_protocol::MAX_PROJECTION_PARTITION_BYTES
    );
    assert!(matches!(
        at_partition.validate(),
        Err(ProjectionDeltaError::InvalidIdentity { .. })
    ));
    let over_partition = ProjectionDeltaPartition::Opaque {
        token: opaque_token_with_serialized_size(
            crate::projection_protocol::MAX_PROJECTION_PARTITION_BYTES + 1,
        ),
    };
    assert!(matches!(
        over_partition.validate(),
        Err(ProjectionDeltaError::PartitionTooLarge {
            len: 4097,
            max: 4096
        })
    ));

    let canonical = golden_delta().canonical_bytes().unwrap();
    let mut at_body = canonical;
    at_body.resize(MAX_DOMAIN_EVENT_BODY_BYTES, b' ');
    ProjectionDelta::from_json(&at_body).unwrap();
    at_body.push(b' ');
    assert!(matches!(
        ProjectionDelta::from_json(&at_body),
        Err(ProjectionDeltaError::BodyTooLarge {
            len: 1_048_577,
            max: 1_048_576
        })
    ));
}

#[test]
fn conditional_recovery_requires_a_same_scope_conditional_patch() {
    let mut delta = golden_delta();
    let patch_index = delta
        .operations
        .iter()
        .position(|operation| matches!(operation.mutation, ProjectionDeltaMutation::Patch { .. }))
        .unwrap();
    delta.operations.remove(patch_index);

    assert!(matches!(
        delta.canonical_bytes(),
        Err(ProjectionDeltaError::InvalidOperation(
            "if_record_missing recovery requires a same-scope conditional patch"
        ))
    ));
}

fn golden_delta() -> ProjectionDelta {
    let composite = scope(
        "CompositeRows",
        vec![
            key(0, "z_tenant_id", "tenant-α"),
            key(1, "a_user_id", "user-1"),
        ],
    );
    let patch_scope = scope("PatchRows", vec![key(0, "todo_id", "todo-1")]);
    let removed_scope = scope("RemovedRows", vec![key(0, "todo_id", "todo-2")]);
    let todo_scope = scope("Todos", vec![key(0, "todo_id", "todo-1")]);
    let old_owner_scope = scope("Owners", vec![key(0, "owner_id", "owner-old")]);
    let new_owner_scope = scope("Owners", vec![key(0, "owner_id", "owner-new")]);
    let organization_scope = scope(
        "Organizations",
        vec![key(0, "organization_id", "organization-1")],
    );
    let member_scope = scope("Users", vec![key(0, "user_id", "user-1")]);

    let operations = canonicalize_operations(vec![
        operation(
            0,
            vec![0],
            ProjectionDeltaMutation::Upsert {
                scope: composite,
                fields: vec![
                    field(
                        "metrics",
                        DeltaValue::Object(vec![
                            field("float", DeltaValue::F64("1.5".to_owned())),
                            field("signed_min", DeltaValue::I64(i64::MIN.to_string())),
                            field("unsigned_max", DeltaValue::U64(u64::MAX.to_string())),
                        ]),
                    ),
                    field("nullable_note", DeltaValue::Null),
                    field("title", DeltaValue::String("Résumé 🚀".to_owned())),
                ],
                replace: vec![
                    "archived_at".to_owned(),
                    "metrics".to_owned(),
                    "nullable_note".to_owned(),
                    "title".to_owned(),
                ],
            },
        ),
        operation(
            1,
            vec![0],
            ProjectionDeltaMutation::Patch {
                scope: patch_scope.clone(),
                set: vec![field(
                    "status",
                    DeltaValue::Enum {
                        enum_type: "TodoStatus".to_owned(),
                        variant: "completed".to_owned(),
                    },
                )],
                unset: vec!["legacy_title".to_owned()],
                if_present: true,
            },
        ),
        operation(
            2,
            vec![0],
            ProjectionDeltaMutation::Delete {
                scope: removed_scope,
            },
        ),
        operation(
            3,
            vec![0],
            ProjectionDeltaMutation::Unlink {
                relationship: "owner".to_owned(),
                source: todo_scope.clone(),
                target: old_owner_scope,
            },
        ),
        operation(
            3,
            vec![0],
            ProjectionDeltaMutation::Link {
                relationship: "owner".to_owned(),
                source: todo_scope.clone(),
                target: new_owner_scope,
            },
        ),
        operation(
            4,
            vec![1],
            ProjectionDeltaMutation::Link {
                relationship: "members".to_owned(),
                source: organization_scope.clone(),
                target: member_scope,
            },
        ),
        operation(
            5,
            vec![1],
            ProjectionDeltaMutation::InvalidateModel {
                partition: Some(partition()),
                model: "TodoSearch".to_owned(),
            },
        ),
        operation(
            5,
            vec![1],
            ProjectionDeltaMutation::InvalidateRelationship {
                relationship: "members".to_owned(),
                source: organization_scope.clone(),
            },
        ),
    ])
    .unwrap();
    let recoveries = canonicalize_recoveries(
        vec![
            ProjectionDeltaRecovery {
                occurrence_ordinal: 1,
                projection_refs: vec![0],
                condition: ProjectionDeltaRecoveryCondition::IfRecordMissing,
                target: ProjectionDeltaRecoveryTarget::Record { scope: patch_scope },
            },
            ProjectionDeltaRecovery {
                occurrence_ordinal: 5,
                projection_refs: vec![1],
                condition: ProjectionDeltaRecoveryCondition::Always,
                target: ProjectionDeltaRecoveryTarget::Relationship {
                    relationship: "members".to_owned(),
                    source: organization_scope,
                },
            },
            ProjectionDeltaRecovery {
                occurrence_ordinal: 5,
                projection_refs: vec![1],
                condition: ProjectionDeltaRecoveryCondition::Always,
                target: ProjectionDeltaRecoveryTarget::Model {
                    partition: Some(partition()),
                    model: "TodoSearch".to_owned(),
                },
            },
        ],
        &operations,
    );
    let delta = ProjectionDelta {
        wire_version: PROJECTION_DELTA_WIRE_VERSION,
        identity: identity(),
        projections: vec![projection_identity(0), projection_identity(1)],
        occurrences: (0..6).map(occurrence).collect(),
        operations,
        recoveries,
    };
    delta.validate().unwrap();
    delta
}

fn identity() -> ProjectionDeltaIdentity {
    ProjectionDeltaIdentity {
        manifest_version: DISTRIBUTED_CLIENT_MANIFEST_VERSION,
        client_protocol_version: DISTRIBUTED_CLIENT_PROTOCOL_VERSION,
        surface: ProjectionDeltaSurfaceIdentity::Role {
            name: "vector-user".to_owned(),
        },
        schema_fingerprint: "sha256:vector-schema".to_owned(),
        protocol_fingerprint: "sha256:vector-protocol".to_owned(),
        authorization_generation: "generation-7".to_owned(),
        cache_scope_token: CACHE_SCOPE_TOKEN.to_owned(),
        command_causation_id: CAUSATION_ID.to_owned(),
    }
}

fn wire_shell() -> ProjectionDelta {
    ProjectionDelta {
        wire_version: PROJECTION_DELTA_WIRE_VERSION,
        identity: identity(),
        projections: vec![],
        occurrences: vec![],
        operations: vec![],
        recoveries: vec![],
    }
}

fn projection_identity(index: usize) -> ProjectionDeltaProjectionIdentity {
    let nibble = format!("{:02x}", index);
    ProjectionDeltaProjectionIdentity {
        program_id: format!("pp1:sha256:{nibble:0>64}"),
        binding_id: format!("pb1:sha256:{nibble:0>64}"),
        epoch: format!("projection-delta-vector-{index:03}"),
        program_ir_version: crate::projection::PROJECTION_PROGRAM_IR_VERSION,
        operation_semantics_version: crate::projection::PROJECTION_OPERATION_SEMANTICS_VERSION,
    }
}

fn occurrence(index: usize) -> ProjectionDeltaOccurrence {
    ProjectionDeltaOccurrence {
        causation_id: CAUSATION_ID.to_owned(),
        ordinal: index as u32,
        occurrence_id: format!("occurrence-{index:03}"),
    }
}

fn operation(
    occurrence_ordinal: u32,
    projection_refs: Vec<u32>,
    mutation: ProjectionDeltaMutation,
) -> ProjectionDeltaOperation {
    ProjectionDeltaOperation {
        occurrence_ordinal,
        projection_refs,
        mutation,
    }
}

fn mutation(
    delta: &ProjectionDelta,
    predicate: impl Fn(&ProjectionDeltaMutation) -> bool,
) -> &ProjectionDeltaMutation {
    &delta
        .operations
        .iter()
        .find(|operation| predicate(&operation.mutation))
        .expect("golden mutation must exist")
        .mutation
}

fn scope(model: &str, key: Vec<DeltaKeyField>) -> ProjectionDeltaScope {
    ProjectionDeltaScope {
        partition: partition(),
        model: model.to_owned(),
        key,
    }
}

fn partition() -> ProjectionDeltaPartition {
    ProjectionDeltaPartition::Opaque {
        token: PARTITION_TOKEN.to_owned(),
    }
}

fn key(ordinal: u32, field: &str, value: &str) -> DeltaKeyField {
    DeltaKeyField {
        ordinal,
        field: field.to_owned(),
        value: DeltaValue::String(value.to_owned()),
    }
}

fn field(field: &str, value: DeltaValue) -> DeltaField {
    DeltaField {
        field: field.to_owned(),
        value,
    }
}

fn nested_value(levels: usize) -> DeltaValue {
    let mut value = DeltaValue::String("leaf".to_owned());
    for _ in 1..levels {
        value = DeltaValue::List(vec![value]);
    }
    value
}

fn key_with_exact_encoded_size(size: usize) -> Vec<DeltaKeyField> {
    let mut key = vec![key(0, "id", "")];
    let overhead = serde_json::to_vec(&key).unwrap().len();
    assert!(size >= overhead);
    key[0].value = DeltaValue::String("k".repeat(size - overhead));
    assert_eq!(serde_json::to_vec(&key).unwrap().len(), size);
    key
}

fn opaque_token_with_serialized_size(size: usize) -> String {
    let empty = ProjectionDeltaPartition::Opaque {
        token: String::new(),
    };
    let overhead = serde_json::to_vec(&empty).unwrap().len();
    assert!(size >= overhead);
    "x".repeat(size - overhead)
}

fn assert_invalid_wire(value: &Value) {
    assert!(matches!(
        ProjectionDelta::from_json(&serde_json::to_vec(value).unwrap()),
        Err(ProjectionDeltaError::InvalidWire(_))
    ));
}

#[derive(Clone, Copy, Debug)]
enum ReplayMutation {
    Surface,
    CacheScope,
    AuthorizationGeneration,
    Causation,
    SchemaFingerprint,
    ProtocolFingerprint,
}

#[derive(Clone)]
struct ReplayRequest {
    generation: String,
    principal: crate::command_ledger::PrincipalPartitionId,
    cache_scope: ProjectionDeltaCacheScopeToken,
    causation: crate::command_ledger::CausationId,
}

impl ReplayRequest {
    fn new(generation: &str, cache_scope: &str, causation: &str) -> Self {
        Self {
            generation: generation.to_owned(),
            principal: crate::command_ledger::PrincipalPartitionId::new("vector-principal")
                .unwrap(),
            cache_scope: ProjectionDeltaCacheScopeToken::parse_wire(cache_scope).unwrap(),
            causation: crate::command_ledger::CausationId::parse_stored(causation.to_owned())
                .unwrap(),
        }
    }
}

impl ProjectionPartitionScopeEncoder for ReplayRequest {
    fn encode(
        &self,
        _authority: ProjectionPartitionAuthority<'_>,
        _partition: &ResolvedProjectionPartition,
    ) -> Result<Option<ProjectionDeltaPartition>, ProjectionDeltaError> {
        Ok(Some(partition()))
    }
}

impl ProjectionVisibilityEvaluator for ReplayRequest {
    fn record_transition(
        &self,
        _source: ProjectionMutationSource,
        _mutation: &ResolvedProjectionMutation,
    ) -> Result<AuthorizationTransition, ProjectionDeltaError> {
        Ok(authorized_transition())
    }

    fn relationship_transition(
        &self,
        _source: ProjectionMutationSource,
        _effect: &ResolvedProjectionRelationshipEffect,
    ) -> Result<AuthorizationTransition, ProjectionDeltaError> {
        Ok(authorized_transition())
    }
}

impl ProjectionDeltaRequestAuthority for ReplayRequest {
    fn authorization_generation(&self) -> &str {
        &self.generation
    }

    fn principal_scope(&self) -> &crate::command_ledger::PrincipalPartitionId {
        &self.principal
    }

    fn cache_scope(&self) -> &ProjectionDeltaCacheScopeToken {
        &self.cache_scope
    }

    fn command_causation_id(&self) -> &crate::command_ledger::CausationId {
        &self.causation
    }
}

const fn authorized_transition() -> AuthorizationTransition {
    AuthorizationTransition {
        before: ProjectionDeltaVisibility::Authorized,
        after: ProjectionDeltaVisibility::Authorized,
    }
}

fn replay_manifest() -> DistributedClientManifest {
    DistributedClientManifest {
        manifest_version: DISTRIBUTED_CLIENT_MANIFEST_VERSION,
        protocol_version: DISTRIBUTED_CLIENT_PROTOCOL_VERSION,
        service_id: "vector-service".to_owned(),
        surface: ClientSurfaceIdentity::role("vector-user"),
        schema_fingerprint: "sha256:vector-schema".to_owned(),
        protocol_fingerprint: "sha256:vector-protocol".to_owned(),
        execution: ClientExecutionLimits::default(),
        capabilities: ClientCapabilities {
            live_queries: false,
            record_revisions: false,
            tombstones: false,
            causal_receipts: true,
            live_resume: false,
            query_fallback: "revalidate".to_owned(),
            cache_scope: true,
            confirmed_persistence: false,
        },
        scalar_codecs: vec![],
        models: vec![],
        roots: vec![],
        commands: vec![],
        protocol_operations: ClientProtocolOperations {
            version: 1,
            command_status: None,
        },
        projectors: vec![],
        projection_programs: vec![],
        projection_bindings: vec![],
        leaderboards: vec![],
    }
}

fn alternate_cache_scope() -> &'static str {
    "v1.cache-scope.AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE"
}
