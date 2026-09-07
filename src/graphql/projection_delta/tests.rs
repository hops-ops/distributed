use std::borrow::Cow;
use std::cell::Cell;
use std::collections::BTreeMap;
use std::time::{Duration, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::json;

use super::authorization::{
    ProjectionPartitionAuthority, ProjectionPartitionScopeEncoder, ProjectionVisibilityEvaluator,
};
use super::lower::{
    ProjectionDeltaAuthority, ProjectionDeltaPlanOccurrence, ProjectionDeltaRequestAuthority,
};
use super::types::{
    AuthorizationTransition, ProjectionDeltaCacheScopeToken, ProjectionDeltaVisibility,
    ProjectionMutationSource,
};
use super::*;

use crate::graphql::{
    build_surface, surface_for_role, DistributedClientSurfaceExport, RoleGrant,
    SurfaceDirectProjection, SurfaceOptions, SurfaceProjector,
};
use crate::projection::catalog::ProjectionBindingActivation;
use crate::projection::placement::{
    ProjectionBinding, ProjectionBindingState, ProjectionExecutionClass, ProjectionExecutorRoute,
    ProjectionOutput, ProjectionOwner, ProjectionPhysicalTopology, ProjectionRelationshipBinding,
    ProjectionSourceBinding, PROJECTION_PARTITION_CODEC_VERSION,
};
use crate::projection::{
    ProjectionArm, ProjectionField, ProjectionOperation, ProjectionPartition, ProjectionTarget,
};
use crate::projection_protocol::{ProjectionEpoch, ProjectorTopologyId};
use crate::table::{
    ColumnType, PrimaryKey, RelationshipDef, RelationshipKind, TableColumn, TableKind, TableSchema,
};
use crate::{
    DomainEventBodyDescriptor, DomainEventBodyKind, DomainEventDescriptor, DomainEventEnvelope,
    DomainEventOccurrence, ProjectionAssignment, ProjectionExpression, ProjectionInvalidation,
    ProjectionKeyField, ProjectionMutationKind, ProjectionProgram, ProjectionRelationship,
    ProjectionRelationshipEffect, ProjectionValue, ProjectionValueType, ResolvedProjectionMutation,
    ResolvedProjectionRelationshipEffect, DOMAIN_EVENT_BODY_CODEC, DOMAIN_EVENT_BODY_CODEC_VERSION,
    MAX_PROJECTION_EXPRESSION_DEPTH,
};

const BODY_FINGERPRINT: &str =
    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const TEST_CAUSATION_ID: &str = "0190a000-0000-7000-8000-000000000017";
const FOREIGN_CAUSATION_ID: &str = "0190a000-0000-7000-8000-000000000018";
const TEST_COMMAND_NAME: &str = "todo.modeled";

#[allow(dead_code)]
#[derive(Deserialize, crate::CommandInput)]
struct ModeledCommandInput {
    todo_id: String,
}

#[derive(Serialize, crate::CommandOutput)]
struct ModeledCommandOutput {
    accepted: bool,
}

#[test]
fn strict_decode_rejects_unknown_wire_fields() {
    let bytes = br#"{"wire_version":1,"identity":{},"projections":[],"occurrences":[],"operations":[],"recoveries":[],"unknown":true}"#;
    assert!(matches!(
        ProjectionDelta::from_json(bytes),
        Err(ProjectionDeltaError::InvalidWire(_))
    ));
}

#[test]
fn authoritative_zero_actual_occurrences_is_valid_and_deterministic() {
    let delta = empty_delta();
    let bytes = delta.canonical_bytes().unwrap();
    assert_eq!(ProjectionDelta::from_json(&bytes).unwrap(), delta);
    assert!(delta.projections.is_empty());
    assert!(delta.occurrences.is_empty());
    assert!(delta.operations.is_empty());
}

#[test]
fn upsert_requires_exact_non_key_mask_and_allows_key_only_rows() {
    let scope = test_scope("tenant-a", "KeyOnly", vec![key_field(0, "id", "1")]);
    let key_only = operation(
        0,
        ProjectionDeltaMutation::Upsert {
            scope: scope.clone(),
            fields: vec![],
            replace: vec![],
        },
    );
    key_only.validate(1, 1).unwrap();

    let mismatched = operation(
        0,
        ProjectionDeltaMutation::Upsert {
            scope,
            fields: vec![DeltaField {
                field: "secret".into(),
                value: DeltaValue::String("safe".into()),
            }],
            replace: vec!["status".into(), "title".into()],
        },
    );
    assert!(matches!(
        mismatched.validate(1, 1),
        Err(ProjectionDeltaError::InvalidOperation(
            "upsert fields must be contained in the replacement mask"
        ))
    ));
}

#[test]
fn composite_keys_preserve_declared_order_instead_of_name_order() {
    let scope = test_scope(
        "tenant-a",
        "Memberships",
        vec![
            key_field(0, "z_tenant_id", "tenant-a"),
            key_field(1, "a_user_id", "user-1"),
        ],
    );
    scope.validate().unwrap();
    assert_eq!(scope.key[0].field, "z_tenant_id");
    assert_eq!(scope.key[1].field, "a_user_id");
}

#[test]
fn tagged_values_distinguish_null_absent_and_unset_and_bound_numbers() {
    DeltaValue::Null.validate().unwrap();
    DeltaValue::I64(i64::MIN.to_string()).validate().unwrap();
    DeltaValue::U64(u64::MAX.to_string()).validate().unwrap();
    DeltaValue::F64("1.5".into()).validate().unwrap();
    assert_eq!(
        serde_json::to_value(DeltaValue::Null).unwrap(),
        json!({"type": "null"})
    );
    assert!(matches!(
        DeltaValue::I64("01".into()).validate(),
        Err(ProjectionDeltaError::InvalidNumber)
    ));
    assert!(matches!(
        DeltaValue::F64("-0".into()).validate(),
        Err(ProjectionDeltaError::InvalidNumber)
    ));

    let patch = ProjectionDeltaMutation::Patch {
        scope: test_scope("tenant-a", "Todos", vec![key_field(0, "todo_id", "todo-1")]),
        set: vec![DeltaField {
            field: "nullable_note".into(),
            value: DeltaValue::Null,
        }],
        unset: vec!["title".into()],
        if_present: true,
    };
    let json = serde_json::to_value(patch).unwrap();
    assert!(json["set"][0].get("value").is_some());
    assert_eq!(json["unset"], json!(["title"]));
    assert!(json.get("absent").is_none());
}

#[test]
fn value_depth_key_partition_and_body_limits_fail_closed() {
    let mut value = DeltaValue::String("leaf".into());
    for _ in 0..MAX_PROJECTION_EXPRESSION_DEPTH {
        value = DeltaValue::List(vec![value]);
    }
    assert!(matches!(
        value.validate(),
        Err(ProjectionDeltaError::ValueTooDeep { .. })
    ));
    let oversized_partition = ProjectionDeltaPartition::Opaque {
        token: "x".repeat(crate::projection_protocol::MAX_PROJECTION_PARTITION_BYTES),
    };
    assert!(matches!(
        oversized_partition.validate(),
        Err(ProjectionDeltaError::PartitionTooLarge { .. })
    ));
    let oversized_body = vec![b' '; crate::MAX_DOMAIN_EVENT_BODY_BYTES + 1];
    assert!(matches!(
        ProjectionDelta::from_json(&oversized_body),
        Err(ProjectionDeltaError::BodyTooLarge { .. })
    ));
}

#[test]
fn sealed_authority_lowers_actual_full_state_and_memoizes_partition() {
    let fixture = modeled_fixture(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let selected = selected_export(&fixture.surface);
    let request = TestRequestAuthority::authorized("auth-generation-7");
    let authority = ProjectionDeltaAuthority::try_new(&selected, &request).unwrap();
    let modeled = selected.surface().projectors[0].modeled[0].clone();
    let source = authority.source(&modeled).unwrap();
    let first = crate::ResolvedProjectionPlan::resolve(
        &fixture.program,
        &state_occurrence(7, "todo-1", "Résumé 🚀"),
    )
    .unwrap();
    let second = crate::ResolvedProjectionPlan::resolve(
        &fixture.program,
        &state_occurrence(8, "todo-1", "Résumé 🚀"),
    )
    .unwrap();
    let batch = vec![
        ProjectionDeltaPlanOccurrence::actual(vec![(&source, &first)]).unwrap(),
        ProjectionDeltaPlanOccurrence::actual(vec![(&source, &second)]).unwrap(),
    ];
    let delta = authority.lower(&batch).unwrap();

    assert_eq!(request.calls.get(), 1);
    assert_eq!(delta.occurrences.len(), 2);
    let upsert = delta
        .operations
        .iter()
        .find_map(|operation| match &operation.mutation {
            ProjectionDeltaMutation::Upsert {
                scope,
                fields,
                replace,
            } => Some((scope, fields, replace)),
            _ => None,
        })
        .expect("full actual state lowers to upsert");
    assert_eq!(upsert.0.model, "TodoView");
    assert_eq!(
        upsert
            .1
            .iter()
            .map(|field| field.field.as_str())
            .collect::<Vec<_>>(),
        vec!["owner_id", "status", "title"]
    );
    assert_eq!(upsert.2, &vec!["owner_id", "status", "title"]);
    assert!(upsert.1.iter().all(|field| field.field != "todo_id"));
    assert!(delta
        .operations
        .iter()
        .any(|operation| matches!(operation.mutation, ProjectionDeltaMutation::Link { .. })));
}

#[test]
#[cfg(feature = "graphql")]
fn production_authority_binds_partition_and_obligation_tokens_to_request_scope() {
    use super::runtime::{
        ModeledProjectionObservationScope, ProjectionRuntimeAuthorityError,
        ProtocolProjectionDeltaRequestAuthority, ProtocolProjectionProgramRegistry,
        MAX_PROJECTION_AUTHORITY_LIFETIME_MS,
    };
    use crate::graphql::protocol::{
        CommandProjectionMetadataV1, ProtocolTokenCodec, ProtocolTokenPurpose,
    };

    let fixture = modeled_fixture(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let selected = selected_export(&fixture.surface);
    let codec = ProtocolTokenCodec::new([0x5a; 32]);
    let cache_scope = codec
        .issue(
            ProtocolTokenPurpose::CacheScope,
            &("delta-user", "auth-generation-7"),
        )
        .unwrap();
    let principal = crate::command_ledger::PrincipalPartitionId::new("principal-scope-9").unwrap();
    let causation =
        crate::command_ledger::CausationId::parse_stored(TEST_CAUSATION_ID.to_owned()).unwrap();
    let request = ProtocolProjectionDeltaRequestAuthority::try_new(
        selected,
        codec.clone(),
        principal.clone(),
        "auth-generation-7",
        &cache_scope,
        causation,
        1_000,
        1_000,
        2_000,
    )
    .unwrap();
    let authority = ProjectionDeltaAuthority::try_new(request.export(), &request).unwrap();
    let modeled = request.export().surface().projectors[0].modeled[0].clone();
    let source = authority.source(&modeled).unwrap();
    let plan = crate::ResolvedProjectionPlan::resolve(
        &fixture.program,
        &state_occurrence(7, "todo-1", "title"),
    )
    .unwrap();
    let occurrence = ProjectionDeltaPlanOccurrence::actual(vec![(&source, &plan)]).unwrap();
    let delta = authority.lower(&[occurrence]).unwrap();
    let partition_token = match &delta.operations[0].mutation {
        ProjectionDeltaMutation::Upsert { scope, .. } => match &scope.partition {
            ProjectionDeltaPartition::Opaque { token } => token.clone(),
            ProjectionDeltaPartition::Unit => panic!("fixture uses a value partition"),
        },
        mutation => panic!("expected fixture upsert, got {mutation:?}"),
    };
    assert!(partition_token.starts_with("v1.projection-partition."));
    request
        .verify_partition_token(
            &partition_token,
            &delta.identity.surface,
            plan.partition(),
            1_999,
        )
        .unwrap();
    assert_eq!(
        request.verify_partition_token(
            &partition_token,
            &delta.identity.surface,
            plan.partition(),
            2_000,
        ),
        Err(ProjectionRuntimeAuthorityError::Expired)
    );
    assert_eq!(
        request.verify_partition_token(
            &partition_token,
            &ProjectionDeltaSurfaceIdentity::Role {
                name: "other-role".into(),
            },
            plan.partition(),
            1_500,
        ),
        Err(ProjectionRuntimeAuthorityError::InvalidAuthority)
    );

    let observation_codec = crate::projection_protocol::ProjectionScopeCodec::with_models(
        ProjectorTopologyId::new(1, "projection-delta-test", [0x44; 32]).unwrap(),
        [("TodoView", &todos())],
    )
    .unwrap();
    let observation_scope = observation_codec
        .encode_row_scope(
            "projection-delta-test",
            "TodoView",
            Some(&json!("tenant-a")),
            &crate::table::RowKey::new([(
                "todo_id",
                crate::table::RowValue::String("todo-1".into()),
            )]),
        )
        .unwrap();
    let registry = ProtocolProjectionProgramRegistry::try_from_surface(&fixture.surface).unwrap();
    let lifecycle_proofs = registry
        .lifecycle_proofs_for_test(&request, &delta, &[&modeled])
        .unwrap();
    let metadata = request
        .metadata(
            delta,
            lifecycle_proofs,
            &[&modeled],
            &[ModeledProjectionObservationScope {
                operation_index: 0,
                projection_ref: 0,
                projector: "projection-delta-test".into(),
                model: "TodoView".into(),
                kind: crate::projection_protocol::ProjectionObservationKind::Record,
                scope: observation_scope,
            }],
            false,
        )
        .unwrap();
    assert!(!metadata.obligations.is_empty());
    assert!(metadata.obligations.iter().all(|obligation| {
        obligation
            .scope_token
            .as_str()
            .starts_with("v1.projection-obligation.")
    }));
    let bytes = metadata.canonical_bytes().unwrap();
    assert_eq!(
        CommandProjectionMetadataV1::from_json(&bytes).unwrap(),
        metadata
    );

    let other_request = ProtocolProjectionDeltaRequestAuthority::try_new(
        request.export().clone(),
        codec.clone(),
        principal.clone(),
        "auth-generation-8",
        &cache_scope,
        crate::command_ledger::CausationId::parse_stored(TEST_CAUSATION_ID.to_owned()).unwrap(),
        1_000,
        1_000,
        2_000,
    )
    .unwrap();
    assert!(matches!(
        other_request.verify_partition_token(
            &partition_token,
            &metadata.delta.identity.surface,
            plan.partition(),
            1_500,
        ),
        Err(ProjectionRuntimeAuthorityError::Token(_))
    ));

    let other_principal = ProtocolProjectionDeltaRequestAuthority::try_new(
        request.export().clone(),
        codec.clone(),
        crate::command_ledger::PrincipalPartitionId::new("principal-scope-other").unwrap(),
        "auth-generation-7",
        &cache_scope,
        crate::command_ledger::CausationId::parse_stored(TEST_CAUSATION_ID.to_owned()).unwrap(),
        1_000,
        1_000,
        2_000,
    )
    .unwrap();
    assert!(matches!(
        other_principal.verify_partition_token(
            &partition_token,
            &metadata.delta.identity.surface,
            plan.partition(),
            1_500,
        ),
        Err(ProjectionRuntimeAuthorityError::Token(_))
    ));

    let other_cache_scope = codec
        .issue(ProtocolTokenPurpose::CacheScope, &("other-cache", 1))
        .unwrap();
    let other_cache = ProtocolProjectionDeltaRequestAuthority::try_new(
        request.export().clone(),
        codec.clone(),
        principal,
        "auth-generation-7",
        &other_cache_scope,
        crate::command_ledger::CausationId::parse_stored(TEST_CAUSATION_ID.to_owned()).unwrap(),
        1_000,
        1_000,
        2_000,
    )
    .unwrap();
    assert!(matches!(
        other_cache.verify_partition_token(
            &partition_token,
            &metadata.delta.identity.surface,
            plan.partition(),
            1_500,
        ),
        Err(ProjectionRuntimeAuthorityError::Token(_))
    ));
    assert!(matches!(
        metadata.validate_not_expired(2_000),
        Err(crate::graphql::protocol::CommandProjectionMetadataError::Expired)
    ));
    assert!(matches!(
        ProtocolProjectionDeltaRequestAuthority::try_new(
            request.export().clone(),
            codec.clone(),
            crate::command_ledger::PrincipalPartitionId::new("principal-expired").unwrap(),
            "auth-generation-7",
            &cache_scope,
            crate::command_ledger::CausationId::parse_stored(TEST_CAUSATION_ID.to_owned()).unwrap(),
            2_000,
            1_000,
            2_000,
        ),
        Err(ProjectionRuntimeAuthorityError::InvalidAuthority)
    ));
    assert!(matches!(
        ProtocolProjectionDeltaRequestAuthority::try_new(
            request.export().clone(),
            codec,
            crate::command_ledger::PrincipalPartitionId::new("principal-overlong").unwrap(),
            "auth-generation-7",
            &cache_scope,
            crate::command_ledger::CausationId::parse_stored(TEST_CAUSATION_ID.to_owned()).unwrap(),
            1_000,
            1_000,
            1_001 + MAX_PROJECTION_AUTHORITY_LIFETIME_MS,
        ),
        Err(ProjectionRuntimeAuthorityError::InvalidAuthority)
    ));
}

#[test]
#[cfg(feature = "graphql")]
fn production_predicate_authorizes_only_complete_actual_after_rows() {
    use super::runtime::ProtocolProjectionDeltaRequestAuthority;
    use crate::graphql::protocol::{
        DistributedTrustedPreset, ProtocolTokenCodec, ProtocolTokenPurpose,
    };

    fn lower(
        fixture: &ModeledFixture,
        occurrence: DomainEventOccurrence,
        claim: Option<&str>,
    ) -> ProjectionDelta {
        let selected = selected_export_with_owner_policy(&fixture.surface);
        let codec = ProtocolTokenCodec::new([0x6a; 32]);
        let cache_scope = codec
            .issue(ProtocolTokenPurpose::CacheScope, &("owner-policy", claim))
            .unwrap();
        let trusted_presets = claim
            .map(|value| {
                vec![DistributedTrustedPreset {
                    name: "x-user-id".into(),
                    codec: "string".into(),
                    value: serde_json::Value::String(value.into()),
                }]
            })
            .unwrap_or_default();
        let request = ProtocolProjectionDeltaRequestAuthority::try_new(
            selected,
            codec,
            crate::command_ledger::PrincipalPartitionId::new("owner-policy-principal").unwrap(),
            "owner-policy-generation",
            &cache_scope,
            crate::command_ledger::CausationId::parse_stored(TEST_CAUSATION_ID.to_owned()).unwrap(),
            1_000,
            1_000,
            2_000,
        )
        .unwrap()
        .with_trusted_presets(trusted_presets);
        let authority = ProjectionDeltaAuthority::try_new(request.export(), &request).unwrap();
        let modeled = request.export().surface().projectors[0].modeled[0].clone();
        let source = authority.source(&modeled).unwrap();
        let plan = crate::ResolvedProjectionPlan::resolve(&fixture.program, &occurrence).unwrap();
        let occurrence = ProjectionDeltaPlanOccurrence::actual(vec![(&source, &plan)]).unwrap();
        authority.lower(&[occurrence]).unwrap()
    }

    let complete = modeled_fixture(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let authorized = lower(
        &complete,
        state_occurrence(7, "todo-1", "visible"),
        Some("owner-secret"),
    );
    assert!(authorized
        .operations
        .iter()
        .any(|operation| matches!(operation.mutation, ProjectionDeltaMutation::Upsert { .. })));
    assert!(!authorized.recoveries.iter().any(|recovery| matches!(
        recovery.target,
        ProjectionDeltaRecoveryTarget::Record { .. }
    )));

    for (occurrence, claim) in [
        (
            state_occurrence(8, "todo-2", "owner transfer"),
            Some("another-owner"),
        ),
        (state_occurrence(9, "todo-3", "missing trusted claim"), None),
    ] {
        let delta = lower(&complete, occurrence, claim);
        assert!(!delta.operations.iter().any(is_record_operation));
        assert!(!delta.recoveries.iter().any(|recovery| matches!(
            recovery.target,
            ProjectionDeltaRecoveryTarget::Record { .. }
        )));
        assert!(delta.recoveries.iter().all(|recovery| matches!(
            recovery.target,
            ProjectionDeltaRecoveryTarget::Model { .. }
        )));
    }

    let patch = modeled_fixture_with_kind(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
        ProjectionMutationKind::Patch,
    );
    let delta = lower(
        &patch,
        partial_state_occurrence(10, "todo-4"),
        Some("owner-secret"),
    );
    assert!(!delta.operations.iter().any(is_record_operation));
    assert!(delta
        .recoveries
        .iter()
        .all(|recovery| matches!(recovery.target, ProjectionDeltaRecoveryTarget::Model { .. })));
}

#[test]
#[cfg(feature = "graphql")]
fn application_wire_surface_uses_authenticated_role_policy_for_actual_rows() {
    use std::sync::Arc;

    use super::runtime::ProtocolProjectionDeltaRequestAuthority;
    use crate::graphql::protocol::{
        DistributedTrustedPreset, ProtocolTokenCodec, ProtocolTokenPurpose,
    };
    use crate::graphql::surface_for_application;

    let fixture = modeled_fixture(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let surface = surface_with_modeled_command(&fixture.surface);
    let user_grants = BTreeMap::from([
        (
            "TodoView".into(),
            RoleGrant::all_columns()
                .rows(crate::graphql::col("owner_id").eq(crate::graphql::claim("x-user-id"))),
        ),
        ("UserView".into(), RoleGrant::all_columns()),
    ]);
    let admin_grants = BTreeMap::from([
        ("TodoView".into(), RoleGrant::all_columns()),
        ("UserView".into(), RoleGrant::all_columns()),
    ]);
    let grants_by_role = BTreeMap::from([
        ("delta-admin".into(), admin_grants),
        ("delta-user".into(), user_grants.clone()),
    ]);
    let role_surface = Arc::new(surface_for_role(&surface, "delta-user", &user_grants).unwrap());
    let application_surface = surface_for_application(
        &surface,
        "delta-app",
        &["delta-admin".into(), "delta-user".into()],
        &["delta-admin".into(), "delta-user".into()],
        &grants_by_role,
    )
    .unwrap();
    assert!(matches!(
        application_surface.models["TodoView"].row_policy,
        crate::graphql::surface::SurfaceRowPolicy::ServerOnly
    ));
    let export =
        DistributedClientSurfaceExport::from_selected("delta-service", application_surface)
            .unwrap();
    let codec = ProtocolTokenCodec::new([0x6b; 32]);
    let cache_scope = codec
        .issue(ProtocolTokenPurpose::CacheScope, &"application-role-policy")
        .unwrap();
    let trusted_presets = vec![DistributedTrustedPreset {
        name: "x-user-id".into(),
        codec: "string".into(),
        value: serde_json::Value::String("owner-secret".into()),
    }];
    let request = ProtocolProjectionDeltaRequestAuthority::try_new(
        export,
        codec,
        crate::command_ledger::PrincipalPartitionId::new("application-role-principal").unwrap(),
        "application-role-generation",
        &cache_scope,
        crate::command_ledger::CausationId::parse_stored(TEST_CAUSATION_ID.to_owned()).unwrap(),
        1_000,
        1_000,
        2_000,
    )
    .unwrap()
    .with_visibility_surface(role_surface)
    .unwrap()
    .with_trusted_presets(trusted_presets);
    let authority = ProjectionDeltaAuthority::try_new(request.export(), &request).unwrap();
    let modeled = request.export().surface().projectors[0].modeled[0].clone();
    let source = authority.source(&modeled).unwrap();
    let occurrence = state_occurrence(10, "todo-app", "visible through role policy");
    let plan = crate::ResolvedProjectionPlan::resolve(&fixture.program, &occurrence).unwrap();
    let occurrence = ProjectionDeltaPlanOccurrence::actual(vec![(&source, &plan)]).unwrap();
    let delta = authority.lower(&[occurrence]).unwrap();

    assert!(delta
        .operations
        .iter()
        .any(|operation| matches!(operation.mutation, ProjectionDeltaMutation::Upsert { .. })));
}

#[test]
#[cfg(feature = "graphql")]
fn zero_obligation_modeled_metadata_is_revalidated_on_every_receipt_emission() {
    use std::sync::Arc;

    use super::runtime::{ProtocolProjectionProgramRegistry, ProtocolProjectionRequestSeed};
    use crate::graphql::protocol::{
        CommandProjectionMetadataV1, DistributedEnvelopeV1, ProtocolResponseAccumulator,
        ProtocolTokenCodec, ProtocolTokenPurpose,
    };
    use crate::microsvc::{
        CausalCommandPublicState, CausalCommandPublicStatus, CausalCommandReceiptSource,
    };

    fn status(metadata: CommandProjectionMetadataV1) -> CausalCommandPublicStatus {
        CausalCommandPublicStatus {
            state: CausalCommandPublicState::Succeeded,
            command_id: "0190a000-0000-7000-8000-000000000011".into(),
            command_name: Some(TEST_COMMAND_NAME.into()),
            causation_id: Some(TEST_CAUSATION_ID.into()),
            consistency: Some(crate::command::CommandConsistency::Eventual),
            outcome: None,
            obligations: Vec::new(),
            projection_metadata: Some(metadata),
            projection_revalidate: false,
            evidence: Vec::new(),
            direct_projection: None,
        }
    }

    fn receipt(metadata: CommandProjectionMetadataV1) -> CausalCommandReceiptSource {
        CausalCommandReceiptSource {
            command_id: "modeled-status-command".into(),
            command_name: TEST_COMMAND_NAME.into(),
            causation_id: TEST_CAUSATION_ID.into(),
            consistency: crate::command::CommandConsistency::Eventual,
            state: crate::command_ledger::CommandLedgerState::SucceededPendingProjection,
            outcome: serde_json::json!({"ok": true}),
            obligations: Vec::new(),
            projection_metadata: Some(metadata),
            direct_projection: None,
        }
    }

    fn accumulator(
        seed: ProtocolProjectionRequestSeed,
        codec: ProtocolTokenCodec,
        cache_scope: crate::graphql::protocol::OpaqueProtocolToken,
    ) -> ProtocolResponseAccumulator {
        let accumulator = ProtocolResponseAccumulator::new(
            DistributedEnvelopeV1::new(
                "sha256:modeled-status",
                "modeled-status-auth",
                cache_scope,
                None,
            ),
            codec,
        );
        accumulator.bind_projection_request(seed).unwrap();
        accumulator
    }

    let fixture = modeled_fixture(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let export = selected_export(&fixture.surface);
    let registry =
        Arc::new(ProtocolProjectionProgramRegistry::try_from_surface(&fixture.surface).unwrap());
    let completion_unix_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let issued_at_unix_ms = completion_unix_ms - 120_000;
    let seed = ProtocolProjectionRequestSeed::new(
        export.clone(),
        Arc::clone(&registry),
        crate::command_ledger::PrincipalPartitionId::new("modeled-status-principal").unwrap(),
        "modeled-status-auth",
        Vec::new(),
        issued_at_unix_ms,
    )
    .unwrap();
    let codec = ProtocolTokenCodec::new([0x6c; 32]);
    let cache_scope = codec
        .issue(
            ProtocolTokenPurpose::CacheScope,
            &("modeled-status", "cache"),
        )
        .unwrap();
    let occurrence = state_occurrence(11, "todo-status", "status");
    let selector =
        crate::ProjectionEventSelector::try_from_descriptor(occurrence.descriptor()).unwrap();
    let metadata = seed
        .metadata_for_actual_at(
            codec.clone(),
            &cache_scope,
            crate::command_ledger::CausationId::parse_stored(TEST_CAUSATION_ID.into()).unwrap(),
            std::time::Duration::from_secs(60),
            &[occurrence],
            &[selector],
            std::time::UNIX_EPOCH + std::time::Duration::from_millis(completion_unix_ms),
        )
        .unwrap();
    assert_eq!(metadata.issued_at_unix_ms, completion_unix_ms);
    assert_eq!(metadata.expires_at_unix_ms, completion_unix_ms + 60_000);
    assert!(
        issued_at_unix_ms + 60_000 < completion_unix_ms,
        "the deterministic handler delay must enter the old request-relative expiry tail"
    );
    let forged_link_model = CommandProjectionMetadataV1::try_new(
        metadata.issued_at_unix_ms,
        metadata.expires_at_unix_ms,
        metadata.delta.clone(),
        metadata.lifecycle_proofs.clone(),
        vec![crate::graphql::protocol::CommandProjectionObligationV1 {
            projection_ref: metadata.obligations[0].projection_ref,
            // Link/Unlink may observe a physical join output that the
            // role-safe delta deliberately does not expose. Structural
            // decoding therefore permits the label, but the exact mounted
            // binding must reject this forged non-output model before emit.
            model: "ForgedJoinOutput".into(),
            scope_token: metadata.obligations[0].scope_token.clone(),
        }],
        metadata.revalidate,
    )
    .unwrap();
    assert!(
        accumulator(seed.clone(), codec.clone(), cache_scope.clone())
            .record_status(&status(forged_link_model))
            .is_err()
    );

    let zero_obligation = CommandProjectionMetadataV1::try_new(
        metadata.issued_at_unix_ms,
        metadata.expires_at_unix_ms,
        metadata.delta.clone(),
        metadata.lifecycle_proofs.clone(),
        Vec::new(),
        metadata.revalidate,
    )
    .unwrap();

    accumulator(seed.clone(), codec.clone(), cache_scope.clone())
        .record_status(&status(zero_obligation.clone()))
        .unwrap();
    accumulator(seed.clone(), codec.clone(), cache_scope.clone())
        .record_receipt(&receipt(zero_obligation.clone()))
        .unwrap();

    let stale_seed = ProtocolProjectionRequestSeed::new(
        export,
        registry,
        crate::command_ledger::PrincipalPartitionId::new("modeled-status-principal").unwrap(),
        "changed-modeled-status-auth",
        Vec::new(),
        issued_at_unix_ms,
    )
    .unwrap();
    assert!(accumulator(stale_seed, codec.clone(), cache_scope.clone())
        .record_status(&status(zero_obligation.clone()))
        .is_err());

    let expired = CommandProjectionMetadataV1::try_new(
        1,
        2,
        zero_obligation.delta.clone(),
        zero_obligation.lifecycle_proofs.clone(),
        Vec::new(),
        zero_obligation.revalidate,
    )
    .unwrap();
    assert!(accumulator(seed, codec, cache_scope)
        .record_status(&status(expired))
        .is_err());
}

#[test]
#[cfg(feature = "graphql")]
fn zero_occurrence_metadata_is_classified_from_the_current_causal_command_contract() {
    use std::sync::Arc;

    use super::runtime::{
        ModeledProjectionStatusDisposition, ProtocolProjectionProgramRegistry,
        ProtocolProjectionRequestSeed,
    };
    use crate::graphql::protocol::{
        DistributedEnvelopeV1, ProtocolResponseAccumulator, ProtocolTokenCodec,
        ProtocolTokenPurpose,
    };
    use crate::microsvc::CausalCommandReceiptSource;

    fn seed(fixture: &ModeledFixture, now_unix_ms: u64) -> ProtocolProjectionRequestSeed {
        ProtocolProjectionRequestSeed::new(
            selected_export(&fixture.surface),
            Arc::new(
                ProtocolProjectionProgramRegistry::try_from_surface(&fixture.surface).unwrap(),
            ),
            crate::command_ledger::PrincipalPartitionId::new("zero-occurrence-principal").unwrap(),
            "zero-occurrence-generation",
            Vec::new(),
            now_unix_ms,
        )
        .unwrap()
    }

    fn receipt(
        metadata: crate::graphql::protocol::CommandProjectionMetadataV1,
    ) -> CausalCommandReceiptSource {
        CausalCommandReceiptSource {
            command_id: "0190a000-0000-7000-8000-000000000012".into(),
            command_name: TEST_COMMAND_NAME.into(),
            causation_id: TEST_CAUSATION_ID.into(),
            consistency: crate::command::CommandConsistency::Eventual,
            state: crate::command_ledger::CommandLedgerState::Succeeded,
            outcome: serde_json::json!({"accepted": true}),
            obligations: Vec::new(),
            projection_metadata: Some(metadata),
            direct_projection: None,
        }
    }

    fn accumulator(
        seed: ProtocolProjectionRequestSeed,
        codec: ProtocolTokenCodec,
        cache_scope: crate::graphql::protocol::OpaqueProtocolToken,
    ) -> ProtocolResponseAccumulator {
        let accumulator = ProtocolResponseAccumulator::new(
            DistributedEnvelopeV1::new(
                "sha256:zero-occurrence",
                "zero-occurrence-generation",
                cache_scope,
                None,
            ),
            codec,
        );
        accumulator.bind_projection_request(seed).unwrap();
        accumulator
    }

    let now = std::time::SystemTime::now();
    let now_unix_ms = now.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let codec = ProtocolTokenCodec::new([0x6d; 32]);
    let active_cache = codec
        .issue(
            ProtocolTokenPurpose::CacheScope,
            &("zero-occurrence", "active"),
        )
        .unwrap();
    let active = modeled_fixture(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let active_seed = seed(&active, now_unix_ms);
    let active_metadata = active_seed
        .metadata_for_actual_at(
            codec.clone(),
            &active_cache,
            crate::command_ledger::CausationId::parse_stored(TEST_CAUSATION_ID.into()).unwrap(),
            Duration::from_secs(60),
            &[],
            &[],
            now,
        )
        .unwrap();
    assert!(active_metadata.delta.projections.is_empty());
    assert!(active_metadata.delta.occurrences.is_empty());
    assert!(active_metadata.delta.operations.is_empty());
    assert_eq!(
        active_seed
            .modeled_evidence_topologies(
                &codec,
                &active_cache,
                TEST_COMMAND_NAME,
                TEST_CAUSATION_ID,
                &active_metadata,
            )
            .unwrap()
            .disposition,
        ModeledProjectionStatusDisposition::Applicable
    );
    assert!(active_seed
        .modeled_evidence_topologies(
            &codec,
            &active_cache,
            "todo.unrelated",
            TEST_CAUSATION_ID,
            &active_metadata,
        )
        .is_err());
    let active_accumulator = accumulator(active_seed, codec.clone(), active_cache.clone());
    active_accumulator
        .record_receipt(&receipt(active_metadata.clone()))
        .unwrap();
    let active_command =
        serde_json::to_value(active_accumulator.snapshot().unwrap()).unwrap()["command"].clone();
    assert!(active_command.get("projectionDisposition").is_none());
    assert_eq!(
        active_command["projection"]["delta"]["operations"],
        serde_json::json!([])
    );

    let draining_cache = codec
        .issue(
            ProtocolTokenPurpose::CacheScope,
            &("zero-occurrence", "draining"),
        )
        .unwrap();
    let draining = modeled_fixture(
        ProjectionBindingState::Draining,
        ProjectionExecutionClass::Causal,
    );
    let draining_seed = seed(&draining, now_unix_ms);
    let draining_metadata = draining_seed
        .metadata_for_actual_at(
            codec.clone(),
            &draining_cache,
            crate::command_ledger::CausationId::parse_stored(TEST_CAUSATION_ID.into()).unwrap(),
            Duration::from_secs(60),
            &[],
            &[],
            now,
        )
        .unwrap();
    assert_eq!(
        draining_seed
            .modeled_evidence_topologies(
                &codec,
                &draining_cache,
                TEST_COMMAND_NAME,
                TEST_CAUSATION_ID,
                &draining_metadata,
            )
            .unwrap()
            .disposition,
        ModeledProjectionStatusDisposition::Revalidate
    );
    assert_eq!(
        draining_seed
            .modeled_evidence_topologies(
                &codec,
                &draining_cache,
                TEST_COMMAND_NAME,
                TEST_CAUSATION_ID,
                &active_metadata,
            )
            .unwrap()
            .disposition,
        ModeledProjectionStatusDisposition::Revalidate,
        "scope-drift empty metadata is safe only as revalidation"
    );
    let draining_accumulator = accumulator(draining_seed, codec, draining_cache);
    draining_accumulator
        .record_receipt(&receipt(draining_metadata))
        .unwrap();
    let draining_command =
        serde_json::to_value(draining_accumulator.snapshot().unwrap()).unwrap()["command"].clone();
    assert_eq!(draining_command["projectionDisposition"], "revalidate");
    assert!(draining_command.get("projection").is_none());
    assert_eq!(draining_command["expects"], serde_json::json!([]));
}

#[test]
#[cfg(feature = "graphql")]
fn opaque_fallback_hint_does_not_hide_empty_draining_modeled_work() {
    use std::sync::Arc;

    use super::runtime::{
        ModeledProjectionStatusDisposition, ProtocolProjectionProgramRegistry,
        ProtocolProjectionRequestSeed,
    };
    use crate::graphql::protocol::{ProtocolTokenCodec, ProtocolTokenPurpose};

    let now = std::time::SystemTime::now();
    let now_unix_ms = now.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let codec = ProtocolTokenCodec::new([0x6e; 32]);
    let occurrence = state_occurrence(12, "todo-opaque-fallback", "opaque");
    let selector =
        crate::ProjectionEventSelector::try_from_descriptor(occurrence.descriptor()).unwrap();

    let active = modeled_fixture_with_opaque_fallback(ProjectionBindingState::Active);
    let active_seed = ProtocolProjectionRequestSeed::new(
        selected_export(&active.surface),
        Arc::new(ProtocolProjectionProgramRegistry::try_from_surface(&active.surface).unwrap()),
        crate::command_ledger::PrincipalPartitionId::new("opaque-fallback-principal").unwrap(),
        "opaque-fallback-generation",
        Vec::new(),
        now_unix_ms,
    )
    .unwrap();
    let active_cache = codec
        .issue(
            ProtocolTokenPurpose::CacheScope,
            &("opaque-fallback", "active"),
        )
        .unwrap();
    let active_metadata = active_seed
        .metadata_for_actual_at(
            codec.clone(),
            &active_cache,
            crate::command_ledger::CausationId::parse_stored(TEST_CAUSATION_ID.into()).unwrap(),
            Duration::from_secs(60),
            std::slice::from_ref(&occurrence),
            std::slice::from_ref(&selector),
            now,
        )
        .unwrap();
    assert!(
        active_metadata.revalidate,
        "the opaque owner retains its conservative fallback hint"
    );
    assert!(!active_metadata.delta.operations.is_empty());
    assert_eq!(
        active_seed
            .modeled_evidence_topologies(
                &codec,
                &active_cache,
                TEST_COMMAND_NAME,
                TEST_CAUSATION_ID,
                &active_metadata,
            )
            .unwrap()
            .disposition,
        ModeledProjectionStatusDisposition::Applicable
    );

    let draining = modeled_fixture_with_opaque_fallback(ProjectionBindingState::Draining);
    let draining_seed = ProtocolProjectionRequestSeed::new(
        selected_export(&draining.surface),
        Arc::new(ProtocolProjectionProgramRegistry::try_from_surface(&draining.surface).unwrap()),
        crate::command_ledger::PrincipalPartitionId::new("opaque-fallback-principal").unwrap(),
        "opaque-fallback-generation",
        Vec::new(),
        now_unix_ms,
    )
    .unwrap();
    let draining_cache = codec
        .issue(
            ProtocolTokenPurpose::CacheScope,
            &("opaque-fallback", "draining"),
        )
        .unwrap();
    let draining_metadata = draining_seed
        .metadata_for_actual_at(
            codec.clone(),
            &draining_cache,
            crate::command_ledger::CausationId::parse_stored(TEST_CAUSATION_ID.into()).unwrap(),
            Duration::from_secs(60),
            &[occurrence],
            &[selector],
            now,
        )
        .unwrap();
    assert!(draining_metadata.revalidate);
    assert!(draining_metadata.delta.projections.is_empty());
    assert!(draining_metadata.delta.occurrences.is_empty());
    assert!(draining_metadata.delta.operations.is_empty());
    assert!(draining_metadata.lifecycle_proofs.is_empty());
    assert!(draining_metadata.obligations.is_empty());
    assert_eq!(
        draining_seed
            .modeled_evidence_topologies(
                &codec,
                &draining_cache,
                TEST_COMMAND_NAME,
                TEST_CAUSATION_ID,
                &draining_metadata,
            )
            .unwrap()
            .disposition,
        ModeledProjectionStatusDisposition::Revalidate
    );
}

#[test]
#[cfg(feature = "graphql")]
fn mounted_registry_derives_one_exact_physical_obligation_from_actual_fanout_ops() {
    use super::runtime::{
        ProjectionRuntimeAuthorityError, ProtocolProjectionDeltaRequestAuthority,
        ProtocolProjectionProgramRegistry,
    };
    use crate::graphql::protocol::{ProtocolTokenCodec, ProtocolTokenPurpose};

    let fixture = modeled_fixture(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let registry = ProtocolProjectionProgramRegistry::try_from_surface(&fixture.surface).unwrap();
    let selected = selected_export(&fixture.surface);
    let codec = ProtocolTokenCodec::new([0x6b; 32]);
    let cache_scope = codec
        .issue(ProtocolTokenPurpose::CacheScope, &("registry-actual", 1))
        .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let request = ProtocolProjectionDeltaRequestAuthority::try_new(
        selected,
        codec,
        crate::command_ledger::PrincipalPartitionId::new("registry-principal").unwrap(),
        "registry-generation",
        &cache_scope,
        crate::command_ledger::CausationId::parse_stored(TEST_CAUSATION_ID.to_owned()).unwrap(),
        now,
        now,
        now + 10_000,
    )
    .unwrap();
    let occurrence = state_occurrence(11, "todo-registry", "actual");
    let selector =
        crate::ProjectionEventSelector::try_from_descriptor(&event_descriptor()).unwrap();
    let metadata = registry
        .metadata_for_actual(
            &request,
            std::slice::from_ref(&occurrence),
            std::slice::from_ref(&selector),
        )
        .unwrap();
    let oversized = vec![occurrence; super::types::MAX_PROJECTION_DELTA_OPERATIONS + 1];
    assert!(matches!(
        registry.metadata_for_actual(&request, &oversized, &[selector]),
        Err(ProjectionRuntimeAuthorityError::Delta(
            ProjectionDeltaError::TooManyOccurrences { len: 129, max: 128 }
        ))
    ));

    assert!(metadata.delta.operations.iter().any(is_record_operation));
    assert!(metadata.delta.operations.iter().any(|operation| matches!(
        operation.mutation,
        ProjectionDeltaMutation::Link { .. } | ProjectionDeltaMutation::Unlink { .. }
    )));
    assert_eq!(metadata.obligations.len(), 1);
    assert_eq!(metadata.obligations[0].model, "TodoView");
    assert!(metadata.obligations[0]
        .scope_token
        .as_str()
        .starts_with("v1.projection-obligation."));
}

#[test]
#[cfg(feature = "graphql")]
fn draining_actual_delta_can_apply_but_cannot_mint_new_obligations() {
    use super::runtime::{
        ProjectionRuntimeAuthorityError, ProtocolProjectionDeltaRequestAuthority,
    };
    use crate::graphql::protocol::{ProtocolTokenCodec, ProtocolTokenPurpose};

    let fixture = modeled_fixture(
        ProjectionBindingState::Draining,
        ProjectionExecutionClass::Causal,
    );
    let selected = selected_export(&fixture.surface);
    let codec = ProtocolTokenCodec::new([0x5b; 32]);
    let cache_scope = codec
        .issue(ProtocolTokenPurpose::CacheScope, &("draining", 1))
        .unwrap();
    let request = ProtocolProjectionDeltaRequestAuthority::try_new(
        selected,
        codec,
        crate::command_ledger::PrincipalPartitionId::new("principal-draining").unwrap(),
        "auth-generation-7",
        &cache_scope,
        crate::command_ledger::CausationId::parse_stored(TEST_CAUSATION_ID.to_owned()).unwrap(),
        1_000,
        1_000,
        2_000,
    )
    .unwrap();
    let authority = ProjectionDeltaAuthority::try_new(request.export(), &request).unwrap();
    let modeled = request.export().surface().projectors[0].modeled[0].clone();
    let source = authority.source(&modeled).unwrap();
    let plan = crate::ResolvedProjectionPlan::resolve(
        &fixture.program,
        &state_occurrence(7, "todo-1", "title"),
    )
    .unwrap();
    let occurrence = ProjectionDeltaPlanOccurrence::actual(vec![(&source, &plan)]).unwrap();
    let delta = authority.lower(&[occurrence]).unwrap();

    assert!(matches!(
        request.metadata(delta, Vec::new(), &[&modeled], &[], false),
        Err(ProjectionRuntimeAuthorityError::IneligibleProjection)
    ));
}

#[test]
#[cfg(feature = "graphql")]
fn active_metadata_revalidates_but_remains_queryable_while_projection_is_draining() {
    use std::sync::Arc;

    use super::runtime::{
        ModeledProjectionEvidence, ProtocolProjectionDeltaRequestAuthority,
        ProtocolProjectionProgramRegistry, ProtocolProjectionRequestSeed,
    };
    use crate::graphql::protocol::{
        DistributedEnvelopeV1, ProtocolResponseAccumulator, ProtocolTokenCodec,
        ProtocolTokenPurpose,
    };
    use crate::microsvc::{
        CausalCommandProjectionEvidence, CausalCommandPublicState, CausalCommandPublicStatus,
        CausalCommandReceiptSource, CausalProjectionEvidenceState,
    };
    use crate::projection_protocol::ProjectionCausationEvidenceBatch;

    let now = std::time::SystemTime::now();
    let now_unix_ms = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let codec = ProtocolTokenCodec::new([0x5c; 32]);
    let cache_scope = codec
        .issue(ProtocolTokenPurpose::CacheScope, &("active-to-draining", 1))
        .unwrap();
    let principal =
        crate::command_ledger::PrincipalPartitionId::new("active-to-draining-principal").unwrap();
    let active = modeled_fixture(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let active_seed = ProtocolProjectionRequestSeed::new(
        selected_export(&active.surface),
        Arc::new(ProtocolProjectionProgramRegistry::try_from_surface(&active.surface).unwrap()),
        principal.clone(),
        "active-to-draining-generation",
        Vec::new(),
        now_unix_ms,
    )
    .unwrap();
    let occurrence = state_occurrence(11, "todo-draining-status", "active");
    let selector =
        crate::ProjectionEventSelector::try_from_descriptor(occurrence.descriptor()).unwrap();
    let metadata = active_seed
        .metadata_for_actual_at(
            codec.clone(),
            &cache_scope,
            crate::command_ledger::CausationId::parse_stored(TEST_CAUSATION_ID.into()).unwrap(),
            Duration::from_secs(60),
            std::slice::from_ref(&occurrence),
            std::slice::from_ref(&selector),
            now,
        )
        .unwrap();
    assert_eq!(metadata.obligations.len(), 1);

    let draining_cache_scope = codec
        .issue(ProtocolTokenPurpose::CacheScope, &("active-to-draining", 2))
        .unwrap();
    assert_ne!(cache_scope, draining_cache_scope);
    assert!(matches!(
        active_seed.modeled_evidence_topologies(
            &codec,
            &draining_cache_scope,
            TEST_COMMAND_NAME,
            TEST_CAUSATION_ID,
            &metadata,
        ),
        Err(super::runtime::ProjectionRuntimeAuthorityError::IneligibleProjection)
    ));

    let draining = modeled_fixture(
        ProjectionBindingState::Draining,
        ProjectionExecutionClass::Causal,
    );
    let draining_export = selected_export(&draining.surface);
    let current_manifest = draining_export.manifest().unwrap();
    let draining_registry =
        Arc::new(ProtocolProjectionProgramRegistry::try_from_surface(&draining.surface).unwrap());
    let draining_seed = ProtocolProjectionRequestSeed::new(
        draining_export.clone(),
        Arc::clone(&draining_registry),
        principal.clone(),
        "active-to-draining-generation",
        Vec::new(),
        now_unix_ms,
    )
    .unwrap();
    let same_cache_plan = draining_seed
        .modeled_evidence_topologies(
            &codec,
            &cache_scope,
            TEST_COMMAND_NAME,
            TEST_CAUSATION_ID,
            &metadata,
        )
        .unwrap();
    assert_eq!(
        same_cache_plan.disposition,
        super::runtime::ModeledProjectionStatusDisposition::Revalidate,
        "Draining remains lifecycle-visible but cannot authorize delta application even under an exact outer scope"
    );
    let current_request = ProtocolProjectionDeltaRequestAuthority::try_new(
        draining_export.clone(),
        codec.clone(),
        principal,
        "active-to-draining-generation",
        &cache_scope,
        crate::command_ledger::CausationId::parse_stored(TEST_CAUSATION_ID.into()).unwrap(),
        now_unix_ms,
        metadata.issued_at_unix_ms,
        metadata.expires_at_unix_ms,
    )
    .unwrap();
    let mut exact_scope_metadata = metadata.clone();
    exact_scope_metadata.delta.identity.schema_fingerprint =
        current_manifest.schema_fingerprint.clone();
    exact_scope_metadata.delta.identity.cache_scope_token = cache_scope.as_str().to_owned();
    let exact_projection = draining_export
        .surface()
        .projection_owners()
        .iter()
        .flat_map(|owner| &owner.modeled)
        .find(|projection| {
            projection.program_id().to_string()
                == exact_scope_metadata.delta.projections[0].program_id
                && projection.binding_id().to_string()
                    == exact_scope_metadata.delta.projections[0].binding_id
        })
        .unwrap();
    exact_scope_metadata.lifecycle_proofs = draining_registry
        .lifecycle_proofs_for_test(
            &current_request,
            &exact_scope_metadata.delta,
            &[exact_projection],
        )
        .unwrap();
    assert_eq!(
        draining_seed
            .modeled_evidence_topologies(
                &codec,
                &cache_scope,
                TEST_COMMAND_NAME,
                TEST_CAUSATION_ID,
                &exact_scope_metadata,
            )
            .unwrap()
            .disposition,
        super::runtime::ModeledProjectionStatusDisposition::Revalidate,
        "an exact-scope authenticated Draining receipt remains lifecycle-only"
    );
    let exact_scope_accumulator = ProtocolResponseAccumulator::new(
        DistributedEnvelopeV1::new(
            current_manifest.schema_fingerprint.clone(),
            "active-to-draining-generation",
            cache_scope.clone(),
            None,
        ),
        codec.clone(),
    );
    exact_scope_accumulator
        .bind_projection_request(draining_seed.clone())
        .unwrap();
    exact_scope_accumulator
        .record_receipt(&CausalCommandReceiptSource {
            command_id: "0190a000-0000-7000-8000-000000000018".into(),
            command_name: TEST_COMMAND_NAME.into(),
            causation_id: TEST_CAUSATION_ID.into(),
            consistency: crate::command::CommandConsistency::Eventual,
            state: crate::command_ledger::CommandLedgerState::Atomic,
            outcome: serde_json::json!({"id": "todo-draining-status"}),
            obligations: Vec::new(),
            projection_metadata: Some(exact_scope_metadata),
            direct_projection: None,
        })
        .unwrap();
    let exact_scope_command = serde_json::to_value(exact_scope_accumulator.snapshot().unwrap())
        .unwrap()["command"]
        .clone();
    assert_eq!(exact_scope_command["projectionDisposition"], "revalidate");
    assert_eq!(exact_scope_command["expects"], serde_json::json!([]));
    assert!(exact_scope_command.get("projection").is_none());
    let plan = draining_seed
        .modeled_evidence_topologies(
            &codec,
            &draining_cache_scope,
            TEST_COMMAND_NAME,
            TEST_CAUSATION_ID,
            &metadata,
        )
        .unwrap();
    assert_eq!(plan.topologies.len(), 1);
    assert_eq!(plan.topologies[0].name(), "projection-delta-test");
    assert_eq!(
        plan.disposition,
        super::runtime::ModeledProjectionStatusDisposition::Revalidate
    );
    assert_eq!(
        draining_seed
            .modeled_evidence(
                &codec,
                &draining_cache_scope,
                TEST_COMMAND_NAME,
                TEST_CAUSATION_ID,
                &metadata,
                &ProjectionCausationEvidenceBatch::default(),
                plan.disposition,
            )
            .unwrap(),
        vec![ModeledProjectionEvidence::Pending]
    );
    let accumulator = ProtocolResponseAccumulator::new(
        DistributedEnvelopeV1::new(
            current_manifest.schema_fingerprint.clone(),
            "active-to-draining-generation",
            draining_cache_scope.clone(),
            None,
        ),
        codec.clone(),
    );
    accumulator
        .bind_projection_request(draining_seed.clone())
        .unwrap();
    accumulator
        .record_status(&CausalCommandPublicStatus {
            state: CausalCommandPublicState::SucceededPendingProjection,
            command_id: "0190a000-0000-7000-8000-000000000019".into(),
            command_name: Some(TEST_COMMAND_NAME.into()),
            causation_id: Some(TEST_CAUSATION_ID.into()),
            consistency: Some(crate::command::CommandConsistency::Eventual),
            outcome: None,
            obligations: Vec::new(),
            projection_metadata: Some(metadata.clone()),
            projection_revalidate: true,
            evidence: vec![CausalCommandProjectionEvidence {
                obligation_index: 0,
                state: CausalProjectionEvidenceState::Pending,
                incarnation: None,
                revision: None,
            }],
            direct_projection: None,
        })
        .unwrap();
    let command = serde_json::to_value(accumulator.snapshot().unwrap()).unwrap()["command"].clone();
    assert_eq!(command["projectionDisposition"], "revalidate");
    assert_eq!(command["expects"], serde_json::json!([]));
    assert!(command.get("projection").is_none());
    assert!(command.get("observations").is_none());
    assert!(command.get("records").is_none());

    let replay_accumulator = ProtocolResponseAccumulator::new(
        DistributedEnvelopeV1::new(
            current_manifest.schema_fingerprint,
            "active-to-draining-generation",
            draining_cache_scope.clone(),
            None,
        ),
        codec.clone(),
    );
    replay_accumulator
        .bind_projection_request(draining_seed.clone())
        .unwrap();
    replay_accumulator
        .record_receipt(&CausalCommandReceiptSource {
            command_id: "0190a000-0000-7000-8000-000000000019".into(),
            command_name: TEST_COMMAND_NAME.into(),
            causation_id: TEST_CAUSATION_ID.into(),
            consistency: crate::command::CommandConsistency::Eventual,
            state: crate::command_ledger::CommandLedgerState::SucceededPendingProjection,
            outcome: serde_json::json!({"id": "todo-draining-status"}),
            obligations: Vec::new(),
            projection_metadata: Some(metadata),
            direct_projection: None,
        })
        .unwrap();
    let replay_command =
        serde_json::to_value(replay_accumulator.snapshot().unwrap()).unwrap()["command"].clone();
    assert_eq!(replay_command["projectionDisposition"], "revalidate");
    assert_eq!(replay_command["expects"], serde_json::json!([]));
    assert!(replay_command.get("projection").is_none());
    assert!(replay_command.get("observations").is_none());
    assert!(replay_command.get("records").is_none());

    let draining_metadata = draining_seed
        .metadata_for_actual_at(
            codec,
            &draining_cache_scope,
            crate::command_ledger::CausationId::parse_stored(TEST_CAUSATION_ID.into()).unwrap(),
            Duration::from_secs(60),
            &[state_occurrence(12, "todo-draining-status", "draining")],
            &[selector],
            now,
        )
        .unwrap();
    assert!(draining_metadata.obligations.is_empty());
    assert!(draining_metadata.delta.projections.is_empty());
}

#[test]
#[cfg(feature = "graphql")]
fn lifecycle_status_rejects_changed_deployment_identity_and_mixed_fanout_tampering() {
    use std::sync::Arc;

    use super::runtime::{
        ModeledProjectionStatusDisposition, ProjectionRuntimeAuthorityError,
        ProtocolProjectionProgramRegistry, ProtocolProjectionRequestSeed,
    };
    use crate::graphql::client_manifest::ClientExecutionLimits;
    use crate::graphql::protocol::{
        DistributedEnvelopeV1, ProtocolResponseAccumulator, ProtocolTokenCodec, ProtocolTokenError,
        ProtocolTokenPurpose,
    };
    use crate::microsvc::CausalCommandReceiptSource;

    fn receipt_is_rejected(
        seed: ProtocolProjectionRequestSeed,
        codec: ProtocolTokenCodec,
        cache_scope: crate::graphql::protocol::OpaqueProtocolToken,
        metadata: crate::graphql::protocol::CommandProjectionMetadataV1,
    ) -> bool {
        let accumulator = ProtocolResponseAccumulator::new(
            DistributedEnvelopeV1::new(
                "sha256:lifecycle-hostile-current",
                "lifecycle-hostile-generation",
                cache_scope,
                None,
            ),
            codec,
        );
        accumulator.bind_projection_request(seed).unwrap();
        accumulator
            .record_receipt(&CausalCommandReceiptSource {
                command_id: "0190a000-0000-7000-8000-000000000020".into(),
                command_name: TEST_COMMAND_NAME.into(),
                causation_id: TEST_CAUSATION_ID.into(),
                consistency: crate::command::CommandConsistency::Eventual,
                state: crate::command_ledger::CommandLedgerState::SucceededPendingProjection,
                outcome: serde_json::json!({"id": "todo-lifecycle-hostile"}),
                obligations: Vec::new(),
                projection_metadata: Some(metadata),
                direct_projection: None,
            })
            .is_err()
    }

    let now = std::time::SystemTime::now();
    let now_unix_ms = now.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let codec = ProtocolTokenCodec::new([0x5d; 32]);
    let old_cache = codec
        .issue(ProtocolTokenPurpose::CacheScope, &("lifecycle-hostile", 1))
        .unwrap();
    let current_cache = codec
        .issue(ProtocolTokenPurpose::CacheScope, &("lifecycle-hostile", 2))
        .unwrap();
    let principal =
        crate::command_ledger::PrincipalPartitionId::new("lifecycle-hostile-principal").unwrap();
    let active = modeled_fixture(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let active_seed = ProtocolProjectionRequestSeed::new(
        selected_export(&active.surface),
        Arc::new(ProtocolProjectionProgramRegistry::try_from_surface(&active.surface).unwrap()),
        principal.clone(),
        "lifecycle-hostile-generation",
        Vec::new(),
        now_unix_ms,
    )
    .unwrap();
    let occurrence = state_occurrence(21, "todo-lifecycle-hostile", "active");
    let selector =
        crate::ProjectionEventSelector::try_from_descriptor(occurrence.descriptor()).unwrap();
    let metadata = active_seed
        .metadata_for_actual_at(
            codec.clone(),
            &old_cache,
            crate::command_ledger::CausationId::parse_stored(TEST_CAUSATION_ID.into()).unwrap(),
            Duration::from_secs(60),
            std::slice::from_ref(&occurrence),
            std::slice::from_ref(&selector),
            now,
        )
        .unwrap();

    let draining = modeled_fixture(
        ProjectionBindingState::Draining,
        ProjectionExecutionClass::Causal,
    );
    let draining_export = selected_export(&draining.surface);
    let baseline_manifest = draining_export.manifest().unwrap();
    let mut changed_limits = ClientExecutionLimits::default();
    changed_limits.max_bool_width += 1;
    let changed_schema_export = DistributedClientSurfaceExport::from_selected_with_execution(
        "delta-service",
        draining_export.surface().clone(),
        changed_limits,
    )
    .unwrap();
    let changed_schema_manifest = changed_schema_export.manifest().unwrap();
    assert_ne!(
        baseline_manifest.schema_fingerprint,
        changed_schema_manifest.schema_fingerprint
    );
    assert_eq!(
        baseline_manifest.protocol_fingerprint,
        changed_schema_manifest.protocol_fingerprint
    );
    let changed_schema_seed = ProtocolProjectionRequestSeed::new(
        changed_schema_export,
        Arc::new(ProtocolProjectionProgramRegistry::try_from_surface(&draining.surface).unwrap()),
        principal.clone(),
        "lifecycle-hostile-generation",
        Vec::new(),
        now_unix_ms,
    )
    .unwrap();
    assert_eq!(
        changed_schema_seed
            .modeled_evidence_topologies(
                &codec,
                &old_cache,
                TEST_COMMAND_NAME,
                TEST_CAUSATION_ID,
                &metadata,
            )
            .unwrap()
            .disposition,
        ModeledProjectionStatusDisposition::Revalidate
    );

    for changed in [
        modeled_fixture_deployment(
            ProjectionBindingState::Draining,
            ProjectionExecutionClass::Causal,
            ProjectionMutationKind::Upsert,
            false,
            "changed-route",
            "projection-delta-test",
            [0x44; 32],
        ),
        modeled_fixture_deployment(
            ProjectionBindingState::Draining,
            ProjectionExecutionClass::Causal,
            ProjectionMutationKind::Upsert,
            false,
            "delta-service",
            "changed-topology",
            [0x47; 32],
        ),
    ] {
        let seed = ProtocolProjectionRequestSeed::new(
            selected_export(&changed.surface),
            Arc::new(
                ProtocolProjectionProgramRegistry::try_from_surface(&changed.surface).unwrap(),
            ),
            principal.clone(),
            "lifecycle-hostile-generation",
            Vec::new(),
            now_unix_ms,
        )
        .unwrap();
        let result = seed.modeled_evidence_topologies(
            &codec,
            &current_cache,
            TEST_COMMAND_NAME,
            TEST_CAUSATION_ID,
            &metadata,
        );
        assert!(
            matches!(
                result,
                Err(
                    ProjectionRuntimeAuthorityError::Token(ProtocolTokenError::Mismatch)
                        | ProjectionRuntimeAuthorityError::IneligibleProjection
                        | ProjectionRuntimeAuthorityError::InvalidAuthority
                )
            ),
            "{result:?}"
        );
        assert!(receipt_is_rejected(
            seed,
            codec.clone(),
            current_cache.clone(),
            metadata.clone(),
        ));
    }

    let removed_surface = build_surface(&[todos(), users()], &SurfaceOptions::sqlite()).unwrap();
    let removed_seed = ProtocolProjectionRequestSeed::new(
        selected_export(&removed_surface),
        Arc::new(ProtocolProjectionProgramRegistry::try_from_surface(&removed_surface).unwrap()),
        principal.clone(),
        "lifecycle-hostile-generation",
        Vec::new(),
        now_unix_ms,
    )
    .unwrap();
    assert!(removed_seed
        .modeled_evidence_topologies(
            &codec,
            &current_cache,
            TEST_COMMAND_NAME,
            TEST_CAUSATION_ID,
            &metadata,
        )
        .is_err());
    assert!(receipt_is_rejected(
        removed_seed,
        codec.clone(),
        current_cache.clone(),
        metadata.clone(),
    ));

    let draining_seed = ProtocolProjectionRequestSeed::new(
        selected_export(&draining.surface),
        Arc::new(ProtocolProjectionProgramRegistry::try_from_surface(&draining.surface).unwrap()),
        principal.clone(),
        "lifecycle-hostile-generation",
        Vec::new(),
        now_unix_ms,
    )
    .unwrap();
    let mut tampered = metadata.clone();
    tampered.lifecycle_proofs[0].token = codec
        .issue(ProtocolTokenPurpose::ProjectionLifecycle, &("tampered", 1))
        .unwrap();
    assert!(matches!(
        draining_seed.modeled_evidence_topologies(
            &codec,
            &current_cache,
            TEST_COMMAND_NAME,
            TEST_CAUSATION_ID,
            &tampered,
        ),
        Err(ProjectionRuntimeAuthorityError::Token(
            ProtocolTokenError::Mismatch
        ))
    ));
    assert!(receipt_is_rejected(
        draining_seed,
        codec.clone(),
        current_cache.clone(),
        tampered,
    ));

    let fanout_active = fanout_fixture();
    let fanout_active_seed = ProtocolProjectionRequestSeed::new(
        selected_export(&fanout_active.surface),
        Arc::new(
            ProtocolProjectionProgramRegistry::try_from_surface(&fanout_active.surface).unwrap(),
        ),
        principal.clone(),
        "lifecycle-hostile-generation",
        Vec::new(),
        now_unix_ms,
    )
    .unwrap();
    let fanout_occurrence = state_occurrence(22, "todo-lifecycle-fanout", "active");
    let fanout_selector =
        crate::ProjectionEventSelector::try_from_descriptor(fanout_occurrence.descriptor())
            .unwrap();
    let fanout_metadata = fanout_active_seed
        .metadata_for_actual_at(
            codec.clone(),
            &old_cache,
            crate::command_ledger::CausationId::parse_stored(TEST_CAUSATION_ID.into()).unwrap(),
            Duration::from_secs(60),
            std::slice::from_ref(&fanout_occurrence),
            std::slice::from_ref(&fanout_selector),
            now,
        )
        .unwrap();
    assert_eq!(fanout_metadata.delta.projections.len(), 2);
    let mixed = fanout_fixture_states(
        ProjectionBindingState::Active,
        ProjectionBindingState::Draining,
    );
    let mixed_seed = ProtocolProjectionRequestSeed::new(
        selected_export(&mixed.surface),
        Arc::new(ProtocolProjectionProgramRegistry::try_from_surface(&mixed.surface).unwrap()),
        principal,
        "lifecycle-hostile-generation",
        Vec::new(),
        now_unix_ms,
    )
    .unwrap();
    assert_eq!(
        mixed_seed
            .modeled_evidence_topologies(
                &codec,
                &current_cache,
                TEST_COMMAND_NAME,
                TEST_CAUSATION_ID,
                &fanout_metadata,
            )
            .unwrap()
            .disposition,
        ModeledProjectionStatusDisposition::Revalidate
    );
    let mut mixed_tampered = fanout_metadata;
    mixed_tampered.lifecycle_proofs[0].token = codec
        .issue(
            ProtocolTokenPurpose::ProjectionLifecycle,
            &("tampered-active-leg", 1),
        )
        .unwrap();
    assert!(matches!(
        mixed_seed.modeled_evidence_topologies(
            &codec,
            &current_cache,
            TEST_COMMAND_NAME,
            TEST_CAUSATION_ID,
            &mixed_tampered,
        ),
        Err(ProjectionRuntimeAuthorityError::Token(
            ProtocolTokenError::Mismatch
        ))
    ));
    assert!(receipt_is_rejected(
        mixed_seed,
        codec,
        current_cache,
        mixed_tampered,
    ));
}

#[test]
fn full_state_preview_lowers_to_upsert() {
    let fixture = modeled_fixture(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let selected = selected_export(&fixture.surface);
    let request = TestRequestAuthority::authorized("auth-generation-7");
    let authority = ProjectionDeltaAuthority::try_new(&selected, &request).unwrap();
    let modeled = selected.surface().projectors[0].modeled[0].clone();
    let source = authority.source(&modeled).unwrap();
    let plan = crate::ResolvedProjectionPlan::resolve(
        &fixture.program,
        &state_occurrence(7, "todo-1", "preview"),
    )
    .unwrap();
    let occurrence = ProjectionDeltaPlanOccurrence::preview(vec![(&source, &plan)]).unwrap();
    let delta = authority.lower(&[occurrence]).unwrap();
    assert!(delta
        .operations
        .iter()
        .any(|operation| matches!(operation.mutation, ProjectionDeltaMutation::Upsert { .. })));
    assert!(delta.recoveries.is_empty());
}

#[test]
fn sparse_patch_program_preview_lowers_to_conditional_patch_with_missing_record_fallback() {
    let fixture = modeled_fixture_with_kind(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
        ProjectionMutationKind::Patch,
    );
    let selected = selected_export(&fixture.surface);
    let request = TestRequestAuthority::authorized("auth-generation-7");
    let authority = ProjectionDeltaAuthority::try_new(&selected, &request).unwrap();
    let modeled = selected.surface().projectors[0].modeled[0].clone();
    let source = authority.source(&modeled).unwrap();
    let plan = crate::ResolvedProjectionPlan::resolve(
        &fixture.program,
        &partial_state_occurrence(7, "todo-1"),
    )
    .unwrap();
    let occurrence = ProjectionDeltaPlanOccurrence::preview(vec![(&source, &plan)]).unwrap();
    let delta = authority.lower(&[occurrence]).unwrap();
    assert!(delta.operations.iter().any(|operation| matches!(
        operation.mutation,
        ProjectionDeltaMutation::Patch {
            if_present: true,
            ..
        }
    )));
    assert!(delta.recoveries.iter().any(|recovery| {
        recovery.condition == ProjectionDeltaRecoveryCondition::IfRecordMissing
            && matches!(
                recovery.target,
                ProjectionDeltaRecoveryTarget::Record { .. }
            )
    }));
}

#[test]
fn selected_manifest_exposes_partition_expression_and_projection_field_slots() {
    use crate::graphql::client_manifest::{
        ClientProjectionAssignment, ClientProjectionExpression, ClientProjectionPartition,
        ClientProjectionValue,
    };

    let fixture = modeled_fixture(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let selected = selected_export(&fixture.surface);
    let manifest = selected.manifest().unwrap();
    let program = manifest
        .projection_programs
        .iter()
        .find(|program| program.name == "projection-delta-test")
        .unwrap();
    let arm = &program.arms[0];
    assert!(matches!(
        &arm.partition,
        ClientProjectionPartition::Expression {
            expression: ClientProjectionExpression::Constant {
                value: ClientProjectionValue::String(value)
            }
        } if value == "tenant-a"
    ));

    let operation = &arm.operations[0];
    assert_eq!(
        operation
            .key
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        vec!["todo_id"],
        "the canonical row identity is carried only by the key"
    );
    assert_eq!(
        operation
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        vec!["owner_id", "title", "status"]
    );
    let mut slots = operation
        .fields
        .iter()
        .map(|field| match &field.assignment {
            ClientProjectionAssignment::Set {
                expression: ClientProjectionExpression::Slot { slot, .. },
            } => slot.clone(),
            assignment => {
                panic!("projection field must remain executable from a slot: {assignment:?}")
            }
        })
        .collect::<Vec<_>>();
    slots.sort();
    slots.dedup();
    assert_eq!(slots.len(), operation.fields.len());
}

#[test]
fn plan_occurrence_rejects_missing_or_foreign_command_causation() {
    let fixture = modeled_fixture(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let selected = selected_export(&fixture.surface);
    let request = TestRequestAuthority::authorized("auth-generation-7");
    let authority = ProjectionDeltaAuthority::try_new(&selected, &request).unwrap();
    let modeled = selected.surface().projectors[0].modeled[0].clone();
    let source = authority.source(&modeled).unwrap();

    for causation in [None, Some(FOREIGN_CAUSATION_ID)] {
        let occurrence =
            state_occurrence_with_metadata(7, "todo-1", "title", causation, BTreeMap::new());
        let plan = crate::ResolvedProjectionPlan::resolve(&fixture.program, &occurrence).unwrap();
        assert!(matches!(
            ProjectionDeltaPlanOccurrence::actual(vec![(&source, &plan)]),
            Err(ProjectionDeltaError::ProjectionIdentityMismatch)
        ));
    }
}

#[test]
fn fanout_requires_the_same_exact_sealed_occurrence_not_only_the_same_id() {
    let fixture = modeled_fixture(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let selected = selected_export(&fixture.surface);
    let request = TestRequestAuthority::authorized("auth-generation-7");
    let authority = ProjectionDeltaAuthority::try_new(&selected, &request).unwrap();
    let modeled = selected.surface().projectors[0].modeled[0].clone();
    let source = authority.source(&modeled).unwrap();
    let first_occurrence = state_occurrence(7, "todo-1", "title");
    let second_occurrence = state_occurrence_with_metadata(
        7,
        "todo-1",
        "title",
        Some(TEST_CAUSATION_ID),
        BTreeMap::from([("request_scope".into(), "different".into())]),
    );
    assert_eq!(first_occurrence.id(), second_occurrence.id());
    assert_ne!(first_occurrence, second_occurrence);
    let first =
        crate::ResolvedProjectionPlan::resolve(&fixture.program, &first_occurrence).unwrap();
    let second =
        crate::ResolvedProjectionPlan::resolve(&fixture.program, &second_occurrence).unwrap();

    assert!(matches!(
        ProjectionDeltaPlanOccurrence::actual(vec![(&source, &first), (&source, &second)]),
        Err(ProjectionDeltaError::ProjectionIdentityMismatch)
    ));
}

#[test]
fn one_delta_cannot_mix_actual_and_preview_occurrence_layers() {
    let fixture = modeled_fixture(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let selected = selected_export(&fixture.surface);
    let request = TestRequestAuthority::authorized("auth-generation-7");
    let authority = ProjectionDeltaAuthority::try_new(&selected, &request).unwrap();
    let modeled = selected.surface().projectors[0].modeled[0].clone();
    let source = authority.source(&modeled).unwrap();
    let occurrence = state_occurrence(7, "todo-1", "title");
    let plan = crate::ResolvedProjectionPlan::resolve(&fixture.program, &occurrence).unwrap();
    let actual = ProjectionDeltaPlanOccurrence::actual(vec![(&source, &plan)]).unwrap();
    let preview = ProjectionDeltaPlanOccurrence::preview(vec![(&source, &plan)]).unwrap();

    assert!(matches!(
        authority.lower(&[actual, preview]),
        Err(ProjectionDeltaError::InvalidOperation(
            "actual and preview occurrences cannot share one projection delta"
        ))
    ));
}

#[test]
fn reversed_nonconflicting_fanout_plan_input_produces_identical_delta_bytes() {
    let fixture = fanout_fixture();
    let selected = selected_export(&fixture.surface);
    let request = TestRequestAuthority::authorized("auth-generation-7");
    let authority = ProjectionDeltaAuthority::try_new(&selected, &request).unwrap();
    let modeled = &selected.surface().projectors[0].modeled;
    let todo_source = authority
        .source(
            modeled
                .iter()
                .find(|modeled| {
                    modeled.program_id().to_string()
                        == fixture.todo_program.id().unwrap().to_string()
                })
                .unwrap(),
        )
        .unwrap();
    let user_source = authority
        .source(
            modeled
                .iter()
                .find(|modeled| {
                    modeled.program_id().to_string()
                        == fixture.user_program.id().unwrap().to_string()
                })
                .unwrap(),
        )
        .unwrap();
    let occurrence = state_occurrence(7, "todo-1", "title");
    let todo = crate::ResolvedProjectionPlan::resolve(&fixture.todo_program, &occurrence).unwrap();
    let user = crate::ResolvedProjectionPlan::resolve(&fixture.user_program, &occurrence).unwrap();
    let forward =
        ProjectionDeltaPlanOccurrence::actual(vec![(&todo_source, &todo), (&user_source, &user)])
            .unwrap();
    let reversed =
        ProjectionDeltaPlanOccurrence::actual(vec![(&user_source, &user), (&todo_source, &todo)])
            .unwrap();

    let forward = authority.lower(&[forward]).unwrap();
    assert!(forward.operations.iter().any(|operation| {
        operation.projection_refs.len() == 2
            && matches!(
                &operation.mutation,
                ProjectionDeltaMutation::InvalidateModel { model, .. } if model == "TodoView"
            )
    }));
    let forward = forward.canonical_bytes().unwrap();
    let reversed = authority
        .lower(&[reversed])
        .unwrap()
        .canonical_bytes()
        .unwrap();

    assert_eq!(forward, reversed);
}

#[test]
fn value_partition_encoder_cannot_collapse_scope_to_unit() {
    let fixture = modeled_fixture(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let selected = selected_export(&fixture.surface);
    let mut request = TestRequestAuthority::authorized("auth-generation-7");
    request.partition_result = PartitionResult::Unit;
    let authority = ProjectionDeltaAuthority::try_new(&selected, &request).unwrap();
    let modeled = selected.surface().projectors[0].modeled[0].clone();
    let source = authority.source(&modeled).unwrap();
    let plan = crate::ResolvedProjectionPlan::resolve(
        &fixture.program,
        &state_occurrence(7, "todo-1", "title"),
    )
    .unwrap();
    let occurrence = ProjectionDeltaPlanOccurrence::actual(vec![(&source, &plan)]).unwrap();
    assert_eq!(
        authority.lower(&[occurrence]),
        Err(ProjectionDeltaError::AuthorizationMapping)
    );
}

#[test]
fn unknown_visibility_emits_only_model_recovery_without_record_or_endpoint_keys() {
    let fixture = modeled_fixture(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let selected = selected_export(&fixture.surface);
    let unknown = AuthorizationTransition {
        before: ProjectionDeltaVisibility::Unknown,
        after: ProjectionDeltaVisibility::Unknown,
    };
    let request = TestRequestAuthority::new("auth-generation-7", unknown, unknown);
    let authority = ProjectionDeltaAuthority::try_new(&selected, &request).unwrap();
    let modeled = selected.surface().projectors[0].modeled[0].clone();
    let source = authority.source(&modeled).unwrap();
    let plan = crate::ResolvedProjectionPlan::resolve(
        &fixture.program,
        &state_occurrence(7, "todo-secret", "secret-title"),
    )
    .unwrap();
    let occurrence = ProjectionDeltaPlanOccurrence::actual(vec![(&source, &plan)]).unwrap();
    let delta = authority.lower(&[occurrence]).unwrap();
    assert!(delta.operations.iter().all(|operation| matches!(
        operation.mutation,
        ProjectionDeltaMutation::InvalidateModel { .. }
    )));
    assert!(delta.recoveries.iter().all(|recovery| {
        recovery.condition == ProjectionDeltaRecoveryCondition::Always
            && matches!(recovery.target, ProjectionDeltaRecoveryTarget::Model { .. })
    }));
    let json = String::from_utf8(delta.canonical_bytes().unwrap()).unwrap();
    assert!(!json.contains("todo-secret"));
    assert!(!json.contains("owner-secret"));
    assert!(!json.contains("secret-title"));
}

#[test]
fn hidden_relationship_target_transforms_link_into_source_invalidation() {
    let fixture = modeled_fixture(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let selected = selected_export_with_embedded_target(&fixture.surface);
    let request = TestRequestAuthority::authorized("auth-generation-7");
    let authority = ProjectionDeltaAuthority::try_new(&selected, &request).unwrap();
    let modeled = selected.surface().projectors[0].modeled[0].clone();
    let source = authority.source(&modeled).unwrap();
    let plan = crate::ResolvedProjectionPlan::resolve(
        &fixture.program,
        &state_occurrence(7, "todo-1", "title"),
    )
    .unwrap();
    let occurrence = ProjectionDeltaPlanOccurrence::actual(vec![(&source, &plan)]).unwrap();
    let delta = authority.lower(&[occurrence]).unwrap();
    assert!(!delta.operations.iter().any(|operation| matches!(
        operation.mutation,
        ProjectionDeltaMutation::Link { .. } | ProjectionDeltaMutation::Unlink { .. }
    )));
    assert!(delta.operations.iter().any(|operation| matches!(
        operation.mutation,
        ProjectionDeltaMutation::InvalidateRelationship { .. }
    )));
}

#[test]
fn explicit_relationship_invalidation_survives_removed_keyed_effect_as_model_recovery() {
    let fixture = modeled_fixture_with_relationship_invalidation(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let selected = selected_export_with_embedded_source(&fixture.surface);
    let request = TestRequestAuthority::authorized("auth-generation-7");
    let authority = ProjectionDeltaAuthority::try_new(&selected, &request).unwrap();
    let modeled = selected.surface().projectors[0].modeled[0].clone();
    let source = authority.source(&modeled).unwrap();
    let plan = crate::ResolvedProjectionPlan::resolve(
        &fixture.program,
        &state_occurrence(7, "todo-1", "secret-title"),
    )
    .unwrap();
    let occurrence = ProjectionDeltaPlanOccurrence::actual(vec![(&source, &plan)]).unwrap();
    let delta = authority.lower(&[occurrence]).unwrap();

    assert!(!delta.operations.iter().any(|operation| matches!(
        operation.mutation,
        ProjectionDeltaMutation::Link { .. }
            | ProjectionDeltaMutation::Unlink { .. }
            | ProjectionDeltaMutation::InvalidateRelationship { .. }
    )));
    assert!(delta.operations.iter().any(|operation| matches!(
        &operation.mutation,
        ProjectionDeltaMutation::InvalidateModel { model, .. } if model == "TodoView"
    )));
    assert!(delta.recoveries.iter().any(|recovery| {
        recovery.condition == ProjectionDeltaRecoveryCondition::Always
            && matches!(
                &recovery.target,
                ProjectionDeltaRecoveryTarget::Model { model, .. } if model == "TodoView"
            )
    }));
    let json = String::from_utf8(delta.canonical_bytes().unwrap()).unwrap();
    assert!(!json.contains("todo-1"));
    assert!(!json.contains("owner-secret"));
    assert!(!json.contains("secret-title"));
}

#[test]
fn hidden_relationship_source_emits_no_keyed_consequence() {
    let fixture = modeled_fixture(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let selected = selected_export_with_embedded_source(&fixture.surface);
    let request = TestRequestAuthority::authorized("auth-generation-7");
    let authority = ProjectionDeltaAuthority::try_new(&selected, &request).unwrap();
    let modeled = selected.surface().projectors[0].modeled[0].clone();
    let source = authority.source(&modeled).unwrap();
    let plan = crate::ResolvedProjectionPlan::resolve(
        &fixture.program,
        &state_occurrence(7, "todo-secret", "secret-title"),
    )
    .unwrap();
    let occurrence = ProjectionDeltaPlanOccurrence::actual(vec![(&source, &plan)]).unwrap();
    let delta = authority.lower(&[occurrence]).unwrap();
    assert!(delta.operations.iter().any(|operation| matches!(
        &operation.mutation,
        ProjectionDeltaMutation::InvalidateModel { model, .. } if model == "TodoView"
    )));
    assert!(delta.recoveries.iter().any(|recovery| {
        recovery.condition == ProjectionDeltaRecoveryCondition::Always
            && matches!(
                &recovery.target,
                ProjectionDeltaRecoveryTarget::Model { model, .. } if model == "TodoView"
            )
    }));
    let json = String::from_utf8(delta.canonical_bytes().unwrap()).unwrap();
    assert!(!json.contains("todo-secret"));
    assert!(!json.contains("owner-secret"));
    assert!(!json.contains("secret-title"));
}

#[test]
fn recovery_condition_is_order_independent_and_obsolete_patch_fallback_is_dropped() {
    let scope = test_scope("tenant-a", "Todos", vec![key_field(0, "todo_id", "todo-1")]);
    let patch = operation(
        0,
        ProjectionDeltaMutation::Patch {
            scope: scope.clone(),
            set: vec![DeltaField {
                field: "title".into(),
                value: DeltaValue::String("new".into()),
            }],
            unset: vec![],
            if_present: true,
        },
    );
    let conditional = ProjectionDeltaRecovery {
        occurrence_ordinal: 0,
        projection_refs: vec![0],
        condition: ProjectionDeltaRecoveryCondition::IfRecordMissing,
        target: ProjectionDeltaRecoveryTarget::Record {
            scope: scope.clone(),
        },
    };
    let always = ProjectionDeltaRecovery {
        occurrence_ordinal: 1,
        projection_refs: vec![1],
        condition: ProjectionDeltaRecoveryCondition::Always,
        target: ProjectionDeltaRecoveryTarget::Record {
            scope: scope.clone(),
        },
    };
    for recoveries in [
        vec![conditional.clone(), always.clone()],
        vec![always.clone(), conditional.clone()],
    ] {
        let merged =
            super::canonical::canonicalize_recoveries(recoveries, std::slice::from_ref(&patch));
        assert_eq!(merged.len(), 1);
        assert_eq!(
            merged[0].condition,
            ProjectionDeltaRecoveryCondition::Always
        );
        assert_eq!(merged[0].projection_refs, vec![0, 1]);
    }

    let upsert = operation(
        1,
        ProjectionDeltaMutation::Upsert {
            scope,
            fields: vec![DeltaField {
                field: "title".into(),
                value: DeltaValue::String("final".into()),
            }],
            replace: vec!["title".into()],
        },
    );
    assert!(super::canonical::canonicalize_recoveries(vec![conditional], &[upsert]).is_empty());
}

#[test]
fn relationship_crossing_authorization_never_serializes_target_values() {
    let fixture = modeled_fixture(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let selected = selected_export(&fixture.surface);
    let request = TestRequestAuthority::new(
        "auth-generation-7",
        authorized_transition(),
        AuthorizationTransition {
            before: ProjectionDeltaVisibility::Denied,
            after: ProjectionDeltaVisibility::Authorized,
        },
    );
    let authority = ProjectionDeltaAuthority::try_new(&selected, &request).unwrap();
    let modeled = selected.surface().projectors[0].modeled[0].clone();
    let source = authority.source(&modeled).unwrap();
    let plan = crate::ResolvedProjectionPlan::resolve(
        &fixture.program,
        &state_occurrence(7, "todo-secret", "safe-title"),
    )
    .unwrap();
    let occurrence = ProjectionDeltaPlanOccurrence::actual(vec![(&source, &plan)]).unwrap();
    let delta = authority.lower(&[occurrence]).unwrap();
    assert!(!delta.operations.iter().any(|operation| matches!(
        operation.mutation,
        ProjectionDeltaMutation::Link { .. } | ProjectionDeltaMutation::Unlink { .. }
    )));
    assert!(delta.operations.iter().any(|operation| matches!(
        operation.mutation,
        ProjectionDeltaMutation::InvalidateRelationship { .. }
    )));
    let relationship_json = serde_json::to_string(
        &delta
            .operations
            .iter()
            .filter(|operation| {
                matches!(
                    operation.mutation,
                    ProjectionDeltaMutation::Link { .. }
                        | ProjectionDeltaMutation::Unlink { .. }
                        | ProjectionDeltaMutation::InvalidateRelationship { .. }
                )
            })
            .collect::<Vec<_>>(),
    )
    .unwrap();
    assert!(!relationship_json.contains("owner-secret"));
}

#[test]
fn draining_accepts_actual_but_rejects_preview_and_background_rejects_all() {
    let draining = modeled_fixture(
        ProjectionBindingState::Draining,
        ProjectionExecutionClass::Causal,
    );
    let selected = selected_export(&draining.surface);
    let request = TestRequestAuthority::authorized("auth-generation");
    let authority = ProjectionDeltaAuthority::try_new(&selected, &request).unwrap();
    let modeled = selected.surface().projectors[0].modeled[0].clone();
    let source = authority.source(&modeled).unwrap();
    let plan = crate::ResolvedProjectionPlan::resolve(
        &draining.program,
        &state_occurrence(7, "todo-1", "title"),
    )
    .unwrap();
    ProjectionDeltaPlanOccurrence::actual(vec![(&source, &plan)]).unwrap();
    assert!(matches!(
        ProjectionDeltaPlanOccurrence::preview(vec![(&source, &plan)]),
        Err(ProjectionDeltaError::IneligibleBinding)
    ));

    let background = modeled_fixture(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Background,
    );
    let selected = selected_export(&background.surface);
    let authority = ProjectionDeltaAuthority::try_new(&selected, &request).unwrap();
    let modeled = selected.surface().projectors[0].modeled[0].clone();
    assert!(matches!(
        authority.source(&modeled),
        Err(ProjectionDeltaError::IneligibleBinding)
    ));
}

#[test]
fn replay_scope_requires_exact_manifest_protocol_surface_generation_and_cause() {
    let fixture = modeled_fixture(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let selected = selected_export(&fixture.surface);
    let manifest = selected.manifest().unwrap();
    let request = TestRequestAuthority::authorized("generation-1");
    let mut delta = empty_delta();
    delta.identity = ProjectionDeltaIdentity {
        manifest_version: manifest.manifest_version,
        client_protocol_version: manifest.protocol_version,
        surface: ProjectionDeltaSurfaceIdentity::from(&manifest.surface),
        schema_fingerprint: manifest.schema_fingerprint.clone(),
        protocol_fingerprint: manifest.protocol_fingerprint.clone(),
        authorization_generation: "generation-1".into(),
        cache_scope_token: request.cache_scope().as_str().into(),
        command_causation_id: request.command_causation_id().as_str().into(),
    };
    delta.validate_replay_scope(&manifest, &request).unwrap();
    let other_request = TestRequestAuthority::authorized("generation-2");
    assert_eq!(
        delta.validate_replay_scope(&manifest, &other_request),
        Err(ProjectionDeltaError::ReplayScopeMismatch)
    );
}

#[test]
fn row_authorization_transition_matrix_is_explicit_and_fail_closed() {
    let authorized = ProjectionDeltaVisibility::Authorized;
    let denied = ProjectionDeltaVisibility::Denied;
    let unknown = ProjectionDeltaVisibility::Unknown;

    let visible = lower_with_transitions(
        transition(authorized, authorized),
        transition(authorized, authorized),
    )
    .unwrap();
    assert!(visible
        .operations
        .iter()
        .any(|operation| { matches!(operation.mutation, ProjectionDeltaMutation::Upsert { .. }) }));

    let hidden = lower_with_transitions(
        transition(denied, denied),
        transition(authorized, authorized),
    )
    .unwrap();
    assert!(!hidden.operations.iter().any(is_record_operation));
    assert!(!hidden.recoveries.iter().any(|recovery| matches!(
        recovery.target,
        ProjectionDeltaRecoveryTarget::Record { .. }
    )));

    for record_transition in [
        transition(authorized, denied),
        transition(denied, authorized),
    ] {
        let delta =
            lower_with_transitions(record_transition, transition(authorized, authorized)).unwrap();
        assert!(!delta.operations.iter().any(is_record_operation));
        assert!(delta.recoveries.iter().any(|recovery| {
            recovery.condition == ProjectionDeltaRecoveryCondition::Always
                && matches!(
                    recovery.target,
                    ProjectionDeltaRecoveryTarget::Record { .. }
                )
        }));
    }

    let complete_after = lower_with_transitions(
        transition(unknown, authorized),
        transition(authorized, authorized),
    )
    .unwrap();
    assert!(complete_after
        .operations
        .iter()
        .any(|operation| matches!(operation.mutation, ProjectionDeltaMutation::Upsert { .. })));

    for record_transition in [
        transition(authorized, unknown),
        transition(unknown, denied),
        transition(denied, unknown),
        transition(unknown, unknown),
    ] {
        let delta =
            lower_with_transitions(record_transition, transition(authorized, authorized)).unwrap();
        assert!(!delta.operations.iter().any(is_record_operation));
        assert!(delta.recoveries.iter().any(|recovery| {
            recovery.condition == ProjectionDeltaRecoveryCondition::Always
                && matches!(recovery.target, ProjectionDeltaRecoveryTarget::Model { .. })
        }));
    }
}

#[test]
fn relationship_authorization_transition_matrix_is_explicit_and_fail_closed() {
    let authorized = ProjectionDeltaVisibility::Authorized;
    let denied = ProjectionDeltaVisibility::Denied;
    let unknown = ProjectionDeltaVisibility::Unknown;
    let record = transition(authorized, authorized);

    let visible = lower_with_transitions(record, transition(authorized, authorized)).unwrap();
    assert!(visible
        .operations
        .iter()
        .any(|operation| { matches!(operation.mutation, ProjectionDeltaMutation::Link { .. }) }));
    assert!(visible
        .operations
        .iter()
        .any(|operation| { matches!(operation.mutation, ProjectionDeltaMutation::Unlink { .. }) }));

    let hidden = lower_with_transitions(record, transition(denied, denied)).unwrap();
    assert!(!hidden.operations.iter().any(is_relationship_operation));
    assert!(!hidden.recoveries.iter().any(|recovery| {
        matches!(
            recovery.target,
            ProjectionDeltaRecoveryTarget::Relationship { .. }
        )
    }));

    for relationship_transition in [
        transition(authorized, denied),
        transition(denied, authorized),
    ] {
        let delta = lower_with_transitions(record, relationship_transition).unwrap();
        assert!(delta.operations.iter().any(|operation| {
            matches!(
                operation.mutation,
                ProjectionDeltaMutation::InvalidateRelationship { .. }
            )
        }));
        assert!(delta.recoveries.iter().any(|recovery| {
            recovery.condition == ProjectionDeltaRecoveryCondition::Always
                && matches!(
                    recovery.target,
                    ProjectionDeltaRecoveryTarget::Relationship { .. }
                )
        }));
    }

    for relationship_transition in [
        transition(unknown, authorized),
        transition(authorized, unknown),
        transition(unknown, denied),
        transition(denied, unknown),
        transition(unknown, unknown),
    ] {
        let delta = lower_with_transitions(record, relationship_transition).unwrap();
        assert!(!delta.operations.iter().any(|operation| {
            matches!(
                operation.mutation,
                ProjectionDeltaMutation::Link { .. }
                    | ProjectionDeltaMutation::Unlink { .. }
                    | ProjectionDeltaMutation::InvalidateRelationship { .. }
            )
        }));
        assert!(delta.operations.iter().any(|operation| {
            matches!(
                operation.mutation,
                ProjectionDeltaMutation::InvalidateModel { .. }
            )
        }));
        assert!(delta.recoveries.iter().any(|recovery| {
            matches!(recovery.target, ProjectionDeltaRecoveryTarget::Model { .. })
        }));
    }
}

#[test]
fn all_visible_explicit_relationship_invalidation_lowers_to_the_narrow_scope() {
    let fixture = modeled_fixture_with_relationship_invalidation(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let selected = selected_export(&fixture.surface);
    let request = TestRequestAuthority::authorized("auth-generation-7");
    let authority = ProjectionDeltaAuthority::try_new(&selected, &request).unwrap();
    let modeled = selected.surface().projectors[0].modeled[0].clone();
    let source = authority.source(&modeled).unwrap();
    let plan = crate::ResolvedProjectionPlan::resolve(
        &fixture.program,
        &state_occurrence(7, "todo-1", "title"),
    )
    .unwrap();
    let occurrence = ProjectionDeltaPlanOccurrence::actual(vec![(&source, &plan)]).unwrap();
    let delta = authority.lower(&[occurrence]).unwrap();

    assert!(delta.operations.iter().any(|operation| {
        matches!(
            operation.mutation,
            ProjectionDeltaMutation::InvalidateRelationship { .. }
        )
    }));
    assert!(delta.recoveries.iter().any(|recovery| {
        recovery.condition == ProjectionDeltaRecoveryCondition::Always
            && matches!(
                recovery.target,
                ProjectionDeltaRecoveryTarget::Relationship { .. }
            )
    }));
    assert!(!delta.recoveries.iter().any(|recovery| {
        matches!(recovery.target, ProjectionDeltaRecoveryTarget::Model { .. })
    }));
}

#[test]
fn placement_and_binding_state_matrix_is_explicit() {
    let active = modeled_fixture(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let selected = selected_export(&active.surface);
    let request = TestRequestAuthority::authorized("auth-generation");
    let authority = ProjectionDeltaAuthority::try_new(&selected, &request).unwrap();
    let modeled = selected.surface().projectors[0].modeled[0].clone();
    let source = authority.source(&modeled).unwrap();
    let plan = crate::ResolvedProjectionPlan::resolve(
        &active.program,
        &state_occurrence(7, "todo-1", "title"),
    )
    .unwrap();
    let actual = ProjectionDeltaPlanOccurrence::actual(vec![(&source, &plan)]).unwrap();
    authority.lower(&[actual]).unwrap();
    let preview = ProjectionDeltaPlanOccurrence::preview(vec![(&source, &plan)]).unwrap();
    authority.lower(&[preview]).unwrap();

    let draining = modeled_fixture(
        ProjectionBindingState::Draining,
        ProjectionExecutionClass::Causal,
    );
    let selected = selected_export(&draining.surface);
    let authority = ProjectionDeltaAuthority::try_new(&selected, &request).unwrap();
    let modeled = selected.surface().projectors[0].modeled[0].clone();
    let source = authority.source(&modeled).unwrap();
    let plan = crate::ResolvedProjectionPlan::resolve(
        &draining.program,
        &state_occurrence(8, "todo-1", "title"),
    )
    .unwrap();
    let actual = ProjectionDeltaPlanOccurrence::actual(vec![(&source, &plan)]).unwrap();
    authority.lower(&[actual]).unwrap();
    assert!(matches!(
        ProjectionDeltaPlanOccurrence::preview(vec![(&source, &plan)]),
        Err(ProjectionDeltaError::IneligibleBinding)
    ));

    let background = modeled_fixture(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Background,
    );
    let selected = selected_export(&background.surface);
    let authority = ProjectionDeltaAuthority::try_new(&selected, &request).unwrap();
    let modeled = selected.surface().projectors[0].modeled[0].clone();
    assert!(matches!(
        authority.source(&modeled),
        Err(ProjectionDeltaError::IneligibleBinding)
    ));

    let direct = modeled_direct_fixture();
    let selected = selected_export(&direct.surface);
    let authority = ProjectionDeltaAuthority::try_new(&selected, &request).unwrap();
    let modeled = selected.surface().projectors[0].modeled[0].clone();
    assert!(matches!(
        authority.source(&modeled),
        Err(ProjectionDeltaError::IneligibleBinding)
    ));
}

#[test]
fn lowerer_rejects_wrong_program_binding_arm_occurrence_and_duplicate_program_source() {
    let fixture = modeled_fixture(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let selected = selected_export(&fixture.surface);
    let request = TestRequestAuthority::authorized("auth-generation");
    let authority = ProjectionDeltaAuthority::try_new(&selected, &request).unwrap();
    let modeled = selected.surface().projectors[0].modeled[0].clone();
    let source = authority.source(&modeled).unwrap();
    let plan = crate::ResolvedProjectionPlan::resolve(
        &fixture.program,
        &state_occurrence(7, "todo-1", "title"),
    )
    .unwrap();

    let other = modeled_fixture_with_kind(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
        ProjectionMutationKind::Patch,
    );
    let wrong_program_plan = crate::ResolvedProjectionPlan::resolve(
        &other.program,
        &state_occurrence(7, "todo-1", "title"),
    )
    .unwrap();
    assert!(matches!(
        ProjectionDeltaPlanOccurrence::actual(vec![(&source, &wrong_program_plan)]),
        Err(ProjectionDeltaError::ProjectionIdentityMismatch)
    ));

    let other_selected = selected_export(&other.surface);
    let wrong_binding = other_selected.surface().projectors[0].modeled[0].clone();
    assert!(matches!(
        authority.source(&wrong_binding),
        Err(ProjectionDeltaError::ProjectionIdentityMismatch)
    ));

    let other_occurrence = crate::ResolvedProjectionPlan::resolve(
        &fixture.program,
        &state_occurrence(8, "todo-1", "title"),
    )
    .unwrap();
    assert!(matches!(
        ProjectionDeltaPlanOccurrence::actual(vec![(&source, &plan), (&source, &other_occurrence)]),
        Err(ProjectionDeltaError::ProjectionIdentityMismatch)
    ));
    assert!(matches!(
        ProjectionDeltaPlanOccurrence::actual(vec![(&source, &plan), (&source, &plan)]),
        Err(ProjectionDeltaError::InvalidOperation(
            "one occurrence cannot select multiple bindings for one program"
        ))
    ));

    let mut wrong_arm_surface = surface_for_role(
        &fixture.surface,
        "delta-user",
        &BTreeMap::from([
            ("TodoView".into(), RoleGrant::all_columns()),
            ("UserView".into(), RoleGrant::all_columns()),
        ]),
    )
    .unwrap();
    let original = wrong_arm_surface.projectors[0].modeled[0].clone();
    let mut selected_program = original.selected_program().unwrap().clone();
    selected_program.arms[0].arm_id = "wrong-arm".to_owned();
    wrong_arm_surface.projectors[0].modeled[0] =
        crate::graphql::SurfaceModeledProjection::selected_for_client_manifest_test(
            original.program_id(),
            original.binding_id(),
            original.placement(),
            original.execution_class(),
            original.state(),
            original.output_models().to_vec(),
            Some(selected_program),
        );
    let wrong_arm_export =
        DistributedClientSurfaceExport::from_selected("delta-service", wrong_arm_surface).unwrap();
    let wrong_arm_authority =
        ProjectionDeltaAuthority::try_new(&wrong_arm_export, &request).unwrap();
    let wrong_arm_modeled = wrong_arm_export.surface().projectors[0].modeled[0].clone();
    let wrong_arm_source = wrong_arm_authority.source(&wrong_arm_modeled).unwrap();
    assert!(matches!(
        ProjectionDeltaPlanOccurrence::actual(vec![(&wrong_arm_source, &plan)]),
        Err(ProjectionDeltaError::ProjectionIdentityMismatch)
    ));
}

#[test]
fn explicit_fk_and_join_row_provenance_lower_to_edges_without_physical_names() {
    let fk = lower_with_transitions(authorized_transition(), authorized_transition()).unwrap();
    assert!(fk
        .operations
        .iter()
        .any(|operation| { matches!(operation.mutation, ProjectionDeltaMutation::Link { .. }) }));
    assert!(fk
        .operations
        .iter()
        .any(|operation| { matches!(operation.mutation, ProjectionDeltaMutation::Unlink { .. }) }));

    let join = modeled_join_fixture();
    let selected = selected_export_join(&join.surface);
    let request = TestRequestAuthority::authorized("auth-generation-7");
    let authority = ProjectionDeltaAuthority::try_new(&selected, &request).unwrap();
    let modeled = selected.surface().projectors[0].modeled[0].clone();
    let source = authority.source(&modeled).unwrap();
    let plan = crate::ResolvedProjectionPlan::resolve(
        &join.program,
        &state_occurrence(7, "todo-1", "title"),
    )
    .unwrap();
    let occurrence = ProjectionDeltaPlanOccurrence::actual(vec![(&source, &plan)]).unwrap();
    let delta = authority.lower(&[occurrence]).unwrap();
    assert!(delta.operations.iter().any(|operation| {
        matches!(
            &operation.mutation,
            ProjectionDeltaMutation::Link { relationship, .. } if relationship == "owner"
        )
    }));
    let bytes = delta.canonical_bytes().unwrap();
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(!text.contains("private_todo_owner_links"));
    assert!(!text.contains("\"storage\""));
    assert!(!text.contains("\"table\""));
}

#[test]
fn logical_partition_boundary_accepts_4k_and_rejects_the_next_byte() {
    let fixture = modeled_fixture(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let resolve = |len: usize| {
        let program = ProjectionProgram::try_new(
            "projection-delta-partition-boundary",
            1,
            ProjectionPartition::Expression(ProjectionExpression::constant(
                ProjectionValue::string("x".repeat(len)),
            )),
            fixture.program.arms().to_vec(),
        )?;
        crate::ResolvedProjectionPlan::resolve(&program, &state_occurrence(7, "todo-1", "title"))
    };
    let limit = crate::projection_protocol::MAX_PROJECTION_PARTITION_BYTES;
    let mut low = 0;
    let mut high = limit;
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if resolve(middle).is_ok() {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    let accepted = resolve(low).unwrap();
    assert_eq!(accepted.partition().canonical_bytes().len(), limit);
    assert!(matches!(
        resolve(low + 1),
        Err(crate::ProjectionProgramError::ValueTooLarge {
            kind: "projection partition",
            len: 4097,
            max: 4096
        })
    ));
}

struct ModeledFixture {
    surface: crate::graphql::Surface,
    program: ProjectionProgram,
}

struct FanoutFixture {
    surface: crate::graphql::Surface,
    todo_program: ProjectionProgram,
    user_program: ProjectionProgram,
}

fn transition(
    before: ProjectionDeltaVisibility,
    after: ProjectionDeltaVisibility,
) -> AuthorizationTransition {
    AuthorizationTransition { before, after }
}

fn is_record_operation(operation: &ProjectionDeltaOperation) -> bool {
    matches!(
        operation.mutation,
        ProjectionDeltaMutation::Upsert { .. }
            | ProjectionDeltaMutation::Patch { .. }
            | ProjectionDeltaMutation::Delete { .. }
    )
}

fn is_relationship_operation(operation: &ProjectionDeltaOperation) -> bool {
    matches!(
        operation.mutation,
        ProjectionDeltaMutation::Link { .. }
            | ProjectionDeltaMutation::Unlink { .. }
            | ProjectionDeltaMutation::InvalidateRelationship { .. }
    )
}

fn lower_with_transitions(
    record: AuthorizationTransition,
    relationship: AuthorizationTransition,
) -> Result<ProjectionDelta, ProjectionDeltaError> {
    let fixture = modeled_fixture(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let selected = selected_export(&fixture.surface);
    let request = TestRequestAuthority::new("auth-generation-7", record, relationship);
    let authority = ProjectionDeltaAuthority::try_new(&selected, &request)?;
    let modeled = selected.surface().projectors[0].modeled[0].clone();
    let source = authority.source(&modeled)?;
    let plan = crate::ResolvedProjectionPlan::resolve(
        &fixture.program,
        &state_occurrence(7, "todo-1", "title"),
    )
    .map_err(|_| ProjectionDeltaError::ProjectionIdentityMismatch)?;
    let occurrence = ProjectionDeltaPlanOccurrence::actual(vec![(&source, &plan)])?;
    authority.lower(&[occurrence])
}

struct TestProgramDescriptor(ProjectionProgram);

impl crate::projection::placement::ProjectionProgramDescriptor for TestProgramDescriptor {
    fn projection_program(&self) -> Result<ProjectionProgram, crate::ProjectionProgramError> {
        Ok(self.0.clone())
    }
}

fn modeled_direct_fixture() -> ModeledFixture {
    let todo_schema = todos();
    let user_schema = users();
    let selector =
        crate::ProjectionEventSelector::try_from_descriptor(&event_descriptor()).unwrap();
    let operation = ProjectionOperation::try_new(
        "upsert-todo-direct",
        0,
        ProjectionMutationKind::Upsert,
        ProjectionTarget::try_new("TodoView", "todos").unwrap(),
        vec![projection_key(0, "todo_id", "todo_id")],
        vec![
            projection_field(0, "todo_id", "todo_id"),
            projection_field(1, "owner_id", "owner_id"),
            projection_field(2, "title", "title"),
            projection_field(3, "status", "status"),
        ],
        vec![],
        vec![],
    )
    .unwrap();
    let program = ProjectionProgram::try_new(
        "projection-delta-direct-test",
        1,
        ProjectionPartition::Expression(ProjectionExpression::constant(ProjectionValue::string(
            "tenant-a",
        ))),
        vec![ProjectionArm::try_new("todo-direct", selector, vec![operation]).unwrap()],
    )
    .unwrap();
    let descriptor = TestProgramDescriptor(program.clone());
    let binding = ProjectionBinding::materialize_direct(
        crate::projection::placement::DirectProjectionPlacement::new(&descriptor),
        ProjectionSourceBinding::try_new("todo-domain", "ordered-domain-events", 1).unwrap(),
        ProjectionOwner::try_new("projection-delta-test").unwrap(),
        "distributed-projection-partition",
        PROJECTION_PARTITION_CODEC_VERSION,
        vec![ProjectionOutput::try_new("TodoView", "todos", todo_schema.clone()).unwrap()],
        vec![],
        Some(ProjectionPhysicalTopology::from_protocol(
            &ProjectorTopologyId::new(1, "projection-delta-direct", [0x45; 32]).unwrap(),
        )),
    )
    .unwrap();
    let catalog =
        crate::projection::catalog::ProjectionCatalog::try_new(vec![binding.clone()]).unwrap();
    let active = catalog
        .activate(
            vec![ProjectionBindingActivation::new(
                binding.id(),
                binding.program_id(),
                ProjectionEpoch::new("projection-delta-direct-v1").unwrap(),
                ProjectionBindingState::Active,
                Some(ProjectionExecutorRoute::local("delta-service").unwrap()),
            )],
            None,
        )
        .unwrap();
    let modeled = crate::graphql::SurfaceModeledProjection::try_from_catalog(
        program.clone(),
        &catalog,
        &active,
        binding.id(),
    )
    .unwrap();
    let surface = build_surface(&[todo_schema, user_schema], &SurfaceOptions::sqlite())
        .unwrap()
        .with_projection_owners([SurfaceDirectProjection::new("projection-delta-test")
            .modeled(modeled)
            .into()])
        .unwrap();
    ModeledFixture { surface, program }
}

fn modeled_join_fixture() -> ModeledFixture {
    let todo_schema = todos();
    let user_schema = users();
    let join_schema = todo_owner_links();
    let selector =
        crate::ProjectionEventSelector::try_from_descriptor(&event_descriptor()).unwrap();
    let relationship = ProjectionRelationship::try_new("TodoView", "owner", "UserView").unwrap();
    let operation = ProjectionOperation::try_new(
        "insert-todo-owner-link",
        0,
        ProjectionMutationKind::InsertRelated,
        ProjectionTarget::try_new("TodoOwnerLink", "private_todo_owner_links").unwrap(),
        vec![
            projection_key(0, "todo_id", "todo_id"),
            projection_key(1, "owner_id", "owner_id"),
        ],
        vec![
            projection_field(0, "todo_id", "todo_id"),
            projection_field(1, "owner_id", "owner_id"),
        ],
        vec![ProjectionRelationshipEffect::link(
            0,
            relationship,
            vec![projection_key(0, "todo_id", "todo_id")],
            vec![projection_key(0, "user_id", "owner_id")],
        )
        .unwrap()],
        vec![],
    )
    .unwrap();
    let program = ProjectionProgram::try_new(
        "projection-delta-join-test",
        1,
        ProjectionPartition::Expression(ProjectionExpression::constant(ProjectionValue::string(
            "tenant-a",
        ))),
        vec![ProjectionArm::try_new("todo-join", selector, vec![operation]).unwrap()],
    )
    .unwrap();
    let binding = ProjectionBinding::from_eventual_program(
        &program,
        ProjectionSourceBinding::try_new("todo-domain", "ordered-domain-events", 1).unwrap(),
        ProjectionOwner::try_new("projection-delta-join-test").unwrap(),
        ProjectionExecutionClass::Causal,
        "distributed-projection-partition",
        PROJECTION_PARTITION_CODEC_VERSION,
        vec![ProjectionOutput::try_new(
            "TodoOwnerLink",
            "private_todo_owner_links",
            join_schema.clone(),
        )
        .unwrap()],
        vec![ProjectionRelationshipBinding::try_new("TodoView", "owner", "UserView").unwrap()],
        Some(ProjectionPhysicalTopology::from_protocol(
            &ProjectorTopologyId::new(1, "projection-delta-join-test", [0x46; 32]).unwrap(),
        )),
    )
    .unwrap();
    let catalog =
        crate::projection::catalog::ProjectionCatalog::try_new(vec![binding.clone()]).unwrap();
    let active = catalog
        .activate(
            vec![ProjectionBindingActivation::new(
                binding.id(),
                binding.program_id(),
                ProjectionEpoch::new("projection-delta-join-v1").unwrap(),
                ProjectionBindingState::Active,
                Some(ProjectionExecutorRoute::local("delta-service").unwrap()),
            )],
            None,
        )
        .unwrap();
    let modeled = crate::graphql::SurfaceModeledProjection::try_from_catalog(
        program.clone(),
        &catalog,
        &active,
        binding.id(),
    )
    .unwrap();
    let surface = build_surface(
        &[todo_schema, user_schema, join_schema],
        &SurfaceOptions::sqlite(),
    )
    .unwrap()
    .with_projectors([SurfaceProjector::new("projection-delta-join-test").modeled(modeled)])
    .unwrap();
    ModeledFixture { surface, program }
}

fn modeled_fixture(
    state: ProjectionBindingState,
    execution: ProjectionExecutionClass,
) -> ModeledFixture {
    modeled_fixture_config(state, execution, ProjectionMutationKind::Upsert, false)
}

#[cfg(feature = "graphql")]
fn modeled_fixture_with_opaque_fallback(state: ProjectionBindingState) -> ModeledFixture {
    let fixture = modeled_fixture(state, ProjectionExecutionClass::Causal);
    let mut surface = fixture.surface;
    let mut modeled = std::mem::take(&mut surface.projectors);
    for owner in &mut modeled {
        if !owner.modeled.is_empty() {
            owner.facts.clear();
            owner.models.clear();
        }
    }
    surface.projectors_attached = false;
    let opaque = SurfaceProjector::new("projection-delta-opaque-fallback")
        .facts(["todo.state-published"])
        .models(["UserView"]);
    let surface = surface
        .with_projection_owners(modeled.into_iter().chain(std::iter::once(opaque.into())))
        .expect("mixed modeled and opaque projection topology");
    ModeledFixture {
        surface,
        program: fixture.program,
    }
}

fn modeled_fixture_with_kind(
    state: ProjectionBindingState,
    execution: ProjectionExecutionClass,
    mutation_kind: ProjectionMutationKind,
) -> ModeledFixture {
    modeled_fixture_config(state, execution, mutation_kind, false)
}

fn modeled_fixture_with_relationship_invalidation(
    state: ProjectionBindingState,
    execution: ProjectionExecutionClass,
) -> ModeledFixture {
    modeled_fixture_config(state, execution, ProjectionMutationKind::Upsert, true)
}

fn modeled_fixture_config(
    state: ProjectionBindingState,
    execution: ProjectionExecutionClass,
    mutation_kind: ProjectionMutationKind,
    invalidate_relationship: bool,
) -> ModeledFixture {
    modeled_fixture_deployment(
        state,
        execution,
        mutation_kind,
        invalidate_relationship,
        "delta-service",
        "projection-delta-test",
        [0x44; 32],
    )
}

fn modeled_fixture_deployment(
    state: ProjectionBindingState,
    execution: ProjectionExecutionClass,
    mutation_kind: ProjectionMutationKind,
    invalidate_relationship: bool,
    route: &str,
    topology_name: &str,
    topology_digest: [u8; 32],
) -> ModeledFixture {
    let todo_schema = todos();
    let user_schema = users();
    let descriptor = event_descriptor();
    let selector = crate::ProjectionEventSelector::try_from_descriptor(&descriptor).unwrap();
    let relationship = ProjectionRelationship::try_new("TodoView", "owner", "UserView").unwrap();
    let (relationship_effects, invalidations) = if invalidate_relationship {
        (
            vec![ProjectionRelationshipEffect::invalidate(
                0,
                relationship,
                vec![projection_key(0, "todo_id", "todo_id")],
            )
            .unwrap()],
            vec![ProjectionInvalidation::relationship("TodoView", "owner", "UserView").unwrap()],
        )
    } else {
        (
            vec![
                ProjectionRelationshipEffect::unlink(
                    0,
                    relationship.clone(),
                    vec![projection_key(0, "todo_id", "todo_id")],
                    vec![projection_key(0, "user_id", "old_owner_id")],
                )
                .unwrap(),
                ProjectionRelationshipEffect::link(
                    1,
                    relationship,
                    vec![projection_key(0, "todo_id", "todo_id")],
                    vec![projection_key(0, "user_id", "owner_id")],
                )
                .unwrap(),
            ],
            vec![],
        )
    };
    let operation = ProjectionOperation::try_new(
        "upsert-todo",
        0,
        mutation_kind,
        ProjectionTarget::try_new("TodoView", "todos").unwrap(),
        vec![projection_key(0, "todo_id", "todo_id")],
        vec![
            projection_field(0, "todo_id", "todo_id"),
            projection_field(1, "owner_id", "owner_id"),
            projection_field(2, "title", "title"),
            projection_field(3, "status", "status"),
        ],
        relationship_effects,
        invalidations,
    )
    .unwrap();
    let arm = ProjectionArm::try_new("todo-state", selector, vec![operation]).unwrap();
    let program = ProjectionProgram::try_new(
        "projection-delta-test",
        1,
        ProjectionPartition::Expression(ProjectionExpression::constant(ProjectionValue::string(
            "tenant-a",
        ))),
        vec![arm],
    )
    .unwrap();
    let binding = ProjectionBinding::from_eventual_program(
        &program,
        ProjectionSourceBinding::try_new("todo-domain", "ordered-domain-events", 1).unwrap(),
        ProjectionOwner::try_new("projection-delta-test").unwrap(),
        execution,
        "distributed-projection-partition",
        PROJECTION_PARTITION_CODEC_VERSION,
        vec![ProjectionOutput::try_new("TodoView", "todos", todo_schema.clone()).unwrap()],
        vec![ProjectionRelationshipBinding::try_new("TodoView", "owner", "UserView").unwrap()],
        Some(ProjectionPhysicalTopology::from_protocol(
            &ProjectorTopologyId::new(1, topology_name, topology_digest).unwrap(),
        )),
    )
    .unwrap();
    let catalog =
        crate::projection::catalog::ProjectionCatalog::try_new(vec![binding.clone()]).unwrap();
    let active = catalog
        .activate(
            vec![ProjectionBindingActivation::new(
                binding.id(),
                binding.program_id(),
                ProjectionEpoch::new("projection-delta-v1").unwrap(),
                state,
                Some(ProjectionExecutorRoute::local(route).unwrap()),
            )],
            None,
        )
        .unwrap();
    let modeled = crate::graphql::SurfaceModeledProjection::try_from_catalog(
        program.clone(),
        &catalog,
        &active,
        binding.id(),
    )
    .unwrap();
    let surface = build_surface(&[todo_schema, user_schema], &SurfaceOptions::sqlite())
        .unwrap()
        .with_projectors([SurfaceProjector::new("projection-delta-test").modeled(modeled)])
        .unwrap();
    ModeledFixture { surface, program }
}

fn fanout_fixture() -> FanoutFixture {
    fanout_fixture_states(
        ProjectionBindingState::Active,
        ProjectionBindingState::Active,
    )
}

fn fanout_fixture_states(
    todo_state: ProjectionBindingState,
    user_state: ProjectionBindingState,
) -> FanoutFixture {
    let todo_schema = todos();
    let user_schema = users();
    let descriptor = event_descriptor();
    let selector = crate::ProjectionEventSelector::try_from_descriptor(&descriptor).unwrap();
    let todo_operation = ProjectionOperation::try_new(
        "fanout-todo",
        0,
        ProjectionMutationKind::Upsert,
        ProjectionTarget::try_new("TodoView", "todos").unwrap(),
        vec![projection_key(0, "todo_id", "todo_id")],
        vec![
            projection_field(0, "todo_id", "todo_id"),
            projection_field(1, "owner_id", "owner_id"),
            projection_field(2, "title", "title"),
            projection_field(3, "status", "status"),
        ],
        Vec::new(),
        vec![ProjectionInvalidation::model("TodoView").unwrap()],
    )
    .unwrap();
    let user_operation = ProjectionOperation::try_new(
        "fanout-user",
        0,
        ProjectionMutationKind::Upsert,
        ProjectionTarget::try_new("UserView", "users").unwrap(),
        vec![projection_key(0, "user_id", "owner_id")],
        vec![
            projection_field(0, "user_id", "owner_id"),
            projection_field(1, "display_name", "title"),
        ],
        Vec::new(),
        vec![ProjectionInvalidation::model("TodoView").unwrap()],
    )
    .unwrap();
    let todo_program = ProjectionProgram::try_new(
        "projection-delta-fanout-todo",
        1,
        ProjectionPartition::Expression(ProjectionExpression::constant(ProjectionValue::string(
            "tenant-a",
        ))),
        vec![ProjectionArm::try_new("todo", selector.clone(), vec![todo_operation]).unwrap()],
    )
    .unwrap();
    let user_program = ProjectionProgram::try_new(
        "projection-delta-fanout-user",
        1,
        ProjectionPartition::Expression(ProjectionExpression::constant(ProjectionValue::string(
            "tenant-a",
        ))),
        vec![ProjectionArm::try_new("user", selector, vec![user_operation]).unwrap()],
    )
    .unwrap();
    let owner = ProjectionOwner::try_new("projection-delta-fanout").unwrap();
    let source =
        ProjectionSourceBinding::try_new("todo-domain", "ordered-domain-events", 1).unwrap();
    let todo_binding = ProjectionBinding::from_eventual_program(
        &todo_program,
        source.clone(),
        owner.clone(),
        ProjectionExecutionClass::Causal,
        "distributed-projection-partition",
        PROJECTION_PARTITION_CODEC_VERSION,
        vec![ProjectionOutput::try_new("TodoView", "todos", todo_schema.clone()).unwrap()],
        Vec::new(),
        Some(ProjectionPhysicalTopology::from_protocol(
            &ProjectorTopologyId::new(1, "projection-delta-fanout", [0x55; 32]).unwrap(),
        )),
    )
    .unwrap();
    let user_binding = ProjectionBinding::from_eventual_program(
        &user_program,
        source,
        owner,
        ProjectionExecutionClass::Causal,
        "distributed-projection-partition",
        PROJECTION_PARTITION_CODEC_VERSION,
        vec![ProjectionOutput::try_new("UserView", "users", user_schema.clone()).unwrap()],
        Vec::new(),
        Some(ProjectionPhysicalTopology::from_protocol(
            &ProjectorTopologyId::new(1, "projection-delta-fanout", [0x55; 32]).unwrap(),
        )),
    )
    .unwrap();
    let catalog = crate::projection::catalog::ProjectionCatalog::try_new(vec![
        todo_binding.clone(),
        user_binding.clone(),
    ])
    .unwrap();
    let active = catalog
        .activate(
            vec![
                ProjectionBindingActivation::new(
                    todo_binding.id(),
                    todo_binding.program_id(),
                    ProjectionEpoch::new("projection-delta-fanout-v1").unwrap(),
                    todo_state,
                    Some(ProjectionExecutorRoute::local("delta-service").unwrap()),
                ),
                ProjectionBindingActivation::new(
                    user_binding.id(),
                    user_binding.program_id(),
                    ProjectionEpoch::new("projection-delta-fanout-v1").unwrap(),
                    user_state,
                    Some(ProjectionExecutorRoute::local("delta-service").unwrap()),
                ),
            ],
            None,
        )
        .unwrap();
    let todo_modeled = crate::graphql::SurfaceModeledProjection::try_from_catalog(
        todo_program.clone(),
        &catalog,
        &active,
        todo_binding.id(),
    )
    .unwrap();
    let user_modeled = crate::graphql::SurfaceModeledProjection::try_from_catalog(
        user_program.clone(),
        &catalog,
        &active,
        user_binding.id(),
    )
    .unwrap();
    let surface = build_surface(&[todo_schema, user_schema], &SurfaceOptions::sqlite())
        .unwrap()
        .with_projectors([SurfaceProjector::new("projection-delta-fanout")
            .modeled(todo_modeled)
            .modeled(user_modeled)])
        .unwrap();
    FanoutFixture {
        surface,
        todo_program,
        user_program,
    }
}

fn selected_export(surface: &crate::graphql::Surface) -> DistributedClientSurfaceExport {
    let surface = surface_with_modeled_command(surface);
    let selected = surface_for_role(
        &surface,
        "delta-user",
        &BTreeMap::from([
            ("TodoView".into(), RoleGrant::all_columns()),
            ("UserView".into(), RoleGrant::all_columns()),
        ]),
    )
    .unwrap();
    DistributedClientSurfaceExport::from_selected("delta-service", selected).unwrap()
}

#[cfg(feature = "graphql")]
fn selected_export_with_owner_policy(
    surface: &crate::graphql::Surface,
) -> DistributedClientSurfaceExport {
    let surface = surface_with_modeled_command(surface);
    let selected = surface_for_role(
        &surface,
        "delta-user",
        &BTreeMap::from([
            (
                "TodoView".into(),
                RoleGrant::all_columns()
                    .rows(crate::graphql::col("owner_id").eq(crate::graphql::claim("x-user-id"))),
            ),
            ("UserView".into(), RoleGrant::all_columns()),
        ]),
    )
    .unwrap();
    DistributedClientSurfaceExport::from_selected("delta-service", selected).unwrap()
}

fn selected_export_with_embedded_target(
    surface: &crate::graphql::Surface,
) -> DistributedClientSurfaceExport {
    let surface = surface_with_modeled_command(surface);
    let selected = surface_for_role(
        &surface,
        "delta-user",
        &BTreeMap::from([
            ("TodoView".into(), RoleGrant::all_columns()),
            ("UserView".into(), RoleGrant::columns(["display_name"])),
        ]),
    )
    .unwrap();
    DistributedClientSurfaceExport::from_selected("delta-service", selected).unwrap()
}

fn selected_export_with_embedded_source(
    surface: &crate::graphql::Surface,
) -> DistributedClientSurfaceExport {
    let surface = surface_with_modeled_command(surface);
    let selected = surface_for_role(
        &surface,
        "delta-user",
        &BTreeMap::from([
            (
                "TodoView".into(),
                RoleGrant::columns(["owner_id", "status", "title"]),
            ),
            ("UserView".into(), RoleGrant::all_columns()),
        ]),
    )
    .unwrap();
    DistributedClientSurfaceExport::from_selected("delta-service", selected).unwrap()
}

fn selected_export_join(surface: &crate::graphql::Surface) -> DistributedClientSurfaceExport {
    let surface = surface_with_modeled_command(surface);
    let selected = surface_for_role(
        &surface,
        "delta-user",
        &BTreeMap::from([
            ("TodoView".into(), RoleGrant::all_columns()),
            ("UserView".into(), RoleGrant::all_columns()),
            ("TodoOwnerLink".into(), RoleGrant::all_columns()),
        ]),
    )
    .unwrap();
    DistributedClientSurfaceExport::from_selected("delta-service", selected).unwrap()
}

fn surface_with_modeled_command(surface: &crate::graphql::Surface) -> crate::graphql::Surface {
    let contract = crate::command::typed_command::<
        ModeledCommandInput,
        crate::command::Eventual<ModeledCommandOutput>,
    >(TEST_COMMAND_NAME)
    .roles(["delta-user"])
    .emits(crate::command::__command_projection_events([Ok(
        event_descriptor(),
    )]))
    .into_contract();
    let binding = crate::command::TypedServiceCommandBinding::from_contracts(
        "delta-service",
        std::slice::from_ref(&contract),
    )
    .expect("modeled test service binding");
    let commands = crate::graphql::commands::TypedCommandInventory::from_contracts(&[contract])
        .expect("modeled test command inventory");
    surface
        .clone()
        .with_typed_commands(&commands)
        .expect("modeled test command")
        .with_service_binding(Some(binding))
}

fn projection_key(ordinal: u32, name: &str, body_field: &str) -> ProjectionKeyField {
    ProjectionKeyField::try_new(
        ordinal,
        name,
        ProjectionExpression::body_path(ProjectionValueType::String, [body_field]).unwrap(),
    )
    .unwrap()
}

fn projection_field(ordinal: u32, name: &str, body_field: &str) -> ProjectionField {
    ProjectionField::try_new(
        ordinal,
        name,
        ProjectionAssignment::Set(
            ProjectionExpression::body_path(ProjectionValueType::String, [body_field]).unwrap(),
        ),
    )
    .unwrap()
}

fn event_descriptor() -> DomainEventDescriptor {
    DomainEventDescriptor {
        name: Cow::Borrowed("todo.state-published"),
        version: 1,
        body: DomainEventBodyDescriptor {
            kind: DomainEventBodyKind::State,
            type_name: Cow::Borrowed("TodoState"),
            version: 1,
            schema: Cow::Borrowed("urn:distributed:test:projection-delta:v1"),
            fingerprint: Cow::Borrowed(BODY_FINGERPRINT),
            codec: Cow::Borrowed(DOMAIN_EVENT_BODY_CODEC),
            codec_version: DOMAIN_EVENT_BODY_CODEC_VERSION,
        },
    }
}

#[derive(Serialize)]
struct StateBody<'a> {
    todo_id: &'a str,
    owner_id: &'a str,
    old_owner_id: &'a str,
    title: &'a str,
    status: &'a str,
}

fn state_occurrence(sequence: u64, todo_id: &str, title: &str) -> DomainEventOccurrence {
    state_occurrence_with_metadata(
        sequence,
        todo_id,
        title,
        Some(TEST_CAUSATION_ID),
        BTreeMap::new(),
    )
}

fn state_occurrence_with_metadata(
    sequence: u64,
    todo_id: &str,
    title: &str,
    causation_id: Option<&str>,
    mut metadata: BTreeMap<String, String>,
) -> DomainEventOccurrence {
    if let Some(causation_id) = causation_id {
        metadata.insert("causation_id".into(), causation_id.into());
    }
    DomainEventOccurrence::capture(
        event_descriptor(),
        DomainEventEnvelope {
            aggregate_type: "todo".into(),
            aggregate_id: todo_id.into(),
            aggregate_sequence: sequence,
            publication_ordinal: 0,
            occurred_at: UNIX_EPOCH + Duration::from_secs(sequence),
            metadata,
        },
        &StateBody {
            todo_id,
            owner_id: "owner-secret",
            old_owner_id: "owner-old",
            title,
            status: "open",
        },
    )
    .unwrap()
}

fn partial_state_occurrence(sequence: u64, todo_id: &str) -> DomainEventOccurrence {
    let mut occurrence = DomainEventOccurrence::capture(
        event_descriptor(),
        DomainEventEnvelope {
            aggregate_type: "todo".into(),
            aggregate_id: todo_id.into(),
            aggregate_sequence: sequence,
            publication_ordinal: 0,
            occurred_at: UNIX_EPOCH + Duration::from_secs(sequence),
            metadata: BTreeMap::new(),
        },
        &json!({
            "todo_id": todo_id,
            "owner_id": "owner-secret",
            "old_owner_id": "owner-old",
            "status": "open"
        }),
    )
    .unwrap();
    occurrence.overwrite_causation_id(TEST_CAUSATION_ID);
    occurrence
}

fn todos() -> TableSchema {
    TableSchema {
        model_name: "TodoView".into(),
        table_name: "todos".into(),
        columns: vec![
            primary_column("todo_id"),
            column("owner_id"),
            column("title"),
            column("status"),
        ],
        primary_key: PrimaryKey::new(["todo_id"]),
        version_column: Some("_sourced_version".into()),
        foreign_keys: vec![],
        indexes: vec![],
        relationships: vec![RelationshipDef {
            field_name: "owner".into(),
            kind: RelationshipKind::BelongsTo,
            target_model: "UserView".into(),
            foreign_key: Some("owner_id".into()),
            through: None,
            target_foreign_key: None,
        }],
        kind: TableKind::ReadModel,
    }
}

fn users() -> TableSchema {
    TableSchema {
        model_name: "UserView".into(),
        table_name: "users".into(),
        columns: vec![primary_column("user_id"), column("display_name")],
        primary_key: PrimaryKey::new(["user_id"]),
        version_column: Some("_sourced_version".into()),
        foreign_keys: vec![],
        indexes: vec![],
        relationships: vec![],
        kind: TableKind::ReadModel,
    }
}

fn todo_owner_links() -> TableSchema {
    TableSchema {
        model_name: "TodoOwnerLink".into(),
        table_name: "private_todo_owner_links".into(),
        columns: vec![primary_column("todo_id"), primary_column("owner_id")],
        primary_key: PrimaryKey::new(["todo_id", "owner_id"]),
        version_column: Some("_sourced_version".into()),
        foreign_keys: vec![],
        indexes: vec![],
        relationships: vec![],
        kind: TableKind::ReadModel,
    }
}

fn column(name: &str) -> TableColumn {
    TableColumn::new(name, name, ColumnType::Text)
}

fn primary_column(name: &str) -> TableColumn {
    TableColumn {
        primary_key: true,
        ..column(name)
    }
}

const TEST_CACHE_SCOPE_TOKEN: &str = "v1.cache-scope.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const TEST_PARTITION_TOKEN: &str =
    "v1.projection-partition.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

struct TestRequestAuthority {
    calls: Cell<usize>,
    generation: String,
    principal: crate::command_ledger::PrincipalPartitionId,
    cache_scope: ProjectionDeltaCacheScopeToken,
    causation: crate::command_ledger::CausationId,
    record: AuthorizationTransition,
    relationship: AuthorizationTransition,
    partition_result: PartitionResult,
}

#[derive(Clone, Copy)]
enum PartitionResult {
    Opaque,
    Unit,
}

impl TestRequestAuthority {
    fn authorized(generation: &str) -> Self {
        Self::new(generation, authorized_transition(), authorized_transition())
    }

    fn new(
        generation: &str,
        record: AuthorizationTransition,
        relationship: AuthorizationTransition,
    ) -> Self {
        Self {
            calls: Cell::new(0),
            generation: generation.into(),
            principal: crate::command_ledger::PrincipalPartitionId::new("principal-scope-9")
                .unwrap(),
            cache_scope: ProjectionDeltaCacheScopeToken::parse_wire(TEST_CACHE_SCOPE_TOKEN)
                .unwrap(),
            causation: crate::command_ledger::CausationId::parse_stored(
                TEST_CAUSATION_ID.to_owned(),
            )
            .unwrap(),
            record,
            relationship,
            partition_result: PartitionResult::Opaque,
        }
    }
}

impl ProjectionPartitionScopeEncoder for TestRequestAuthority {
    fn encode(
        &self,
        authority: ProjectionPartitionAuthority<'_>,
        partition: &crate::ResolvedProjectionPartition,
    ) -> Result<Option<ProjectionDeltaPartition>, ProjectionDeltaError> {
        self.calls.set(self.calls.get() + 1);
        assert_eq!(
            authority.surface,
            &ProjectionDeltaSurfaceIdentity::Role {
                name: "delta-user".into()
            }
        );
        assert!(!authority.authorization_generation.is_empty());
        assert!(!authority.principal_scope.as_str().is_empty());
        assert_eq!(authority.cache_scope, &self.cache_scope);
        assert!(!partition.canonical_bytes().is_empty());
        Ok(Some(match self.partition_result {
            PartitionResult::Opaque => ProjectionDeltaPartition::Opaque {
                token: TEST_PARTITION_TOKEN.into(),
            },
            PartitionResult::Unit => ProjectionDeltaPartition::Unit,
        }))
    }
}

impl ProjectionVisibilityEvaluator for TestRequestAuthority {
    fn record_transition(
        &self,
        _source: ProjectionMutationSource,
        _mutation: &ResolvedProjectionMutation,
    ) -> Result<AuthorizationTransition, ProjectionDeltaError> {
        Ok(self.record)
    }

    fn relationship_transition(
        &self,
        _source: ProjectionMutationSource,
        _effect: &ResolvedProjectionRelationshipEffect,
    ) -> Result<AuthorizationTransition, ProjectionDeltaError> {
        Ok(self.relationship)
    }
}

impl ProjectionDeltaRequestAuthority for TestRequestAuthority {
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

fn empty_delta() -> ProjectionDelta {
    ProjectionDelta {
        wire_version: PROJECTION_DELTA_WIRE_VERSION,
        identity: ProjectionDeltaIdentity {
            manifest_version: crate::graphql::client_manifest::DISTRIBUTED_CLIENT_MANIFEST_VERSION,
            client_protocol_version:
                crate::graphql::client_manifest::DISTRIBUTED_CLIENT_PROTOCOL_VERSION,
            surface: ProjectionDeltaSurfaceIdentity::Role {
                name: "delta-user".into(),
            },
            schema_fingerprint: "sha256:schema".into(),
            protocol_fingerprint: "sha256:protocol".into(),
            authorization_generation: "generation-1".into(),
            cache_scope_token: TEST_CACHE_SCOPE_TOKEN.into(),
            command_causation_id: "cause-1".into(),
        },
        projections: vec![],
        occurrences: vec![],
        operations: vec![],
        recoveries: vec![],
    }
}

fn operation(
    occurrence_ordinal: u32,
    mutation: ProjectionDeltaMutation,
) -> ProjectionDeltaOperation {
    ProjectionDeltaOperation {
        occurrence_ordinal,
        projection_refs: vec![0],
        mutation,
    }
}

fn test_scope(_partition: &str, model: &str, key: Vec<DeltaKeyField>) -> ProjectionDeltaScope {
    ProjectionDeltaScope {
        partition: ProjectionDeltaPartition::Opaque {
            token: TEST_PARTITION_TOKEN.into(),
        },
        model: model.into(),
        key,
    }
}

fn key_field(ordinal: u32, field: &str, value: &str) -> DeltaKeyField {
    DeltaKeyField {
        ordinal,
        field: field.into(),
        value: DeltaValue::String(value.into()),
    }
}
