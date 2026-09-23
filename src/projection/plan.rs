use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value;

use crate::projection_protocol::{MAX_PROJECTION_PARTITION_BYTES, MAX_PROJECTION_RECORD_KEY_BYTES};
use crate::DomainEventOccurrence;

use super::canonical::{bounded_key_bytes, bounded_partition_bytes, canonical_json_bytes};
use super::{
    ProjectionArm, ProjectionInvalidation, ProjectionKeyField, ProjectionMutationKind,
    ProjectionOperation, ProjectionPartition, ProjectionProgram, ProjectionProgramError,
    ProjectionProgramId, ProjectionRelationship, ProjectionRelationshipEffectKind,
    ProjectionTarget, ProjectionValue, ResolvedProjectionValue,
};

/// Resolved logical partition and its bounded canonical encoding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ResolvedProjectionPartition {
    partition: ResolvedProjectionPartitionValue,
    canonical_bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum ResolvedProjectionPartitionValue {
    Unit,
    Value(ProjectionValue),
}

/// Borrowed view of a resolved unit or value partition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResolvedProjectionPartitionRef<'a> {
    /// The declaration uses the distinguished unit partition.
    Unit,
    /// The declaration resolved an explicit partition value.
    Value(&'a ProjectionValue),
}

impl ResolvedProjectionPartition {
    /// Return the distinguished unit tag or concrete logical value.
    pub fn as_ref(&self) -> ResolvedProjectionPartitionRef<'_> {
        match &self.partition {
            ResolvedProjectionPartitionValue::Unit => ResolvedProjectionPartitionRef::Unit,
            ResolvedProjectionPartitionValue::Value(value) => {
                ResolvedProjectionPartitionRef::Value(value)
            }
        }
    }

    /// Return the bounded domain-separated canonical partition encoding.
    ///
    /// This is a portable logical encoding. A physical adapter must lower the
    /// logical value through its registered model codec; these bytes are not a
    /// relational table key.
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }
}

/// One concrete component of a composite logical key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ResolvedProjectionKeyField {
    ordinal: u32,
    name: String,
    value: ProjectionValue,
}

impl ResolvedProjectionKeyField {
    /// Return the declared component ordinal.
    pub fn ordinal(&self) -> u32 {
        self.ordinal
    }

    /// Return the declared component name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the resolved scalar or typed-enum value.
    pub fn value(&self) -> &ProjectionValue {
        &self.value
    }
}

/// A concrete complete key with its bounded canonical encoding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ResolvedProjectionKey {
    fields: Vec<ResolvedProjectionKeyField>,
    canonical_bytes: Vec<u8>,
}

impl ResolvedProjectionKey {
    /// Return components in explicit ordinal order.
    pub fn fields(&self) -> &[ResolvedProjectionKeyField] {
        &self.fields
    }

    /// Return the bounded domain-separated canonical key encoding.
    ///
    /// This is a portable logical encoding, not a physical ORM key encoding.
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }
}

/// Complete portable scope of one final logical record mutation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ResolvedProjectionMutationScope {
    partition: ResolvedProjectionPartition,
    model: String,
    storage: String,
    key: ResolvedProjectionKey,
}

impl ResolvedProjectionMutationScope {
    /// Return the logical projection partition.
    pub fn partition(&self) -> &ResolvedProjectionPartition {
        &self.partition
    }

    /// Return the logical model name.
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Return the opaque registered storage identity.
    pub fn storage(&self) -> &str {
        &self.storage
    }

    /// Return the complete portable logical key.
    pub fn key(&self) -> &ResolvedProjectionKey {
        &self.key
    }
}

/// One concrete projected field, retaining source ordinals and presence state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ResolvedProjectionField {
    operation_staging_ordinal: u32,
    field_ordinal: u32,
    name: String,
    value: ResolvedProjectionValue,
}

impl ResolvedProjectionField {
    /// Return the operation staging ordinal that first supplied this field.
    pub fn operation_staging_ordinal(&self) -> u32 {
        self.operation_staging_ordinal
    }

    /// Return the field ordinal within its source operation.
    pub fn field_ordinal(&self) -> u32 {
        self.field_ordinal
    }

    /// Return the projected field name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return concrete, absent, or explicit-unset state.
    pub fn value(&self) -> &ResolvedProjectionValue {
        &self.value
    }
}

/// Exact semantic origins retained for one final logical mutation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ProjectionMutationProvenance {
    occurrence: ProjectionOccurrenceProvenance,
    program_id: ProjectionProgramId,
    arm_id: String,
    operation_ids: Vec<String>,
    staging_ordinals: Vec<u32>,
    relationship_effects: Vec<ResolvedProjectionRelationshipEffect>,
    invalidations: Vec<ProjectionInvalidation>,
}

/// One resolved link, unlink, or conservative relationship invalidation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ResolvedProjectionRelationshipEffect {
    ordinal: u32,
    kind: ProjectionRelationshipEffectKind,
    relationship: ProjectionRelationship,
    source_key: Option<ResolvedProjectionKey>,
    target_key: Option<ResolvedProjectionKey>,
}

impl ResolvedProjectionRelationshipEffect {
    /// Return the explicit effect ordinal.
    pub fn ordinal(&self) -> u32 {
        self.ordinal
    }

    /// Return link, unlink, or invalidation.
    pub fn kind(&self) -> ProjectionRelationshipEffectKind {
        self.kind
    }

    /// Return the stable relationship descriptor.
    pub fn relationship(&self) -> &ProjectionRelationship {
        &self.relationship
    }

    /// Return the complete source endpoint key for link or unlink.
    pub fn source_key(&self) -> Option<&ResolvedProjectionKey> {
        self.source_key.as_ref()
    }

    /// Return the complete target endpoint key for link or unlink.
    pub fn target_key(&self) -> Option<&ResolvedProjectionKey> {
        self.target_key.as_ref()
    }
}

/// Retry-stable semantic identity of the occurrence behind one mutation.
///
/// Volatile timestamps, tracing, workflow metadata, and delivery state are
/// excluded. The parent plan retains the complete immutable occurrence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ProjectionOccurrenceProvenance {
    occurrence_version: u16,
    occurrence_id: String,
    event_name: String,
    event_version: u64,
    body_fingerprint: String,
    aggregate_type: String,
    aggregate_id: String,
    aggregate_sequence: u64,
    publication_ordinal: u32,
}

impl ProjectionOccurrenceProvenance {
    /// Return the canonical occurrence-envelope version.
    pub fn occurrence_version(&self) -> u16 {
        self.occurrence_version
    }

    /// Return the retry-stable occurrence ID.
    pub fn occurrence_id(&self) -> &str {
        &self.occurrence_id
    }

    /// Return the semantic event name.
    pub fn event_name(&self) -> &str {
        &self.event_name
    }

    /// Return the semantic event version.
    pub fn event_version(&self) -> u64 {
        self.event_version
    }

    /// Return the canonical event-body schema fingerprint.
    pub fn body_fingerprint(&self) -> &str {
        &self.body_fingerprint
    }

    /// Return the stable aggregate type.
    pub fn aggregate_type(&self) -> &str {
        &self.aggregate_type
    }

    /// Return the stable aggregate stream ID.
    pub fn aggregate_id(&self) -> &str {
        &self.aggregate_id
    }

    /// Return the causing aggregate sequence.
    pub fn aggregate_sequence(&self) -> u64 {
        self.aggregate_sequence
    }

    /// Return the event publication ordinal within that aggregate sequence.
    pub fn publication_ordinal(&self) -> u32 {
        self.publication_ordinal
    }
}

impl ProjectionMutationProvenance {
    /// Return stable occurrence identity retained directly by the mutation.
    pub fn occurrence(&self) -> &ProjectionOccurrenceProvenance {
        &self.occurrence
    }

    /// Return the program digest that defined this mutation.
    pub fn program_id(&self) -> ProjectionProgramId {
        self.program_id
    }

    /// Return the selected arm identifier.
    pub fn arm_id(&self) -> &str {
        &self.arm_id
    }

    /// Return contributing operation IDs in staging order.
    pub fn operation_ids(&self) -> &[String] {
        &self.operation_ids
    }

    /// Return contributing source staging ordinals.
    pub fn staging_ordinals(&self) -> &[u32] {
        &self.staging_ordinals
    }

    /// Return resolved relationship consequences in explicit ordinal order.
    pub fn relationship_effects(&self) -> &[ResolvedProjectionRelationshipEffect] {
        &self.relationship_effects
    }

    /// Return canonical affected model and relationship inventory.
    pub fn invalidations(&self) -> &[ProjectionInvalidation] {
        &self.invalidations
    }
}

/// One final authoritative, adapter-neutral record mutation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ResolvedProjectionMutation {
    kind: ProjectionMutationKind,
    target: ProjectionTarget,
    scope: ResolvedProjectionMutationScope,
    fields: Vec<ResolvedProjectionField>,
    provenance: ProjectionMutationProvenance,
}

impl ResolvedProjectionMutation {
    /// Return the authoritative mutation kind.
    pub fn kind(&self) -> ProjectionMutationKind {
        self.kind
    }

    /// Return the portable logical target.
    pub fn target(&self) -> &ProjectionTarget {
        &self.target
    }

    /// Return the complete logical key.
    pub fn key(&self) -> &ResolvedProjectionKey {
        &self.scope.key
    }

    /// Return the complete partition/model/storage/key scope.
    pub fn scope(&self) -> &ResolvedProjectionMutationScope {
        &self.scope
    }

    /// Return fields in stable source-ordinal order.
    pub fn fields(&self) -> &[ResolvedProjectionField] {
        &self.fields
    }

    /// Return exact event-program-operation provenance.
    pub fn provenance(&self) -> &ProjectionMutationProvenance {
        &self.provenance
    }
}

/// One event occurrence evaluated through one canonical projection program.
///
/// This is a logical semantic plan. It deliberately exposes no constructor
/// from a physical `TableWritePlan`; adapters may only lower from this type.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ResolvedProjectionPlan {
    program_id: ProjectionProgramId,
    occurrence: DomainEventOccurrence,
    arm_id: String,
    partition: ResolvedProjectionPartition,
    mutations: Vec<ResolvedProjectionMutation>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    source_snapshots: bool,
}

impl ResolvedProjectionPlan {
    /// Resolve one occurrence against a validated projection program.
    ///
    /// Public so mutation-backed descriptors share the same resolution entry
    /// point as generated `projection!` programs.
    ///
    /// # Errors
    ///
    /// Rejects selector mismatches, invalid body values, and bounded-value
    /// violations.
    pub fn resolve(
        program: &ProjectionProgram,
        occurrence: &DomainEventOccurrence,
    ) -> Result<Self, ProjectionProgramError> {
        if program.source_snapshots() && occurrence.derivation().is_some() {
            return Err(ProjectionProgramError::DerivedSourceSnapshot);
        }
        let matches = program
            .arms()
            .iter()
            .filter(|arm| arm.selector().matches(occurrence))
            .collect::<Vec<_>>();
        let arm = match matches.as_slice() {
            [] => return Err(ProjectionProgramError::NoMatchingArm),
            [arm] => *arm,
            _ => return Err(ProjectionProgramError::MultipleMatchingArms),
        };
        let body: Value = occurrence
            .decode_body()
            .map_err(|error| ProjectionProgramError::CanonicalJson(error.to_string()))?;
        let program_id = program.id()?;
        let partition = resolve_partition(program.partition(), occurrence, &body)?;
        let mut mutations = arm
            .operations()
            .iter()
            .map(|operation| {
                resolve_operation(program_id, arm, operation, occurrence, &body, &partition)
            })
            .collect::<Result<Vec<_>, _>>()?;
        mutations = coalesce_mutations(mutations)?;
        mutations.sort_by(|left, right| {
            left.kind
                .cmp(&right.kind)
                .then_with(|| left.target.model().cmp(right.target.model()))
                .then_with(|| left.target.storage().cmp(right.target.storage()))
                .then_with(|| {
                    left.scope
                        .key
                        .canonical_bytes
                        .cmp(&right.scope.key.canonical_bytes)
                })
                .then_with(|| {
                    left.provenance.staging_ordinals[0].cmp(&right.provenance.staging_ordinals[0])
                })
        });
        Ok(Self {
            program_id,
            occurrence: occurrence.clone(),
            arm_id: arm.arm_id().to_owned(),
            partition,
            mutations,
            source_snapshots: program.source_snapshots(),
        })
    }

    /// Whether full-state writes require authoritative source-version fencing.
    pub fn source_snapshots(&self) -> bool {
        self.source_snapshots
    }

    /// Return the canonical program identity.
    pub fn program_id(&self) -> ProjectionProgramId {
        self.program_id
    }

    /// Return the exact immutable occurrence that was evaluated.
    pub fn occurrence(&self) -> &DomainEventOccurrence {
        &self.occurrence
    }

    /// Return the selected arm identifier.
    pub fn arm_id(&self) -> &str {
        &self.arm_id
    }

    /// Return the concrete bounded logical partition.
    pub fn partition(&self) -> &ResolvedProjectionPartition {
        &self.partition
    }

    /// Return at most one final mutation for each partition/model/key scope.
    pub fn mutations(&self) -> &[ResolvedProjectionMutation] {
        &self.mutations
    }

    /// Encode canonical JSON for deterministic conformance and handoff.
    ///
    /// # Errors
    ///
    /// Returns a typed error if canonical serialization fails.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ProjectionProgramError> {
        canonical_json_bytes(self)
    }
}

fn resolve_partition(
    partition: &ProjectionPartition,
    occurrence: &DomainEventOccurrence,
    body: &Value,
) -> Result<ResolvedProjectionPartition, ProjectionProgramError> {
    let partition = match partition {
        ProjectionPartition::Unit => ResolvedProjectionPartitionValue::Unit,
        ProjectionPartition::Expression(expression) => {
            let value = match expression.resolve(occurrence, body)? {
                ResolvedProjectionValue::Value(value) => value,
                ResolvedProjectionValue::Absent => {
                    return Err(ProjectionProgramError::RequiredValueAbsent {
                        path: "projection partition".to_owned(),
                    });
                }
                ResolvedProjectionValue::Unset => {
                    return Err(ProjectionProgramError::UnsetNotAllowed {
                        field: "projection partition".to_owned(),
                    });
                }
            };
            ResolvedProjectionPartitionValue::Value(value)
        }
    };
    let canonical_bytes = bounded_partition_bytes(&partition, MAX_PROJECTION_PARTITION_BYTES)?;
    Ok(ResolvedProjectionPartition {
        partition,
        canonical_bytes,
    })
}

fn resolve_operation(
    program_id: ProjectionProgramId,
    arm: &ProjectionArm,
    operation: &ProjectionOperation,
    occurrence: &DomainEventOccurrence,
    body: &Value,
    partition: &ResolvedProjectionPartition,
) -> Result<ResolvedProjectionMutation, ProjectionProgramError> {
    let key = resolve_key(operation.key(), occurrence, body)?;
    let mut fields = Vec::with_capacity(operation.fields().len());
    for field in operation.fields() {
        let value = field.assignment().resolve(occurrence, body)?;
        if operation.kind().is_complete_write() {
            match &value {
                ResolvedProjectionValue::Absent => {
                    return Err(ProjectionProgramError::RequiredValueAbsent {
                        path: field.name().to_owned(),
                    });
                }
                ResolvedProjectionValue::Unset => {
                    return Err(ProjectionProgramError::UnsetNotAllowed {
                        field: field.name().to_owned(),
                    });
                }
                ResolvedProjectionValue::Value(_) => {}
            }
        }
        fields.push(ResolvedProjectionField {
            operation_staging_ordinal: operation.staging_ordinal(),
            field_ordinal: field.ordinal(),
            name: field.name().to_owned(),
            value,
        });
    }
    let relationship_effects = operation
        .relationship_effects()
        .iter()
        .map(|effect| {
            let (source_key, target_key) = match effect.kind() {
                ProjectionRelationshipEffectKind::Link
                | ProjectionRelationshipEffectKind::Unlink => (
                    Some(resolve_key(effect.source_key(), occurrence, body)?),
                    Some(resolve_key(effect.target_key(), occurrence, body)?),
                ),
                ProjectionRelationshipEffectKind::Invalidate => (
                    Some(resolve_key(effect.source_key(), occurrence, body)?),
                    None,
                ),
            };
            Ok(ResolvedProjectionRelationshipEffect {
                ordinal: effect.ordinal(),
                kind: effect.kind(),
                relationship: effect.relationship().clone(),
                source_key,
                target_key,
            })
        })
        .collect::<Result<Vec<_>, ProjectionProgramError>>()?;
    Ok(ResolvedProjectionMutation {
        kind: operation.kind(),
        target: operation.target().clone(),
        scope: ResolvedProjectionMutationScope {
            partition: partition.clone(),
            model: operation.target().model().to_owned(),
            storage: operation.target().storage().to_owned(),
            key,
        },
        fields,
        provenance: ProjectionMutationProvenance {
            occurrence: ProjectionOccurrenceProvenance {
                occurrence_version: occurrence.occurrence_version(),
                occurrence_id: occurrence.id().to_owned(),
                event_name: occurrence.descriptor().name.to_string(),
                event_version: occurrence.descriptor().version,
                body_fingerprint: occurrence.descriptor().body.fingerprint.to_string(),
                aggregate_type: occurrence.aggregate_type().to_owned(),
                aggregate_id: occurrence.aggregate_id().to_owned(),
                aggregate_sequence: occurrence.aggregate_sequence(),
                publication_ordinal: occurrence.publication_ordinal(),
            },
            program_id,
            arm_id: arm.arm_id().to_owned(),
            operation_ids: vec![operation.operation_id().to_owned()],
            staging_ordinals: vec![operation.staging_ordinal()],
            relationship_effects,
            invalidations: operation.invalidations().to_vec(),
        },
    })
}

fn resolve_key(
    key: &[ProjectionKeyField],
    occurrence: &DomainEventOccurrence,
    body: &Value,
) -> Result<ResolvedProjectionKey, ProjectionProgramError> {
    let mut key_fields = Vec::with_capacity(key.len());
    for field in key {
        let value = match field.expression().resolve(occurrence, body)? {
            ResolvedProjectionValue::Value(value) if value.valid_key_component() => value,
            _ => {
                return Err(ProjectionProgramError::InvalidKeyValue {
                    field: field.name().to_owned(),
                });
            }
        };
        key_fields.push(ResolvedProjectionKeyField {
            ordinal: field.ordinal(),
            name: field.name().to_owned(),
            value,
        });
    }
    let canonical_bytes = bounded_key_bytes(&key_fields, MAX_PROJECTION_RECORD_KEY_BYTES)?;
    Ok(ResolvedProjectionKey {
        fields: key_fields,
        canonical_bytes,
    })
}

fn coalesce_mutations(
    mutations: Vec<ResolvedProjectionMutation>,
) -> Result<Vec<ResolvedProjectionMutation>, ProjectionProgramError> {
    let mut by_scope: BTreeMap<(String, String, Vec<u8>), ResolvedProjectionMutation> =
        BTreeMap::new();
    for mutation in mutations {
        let scope = (
            mutation.target.model().to_owned(),
            mutation.target.storage().to_owned(),
            mutation.scope.key.canonical_bytes.clone(),
        );
        if let Some(existing) = by_scope.get_mut(&scope) {
            merge_mutation(existing, mutation)?;
        } else {
            by_scope.insert(scope, mutation);
        }
    }
    Ok(by_scope.into_values().collect())
}

fn merge_mutation(
    existing: &mut ResolvedProjectionMutation,
    incoming: ResolvedProjectionMutation,
) -> Result<(), ProjectionProgramError> {
    if existing.kind != incoming.kind
        || existing.target != incoming.target
        || existing.scope != incoming.scope
        || existing.provenance.relationship_effects != incoming.provenance.relationship_effects
    {
        return Err(ProjectionProgramError::AmbiguousMutation {
            model: existing.target.model().to_owned(),
            reason: "same record scope resolves to incompatible logical mutations".to_owned(),
        });
    }

    if existing.kind.is_patch() {
        for field in incoming.fields {
            if let Some(prior) = existing
                .fields
                .iter()
                .find(|candidate| candidate.name == field.name)
            {
                if prior.value != field.value {
                    return Err(ProjectionProgramError::AmbiguousMutation {
                        model: existing.target.model().to_owned(),
                        reason: format!(
                            "field `{}` receives conflicting values in one occurrence",
                            field.name
                        ),
                    });
                }
            } else {
                existing.fields.push(field);
            }
        }
        existing.fields.sort_by(|left, right| {
            left.operation_staging_ordinal
                .cmp(&right.operation_staging_ordinal)
                .then_with(|| left.field_ordinal.cmp(&right.field_ordinal))
                .then_with(|| left.name.cmp(&right.name))
        });
    } else if !same_resolved_fields_ignoring_staging(&existing.fields, &incoming.fields) {
        return Err(ProjectionProgramError::AmbiguousMutation {
            model: existing.target.model().to_owned(),
            reason: "duplicate complete mutations are not byte-identical".to_owned(),
        });
    }

    existing
        .provenance
        .operation_ids
        .extend(incoming.provenance.operation_ids);
    existing
        .provenance
        .staging_ordinals
        .extend(incoming.provenance.staging_ordinals);
    existing
        .provenance
        .invalidations
        .extend(incoming.provenance.invalidations);
    existing.provenance.invalidations.sort();
    existing.provenance.invalidations.dedup();
    Ok(())
}

fn same_resolved_fields_ignoring_staging(
    left: &[ResolvedProjectionField],
    right: &[ResolvedProjectionField],
) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            left.field_ordinal == right.field_ordinal
                && left.name == right.name
                && left.value == right.value
        })
}
