use std::collections::BTreeSet;
use std::fmt;
use std::marker::PhantomData;

use serde::{Serialize, Serializer};

use crate::projection_protocol::{MAX_PROJECTION_PARTITION_BYTES, MAX_PROJECTION_RECORD_KEY_BYTES};
use crate::DomainEventOccurrence;

use super::canonical::{canonical_json_bytes, digest_program};
use super::expression::{
    expressions_statically_distinct, non_empty, validate_named_ordinals, validate_ordinals,
};
use super::{
    ProjectionAssignment, ProjectionEventSelector, ProjectionExpression, ProjectionInvalidation,
    ProjectionProgramError, ProjectionRelationship, ProjectionTarget, ResolvedProjectionPlan,
};
use super::{MAX_PROJECTION_EXPRESSION_DEPTH, MAX_PROJECTION_PATH_SEGMENTS};

/// Canonical portable projection IR version.
pub const PROJECTION_PROGRAM_IR_VERSION: u16 = 1;

/// Canonical logical mutation semantics version.
pub const PROJECTION_OPERATION_SEMANTICS_VERSION: u16 = 1;

/// Maximum operations one selected arm may resolve for one occurrence.
pub const MAX_PROJECTION_OPERATIONS_PER_OCCURRENCE: usize = 128;

/// Version-one resource ceilings included in every program digest.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct ProjectionProgramLimits {
    expression_value_levels: u16,
    path_segments: u16,
    operations_per_occurrence: u16,
    key_bytes: u32,
    partition_bytes: u32,
}

impl ProjectionProgramLimits {
    fn version_one() -> Self {
        Self {
            expression_value_levels: MAX_PROJECTION_EXPRESSION_DEPTH as u16,
            path_segments: MAX_PROJECTION_PATH_SEGMENTS as u16,
            operations_per_occurrence: MAX_PROJECTION_OPERATIONS_PER_OCCURRENCE as u16,
            key_bytes: MAX_PROJECTION_RECORD_KEY_BYTES as u32,
            partition_bytes: MAX_PROJECTION_PARTITION_BYTES as u32,
        }
    }

    /// Return the expression and literal nesting ceiling.
    pub fn expression_value_levels(&self) -> u16 {
        self.expression_value_levels
    }

    /// Return the body-path segment ceiling.
    pub fn path_segments(&self) -> u16 {
        self.path_segments
    }

    /// Return the selected-arm operation ceiling.
    pub fn operations_per_occurrence(&self) -> u16 {
        self.operations_per_occurrence
    }

    /// Return the portable logical key encoding ceiling.
    pub fn key_bytes(&self) -> u32 {
        self.key_bytes
    }

    /// Return the portable logical partition encoding ceiling.
    pub fn partition_bytes(&self) -> u32 {
        self.partition_bytes
    }
}

/// How a program derives its logical projection partition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "expression", rename_all = "snake_case")]
pub enum ProjectionPartition {
    /// All occurrences share one logical unit partition.
    Unit,
    /// Evaluate an explicit deterministic partition expression.
    Expression(ProjectionExpression),
}

/// One ordered component of a logical composite key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ProjectionKeyField {
    ordinal: u32,
    name: String,
    expression: ProjectionExpression,
}

impl ProjectionKeyField {
    /// Construct one explicitly ordered key component.
    ///
    /// # Errors
    ///
    /// Rejects an empty field name.
    pub fn try_new(
        ordinal: u32,
        name: impl Into<String>,
        expression: ProjectionExpression,
    ) -> Result<Self, ProjectionProgramError> {
        Ok(Self {
            ordinal,
            name: non_empty(name.into(), "projection key field")?,
            expression,
        })
    }

    /// Return the explicit zero-based key ordinal.
    pub fn ordinal(&self) -> u32 {
        self.ordinal
    }

    /// Return the stable key field name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the value expression.
    pub fn expression(&self) -> &ProjectionExpression {
        &self.expression
    }
}

/// One ordered field assignment in a logical mutation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ProjectionField {
    ordinal: u32,
    name: String,
    assignment: ProjectionAssignment,
}

impl ProjectionField {
    /// Construct one explicitly ordered field assignment.
    ///
    /// # Errors
    ///
    /// Rejects an empty field name.
    pub fn try_new(
        ordinal: u32,
        name: impl Into<String>,
        assignment: ProjectionAssignment,
    ) -> Result<Self, ProjectionProgramError> {
        Ok(Self {
            ordinal,
            name: non_empty(name.into(), "projection field")?,
            assignment,
        })
    }

    /// Return the explicit zero-based field ordinal.
    pub fn ordinal(&self) -> u32 {
        self.ordinal
    }

    /// Return the stable projected field name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the portable assignment.
    pub fn assignment(&self) -> &ProjectionAssignment {
        &self.assignment
    }
}

/// Closed authoritative logical mutation set.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionMutationKind {
    /// Create a row and conflict if it already exists.
    Insert,
    /// Replace or create a complete row.
    Upsert,
    /// Modify selected fields of an existing row.
    Patch,
    /// Create a partial row or patch an existing row.
    UpsertPatch,
    /// Remove a row by its complete logical key.
    Delete,
    /// Delete and then insert a complete row as one logical mutation.
    Recreate,
    /// Insert a relationship-owned row.
    InsertRelated,
    /// Replace or create a complete relationship-owned row.
    UpsertRelated,
}

impl ProjectionMutationKind {
    pub(crate) fn is_complete_write(self) -> bool {
        matches!(
            self,
            Self::Insert
                | Self::Upsert
                | Self::Recreate
                | Self::InsertRelated
                | Self::UpsertRelated
        )
    }

    pub(crate) fn is_patch(self) -> bool {
        matches!(self, Self::Patch | Self::UpsertPatch)
    }

    fn is_related(self) -> bool {
        matches!(self, Self::InsertRelated | Self::UpsertRelated)
    }
}

/// Derived relationship consequence carried by an authoritative row mutation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionRelationshipEffectKind {
    /// The mutation proves a relationship edge is added.
    Link,
    /// The mutation proves a relationship edge is removed.
    Unlink,
    /// Endpoint identity is not safely known; invalidate the relationship.
    Invalidate,
}

/// Portable relationship provenance with explicit endpoint-key expressions.
///
/// Link and unlink remain derived replica consequences, never authoritative
/// server table operations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ProjectionRelationshipEffect {
    ordinal: u32,
    kind: ProjectionRelationshipEffectKind,
    relationship: ProjectionRelationship,
    source_key: Vec<ProjectionKeyField>,
    target_key: Vec<ProjectionKeyField>,
}

impl ProjectionRelationshipEffect {
    /// Declare a provable link using complete source and target keys.
    ///
    /// # Errors
    ///
    /// Rejects empty, duplicate, or non-contiguous endpoint key declarations.
    pub fn link(
        ordinal: u32,
        relationship: ProjectionRelationship,
        source_key: Vec<ProjectionKeyField>,
        target_key: Vec<ProjectionKeyField>,
    ) -> Result<Self, ProjectionProgramError> {
        Self::with_keys(
            ordinal,
            ProjectionRelationshipEffectKind::Link,
            relationship,
            source_key,
            target_key,
        )
    }

    /// Declare a provable unlink using complete old source and target keys.
    ///
    /// # Errors
    ///
    /// Rejects empty, duplicate, or non-contiguous endpoint key declarations.
    pub fn unlink(
        ordinal: u32,
        relationship: ProjectionRelationship,
        source_key: Vec<ProjectionKeyField>,
        target_key: Vec<ProjectionKeyField>,
    ) -> Result<Self, ProjectionProgramError> {
        Self::with_keys(
            ordinal,
            ProjectionRelationshipEffectKind::Unlink,
            relationship,
            source_key,
            target_key,
        )
    }

    /// Declare conservative invalidation rooted at a complete source key.
    ///
    /// # Errors
    ///
    /// Rejects an empty, duplicate, or non-contiguous source key.
    pub fn invalidate(
        ordinal: u32,
        relationship: ProjectionRelationship,
        mut source_key: Vec<ProjectionKeyField>,
    ) -> Result<Self, ProjectionProgramError> {
        source_key.sort_by_key(ProjectionKeyField::ordinal);
        validate_named_ordinals(
            &source_key,
            "relationship invalidation source key",
            ProjectionKeyField::ordinal,
            ProjectionKeyField::name,
        )?;
        if source_key.is_empty() {
            return Err(ProjectionProgramError::InvalidOperation {
                operation: "relationship invalidation".to_owned(),
                reason: "relationship invalidation requires a complete source key".to_owned(),
            });
        }
        Ok(Self {
            ordinal,
            kind: ProjectionRelationshipEffectKind::Invalidate,
            relationship,
            source_key,
            target_key: Vec::new(),
        })
    }

    fn with_keys(
        ordinal: u32,
        kind: ProjectionRelationshipEffectKind,
        relationship: ProjectionRelationship,
        mut source_key: Vec<ProjectionKeyField>,
        mut target_key: Vec<ProjectionKeyField>,
    ) -> Result<Self, ProjectionProgramError> {
        source_key.sort_by_key(ProjectionKeyField::ordinal);
        target_key.sort_by_key(ProjectionKeyField::ordinal);
        validate_named_ordinals(
            &source_key,
            "relationship source key",
            ProjectionKeyField::ordinal,
            ProjectionKeyField::name,
        )?;
        validate_named_ordinals(
            &target_key,
            "relationship target key",
            ProjectionKeyField::ordinal,
            ProjectionKeyField::name,
        )?;
        if source_key.is_empty() || target_key.is_empty() {
            return Err(ProjectionProgramError::InvalidOperation {
                operation: "relationship effect".to_owned(),
                reason: "link and unlink require complete source and target keys".to_owned(),
            });
        }
        Ok(Self {
            ordinal,
            kind,
            relationship,
            source_key,
            target_key,
        })
    }

    /// Return the explicit effect ordinal within its operation.
    pub fn ordinal(&self) -> u32 {
        self.ordinal
    }

    /// Return link, unlink, or conservative invalidation.
    pub fn kind(&self) -> ProjectionRelationshipEffectKind {
        self.kind
    }

    /// Return the stable relationship descriptor.
    pub fn relationship(&self) -> &ProjectionRelationship {
        &self.relationship
    }

    /// Return source endpoint key expressions.
    pub fn source_key(&self) -> &[ProjectionKeyField] {
        &self.source_key
    }

    /// Return target endpoint key expressions.
    pub fn target_key(&self) -> &[ProjectionKeyField] {
        &self.target_key
    }
}

/// One ordered logical operation template within a selected event arm.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ProjectionOperation {
    operation_id: String,
    staging_ordinal: u32,
    kind: ProjectionMutationKind,
    target: ProjectionTarget,
    key: Vec<ProjectionKeyField>,
    fields: Vec<ProjectionField>,
    relationship_effects: Vec<ProjectionRelationshipEffect>,
    invalidations: Vec<ProjectionInvalidation>,
}

impl ProjectionOperation {
    /// Construct and validate one adapter-neutral logical operation.
    ///
    /// # Errors
    ///
    /// Rejects invalid ordinals, missing keys or fields, invalid relationship
    /// provenance, and `unset` in complete-row operations.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        operation_id: impl Into<String>,
        staging_ordinal: u32,
        kind: ProjectionMutationKind,
        target: ProjectionTarget,
        mut key: Vec<ProjectionKeyField>,
        mut fields: Vec<ProjectionField>,
        mut relationship_effects: Vec<ProjectionRelationshipEffect>,
        mut invalidations: Vec<ProjectionInvalidation>,
    ) -> Result<Self, ProjectionProgramError> {
        let operation_id = non_empty(operation_id.into(), "projection operation ID")?;
        key.sort_by_key(ProjectionKeyField::ordinal);
        fields.sort_by_key(ProjectionField::ordinal);
        validate_named_ordinals(
            &key,
            "projection key field",
            ProjectionKeyField::ordinal,
            ProjectionKeyField::name,
        )?;
        validate_named_ordinals(
            &fields,
            "projection field",
            ProjectionField::ordinal,
            ProjectionField::name,
        )?;
        if key.is_empty() {
            return Err(ProjectionProgramError::InvalidOperation {
                operation: operation_id,
                reason: "a complete logical key is required".to_owned(),
            });
        }
        if kind == ProjectionMutationKind::Delete && !fields.is_empty() {
            return Err(ProjectionProgramError::InvalidOperation {
                operation: operation_id,
                reason: "delete cannot declare fields".to_owned(),
            });
        }
        if kind != ProjectionMutationKind::Delete && fields.is_empty() {
            return Err(ProjectionProgramError::InvalidOperation {
                operation: operation_id,
                reason: "a write operation requires at least one field".to_owned(),
            });
        }
        if kind.is_complete_write()
            && fields
                .iter()
                .any(|field| field.assignment == ProjectionAssignment::Unset)
        {
            return Err(ProjectionProgramError::InvalidOperation {
                operation: operation_id,
                reason: "complete-row writes cannot unset fields".to_owned(),
            });
        }
        if kind.is_complete_write() {
            for key_field in &key {
                let Some(field) = fields.iter().find(|field| field.name() == key_field.name())
                else {
                    continue;
                };
                if !matches!(
                    field.assignment(),
                    ProjectionAssignment::Set(expression)
                        if expression == key_field.expression()
                ) {
                    return Err(ProjectionProgramError::InvalidOperation {
                        operation: operation_id,
                        reason: format!(
                            "complete-row key field `{}` must use the exact key expression",
                            key_field.name()
                        ),
                    });
                }
            }
        }
        relationship_effects.sort_by_key(ProjectionRelationshipEffect::ordinal);
        validate_ordinals(
            &relationship_effects,
            "relationship effect",
            ProjectionRelationshipEffect::ordinal,
        )?;
        if kind.is_related() && relationship_effects.is_empty() {
            return Err(ProjectionProgramError::InvalidOperation {
                operation: operation_id,
                reason: "related-row writes require relationship provenance".to_owned(),
            });
        }
        invalidations.sort();
        invalidations.dedup();
        validate_relationship_invalidations(&operation_id, &relationship_effects, &invalidations)?;
        Ok(Self {
            operation_id,
            staging_ordinal,
            kind,
            target,
            key,
            fields,
            relationship_effects,
            invalidations,
        })
    }

    /// Return the stable operation identifier.
    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }

    /// Return the explicit source staging ordinal.
    pub fn staging_ordinal(&self) -> u32 {
        self.staging_ordinal
    }

    /// Return the authoritative logical mutation kind.
    pub fn kind(&self) -> ProjectionMutationKind {
        self.kind
    }

    /// Return the target read model.
    pub fn target(&self) -> &ProjectionTarget {
        &self.target
    }

    /// Return composite key fields in ordinal order.
    pub fn key(&self) -> &[ProjectionKeyField] {
        &self.key
    }

    /// Return projected fields in ordinal order.
    pub fn fields(&self) -> &[ProjectionField] {
        &self.fields
    }

    /// Return ordered relationship consequences and provenance.
    pub fn relationship_effects(&self) -> &[ProjectionRelationshipEffect] {
        &self.relationship_effects
    }

    /// Return sorted invalidation inventory associated with this mutation.
    pub fn invalidations(&self) -> &[ProjectionInvalidation] {
        &self.invalidations
    }
}

fn validate_relationship_invalidations(
    operation_id: &str,
    effects: &[ProjectionRelationshipEffect],
    invalidations: &[ProjectionInvalidation],
) -> Result<(), ProjectionProgramError> {
    let declared = invalidations
        .iter()
        .filter_map(|invalidation| match invalidation {
            ProjectionInvalidation::Relationship {
                source_model,
                relationship,
                target_model,
            } => Some((
                source_model.as_str(),
                relationship.as_str(),
                target_model.as_str(),
            )),
            ProjectionInvalidation::Model { .. } => None,
        })
        .collect::<BTreeSet<_>>();
    let proven = effects
        .iter()
        .filter(|effect| effect.kind == ProjectionRelationshipEffectKind::Invalidate)
        .map(|effect| {
            (
                effect.relationship.source_model(),
                effect.relationship.relationship(),
                effect.relationship.target_model(),
            )
        })
        .collect::<BTreeSet<_>>();
    if declared != proven {
        return Err(ProjectionProgramError::InvalidOperation {
            operation: operation_id.to_owned(),
            reason: "relationship invalidation inventory must exactly match keyed \
                     relationship invalidation effects"
                .to_owned(),
        });
    }
    Ok(())
}

/// Exact event selector and its ordered logical operations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ProjectionArm {
    arm_id: String,
    selector: ProjectionEventSelector,
    operations: Vec<ProjectionOperation>,
}

impl ProjectionArm {
    /// Construct one exact projection arm.
    ///
    /// # Errors
    ///
    /// Rejects empty IDs, too many operations, duplicate operation IDs or
    /// staging ordinals, and statically ambiguous same-scope operations.
    pub fn try_new(
        arm_id: impl Into<String>,
        selector: ProjectionEventSelector,
        mut operations: Vec<ProjectionOperation>,
    ) -> Result<Self, ProjectionProgramError> {
        let arm_id = non_empty(arm_id.into(), "projection arm ID")?;
        if operations.len() > MAX_PROJECTION_OPERATIONS_PER_OCCURRENCE {
            return Err(ProjectionProgramError::TooManyOperations {
                count: operations.len(),
                max: MAX_PROJECTION_OPERATIONS_PER_OCCURRENCE,
            });
        }
        operations.sort_by_key(ProjectionOperation::staging_ordinal);
        validate_named_ordinals(
            &operations,
            "projection operation",
            ProjectionOperation::staging_ordinal,
            ProjectionOperation::operation_id,
        )?;
        validate_static_ambiguity(&operations)?;
        Ok(Self {
            arm_id,
            selector,
            operations,
        })
    }

    /// Return the stable arm identifier.
    pub fn arm_id(&self) -> &str {
        &self.arm_id
    }

    /// Return the exact event selector.
    pub fn selector(&self) -> &ProjectionEventSelector {
        &self.selector
    }

    /// Return operations in explicit staging order.
    pub fn operations(&self) -> &[ProjectionOperation] {
        &self.operations
    }
}

/// A validated, canonical and adapter-neutral projection program.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ProjectionProgram {
    ir_version: u16,
    operation_semantics_version: u16,
    limits: ProjectionProgramLimits,
    name: String,
    version: u64,
    partition: ProjectionPartition,
    arms: Vec<ProjectionArm>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    source_snapshots: bool,
}

impl ProjectionProgram {
    /// Construct a canonical version-one projection program.
    ///
    /// Arms are sorted by their exact selector, so declaration map iteration
    /// cannot affect the canonical bytes or digest.
    ///
    /// # Errors
    ///
    /// Rejects empty names, zero versions, empty programs, duplicate selectors,
    /// and any invalid nested declaration.
    pub fn try_new(
        name: impl Into<String>,
        version: u64,
        partition: ProjectionPartition,
        mut arms: Vec<ProjectionArm>,
    ) -> Result<Self, ProjectionProgramError> {
        let name = non_empty(name.into(), "projection program name")?;
        if version == 0 {
            return Err(ProjectionProgramError::ZeroVersion(
                "projection program version",
            ));
        }
        if arms.is_empty() {
            return Err(ProjectionProgramError::InvalidOperation {
                operation: name,
                reason: "a program requires at least one event arm".to_owned(),
            });
        }
        arms.sort_by(|left, right| {
            left.selector
                .canonical_cmp(&right.selector)
                .then_with(|| left.arm_id.cmp(&right.arm_id))
        });
        for pair in arms.windows(2) {
            if pair[0].selector == pair[1].selector {
                return Err(ProjectionProgramError::DuplicateSelector);
            }
        }
        Ok(Self {
            ir_version: PROJECTION_PROGRAM_IR_VERSION,
            operation_semantics_version: PROJECTION_OPERATION_SEMANTICS_VERSION,
            limits: ProjectionProgramLimits::version_one(),
            name,
            version,
            partition,
            arms,
            source_snapshots: false,
        })
    }

    /// Fence complete row snapshots by their canonical aggregate occurrence.
    ///
    /// This is not appropriate for delta folds: dropping an older increment
    /// would lose work. Snapshot programs must use a unit partition and only
    /// full-row upserts or deletes, without relationship side effects.
    pub fn with_source_snapshots(mut self) -> Result<Self, ProjectionProgramError> {
        if !matches!(self.partition, ProjectionPartition::Unit)
            || self.arms.iter().flat_map(|arm| arm.operations()).any(|op| {
                !matches!(
                    op.kind(),
                    ProjectionMutationKind::Upsert | ProjectionMutationKind::Delete
                ) || !op.relationship_effects().is_empty()
                    || !op.invalidations().is_empty()
            })
        {
            return Err(ProjectionProgramError::InvalidOperation {
                operation: self.name.clone(),
                reason: "source snapshots require unit-partition full-row upserts/deletes without relationship effects".into(),
            });
        }
        self.source_snapshots = true;
        Ok(self)
    }

    /// Whether authoritative row snapshots are fenced by aggregate version.
    pub fn source_snapshots(&self) -> bool {
        self.source_snapshots
    }

    /// Return the stable program name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the canonical portable IR version.
    pub fn ir_version(&self) -> u16 {
        self.ir_version
    }

    /// Return the closed logical mutation semantics version.
    pub fn operation_semantics_version(&self) -> u16 {
        self.operation_semantics_version
    }

    /// Return the resource ceilings bound into this program's identity.
    pub fn limits(&self) -> ProjectionProgramLimits {
        self.limits
    }

    /// Return the independently evolving program version.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Return the logical partition expression.
    pub fn partition(&self) -> &ProjectionPartition {
        &self.partition
    }

    /// Return arms in canonical selector order.
    pub fn arms(&self) -> &[ProjectionArm] {
        &self.arms
    }

    /// Encode canonical versioned JSON for manifests and golden vectors.
    ///
    /// # Errors
    ///
    /// Returns a typed error if canonical serialization fails.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ProjectionProgramError> {
        canonical_json_bytes(self)
    }

    /// Compute the domain-separated identity of the complete program semantics.
    ///
    /// # Errors
    ///
    /// Returns a typed error if canonical serialization fails.
    pub fn id(&self) -> Result<ProjectionProgramId, ProjectionProgramError> {
        Ok(ProjectionProgramId(digest_program(
            &self.canonical_bytes()?,
        )))
    }
}

/// Domain-separated digest of a complete canonical projection program.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProjectionProgramId([u8; 32]);

impl ProjectionProgramId {
    /// Parse a canonical `pp1:sha256:<lowercase-hex>` program identity.
    ///
    /// # Errors
    ///
    /// Rejects alternate prefixes, lengths, uppercase, and non-hex text.
    pub fn parse(value: &str) -> Result<Self, ProjectionProgramError> {
        let Some(hex) = value.strip_prefix("pp1:sha256:") else {
            return Err(ProjectionProgramError::InvalidProgramId);
        };
        if hex.len() != 64
            || !hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ProjectionProgramError::InvalidProgramId);
        }
        let mut bytes = [0_u8; 32];
        for (index, pair) in hex.as_bytes().chunks_exact(2).enumerate() {
            let high = hex_nibble(pair[0]).ok_or(ProjectionProgramError::InvalidProgramId)?;
            let low = hex_nibble(pair[1]).ok_or(ProjectionProgramError::InvalidProgramId)?;
            bytes[index] = (high << 4) | low;
        }
        Ok(Self(bytes))
    }

    /// Return the raw SHA-256 digest.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for ProjectionProgramId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("pp1:sha256:")?;
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl Serialize for ProjectionProgramId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

/// Typed authoring handle for a validated projection program.
///
/// `E` supplies the exact generated event-selector set at registration sites.
/// Resolution still accepts an immutable occurrence because an occurrence
/// carries the exact selected descriptor and canonical body.
#[derive(Clone, Debug)]
pub struct ProjectionPlanTemplate<E: ProjectionEventSet> {
    program: ProjectionProgram,
    marker: PhantomData<fn() -> E>,
}

/// Typed marker implemented by generated event or tagged event-set types.
pub trait ProjectionEventSet {
    /// Return every exact selector this typed set permits.
    ///
    /// # Errors
    ///
    /// Returns a typed error when a generated descriptor is invalid.
    fn projection_event_selectors() -> Result<Vec<ProjectionEventSelector>, ProjectionProgramError>;
}

impl<E: ProjectionEventSet> ProjectionPlanTemplate<E> {
    /// Construct a typed template from a validated program.
    ///
    /// # Errors
    ///
    /// Re-runs canonical encoding and requires the marker's exact selector set.
    pub fn try_new(program: ProjectionProgram) -> Result<Self, ProjectionProgramError> {
        program.canonical_bytes()?;
        let mut expected = E::projection_event_selectors()?;
        expected.sort_by(ProjectionEventSelector::canonical_cmp);
        let actual = program
            .arms()
            .iter()
            .map(|arm| arm.selector().clone())
            .collect::<Vec<_>>();
        if expected != actual {
            return Err(ProjectionProgramError::EventSetMismatch);
        }
        Ok(Self {
            program,
            marker: PhantomData,
        })
    }

    /// Return the canonical program.
    pub fn program(&self) -> &ProjectionProgram {
        &self.program
    }

    /// Resolve one exact occurrence to its final logical mutations.
    ///
    /// # Errors
    ///
    /// Rejects selector mismatches, invalid body values, bounded-value
    /// violations, and ambiguous same-scope mutations.
    pub fn resolve(
        &self,
        occurrence: &DomainEventOccurrence,
    ) -> Result<ResolvedProjectionPlan, ProjectionProgramError> {
        ResolvedProjectionPlan::resolve(&self.program, occurrence)
    }
}

fn validate_static_ambiguity(
    operations: &[ProjectionOperation],
) -> Result<(), ProjectionProgramError> {
    for (index, left) in operations.iter().enumerate() {
        for right in &operations[index + 1..] {
            if left.target != right.target {
                continue;
            }
            if left.key == right.key {
                let compatible = left.kind == right.kind
                    && left.relationship_effects == right.relationship_effects
                    && if left.kind.is_patch() {
                        left.fields.iter().all(|left_field| {
                            right
                                .fields
                                .iter()
                                .find(|right_field| right_field.name == left_field.name)
                                .is_none_or(|right_field| {
                                    right_field.assignment == left_field.assignment
                                })
                        })
                    } else {
                        same_fields_ignoring_operation(&left.fields, &right.fields)
                    };
                if !compatible {
                    return Err(ProjectionProgramError::AmbiguousMutation {
                        model: left.target.model().to_owned(),
                        reason: format!(
                            "operations `{}` and `{}` statically target the same key \
                             with order-dependent semantics",
                            left.operation_id, right.operation_id
                        ),
                    });
                }
            } else if !keys_statically_disjoint(&left.key, &right.key) {
                return Err(ProjectionProgramError::AmbiguousMutation {
                    model: left.target.model().to_owned(),
                    reason: format!(
                        "operations `{}` and `{}` have dynamic keys whose overlap \
                         cannot be disproved at registration",
                        left.operation_id, right.operation_id
                    ),
                });
            }
        }
    }
    Ok(())
}

fn keys_statically_disjoint(left: &[ProjectionKeyField], right: &[ProjectionKeyField]) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).any(|(left, right)| {
            left.ordinal == right.ordinal
                && left.name == right.name
                && expressions_statically_distinct(&left.expression, &right.expression)
        })
}

fn same_fields_ignoring_operation(left: &[ProjectionField], right: &[ProjectionField]) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            left.ordinal == right.ordinal
                && left.name == right.name
                && left.assignment == right.assignment
        })
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}
