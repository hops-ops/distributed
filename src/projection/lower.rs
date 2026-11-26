//! Typed projection authoring metadata and authoritative ORM lowering.
//!
//! Portable declarations resolve through [`super::ProjectionPlanTemplate`].
//! This module then maps each resolved mutation's logical field names through
//! the existing [`crate::RelationalReadModel`] schema into one
//! [`LoweredRecord`]. The executor turns each record into its table mutation
//! once it knows the record's current state.

use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::{OnceLock, RwLock};

use super::ResolvedProjectionField;
use crate::read_model::{ReadModelWritePlanBuilder, RelationalReadModel};
#[cfg(any(feature = "graphql", test))]
use crate::table::PatchTableRowMutation;
use crate::table::{
    sort_writes, validate_key, validate_row_values, DeleteTableRowMutation, TableMutation,
    TableRowMutation, WritePosition,
};
use crate::{
    ColumnType, DomainEvent, DomainEventOccurrence, DomainState, ExpectedVersion, MutationKind,
    ProjectionArm, ProjectionAssignment, ProjectionEventSelector, ProjectionEventSet,
    ProjectionExpression, ProjectionField, ProjectionInvalidation, ProjectionKeyField,
    ProjectionOperation, ProjectionPartition, ProjectionPlanTemplate, ProjectionProgram,
    ProjectionProgramError, ProjectionProgramId, ProjectionRelationship,
    ProjectionRelationshipEffect, ProjectionTarget, ProjectionValue, ProjectionValueRef,
    ProjectionValueType, ResolvedProjectionKey, ResolvedProjectionMutation, ResolvedProjectionPlan,
    ResolvedProjectionValue, RowKey, RowValue, RowValues, RowWriteMode, TableSchema,
    TableStoreError,
};
#[cfg(any(feature = "graphql", test))]
use crate::{PatchMode, RowPatch};

/// Flat portable Rust field category emitted by the domain-state/event derives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionPortableType {
    /// Boolean scalar.
    Boolean,
    /// Signed integer scalar.
    I64,
    /// Unsigned integer scalar.
    U64,
    /// Floating-point scalar.
    F64,
    /// UTF-8 string.
    String,
    /// Byte sequence.
    Bytes,
    /// Structured JSON value.
    Json,
    /// A user type whose exact Rust identity must match.
    Custom,
}

/// Hidden compile-time field metadata for a domain-event body.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProjectionBodyFieldMetadata {
    /// Rust source-field name.
    pub rust_name: &'static str,
    /// Serialized body-field name.
    pub wire_name: &'static str,
    /// Canonical inner Rust type identity (without `Option`).
    pub rust_type: &'static str,
    /// Flat portable category.
    pub portable_type: ProjectionPortableType,
    /// Whether the serialized value may be null.
    pub nullable: bool,
    /// Whether this field is present in the serialized body.
    pub present: bool,
    /// Whether serialization always includes the field rather than applying a
    /// conditional omission rule.
    pub always_present: bool,
}

/// Hidden compile-time portable-body metadata emitted by `DomainState` and
/// `DomainEvent`.
#[doc(hidden)]
pub trait ProjectionBodyMetadata {
    /// Canonical flat body fields in declaration order.
    const PROJECTION_FIELDS: &'static [ProjectionBodyFieldMetadata];
    /// Independent fingerprint of the portable field inventory.
    const PROJECTION_SCHEMA_FINGERPRINT: &'static str;
}

/// Hidden compile-time field metadata for a relational read model.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProjectionModelFieldMetadata {
    /// Rust model-field name used by projection declarations.
    pub field_name: &'static str,
    /// Physical column name owned by the existing ORM.
    pub column_name: &'static str,
    /// Canonical inner Rust type identity (without `Option`).
    pub rust_type: &'static str,
    /// Flat portable category.
    pub portable_type: ProjectionPortableType,
    /// Whether the mapped field may be null.
    pub nullable: bool,
    /// Whether a complete write requires an explicit value.
    pub required: bool,
    /// Whether this field is part of the complete primary key.
    pub primary_key: bool,
}

/// Hidden compile-time portable read-model metadata emitted by `ReadModel`.
#[doc(hidden)]
pub trait ProjectionReadModelMetadata: RelationalReadModel {
    /// Canonical mapped fields in declaration order.
    const PROJECTION_FIELDS: &'static [ProjectionModelFieldMetadata];
    /// Independent fingerprint of fields, keys, storage and relationships.
    const PROJECTION_SCHEMA_FINGERPRINT: &'static str;
}

/// Hidden relationship marker implemented on the marker types emitted by
/// `ReadModel`.
#[doc(hidden)]
pub trait ProjectionRelationshipMetadata {
    /// Relationship source read model.
    type Source: ProjectionReadModelMetadata;
    /// Relationship target read model.
    type Target: ProjectionReadModelMetadata;
    /// Stable relationship field.
    const FIELD: &'static str;
}

/// Hidden exact deletion-body descriptor emitted for a sourced aggregate
/// identity used by `domain = deleted`.
#[doc(hidden)]
pub trait ProjectionDeletionMetadata {
    /// Stable `DomainDeletion<Identity>` body type name.
    const BODY_TYPE_NAME: &'static str;
    /// Independently versioned canonical deletion body schema.
    const BODY_SCHEMA: &'static str;
    /// Canonical SHA-256 body-schema fingerprint.
    const BODY_FINGERPRINT: &'static str;
}

/// Compile-time proof that the common state-upsert shorthand has exact flat
/// coverage.
///
/// Renamed, missing, extra, omitted, nested, type-incompatible, or
/// nullability-incompatible fields intentionally fail constant evaluation.
#[doc(hidden)]
pub const fn assert_state_upsert_compatible(
    state: &[ProjectionBodyFieldMetadata],
    model: &[ProjectionModelFieldMetadata],
) {
    let mut state_index = 0;
    while state_index < state.len() {
        if !state[state_index].present || !state[state_index].always_present {
            panic!("projection state-upsert rejects omitted state fields");
        }
        state_index += 1;
    }
    if state.len() != model.len() {
        panic!("projection state-upsert requires exact field coverage");
    }

    let mut model_index = 0;
    while model_index < model.len() {
        let model_field = &model[model_index];
        let mut found = false;
        let mut candidate_index = 0;
        while candidate_index < state.len() {
            let candidate = &state[candidate_index];
            if candidate.present && const_str_eq(candidate.wire_name, model_field.field_name) {
                if !const_str_eq(candidate.rust_name, candidate.wire_name) {
                    panic!("projection state-upsert rejects renamed state fields");
                }
                if !const_str_eq(candidate.rust_type, model_field.rust_type)
                    || !portable_type_eq(candidate.portable_type, model_field.portable_type)
                {
                    panic!("projection state-upsert field type mismatch");
                }
                if candidate.nullable != model_field.nullable {
                    panic!("projection state-upsert field nullability mismatch");
                }
                if matches!(
                    candidate.portable_type,
                    ProjectionPortableType::Json | ProjectionPortableType::Bytes
                ) {
                    panic!(
                        "projection state-upsert nested/collection fields require an explicit block"
                    );
                }
                found = true;
                break;
            }
            candidate_index += 1;
        }
        if !found {
            panic!("projection state-upsert field is missing or renamed");
        }
        model_index += 1;
    }
}

/// Compile-time proof for one explicit direct body-field mapping.
#[doc(hidden)]
pub const fn assert_explicit_field_compatible(
    body: &[ProjectionBodyFieldMetadata],
    source: &str,
    model: &[ProjectionModelFieldMetadata],
    target: &str,
) {
    let mut source_index = 0;
    while source_index < body.len() && !const_str_eq(body[source_index].rust_name, source) {
        source_index += 1;
    }
    if source_index == body.len() || !body[source_index].present {
        panic!("projection body field does not exist");
    }
    let mut target_index = 0;
    while target_index < model.len() && !const_str_eq(model[target_index].field_name, target) {
        target_index += 1;
    }
    if target_index == model.len() {
        panic!("projection read-model field does not exist");
    }
    let source_field = &body[source_index];
    let target_field = &model[target_index];
    if !const_str_eq(source_field.rust_type, target_field.rust_type)
        || !portable_type_eq(source_field.portable_type, target_field.portable_type)
    {
        panic!("projection explicit field type mismatch");
    }
    if source_field.nullable && !target_field.nullable {
        panic!("projection nullable body field cannot target a non-null read-model field");
    }
}

const fn portable_type_eq(left: ProjectionPortableType, right: ProjectionPortableType) -> bool {
    left as u8 == right as u8
}

const fn const_str_eq(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    if left.len() != right.len() {
        return false;
    }
    let mut index = 0;
    while index < left.len() {
        if left[index] != right[index] {
            return false;
        }
        index += 1;
    }
    true
}

/// Generated candidate for direct placement.
///
/// The macro emits this marker only for the exact full-state upsert shorthand.
/// It is a syntactic capability, not the final direct-placement proof:
/// `.direct()` revalidates the materialized program, schemas, and placement
/// constraints before a catalog binding can be constructed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DirectCandidate;

/// Marker for a program that must use eventual execution until a stronger
/// direct evidence protocol exists.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EventualOnly;

/// One model in a projection's generated output inventory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionOutputModel {
    /// Logical model name.
    pub model: String,
    /// Physical storage identity.
    pub storage: String,
    /// Fingerprint emitted by the model derive.
    pub schema_fingerprint: String,
    /// Exact validated schema used by the authoritative ORM lowerer.
    pub schema: TableSchema,
}

impl ProjectionOutputModel {
    /// Construct an inventory entry from the authoritative ORM model metadata.
    ///
    /// # Errors
    ///
    /// Returns a schema validation error before any storage operation.
    pub fn of<M>() -> Result<Self, TableStoreError>
    where
        M: ProjectionReadModelMetadata,
    {
        let schema = ReadModelWritePlanBuilder::projection_validated_schema::<M>()?;
        Ok(Self {
            model: schema.model_name.clone(),
            storage: schema.table_name.clone(),
            schema_fingerprint: M::PROJECTION_SCHEMA_FINGERPRINT.to_owned(),
            schema: schema.clone(),
        })
    }
}

/// Canonical model and relationship inventory generated by one declaration.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProjectionOutputInventory {
    /// Sorted, duplicate-free output models.
    pub models: Vec<ProjectionOutputModel>,
    /// Sorted, duplicate-free logical relationships.
    pub relationships: Vec<ProjectionRelationship>,
}

impl ProjectionOutputInventory {
    /// Canonicalize a generated inventory.
    pub fn new(
        mut models: Vec<ProjectionOutputModel>,
        mut relationships: Vec<ProjectionRelationship>,
    ) -> Self {
        models.sort_by(|left, right| {
            (&left.model, &left.storage, &left.schema_fingerprint).cmp(&(
                &right.model,
                &right.storage,
                &right.schema_fingerprint,
            ))
        });
        models.dedup();
        relationships.sort();
        relationships.dedup();
        Self {
            models,
            relationships,
        }
    }
}

/// Returns a descriptor's compiled program, building it on first use.
#[doc(hidden)]
pub type ProjectionProgramFactory =
    fn() -> Result<&'static ProjectionProgram, ProjectionProgramError>;

/// Holds one descriptor's compiled program for the life of the process.
///
/// `projection!` declares one of these per descriptor, so a program is built
/// and identified once and every resolve reads it by reference. A failed build
/// is kept too: a declaration that is invalid stays invalid.
pub struct ProjectionProgramCell(OnceLock<Result<ProjectionProgram, ProjectionProgramError>>);

impl ProjectionProgramCell {
    /// An empty cell.
    pub const fn new() -> Self {
        Self(OnceLock::new())
    }

    /// Return the program, calling `build` only if no call has finished yet.
    ///
    /// # Errors
    ///
    /// Returns the error of the one build that ran.
    pub fn get_or_build(
        &'static self,
        build: impl FnOnce() -> Result<ProjectionProgram, ProjectionProgramError>,
    ) -> Result<&'static ProjectionProgram, ProjectionProgramError> {
        self.0.get_or_init(build).as_ref().map_err(Clone::clone)
    }
}

impl Default for ProjectionProgramCell {
    fn default() -> Self {
        Self::new()
    }
}

/// Generated authoritative ORM lowerer function.
#[doc(hidden)]
pub type ProjectionLowerer =
    fn(&ResolvedProjectionPlan) -> Result<LoweredProjectionPlan, ProjectionLoweringError>;
/// Generated output inventory factory function.
#[doc(hidden)]
pub type ProjectionInventoryFactory =
    fn() -> Result<ProjectionOutputInventory, ProjectionLoweringError>;

/// Const descriptor generated by `projection!`.
///
/// `D` is a compiler-generated placement-candidate classification. Every
/// projection may be mounted eventually; only [`DirectCandidate`] descriptors
/// may attempt the direct-placement extension, which revalidates the generated
/// candidate before constructing a binding.
#[derive(Clone, Copy)]
pub struct ProjectionDescriptor<D = EventualOnly> {
    name: &'static str,
    version: u64,
    epoch: &'static str,
    program: ProjectionProgramFactory,
    lower: ProjectionLowerer,
    inventory: ProjectionInventoryFactory,
    marker: PhantomData<fn() -> D>,
}

impl<D> fmt::Debug for ProjectionDescriptor<D> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProjectionDescriptor")
            .field("name", &self.name)
            .field("version", &self.version)
            .field("epoch", &self.epoch)
            .finish_non_exhaustive()
    }
}

impl<D> ProjectionDescriptor<D> {
    /// Construct a generated descriptor from closed function items.
    #[doc(hidden)]
    pub const fn new(
        name: &'static str,
        version: u64,
        epoch: &'static str,
        program: ProjectionProgramFactory,
        lower: ProjectionLowerer,
        inventory: ProjectionInventoryFactory,
    ) -> Self {
        Self {
            name,
            version,
            epoch,
            program,
            lower,
            inventory,
            marker: PhantomData,
        }
    }

    /// Return the stable projection name.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Return the independently evolving declaration version.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Return the rebuild/ownership incarnation.
    ///
    /// Epoch is binding metadata and is intentionally excluded from
    /// [`ProjectionProgramId`].
    pub fn epoch(&self) -> &'static str {
        self.epoch
    }

    /// Return the compiled program. It is built on the first call and shared
    /// by every later one.
    ///
    /// # Errors
    ///
    /// Returns declaration or ORM metadata validation failures.
    pub fn program(&self) -> Result<&'static ProjectionProgram, ProjectionProgramError> {
        (self.program)()
    }

    /// Return the canonical program digest.
    ///
    /// # Errors
    ///
    /// Returns declaration failures.
    pub fn program_id(&self) -> Result<ProjectionProgramId, ProjectionProgramError> {
        Ok(self.program()?.id())
    }

    /// Resolve one exact domain-event occurrence.
    ///
    /// # Errors
    ///
    /// Returns exact-selector, expression, or logical-plan failures.
    pub fn resolve(
        &self,
        occurrence: &DomainEventOccurrence,
    ) -> Result<ResolvedProjectionPlan, ProjectionProgramError> {
        ResolvedProjectionPlan::resolve(self.program()?, occurrence)
    }

    /// Lower a resolved logical plan through the existing relational ORM.
    ///
    /// # Errors
    ///
    /// Returns unknown generated operations, metadata, completeness, key,
    /// relationship, or adapter-capability failures before I/O.
    pub fn lower(
        &self,
        plan: &ResolvedProjectionPlan,
    ) -> Result<LoweredProjectionPlan, ProjectionLoweringError> {
        (self.lower)(plan)
    }

    /// Return canonical generated output inventory.
    ///
    /// # Errors
    ///
    /// Returns invalid ORM metadata.
    pub fn output_inventory(&self) -> Result<ProjectionOutputInventory, ProjectionLoweringError> {
        (self.inventory)()
    }

    /// Return the generated server executor descriptor.
    ///
    /// # Errors
    ///
    /// Returns program or inventory construction failures.
    pub fn server_executor(
        &self,
    ) -> Result<ProjectionServerExecutorDescriptor, ProjectionLoweringError> {
        let program = self.program()?;
        Ok(ProjectionServerExecutorDescriptor {
            name: self.name,
            version: self.version,
            epoch: self.epoch,
            program_id: program.id(),
            outputs: self.output_inventory()?,
            selectors: program
                .arms()
                .iter()
                .map(|arm| arm.selector().clone())
                .collect(),
            unit_partition: matches!(program.partition(), ProjectionPartition::Unit),
            program,
            lower: self.lower,
        })
    }
}

/// Store-free generated server executor metadata.
#[derive(Clone)]
pub struct ProjectionServerExecutorDescriptor {
    /// Stable program name.
    pub name: &'static str,
    /// Declaration version.
    pub version: u64,
    /// Rebuild/ownership epoch.
    pub epoch: &'static str,
    /// Canonical portable program identity.
    pub program_id: ProjectionProgramId,
    /// Canonical output inventory.
    pub outputs: ProjectionOutputInventory,
    selectors: Vec<ProjectionEventSelector>,
    #[cfg_attr(not(feature = "graphql"), allow(dead_code))]
    unit_partition: bool,
    program: &'static ProjectionProgram,
    lower: ProjectionLowerer,
}

impl ProjectionServerExecutorDescriptor {
    pub(crate) fn matches(&self, occurrence: &DomainEventOccurrence) -> bool {
        self.selectors
            .iter()
            .any(|selector| selector.matches(occurrence))
    }

    pub(crate) fn source_snapshots(&self) -> bool {
        self.program.source_snapshots()
    }

    #[cfg(feature = "graphql")]
    pub(crate) fn has_unit_partition(&self) -> bool {
        self.unit_partition
    }

    /// Resolve and physically lower an exact occurrence without performing I/O.
    ///
    /// # Errors
    ///
    /// Returns portable resolution or authoritative ORM validation failures.
    pub fn plan(
        &self,
        occurrence: &DomainEventOccurrence,
    ) -> Result<LoweredProjectionPlan, ProjectionLoweringError> {
        let resolved = ResolvedProjectionPlan::resolve(self.program, occurrence)?;
        (self.lower)(&resolved)
    }
}

/// Process-wide registry of placement-selected direct projection executors.
///
/// Service construction registers each direct modeled owner by output model
/// name. Command handlers then call `commit()?.atomic()` without naming a
/// projection selector; the runtime looks up the executor by returned model.
static PLACEMENT_SELECTED_DIRECT: OnceLock<
    RwLock<BTreeMap<String, ProjectionServerExecutorDescriptor>>,
> = OnceLock::new();

fn placement_selected_direct_registry(
) -> &'static RwLock<BTreeMap<String, ProjectionServerExecutorDescriptor>> {
    PLACEMENT_SELECTED_DIRECT.get_or_init(|| RwLock::new(BTreeMap::new()))
}

/// Register the placement-selected direct executor for one output model.
///
/// Called from service/projection registration. Replacing an existing model
/// entry is allowed so tests can rebuild services.
pub fn register_placement_selected_direct(
    model: impl Into<String>,
    executor: ProjectionServerExecutorDescriptor,
) {
    let model = model.into();
    if let Ok(mut guard) = placement_selected_direct_registry().write() {
        guard.insert(model, executor);
    }
}

/// Look up the placement-selected direct executor for a returned model.
pub fn placement_selected_direct_for_model(
    model: &str,
) -> Option<ProjectionServerExecutorDescriptor> {
    placement_selected_direct_registry()
        .read()
        .ok()
        .and_then(|guard| guard.get(model).cloned())
}

/// Register every single-model output of a direct descriptor for placement
/// selection. Multi-model descriptors are not eligible for `Atomic<M>`.
///
/// # Errors
///
/// Returns executor construction failures.
pub fn register_placement_selected_direct_descriptor(
    descriptor: &ProjectionDescriptor<DirectCandidate>,
) -> Result<(), ProjectionLoweringError> {
    let executor = descriptor.server_executor()?;
    if executor.outputs.models.len() == 1 {
        let model = executor.outputs.models[0].model.clone();
        register_placement_selected_direct(model, executor);
    }
    Ok(())
}

/// The resolved logical plan plus one [`LoweredRecord`] per resolved mutation.
///
/// Records are held in canonical apply order (rows, then patches, then
/// deletes; parents before children). Each record carries what lowering
/// knows; the record's lifecycle (insert or upsert, patch or insert-missing)
/// depends on its current state and is chosen once, by the executor.
#[derive(Clone, Debug, PartialEq)]
pub struct LoweredProjectionPlan {
    pub(crate) resolved: ResolvedProjectionPlan,
    /// `(resolved mutation index, record)` in apply order; every index occurs
    /// exactly once.
    pub(crate) records: Vec<(usize, LoweredRecord)>,
}

impl LoweredProjectionPlan {
    /// The resolved logical plan, retaining occurrence and relationship
    /// provenance.
    pub fn resolved(&self) -> &ResolvedProjectionPlan {
        &self.resolved
    }

    /// Each resolved mutation with its lowered record, in apply order.
    pub fn records(&self) -> impl Iterator<Item = (&ResolvedProjectionMutation, &LoweredRecord)> {
        self.records
            .iter()
            .map(|(index, record)| (&self.resolved.mutations()[*index], record))
    }
}

/// One resolved mutation mapped onto its read-model table.
///
/// `values` are complete for row kinds, the sparse patch for patch kinds and
/// empty for deletes. Turning a record into a [`TableMutation`] needs the
/// record's current state, so only the executor (and rebuild and the direct
/// path, which fix that state by construction) does it.
#[derive(Clone, Debug, PartialEq)]
pub struct LoweredRecord {
    pub(crate) schema: &'static TableSchema,
    pub(crate) key: RowKey,
    pub(crate) values: RowValues,
}

impl LoweredRecord {
    /// Target table schema.
    pub fn schema(&self) -> &'static TableSchema {
        self.schema
    }

    /// Primary key columns.
    pub fn key(&self) -> &RowKey {
        &self.key
    }

    /// Column values: the full row, the patch, or nothing for a delete.
    pub fn values(&self) -> &RowValues {
        &self.values
    }

    /// A full row that must not exist yet.
    #[cfg(any(feature = "graphql", test))]
    pub(crate) fn insert(self) -> TableMutation {
        self.full_row(RowWriteMode::Insert, ExpectedVersion::NotExists)
    }

    /// A full row that replaces whatever is there.
    pub(crate) fn upsert(self) -> TableMutation {
        self.full_row(RowWriteMode::Upsert, ExpectedVersion::Any)
    }

    /// The patch applied to an existing row.
    #[cfg(any(feature = "graphql", test))]
    pub(crate) fn patch(self) -> TableMutation {
        let patch = self
            .values
            .into_iter()
            .fold(RowPatch::new(), |patch, (column, value)| {
                patch.set(column, value)
            });
        TableMutation::PatchRow(PatchTableRowMutation {
            schema: self.schema,
            key: self.key,
            patch,
            expected_version: ExpectedVersion::Any,
            mode: PatchMode::UpdateExisting,
        })
    }

    /// The patch inserted as a new row, completed by its primary key.
    #[cfg(any(feature = "graphql", test))]
    pub(crate) fn insert_patched(self) -> Result<TableMutation, TableStoreError> {
        let mut values = RowValues::new();
        for (column, value) in self.key.iter() {
            values.insert(column, value.clone());
        }
        for (column, value) in self.values {
            if self.key.get(&column).is_some_and(|key| *key != value) {
                return Err(TableStoreError::Metadata(format!(
                    "model `{}` patch changes primary-key column `{column}`",
                    self.schema.model_name
                )));
            }
            values.insert(column, value);
        }
        Ok(Self { values, ..self }.insert())
    }

    /// A delete of whatever row has this key.
    pub(crate) fn delete(self) -> TableMutation {
        TableMutation::DeleteRow(DeleteTableRowMutation {
            schema: self.schema,
            key: self.key,
            expected_version: ExpectedVersion::Any,
        })
    }

    fn full_row(self, mode: RowWriteMode, expected_version: ExpectedVersion) -> TableMutation {
        TableMutation::UpsertRow(TableRowMutation {
            schema: self.schema,
            key: self.key,
            values: self.values,
            expected_version,
            mode,
        })
    }

    fn write_position(&self, kind: MutationKind, sequence: u64) -> WritePosition {
        match kind {
            MutationKind::Patch | MutationKind::UpsertPatch => {
                WritePosition::patch(self.schema, &self.key, sequence)
            }
            MutationKind::Delete => WritePosition::delete(self.schema, &self.key, sequence),
            MutationKind::Insert
            | MutationKind::Upsert
            | MutationKind::Recreate
            | MutationKind::InsertRelated
            | MutationKind::UpsertRelated => {
                WritePosition::full_row(self.schema, &self.key, sequence)
            }
        }
    }
}

/// Typed physical lowering failure.
#[derive(Debug)]
#[non_exhaustive]
pub enum ProjectionLoweringError {
    /// Portable program construction or resolution failed.
    Program(ProjectionProgramError),
    /// Existing ORM metadata or write-plan validation failed.
    Table(TableStoreError),
    /// Generated lowering did not recognize a resolved arm/operation.
    UnknownOperation {
        /// Selected arm.
        arm: String,
        /// Contributing operation IDs.
        operations: Vec<String>,
    },
    /// A related operation could not find its parent row mutation.
    MissingRelatedParent {
        /// Selected arm.
        arm: String,
        /// Parent operation ID.
        operation: String,
    },
}

impl fmt::Display for ProjectionLoweringError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Program(error) => error.fmt(formatter),
            Self::Table(error) => error.fmt(formatter),
            Self::UnknownOperation { arm, operations } => write!(
                formatter,
                "projection arm `{arm}` has no generated lowerer for operations {operations:?}"
            ),
            Self::MissingRelatedParent { arm, operation } => write!(
                formatter,
                "projection arm `{arm}` is missing related parent operation `{operation}`"
            ),
        }
    }
}

impl std::error::Error for ProjectionLoweringError {}

impl From<ProjectionProgramError> for ProjectionLoweringError {
    fn from(error: ProjectionProgramError) -> Self {
        Self::Program(error)
    }
}

impl From<TableStoreError> for ProjectionLoweringError {
    fn from(error: TableStoreError) -> Self {
        Self::Table(error)
    }
}

/// Hidden explicit field/expression pair generated by `projection!`.
#[doc(hidden)]
#[derive(Clone)]
pub struct ProjectionAuthoringField {
    field: String,
    assignment: ProjectionAssignment,
}

impl ProjectionAuthoringField {
    /// Construct a generated field assignment.
    pub fn set(field: impl Into<String>, expression: ProjectionExpression) -> Self {
        Self {
            field: field.into(),
            assignment: ProjectionAssignment::Set(expression),
        }
    }

    /// Construct a generated explicit-unset assignment.
    pub fn unset(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            assignment: ProjectionAssignment::Unset,
        }
    }
}

/// Build a body path using derived flat portable field metadata.
///
/// # Errors
///
/// Rejects unknown or omitted fields.
#[doc(hidden)]
pub fn body_path<B>(field: &'static str) -> Result<ProjectionExpression, ProjectionProgramError>
where
    B: ProjectionBodyMetadata,
{
    let metadata = B::PROJECTION_FIELDS
        .iter()
        .find(|candidate| candidate.rust_name == field && candidate.present)
        .ok_or_else(|| ProjectionProgramError::RequiredValueAbsent {
            path: format!("body metadata field `{field}`"),
        })?;
    ProjectionExpression::body_path(
        projection_value_type(metadata.portable_type),
        [metadata.wire_name],
    )
}

/// Build an exact state-body event selector.
///
/// # Errors
///
/// Returns descriptor validation failures.
#[doc(hidden)]
pub fn state_selector<S>(
    name: &'static str,
    version: u64,
) -> Result<ProjectionEventSelector, ProjectionProgramError>
where
    S: DomainState,
{
    ProjectionEventSelector::try_from_descriptor(&crate::DomainEventDescriptor::state::<S>(
        name, version,
    ))
}

/// Build an exact typed domain-event selector.
///
/// # Errors
///
/// Returns descriptor validation failures.
#[doc(hidden)]
pub fn event_selector<E>() -> Result<ProjectionEventSelector, ProjectionProgramError>
where
    E: DomainEvent,
{
    ProjectionEventSelector::try_from_descriptor(&E::DESCRIPTOR)
}

/// Build an exact sourced deletion selector without a duplicate event DTO.
///
/// # Errors
///
/// Returns descriptor validation failures.
#[doc(hidden)]
pub fn deletion_selector<I>(
    name: &'static str,
    version: u64,
) -> Result<ProjectionEventSelector, ProjectionProgramError>
where
    I: ProjectionDeletionMetadata,
{
    ProjectionEventSelector::try_from_descriptor(&crate::DomainEventDescriptor {
        name: std::borrow::Cow::Borrowed(name),
        version,
        body: crate::DomainEventBodyDescriptor::distributed_json(
            crate::DomainEventBodyKind::Deletion,
            I::BODY_TYPE_NAME,
            1,
            I::BODY_SCHEMA,
            I::BODY_FINGERPRINT,
        ),
    })
}

/// Construct a model target from the existing ORM schema.
///
/// # Errors
///
/// Returns invalid schema metadata.
#[doc(hidden)]
pub fn projection_target<M>() -> Result<ProjectionTarget, ProjectionProgramError>
where
    M: ProjectionReadModelMetadata,
{
    let schema = projection_schema::<M>()?;
    ProjectionTarget::try_new(schema.model_name.clone(), schema.table_name.clone())
}

/// Construct the exact common full-state upsert operation.
///
/// # Errors
///
/// Returns invalid body/model metadata or task-7 operation validation failures.
#[doc(hidden)]
pub fn state_upsert_operation<S, M>(
    operation_id: &'static str,
    staging_ordinal: u32,
) -> Result<ProjectionOperation, ProjectionProgramError>
where
    S: DomainState + ProjectionBodyMetadata,
    M: ProjectionReadModelMetadata,
{
    let schema = projection_schema::<M>()?;
    let mut key = Vec::with_capacity(schema.primary_key.columns.len());
    for (ordinal, column) in schema.primary_key.columns.iter().enumerate() {
        let model_field = model_field_for_column::<M>(column)?;
        key.push(ProjectionKeyField::try_new(
            ordinal as u32,
            model_field.field_name,
            body_path_for_model_field::<S>(model_field)?,
        )?);
    }
    let mut fields = Vec::with_capacity(M::PROJECTION_FIELDS.len());
    for (ordinal, model_field) in M::PROJECTION_FIELDS.iter().enumerate() {
        fields.push(ProjectionField::try_new(
            ordinal as u32,
            model_field.field_name,
            ProjectionAssignment::Set(body_path_for_model_field::<S>(model_field)?),
        )?);
    }
    ProjectionOperation::try_new(
        operation_id,
        staging_ordinal,
        MutationKind::Upsert,
        projection_target::<M>()?,
        key,
        fields,
        Vec::new(),
        Vec::new(),
    )
}

/// Construct one explicit model operation from closed expressions.
///
/// # Errors
///
/// Returns unknown fields, incomplete keys/rows, unsupported mutations, or
/// task-7 validation failures.
#[doc(hidden)]
pub fn model_operation<M>(
    operation_id: &'static str,
    staging_ordinal: u32,
    kind: MutationKind,
    key: Vec<ProjectionAuthoringField>,
    fields: Vec<ProjectionAuthoringField>,
) -> Result<ProjectionOperation, ProjectionProgramError>
where
    M: ProjectionReadModelMetadata,
{
    build_model_operation::<M>(operation_id, staging_ordinal, kind, key, fields, Vec::new())
}

/// Construct a relationship-aware child operation using the existing ORM
/// relationship metadata and delegated foreign-key mapping.
///
/// # Errors
///
/// Returns invalid relationship metadata, caller attempts to author delegated
/// keys directly, incomplete keys/rows, or task-7 validation failures.
#[doc(hidden)]
pub fn related_operation<R>(
    operation_id: &'static str,
    staging_ordinal: u32,
    kind: MutationKind,
    parent: &ProjectionOperation,
    key: Vec<ProjectionAuthoringField>,
    fields: Vec<ProjectionAuthoringField>,
) -> Result<ProjectionOperation, ProjectionProgramError>
where
    R: ProjectionRelationshipMetadata,
{
    let parent_schema = projection_schema::<R::Source>()?;
    let child_schema = projection_schema::<R::Target>()?;
    let relationship = ReadModelWritePlanBuilder::projection_relationship_for(
        parent_schema,
        R::FIELD,
        child_schema,
    )
    .map_err(program_metadata_error)?;
    let delegations = ReadModelWritePlanBuilder::projection_delegated_relationship_columns(
        parent_schema,
        relationship,
        child_schema,
    )
    .map_err(program_metadata_error)?;
    let mut key = key;
    let mut fields = fields;
    for (child_column, parent_column) in delegations {
        let child = model_field_for_column::<R::Target>(&child_column)?;
        if key.iter().any(|field| field.field == child.field_name)
            || fields.iter().any(|field| field.field == child.field_name)
        {
            return Err(ProjectionProgramError::InvalidOperation {
                operation: operation_id.to_owned(),
                reason: format!(
                    "delegated relationship field `{}` is owned by ORM metadata",
                    child.field_name
                ),
            });
        }
        let parent_expression =
            operation_expression_for_column(parent, parent_schema, &parent_column)?;
        if child.primary_key {
            key.push(ProjectionAuthoringField::set(
                child.field_name,
                parent_expression.clone(),
            ));
        } else if kind != MutationKind::Delete {
            fields.push(ProjectionAuthoringField::set(
                child.field_name,
                parent_expression,
            ));
        }
    }
    let effect_kind = match kind {
        MutationKind::InsertRelated | MutationKind::UpsertRelated => {
            Some(crate::ProjectionRelationshipEffectKind::Link)
        }
        MutationKind::Delete => Some(crate::ProjectionRelationshipEffectKind::Unlink),
        _ => None,
    }
    .ok_or_else(|| ProjectionProgramError::InvalidOperation {
        operation: operation_id.to_owned(),
        reason: "relationship authoring supports insert_related, upsert_related, or delete"
            .to_owned(),
    })?;

    let row_kind = match kind {
        MutationKind::InsertRelated => MutationKind::Insert,
        MutationKind::UpsertRelated => MutationKind::Upsert,
        other => other,
    };
    let built = build_model_operation::<R::Target>(
        operation_id,
        staging_ordinal,
        row_kind,
        key,
        fields,
        Vec::new(),
    )?;
    let relationship = ProjectionRelationship::try_new(
        parent_schema.model_name.clone(),
        R::FIELD,
        child_schema.model_name.clone(),
    )?;
    let effect = match effect_kind {
        crate::ProjectionRelationshipEffectKind::Link => ProjectionRelationshipEffect::link(
            0,
            relationship,
            parent.key().to_vec(),
            built.key().to_vec(),
        )?,
        crate::ProjectionRelationshipEffectKind::Unlink => ProjectionRelationshipEffect::unlink(
            0,
            relationship,
            parent.key().to_vec(),
            built.key().to_vec(),
        )?,
        crate::ProjectionRelationshipEffectKind::Invalidate => unreachable!(),
    };
    ProjectionOperation::try_new(
        built.operation_id().to_owned(),
        built.staging_ordinal(),
        kind,
        built.target().clone(),
        built.key().to_vec(),
        built.fields().to_vec(),
        vec![effect],
        Vec::new(),
    )
}

/// Lower one ordinary generated operation through its concrete read-model
/// schema.
///
/// # Errors
///
/// Returns target, key, row, patch, nullability or completeness failures.
#[doc(hidden)]
pub fn lower_model_mutation<M>(
    mutation: &ResolvedProjectionMutation,
) -> Result<LoweredRecord, ProjectionLoweringError>
where
    M: ProjectionReadModelMetadata,
{
    validate_target::<M>(mutation)?;
    let schema = ReadModelWritePlanBuilder::projection_validated_schema::<M>()?;
    let key = resolved_key::<M>(mutation.key())?;
    validate_key(schema, &key)?;
    let values = match mutation.kind() {
        MutationKind::Insert | MutationKind::Upsert | MutationKind::Recreate => {
            resolved_full_row::<M>(mutation.fields())?
        }
        MutationKind::Patch => resolved_patch::<M>(mutation.fields())?,
        MutationKind::UpsertPatch => {
            let patch = resolved_patch::<M>(mutation.fields())?;
            validate_insert_missing_patch::<M>(&key, &patch)?;
            patch
        }
        MutationKind::Delete => RowValues::new(),
        MutationKind::InsertRelated | MutationKind::UpsertRelated => {
            return Err(ProjectionLoweringError::Table(TableStoreError::Metadata(
                format!(
                    "related mutation for `{}` requires generated relationship lowering",
                    mutation.target().model()
                ),
            )));
        }
    };
    Ok(LoweredRecord {
        schema,
        key,
        values,
    })
}

/// Lower one generated related child mutation through the existing
/// relationship mapper.
///
/// # Errors
///
/// Returns missing parent provenance, relationship, delegated-key, or row
/// validation failures.
#[doc(hidden)]
pub fn lower_related_mutation<R>(
    plan: &ResolvedProjectionPlan,
    mutation: &ResolvedProjectionMutation,
    parent_operation_id: &'static str,
) -> Result<LoweredRecord, ProjectionLoweringError>
where
    R: ProjectionRelationshipMetadata,
{
    validate_target::<R::Target>(mutation)?;
    let schema = ReadModelWritePlanBuilder::projection_validated_schema::<R::Target>()?;
    let parent = plan
        .mutations()
        .iter()
        .find(|candidate| {
            candidate.provenance().arm_id() == mutation.provenance().arm_id()
                && candidate
                    .provenance()
                    .operation_ids()
                    .iter()
                    .any(|operation| operation == parent_operation_id)
        })
        .ok_or_else(|| ProjectionLoweringError::MissingRelatedParent {
            arm: mutation.provenance().arm_id().to_owned(),
            operation: parent_operation_id.to_owned(),
        })?;
    validate_target::<R::Source>(parent)?;
    let parent_row = resolved_full_row::<R::Source>(parent.fields())?;
    match mutation.kind() {
        MutationKind::InsertRelated | MutationKind::UpsertRelated => {
            let mut values = resolved_full_row::<R::Target>(mutation.fields())?;
            let key = ReadModelWritePlanBuilder::projection_related_row::<R::Source, R::Target>(
                R::FIELD,
                &parent_row,
                &mut values,
            )?;
            validate_key(schema, &key)?;
            Ok(LoweredRecord {
                schema,
                key,
                values,
            })
        }
        MutationKind::Delete => {
            let key = resolved_key::<R::Target>(mutation.key())?;
            validate_key(schema, &key)?;
            Ok(LoweredRecord {
                schema,
                key,
                values: RowValues::new(),
            })
        }
        kind => Err(ProjectionLoweringError::Table(TableStoreError::Metadata(
            format!("unsupported related projection mutation `{kind:?}`"),
        ))),
    }
}

/// Pair each resolved mutation with its lowered record and put the records
/// in canonical apply order.
///
/// `records` must hold exactly one record per resolved mutation, in the
/// resolved plan's order.
///
/// # Errors
///
/// Returns a metadata failure when the counts differ.
#[doc(hidden)]
pub fn finish_lowering(
    resolved: &ResolvedProjectionPlan,
    records: Vec<LoweredRecord>,
) -> Result<LoweredProjectionPlan, ProjectionLoweringError> {
    let mutations = resolved.mutations();
    if records.len() != mutations.len() {
        return Err(ProjectionLoweringError::Table(TableStoreError::Metadata(
            format!(
                "projection lowering produced {} records for {} resolved mutations",
                records.len(),
                mutations.len()
            ),
        )));
    }
    let records = sort_writes(
        records.into_iter().enumerate().collect(),
        |(index, record): &(usize, LoweredRecord)| {
            record.write_position(mutations[*index].kind(), *index as u64)
        },
    );
    Ok(LoweredProjectionPlan {
        resolved: resolved.clone(),
        records,
    })
}

fn projection_schema<M>() -> Result<&'static crate::TableSchema, ProjectionProgramError>
where
    M: ProjectionReadModelMetadata,
{
    ReadModelWritePlanBuilder::projection_validated_schema::<M>().map_err(program_metadata_error)
}

fn program_metadata_error(error: TableStoreError) -> ProjectionProgramError {
    ProjectionProgramError::InvalidOperation {
        operation: "read-model metadata".to_owned(),
        reason: error.to_string(),
    }
}

fn projection_value_type(kind: ProjectionPortableType) -> ProjectionValueType {
    match kind {
        ProjectionPortableType::Boolean => ProjectionValueType::Boolean,
        ProjectionPortableType::I64 => ProjectionValueType::I64,
        ProjectionPortableType::U64 => ProjectionValueType::U64,
        ProjectionPortableType::F64 => ProjectionValueType::F64,
        ProjectionPortableType::String => ProjectionValueType::String,
        ProjectionPortableType::Bytes
        | ProjectionPortableType::Json
        | ProjectionPortableType::Custom => ProjectionValueType::Json,
    }
}

fn model_field_for_column<M>(
    column: &str,
) -> Result<&'static ProjectionModelFieldMetadata, ProjectionProgramError>
where
    M: ProjectionReadModelMetadata,
{
    M::PROJECTION_FIELDS
        .iter()
        .find(|field| field.column_name == column)
        .ok_or_else(|| ProjectionProgramError::InvalidOperation {
            operation: "read-model metadata".to_owned(),
            reason: format!("column `{column}` has no portable field metadata"),
        })
}

fn body_path_for_model_field<B>(
    model: &ProjectionModelFieldMetadata,
) -> Result<ProjectionExpression, ProjectionProgramError>
where
    B: ProjectionBodyMetadata,
{
    let body = B::PROJECTION_FIELDS
        .iter()
        .find(|field| field.present && field.wire_name == model.field_name)
        .ok_or_else(|| ProjectionProgramError::RequiredValueAbsent {
            path: model.field_name.to_owned(),
        })?;
    ProjectionExpression::body_path(value_type_for_model(model), [body.wire_name])
}

fn value_type_for_model(model: &ProjectionModelFieldMetadata) -> ProjectionValueType {
    projection_value_type(model.portable_type)
}

fn build_model_operation<M>(
    operation_id: &'static str,
    staging_ordinal: u32,
    kind: MutationKind,
    key: Vec<ProjectionAuthoringField>,
    fields: Vec<ProjectionAuthoringField>,
    invalidations: Vec<ProjectionInvalidation>,
) -> Result<ProjectionOperation, ProjectionProgramError>
where
    M: ProjectionReadModelMetadata,
{
    let schema = projection_schema::<M>()?;
    let mut key_by_name = unique_authoring_fields(operation_id, "key", key)?;
    let fields_by_name = unique_authoring_fields(operation_id, "set", fields)?;
    let mut projected_key = Vec::with_capacity(schema.primary_key.columns.len());
    for (ordinal, column) in schema.primary_key.columns.iter().enumerate() {
        let metadata = model_field_for_column::<M>(column)?;
        let field = key_by_name.remove(metadata.field_name).ok_or_else(|| {
            ProjectionProgramError::InvalidOperation {
                operation: operation_id.to_owned(),
                reason: format!("key is missing primary-key field `{}`", metadata.field_name),
            }
        })?;
        let ProjectionAssignment::Set(expression) = field.assignment else {
            return Err(ProjectionProgramError::UnsetNotAllowed {
                field: metadata.field_name.to_owned(),
            });
        };
        projected_key.push(ProjectionKeyField::try_new(
            ordinal as u32,
            metadata.field_name,
            expression,
        )?);
    }
    if let Some(extra) = key_by_name.keys().next() {
        return Err(ProjectionProgramError::InvalidOperation {
            operation: operation_id.to_owned(),
            reason: format!("key contains non-primary-key field `{extra}`"),
        });
    }

    let mut all_fields = fields_by_name;
    if kind.is_complete_write() {
        for key_field in &projected_key {
            if all_fields.contains_key(key_field.name()) {
                return Err(ProjectionProgramError::InvalidOperation {
                    operation: operation_id.to_owned(),
                    reason: format!("complete-row set repeats key field `{}`", key_field.name()),
                });
            }
            all_fields.insert(
                key_field.name().to_owned(),
                ProjectionAuthoringField::set(
                    key_field.name().to_owned(),
                    key_field.expression().clone(),
                ),
            );
        }
        for metadata in M::PROJECTION_FIELDS {
            if metadata.required && !all_fields.contains_key(metadata.field_name) {
                return Err(ProjectionProgramError::InvalidOperation {
                    operation: operation_id.to_owned(),
                    reason: format!(
                        "complete row is missing required field `{}`",
                        metadata.field_name
                    ),
                });
            }
        }
    }
    let mut projected_fields = Vec::new();
    for metadata in M::PROJECTION_FIELDS {
        if let Some(field) = all_fields.remove(metadata.field_name) {
            projected_fields.push(ProjectionField::try_new(
                projected_fields.len() as u32,
                metadata.field_name,
                field.assignment,
            )?);
        }
    }
    if let Some(extra) = all_fields.keys().next() {
        return Err(ProjectionProgramError::InvalidOperation {
            operation: operation_id.to_owned(),
            reason: format!("set references unknown read-model field `{extra}`"),
        });
    }

    ProjectionOperation::try_new(
        operation_id,
        staging_ordinal,
        kind,
        projection_target::<M>()?,
        projected_key,
        projected_fields,
        Vec::new(),
        invalidations,
    )
}

fn unique_authoring_fields(
    operation: &str,
    kind: &'static str,
    fields: Vec<ProjectionAuthoringField>,
) -> Result<BTreeMap<String, ProjectionAuthoringField>, ProjectionProgramError> {
    let mut result = BTreeMap::new();
    for field in fields {
        if result.insert(field.field.clone(), field).is_some() {
            return Err(ProjectionProgramError::InvalidOperation {
                operation: operation.to_owned(),
                reason: format!("duplicate {kind} field"),
            });
        }
    }
    Ok(result)
}

fn operation_expression_for_column(
    operation: &ProjectionOperation,
    schema: &crate::TableSchema,
    column: &str,
) -> Result<ProjectionExpression, ProjectionProgramError> {
    let logical_name = schema
        .columns
        .iter()
        .find(|candidate| candidate.column_name == column)
        .map(|candidate| candidate.field_name.as_str())
        .ok_or_else(|| ProjectionProgramError::InvalidOperation {
            operation: operation.operation_id().to_owned(),
            reason: format!("relationship source column `{column}` is not mapped"),
        })?;
    if let Some(key) = operation
        .key()
        .iter()
        .find(|field| field.name() == logical_name)
    {
        return Ok(key.expression().clone());
    }
    let assignment = operation
        .fields()
        .iter()
        .find(|field| field.name() == logical_name)
        .ok_or_else(|| ProjectionProgramError::InvalidOperation {
            operation: operation.operation_id().to_owned(),
            reason: format!(
                "parent operation does not provide delegated source field `{logical_name}`"
            ),
        })?
        .assignment();
    match assignment {
        ProjectionAssignment::Set(expression) => Ok(expression.clone()),
        ProjectionAssignment::Unset => Err(ProjectionProgramError::UnsetNotAllowed {
            field: logical_name.to_owned(),
        }),
    }
}

fn validate_target<M>(mutation: &ResolvedProjectionMutation) -> Result<(), ProjectionLoweringError>
where
    M: ProjectionReadModelMetadata,
{
    let schema = ReadModelWritePlanBuilder::projection_validated_schema::<M>()?;
    if mutation.target().model() != schema.model_name
        || mutation.target().storage() != schema.table_name
    {
        return Err(ProjectionLoweringError::Table(TableStoreError::Metadata(
            format!(
                "resolved target `{}/{}` does not match ORM model `{}/{}`",
                mutation.target().model(),
                mutation.target().storage(),
                schema.model_name,
                schema.table_name
            ),
        )));
    }
    Ok(())
}

fn resolved_key<M>(key: &ResolvedProjectionKey) -> Result<RowKey, ProjectionLoweringError>
where
    M: ProjectionReadModelMetadata,
{
    let schema = ReadModelWritePlanBuilder::projection_validated_schema::<M>()?;
    let mut result = RowKey::default();
    for field in key.fields() {
        let metadata = M::PROJECTION_FIELDS
            .iter()
            .find(|candidate| candidate.field_name == field.name())
            .ok_or_else(|| {
                TableStoreError::Metadata(format!(
                    "model `{}` has no projection key field `{}`",
                    schema.model_name,
                    field.name()
                ))
            })?;
        let column = schema
            .columns
            .iter()
            .find(|column| column.column_name == metadata.column_name)
            .ok_or_else(|| {
                TableStoreError::Metadata(format!(
                    "model `{}` is missing column `{}`",
                    schema.model_name, metadata.column_name
                ))
            })?;
        result.insert(
            metadata.column_name,
            projection_value_to_row(field.value(), &column.column_type)?,
        );
    }
    Ok(result)
}

fn resolved_full_row<M>(
    fields: &[ResolvedProjectionField],
) -> Result<RowValues, ProjectionLoweringError>
where
    M: ProjectionReadModelMetadata,
{
    let row = resolved_values::<M>(fields, false)?;
    validate_row_values(
        ReadModelWritePlanBuilder::projection_validated_schema::<M>()?,
        &row,
        true,
    )?;
    Ok(row)
}

fn resolved_patch<M>(
    fields: &[ResolvedProjectionField],
) -> Result<RowValues, ProjectionLoweringError>
where
    M: ProjectionReadModelMetadata,
{
    let schema = ReadModelWritePlanBuilder::projection_validated_schema::<M>()?;
    let patch = resolved_values::<M>(fields, true)?;
    if patch.is_empty() {
        return Err(TableStoreError::Metadata(format!(
            "model `{}` patch must set at least one column",
            schema.model_name
        ))
        .into());
    }
    validate_row_values(schema, &patch, false)?;
    Ok(patch)
}

fn resolved_values<M>(
    fields: &[ResolvedProjectionField],
    allow_absent: bool,
) -> Result<RowValues, ProjectionLoweringError>
where
    M: ProjectionReadModelMetadata,
{
    let schema = ReadModelWritePlanBuilder::projection_validated_schema::<M>()?;
    let mut values = RowValues::new();
    for field in fields {
        let metadata = M::PROJECTION_FIELDS
            .iter()
            .find(|candidate| candidate.field_name == field.name())
            .ok_or_else(|| {
                TableStoreError::Metadata(format!(
                    "model `{}` has no projection field `{}`",
                    schema.model_name,
                    field.name()
                ))
            })?;
        let column = schema
            .columns
            .iter()
            .find(|column| column.column_name == metadata.column_name)
            .ok_or_else(|| {
                TableStoreError::Metadata(format!(
                    "model `{}` is missing column `{}`",
                    schema.model_name, metadata.column_name
                ))
            })?;
        match field.value() {
            ResolvedProjectionValue::Value(value) => {
                values.insert(
                    metadata.column_name,
                    projection_value_to_row(value, &column.column_type)?,
                );
            }
            ResolvedProjectionValue::Absent if allow_absent => {}
            ResolvedProjectionValue::Absent => {
                return Err(ProjectionLoweringError::Program(
                    ProjectionProgramError::RequiredValueAbsent {
                        path: field.name().to_owned(),
                    },
                ));
            }
            ResolvedProjectionValue::Unset => {
                return Err(ProjectionLoweringError::Table(TableStoreError::Metadata(
                    format!(
                        "authoritative relational projection cannot unset field `{}`; use explicit null for nullable columns",
                        field.name()
                    ),
                )));
            }
        }
    }
    Ok(values)
}

fn validate_insert_missing_patch<M>(
    key: &RowKey,
    patch: &RowValues,
) -> Result<(), ProjectionLoweringError>
where
    M: ProjectionReadModelMetadata,
{
    let schema = ReadModelWritePlanBuilder::projection_validated_schema::<M>()?;
    let mut candidate = RowValues::new();
    for (column, value) in key.iter() {
        candidate.insert(column, value.clone());
    }
    for (column, value) in patch.iter() {
        if let Some(key_value) = key.get(column) {
            if key_value != value {
                return Err(TableStoreError::Metadata(format!(
                    "model `{}` patch changes primary-key column `{column}`",
                    schema.model_name
                ))
                .into());
            }
        }
        candidate.insert(column, value.clone());
    }
    validate_row_values(schema, &candidate, true)?;
    Ok(())
}

fn projection_value_to_row(
    value: &ProjectionValue,
    column_type: &ColumnType,
) -> Result<RowValue, ProjectionLoweringError> {
    if value.is_null() {
        return Ok(RowValue::Null);
    }
    let invalid = || {
        ProjectionLoweringError::Table(TableStoreError::Metadata(format!(
            "portable projection value is incompatible with column type `{column_type:?}`"
        )))
    };
    match (column_type, value.as_ref()) {
        (ColumnType::Boolean, ProjectionValueRef::Boolean(value)) => Ok(RowValue::Bool(value)),
        (ColumnType::Integer, ProjectionValueRef::I64(value)) => value
            .parse::<i64>()
            .map(RowValue::I64)
            .map_err(|_| invalid()),
        (ColumnType::UnsignedInteger, ProjectionValueRef::U64(value)) => value
            .parse::<u64>()
            .map(|value| {
                i64::try_from(value)
                    .map(RowValue::I64)
                    .unwrap_or_else(|_| RowValue::U64(value))
            })
            .map_err(|_| invalid()),
        (ColumnType::Float, ProjectionValueRef::F64(value)) => value
            .parse::<f64>()
            .map(RowValue::F64)
            .map_err(|_| invalid()),
        (ColumnType::Text | ColumnType::Timestamp, ProjectionValueRef::String(value)) => {
            Ok(RowValue::String(value.to_owned()))
        }
        (ColumnType::Text | ColumnType::Timestamp, ProjectionValueRef::Enum { variant, .. }) => {
            Ok(RowValue::String(variant.to_owned()))
        }
        (ColumnType::Bytes, ProjectionValueRef::List(values)) => values
            .iter()
            .map(|value| match value.as_ref() {
                ProjectionValueRef::U64(value) => value.parse::<u8>().map_err(|_| invalid()),
                _ => Err(invalid()),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(RowValue::Bytes),
        (ColumnType::Json, _) => Ok(RowValue::Json(projection_value_to_json(value)?)),
        (ColumnType::Unsupported(_), _) => Err(invalid()),
        _ => Err(invalid()),
    }
}

fn projection_value_to_json(
    value: &ProjectionValue,
) -> Result<serde_json::Value, ProjectionLoweringError> {
    Ok(match value.as_ref() {
        ProjectionValueRef::Null => serde_json::Value::Null,
        ProjectionValueRef::Boolean(value) => serde_json::Value::Bool(value),
        ProjectionValueRef::I64(value) => serde_json::Value::Number(
            value
                .parse::<i64>()
                .map_err(|_| {
                    TableStoreError::Metadata("invalid canonical i64 projection value".into())
                })?
                .into(),
        ),
        ProjectionValueRef::U64(value) => serde_json::Value::Number(
            value
                .parse::<u64>()
                .map_err(|_| {
                    TableStoreError::Metadata("invalid canonical u64 projection value".into())
                })?
                .into(),
        ),
        ProjectionValueRef::F64(value) => serde_json::Value::Number(
            serde_json::Number::from_f64(value.parse::<f64>().map_err(|_| {
                TableStoreError::Metadata("invalid canonical f64 projection value".into())
            })?)
            .ok_or_else(|| {
                TableStoreError::Metadata("non-finite canonical f64 projection value".into())
            })?,
        ),
        ProjectionValueRef::String(value) => serde_json::Value::String(value.to_owned()),
        ProjectionValueRef::Enum { variant, .. } => serde_json::Value::String(variant.to_owned()),
        ProjectionValueRef::List(values) => serde_json::Value::Array(
            values
                .iter()
                .map(projection_value_to_json)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        ProjectionValueRef::Object(fields) => serde_json::Value::Object(
            fields
                .iter()
                .map(|field| {
                    Ok((
                        field.name().to_owned(),
                        projection_value_to_json(field.value())?,
                    ))
                })
                .collect::<Result<serde_json::Map<_, _>, ProjectionLoweringError>>()?,
        ),
    })
}

/// Construct a complete projection program from generated arms.
///
/// # Errors
///
/// Returns task-7 validation failures.
#[doc(hidden)]
pub fn projection_program(
    name: &'static str,
    version: u64,
    partition: ProjectionPartition,
    arms: Vec<ProjectionArm>,
) -> Result<ProjectionProgram, ProjectionProgramError> {
    ProjectionProgram::try_new(name, version, partition, arms)
}

/// Resolve through an exact generated event-set marker.
///
/// # Errors
///
/// Returns event-set, selector, expression, or logical-plan failures.
#[doc(hidden)]
pub fn resolve_typed<E>(
    program: ProjectionProgram,
    occurrence: &DomainEventOccurrence,
) -> Result<ResolvedProjectionPlan, ProjectionProgramError>
where
    E: ProjectionEventSet,
{
    ProjectionPlanTemplate::<E>::try_new(program)?.resolve(occurrence)
}

/// Build a canonical relationship output entry.
///
/// # Errors
///
/// Returns invalid ORM relationship metadata.
#[doc(hidden)]
pub fn output_relationship<R>() -> Result<ProjectionRelationship, ProjectionLoweringError>
where
    R: ProjectionRelationshipMetadata,
{
    let source = ReadModelWritePlanBuilder::projection_validated_schema::<R::Source>()?;
    let target = ReadModelWritePlanBuilder::projection_validated_schema::<R::Target>()?;
    ReadModelWritePlanBuilder::projection_relationship_for(source, R::FIELD, target)?;
    Ok(ProjectionRelationship::try_new(
        source.model_name.clone(),
        R::FIELD,
        target.model_name.clone(),
    )?)
}
