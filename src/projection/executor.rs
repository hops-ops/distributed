//! Causal execution of already-resolved authoritative projection plans.
//!
//! This module chooses protocol lifecycle operations from one coherent,
//! explicitly scoped snapshot batch. It never commits the physical
//! [`crate::TableWritePlan`] directly.

use std::collections::{HashMap, HashSet};

use super::lower::LoweredProjectionPlan;
use super::{
    ProjectionMutationKind as LogicalMutationKind, ProjectionValueRef, ResolvedProjectionKey,
    ResolvedProjectionPartitionRef, MAX_PROJECTION_OPERATIONS_PER_OCCURRENCE,
};
use crate::projection_protocol::{
    ProjectionExecutionSnapshotBatch, ProjectionExecutionSnapshotBatchRequest,
    ProjectionMutationKind, ProjectionProtocolError, ProjectionRecordExpectation,
    ProjectionRecordScope, ProjectionScopedRowSnapshot, ProjectionWorkspace, RecordRevision,
};
use crate::table::{
    key_from_row, ExpectedVersion, PatchMode, RowKey, RowValue, RowValues, RowWriteMode,
    TableMutation, TableRowMutation, TableSchema, TableWritePlan,
};

/// Maximum distinct protocol/query scopes one occurrence may inspect.
pub(crate) const MAX_PROJECTION_EXECUTION_SCOPES: usize = 4_096;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LifecycleIntent {
    Insert,
    Upsert,
    Patch,
    UpsertPatch,
    Delete,
    Recreate,
}

#[derive(Clone, Debug)]
struct ExecutionMutation {
    scope: ProjectionRecordScope,
    intent: LifecycleIntent,
    mutation: TableMutation,
    source_snapshot: Option<crate::projection_protocol::SourceSnapshotVersion>,
}

/// Store-free execution preflight produced before any adapter read.
#[derive(Debug)]
pub(crate) struct PreparedProjectionExecution {
    mutations: Vec<ExecutionMutation>,
    snapshots: ProjectionExecutionSnapshotBatchRequest,
    cached: HashMap<ProjectionRecordScope, ProjectionScopedRowSnapshot>,
}

impl PreparedProjectionExecution {
    pub(crate) fn snapshot_request(&self) -> &ProjectionExecutionSnapshotBatchRequest {
        &self.snapshots
    }

    pub(crate) fn needs_snapshot_read(&self) -> bool {
        !self.snapshots.requests.is_empty()
    }

    /// Validate exact returned scopes, choose every lifecycle, and stage the
    /// complete mutation set atomically into the framework workspace.
    pub(crate) fn stage(
        self,
        workspace: &mut ProjectionWorkspace,
        returned: ProjectionExecutionSnapshotBatch,
    ) -> Result<(), ProjectionProtocolError> {
        let mut snapshots = self.cached;
        if returned.snapshots.len() != self.snapshots.requests.len() {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projection execution snapshot returned {} scopes for {} requests",
                returned.snapshots.len(),
                self.snapshots.requests.len()
            )));
        }

        let requested = self
            .snapshots
            .requests
            .iter()
            .map(|request| request.scope.clone())
            .collect::<HashSet<_>>();
        let requested_rows = self
            .snapshots
            .requests
            .iter()
            .map(|request| (request.scope.clone(), request))
            .collect::<HashMap<_, _>>();
        let mut returned_scopes = HashSet::new();
        for snapshot in returned.snapshots {
            if !requested.contains(&snapshot.scope) {
                return Err(ProjectionProtocolError::ScopeMismatch {
                    field: "projection execution snapshot result",
                });
            }
            if !returned_scopes.insert(snapshot.scope.clone()) {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection execution snapshot repeats model `{}` record scope",
                    snapshot.scope.model()
                )));
            }
            validate_snapshot_scope(&snapshot)?;
            if let Some(row) = &snapshot.row {
                let request = requested_rows
                    .get(&snapshot.scope)
                    .expect("returned scope membership was checked above");
                if key_from_row(&request.schema, row)? != request.key {
                    return Err(ProjectionProtocolError::ScopeMismatch {
                        field: "projection execution snapshot row key",
                    });
                }
            }
            snapshots.insert(snapshot.scope.clone(), snapshot);
        }
        if returned_scopes != requested {
            return Err(ProjectionProtocolError::InvalidBatch(
                "projection execution snapshot omitted a requested record scope".into(),
            ));
        }

        let mut next = workspace.clone();
        for mutation in self.mutations {
            let snapshot = snapshots.get(&mutation.scope).ok_or_else(|| {
                ProjectionProtocolError::InvalidBatch(format!(
                    "projection execution has no snapshot for model `{}` record scope",
                    mutation.scope.model()
                ))
            })?;
            validate_snapshot_scope(snapshot)?;
            let source = mutation.source_snapshot.clone();
            if let Some(record) = &snapshot.record {
                match (&source, &record.source_snapshot) {
                    (Some(incoming), Some(current)) if !incoming.advances(current)? => {
                        // A stale command still observes the current authoritative
                        // revision. The commit validates that exact revision, so a
                        // racing writer cannot turn this into a false confirmation.
                        let (schema, _) = mutation_schema_key(&mutation.mutation);
                        next.confirm_existing(schema, record.revision.clone())?;
                        continue;
                    }
                    (Some(_), None) | (None, Some(_)) => {
                        return Err(ProjectionProtocolError::InvalidBatch(
                            "projection source-ordering policy changed; rebuild the read model"
                                .into(),
                        ));
                    }
                    _ => {}
                }
            }
            let (physical, expectation, kind) = if source.is_some() {
                stage_source_snapshot(mutation, snapshot)?
            } else {
                stage_for_snapshot(mutation, snapshot)?
            };
            next.stage_execution_mutation(physical, expectation, kind, source)?;
        }
        *workspace = next;
        Ok(())
    }
}

/// Prepare one portable resolved plan without touching an adapter.
pub(crate) fn prepare_portable_projection(
    workspace: &ProjectionWorkspace,
    lowered: LoweredProjectionPlan,
) -> Result<PreparedProjectionExecution, ProjectionProtocolError> {
    validate_resolved_partition(workspace, lowered.resolved.partition())?;
    let client_visible = lowered
        .resolved
        .mutations()
        .iter()
        .try_fold(0usize, |count, mutation| {
            count
                .checked_add(1)
                .and_then(|count| {
                    count.checked_add(mutation.provenance().relationship_effects().len())
                })
                .and_then(|count| count.checked_add(mutation.provenance().invalidations().len()))
        });
    let Some(client_visible) = client_visible else {
        return Err(ProjectionProtocolError::InvalidBatch(
            "portable projection client-visible mutation count overflowed".into(),
        ));
    };
    validate_execution_bounds(lowered.write_plan.mutations.len(), client_visible)?;
    lowered.write_plan.validate()?;
    let source_snapshot = lowered
        .resolved
        .source_snapshots()
        .then(|| {
            crate::projection_protocol::SourceSnapshotVersion::from_occurrence(
                lowered.resolved.occurrence(),
            )
        })
        .transpose()?;

    let mut used_logical = vec![false; lowered.resolved.mutations().len()];
    let mut mutations = Vec::with_capacity(lowered.write_plan.mutations.len());
    let mut scopes = HashSet::new();
    for physical in lowered.write_plan.mutations {
        let (schema, physical_key) = mutation_schema_key(&physical);
        let scope = workspace.record_scope(schema, physical_key)?;
        if !scopes.insert(scope.clone()) {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projection execution repeats model `{}` record scope",
                scope.model()
            )));
        }

        let matches = lowered
            .resolved
            .mutations()
            .iter()
            .enumerate()
            .filter(|(index, logical)| {
                !used_logical[*index]
                    && logical.target().model() == schema.model_name
                    && logical.target().storage() == schema.table_name
                    && resolved_row_key(schema, logical.key()).is_ok_and(|key| key == *physical_key)
            })
            .collect::<Vec<_>>();
        let [(logical_index, logical)] = matches.as_slice() else {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "physical projection mutation for model `{}` does not have one exact logical key match",
                schema.model_name
            )));
        };
        used_logical[*logical_index] = true;
        let intent = intent_for_logical(logical.kind());
        validate_physical_intent(intent, &physical)?;
        mutations.push(ExecutionMutation {
            scope,
            intent,
            mutation: physical,
            source_snapshot: source_snapshot.clone(),
        });
    }
    if used_logical.iter().any(|used| !used) {
        return Err(ProjectionProtocolError::InvalidBatch(
            "resolved projection contains a logical mutation with no physical ORM lowering".into(),
        ));
    }
    prepare(workspace, mutations, HashMap::new())
}

/// Prepare a stateful graph diff, retaining exact already-loaded revisions and
/// querying only previously unseen mutation scopes.
///
/// Used by projection protocol unit tests; portable/modeled handlers use
/// [`prepare_portable_projection`].
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn prepare_graph_projection(
    workspace: &ProjectionWorkspace,
    plan: TableWritePlan,
    cached: HashMap<ProjectionRecordScope, ProjectionScopedRowSnapshot>,
) -> Result<PreparedProjectionExecution, ProjectionProtocolError> {
    validate_execution_bounds(plan.mutations.len(), 0)?;
    plan.validate()?;
    let mut scopes = HashSet::new();
    let mut mutations = Vec::with_capacity(plan.mutations.len());
    for mutation in plan.mutations {
        let (schema, key) = mutation_schema_key(&mutation);
        let scope = workspace.record_scope(schema, key)?;
        if !scopes.insert(scope.clone()) {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projection graph diff repeats model `{}` record scope",
                scope.model()
            )));
        }
        mutations.push(ExecutionMutation {
            scope,
            intent: intent_for_physical(&mutation),
            mutation,
            source_snapshot: None,
        });
    }
    prepare(workspace, mutations, cached)
}

fn prepare(
    workspace: &ProjectionWorkspace,
    mutations: Vec<ExecutionMutation>,
    cached: HashMap<ProjectionRecordScope, ProjectionScopedRowSnapshot>,
) -> Result<PreparedProjectionExecution, ProjectionProtocolError> {
    if cached.len() > MAX_PROJECTION_EXECUTION_SCOPES {
        return Err(ProjectionProtocolError::InvalidBatch(format!(
            "projection execution has {} cached scopes; maximum is {}",
            cached.len(),
            MAX_PROJECTION_EXECUTION_SCOPES
        )));
    }
    for (scope, snapshot) in &cached {
        if scope != &snapshot.scope {
            return Err(ProjectionProtocolError::ScopeMismatch {
                field: "cached projection execution snapshot",
            });
        }
        validate_snapshot_scope(snapshot)?;
    }
    for mutation in &mutations {
        if let Some(snapshot) = cached.get(&mutation.scope) {
            if let Some(row) = &snapshot.row {
                let (schema, expected_key) = mutation_schema_key(&mutation.mutation);
                if key_from_row(schema, row)? != *expected_key {
                    return Err(ProjectionProtocolError::ScopeMismatch {
                        field: "cached projection execution snapshot row key",
                    });
                }
            }
        }
    }
    let pending = mutations
        .iter()
        .filter(|mutation| !cached.contains_key(&mutation.scope))
        .map(|mutation| mutation.mutation.clone())
        .collect::<Vec<_>>();
    let total = cached.len().checked_add(pending.len()).ok_or_else(|| {
        ProjectionProtocolError::InvalidBatch("projection execution scope count overflowed".into())
    })?;
    if total > MAX_PROJECTION_EXECUTION_SCOPES {
        return Err(ProjectionProtocolError::InvalidBatch(format!(
            "projection execution has {total} scopes; maximum is {MAX_PROJECTION_EXECUTION_SCOPES}"
        )));
    }
    let snapshots = workspace.execution_snapshot_request(&pending)?;
    Ok(PreparedProjectionExecution {
        mutations,
        snapshots,
        cached,
    })
}

fn validate_resolved_partition(
    workspace: &ProjectionWorkspace,
    partition: &super::ResolvedProjectionPartition,
) -> Result<(), ProjectionProtocolError> {
    let matches = match partition.as_ref() {
        ResolvedProjectionPartitionRef::Unit => workspace.partition_value().is_none(),
        ResolvedProjectionPartitionRef::Value(value) => {
            workspace.partition_value() == Some(&projection_value_json(value.as_ref())?)
        }
    };
    if matches {
        Ok(())
    } else {
        Err(ProjectionProtocolError::ScopeMismatch {
            field: "resolved projection partition",
        })
    }
}

fn projection_value_json(
    value: ProjectionValueRef<'_>,
) -> Result<serde_json::Value, ProjectionProtocolError> {
    match value {
        ProjectionValueRef::Null => Ok(serde_json::Value::Null),
        ProjectionValueRef::Boolean(value) => Ok(serde_json::Value::Bool(value)),
        ProjectionValueRef::I64(value) => value
            .parse::<i64>()
            .map(serde_json::Number::from)
            .map(serde_json::Value::Number)
            .map_err(|_| {
                ProjectionProtocolError::InvalidBatch(
                    "resolved projection partition has invalid i64".into(),
                )
            }),
        ProjectionValueRef::U64(value) => value
            .parse::<u64>()
            .map(serde_json::Number::from)
            .map(serde_json::Value::Number)
            .map_err(|_| {
                ProjectionProtocolError::InvalidBatch(
                    "resolved projection partition has invalid u64".into(),
                )
            }),
        ProjectionValueRef::F64(value) => value
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(serde_json::Value::Number)
            .ok_or_else(|| {
                ProjectionProtocolError::InvalidBatch(
                    "resolved projection partition has invalid f64".into(),
                )
            }),
        ProjectionValueRef::String(value) | ProjectionValueRef::Enum { variant: value, .. } => {
            Ok(serde_json::Value::String(value.to_owned()))
        }
        ProjectionValueRef::List(values) => values
            .iter()
            .map(|value| projection_value_json(value.as_ref()))
            .collect::<Result<Vec<_>, _>>()
            .map(serde_json::Value::Array),
        ProjectionValueRef::Object(fields) => fields
            .iter()
            .map(|field| {
                Ok((
                    field.name().to_owned(),
                    projection_value_json(field.value().as_ref())?,
                ))
            })
            .collect::<Result<serde_json::Map<_, _>, ProjectionProtocolError>>()
            .map(serde_json::Value::Object),
    }
}

pub(crate) fn resolved_partition_json(
    partition: &super::ResolvedProjectionPartition,
) -> Result<Option<serde_json::Value>, ProjectionProtocolError> {
    match partition.as_ref() {
        ResolvedProjectionPartitionRef::Unit => Ok(None),
        ResolvedProjectionPartitionRef::Value(value) => {
            projection_value_json(value.as_ref()).map(Some)
        }
    }
}

fn validate_execution_bounds(
    scopes: usize,
    client_visible: usize,
) -> Result<(), ProjectionProtocolError> {
    if scopes > MAX_PROJECTION_EXECUTION_SCOPES {
        return Err(ProjectionProtocolError::InvalidBatch(format!(
            "projection execution has {scopes} scopes; maximum is {MAX_PROJECTION_EXECUTION_SCOPES}"
        )));
    }
    if client_visible > MAX_PROJECTION_OPERATIONS_PER_OCCURRENCE {
        return Err(ProjectionProtocolError::InvalidBatch(format!(
            "portable projection has {client_visible} client-visible mutations; maximum is {}",
            MAX_PROJECTION_OPERATIONS_PER_OCCURRENCE
        )));
    }
    Ok(())
}

pub(crate) fn validate_snapshot_scope(
    snapshot: &ProjectionScopedRowSnapshot,
) -> Result<(), ProjectionProtocolError> {
    if snapshot
        .record
        .as_ref()
        .is_some_and(|record| record.revision.scope() != &snapshot.scope)
    {
        return Err(ProjectionProtocolError::ScopeMismatch {
            field: "projection snapshot record revision",
        });
    }
    if snapshot.record.as_ref().is_some_and(|record| {
        record.change.topology() != snapshot.scope.topology()
            || record.change.projection_partition() != snapshot.scope.projection_partition()
    }) {
        return Err(ProjectionProtocolError::ScopeMismatch {
            field: "projection snapshot record change",
        });
    }
    match (&snapshot.row, &snapshot.record) {
        (None, None) => Ok(()),
        (Some(_), Some(record)) if !record.tombstone => Ok(()),
        (None, Some(record)) if record.tombstone => Ok(()),
        _ => Err(ProjectionProtocolError::InvalidBatch(format!(
            "projection snapshot for model `{}` has inconsistent physical row and protocol metadata",
            snapshot.scope.model()
        ))),
    }
}

fn stage_source_snapshot(
    execution: ExecutionMutation,
    snapshot: &ProjectionScopedRowSnapshot,
) -> Result<
    (
        TableMutation,
        ProjectionRecordExpectation,
        ProjectionMutationKind,
    ),
    ProjectionProtocolError,
> {
    let expected = snapshot
        .record
        .as_ref()
        .map_or(ProjectionRecordExpectation::Missing, |record| {
            ProjectionRecordExpectation::Exact(record.revision.clone())
        });
    match execution.intent {
        LifecycleIntent::Upsert => {
            let tombstone = snapshot
                .record
                .as_ref()
                .is_some_and(|record| record.tombstone);
            let mutation = if snapshot.row.is_some() {
                normalize_save(execution.mutation)?
            } else {
                normalize_create(execution.mutation)?
            };
            Ok((
                mutation,
                expected,
                if tombstone {
                    ProjectionMutationKind::Recreate
                } else {
                    ProjectionMutationKind::Upsert
                },
            ))
        }
        LifecycleIntent::Delete => Ok((
            normalize_delete(execution.mutation)?,
            expected,
            ProjectionMutationKind::Delete,
        )),
        _ => Err(ProjectionProtocolError::InvalidBatch(
            "source snapshots cannot apply partial/delta mutations".into(),
        )),
    }
}

fn stage_for_snapshot(
    execution: ExecutionMutation,
    snapshot: &ProjectionScopedRowSnapshot,
) -> Result<
    (
        TableMutation,
        ProjectionRecordExpectation,
        ProjectionMutationKind,
    ),
    ProjectionProtocolError,
> {
    enum State<'a> {
        Missing,
        Live(&'a RecordRevision),
        Tombstone(&'a RecordRevision),
    }
    let state = match (&snapshot.row, &snapshot.record) {
        (None, None) => State::Missing,
        (Some(_), Some(record)) if !record.tombstone => State::Live(&record.revision),
        (None, Some(record)) if record.tombstone => State::Tombstone(&record.revision),
        _ => {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projection snapshot for model `{}` has inconsistent physical row and protocol metadata",
                snapshot.scope.model()
            )))
        }
    };
    let model = snapshot.scope.model().to_owned();
    match (execution.intent, state) {
        (LifecycleIntent::Insert, State::Missing) => Ok((
            normalize_create(execution.mutation)?,
            ProjectionRecordExpectation::Missing,
            ProjectionMutationKind::Upsert,
        )),
        (LifecycleIntent::Insert, _) => Err(ProjectionProtocolError::RecordAlreadyExists { model }),
        (LifecycleIntent::Upsert, State::Missing) => Ok((
            normalize_create(execution.mutation)?,
            ProjectionRecordExpectation::Missing,
            ProjectionMutationKind::Upsert,
        )),
        (LifecycleIntent::Upsert, State::Live(revision)) => Ok((
            normalize_save(execution.mutation)?,
            ProjectionRecordExpectation::Exact(revision.clone()),
            ProjectionMutationKind::Upsert,
        )),
        (LifecycleIntent::Upsert, State::Tombstone(_)) => {
            Err(ProjectionProtocolError::RecordTombstoned { model })
        }
        (LifecycleIntent::Patch, State::Live(revision)) => Ok((
            normalize_patch(execution.mutation)?,
            ProjectionRecordExpectation::Exact(revision.clone()),
            ProjectionMutationKind::Upsert,
        )),
        (LifecycleIntent::Patch, State::Missing) => {
            Err(ProjectionProtocolError::RecordMissing { model })
        }
        (LifecycleIntent::Patch, State::Tombstone(_)) => {
            Err(ProjectionProtocolError::RecordTombstoned { model })
        }
        (LifecycleIntent::UpsertPatch, State::Missing) => Ok((
            upsert_patch_create(execution.mutation)?,
            ProjectionRecordExpectation::Missing,
            ProjectionMutationKind::Upsert,
        )),
        (LifecycleIntent::UpsertPatch, State::Live(revision)) => Ok((
            normalize_patch(execution.mutation)?,
            ProjectionRecordExpectation::Exact(revision.clone()),
            ProjectionMutationKind::Upsert,
        )),
        (LifecycleIntent::UpsertPatch, State::Tombstone(_)) => {
            Err(ProjectionProtocolError::RecordTombstoned { model })
        }
        (LifecycleIntent::Delete, State::Live(revision)) => Ok((
            normalize_delete(execution.mutation)?,
            ProjectionRecordExpectation::Exact(revision.clone()),
            ProjectionMutationKind::Delete,
        )),
        (LifecycleIntent::Delete, State::Missing) => {
            Err(ProjectionProtocolError::RecordMissing { model })
        }
        (LifecycleIntent::Delete, State::Tombstone(_)) => {
            Err(ProjectionProtocolError::RecordTombstoned { model })
        }
        (LifecycleIntent::Recreate, State::Tombstone(revision)) => Ok((
            normalize_create(execution.mutation)?,
            ProjectionRecordExpectation::Exact(revision.clone()),
            ProjectionMutationKind::Recreate,
        )),
        (LifecycleIntent::Recreate, State::Missing | State::Live(_)) => {
            Err(ProjectionProtocolError::RecreateRequiresTombstone { model })
        }
    }
}

fn normalize_create(mutation: TableMutation) -> Result<TableMutation, ProjectionProtocolError> {
    let TableMutation::UpsertRow(mut row) = mutation else {
        return Err(invalid_shape("create", &mutation));
    };
    row.mode = RowWriteMode::Insert;
    row.expected_version = ExpectedVersion::NotExists;
    Ok(TableMutation::UpsertRow(row))
}

fn normalize_save(mutation: TableMutation) -> Result<TableMutation, ProjectionProtocolError> {
    let TableMutation::UpsertRow(mut row) = mutation else {
        return Err(invalid_shape("save", &mutation));
    };
    row.mode = RowWriteMode::Upsert;
    row.expected_version = ExpectedVersion::Any;
    Ok(TableMutation::UpsertRow(row))
}

fn normalize_patch(mutation: TableMutation) -> Result<TableMutation, ProjectionProtocolError> {
    let TableMutation::PatchRow(mut patch) = mutation else {
        return Err(invalid_shape("patch", &mutation));
    };
    patch.mode = PatchMode::UpdateExisting;
    patch.expected_version = ExpectedVersion::Any;
    Ok(TableMutation::PatchRow(patch))
}

fn normalize_delete(mutation: TableMutation) -> Result<TableMutation, ProjectionProtocolError> {
    let TableMutation::DeleteRow(mut delete) = mutation else {
        return Err(invalid_shape("delete", &mutation));
    };
    delete.expected_version = ExpectedVersion::Any;
    Ok(TableMutation::DeleteRow(delete))
}

fn upsert_patch_create(mutation: TableMutation) -> Result<TableMutation, ProjectionProtocolError> {
    let TableMutation::PatchRow(patch) = mutation else {
        return Err(invalid_shape("upsert-patch", &mutation));
    };
    let mut values = RowValues::new();
    for (column, value) in patch.key.iter() {
        values.insert(column, value.clone());
    }
    for (column, value) in patch.patch.iter() {
        if patch.key.get(column).is_some_and(|key| key != value) {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projection upsert-patch changes primary-key column `{column}`"
            )));
        }
        values.insert(column, value.clone());
    }
    Ok(TableMutation::UpsertRow(TableRowMutation {
        schema: patch.schema,
        key: patch.key,
        values,
        expected_version: ExpectedVersion::NotExists,
        mode: RowWriteMode::Insert,
    }))
}

fn invalid_shape(operation: &str, mutation: &TableMutation) -> ProjectionProtocolError {
    ProjectionProtocolError::InvalidBatch(format!(
        "projection {operation} lifecycle is incompatible with physical mutation `{}`",
        mutation_kind_name(mutation)
    ))
}

fn mutation_kind_name(mutation: &TableMutation) -> &'static str {
    match mutation {
        TableMutation::UpsertRow(_) => "full_row",
        TableMutation::PatchRow(_) => "patch",
        TableMutation::DeleteRow(_) => "delete",
    }
}

fn intent_for_logical(kind: LogicalMutationKind) -> LifecycleIntent {
    match kind {
        LogicalMutationKind::Insert | LogicalMutationKind::InsertRelated => LifecycleIntent::Insert,
        LogicalMutationKind::Upsert | LogicalMutationKind::UpsertRelated => LifecycleIntent::Upsert,
        LogicalMutationKind::Patch => LifecycleIntent::Patch,
        LogicalMutationKind::UpsertPatch => LifecycleIntent::UpsertPatch,
        LogicalMutationKind::Delete => LifecycleIntent::Delete,
        LogicalMutationKind::Recreate => LifecycleIntent::Recreate,
    }
}

fn intent_for_physical(mutation: &TableMutation) -> LifecycleIntent {
    match mutation {
        TableMutation::UpsertRow(row) if row.mode == RowWriteMode::Insert => {
            LifecycleIntent::Insert
        }
        TableMutation::UpsertRow(_) => LifecycleIntent::Upsert,
        TableMutation::PatchRow(patch) if patch.mode == PatchMode::InsertMissing => {
            LifecycleIntent::UpsertPatch
        }
        TableMutation::PatchRow(_) => LifecycleIntent::Patch,
        TableMutation::DeleteRow(_) => LifecycleIntent::Delete,
    }
}

fn validate_physical_intent(
    intent: LifecycleIntent,
    mutation: &TableMutation,
) -> Result<(), ProjectionProtocolError> {
    let compatible = matches!(
        (intent, mutation),
        (
            LifecycleIntent::Insert | LifecycleIntent::Upsert | LifecycleIntent::Recreate,
            TableMutation::UpsertRow(_)
        ) | (
            LifecycleIntent::Patch | LifecycleIntent::UpsertPatch,
            TableMutation::PatchRow(_)
        ) | (LifecycleIntent::Delete, TableMutation::DeleteRow(_))
    );
    if compatible {
        Ok(())
    } else {
        Err(invalid_shape("logical", mutation))
    }
}

fn mutation_schema_key(mutation: &TableMutation) -> (&'static TableSchema, &RowKey) {
    match mutation {
        TableMutation::UpsertRow(mutation) => (mutation.schema, &mutation.key),
        TableMutation::PatchRow(mutation) => (mutation.schema, &mutation.key),
        TableMutation::DeleteRow(mutation) => (mutation.schema, &mutation.key),
    }
}

fn resolved_row_key(
    schema: &TableSchema,
    key: &ResolvedProjectionKey,
) -> Result<RowKey, ProjectionProtocolError> {
    let mut row = RowKey::new(std::iter::empty::<(&str, RowValue)>());
    for field in key.fields() {
        let column = schema
            .columns
            .iter()
            .find(|column| column.field_name == field.name())
            .ok_or_else(|| {
                ProjectionProtocolError::InvalidBatch(format!(
                    "projection key field `{}` is absent from model `{}`",
                    field.name(),
                    schema.model_name
                ))
            })?;
        let value = match field.value().as_ref() {
            ProjectionValueRef::Boolean(value) => RowValue::Bool(value),
            ProjectionValueRef::I64(value) => RowValue::I64(value.parse().map_err(|_| {
                ProjectionProtocolError::InvalidBatch(
                    "projection key contains an invalid canonical i64".into(),
                )
            })?),
            ProjectionValueRef::U64(value) => {
                let value = value.parse::<u64>().map_err(|_| {
                    ProjectionProtocolError::InvalidBatch(
                        "projection key contains an invalid canonical u64".into(),
                    )
                })?;
                i64::try_from(value)
                    .map(RowValue::I64)
                    .unwrap_or(RowValue::U64(value))
            }
            ProjectionValueRef::F64(value) => RowValue::F64(value.parse().map_err(|_| {
                ProjectionProtocolError::InvalidBatch(
                    "projection key contains an invalid canonical f64".into(),
                )
            })?),
            ProjectionValueRef::String(value) | ProjectionValueRef::Enum { variant: value, .. } => {
                RowValue::String(value.to_owned())
            }
            ProjectionValueRef::Null
            | ProjectionValueRef::List(_)
            | ProjectionValueRef::Object(_) => {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection key field `{}` is not a portable scalar",
                    field.name()
                )))
            }
        };
        row.insert(column.column_name.clone(), value);
    }
    crate::table::validate_key(schema, &row)?;
    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::projection_protocol::{
        ProjectionChangeCursor, ProjectionEpoch, ProjectionGeneration, ProjectionInputCursor,
        ProjectionInputFingerprint, ProjectionPartition, ProjectionRecordMetadata,
        ProjectionScopeCodec, ProjectionSource, ProjectorTopologyId, TrustedProjectionInput,
    };
    use crate::read_model::{ReadModelWritePlanBuilder, RelationalReadModel};
    use crate::table::RowPatch;

    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, crate::ReadModel)]
    #[readmodel(table = "executor_todos", primary_key = ["id"])]
    struct TodoView {
        id: String,
        title: String,
    }

    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, crate::ReadModel)]
    #[readmodel(table = "executor_audits", primary_key = ["id"])]
    struct AuditView {
        id: String,
        action: String,
    }

    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, crate::DomainState)]
    #[domain_state(version = 1)]
    struct ExecutorState {
        id: String,
        title: String,
        action: String,
    }

    fn executor_multi_table_program(
    ) -> Result<crate::ProjectionProgram, crate::ProjectionProgramError> {
        use crate::mutation::{
            body_field_binding, compile_projection, MutationAssignment, MutationConflictTarget,
            MutationEventBinding, MutationExpression, MutationField, MutationKeyField,
            MutationKind, MutationOperation, MutationProgram, ProjectionHandler,
        };
        use crate::projection::{
            ProjectionEventSelector, ProjectionPartition, ProjectionTarget, ProjectionValueType,
        };

        let todo_target = ProjectionTarget::try_new("TodoView", "executor_todos")?;
        let audit_target = ProjectionTarget::try_new("AuditView", "executor_audits")?;
        let todo_op = MutationOperation::try_new(
            "upsert-todo",
            0,
            MutationKind::Upsert,
            todo_target,
            vec![MutationKeyField::try_new(
                0,
                "id",
                MutationExpression::input_path(ProjectionValueType::String, ["id"]).map_err(
                    |e| crate::ProjectionProgramError::InvalidOperation {
                        operation: "executor multi".into(),
                        reason: e.to_string(),
                    },
                )?,
            )
            .map_err(|e| crate::ProjectionProgramError::InvalidOperation {
                operation: "executor multi".into(),
                reason: e.to_string(),
            })?],
            vec![
                MutationField::try_new(
                    0,
                    "id",
                    MutationAssignment::set(
                        MutationExpression::input_path(ProjectionValueType::String, ["id"])
                            .map_err(|e| crate::ProjectionProgramError::InvalidOperation {
                                operation: "executor multi".into(),
                                reason: e.to_string(),
                            })?,
                    ),
                )
                .map_err(|e| crate::ProjectionProgramError::InvalidOperation {
                    operation: "executor multi".into(),
                    reason: e.to_string(),
                })?,
                MutationField::try_new(
                    1,
                    "title",
                    MutationAssignment::set(
                        MutationExpression::input_path(ProjectionValueType::String, ["title"])
                            .map_err(|e| crate::ProjectionProgramError::InvalidOperation {
                                operation: "executor multi".into(),
                                reason: e.to_string(),
                            })?,
                    ),
                )
                .map_err(|e| crate::ProjectionProgramError::InvalidOperation {
                    operation: "executor multi".into(),
                    reason: e.to_string(),
                })?,
            ],
            Some(MutationConflictTarget::PrimaryKey),
            Vec::new(),
            Vec::new(),
            None,
        )
        .map_err(|e| crate::ProjectionProgramError::InvalidOperation {
            operation: "executor multi".into(),
            reason: e.to_string(),
        })?;
        let audit_op = MutationOperation::try_new(
            "upsert-audit",
            1,
            MutationKind::Upsert,
            audit_target,
            vec![MutationKeyField::try_new(
                0,
                "id",
                MutationExpression::input_path(ProjectionValueType::String, ["id"]).map_err(
                    |e| crate::ProjectionProgramError::InvalidOperation {
                        operation: "executor multi".into(),
                        reason: e.to_string(),
                    },
                )?,
            )
            .map_err(|e| crate::ProjectionProgramError::InvalidOperation {
                operation: "executor multi".into(),
                reason: e.to_string(),
            })?],
            vec![
                MutationField::try_new(
                    0,
                    "id",
                    MutationAssignment::set(
                        MutationExpression::input_path(ProjectionValueType::String, ["id"])
                            .map_err(|e| crate::ProjectionProgramError::InvalidOperation {
                                operation: "executor multi".into(),
                                reason: e.to_string(),
                            })?,
                    ),
                )
                .map_err(|e| crate::ProjectionProgramError::InvalidOperation {
                    operation: "executor multi".into(),
                    reason: e.to_string(),
                })?,
                MutationField::try_new(
                    1,
                    "action",
                    MutationAssignment::set(
                        MutationExpression::input_path(ProjectionValueType::String, ["action"])
                            .map_err(|e| crate::ProjectionProgramError::InvalidOperation {
                                operation: "executor multi".into(),
                                reason: e.to_string(),
                            })?,
                    ),
                )
                .map_err(|e| crate::ProjectionProgramError::InvalidOperation {
                    operation: "executor multi".into(),
                    reason: e.to_string(),
                })?,
            ],
            Some(MutationConflictTarget::PrimaryKey),
            Vec::new(),
            Vec::new(),
            None,
        )
        .map_err(|e| crate::ProjectionProgramError::InvalidOperation {
            operation: "executor multi".into(),
            reason: e.to_string(),
        })?;
        let mutation_program =
            MutationProgram::try_new("executor_multi_mutation", 1, vec![todo_op, audit_op])
                .map_err(|e| crate::ProjectionProgramError::InvalidOperation {
                    operation: "executor multi".into(),
                    reason: e.to_string(),
                })?;
        let selector =
            ProjectionEventSelector::try_from_descriptor(&crate::DomainEventDescriptor::state::<
                ExecutorState,
            >("executor.changed", 1))?;
        let bindings = vec![
            body_field_binding(["id"], ["id"], ProjectionValueType::String).map_err(|e| {
                crate::ProjectionProgramError::InvalidOperation {
                    operation: "executor multi".into(),
                    reason: e.to_string(),
                }
            })?,
            body_field_binding(["title"], ["title"], ProjectionValueType::String).map_err(|e| {
                crate::ProjectionProgramError::InvalidOperation {
                    operation: "executor multi".into(),
                    reason: e.to_string(),
                }
            })?,
            body_field_binding(["action"], ["action"], ProjectionValueType::String).map_err(
                |e| crate::ProjectionProgramError::InvalidOperation {
                    operation: "executor multi".into(),
                    reason: e.to_string(),
                },
            )?,
        ];
        let binding =
            MutationEventBinding::try_new(selector, bindings, mutation_program).map_err(|e| {
                crate::ProjectionProgramError::InvalidOperation {
                    operation: "executor multi".into(),
                    reason: e.to_string(),
                }
            })?;
        compile_projection(
            "executor-generated-multi-table",
            1,
            ProjectionPartition::Unit,
            [ProjectionHandler::from_binding("changed", binding)],
        )
        .map_err(|e| crate::ProjectionProgramError::InvalidOperation {
            operation: "executor multi".into(),
            reason: e.to_string(),
        })
    }

    fn executor_multi_table_resolve(
        occurrence: &crate::DomainEventOccurrence,
    ) -> Result<crate::ResolvedProjectionPlan, crate::ProjectionProgramError> {
        crate::mutation::resolve_mutation_program(&executor_multi_table_program()?, occurrence)
    }

    fn executor_multi_table_lower(
        plan: &crate::ResolvedProjectionPlan,
    ) -> Result<
        crate::projection::lower::LoweredProjectionPlan,
        crate::projection::lower::ProjectionLoweringError,
    > {
        use crate::projection::lower::{finish_lowering, lower_model_mutation};
        let mut builder = ReadModelWritePlanBuilder::new();
        for mutation in plan.mutations() {
            match mutation.target().model() {
                "TodoView" => lower_model_mutation::<TodoView>(&mut builder, mutation)?,
                "AuditView" => lower_model_mutation::<AuditView>(&mut builder, mutation)?,
                other => {
                    return Err(crate::projection::lower::ProjectionLoweringError::Table(
                        crate::TableStoreError::Metadata(format!("unknown model `{other}`")),
                    ));
                }
            }
        }
        finish_lowering(builder, plan)
    }

    fn executor_multi_table_inventory() -> Result<
        crate::projection::lower::ProjectionOutputInventory,
        crate::projection::lower::ProjectionLoweringError,
    > {
        use crate::projection::lower::{ProjectionOutputInventory, ProjectionOutputModel};
        Ok(ProjectionOutputInventory::new(
            vec![
                ProjectionOutputModel::of::<TodoView>()?,
                ProjectionOutputModel::of::<AuditView>()?,
            ],
            Vec::new(),
        ))
    }

    const GENERATED_MULTI_TABLE: crate::projection::lower::ProjectionDescriptor<
        crate::projection::lower::EventualOnly,
    > = crate::mutation::descriptor_from_factories(
        "executor-generated-multi-table",
        1,
        "executor-generated-v1",
        executor_multi_table_program,
        executor_multi_table_resolve,
        executor_multi_table_lower,
        executor_multi_table_inventory,
    );

    fn topology() -> ProjectorTopologyId {
        ProjectorTopologyId::new(1, "executor-tests", [31; 32]).unwrap()
    }

    fn partition() -> ProjectionPartition {
        ProjectionScopeCodec::new(topology())
            .encode_partition(None)
            .unwrap()
    }

    fn workspace() -> ProjectionWorkspace {
        workspace_with_partition(None)
    }

    fn workspace_with_partition(partition_value: Option<serde_json::Value>) -> ProjectionWorkspace {
        let topology = topology();
        let mut codec = ProjectionScopeCodec::new(topology.clone());
        codec
            .register_model("TodoView", TodoView::schema())
            .unwrap();
        codec
            .register_model("AuditView", AuditView::schema())
            .unwrap();
        let encoded_partition = codec.encode_partition(partition_value.as_ref()).unwrap();
        let cursor = ProjectionInputCursor::new(
            topology,
            encoded_partition,
            ProjectionSource::new("executor-source", b"scope".to_vec()).unwrap(),
            ProjectionEpoch::new("source-v1").unwrap(),
            1,
        )
        .unwrap();
        let input = TrustedProjectionInput::mint(
            cursor,
            ProjectionInputFingerprint::from_canonical_bytes(b"executor-input"),
            "message-1",
            "cause-1",
            ProjectionGeneration::initial(),
            true,
        )
        .unwrap();
        ProjectionWorkspace::new(
            Arc::new(codec),
            partition_value,
            input,
            ProjectionEpoch::new("changes-v1").unwrap(),
        )
        .unwrap()
    }

    fn todo(id: &str, title: &str) -> TodoView {
        TodoView {
            id: id.into(),
            title: title.into(),
        }
    }

    fn todo_key(id: &str) -> RowKey {
        RowKey::new([("id", RowValue::String(id.into()))])
    }

    fn one_mutation(configure: impl FnOnce(&mut ReadModelWritePlanBuilder)) -> TableMutation {
        let mut builder = ReadModelWritePlanBuilder::new();
        configure(&mut builder);
        builder
            .into_write_plan()
            .unwrap()
            .mutations
            .into_iter()
            .next()
            .unwrap()
    }

    fn execution(
        workspace: &ProjectionWorkspace,
        intent: LifecycleIntent,
        mutation: TableMutation,
    ) -> ExecutionMutation {
        let (schema, key) = mutation_schema_key(&mutation);
        ExecutionMutation {
            scope: workspace.record_scope(schema, key).unwrap(),
            intent,
            mutation,
            source_snapshot: None,
        }
    }

    fn metadata(
        scope: ProjectionRecordScope,
        tombstone: bool,
        revision: u64,
    ) -> ProjectionRecordMetadata {
        ProjectionRecordMetadata {
            source_snapshot: None,
            revision: RecordRevision::new(scope, 1, revision).unwrap(),
            tombstone,
            change: ProjectionChangeCursor::new(
                topology(),
                partition(),
                ProjectionEpoch::new("changes-v1").unwrap(),
                revision,
            )
            .unwrap(),
        }
    }

    fn missing(scope: ProjectionRecordScope) -> ProjectionScopedRowSnapshot {
        ProjectionScopedRowSnapshot {
            scope,
            row: None,
            record: None,
        }
    }

    fn live(
        scope: ProjectionRecordScope,
        model: &TodoView,
        revision: u64,
    ) -> ProjectionScopedRowSnapshot {
        ProjectionScopedRowSnapshot {
            row: Some(model.to_row().unwrap()),
            record: Some(metadata(scope.clone(), false, revision)),
            scope,
        }
    }

    fn tombstone(scope: ProjectionRecordScope, revision: u64) -> ProjectionScopedRowSnapshot {
        ProjectionScopedRowSnapshot {
            record: Some(metadata(scope.clone(), true, revision)),
            row: None,
            scope,
        }
    }

    fn generated_occurrence() -> crate::DomainEventOccurrence {
        crate::DomainEventOccurrence::capture(
            crate::DomainEventDescriptor::state::<ExecutorState>("executor.changed", 1),
            crate::DomainEventEnvelope {
                aggregate_type: "executor".into(),
                aggregate_id: "todo-1".into(),
                aggregate_sequence: 1,
                publication_ordinal: 0,
                occurred_at: std::time::UNIX_EPOCH + std::time::Duration::from_secs(1),
                metadata: std::collections::BTreeMap::new(),
            },
            &ExecutorState {
                id: "todo-1".into(),
                title: "generated".into(),
                action: "changed".into(),
            },
        )
        .unwrap()
    }

    #[test]
    fn execution_bounds_accept_exact_limits() {
        assert!(validate_execution_bounds(
            MAX_PROJECTION_EXECUTION_SCOPES,
            MAX_PROJECTION_OPERATIONS_PER_OCCURRENCE
        )
        .is_ok());
    }

    #[test]
    fn execution_scope_bound_rejects_before_a_reader_is_needed() {
        let error = validate_execution_bounds(MAX_PROJECTION_EXECUTION_SCOPES + 1, 0).unwrap_err();
        assert!(error.to_string().contains("4097 scopes"));
    }

    #[test]
    fn portable_mutation_bound_rejects_before_a_reader_is_needed() {
        let error =
            validate_execution_bounds(0, MAX_PROJECTION_OPERATIONS_PER_OCCURRENCE + 1).unwrap_err();
        assert!(error.to_string().contains("129 client-visible"));
    }

    #[test]
    fn lifecycle_create_save_patch_delete_and_recreate_are_exact() {
        let workspace = workspace();
        let model = todo("todo-1", "before");
        let full = || {
            one_mutation(|builder| {
                builder.upsert(&model).unwrap();
            })
        };
        let patch = || {
            one_mutation(|builder| {
                builder
                    .patch::<TodoView>(
                        todo_key("todo-1"),
                        RowPatch::new().set("title", RowValue::String("after".into())),
                    )
                    .unwrap();
            })
        };
        let delete = || {
            one_mutation(|builder| {
                builder.delete::<TodoView>(todo_key("todo-1")).unwrap();
            })
        };
        let scope = workspace
            .record_scope(TodoView::schema(), &todo_key("todo-1"))
            .unwrap();

        let (mutation, expectation, kind) = stage_for_snapshot(
            execution(&workspace, LifecycleIntent::Insert, full()),
            &missing(scope.clone()),
        )
        .unwrap();
        assert!(matches!(
            mutation,
            TableMutation::UpsertRow(TableRowMutation {
                mode: RowWriteMode::Insert,
                expected_version: ExpectedVersion::NotExists,
                ..
            })
        ));
        assert_eq!(expectation, ProjectionRecordExpectation::Missing);
        assert_eq!(kind, ProjectionMutationKind::Upsert);

        let live = live(scope.clone(), &model, 7);
        let expected_revision = live.record.as_ref().unwrap().revision.clone();
        let (mutation, expectation, kind) = stage_for_snapshot(
            execution(&workspace, LifecycleIntent::Upsert, full()),
            &live,
        )
        .unwrap();
        assert!(matches!(
            mutation,
            TableMutation::UpsertRow(TableRowMutation {
                mode: RowWriteMode::Upsert,
                expected_version: ExpectedVersion::Any,
                ..
            })
        ));
        assert_eq!(
            expectation,
            ProjectionRecordExpectation::Exact(expected_revision.clone())
        );
        assert_eq!(kind, ProjectionMutationKind::Upsert);

        let (mutation, expectation, kind) = stage_for_snapshot(
            execution(&workspace, LifecycleIntent::Patch, patch()),
            &live,
        )
        .unwrap();
        assert!(matches!(
            mutation,
            TableMutation::PatchRow(crate::table::PatchTableRowMutation {
                mode: PatchMode::UpdateExisting,
                expected_version: ExpectedVersion::Any,
                ..
            })
        ));
        assert_eq!(
            expectation,
            ProjectionRecordExpectation::Exact(expected_revision.clone())
        );
        assert_eq!(kind, ProjectionMutationKind::Upsert);

        let (mutation, expectation, kind) = stage_for_snapshot(
            execution(&workspace, LifecycleIntent::Delete, delete()),
            &live,
        )
        .unwrap();
        assert!(matches!(
            mutation,
            TableMutation::DeleteRow(crate::table::DeleteTableRowMutation {
                expected_version: ExpectedVersion::Any,
                ..
            })
        ));
        assert_eq!(
            expectation,
            ProjectionRecordExpectation::Exact(expected_revision)
        );
        assert_eq!(kind, ProjectionMutationKind::Delete);

        let tombstone = tombstone(scope, 8);
        let tombstone_revision = tombstone.record.as_ref().unwrap().revision.clone();
        let (mutation, expectation, kind) = stage_for_snapshot(
            execution(&workspace, LifecycleIntent::Recreate, full()),
            &tombstone,
        )
        .unwrap();
        assert!(matches!(
            mutation,
            TableMutation::UpsertRow(TableRowMutation {
                mode: RowWriteMode::Insert,
                expected_version: ExpectedVersion::NotExists,
                ..
            })
        ));
        assert_eq!(
            expectation,
            ProjectionRecordExpectation::Exact(tombstone_revision)
        );
        assert_eq!(kind, ProjectionMutationKind::Recreate);
    }

    #[test]
    fn upsert_patch_creates_a_complete_missing_row_and_patches_a_live_row() {
        let workspace = workspace();
        let model = todo("todo-1", "before");
        let upsert_patch = || {
            one_mutation(|builder| {
                builder
                    .upsert_patch::<TodoView>(
                        todo_key("todo-1"),
                        RowPatch::new().set("title", RowValue::String("after".into())),
                    )
                    .unwrap();
            })
        };
        let scope = workspace
            .record_scope(TodoView::schema(), &todo_key("todo-1"))
            .unwrap();

        let (created, expectation, _) = stage_for_snapshot(
            execution(&workspace, LifecycleIntent::UpsertPatch, upsert_patch()),
            &missing(scope.clone()),
        )
        .unwrap();
        let TableMutation::UpsertRow(created) = created else {
            panic!("missing upsert-patch must become a full-row insert");
        };
        assert_eq!(
            created.values.get("id"),
            Some(&RowValue::String("todo-1".into()))
        );
        assert_eq!(
            created.values.get("title"),
            Some(&RowValue::String("after".into()))
        );
        assert_eq!(created.mode, RowWriteMode::Insert);
        assert_eq!(created.expected_version, ExpectedVersion::NotExists);
        assert_eq!(expectation, ProjectionRecordExpectation::Missing);

        let (patched, expectation, _) = stage_for_snapshot(
            execution(&workspace, LifecycleIntent::UpsertPatch, upsert_patch()),
            &live(scope, &model, 9),
        )
        .unwrap();
        assert!(matches!(
            patched,
            TableMutation::PatchRow(crate::table::PatchTableRowMutation {
                mode: PatchMode::UpdateExisting,
                expected_version: ExpectedVersion::Any,
                ..
            })
        ));
        assert!(matches!(expectation, ProjectionRecordExpectation::Exact(_)));
    }

    #[test]
    fn unsafe_missing_tombstone_and_resurrection_transitions_fail_closed() {
        let workspace = workspace();
        let model = todo("todo-1", "before");
        let scope = workspace
            .record_scope(TodoView::schema(), &todo_key("todo-1"))
            .unwrap();
        let full = || {
            one_mutation(|builder| {
                builder.upsert(&model).unwrap();
            })
        };
        let patch = || {
            one_mutation(|builder| {
                builder
                    .patch::<TodoView>(
                        todo_key("todo-1"),
                        RowPatch::new().set("title", RowValue::String("after".into())),
                    )
                    .unwrap();
            })
        };
        let delete = || {
            one_mutation(|builder| {
                builder.delete::<TodoView>(todo_key("todo-1")).unwrap();
            })
        };

        assert!(matches!(
            stage_for_snapshot(
                execution(&workspace, LifecycleIntent::Patch, patch()),
                &missing(scope.clone())
            ),
            Err(ProjectionProtocolError::RecordMissing { .. })
        ));
        assert!(matches!(
            stage_for_snapshot(
                execution(&workspace, LifecycleIntent::Delete, delete()),
                &missing(scope.clone())
            ),
            Err(ProjectionProtocolError::RecordMissing { .. })
        ));
        let tombstone = tombstone(scope.clone(), 10);
        assert!(matches!(
            stage_for_snapshot(
                execution(&workspace, LifecycleIntent::Upsert, full()),
                &tombstone
            ),
            Err(ProjectionProtocolError::RecordTombstoned { .. })
        ));
        assert!(matches!(
            stage_for_snapshot(
                execution(&workspace, LifecycleIntent::Recreate, full()),
                &missing(scope.clone())
            ),
            Err(ProjectionProtocolError::RecreateRequiresTombstone { .. })
        ));
        assert!(matches!(
            stage_for_snapshot(
                execution(&workspace, LifecycleIntent::Recreate, full()),
                &live(scope, &model, 11)
            ),
            Err(ProjectionProtocolError::RecreateRequiresTombstone { .. })
        ));
    }

    #[test]
    fn snapshot_batch_is_scope_matched_not_positionally_zipped_and_stages_atomically() {
        let workspace = workspace();
        let todo_model = todo("todo-1", "causal");
        let audit_model = AuditView {
            id: "audit-1".into(),
            action: "created".into(),
        };
        let mut builder = ReadModelWritePlanBuilder::new();
        builder.upsert(&todo_model).unwrap();
        builder.upsert(&audit_model).unwrap();
        let physical = builder.into_write_plan().unwrap();
        let physical_order = physical
            .mutations
            .iter()
            .map(|mutation| mutation.table_name().to_owned())
            .collect::<Vec<_>>();
        let prepared = prepare_graph_projection(&workspace, physical, HashMap::new()).unwrap();
        let request_scopes = prepared
            .snapshot_request()
            .requests
            .iter()
            .map(|request| request.scope.clone())
            .collect::<Vec<_>>();
        assert_eq!(request_scopes.len(), 2);

        let reversed = ProjectionExecutionSnapshotBatch {
            snapshots: request_scopes.iter().rev().cloned().map(missing).collect(),
        };
        let mut staged = workspace.clone();
        prepared.stage(&mut staged, reversed).unwrap();
        let batch = staged.into_batch().unwrap();
        assert_eq!(
            batch
                .mutations
                .iter()
                .map(|mutation| mutation.mutation.table_name().to_owned())
                .collect::<Vec<_>>(),
            physical_order,
            "explicit scope matching must retain canonical physical-plan order"
        );
        assert_eq!(batch.observations.len(), 2);

        let mut builder = ReadModelWritePlanBuilder::new();
        builder.upsert(&todo_model).unwrap();
        builder
            .delete::<AuditView>(RowKey::new([("id", RowValue::String("audit-1".into()))]))
            .unwrap();
        let prepared = prepare_graph_projection(
            &workspace,
            builder.into_write_plan().unwrap(),
            HashMap::new(),
        )
        .unwrap();
        let returned = ProjectionExecutionSnapshotBatch {
            snapshots: prepared
                .snapshot_request()
                .requests
                .iter()
                .map(|request| missing(request.scope.clone()))
                .collect(),
        };
        let mut unchanged = workspace.clone();
        let error = prepared.stage(&mut unchanged, returned).unwrap_err();
        assert!(matches!(
            error,
            ProjectionProtocolError::RecordMissing { .. }
        ));
        assert!(
            unchanged.into_batch().unwrap().mutations.is_empty(),
            "one invalid lifecycle must not partially stage earlier mutations"
        );
    }

    #[test]
    fn duplicate_mutation_scope_is_rejected_before_snapshot_io() {
        let workspace = workspace();
        let model = todo("todo-1", "causal");
        let mutation = one_mutation(|builder| {
            builder.upsert(&model).unwrap();
        });
        let error = prepare_graph_projection(
            &workspace,
            TableWritePlan::new(vec![mutation.clone(), mutation]),
            HashMap::new(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("repeats model `TodoView`"));
    }

    #[test]
    fn generated_multi_table_plan_matches_by_exact_model_storage_and_key() {
        let workspace = workspace();
        let lowered = GENERATED_MULTI_TABLE
            .server_executor()
            .unwrap()
            .plan(&generated_occurrence())
            .unwrap();
        let expected_order = lowered
            .write_plan
            .mutations
            .iter()
            .map(|mutation| mutation.table_name().to_owned())
            .collect::<Vec<_>>();
        let prepared = prepare_portable_projection(&workspace, lowered.clone()).unwrap();
        assert_eq!(prepared.snapshot_request().requests.len(), 2);
        let returned = ProjectionExecutionSnapshotBatch {
            snapshots: prepared
                .snapshot_request()
                .requests
                .iter()
                .rev()
                .map(|request| missing(request.scope.clone()))
                .collect(),
        };
        let mut staged = workspace.clone();
        prepared.stage(&mut staged, returned).unwrap();
        let batch = staged.into_batch().unwrap();
        assert_eq!(
            batch
                .mutations
                .iter()
                .map(|mutation| mutation.mutation.table_name().to_owned())
                .collect::<Vec<_>>(),
            expected_order
        );

        let mut divergent = lowered;
        let first = divergent.write_plan.mutations.first_mut().unwrap();
        match first {
            TableMutation::UpsertRow(row) => {
                row.key = RowKey::new([("id", RowValue::String("wrong-key".into()))]);
                row.values
                    .insert("id", RowValue::String("wrong-key".into()));
            }
            _ => panic!("generated fixture uses full-row upserts"),
        }
        let error = prepare_portable_projection(&workspace, divergent).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not have one exact logical key match"),
            "physical rows cannot be positionally paired with unrelated logical mutations"
        );

        let divergent_partition = workspace_with_partition(Some(serde_json::json!("tenant-b")));
        let error = prepare_portable_projection(
            &divergent_partition,
            GENERATED_MULTI_TABLE
                .server_executor()
                .unwrap()
                .plan(&generated_occurrence())
                .unwrap(),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ProjectionProtocolError::ScopeMismatch {
                field: "resolved projection partition"
            }
        ));
    }

    #[test]
    fn mismatched_revision_scope_and_unchanged_plan_cannot_invent_evidence() {
        let workspace = workspace();
        let scope = workspace
            .record_scope(TodoView::schema(), &todo_key("todo-1"))
            .unwrap();
        let wrong_scope = workspace
            .record_scope(TodoView::schema(), &todo_key("todo-2"))
            .unwrap();
        let incoherent = ProjectionScopedRowSnapshot {
            scope,
            row: Some(todo("todo-1", "causal").to_row().unwrap()),
            record: Some(metadata(wrong_scope, false, 12)),
        };
        assert!(matches!(
            validate_snapshot_scope(&incoherent),
            Err(ProjectionProtocolError::ScopeMismatch { .. })
        ));

        let prepared =
            prepare_graph_projection(&workspace, TableWritePlan::default(), HashMap::new())
                .unwrap();
        assert!(!prepared.needs_snapshot_read());
        let mut staged = workspace;
        prepared
            .stage(&mut staged, ProjectionExecutionSnapshotBatch::default())
            .unwrap();
        let batch = staged.into_batch().unwrap();
        assert!(batch.mutations.is_empty());
        assert!(batch.observations.is_empty());
    }
}
