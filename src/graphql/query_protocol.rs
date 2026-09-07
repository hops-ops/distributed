//! Compiler-owned projection topology used to attach safe query/live evidence.
//!
//! This module never derives a projector partition from returned values. A
//! record key may recover its unique durable scope through Task 15 metadata,
//! while resumable index scopes are limited to Unit/Constant projectors whose
//! partition exists even for an empty query result.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_graphql::Value;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
use sqlx::{Encode, Executor, IntoArguments, Type};

use super::compile::{ExtractedQueryEvidence, QueryResponsePathSegment, SqlPlan};
use super::engine::EngineInner;
use super::engine::GraphqlPool;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
use super::execute;
use super::protocol::{
    DistributedIndexRevision, DistributedLiveMetadata, DistributedQuerySnapshot,
    DistributedRecordRevision, ProtocolResponseAccumulator, RequestedLiveResume,
    MAX_LIVE_RESUME_CURSORS,
};
use super::surface::{Surface, SurfaceProjectionOwner, SurfaceRowPolicy};
use crate::projection::placement::ProjectionBindingState;
use crate::projection_protocol::{
    CompiledProjectionTopology, ProjectionChangeCursor, ProjectionChangeRead, ProjectionEpoch,
    ProjectionLiveRecordBatchRequest, ProjectionLiveRecordRequest, ProjectionPartition,
    ProjectionPartitionSnapshot, ProjectionPartitionSpec, ProjectionProtocolError,
    ProjectionScopeCodec, ProjectorTopologyId,
};
#[cfg(any(feature = "sqlite", feature = "postgres"))]
use crate::sqlx_repo::projection_protocol::{
    read_projection_changes_in_executor, read_projection_live_record_batch_in_executor,
    read_projection_partition_snapshot_in_executor, with_projection_read_snapshot,
};
#[cfg(any(feature = "sqlite", feature = "postgres"))]
use crate::sqlx_repo::repo::SqlxRepoBackend;

#[derive(Clone, Debug)]
pub(crate) struct QueryProjectorRuntime {
    pub(crate) name: String,
    pub(crate) codec: Arc<ProjectionScopeCodec>,
    pub(crate) static_partition: Option<ProjectionPartition>,
    pub(crate) change_epoch: Option<ProjectionEpoch>,
    models: BTreeSet<String>,
    dependencies: BTreeSet<String>,
}

impl QueryProjectorRuntime {
    pub(crate) fn supports_resume(&self) -> bool {
        self.static_partition.is_some() && self.change_epoch.is_some()
    }

    /// A partition-wide change cursor is safe to expose only when every model
    /// sharing that projector partition is visible without row filtering on
    /// this exact authorization surface. Otherwise positions, causations, and
    /// tombstones could reveal activity from denied rows or models.
    fn partition_matches_authorization(&self, surface: &Surface) -> bool {
        self.models.iter().all(|model| {
            surface
                .models
                .get(model)
                .is_some_and(|model| matches!(model.row_policy, SurfaceRowPolicy::Unrestricted))
        })
    }
}

/// Full, authorization-independent topology compiled from complete schemas.
///
/// Legacy owners derive their canonical topology here. Modeled owners instead
/// reuse the exact active binding's physical identity and register the
/// authoritative Surface schemas beneath it. Role selection only filters
/// these already-validated projector identities.
#[derive(Clone, Debug, Default)]
pub(crate) struct QueryProtocolRuntime {
    projectors: BTreeMap<String, Arc<QueryProjectorRuntime>>,
    model_owners: BTreeMap<String, Arc<QueryProjectorRuntime>>,
    table_owners: BTreeMap<String, Arc<QueryProjectorRuntime>>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct QueryIndexPlan {
    pub(crate) comparable: bool,
    pub(crate) projectors: Vec<Arc<QueryProjectorRuntime>>,
}

impl QueryProtocolRuntime {
    pub(crate) fn compile(surface: &Surface) -> Result<Self, String> {
        let mut runtime = Self::default();
        for projector in &surface.projectors {
            let projection = if projector.modeled.is_empty() {
                Some(compile_legacy_query_projector(surface, projector)?)
            } else {
                compile_modeled_query_projector(surface, projector)?
            };
            if let Some(projection) = projection {
                runtime.register(surface, projection)?;
            }
        }
        Ok(runtime)
    }

    fn register(
        &mut self,
        surface: &Surface,
        projection: Arc<QueryProjectorRuntime>,
    ) -> Result<(), String> {
        for model in &projection.models {
            if let Some(existing) = self
                .model_owners
                .insert(model.clone(), Arc::clone(&projection))
            {
                return Err(format!(
                    "query protocol model `{model}` has ambiguous owners `{}` and `{}`",
                    existing.name, projection.name
                ));
            }
            let table = surface
                .models
                .get(model)
                .expect("compiled projector model exists")
                .table_name
                .clone();
            if let Some(existing) = self
                .table_owners
                .insert(table.clone(), Arc::clone(&projection))
            {
                return Err(format!(
                    "query protocol table `{table}` has ambiguous owners `{}` and `{}`",
                    existing.name, projection.name
                ));
            }
        }
        if self
            .projectors
            .insert(projection.name.clone(), Arc::clone(&projection))
            .is_some()
        {
            return Err(format!(
                "query protocol projection owner `{}` is registered more than once",
                projection.name
            ));
        }
        Ok(())
    }

    pub(crate) fn visible_model_owner(
        &self,
        role_surface: &Surface,
        model: &str,
    ) -> Option<Arc<QueryProjectorRuntime>> {
        let owner = self.model_owners.get(model)?;
        role_surface
            .projectors
            .iter()
            .any(|projector| projector.name == owner.name)
            .then(|| Arc::clone(owner))
    }

    /// Build one conservative vector over every physical dependency touched by
    /// an exact query plan. Missing, denied, dynamic, epochless, or ambiguous
    /// ownership makes the vector incomparable; no scalar maximum is invented.
    ///
    /// Tables with **zero** surface projectors are skipped rather than failing
    /// the plan. Join targets such as `auth_users` are often populated by
    /// integration handlers without a unit-resume owner; they still participate
    /// in dirty matching via `tables_touched`, but must not force
    /// `live.supported = false` for an otherwise resumable root model (e.g.
    /// `chat_messages` with a nested `author` selection).
    pub(crate) fn index_plan(&self, role_surface: &Surface, tables: &[String]) -> QueryIndexPlan {
        if tables.is_empty() {
            return QueryIndexPlan::default();
        }
        let visible = role_surface
            .projectors
            .iter()
            .map(|projector| projector.name.as_str())
            .collect::<BTreeSet<_>>();
        let mut selected = BTreeMap::<String, Arc<QueryProjectorRuntime>>::new();
        for table in tables {
            let candidates = self
                .projectors
                .values()
                .filter(|projector| {
                    visible.contains(projector.name.as_str())
                        && projector.dependencies.contains(table)
                })
                .cloned()
                .collect::<Vec<_>>();
            match candidates.as_slice() {
                [] => {
                    // Unowned join/lookup table — omit from the resume vector.
                    continue;
                }
                [candidate] => {
                    if !candidate.supports_resume()
                        || !candidate.partition_matches_authorization(role_surface)
                    {
                        return QueryIndexPlan {
                            comparable: false,
                            projectors: Vec::new(),
                        };
                    }
                    selected.insert(candidate.name.clone(), Arc::clone(candidate));
                }
                _ => {
                    // Ambiguous ownership — fail closed.
                    return QueryIndexPlan {
                        comparable: false,
                        projectors: Vec::new(),
                    };
                }
            }
        }
        if selected.is_empty() {
            return QueryIndexPlan {
                comparable: false,
                projectors: Vec::new(),
            };
        }
        QueryIndexPlan {
            comparable: true,
            projectors: selected.into_values().collect(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.projectors.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn topology_for_model(
        &self,
        model: &str,
    ) -> Option<&crate::projection_protocol::ProjectorTopologyId> {
        self.model_owners
            .get(model)
            .map(|owner| owner.codec.topology())
    }

    #[cfg(test)]
    pub(crate) fn model_has_static_partition(&self, model: &str) -> Option<bool> {
        self.model_owners
            .get(model)
            .map(|owner| owner.static_partition.is_some())
    }
}

fn compile_legacy_query_projector(
    surface: &Surface,
    projector: &SurfaceProjectionOwner,
) -> Result<Arc<QueryProjectorRuntime>, String> {
    if projector.partition.is_modeled_only() {
        return Err(format!(
            "query protocol legacy projector `{}` cannot execute a modeled-only partition contract",
            projector.name
        ));
    }
    let schemas = projector
        .models
        .iter()
        .map(|model| {
            surface
                .models
                .get(model)
                .map(|model| &model.schema)
                .ok_or_else(|| {
                    format!(
                        "query protocol projector `{}` references unknown model `{model}`",
                        projector.name
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let compiled = CompiledProjectionTopology::compile(
        &projector.name,
        &projector.facts,
        &projector.models,
        &projector.partition,
        schemas,
    )
    .map_err(|error| {
        format!(
            "query protocol projector `{}` cannot compile: {error}",
            projector.name
        )
    })?;
    let codec = compiled.codec();
    let static_partition = match &projector.partition {
        ProjectionPartitionSpec::Unit => codec.encode_partition(None).map(Some),
        ProjectionPartitionSpec::Constant { value } => {
            codec.encode_partition(Some(value)).map(Some)
        }
        ProjectionPartitionSpec::InputPath { .. } => Ok(None),
        ProjectionPartitionSpec::ModeledExpression { .. }
        | ProjectionPartitionSpec::ModeledInactive => {
            unreachable!("modeled-only partitions were rejected above")
        }
    }
    .map_err(|error| {
        format!(
            "query protocol projector `{}` has invalid static partition: {error}",
            projector.name
        )
    })?;
    let change_epoch = projector
        .change_epoch
        .as_deref()
        .map(ProjectionEpoch::new)
        .transpose()
        .map_err(|error| {
            format!(
                "query protocol projector `{}` has invalid change epoch: {error}",
                projector.name
            )
        })?;
    Ok(Arc::new(QueryProjectorRuntime {
        name: projector.name.clone(),
        codec,
        static_partition,
        change_epoch,
        models: projector.models.iter().cloned().collect(),
        dependencies: projector.dependencies.iter().cloned().collect(),
    }))
}

fn compile_modeled_query_projector(
    surface: &Surface,
    projector: &SurfaceProjectionOwner,
) -> Result<Option<Arc<QueryProjectorRuntime>>, String> {
    let active = projector
        .modeled
        .iter()
        .filter(|modeled| modeled.state() == ProjectionBindingState::Active)
        .collect::<Vec<_>>();
    let Some(first) = active.first() else {
        // Draining registrations remain on the catalog Surface so outstanding
        // work can finish, but they cannot mint new query or live evidence.
        return Ok(None);
    };
    let (first_program, first_binding) = first.raw().ok_or_else(|| {
        format!(
            "query protocol modeled owner `{}` requires the unselected catalog Surface",
            projector.name
        )
    })?;
    let first_physical = first_binding.physical_topology().ok_or_else(|| {
        format!(
            "query protocol modeled owner `{}` binding `{}` has no physical observation topology",
            projector.name,
            first.binding_id()
        )
    })?;
    let topology = ProjectorTopologyId::new(
        first_physical.version(),
        first_physical.name(),
        first_physical.digest(),
    )
    .map_err(|error| {
        format!(
            "query protocol modeled owner `{}` has invalid physical observation topology: {error}",
            projector.name
        )
    })?;
    validate_modeled_partition_binding(projector, first_program, first_binding)?;

    let mut models = BTreeSet::new();
    for modeled in &active {
        let (program, binding) = modeled.raw().ok_or_else(|| {
            format!(
                "query protocol modeled owner `{}` requires the unselected catalog Surface",
                projector.name
            )
        })?;
        let physical = binding.physical_topology().ok_or_else(|| {
            format!(
                "query protocol modeled owner `{}` binding `{}` has no physical observation topology",
                projector.name,
                modeled.binding_id()
            )
        })?;
        validate_modeled_partition_binding(projector, program, binding)?;
        if physical != first_physical {
            return Err(format!(
                "query protocol modeled owner `{}` has incompatible active physical topologies",
                projector.name
            ));
        }
        if modeled.epoch() != first.epoch() {
            return Err(format!(
                "query protocol modeled owner `{}` has incompatible active change epochs `{}` and `{}`",
                projector.name,
                first.epoch().as_str(),
                modeled.epoch().as_str()
            ));
        }
        if binding.partition() != first_binding.partition()
            || program.partition() != first_program.partition()
        {
            return Err(format!(
                "query protocol modeled owner `{}` has incompatible active partition protocols",
                projector.name
            ));
        }
        models.extend(modeled.output_models().iter().cloned());
    }
    if models.is_empty() {
        return Err(format!(
            "query protocol modeled owner `{}` has no active output models",
            projector.name
        ));
    }

    let schemas = models
        .iter()
        .map(|model| {
            surface
                .models
                .get(model)
                .map(|model| (model.model_name.as_str(), &model.schema))
                .ok_or_else(|| {
                    format!(
                        "query protocol modeled owner `{}` references unknown active model `{model}`",
                        projector.name
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let codec = Arc::new(
        ProjectionScopeCodec::with_models(topology, schemas).map_err(|error| {
            format!(
                "query protocol modeled owner `{}` cannot register its authoritative schemas: {error}",
                projector.name
            )
        })?,
    );
    let static_partition = if matches!(
        first_program.partition(),
        crate::projection::ProjectionPartition::Unit
    ) {
        Some(codec.encode_partition(None).map_err(|error| {
            format!(
                "query protocol modeled owner `{}` has invalid unit partition: {error}",
                projector.name
            )
        })?)
    } else {
        None
    };
    let change_epoch = ProjectionEpoch::new(first.epoch().as_str()).map_err(|error| {
        format!(
            "query protocol modeled owner `{}` has invalid active change epoch: {error}",
            projector.name
        )
    })?;
    let dependencies = models
        .iter()
        .map(|model| {
            surface
                .models
                .get(model)
                .expect("modeled schema registration validated the model")
                .table_name
                .clone()
        })
        .collect();

    Ok(Some(Arc::new(QueryProjectorRuntime {
        name: projector.name.clone(),
        codec,
        static_partition,
        change_epoch: Some(change_epoch),
        models,
        dependencies,
    })))
}

fn validate_modeled_partition_binding(
    projector: &SurfaceProjectionOwner,
    program: &crate::projection::ProjectionProgram,
    binding: &crate::projection::placement::ProjectionBinding,
) -> Result<(), String> {
    let expected = serde_json::to_value(program.partition()).map_err(|error| {
        format!(
            "query protocol modeled owner `{}` cannot encode its partition expression: {error}",
            projector.name
        )
    })?;
    if binding.partition().expression() != &expected {
        return Err(format!(
            "query protocol modeled owner `{}` binding `{}` partition differs from its program",
            projector.name,
            binding.id()
        ));
    }
    Ok(())
}

pub(crate) struct ProtocolQueryExecution {
    pub(crate) value: Value,
    pub(crate) snapshot: DistributedQuerySnapshot,
    pub(crate) live: Option<DistributedLiveMetadata>,
}

struct PreparedRecordProbe {
    request: ProjectionLiveRecordRequest,
    paths: Vec<Vec<String>>,
}

struct PreparedQueryEvidence {
    records_complete: bool,
    records: Vec<PreparedRecordProbe>,
    indexes: QueryIndexPlan,
}

const MAX_PROTOCOL_EVIDENCE_ITEMS: usize = 4_096;

struct PreparedLiveChange {
    projection: String,
    change: crate::projection_protocol::ProjectionChange,
}

struct PreparedLiveMetadata {
    metadata: DistributedLiveMetadata,
    changes: Vec<PreparedLiveChange>,
}

/// Execute the physical query and all record/index evidence reads inside one
/// adapter snapshot. The returned GraphQL value has already had every hidden
/// compiler key removed.
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub(crate) async fn execute_query_with_protocol(
    inner: &EngineInner,
    pool: &GraphqlPool,
    primary: bool,
    role_surface: Arc<Surface>,
    accumulator: ProtocolResponseAccumulator,
    plan: &SqlPlan,
    live_resume: Option<RequestedLiveResume>,
) -> Result<ProtocolQueryExecution, String> {
    #[derive(serde::Serialize)]
    struct QueryPlanInstance<'a> {
        domain: &'static str,
        version: u32,
        sql: &'a str,
        binds: &'a [super::compile::BindValue],
        tables: &'a [String],
    }

    // Query and subscription documents retain distinct public operation
    // hashes, but matching compiler plans share one comparable snapshot scope.
    // This prevents a lagging refresh from being treated as authoritative over
    // a newer live frame merely because the transport operation differs.
    accumulator
        .bind_query_snapshot_scope(&QueryPlanInstance {
            domain: "distributed.graphql.query-plan-instance",
            version: 1,
            sql: &plan.sql,
            binds: &plan.binds,
            tables: &plan.tables_touched,
        })
        .map_err(|error| format!("query snapshot scope failed: {error}"))?;

    // The snapshot helper accepts an HRTB closure whose returned future may
    // borrow only its connection. Keep every other input owned by the closure
    // so plan/runtime references cannot escape into that future.
    let _ = primary;
    let plan = plan.clone();
    let runtime = inner.query_protocol.clone();
    let statement_timeout = inner.statement_timeout;
    #[cfg(feature = "gateway-delivery")]
    let gateway_versions = inner.gateway_versions.clone();
    match pool {
        #[cfg(feature = "sqlite")]
        GraphqlPool::Sqlite(pool) => {
            let run = with_projection_read_snapshot(pool, move |connection| {
                let role_surface = Arc::clone(&role_surface);
                let accumulator = accumulator.clone();
                let plan = plan.clone();
                let runtime = runtime.clone();
                let live_resume = live_resume.clone();
                #[cfg(feature = "gateway-delivery")]
                let gateway_versions = gateway_versions.clone();
                Box::pin(async move {
                    #[cfg(feature = "gateway-delivery")]
                    if let Some(store) = &gateway_versions {
                        store.record_result();
                    }
                    let executed = execute::execute_sqlite_in_connection(connection, &plan)
                        .await
                        .map_err(query_execution_error)?;
                    let result = finish_protocol_query::<sqlx::Sqlite>(
                        connection,
                        &runtime,
                        &role_surface,
                        &accumulator,
                        &plan,
                        executed,
                        live_resume,
                    )
                    .await?;
                    #[cfg(feature = "gateway-delivery")]
                    if accumulator.gateway_enabled() {
                        if let Some(store) = &gateway_versions {
                            if store.covers(&plan.tables_touched) {
                                let versions = store
                                    .sqlite(connection, &plan.tables_touched)
                                    .await
                                    .map_err(query_execution_error)?;
                                accumulator
                                    .record_gateway_versions(&versions)
                                    .map_err(query_execution_error)?;
                            }
                        }
                    }
                    Ok(result)
                })
            });
            execute::apply_statement_timeout(statement_timeout, async {
                run.await.map_err(|error| error.to_string())
            })
            .await
        }
        #[cfg(feature = "postgres")]
        GraphqlPool::Postgres(pool) => {
            let run = with_projection_read_snapshot(pool, move |connection| {
                let role_surface = Arc::clone(&role_surface);
                let accumulator = accumulator.clone();
                let plan = plan.clone();
                let runtime = runtime.clone();
                let live_resume = live_resume.clone();
                #[cfg(feature = "gateway-delivery")]
                let gateway_versions = gateway_versions.clone();
                Box::pin(async move {
                    let timeout_ms =
                        i64::try_from(statement_timeout.as_millis()).unwrap_or(i64::MAX);
                    sqlx::query(sqlx::AssertSqlSafe(format!(
                        "SET LOCAL statement_timeout = '{timeout_ms}ms'"
                    )))
                    .execute(&mut *connection)
                    .await
                    .map_err(|error| {
                        query_execution_error(format!("statement_timeout: {error}"))
                    })?;
                    execute::ensure_primary_backend(connection, primary)
                        .await
                        .map_err(query_execution_error)?;
                    #[cfg(feature = "gateway-delivery")]
                    if let Some(store) = &gateway_versions {
                        store.record_result();
                    }
                    let executed = execute::execute_postgres_in_connection(connection, &plan)
                        .await
                        .map_err(query_execution_error)?;
                    let result = finish_protocol_query::<sqlx::Postgres>(
                        connection,
                        &runtime,
                        &role_surface,
                        &accumulator,
                        &plan,
                        executed,
                        live_resume,
                    )
                    .await?;
                    #[cfg(feature = "gateway-delivery")]
                    if accumulator.gateway_enabled() {
                        if let Some(store) = &gateway_versions {
                            if store.covers(&plan.tables_touched) {
                                let versions = store
                                    .postgres(connection, &plan.tables_touched)
                                    .await
                                    .map_err(query_execution_error)?;
                                accumulator
                                    .record_gateway_versions(&versions)
                                    .map_err(query_execution_error)?;
                            }
                        }
                    }
                    Ok(result)
                })
            });
            execute::apply_statement_timeout(statement_timeout, async {
                run.await.map_err(|error| error.to_string())
            })
            .await
        }
        #[allow(unreachable_patterns)]
        _ => Err("no database pool available for GraphQL execution".into()),
    }
}

#[cfg(not(any(feature = "sqlite", feature = "postgres")))]
pub(crate) async fn execute_query_with_protocol(
    _inner: &EngineInner,
    _pool: &GraphqlPool,
    _primary: bool,
    _role_surface: Arc<Surface>,
    _accumulator: ProtocolResponseAccumulator,
    _plan: &SqlPlan,
    _live_resume: Option<RequestedLiveResume>,
) -> Result<ProtocolQueryExecution, String> {
    Err("no database pool available for GraphQL execution".into())
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn query_execution_error(error: String) -> ProjectionProtocolError {
    ProjectionProtocolError::InvalidBatch(format!("GraphQL query snapshot failed: {error}"))
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
async fn finish_protocol_query<DB>(
    connection: &mut DB::Connection,
    runtime: &QueryProtocolRuntime,
    role_surface: &Surface,
    accumulator: &ProtocolResponseAccumulator,
    plan: &SqlPlan,
    executed: execute::ExecutedSql,
    live_resume: Option<RequestedLiveResume>,
) -> Result<ProtocolQueryExecution, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let prepared = prepare_query_evidence(runtime, role_surface, plan, executed.evidence)?;
    // Store evidence batches are deliberately capped at 128 to bound one SQL
    // statement. A query snapshot may carry up to 4,096 records, so read it in
    // bounded chunks inside this same database snapshot and preserve positional
    // alignment for the final wire envelope.
    let mut record_metadata = Vec::with_capacity(prepared.records.len());
    for records in prepared
        .records
        .chunks(crate::projection_protocol::MAX_PROJECTION_EVIDENCE_BATCH_ITEMS)
    {
        let record_batch = ProjectionLiveRecordBatchRequest::new(
            records
                .iter()
                .map(|record| record.request.clone())
                .collect(),
        )?;
        let batch =
            read_projection_live_record_batch_in_executor::<DB>(connection, &record_batch).await?;
        record_metadata.extend(batch.records);
    }

    let mut partitions = Vec::with_capacity(prepared.indexes.projectors.len());
    for projector in &prepared.indexes.projectors {
        let partition = projector
            .static_partition
            .as_ref()
            .expect("comparable index plans retain a static partition");
        let epoch = projector
            .change_epoch
            .as_ref()
            .expect("comparable index plans retain a change epoch");
        partitions.push(
            read_projection_partition_snapshot_in_executor::<DB>(
                connection,
                projector.codec.topology(),
                partition,
                epoch,
            )
            .await?,
        );
    }

    let (live, live_changes) = match live_resume {
        Some(requested) => {
            let prepared_live = wire_live_metadata::<DB>(
                connection,
                accumulator,
                &prepared,
                &partitions,
                requested,
            )
            .await?;
            (Some(prepared_live.metadata), prepared_live.changes)
        }
        None => (None, Vec::new()),
    };
    let snapshot = wire_query_snapshot(
        accumulator,
        prepared,
        record_metadata,
        partitions,
        live_changes,
    )?;
    Ok(ProtocolQueryExecution {
        value: executed.value,
        snapshot,
        live,
    })
}

fn prepare_query_evidence(
    runtime: &QueryProtocolRuntime,
    role_surface: &Surface,
    plan: &SqlPlan,
    extracted: ExtractedQueryEvidence,
) -> Result<PreparedQueryEvidence, ProjectionProtocolError> {
    let mut records_complete = extracted.complete;
    let mut records = Vec::<PreparedRecordProbe>::new();
    let mut identities = BTreeMap::<(String, String, [u8; 32], Vec<u8>), usize>::new();

    for record in extracted.records {
        let Some(owner) = runtime.visible_model_owner(role_surface, &record.model) else {
            records_complete = false;
            continue;
        };
        let key = owner
            .codec
            .row_key_from_json_columns(&record.model, &record.key_columns)
            .map_err(|error| {
                ProjectionProtocolError::InvalidBatch(format!(
                    "query evidence key for model `{}` is invalid: {error}",
                    record.model
                ))
            })?;
        let request = ProjectionLiveRecordRequest::new(&owner.codec, &record.model, key)?;
        let identity = (
            owner.name.clone(),
            record.model,
            request.canonical_key_hash,
            request.canonical_key_bytes.clone(),
        );
        let path = record
            .response_path
            .into_iter()
            .map(|segment| match segment {
                QueryResponsePathSegment::Field(field) => field,
                QueryResponsePathSegment::Index(index) => index.to_string(),
            })
            .collect::<Vec<_>>();
        match identities.get(&identity).copied() {
            Some(index) => records[index].paths.push(path),
            None => {
                identities.insert(identity, records.len());
                records.push(PreparedRecordProbe {
                    request,
                    paths: vec![path],
                });
            }
        }
    }

    let mut indexes = runtime.index_plan(role_surface, &plan.tables_touched);
    if !query_index_budget_allows(indexes.projectors.len()) {
        indexes.projectors.clear();
        indexes.comparable = false;
    }
    Ok(PreparedQueryEvidence {
        records_complete,
        records,
        indexes,
    })
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
async fn wire_live_metadata<DB>(
    connection: &mut DB::Connection,
    accumulator: &ProtocolResponseAccumulator,
    prepared: &PreparedQueryEvidence,
    partitions: &[ProjectionPartitionSnapshot],
    requested: RequestedLiveResume,
) -> Result<PreparedLiveMetadata, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    if partitions.len() != prepared.indexes.projectors.len() {
        return Err(ProjectionProtocolError::InvalidBatch(
            "query live adapter response length mismatch".into(),
        ));
    }
    if !prepared.indexes.comparable
        || prepared.indexes.projectors.is_empty()
        || !live_resume_cursor_budget_allows(prepared.indexes.projectors.len())
    {
        return Ok(PreparedLiveMetadata {
            metadata: DistributedLiveMetadata {
                supported: false,
                reset: true,
                cursors: Vec::new(),
            },
            changes: Vec::new(),
        });
    }

    let snapshot_scope = accumulator
        .query_snapshot_scope()
        .map_err(|error| ProjectionProtocolError::InvalidBatch(error.to_string()))?;
    let mut current = Vec::with_capacity(prepared.indexes.projectors.len());
    for (projector, partition_snapshot) in prepared.indexes.projectors.iter().zip(partitions) {
        let partition = projector
            .static_partition
            .as_ref()
            .expect("comparable live plans retain a static partition");
        let epoch = projector
            .change_epoch
            .as_ref()
            .expect("comparable live plans retain a change epoch");
        let position = partition_snapshot
            .head
            .as_ref()
            .map(ProjectionChangeCursor::position)
            .unwrap_or(0);
        current.push(
            accumulator
                .issue_live_resume_position(
                    &projector.name,
                    &snapshot_scope,
                    projector.codec.topology(),
                    partition,
                    epoch,
                    position,
                )
                .map_err(|error| ProjectionProtocolError::InvalidBatch(error.to_string()))?,
        );
    }

    let mut reset = matches!(requested, RequestedLiveResume::Invalid);
    let mut replayed_changes = Vec::new();
    if let RequestedLiveResume::Cursors(supplied) = requested {
        let mut supplied_by_projection = BTreeMap::new();
        for cursor in supplied {
            let projection = cursor.projection.clone();
            if supplied_by_projection.insert(projection, cursor).is_some() {
                reset = true;
            }
        }
        if supplied_by_projection.len() != prepared.indexes.projectors.len() {
            reset = true;
        }

        // Validate the complete cursor set before any change-log read. One
        // invalid, missing, future, or duplicate cursor resets the whole
        // logical snapshot; reading other partitions first would spend an
        // attacker-amplifiable amount of work on evidence we must discard.
        let mut validated = Vec::with_capacity(prepared.indexes.projectors.len());
        for (projector, partition_snapshot) in prepared.indexes.projectors.iter().zip(partitions) {
            let Some(supplied) = supplied_by_projection.remove(&projector.name) else {
                reset = true;
                continue;
            };
            let Ok(position) = supplied.position.parse::<u64>() else {
                reset = true;
                continue;
            };
            if position.to_string() != supplied.position {
                reset = true;
                continue;
            }
            let partition = projector
                .static_partition
                .as_ref()
                .expect("comparable live plans retain a static partition");
            let epoch = projector
                .change_epoch
                .as_ref()
                .expect("comparable live plans retain a change epoch");
            if accumulator
                .verify_live_resume_position(
                    &supplied,
                    &snapshot_scope,
                    &projector.name,
                    projector.codec.topology(),
                    partition,
                    epoch,
                    position,
                )
                .is_err()
            {
                reset = true;
                continue;
            }

            let after = if position == 0 {
                None
            } else {
                match ProjectionChangeCursor::new(
                    projector.codec.topology().clone(),
                    partition.clone(),
                    epoch.clone(),
                    position,
                ) {
                    Ok(cursor) => Some(cursor),
                    Err(_) => {
                        reset = true;
                        continue;
                    }
                }
            };
            let current_position = partition_snapshot
                .head
                .as_ref()
                .map(ProjectionChangeCursor::position)
                .unwrap_or(0);
            if position > current_position {
                reset = true;
                continue;
            }
            validated.push((
                projector,
                partition_snapshot,
                after,
                position,
                current_position,
            ));
        }
        if !supplied_by_projection.is_empty() {
            reset = true;
        }

        let current_record_entries = prepared
            .records
            .iter()
            .map(|record| record.paths.len())
            .sum::<usize>();
        if !reset {
            for (projector, partition_snapshot, after, position, current_position) in validated {
                if position == current_position {
                    continue;
                }
                let remaining = MAX_PROTOCOL_EVIDENCE_ITEMS
                    .saturating_sub(current_record_entries.saturating_add(replayed_changes.len()));
                if remaining == 0 {
                    reset = true;
                    break;
                }
                let partition = projector
                    .static_partition
                    .as_ref()
                    .expect("comparable live plans retain a static partition");
                let read_limit = remaining.saturating_add(1);
                let read = read_projection_changes_in_executor::<DB>(
                    connection,
                    projector.codec.topology(),
                    partition,
                    after.as_ref(),
                    read_limit,
                )
                .await?;
                match read {
                    ProjectionChangeRead::ResetRequired { .. } => {
                        reset = true;
                        break;
                    }
                    ProjectionChangeRead::Changes { head, changes, .. } => {
                        let last_position = changes
                            .last()
                            .map(|change| change.cursor.position())
                            .unwrap_or(position);
                        if head != partition_snapshot.head
                            || changes.len() > remaining
                            || last_position != current_position
                        {
                            reset = true;
                            break;
                        }
                        replayed_changes.extend(changes.into_iter().map(|change| {
                            PreparedLiveChange {
                                projection: projector.name.clone(),
                                change,
                            }
                        }));
                    }
                }
            }
        }
    }

    if reset {
        // A reset frame is a fresh authoritative snapshot. Never attach a
        // partial or untrusted change-log suffix to data that the client must
        // merge only after discarding its previous scoped state.
        replayed_changes.clear();
    }

    Ok(PreparedLiveMetadata {
        metadata: DistributedLiveMetadata {
            supported: true,
            reset,
            cursors: current,
        },
        changes: replayed_changes,
    })
}

fn live_resume_cursor_budget_allows(projector_count: usize) -> bool {
    projector_count <= MAX_LIVE_RESUME_CURSORS
}

fn query_index_budget_allows(index_count: usize) -> bool {
    index_count <= MAX_PROTOCOL_EVIDENCE_ITEMS
}

fn wire_query_snapshot(
    accumulator: &ProtocolResponseAccumulator,
    mut prepared: PreparedQueryEvidence,
    metadata: Vec<Option<crate::projection_protocol::ProjectionRecordMetadata>>,
    partitions: Vec<ProjectionPartitionSnapshot>,
    live_changes: Vec<PreparedLiveChange>,
) -> Result<DistributedQuerySnapshot, ProjectionProtocolError> {
    if metadata.len() != prepared.records.len()
        || partitions.len() != prepared.indexes.projectors.len()
    {
        return Err(ProjectionProtocolError::InvalidBatch(
            "query evidence adapter response length mismatch".into(),
        ));
    }
    let snapshot_scope = accumulator
        .query_snapshot_scope()
        .map_err(|error| ProjectionProtocolError::InvalidBatch(error.to_string()))?;
    let mut records = Vec::new();
    let mut current_record_clocks = BTreeMap::<String, (u64, u64)>::new();
    for (probe, metadata) in prepared.records.drain(..).zip(metadata) {
        let Some(metadata) = metadata else {
            prepared.records_complete = false;
            continue;
        };
        let scope_token = accumulator
            .issue_record_scope(metadata.revision.scope())
            .map_err(|error| ProjectionProtocolError::InvalidBatch(error.to_string()))?;
        current_record_clocks.insert(
            scope_token.as_str().to_string(),
            (
                metadata.revision.incarnation(),
                metadata.revision.revision(),
            ),
        );
        for path in probe.paths {
            if records.len() == MAX_PROTOCOL_EVIDENCE_ITEMS {
                prepared.records_complete = false;
                break;
            }
            records.push(DistributedRecordRevision {
                path: Some(path),
                model: metadata.revision.scope().model().to_string(),
                scope_token: scope_token.clone(),
                incarnation: metadata.revision.incarnation().to_string(),
                revision: metadata.revision.revision().to_string(),
                tombstone: metadata.tombstone,
            });
        }
    }

    let mut indexes = Vec::new();
    let mut indexes_comparable = prepared.indexes.comparable;
    let issue_index_resumes = live_resume_cursor_budget_allows(prepared.indexes.projectors.len());
    for (projector, partition_snapshot) in prepared.indexes.projectors.into_iter().zip(partitions) {
        if !query_index_budget_allows(indexes.len().saturating_add(1)) {
            indexes_comparable = false;
            break;
        }
        let partition = projector
            .static_partition
            .as_ref()
            .expect("comparable index plans retain a static partition");
        let epoch = projector
            .change_epoch
            .as_ref()
            .expect("comparable index plans retain a change epoch");
        let position = partition_snapshot
            .head
            .as_ref()
            .map(|cursor| cursor.position())
            .unwrap_or(0);
        let scope_token = accumulator
            .issue_index_scope_parts(
                &snapshot_scope,
                projector.codec.topology(),
                partition,
                epoch,
            )
            .map_err(|error| ProjectionProtocolError::InvalidBatch(error.to_string()))?;
        let resume = if issue_index_resumes {
            Some(
                accumulator
                    .issue_live_resume_position(
                        &projector.name,
                        &snapshot_scope,
                        projector.codec.topology(),
                        partition,
                        epoch,
                        position,
                    )
                    .map_err(|error| ProjectionProtocolError::InvalidBatch(error.to_string()))?,
            )
        } else {
            None
        };
        indexes.push(DistributedIndexRevision {
            projection: projector.name.clone(),
            scope_token,
            position: position.to_string(),
            resume,
        });
    }

    let mut observations = Vec::new();
    let mut observation_tokens = BTreeSet::new();
    let mut live_record_fences = BTreeMap::<String, ((u64, u64), DistributedRecordRevision)>::new();
    for live in live_changes {
        let change = live.change;
        match change.kind {
            crate::projection_protocol::ProjectionChangeKind::RecordUpsert
            | crate::projection_protocol::ProjectionChangeKind::RecordDelete
            | crate::projection_protocol::ProjectionChangeKind::RecordRecreate => {
                let scope = change.scope.as_ref().ok_or_else(|| {
                    ProjectionProtocolError::InvalidBatch(
                        "live record change omitted its canonical scope".into(),
                    )
                })?;
                let revision = change.revision.as_ref().ok_or_else(|| {
                    ProjectionProtocolError::InvalidBatch(
                        "live record change omitted its revision".into(),
                    )
                })?;
                let record_scope_token = accumulator
                    .issue_record_scope(scope)
                    .map_err(|error| ProjectionProtocolError::InvalidBatch(error.to_string()))?;
                let clock = (revision.incarnation(), revision.revision());
                let wire_record = DistributedRecordRevision {
                    path: None,
                    model: scope.model().to_string(),
                    scope_token: record_scope_token,
                    incarnation: revision.incarnation().to_string(),
                    revision: revision.revision().to_string(),
                    tombstone: change.kind
                        == crate::projection_protocol::ProjectionChangeKind::RecordDelete,
                };
                let scope_key = wire_record.scope_token.as_str().to_string();
                match live_record_fences.get(&scope_key) {
                    Some((existing, record)) if *existing > clock => {}
                    Some((existing, record)) if *existing == clock => {
                        if record.tombstone != wire_record.tombstone {
                            return Err(ProjectionProtocolError::InvalidBatch(
                                "one live record clock carried conflicting tombstone state".into(),
                            ));
                        }
                    }
                    _ => {
                        live_record_fences.insert(scope_key, (clock, wire_record));
                    }
                }
                let observation = super::protocol::DistributedProjectionObservation {
                    causation_id: change.causation_id.clone(),
                    projection: live.projection.clone(),
                    model: scope.model().to_string(),
                    scope_token: accumulator
                        .issue_projection_obligation_scope(
                            &change.causation_id,
                            &live.projection,
                            scope.model(),
                            crate::projection_protocol::ProjectionObservationKind::Record,
                            scope,
                        )
                        .map_err(|error| {
                            ProjectionProtocolError::InvalidBatch(error.to_string())
                        })?,
                };
                if observation_tokens.insert(observation.scope_token.as_str().to_string()) {
                    observations.push(observation);
                }
            }
            crate::projection_protocol::ProjectionChangeKind::Observation => {
                let scope = change.scope.as_ref().ok_or_else(|| {
                    ProjectionProtocolError::InvalidBatch(
                        "live projection observation omitted its canonical scope".into(),
                    )
                })?;
                let kind = change.observation_kind.ok_or_else(|| {
                    ProjectionProtocolError::InvalidBatch(
                        "live projection observation omitted its kind".into(),
                    )
                })?;
                let observation = super::protocol::DistributedProjectionObservation {
                    causation_id: change.causation_id.clone(),
                    projection: live.projection.clone(),
                    model: scope.model().to_string(),
                    scope_token: accumulator
                        .issue_projection_obligation_scope(
                            &change.causation_id,
                            &live.projection,
                            scope.model(),
                            kind,
                            scope,
                        )
                        .map_err(|error| {
                            ProjectionProtocolError::InvalidBatch(error.to_string())
                        })?,
                };
                if observation_tokens.insert(observation.scope_token.as_str().to_string()) {
                    observations.push(observation);
                }
            }
            crate::projection_protocol::ProjectionChangeKind::Checkpoint
            | crate::projection_protocol::ProjectionChangeKind::Failure => {}
        }
    }

    for (scope, (clock, record)) in live_record_fences {
        match current_record_clocks.get(&scope) {
            Some(current) if *current == clock && !record.tombstone => continue,
            Some(current) if *current >= clock => {
                return Err(ProjectionProtocolError::InvalidBatch(
                    "live record suffix conflicts with current query-row evidence".into(),
                ));
            }
            Some(_) => {
                return Err(ProjectionProtocolError::InvalidBatch(
                    "current query-row evidence is older than its live suffix".into(),
                ));
            }
            None => {}
        }
        if records.len() == MAX_PROTOCOL_EVIDENCE_ITEMS {
            return Err(ProjectionProtocolError::InvalidBatch(
                "live record evidence exceeded the bounded response budget".into(),
            ));
        }
        records.push(record);
    }

    Ok(DistributedQuerySnapshot {
        scope_token: snapshot_scope,
        records_complete: prepared.records_complete,
        indexes_comparable,
        records,
        indexes,
        observations,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_and_index_resume_budget_accepts_64_and_rejects_65() {
        assert!(live_resume_cursor_budget_allows(MAX_LIVE_RESUME_CURSORS));
        assert!(!live_resume_cursor_budget_allows(
            MAX_LIVE_RESUME_CURSORS + 1
        ));
    }

    #[test]
    fn query_index_budget_accepts_4096_and_rejects_4097() {
        assert!(query_index_budget_allows(MAX_PROTOCOL_EVIDENCE_ITEMS));
        assert!(!query_index_budget_allows(MAX_PROTOCOL_EVIDENCE_ITEMS + 1));
    }
}
