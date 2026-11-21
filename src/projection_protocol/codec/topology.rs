use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use sha2::{Digest, Sha256};

use super::constants::{COMPILED_TOPOLOGY_DOMAIN, COMPILED_TOPOLOGY_VERSION, SCOPE_CODEC_VERSION};
use super::{ProjectionPartitionSpec, ProjectionScopeCodec};
use crate::projection_protocol::{
    ProjectionModelOwnership, ProjectionProtocolError, ProjectorTopologyId,
};
use crate::table::TableSchema;

/// One compiler-owned projector identity, codec, and complete physical
/// ownership inventory.
///
/// GraphQL direct projection binding and the asynchronous projector runtime
/// both derive their protocol identity through [`compile_projection_topology`].
/// This product additionally retains the full typed schema registry needed by
/// a running asynchronous projector. Application code never receives the
/// codec or the authority to mint protocol scopes.
#[derive(Clone, Debug)]
pub(crate) struct CompiledProjectionTopology {
    topology: ProjectorTopologyId,
    codec: Arc<ProjectionScopeCodec>,
    ownership: Vec<ProjectionModelOwnership>,
    partition: ProjectionPartitionSpec,
}

impl CompiledProjectionTopology {
    pub(crate) fn compile<'a>(
        name: &str,
        facts: &[String],
        declared_models: &[String],
        partition: &ProjectionPartitionSpec,
        schemas: impl IntoIterator<Item = &'a TableSchema>,
    ) -> Result<Self, ProjectionProtocolError> {
        let schemas = schemas.into_iter().collect::<Vec<_>>();
        let (topology, ownership) = compile_projection_topology(
            name,
            facts,
            declared_models,
            partition,
            schemas.iter().copied(),
        )?;
        let codec = ProjectionScopeCodec::with_models(
            topology.clone(),
            schemas
                .iter()
                .map(|schema| (schema.model_name.as_str(), *schema)),
        )
        .map_err(|error| {
            ProjectionProtocolError::InvalidBatch(format!(
                "invalid compiled projection scope codec: {error}"
            ))
        })?;
        Ok(Self {
            topology,
            codec: Arc::new(codec),
            ownership,
            partition: partition.clone(),
        })
    }

    /// Rehydrate one generated modeled executor from its catalog-pinned
    /// physical topology and exact output schemas.
    pub(crate) fn from_modeled_binding<'a>(
        topology: ProjectorTopologyId,
        outputs: impl IntoIterator<Item = (&'a str, &'a str, &'a TableSchema)>,
    ) -> Result<Self, ProjectionProtocolError> {
        let outputs = outputs.into_iter().collect::<Vec<_>>();
        let codec = ProjectionScopeCodec::with_models(
            topology.clone(),
            outputs.iter().map(|(model, _, schema)| (*model, *schema)),
        )
        .map_err(|error| {
            ProjectionProtocolError::InvalidBatch(format!(
                "invalid modeled projection scope codec: {error}"
            ))
        })?;
        let mut ownership = outputs
            .iter()
            .map(|(model, storage, _)| {
                ProjectionModelOwnership::new((*model).to_owned(), (*storage).to_owned())
            })
            .collect::<Result<Vec<_>, _>>()?;
        ownership.sort_by(|left, right| left.model.cmp(&right.model));
        if ownership.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ProjectionProtocolError::InvalidBatch(
                "modeled projection repeats output ownership".into(),
            ));
        }
        Ok(Self {
            topology,
            codec: Arc::new(codec),
            ownership,
            // Portable modeled executors resolve their partition from the
            // actual occurrence plan, not from raw transport JSON.
            partition: ProjectionPartitionSpec::unit(),
        })
    }

    pub(crate) fn topology(&self) -> &ProjectorTopologyId {
        &self.topology
    }

    pub(crate) fn codec(&self) -> Arc<ProjectionScopeCodec> {
        Arc::clone(&self.codec)
    }

    pub(crate) fn ownership(&self) -> &[ProjectionModelOwnership] {
        &self.ownership
    }

    pub(crate) fn partition(&self) -> &ProjectionPartitionSpec {
        &self.partition
    }
}

/// Compile the exact protocol identity and model/table ownership for a
/// projector declaration.
///
/// The digest includes the accepted fact inventory (empty for a direct-only
/// owner), a fixed version of the canonical partition/key codec, and each
/// complete registered table schema. It therefore changes whenever a model's
/// physical table, field/column mapping, primary-key scope, or other schema
/// contract changes. Callers may supply schemas in any order; the compiler
/// sorts facts and models and rejects duplicates.
pub(crate) fn compile_projection_topology<'a>(
    name: &str,
    facts: &[String],
    declared_models: &[String],
    partition: &ProjectionPartitionSpec,
    schemas: impl IntoIterator<Item = &'a TableSchema>,
) -> Result<(ProjectorTopologyId, Vec<ProjectionModelOwnership>), ProjectionProtocolError> {
    partition.validate()?;
    if partition.is_modeled_only() {
        return Err(ProjectionProtocolError::InvalidBatch(
            "modeled projection partition contracts require an exact catalog physical topology and cannot be compiled by the legacy projector path"
                .into(),
        ));
    }
    let mut facts = facts.to_vec();
    facts.sort();
    if facts.iter().any(|fact| fact.trim().is_empty()) {
        return Err(ProjectionProtocolError::InvalidBatch(format!(
            "projector `{name}` contains an empty accepted fact"
        )));
    }
    if facts.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(ProjectionProtocolError::InvalidBatch(format!(
            "projector `{name}` repeats an accepted fact"
        )));
    }

    if declared_models.is_empty() {
        return Err(ProjectionProtocolError::InvalidBatch(format!(
            "projector `{name}` must declare at least one output model"
        )));
    }
    let mut declared_models = declared_models.to_vec();
    declared_models.sort();
    if declared_models.iter().any(|model| model.trim().is_empty()) {
        return Err(ProjectionProtocolError::InvalidBatch(format!(
            "projector `{name}` contains an empty output model"
        )));
    }
    if declared_models.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(ProjectionProtocolError::InvalidBatch(format!(
            "projector `{name}` repeats an output model"
        )));
    }

    let mut schemas_by_model = BTreeMap::new();
    for schema in schemas {
        schema.validate()?;
        if !schema.kind.is_read_model() {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projector `{name}` model `{}` is not a read model",
                schema.model_name
            )));
        }
        if schemas_by_model
            .insert(schema.model_name.clone(), schema)
            .is_some()
        {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projector `{name}` repeats schema `{}`",
                schema.model_name
            )));
        }
    }
    let registered_models = schemas_by_model.keys().cloned().collect::<Vec<_>>();
    if registered_models != declared_models {
        return Err(ProjectionProtocolError::InvalidBatch(format!(
            "projector `{name}` declared models {:?} but registered typed schemas {:?}",
            declared_models, registered_models
        )));
    }

    let mut ownership = Vec::with_capacity(schemas_by_model.len());
    let mut compiled_models = Vec::with_capacity(schemas_by_model.len());
    let mut physical_tables = BTreeSet::new();
    for (model, schema) in schemas_by_model {
        if !physical_tables.insert(schema.table_name.as_str()) {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projector `{name}` assigns more than one model to physical table `{}`",
                schema.table_name
            )));
        }
        ownership.push(ProjectionModelOwnership::new(
            model.clone(),
            schema.table_name.clone(),
        )?);
        compiled_models.push(serde_json::json!({
            "model": model,
            "table": schema.table_name,
            "schema": schema,
        }));
    }

    let canonical = serde_json::json!({
        "topology_version": COMPILED_TOPOLOGY_VERSION,
        "scope_codec_version": SCOPE_CODEC_VERSION,
        "name": name,
        "partition": partition,
        "facts": facts,
        "models": compiled_models,
    });
    let mut digest = Sha256::new();
    digest.update(COMPILED_TOPOLOGY_DOMAIN);
    digest.update(
        serde_json::to_vec(&canonical)
            .expect("compiled projection topology contains only serializable schema metadata"),
    );
    let topology =
        ProjectorTopologyId::new(COMPILED_TOPOLOGY_VERSION, name, digest.finalize().into())?;
    Ok((topology, ownership))
}
