use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use super::constants::{PARTITION_ENCODING_DOMAIN, RECORD_KEY_ENCODING_DOMAIN};
use super::key::{
    encode_json, encode_typed_key_value, key_column, row_value_from_graphql_json,
    typed_value_from_json, typed_value_from_row, validate_model_schema, validate_registration_name,
    CanonicalEncoder, TypedKeyValue,
};
use super::ProjectionScopeCodecError;
use crate::projection_protocol::{
    PartitionId, ProjectionRecordScope, ProjectorTopologyId, RecordKey,
    ResolvedProjectionObligation, MAX_PROJECTION_PARTITION_BYTES, MAX_PROJECTION_RECORD_KEY_BYTES,
};
use crate::table::{RowKey, TableColumn, TableSchema};

/// A topology-bound registry for canonical projection partitions and keys.
///
/// Registration is deliberately explicit: the projector's declared model name
/// must exactly match the registered schema's model name. Obligation keys use
/// Rust/GraphQL `field_name`s, while projector [`RowKey`] values use storage
/// `column_name`s; both are lowered in the schema's primary-key column order.
#[derive(Clone, Debug)]
pub(crate) struct ProjectionScopeCodec {
    topology: ProjectorTopologyId,
    models: BTreeMap<String, Arc<TableSchema>>,
}

impl ProjectionScopeCodec {
    pub(crate) fn new(topology: ProjectorTopologyId) -> Self {
        Self {
            topology,
            models: BTreeMap::new(),
        }
    }

    pub(crate) fn with_models<'a>(
        topology: ProjectorTopologyId,
        models: impl IntoIterator<Item = (&'a str, &'a TableSchema)>,
    ) -> Result<Self, ProjectionScopeCodecError> {
        let mut codec = Self::new(topology);
        for (model, schema) in models {
            codec.register_model(model, schema)?;
        }
        Ok(codec)
    }

    pub(crate) fn topology(&self) -> &ProjectorTopologyId {
        &self.topology
    }

    /// Register one projection model under its compiler-declared model name.
    ///
    /// The codec owns an immutable clone so runtime-loaded manifests and
    /// generated static schemas have identical lifetime and mutation
    /// semantics. A caller may discard or mutate its original clone after
    /// registration without changing the compiled topology.
    pub(crate) fn register_model(
        &mut self,
        declared_model: &str,
        schema: &TableSchema,
    ) -> Result<&mut Self, ProjectionScopeCodecError> {
        validate_registration_name(declared_model)?;
        if declared_model != schema.model_name {
            return Err(ProjectionScopeCodecError::ModelRegistrationMismatch {
                declared: declared_model.to_string(),
                schema: schema.model_name.clone(),
            });
        }
        if self.models.contains_key(declared_model) {
            return Err(ProjectionScopeCodecError::DuplicateModelRegistration {
                model: declared_model.to_string(),
            });
        }
        validate_model_schema(schema)?;
        self.models
            .insert(declared_model.to_string(), Arc::new(schema.clone()));
        Ok(self)
    }

    /// Encode the declaration's optional partition.
    ///
    /// `None` is a canonical unit scope. `Some(Value::Null)` is an explicit
    /// JSON null and therefore has a distinct byte representation and digest.
    pub(crate) fn encode_partition(
        &self,
        partition: Option<&serde_json::Value>,
    ) -> Result<PartitionId, ProjectionScopeCodecError> {
        let mut encoder = CanonicalEncoder::new(
            "projection partition",
            PARTITION_ENCODING_DOMAIN,
            MAX_PROJECTION_PARTITION_BYTES,
        )?;
        match partition {
            None => encoder.push_tag(0)?,
            Some(partition) => {
                encoder.push_tag(1)?;
                encode_json(&mut encoder, partition)?;
            }
        }
        PartitionId::new(encoder.finish()).map_err(Into::into)
    }

    /// Lower a command-ledger obligation into its exact durable record scope.
    #[allow(dead_code)]
    pub(crate) fn encode_obligation_scope(
        &self,
        obligation: &ResolvedProjectionObligation,
    ) -> Result<ProjectionRecordScope, ProjectionScopeCodecError> {
        let computed = self.encode_resolved_obligation_scope(
            &obligation.projector,
            &obligation.model,
            &obligation.key,
            obligation.partition.as_ref(),
        )?;
        if computed != obligation.scope {
            return Err(ProjectionScopeCodecError::StoredScopeMismatch {
                projector: obligation.projector.clone(),
                model: obligation.model.clone(),
            });
        }
        Ok(computed)
    }

    pub(crate) fn encode_resolved_obligation_scope(
        &self,
        projector: &str,
        model: &str,
        key: &RecordKey,
        partition_value: Option<&serde_json::Value>,
    ) -> Result<ProjectionRecordScope, ProjectionScopeCodecError> {
        self.validate_projector(projector)?;
        let schema = self.model(model)?;
        let partition = self.encode_partition(partition_value)?;

        let mut fields = BTreeMap::new();
        for field in &key.fields {
            if fields.insert(field.field.as_str(), &field.value).is_some() {
                return Err(ProjectionScopeCodecError::DuplicateKeyField {
                    model: model.to_string(),
                    field: field.field.clone(),
                });
            }
        }

        let primary_key_fields = schema
            .primary_key
            .columns
            .iter()
            .map(|column_name| {
                key_column(schema, column_name)
                    .expect("registered projection schemas retain their validated key columns")
                    .field_name
                    .as_str()
            })
            .collect::<BTreeSet<_>>();
        if let Some(extra) = fields
            .keys()
            .find(|field| !primary_key_fields.contains(**field))
        {
            return Err(ProjectionScopeCodecError::ExtraKeyField {
                model: model.to_string(),
                field: (*extra).to_string(),
            });
        }

        let canonical_key_bytes = self.encode_key(schema, |column| {
            fields
                .get(column.field_name.as_str())
                .copied()
                .ok_or_else(|| ProjectionScopeCodecError::MissingKeyField {
                    model: schema.model_name.clone(),
                    field: column.field_name.clone(),
                })
                .and_then(|value| typed_value_from_json(schema, column, value))
        })?;

        ProjectionRecordScope::new(
            self.topology.clone(),
            partition,
            schema.model_name.clone(),
            canonical_key_bytes,
        )
        .map_err(Into::into)
    }

    /// Lower a projector-side row key into the same durable record scope used
    /// by command obligations.
    pub(crate) fn encode_row_scope(
        &self,
        projector: &str,
        model: &str,
        partition: Option<&serde_json::Value>,
        key: &RowKey,
    ) -> Result<ProjectionRecordScope, ProjectionScopeCodecError> {
        self.validate_projector(projector)?;
        let partition = self.encode_partition(partition)?;
        self.encode_row_scope_in_partition(model, partition, key)
    }

    /// Lower a row key after the framework has already resolved the exact
    /// canonical projector partition.
    ///
    /// Query snapshots use this seam so the physical row key and durable
    /// record scope are derived by the same registered codec. Callers cannot
    /// pair arbitrary row values with independently supplied scope bytes.
    pub(crate) fn encode_row_scope_in_partition(
        &self,
        model: &str,
        partition: PartitionId,
        key: &RowKey,
    ) -> Result<ProjectionRecordScope, ProjectionScopeCodecError> {
        let schema = self.model(model)?;
        let canonical_key_bytes = self.encode_row_key(schema, key)?;

        ProjectionRecordScope::new(
            self.topology.clone(),
            partition,
            schema.model_name.clone(),
            canonical_key_bytes,
        )
        .map_err(Into::into)
    }

    /// Encode only the typed primary-key identity, without choosing a
    /// projector partition.
    ///
    /// Physical query rows do not carry their hidden causal partition. Query
    /// evidence uses these canonical bytes to find the one live record across
    /// partitions, then returns that record's exact stored scope.
    pub(crate) fn encode_unpartitioned_row_key(
        &self,
        model: &str,
        key: &RowKey,
    ) -> Result<Vec<u8>, ProjectionScopeCodecError> {
        let schema = self.model(model)?;
        self.encode_row_key(schema, key)
    }

    /// Decode complete primary-key columns from GraphQL/JSON without passing
    /// through JavaScript numeric coercion or a second schema interpretation.
    ///
    /// Integer keys accept either an exact JSON integer or the canonical
    /// decimal string used by GraphQL `BigInt`. Byte keys require canonical
    /// standard-base64. Returned [`RowKey`] values use physical column names,
    /// ready for [`Self::encode_unpartitioned_row_key`].
    pub(crate) fn row_key_from_json_columns(
        &self,
        model: &str,
        values: &BTreeMap<String, serde_json::Value>,
    ) -> Result<RowKey, ProjectionScopeCodecError> {
        let schema = self.model(model)?;
        let primary_key_columns = schema
            .primary_key
            .columns
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        if let Some(extra) = values
            .keys()
            .find(|column| !primary_key_columns.contains(column.as_str()))
        {
            return Err(ProjectionScopeCodecError::ExtraKeyColumn {
                model: schema.model_name.clone(),
                column: extra.clone(),
            });
        }

        let mut key = RowKey::default();
        for column_name in &schema.primary_key.columns {
            let column = key_column(schema, column_name)
                .expect("registered projection schemas retain their validated key columns");
            let value = values.get(column_name).ok_or_else(|| {
                ProjectionScopeCodecError::MissingKeyColumn {
                    model: schema.model_name.clone(),
                    column: column_name.clone(),
                }
            })?;
            key.insert(
                column_name,
                row_value_from_graphql_json(schema, column, value)?,
            );
        }
        Ok(key)
    }

    fn encode_row_key(
        &self,
        schema: &TableSchema,
        key: &RowKey,
    ) -> Result<Vec<u8>, ProjectionScopeCodecError> {
        let primary_key_columns = schema
            .primary_key
            .columns
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        if let Some((extra, _)) = key
            .iter()
            .find(|(column, _)| !primary_key_columns.contains(*column))
        {
            return Err(ProjectionScopeCodecError::ExtraKeyColumn {
                model: schema.model_name.clone(),
                column: extra.to_string(),
            });
        }

        self.encode_key(schema, |column| {
            key.get(&column.column_name)
                .ok_or_else(|| ProjectionScopeCodecError::MissingKeyColumn {
                    model: schema.model_name.clone(),
                    column: column.column_name.clone(),
                })
                .and_then(|value| typed_value_from_row(schema, column, value))
        })
    }

    pub(crate) fn registered_schema(
        &self,
        model: &str,
    ) -> Result<&TableSchema, ProjectionScopeCodecError> {
        self.model(model)
    }

    pub(crate) fn registered_schema_owned(
        &self,
        model: &str,
    ) -> Result<Arc<TableSchema>, ProjectionScopeCodecError> {
        self.models
            .get(model)
            .cloned()
            .ok_or_else(|| ProjectionScopeCodecError::UnknownModel {
                projector: self.topology.name().to_string(),
                model: model.to_string(),
            })
    }

    fn validate_projector(&self, projector: &str) -> Result<(), ProjectionScopeCodecError> {
        if projector == self.topology.name() {
            Ok(())
        } else {
            Err(ProjectionScopeCodecError::ProjectorMismatch {
                expected: self.topology.name().to_string(),
                actual: projector.to_string(),
            })
        }
    }

    fn model(&self, model: &str) -> Result<&TableSchema, ProjectionScopeCodecError> {
        self.models.get(model).map(AsRef::as_ref).ok_or_else(|| {
            ProjectionScopeCodecError::UnknownModel {
                projector: self.topology.name().to_string(),
                model: model.to_string(),
            }
        })
    }

    fn encode_key(
        &self,
        schema: &TableSchema,
        mut value_for: impl FnMut(&TableColumn) -> Result<TypedKeyValue, ProjectionScopeCodecError>,
    ) -> Result<Vec<u8>, ProjectionScopeCodecError> {
        let mut encoder = CanonicalEncoder::new(
            "projection record key",
            RECORD_KEY_ENCODING_DOMAIN,
            MAX_PROJECTION_RECORD_KEY_BYTES,
        )?;
        encoder.push_len(schema.primary_key.columns.len())?;
        for column_name in &schema.primary_key.columns {
            let column = key_column(schema, column_name)
                .expect("registered projection schemas retain their validated key columns");
            encoder.push_bytes(column.column_name.as_bytes())?;
            encode_typed_key_value(&mut encoder, value_for(column)?)?;
        }
        Ok(encoder.finish())
    }
}
