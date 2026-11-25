use super::types::{AuthorizationTransition, ProjectionDeltaVisibility, ProjectionMutationSource};
use super::{DeltaKeyField, DeltaValue, ProjectionDeltaError, ProjectionDeltaPartition};
use crate::{
    ResolvedProjectionKey, ResolvedProjectionMutation, ResolvedProjectionPartition,
    ResolvedProjectionPartitionRef, ResolvedProjectionRelationshipEffect,
};

/// The exact authority scope that an authenticated partition token must bind.
#[derive(Clone, Copy)]
pub(crate) struct ProjectionPartitionAuthority<'a> {
    pub surface: &'a super::ProjectionDeltaSurfaceIdentity,
    pub authorization_generation: &'a str,
    pub principal_scope: &'a crate::command_ledger::PrincipalPartitionId,
    pub cache_scope: &'a super::types::ProjectionDeltaCacheScopeToken,
}

/// Framework-owned authenticated encoder for logical projection partitions.
///
/// Implementations must produce opaque, integrity-protected tokens and bind
/// them to both fields of [`ProjectionPartitionAuthority`]. The protocol layer
/// implements this with its deployment-keyed token codec; raw partition bytes
/// and unkeyed digests are never valid wire identities.
pub(crate) trait ProjectionPartitionScopeEncoder {
    fn encode(
        &self,
        authority: ProjectionPartitionAuthority<'_>,
        partition: &ResolvedProjectionPartition,
    ) -> Result<Option<ProjectionDeltaPartition>, ProjectionDeltaError>;
}

/// An authorized normalized model identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AuthorizedModel {
    pub wire_model: String,
    /// Exact authorized non-key replacement mask for complete rows.
    pub replacement_fields: Vec<String>,
}

/// An authorized logical-to-wire field mapping.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AuthorizedField {
    pub wire_field: String,
}

/// An authorized, encoded normalized record identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AuthorizedRecordKey {
    pub wire_model: String,
    /// Complete key in declared primary-key order.
    pub fields: Vec<DeltaKeyField>,
}

/// An explicit authorized relationship identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AuthorizedRelationship {
    pub wire_relationship: String,
    pub source_wire_model: String,
    pub target_wire_model: String,
}

/// Authorization boundary between logical projection provenance and client
/// physical/wire identities.
pub(crate) trait ProjectionAuthorization {
    fn partition(
        &self,
        logical_partition: &ResolvedProjectionPartition,
    ) -> Result<Option<ProjectionDeltaPartition>, ProjectionDeltaError>;

    fn model(&self, logical_model: &str) -> Option<AuthorizedModel>;

    fn field(&self, logical_model: &str, logical_field: &str) -> Option<AuthorizedField>;

    fn record_key(
        &self,
        logical_model: &str,
        logical_key: &ResolvedProjectionKey,
    ) -> Result<Option<AuthorizedRecordKey>, ProjectionDeltaError>;

    fn relationship(
        &self,
        source_logical_model: &str,
        logical_relationship: &str,
        target_logical_model: &str,
    ) -> Option<AuthorizedRelationship>;

    fn record_transition(
        &self,
        source: ProjectionMutationSource,
        mutation: &ResolvedProjectionMutation,
    ) -> Result<AuthorizationTransition, ProjectionDeltaError>;

    fn relationship_transition(
        &self,
        source: ProjectionMutationSource,
        effect: &ResolvedProjectionRelationshipEffect,
    ) -> Result<AuthorizationTransition, ProjectionDeltaError>;
}

/// Runtime/compiler evaluator for authorization transitions.
///
/// Implementations may consume trusted before/after policy evidence. The
/// selected-Surface mapper is deliberately separate so such evidence can
/// never rename models, fields, keys, or relationships.
pub(crate) trait ProjectionVisibilityEvaluator {
    fn record_transition(
        &self,
        source: ProjectionMutationSource,
        mutation: &ResolvedProjectionMutation,
    ) -> Result<AuthorizationTransition, ProjectionDeltaError>;

    fn relationship_transition(
        &self,
        source: ProjectionMutationSource,
        effect: &ResolvedProjectionRelationshipEffect,
    ) -> Result<AuthorizationTransition, ProjectionDeltaError>;
}

/// Concrete fail-closed evaluator for the selected Surface's row policies.
///
/// Unrestricted models are safe for exact local consequences. Predicate and
/// server-only policies require trusted runtime before/after evidence, so this
/// evaluator reports them as unknown and lowering emits only recovery.
// Only the graphql runtime constructs it; a test build without that feature
// compiles this module for the lowering tests alone.
#[cfg_attr(not(feature = "graphql"), allow(dead_code))]
pub(crate) struct SelectedSurfacePolicyVisibility<'a> {
    surface: &'a crate::graphql::surface::Surface,
}

#[cfg_attr(not(feature = "graphql"), allow(dead_code))]
impl<'a> SelectedSurfacePolicyVisibility<'a> {
    pub(crate) fn try_new(
        surface: &'a crate::graphql::surface::Surface,
    ) -> Result<Self, ProjectionDeltaError> {
        if matches!(
            surface.selection,
            crate::graphql::surface::SurfaceSelection::Catalog
        ) {
            return Err(ProjectionDeltaError::AuthorizationMapping);
        }
        Ok(Self { surface })
    }

    fn model_visibility(&self, model: &str) -> ProjectionDeltaVisibility {
        match self
            .surface
            .models
            .get(model)
            .map(|model| &model.row_policy)
        {
            Some(crate::graphql::surface::SurfaceRowPolicy::Unrestricted) => {
                ProjectionDeltaVisibility::Authorized
            }
            Some(
                crate::graphql::surface::SurfaceRowPolicy::Predicate(_)
                | crate::graphql::surface::SurfaceRowPolicy::ServerOnly(_),
            ) => ProjectionDeltaVisibility::Unknown,
            None => ProjectionDeltaVisibility::Denied,
        }
    }
}

impl ProjectionVisibilityEvaluator for SelectedSurfacePolicyVisibility<'_> {
    fn record_transition(
        &self,
        _source: ProjectionMutationSource,
        mutation: &ResolvedProjectionMutation,
    ) -> Result<AuthorizationTransition, ProjectionDeltaError> {
        let visibility = self.model_visibility(mutation.target().model());
        Ok(AuthorizationTransition {
            before: visibility,
            after: visibility,
        })
    }

    fn relationship_transition(
        &self,
        _source: ProjectionMutationSource,
        effect: &ResolvedProjectionRelationshipEffect,
    ) -> Result<AuthorizationTransition, ProjectionDeltaError> {
        let relationship = effect.relationship();
        let source = self.model_visibility(relationship.source_model());
        let target = self.model_visibility(relationship.target_model());
        let visibility = if source == ProjectionDeltaVisibility::Authorized
            && target == ProjectionDeltaVisibility::Authorized
        {
            ProjectionDeltaVisibility::Authorized
        } else if source == ProjectionDeltaVisibility::Denied
            || target == ProjectionDeltaVisibility::Denied
        {
            ProjectionDeltaVisibility::Denied
        } else {
            ProjectionDeltaVisibility::Unknown
        };
        Ok(AuthorizationTransition {
            before: visibility,
            after: visibility,
        })
    }
}

/// Concrete logical-to-wire mapper backed by one selected Surface.
pub(crate) struct SelectedSurfaceAuthorization<'a, V, E> {
    surface: &'a crate::graphql::surface::Surface,
    visibility: &'a V,
    partition_encoder: &'a E,
    partition_surface: super::ProjectionDeltaSurfaceIdentity,
    authorization_generation: String,
    principal_scope: crate::command_ledger::PrincipalPartitionId,
    cache_scope: super::types::ProjectionDeltaCacheScopeToken,
}

impl<'a, V, E> SelectedSurfaceAuthorization<'a, V, E>
where
    V: ProjectionVisibilityEvaluator,
    E: ProjectionPartitionScopeEncoder,
{
    pub(crate) fn try_new(
        surface: &'a crate::graphql::surface::Surface,
        visibility: &'a V,
        partition_encoder: &'a E,
        partition_surface: super::ProjectionDeltaSurfaceIdentity,
        authorization_generation: impl Into<String>,
        principal_scope: crate::command_ledger::PrincipalPartitionId,
        cache_scope: super::types::ProjectionDeltaCacheScopeToken,
    ) -> Result<Self, ProjectionDeltaError> {
        if matches!(
            surface.selection,
            crate::graphql::surface::SurfaceSelection::Catalog
        ) {
            return Err(ProjectionDeltaError::AuthorizationMapping);
        }
        Ok(Self {
            surface,
            visibility,
            partition_encoder,
            partition_surface,
            authorization_generation: authorization_generation.into(),
            principal_scope,
            cache_scope,
        })
    }

    fn selected_model(
        &self,
        logical_model: &str,
    ) -> Option<&crate::graphql::surface::SurfaceModel> {
        self.surface.models.get(logical_model)
    }

    fn normalized_model(
        &self,
        logical_model: &str,
    ) -> Option<&crate::graphql::surface::SurfaceModel> {
        self.selected_model(logical_model)
            .filter(|model| crate::graphql::surface::model_has_client_normalized_identity(model))
    }

    fn selected_field(&self, logical_model: &str, logical_field: &str) -> Option<String> {
        let model = self.normalized_model(logical_model)?;
        let column = model
            .schema
            .columns
            .iter()
            .find(|column| !column.skipped && column.field_name == logical_field)?;
        model
            .columns
            .iter()
            .any(|selected| selected.name == column.column_name)
            .then(|| column.column_name.clone())
    }
}

impl<V, E> ProjectionAuthorization for SelectedSurfaceAuthorization<'_, V, E>
where
    V: ProjectionVisibilityEvaluator,
    E: ProjectionPartitionScopeEncoder,
{
    fn partition(
        &self,
        logical_partition: &ResolvedProjectionPartition,
    ) -> Result<Option<ProjectionDeltaPartition>, ProjectionDeltaError> {
        match logical_partition.as_ref() {
            ResolvedProjectionPartitionRef::Unit => Ok(Some(ProjectionDeltaPartition::Unit)),
            ResolvedProjectionPartitionRef::Value(_) => self.partition_encoder.encode(
                ProjectionPartitionAuthority {
                    surface: &self.partition_surface,
                    authorization_generation: &self.authorization_generation,
                    principal_scope: &self.principal_scope,
                    cache_scope: &self.cache_scope,
                },
                logical_partition,
            ),
        }
    }

    fn model(&self, logical_model: &str) -> Option<AuthorizedModel> {
        let model = self.selected_model(logical_model)?;
        let mut replacement_fields = model
            .columns
            .iter()
            .filter(|field| !model.primary_key.contains(&field.name))
            .map(|field| field.name.clone())
            .collect::<Vec<_>>();
        replacement_fields.sort();
        Some(AuthorizedModel {
            wire_model: model.model_name.clone(),
            replacement_fields,
        })
    }

    fn field(&self, logical_model: &str, logical_field: &str) -> Option<AuthorizedField> {
        self.selected_field(logical_model, logical_field)
            .map(|wire_field| AuthorizedField { wire_field })
    }

    fn record_key(
        &self,
        logical_model: &str,
        logical_key: &ResolvedProjectionKey,
    ) -> Result<Option<AuthorizedRecordKey>, ProjectionDeltaError> {
        let Some(model) = self.normalized_model(logical_model) else {
            return Ok(None);
        };
        let mut logical_by_wire = std::collections::BTreeMap::new();
        for field in logical_key.fields() {
            let Some(wire_field) = self.selected_field(logical_model, field.name()) else {
                return Ok(None);
            };
            logical_by_wire.insert(wire_field, field);
        }
        let mut fields = Vec::with_capacity(model.primary_key.len());
        for (ordinal, primary) in model.primary_key.iter().enumerate() {
            let Some(field) = logical_by_wire.remove(primary) else {
                return Ok(None);
            };
            fields.push(DeltaKeyField {
                ordinal: ordinal as u32,
                field: primary.clone(),
                value: DeltaValue::try_from_projection_ref(field.value().as_ref())?,
            });
        }
        if !logical_by_wire.is_empty() {
            return Ok(None);
        }
        Ok(Some(AuthorizedRecordKey {
            wire_model: model.model_name.clone(),
            fields,
        }))
    }

    fn relationship(
        &self,
        source_logical_model: &str,
        logical_relationship: &str,
        target_logical_model: &str,
    ) -> Option<AuthorizedRelationship> {
        let source = self.normalized_model(source_logical_model)?;
        let target = self.surface.models.get(target_logical_model)?;
        source
            .relationships
            .iter()
            .find(|relationship| {
                relationship.name == logical_relationship
                    && relationship.target_model == target.model_name
            })
            .map(|relationship| AuthorizedRelationship {
                wire_relationship: relationship.name.clone(),
                source_wire_model: source.model_name.clone(),
                target_wire_model: target.model_name.clone(),
            })
    }

    fn record_transition(
        &self,
        source: ProjectionMutationSource,
        mutation: &ResolvedProjectionMutation,
    ) -> Result<AuthorizationTransition, ProjectionDeltaError> {
        self.visibility.record_transition(source, mutation)
    }

    fn relationship_transition(
        &self,
        source: ProjectionMutationSource,
        effect: &ResolvedProjectionRelationshipEffect,
    ) -> Result<AuthorizationTransition, ProjectionDeltaError> {
        self.visibility.relationship_transition(source, effect)
    }
}
