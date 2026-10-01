//! Deterministic projection compatibility catalog and exact active bindings.
//!
//! Catalog membership is static deployment inventory. It never proves that an
//! executor is reachable. [`ActiveProjectionBindings`] is the separate
//! service-construction view used for liveness and causal eligibility.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::projection_protocol::canonical_projection_topology_bytes;
pub use crate::projection_protocol::ProjectionEpoch;

use super::placement::{
    ProjectionBinding, ProjectionBindingId, ProjectionBindingState, ProjectionEventSchema,
    ProjectionExecutionClass, ProjectionExecutorRoute, ProjectionPlacement,
    ProjectionTopologyError, PROJECTION_ACTIVE_BINDINGS_WIRE_VERSION,
    PROJECTION_CATALOG_WIRE_VERSION,
};
use super::ProjectionProgramId;

/// One deployment-wide deterministic inventory of projection bindings.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionCatalog {
    wire_version: u16,
    bindings: Vec<ProjectionBinding>,
}

impl ProjectionCatalog {
    /// Validate and canonicalize deployment projection inventory.
    ///
    /// Registration order is discarded. The catalog rejects divergent
    /// event/model/storage/relationship schemas before a service can use it.
    ///
    /// # Errors
    ///
    /// Returns a typed error for duplicate identities or incompatible schemas.
    pub fn try_new(mut bindings: Vec<ProjectionBinding>) -> Result<Self, ProjectionCatalogError> {
        for binding in &bindings {
            binding.validate()?;
        }
        bindings.sort_by_key(ProjectionBinding::id);
        if let Some(pair) = bindings
            .windows(2)
            .find(|pair| pair[0].id() == pair[1].id())
        {
            return Err(ProjectionCatalogError::DuplicateBinding {
                binding: pair[0].id(),
            });
        }
        validate_event_schemas(&bindings)?;
        validate_output_schemas(&bindings)?;
        validate_relationship_schemas(&bindings)?;
        Ok(Self {
            wire_version: PROJECTION_CATALOG_WIRE_VERSION,
            bindings,
        })
    }

    /// Decode exact canonical catalog JSON.
    ///
    /// # Errors
    ///
    /// Rejects malformed, noncanonical, unsupported, or invalid catalogs.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, ProjectionCatalogError> {
        let decoded: Self = serde_json::from_slice(bytes)
            .map_err(|error| ProjectionCatalogError::Canonical(error.to_string()))?;
        if decoded.wire_version != PROJECTION_CATALOG_WIRE_VERSION {
            return Err(ProjectionCatalogError::UnsupportedVersion {
                field: "projection catalog",
                expected: PROJECTION_CATALOG_WIRE_VERSION,
                actual: decoded.wire_version,
            });
        }
        let canonical = Self::try_new(decoded.bindings)?;
        if canonical.canonical_bytes()? != bytes {
            return Err(ProjectionCatalogError::NonCanonical {
                field: "projection catalog",
            });
        }
        Ok(canonical)
    }

    /// Encode deterministic versioned catalog JSON.
    ///
    /// # Errors
    ///
    /// Returns a typed error if canonical serialization fails.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ProjectionCatalogError> {
        canonical_projection_topology_bytes(self)
            .map_err(|error| ProjectionCatalogError::Canonical(error.to_string()))
    }

    /// Return bindings in canonical identity order.
    pub fn bindings(&self) -> &[ProjectionBinding] {
        &self.bindings
    }

    /// Find one exact binding identity.
    pub fn binding(&self, id: ProjectionBindingId) -> Option<&ProjectionBinding> {
        self.bindings
            .binary_search_by_key(&id, ProjectionBinding::id)
            .ok()
            .map(|index| &self.bindings[index])
    }

    /// Validate an exact live view, optionally against the previously active
    /// deployment.
    ///
    /// # Errors
    ///
    /// Fails service construction for unknown identities, missing routes,
    /// duplicate writers, direct/eventual overlap, or same-epoch takeover.
    pub fn activate(
        &self,
        activations: Vec<ProjectionBindingActivation>,
        previous: Option<(&ProjectionCatalog, &ActiveProjectionBindings)>,
    ) -> Result<ActiveProjectionBindings, ProjectionCatalogError> {
        ActiveProjectionBindings::try_new(self, activations, previous)
    }
}

/// One exact live or draining executor registration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionBindingActivation {
    binding_id: ProjectionBindingId,
    #[serde(with = "program_id_serde")]
    program_id: ProjectionProgramId,
    epoch: ProjectionEpoch,
    state: ProjectionBindingState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    route: Option<ProjectionExecutorRoute>,
}

impl ProjectionBindingActivation {
    /// Construct one exact activation candidate.
    ///
    /// `None` is accepted as input so service construction can report a typed
    /// missing-route error before any executor starts.
    pub fn new(
        binding_id: ProjectionBindingId,
        program_id: ProjectionProgramId,
        epoch: ProjectionEpoch,
        state: ProjectionBindingState,
        route: Option<ProjectionExecutorRoute>,
    ) -> Self {
        Self {
            binding_id,
            program_id,
            epoch,
            state,
            route,
        }
    }

    /// Return the exact binding identity.
    pub fn binding_id(&self) -> ProjectionBindingId {
        self.binding_id
    }

    /// Return the exact semantic program identity.
    pub fn program_id(&self) -> ProjectionProgramId {
        self.program_id
    }

    /// Return the physical projection incarnation.
    pub fn epoch(&self) -> &ProjectionEpoch {
        &self.epoch
    }

    /// Return active or draining lifecycle.
    pub fn state(&self) -> ProjectionBindingState {
        self.state
    }

    /// Return the current logical executor route.
    pub fn route(&self) -> Option<&ProjectionExecutorRoute> {
        self.route.as_ref()
    }
}

/// Exact service-construction view of currently executable bindings.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveProjectionBindings {
    wire_version: u16,
    bindings: Vec<ProjectionBindingActivation>,
}

impl ActiveProjectionBindings {
    fn try_new(
        catalog: &ProjectionCatalog,
        mut activations: Vec<ProjectionBindingActivation>,
        previous: Option<(&ProjectionCatalog, &Self)>,
    ) -> Result<Self, ProjectionCatalogError> {
        activations.sort_by_key(ProjectionBindingActivation::binding_id);
        for pair in activations.windows(2) {
            if pair[0].binding_id == pair[1].binding_id {
                return Err(ProjectionCatalogError::DuplicateActivation {
                    binding: pair[0].binding_id,
                });
            }
        }
        for activation in &activations {
            validate_activation(catalog, activation)?;
        }
        validate_primary_binding_uniqueness(catalog, &activations)?;
        validate_authoritative_writers(catalog, &activations)?;
        if let Some((previous_catalog, previous_bindings)) = previous {
            validate_epoch_takeover(
                catalog,
                &activations,
                previous_catalog,
                &previous_bindings.bindings,
            )?;
        }
        Ok(Self {
            wire_version: PROJECTION_ACTIVE_BINDINGS_WIRE_VERSION,
            bindings: activations,
        })
    }

    /// Decode and validate an exact canonical active-binding view.
    ///
    /// Historical same-epoch takeover is validated at service construction by
    /// [`ProjectionCatalog::activate`], where the previous catalog is present.
    ///
    /// # Errors
    ///
    /// Rejects malformed, noncanonical, unsupported, or incompatible views.
    pub fn from_canonical_bytes(
        catalog: &ProjectionCatalog,
        bytes: &[u8],
    ) -> Result<Self, ProjectionCatalogError> {
        let decoded: Self = serde_json::from_slice(bytes)
            .map_err(|error| ProjectionCatalogError::Canonical(error.to_string()))?;
        if decoded.wire_version != PROJECTION_ACTIVE_BINDINGS_WIRE_VERSION {
            return Err(ProjectionCatalogError::UnsupportedVersion {
                field: "active projection bindings",
                expected: PROJECTION_ACTIVE_BINDINGS_WIRE_VERSION,
                actual: decoded.wire_version,
            });
        }
        let canonical = Self::try_new(catalog, decoded.bindings, None)?;
        if canonical.canonical_bytes()? != bytes {
            return Err(ProjectionCatalogError::NonCanonical {
                field: "active projection bindings",
            });
        }
        Ok(canonical)
    }

    /// Encode deterministic active-binding JSON.
    ///
    /// # Errors
    ///
    /// Returns a typed error if canonical serialization fails.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ProjectionCatalogError> {
        canonical_projection_topology_bytes(self)
            .map_err(|error| ProjectionCatalogError::Canonical(error.to_string()))
    }

    /// Return exact active and draining registrations in identity order.
    pub fn bindings(&self) -> &[ProjectionBindingActivation] {
        &self.bindings
    }

    /// Return whether an exact compatible executor is live.
    ///
    /// A draining executor remains live for already committed work.
    pub fn is_live(
        &self,
        catalog: &ProjectionCatalog,
        binding_id: ProjectionBindingId,
        program_id: ProjectionProgramId,
        epoch: &ProjectionEpoch,
    ) -> bool {
        self.exact(catalog, binding_id, program_id, epoch).is_some()
    }

    /// Return whether an exact binding may mint ordinary UI causal
    /// obligations.
    ///
    /// Direct, background, draining, missing, or digest-incompatible bindings
    /// are never eligible.
    pub fn is_causally_eligible(
        &self,
        catalog: &ProjectionCatalog,
        binding_id: ProjectionBindingId,
        program_id: ProjectionProgramId,
        epoch: &ProjectionEpoch,
    ) -> bool {
        let Some((binding, activation)) = self.exact(catalog, binding_id, program_id, epoch) else {
            return false;
        };
        activation.state == ProjectionBindingState::Active
            && binding.placement() == ProjectionPlacement::Eventual
            && binding.execution_class() == ProjectionExecutionClass::Causal
    }

    /// Materialize every independent compatibility pin for one exact live
    /// registration.
    pub fn compatibility_pins(
        &self,
        catalog: &ProjectionCatalog,
        binding_id: ProjectionBindingId,
    ) -> Option<ProjectionCompatibilityPins> {
        let activation = self
            .bindings
            .binary_search_by_key(&binding_id, ProjectionBindingActivation::binding_id)
            .ok()
            .map(|index| &self.bindings[index])?;
        let binding = catalog.binding(binding_id)?;
        if activation.program_id != binding.program_id() {
            return None;
        }
        Some(ProjectionCompatibilityPins {
            events: binding.events().to_vec(),
            program_id: binding.program_id(),
            binding_id,
            program_ir_version: binding.program_ir_version(),
            operation_semantics_version: binding.operation_semantics_version(),
            epoch: activation.epoch.clone(),
        })
    }

    fn exact<'bindings, 'catalog>(
        &'bindings self,
        catalog: &'catalog ProjectionCatalog,
        binding_id: ProjectionBindingId,
        program_id: ProjectionProgramId,
        epoch: &ProjectionEpoch,
    ) -> Option<(
        &'catalog ProjectionBinding,
        &'bindings ProjectionBindingActivation,
    )> {
        let activation = self
            .bindings
            .binary_search_by_key(&binding_id, ProjectionBindingActivation::binding_id)
            .ok()
            .map(|index| &self.bindings[index])?;
        let binding = catalog.binding(binding_id)?;
        (activation.program_id == program_id
            && binding.program_id() == program_id
            && activation.epoch == *epoch
            && activation.route.is_some())
        .then_some((binding, activation))
    }
}

/// Independent compatibility pins attached to an exact active binding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionCompatibilityPins {
    events: Vec<ProjectionEventSchema>,
    #[serde(with = "program_id_serde")]
    program_id: ProjectionProgramId,
    binding_id: ProjectionBindingId,
    program_ir_version: u16,
    operation_semantics_version: u16,
    epoch: ProjectionEpoch,
}

impl ProjectionCompatibilityPins {
    /// Return exact event-schema and codec pins.
    pub fn events(&self) -> &[ProjectionEventSchema] {
        &self.events
    }

    /// Return the semantic program pin.
    pub fn program_id(&self) -> ProjectionProgramId {
        self.program_id
    }

    /// Return the deployment binding pin.
    pub fn binding_id(&self) -> ProjectionBindingId {
        self.binding_id
    }

    /// Return the portable program IR pin.
    pub fn program_ir_version(&self) -> u16 {
        self.program_ir_version
    }

    /// Return the operation-semantics pin.
    pub fn operation_semantics_version(&self) -> u16 {
        self.operation_semantics_version
    }

    /// Return the physical incarnation pin.
    pub fn epoch(&self) -> &ProjectionEpoch {
        &self.epoch
    }
}

/// Projection catalog or activation validation failure.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProjectionCatalogError {
    /// A nested topology declaration was invalid.
    Topology(ProjectionTopologyError),
    /// A catalog repeated one exact binding.
    DuplicateBinding {
        /// Repeated binding.
        binding: ProjectionBindingId,
    },
    /// Event name/version was registered with divergent wire schemas.
    DivergentEventSchema {
        /// Semantic event name.
        event: String,
        /// Semantic event version.
        version: u64,
    },
    /// A model or storage identity was registered with divergent schemas.
    DivergentOutputSchema {
        /// Logical model.
        model: String,
        /// Physical storage.
        storage: String,
    },
    /// A relationship name was registered with divergent endpoints.
    DivergentRelationship {
        /// Source model.
        source_model: String,
        /// Relationship name.
        relationship: String,
    },
    /// An active view repeated one exact binding.
    DuplicateActivation {
        /// Repeated binding.
        binding: ProjectionBindingId,
    },
    /// An activation referenced no catalog binding.
    UnknownBinding {
        /// Missing binding.
        binding: ProjectionBindingId,
    },
    /// An activation's program did not match the catalog binding.
    ProgramMismatch {
        /// Affected binding.
        binding: ProjectionBindingId,
    },
    /// An executable binding had no exact local or remote route.
    MissingRoute {
        /// Affected binding.
        binding: ProjectionBindingId,
    },
    /// A direct binding attempted to execute remotely.
    RemoteDirectBinding {
        /// Affected binding.
        binding: ProjectionBindingId,
    },
    /// Two activations share the same event, primary read model, owner, and epoch.
    DuplicatePrimaryBinding {
        /// Semantic event name.
        event: String,
        /// Declared read-model identity.
        read_model_id: String,
        /// Projector owner.
        owner: String,
        /// Shared epoch.
        epoch: String,
    },
    /// Two live bindings can authoritatively write one logical scope.
    AuthoritativeWriterConflict {
        /// Logical output model.
        model: String,
        /// First writer.
        first: ProjectionBindingId,
        /// Second writer.
        second: ProjectionBindingId,
    },
    /// Behavior or ownership changed without advancing the physical epoch.
    SameEpochTakeover {
        /// Logical output model.
        model: String,
        /// Reused epoch.
        epoch: String,
    },
    /// A canonical wire version is unsupported.
    UnsupportedVersion {
        /// Versioned contract.
        field: &'static str,
        /// Supported version.
        expected: u16,
        /// Observed version.
        actual: u16,
    },
    /// Wire bytes were valid JSON but not the exact canonical representation.
    NonCanonical {
        /// Noncanonical contract.
        field: &'static str,
    },
    /// Canonical encoding failed.
    Canonical(String),
}

impl From<ProjectionTopologyError> for ProjectionCatalogError {
    fn from(error: ProjectionTopologyError) -> Self {
        Self::Topology(error)
    }
}

impl fmt::Display for ProjectionCatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Topology(error) => error.fmt(formatter),
            Self::DuplicateBinding { binding } => {
                write!(formatter, "projection catalog repeats binding `{binding}`")
            }
            Self::DivergentEventSchema { event, version } => write!(
                formatter,
                "domain event `{event}` version {version} has divergent schemas or codecs"
            ),
            Self::DivergentOutputSchema { model, storage } => write!(
                formatter,
                "projection output `{model}`/`{storage}` has divergent physical schemas"
            ),
            Self::DivergentRelationship {
                source_model,
                relationship,
            } => write!(
                formatter,
                "projection relationship `{source_model}.{relationship}` has divergent endpoints"
            ),
            Self::DuplicateActivation { binding } => {
                write!(formatter, "active projection view repeats binding `{binding}`")
            }
            Self::UnknownBinding { binding } => {
                write!(formatter, "active projection binding `{binding}` is not cataloged")
            }
            Self::ProgramMismatch { binding } => write!(
                formatter,
                "active projection binding `{binding}` has a different program digest"
            ),
            Self::MissingRoute { binding } => write!(
                formatter,
                "active projection binding `{binding}` has no exact executor route"
            ),
            Self::RemoteDirectBinding { binding } => write!(
                formatter,
                "direct projection binding `{binding}` must execute through a local route"
            ),
            Self::DuplicatePrimaryBinding {
                event,
                read_model_id,
                owner,
                epoch,
            } => write!(
                formatter,
                "duplicate primary binding for event `{event}` read model `{read_model_id}` owner `{owner}` epoch `{epoch}`"
            ),
            Self::AuthoritativeWriterConflict {
                model,
                first,
                second,
            } => write!(
                formatter,
                "projection model `{model}` has overlapping authoritative writers `{first}` and `{second}`"
            ),
            Self::SameEpochTakeover { model, epoch } => write!(
                formatter,
                "projection model `{model}` changed binding behavior while retaining epoch `{epoch}`"
            ),
            Self::UnsupportedVersion {
                field,
                expected,
                actual,
            } => write!(
                formatter,
                "{field} version {actual} is unsupported; expected {expected}"
            ),
            Self::NonCanonical { field } => {
                write!(formatter, "{field} bytes are not in canonical order")
            }
            Self::Canonical(error) => write!(formatter, "canonical topology JSON failed: {error}"),
        }
    }
}

impl std::error::Error for ProjectionCatalogError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Topology(error) => Some(error),
            _ => None,
        }
    }
}

fn validate_activation(
    catalog: &ProjectionCatalog,
    activation: &ProjectionBindingActivation,
) -> Result<(), ProjectionCatalogError> {
    let binding =
        catalog
            .binding(activation.binding_id)
            .ok_or(ProjectionCatalogError::UnknownBinding {
                binding: activation.binding_id,
            })?;
    if activation.program_id != binding.program_id() {
        return Err(ProjectionCatalogError::ProgramMismatch {
            binding: activation.binding_id,
        });
    }
    let route = activation
        .route
        .as_ref()
        .ok_or(ProjectionCatalogError::MissingRoute {
            binding: activation.binding_id,
        })?;
    route.validate()?;
    if binding.placement() == ProjectionPlacement::Direct
        && matches!(route, ProjectionExecutorRoute::Remote { .. })
    {
        return Err(ProjectionCatalogError::RemoteDirectBinding {
            binding: activation.binding_id,
        });
    }
    Ok(())
}

fn validate_primary_binding_uniqueness(
    catalog: &ProjectionCatalog,
    activations: &[ProjectionBindingActivation],
) -> Result<(), ProjectionCatalogError> {
    let mut seen = BTreeSet::<(String, u64, String, String, String)>::new();
    for activation in activations {
        let binding = catalog.binding(activation.binding_id).ok_or(
            ProjectionCatalogError::UnknownBinding {
                binding: activation.binding_id,
            },
        )?;
        let read_model_ids = if let Some(primary) = binding.primary_read_model_id() {
            vec![primary]
        } else {
            binding
                .outputs()
                .iter()
                .map(|output| output.read_model_id())
                .collect()
        };
        if read_model_ids.is_empty() {
            continue;
        }
        let owner = binding.owner().name().to_string();
        let epoch = activation.epoch.as_str().to_string();
        for read_model_id in read_model_ids {
            for event in binding.events() {
                let key = (
                    event.name().to_string(),
                    event.version(),
                    read_model_id.to_string(),
                    owner.clone(),
                    epoch.clone(),
                );
                if !seen.insert(key.clone()) {
                    return Err(ProjectionCatalogError::DuplicatePrimaryBinding {
                        event: key.0,
                        read_model_id: key.2,
                        owner: key.3,
                        epoch: key.4,
                    });
                }
            }
        }
    }
    Ok(())
}

fn validate_authoritative_writers(
    catalog: &ProjectionCatalog,
    activations: &[ProjectionBindingActivation],
) -> Result<(), ProjectionCatalogError> {
    for (index, left_activation) in activations.iter().enumerate() {
        let left = catalog.binding(left_activation.binding_id).ok_or(
            ProjectionCatalogError::UnknownBinding {
                binding: left_activation.binding_id,
            },
        )?;
        for right_activation in &activations[index + 1..] {
            let right = catalog.binding(right_activation.binding_id).ok_or(
                ProjectionCatalogError::UnknownBinding {
                    binding: right_activation.binding_id,
                },
            )?;
            if let Some(model) = overlapping_model(left, right) {
                return Err(ProjectionCatalogError::AuthoritativeWriterConflict {
                    model,
                    first: left.id(),
                    second: right.id(),
                });
            }
        }
    }
    Ok(())
}

fn validate_epoch_takeover(
    catalog: &ProjectionCatalog,
    activations: &[ProjectionBindingActivation],
    previous_catalog: &ProjectionCatalog,
    previous_activations: &[ProjectionBindingActivation],
) -> Result<(), ProjectionCatalogError> {
    for activation in activations {
        let current = catalog.binding(activation.binding_id).ok_or(
            ProjectionCatalogError::UnknownBinding {
                binding: activation.binding_id,
            },
        )?;
        for previous_activation in previous_activations {
            if activation.epoch != previous_activation.epoch
                || activation.binding_id == previous_activation.binding_id
            {
                continue;
            }
            let previous = previous_catalog
                .binding(previous_activation.binding_id)
                .ok_or(ProjectionCatalogError::UnknownBinding {
                    binding: previous_activation.binding_id,
                })?;
            if let Some(model) = overlapping_model(current, previous) {
                return Err(ProjectionCatalogError::SameEpochTakeover {
                    model,
                    epoch: activation.epoch.as_str().to_owned(),
                });
            }
        }
    }
    Ok(())
}

fn overlapping_model(left: &ProjectionBinding, right: &ProjectionBinding) -> Option<String> {
    left.outputs()
        .iter()
        .find(|left_output| {
            right.outputs().iter().any(|right_output| {
                left_output.model() == right_output.model()
                    || left_output.storage() == right_output.storage()
            })
        })
        .map(|output| output.model().to_owned())
}

fn validate_event_schemas(bindings: &[ProjectionBinding]) -> Result<(), ProjectionCatalogError> {
    let mut schemas = BTreeMap::<(String, u64), ProjectionEventSchema>::new();
    for event in bindings.iter().flat_map(ProjectionBinding::events) {
        let key = (event.name().to_owned(), event.version());
        if schemas
            .insert(key.clone(), event.clone())
            .is_some_and(|registered| registered != *event)
        {
            return Err(ProjectionCatalogError::DivergentEventSchema {
                event: key.0,
                version: key.1,
            });
        }
    }
    Ok(())
}

fn validate_output_schemas(bindings: &[ProjectionBinding]) -> Result<(), ProjectionCatalogError> {
    let mut by_model = BTreeMap::new();
    let mut by_storage = BTreeMap::new();
    for output in bindings.iter().flat_map(ProjectionBinding::outputs) {
        if by_model.insert(output.model(), output).is_some_and(
            |registered: &crate::projection::placement::ProjectionOutput| {
                registered.storage() != output.storage() || registered.schema() != output.schema()
            },
        ) {
            return Err(ProjectionCatalogError::DivergentOutputSchema {
                model: output.model().to_owned(),
                storage: output.storage().to_owned(),
            });
        }
        if by_storage.insert(output.storage(), output).is_some_and(
            |registered: &crate::projection::placement::ProjectionOutput| {
                registered.model() != output.model() || registered.schema() != output.schema()
            },
        ) {
            return Err(ProjectionCatalogError::DivergentOutputSchema {
                model: output.model().to_owned(),
                storage: output.storage().to_owned(),
            });
        }
    }
    Ok(())
}

fn validate_relationship_schemas(
    bindings: &[ProjectionBinding],
) -> Result<(), ProjectionCatalogError> {
    let mut relationships = BTreeMap::new();
    for relationship in bindings.iter().flat_map(ProjectionBinding::relationships) {
        let key = (
            relationship.source_model().to_owned(),
            relationship.relationship().to_owned(),
        );
        if relationships
            .insert(key.clone(), relationship.target_model())
            .is_some_and(|registered| registered != relationship.target_model())
        {
            return Err(ProjectionCatalogError::DivergentRelationship {
                source_model: key.0,
                relationship: key.1,
            });
        }
    }
    Ok(())
}

mod program_id_serde {
    use serde::{Deserialize, Deserializer, Serializer};

    use super::ProjectionProgramId;

    pub(super) fn serialize<S>(
        value: &ProjectionProgramId,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<ProjectionProgramId, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        ProjectionProgramId::parse(&value).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use crate::projection::lower::{
        DirectCandidate, EventualOnly, LoweredProjectionPlan, ProjectionDescriptor,
        ProjectionLoweringError, ProjectionOutputInventory,
    };
    use crate::projection::placement::{
        DirectProjectionPlacement, ProjectionBinding, ProjectionBindingState, ProjectionEpoch,
        ProjectionExecutionClass, ProjectionExecutorRoute, ProjectionOutput, ProjectionOwner,
        ProjectionPhysicalTopology, ProjectionProgramDescriptor, ProjectionRelationshipBinding,
        ProjectionSourceBinding, ProjectionTopologyError, PROJECTION_PARTITION_CODEC_VERSION,
    };
    use crate::projection::{
        ProjectionArm, ProjectionAssignment, ProjectionExpression, ProjectionField,
        ProjectionKeyField, ProjectionMutationKind, ProjectionOperation, ProjectionPartition,
        ProjectionProgram, ProjectionTarget, ProjectionValue,
        PROJECTION_OPERATION_SEMANTICS_VERSION, PROJECTION_PROGRAM_IR_VERSION,
    };
    use crate::projection_protocol::{
        ProjectionInputCursor, ProjectionPartition as ProtocolProjectionPartition,
        ProjectionSource, ProjectorTopologyId,
    };
    use crate::table::{
        ColumnType, PrimaryKey, RelationshipDef, RelationshipKind, TableColumn, TableKind,
        TableSchema,
    };
    use crate::{DomainEventBodyKind, DOMAIN_EVENT_OCCURRENCE_VERSION};

    use super::{
        ActiveProjectionBindings, ProjectionBindingActivation, ProjectionCatalog,
        ProjectionCatalogError,
    };

    const FINGERPRINT_A: &str =
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const FINGERPRINT_B: &str =
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    struct TestDescriptor(ProjectionProgram);

    impl ProjectionProgramDescriptor for TestDescriptor {
        fn projection_program(&self) -> Result<ProjectionProgram, crate::ProjectionProgramError> {
            Ok(self.0.clone())
        }
    }

    fn generated_program() -> Result<ProjectionProgram, crate::ProjectionProgramError> {
        Ok(program("project_todos", FINGERPRINT_A))
    }

    fn generated_resolve(
        _: &crate::DomainEventOccurrence,
    ) -> Result<crate::ResolvedProjectionPlan, crate::ProjectionProgramError> {
        unreachable!("placement selection never resolves an occurrence")
    }

    fn generated_lower(
        _: &crate::ResolvedProjectionPlan,
    ) -> Result<LoweredProjectionPlan, ProjectionLoweringError> {
        unreachable!("placement selection never lowers a plan")
    }

    fn generated_inventory() -> Result<ProjectionOutputInventory, ProjectionLoweringError> {
        Ok(ProjectionOutputInventory::default())
    }

    fn generated_descriptor<D>() -> ProjectionDescriptor<D> {
        ProjectionDescriptor::__generated(
            "project_todos",
            1,
            "todos-v1",
            generated_program,
            generated_resolve,
            generated_lower,
            generated_inventory,
        )
    }

    fn program(name: &str, fingerprint: &str) -> ProjectionProgram {
        program_with_target_and_partition(
            name,
            fingerprint,
            "Todos",
            "todos",
            ProjectionPartition::Unit,
        )
    }

    fn program_with_partition(
        name: &str,
        fingerprint: &str,
        partition: ProjectionPartition,
    ) -> ProjectionProgram {
        program_with_target_and_partition(name, fingerprint, "Todos", "todos", partition)
    }

    fn program_with_target_and_partition(
        name: &str,
        fingerprint: &str,
        model: &str,
        storage: &str,
        partition: ProjectionPartition,
    ) -> ProjectionProgram {
        program_with_kind_and_partition(
            name,
            fingerprint,
            model,
            storage,
            ProjectionMutationKind::Upsert,
            partition,
        )
    }

    fn program_with_kind_and_partition(
        name: &str,
        fingerprint: &str,
        model: &str,
        storage: &str,
        mutation_kind: ProjectionMutationKind,
        partition: ProjectionPartition,
    ) -> ProjectionProgram {
        let selector = crate::projection::ProjectionEventSelector::try_new(
            DOMAIN_EVENT_OCCURRENCE_VERSION,
            "todo.completed",
            1,
            DomainEventBodyKind::State,
            "TodoState",
            1,
            "distributed.domain-state:tests::TodoState",
            fingerprint,
            "distributed-json",
            1,
        )
        .unwrap();
        let key = ProjectionKeyField::try_new(
            0,
            "todo_id",
            ProjectionExpression::constant(ProjectionValue::string("todo-1")),
        )
        .unwrap();
        let id = ProjectionField::try_new(
            0,
            "todo_id",
            ProjectionAssignment::Set(ProjectionExpression::constant(ProjectionValue::string(
                "todo-1",
            ))),
        )
        .unwrap();
        let title = ProjectionField::try_new(
            1,
            "title",
            ProjectionAssignment::Set(ProjectionExpression::constant(ProjectionValue::string(
                "write tests",
            ))),
        )
        .unwrap();
        let operation = ProjectionOperation::try_new(
            "upsert-todo",
            0,
            mutation_kind,
            ProjectionTarget::try_new(model, storage).unwrap(),
            vec![key],
            vec![id, title],
            vec![],
            vec![],
        )
        .unwrap();
        let arm = ProjectionArm::try_new("completed", selector, vec![operation]).unwrap();
        ProjectionProgram::try_new(name, 1, partition, vec![arm]).unwrap()
    }

    fn todo_schema(storage: &str) -> TableSchema {
        model_schema("Todos", storage)
    }

    fn model_schema(model: &str, storage: &str) -> TableSchema {
        TableSchema {
            model_name: model.into(),
            table_name: storage.into(),
            columns: vec![
                TableColumn {
                    primary_key: true,
                    ..TableColumn::new("todo_id", "todo_id", ColumnType::Text)
                },
                TableColumn::new("title", "title", ColumnType::Text),
            ],
            primary_key: PrimaryKey::new(["todo_id"]),
            version_column: Some("_sourced_version".into()),
            foreign_keys: vec![],
            indexes: vec![],
            relationships: vec![],
            kind: TableKind::ReadModel,
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "this test helper keeps each catalog conflict dimension explicit at call sites"
    )]
    fn binding_with(
        program: &ProjectionProgram,
        owner: &str,
        source: &str,
        placement: crate::projection::placement::ProjectionPlacement,
        execution_class: ProjectionExecutionClass,
        storage: &str,
        partition_codec_version: u16,
        relationships: Vec<ProjectionRelationshipBinding>,
    ) -> ProjectionBinding {
        ProjectionBinding::try_new(
            program,
            ProjectionSourceBinding::try_new(source, "ordered-domain-events", 1).unwrap(),
            ProjectionOwner::try_new(owner).unwrap(),
            placement,
            execution_class,
            "distributed-projection-partition",
            partition_codec_version,
            vec![ProjectionOutput::try_new("Todos", storage, todo_schema(storage)).unwrap()],
            relationships,
            Some(ProjectionPhysicalTopology::from_protocol(
                &ProjectorTopologyId::new(1, "project_todos", [0x44; 32]).unwrap(),
            )),
        )
        .unwrap()
    }

    fn eventual(
        program: &ProjectionProgram,
        owner: &str,
        execution_class: ProjectionExecutionClass,
    ) -> ProjectionBinding {
        ProjectionBinding::from_eventual_program(
            program,
            ProjectionSourceBinding::try_new("todo-domain", "ordered-domain-events", 1).unwrap(),
            ProjectionOwner::try_new(owner).unwrap(),
            execution_class,
            "distributed-projection-partition",
            PROJECTION_PARTITION_CODEC_VERSION,
            vec![ProjectionOutput::try_new("Todos", "todos", todo_schema("todos")).unwrap()],
            vec![],
            Some(ProjectionPhysicalTopology::from_protocol(
                &ProjectorTopologyId::new(1, "project_todos", [0x44; 32]).unwrap(),
            )),
        )
        .unwrap()
    }

    fn direct(program: ProjectionProgram, owner: &str) -> ProjectionBinding {
        let descriptor = TestDescriptor(program);
        ProjectionBinding::materialize_direct(
            DirectProjectionPlacement::new(&descriptor),
            ProjectionSourceBinding::try_new("todo-domain", "ordered-domain-events", 1).unwrap(),
            ProjectionOwner::try_new(owner).unwrap(),
            "distributed-projection-partition",
            PROJECTION_PARTITION_CODEC_VERSION,
            vec![ProjectionOutput::try_new("Todos", "todos", todo_schema("todos")).unwrap()],
            vec![],
            Some(ProjectionPhysicalTopology::from_protocol(
                &ProjectorTopologyId::new(1, "project_todos", [0x44; 32]).unwrap(),
            )),
        )
        .unwrap()
    }

    #[test]
    fn eventual_intent_defaults_causal_and_can_opt_into_background() {
        let descriptor = generated_descriptor::<EventualOnly>();
        let intent = descriptor.eventual().background();
        let binding = ProjectionBinding::materialize_eventual(
            intent,
            ProjectionSourceBinding::try_new("todo-domain", "ordered-domain-events", 1).unwrap(),
            ProjectionOwner::try_new("todo-analytics").unwrap(),
            "distributed-projection-partition",
            1,
            vec![ProjectionOutput::try_new("Todos", "todos", todo_schema("todos")).unwrap()],
            vec![],
            None,
        )
        .unwrap();

        assert_eq!(
            binding.execution_class(),
            ProjectionExecutionClass::Background
        );
    }

    #[test]
    fn generated_descriptor_exposes_only_its_candidate_gated_placement_intents() {
        let eventual = generated_descriptor::<EventualOnly>();
        let eventual_intent = eventual.eventual().background();
        let direct = generated_descriptor::<DirectCandidate>();
        let direct_intent = direct.direct();

        assert_eq!(
            eventual_intent.execution_class(),
            ProjectionExecutionClass::Background
        );
        assert_eq!(eventual_intent.descriptor().name(), "project_todos");
        assert_eq!(direct_intent.descriptor().name(), "project_todos");
    }

    #[test]
    fn direct_materialization_revalidates_the_generated_candidate() {
        let descriptor = TestDescriptor(program_with_kind_and_partition(
            "project_todos",
            FINGERPRINT_A,
            "Todos",
            "todos",
            ProjectionMutationKind::Patch,
            ProjectionPartition::Unit,
        ));
        let error = ProjectionBinding::materialize_direct(
            DirectProjectionPlacement::new(&descriptor),
            ProjectionSourceBinding::try_new("todo-domain", "ordered-domain-events", 1).unwrap(),
            ProjectionOwner::try_new("todo-direct").unwrap(),
            "distributed-projection-partition",
            1,
            vec![ProjectionOutput::try_new("Todos", "todos", todo_schema("todos")).unwrap()],
            vec![],
            None,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            ProjectionTopologyError::DirectIneligible { .. }
        ));

        let descriptor = TestDescriptor(program("project_todos", FINGERPRINT_A));
        let mut wider_schema = todo_schema("todos");
        wider_schema
            .columns
            .push(TableColumn::new("status", "status", ColumnType::Text));
        let error = ProjectionBinding::materialize_direct(
            DirectProjectionPlacement::new(&descriptor),
            ProjectionSourceBinding::try_new("todo-domain", "ordered-domain-events", 1).unwrap(),
            ProjectionOwner::try_new("todo-direct").unwrap(),
            "distributed-projection-partition",
            1,
            vec![ProjectionOutput::try_new("Todos", "todos", wider_schema).unwrap()],
            vec![],
            None,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            ProjectionTopologyError::DirectIneligible { .. }
        ));
    }

    fn activation(
        binding: &ProjectionBinding,
        epoch: &str,
        state: ProjectionBindingState,
        route: Option<ProjectionExecutorRoute>,
    ) -> ProjectionBindingActivation {
        ProjectionBindingActivation::new(
            binding.id(),
            binding.program_id(),
            ProjectionEpoch::new(epoch).unwrap(),
            state,
            route,
        )
    }

    #[test]
    fn program_identity_is_independent_while_binding_tracks_every_deployment_input() {
        let program = program("project_todos", FINGERPRINT_A);
        let baseline = eventual(&program, "todo-reads", ProjectionExecutionClass::Causal);
        let relationship =
            ProjectionRelationshipBinding::try_new("Todos", "owner", "Owners").unwrap();
        let variants = [
            binding_with(
                &program,
                "other-owner",
                "todo-domain",
                crate::projection::placement::ProjectionPlacement::Eventual,
                ProjectionExecutionClass::Causal,
                "todos",
                1,
                vec![],
            ),
            binding_with(
                &program,
                "todo-reads",
                "other-source",
                crate::projection::placement::ProjectionPlacement::Eventual,
                ProjectionExecutionClass::Causal,
                "todos",
                1,
                vec![],
            ),
            binding_with(
                &program,
                "todo-reads",
                "todo-domain",
                crate::projection::placement::ProjectionPlacement::Direct,
                ProjectionExecutionClass::Causal,
                "todos",
                1,
                vec![],
            ),
            binding_with(
                &program,
                "todo-reads",
                "todo-domain",
                crate::projection::placement::ProjectionPlacement::Eventual,
                ProjectionExecutionClass::Background,
                "todos",
                1,
                vec![],
            ),
            binding_with(
                &program,
                "todo-reads",
                "todo-domain",
                crate::projection::placement::ProjectionPlacement::Eventual,
                ProjectionExecutionClass::Causal,
                "todos",
                2,
                vec![],
            ),
            binding_with(
                &program,
                "todo-reads",
                "todo-domain",
                crate::projection::placement::ProjectionPlacement::Eventual,
                ProjectionExecutionClass::Causal,
                "todos",
                1,
                vec![relationship],
            ),
        ];

        assert!(variants.iter().all(|variant| {
            variant.program_id() == baseline.program_id() && variant.id() != baseline.id()
        }));

        let moved_program = program_with_target_and_partition(
            "project_todos",
            FINGERPRINT_A,
            "Todos",
            "todos_v2",
            ProjectionPartition::Unit,
        );
        let moved_storage = binding_with(
            &moved_program,
            "todo-reads",
            "todo-domain",
            crate::projection::placement::ProjectionPlacement::Eventual,
            ProjectionExecutionClass::Causal,
            "todos_v2",
            1,
            vec![],
        );
        assert_ne!(moved_storage.id(), baseline.id());
    }

    #[test]
    fn route_relocation_and_epoch_rotation_leave_both_static_identities_stable() {
        let program = program("project_todos", FINGERPRINT_A);
        let binding = eventual(&program, "todo-reads", ProjectionExecutionClass::Causal);
        let catalog = ProjectionCatalog::try_new(vec![binding.clone()]).unwrap();
        let local = catalog
            .activate(
                vec![activation(
                    &binding,
                    "todos-v1",
                    ProjectionBindingState::Active,
                    Some(ProjectionExecutorRoute::local("todo-api").unwrap()),
                )],
                None,
            )
            .unwrap();
        let remote = catalog
            .activate(
                vec![activation(
                    &binding,
                    "todos-v1",
                    ProjectionBindingState::Active,
                    Some(ProjectionExecutorRoute::remote("todo-projector").unwrap()),
                )],
                Some((&catalog, &local)),
            )
            .unwrap();
        let rotated = catalog
            .activate(
                vec![activation(
                    &binding,
                    "todos-v2",
                    ProjectionBindingState::Active,
                    Some(ProjectionExecutorRoute::remote("todo-projector").unwrap()),
                )],
                Some((&catalog, &remote)),
            )
            .unwrap();

        assert_eq!(binding.program_id(), program.id().unwrap());
        assert_eq!(local.bindings()[0].binding_id(), binding.id());
        assert_eq!(remote.bindings()[0].binding_id(), binding.id());
        assert_eq!(rotated.bindings()[0].binding_id(), binding.id());
        assert_ne!(local.bindings()[0].route(), remote.bindings()[0].route());
        assert_ne!(remote.bindings()[0].epoch(), rotated.bindings()[0].epoch());
    }

    #[test]
    fn catalog_presence_never_implies_liveness_or_causal_eligibility() {
        let program = program("project_todos", FINGERPRINT_A);
        let binding = eventual(&program, "todo-reads", ProjectionExecutionClass::Causal);
        let catalog = ProjectionCatalog::try_new(vec![binding.clone()]).unwrap();
        let inactive = catalog.activate(vec![], None).unwrap();
        let epoch = ProjectionEpoch::new("todos-v1").unwrap();

        assert!(!inactive.is_live(&catalog, binding.id(), binding.program_id(), &epoch));
        assert!(!inactive.is_causally_eligible(
            &catalog,
            binding.id(),
            binding.program_id(),
            &epoch
        ));
    }

    #[test]
    fn exact_active_remote_binding_is_live_and_causally_eligible() {
        let todo_program = program("project_todos", FINGERPRINT_A);
        let other_program = program("project_other_todos", FINGERPRINT_A);
        let binding = eventual(
            &todo_program,
            "todo-reads",
            ProjectionExecutionClass::Causal,
        );
        let catalog = ProjectionCatalog::try_new(vec![binding.clone()]).unwrap();
        let epoch = ProjectionEpoch::new("todos-v1").unwrap();
        let active = catalog
            .activate(
                vec![activation(
                    &binding,
                    epoch.as_str(),
                    ProjectionBindingState::Active,
                    Some(ProjectionExecutorRoute::remote("todo-projector").unwrap()),
                )],
                None,
            )
            .unwrap();

        assert!(active.is_live(&catalog, binding.id(), binding.program_id(), &epoch));
        assert!(active.is_causally_eligible(&catalog, binding.id(), binding.program_id(), &epoch));
        assert!(!active.is_live(&catalog, binding.id(), other_program.id().unwrap(), &epoch));
        assert!(!active.is_live(
            &catalog,
            binding.id(),
            binding.program_id(),
            &ProjectionEpoch::new("todos-v2").unwrap()
        ));
    }

    #[test]
    fn activation_epoch_flows_into_protocol_cursor_without_conversion() {
        let program = program("project_todos", FINGERPRINT_A);
        let binding = eventual(&program, "todo-reads", ProjectionExecutionClass::Causal);
        let catalog = ProjectionCatalog::try_new(vec![binding.clone()]).unwrap();
        let active = catalog
            .activate(
                vec![activation(
                    &binding,
                    "todos-v1",
                    ProjectionBindingState::Active,
                    Some(ProjectionExecutorRoute::remote("todo-projector").unwrap()),
                )],
                None,
            )
            .unwrap();
        let cursor = ProjectionInputCursor::new(
            ProjectorTopologyId::new(1, "project_todos", [0x44; 32]).unwrap(),
            ProtocolProjectionPartition::new(b"tenant-a".to_vec()).unwrap(),
            ProjectionSource::new("todo-domain", b"todo-1".to_vec()).unwrap(),
            active.bindings()[0].epoch().clone(),
            1,
        )
        .unwrap();

        assert_eq!(cursor.epoch(), active.bindings()[0].epoch());
    }

    #[test]
    fn background_binding_is_live_without_becoming_a_ui_obligation() {
        let program = program("project_todos", FINGERPRINT_A);
        let binding = eventual(
            &program,
            "todo-analytics",
            ProjectionExecutionClass::Background,
        );
        let catalog = ProjectionCatalog::try_new(vec![binding.clone()]).unwrap();
        let epoch = ProjectionEpoch::new("analytics-v1").unwrap();
        let active = catalog
            .activate(
                vec![activation(
                    &binding,
                    epoch.as_str(),
                    ProjectionBindingState::Active,
                    Some(ProjectionExecutorRoute::remote("analytics").unwrap()),
                )],
                None,
            )
            .unwrap();

        assert!(active.is_live(&catalog, binding.id(), binding.program_id(), &epoch));
        assert!(!active.is_causally_eligible(&catalog, binding.id(), binding.program_id(), &epoch));
    }

    #[test]
    fn compatibility_pins_event_program_binding_semantics_and_epoch_separately() {
        let program = program("project_todos", FINGERPRINT_A);
        let binding = eventual(&program, "todo-reads", ProjectionExecutionClass::Causal);
        let catalog = ProjectionCatalog::try_new(vec![binding.clone()]).unwrap();
        let active = catalog
            .activate(
                vec![activation(
                    &binding,
                    "todos-v7",
                    ProjectionBindingState::Active,
                    Some(ProjectionExecutorRoute::local("todo-api").unwrap()),
                )],
                None,
            )
            .unwrap();
        let pins = active.compatibility_pins(&catalog, binding.id()).unwrap();

        assert_eq!(pins.events()[0].body_fingerprint(), FINGERPRINT_A);
        assert_eq!(pins.events()[0].body_codec(), "distributed-json");
        assert_eq!(pins.program_id(), binding.program_id());
        assert_eq!(pins.binding_id(), binding.id());
        assert_eq!(pins.program_ir_version(), PROJECTION_PROGRAM_IR_VERSION);
        assert_eq!(
            pins.operation_semantics_version(),
            PROJECTION_OPERATION_SEMANTICS_VERSION
        );
        assert_eq!(pins.epoch().as_str(), "todos-v7");
    }

    #[test]
    fn two_authoritative_writers_fail_service_construction() {
        let program = program("project_todos", FINGERPRINT_A);
        let first = eventual(&program, "todo-reads-a", ProjectionExecutionClass::Causal);
        let second = eventual(&program, "todo-reads-b", ProjectionExecutionClass::Causal);
        let catalog = ProjectionCatalog::try_new(vec![first.clone(), second.clone()]).unwrap();
        let error = catalog
            .activate(
                vec![
                    activation(
                        &first,
                        "todos-v1",
                        ProjectionBindingState::Active,
                        Some(ProjectionExecutorRoute::local("worker-a").unwrap()),
                    ),
                    activation(
                        &second,
                        "todos-v2",
                        ProjectionBindingState::Active,
                        Some(ProjectionExecutorRoute::remote("worker-b").unwrap()),
                    ),
                ],
                None,
            )
            .unwrap_err();

        assert!(matches!(
            error,
            ProjectionCatalogError::AuthoritativeWriterConflict { .. }
        ));
    }

    #[test]
    fn one_event_feeds_two_named_read_models() {
        let operational = program_with_target_and_partition(
            "project_operational_todos",
            FINGERPRINT_A,
            "operational.todos",
            "operational_todos",
            ProjectionPartition::Unit,
        );
        let analytics = program_with_target_and_partition(
            "project_todo_throughput",
            FINGERPRINT_A,
            "analytics.todos",
            "todo_throughput",
            ProjectionPartition::Unit,
        );
        let first = ProjectionBinding::from_eventual_program(
            &operational,
            ProjectionSourceBinding::try_new("todo-domain", "ordered-domain-events", 1).unwrap(),
            ProjectionOwner::try_new("ops-writer").unwrap(),
            ProjectionExecutionClass::Causal,
            "distributed-projection-partition",
            PROJECTION_PARTITION_CODEC_VERSION,
            vec![ProjectionOutput::try_new(
                "operational.todos",
                "operational_todos",
                model_schema("operational.todos", "operational_todos"),
            )
            .unwrap()],
            Vec::new(),
            None,
        )
        .unwrap();
        let second = ProjectionBinding::from_eventual_program(
            &analytics,
            ProjectionSourceBinding::try_new("todo-domain", "ordered-domain-events", 1).unwrap(),
            ProjectionOwner::try_new("analytics-writer").unwrap(),
            ProjectionExecutionClass::Causal,
            "distributed-projection-partition",
            PROJECTION_PARTITION_CODEC_VERSION,
            vec![ProjectionOutput::try_new(
                "analytics.todos",
                "todo_throughput",
                model_schema("analytics.todos", "todo_throughput"),
            )
            .unwrap()],
            Vec::new(),
            None,
        )
        .unwrap();
        assert_eq!(first.primary_read_model_id(), Some("operational.todos"));
        assert_eq!(second.primary_read_model_id(), Some("analytics.todos"));
        assert_eq!(
            first.primary_read_model_id(),
            Some(first.outputs()[0].read_model_id())
        );
        let moved = ProjectionBinding::from_eventual_program(
            &operational,
            ProjectionSourceBinding::try_new("todo-domain", "ordered-domain-events", 1).unwrap(),
            ProjectionOwner::try_new("other-process").unwrap(),
            ProjectionExecutionClass::Causal,
            "distributed-projection-partition",
            PROJECTION_PARTITION_CODEC_VERSION,
            vec![ProjectionOutput::try_new(
                "operational.todos",
                "operational_todos",
                model_schema("operational.todos", "operational_todos"),
            )
            .unwrap()],
            Vec::new(),
            None,
        )
        .unwrap();
        assert_eq!(moved.primary_read_model_id(), first.primary_read_model_id());
        assert_ne!(moved.owner().name(), first.owner().name());

        let catalog = ProjectionCatalog::try_new(vec![first.clone(), second.clone()]).unwrap();
        catalog
            .activate(
                vec![
                    activation(
                        &first,
                        "ops-v1",
                        ProjectionBindingState::Active,
                        Some(ProjectionExecutorRoute::local("ops").unwrap()),
                    ),
                    activation(
                        &second,
                        "analytics-v1",
                        ProjectionBindingState::Active,
                        Some(ProjectionExecutorRoute::local("analytics").unwrap()),
                    ),
                ],
                None,
            )
            .unwrap();
    }

    #[test]
    fn direct_and_eventual_overlap_fails_service_construction() {
        let program = program("project_todos", FINGERPRINT_A);
        let eventual = eventual(&program, "todo-reads", ProjectionExecutionClass::Causal);
        let direct = direct(program, "todo-direct");
        let catalog = ProjectionCatalog::try_new(vec![eventual.clone(), direct.clone()]).unwrap();
        let error = catalog
            .activate(
                vec![
                    activation(
                        &eventual,
                        "todos-v1",
                        ProjectionBindingState::Active,
                        Some(ProjectionExecutorRoute::remote("todo-projector").unwrap()),
                    ),
                    activation(
                        &direct,
                        "todos-v2",
                        ProjectionBindingState::Active,
                        Some(ProjectionExecutorRoute::local("todo-api").unwrap()),
                    ),
                ],
                None,
            )
            .unwrap_err();

        assert!(matches!(
            error,
            ProjectionCatalogError::AuthoritativeWriterConflict { .. }
        ));
    }

    #[test]
    fn different_partition_expressions_do_not_prove_writer_disjointness() {
        let unit_program = program("project_todos_unit", FINGERPRINT_A);
        let dynamic_program = program_with_partition(
            "project_todos_dynamic",
            FINGERPRINT_A,
            ProjectionPartition::Expression(ProjectionExpression::constant(
                ProjectionValue::string("tenant-a"),
            )),
        );
        let first = eventual(
            &unit_program,
            "todo-reads-unit",
            ProjectionExecutionClass::Causal,
        );
        let second = eventual(
            &dynamic_program,
            "todo-reads-dynamic",
            ProjectionExecutionClass::Causal,
        );
        let catalog = ProjectionCatalog::try_new(vec![first.clone(), second.clone()]).unwrap();
        let error = catalog
            .activate(
                vec![
                    activation(
                        &first,
                        "todos-v1",
                        ProjectionBindingState::Active,
                        Some(ProjectionExecutorRoute::local("worker-a").unwrap()),
                    ),
                    activation(
                        &second,
                        "todos-v2",
                        ProjectionBindingState::Active,
                        Some(ProjectionExecutorRoute::remote("worker-b").unwrap()),
                    ),
                ],
                None,
            )
            .unwrap_err();

        assert!(matches!(
            error,
            ProjectionCatalogError::AuthoritativeWriterConflict { .. }
        ));
    }

    #[test]
    fn missing_executor_route_fails_service_construction() {
        let program = program("project_todos", FINGERPRINT_A);
        let binding = eventual(&program, "todo-reads", ProjectionExecutionClass::Causal);
        let catalog = ProjectionCatalog::try_new(vec![binding.clone()]).unwrap();
        let error = catalog
            .activate(
                vec![activation(
                    &binding,
                    "todos-v1",
                    ProjectionBindingState::Active,
                    None,
                )],
                None,
            )
            .unwrap_err();

        assert!(matches!(error, ProjectionCatalogError::MissingRoute { .. }));
    }

    #[test]
    fn remote_direct_route_fails_service_construction() {
        let binding = direct(program("project_todos", FINGERPRINT_A), "todo-direct");
        let catalog = ProjectionCatalog::try_new(vec![binding.clone()]).unwrap();
        let error = catalog
            .activate(
                vec![activation(
                    &binding,
                    "todos-v1",
                    ProjectionBindingState::Active,
                    Some(ProjectionExecutorRoute::remote("other-service").unwrap()),
                )],
                None,
            )
            .unwrap_err();

        assert!(matches!(
            error,
            ProjectionCatalogError::RemoteDirectBinding { .. }
        ));
    }

    #[test]
    fn divergent_event_schema_fails_catalog_construction() {
        let first_program = program("project_todos_a", FINGERPRINT_A);
        let second_program = program("project_todos_b", FINGERPRINT_B);
        let first = eventual(
            &first_program,
            "todo-reads-a",
            ProjectionExecutionClass::Causal,
        );
        let second = eventual(
            &second_program,
            "todo-reads-b",
            ProjectionExecutionClass::Causal,
        );

        assert!(matches!(
            ProjectionCatalog::try_new(vec![first, second]),
            Err(ProjectionCatalogError::DivergentEventSchema { .. })
        ));
    }

    #[test]
    fn divergent_output_schema_fails_catalog_construction() {
        let program = program("project_todos", FINGERPRINT_A);
        let first = eventual(&program, "todo-reads-a", ProjectionExecutionClass::Causal);
        let mut divergent = todo_schema("todos");
        divergent
            .columns
            .push(TableColumn::new("status", "status", ColumnType::Text));
        let second = ProjectionBinding::from_eventual_program(
            &program,
            ProjectionSourceBinding::try_new("todo-domain", "ordered-domain-events", 1).unwrap(),
            ProjectionOwner::try_new("todo-reads-b").unwrap(),
            ProjectionExecutionClass::Causal,
            "distributed-projection-partition",
            1,
            vec![ProjectionOutput::try_new("Todos", "todos", divergent).unwrap()],
            vec![],
            None,
        )
        .unwrap();

        assert!(matches!(
            ProjectionCatalog::try_new(vec![first, second]),
            Err(ProjectionCatalogError::DivergentOutputSchema { .. })
        ));
    }

    #[test]
    fn same_epoch_behavior_takeover_fails_before_start() {
        let program = program("project_todos", FINGERPRINT_A);
        let previous_binding =
            eventual(&program, "todo-reads-v1", ProjectionExecutionClass::Causal);
        let previous_catalog = ProjectionCatalog::try_new(vec![previous_binding.clone()]).unwrap();
        let previous_active = previous_catalog
            .activate(
                vec![activation(
                    &previous_binding,
                    "todos-rebuild-1",
                    ProjectionBindingState::Active,
                    Some(ProjectionExecutorRoute::remote("projector-v1").unwrap()),
                )],
                None,
            )
            .unwrap();
        let next_binding = eventual(&program, "todo-reads-v2", ProjectionExecutionClass::Causal);
        let next_catalog = ProjectionCatalog::try_new(vec![next_binding.clone()]).unwrap();
        let error = next_catalog
            .activate(
                vec![activation(
                    &next_binding,
                    "todos-rebuild-1",
                    ProjectionBindingState::Active,
                    Some(ProjectionExecutorRoute::remote("projector-v2").unwrap()),
                )],
                Some((&previous_catalog, &previous_active)),
            )
            .unwrap_err();

        assert!(matches!(
            error,
            ProjectionCatalogError::SameEpochTakeover { .. }
        ));
    }

    #[test]
    fn semantic_program_replacement_requires_epoch_rotation_before_replay() {
        let previous_program = program_with_kind_and_partition(
            "project_todos",
            FINGERPRINT_A,
            "Todos",
            "todos",
            ProjectionMutationKind::Upsert,
            ProjectionPartition::Unit,
        );
        let replacement_program = program_with_kind_and_partition(
            "project_todos",
            FINGERPRINT_A,
            "Todos",
            "todos",
            ProjectionMutationKind::Patch,
            ProjectionPartition::Unit,
        );
        assert_ne!(
            previous_program.id().unwrap(),
            replacement_program.id().unwrap(),
            "different projection behavior must have different semantic identities"
        );
        let previous_binding = eventual(
            &previous_program,
            "todo-reads-v1",
            ProjectionExecutionClass::Causal,
        );
        let replacement_binding = eventual(
            &replacement_program,
            "todo-reads-v2",
            ProjectionExecutionClass::Causal,
        );
        assert_eq!(
            previous_binding.physical_topology(),
            replacement_binding.physical_topology(),
            "the physical owner may remain stable while the program changes"
        );

        let previous_catalog = ProjectionCatalog::try_new(vec![previous_binding.clone()]).unwrap();
        let previous_active = previous_catalog
            .activate(
                vec![activation(
                    &previous_binding,
                    "todos-rebuild-1",
                    ProjectionBindingState::Active,
                    Some(ProjectionExecutorRoute::remote("projector-v1").unwrap()),
                )],
                None,
            )
            .unwrap();
        let replacement_catalog =
            ProjectionCatalog::try_new(vec![replacement_binding.clone()]).unwrap();
        let same_epoch = replacement_catalog
            .activate(
                vec![activation(
                    &replacement_binding,
                    "todos-rebuild-1",
                    ProjectionBindingState::Active,
                    Some(ProjectionExecutorRoute::remote("projector-v2").unwrap()),
                )],
                Some((&previous_catalog, &previous_active)),
            )
            .unwrap_err();
        assert!(matches!(
            same_epoch,
            ProjectionCatalogError::SameEpochTakeover { .. }
        ));

        let rotated = replacement_catalog
            .activate(
                vec![activation(
                    &replacement_binding,
                    "todos-rebuild-2",
                    ProjectionBindingState::Active,
                    Some(ProjectionExecutorRoute::remote("projector-v2").unwrap()),
                )],
                Some((&previous_catalog, &previous_active)),
            )
            .unwrap();
        assert_eq!(
            rotated.bindings()[0].program_id(),
            replacement_program.id().unwrap()
        );
        assert_eq!(rotated.bindings()[0].epoch().as_str(), "todos-rebuild-2");
    }

    #[test]
    fn new_epoch_allows_rollout_then_draining_stops_new_obligations() {
        let program = program("project_todos", FINGERPRINT_A);
        let previous_binding =
            eventual(&program, "todo-reads-v1", ProjectionExecutionClass::Causal);
        let previous_catalog = ProjectionCatalog::try_new(vec![previous_binding.clone()]).unwrap();
        let previous_active = previous_catalog
            .activate(
                vec![activation(
                    &previous_binding,
                    "todos-rebuild-1",
                    ProjectionBindingState::Active,
                    Some(ProjectionExecutorRoute::remote("projector-v1").unwrap()),
                )],
                None,
            )
            .unwrap();
        let next_binding = eventual(&program, "todo-reads-v2", ProjectionExecutionClass::Causal);
        let next_catalog = ProjectionCatalog::try_new(vec![next_binding.clone()]).unwrap();
        let next_active = next_catalog
            .activate(
                vec![activation(
                    &next_binding,
                    "todos-rebuild-2",
                    ProjectionBindingState::Active,
                    Some(ProjectionExecutorRoute::remote("projector-v2").unwrap()),
                )],
                Some((&previous_catalog, &previous_active)),
            )
            .unwrap();
        let draining = next_catalog
            .activate(
                vec![activation(
                    &next_binding,
                    "todos-rebuild-2",
                    ProjectionBindingState::Draining,
                    Some(ProjectionExecutorRoute::remote("projector-v2").unwrap()),
                )],
                Some((&next_catalog, &next_active)),
            )
            .unwrap();
        let epoch = ProjectionEpoch::new("todos-rebuild-2").unwrap();
        let rolled_back = previous_catalog
            .activate(
                vec![activation(
                    &previous_binding,
                    "todos-rebuild-1",
                    ProjectionBindingState::Active,
                    Some(ProjectionExecutorRoute::remote("projector-v1").unwrap()),
                )],
                Some((&next_catalog, &draining)),
            )
            .unwrap();
        let previous_epoch = ProjectionEpoch::new("todos-rebuild-1").unwrap();

        assert!(draining.is_live(
            &next_catalog,
            next_binding.id(),
            next_binding.program_id(),
            &epoch
        ));
        assert!(!draining.is_causally_eligible(
            &next_catalog,
            next_binding.id(),
            next_binding.program_id(),
            &epoch
        ));
        assert!(rolled_back.is_causally_eligible(
            &previous_catalog,
            previous_binding.id(),
            previous_binding.program_id(),
            &previous_epoch
        ));
    }

    #[test]
    fn canonical_catalog_round_trips_and_ignores_registration_order() {
        let program = program("project_todos", FINGERPRINT_A);
        let first = eventual(&program, "todo-reads-a", ProjectionExecutionClass::Causal);
        let second = eventual(
            &program,
            "todo-reads-b",
            ProjectionExecutionClass::Background,
        );
        let left = ProjectionCatalog::try_new(vec![first.clone(), second.clone()]).unwrap();
        let right = ProjectionCatalog::try_new(vec![second, first]).unwrap();
        let bytes = left.canonical_bytes().unwrap();

        assert_eq!(bytes, right.canonical_bytes().unwrap());
        assert_eq!(
            ProjectionCatalog::from_canonical_bytes(&bytes).unwrap(),
            left
        );
    }

    #[test]
    fn canonical_active_view_round_trips_and_ignores_registration_order() {
        let program = program("project_todos", FINGERPRINT_A);
        let first = eventual(&program, "todo-reads-a", ProjectionExecutionClass::Causal);
        let archive_program = program_with_target_and_partition(
            "project_archived_todos",
            FINGERPRINT_A,
            "ArchivedTodos",
            "archived_todos",
            ProjectionPartition::Unit,
        );
        let second = ProjectionBinding::from_eventual_program(
            &archive_program,
            ProjectionSourceBinding::try_new("todo-domain", "ordered-domain-events", 1).unwrap(),
            ProjectionOwner::try_new("todo-reads-b").unwrap(),
            ProjectionExecutionClass::Background,
            "distributed-projection-partition",
            1,
            vec![ProjectionOutput::try_new(
                "ArchivedTodos",
                "archived_todos",
                model_schema("ArchivedTodos", "archived_todos"),
            )
            .unwrap()],
            vec![],
            None,
        )
        .unwrap();
        let catalog = ProjectionCatalog::try_new(vec![first.clone(), second.clone()]).unwrap();
        let first_activation = activation(
            &first,
            "todos-v1",
            ProjectionBindingState::Active,
            Some(ProjectionExecutorRoute::local("todo-api").unwrap()),
        );
        let second_activation = activation(
            &second,
            "archive-v1",
            ProjectionBindingState::Active,
            Some(ProjectionExecutorRoute::remote("archive-projector").unwrap()),
        );
        let left = catalog
            .activate(
                vec![first_activation.clone(), second_activation.clone()],
                None,
            )
            .unwrap();
        let right = catalog
            .activate(vec![second_activation, first_activation], None)
            .unwrap();
        let bytes = left.canonical_bytes().unwrap();

        assert_eq!(bytes, right.canonical_bytes().unwrap());
        assert_eq!(
            ActiveProjectionBindings::from_canonical_bytes(&catalog, &bytes).unwrap(),
            left
        );
    }

    #[test]
    fn canonical_catalog_fixture_is_frozen_without_routes_or_epochs() {
        let program = program("project_todos", FINGERPRINT_A);
        let binding = eventual(&program, "todo-reads", ProjectionExecutionClass::Causal);
        let catalog = ProjectionCatalog::try_new(vec![binding]).unwrap();
        let actual = String::from_utf8(catalog.canonical_bytes().unwrap()).unwrap();
        let expected = include_str!("fixtures/projection-catalog-v1.json").trim_end();

        assert_eq!(actual, expected);
        assert!(!actual.contains("executor"));
        assert!(!actual.contains("epoch"));
        assert!(!actual.contains("http"));
    }

    #[test]
    fn endpoint_shaped_routes_and_physical_names_are_rejected() {
        assert!(ProjectionExecutorRoute::remote("https://projector.internal").is_err());

        let program = program("project_todos", FINGERPRINT_A);
        let endpoint_topology =
            ProjectorTopologyId::new(1, "https://database.internal", [0x44; 32]).unwrap();
        let error = ProjectionBinding::from_eventual_program(
            &program,
            ProjectionSourceBinding::try_new("todo-domain", "ordered-domain-events", 1).unwrap(),
            ProjectionOwner::try_new("todo-reads").unwrap(),
            ProjectionExecutionClass::Causal,
            "distributed-projection-partition",
            1,
            vec![ProjectionOutput::try_new("Todos", "todos", todo_schema("todos")).unwrap()],
            vec![],
            Some(ProjectionPhysicalTopology::from_protocol(
                &endpoint_topology,
            )),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            ProjectionTopologyError::InvalidName {
                field: "physical topology name"
            }
        ));
    }

    #[test]
    fn noncanonical_catalog_bytes_are_rejected() {
        let program = program("project_todos", FINGERPRINT_A);
        let binding = eventual(&program, "todo-reads", ProjectionExecutionClass::Causal);
        let catalog = ProjectionCatalog::try_new(vec![binding]).unwrap();
        let mut bytes = catalog.canonical_bytes().unwrap();
        bytes.push(b'\n');

        assert!(matches!(
            ProjectionCatalog::from_canonical_bytes(&bytes),
            Err(ProjectionCatalogError::NonCanonical { .. })
        ));
    }

    #[test]
    fn relationship_kind_is_part_of_the_output_schema_identity() {
        let mut schema = todo_schema("todos");
        schema.relationships.push(RelationshipDef {
            references: None,
            field_name: "owner".into(),
            kind: RelationshipKind::BelongsTo,
            target_model: "Owners".into(),
            foreign_key: Some("owner_id".into()),
            through: None,
            target_foreign_key: None,
        });

        assert_ne!(schema, todo_schema("todos"));
    }
}
