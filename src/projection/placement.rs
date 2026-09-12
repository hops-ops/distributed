//! Projection deployment identity, placement, and active executor routing.
//!
//! A binding describes durable ownership and compatibility. Executor routes
//! and epochs are deliberately separate: a route may move without changing
//! ownership semantics, while an epoch fences one physical incarnation.

use std::collections::BTreeSet;
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub use crate::projection_protocol::ProjectionEpoch;
use crate::projection_protocol::{
    canonical_projection_topology_bytes, digest_projection_binding, ProjectorTopologyId,
};
use crate::table::TableSchema;
use crate::DomainEventBodyKind;

use super::lower::{DirectCandidate, ProjectionDescriptor};
use super::{
    ProjectionEventSelector, ProjectionMutationKind, ProjectionPartition, ProjectionProgram,
    ProjectionProgramError, ProjectionProgramId, ProjectionRelationship,
};

/// Canonical projection-binding identity format.
pub const PROJECTION_BINDING_IDENTITY_VERSION: u16 = 1;
/// Canonical deployment catalog wire format.
pub const PROJECTION_CATALOG_WIRE_VERSION: u16 = 1;
/// Canonical active-binding view wire format.
pub const PROJECTION_ACTIVE_BINDINGS_WIRE_VERSION: u16 = 1;
/// Version-one physical partition codec used by projection bindings.
pub const PROJECTION_PARTITION_CODEC_VERSION: u16 = 1;

const MAX_TOPOLOGY_NAME_BYTES: usize = 128;

/// Whether a projection is applied after event delivery or in the command
/// transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionPlacement {
    /// Apply through ordered domain-event delivery.
    Eventual,
    /// Apply inside the producing command transaction.
    Direct,
}

/// Whether a live eventual consumer may become an ordinary UI obligation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionExecutionClass {
    /// Eligible application surfaces may wait for exact finite obligations.
    Causal,
    /// Execute normally without delaying ordinary UI command completion.
    Background,
}

/// Current executor location for one exact active binding.
///
/// Routes are deployment observations, not binding identity. `service` is a
/// logical service name; network endpoints and credentials have no field in
/// this type.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProjectionExecutorRoute {
    /// The service constructing this active view hosts the executor.
    Local {
        /// Stable logical service name.
        service: String,
    },
    /// Another logical service hosts the executor.
    Remote {
        /// Stable logical service name, never a URL or credential.
        service: String,
    },
}

impl ProjectionExecutorRoute {
    /// Construct a local executor route.
    ///
    /// # Errors
    ///
    /// Rejects blank, oversized, or endpoint-shaped service names.
    pub fn local(service: impl Into<String>) -> Result<Self, ProjectionTopologyError> {
        Ok(Self::Local {
            service: validate_identity_name("local executor service", service.into())?,
        })
    }

    /// Construct a remote executor route.
    ///
    /// # Errors
    ///
    /// Rejects blank, oversized, or endpoint-shaped service names.
    pub fn remote(service: impl Into<String>) -> Result<Self, ProjectionTopologyError> {
        Ok(Self::Remote {
            service: validate_identity_name("remote executor service", service.into())?,
        })
    }

    /// Return the logical service hosting this executor.
    pub fn service(&self) -> &str {
        match self {
            Self::Local { service } | Self::Remote { service } => service,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), ProjectionTopologyError> {
        validate_identity_name("executor service", self.service().to_owned()).map(drop)
    }
}

/// Lifecycle of a route retained in the active-binding view.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionBindingState {
    /// Execute new occurrences and permit eligible obligation minting.
    Active,
    /// Finish already committed work without minting new obligations.
    Draining,
}

/// Logical bounded-context source of a projection's domain events.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ProjectionSourceBinding {
    name: String,
    codec: String,
    codec_version: u16,
}

impl ProjectionSourceBinding {
    /// Construct an exact logical event source and ordered-delivery codec pin.
    ///
    /// # Errors
    ///
    /// Rejects invalid names or a zero codec version.
    pub fn try_new(
        name: impl Into<String>,
        codec: impl Into<String>,
        codec_version: u16,
    ) -> Result<Self, ProjectionTopologyError> {
        if codec_version == 0 {
            return Err(ProjectionTopologyError::ZeroVersion {
                field: "projection source codec version",
            });
        }
        Ok(Self {
            name: validate_identity_name("projection source", name.into())?,
            codec: validate_identity_name("projection source codec", codec.into())?,
            codec_version,
        })
    }

    /// Return the logical source name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the ordered-delivery codec name.
    pub fn codec(&self) -> &str {
        &self.codec
    }

    /// Return the ordered-delivery codec version.
    pub fn codec_version(&self) -> u16 {
        self.codec_version
    }

    fn validate(&self) -> Result<(), ProjectionTopologyError> {
        Self::try_new(&self.name, &self.codec, self.codec_version).map(drop)
    }
}

/// Logical authoritative owner of a projection's output scopes.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ProjectionOwner {
    name: String,
}

impl ProjectionOwner {
    /// Construct a stable logical owner.
    ///
    /// # Errors
    ///
    /// Rejects blank, oversized, or endpoint-shaped names.
    pub fn try_new(name: impl Into<String>) -> Result<Self, ProjectionTopologyError> {
        Ok(Self {
            name: validate_identity_name("projection owner", name.into())?,
        })
    }

    /// Return the logical owner name.
    pub fn name(&self) -> &str {
        &self.name
    }

    fn validate(&self) -> Result<(), ProjectionTopologyError> {
        Self::try_new(&self.name).map(drop)
    }
}

/// Exact portable partition expression and physical codec compatibility pin.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionPartitionBinding {
    expression: serde_json::Value,
    codec: String,
    codec_version: u16,
}

impl ProjectionPartitionBinding {
    /// Bind a program's exact partition expression to a physical codec.
    ///
    /// # Errors
    ///
    /// Rejects serialization failures, invalid codec names, or zero versions.
    pub fn try_from_program(
        partition: &ProjectionPartition,
        codec: impl Into<String>,
        codec_version: u16,
    ) -> Result<Self, ProjectionTopologyError> {
        if codec_version == 0 {
            return Err(ProjectionTopologyError::ZeroVersion {
                field: "projection partition codec version",
            });
        }
        let expression = serde_json::to_value(partition)
            .map_err(|error| ProjectionTopologyError::Canonical(error.to_string()))?;
        Ok(Self {
            expression,
            codec: validate_identity_name("projection partition codec", codec.into())?,
            codec_version,
        })
    }

    /// Return the canonical portable partition expression.
    pub fn expression(&self) -> &serde_json::Value {
        &self.expression
    }

    /// Return the physical partition codec.
    pub fn codec(&self) -> &str {
        &self.codec
    }

    /// Return the physical partition codec version.
    pub fn codec_version(&self) -> u16 {
        self.codec_version
    }

    fn validate(&self) -> Result<(), ProjectionTopologyError> {
        if self.codec_version == 0 {
            return Err(ProjectionTopologyError::ZeroVersion {
                field: "projection partition codec version",
            });
        }
        validate_identity_name("projection partition codec", self.codec.clone())?;
        canonical_projection_topology_bytes(&self.expression)
            .map_err(|error| ProjectionTopologyError::Canonical(error.to_string()))?;
        Ok(())
    }
}

/// Exact domain-event schema and codec accepted by one projection program.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProjectionEventSchema {
    occurrence_version: u16,
    name: String,
    version: u64,
    body_kind: DomainEventBodyKind,
    body_type_name: String,
    body_version: u64,
    body_schema: String,
    body_fingerprint: String,
    body_codec: String,
    body_codec_version: u16,
}

impl ProjectionEventSchema {
    /// Copy every independent event wire-contract pin from a program selector.
    pub fn from_selector(selector: &ProjectionEventSelector) -> Self {
        Self {
            occurrence_version: selector.occurrence_version(),
            name: selector.event_name().to_owned(),
            version: selector.event_version(),
            body_kind: selector.body_kind(),
            body_type_name: selector.body_type_name().to_owned(),
            body_version: selector.body_version(),
            body_schema: selector.body_schema().to_owned(),
            body_fingerprint: selector.body_fingerprint().to_owned(),
            body_codec: selector.body_codec().to_owned(),
            body_codec_version: selector.body_codec_version(),
        }
    }

    /// Return the occurrence-envelope version.
    pub fn occurrence_version(&self) -> u16 {
        self.occurrence_version
    }

    /// Return the semantic event name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the semantic event version.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Return whether the body is a structured DTO or raw bytes.
    pub fn body_kind(&self) -> DomainEventBodyKind {
        self.body_kind
    }

    /// Return the stable body type name.
    pub fn body_type_name(&self) -> &str {
        &self.body_type_name
    }

    /// Return the independently versioned body contract.
    pub fn body_version(&self) -> u64 {
        self.body_version
    }

    /// Return the canonical body schema.
    pub fn body_schema(&self) -> &str {
        &self.body_schema
    }

    /// Return the canonical body fingerprint.
    pub fn body_fingerprint(&self) -> &str {
        &self.body_fingerprint
    }

    /// Return the canonical body codec.
    pub fn body_codec(&self) -> &str {
        &self.body_codec
    }

    /// Return the canonical body codec version.
    pub fn body_codec_version(&self) -> u16 {
        self.body_codec_version
    }

    fn validate(&self) -> Result<(), ProjectionTopologyError> {
        ProjectionEventSelector::try_new(
            self.occurrence_version,
            self.name.clone(),
            self.version,
            self.body_kind,
            self.body_type_name.clone(),
            self.body_version,
            self.body_schema.clone(),
            self.body_fingerprint.clone(),
            self.body_codec.clone(),
            self.body_codec_version,
        )
        .map(drop)
        .map_err(ProjectionTopologyError::program)
    }

    fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.occurrence_version
            .cmp(&other.occurrence_version)
            .then_with(|| self.name.cmp(&other.name))
            .then_with(|| self.version.cmp(&other.version))
            .then_with(|| body_kind_rank(self.body_kind).cmp(&body_kind_rank(other.body_kind)))
            .then_with(|| self.body_type_name.cmp(&other.body_type_name))
            .then_with(|| self.body_version.cmp(&other.body_version))
            .then_with(|| self.body_schema.cmp(&other.body_schema))
            .then_with(|| self.body_fingerprint.cmp(&other.body_fingerprint))
            .then_with(|| self.body_codec.cmp(&other.body_codec))
            .then_with(|| self.body_codec_version.cmp(&other.body_codec_version))
    }
}

impl PartialOrd for ProjectionEventSchema {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ProjectionEventSchema {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.canonical_cmp(other)
    }
}

/// One exact output model schema and physical storage identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionOutput {
    model: String,
    storage: String,
    schema: TableSchema,
}

impl ProjectionOutput {
    /// Construct one exact output schema pin.
    ///
    /// # Errors
    ///
    /// Rejects invalid schemas or identities that disagree with the schema.
    pub fn try_new(
        model: impl Into<String>,
        storage: impl Into<String>,
        schema: TableSchema,
    ) -> Result<Self, ProjectionTopologyError> {
        let model = validate_identity_name("projection output model", model.into())?;
        let storage = validate_identity_name("projection output storage", storage.into())?;
        schema
            .validate()
            .map_err(|error| ProjectionTopologyError::InvalidSchema {
                model: model.clone(),
                reason: error.to_string(),
            })?;
        if schema.model_name != model {
            return Err(ProjectionTopologyError::ModelSchemaMismatch {
                declared: model,
                schema: schema.model_name,
            });
        }
        if schema.table_name != storage {
            return Err(ProjectionTopologyError::StorageSchemaMismatch {
                declared: storage,
                schema: schema.table_name,
            });
        }
        Ok(Self {
            model,
            storage,
            schema,
        })
    }

    /// Return the logical model name.
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Primary `ReadModelId` for this output (schema model name).
    pub fn read_model_id(&self) -> &str {
        self.schema.model_name.as_str()
    }

    /// Return the physical storage identity.
    pub fn storage(&self) -> &str {
        &self.storage
    }

    /// Return the exact relational schema.
    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    fn validate(&self) -> Result<(), ProjectionTopologyError> {
        Self::try_new(&self.model, &self.storage, self.schema.clone()).map(drop)
    }
}

/// Explicit relationship inventory participating in binding compatibility.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ProjectionRelationshipBinding {
    source_model: String,
    relationship: String,
    target_model: String,
}

impl ProjectionRelationshipBinding {
    /// Construct one exact logical relationship descriptor.
    ///
    /// # Errors
    ///
    /// Rejects blank or endpoint-shaped names.
    pub fn try_new(
        source_model: impl Into<String>,
        relationship: impl Into<String>,
        target_model: impl Into<String>,
    ) -> Result<Self, ProjectionTopologyError> {
        Ok(Self {
            source_model: validate_identity_name(
                "projection relationship source model",
                source_model.into(),
            )?,
            relationship: validate_identity_name(
                "projection relationship name",
                relationship.into(),
            )?,
            target_model: validate_identity_name(
                "projection relationship target model",
                target_model.into(),
            )?,
        })
    }

    /// Copy one relationship descriptor from a portable program.
    ///
    /// # Errors
    ///
    /// Rejects invalid names.
    pub fn try_from_relationship(
        relationship: &ProjectionRelationship,
    ) -> Result<Self, ProjectionTopologyError> {
        Self::try_new(
            relationship.source_model(),
            relationship.relationship(),
            relationship.target_model(),
        )
    }

    /// Return the source model.
    pub fn source_model(&self) -> &str {
        &self.source_model
    }

    /// Return the stable relationship name.
    pub fn relationship(&self) -> &str {
        &self.relationship
    }

    /// Return the target model.
    pub fn target_model(&self) -> &str {
        &self.target_model
    }

    fn validate(&self) -> Result<(), ProjectionTopologyError> {
        Self::try_new(&self.source_model, &self.relationship, &self.target_model).map(drop)
    }
}

/// Existing adapter topology retained while storage protocols migrate to the
/// canonical projection catalog.
///
/// This is a logical scope-codec/storage compatibility pin. It is not an
/// executor route or physical incarnation and therefore contains no endpoint
/// or epoch.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ProjectionPhysicalTopology {
    version: u32,
    name: String,
    digest: [u8; 32],
}

impl ProjectionPhysicalTopology {
    /// Capture the exact existing adapter topology envelope.
    pub fn from_protocol(topology: &ProjectorTopologyId) -> Self {
        Self {
            version: topology.version(),
            name: topology.name().to_owned(),
            digest: topology.digest(),
        }
    }

    /// Return the physical topology version.
    pub fn version(&self) -> u32 {
        self.version
    }

    /// Return the physical topology name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the physical topology digest.
    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }

    fn validate(&self) -> Result<(), ProjectionTopologyError> {
        validate_identity_name("physical topology name", self.name.clone())?;
        ProjectorTopologyId::new(self.version, &self.name, self.digest)
            .map(drop)
            .map_err(|error| ProjectionTopologyError::InvalidPhysicalTopology(error.to_string()))
    }
}

/// Domain-separated identity of one deployment binding contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProjectionBindingId([u8; 32]);

impl ProjectionBindingId {
    /// Parse canonical `pb1:sha256:<lowercase-hex>` text.
    ///
    /// # Errors
    ///
    /// Rejects alternate prefixes, lengths, uppercase, and non-hex text.
    pub fn parse(value: &str) -> Result<Self, ProjectionTopologyError> {
        let Some(hex) = value.strip_prefix("pb1:sha256:") else {
            return Err(ProjectionTopologyError::InvalidBindingId);
        };
        if hex.len() != 64
            || !hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ProjectionTopologyError::InvalidBindingId);
        }
        let mut bytes = [0_u8; 32];
        for (index, pair) in hex.as_bytes().chunks_exact(2).enumerate() {
            let high = hex_nibble(pair[0]).ok_or(ProjectionTopologyError::InvalidBindingId)?;
            let low = hex_nibble(pair[1]).ok_or(ProjectionTopologyError::InvalidBindingId)?;
            bytes[index] = (high << 4) | low;
        }
        Ok(Self(bytes))
    }

    /// Return the raw SHA-256 digest.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for ProjectionBindingId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("pb1:sha256:")?;
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl Serialize for ProjectionBindingId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ProjectionBindingId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(serde::de::Error::custom)
    }
}

/// One complete canonical deployment binding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionBinding {
    identity_version: u16,
    program_ir_version: u16,
    operation_semantics_version: u16,
    #[serde(with = "program_id_serde")]
    program_id: ProjectionProgramId,
    events: Vec<ProjectionEventSchema>,
    source: ProjectionSourceBinding,
    owner: ProjectionOwner,
    placement: ProjectionPlacement,
    execution_class: ProjectionExecutionClass,
    partition: ProjectionPartitionBinding,
    outputs: Vec<ProjectionOutput>,
    relationships: Vec<ProjectionRelationshipBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    physical_topology: Option<ProjectionPhysicalTopology>,
    binding_id: ProjectionBindingId,
}

/// Framework descriptor capable of exposing its exact validated program.
///
/// Generated descriptor types implement this trait inside Distributed. The
/// descriptor marker narrows the supported authoring surface, while binding
/// materialization independently revalidates direct eligibility.
pub trait ProjectionProgramDescriptor {
    /// Build the exact portable program.
    ///
    /// # Errors
    ///
    /// Returns declaration or canonical program validation failures.
    fn projection_program(&self) -> Result<ProjectionProgram, ProjectionProgramError>;
}

impl<D> ProjectionProgramDescriptor for ProjectionDescriptor<D> {
    fn projection_program(&self) -> Result<ProjectionProgram, ProjectionProgramError> {
        self.program()
    }
}

impl<D> ProjectionDescriptor<D> {
    /// Select ordered domain-event delivery for this projection.
    ///
    /// Eventual placement is available for every generated projection.
    #[must_use]
    pub fn eventual(&self) -> EventualProjectionPlacement<'_, Self> {
        EventualProjectionPlacement::new(self)
    }
}

impl ProjectionDescriptor<DirectCandidate> {
    /// Attempt same-transaction execution for a generated direct candidate.
    ///
    /// Binding materialization revalidates the exact program and registered
    /// output schema before accepting direct placement.
    #[must_use]
    pub fn direct(&self) -> DirectProjectionPlacement<'_, Self> {
        DirectProjectionPlacement::new(self)
    }
}

/// Author intent to mount any generated descriptor eventually.
///
/// Generated descriptors return this value from `.eventual()`. The default is
/// causal; `.background()` opts out of ordinary UI obligations.
#[derive(Clone, Copy, Debug)]
pub struct EventualProjectionPlacement<'a, D: ?Sized> {
    descriptor: &'a D,
    execution_class: ProjectionExecutionClass,
}

impl<'a, D: ?Sized> EventualProjectionPlacement<'a, D> {
    /// Mark this eventual consumer as background-only.
    #[must_use]
    pub fn background(mut self) -> Self {
        self.execution_class = ProjectionExecutionClass::Background;
        self
    }

    /// Return the generated descriptor.
    pub fn descriptor(&self) -> &'a D {
        self.descriptor
    }

    /// Return causal or background execution class.
    pub fn execution_class(&self) -> ProjectionExecutionClass {
        self.execution_class
    }

    pub(crate) fn new(descriptor: &'a D) -> Self {
        Self {
            descriptor,
            execution_class: ProjectionExecutionClass::Causal,
        }
    }
}

/// Candidate-bearing author intent to attempt direct placement.
///
/// Fields and construction remain framework-owned. Task-8 generated
/// `ProjectionDescriptor<DirectCandidate>::direct()` is the supported public
/// producer, so eventual-only descriptors do not expose this method.
#[derive(Clone, Copy, Debug)]
pub struct DirectProjectionPlacement<'a, D: ?Sized> {
    descriptor: &'a D,
}

impl<'a, D: ?Sized> DirectProjectionPlacement<'a, D> {
    /// Return the generated direct candidate.
    pub fn descriptor(&self) -> &'a D {
        self.descriptor
    }

    pub(crate) fn new(descriptor: &'a D) -> Self {
        Self { descriptor }
    }
}

impl ProjectionBinding {
    /// Build an eventual deployment binding from any validated portable
    /// program.
    ///
    /// # Errors
    ///
    /// Rejects invalid schemas, duplicate inventories, or canonical encoding
    /// failures.
    #[expect(
        clippy::too_many_arguments,
        reason = "each argument is an independent deployment compatibility pin"
    )]
    pub fn from_eventual_program(
        program: &ProjectionProgram,
        source: ProjectionSourceBinding,
        owner: ProjectionOwner,
        execution_class: ProjectionExecutionClass,
        partition_codec: impl Into<String>,
        partition_codec_version: u16,
        outputs: Vec<ProjectionOutput>,
        relationships: Vec<ProjectionRelationshipBinding>,
        physical_topology: Option<ProjectionPhysicalTopology>,
    ) -> Result<Self, ProjectionTopologyError> {
        Self::try_new(
            program,
            source,
            owner,
            ProjectionPlacement::Eventual,
            execution_class,
            partition_codec,
            partition_codec_version,
            outputs,
            relationships,
            physical_topology,
        )
    }

    /// Materialize a generated eventual placement intent.
    ///
    /// Service construction supplies logical source/owner and physical schema
    /// defaults after the application selects `.eventual()` or
    /// `.eventual().background()`.
    ///
    /// # Errors
    ///
    /// Rejects invalid schemas, duplicate inventories, or canonical encoding
    /// failures.
    #[expect(
        clippy::too_many_arguments,
        reason = "each argument is an independent deployment compatibility pin"
    )]
    pub fn materialize_eventual<D: ProjectionProgramDescriptor + ?Sized>(
        intent: EventualProjectionPlacement<'_, D>,
        source: ProjectionSourceBinding,
        owner: ProjectionOwner,
        partition_codec: impl Into<String>,
        partition_codec_version: u16,
        outputs: Vec<ProjectionOutput>,
        relationships: Vec<ProjectionRelationshipBinding>,
        physical_topology: Option<ProjectionPhysicalTopology>,
    ) -> Result<Self, ProjectionTopologyError> {
        let program = intent
            .descriptor()
            .projection_program()
            .map_err(ProjectionTopologyError::program)?;
        Self::from_eventual_program(
            &program,
            source,
            owner,
            intent.execution_class(),
            partition_codec,
            partition_codec_version,
            outputs,
            relationships,
            physical_topology,
        )
    }

    /// Materialize a direct deployment binding from a generated candidate.
    ///
    /// # Errors
    ///
    /// Rejects invalid schemas, duplicate inventories, or canonical encoding
    /// failures.
    #[expect(
        clippy::too_many_arguments,
        reason = "each argument is an independent deployment compatibility pin"
    )]
    pub fn materialize_direct<D: ProjectionProgramDescriptor + ?Sized>(
        intent: DirectProjectionPlacement<'_, D>,
        source: ProjectionSourceBinding,
        owner: ProjectionOwner,
        partition_codec: impl Into<String>,
        partition_codec_version: u16,
        outputs: Vec<ProjectionOutput>,
        relationships: Vec<ProjectionRelationshipBinding>,
        physical_topology: Option<ProjectionPhysicalTopology>,
    ) -> Result<Self, ProjectionTopologyError> {
        let program = intent
            .descriptor()
            .projection_program()
            .map_err(ProjectionTopologyError::program)?;
        validate_direct_eligibility(&program, &outputs)?;
        Self::try_new(
            &program,
            source,
            owner,
            ProjectionPlacement::Direct,
            ProjectionExecutionClass::Causal,
            partition_codec,
            partition_codec_version,
            outputs,
            relationships,
            physical_topology,
        )
    }

    /// Build a canonical deployment binding from one validated program.
    ///
    /// # Errors
    ///
    /// Rejects invalid schemas, duplicate inventories, direct-background
    /// placement, or canonical encoding failures.
    #[expect(
        clippy::too_many_arguments,
        reason = "each argument is an independent deployment compatibility pin"
    )]
    pub(crate) fn try_new(
        program: &ProjectionProgram,
        source: ProjectionSourceBinding,
        owner: ProjectionOwner,
        placement: ProjectionPlacement,
        execution_class: ProjectionExecutionClass,
        partition_codec: impl Into<String>,
        partition_codec_version: u16,
        mut outputs: Vec<ProjectionOutput>,
        mut relationships: Vec<ProjectionRelationshipBinding>,
        physical_topology: Option<ProjectionPhysicalTopology>,
    ) -> Result<Self, ProjectionTopologyError> {
        if placement == ProjectionPlacement::Direct
            && execution_class == ProjectionExecutionClass::Background
        {
            return Err(ProjectionTopologyError::DirectBackground);
        }
        let program_id = program.id().map_err(ProjectionTopologyError::program)?;
        let mut events = program
            .arms()
            .iter()
            .map(|arm| ProjectionEventSchema::from_selector(arm.selector()))
            .collect::<Vec<_>>();
        events.sort_by(ProjectionEventSchema::canonical_cmp);
        reject_duplicates(&events, "projection event schema")?;
        outputs.sort_by(|left, right| {
            left.model
                .cmp(&right.model)
                .then_with(|| left.storage.cmp(&right.storage))
        });
        reject_duplicates_by(&outputs, "projection output", |left, right| {
            left.model == right.model || left.storage == right.storage
        })?;
        let declared_outputs = outputs
            .iter()
            .map(|output| (output.model().to_owned(), output.storage().to_owned()))
            .collect::<BTreeSet<_>>();
        let program_outputs = program
            .arms()
            .iter()
            .flat_map(|arm| arm.operations())
            .map(|operation| {
                (
                    operation.target().model().to_owned(),
                    operation.target().storage().to_owned(),
                )
            })
            .collect::<BTreeSet<_>>();
        if declared_outputs != program_outputs {
            return Err(ProjectionTopologyError::OutputInventoryMismatch {
                declared: declared_outputs.into_iter().collect(),
                program: program_outputs.into_iter().collect(),
            });
        }
        relationships.sort();
        reject_duplicates(&relationships, "projection relationship")?;
        let partition = ProjectionPartitionBinding::try_from_program(
            program.partition(),
            partition_codec,
            partition_codec_version,
        )?;
        let mut binding = Self {
            identity_version: PROJECTION_BINDING_IDENTITY_VERSION,
            program_ir_version: program.ir_version(),
            operation_semantics_version: program.operation_semantics_version(),
            program_id,
            events,
            source,
            owner,
            placement,
            execution_class,
            partition,
            outputs,
            relationships,
            physical_topology,
            binding_id: ProjectionBindingId([0; 32]),
        };
        binding.validate_fields()?;
        binding.binding_id = binding.compute_id()?;
        Ok(binding)
    }

    /// Return the canonical binding identity.
    pub fn id(&self) -> ProjectionBindingId {
        self.binding_id
    }

    /// Return the binding-identity algorithm version.
    pub fn identity_version(&self) -> u16 {
        self.identity_version
    }

    /// Return the sole semantic program identity.
    pub fn program_id(&self) -> ProjectionProgramId {
        self.program_id
    }

    /// Return the portable program IR version.
    pub fn program_ir_version(&self) -> u16 {
        self.program_ir_version
    }

    /// Return the logical operation-semantics version.
    pub fn operation_semantics_version(&self) -> u16 {
        self.operation_semantics_version
    }

    /// Return exact accepted event schemas.
    pub fn events(&self) -> &[ProjectionEventSchema] {
        &self.events
    }

    /// Return the logical event source.
    pub fn source(&self) -> &ProjectionSourceBinding {
        &self.source
    }

    /// Return the logical authoritative owner.
    pub fn owner(&self) -> &ProjectionOwner {
        &self.owner
    }

    /// Return eventual or direct placement.
    pub fn placement(&self) -> ProjectionPlacement {
        self.placement
    }

    /// Return causal or background execution class.
    pub fn execution_class(&self) -> ProjectionExecutionClass {
        self.execution_class
    }

    /// Return the exact partition expression and codec.
    pub fn partition(&self) -> &ProjectionPartitionBinding {
        &self.partition
    }

    /// Return sorted output schema pins.
    pub fn outputs(&self) -> &[ProjectionOutput] {
        &self.outputs
    }

    /// Primary read-model identity this binding writes.
    ///
    /// Independent of projector owner and physical placement. Fan-out
    /// bindings (more than one output) have no single primary.
    pub fn primary_read_model_id(&self) -> Option<&str> {
        if self.outputs.len() == 1 {
            self.outputs.first().map(|output| output.read_model_id())
        } else {
            None
        }
    }

    /// Return sorted relationship pins.
    pub fn relationships(&self) -> &[ProjectionRelationshipBinding] {
        &self.relationships
    }

    /// Return the existing adapter topology envelope, when retained.
    pub fn physical_topology(&self) -> Option<&ProjectionPhysicalTopology> {
        self.physical_topology.as_ref()
    }

    pub(crate) fn validate(&self) -> Result<(), ProjectionTopologyError> {
        self.validate_fields()?;
        if self.binding_id != self.compute_id()? {
            return Err(ProjectionTopologyError::BindingIdMismatch);
        }
        Ok(())
    }

    fn validate_fields(&self) -> Result<(), ProjectionTopologyError> {
        if self.identity_version != PROJECTION_BINDING_IDENTITY_VERSION {
            return Err(ProjectionTopologyError::UnsupportedVersion {
                field: "projection binding identity",
                expected: PROJECTION_BINDING_IDENTITY_VERSION,
                actual: self.identity_version,
            });
        }
        if self.program_ir_version == 0 {
            return Err(ProjectionTopologyError::ZeroVersion {
                field: "projection program IR version",
            });
        }
        if self.operation_semantics_version == 0 {
            return Err(ProjectionTopologyError::ZeroVersion {
                field: "projection operation semantics version",
            });
        }
        if self.events.is_empty() {
            return Err(ProjectionTopologyError::Empty {
                field: "projection event inventory",
            });
        }
        if self.outputs.is_empty() {
            return Err(ProjectionTopologyError::Empty {
                field: "projection output inventory",
            });
        }
        if self.placement == ProjectionPlacement::Direct
            && self.execution_class == ProjectionExecutionClass::Background
        {
            return Err(ProjectionTopologyError::DirectBackground);
        }
        self.source.validate()?;
        self.owner.validate()?;
        self.partition.validate()?;
        for event in &self.events {
            event.validate()?;
        }
        for output in &self.outputs {
            output.validate()?;
        }
        for relationship in &self.relationships {
            relationship.validate()?;
        }
        if let Some(topology) = &self.physical_topology {
            topology.validate()?;
        }
        if !self.events.windows(2).all(|pair| pair[0] < pair[1]) {
            return Err(ProjectionTopologyError::NonCanonicalOrder {
                field: "projection events",
            });
        }
        if !self
            .outputs
            .windows(2)
            .all(|pair| (pair[0].model(), pair[0].storage()) < (pair[1].model(), pair[1].storage()))
        {
            return Err(ProjectionTopologyError::NonCanonicalOrder {
                field: "projection outputs",
            });
        }
        if !self.relationships.windows(2).all(|pair| pair[0] < pair[1]) {
            return Err(ProjectionTopologyError::NonCanonicalOrder {
                field: "projection relationships",
            });
        }
        Ok(())
    }

    fn compute_id(&self) -> Result<ProjectionBindingId, ProjectionTopologyError> {
        #[derive(Serialize)]
        struct Identity<'a> {
            identity_version: u16,
            program_ir_version: u16,
            operation_semantics_version: u16,
            #[serde(with = "program_id_serde")]
            program_id: ProjectionProgramId,
            events: &'a [ProjectionEventSchema],
            source: &'a ProjectionSourceBinding,
            owner: &'a ProjectionOwner,
            placement: ProjectionPlacement,
            execution_class: ProjectionExecutionClass,
            partition: &'a ProjectionPartitionBinding,
            outputs: &'a [ProjectionOutput],
            relationships: &'a [ProjectionRelationshipBinding],
            physical_topology: Option<&'a ProjectionPhysicalTopology>,
        }
        let canonical = canonical_projection_topology_bytes(&Identity {
            identity_version: self.identity_version,
            program_ir_version: self.program_ir_version,
            operation_semantics_version: self.operation_semantics_version,
            program_id: self.program_id,
            events: &self.events,
            source: &self.source,
            owner: &self.owner,
            placement: self.placement,
            execution_class: self.execution_class,
            partition: &self.partition,
            outputs: &self.outputs,
            relationships: &self.relationships,
            physical_topology: self.physical_topology.as_ref(),
        })
        .map_err(|error| ProjectionTopologyError::Canonical(error.to_string()))?;
        Ok(ProjectionBindingId(digest_projection_binding(&canonical)))
    }
}

/// Invalid projection topology or identity input.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProjectionTopologyError {
    /// A required identity was empty.
    Empty {
        /// Invalid field.
        field: &'static str,
    },
    /// A bounded identity exceeded its limit.
    TooLong {
        /// Invalid field.
        field: &'static str,
        /// Actual UTF-8 byte length.
        len: usize,
        /// Supported limit.
        max: usize,
    },
    /// An identity contained whitespace, control characters, or endpoint text.
    InvalidName {
        /// Invalid field.
        field: &'static str,
    },
    /// A version was zero.
    ZeroVersion {
        /// Invalid field.
        field: &'static str,
    },
    /// A canonical format version is unsupported.
    UnsupportedVersion {
        /// Versioned contract.
        field: &'static str,
        /// Supported version.
        expected: u16,
        /// Observed version.
        actual: u16,
    },
    /// A direct projection was incorrectly marked background.
    DirectBackground,
    /// A program did not satisfy the current direct-evidence proof.
    DirectIneligible {
        /// Failed proof condition.
        reason: String,
    },
    /// A nested program declaration was invalid.
    Program(String),
    /// A table schema was invalid.
    InvalidSchema {
        /// Logical model.
        model: String,
        /// Validation reason.
        reason: String,
    },
    /// Output model identity disagreed with its schema.
    ModelSchemaMismatch {
        /// Declared model.
        declared: String,
        /// Schema model.
        schema: String,
    },
    /// Output storage identity disagreed with its schema.
    StorageSchemaMismatch {
        /// Declared storage.
        declared: String,
        /// Schema storage.
        schema: String,
    },
    /// Registered output inventory disagreed with the portable program.
    OutputInventoryMismatch {
        /// Registered model/storage pairs.
        declared: Vec<(String, String)>,
        /// Program model/storage pairs.
        program: Vec<(String, String)>,
    },
    /// An existing physical topology envelope was invalid.
    InvalidPhysicalTopology(String),
    /// A sorted inventory contained duplicates.
    Duplicate {
        /// Duplicated inventory.
        field: &'static str,
    },
    /// A wire inventory was not in canonical order.
    NonCanonicalOrder {
        /// Unsorted inventory.
        field: &'static str,
    },
    /// Binding identity text was malformed.
    InvalidBindingId,
    /// A decoded binding did not match its canonical fields.
    BindingIdMismatch,
    /// Canonical JSON encoding failed.
    Canonical(String),
}

impl ProjectionTopologyError {
    fn program(error: ProjectionProgramError) -> Self {
        Self::Program(error.to_string())
    }
}

impl fmt::Display for ProjectionTopologyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty { field } => write!(formatter, "{field} must not be empty"),
            Self::TooLong { field, len, max } => write!(
                formatter,
                "{field} is {len} bytes, exceeding the maximum of {max}"
            ),
            Self::InvalidName { field } => write!(
                formatter,
                "{field} must be a logical name, not whitespace, a URL, or credentials"
            ),
            Self::ZeroVersion { field } => write!(formatter, "{field} must be non-zero"),
            Self::UnsupportedVersion {
                field,
                expected,
                actual,
            } => write!(
                formatter,
                "{field} version {actual} is unsupported; expected {expected}"
            ),
            Self::DirectBackground => {
                formatter.write_str("a direct projection cannot be a background consumer")
            }
            Self::DirectIneligible { reason } => {
                write!(formatter, "projection is not direct-eligible: {reason}")
            }
            Self::Program(error) => write!(formatter, "invalid projection program: {error}"),
            Self::InvalidSchema { model, reason } => {
                write!(formatter, "invalid projection schema `{model}`: {reason}")
            }
            Self::ModelSchemaMismatch { declared, schema } => write!(
                formatter,
                "projection model `{declared}` does not match schema model `{schema}`"
            ),
            Self::StorageSchemaMismatch { declared, schema } => write!(
                formatter,
                "projection storage `{declared}` does not match schema table `{schema}`"
            ),
            Self::OutputInventoryMismatch { declared, program } => write!(
                formatter,
                "projection outputs {declared:?} do not match program targets {program:?}"
            ),
            Self::InvalidPhysicalTopology(error) => {
                write!(formatter, "invalid physical projection topology: {error}")
            }
            Self::Duplicate { field } => write!(formatter, "{field} contains duplicates"),
            Self::NonCanonicalOrder { field } => {
                write!(formatter, "{field} is not in strict canonical order")
            }
            Self::InvalidBindingId => formatter.write_str(
                "projection binding ID must be `pb1:sha256:` followed by 64 lowercase hex digits",
            ),
            Self::BindingIdMismatch => {
                formatter.write_str("projection binding ID does not match its canonical fields")
            }
            Self::Canonical(error) => write!(formatter, "canonical topology JSON failed: {error}"),
        }
    }
}

impl std::error::Error for ProjectionTopologyError {}

fn validate_identity_name(
    field: &'static str,
    value: String,
) -> Result<String, ProjectionTopologyError> {
    if value.is_empty() {
        return Err(ProjectionTopologyError::Empty { field });
    }
    if value.len() > MAX_TOPOLOGY_NAME_BYTES {
        return Err(ProjectionTopologyError::TooLong {
            field,
            len: value.len(),
            max: MAX_TOPOLOGY_NAME_BYTES,
        });
    }
    if value.chars().any(char::is_whitespace)
        || value.chars().any(char::is_control)
        || value.contains("://")
        || value.contains('@')
        || value.contains('/')
        || value.contains('\\')
        || value.contains('?')
        || value.contains('#')
    {
        return Err(ProjectionTopologyError::InvalidName { field });
    }
    Ok(value)
}

fn validate_direct_eligibility(
    program: &ProjectionProgram,
    outputs: &[ProjectionOutput],
) -> Result<(), ProjectionTopologyError> {
    if program.source_snapshots() {
        return Err(ProjectionTopologyError::DirectIneligible {
            reason: "source snapshots require canonical occurrences at the eventual projector"
                .into(),
        });
    }
    let [output] = outputs else {
        return Err(ProjectionTopologyError::DirectIneligible {
            reason: "direct evidence requires exactly one registered output schema".to_owned(),
        });
    };
    let expected_target = (output.model(), output.storage());
    let expected_fields = output
        .schema()
        .columns
        .iter()
        .filter(|column| !column.skipped)
        .map(|column| column.field_name.as_str())
        .collect::<BTreeSet<_>>();
    let expected_key = output
        .schema()
        .primary_key
        .columns
        .iter()
        .map(|column_name| {
            output
                .schema()
                .columns
                .iter()
                .find(|column| column.column_name == *column_name)
                .map(|column| column.field_name.as_str())
                .ok_or_else(|| ProjectionTopologyError::DirectIneligible {
                    reason: format!(
                        "registered primary-key column `{column_name}` has no mapped field"
                    ),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    for arm in program.arms() {
        let [operation] = arm.operations() else {
            return Err(ProjectionTopologyError::DirectIneligible {
                reason: format!(
                    "event arm `{}` must resolve exactly one operation",
                    arm.arm_id()
                ),
            });
        };
        if operation.kind() != ProjectionMutationKind::Upsert {
            return Err(ProjectionTopologyError::DirectIneligible {
                reason: format!(
                    "event arm `{}` must resolve one complete upsert",
                    arm.arm_id()
                ),
            });
        }
        if (operation.target().model(), operation.target().storage()) != expected_target {
            return Err(ProjectionTopologyError::DirectIneligible {
                reason: format!(
                    "event arm `{}` targets another model or storage",
                    arm.arm_id()
                ),
            });
        }
        if !operation.relationship_effects().is_empty() || !operation.invalidations().is_empty() {
            return Err(ProjectionTopologyError::DirectIneligible {
                reason: format!(
                    "event arm `{}` has relationship or invalidation consequences",
                    arm.arm_id()
                ),
            });
        }
        let actual_fields = operation
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<BTreeSet<_>>();
        if actual_fields != expected_fields {
            return Err(ProjectionTopologyError::DirectIneligible {
                reason: format!(
                    "event arm `{}` does not provide every registered row field",
                    arm.arm_id()
                ),
            });
        }
        let actual_key = operation
            .key()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>();
        if actual_key != expected_key {
            return Err(ProjectionTopologyError::DirectIneligible {
                reason: format!(
                    "event arm `{}` does not provide the exact registered primary key",
                    arm.arm_id()
                ),
            });
        }
    }
    Ok(())
}

fn reject_duplicates<T: PartialEq>(
    values: &[T],
    field: &'static str,
) -> Result<(), ProjectionTopologyError> {
    reject_duplicates_by(values, field, PartialEq::eq)
}

fn reject_duplicates_by<T>(
    values: &[T],
    field: &'static str,
    duplicates: impl Fn(&T, &T) -> bool,
) -> Result<(), ProjectionTopologyError> {
    if values.iter().enumerate().any(|(index, left)| {
        values[index + 1..]
            .iter()
            .any(|right| duplicates(left, right))
    }) {
        Err(ProjectionTopologyError::Duplicate { field })
    } else {
        Ok(())
    }
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn body_kind_rank(kind: DomainEventBodyKind) -> u8 {
    match kind {
        DomainEventBodyKind::State => 0,
        DomainEventBodyKind::Event => 1,
        DomainEventBodyKind::Deletion => 2,
    }
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
