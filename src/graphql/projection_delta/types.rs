use std::collections::BTreeSet;
use std::fmt;
use std::marker::PhantomData;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use serde::de::{Error as _, IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use crate::projection_protocol::{MAX_PROJECTION_PARTITION_BYTES, MAX_PROJECTION_RECORD_KEY_BYTES};
use crate::{MAX_DOMAIN_EVENT_BODY_BYTES, MAX_PROJECTION_EXPRESSION_DEPTH};

/// Version of the role-safe projection-delta wire contract.
pub const PROJECTION_DELTA_WIRE_VERSION: u16 = 1;
/// Maximum operations emitted for one command.
pub const MAX_PROJECTION_DELTA_OPERATIONS: usize = 128;

const MAX_IDENTITY_BYTES: usize = 4 * 1024;

/// Typed, already-authenticated GraphQL cache-scope token retained by the
/// sealed request authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProjectionDeltaCacheScopeToken(String);

impl ProjectionDeltaCacheScopeToken {
    pub(crate) fn parse_wire(value: &str) -> Result<Self, ProjectionDeltaError> {
        validate_opaque_protocol_token(value, Some("cache-scope"))?;
        Ok(Self(value.to_owned()))
    }

    #[cfg(feature = "graphql")]
    pub(crate) fn from_protocol(
        token: &crate::graphql::protocol::OpaqueProtocolToken,
    ) -> Result<Self, ProjectionDeltaError> {
        Self::parse_wire(token.as_str())
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

fn validate_opaque_protocol_token(
    value: &str,
    expected_purpose: Option<&str>,
) -> Result<(), ProjectionDeltaError> {
    let mut segments = value.split('.');
    let version = segments.next();
    let purpose = segments.next();
    let encoded = segments.next();
    if segments.next().is_some()
        || version != Some("v1")
        || purpose.is_none()
        || expected_purpose.is_some_and(|expected| purpose != Some(expected))
        || !purpose.is_some_and(|purpose| {
            !purpose.is_empty()
                && purpose
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte == b'-')
        })
        || encoded.is_none()
    {
        return Err(ProjectionDeltaError::InvalidIdentity {
            field: "opaque_protocol_token",
        });
    }
    let encoded = encoded.expect("checked above");
    let decoded =
        URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| ProjectionDeltaError::InvalidIdentity {
                field: "opaque_protocol_token",
            })?;
    if decoded.len() != 32 || URL_SAFE_NO_PAD.encode(decoded) != encoded {
        return Err(ProjectionDeltaError::InvalidIdentity {
            field: "opaque_protocol_token",
        });
    }
    Ok(())
}

/// Exact authorization and projection identity required to replay a delta.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionDeltaIdentity {
    pub manifest_version: u32,
    pub client_protocol_version: u32,
    pub surface: ProjectionDeltaSurfaceIdentity,
    pub schema_fingerprint: String,
    pub protocol_fingerprint: String,
    pub authorization_generation: String,
    /// Opaque authenticated binding of principal, cache scope, selected
    /// Surface, fingerprints, and authorization generation.
    pub cache_scope_token: String,
    pub command_causation_id: String,
}

/// Exact role or named-application surface selected for the client.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProjectionDeltaSurfaceIdentity {
    Role {
        name: String,
    },
    Application {
        name: String,
        eligible_roles: Vec<String>,
        schema_roles: Vec<String>,
    },
}

/// Exact selected program/binding compatibility pins.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionDeltaProjectionIdentity {
    pub program_id: String,
    pub binding_id: String,
    pub epoch: String,
    pub program_ir_version: u16,
    pub operation_semantics_version: u16,
}

/// One versioned command delta containing zero-based ordered occurrences.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionDelta {
    pub wire_version: u16,
    pub identity: ProjectionDeltaIdentity,
    #[serde(deserialize_with = "deserialize_bounded_projections")]
    pub projections: Vec<ProjectionDeltaProjectionIdentity>,
    #[serde(deserialize_with = "deserialize_bounded_occurrences")]
    pub occurrences: Vec<ProjectionDeltaOccurrence>,
    #[serde(deserialize_with = "deserialize_bounded_operations")]
    pub operations: Vec<ProjectionDeltaOperation>,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_bounded_recoveries"
    )]
    pub recoveries: Vec<ProjectionDeltaRecovery>,
}

fn deserialize_bounded_projections<'de, D>(
    deserializer: D,
) -> Result<Vec<ProjectionDeltaProjectionIdentity>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_delta_sequence(deserializer, "projections")
}

fn deserialize_bounded_occurrences<'de, D>(
    deserializer: D,
) -> Result<Vec<ProjectionDeltaOccurrence>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_delta_sequence(deserializer, "occurrences")
}

fn deserialize_bounded_operations<'de, D>(
    deserializer: D,
) -> Result<Vec<ProjectionDeltaOperation>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_delta_sequence(deserializer, "operations")
}

fn deserialize_bounded_recoveries<'de, D>(
    deserializer: D,
) -> Result<Vec<ProjectionDeltaRecovery>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_delta_sequence(deserializer, "recoveries")
}

fn deserialize_bounded_delta_sequence<'de, D, T>(
    deserializer: D,
    field: &'static str,
) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct BoundedDeltaSequenceVisitor<T> {
        field: &'static str,
        marker: PhantomData<fn() -> T>,
    }

    impl<'de, T> Visitor<'de> for BoundedDeltaSequenceVisitor<T>
    where
        T: Deserialize<'de>,
    {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                formatter,
                "at most {MAX_PROJECTION_DELTA_OPERATIONS} projection-delta `{}` items",
                self.field
            )
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            if sequence
                .size_hint()
                .is_some_and(|len| len > MAX_PROJECTION_DELTA_OPERATIONS)
            {
                return Err(A::Error::custom(format_args!(
                    "projection-delta `{}` has more than \
                     {MAX_PROJECTION_DELTA_OPERATIONS} items",
                    self.field
                )));
            }
            let mut items = Vec::with_capacity(
                sequence
                    .size_hint()
                    .unwrap_or_default()
                    .min(MAX_PROJECTION_DELTA_OPERATIONS),
            );
            while items.len() < MAX_PROJECTION_DELTA_OPERATIONS {
                let Some(item) = sequence.next_element()? else {
                    return Ok(items);
                };
                items.push(item);
            }
            if sequence.next_element::<IgnoredAny>()?.is_some() {
                return Err(A::Error::custom(format_args!(
                    "projection-delta `{}` has more than \
                     {MAX_PROJECTION_DELTA_OPERATIONS} items",
                    self.field
                )));
            }
            Ok(items)
        }
    }

    deserializer.deserialize_seq(BoundedDeltaSequenceVisitor {
        field,
        marker: PhantomData,
    })
}

impl ProjectionDelta {
    /// Decode and fully validate a delta.
    ///
    /// # Errors
    ///
    /// Rejects unknown fields or versions, noncanonical values/order, resource
    /// overflows, and malformed or duplicate scopes.
    pub fn from_json(bytes: &[u8]) -> Result<Self, ProjectionDeltaError> {
        if bytes.len() > MAX_DOMAIN_EVENT_BODY_BYTES {
            return Err(ProjectionDeltaError::BodyTooLarge {
                len: bytes.len(),
                max: MAX_DOMAIN_EVENT_BODY_BYTES,
            });
        }
        let decoded: Self = serde_json::from_slice(bytes)
            .map_err(|error| ProjectionDeltaError::InvalidWire(error.to_string()))?;
        decoded.validate()?;
        Ok(decoded)
    }

    /// Encode deterministic JSON after validation.
    ///
    /// # Errors
    ///
    /// Rejects invalid or oversized deltas.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ProjectionDeltaError> {
        self.validate()?;
        let bytes = serde_json::to_vec(self)
            .map_err(|error| ProjectionDeltaError::InvalidWire(error.to_string()))?;
        if bytes.len() > MAX_DOMAIN_EVENT_BODY_BYTES {
            return Err(ProjectionDeltaError::BodyTooLarge {
                len: bytes.len(),
                max: MAX_DOMAIN_EVENT_BODY_BYTES,
            });
        }
        Ok(bytes)
    }

    /// Reject replay under a different authorization scope.
    ///
    /// # Errors
    ///
    /// Returns [`ProjectionDeltaError::ReplayScopeMismatch`] when any
    /// role/application/generation identity differs.
    pub(crate) fn validate_replay_scope(
        &self,
        manifest: &crate::graphql::client_manifest::DistributedClientManifest,
        request: &impl super::lower::ProjectionDeltaRequestAuthority,
    ) -> Result<(), ProjectionDeltaError> {
        if self.identity.manifest_version != manifest.manifest_version
            || self.identity.client_protocol_version != manifest.protocol_version
            || self.identity.surface != ProjectionDeltaSurfaceIdentity::from(&manifest.surface)
            || self.identity.schema_fingerprint != manifest.schema_fingerprint
            || self.identity.protocol_fingerprint != manifest.protocol_fingerprint
            || self.identity.authorization_generation != request.authorization_generation()
            || self.identity.cache_scope_token != request.cache_scope().as_str()
            || self.identity.command_causation_id != request.command_causation_id().as_str()
        {
            return Err(ProjectionDeltaError::ReplayScopeMismatch);
        }
        Ok(())
    }

    pub(crate) fn validate(&self) -> Result<(), ProjectionDeltaError> {
        if self.wire_version != PROJECTION_DELTA_WIRE_VERSION {
            return Err(ProjectionDeltaError::UnsupportedVersion {
                actual: self.wire_version,
            });
        }
        self.identity.validate()?;
        if self.projections.is_empty()
            && (!self.occurrences.is_empty()
                || !self.operations.is_empty()
                || !self.recoveries.is_empty())
        {
            return Err(ProjectionDeltaError::InvalidOperation(
                "empty projection inventory requires an empty authoritative delta",
            ));
        }
        if self.projections.len() > MAX_PROJECTION_DELTA_OPERATIONS {
            return Err(ProjectionDeltaError::TooManyProjections {
                len: self.projections.len(),
                max: MAX_PROJECTION_DELTA_OPERATIONS,
            });
        }
        if !self.projections.windows(2).all(|pair| pair[0] < pair[1]) {
            return Err(ProjectionDeltaError::NonCanonicalOrder {
                field: "projections",
            });
        }
        for projection in &self.projections {
            projection.validate()?;
        }
        if self.operations.len() > MAX_PROJECTION_DELTA_OPERATIONS {
            return Err(ProjectionDeltaError::TooManyOperations {
                len: self.operations.len(),
                max: MAX_PROJECTION_DELTA_OPERATIONS,
            });
        }
        if self.occurrences.len() > MAX_PROJECTION_DELTA_OPERATIONS {
            return Err(ProjectionDeltaError::TooManyOccurrences {
                len: self.occurrences.len(),
                max: MAX_PROJECTION_DELTA_OPERATIONS,
            });
        }
        if self.recoveries.len() > MAX_PROJECTION_DELTA_OPERATIONS {
            return Err(ProjectionDeltaError::TooManyRecoveries {
                len: self.recoveries.len(),
                max: MAX_PROJECTION_DELTA_OPERATIONS,
            });
        }
        validate_occurrences(&self.occurrences, &self.identity.command_causation_id)?;
        let mut scopes = BTreeSet::new();
        for operation in &self.operations {
            operation.validate(self.projections.len(), self.occurrences.len())?;
            if !scopes.insert(operation.canonical_scope()) {
                return Err(ProjectionDeltaError::DuplicateScope);
            }
        }
        let mut recovery_targets = BTreeSet::new();
        for recovery in &self.recoveries {
            recovery.validate(self.projections.len(), self.occurrences.len())?;
            if !recovery_targets.insert(&recovery.target) {
                return Err(ProjectionDeltaError::DuplicateScope);
            }
            if recovery.condition == ProjectionDeltaRecoveryCondition::IfRecordMissing {
                let ProjectionDeltaRecoveryTarget::Record { scope } = &recovery.target else {
                    return Err(ProjectionDeltaError::InvalidOperation(
                        "if_record_missing recovery requires a record target",
                    ));
                };
                if !self.operations.iter().any(|operation| {
                    matches!(
                        &operation.mutation,
                        ProjectionDeltaMutation::Patch {
                            scope: patch_scope,
                            if_present: true,
                            ..
                        } if patch_scope == scope
                    )
                }) {
                    return Err(ProjectionDeltaError::InvalidOperation(
                        "if_record_missing recovery requires a same-scope conditional patch",
                    ));
                }
            }
        }
        if !self
            .operations
            .windows(2)
            .all(|pair| pair[0].canonical_order() < pair[1].canonical_order())
        {
            return Err(ProjectionDeltaError::NonCanonicalOrder {
                field: "operations",
            });
        }
        if !self
            .recoveries
            .windows(2)
            .all(|pair| pair[0].canonical_order() < pair[1].canonical_order())
        {
            return Err(ProjectionDeltaError::NonCanonicalOrder {
                field: "recoveries",
            });
        }
        Ok(())
    }
}

impl ProjectionDeltaIdentity {
    pub(crate) fn validate(&self) -> Result<(), ProjectionDeltaError> {
        if self.manifest_version
            != crate::graphql::client_manifest::DISTRIBUTED_CLIENT_MANIFEST_VERSION
        {
            return Err(ProjectionDeltaError::UnsupportedClientVersion {
                field: "manifest_version",
                actual: self.manifest_version,
            });
        }
        if self.client_protocol_version
            != crate::graphql::client_manifest::DISTRIBUTED_CLIENT_PROTOCOL_VERSION
        {
            return Err(ProjectionDeltaError::UnsupportedClientVersion {
                field: "client_protocol_version",
                actual: self.client_protocol_version,
            });
        }
        self.surface.validate()?;
        for (field, value) in [
            ("schema_fingerprint", self.schema_fingerprint.as_str()),
            ("protocol_fingerprint", self.protocol_fingerprint.as_str()),
            (
                "authorization_generation",
                self.authorization_generation.as_str(),
            ),
            ("cache_scope_token", self.cache_scope_token.as_str()),
            ("command_causation_id", self.command_causation_id.as_str()),
        ] {
            validate_identity(field, value)?;
        }
        ProjectionDeltaCacheScopeToken::parse_wire(&self.cache_scope_token)?;
        Ok(())
    }
}

impl From<&crate::graphql::client_manifest::ClientSurfaceIdentity>
    for ProjectionDeltaSurfaceIdentity
{
    fn from(surface: &crate::graphql::client_manifest::ClientSurfaceIdentity) -> Self {
        match surface {
            crate::graphql::client_manifest::ClientSurfaceIdentity::Role { name } => {
                Self::Role { name: name.clone() }
            }
            crate::graphql::client_manifest::ClientSurfaceIdentity::Application {
                name,
                eligible_roles,
                schema_roles,
            } => Self::Application {
                name: name.clone(),
                eligible_roles: eligible_roles.clone(),
                schema_roles: schema_roles.clone(),
            },
        }
    }
}

impl ProjectionDeltaSurfaceIdentity {
    fn validate(&self) -> Result<(), ProjectionDeltaError> {
        match self {
            Self::Role { name } => validate_identity("role surface", name),
            Self::Application {
                name,
                eligible_roles,
                schema_roles,
            } => {
                validate_identity("application surface", name)?;
                if eligible_roles.is_empty() {
                    return Err(ProjectionDeltaError::InvalidIdentity {
                        field: "application eligible roles",
                    });
                }
                validate_names("application eligible roles", eligible_roles)?;
                if schema_roles.is_empty() {
                    return Err(ProjectionDeltaError::InvalidIdentity {
                        field: "application schema roles",
                    });
                }
                validate_names("application schema roles", schema_roles)?;
                if schema_roles
                    .iter()
                    .any(|role| !eligible_roles.iter().any(|eligible| eligible == role))
                {
                    return Err(ProjectionDeltaError::InvalidIdentity {
                        field: "application schema roles",
                    });
                }
                Ok(())
            }
        }
    }
}

impl ProjectionDeltaProjectionIdentity {
    pub(crate) fn validate(&self) -> Result<(), ProjectionDeltaError> {
        for (field, value) in [
            ("program_id", self.program_id.as_str()),
            ("binding_id", self.binding_id.as_str()),
            ("epoch", self.epoch.as_str()),
        ] {
            validate_identity(field, value)?;
        }
        if self.program_ir_version != crate::projection::PROJECTION_PROGRAM_IR_VERSION {
            return Err(ProjectionDeltaError::UnsupportedExecutableVersion {
                field: "program_ir_version",
                actual: self.program_ir_version,
            });
        }
        if self.operation_semantics_version
            != crate::projection::PROJECTION_OPERATION_SEMANTICS_VERSION
        {
            return Err(ProjectionDeltaError::UnsupportedExecutableVersion {
                field: "operation_semantics_version",
                actual: self.operation_semantics_version,
            });
        }
        crate::ProjectionProgramId::parse(&self.program_id).map_err(|_| {
            ProjectionDeltaError::InvalidIdentity {
                field: "program_id",
            }
        })?;
        crate::projection::placement::ProjectionBindingId::parse(&self.binding_id).map_err(
            |_| ProjectionDeltaError::InvalidIdentity {
                field: "binding_id",
            },
        )?;
        crate::projection_protocol::ProjectionEpoch::new(&self.epoch)
            .map_err(|_| ProjectionDeltaError::InvalidIdentity { field: "epoch" })?;
        Ok(())
    }
}

/// Stable occurrence ordering retained on every command delta.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionDeltaOccurrence {
    pub causation_id: String,
    pub ordinal: u32,
    pub occurrence_id: String,
}

/// A complete normalized-record scope.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionDeltaScope {
    pub partition: ProjectionDeltaPartition,
    pub model: String,
    pub key: Vec<DeltaKeyField>,
}

impl ProjectionDeltaScope {
    pub(crate) fn validate(&self) -> Result<(), ProjectionDeltaError> {
        self.partition.validate()?;
        validate_identity("model", &self.model)?;
        validate_key(&self.key)
    }
}

/// Role-safe projection partition. Arbitrary logical values are replaced by
/// an authorizer-produced opaque token.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProjectionDeltaPartition {
    Unit,
    Opaque { token: String },
}

impl ProjectionDeltaPartition {
    pub(crate) fn validate(&self) -> Result<(), ProjectionDeltaError> {
        let encoded = serde_json::to_vec(self)
            .map_err(|error| ProjectionDeltaError::InvalidWire(error.to_string()))?;
        if encoded.len() > MAX_PROJECTION_PARTITION_BYTES {
            return Err(ProjectionDeltaError::PartitionTooLarge {
                len: encoded.len(),
                max: MAX_PROJECTION_PARTITION_BYTES,
            });
        }
        if let Self::Opaque { token } = self {
            validate_identity("opaque partition", token)?;
            validate_opaque_protocol_token(token, Some("projection-partition"))?;
        }
        Ok(())
    }
}

/// One authorized component of a composite normalized key.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeltaKeyField {
    pub ordinal: u32,
    pub field: String,
    pub value: DeltaValue,
}

/// One authorized record-field assignment.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeltaField {
    pub field: String,
    pub value: DeltaValue,
}

/// Strict tagged value. Null is a value; omitted fields are unknown; unset is
/// represented by the patch operation's independent `unset` list.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum DeltaValue {
    Null,
    Boolean(bool),
    I64(String),
    U64(String),
    F64(String),
    String(String),
    Enum { enum_type: String, variant: String },
    List(Vec<DeltaValue>),
    Object(Vec<DeltaField>),
}

impl DeltaValue {
    pub(crate) fn try_from_projection_ref(
        value: crate::ProjectionValueRef<'_>,
    ) -> Result<Self, ProjectionDeltaError> {
        let value = match value {
            crate::ProjectionValueRef::Null => Self::Null,
            crate::ProjectionValueRef::Boolean(value) => Self::Boolean(value),
            crate::ProjectionValueRef::I64(value) => Self::I64(value.to_owned()),
            crate::ProjectionValueRef::U64(value) => Self::U64(value.to_owned()),
            crate::ProjectionValueRef::F64(value) => Self::F64(value.to_owned()),
            crate::ProjectionValueRef::String(value) => Self::String(value.to_owned()),
            crate::ProjectionValueRef::Enum { enum_type, variant } => Self::Enum {
                enum_type: enum_type.to_owned(),
                variant: variant.to_owned(),
            },
            crate::ProjectionValueRef::List(values) => Self::List(
                values
                    .iter()
                    .map(|value| Self::try_from_projection_ref(value.as_ref()))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            crate::ProjectionValueRef::Object(fields) => Self::Object(
                fields
                    .iter()
                    .map(|field| {
                        Ok(DeltaField {
                            field: field.name().to_owned(),
                            value: Self::try_from_projection_ref(field.value().as_ref())?,
                        })
                    })
                    .collect::<Result<Vec<_>, ProjectionDeltaError>>()?,
            ),
        };
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn validate(&self) -> Result<(), ProjectionDeltaError> {
        self.validate_at_depth(1)
    }

    fn validate_at_depth(&self, depth: usize) -> Result<(), ProjectionDeltaError> {
        if depth > MAX_PROJECTION_EXPRESSION_DEPTH {
            return Err(ProjectionDeltaError::ValueTooDeep {
                depth,
                max: MAX_PROJECTION_EXPRESSION_DEPTH,
            });
        }
        match self {
            Self::Null | Self::Boolean(_) => Ok(()),
            Self::I64(value) => {
                let parsed = value
                    .parse::<i64>()
                    .map_err(|_| ProjectionDeltaError::InvalidNumber)?;
                if parsed.to_string() != *value {
                    return Err(ProjectionDeltaError::InvalidNumber);
                }
                Ok(())
            }
            Self::U64(value) => {
                let parsed = value
                    .parse::<u64>()
                    .map_err(|_| ProjectionDeltaError::InvalidNumber)?;
                if parsed.to_string() != *value {
                    return Err(ProjectionDeltaError::InvalidNumber);
                }
                Ok(())
            }
            Self::F64(value) => {
                let parsed = value
                    .parse::<f64>()
                    .map_err(|_| ProjectionDeltaError::InvalidNumber)?;
                let canonical =
                    serde_json::Number::from_f64(if parsed == 0.0 { 0.0 } else { parsed })
                        .ok_or(ProjectionDeltaError::InvalidNumber)?
                        .to_string();
                if canonical != *value {
                    return Err(ProjectionDeltaError::InvalidNumber);
                }
                Ok(())
            }
            Self::String(value) => validate_payload_string(value),
            Self::Enum { enum_type, variant } => {
                validate_identity("enum_type", enum_type)?;
                validate_identity("enum_variant", variant)
            }
            Self::List(values) => {
                for value in values {
                    value.validate_at_depth(depth + 1)?;
                }
                Ok(())
            }
            Self::Object(fields) => {
                validate_fields(fields)?;
                for field in fields {
                    field.value.validate_at_depth(depth + 1)?;
                }
                Ok(())
            }
        }
    }
}

/// Whether provenance is an applied event or a compiler preview.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProjectionMutationSource {
    Actual,
    Preview,
}

/// Authorization visibility independent of record existence or liveness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProjectionDeltaVisibility {
    Authorized,
    Denied,
    Unknown,
}

/// Explicit authorization transition. A domain delete under stable permission
/// is `Authorized` to `Authorized`; it is not an authorization transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AuthorizationTransition {
    pub before: ProjectionDeltaVisibility,
    pub after: ProjectionDeltaVisibility,
}

/// One final operation with exact contributing projection identities.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionDeltaOperation {
    pub occurrence_ordinal: u32,
    pub projection_refs: Vec<u32>,
    pub mutation: ProjectionDeltaMutation,
}

/// Closed portable client mutation set.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProjectionDeltaMutation {
    Upsert {
        scope: ProjectionDeltaScope,
        fields: Vec<DeltaField>,
        replace: Vec<String>,
    },
    Patch {
        scope: ProjectionDeltaScope,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        set: Vec<DeltaField>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        unset: Vec<String>,
        if_present: bool,
    },
    Delete {
        scope: ProjectionDeltaScope,
    },
    Link {
        relationship: String,
        source: ProjectionDeltaScope,
        target: ProjectionDeltaScope,
    },
    Unlink {
        relationship: String,
        source: ProjectionDeltaScope,
        target: ProjectionDeltaScope,
    },
    InvalidateModel {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        partition: Option<ProjectionDeltaPartition>,
        model: String,
    },
    InvalidateRelationship {
        relationship: String,
        source: ProjectionDeltaScope,
    },
}

impl ProjectionDeltaOperation {
    pub(crate) fn occurrence_ordinal(&self) -> u32 {
        self.occurrence_ordinal
    }

    pub(crate) fn validate(
        &self,
        projection_count: usize,
        occurrence_count: usize,
    ) -> Result<(), ProjectionDeltaError> {
        validate_refs(&self.projection_refs, projection_count)?;
        if self.occurrence_ordinal as usize >= occurrence_count {
            return Err(ProjectionDeltaError::InvalidOperation(
                "operation references an unknown occurrence",
            ));
        }
        match &self.mutation {
            ProjectionDeltaMutation::Upsert {
                scope,
                fields,
                replace,
            } => {
                scope.validate()?;
                validate_fields(fields)?;
                validate_names("replace", replace)?;
                if fields.iter().any(|field| !replace.contains(&field.field)) {
                    return Err(ProjectionDeltaError::InvalidOperation(
                        "upsert fields must be contained in the replacement mask",
                    ));
                }
            }
            ProjectionDeltaMutation::Patch {
                scope,
                set,
                unset,
                if_present,
            } => {
                scope.validate()?;
                validate_fields(set)?;
                validate_names("unset", unset)?;
                if !if_present || (set.is_empty() && unset.is_empty()) {
                    return Err(ProjectionDeltaError::InvalidOperation(
                        "patch must be conditional and non-empty",
                    ));
                }
                if set.iter().any(|field| unset.contains(&field.field)) {
                    return Err(ProjectionDeltaError::InvalidOperation(
                        "patch cannot set and unset one field",
                    ));
                }
            }
            ProjectionDeltaMutation::Delete { scope } => scope.validate()?,
            ProjectionDeltaMutation::Link {
                relationship,
                source,
                target,
            }
            | ProjectionDeltaMutation::Unlink {
                relationship,
                source,
                target,
            } => {
                validate_identity("relationship", relationship)?;
                source.validate()?;
                target.validate()?;
            }
            ProjectionDeltaMutation::InvalidateModel { partition, model } => {
                if let Some(partition) = partition {
                    partition.validate()?;
                }
                validate_identity("model", model)?
            }
            ProjectionDeltaMutation::InvalidateRelationship {
                relationship,
                source,
            } => {
                validate_identity("relationship", relationship)?;
                source.validate()?;
            }
        }
        Ok(())
    }

    pub(crate) fn canonical_scope(&self) -> OperationScope {
        match &self.mutation {
            ProjectionDeltaMutation::Upsert { scope, .. }
            | ProjectionDeltaMutation::Patch { scope, .. }
            | ProjectionDeltaMutation::Delete { scope } => OperationScope::Record(scope.clone()),
            ProjectionDeltaMutation::Link {
                relationship,
                source,
                target,
            }
            | ProjectionDeltaMutation::Unlink {
                relationship,
                source,
                target,
            } => OperationScope::Edge {
                relationship: relationship.clone(),
                source: source.clone(),
                target: target.clone(),
            },
            ProjectionDeltaMutation::InvalidateModel { partition, model } => {
                OperationScope::Model {
                    partition: partition.clone(),
                    model: model.clone(),
                }
            }
            ProjectionDeltaMutation::InvalidateRelationship {
                relationship,
                source,
            } => OperationScope::Relationship {
                relationship: relationship.clone(),
                source: source.clone(),
            },
        }
    }

    pub(crate) fn canonical_order(&self) -> (OperationScope, u32) {
        (self.canonical_scope(), self.occurrence_ordinal())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum OperationScope {
    Record(ProjectionDeltaScope),
    Edge {
        relationship: String,
        source: ProjectionDeltaScope,
        target: ProjectionDeltaScope,
    },
    Model {
        partition: Option<ProjectionDeltaPartition>,
        model: String,
    },
    Relationship {
        relationship: String,
        source: ProjectionDeltaScope,
    },
}

/// A conservative client recovery request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionDeltaRecovery {
    pub occurrence_ordinal: u32,
    pub projection_refs: Vec<u32>,
    pub condition: ProjectionDeltaRecoveryCondition,
    pub target: ProjectionDeltaRecoveryTarget,
}

/// When a conservative recovery is applied by the client.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionDeltaRecoveryCondition {
    /// Apply immediately.
    Always,
    /// Apply only when the paired same-record conditional patch finds no live
    /// row (including a tombstone).
    IfRecordMissing,
}

impl ProjectionDeltaRecovery {
    fn validate(
        &self,
        projection_count: usize,
        occurrence_count: usize,
    ) -> Result<(), ProjectionDeltaError> {
        validate_refs(&self.projection_refs, projection_count)?;
        if self.occurrence_ordinal as usize >= occurrence_count {
            return Err(ProjectionDeltaError::InvalidOperation(
                "recovery references an unknown occurrence",
            ));
        }
        self.target.validate()
    }

    pub(crate) fn canonical_order(&self) -> (ProjectionDeltaRecoveryTarget, u32) {
        (self.target.clone(), self.occurrence_ordinal)
    }
}

/// Narrowest safely addressable recovery scope.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProjectionDeltaRecoveryTarget {
    Record {
        scope: ProjectionDeltaScope,
    },
    Relationship {
        relationship: String,
        source: ProjectionDeltaScope,
    },
    Model {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        partition: Option<ProjectionDeltaPartition>,
        model: String,
    },
}

impl ProjectionDeltaRecoveryTarget {
    fn validate(&self) -> Result<(), ProjectionDeltaError> {
        match self {
            Self::Record { scope } => scope.validate(),
            Self::Relationship {
                relationship,
                source,
            } => {
                validate_identity("relationship", relationship)?;
                source.validate()
            }
            Self::Model { partition, model } => {
                if let Some(partition) = partition {
                    partition.validate()?;
                }
                validate_identity("model", model)
            }
        }
    }
}

/// Projection-delta validation and lowering failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProjectionDeltaError {
    InvalidWire(String),
    UnsupportedVersion { actual: u16 },
    UnsupportedClientVersion { field: &'static str, actual: u32 },
    UnsupportedExecutableVersion { field: &'static str, actual: u16 },
    ZeroVersion { field: &'static str },
    InvalidIdentity { field: &'static str },
    ReplayScopeMismatch,
    TooManyOperations { len: usize, max: usize },
    TooManyProjections { len: usize, max: usize },
    TooManyOccurrences { len: usize, max: usize },
    TooManyRecoveries { len: usize, max: usize },
    BodyTooLarge { len: usize, max: usize },
    ValueTooDeep { depth: usize, max: usize },
    InvalidNumber,
    KeyTooLarge { len: usize, max: usize },
    PartitionTooLarge { len: usize, max: usize },
    DuplicateScope,
    NonCanonicalOrder { field: &'static str },
    InvalidOperation(&'static str),
    IneligibleBinding,
    ProjectionIdentityMismatch,
    AuthorizationMapping,
}

impl std::fmt::Display for ProjectionDeltaError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidWire(error) => {
                write!(formatter, "invalid projection-delta wire payload: {error}")
            }
            Self::UnsupportedVersion { actual } => {
                write!(
                    formatter,
                    "unsupported projection-delta wire version `{actual}`"
                )
            }
            Self::UnsupportedClientVersion { field, actual } => write!(
                formatter,
                "unsupported projection-delta client `{field}` version `{actual}`"
            ),
            Self::UnsupportedExecutableVersion { field, actual } => write!(
                formatter,
                "unsupported projection-delta executable `{field}` version `{actual}`"
            ),
            Self::ZeroVersion { field } => {
                write!(formatter, "projection-delta `{field}` must be non-zero")
            }
            Self::InvalidIdentity { field } => {
                write!(formatter, "invalid projection-delta identity `{field}`")
            }
            Self::ReplayScopeMismatch => formatter.write_str(
                "projection-delta replay scope does not match the current authorization scope",
            ),
            Self::TooManyOperations { len, max } => write!(
                formatter,
                "projection-delta has {len} operations, exceeding the maximum of {max}"
            ),
            Self::TooManyProjections { len, max } => write!(
                formatter,
                "projection-delta has {len} projections, exceeding the maximum of {max}"
            ),
            Self::TooManyOccurrences { len, max } => write!(
                formatter,
                "projection-delta has {len} occurrences, exceeding the maximum of {max}"
            ),
            Self::TooManyRecoveries { len, max } => write!(
                formatter,
                "projection-delta has {len} recoveries, exceeding the maximum of {max}"
            ),
            Self::BodyTooLarge { len, max } => write!(
                formatter,
                "projection-delta body is {len} bytes, exceeding the maximum of {max}"
            ),
            Self::ValueTooDeep { depth, max } => write!(
                formatter,
                "projection-delta value depth {depth} exceeds the maximum of {max}"
            ),
            Self::InvalidNumber => formatter
                .write_str("projection-delta contains a noncanonical or out-of-range number"),
            Self::KeyTooLarge { len, max } => write!(
                formatter,
                "projection-delta key is {len} bytes, exceeding the maximum of {max}"
            ),
            Self::PartitionTooLarge { len, max } => write!(
                formatter,
                "projection-delta partition is {len} bytes, exceeding the maximum of {max}"
            ),
            Self::DuplicateScope => {
                formatter.write_str("projection-delta contains duplicate mutation scopes")
            }
            Self::NonCanonicalOrder { field } => {
                write!(
                    formatter,
                    "projection-delta `{field}` order is noncanonical"
                )
            }
            Self::InvalidOperation(message) => {
                write!(formatter, "invalid projection-delta operation: {message}")
            }
            Self::IneligibleBinding => {
                formatter.write_str("projection binding is not an eligible eventual causal binding")
            }
            Self::ProjectionIdentityMismatch => {
                formatter.write_str("projection plan identity does not match the selected binding")
            }
            Self::AuthorizationMapping => formatter
                .write_str("projection authorization denied or could not map a required identity"),
        }
    }
}

impl std::error::Error for ProjectionDeltaError {}

fn validate_occurrences(
    occurrences: &[ProjectionDeltaOccurrence],
    causation_id: &str,
) -> Result<(), ProjectionDeltaError> {
    let mut ids = BTreeSet::new();
    for (expected, occurrence) in occurrences.iter().enumerate() {
        validate_identity("occurrence_id", &occurrence.occurrence_id)?;
        if occurrence.causation_id != causation_id || occurrence.ordinal as usize != expected {
            return Err(ProjectionDeltaError::NonCanonicalOrder {
                field: "occurrences",
            });
        }
        if !ids.insert(&occurrence.occurrence_id) {
            return Err(ProjectionDeltaError::InvalidOperation(
                "occurrence IDs must be unique",
            ));
        }
    }
    Ok(())
}

fn validate_key(fields: &[DeltaKeyField]) -> Result<(), ProjectionDeltaError> {
    if fields.is_empty() {
        return Err(ProjectionDeltaError::InvalidOperation(
            "record key must be non-empty",
        ));
    }
    let mut names = BTreeSet::new();
    for (expected, field) in fields.iter().enumerate() {
        validate_identity("key field", &field.field)?;
        if field.ordinal as usize != expected || !names.insert(&field.field) {
            return Err(ProjectionDeltaError::NonCanonicalOrder { field: "key" });
        }
        field.value.validate()?;
        if matches!(
            field.value,
            DeltaValue::Null | DeltaValue::List(_) | DeltaValue::Object(_)
        ) {
            return Err(ProjectionDeltaError::InvalidOperation(
                "record key values must be non-null scalars",
            ));
        }
    }
    let bytes = serde_json::to_vec(fields)
        .map_err(|error| ProjectionDeltaError::InvalidWire(error.to_string()))?;
    if bytes.len() > MAX_PROJECTION_RECORD_KEY_BYTES {
        return Err(ProjectionDeltaError::KeyTooLarge {
            len: bytes.len(),
            max: MAX_PROJECTION_RECORD_KEY_BYTES,
        });
    }
    Ok(())
}

fn validate_fields(fields: &[DeltaField]) -> Result<(), ProjectionDeltaError> {
    for field in fields {
        validate_identity("field", &field.field)?;
        field.value.validate()?;
    }
    if !fields.windows(2).all(|pair| pair[0].field < pair[1].field) {
        return Err(ProjectionDeltaError::NonCanonicalOrder { field: "fields" });
    }
    Ok(())
}

fn validate_names(field: &'static str, names: &[String]) -> Result<(), ProjectionDeltaError> {
    for name in names {
        validate_identity(field, name)?;
    }
    if !names.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(ProjectionDeltaError::NonCanonicalOrder { field });
    }
    Ok(())
}

fn validate_identity(field: &'static str, value: &str) -> Result<(), ProjectionDeltaError> {
    if value.is_empty() || value.len() > MAX_IDENTITY_BYTES || value.trim() != value {
        return Err(ProjectionDeltaError::InvalidIdentity { field });
    }
    Ok(())
}

fn validate_refs(refs: &[u32], projection_count: usize) -> Result<(), ProjectionDeltaError> {
    if refs.is_empty()
        || refs.iter().any(|index| *index as usize >= projection_count)
        || !refs.windows(2).all(|pair| pair[0] < pair[1])
    {
        return Err(ProjectionDeltaError::InvalidOperation(
            "projection references must be non-empty, ordered, unique, and in range",
        ));
    }
    Ok(())
}

fn validate_payload_string(value: &str) -> Result<(), ProjectionDeltaError> {
    if value.len() > MAX_DOMAIN_EVENT_BODY_BYTES {
        return Err(ProjectionDeltaError::BodyTooLarge {
            len: value.len(),
            max: MAX_DOMAIN_EVENT_BODY_BYTES,
        });
    }
    Ok(())
}
