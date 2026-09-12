//! Adapter-neutral identities and ordering vocabulary for durable projections.
//!
//! These types deliberately separate four different notions of progress:
//!
//! - [`ProjectionInputCursor`] orders trusted inputs from one exact source;
//! - [`SourceSnapshotVersion`] fences snapshots from one aggregate stream;
//! - [`RecordRevision`] orders versions of one exact projected record; and
//! - [`ProjectionChangeCursor`] orders durable changes emitted by a projector.
//!
//! None of them derives order from message IDs, timestamps, or arrival order.
//! Cross-scope values are explicitly incomparable.

use std::cmp::Ordering;
use std::fmt;
use std::num::{NonZeroU32, NonZeroU64};

use serde::{Deserialize, Serialize};

mod codec;
mod source_snapshot;
mod store;
mod workspace;
pub(crate) use source_snapshot::validate_snapshot_write;
pub use source_snapshot::SourceSnapshotVersion;

pub(crate) use codec::{
    canonical_projection_topology_bytes, compile_projection_topology, digest_projection_binding,
    CompiledProjectionTopology, ProjectionPartitionSpec, ProjectionScopeCodec,
};
use store::domain_separated_digest;
pub use store::*;
pub use workspace::ProjectionWorkspace;

/// Maximum UTF-8 byte length of a projector topology name.
pub const MAX_PROJECTOR_NAME_BYTES: usize = 128;
/// Maximum UTF-8 byte length of a projection source name.
pub const MAX_PROJECTION_SOURCE_NAME_BYTES: usize = 128;
/// Maximum UTF-8 byte length of a projected model name.
pub const MAX_PROJECTION_MODEL_NAME_BYTES: usize = 128;
/// Maximum UTF-8 byte length of an opaque cursor epoch.
pub const MAX_PROJECTION_EPOCH_BYTES: usize = 128;
/// Maximum length of one canonical projection-partition encoding.
pub const MAX_PROJECTION_PARTITION_BYTES: usize = 4 * 1024;
/// Maximum length of one canonical source-partition encoding.
pub const MAX_PROJECTION_SOURCE_PARTITION_BYTES: usize = 4 * 1024;
/// Maximum length of one canonical projected-record key encoding.
pub const MAX_PROJECTION_RECORD_KEY_BYTES: usize = 4 * 1024;
/// Largest cursor/revision/generation value representable identically by every
/// supported durable adapter (`INTEGER`/`BIGINT`).
pub const MAX_PROJECTION_POSITION: u64 = i64::MAX as u64;

const PROJECTION_PARTITION_DIGEST_DOMAIN: &[u8] = b"distributed.projection.partition.v1\0";
const PROJECTOR_TOPOLOGY_IDENTITY_ENCODING_DOMAIN: &[u8] =
    b"distributed.projection.topology-identity.v1\0";
const PROJECTION_SOURCE_NAME_ENCODING_DOMAIN: &[u8] =
    b"distributed.projection.source-name-encoding.v1\0";
const PROJECTION_SOURCE_NAME_DIGEST_DOMAIN: &[u8] = b"distributed.projection.source-name.v1\0";
const PROJECTION_SOURCE_PARTITION_DIGEST_DOMAIN: &[u8] =
    b"distributed.projection.source-partition.v1\0";
const PROJECTION_RECORD_KEY_DIGEST_DOMAIN: &[u8] = b"distributed.projection.record-key.v1\0";

/// One declaration-owned projection obligation after every portable command
/// expression has been resolved against the retained canonical GraphQL input.
///
/// The command ledger persists this adapter-neutral value. Projector
/// registration later lowers it through the same scope codec that encodes
/// projector-side rows, so there is no second string or key interpretation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ResolvedProjectionObligation {
    pub(crate) projector: String,
    pub(crate) model: String,
    pub(crate) key: ResolvedProjectionKey,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_json_value"
    )]
    pub(crate) partition: Option<serde_json::Value>,
    /// Canonical topology/partition/key identity computed by the bound compiler
    /// before ledger I/O. Consumers validate the logical fields against this
    /// exact scope; they never rebind old strings under a newer topology.
    pub(crate) scope: ProjectionRecordScope,
}

fn deserialize_present_json_value<'de, D>(
    deserializer: D,
) -> Result<Option<serde_json::Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    serde_json::Value::deserialize(deserializer).map(Some)
}

/// Complete projection key in declaration order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ResolvedProjectionKey {
    pub(crate) fields: Vec<ResolvedProjectionKeyField>,
}

/// One resolved field in a complete projection key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ResolvedProjectionKeyField {
    pub(crate) field: String,
    pub(crate) value: serde_json::Value,
}

/// Invalid adapter-neutral projection protocol input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProjectionProtocolValidationError {
    /// A required string or canonical byte sequence was empty.
    Empty { field: &'static str },
    /// A bounded value exceeded its maximum UTF-8/byte length.
    TooLong {
        field: &'static str,
        len: usize,
        max: usize,
    },
    /// An identity name contained whitespace or a control character.
    InvalidNameCharacter {
        field: &'static str,
        byte_index: usize,
        character: char,
    },
    /// An opaque string contained a control character.
    InvalidOpaqueCharacter {
        field: &'static str,
        byte_index: usize,
        character: char,
    },
    /// A value whose protocol domain starts at one was zero.
    Zero { field: &'static str },
    /// A numeric protocol value exceeded the cross-adapter signed-BIGINT range.
    TooLarge {
        field: &'static str,
        value: u64,
        max: u64,
    },
    /// Two values combined into one protocol object did not share its scope.
    ScopeMismatch { field: &'static str },
    /// Persisted canonical bytes do not conform to the named versioned format.
    MalformedCanonicalEncoding { field: &'static str },
}

impl fmt::Display for ProjectionProtocolValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty { field } => write!(formatter, "{field} must not be empty"),
            Self::TooLong { field, len, max } => {
                write!(
                    formatter,
                    "{field} is {len} bytes, exceeding the maximum of {max}"
                )
            }
            Self::InvalidNameCharacter {
                field,
                byte_index,
                character,
            } => write!(
                formatter,
                "{field} contains invalid character {:?} at byte {byte_index}",
                character.escape_default().to_string()
            ),
            Self::InvalidOpaqueCharacter {
                field,
                byte_index,
                character,
            } => write!(
                formatter,
                "{field} contains control character {:?} at byte {byte_index}",
                character.escape_default().to_string()
            ),
            Self::Zero { field } => write!(formatter, "{field} must be greater than zero"),
            Self::TooLarge { field, value, max } => {
                write!(
                    formatter,
                    "{field} value {value} exceeds the maximum of {max}"
                )
            }
            Self::ScopeMismatch { field } => {
                write!(
                    formatter,
                    "{field} does not match the enclosing projection scope"
                )
            }
            Self::MalformedCanonicalEncoding { field } => {
                write!(formatter, "{field} has malformed canonical encoding")
            }
        }
    }
}

impl std::error::Error for ProjectionProtocolValidationError {}

/// Stable identity of one versioned projector topology declaration.
///
/// The digest is supplied by the topology compiler because this foundational
/// layer does not know the declaration's canonical facts/models encoding.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProjectorTopologyId {
    version: NonZeroU32,
    name: String,
    digest: [u8; 32],
}

impl ProjectorTopologyId {
    /// Build a topology identity from its validated version, name, and
    /// compiler-produced SHA-256 digest.
    pub fn new(
        version: u32,
        name: impl Into<String>,
        digest: [u8; 32],
    ) -> Result<Self, ProjectionProtocolValidationError> {
        let version = NonZeroU32::new(version).ok_or(ProjectionProtocolValidationError::Zero {
            field: "projector topology version",
        })?;
        let name = validate_name(
            "projector topology name",
            name.into(),
            MAX_PROJECTOR_NAME_BYTES,
        )?;
        Ok(Self {
            version,
            name,
            digest,
        })
    }

    pub fn version(&self) -> u32 {
        self.version.get()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }

    /// Stable database identity bytes for this exact compiled topology.
    ///
    /// The encoding is domain/version tagged and length-prefixes every
    /// component, including the fixed-width values, so future formats cannot
    /// alias this one by concatenation.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let version = self.version.get().to_be_bytes();
        let mut bytes = Vec::with_capacity(
            PROJECTOR_TOPOLOGY_IDENTITY_ENCODING_DOMAIN.len()
                + (3 * std::mem::size_of::<u64>())
                + version.len()
                + self.name.len()
                + self.digest.len(),
        );
        bytes.extend_from_slice(PROJECTOR_TOPOLOGY_IDENTITY_ENCODING_DOMAIN);
        append_length_prefixed(&mut bytes, &version);
        append_length_prefixed(&mut bytes, self.name.as_bytes());
        append_length_prefixed(&mut bytes, &self.digest);
        bytes
    }

    /// Reconstruct an exact compiler identity from adapter-owned canonical
    /// bytes.
    ///
    /// This remains crate-private: public callers select registered projector
    /// declarations and cannot mint topology authority from database-shaped
    /// bytes.
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    pub(crate) fn from_canonical_bytes(
        canonical_bytes: &[u8],
    ) -> Result<Self, ProjectionProtocolValidationError> {
        let encoded = canonical_bytes
            .strip_prefix(PROJECTOR_TOPOLOGY_IDENTITY_ENCODING_DOMAIN)
            .ok_or(
                ProjectionProtocolValidationError::MalformedCanonicalEncoding {
                    field: "projector topology identity",
                },
            )?;
        let (version, encoded) = take_length_prefixed(encoded).ok_or(
            ProjectionProtocolValidationError::MalformedCanonicalEncoding {
                field: "projector topology identity",
            },
        )?;
        let (name, encoded) = take_length_prefixed(encoded).ok_or(
            ProjectionProtocolValidationError::MalformedCanonicalEncoding {
                field: "projector topology identity",
            },
        )?;
        let (digest, trailing) = take_length_prefixed(encoded).ok_or(
            ProjectionProtocolValidationError::MalformedCanonicalEncoding {
                field: "projector topology identity",
            },
        )?;
        let version: [u8; std::mem::size_of::<u32>()] = version.try_into().map_err(|_| {
            ProjectionProtocolValidationError::MalformedCanonicalEncoding {
                field: "projector topology identity",
            }
        })?;
        let name = std::str::from_utf8(name).map_err(|_| {
            ProjectionProtocolValidationError::MalformedCanonicalEncoding {
                field: "projector topology identity",
            }
        })?;
        let digest: [u8; 32] = digest.try_into().map_err(|_| {
            ProjectionProtocolValidationError::MalformedCanonicalEncoding {
                field: "projector topology identity",
            }
        })?;
        if !trailing.is_empty() {
            return Err(
                ProjectionProtocolValidationError::MalformedCanonicalEncoding {
                    field: "projector topology identity",
                },
            );
        }
        let topology = Self::new(u32::from_be_bytes(version), name, digest)?;
        if topology.canonical_bytes() != canonical_bytes {
            return Err(
                ProjectionProtocolValidationError::MalformedCanonicalEncoding {
                    field: "projector topology identity",
                },
            );
        }
        Ok(topology)
    }
}

impl Serialize for ProjectorTopologyId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct Wire<'a> {
            version: u32,
            name: &'a str,
            digest: [u8; 32],
        }
        Wire {
            version: self.version(),
            name: self.name(),
            digest: self.digest(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ProjectorTopologyId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            version: u32,
            name: String,
            digest: [u8; 32],
        }
        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.version, wire.name, wire.digest).map_err(serde::de::Error::custom)
    }
}

/// Canonical projector partition and its domain-separated digest.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProjectionPartition {
    canonical_bytes: Vec<u8>,
    digest: [u8; 32],
}

impl ProjectionPartition {
    pub fn new(
        canonical_bytes: impl Into<Vec<u8>>,
    ) -> Result<Self, ProjectionProtocolValidationError> {
        let canonical_bytes = validate_canonical_bytes(
            "projection partition",
            canonical_bytes.into(),
            MAX_PROJECTION_PARTITION_BYTES,
        )?;
        let digest = domain_separated_digest(PROJECTION_PARTITION_DIGEST_DOMAIN, &canonical_bytes);
        Ok(Self {
            canonical_bytes,
            digest,
        })
    }

    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

impl Serialize for ProjectionPartition {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.canonical_bytes.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ProjectionPartition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let canonical_bytes = Vec::<u8>::deserialize(deserializer)?;
        Self::new(canonical_bytes).map_err(serde::de::Error::custom)
    }
}

/// One ordered projection input source.
///
/// `name` identifies the source domain (for example an aggregate type), while
/// `canonical_partition_bytes` identifies one ordered stream within it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProjectionSource {
    name: String,
    name_digest: [u8; 32],
    canonical_partition_bytes: Vec<u8>,
    partition_digest: [u8; 32],
}

impl ProjectionSource {
    pub fn new(
        name: impl Into<String>,
        canonical_partition_bytes: impl Into<Vec<u8>>,
    ) -> Result<Self, ProjectionProtocolValidationError> {
        let name = validate_name(
            "projection source name",
            name.into(),
            MAX_PROJECTION_SOURCE_NAME_BYTES,
        )?;
        let canonical_partition_bytes = validate_canonical_bytes(
            "projection source partition",
            canonical_partition_bytes.into(),
            MAX_PROJECTION_SOURCE_PARTITION_BYTES,
        )?;
        let name_digest =
            domain_separated_digest(PROJECTION_SOURCE_NAME_DIGEST_DOMAIN, name.as_bytes());
        let partition_digest = domain_separated_digest(
            PROJECTION_SOURCE_PARTITION_DIGEST_DOMAIN,
            &canonical_partition_bytes,
        );
        Ok(Self {
            name,
            name_digest,
            canonical_partition_bytes,
            partition_digest,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Domain/version-tagged canonical bytes for the source name.
    pub fn canonical_name_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(
            PROJECTION_SOURCE_NAME_ENCODING_DOMAIN.len()
                + std::mem::size_of::<u64>()
                + self.name.len(),
        );
        bytes.extend_from_slice(PROJECTION_SOURCE_NAME_ENCODING_DOMAIN);
        append_length_prefixed(&mut bytes, self.name.as_bytes());
        bytes
    }

    /// Reconstruct a source identity from the exact bytes stored by adapters.
    ///
    /// This is crate-private because source adapters mint trusted identities;
    /// public callers cannot turn arbitrary database-shaped bytes into cursor
    /// authority.
    pub(crate) fn from_canonical_name_bytes(
        canonical_name_bytes: &[u8],
        canonical_partition_bytes: impl Into<Vec<u8>>,
    ) -> Result<Self, ProjectionProtocolValidationError> {
        let encoded_name = canonical_name_bytes
            .strip_prefix(PROJECTION_SOURCE_NAME_ENCODING_DOMAIN)
            .ok_or(
                ProjectionProtocolValidationError::MalformedCanonicalEncoding {
                    field: "projection source name",
                },
            )?;
        let (name, trailing) = take_length_prefixed(encoded_name).ok_or(
            ProjectionProtocolValidationError::MalformedCanonicalEncoding {
                field: "projection source name",
            },
        )?;
        if !trailing.is_empty() {
            return Err(
                ProjectionProtocolValidationError::MalformedCanonicalEncoding {
                    field: "projection source name",
                },
            );
        }
        let name = std::str::from_utf8(name).map_err(|_| {
            ProjectionProtocolValidationError::MalformedCanonicalEncoding {
                field: "projection source name",
            }
        })?;
        let source = Self::new(name, canonical_partition_bytes)?;
        if source.canonical_name_bytes() != canonical_name_bytes {
            return Err(
                ProjectionProtocolValidationError::MalformedCanonicalEncoding {
                    field: "projection source name",
                },
            );
        }
        Ok(source)
    }

    /// Domain-separated digest of the source name.
    ///
    /// This is distinct from [`partition_digest`](Self::partition_digest);
    /// neither digest implies input ordering.
    pub fn digest(&self) -> [u8; 32] {
        self.name_digest
    }

    pub fn canonical_partition_bytes(&self) -> &[u8] {
        &self.canonical_partition_bytes
    }

    pub fn partition_digest(&self) -> [u8; 32] {
        self.partition_digest
    }
}

/// Opaque generation identifier for a cursor domain.
///
/// Epochs are compared only for exact equality. Their contents never imply
/// ordering, even if an application chooses a timestamp- or UUID-shaped value.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProjectionEpoch(String);

impl ProjectionEpoch {
    pub fn new(value: impl Into<String>) -> Result<Self, ProjectionProtocolValidationError> {
        validate_opaque(
            "projection cursor epoch",
            value.into(),
            MAX_PROJECTION_EPOCH_BYTES,
        )
        .map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Serialize for ProjectionEpoch {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ProjectionEpoch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Trusted ordered input position for one exact projector/source scope.
///
/// There is intentionally no `Ord` or `PartialOrd` implementation: callers
/// must use [`compare_position`](Self::compare_position), which rejects
/// cross-scope and cross-epoch comparisons.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProjectionInputCursor {
    topology: ProjectorTopologyId,
    projection_partition: ProjectionPartition,
    source: ProjectionSource,
    epoch: ProjectionEpoch,
    position: u64,
}

impl ProjectionInputCursor {
    pub fn new(
        topology: ProjectorTopologyId,
        projection_partition: ProjectionPartition,
        source: ProjectionSource,
        epoch: ProjectionEpoch,
        position: u64,
    ) -> Result<Self, ProjectionProtocolValidationError> {
        validate_portable_position("projection input position", position)?;
        Ok(Self {
            topology,
            projection_partition,
            source,
            epoch,
            position,
        })
    }

    pub fn topology(&self) -> &ProjectorTopologyId {
        &self.topology
    }

    pub fn projection_partition(&self) -> &ProjectionPartition {
        &self.projection_partition
    }

    pub fn source(&self) -> &ProjectionSource {
        &self.source
    }

    pub fn epoch(&self) -> &ProjectionEpoch {
        &self.epoch
    }

    pub fn position(&self) -> u64 {
        self.position
    }

    #[must_use]
    pub fn compare_position(&self, other: &Self) -> RevisionComparison {
        if self.topology != other.topology
            || self.projection_partition != other.projection_partition
            || self.source != other.source
            || self.epoch != other.epoch
        {
            return RevisionComparison::Incomparable;
        }
        compare_ordering(self.position.cmp(&other.position))
    }
}

/// Exact identity scope of one projected record.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProjectionRecordScope {
    topology: ProjectorTopologyId,
    projection_partition: ProjectionPartition,
    model: String,
    canonical_key_bytes: Vec<u8>,
    key_digest: [u8; 32],
}

impl ProjectionRecordScope {
    pub fn new(
        topology: ProjectorTopologyId,
        projection_partition: ProjectionPartition,
        model: impl Into<String>,
        canonical_key_bytes: impl Into<Vec<u8>>,
    ) -> Result<Self, ProjectionProtocolValidationError> {
        let model = validate_name(
            "projection model",
            model.into(),
            MAX_PROJECTION_MODEL_NAME_BYTES,
        )?;
        let canonical_key_bytes = validate_canonical_bytes(
            "projection record key",
            canonical_key_bytes.into(),
            MAX_PROJECTION_RECORD_KEY_BYTES,
        )?;
        let key_digest = Self::key_digest_for(&canonical_key_bytes);
        Ok(Self {
            topology,
            projection_partition,
            model,
            canonical_key_bytes,
            key_digest,
        })
    }

    pub fn topology(&self) -> &ProjectorTopologyId {
        &self.topology
    }

    pub fn projection_partition(&self) -> &ProjectionPartition {
        &self.projection_partition
    }

    pub fn model(&self) -> &str {
        &self.model
    }

    pub fn canonical_key_bytes(&self) -> &[u8] {
        &self.canonical_key_bytes
    }

    pub fn key_digest(&self) -> [u8; 32] {
        self.key_digest
    }

    pub(crate) fn key_digest_for(canonical_key_bytes: &[u8]) -> [u8; 32] {
        domain_separated_digest(PROJECTION_RECORD_KEY_DIGEST_DOMAIN, canonical_key_bytes)
    }
}

impl Serialize for ProjectionRecordScope {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct Wire<'a> {
            topology: &'a ProjectorTopologyId,
            partition: &'a ProjectionPartition,
            model: &'a str,
            key: &'a [u8],
        }
        Wire {
            topology: self.topology(),
            partition: self.projection_partition(),
            model: self.model(),
            key: self.canonical_key_bytes(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ProjectionRecordScope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            topology: ProjectorTopologyId,
            partition: ProjectionPartition,
            model: String,
            key: Vec<u8>,
        }
        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.topology, wire.partition, wire.model, wire.key)
            .map_err(serde::de::Error::custom)
    }
}

/// Durable version of one exact projected record.
///
/// Incarnation advances only on explicit recreation. Revision advances within
/// that incarnation. Comparison is lexicographic and only valid for an
/// identical [`ProjectionRecordScope`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RecordRevision {
    scope: ProjectionRecordScope,
    incarnation: NonZeroU64,
    revision: NonZeroU64,
}

impl RecordRevision {
    pub fn new(
        scope: ProjectionRecordScope,
        incarnation: u64,
        revision: u64,
    ) -> Result<Self, ProjectionProtocolValidationError> {
        validate_portable_position("projection record incarnation", incarnation)?;
        validate_portable_position("projection record revision", revision)?;
        let incarnation =
            NonZeroU64::new(incarnation).ok_or(ProjectionProtocolValidationError::Zero {
                field: "projection record incarnation",
            })?;
        let revision =
            NonZeroU64::new(revision).ok_or(ProjectionProtocolValidationError::Zero {
                field: "projection record revision",
            })?;
        Ok(Self {
            scope,
            incarnation,
            revision,
        })
    }

    pub fn scope(&self) -> &ProjectionRecordScope {
        &self.scope
    }

    pub fn incarnation(&self) -> u64 {
        self.incarnation.get()
    }

    pub fn revision(&self) -> u64 {
        self.revision.get()
    }

    #[must_use]
    pub fn compare(&self, other: &Self) -> RevisionComparison {
        if self.scope != other.scope {
            return RevisionComparison::Incomparable;
        }
        compare_ordering(
            (self.incarnation, self.revision).cmp(&(other.incarnation, other.revision)),
        )
    }
}

/// Durable change-log position emitted by one projector partition.
///
/// This is intentionally distinct from [`ProjectionInputCursor`]: accepting an
/// input and publishing a resumable change are different protocol facts.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProjectionChangeCursor {
    topology: ProjectorTopologyId,
    projection_partition: ProjectionPartition,
    epoch: ProjectionEpoch,
    position: NonZeroU64,
}

impl ProjectionChangeCursor {
    pub fn new(
        topology: ProjectorTopologyId,
        projection_partition: ProjectionPartition,
        epoch: ProjectionEpoch,
        position: u64,
    ) -> Result<Self, ProjectionProtocolValidationError> {
        validate_portable_position("projection change position", position)?;
        let position =
            NonZeroU64::new(position).ok_or(ProjectionProtocolValidationError::Zero {
                field: "projection change position",
            })?;
        Ok(Self {
            topology,
            projection_partition,
            epoch,
            position,
        })
    }

    pub fn topology(&self) -> &ProjectorTopologyId {
        &self.topology
    }

    pub fn projection_partition(&self) -> &ProjectionPartition {
        &self.projection_partition
    }

    pub fn epoch(&self) -> &ProjectionEpoch {
        &self.epoch
    }

    pub fn position(&self) -> u64 {
        self.position.get()
    }

    #[must_use]
    pub fn compare_position(&self, other: &Self) -> RevisionComparison {
        if self.topology != other.topology
            || self.projection_partition != other.projection_partition
            || self.epoch != other.epoch
        {
            return RevisionComparison::Incomparable;
        }
        compare_ordering(self.position.cmp(&other.position))
    }
}

/// Successful asynchronous advancement from a trusted input to a durable
/// change-log position.
///
/// Same-transaction command projection does not construct this type: it has
/// direct ledger-fenced record/change evidence and no asynchronous input
/// checkpoint.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProjectionCheckpoint {
    input: ProjectionInputCursor,
    change: ProjectionChangeCursor,
    gap_free: bool,
}

impl ProjectionCheckpoint {
    pub fn new(
        input: ProjectionInputCursor,
        change: ProjectionChangeCursor,
        gap_free: bool,
    ) -> Result<Self, ProjectionProtocolValidationError> {
        if input.topology != change.topology {
            return Err(ProjectionProtocolValidationError::ScopeMismatch {
                field: "projection checkpoint topology",
            });
        }
        if input.projection_partition != change.projection_partition {
            return Err(ProjectionProtocolValidationError::ScopeMismatch {
                field: "projection checkpoint partition",
            });
        }
        Ok(Self {
            input,
            change,
            gap_free,
        })
    }

    pub fn input(&self) -> &ProjectionInputCursor {
        &self.input
    }

    pub fn change(&self) -> &ProjectionChangeCursor {
        &self.change
    }

    /// Whether the registered ordered source proves there can be no omitted
    /// input positions before this checkpoint.
    ///
    /// Only gap-free checkpoints may imply coverage of earlier causations;
    /// all other sources require exact stored observations.
    pub fn is_gap_free(&self) -> bool {
        self.gap_free
    }
}

/// Result of attempting one projection commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ProjectionCommitOutcome {
    Applied,
    Duplicate,
    StaleInput,
}

/// Explicit comparison result for scoped revisions and cursor positions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RevisionComparison {
    Older,
    Equal,
    Newer,
    Incomparable,
}

fn validate_name(
    field: &'static str,
    value: String,
    max: usize,
) -> Result<String, ProjectionProtocolValidationError> {
    if value.is_empty() {
        return Err(ProjectionProtocolValidationError::Empty { field });
    }
    if value.len() > max {
        return Err(ProjectionProtocolValidationError::TooLong {
            field,
            len: value.len(),
            max,
        });
    }
    if let Some((byte_index, character)) = value
        .char_indices()
        .find(|(_, character)| character.is_control() || character.is_whitespace())
    {
        return Err(ProjectionProtocolValidationError::InvalidNameCharacter {
            field,
            byte_index,
            character,
        });
    }
    Ok(value)
}

fn validate_opaque(
    field: &'static str,
    value: String,
    max: usize,
) -> Result<String, ProjectionProtocolValidationError> {
    if value.is_empty() {
        return Err(ProjectionProtocolValidationError::Empty { field });
    }
    if value.len() > max {
        return Err(ProjectionProtocolValidationError::TooLong {
            field,
            len: value.len(),
            max,
        });
    }
    if let Some((byte_index, character)) = value
        .char_indices()
        .find(|(_, character)| character.is_control())
    {
        return Err(ProjectionProtocolValidationError::InvalidOpaqueCharacter {
            field,
            byte_index,
            character,
        });
    }
    Ok(value)
}

fn validate_canonical_bytes(
    field: &'static str,
    value: Vec<u8>,
    max: usize,
) -> Result<Vec<u8>, ProjectionProtocolValidationError> {
    if value.is_empty() {
        return Err(ProjectionProtocolValidationError::Empty { field });
    }
    if value.len() > max {
        return Err(ProjectionProtocolValidationError::TooLong {
            field,
            len: value.len(),
            max,
        });
    }
    Ok(value)
}

fn validate_portable_position(
    field: &'static str,
    value: u64,
) -> Result<(), ProjectionProtocolValidationError> {
    if value > MAX_PROJECTION_POSITION {
        return Err(ProjectionProtocolValidationError::TooLarge {
            field,
            value,
            max: MAX_PROJECTION_POSITION,
        });
    }
    Ok(())
}

fn append_length_prefixed(target: &mut Vec<u8>, value: &[u8]) {
    target.extend_from_slice(&(value.len() as u64).to_be_bytes());
    target.extend_from_slice(value);
}

fn take_length_prefixed(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    let length_bytes: [u8; std::mem::size_of::<u64>()] =
        bytes.get(..std::mem::size_of::<u64>())?.try_into().ok()?;
    let length = usize::try_from(u64::from_be_bytes(length_bytes)).ok()?;
    let value_start = std::mem::size_of::<u64>();
    let value_end = value_start.checked_add(length)?;
    Some((bytes.get(value_start..value_end)?, bytes.get(value_end..)?))
}

fn compare_ordering(ordering: Ordering) -> RevisionComparison {
    match ordering {
        Ordering::Less => RevisionComparison::Older,
        Ordering::Equal => RevisionComparison::Equal,
        Ordering::Greater => RevisionComparison::Newer,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topology(name: &str, marker: u8) -> ProjectorTopologyId {
        ProjectorTopologyId::new(1, name, [marker; 32]).unwrap()
    }

    fn partition(value: &[u8]) -> ProjectionPartition {
        ProjectionPartition::new(value.to_vec()).unwrap()
    }

    fn source(name: &str, value: &[u8]) -> ProjectionSource {
        ProjectionSource::new(name, value.to_vec()).unwrap()
    }

    fn epoch(value: &str) -> ProjectionEpoch {
        ProjectionEpoch::new(value).unwrap()
    }

    fn input_cursor(position: u64) -> ProjectionInputCursor {
        ProjectionInputCursor::new(
            topology("todos", 1),
            partition(b"tenant:a"),
            source("todo", b"todo:1"),
            epoch("aggregate-stream-v1"),
            position,
        )
        .unwrap()
    }

    fn change_cursor(position: u64) -> ProjectionChangeCursor {
        ProjectionChangeCursor::new(
            topology("todos", 1),
            partition(b"tenant:a"),
            epoch("projection-log-v1"),
            position,
        )
        .unwrap()
    }

    fn record_scope() -> ProjectionRecordScope {
        ProjectionRecordScope::new(
            topology("todos", 1),
            partition(b"tenant:a"),
            "TodoView",
            b"todo:1".to_vec(),
        )
        .unwrap()
    }

    #[test]
    fn topology_validates_version_name_and_preserves_digest() {
        let digest = [9; 32];
        let topology = ProjectorTopologyId::new(7, "project_todos", digest).unwrap();
        assert_eq!(topology.version(), 7);
        assert_eq!(topology.name(), "project_todos");
        assert_eq!(topology.digest(), digest);

        assert_eq!(
            ProjectorTopologyId::new(0, "project_todos", digest),
            Err(ProjectionProtocolValidationError::Zero {
                field: "projector topology version",
            })
        );
        assert_eq!(
            ProjectorTopologyId::new(1, "", digest),
            Err(ProjectionProtocolValidationError::Empty {
                field: "projector topology name",
            })
        );
        assert!(matches!(
            ProjectorTopologyId::new(1, "project todos", digest),
            Err(ProjectionProtocolValidationError::InvalidNameCharacter {
                field: "projector topology name",
                ..
            })
        ));
        assert!(matches!(
            ProjectorTopologyId::new(1, "project\ntodos", digest),
            Err(ProjectionProtocolValidationError::InvalidNameCharacter {
                field: "projector topology name",
                ..
            })
        ));

        let boundary = "a".repeat(MAX_PROJECTOR_NAME_BYTES);
        assert!(ProjectorTopologyId::new(1, boundary, digest).is_ok());
        let overlong = "a".repeat(MAX_PROJECTOR_NAME_BYTES + 1);
        assert_eq!(
            ProjectorTopologyId::new(1, overlong, digest),
            Err(ProjectionProtocolValidationError::TooLong {
                field: "projector topology name",
                len: MAX_PROJECTOR_NAME_BYTES + 1,
                max: MAX_PROJECTOR_NAME_BYTES,
            })
        );
    }

    #[test]
    fn topology_canonical_identity_is_deterministic_and_component_bound() {
        let first = ProjectorTopologyId::new(7, "project_todos", [9; 32]).unwrap();
        let same = ProjectorTopologyId::new(7, "project_todos", [9; 32]).unwrap();
        let different_version = ProjectorTopologyId::new(8, "project_todos", [9; 32]).unwrap();
        let different_name = ProjectorTopologyId::new(7, "project_todos_v2", [9; 32]).unwrap();
        let different_digest = ProjectorTopologyId::new(7, "project_todos", [8; 32]).unwrap();

        assert_eq!(first.canonical_bytes(), same.canonical_bytes());
        assert_ne!(first.canonical_bytes(), different_version.canonical_bytes());
        assert_ne!(first.canonical_bytes(), different_name.canonical_bytes());
        assert_ne!(first.canonical_bytes(), different_digest.canonical_bytes());
        assert!(first
            .canonical_bytes()
            .starts_with(PROJECTOR_TOPOLOGY_IDENTITY_ENCODING_DOMAIN));

        let boundary =
            ProjectorTopologyId::new(1, "a".repeat(MAX_PROJECTOR_NAME_BYTES), [0; 32]).unwrap();
        assert!(boundary.canonical_bytes().len() > MAX_PROJECTOR_NAME_BYTES);
    }

    #[test]
    fn canonical_values_are_nonempty_and_bounded() {
        assert_eq!(
            ProjectionPartition::new(Vec::new()),
            Err(ProjectionProtocolValidationError::Empty {
                field: "projection partition",
            })
        );
        assert!(ProjectionPartition::new(vec![1; MAX_PROJECTION_PARTITION_BYTES]).is_ok());
        assert_eq!(
            ProjectionPartition::new(vec![1; MAX_PROJECTION_PARTITION_BYTES + 1]),
            Err(ProjectionProtocolValidationError::TooLong {
                field: "projection partition",
                len: MAX_PROJECTION_PARTITION_BYTES + 1,
                max: MAX_PROJECTION_PARTITION_BYTES,
            })
        );

        assert_eq!(
            ProjectionSource::new("todo", Vec::new()),
            Err(ProjectionProtocolValidationError::Empty {
                field: "projection source partition",
            })
        );
        assert!(
            ProjectionSource::new("todo", vec![1; MAX_PROJECTION_SOURCE_PARTITION_BYTES]).is_ok()
        );
        assert_eq!(
            ProjectionSource::new("todo", vec![1; MAX_PROJECTION_SOURCE_PARTITION_BYTES + 1],),
            Err(ProjectionProtocolValidationError::TooLong {
                field: "projection source partition",
                len: MAX_PROJECTION_SOURCE_PARTITION_BYTES + 1,
                max: MAX_PROJECTION_SOURCE_PARTITION_BYTES,
            })
        );

        assert_eq!(
            ProjectionRecordScope::new(
                topology("todos", 1),
                partition(b"tenant:a"),
                "TodoView",
                Vec::new(),
            ),
            Err(ProjectionProtocolValidationError::Empty {
                field: "projection record key",
            })
        );
        assert!(ProjectionRecordScope::new(
            topology("todos", 1),
            partition(b"tenant:a"),
            "TodoView",
            vec![1; MAX_PROJECTION_RECORD_KEY_BYTES],
        )
        .is_ok());
        assert_eq!(
            ProjectionRecordScope::new(
                topology("todos", 1),
                partition(b"tenant:a"),
                "TodoView",
                vec![1; MAX_PROJECTION_RECORD_KEY_BYTES + 1],
            ),
            Err(ProjectionProtocolValidationError::TooLong {
                field: "projection record key",
                len: MAX_PROJECTION_RECORD_KEY_BYTES + 1,
                max: MAX_PROJECTION_RECORD_KEY_BYTES,
            })
        );
    }

    #[test]
    fn names_are_bounded_and_reject_whitespace_or_controls() {
        assert_eq!(
            ProjectionSource::new("", b"one".to_vec()),
            Err(ProjectionProtocolValidationError::Empty {
                field: "projection source name",
            })
        );
        assert!(matches!(
            ProjectionSource::new("todo source", b"one".to_vec()),
            Err(ProjectionProtocolValidationError::InvalidNameCharacter {
                field: "projection source name",
                ..
            })
        ));
        assert_eq!(
            ProjectionSource::new(
                "a".repeat(MAX_PROJECTION_SOURCE_NAME_BYTES + 1),
                b"one".to_vec(),
            ),
            Err(ProjectionProtocolValidationError::TooLong {
                field: "projection source name",
                len: MAX_PROJECTION_SOURCE_NAME_BYTES + 1,
                max: MAX_PROJECTION_SOURCE_NAME_BYTES,
            })
        );

        assert!(matches!(
            ProjectionRecordScope::new(
                topology("todos", 1),
                partition(b"tenant:a"),
                "Todo View",
                b"todo:1".to_vec(),
            ),
            Err(ProjectionProtocolValidationError::InvalidNameCharacter {
                field: "projection model",
                ..
            })
        ));
        assert_eq!(
            ProjectionRecordScope::new(
                topology("todos", 1),
                partition(b"tenant:a"),
                "a".repeat(MAX_PROJECTION_MODEL_NAME_BYTES + 1),
                b"todo:1".to_vec(),
            ),
            Err(ProjectionProtocolValidationError::TooLong {
                field: "projection model",
                len: MAX_PROJECTION_MODEL_NAME_BYTES + 1,
                max: MAX_PROJECTION_MODEL_NAME_BYTES,
            })
        );
    }

    #[test]
    fn digests_are_deterministic_and_domain_separated() {
        let bytes = b"same-canonical-value".to_vec();
        let first_partition = ProjectionPartition::new(bytes.clone()).unwrap();
        let second_partition = ProjectionPartition::new(bytes.clone()).unwrap();
        let source = ProjectionSource::new("source", bytes.clone()).unwrap();
        let scope = ProjectionRecordScope::new(
            topology("todos", 1),
            first_partition.clone(),
            "TodoView",
            bytes,
        )
        .unwrap();

        assert_eq!(first_partition.digest(), second_partition.digest());
        assert_eq!(
            source.digest(),
            ProjectionSource::new("source", b"different-partition".to_vec())
                .unwrap()
                .digest()
        );
        assert_ne!(source.digest(), source.partition_digest());
        assert_ne!(first_partition.digest(), source.partition_digest());
        assert_ne!(first_partition.digest(), scope.key_digest());
        assert_ne!(source.partition_digest(), scope.key_digest());
        assert!(source
            .canonical_name_bytes()
            .starts_with(PROJECTION_SOURCE_NAME_ENCODING_DOMAIN));
        assert_ne!(
            source.canonical_name_bytes(),
            source.canonical_partition_bytes()
        );
        assert_eq!(first_partition.canonical_bytes(), b"same-canonical-value");
        assert_eq!(source.canonical_partition_bytes(), b"same-canonical-value");
        assert_eq!(scope.canonical_key_bytes(), b"same-canonical-value");

        let decoded = ProjectionSource::from_canonical_name_bytes(
            &source.canonical_name_bytes(),
            source.canonical_partition_bytes().to_vec(),
        )
        .unwrap();
        assert_eq!(decoded, source);
        assert_eq!(
            ProjectionSource::from_canonical_name_bytes(
                b"not-a-versioned-source",
                b"partition".to_vec(),
            ),
            Err(
                ProjectionProtocolValidationError::MalformedCanonicalEncoding {
                    field: "projection source name",
                }
            )
        );
    }

    #[test]
    fn epoch_is_bounded_opaque_and_never_ordered_by_contents() {
        let opaque = ProjectionEpoch::new("2026-07-22 10:00:00Z").unwrap();
        assert_eq!(opaque.as_str(), "2026-07-22 10:00:00Z");
        assert_eq!(
            serde_json::from_str::<ProjectionEpoch>(
                &serde_json::to_string(&opaque).expect("epoch serializes")
            )
            .expect("serialized epoch validates"),
            opaque
        );
        assert!(serde_json::from_str::<ProjectionEpoch>("\"epoch\\n2\"").is_err());
        assert_eq!(
            ProjectionEpoch::new(""),
            Err(ProjectionProtocolValidationError::Empty {
                field: "projection cursor epoch",
            })
        );
        assert!(matches!(
            ProjectionEpoch::new("epoch\n2"),
            Err(ProjectionProtocolValidationError::InvalidOpaqueCharacter {
                field: "projection cursor epoch",
                ..
            })
        ));
        assert!(ProjectionEpoch::new("a".repeat(MAX_PROJECTION_EPOCH_BYTES)).is_ok());
        assert_eq!(
            ProjectionEpoch::new("a".repeat(MAX_PROJECTION_EPOCH_BYTES + 1)),
            Err(ProjectionProtocolValidationError::TooLong {
                field: "projection cursor epoch",
                len: MAX_PROJECTION_EPOCH_BYTES + 1,
                max: MAX_PROJECTION_EPOCH_BYTES,
            })
        );
    }

    #[test]
    fn input_cursor_allows_zero_and_compares_only_exact_scope() {
        let zero = input_cursor(0);
        assert_eq!(zero.position(), 0);
        assert_eq!(
            ProjectionInputCursor::new(
                topology("todos", 1),
                partition(b"tenant:a"),
                source("todo", b"todo:1"),
                epoch("aggregate-stream-v1"),
                MAX_PROJECTION_POSITION + 1,
            ),
            Err(ProjectionProtocolValidationError::TooLarge {
                field: "projection input position",
                value: MAX_PROJECTION_POSITION + 1,
                max: MAX_PROJECTION_POSITION,
            })
        );
        assert_eq!(
            zero.compare_position(&input_cursor(1)),
            RevisionComparison::Older
        );
        let base = input_cursor(10);
        assert_eq!(
            input_cursor(9).compare_position(&base),
            RevisionComparison::Older
        );
        assert_eq!(
            base.compare_position(&input_cursor(10)),
            RevisionComparison::Equal
        );
        assert_eq!(
            input_cursor(11).compare_position(&base),
            RevisionComparison::Newer
        );

        let different_topology = ProjectionInputCursor::new(
            topology("todos", 2),
            partition(b"tenant:a"),
            source("todo", b"todo:1"),
            epoch("aggregate-stream-v1"),
            10,
        )
        .unwrap();
        let different_partition = ProjectionInputCursor::new(
            topology("todos", 1),
            partition(b"tenant:b"),
            source("todo", b"todo:1"),
            epoch("aggregate-stream-v1"),
            10,
        )
        .unwrap();
        let different_source = ProjectionInputCursor::new(
            topology("todos", 1),
            partition(b"tenant:a"),
            source("todo", b"todo:2"),
            epoch("aggregate-stream-v1"),
            10,
        )
        .unwrap();
        let different_epoch = ProjectionInputCursor::new(
            topology("todos", 1),
            partition(b"tenant:a"),
            source("todo", b"todo:1"),
            epoch("aggregate-stream-v2"),
            10,
        )
        .unwrap();

        for other in [
            different_topology,
            different_partition,
            different_source,
            different_epoch,
        ] {
            assert_eq!(
                base.compare_position(&other),
                RevisionComparison::Incomparable
            );
        }
    }

    #[test]
    fn record_revision_is_nonzero_lexicographic_and_scope_bound() {
        let scope = record_scope();
        assert_eq!(
            RecordRevision::new(scope.clone(), 0, 1),
            Err(ProjectionProtocolValidationError::Zero {
                field: "projection record incarnation",
            })
        );
        assert_eq!(
            RecordRevision::new(scope.clone(), 1, 0),
            Err(ProjectionProtocolValidationError::Zero {
                field: "projection record revision",
            })
        );
        assert_eq!(
            RecordRevision::new(scope.clone(), MAX_PROJECTION_POSITION + 1, 1),
            Err(ProjectionProtocolValidationError::TooLarge {
                field: "projection record incarnation",
                value: MAX_PROJECTION_POSITION + 1,
                max: MAX_PROJECTION_POSITION,
            })
        );

        let one_one = RecordRevision::new(scope.clone(), 1, 1).unwrap();
        let one_two = RecordRevision::new(scope.clone(), 1, 2).unwrap();
        let two_one = RecordRevision::new(scope.clone(), 2, 1).unwrap();
        assert_eq!(one_one.compare(&one_two), RevisionComparison::Older);
        assert_eq!(one_two.compare(&one_one), RevisionComparison::Newer);
        assert_eq!(
            one_one.compare(&RecordRevision::new(scope.clone(), 1, 1).unwrap()),
            RevisionComparison::Equal
        );
        assert_eq!(one_two.compare(&two_one), RevisionComparison::Older);

        let scopes = [
            ProjectionRecordScope::new(
                topology("other", 1),
                partition(b"tenant:a"),
                "TodoView",
                b"todo:1".to_vec(),
            )
            .unwrap(),
            ProjectionRecordScope::new(
                topology("todos", 1),
                partition(b"tenant:b"),
                "TodoView",
                b"todo:1".to_vec(),
            )
            .unwrap(),
            ProjectionRecordScope::new(
                topology("todos", 1),
                partition(b"tenant:a"),
                "OtherView",
                b"todo:1".to_vec(),
            )
            .unwrap(),
            ProjectionRecordScope::new(
                topology("todos", 1),
                partition(b"tenant:a"),
                "TodoView",
                b"todo:2".to_vec(),
            )
            .unwrap(),
        ];
        for other_scope in scopes {
            let other = RecordRevision::new(other_scope, 1, 1).unwrap();
            assert_eq!(one_one.compare(&other), RevisionComparison::Incomparable);
        }
    }

    #[test]
    fn change_cursor_is_distinct_nonzero_and_scope_bound() {
        assert_eq!(
            ProjectionChangeCursor::new(
                topology("todos", 1),
                partition(b"tenant:a"),
                epoch("projection-log-v1"),
                0,
            ),
            Err(ProjectionProtocolValidationError::Zero {
                field: "projection change position",
            })
        );
        assert_eq!(
            ProjectionChangeCursor::new(
                topology("todos", 1),
                partition(b"tenant:a"),
                epoch("projection-log-v1"),
                MAX_PROJECTION_POSITION + 1,
            ),
            Err(ProjectionProtocolValidationError::TooLarge {
                field: "projection change position",
                value: MAX_PROJECTION_POSITION + 1,
                max: MAX_PROJECTION_POSITION,
            })
        );

        let base = change_cursor(10);
        assert_eq!(
            change_cursor(9).compare_position(&base),
            RevisionComparison::Older
        );
        assert_eq!(
            base.compare_position(&change_cursor(10)),
            RevisionComparison::Equal
        );
        assert_eq!(
            change_cursor(11).compare_position(&base),
            RevisionComparison::Newer
        );

        let different_topology = ProjectionChangeCursor::new(
            topology("todos", 2),
            partition(b"tenant:a"),
            epoch("projection-log-v1"),
            10,
        )
        .unwrap();
        let different_partition = ProjectionChangeCursor::new(
            topology("todos", 1),
            partition(b"tenant:b"),
            epoch("projection-log-v1"),
            10,
        )
        .unwrap();
        let different_epoch = ProjectionChangeCursor::new(
            topology("todos", 1),
            partition(b"tenant:a"),
            epoch("projection-log-v2"),
            10,
        )
        .unwrap();

        for other in [different_topology, different_partition, different_epoch] {
            assert_eq!(
                base.compare_position(&other),
                RevisionComparison::Incomparable
            );
        }
    }

    #[test]
    fn checkpoint_requires_matching_topology_and_projection_partition() {
        let checkpoint =
            ProjectionCheckpoint::new(input_cursor(7), change_cursor(11), true).unwrap();
        assert_eq!(checkpoint.input().position(), 7);
        assert_eq!(checkpoint.change().position(), 11);
        assert!(checkpoint.is_gap_free());

        let wrong_topology = ProjectionChangeCursor::new(
            topology("other", 2),
            partition(b"tenant:a"),
            epoch("projection-log-v1"),
            11,
        )
        .unwrap();
        assert_eq!(
            ProjectionCheckpoint::new(input_cursor(7), wrong_topology, true),
            Err(ProjectionProtocolValidationError::ScopeMismatch {
                field: "projection checkpoint topology",
            })
        );

        let wrong_partition = ProjectionChangeCursor::new(
            topology("todos", 1),
            partition(b"tenant:b"),
            epoch("projection-log-v1"),
            11,
        )
        .unwrap();
        assert_eq!(
            ProjectionCheckpoint::new(input_cursor(7), wrong_partition, true),
            Err(ProjectionProtocolValidationError::ScopeMismatch {
                field: "projection checkpoint partition",
            })
        );
    }

    #[test]
    fn commit_outcomes_are_closed_and_distinct() {
        assert_ne!(
            ProjectionCommitOutcome::Applied,
            ProjectionCommitOutcome::Duplicate
        );
        assert_ne!(
            ProjectionCommitOutcome::Applied,
            ProjectionCommitOutcome::StaleInput
        );
        assert_ne!(
            ProjectionCommitOutcome::Duplicate,
            ProjectionCommitOutcome::StaleInput
        );
    }
}
