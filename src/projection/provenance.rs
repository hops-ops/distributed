use serde::Serialize;

use crate::domain_event::{
    DomainEventBodyKind, DomainEventDescriptor, DomainEventOccurrence,
    DOMAIN_EVENT_OCCURRENCE_VERSION,
};

use super::expression::non_empty;
use super::ProjectionProgramError;

/// Exact semantic domain-event contract selected by a projection arm.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
pub struct ProjectionEventSelector {
    occurrence_version: u16,
    event_name: String,
    event_version: u64,
    body_kind: DomainEventBodyKind,
    body_type_name: String,
    body_version: u64,
    body_schema: String,
    body_fingerprint: String,
    body_codec: String,
    body_codec_version: u16,
}

impl ProjectionEventSelector {
    /// Construct an exact event-name, version, and body-schema selector.
    ///
    /// # Errors
    ///
    /// Rejects empty names, zero versions, and non-canonical fingerprints.
    #[expect(
        clippy::too_many_arguments,
        reason = "the selector binds every independent wire-contract identity field"
    )]
    pub fn try_new(
        occurrence_version: u16,
        event_name: impl Into<String>,
        event_version: u64,
        body_kind: DomainEventBodyKind,
        body_type_name: impl Into<String>,
        body_version: u64,
        body_schema: impl Into<String>,
        body_fingerprint: impl Into<String>,
        body_codec: impl Into<String>,
        body_codec_version: u16,
    ) -> Result<Self, ProjectionProgramError> {
        if occurrence_version == 0 {
            return Err(ProjectionProgramError::ZeroVersion("occurrence version"));
        }
        let event_name = non_empty(event_name.into(), "event name")?;
        if event_version == 0 {
            return Err(ProjectionProgramError::ZeroVersion("event version"));
        }
        let body_type_name = non_empty(body_type_name.into(), "event body type")?;
        if body_version == 0 {
            return Err(ProjectionProgramError::ZeroVersion("event body version"));
        }
        let body_schema = non_empty(body_schema.into(), "event body schema")?;
        let body_fingerprint = body_fingerprint.into();
        validate_sha256(&body_fingerprint)?;
        let body_codec = non_empty(body_codec.into(), "event body codec")?;
        if body_codec_version == 0 {
            return Err(ProjectionProgramError::ZeroVersion(
                "event body codec version",
            ));
        }
        Ok(Self {
            occurrence_version,
            event_name,
            event_version,
            body_kind,
            body_type_name,
            body_version,
            body_schema,
            body_fingerprint,
            body_codec,
            body_codec_version,
        })
    }

    /// Construct a selector from an exact domain-event descriptor.
    ///
    /// # Errors
    ///
    /// Rejects an invalid descriptor identity.
    pub fn try_from_descriptor(
        descriptor: &DomainEventDescriptor,
    ) -> Result<Self, ProjectionProgramError> {
        Self::try_new(
            DOMAIN_EVENT_OCCURRENCE_VERSION,
            descriptor.name.to_string(),
            descriptor.version,
            descriptor.body.kind,
            descriptor.body.type_name.to_string(),
            descriptor.body.version,
            descriptor.body.schema.to_string(),
            descriptor.body.fingerprint.to_string(),
            descriptor.body.codec.to_string(),
            descriptor.body.codec_version,
        )
    }

    /// Return the exact occurrence-envelope version.
    pub fn occurrence_version(&self) -> u16 {
        self.occurrence_version
    }

    /// Return the semantic event name.
    pub fn event_name(&self) -> &str {
        &self.event_name
    }

    /// Return the semantic event version.
    pub fn event_version(&self) -> u64 {
        self.event_version
    }

    /// Return the exact canonical body-schema fingerprint.
    pub fn body_fingerprint(&self) -> &str {
        &self.body_fingerprint
    }

    /// Return whether the body is state, sparse event, or deletion identity.
    pub fn body_kind(&self) -> DomainEventBodyKind {
        self.body_kind
    }

    /// Return the stable event-body type name.
    pub fn body_type_name(&self) -> &str {
        &self.body_type_name
    }

    /// Return the independently evolving event-body schema version.
    pub fn body_version(&self) -> u64 {
        self.body_version
    }

    /// Return the canonical event-body schema identity.
    pub fn body_schema(&self) -> &str {
        &self.body_schema
    }

    /// Return the canonical event-body codec.
    pub fn body_codec(&self) -> &str {
        &self.body_codec
    }

    /// Return the canonical event-body codec version.
    pub fn body_codec_version(&self) -> u16 {
        self.body_codec_version
    }

    pub(crate) fn matches(&self, occurrence: &DomainEventOccurrence) -> bool {
        let descriptor = occurrence.descriptor();
        let body = &descriptor.body;
        occurrence.occurrence_version() == self.occurrence_version
            && descriptor.name == self.event_name
            && descriptor.version == self.event_version
            && body.kind == self.body_kind
            && body.type_name == self.body_type_name
            && body.version == self.body_version
            && body.schema == self.body_schema
            && body.fingerprint == self.body_fingerprint
            && body.codec == self.body_codec
            && body.codec_version == self.body_codec_version
    }

    pub(crate) fn canonical_cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.occurrence_version
            .cmp(&other.occurrence_version)
            .then_with(|| self.event_name.cmp(&other.event_name))
            .then_with(|| self.event_version.cmp(&other.event_version))
            .then_with(|| body_kind_rank(self.body_kind).cmp(&body_kind_rank(other.body_kind)))
            .then_with(|| self.body_type_name.cmp(&other.body_type_name))
            .then_with(|| self.body_version.cmp(&other.body_version))
            .then_with(|| self.body_schema.cmp(&other.body_schema))
            .then_with(|| self.body_fingerprint.cmp(&other.body_fingerprint))
            .then_with(|| self.body_codec.cmp(&other.body_codec))
            .then_with(|| self.body_codec_version.cmp(&other.body_codec_version))
    }
}

/// Stable logical and physical identity of a projected read model.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct ProjectionTarget {
    model: String,
    storage: String,
}

impl ProjectionTarget {
    /// Construct a portable logical target.
    ///
    /// `storage` is an opaque stable identity. It is not an ORM write plan.
    /// Physical dependency order comes from registered ORM metadata during
    /// lowering; application-authored portable programs cannot override it.
    ///
    /// # Errors
    ///
    /// Rejects empty model and storage names.
    pub fn try_new(
        model: impl Into<String>,
        storage: impl Into<String>,
    ) -> Result<Self, ProjectionProgramError> {
        Ok(Self {
            model: non_empty(model.into(), "projection model")?,
            storage: non_empty(storage.into(), "projection storage")?,
        })
    }

    /// Return the logical model name.
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Return the opaque storage identity used by a future adapter.
    pub fn storage(&self) -> &str {
        &self.storage
    }
}

/// Provenance for a mutation of a relationship-owned row.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct ProjectionRelationship {
    source_model: String,
    relationship: String,
    target_model: String,
}

impl ProjectionRelationship {
    /// Construct stable relationship provenance.
    ///
    /// # Errors
    ///
    /// Rejects empty model and relationship names.
    pub fn try_new(
        source_model: impl Into<String>,
        relationship: impl Into<String>,
        target_model: impl Into<String>,
    ) -> Result<Self, ProjectionProgramError> {
        Ok(Self {
            source_model: non_empty(source_model.into(), "relationship source model")?,
            relationship: non_empty(relationship.into(), "relationship name")?,
            target_model: non_empty(target_model.into(), "relationship target model")?,
        })
    }

    /// Return the relationship source model.
    pub fn source_model(&self) -> &str {
        &self.source_model
    }

    /// Return the stable relationship name.
    pub fn relationship(&self) -> &str {
        &self.relationship
    }

    /// Return the relationship target model.
    pub fn target_model(&self) -> &str {
        &self.target_model
    }
}

/// Read-model inventory that a mutation invalidates for derived consumers.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProjectionInvalidation {
    /// Invalidate selections of a whole logical model.
    Model {
        /// Stable logical model name.
        model: String,
    },
    /// Invalidate one named relationship surface.
    Relationship {
        /// Stable source model.
        source_model: String,
        /// Stable relationship name.
        relationship: String,
        /// Stable target model.
        target_model: String,
    },
}

impl ProjectionInvalidation {
    /// Construct a model invalidation.
    ///
    /// # Errors
    ///
    /// Rejects an empty model name.
    pub fn model(model: impl Into<String>) -> Result<Self, ProjectionProgramError> {
        Ok(Self::Model {
            model: non_empty(model.into(), "invalidation model")?,
        })
    }

    /// Construct a relationship invalidation.
    ///
    /// # Errors
    ///
    /// Rejects empty model and relationship names.
    pub fn relationship(
        source_model: impl Into<String>,
        relationship: impl Into<String>,
        target_model: impl Into<String>,
    ) -> Result<Self, ProjectionProgramError> {
        Ok(Self::Relationship {
            source_model: non_empty(source_model.into(), "invalidation source model")?,
            relationship: non_empty(relationship.into(), "invalidation relationship")?,
            target_model: non_empty(target_model.into(), "invalidation target model")?,
        })
    }
}

fn validate_sha256(value: &str) -> Result<(), ProjectionProgramError> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(ProjectionProgramError::InvalidBodyFingerprint);
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ProjectionProgramError::InvalidBodyFingerprint);
    }
    Ok(())
}

fn body_kind_rank(kind: DomainEventBodyKind) -> u8 {
    match kind {
        DomainEventBodyKind::State => 0,
        DomainEventBodyKind::Event => 1,
        DomainEventBodyKind::Deletion => 2,
    }
}
