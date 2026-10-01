#![cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "compiler-owned mirror of the frozen wire contract; runtime consumption lands in Task 16"
    )
)]

use std::collections::BTreeSet;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use serde::{Deserialize, Serialize};

use super::super::manifest::{hash_bytes, validate_projection_epoch};
use super::super::ClientCompileError;

pub(crate) const PROJECTION_DELTA_WIRE_VERSION: u16 = 1;
const MAX_ITEMS: usize = 128;
const MAX_VALUE_DEPTH: usize = 64;
const MAX_BODY_BYTES: usize = 1024 * 1024;
const MAX_PARTITION_BYTES: usize = 4 * 1024;
const MAX_RECORD_KEY_BYTES: usize = 4 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProjectionDeltaWire {
    pub(crate) wire_version: u16,
    pub(crate) identity: ProjectionDeltaIdentity,
    pub(crate) projections: Vec<ProjectionIdentity>,
    pub(crate) occurrences: Vec<ProjectionOccurrence>,
    pub(crate) operations: Vec<ProjectionOperation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) recoveries: Vec<ProjectionRecovery>,
}

impl ProjectionDeltaWire {
    pub(crate) fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, ClientCompileError> {
        if bytes.len() > MAX_BODY_BYTES {
            return Err(invalid(format!(
                "ProjectionDelta body exceeds {MAX_BODY_BYTES} bytes"
            )));
        }
        let decoded: Self = serde_json::from_slice(bytes).map_err(|error| {
            invalid(format!("invalid ProjectionDelta wire-v1 payload: {error}"))
        })?;
        decoded.validate()?;
        let canonical = decoded.canonical_bytes()?;
        if canonical != bytes {
            return Err(invalid(
                "ProjectionDelta wire-v1 payload is not canonical JSON",
            ));
        }
        Ok(decoded)
    }

    pub(crate) fn canonical_bytes(&self) -> Result<Vec<u8>, ClientCompileError> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|error| {
            invalid(format!("cannot serialize ProjectionDelta wire-v1: {error}"))
        })?;
        if bytes.len() > MAX_BODY_BYTES {
            return Err(invalid(format!(
                "ProjectionDelta body exceeds {MAX_BODY_BYTES} bytes"
            )));
        }
        Ok(bytes)
    }

    pub(crate) fn fingerprint(&self) -> Result<String, ClientCompileError> {
        Ok(hash_bytes(&self.canonical_bytes()?))
    }

    fn validate(&self) -> Result<(), ClientCompileError> {
        if self.wire_version != PROJECTION_DELTA_WIRE_VERSION {
            return Err(invalid("unsupported ProjectionDelta wire version"));
        }
        self.identity.validate()?;
        for (label, len) in [
            ("projections", self.projections.len()),
            ("occurrences", self.occurrences.len()),
            ("operations", self.operations.len()),
            ("recoveries", self.recoveries.len()),
        ] {
            if len > MAX_ITEMS {
                return Err(invalid(format!(
                    "ProjectionDelta {label} exceeds {MAX_ITEMS} entries"
                )));
            }
        }
        if self.projections.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(invalid(
                "ProjectionDelta projections must be sorted and unique",
            ));
        }
        for projection in &self.projections {
            projection.validate()?;
        }
        let mut occurrence_ids = BTreeSet::new();
        for (index, occurrence) in self.occurrences.iter().enumerate() {
            if occurrence.ordinal as usize != index
                || occurrence.causation_id != self.identity.command_causation_id
            {
                return Err(invalid(
                    "ProjectionDelta occurrences must be dense and causation-bound",
                ));
            }
            nonempty(&occurrence.occurrence_id, "occurrence id")?;
            if !occurrence_ids.insert(&occurrence.occurrence_id) {
                return Err(invalid("ProjectionDelta occurrence IDs must be unique"));
            }
        }
        if self.projections.is_empty()
            && (!self.occurrences.is_empty()
                || !self.operations.is_empty()
                || !self.recoveries.is_empty())
        {
            return Err(invalid(
                "empty projection inventory requires an empty authoritative delta",
            ));
        }
        let mut operation_scopes = BTreeSet::new();
        let mut previous_operation = None;
        for operation in &self.operations {
            operation.validate(self.projections.len(), self.occurrences.len())?;
            let scope = operation.scope_key();
            let order = (scope.clone(), operation.occurrence_ordinal);
            if previous_operation
                .as_ref()
                .is_some_and(|previous| previous >= &order)
                || !operation_scopes.insert(scope)
            {
                return Err(invalid(
                    "ProjectionDelta operations must use canonical unique scope order",
                ));
            }
            previous_operation = Some(order);
        }
        let mut recovery_targets = BTreeSet::new();
        let mut previous_recovery = None;
        for recovery in &self.recoveries {
            recovery.validate(self.projections.len(), self.occurrences.len())?;
            let target = recovery.target.clone();
            let order = (target.clone(), recovery.occurrence_ordinal);
            if previous_recovery
                .as_ref()
                .is_some_and(|previous| previous >= &order)
                || !recovery_targets.insert(target)
            {
                return Err(invalid(
                    "ProjectionDelta recoveries must use canonical unique target order",
                ));
            }
            if recovery.condition == RecoveryCondition::IfRecordMissing {
                let RecoveryTarget::Record { scope } = &recovery.target else {
                    return Err(invalid(
                        "if_record_missing recovery requires a record target",
                    ));
                };
                if !self.operations.iter().any(|operation| {
                    matches!(
                        &operation.mutation,
                        ProjectionMutation::Patch {
                            scope: patch_scope,
                            if_present: true,
                            ..
                        } if patch_scope == scope
                    )
                }) {
                    return Err(invalid(
                        "if_record_missing recovery requires a same-scope conditional patch",
                    ));
                }
            }
            previous_recovery = Some(order);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProjectionDeltaIdentity {
    pub(crate) manifest_version: u32,
    pub(crate) client_protocol_version: u32,
    pub(crate) surface: ProjectionSurfaceIdentity,
    pub(crate) schema_fingerprint: String,
    pub(crate) protocol_fingerprint: String,
    pub(crate) authorization_generation: String,
    pub(crate) cache_scope_token: String,
    pub(crate) command_causation_id: String,
}

impl ProjectionDeltaIdentity {
    fn validate(&self) -> Result<(), ClientCompileError> {
        if self.manifest_version != 2 || self.client_protocol_version != 1 {
            return Err(invalid(
                "ProjectionDelta identity requires manifest v2 and client protocol v1",
            ));
        }
        self.surface.validate()?;
        nonempty(&self.schema_fingerprint, "schema fingerprint")?;
        nonempty(&self.protocol_fingerprint, "protocol fingerprint")?;
        nonempty(&self.authorization_generation, "authorization generation")?;
        validate_token(&self.cache_scope_token, "cache-scope")?;
        nonempty(&self.command_causation_id, "command causation id")
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ProjectionSurfaceIdentity {
    Role {
        name: String,
    },
    Application {
        name: String,
        eligible_roles: Vec<String>,
        schema_roles: Vec<String>,
    },
}

impl ProjectionSurfaceIdentity {
    fn validate(&self) -> Result<(), ClientCompileError> {
        match self {
            Self::Role { name } => nonempty(name, "role surface"),
            Self::Application {
                name,
                eligible_roles,
                schema_roles,
            } => {
                nonempty(name, "application surface")?;
                if eligible_roles.is_empty() {
                    return Err(invalid(
                        "ProjectionDelta application eligible roles must be sorted, unique, and non-empty",
                    ));
                }
                validate_names(eligible_roles, "application eligible roles")?;
                if schema_roles.is_empty()
                    || !schema_roles
                        .iter()
                        .all(|role| eligible_roles.iter().any(|eligible| eligible == role))
                {
                    return Err(invalid(
                        "ProjectionDelta application schema roles must be a sorted, unique, non-empty subset of eligible roles",
                    ));
                }
                validate_names(schema_roles, "application schema roles")
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProjectionIdentity {
    pub(crate) program_id: String,
    pub(crate) binding_id: String,
    pub(crate) epoch: String,
    pub(crate) program_ir_version: u16,
    pub(crate) operation_semantics_version: u16,
}

impl ProjectionIdentity {
    fn validate(&self) -> Result<(), ClientCompileError> {
        validate_prefixed_hash(&self.program_id, "pp1:")?;
        validate_prefixed_hash(&self.binding_id, "pb1:")?;
        validate_projection_epoch(&self.epoch, "ProjectionDelta projection epoch")?;
        if self.program_ir_version != 1 || self.operation_semantics_version != 1 {
            return Err(invalid(
                "ProjectionDelta executable identities require IR/semantics v1",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProjectionOccurrence {
    pub(crate) causation_id: String,
    pub(crate) ordinal: u32,
    pub(crate) occurrence_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProjectionScope {
    pub(crate) partition: ProjectionPartition,
    pub(crate) model: String,
    pub(crate) key: Vec<DeltaKeyField>,
}

impl ProjectionScope {
    fn validate(&self) -> Result<(), ClientCompileError> {
        self.partition.validate()?;
        nonempty(&self.model, "projection model")?;
        if self.key.is_empty() {
            return Err(invalid("ProjectionDelta record key must not be empty"));
        }
        let mut names = BTreeSet::new();
        for (index, field) in self.key.iter().enumerate() {
            if field.ordinal as usize != index || !names.insert(&field.field) {
                return Err(invalid(
                    "ProjectionDelta key ordinals/names must be dense and unique",
                ));
            }
            nonempty(&field.field, "projection key field")?;
            field.value.validate(1)?;
            if matches!(
                field.value,
                DeltaValue::Null | DeltaValue::List(_) | DeltaValue::Object(_)
            ) {
                return Err(invalid(
                    "ProjectionDelta record key values must be non-null scalars",
                ));
            }
        }
        let key_bytes = serde_json::to_vec(&self.key)
            .map_err(|error| invalid(format!("cannot serialize ProjectionDelta key: {error}")))?;
        if key_bytes.len() > MAX_RECORD_KEY_BYTES {
            return Err(invalid(format!(
                "ProjectionDelta record key exceeds {MAX_RECORD_KEY_BYTES} bytes"
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ProjectionPartition {
    Unit,
    Opaque { token: String },
}

impl ProjectionPartition {
    fn validate(&self) -> Result<(), ClientCompileError> {
        let encoded = serde_json::to_vec(self).map_err(|error| {
            invalid(format!(
                "cannot serialize ProjectionDelta partition: {error}"
            ))
        })?;
        if encoded.len() > MAX_PARTITION_BYTES {
            return Err(invalid(format!(
                "ProjectionDelta partition exceeds {MAX_PARTITION_BYTES} bytes"
            )));
        }
        if let Self::Opaque { token } = self {
            nonempty(token, "opaque partition")?;
            validate_token(token, "projection-partition")?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeltaKeyField {
    pub(crate) ordinal: u32,
    pub(crate) field: String,
    pub(crate) value: DeltaValue,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeltaField {
    pub(crate) field: String,
    pub(crate) value: DeltaValue,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum DeltaValue {
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
    fn validate(&self, depth: usize) -> Result<(), ClientCompileError> {
        if depth > MAX_VALUE_DEPTH {
            return Err(invalid("ProjectionDelta value exceeds depth 64"));
        }
        match self {
            Self::Null | Self::Boolean(_) => Ok(()),
            Self::I64(value) => canonical_number::<i64>(value, "i64"),
            Self::U64(value) => canonical_number::<u64>(value, "u64"),
            Self::F64(value) => {
                let parsed = value
                    .parse::<f64>()
                    .ok()
                    .filter(|value| value.is_finite())
                    .ok_or_else(|| invalid("invalid ProjectionDelta f64"))?;
                let canonical =
                    serde_json::Number::from_f64(if parsed == 0.0 { 0.0 } else { parsed })
                        .expect("finite float")
                        .to_string();
                if canonical != *value {
                    return Err(invalid("noncanonical ProjectionDelta f64"));
                }
                Ok(())
            }
            Self::String(value) => {
                if value.len() > MAX_BODY_BYTES {
                    return Err(invalid(format!(
                        "ProjectionDelta string exceeds {MAX_BODY_BYTES} bytes"
                    )));
                }
                Ok(())
            }
            Self::Enum { enum_type, variant } => {
                nonempty(enum_type, "enum type")?;
                nonempty(variant, "enum variant")
            }
            Self::List(values) => {
                for value in values {
                    value.validate(depth + 1)?;
                }
                Ok(())
            }
            Self::Object(fields) => {
                validate_fields(fields)?;
                for field in fields {
                    field.value.validate(depth + 1)?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProjectionOperation {
    pub(crate) occurrence_ordinal: u32,
    pub(crate) projection_refs: Vec<u32>,
    pub(crate) mutation: ProjectionMutation,
}

impl ProjectionOperation {
    fn validate(
        &self,
        projection_count: usize,
        occurrence_count: usize,
    ) -> Result<(), ClientCompileError> {
        validate_refs(&self.projection_refs, projection_count)?;
        if self.occurrence_ordinal as usize >= occurrence_count {
            return Err(invalid(
                "ProjectionDelta operation references an absent occurrence",
            ));
        }
        self.mutation.validate()
    }

    fn scope_key(&self) -> OperationScope {
        self.mutation.scope()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ProjectionMutation {
    Upsert {
        scope: ProjectionScope,
        fields: Vec<DeltaField>,
        replace: Vec<String>,
    },
    Patch {
        scope: ProjectionScope,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        set: Vec<DeltaField>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        unset: Vec<String>,
        if_present: bool,
    },
    Delete {
        scope: ProjectionScope,
    },
    Link {
        relationship: String,
        source: ProjectionScope,
        target: ProjectionScope,
    },
    Unlink {
        relationship: String,
        source: ProjectionScope,
        target: ProjectionScope,
    },
    InvalidateModel {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        partition: Option<ProjectionPartition>,
        model: String,
    },
    InvalidateRelationship {
        relationship: String,
        source: ProjectionScope,
    },
}

impl ProjectionMutation {
    fn validate(&self) -> Result<(), ClientCompileError> {
        match self {
            Self::Upsert {
                scope,
                fields,
                replace,
            } => {
                scope.validate()?;
                validate_fields(fields)?;
                validate_names(replace, "upsert replacement mask")?;
                if fields.iter().any(|field| !replace.contains(&field.field)) {
                    return Err(invalid(
                        "ProjectionDelta upsert field is outside its replacement mask",
                    ));
                }
            }
            Self::Patch {
                scope,
                set,
                unset,
                if_present,
            } => {
                scope.validate()?;
                validate_fields(set)?;
                validate_names(unset, "patch unset fields")?;
                if !if_present || (set.is_empty() && unset.is_empty()) {
                    return Err(invalid(
                        "ProjectionDelta patch must be conditional and non-empty",
                    ));
                }
                if set.iter().any(|field| unset.contains(&field.field)) {
                    return Err(invalid(
                        "ProjectionDelta patch cannot set and unset one field",
                    ));
                }
            }
            Self::Delete { scope } => scope.validate()?,
            Self::Link {
                relationship,
                source,
                target,
            }
            | Self::Unlink {
                relationship,
                source,
                target,
            } => {
                nonempty(relationship, "relationship")?;
                source.validate()?;
                target.validate()?;
            }
            Self::InvalidateModel { partition, model } => {
                if let Some(partition) = partition {
                    partition.validate()?;
                }
                nonempty(model, "model")?;
            }
            Self::InvalidateRelationship {
                relationship,
                source,
            } => {
                nonempty(relationship, "relationship")?;
                source.validate()?;
            }
        }
        Ok(())
    }

    fn scope(&self) -> OperationScope {
        match self {
            Self::Upsert { scope, .. } | Self::Patch { scope, .. } | Self::Delete { scope } => {
                OperationScope::Record(scope.clone())
            }
            Self::Link {
                relationship,
                source,
                target,
            }
            | Self::Unlink {
                relationship,
                source,
                target,
            } => OperationScope::Edge {
                relationship: relationship.clone(),
                source: source.clone(),
                target: target.clone(),
            },
            Self::InvalidateModel { partition, model } => OperationScope::Model {
                partition: partition.clone(),
                model: model.clone(),
            },
            Self::InvalidateRelationship {
                relationship,
                source,
            } => OperationScope::Relationship {
                relationship: relationship.clone(),
                source: source.clone(),
            },
        }
    }
}

/// Mirrors the declaration order of the authoritative Rust wire type. Derived
/// `Ord` is part of the frozen canonical contract; JSON tag spelling is not.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum OperationScope {
    Record(ProjectionScope),
    Edge {
        relationship: String,
        source: ProjectionScope,
        target: ProjectionScope,
    },
    Model {
        partition: Option<ProjectionPartition>,
        model: String,
    },
    Relationship {
        relationship: String,
        source: ProjectionScope,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProjectionRecovery {
    pub(crate) occurrence_ordinal: u32,
    pub(crate) projection_refs: Vec<u32>,
    pub(crate) condition: RecoveryCondition,
    pub(crate) target: RecoveryTarget,
}

impl ProjectionRecovery {
    fn validate(
        &self,
        projection_count: usize,
        occurrence_count: usize,
    ) -> Result<(), ClientCompileError> {
        validate_refs(&self.projection_refs, projection_count)?;
        if self.occurrence_ordinal as usize >= occurrence_count {
            return Err(invalid(
                "ProjectionDelta recovery references an absent occurrence",
            ));
        }
        self.target.validate()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecoveryCondition {
    Always,
    IfRecordMissing,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum RecoveryTarget {
    Record {
        scope: ProjectionScope,
    },
    Relationship {
        relationship: String,
        source: ProjectionScope,
    },
    Model {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        partition: Option<ProjectionPartition>,
        model: String,
    },
}

impl RecoveryTarget {
    fn validate(&self) -> Result<(), ClientCompileError> {
        match self {
            Self::Record { scope } => scope.validate(),
            Self::Relationship {
                relationship,
                source,
            } => {
                nonempty(relationship, "relationship")?;
                source.validate()
            }
            Self::Model { partition, model } => {
                if let Some(partition) = partition {
                    partition.validate()?;
                }
                nonempty(model, "model")
            }
        }
    }
}

fn validate_refs(refs: &[u32], count: usize) -> Result<(), ClientCompileError> {
    if refs.is_empty()
        || refs.windows(2).any(|pair| pair[0] >= pair[1])
        || refs.iter().any(|value| *value as usize >= count)
    {
        return Err(invalid(
            "ProjectionDelta projection refs must be sorted, unique, and in range",
        ));
    }
    Ok(())
}

fn validate_fields(fields: &[DeltaField]) -> Result<(), ClientCompileError> {
    if fields.windows(2).any(|pair| pair[0].field >= pair[1].field) {
        return Err(invalid("ProjectionDelta fields must be sorted and unique"));
    }
    for field in fields {
        nonempty(&field.field, "delta field")?;
        field.value.validate(1)?;
    }
    Ok(())
}

fn validate_names(names: &[String], label: &str) -> Result<(), ClientCompileError> {
    if names.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(invalid(format!(
            "ProjectionDelta {label} must be sorted and unique"
        )));
    }
    for name in names {
        nonempty(name, label)?;
    }
    Ok(())
}

fn validate_prefixed_hash(value: &str, prefix: &str) -> Result<(), ClientCompileError> {
    let Some(hash) = value.strip_prefix(prefix) else {
        return Err(invalid("invalid ProjectionDelta opaque identity"));
    };
    if hash.len() != 71
        || !hash.starts_with("sha256:")
        || !hash[7..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid("invalid ProjectionDelta opaque identity"));
    }
    Ok(())
}

fn validate_token(value: &str, purpose: &str) -> Result<(), ClientCompileError> {
    let mut parts = value.split('.');
    if parts.next() != Some("v1") || parts.next() != Some(purpose) || parts.clone().count() != 1 {
        return Err(invalid("invalid ProjectionDelta protocol token"));
    }
    let encoded = parts
        .next()
        .ok_or_else(|| invalid("invalid ProjectionDelta protocol token"))?;
    let decoded = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| invalid("invalid ProjectionDelta protocol token"))?;
    if decoded.len() != 32 || URL_SAFE_NO_PAD.encode(decoded) != encoded {
        return Err(invalid("invalid ProjectionDelta protocol token"));
    }
    Ok(())
}

fn canonical_number<T>(value: &str, label: &str) -> Result<(), ClientCompileError>
where
    T: std::str::FromStr + ToString,
{
    value
        .parse::<T>()
        .ok()
        .filter(|parsed| parsed.to_string() == value)
        .map(|_| ())
        .ok_or_else(|| invalid(format!("invalid or noncanonical ProjectionDelta {label}")))
}

fn nonempty(value: &str, label: &str) -> Result<(), ClientCompileError> {
    if value.is_empty() || value.len() > 4 * 1024 || value.trim() != value {
        return Err(invalid(format!("invalid ProjectionDelta {label}")));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> ClientCompileError {
    ClientCompileError::manifest("client.projection_delta.invalid", message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vector() -> ProjectionDeltaWire {
        serde_json::from_slice(
            include_bytes!("../../../../tests/fixtures/projection-delta-v1.json")
                .strip_suffix(b"\n")
                .unwrap(),
        )
        .unwrap()
    }

    fn first_record_scope_mut(delta: &mut ProjectionDeltaWire) -> &mut ProjectionScope {
        delta
            .operations
            .iter_mut()
            .find_map(|operation| match &mut operation.mutation {
                ProjectionMutation::Upsert { scope, .. }
                | ProjectionMutation::Patch { scope, .. }
                | ProjectionMutation::Delete { scope } => Some(scope),
                _ => None,
            })
            .unwrap()
    }

    #[test]
    fn task13_projection_delta_vector_is_byte_exact() {
        let file = include_bytes!("../../../../tests/fixtures/projection-delta-v1.json");
        let canonical = file
            .strip_suffix(b"\n")
            .expect("checked fixture ends in one newline");
        let decoded = ProjectionDeltaWire::from_canonical_bytes(canonical)
            .expect("task13 vector must satisfy compiler wire-v1");
        assert_eq!(decoded.canonical_bytes().unwrap(), canonical);
        assert_eq!(
            decoded.fingerprint().unwrap(),
            "sha256:7bdc06e1d3accc4c62132f967df1310d31f2f3b856fa7e97a7c5d0907a4ae17b"
        );
        assert_eq!(
            hash_bytes(file),
            "sha256:6d406bade34c766ed12554222bd2696a532448b46f8dd48bba2e286b82de94d1"
        );
    }

    #[test]
    fn projection_delta_f64_notation_matches_serde_json_ryu() {
        let canonical = |value: f64| {
            serde_json::Number::from_f64(value)
                .expect("test values are finite")
                .to_string()
        };
        assert_eq!(canonical(1.0), "1.0");
        assert_eq!(canonical(1.5), "1.5");
        assert_eq!(canonical(1e20), "1e+20");
        assert_eq!(canonical(1e-6), "1e-6");
        assert_eq!(canonical(1e-5), "0.00001");
        assert_eq!(canonical(1e15), "1000000000000000.0");
        assert_eq!(canonical(1e16), "1e+16");
    }

    #[test]
    fn projection_delta_wire_rejects_unknown_fields_and_versions() {
        let file = include_bytes!("../../../../tests/fixtures/projection-delta-v1.json");
        let canonical = file.strip_suffix(b"\n").unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(canonical).unwrap();
        value["wire_version"] = serde_json::json!(2);
        assert!(
            ProjectionDeltaWire::from_canonical_bytes(&serde_json::to_vec(&value).unwrap())
                .is_err()
        );
        value["wire_version"] = serde_json::json!(1);
        value["unexpected"] = serde_json::json!(true);
        assert!(
            ProjectionDeltaWire::from_canonical_bytes(&serde_json::to_vec(&value).unwrap())
                .is_err()
        );
    }

    #[test]
    fn application_surface_roles_match_server_identity_validation() {
        for roles in [
            Vec::<String>::new(),
            vec!["".into()],
            vec![" user".into()],
            vec!["user".into(), "user".into()],
            vec!["user".into(), "admin".into()],
            vec!["x".repeat(4 * 1024 + 1)],
        ] {
            let mut delta = vector();
            delta.identity.surface = ProjectionSurfaceIdentity::Application {
                name: "web".into(),
                eligible_roles: roles,
                schema_roles: Vec::new(),
            };
            assert!(
                delta.canonical_bytes().is_err(),
                "invalid application role inventory must fail closed"
            );
        }

        let mut delta = vector();
        delta.identity.surface = ProjectionSurfaceIdentity::Application {
            name: "web".into(),
            eligible_roles: vec!["admin".into(), "user".into()],
            schema_roles: vec!["admin".into(), "user".into()],
        };
        assert!(delta.canonical_bytes().is_ok());
    }

    #[test]
    fn mixed_operation_and_recovery_kinds_use_frozen_enum_order() {
        let canonical = vector();
        assert!(matches!(
            canonical.operations[0].mutation,
            ProjectionMutation::Upsert { .. }
        ));
        assert!(matches!(
            canonical.operations[3].mutation,
            ProjectionMutation::Link { .. }
        ));
        assert!(matches!(
            canonical.operations[6].mutation,
            ProjectionMutation::InvalidateModel { .. }
        ));
        assert!(matches!(
            canonical.operations[7].mutation,
            ProjectionMutation::InvalidateRelationship { .. }
        ));
        assert!(matches!(
            canonical.recoveries[0].target,
            RecoveryTarget::Record { .. }
        ));
        assert!(matches!(
            canonical.recoveries[1].target,
            RecoveryTarget::Relationship { .. }
        ));
        assert!(matches!(
            canonical.recoveries[2].target,
            RecoveryTarget::Model { .. }
        ));

        let mut operation_permutation = canonical.clone();
        operation_permutation.operations.swap(2, 3);
        assert!(operation_permutation.validate().is_err());

        let mut recovery_permutation = canonical;
        recovery_permutation.recoveries.swap(0, 1);
        assert!(recovery_permutation.validate().is_err());
    }

    #[test]
    fn wire_validation_rejects_empty_authority_duplicate_occurrences_and_oversized_body() {
        let mut empty_authority = vector();
        empty_authority.projections.clear();
        assert!(empty_authority.validate().is_err());

        let mut duplicate_occurrence = vector();
        duplicate_occurrence.occurrences[1].occurrence_id =
            duplicate_occurrence.occurrences[0].occurrence_id.clone();
        assert!(duplicate_occurrence.validate().is_err());

        assert!(
            ProjectionDeltaWire::from_canonical_bytes(&vec![b' '; MAX_BODY_BYTES + 1]).is_err()
        );
    }

    #[test]
    fn wire_key_validation_matches_scalar_unique_and_size_rules() {
        let mut duplicate_name = vector();
        let scope = first_record_scope_mut(&mut duplicate_name);
        scope.key[1].field = scope.key[0].field.clone();
        assert!(duplicate_name.validate().is_err());

        let mut null_key = vector();
        first_record_scope_mut(&mut null_key).key[0].value = DeltaValue::Null;
        assert!(null_key.validate().is_err());

        let mut composite_key = vector();
        first_record_scope_mut(&mut composite_key).key[0].value =
            DeltaValue::String("x".repeat(MAX_RECORD_KEY_BYTES));
        assert!(composite_key.validate().is_err());
    }

    #[test]
    fn patch_and_conditional_recovery_are_fail_closed() {
        let mut overlap = vector();
        let ProjectionMutation::Patch { set, unset, .. } = &mut overlap.operations[1].mutation
        else {
            panic!("fixture operation 1 is the conditional patch");
        };
        unset.push(set[0].field.clone());
        unset.sort();
        assert!(overlap.validate().is_err());

        let mut unpaired = vector();
        unpaired.operations.remove(1);
        assert!(unpaired.validate().is_err());
    }

    #[test]
    fn empty_masks_are_valid_but_payload_strings_and_epochs_are_bounded() {
        let mut delta = vector();
        let ProjectionMutation::Upsert {
            fields, replace, ..
        } = &mut delta.operations[0].mutation
        else {
            panic!("fixture operation 0 is an upsert");
        };
        fields.clear();
        replace.clear();
        delta.operations[0].mutation.validate().unwrap();

        assert!(DeltaValue::String("x".repeat(MAX_BODY_BYTES + 1))
            .validate(1)
            .is_err());
        let mut identity = delta.projections[0].clone();
        identity.epoch = "x".repeat(129);
        assert!(identity.validate().is_err());
    }
}
