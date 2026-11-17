use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::canonical_json_value;

#[derive(Clone, Debug)]
pub(crate) struct ClientManifest {
    pub(crate) service_id: String,
    pub(crate) surface: ManifestSurface,
    pub(crate) schema_fingerprint: String,
    pub(crate) protocol_fingerprint: String,
    pub(crate) execution: ManifestExecutionLimits,
    pub(crate) capabilities: ManifestCapabilities,
    pub(crate) scalar_codecs: BTreeMap<String, String>,
    pub(crate) models: BTreeMap<String, ManifestModel>,
    pub(crate) roots: BTreeMap<(RootOperation, String), ManifestRoot>,
    pub(crate) commands: Vec<ManifestCommand>,
    /// Exact scope-wide descriptor inventory derived from command-local
    /// presets plus every client-visible row-policy claim/column codec.
    pub(crate) trusted_presets: Vec<ManifestTrustedPresetDescriptor>,
    pub(crate) commands_requiring_revalidation: BTreeSet<String>,
    pub(crate) protocol_operations: ManifestProtocolOperations,
    pub(crate) projectors: Vec<ManifestProjector>,
    pub(crate) projection_programs: Vec<ManifestProjectionProgram>,
    pub(crate) projection_bindings: Vec<ManifestProjectionBinding>,
    pub(crate) leaderboards: Vec<super::ManifestLeaderboard>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestExecutionLimits {
    pub(crate) max_depth: u64,
    pub(crate) max_complexity: u64,
    pub(crate) max_bool_width: u64,
    pub(crate) max_in_list: u64,
    pub(crate) complexity: ManifestComplexityWeights,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestComplexityWeights {
    pub(crate) version: u32,
    pub(crate) scalar: u64,
    pub(crate) belongs_to: u64,
    pub(crate) has_many: u64,
    pub(crate) m2m: u64,
    pub(crate) aggregate: u64,
    pub(crate) list_root: u64,
    pub(crate) by_pk: u64,
    pub(crate) list_fanout: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ManifestSurface {
    Role {
        name: String,
    },
    Application {
        name: String,
        eligible_roles: Vec<String>,
        schema_roles: Vec<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestCapabilities {
    pub(crate) live_queries: bool,
    pub(crate) record_revisions: bool,
    pub(crate) tombstones: bool,
    pub(crate) causal_receipts: bool,
    pub(crate) live_resume: bool,
    pub(crate) query_fallback: String,
    pub(crate) cache_scope: bool,
    pub(crate) confirmed_persistence: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestScalarCodec {
    pub(crate) scalar: String,
    pub(crate) codec: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestModel {
    pub(crate) id: String,
    pub(crate) typename: String,
    pub(crate) source_table: String,
    pub(crate) dependencies: Vec<String>,
    pub(crate) normalization: ManifestNormalization,
    pub(crate) fields: Vec<ManifestField>,
    pub(crate) relationships: Vec<ManifestRelationship>,
    pub(crate) filter_input: ManifestFilterInput,
    pub(crate) row_policy: ManifestRowPolicy,
    pub(crate) record_revisions: bool,
    pub(crate) tombstones: bool,
}

impl ManifestModel {
    pub(crate) fn field(&self, name: &str) -> Option<&ManifestField> {
        self.fields.iter().find(|field| field.name == name)
    }

    pub(crate) fn relationship(&self, name: &str) -> Option<&ManifestRelationship> {
        self.relationships
            .iter()
            .find(|relationship| relationship.name == name)
    }

    pub(crate) fn identity(&self) -> Option<&[ManifestKeyField]> {
        match &self.normalization {
            ManifestNormalization::Normalized { fields, .. } => Some(fields),
            ManifestNormalization::Embedded => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ManifestNormalization {
    Normalized {
        fields: Vec<ManifestKeyField>,
        encoding: String,
    },
    Embedded,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestKeyField {
    pub(crate) name: String,
    pub(crate) codec: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestField {
    pub(crate) name: String,
    pub(crate) scalar: String,
    pub(crate) codec: String,
    pub(crate) nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestFilterInput {
    pub(crate) type_name: String,
    pub(crate) fields: Vec<ManifestFilterField>,
    pub(crate) relationships: Vec<ManifestFilterInputRelationship>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestFilterInputRelationship {
    pub(crate) field: String,
    pub(crate) target_type: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ManifestRowPolicy {
    Unrestricted,
    Predicate { expression: ManifestFilterExpr },
    ServerOnly,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub(crate) enum ManifestFilterExpr {
    And(Vec<ManifestFilterExpr>),
    Or(Vec<ManifestFilterExpr>),
    Not(Box<ManifestFilterExpr>),
    Cmp {
        column: String,
        op: ManifestCmpOp,
        rhs: ManifestOperand,
    },
    In {
        column: String,
        values: Vec<ManifestOperand>,
        negated: bool,
    },
    IsNull {
        column: String,
        is_null: bool,
    },
    Rel {
        field: String,
        predicate: Box<ManifestFilterExpr>,
    },
}

impl Serialize for ManifestFilterExpr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde_json::json;

        fn wire_value(expression: &ManifestFilterExpr) -> JsonValue {
            match expression {
                ManifestFilterExpr::And(items) => json!({
                    "kind": "and",
                    "value": items.iter().map(wire_value).collect::<Vec<_>>()
                }),
                ManifestFilterExpr::Or(items) => json!({
                    "kind": "or",
                    "value": items.iter().map(wire_value).collect::<Vec<_>>()
                }),
                ManifestFilterExpr::Not(item) => {
                    json!({"kind": "not", "value": wire_value(item)})
                }
                ManifestFilterExpr::Cmp { column, op, rhs } => json!({
                    "kind": "cmp",
                    "value": {"column": column, "op": op, "rhs": rhs}
                }),
                ManifestFilterExpr::In {
                    column,
                    values,
                    negated,
                } => json!({
                    "kind": "in",
                    "value": {
                        "column": column,
                        "values": values,
                        "negated": negated
                    }
                }),
                ManifestFilterExpr::IsNull { column, is_null } => json!({
                    "kind": "is_null",
                    "value": {"column": column, "is_null": is_null}
                }),
                ManifestFilterExpr::Rel { field, predicate } => json!({
                    "kind": "rel",
                    "value": {"field": field, "predicate": wire_value(predicate)}
                }),
            }
        }

        canonical_json_value(wire_value(self)).serialize(serializer)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub(crate) enum ManifestOperand {
    Lit(ManifestLitValue),
    Claim(ManifestClaimRef),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub(crate) enum ManifestLitValue {
    String(String),
    I64(i64),
    F64(f64),
    Bool(bool),
    Json(JsonValue),
    Null,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestClaimRef {
    pub(crate) header: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ManifestCmpOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    Like,
    Ilike,
    Contains,
    ContainedIn,
    HasKey,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestRelationship {
    pub(crate) name: String,
    pub(crate) target_model: String,
    pub(crate) target_typename: String,
    pub(crate) kind: ManifestRelationshipKind,
    pub(crate) list: bool,
    pub(crate) nullable: bool,
    pub(crate) arguments: Vec<ManifestArgument>,
    pub(crate) key_mapping: ManifestRelationshipKeyMapping,
    pub(crate) maintenance: ManifestRelationshipMaintenance,
    pub(crate) dependencies: Vec<String>,
    pub(crate) filter: Option<ManifestFilterSemantics>,
    pub(crate) order: Option<ManifestOrderSemantics>,
    pub(crate) pagination: Option<ManifestPagination>,
    pub(crate) aggregate: Option<ManifestRelationshipAggregate>,
    pub(crate) live: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ManifestRelationshipKind {
    HasMany,
    BelongsTo,
    ManyToMany,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ManifestRelationshipKeyMapping {
    Direct {
        local: Vec<String>,
        remote: Vec<String>,
    },
    Through {
        local: Vec<String>,
        remote: Vec<String>,
        table: String,
        source_foreign_key: Vec<String>,
        target_foreign_key: Vec<String>,
    },
    ThroughOpaque {
        local: Vec<String>,
        remote: Vec<String>,
        dependency: String,
    },
    Embedded,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ManifestRelationshipMaintenance {
    Local,
    Revalidate,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestRelationshipAggregate {
    pub(crate) name: String,
    pub(crate) arguments: Vec<ManifestArgument>,
    pub(crate) semantics: ManifestAggregateSemantics,
    pub(crate) dependencies: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RootOperation {
    Query,
    Subscription,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RootKind {
    List,
    ByPk,
    Aggregate,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestRoot {
    pub(crate) id: String,
    pub(crate) operation: RootOperation,
    pub(crate) name: String,
    pub(crate) kind: RootKind,
    pub(crate) model: String,
    pub(crate) arguments: Vec<ManifestArgument>,
    pub(crate) filter: Option<ManifestFilterSemantics>,
    pub(crate) order: Option<ManifestOrderSemantics>,
    pub(crate) pagination: Option<ManifestPagination>,
    pub(crate) aggregate: Option<ManifestAggregateSemantics>,
    pub(crate) dependencies: Vec<String>,
    pub(crate) live: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestArgument {
    pub(crate) name: String,
    pub(crate) kind: ManifestArgumentKind,
    pub(crate) type_name: String,
    pub(crate) nullable: bool,
    pub(crate) list: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) codec: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ManifestArgumentKind {
    Filter,
    Order,
    Limit,
    Offset,
    PrimaryKey,
}

impl ManifestArgument {
    pub(crate) fn graphql_type(&self) -> String {
        let base = if self.list {
            format!("[{}!]", self.type_name)
        } else {
            self.type_name.clone()
        };
        if self.nullable {
            base
        } else {
            format!("{base}!")
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestFilterSemantics {
    pub(crate) fields: Vec<ManifestFilterField>,
    pub(crate) relationships: Vec<String>,
    pub(crate) row_policy: ManifestRowPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestFilterField {
    pub(crate) name: String,
    pub(crate) operators: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestOrderSemantics {
    pub(crate) fields: Vec<String>,
    pub(crate) values: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestPagination {
    pub(crate) kind: String,
    pub(crate) default_limit: u64,
    pub(crate) max_limit: u64,
    pub(crate) coverage: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestAggregateSemantics {
    pub(crate) wrapper_typename: String,
    pub(crate) fields_typename: String,
    pub(crate) nodes_pagination: ManifestPagination,
    pub(crate) count: bool,
    pub(crate) nodes: bool,
    pub(crate) sum: Vec<String>,
    pub(crate) avg: Vec<String>,
    pub(crate) min: Vec<String>,
    pub(crate) max: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestCommand {
    pub(crate) version: u32,
    pub(crate) name: String,
    pub(crate) mutation_field: String,
    pub(crate) grants: Vec<String>,
    pub(crate) input: ManifestCommandShape,
    pub(crate) output: ManifestCommandShape,
    pub(crate) operation: String,
    pub(crate) operation_hash: String,
    pub(crate) extensions: ManifestCommandExtensions,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ManifestCommandShape {
    None,
    Object { definition: ManifestTypeDef },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestTypeDef {
    pub(crate) name: String,
    pub(crate) fields: Vec<ManifestTypeField>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestTypeField {
    pub(crate) name: String,
    pub(crate) type_name: String,
    pub(crate) nullable: bool,
    pub(crate) list: bool,
    pub(crate) item_nullable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) codec: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) nested: Option<Box<ManifestTypeDef>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestCommandExtensions {
    pub(crate) version: u32,
    pub(crate) consistency: ManifestCommandConsistency,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) direct_projection: Option<ManifestDirectProjection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) input_defaults: Option<ManifestInputDefaults>,
    #[serde(
        default,
        skip_serializing,
        deserialize_with = "reject_legacy_command_authority"
    )]
    pub(crate) effects: Option<ManifestEffects>,
    #[serde(
        default,
        skip_serializing,
        deserialize_with = "reject_legacy_command_authority"
    )]
    pub(crate) confirmations: Option<ManifestConfirmations>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) projection: Option<ManifestCommandProjection>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) trusted_presets: Vec<ManifestTrustedPresetDescriptor>,
}

fn reject_legacy_command_authority<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let _ = serde::de::IgnoredAny::deserialize(deserializer)?;
    Err(serde::de::Error::custom(
        "client manifest v2 rejects legacy command effects/confirmations authority",
    ))
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestTrustedPresetDescriptor {
    pub(crate) name: String,
    pub(crate) codec: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestCommandConsistency {
    pub(crate) version: u32,
    pub(crate) kind: ManifestConsistencyKind,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestDirectProjection {
    pub(crate) topology: ManifestProjectionTopologyIdentity,
    pub(crate) model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) partition: Option<ManifestEffectExpression>,
    pub(crate) change_epoch: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestProjectionTopologyIdentity {
    pub(crate) version: u32,
    pub(crate) name: String,
    pub(crate) digest: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ManifestConsistencyKind {
    Succeeded,
    Eventual,
    Atomic,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestInputDefaults {
    pub(crate) version: u32,
    pub(crate) defaults: Vec<ManifestInputDefault>,
}

impl Serialize for ManifestInputDefaults {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct Wire<'a> {
            version: u32,
            defaults: &'a [JsonValue],
        }

        let defaults = self
            .defaults
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()
            .map_err(serde::ser::Error::custom)?;
        Wire {
            version: self.version,
            defaults: &defaults,
        }
        .serialize(serializer)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestInputDefault {
    pub(crate) path: Vec<String>,
    pub(crate) generator: ManifestInputDefaultGenerator,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ManifestInputDefaultGenerator {
    UuidV7,
    Ulid,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestEffects {
    pub(crate) version: u32,
    pub(crate) operations: Vec<ManifestEffect>,
    pub(crate) fallback: ManifestRevalidationFallback,
}

impl Serialize for ManifestEffects {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct Wire<'a> {
            version: u32,
            operations: &'a [JsonValue],
            fallback: ManifestRevalidationFallback,
        }

        let operations = self
            .operations
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()
            .map_err(serde::ser::Error::custom)?;
        Wire {
            version: self.version,
            operations: &operations,
            fallback: self.fallback,
        }
        .serialize(serializer)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ManifestEffect {
    Upsert {
        model: String,
        key: ManifestEffectKey,
        fields: Vec<ManifestEffectField>,
    },
    Patch {
        model: String,
        key: ManifestEffectKey,
        fields: Vec<ManifestEffectField>,
    },
    Delete {
        model: String,
        key: ManifestEffectKey,
    },
    Link {
        relationship: ManifestEffectRelationship,
        source: ManifestEffectKey,
        target: ManifestEffectKey,
    },
    Unlink {
        relationship: ManifestEffectRelationship,
        source: ManifestEffectKey,
        target: ManifestEffectKey,
    },
    InvalidateModel {
        model: String,
    },
    InvalidateRelationship {
        relationship: ManifestEffectRelationship,
        source: ManifestEffectKey,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestEffectRelationship {
    pub(crate) source_model: String,
    pub(crate) field: String,
    pub(crate) target_model: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestEffectKey {
    pub(crate) fields: Vec<ManifestEffectField>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestEffectField {
    pub(crate) field: String,
    pub(crate) value: ManifestEffectExpression,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ManifestEffectExpression {
    Input { path: Vec<String> },
    TrustedPreset { name: String },
    Constant { value: JsonValue },
    Null,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ManifestRevalidationFallback {
    Revalidate,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestConfirmations {
    pub(crate) version: u32,
    pub(crate) kind: ManifestConfirmationKind,
    pub(crate) expected: Vec<ManifestConfirmation>,
    pub(crate) fallback: ManifestRevalidationFallback,
}

impl Serialize for ManifestConfirmations {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct Wire<'a> {
            version: u32,
            kind: ManifestConfirmationKind,
            expected: &'a [JsonValue],
            fallback: ManifestRevalidationFallback,
        }

        let expected = self
            .expected
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()
            .map_err(serde::ser::Error::custom)?;
        Wire {
            version: self.version,
            kind: self.kind,
            expected: &expected,
            fallback: self.fallback,
        }
        .serialize(serializer)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ManifestConfirmationKind {
    Finite,
    Unavailable,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestConfirmation {
    pub(crate) projector: String,
    pub(crate) model: String,
    pub(crate) key: ManifestEffectKey,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) partition: Option<ManifestEffectExpression>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestProtocolOperations {
    pub(crate) version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) command_status: Option<ManifestProtocolOperation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestProtocolOperation {
    pub(crate) name: String,
    pub(crate) operation: String,
    pub(crate) operation_hash: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestProjector {
    pub(crate) version: u32,
    pub(crate) name: String,
    pub(crate) facts: Vec<String>,
    pub(crate) models: Vec<String>,
    pub(crate) dependencies: Vec<String>,
    pub(crate) causal_confirmation: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestProjectionEventRef {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestProjectionBinding {
    pub(crate) version: u32,
    pub(crate) binding_id: String,
    pub(crate) program_id: String,
    pub(crate) epoch: String,
    pub(crate) state: ManifestProjectionBindingState,
    pub(crate) placement: ManifestProjectionPlacement,
    pub(crate) execution_class: ManifestProjectionExecutionClass,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ManifestProjectionBindingState {
    Active,
    Draining,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ManifestProjectionPlacement {
    Eventual,
    Direct,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ManifestProjectionExecutionClass {
    Causal,
    Background,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestProjectionProgram {
    pub(crate) version: u32,
    pub(crate) program_id: String,
    pub(crate) name: String,
    pub(crate) program_version: u64,
    pub(crate) ir_version: u16,
    pub(crate) operation_semantics_version: u16,
    pub(crate) arms: Vec<ManifestProjectionArm>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestProjectionArm {
    pub(crate) arm: String,
    pub(crate) event: ManifestProjectionEventRef,
    pub(crate) partition: ManifestProjectionPartition,
    pub(crate) operations: Vec<ManifestProjectionOperation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ManifestProjectionPartition {
    Unit,
    Expression {
        expression: ManifestProjectionExpression,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestProjectionOperation {
    pub(crate) operation: String,
    pub(crate) ordinal: u32,
    pub(crate) kind: ManifestProjectionMutationKind,
    pub(crate) model: String,
    pub(crate) key: Vec<ManifestProjectionKeyField>,
    pub(crate) fields: Vec<ManifestProjectionField>,
    pub(crate) relationships: Vec<ManifestProjectionRelationshipEffect>,
    pub(crate) invalidations: Vec<ManifestProjectionInvalidation>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ManifestProjectionMutationKind {
    Insert,
    Upsert,
    Patch,
    UpsertPatch,
    Delete,
    Recreate,
    InsertRelated,
    UpsertRelated,
    InvalidateModel,
    InvalidateRelationship,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestProjectionKeyField {
    pub(crate) ordinal: u32,
    pub(crate) name: String,
    pub(crate) expression: ManifestProjectionExpression,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestProjectionField {
    pub(crate) ordinal: u32,
    pub(crate) name: String,
    pub(crate) assignment: ManifestProjectionAssignment,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ManifestProjectionAssignment {
    Set {
        expression: ManifestProjectionExpression,
    },
    Unset,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ManifestProjectionExpression {
    Slot {
        slot: String,
        value_type: ManifestProjectionValueType,
    },
    Envelope {
        field: ManifestProjectionEnvelopeField,
    },
    Constant {
        value: ManifestProjectionValue,
    },
    Enum {
        enum_type: String,
        variant: String,
    },
    List {
        values: Vec<ManifestProjectionExpression>,
    },
    Object {
        fields: Vec<ManifestProjectionObjectField>,
    },
    Transform {
        transform: ManifestProjectionScalarTransform,
        arguments: Vec<ManifestProjectionExpression>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestProjectionObjectField {
    pub(crate) name: String,
    pub(crate) value: ManifestProjectionExpression,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "name",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum ManifestProjectionValueType {
    Boolean,
    I64,
    U64,
    F64,
    String,
    Enum(String),
    Json,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ManifestProjectionScalarTransform {
    StringConcat,
    FirstPresent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ManifestProjectionEnvelopeField {
    OccurrenceVersion,
    EventName,
    EventVersion,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum ManifestProjectionValue {
    Null,
    Boolean(bool),
    I64(String),
    U64(String),
    F64(String),
    String(String),
    Enum { enum_type: String, variant: String },
    List(Vec<ManifestProjectionValue>),
    Object(Vec<ManifestProjectionValueField>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestProjectionValueField {
    pub(crate) name: String,
    pub(crate) value: ManifestProjectionValue,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestProjectionRelationshipEffect {
    pub(crate) ordinal: u32,
    pub(crate) kind: ManifestProjectionRelationshipEffectKind,
    pub(crate) source_model: String,
    pub(crate) relationship: String,
    pub(crate) target_model: String,
    pub(crate) source_key: Vec<ManifestProjectionKeyField>,
    pub(crate) target_key: Vec<ManifestProjectionKeyField>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ManifestProjectionRelationshipEffectKind {
    Link,
    Unlink,
    Invalidate,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ManifestProjectionInvalidation {
    Model {
        model: String,
    },
    Relationship {
        source_model: String,
        relationship: String,
        target_model: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestCommandProjection {
    pub(crate) version: u32,
    pub(crate) event_set: Vec<ManifestProjectionEventRef>,
    pub(crate) program_arms: Vec<ManifestCommandProjectionArmRef>,
    pub(crate) preview_occurrences: Vec<ManifestCommandProjectionPreviewOccurrence>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) pure_reduces: Vec<ManifestCommandPureReduce>,
    pub(crate) fallback: ManifestProjectionFallback,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestCommandPureReduce {
    pub(crate) fn_name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub(crate) client_module: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub(crate) client_export: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub(crate) wasm_package: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub(crate) wasm_export: String,
    pub(crate) model: String,
    pub(crate) key: Vec<ManifestCommandPureArg>,
    pub(crate) args: Vec<ManifestCommandPureArg>,
    pub(crate) assign: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestCommandPureArg {
    pub(crate) name: String,
    pub(crate) source: ManifestProjectionPreviewSource,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ManifestProjectionFallback {
    Revalidate,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestCommandProjectionArmRef {
    pub(crate) event: ManifestProjectionEventRef,
    pub(crate) program_id: String,
    pub(crate) arm: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestCommandProjectionPreviewOccurrence {
    pub(crate) ordinal: u32,
    pub(crate) event: ManifestProjectionEventRef,
    pub(crate) values: Vec<ManifestCommandProjectionPreviewValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestCommandProjectionPreviewValue {
    pub(crate) slot: String,
    pub(crate) source: ManifestProjectionPreviewSource,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ManifestProjectionPreviewSource {
    Input { path: Vec<String> },
    GeneratedDefault { path: Vec<String> },
    TrustedPreset { name: String, codec: String },
    Constant { value: ManifestProjectionValue },
    Null,
    Absent,
    Unknown,
}
