use super::*;

fn deserialize_exact_u32<'de, D>(
    deserializer: D,
    expected: u32,
    label: &str,
) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let actual = u32::deserialize(deserializer)?;
    if actual != expected {
        return Err(serde::de::Error::custom(format!(
            "unsupported {label} version {actual}; expected {expected}"
        )));
    }
    Ok(actual)
}

fn deserialize_exact_u16<'de, D>(
    deserializer: D,
    expected: u16,
    label: &str,
) -> Result<u16, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let actual = u16::deserialize(deserializer)?;
    if actual != expected {
        return Err(serde::de::Error::custom(format!(
            "unsupported {label} version {actual}; expected {expected}"
        )));
    }
    Ok(actual)
}

fn deserialize_manifest_version<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_exact_u32(
        deserializer,
        DISTRIBUTED_CLIENT_MANIFEST_VERSION,
        "client manifest",
    )
}

fn deserialize_protocol_version<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_exact_u32(
        deserializer,
        DISTRIBUTED_CLIENT_PROTOCOL_VERSION,
        "client protocol",
    )
}

fn deserialize_command_slots_version<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_exact_u32(
        deserializer,
        COMMAND_EXTENSION_SLOTS_VERSION,
        "command extension slots",
    )
}

fn deserialize_projection_binding_version<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_exact_u32(
        deserializer,
        super::projections::CLIENT_PROJECTION_BINDING_VERSION,
        "projection binding",
    )
}

fn deserialize_projection_program_version<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_exact_u32(
        deserializer,
        super::projections::CLIENT_PROJECTION_PROGRAM_VERSION,
        "projection program",
    )
}

fn deserialize_command_projection_version<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_exact_u32(
        deserializer,
        super::projections::COMMAND_PROJECTION_EXTENSION_VERSION,
        "command projection extension",
    )
}

fn deserialize_projection_ir_version<'de, D>(deserializer: D) -> Result<u16, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_exact_u16(
        deserializer,
        crate::projection::PROJECTION_PROGRAM_IR_VERSION,
        "projection IR",
    )
}

fn deserialize_projection_operation_semantics_version<'de, D>(
    deserializer: D,
) -> Result<u16, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_exact_u16(
        deserializer,
        crate::projection::PROJECTION_OPERATION_SEMANTICS_VERSION,
        "projection operation semantics",
    )
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DistributedClientManifest {
    #[serde(deserialize_with = "deserialize_manifest_version")]
    pub manifest_version: u32,
    #[serde(deserialize_with = "deserialize_protocol_version")]
    pub protocol_version: u32,
    pub service_id: String,
    pub surface: ClientSurfaceIdentity,
    pub schema_fingerprint: String,
    pub protocol_fingerprint: String,
    pub execution: ClientExecutionLimits,
    pub capabilities: ClientCapabilities,
    pub scalar_codecs: Vec<ScalarCodec>,
    pub models: Vec<ClientModel>,
    pub roots: Vec<ClientRoot>,
    pub commands: Vec<ClientCommand>,
    pub protocol_operations: ClientProtocolOperations,
    pub projectors: Vec<ClientProjector>,
    pub projection_programs: Vec<ClientProjectionProgram>,
    pub projection_bindings: Vec<ClientProjectionBinding>,
    /// Leaderboard queries. Omitted when empty, so a manifest without
    /// leaderboards (and its fingerprint) is unchanged; a compiler that does
    /// not know this field refuses a manifest that has one.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub leaderboards: Vec<ClientLeaderboard>,
}

/// One leaderboard surface on the client contract.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientLeaderboard {
    /// GraphQL base name (`xp` → `xpLeaderboard`, …).
    pub name: String,
    /// Object type of each row (`LeaderboardEntry`: `rank`, `member`, `score`).
    pub entry_typename: String,
    /// `high_first` or `low_first`.
    pub order: String,
    /// `single` (no `board` argument) or `family` (`board: String!`).
    pub boards: String,
    pub default_first: u32,
    pub max_first: u32,
    pub max_radius: u32,
    pub roots: Vec<ClientLeaderboardRoot>,
}

/// One leaderboard root the selected roles may run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientLeaderboardRoot {
    pub name: String,
    /// `top`, `around_me`, `my_rank` or `rank`.
    pub operation: String,
    /// `true`: `[LeaderboardEntry!]!`; `false`: nullable `LeaderboardEntry`.
    pub list: bool,
    /// Whether a same-named subscription root exists (`@live`).
    pub live: bool,
    pub arguments: Vec<ClientLeaderboardArgument>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientLeaderboardArgument {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
}

/// Framework-owned operations generated alongside application operations.
///
/// Keeping these documents in the manifest means clients never synthesize a
/// status query or guess the server's protocol field selection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientProtocolOperations {
    pub version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_status: Option<ClientProtocolOperation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientProtocolOperation {
    pub name: String,
    pub operation: String,
    pub operation_hash: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientCapabilities {
    pub live_queries: bool,
    pub record_revisions: bool,
    pub tombstones: bool,
    pub causal_receipts: bool,
    pub live_resume: bool,
    /// Safe behavior whenever exact query evidence or resume is unavailable.
    pub query_fallback: String,
    pub cache_scope: bool,
    /// Durable restore of confirmed normalized state.
    pub confirmed_persistence: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScalarCodec {
    pub scalar: String,
    pub codec: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClientModel {
    pub id: String,
    pub typename: String,
    pub source_table: String,
    pub dependencies: Vec<String>,
    pub normalization: ModelNormalization,
    pub fields: Vec<ClientField>,
    pub relationships: Vec<ClientRelationship>,
    /// Exact GraphQL predicate input owned by this role-selected model.
    ///
    /// This is deliberately model-level rather than copied from a list root or
    /// relationship selection. GraphQL bool-exp relationships exist regardless
    /// of whether the corresponding object field accepts list arguments.
    pub filter_input: ClientFilterInput,
    pub row_policy: ClientRowPolicy,
    pub record_revisions: bool,
    pub tombstones: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ModelNormalization {
    Normalized {
        fields: Vec<ClientKeyField>,
        encoding: String,
    },
    Embedded,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientKeyField {
    pub name: String,
    pub codec: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientField {
    pub name: String,
    pub scalar: String,
    pub codec: String,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientFilterInput {
    pub type_name: String,
    pub fields: Vec<ClientFilterField>,
    pub relationships: Vec<ClientFilterInputRelationship>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientFilterInputRelationship {
    pub field: String,
    pub target_type: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClientRowPolicy {
    Unrestricted,
    Predicate { expression: FilterExpr },
    ServerOnly,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClientRelationship {
    pub name: String,
    pub target_model: String,
    pub target_typename: String,
    pub kind: ClientRelationshipKind,
    pub list: bool,
    /// Copied from the role-filtered Surface. Lists are non-null collections;
    /// singular relationships retain the authoritative object nullability.
    pub nullable: bool,
    pub arguments: Vec<ClientArgument>,
    pub key_mapping: RelationshipKeyMapping,
    pub maintenance: ClientRelationshipMaintenance,
    pub dependencies: Vec<String>,
    pub filter: Option<ClientFilterSemantics>,
    pub order: Option<ClientOrderSemantics>,
    pub pagination: Option<ClientPaginationSemantics>,
    pub aggregate: Option<ClientRelationshipAggregate>,
    pub live: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientRelationshipAggregate {
    pub name: String,
    pub arguments: Vec<ClientArgument>,
    pub semantics: ClientAggregateSemantics,
    pub dependencies: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientRelationshipKind {
    HasMany,
    BelongsTo,
    ManyToMany,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RelationshipKeyMapping {
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
pub enum ClientRelationshipMaintenance {
    /// Incoming normalized entities are sufficient to update membership.
    Local,
    /// Dependency changes mark the relationship stale and trigger revalidation.
    Revalidate,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClientRoot {
    pub id: String,
    pub operation: ClientRootOperation,
    pub name: String,
    pub kind: ClientRootKind,
    pub model: String,
    pub arguments: Vec<ClientArgument>,
    pub filter: Option<ClientFilterSemantics>,
    pub order: Option<ClientOrderSemantics>,
    pub pagination: Option<ClientPaginationSemantics>,
    pub aggregate: Option<ClientAggregateSemantics>,
    pub dependencies: Vec<String>,
    pub live: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientRootOperation {
    Query,
    Subscription,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientRootKind {
    List,
    ByPk,
    Aggregate,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientArgument {
    pub name: String,
    pub kind: ClientArgumentKind,
    pub type_name: String,
    pub nullable: bool,
    pub list: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientArgumentKind {
    Filter,
    Order,
    Limit,
    Offset,
    PrimaryKey,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClientFilterSemantics {
    pub fields: Vec<ClientFilterField>,
    pub relationships: Vec<String>,
    pub row_policy: ClientRowPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientFilterField {
    pub name: String,
    pub operators: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientOrderSemantics {
    pub fields: Vec<String>,
    pub values: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientPaginationSemantics {
    pub kind: String,
    pub default_limit: u64,
    pub max_limit: u64,
    pub coverage: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientAggregateSemantics {
    /// Exact GraphQL wrapper object selected by this root or relationship.
    pub wrapper_typename: String,
    /// Exact GraphQL object returned by the wrapper's `aggregate` field.
    pub fields_typename: String,
    /// Authoritative bounded-window semantics for the wrapper's `nodes` field.
    ///
    /// Aggregate roots do not expose the ordinary list root's pagination
    /// metadata, and relationship aggregates are distinct fields from their
    /// sibling list relationships. Carrying this plan on the aggregate itself
    /// prevents clients from treating an omitted/default-limited `nodes`
    /// selection as a complete model collection.
    pub nodes_pagination: ClientPaginationSemantics,
    pub count: bool,
    pub nodes: bool,
    pub sum: Vec<String>,
    pub avg: Vec<String>,
    pub min: Vec<String>,
    pub max: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientCommand {
    pub version: u32,
    pub name: String,
    pub mutation_field: String,
    pub grants: Vec<String>,
    pub input: ClientCommandShape,
    pub output: ClientCommandShape,
    /// Canonical executable GraphQL operation; clients never synthesize it.
    pub operation: String,
    pub operation_hash: String,
    pub extensions: ClientCommandExtensionSlots,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClientCommandShape {
    None,
    Object { definition: ClientTypeDef },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientTypeDef {
    pub name: String,
    pub fields: Vec<ClientTypeField>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientTypeField {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
    pub list: bool,
    pub item_nullable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nested: Option<Box<ClientTypeDef>>,
}

/// Versioned typed command semantics exported from the executable service.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientCommandExtensionSlots {
    #[serde(deserialize_with = "deserialize_command_slots_version")]
    pub version: u32,
    pub consistency: CommandConsistencyExtension,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direct_projection: Option<CommandDirectProjectionExtension>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_defaults: Option<CommandInputDefaultsExtension>,
    #[serde(
        default,
        skip_serializing,
        deserialize_with = "reject_legacy_command_authority"
    )]
    pub effects: Option<CommandEffectsExtension>,
    #[serde(
        default,
        skip_serializing,
        deserialize_with = "reject_legacy_command_authority"
    )]
    pub confirmations: Option<CommandConfirmationsExtension>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub projection: Option<CommandProjectionExtension>,
    /// Names and wire codecs only. Values are server-derived for the current
    /// verified Session and are never frozen into a generated artifact.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub trusted_presets: Vec<ClientTrustedPresetDescriptor>,
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
pub struct ClientTrustedPresetDescriptor {
    pub name: String,
    pub codec: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandConsistencyExtension {
    pub version: u32,
    pub kind: String,
}

/// Opaque same-transaction target for one `Atomic<T>` command.
///
/// The topology digest binds the scope-codec version, accepted facts, complete
/// owned schemas, partition declaration, and physical ownership on the server.
/// The role-selected client contract therefore needs only this exact identity,
/// never the hidden topology inventory used to compile it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandDirectProjectionExtension {
    pub topology: ClientProjectionTopologyIdentity,
    pub model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition: Option<serde_json::Value>,
    pub change_epoch: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientProjectionTopologyIdentity {
    pub version: u32,
    pub name: String,
    pub digest: String,
}

/// Generators applied once to the canonical command input before dispatch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandInputDefaultsExtension {
    pub version: u32,
    pub defaults: Vec<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandEffectsExtension {
    pub version: u32,
    pub operations: Vec<serde_json::Value>,
    pub fallback: String,
}

/// Declaration-owned projector progress expected after a fact commit.
/// Entries use the same closed input-expression/key IR as optimistic effects.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandConfirmationsExtension {
    pub version: u32,
    /// `finite` contains the complete authorized edge set; `unavailable`
    /// intentionally carries no topology and requires revalidation.
    pub kind: String,
    pub expected: Vec<serde_json::Value>,
    pub fallback: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientProjector {
    pub version: u32,
    pub name: String,
    pub facts: Vec<String>,
    pub models: Vec<String>,
    pub dependencies: Vec<String>,
    pub causal_confirmation: bool,
}

/// Opaque exact outward-event reference.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientProjectionEventRef {
    pub id: String,
    pub name: String,
    pub version: u64,
}

/// Exact role-selected deployment binding. No route, physical topology,
/// output schema, body schema, or join-table details are serialized.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientProjectionBinding {
    #[serde(deserialize_with = "deserialize_projection_binding_version")]
    pub version: u32,
    pub binding_id: String,
    pub program_id: String,
    pub epoch: String,
    pub state: ClientProjectionBindingState,
    pub placement: ClientProjectionPlacement,
    pub execution_class: ClientProjectionExecutionClass,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientProjectionBindingState {
    Active,
    Draining,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientProjectionPlacement {
    Eventual,
    Direct,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientProjectionExecutionClass {
    Causal,
    Background,
}

/// One authorized portable projection program.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientProjectionProgram {
    #[serde(deserialize_with = "deserialize_projection_program_version")]
    pub version: u32,
    pub program_id: String,
    pub name: String,
    pub program_version: u64,
    #[serde(deserialize_with = "deserialize_projection_ir_version")]
    pub ir_version: u16,
    #[serde(deserialize_with = "deserialize_projection_operation_semantics_version")]
    pub operation_semantics_version: u16,
    pub arms: Vec<ClientProjectionArm>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientProjectionArm {
    pub arm: String,
    pub event: ClientProjectionEventRef,
    pub partition: ClientProjectionPartition,
    pub operations: Vec<ClientProjectionOperation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClientProjectionPartition {
    Unit,
    Expression {
        expression: ClientProjectionExpression,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientProjectionOperation {
    pub operation: String,
    pub ordinal: u32,
    pub kind: ClientProjectionMutationKind,
    pub model: String,
    pub key: Vec<ClientProjectionKeyField>,
    pub fields: Vec<ClientProjectionField>,
    pub relationships: Vec<ClientProjectionRelationshipEffect>,
    pub invalidations: Vec<ClientProjectionInvalidation>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientProjectionMutationKind {
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
pub struct ClientProjectionKeyField {
    pub ordinal: u32,
    pub name: String,
    pub expression: ClientProjectionExpression,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientProjectionField {
    pub ordinal: u32,
    pub name: String,
    pub assignment: ClientProjectionAssignment,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClientProjectionAssignment {
    Set {
        expression: ClientProjectionExpression,
    },
    Unset,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClientProjectionExpression {
    Slot {
        slot: String,
        value_type: ClientProjectionValueType,
    },
    Envelope {
        field: ClientProjectionEnvelopeField,
    },
    Constant {
        value: ClientProjectionValue,
    },
    Enum {
        enum_type: String,
        variant: String,
    },
    List {
        values: Vec<ClientProjectionExpression>,
    },
    Object {
        fields: Vec<ClientProjectionObjectField>,
    },
    Transform {
        transform: ClientProjectionScalarTransform,
        arguments: Vec<ClientProjectionExpression>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientProjectionObjectField {
    pub name: String,
    pub value: ClientProjectionExpression,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "name",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ClientProjectionValueType {
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
pub enum ClientProjectionScalarTransform {
    StringConcat,
    FirstPresent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientProjectionEnvelopeField {
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
pub enum ClientProjectionValue {
    Null,
    Boolean(bool),
    I64(String),
    U64(String),
    F64(String),
    String(String),
    Enum { enum_type: String, variant: String },
    List(Vec<ClientProjectionValue>),
    Object(Vec<ClientProjectionValueField>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientProjectionValueField {
    pub name: String,
    pub value: ClientProjectionValue,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientProjectionRelationshipEffect {
    pub ordinal: u32,
    pub kind: ClientProjectionRelationshipEffectKind,
    pub source_model: String,
    pub relationship: String,
    pub target_model: String,
    pub source_key: Vec<ClientProjectionKeyField>,
    pub target_key: Vec<ClientProjectionKeyField>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientProjectionRelationshipEffectKind {
    Link,
    Unlink,
    Invalidate,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClientProjectionInvalidation {
    Model {
        model: String,
    },
    Relationship {
        source_model: String,
        relationship: String,
        target_model: String,
    },
}

/// Projection semantics attached to one generated command.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CommandProjectionExtension {
    #[serde(deserialize_with = "deserialize_command_projection_version")]
    pub version: u32,
    pub event_set: Vec<ClientProjectionEventRef>,
    pub program_arms: Vec<CommandProjectionArmRef>,
    /// Ordered, non-authoritative optimistic occurrences derived by composing
    /// command-known event values with the role-visible projection arms.
    ///
    /// The client applies these in ordinal order as one overlay. Eventual
    /// projection deltas or atomic returned records reconcile that overlay.
    pub preview_occurrences: Vec<CommandProjectionPreviewOccurrence>,
    /// Pure reducers over known cache rows (client auto-optimism).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pure_reduces: Vec<ClientCommandPureReduce>,
    pub fallback: ClientProjectionFallback,
}

/// Pure reduce declaration on the client manifest (server-exported).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientCommandPureReduce {
    pub fn_name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub client_module: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub client_export: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub wasm_package: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub wasm_export: String,
    pub model: String,
    pub key: Vec<ClientCommandPureArg>,
    pub args: Vec<ClientCommandPureArg>,
    pub assign: Vec<String>,
}

/// Pure reduce key/arg with preview-style source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientCommandPureArg {
    pub name: String,
    pub source: ClientProjectionPreviewSource,
}

impl<'de> Deserialize<'de> for CommandProjectionExtension {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            #[serde(deserialize_with = "deserialize_command_projection_version")]
            version: u32,
            event_set: Vec<ClientProjectionEventRef>,
            program_arms: Vec<CommandProjectionArmRef>,
            preview_occurrences: Vec<CommandProjectionPreviewOccurrence>,
            #[serde(default)]
            pure_reduces: Vec<ClientCommandPureReduce>,
            fallback: ClientProjectionFallback,
        }

        let wire = Wire::deserialize(deserializer)?;
        if wire.event_set.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(serde::de::Error::custom(
                "command projection event_set must be sorted and unique",
            ));
        }
        if wire.program_arms.windows(2).any(|pair| {
            (&pair[0].event, &pair[0].program_id, &pair[0].arm)
                >= (&pair[1].event, &pair[1].program_id, &pair[1].arm)
        }) {
            return Err(serde::de::Error::custom(
                "command projection program_arms must be sorted and unique",
            ));
        }
        let arm_events = wire
            .program_arms
            .iter()
            .map(|arm| arm.event.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if arm_events != wire.event_set {
            return Err(serde::de::Error::custom(
                "command projection event_set must exactly match program_arms",
            ));
        }
        for (index, occurrence) in wire.preview_occurrences.iter().enumerate() {
            if usize::try_from(occurrence.ordinal).ok() != Some(index) {
                return Err(serde::de::Error::custom(
                    "command projection preview ordinals must be dense and zero-based",
                ));
            }
            if wire
                .program_arms
                .iter()
                .all(|arm| arm.event != occurrence.event)
            {
                return Err(serde::de::Error::custom(
                    "command projection preview event has no selected program arm",
                ));
            }
            if occurrence
                .values
                .windows(2)
                .any(|pair| pair[0].slot >= pair[1].slot)
            {
                return Err(serde::de::Error::custom(
                    "command projection preview values must be sorted and unique",
                ));
            }
        }

        Ok(Self {
            version: wire.version,
            event_set: wire.event_set,
            program_arms: wire.program_arms,
            preview_occurrences: wire.preview_occurrences,
            pure_reduces: wire.pure_reduces,
            fallback: wire.fallback,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientProjectionFallback {
    Revalidate,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandProjectionArmRef {
    pub event: ClientProjectionEventRef,
    pub program_id: String,
    pub arm: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandProjectionPreviewOccurrence {
    /// Dense ordinal after authorization and eligible-arm filtering.
    pub ordinal: u32,
    pub event: ClientProjectionEventRef,
    pub values: Vec<CommandProjectionPreviewValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandProjectionPreviewValue {
    pub slot: String,
    pub source: ClientProjectionPreviewSource,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClientProjectionPreviewSource {
    Input { path: Vec<String> },
    GeneratedDefault { path: Vec<String> },
    TrustedPreset { name: String, codec: String },
    Constant { value: ClientProjectionValue },
    Null,
    Absent,
    Unknown,
}
