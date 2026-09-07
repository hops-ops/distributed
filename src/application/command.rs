use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::error::{ApplicationError, ApplicationResult};
use super::identity::{canonical_json, sha256_fingerprint, LogicalId};
use crate::command::{
    CommandConsistency, CommandInputType, CommandOutcome, CommandTypeDef, TypedCommand,
    TypedCommandContract,
};

/// Serializable command type field used by a portable command contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CommandTypeField {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
    pub list: bool,
    pub item_nullable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nested: Option<Box<CommandTypeSpec>>,
}

/// Serializable command input/output type used by a portable command contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CommandTypeSpec {
    pub name: String,
    pub fields: Vec<CommandTypeField>,
}

pub type TypeSpec = CommandTypeSpec;

impl From<&CommandTypeDef> for CommandTypeSpec {
    fn from(definition: &CommandTypeDef) -> Self {
        Self {
            name: definition.name.clone(),
            fields: definition
                .fields
                .iter()
                .map(|field| CommandTypeField {
                    name: field.name.clone(),
                    type_name: field.type_name.clone(),
                    nullable: field.nullable,
                    list: field.list,
                    item_nullable: field.item_nullable,
                    nested: field.nested.as_deref().map(Self::from).map(Box::new),
                })
                .collect(),
        }
    }
}

/// One exact outward event identity referenced by a command declaration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EventSpec {
    pub name: String,
    pub version: u64,
    pub body_type: String,
    pub body_version: u64,
    pub body_schema: String,
    pub body_fingerprint: String,
    pub body_codec: String,
    pub body_codec_version: u16,
}

/// The portable half of one typed command declaration.
///
/// The effect, default, confirmation, and projection values are copied from
/// the existing typed-command IR. They remain explicit JSON-shaped data; no
/// handler symbol, Rust `TypeId`, closure, or machine path is retained.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CommandSpec {
    pub id: String,
    pub field_name: String,
    pub roles: Vec<String>,
    pub input: CommandTypeSpec,
    pub output: CommandTypeSpec,
    pub consistency: CommandConsistency,
    pub defaults: serde_json::Value,
    pub effects: serde_json::Value,
    pub emits: Vec<EventSpec>,
    pub applies: serde_json::Value,
    pub projection_contract: serde_json::Value,
    /// Declaration-owned projector/model/key confirmations. These are the
    /// portable proof inputs; topology pointers and schemas are intentionally
    /// reduced to their canonical identities by `canonical_value()`.
    #[serde(default)]
    pub confirmations: Vec<serde_json::Value>,
    /// Canonical direct-projection proof material, if the outcome is Atomic.
    /// The erased Rust type identity is never copied into this value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direct_projection: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projected_model: Option<String>,
    pub fingerprint: String,
}

/// Session admission implied by command roles (no extra login guard).
pub fn command_roles_require_principal(roles: &[String]) -> bool {
    !roles.is_empty() && roles.iter().all(|role| role != "anonymous")
}

/// Admit a command from roles + optional user id + asserted session roles.
pub fn admit_command_session(
    roles: &[String],
    user_id: Option<&str>,
    session_roles: &[&str],
) -> Result<(), &'static str> {
    if command_roles_require_principal(roles) && user_id.map(str::trim).unwrap_or("").is_empty() {
        return Err("unauthenticated");
    }
    if roles.is_empty() {
        return Ok(());
    }
    if session_roles
        .iter()
        .any(|role| roles.iter().any(|allowed| allowed == *role))
    {
        return Ok(());
    }
    if roles.iter().any(|role| role == "anonymous") && session_roles.is_empty() {
        return Ok(());
    }
    Err("forbidden")
}

/// One review-visible declaration containing its portable spec and, when the
/// runtime feature is present, the derived executable mount. Keeping these
/// values together prevents parallel `commands`/`mounts` inventories.
pub struct CommandDefinition {
    spec: CommandSpec,
    /// The exact typed declaration that produced `spec`. This is retained for
    /// contract-only composition so Surface authorization can bind from the
    /// declaration-owned GraphQL shapes and effects without reconstructing a
    /// lossy command from public JSON.
    typed_contract: Option<TypedCommandContract>,
    mount: Option<CommandMount>,
}

impl Clone for CommandDefinition {
    fn clone(&self) -> Self {
        Self {
            spec: self.spec.clone(),
            typed_contract: self.typed_contract.clone(),
            mount: self.mount.clone(),
        }
    }
}

impl std::fmt::Debug for CommandDefinition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CommandDefinition")
            .field("command", &self.spec.id)
            .field("typed_contract", &self.typed_contract.is_some())
            .field("runtime_mount", &self.mount.is_some())
            .finish()
    }
}

impl CommandDefinition {
    pub fn contract(spec: CommandSpec) -> Self {
        Self {
            spec,
            typed_contract: None,
            mount: None,
        }
    }

    pub fn with_mount(spec: CommandSpec, mount: CommandMount) -> ApplicationResult<Self> {
        if spec.id != mount.spec().id || spec.fingerprint != mount.spec().fingerprint {
            return Err(ApplicationError::Collision {
                kind: "command",
                identity: spec.id,
                reason: "command definition and executable mount do not share one spec identity"
                    .into(),
            });
        }
        Ok(Self {
            spec,
            typed_contract: None,
            mount: Some(mount),
        })
    }

    /// Retain one exact typed declaration beside its portable spec and
    /// optional executable mount. The typed contract is never serialized; it
    /// exists so framework-owned Surface compilation can consume the original
    /// declaration before role/application authorization selection.
    pub fn from_typed_command<I, K>(
        command: TypedCommand<I, K>,
        mount: Option<CommandMount>,
    ) -> ApplicationResult<Self>
    where
        I: CommandInputType + serde::de::DeserializeOwned + Send + 'static,
        K: CommandOutcome,
    {
        let (_, typed_contract) = command.into_parts();
        let spec = CommandSpec::from_contract(&typed_contract)?;
        if let Some(mount) = &mount {
            validate_mount_spec(&spec, mount)?;
        }
        Ok(Self {
            spec,
            typed_contract: Some(typed_contract),
            mount,
        })
    }

    pub fn spec(&self) -> &CommandSpec {
        &self.spec
    }

    pub fn mount(&self) -> Option<&CommandMount> {
        self.mount.as_ref()
    }

    pub(crate) fn typed_contract(&self) -> Option<&TypedCommandContract> {
        self.typed_contract.as_ref()
    }
}

fn validate_mount_spec(spec: &CommandSpec, mount: &CommandMount) -> ApplicationResult<()> {
    if spec.id != mount.spec().id || spec.fingerprint != mount.spec().fingerprint {
        return Err(ApplicationError::Collision {
            kind: "command",
            identity: spec.id.clone(),
            reason: "command definition and executable mount do not share one spec identity".into(),
        });
    }
    Ok(())
}

impl CommandSpec {
    /// Roles that are not `anonymous` require a non-empty session user.
    pub fn requires_authenticated_principal(&self) -> bool {
        command_roles_require_principal(&self.roles)
    }

    /// Construct a command spec from already-portable pieces.
    pub fn try_new(
        id: impl Into<String>,
        field_name: impl Into<String>,
        input: CommandTypeSpec,
        output: CommandTypeSpec,
        consistency: CommandConsistency,
    ) -> ApplicationResult<Self> {
        let id = LogicalId::try_new("command", id)?.into_string();
        let field_name = field_name.into();
        if field_name.is_empty() {
            return Err(ApplicationError::InvalidSpec(
                "command field name must not be empty".into(),
            ));
        }
        let mut spec = Self {
            id,
            field_name,
            roles: Vec::new(),
            input,
            output,
            consistency,
            defaults: serde_json::Value::Array(Vec::new()),
            effects: serde_json::Value::Null,
            emits: Vec::new(),
            applies: serde_json::Value::Array(Vec::new()),
            projection_contract: serde_json::Value::Null,
            confirmations: Vec::new(),
            direct_projection: None,
            projected_model: None,
            fingerprint: String::new(),
        };
        spec.refresh_fingerprint()?;
        spec.validate()?;
        Ok(spec)
    }

    /// Attach Atomic direct-projection proof material and recompute the fingerprint.
    pub fn with_direct_projection(
        mut self,
        projected_model: impl Into<String>,
        proof: serde_json::Value,
    ) -> ApplicationResult<Self> {
        self.consistency = CommandConsistency::Atomic;
        self.projected_model =
            Some(LogicalId::try_new("projected model", projected_model)?.into_string());
        self.direct_projection = Some(proof);
        self.refresh_fingerprint()?;
        self.validate()?;
        Ok(self)
    }

    /// Bind the authorization- and projector-enriched command material from
    /// an already compiled application Surface back to its portable command.
    pub fn with_surface_binding(
        mut self,
        exposed: &super::module::SurfaceCommandSpec,
    ) -> ApplicationResult<Self> {
        let mut mismatches = Vec::new();
        if self.id != exposed.id {
            mismatches.push("identity");
        }
        if self.field_name != exposed.field_name {
            mismatches.push("field");
        }
        if self.consistency != exposed.consistency {
            mismatches.push("consistency");
        }
        if exposed.confirmation_unavailable {
            mismatches.push("confirmation availability");
        }
        if !mismatches.is_empty() {
            return Err(ApplicationError::Collision {
                kind: "command",
                identity: self.id,
                reason: format!(
                    "compiled Surface command disagrees on {}",
                    mismatches.join(", ")
                ),
            });
        }
        let confirmations = exposed.confirmations.as_array().cloned().ok_or_else(|| {
            ApplicationError::InvalidSpec("surface confirmations must be an array".into())
        })?;
        self.input = exposed
            .input
            .clone()
            .ok_or_else(|| ApplicationError::Missing {
                kind: "command input",
                identity: exposed.id.clone(),
            })?;
        self.output = exposed
            .output
            .clone()
            .ok_or_else(|| ApplicationError::Missing {
                kind: "command output",
                identity: exposed.id.clone(),
            })?;
        self.roles = exposed.roles.clone();
        self.defaults = exposed.defaults.clone();
        self.effects = exposed.effects.clone();
        self.applies = exposed.applies.clone();
        self.projection_contract = exposed.projection_contract.clone();
        self.confirmations = confirmations;
        self.direct_projection = exposed.direct_projection.clone();
        self.projected_model = exposed.projected_model.clone();
        self.refresh_fingerprint()?;
        self.validate()?;
        Ok(self)
    }

    /// Build a portable spec from the framework's existing typed declaration.
    pub fn from_typed_command<I, K>(command: &TypedCommand<I, K>) -> ApplicationResult<Self>
    where
        I: CommandInputType + serde::de::DeserializeOwned + Send + 'static,
        K: CommandOutcome,
    {
        let (_, contract) = command.clone().into_parts();
        Self::from_contract(&contract)
    }

    pub(crate) fn from_contract(
        contract: &crate::command::TypedCommandContract,
    ) -> ApplicationResult<Self> {
        let projection_contract = serde_json::to_value(&contract.projections)?;
        let applies = serde_json::to_value(&contract.projections.previews)?;
        let confirmations = contract
            .confirmations
            .iter()
            .map(crate::command::CommandProjectionConfirmation::canonical_value)
            .collect();
        let direct_projection = contract
            .direct_projection
            .as_ref()
            .map(crate::command::CommandDirectProjectionTarget::canonical_value);
        let mut emits: Vec<EventSpec> = contract
            .projections
            .selectors
            .iter()
            .map(|selector| EventSpec {
                name: selector.event_name().to_owned(),
                version: selector.event_version(),
                body_type: selector.body_type_name().to_owned(),
                body_version: selector.body_version(),
                body_schema: selector.body_schema().to_owned(),
                body_fingerprint: selector.body_fingerprint().to_owned(),
                body_codec: selector.body_codec().to_owned(),
                body_codec_version: selector.body_codec_version(),
            })
            .collect();
        emits.sort_by(|left, right| {
            (
                left.name.as_str(),
                left.version,
                left.body_fingerprint.as_str(),
            )
                .cmp(&(
                    right.name.as_str(),
                    right.version,
                    right.body_fingerprint.as_str(),
                ))
        });
        let mut roles = contract.roles.clone();
        roles.sort();
        roles.dedup();
        let mut spec = Self {
            id: LogicalId::try_new("command", contract.name.clone())?.into_string(),
            field_name: contract.field_name.clone(),
            roles,
            input: CommandTypeSpec::from(&contract.input),
            output: CommandTypeSpec::from(&contract.output),
            consistency: contract.consistency,
            defaults: serde_json::to_value(&contract.input_defaults)?,
            effects: serde_json::to_value(&contract.effects)?,
            emits,
            applies,
            projection_contract,
            confirmations,
            direct_projection,
            projected_model: contract
                .projected_model
                .as_ref()
                .map(|projected| projected.model.clone()),
            fingerprint: String::new(),
        };
        spec.refresh_fingerprint()?;
        spec.validate()?;
        Ok(spec)
    }

    pub fn canonical_bytes(&self) -> ApplicationResult<Vec<u8>> {
        let mut value = serde_json::to_value(self)?;
        if let serde_json::Value::Object(fields) = &mut value {
            fields.insert(
                "fingerprint".into(),
                serde_json::Value::String(String::new()),
            );
        }
        serde_json::to_vec(&canonical_json(&value)).map_err(Into::into)
    }

    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    pub fn identity(&self) -> &str {
        &self.id
    }

    /// Validate the portable identity and schema surface without touching a
    /// handler or any runtime state.
    pub fn validate(&self) -> ApplicationResult<()> {
        LogicalId::try_new("command", self.id.clone())?;
        validate_text("command field", &self.field_name)?;
        validate_type("command input", &self.input)?;
        validate_type("command output", &self.output)?;
        for role in &self.roles {
            LogicalId::try_new("command role", role.clone())?;
        }
        if self.roles.windows(2).any(|roles| roles[0] >= roles[1]) {
            return Err(ApplicationError::NonCanonical("command role ordering"));
        }
        let mut event_names = BTreeSet::new();
        for event in &self.emits {
            if !event_names.insert(event.name.clone()) {
                return Err(ApplicationError::Duplicate {
                    kind: "command event",
                    identity: event.name.clone(),
                });
            }
            LogicalId::try_new("event", event.name.clone())?;
            if event.version == 0 || event.body_version == 0 || event.body_codec_version == 0 {
                return Err(ApplicationError::InvalidSpec(format!(
                    "event `{}` versions must be non-zero",
                    event.name
                )));
            }
            validate_text("event body type", &event.body_type)?;
            validate_text("event body schema", &event.body_schema)?;
            validate_sha256("event body fingerprint", &event.body_fingerprint)?;
            validate_text("event body codec", &event.body_codec)?;
        }
        validate_json_contract("command defaults", &self.defaults)?;
        validate_json_contract("command effects", &self.effects)?;
        validate_json_contract("command applies", &self.applies)?;
        validate_json_contract("command projection contract", &self.projection_contract)?;
        for confirmation in &self.confirmations {
            validate_json_contract("command confirmation", confirmation)?;
        }
        if let Some(direct) = &self.direct_projection {
            validate_json_contract("command direct projection", direct)?;
        }
        if let Some(model) = &self.projected_model {
            LogicalId::try_new("model", model.clone())?;
        }
        Ok(())
    }

    pub(crate) fn validate_fingerprint(&self) -> ApplicationResult<()> {
        if self.fingerprint.is_empty() {
            return Err(ApplicationError::NonCanonical("command fingerprint"));
        }
        if sha256_fingerprint(&self.canonical_bytes()?) != self.fingerprint {
            return Err(ApplicationError::NonCanonical("command fingerprint"));
        }
        Ok(())
    }

    pub(crate) fn refresh_fingerprint(&mut self) -> ApplicationResult<()> {
        let bytes = self.canonical_bytes()?;
        self.fingerprint = sha256_fingerprint(&bytes);
        Ok(())
    }
}

fn validate_type(kind: &'static str, definition: &CommandTypeSpec) -> ApplicationResult<()> {
    validate_type_at_depth(kind, definition, 0)
}

fn validate_type_at_depth(
    kind: &'static str,
    definition: &CommandTypeSpec,
    depth: usize,
) -> ApplicationResult<()> {
    if depth > super::manifest::MAX_MANIFEST_JSON_DEPTH {
        return Err(ApplicationError::InvalidSpec(format!(
            "{kind} nesting exceeds {}",
            super::manifest::MAX_MANIFEST_JSON_DEPTH
        )));
    }
    if definition.fields.len() > super::manifest::MAX_MANIFEST_COLLECTION_ITEMS {
        return Err(ApplicationError::InvalidSpec(format!(
            "command type fields count exceeds {}",
            super::manifest::MAX_MANIFEST_COLLECTION_ITEMS
        )));
    }
    validate_text(kind, &definition.name)?;
    let mut field_names = BTreeSet::new();
    for field in &definition.fields {
        if !field_names.insert(field.name.clone()) {
            return Err(ApplicationError::Duplicate {
                kind: "command type field",
                identity: field.name.clone(),
            });
        }
        validate_text("command field", &field.name)?;
        validate_text("command field type", &field.type_name)?;
        if let Some(nested) = &field.nested {
            validate_type_at_depth(kind, nested, depth + 1)?;
        }
    }
    Ok(())
}

fn validate_sha256(kind: &'static str, value: &str) -> ApplicationResult<()> {
    validate_text(kind, value)?;
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(ApplicationError::InvalidSpec(format!(
            "{kind} must use the sha256:<64 lowercase hex> form"
        )));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ApplicationError::InvalidSpec(format!(
            "{kind} must use the sha256:<64 lowercase hex> form"
        )));
    }
    Ok(())
}

fn validate_text(kind: &'static str, value: &str) -> ApplicationResult<()> {
    if value.trim().is_empty()
        || value.trim() != value
        || value.len() > super::manifest::MAX_MANIFEST_STRING_BYTES
        || value.contains('\0')
    {
        return Err(ApplicationError::InvalidIdentity {
            kind,
            value: value.into(),
            reason: "must be a non-empty portable value",
        });
    }
    Ok(())
}

fn validate_json_contract(kind: &'static str, value: &serde_json::Value) -> ApplicationResult<()> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > super::manifest::MAX_MANIFEST_JSON_BYTES {
        return Err(ApplicationError::InvalidSpec(format!(
            "{kind} exceeds {} JSON bytes",
            super::manifest::MAX_MANIFEST_JSON_BYTES
        )));
    }
    fn walk(kind: &'static str, value: &serde_json::Value, depth: usize) -> ApplicationResult<()> {
        if depth > super::manifest::MAX_MANIFEST_JSON_DEPTH {
            return Err(ApplicationError::InvalidSpec(format!(
                "{kind} exceeds JSON depth {}",
                super::manifest::MAX_MANIFEST_JSON_DEPTH
            )));
        }
        match value {
            serde_json::Value::String(value) => {
                if value.len() > super::manifest::MAX_MANIFEST_STRING_BYTES || value.contains('\0')
                {
                    return Err(ApplicationError::InvalidSpec(format!(
                        "{kind} contains oversized or NUL string material"
                    )));
                }
            }
            serde_json::Value::Array(values) => {
                if values.len() > super::manifest::MAX_MANIFEST_COLLECTION_ITEMS {
                    return Err(ApplicationError::InvalidSpec(format!(
                        "{kind} contains too many values"
                    )));
                }
                for value in values {
                    walk(kind, value, depth + 1)?;
                }
            }
            serde_json::Value::Object(fields) => {
                if fields.len() > super::manifest::MAX_MANIFEST_COLLECTION_ITEMS {
                    return Err(ApplicationError::InvalidSpec(format!(
                        "{kind} contains too many object fields"
                    )));
                }
                for (key, value) in fields {
                    if key.len() > super::manifest::MAX_MANIFEST_STRING_BYTES || key.contains('\0')
                    {
                        return Err(ApplicationError::InvalidSpec(format!(
                            "{kind} contains oversized or NUL object-key material"
                        )));
                    }
                    walk(kind, value, depth + 1)?;
                }
            }
            serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {
            }
        }
        Ok(())
    }
    walk(kind, value, 0)
}

/// Executable command material retained only at the heterogeneous runtime
/// boundary. Its handler is intentionally absent from serialization. The
/// boundary is deliberately a callable request/response trait rather than an
/// `Any` value: a runtime can invoke it without knowing the concrete function
/// item type or attempting a downcast.
pub type CommandMountFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<crate::microsvc::CommandResponse, crate::microsvc::HandlerError>>
            + Send
            + 'a,
    >,
>;

pub trait CommandMountHandler: Send + Sync {
    fn call(&self, request: crate::microsvc::CommandRequest) -> CommandMountFuture<'_>;
}

/// Explicit registration seam used by service/runtime adapters. A mount is
/// not executable merely because it contains a value; an adapter must accept
/// it into its registry before it can be dispatched.
pub trait CommandMountRegistrar {
    fn register_command_mount(
        &mut self,
        mount: CommandMount,
    ) -> Result<(), crate::microsvc::HandlerError>;
}

/// Invocation context for the heterogeneous runtime boundary. The ordinary
/// transport variant is deliberately unable to execute a typed causal mount;
/// only the authenticated variant carries the framework-issued principal and
/// bearer-scoped command identity needed by the existing causal ledger path.
#[cfg(feature = "graphql")]
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum CommandMountInvocation {
    Transport,
    Authenticated {
        command_id: String,
        session: crate::microsvc::Session,
        principal: crate::graphql::identity::VerifiedPrincipal,
    },
}

#[cfg(not(feature = "graphql"))]
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum CommandMountInvocation {
    Transport,
}

#[cfg(feature = "graphql")]
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum CommandMountExecutionResult {
    Transport(crate::microsvc::CommandResponse),
    Causal(crate::microsvc::CausalDispatchResult),
}

#[cfg(not(feature = "graphql"))]
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum CommandMountExecutionResult {
    Transport(crate::microsvc::CommandResponse),
}

#[cfg(feature = "graphql")]
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum CommandMountExecutionError {
    Handler(crate::microsvc::HandlerError),
    Causal(crate::microsvc::CausalDispatchError),
}

#[cfg(not(feature = "graphql"))]
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum CommandMountExecutionError {
    Handler(crate::microsvc::HandlerError),
}

#[allow(dead_code)]
pub(crate) type CommandMountExecutionFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<CommandMountExecutionResult, CommandMountExecutionError>>
            + Send
            + 'a,
    >,
>;

/// Runtime adapter for mounts whose authorization and causal commit protocol
/// lives in a service/router. Adapters receive the same mount spec and must
/// route through the existing `CommandRequest`/`CommandResponse` boundary;
/// authenticated typed mounts additionally enter the existing causal receipt
/// and projection-proof protocol.
#[allow(dead_code)]
pub(crate) trait CommandMountExecution: Send + Sync {
    fn invoke_mount<'a>(
        &'a self,
        mount: &'a CommandMount,
        request: crate::microsvc::CommandRequest,
        invocation: CommandMountInvocation,
    ) -> CommandMountExecutionFuture<'a>;
}

struct RequestCommandMountHandler<H>(H);

impl<H, F> CommandMountHandler for RequestCommandMountHandler<H>
where
    H: Fn(crate::microsvc::CommandRequest) -> F + Send + Sync,
    F: Future<Output = Result<crate::microsvc::CommandResponse, crate::microsvc::HandlerError>>
        + Send
        + 'static,
{
    fn call(&self, request: crate::microsvc::CommandRequest) -> CommandMountFuture<'_> {
        Box::pin((self.0)(request))
    }
}

pub struct CommandMount {
    spec: CommandSpec,
    handler: Option<Arc<dyn CommandMountHandler>>,
    typed_route: Option<String>,
}

impl Clone for CommandMount {
    fn clone(&self) -> Self {
        Self {
            spec: self.spec.clone(),
            handler: self.handler.clone(),
            typed_route: self.typed_route.clone(),
        }
    }
}

impl std::fmt::Debug for CommandMount {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CommandMount")
            .field("command", &self.spec.id)
            .field("callable", &self.handler.is_some())
            .field("typed_route", &self.typed_route)
            .finish()
    }
}

impl CommandMount {
    /// Create a contract-only mount with no executable handler.
    pub fn contract(spec: CommandSpec) -> Self {
        Self {
            spec,
            handler: None,
            typed_route: None,
        }
    }

    /// Erase one executable handler without changing the portable spec.
    pub fn from_handler<H>(spec: CommandSpec, handler: H) -> Self
    where
        H: CommandMountHandler + 'static,
    {
        Self {
            spec,
            handler: Some(Arc::new(handler)),
            typed_route: None,
        }
    }

    /// Adapt an owned request handler to the type-erased mount boundary.
    /// Request and response values remain the existing transport types and
    /// failures retain the framework's typed [`HandlerError`].
    pub fn from_request_handler<H, F>(spec: CommandSpec, handler: H) -> Self
    where
        H: Fn(crate::microsvc::CommandRequest) -> F + Send + Sync + 'static,
        F: Future<Output = Result<crate::microsvc::CommandResponse, crate::microsvc::HandlerError>>
            + Send
            + 'static,
    {
        Self::from_handler(spec, RequestCommandMountHandler(handler))
    }

    /// Create the runtime-facing registration token for a typed causal route.
    /// The token deliberately contains no fake handler closure: execution is
    /// owned by the registered typed route and can only enter through the
    /// authenticated causal protocol.
    pub fn from_typed_route(spec: CommandSpec, route_name: impl Into<String>) -> Self {
        Self {
            spec,
            handler: None,
            typed_route: Some(route_name.into()),
        }
    }

    pub fn spec(&self) -> &CommandSpec {
        &self.spec
    }

    pub(crate) fn typed_route_name(&self) -> Option<&str> {
        self.typed_route.as_deref()
    }

    /// Invoke the erased handler. Contract-only mounts fail closed with a
    /// typed authorization error instead of pretending that a mount is
    /// executable.
    pub fn invoke(&self, request: &crate::microsvc::CommandRequest) -> CommandMountFuture<'_> {
        match &self.handler {
            Some(handler) => handler.call(request.clone()),
            None => Box::pin(async {
                Err(crate::microsvc::HandlerError::Unauthorized(
                    "command mount has no runtime handler".into(),
                ))
            }),
        }
    }

    /// Invoke through a registered runtime adapter. Typed mounts must receive
    /// an authenticated invocation context; the adapter then enters the
    /// existing causal route, preserving authorization, receipts, and
    /// projection proofs. A transport-only invocation remains fail-closed.
    #[allow(dead_code)]
    pub(crate) fn invoke_with<'a, E: CommandMountExecution>(
        &'a self,
        executor: &'a E,
        request: &crate::microsvc::CommandRequest,
        invocation: CommandMountInvocation,
    ) -> CommandMountExecutionFuture<'a> {
        executor.invoke_mount(self, request.clone(), invocation)
    }

    /// Register this mount with an explicit runtime adapter.
    pub fn register_with<R: CommandMountRegistrar>(
        &self,
        registrar: &mut R,
    ) -> Result<(), crate::microsvc::HandlerError> {
        registrar.register_command_mount(self.clone())
    }
}

impl<I, K> TypedCommand<I, K>
where
    I: CommandInputType + serde::de::DeserializeOwned + Send + 'static,
    K: CommandOutcome,
{
    /// Compile the exact declaration into its portable, serializable spec.
    pub fn spec(&self) -> ApplicationResult<CommandSpec> {
        CommandSpec::from_typed_command(self)
    }
}
