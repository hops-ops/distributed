use super::*;

/// Dialect gate for comparison operators (JSON ops only on Postgres).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SurfaceDialect {
    Sqlite,
    Postgres,
}

impl SurfaceDialect {
    pub fn is_postgres(self) -> bool {
        matches!(self, Self::Postgres)
    }
}

/// Options for building a surface from a table catalog.
#[derive(Clone, Debug)]
pub struct SurfaceOptions {
    pub dialect: SurfaceDialect,
    pub aggregates: bool,
    pub subscriptions: bool,
    /// Default page size used when a list request omits `limit`.
    pub default_limit: u64,
    /// Absolute page-size ceiling enforced by the query compiler.
    pub max_limit: u64,
}

impl SurfaceOptions {
    pub fn sqlite() -> Self {
        Self {
            dialect: SurfaceDialect::Sqlite,
            aggregates: true,
            subscriptions: true,
            default_limit: 100,
            max_limit: 1000,
        }
    }

    pub fn postgres() -> Self {
        Self {
            dialect: SurfaceDialect::Postgres,
            aggregates: true,
            subscriptions: true,
            default_limit: 100,
            max_limit: 1000,
        }
    }
}

/// Row-authorization semantics retained on the role/application surface.
///
/// `ServerOnly` is the fail-closed representation when a predicate differs
/// across application roles or references a field that is not authorized on
/// the selected surface. Clients may revalidate such collections but must not
/// evaluate membership locally.
#[derive(Clone, Debug, PartialEq)]
pub enum SurfaceRowPolicy {
    Unrestricted,
    Predicate(FilterExpr),
    ServerOnly,
}

/// Semantic category for one GraphQL field argument.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub enum SurfaceArgumentKind {
    Filter,
    Order,
    Limit,
    Offset,
    PrimaryKey,
}

/// One accepted root/relationship argument from the shared Surface IR.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct SurfaceArgument {
    pub name: String,
    pub kind: SurfaceArgumentKind,
    pub type_name: String,
    pub nullable: bool,
    pub list: bool,
}

/// Kind of a GraphQL root field on Query / Subscription.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RootKind {
    List,
    ByPk,
    Aggregate,
}

/// One Query or Subscription root field inventory entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootField {
    pub name: String,
    pub kind: RootKind,
    /// GraphQL object type name (`model_name`).
    pub object: String,
    /// Model name in the catalog (`schema.model_name`).
    pub model_name: String,
    pub arguments: Vec<SurfaceArgument>,
    /// Physical read-model dependencies used only for invalidation planning.
    pub dependencies: Vec<String>,
    pub default_limit: Option<u64>,
    pub max_limit: Option<u64>,
}

/// Column field on an object type (after skips / role filter).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct ColumnField {
    pub name: String,
    pub scalar: String,
    pub nullable: bool,
}

/// Relationship field inventory (target must be on the surface).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelField {
    pub name: String,
    pub target_model: String,
    pub target_object: String,
    pub kind: RelationshipKind,
    pub list: bool,
    /// Nullability of an object relationship. List relationships are always
    /// non-null lists and ignore this flag.
    pub nullable: bool,
    pub arguments: Vec<SurfaceArgument>,
    pub keys: SurfaceRelationshipKeys,
    pub dependencies: Vec<String>,
    pub aggregate: Option<SurfaceRelationshipAggregate>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct SurfaceRelationshipAggregate {
    pub name: String,
    pub type_name: String,
    pub arguments: Vec<SurfaceArgument>,
    pub dependencies: Vec<String>,
}

/// Key/join metadata derived once from `RelationshipDef` while building the
/// Surface. Manifest and compiler consumers never walk the table catalog again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SurfaceRelationshipKeys {
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
    /// Source/target identities are authorized, while the operational join
    /// table remains private. The opaque dependency is sufficient to mark
    /// cached relationship edges stale without exposing join internals.
    ThroughOpaque {
        local: Vec<String>,
        remote: Vec<String>,
        dependency: String,
    },
    /// Relationship remains server-queryable, but its local/client identity
    /// mapping is not authorized on this selected surface.
    Embedded,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SurfaceTypeField {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
    pub list: bool,
    pub item_nullable: bool,
    pub nested: Option<Box<SurfaceTypeDef>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SurfaceTypeDef {
    pub name: String,
    pub fields: Vec<SurfaceTypeField>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SurfaceCommandShape {
    None,
    Typed(SurfaceTypeDef),
}

/// Structural command mutation carried by the same role-filtered Surface.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SurfaceCommand {
    pub command_name: String,
    pub field_name: String,
    pub roles: Vec<String>,
    pub input: SurfaceCommandShape,
    pub output: SurfaceCommandShape,
    pub consistency: CommandConsistency,
    pub(crate) input_defaults: Vec<CommandInputDefault>,
    pub(crate) effects: Option<CommandEffects>,
    pub(crate) confirmations: Vec<CommandProjectionConfirmation>,
    pub(crate) projected_model: Option<CommandProjectedModel>,
    pub(crate) direct_projection: Option<CommandDirectProjectionTarget>,
    pub(crate) projections: CommandProjectionEvents,
    /// Authorization selection erased at least one required confirmation.
    /// No hidden projector/model/key IDs may survive into client artifacts.
    pub(crate) confirmation_unavailable: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::graphql::surface) enum SurfaceProjectionOwnerKind {
    Direct,
    Async,
}

/// Projection topology and complete read-model ownership declaration.
///
/// Construct an asynchronous owner through [`SurfaceProjector`] or a
/// same-transaction-only owner through [`SurfaceDirectProjection`]. Keeping
/// those public builder types separate prevents a direct owner from being
/// registered as an asynchronous service route.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SurfaceProjectionOwner {
    pub name: String,
    pub facts: Vec<String>,
    pub models: Vec<String>,
    pub dependencies: Vec<String>,
    pub(crate) change_epoch: Option<String>,
    pub(crate) partition: ProjectionPartitionSpec,
    pub(in crate::graphql::surface) kind: SurfaceProjectionOwnerKind,
    pub(crate) modeled: Vec<SurfaceModeledProjection>,
}

impl SurfaceProjectionOwner {
    pub fn is_direct(&self) -> bool {
        matches!(self.kind, SurfaceProjectionOwnerKind::Direct)
    }

    pub(crate) fn binding_models(&self) -> Vec<String> {
        if self.modeled.is_empty() {
            return self.models.clone();
        }
        self.modeled
            .iter()
            .flat_map(|modeled| modeled.output_models().iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub(crate) fn binding_facts(&self) -> Vec<String> {
        if self.modeled.is_empty() {
            return self.facts.clone();
        }
        if self.kind == SurfaceProjectionOwnerKind::Direct {
            return Vec::new();
        }
        self.modeled
            .iter()
            .flat_map(SurfaceModeledProjection::event_names)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub(crate) fn binding_change_epoch(&self) -> Option<String> {
        if self.modeled.is_empty() {
            return self.change_epoch.clone();
        }
        self.modeled
            .iter()
            .find(|modeled| {
                modeled.state() == crate::projection::placement::ProjectionBindingState::Active
            })
            .or_else(|| self.modeled.first())
            .map(|modeled| modeled.epoch().as_str().to_owned())
    }

    pub(crate) fn active_modeled_program_id_for(
        &self,
        model: &str,
    ) -> Option<crate::ProjectionProgramId> {
        self.modeled
            .iter()
            .find(|modeled| {
                modeled.state() == crate::projection::placement::ProjectionBindingState::Active
                    && modeled.output_models().iter().any(|output| output == model)
            })
            .map(SurfaceModeledProjection::program_id)
    }
}

/// Asynchronous fact-consuming projection declaration.
///
/// This is the only projection declaration accepted by
/// [`crate::microsvc::Routes::causal_projector`]. Use
/// [`SurfaceDirectProjection`] when `Atomic<T>` owns the row entirely
/// inside the command transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SurfaceProjector {
    owner: SurfaceProjectionOwner,
}

impl Deref for SurfaceProjector {
    type Target = SurfaceProjectionOwner;

    fn deref(&self) -> &Self::Target {
        &self.owner
    }
}

impl SurfaceProjector {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            owner: SurfaceProjectionOwner {
                name: name.into(),
                facts: Vec::new(),
                models: Vec::new(),
                dependencies: Vec::new(),
                change_epoch: None,
                partition: ProjectionPartitionSpec::unit(),
                kind: SurfaceProjectionOwnerKind::Async,
                modeled: Vec::new(),
            },
        }
    }

    pub fn facts(mut self, facts: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.owner.facts = facts.into_iter().map(Into::into).collect();
        self
    }

    pub fn models(mut self, models: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.owner.models = models.into_iter().map(Into::into).collect();
        self
    }

    /// Attach one exact program/binding/activation tuple.
    ///
    /// Repeat this for retained draining bindings. Exactly one active binding
    /// per program is accepted when the owner registry is attached.
    pub fn modeled(mut self, projection: SurfaceModeledProjection) -> Self {
        self.owner.modeled.push(projection);
        self
    }

    /// Register the opaque change-log epoch owned by this projector topology.
    ///
    /// Epoch contents have no ordering meaning. They fence live resume and
    /// same-transaction record-change evidence across projector rebuilds.
    pub fn change_epoch(mut self, epoch: impl Into<String>) -> Self {
        self.owner.change_epoch = Some(epoch.into());
        self
    }

    /// Derive a stable projection partition from one raw event JSON path.
    ///
    /// This closed declaration is evaluated before typed event decoding and is
    /// hashed into the durable topology. Reuse this exact projector value for
    /// GraphQL/direct binding and the asynchronous runtime.
    pub fn partition_by(mut self, path: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.owner.partition = ProjectionPartitionSpec::input_path(path);
        self
    }

    /// Use one deterministic constant partition (including explicit JSON null).
    pub fn partition_constant(mut self, value: serde_json::Value) -> Self {
        self.owner.partition = ProjectionPartitionSpec::constant(value);
        self
    }

    /// Compiler seam for binding one `Atomic<M>` command to this exact
    /// registered topology. Ordinary handlers never receive or construct it.
    #[doc(hidden)]
    pub fn __distributed_direct_projection<I, M>(&self) -> CompiledDirectProjectionTarget<I, M>
    where
        M: crate::read_model::RelationalReadModel + 'static,
    {
        compiled_direct_projection_target(
            &self.owner.name,
            &self.owner.facts,
            &self.owner.models,
            &self.owner.partition,
            self.owner.change_epoch.as_deref(),
        )
    }
}

impl From<SurfaceProjector> for SurfaceProjectionOwner {
    fn from(projector: SurfaceProjector) -> Self {
        projector.owner
    }
}

/// Same-transaction-only projection owner for `Atomic<T>` commands.
///
/// It intentionally has no fact inventory and cannot be passed to an
/// asynchronous projector route. The owner still supplies the complete model
/// topology, partition codec, and change epoch used by direct commits, query
/// evidence, live changes, and generated clients.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SurfaceDirectProjection {
    owner: SurfaceProjectionOwner,
}

impl SurfaceDirectProjection {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            owner: SurfaceProjectionOwner {
                name: name.into(),
                facts: Vec::new(),
                models: Vec::new(),
                dependencies: Vec::new(),
                change_epoch: None,
                partition: ProjectionPartitionSpec::unit(),
                kind: SurfaceProjectionOwnerKind::Direct,
                modeled: Vec::new(),
            },
        }
    }

    pub fn models(mut self, models: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.owner.models = models.into_iter().map(Into::into).collect();
        self
    }

    /// Add one compile-checked relational model to this owner's inventory.
    pub fn model<M>(mut self) -> Self
    where
        M: crate::read_model::RelationalReadModel,
    {
        self.owner.models.push(M::schema().model_name.clone());
        self
    }

    /// Attach one exact same-transaction modeled projection.
    pub fn modeled(mut self, projection: SurfaceModeledProjection) -> Self {
        self.owner.modeled.push(projection);
        self
    }

    /// Register the opaque change-log epoch for direct record evidence.
    pub fn change_epoch(mut self, epoch: impl Into<String>) -> Self {
        self.owner.change_epoch = Some(epoch.into());
        self
    }

    /// Derive a stable direct-projection partition from a command input path.
    pub fn partition_by(mut self, path: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.owner.partition = ProjectionPartitionSpec::input_path(path);
        self
    }

    /// Use one deterministic constant partition (including explicit JSON null).
    pub fn partition_constant(mut self, value: serde_json::Value) -> Self {
        self.owner.partition = ProjectionPartitionSpec::constant(value);
        self
    }
}

impl From<SurfaceDirectProjection> for SurfaceProjectionOwner {
    fn from(projection: SurfaceDirectProjection) -> Self {
        projection.owner
    }
}

/// One exposed read-model on the surface.
#[derive(Clone)]
pub struct SurfaceModel {
    pub model_name: String,
    pub table_name: String,
    pub object_name: String,
    pub columns: Vec<ColumnField>,
    pub relationships: Vec<RelField>,
    pub primary_key: Vec<String>,
    pub row_policy: SurfaceRowPolicy,
    pub role_limit: Option<u64>,
    pub aggregations: bool,
    /// Filtered schema clone (columns limited for role surfaces).
    pub(crate) schema: TableSchema,
}

/// Whether this selected Surface exposes a complete client-safe normalized
/// identity for the model. Both manifest normalization and keyed optimistic
/// effects use this single predicate so an embedded model can never receive an
/// operation that assumes a stable normalized cache key.
pub(crate) fn model_has_client_normalized_identity(model: &SurfaceModel) -> bool {
    !model.primary_key.is_empty()
        && model.primary_key.iter().all(|key| {
            model
                .columns
                .iter()
                .find(|column| column.name == *key)
                .is_some_and(|column| {
                    !column.nullable
                        && column.scalar != "BigInt"
                        && matches!(
                            column.scalar.as_str(),
                            "Boolean"
                                | "Bytea"
                                | "Float"
                                | "ID"
                                | "Int"
                                | "JSON"
                                | "String"
                                | "Timestamptz"
                        )
                })
        })
}

/// Intermediate surface IR.
#[derive(Clone)]
pub struct Surface {
    pub(crate) selection: SurfaceSelection,
    // Structural fields stay crate-private so an authorized Surface cannot be
    // mutated after selection and then exported under its original role/app
    // provenance. Public consumers inspect derived artifacts or the read-only
    // helpers below; only the selection/compiler pipeline may change the IR.
    pub(crate) dialect: SurfaceDialect,
    pub(crate) aggregates: bool,
    pub(crate) subscriptions: bool,
    pub(crate) default_limit: u64,
    pub(crate) max_limit: u64,
    /// Complete validated table catalog, including operational relationship
    /// targets. It stays private and is carried through selection solely for
    /// shared policy/topology validation; manifests never serialize it.
    pub(crate) catalog: BTreeMap<String, TableSchema>,
    /// Keyed by `model_name`.
    pub(crate) models: BTreeMap<String, SurfaceModel>,
    pub(crate) query_fields: Vec<RootField>,
    pub(crate) subscription_fields: Vec<RootField>,
    /// GraphQL comparison input name → operator field names (from `naming` only).
    pub(crate) comparison_ops: BTreeMap<String, Vec<String>>,
    pub(crate) commands: Vec<SurfaceCommand>,
    /// Distinguishes an explicitly attached empty registry from a Surface that
    /// has not selected its one authoritative command source yet.
    pub(crate) commands_attached: bool,
    pub(crate) projectors: Vec<SurfaceProjectionOwner>,
    pub(crate) projectors_attached: bool,
    /// Non-serializable provenance proving typed commands came from one
    /// executable Service inventory rather than a lookalike command list.
    pub(crate) service_binding: Option<crate::command::TypedServiceCommandBinding>,
}

/// Debug output is intentionally limited to already-authorized public IDs.
/// The private catalog and filtered schema clones may contain denied names and
/// must never become an authorization side channel through derived formatting.
impl std::fmt::Debug for Surface {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Surface")
            .field("selection", &self.selection)
            .field("dialect", &self.dialect)
            .field("models", &self.models.keys().collect::<Vec<_>>())
            .field("query_roots", &self.query_root_names())
            .field("commands", &self.commands)
            .field("projectors", &self.projectors)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SurfaceSelection {
    Catalog,
    Role {
        name: String,
    },
    Application {
        name: String,
        eligible_roles: Vec<String>,
        schema_roles: Vec<String>,
    },
}

impl Surface {
    /// Serialize the complete behavior-affecting Surface IR once. Application
    /// manifests, SDL metadata, and client exports use this snapshot as their
    /// shared identity input; none of them re-walks a table catalog.
    pub(crate) fn canonical_contract_value(&self) -> Result<serde_json::Value, String> {
        let selection = match &self.selection {
            SurfaceSelection::Catalog => serde_json::json!({"kind": "catalog"}),
            SurfaceSelection::Role { name } => serde_json::json!({"kind": "role", "name": name}),
            SurfaceSelection::Application {
                name,
                eligible_roles,
                schema_roles,
            } => {
                let mut eligible_roles = eligible_roles.clone();
                let mut schema_roles = schema_roles.clone();
                eligible_roles.sort();
                eligible_roles.dedup();
                schema_roles.sort();
                schema_roles.dedup();
                serde_json::json!({
                    "kind": "application",
                    "name": name,
                    "eligible_roles": eligible_roles,
                    "schema_roles": schema_roles,
                })
            }
        };
        let models = self
            .models
            .values()
            .map(|model| {
                let mut columns = model.columns.clone();
                columns.sort_by(|left, right| left.name.cmp(&right.name));
                let mut primary_key = model.primary_key.clone();
                primary_key.sort();
                let mut relationships = model
                    .relationships
                    .iter()
                    .map(|relationship| {
                        serde_json::json!({
                            "name": relationship.name,
                            "target_model": relationship.target_model,
                            "target_object": relationship.target_object,
                            "kind": format!("{:?}", relationship.kind).to_ascii_lowercase(),
                            "list": relationship.list,
                            "nullable": relationship.nullable,
                            "arguments": relationship
                                .arguments
                                .iter()
                                .map(argument_value)
                                .collect::<Vec<_>>(),
                            "keys": relationship_keys_value(&relationship.keys),
                            "dependencies": relationship.dependencies,
                            "aggregate": relationship.aggregate.as_ref().map(|aggregate| {
                                serde_json::json!({
                                    "name": aggregate.name,
                                    "type_name": aggregate.type_name,
                                    "arguments": aggregate
                                        .arguments
                                        .iter()
                                        .map(argument_value)
                                        .collect::<Vec<_>>(),
                                    "dependencies": aggregate.dependencies,
                                })
                            }),
                        })
                    })
                    .collect::<Vec<_>>();
                relationships
                    .sort_by(|left, right| left["name"].as_str().cmp(&right["name"].as_str()));
                serde_json::json!({
                    "model_name": model.model_name,
                    "table_name": model.table_name,
                    "object_name": model.object_name,
                    "columns": columns,
                    "relationships": relationships,
                    "primary_key": primary_key,
                    "row_policy": row_policy_value(&model.row_policy),
                    "role_limit": model.role_limit,
                    "aggregations": model.aggregations,
                })
            })
            .collect::<Vec<_>>();
        let mut roots = self
            .query_fields
            .iter()
            .map(|root| root_value("query", root))
            .chain(
                self.subscription_fields
                    .iter()
                    .map(|root| root_value("subscription", root)),
            )
            .collect::<Vec<_>>();
        roots.sort_by(|left, right| {
            (left["operation"].as_str(), left["name"].as_str())
                .cmp(&(right["operation"].as_str(), right["name"].as_str()))
        });
        let mut commands = self
            .commands
            .iter()
            .map(|command| {
                serde_json::json!({
                    "command_name": command.command_name,
                    "field_name": command.field_name,
                    "roles": command.roles,
                    "input": command_shape_value(&command.input),
                    "output": command_shape_value(&command.output),
                    "consistency": command.consistency,
                    "input_defaults": command.input_defaults,
                    "effects": command.effects,
                    "confirmations": command.confirmations,
                    "projected_model": command.projected_model.as_ref().map(|model| model.model.clone()),
                    "direct_projection": command.direct_projection.as_ref().map(|target| target.canonical_value()),
                    "projections": command.projections,
                    "confirmation_unavailable": command.confirmation_unavailable,
                })
            })
            .collect::<Vec<_>>();
        commands.sort_by(|left, right| {
            left["command_name"]
                .as_str()
                .cmp(&right["command_name"].as_str())
        });
        let mut projectors = self
            .projectors
            .iter()
            .map(|owner| {
                let modeled = owner
                    .modeled
                    .iter()
                    .map(|modeled| modeled.canonical_contract_value())
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<_, String>(serde_json::json!({
                    "name": owner.name,
                    "facts": owner.facts,
                    "models": owner.models,
                    "dependencies": owner.dependencies,
                    "change_epoch": owner.change_epoch,
                    "partition": owner.partition,
                    "kind": if owner.is_direct() { "direct" } else { "async" },
                    "modeled": modeled,
                }))
            })
            .collect::<Result<Vec<_>, _>>()?;
        projectors.sort_by(|left, right| left["name"].as_str().cmp(&right["name"].as_str()));
        let value = serde_json::json!({
            "version": 1,
            "selection": selection,
            "dialect": format!("{:?}", self.dialect).to_ascii_lowercase(),
            "aggregates": self.aggregates,
            "subscriptions": self.subscriptions,
            "default_limit": self.default_limit,
            "max_limit": self.max_limit,
            "models": models,
            "roots": roots,
            "comparison_ops": self.comparison_ops,
            "commands": commands,
            "commands_attached": self.commands_attached,
            "projectors": projectors,
            "projectors_attached": self.projectors_attached,
        });
        Ok(crate::application::canonical_json(&value))
    }

    /// Inventory of query root field names (sorted).
    pub fn query_root_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.query_fields.iter().map(|f| f.name.as_str()).collect();
        names.sort();
        names
    }

    /// Comparison operator fields for a scalar, empty if scalar unused.
    pub fn comparison_ops_for_scalar(&self, scalar: &str) -> Vec<&str> {
        let name = comparison_exp_name(scalar);
        self.comparison_ops
            .get(&name)
            .map(|ops| ops.iter().map(String::as_str).collect())
            .unwrap_or_default()
    }

    pub fn commands(&self) -> &[SurfaceCommand] {
        &self.commands
    }

    pub fn projection_owners(&self) -> &[SurfaceProjectionOwner] {
        &self.projectors
    }

    /// Backward-compatible name for the complete projection-owner registry.
    ///
    /// Direct-only owners are included even though they are not asynchronous
    /// projectors.
    pub fn projectors(&self) -> &[SurfaceProjectionOwner] {
        self.projection_owners()
    }

    /// Attach the crate-private typed command inventory to this unselected
    /// catalog surface. The inventory must be bound before authorization
    /// selection so role/application filtering operates on the declaration-owned
    /// typed command contracts.
    pub(crate) fn with_typed_commands(
        mut self,
        commands: &crate::graphql::commands::TypedCommandInventory,
    ) -> Result<Self, String> {
        if !matches!(self.selection, SurfaceSelection::Catalog) {
            return Err(
                "commands can only be attached to the unselected catalog Surface before authorization selection"
                    .into(),
            );
        }
        if self.service_binding.is_some() {
            return Err(
                "commands are frozen after attachment from the executable Service inventory".into(),
            );
        }
        if self.commands_attached {
            return Err("a command registry has already been attached to this Surface".into());
        }
        self.commands = commands.surface_commands();
        validate_and_canonicalize_commands(&self.models, &self.comparison_ops, &mut self.commands)?;
        if self.projectors_attached {
            bind_surface_direct_projection_targets(
                &mut self.commands,
                &self.projectors,
                &self.models,
            )?;
            validate_command_confirmation_topology(&self.commands, &self.projectors, &self.models)?;
        }
        self.commands_attached = true;
        Ok(self)
    }

    /// Bind one explicit logical module's retained typed command contracts to
    /// this unselected catalog Surface without constructing executable runtime
    /// state. The module definitions are the authoritative source; public
    /// command JSON is never used to reconstruct typed shapes or effects.
    pub fn with_module(self, module: &crate::application::Module) -> Result<Self, String> {
        self.with_modules(std::iter::once(module))
    }

    /// Bind several explicit logical modules before role/application
    /// authorization selection.
    pub fn with_modules<'a, I>(mut self, modules: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = &'a crate::application::Module>,
    {
        if !matches!(self.selection, SurfaceSelection::Catalog) {
            return Err(
                "module commands can only be attached to the unselected catalog Surface before authorization selection"
                    .into(),
            );
        }
        if self.commands_attached {
            return Err("a command registry has already been attached to this Surface".into());
        }
        let mut contracts = Vec::new();
        for module in modules {
            contracts.extend(module.typed_command_contracts()?);
        }
        let inventory =
            crate::graphql::commands::TypedCommandInventory::from_contracts(&contracts)?;
        self = self.with_typed_commands(&inventory)?;
        Ok(self)
    }

    /// Pool-free authoritative typed command path. The executable Routes
    /// inventory supplies both GraphQL declarations and non-forgeable service
    /// provenance used by static client export.
    pub fn with_service(mut self, service: &crate::microsvc::Service) -> Result<Self, String> {
        if !matches!(self.selection, SurfaceSelection::Catalog) {
            return Err(
                "service commands can only be attached to the unselected catalog Surface before authorization selection"
                    .into(),
            );
        }
        if self.commands_attached {
            return Err(
                "service commands cannot replace an already attached command inventory".into(),
            );
        }
        let binding = service.typed_command_binding()?;
        let contracts = service.typed_command_contracts();
        let commands = crate::graphql::commands::TypedCommandInventory::from_contracts(&contracts)?;
        self = self.with_typed_commands(&commands)?;
        self.service_binding = Some(binding);
        Ok(self)
    }

    #[cfg(any(test, feature = "graphql"))]
    pub(crate) fn with_service_binding(
        mut self,
        binding: Option<crate::command::TypedServiceCommandBinding>,
    ) -> Self {
        self.service_binding = binding;
        self
    }

    /// Attach and validate projector topology against the already-built model
    /// graph, deriving physical dependencies exactly once.
    pub fn with_projectors(
        self,
        projectors: impl IntoIterator<Item = SurfaceProjector>,
    ) -> Result<Self, String> {
        self.with_projection_owners(projectors.into_iter().map(Into::into))
    }

    /// Attach and validate a mixed registry of asynchronous projectors and
    /// same-transaction-only projection owners.
    pub fn with_projection_owners(
        mut self,
        projectors: impl IntoIterator<Item = SurfaceProjectionOwner>,
    ) -> Result<Self, String> {
        if !matches!(self.selection, SurfaceSelection::Catalog) {
            return Err(
                "projection owners can only be attached to the unselected catalog Surface before authorization selection"
                    .into(),
            );
        }
        let mut out = Vec::new();
        let mut names = BTreeSet::new();
        let mut active_programs = BTreeSet::new();
        let mut active_models = BTreeMap::new();
        let mut modeled_registrations = BTreeSet::new();
        let projectors = projectors.into_iter().collect::<Vec<_>>();
        validate_direct_modeled_owner_compatibility(&projectors)?;
        for mut projector in projectors {
            if projector.name.trim().is_empty() {
                return Err("projector name must not be empty".into());
            }
            if !names.insert(projector.name.clone()) {
                return Err(format!("duplicate projector name `{}`", projector.name));
            }
            if !projector.modeled.is_empty() {
                if !projector.facts.is_empty() || !projector.models.is_empty() {
                    return Err(format!(
                        "modeled projection owner `{}` must derive event and model inventory from its exact bindings",
                        projector.name
                    ));
                }
                for modeled in &projector.modeled {
                    modeled.validate_for_surface(&projector.name, projector.kind, &self.models)?;
                    if !modeled_registrations.insert((
                        modeled.binding_id(),
                        modeled.epoch().as_str().to_owned(),
                        modeled.state(),
                    )) {
                        return Err(format!(
                            "projection binding `{}` is registered more than once on the Surface",
                            modeled.binding_id()
                        ));
                    }
                    if modeled.state()
                        == crate::projection::placement::ProjectionBindingState::Active
                    {
                        if !active_programs.insert(modeled.program_id()) {
                            return Err(format!(
                                "projection program `{}` has more than one active Surface binding",
                                modeled.program_id()
                            ));
                        }
                        for model in modeled.output_models() {
                            if let Some(previous) =
                                active_models.insert(model.clone(), modeled.program_id())
                            {
                                return Err(format!(
                                    "active projection programs `{previous}` and `{}` both own model `{model}`",
                                    modeled.program_id()
                                ));
                            }
                        }
                    }
                }
                projector.models = projector.binding_models();
                projector.facts = projector.binding_facts();
                projector.change_epoch = projector.binding_change_epoch();
                projector.partition = modeled_owner_partition_contract(&projector)?;
            }
            match projector.kind {
                SurfaceProjectionOwnerKind::Async if projector.facts.is_empty() => {
                    return Err(format!(
                        "projector `{}` must declare at least one fact",
                        projector.name
                    ));
                }
                SurfaceProjectionOwnerKind::Direct if !projector.facts.is_empty() => {
                    return Err(format!(
                        "direct projection owner `{}` cannot declare asynchronous facts",
                        projector.name
                    ));
                }
                SurfaceProjectionOwnerKind::Direct | SurfaceProjectionOwnerKind::Async => {}
            }
            validate_nonempty_unique_ids(
                &projector.facts,
                &format!("projector `{}` fact", projector.name),
            )?;
            if projector.models.is_empty() {
                return Err(format!(
                    "projector `{}` must declare at least one model",
                    projector.name
                ));
            }
            validate_nonempty_unique_ids(
                &projector.models,
                &format!("projector `{}` model", projector.name),
            )?;
            if let Some(epoch) = projector.change_epoch.as_deref() {
                crate::projection_protocol::ProjectionEpoch::new(epoch).map_err(|error| {
                    format!(
                        "projector `{}` change-log epoch is invalid: {error}",
                        projector.name
                    )
                })?;
            }
            projector.partition.validate().map_err(|error| {
                format!(
                    "projector `{}` has invalid partition declaration: {error}",
                    projector.name
                )
            })?;
            projector.facts.sort();
            projector.models.sort();
            let mut dependencies = BTreeSet::new();
            for model in &projector.models {
                let Some(surface_model) = self.models.get(model) else {
                    return Err(format!(
                        "projector `{}` targets unknown surface model `{model}`",
                        projector.name
                    ));
                };
                dependencies.insert(surface_model.table_name.clone());
            }
            projector.dependencies = dependencies.into_iter().collect();
            out.push(projector);
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        bind_surface_direct_projection_targets(&mut self.commands, &out, &self.models)?;
        self.projectors = out;
        self.projectors_attached = true;
        validate_command_confirmation_topology(&self.commands, &self.projectors, &self.models)?;
        Ok(self)
    }
}

fn root_value(operation: &str, root: &RootField) -> serde_json::Value {
    serde_json::json!({
        "operation": operation,
        "name": root.name,
        "kind": match root.kind {
            RootKind::List => "list",
            RootKind::ByPk => "by_pk",
            RootKind::Aggregate => "aggregate",
        },
        "object": root.object,
        "model_name": root.model_name,
        "arguments": root.arguments.iter().map(argument_value).collect::<Vec<_>>(),
        "dependencies": root.dependencies,
        "default_limit": root.default_limit,
        "max_limit": root.max_limit,
    })
}

fn argument_value(argument: &SurfaceArgument) -> serde_json::Value {
    serde_json::json!({
        "name": argument.name,
        "kind": argument_kind_name(argument.kind),
        "type_name": argument.type_name,
        "nullable": argument.nullable,
        "list": argument.list,
    })
}

fn argument_kind_name(kind: SurfaceArgumentKind) -> &'static str {
    match kind {
        SurfaceArgumentKind::Filter => "filter",
        SurfaceArgumentKind::Order => "order",
        SurfaceArgumentKind::Limit => "limit",
        SurfaceArgumentKind::Offset => "offset",
        SurfaceArgumentKind::PrimaryKey => "primary_key",
    }
}

fn command_shape_value(shape: &SurfaceCommandShape) -> serde_json::Value {
    match shape {
        SurfaceCommandShape::None => serde_json::Value::Null,
        SurfaceCommandShape::Typed(definition) => type_def_value(definition),
    }
}

fn type_def_value(definition: &SurfaceTypeDef) -> serde_json::Value {
    serde_json::json!({
        "name": definition.name,
        "fields": definition.fields.iter().map(|field| serde_json::json!({
            "name": field.name,
            "type_name": field.type_name,
            "nullable": field.nullable,
            "list": field.list,
            "item_nullable": field.item_nullable,
            "nested": field.nested.as_deref().map(type_def_value),
        })).collect::<Vec<_>>(),
    })
}

fn row_policy_value(policy: &SurfaceRowPolicy) -> serde_json::Value {
    match policy {
        SurfaceRowPolicy::Unrestricted => serde_json::json!({"kind": "unrestricted"}),
        SurfaceRowPolicy::Predicate(predicate) => {
            serde_json::json!({"kind": "predicate", "expression": predicate})
        }
        SurfaceRowPolicy::ServerOnly => serde_json::json!({"kind": "server_only"}),
    }
}

fn relationship_keys_value(keys: &SurfaceRelationshipKeys) -> serde_json::Value {
    match keys {
        SurfaceRelationshipKeys::Direct { local, remote } => {
            serde_json::json!({"kind": "direct", "local": local, "remote": remote})
        }
        SurfaceRelationshipKeys::Through {
            local,
            remote,
            table,
            source_foreign_key,
            target_foreign_key,
        } => serde_json::json!({
            "kind": "through",
            "local": local,
            "remote": remote,
            "table": table,
            "source_foreign_key": source_foreign_key,
            "target_foreign_key": target_foreign_key,
        }),
        SurfaceRelationshipKeys::ThroughOpaque {
            local,
            remote,
            dependency,
        } => serde_json::json!({
            "kind": "through_opaque",
            "local": local,
            "remote": remote,
            "dependency": dependency,
        }),
        SurfaceRelationshipKeys::Embedded => serde_json::json!({"kind": "embedded"}),
    }
}
