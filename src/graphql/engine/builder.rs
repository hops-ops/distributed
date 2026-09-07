use super::*;

impl GraphqlEngineBuilder {
    pub(crate) fn new(source: GraphqlPoolSource) -> Self {
        Self {
            service_id: None,
            protocol_token_key: None,
            protocol_namespace: None,
            client_applications: BTreeMap::new(),
            command_binding: None,
            causal_storage_identity: source.causal_storage_identity,
            pool: source.pool,
            #[cfg(feature = "gateway-delivery")]
            read_routing: None,
            #[cfg(feature = "gateway-delivery")]
            gateway_versions: None,
            catalog: BTreeMap::new(),
            by_table: BTreeMap::new(),
            permissions: BTreeMap::new(),
            roles: None,
            anonymous_role: "anonymous".into(),
            default_limit: 100,
            max_limit: 1000,
            max_depth: crate::graphql::complexity::DEFAULT_MAX_DEPTH,
            max_complexity: crate::graphql::complexity::DEFAULT_MAX_COMPLEXITY,
            max_in_list: 1000,
            max_bool_width: 256,
            // Fail-closed by default for unshipped GA: unknown/ungranted filter
            // and order keys must not silently no-op.
            strict_where: true,
            introspection_for_anonymous: true,
            statement_timeout: Duration::from_secs(5),
            graphiql: false,
            subscriptions: true,
            typed_commands: TypedCommandInventory::empty(),
            projectors: Vec::new(),
            change_rx: None,
            pending_errors: Vec::new(),
            // DevHeaders keeps ambient header tests/green; public scaffolds set OidcBearer (D6).
            identity: IdentityConfig::dev_headers(),
            read_stores: BTreeMap::new(),
        }
    }

    /// Include live roots in the runtime schema and generated client surface.
    /// Query-only gateway executors disable this before schema construction.
    pub fn subscriptions(mut self, enabled: bool) -> Self {
        self.subscriptions = enabled;
        self
    }

    /// Opt in selected exact query documents to a read replica. The engine's
    /// original repository remains the primary for current reads and evidence.
    #[cfg(feature = "gateway-delivery")]
    pub fn read_routing(mut self, routing: ReadRouting) -> Self {
        if std::mem::discriminant(&self.pool) != std::mem::discriminant(&routing.replica) {
            self.pending_errors
                .push("read replica must use the primary SQL dialect".into());
        }
        self.read_routing = Some(routing);
        self
    }

    /// Enable origin dependency validation after installing version coverage
    /// in the engine's database. This allocates no gateway cache or coordinator.
    #[cfg(feature = "gateway-delivery")]
    pub fn gateway_versions(
        mut self,
        store: crate::graphql::delivery::GatewayVersionStore,
    ) -> Self {
        self.gateway_versions = Some(store);
        self
    }

    pub fn model<M: RelationalReadModelIncludes>(mut self, perms: ModelPermissions<M>) -> Self {
        let schema = M::schema().clone();
        if let Err(e) = self.insert_catalog(schema.clone(), true) {
            self.pending_errors.push(e.0);
            return self;
        }
        // Shadow-register one-hop relationship targets.
        for rel in &schema.relationships {
            match M::include_target_schema(&rel.field_name) {
                Ok(target) => {
                    if let Err(e) = self.insert_catalog(target.clone(), false) {
                        // Shadow insert conflict only if different schema.
                        if !e.0.contains("identical") {
                            self.pending_errors.push(e.0);
                        }
                    }
                }
                Err(err) => {
                    self.pending_errors.push(format!(
                        "include_target_schema for `{}`.`{}`: {err}",
                        schema.model_name, rel.field_name
                    ));
                }
            }
        }
        for (role, perm) in perms.entries {
            let key = (schema.model_name.clone(), role.clone());
            match self.permissions.entry(key) {
                Entry::Vacant(entry) => {
                    entry.insert(RoleModelPerm { permission: perm });
                }
                Entry::Occupied(_) => {
                    self.pending_errors.push(format!(
                        "duplicate permission for model `{}` role `{role}`",
                        schema.model_name
                    ));
                }
            }
        }
        self
    }

    /// Expose a model with deployment-composed query relationships.
    ///
    /// The supplied schema must retain `M`'s exact physical storage contract;
    /// only relationship metadata may differ. This lets independently owned
    /// model crates form a GraphQL relationship graph without introducing
    /// circular Rust dependencies or a second projection model.
    pub fn model_schema<M: crate::RelationalReadModel>(
        mut self,
        schema: TableSchema,
        perms: ModelPermissions<M>,
    ) -> Self {
        if !schema.has_same_storage_contract(M::schema()) {
            self.pending_errors.push(format!(
                "deployment schema for model `{}` changes its canonical storage contract",
                M::schema().model_name
            ));
            return self;
        }
        if let Err(error) = self.insert_catalog(schema.clone(), true) {
            self.pending_errors.push(error.0);
            return self;
        }
        for (role, permission) in perms.entries {
            let key = (schema.model_name.clone(), role.clone());
            match self.permissions.entry(key) {
                Entry::Vacant(entry) => {
                    entry.insert(RoleModelPerm { permission });
                }
                Entry::Occupied(_) => {
                    self.pending_errors.push(format!(
                        "duplicate permission for model `{}` role `{role}`",
                        schema.model_name
                    ));
                }
            }
        }
        self
    }

    pub fn table_schema(mut self, schema: TableSchema) -> Self {
        if let Err(e) = self.insert_catalog(schema, false) {
            self.pending_errors.push(e.0);
        }
        self
    }

    pub(crate) fn register_schema_exposed(
        mut self,
        schema: TableSchema,
    ) -> Result<Self, GraphqlBuildError> {
        self.insert_catalog(schema, true)?;
        Ok(self)
    }

    fn insert_catalog(
        &mut self,
        schema: TableSchema,
        exposed: bool,
    ) -> Result<(), GraphqlBuildError> {
        schema
            .validate()
            .map_err(|e| GraphqlBuildError(e.to_string()))?;
        if let Some(existing) = self.by_table.get(&schema.table_name) {
            if existing != &schema.model_name {
                return Err(GraphqlBuildError(format!(
                    "duplicate table_name `{}` for models `{existing}` and `{}`",
                    schema.table_name, schema.model_name
                )));
            }
        }
        match self.catalog.get(&schema.model_name) {
            Some(entry) if entry.schema != schema => {
                return Err(GraphqlBuildError(format!(
                    "conflicting schemas for model `{}`",
                    schema.model_name
                )));
            }
            Some(entry) => {
                // Identical: upgrade shadow → exposed if needed.
                if exposed && !entry.exposed {
                    let mut upgraded = entry.clone();
                    upgraded.exposed = true;
                    self.catalog.insert(schema.model_name.clone(), upgraded);
                }
                return Ok(());
            }
            None => {}
        }
        self.by_table
            .insert(schema.table_name.clone(), schema.model_name.clone());
        self.catalog
            .insert(schema.model_name.clone(), CatalogEntry { schema, exposed });
        Ok(())
    }

    pub fn roles(mut self, roles: &[&str]) -> Self {
        self.roles = Some(roles.iter().map(|r| (*r).to_string()).collect());
        self
    }

    pub fn grant_all(mut self, role: &str) -> Self {
        let exposed: Vec<String> = self
            .catalog
            .iter()
            .filter(|(_, e)| e.exposed)
            .map(|(n, _)| n.clone())
            .collect();
        for model in exposed {
            let key = (model.clone(), role.to_string());
            match self.permissions.entry(key) {
                Entry::Vacant(entry) => {
                    entry.insert(RoleModelPerm {
                        permission: read().all_columns().aggregations(),
                    });
                }
                Entry::Occupied(_) => {
                    self.pending_errors.push(format!(
                        "duplicate permission for model `{model}` role `{role}`"
                    ));
                }
            }
        }
        self
    }

    pub fn permission<M: RelationalReadModelIncludes>(
        mut self,
        role: &str,
        p: ReadPermission,
    ) -> Self {
        let model = M::schema().model_name.clone();
        if !self.catalog.contains_key(&model) {
            self.pending_errors.push(format!(
                "permission for unregistered model `{model}` (role `{role}`)"
            ));
            return self;
        }
        let key = (model.clone(), role.to_string());
        match self.permissions.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(RoleModelPerm { permission: p });
            }
            Entry::Occupied(_) => {
                self.pending_errors.push(format!(
                    "duplicate permission for model `{model}` role `{role}`"
                ));
            }
        }
        self
    }

    pub fn anonymous_role(mut self, name: &str) -> Self {
        self.anonymous_role = name.to_string();
        self
    }
    /// Set the stable service identity used by generated client manifests.
    ///
    /// [`GraphqlEngine::from_schema_catalog`] supplies this automatically from the
    /// read-model schema catalog. This setter is intended for manually assembled
    /// engines, which otherwise cannot export a client manifest safely.
    pub fn service_id(mut self, service_id: impl Into<String>) -> Self {
        let service_id = service_id.into();
        if service_id.trim().is_empty() {
            self.pending_errors
                .push("GraphQL client service ID must not be empty".into());
            return self;
        }
        if let Some(binding) = &self.command_binding {
            if binding.service_id != service_id {
                self.pending_errors.push(format!(
                    "GraphQL service ID `{service_id}` does not match bound executable service ID `{}`",
                    binding.service_id
                ));
                return self;
            }
        }
        if let Some(existing) = self.service_id.as_deref() {
            if existing != service_id {
                self.pending_errors.push(format!(
                    "GraphQL service ID was already configured as `{existing}` and cannot be changed to `{service_id}`"
                ));
                return self;
            }
        }
        self.service_id = Some(service_id);
        self
    }

    /// Configure the stable deployment key used for opaque GraphQL protocol
    /// tokens.
    ///
    /// All replicas serving the same endpoint namespace must receive the same
    /// key, and rotations intentionally create new cache/projection scopes.
    /// The key is never exposed to resolvers or serialized to clients.
    pub fn protocol_token_key(mut self, key: [u8; 32]) -> Self {
        if key.iter().all(|byte| *byte == 0) {
            self.pending_errors
                .push("GraphQL protocol token key must not be all zero".into());
            return self;
        }
        self.protocol_token_key = Some(key);
        self
    }

    /// Optionally isolate protocol tokens for one stable public endpoint.
    ///
    /// When omitted, the engine's service ID is the namespace. This is useful
    /// when the same service exposes independently deployed GraphQL surfaces.
    pub fn protocol_namespace(mut self, namespace: impl Into<String>) -> Self {
        let namespace = namespace.into();
        if namespace.trim().is_empty() {
            self.pending_errors
                .push("GraphQL protocol namespace must not be empty".into());
            return self;
        }
        if namespace.len() > 255 || namespace.chars().any(char::is_control) {
            self.pending_errors.push(
                "GraphQL protocol namespace must be at most 255 bytes and contain no control characters"
                    .into(),
            );
            return self;
        }
        self.protocol_namespace = Some(namespace);
        self
    }

    /// Register one exact named application surface for generated clients.
    /// Both role sets are required: eligible principals may open the surface,
    /// while schema roles determine its portable privilege intersection.
    ///
    /// The server still authorizes every request as its verified concrete role.
    pub fn client_application_surface(
        self,
        application: impl Into<String>,
        eligible_roles: impl IntoIterator<Item = impl Into<String>>,
        schema_roles: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.client_application_surface_with_schema_roles(application, eligible_roles, schema_roles)
    }

    /// Register an application surface with distinct eligible and schema roles.
    ///
    /// - `eligible_roles`: wire/protocol role list; multi-role principals may
    ///   open the surface when any asserted role is in this set.
    /// - `schema_roles`: grant intersection for the client schema (must be a
    ///   non-empty subset of `eligible_roles`). Use e.g. eligible
    ///   `{admin,user}` + schema `{user}` so portable owner policies survive
    ///   without collapsing model permission definitions.
    pub fn client_application_surface_with_schema_roles(
        mut self,
        application: impl Into<String>,
        eligible_roles: impl IntoIterator<Item = impl Into<String>>,
        schema_roles: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let application = application.into();
        let mut eligible_roles = eligible_roles
            .into_iter()
            .map(Into::into)
            .collect::<Vec<String>>();
        let mut schema_roles = schema_roles
            .into_iter()
            .map(Into::into)
            .collect::<Vec<String>>();
        let eligible_roles_were_unique = {
            let mut sorted = eligible_roles.clone();
            sorted.sort();
            sorted.windows(2).all(|roles| roles[0] != roles[1])
        };
        let schema_roles_were_unique = {
            let mut sorted = schema_roles.clone();
            sorted.sort();
            sorted.windows(2).all(|roles| roles[0] != roles[1])
        };
        eligible_roles.sort();
        schema_roles.sort();
        if application.is_empty()
            || application.len() > 128
            || application.trim() != application
            || application.chars().any(char::is_control)
        {
            self.pending_errors.push(
                "GraphQL client application name must be 1..=128 bytes, have no surrounding whitespace, and contain no control characters"
                    .into(),
            );
            return self;
        }
        let invalid_role = |role: &String| {
            role.is_empty()
                || role.len() > 128
                || role.trim() != role
                || role.chars().any(char::is_control)
        };
        if eligible_roles.is_empty() || eligible_roles.iter().any(invalid_role) {
            self.pending_errors.push(format!(
                "GraphQL client application `{application}` must declare one or more bounded non-empty eligible roles"
            ));
            return self;
        }
        if !eligible_roles_were_unique {
            self.pending_errors.push(format!(
                "GraphQL client application `{application}` eligible roles must be unique"
            ));
            return self;
        }
        if schema_roles.is_empty() || schema_roles.iter().any(invalid_role) {
            self.pending_errors.push(format!(
                "GraphQL client application `{application}` must declare one or more bounded non-empty schema roles"
            ));
            return self;
        }
        if !schema_roles_were_unique {
            self.pending_errors.push(format!(
                "GraphQL client application `{application}` schema roles must be unique"
            ));
            return self;
        }
        if schema_roles
            .iter()
            .any(|role| !eligible_roles.iter().any(|eligible| eligible == role))
        {
            self.pending_errors.push(format!(
                "GraphQL client application `{application}` schema roles must be a subset of eligible roles"
            ));
            return self;
        }
        let registration = ClientApplicationRegistration {
            eligible_roles,
            schema_roles,
        };
        if self
            .client_applications
            .insert(application.clone(), registration)
            .is_some()
        {
            self.pending_errors.push(format!(
                "GraphQL client application `{application}` was registered more than once"
            ));
        }
        self
    }

    /// Derive GraphQL command mutations from the service's typed executable
    /// inventory. This is the authoritative path: no second command list is
    /// accepted, and attachment later verifies the complete structural digest
    /// plus exact Rust input/output `TypeId`s.
    pub fn service(mut self, service: &Service) -> Self {
        if self.command_binding.is_some() {
            self.pending_errors
                .push("GraphQL service command inventory was configured more than once".into());
            return self;
        }
        let binding = match service.typed_command_binding() {
            Ok(binding) => binding,
            Err(error) => {
                self.pending_errors.push(error);
                return self;
            }
        };
        if let Some(configured) = self.service_id.as_deref() {
            if configured != binding.service_id {
                self.pending_errors.push(format!(
                    "GraphQL service ID `{configured}` does not match executable service ID `{}`",
                    binding.service_id
                ));
                return self;
            }
        }
        let contracts = service.typed_command_contracts();
        match TypedCommandInventory::from_contracts(&contracts) {
            Ok(commands) => self.typed_commands = commands,
            Err(error) => {
                self.pending_errors.push(error);
                return self;
            }
        }
        self.service_id = Some(binding.service_id.clone());
        self.command_binding = Some(binding);
        self
    }

    /// Mount a [`crate::graphql::ReadStore`] for one model. Default is SQL scan.
    /// Does not record the store on the [`crate::RelationalReadModel`] type
    /// (`DCS-DEC-008`).
    pub fn read_store<M: crate::RelationalReadModel>(
        mut self,
        store: crate::graphql::ReadStore,
    ) -> Self {
        let name = M::schema().model_name.clone();
        if !self.catalog.contains_key(&name) {
            self.pending_errors.push(format!(
                "read_store for unregistered model `{name}` (call `.model` first)"
            ));
            return self;
        }
        if self.read_stores.contains_key(&name) {
            self.pending_errors.push(format!(
                "read_store for model `{name}` was configured more than once"
            ));
            return self;
        }
        self.read_stores.insert(name, store);
        self
    }

    pub fn default_limit(mut self, n: u64) -> Self {
        self.default_limit = n;
        self
    }
    pub fn max_limit(mut self, n: u64) -> Self {
        self.max_limit = n;
        self
    }
    pub fn max_depth(mut self, n: usize) -> Self {
        self.max_depth = n;
        self
    }
    /// Maximum nested-selection complexity budget (default
    /// [`crate::graphql::complexity::DEFAULT_MAX_COMPLEXITY`]).
    ///
    /// Cost uses relationship-aware weights (see `complexity` module), not only
    /// flat GraphQL field counts, so multi-level `has_many` trees are bounded.
    pub fn max_complexity(mut self, n: usize) -> Self {
        self.max_complexity = n;
        self
    }
    pub fn max_in_list(mut self, n: usize) -> Self {
        self.max_in_list = n;
        self
    }
    /// Cap width of a single `_and` / `_or` list in client `where` (default 256).
    pub fn max_bool_width(mut self, n: usize) -> Self {
        self.max_bool_width = n;
        self
    }
    /// Fail closed on unknown or ungranted client `where` / `order_by` keys.
    ///
    /// **Default: `true`.** Set `false` only for intentional Hasura-style
    /// soft-skip of unknown keys (not recommended for production).
    pub fn strict_where(mut self, on: bool) -> Self {
        self.strict_where = on;
        self
    }
    pub fn introspection_for_anonymous(mut self, on: bool) -> Self {
        self.introspection_for_anonymous = on;
        self
    }
    /// Declare projector topology for client invalidation planning. The model
    /// and dependency IDs are validated when the one shared Surface is built.
    pub fn client_projectors(
        mut self,
        projectors: impl IntoIterator<Item = SurfaceProjector>,
    ) -> Self {
        self.projectors = projectors.into_iter().map(Into::into).collect();
        self
    }

    /// Declare a mixed client/query registry of async projectors and
    /// same-transaction-only projection owners.
    pub fn client_projection_owners(
        mut self,
        projectors: impl IntoIterator<Item = SurfaceProjectionOwner>,
    ) -> Self {
        self.projectors = projectors.into_iter().collect();
        self
    }
    pub fn statement_timeout(mut self, d: Duration) -> Self {
        self.statement_timeout = d;
        self
    }
    /// Enable the GraphiQL IDE on `GET /graphql`.
    ///
    /// GraphiQL's full introspection query receives an isolated depth/complexity
    /// allowance. Enabling an operational IDE never changes application query
    /// limits, generated client manifests, or schema fingerprints.
    pub fn graphiql(mut self, on: bool) -> Self {
        self.graphiql = on;
        self
    }
    /// Configure GraphQL HTTP identity mode (TrustedProxy / OidcBearer / Hybrid / DevHeaders).
    pub fn identity(mut self, config: IdentityConfig) -> Self {
        self.identity = config;
        self
    }
    pub fn change_stream(mut self, rx: tokio::sync::broadcast::Receiver<ReadModelChange>) -> Self {
        self.change_rx = Some(rx);
        self
    }

    pub fn build(mut self) -> Result<GraphqlEngine, GraphqlBuildError> {
        if !self.pending_errors.is_empty() {
            return Err(GraphqlBuildError(self.pending_errors.join("; ")));
        }
        if self.protocol_namespace.is_some() && self.protocol_token_key.is_none() {
            return Err(GraphqlBuildError(
                "GraphQL protocol namespace requires a protocol token key".into(),
            ));
        }
        if self.protocol_token_key.is_some() && self.service_id.is_none() {
            return Err(GraphqlBuildError(
                "GraphQL protocol tokens require a stable service ID; construct the engine with GraphqlEngine::from_schema_catalog or GraphqlEngineBuilder::service_id"
                    .into(),
            ));
        }

        let model_schemas = self
            .catalog
            .iter()
            .map(|(model, entry)| (model.clone(), entry.schema.clone()))
            .collect::<BTreeMap<_, _>>();
        self.typed_commands
            .bind_direct_projection_targets(&self.projectors, &model_schemas)
            .map_err(GraphqlBuildError)?;

        if let Some(expected) = self.command_binding.as_ref() {
            let service_id = self.service_id.as_deref().ok_or_else(|| {
                GraphqlBuildError(
                    "bound typed command inventory is missing its GraphQL service ID".into(),
                )
            })?;
            let contracts = self.typed_commands.contracts_for_binding();
            let actual = TypedServiceCommandBinding::from_contracts(service_id, &contracts)
                .map_err(GraphqlBuildError)?;
            // The builder copied this exact private command inventory from the
            // service and prevents later replacement. Its only intentional
            // structural change is the framework-owned direct-owner binding
            // above, so retain the pre-bind exact Rust type proof and publish
            // the final bound fingerprint for Service attachment.
            if actual.service_id != expected.service_id || actual.types != expected.types {
                return Err(GraphqlBuildError(
                    "final GraphQL command inventory differs from the bound executable service inventory"
                        .into(),
                ));
            }
            self.command_binding = Some(actual);
        }

        // Validate permissions.
        let declared_roles = self.roles.clone();
        let anonymous = self.anonymous_role.clone();
        let perm_keys: Vec<_> = self.permissions.keys().cloned().collect();
        for (model, role) in &perm_keys {
            if let Some(roles) = &declared_roles {
                if !roles.contains(role) && role != &anonymous {
                    return Err(GraphqlBuildError(format!(
                        "permission for undeclared role `{role}` (model `{model}`)"
                    )));
                }
            }
            let entry = self.catalog.get(model).ok_or_else(|| {
                GraphqlBuildError(format!("permission for unknown model `{model}`"))
            })?;
            let perm = &self.permissions[&(model.clone(), role.clone())].permission;

            // Column allowlist validation.
            if !perm.all_columns {
                if let Some(cols) = &perm.columns {
                    for col in cols {
                        if !entry
                            .schema
                            .columns
                            .iter()
                            .any(|c| c.column_name == *col && !c.skipped)
                        {
                            return Err(GraphqlBuildError(format!(
                                "unknown column `{col}` in permission for `{model}` role `{role}`"
                            )));
                        }
                    }
                }
            }

            if let Some(filter) = &perm.row_filter {
                validate_filter(
                    filter,
                    &entry.schema,
                    &self.catalog,
                    role == &anonymous,
                    model,
                    role,
                )?;
            }
        }

        // Name grammar / collisions across exposed models.
        validate_generated_names(&self.catalog)?;

        // Join-key resolution for catalog relationships used in permissions/schema.
        for entry in self.catalog.values() {
            for rel in &entry.schema.relationships {
                let Some(target) = self.catalog.get(&rel.target_model) else {
                    continue;
                };
                match rel.kind {
                    RelationshipKind::HasMany | RelationshipKind::BelongsTo => {
                        resolve_direct_join_keys(&entry.schema, rel, &target.schema)
                            .map_err(|e| GraphqlBuildError(e.to_string()))?;
                    }
                    RelationshipKind::ManyToMany => {
                        if rel.through.is_none() {
                            return Err(GraphqlBuildError(format!(
                                "model `{}` relationship `{}` many-to-many must declare `through`",
                                entry.schema.model_name, rel.field_name
                            )));
                        }
                        if let Some(through_name) = rel.through.as_deref() {
                            if let Some(through_model) = self.by_table.get(through_name) {
                                if let Some(through) = self.catalog.get(through_model) {
                                    resolve_m2m_join_keys(
                                        &entry.schema,
                                        rel,
                                        &through.schema,
                                        &target.schema,
                                    )
                                    .map_err(|e| GraphqlBuildError(e.to_string()))?;
                                }
                            }
                        }
                    }
                }
            }
        }

        let dialect = match &self.pool {
            #[cfg(feature = "postgres")]
            GraphqlPool::Postgres(_) => SqlDialect::Postgres,
            #[cfg(feature = "sqlite")]
            GraphqlPool::Sqlite(_) => SqlDialect::Sqlite,
            #[allow(unreachable_patterns)]
            _ => {
                return Err(GraphqlBuildError(
                    "GraphqlPool has no store feature enabled".into(),
                ));
            }
        };

        let tables: Vec<TableSchema> = self
            .catalog
            .values()
            .map(|entry| entry.schema.clone())
            .collect();
        let surface_options = SurfaceOptions {
            dialect: match dialect {
                SqlDialect::Sqlite => SurfaceDialect::Sqlite,
                SqlDialect::Postgres => SurfaceDialect::Postgres,
            },
            aggregates: true,
            subscriptions: self.subscriptions,
            default_limit: self.default_limit,
            max_limit: self.max_limit,
        };
        let full_surface = Arc::new(
            build_surface(&tables, &surface_options)
                .map_err(GraphqlBuildError)?
                .with_typed_commands(&self.typed_commands)
                .map_err(GraphqlBuildError)?
                .with_service_binding(self.command_binding.clone())
                .with_projection_owners(self.projectors.clone())
                .map_err(GraphqlBuildError)?,
        );
        let query_protocol = if self.protocol_token_key.is_some() {
            QueryProtocolRuntime::compile(&full_surface).map_err(GraphqlBuildError)?
        } else {
            QueryProtocolRuntime::default()
        };
        let projection_programs = self
            .protocol_token_key
            .is_some()
            .then(|| {
                crate::graphql::projection_delta::runtime::ProtocolProjectionProgramRegistry::try_from_surface(
                    &full_surface,
                )
                .map(Arc::new)
                .map_err(|error| GraphqlBuildError(error.to_string()))
            })
            .transpose()?;

        let mut roles: BTreeSet<String> = self.permissions.keys().map(|(_, r)| r.clone()).collect();
        if let Some(declared) = &declared_roles {
            roles.extend(declared.iter().cloned());
        }
        roles.insert(anonymous.clone());

        // Build per-role dynamic schemas.
        let mut schemas = HashMap::new();
        let mut graphiql_schemas = HashMap::new();
        let mut role_surfaces = BTreeMap::new();
        let mut protocol_roles = BTreeMap::new();
        for role in &roles {
            let grants = role_grants_from_model_role_perms(
                role,
                self.permissions
                    .iter()
                    .map(|(key, value)| (key, &value.permission)),
            );
            let role_surface = Arc::new(
                surface_for_role(&full_surface, role, &grants).map_err(GraphqlBuildError)?,
            );
            let schema = dyn_schema::build_role_schema(
                &role_surface,
                self.max_depth,
                self.max_complexity,
                role == &anonymous && !self.introspection_for_anonymous,
            )
            .map_err(GraphqlBuildError)?;
            if self.graphiql {
                let graphiql_schema = dyn_schema::build_role_schema(
                    &role_surface,
                    self.max_depth.max(GRAPHIQL_INTROSPECTION_MAX_DEPTH_FLOOR),
                    self.max_complexity
                        .max(GRAPHIQL_INTROSPECTION_MAX_COMPLEXITY_FLOOR),
                    role == &anonymous && !self.introspection_for_anonymous,
                )
                .map_err(GraphqlBuildError)?;
                graphiql_schemas.insert(role.clone(), graphiql_schema);
            }
            if self.protocol_token_key.is_some() {
                let service_id = self
                    .service_id
                    .as_deref()
                    .expect("protocol configuration validated a service ID");
                let (authorization_fingerprint, claim_keys) =
                    role_authorization_info(role, &self.permissions)?;
                let manifest = DistributedClientSurfaceExport::from_selected_with_execution(
                    service_id,
                    Arc::clone(&role_surface),
                    ClientExecutionLimits::from_runtime(
                        self.max_depth,
                        self.max_complexity,
                        self.max_bool_width,
                        self.max_in_list,
                    )
                    .map_err(|error| GraphqlBuildError(error.to_string()))?,
                )
                .and_then(|export| export.manifest())
                .map_err(|error| {
                    GraphqlBuildError(format!(
                        "failed to derive GraphQL protocol surface for role `{role}`: {error}"
                    ))
                })?;
                let trusted_presets = protocol_trusted_presets(&manifest)?;
                protocol_roles.insert(
                    role.clone(),
                    ProtocolRoleInfo {
                        surface: ProtocolSurfaceInfo {
                            schema_fingerprint: manifest.schema_fingerprint,
                            protocol_fingerprint: manifest.protocol_fingerprint,
                            trusted_presets,
                        },
                        authorization_fingerprint,
                        claim_keys,
                    },
                );
            }
            schemas.insert(role.clone(), schema);
            role_surfaces.insert(role.clone(), role_surface);
        }

        let mut application_surfaces = BTreeMap::new();
        let mut protocol_applications = BTreeMap::new();
        for (application, registration) in &self.client_applications {
            // Schema privilege set drives grant intersection (portable policies).
            // Eligible set is stamped on the surface identity for protocol open.
            let mut grants_by_role = BTreeMap::new();
            for role in &registration.schema_roles {
                if !role_surfaces.contains_key(role) {
                    return Err(GraphqlBuildError(format!(
                        "client application surface `{application}` schema role `{role}` is not configured"
                    )));
                }
                grants_by_role.insert(
                    role.clone(),
                    role_grants_from_model_role_perms(
                        role,
                        self.permissions
                            .iter()
                            .map(|(key, value)| (key, &value.permission)),
                    ),
                );
            }
            for role in &registration.eligible_roles {
                if !role_surfaces.contains_key(role) {
                    return Err(GraphqlBuildError(format!(
                        "client application surface `{application}` eligible role `{role}` is not configured"
                    )));
                }
            }
            let application_surface = Arc::new(
                surface_for_application_contract(
                    &full_surface,
                    application,
                    &registration.eligible_roles,
                    &registration.schema_roles,
                    &grants_by_role,
                )
                .map_err(GraphqlBuildError)?,
            );
            if self.protocol_token_key.is_some() {
                let service_id = self
                    .service_id
                    .as_deref()
                    .expect("protocol configuration validated a service ID");
                let manifest = DistributedClientSurfaceExport::from_selected_with_execution(
                    service_id,
                    Arc::clone(&application_surface),
                    ClientExecutionLimits::from_runtime(
                        self.max_depth,
                        self.max_complexity,
                        self.max_bool_width,
                        self.max_in_list,
                    )
                    .map_err(|error| GraphqlBuildError(error.to_string()))?,
                )
                .and_then(|export| export.manifest())
                .map_err(|error| {
                    GraphqlBuildError(format!(
                        "failed to derive GraphQL protocol surface for application `{application}`: {error}"
                    ))
                })?;
                let trusted_presets = protocol_trusted_presets(&manifest)?;
                // Privilege pack for execution: single schema role reuses that
                // role's schema/grants; multi-privilege uses a synthetic key.
                let privilege_key = if registration.schema_roles.len() == 1 {
                    registration.schema_roles[0].clone()
                } else {
                    format!("app:{application}")
                };
                let (authorization_fingerprint, claim_keys) =
                    if registration.schema_roles.len() == 1 {
                        role_authorization_info(&registration.schema_roles[0], &self.permissions)?
                    } else {
                        // Multi-privilege: fingerprint the application surface grant
                        // intersection under the synthetic privilege key.
                        role_authorization_info_for_roles(
                            &privilege_key,
                            &registration.schema_roles,
                            &self.permissions,
                        )?
                    };
                // Ensure the privilege key has a role surface for projection
                // visibility and a GraphQL schema when synthetic.
                if registration.schema_roles.len() > 1 {
                    let app_schema = dyn_schema::build_role_schema(
                        &application_surface,
                        self.max_depth,
                        self.max_complexity,
                        false,
                    )
                    .map_err(GraphqlBuildError)?;
                    schemas.insert(privilege_key.clone(), app_schema);
                    if self.graphiql {
                        let graphiql_schema = dyn_schema::build_role_schema(
                            &application_surface,
                            self.max_depth.max(GRAPHIQL_INTROSPECTION_MAX_DEPTH_FLOOR),
                            self.max_complexity
                                .max(GRAPHIQL_INTROSPECTION_MAX_COMPLEXITY_FLOOR),
                            false,
                        )
                        .map_err(GraphqlBuildError)?;
                        graphiql_schemas.insert(privilege_key.clone(), graphiql_schema);
                    }
                    role_surfaces.insert(privilege_key.clone(), Arc::clone(&application_surface));
                    // Intersected grants as ReadPermission under the synthetic key
                    // so compile_root finds privilege packs.
                    insert_synthetic_privilege_permissions(
                        &privilege_key,
                        &registration.schema_roles,
                        &mut self.permissions,
                    );
                }
                protocol_applications.insert(
                    application.clone(),
                    ProtocolApplicationInfo {
                        eligible_roles: registration.eligible_roles.clone(),
                        schema_roles: registration.schema_roles.clone(),
                        privilege_key,
                        surface: ProtocolSurfaceInfo {
                            schema_fingerprint: manifest.schema_fingerprint,
                            protocol_fingerprint: manifest.protocol_fingerprint,
                            trusted_presets,
                        },
                        authorization_fingerprint,
                        claim_keys,
                    },
                );
            }
            application_surfaces.insert(application.clone(), application_surface);
        }

        let change_hub = crate::graphql::subscribe::ChangeHub::new();
        if let Some(rx) = self.change_rx {
            crate::graphql::subscribe::spawn_change_forwarder(change_hub.clone(), rx);
        }

        let protocol = self.protocol_token_key.map(|key| {
            let service_id = self
                .service_id
                .clone()
                .expect("protocol configuration validated a service ID");
            ProtocolRuntime {
                codec: ProtocolTokenCodec::new(key),
                projection_programs: projection_programs
                    .expect("protocol configuration compiled projection programs"),
                namespace: self
                    .protocol_namespace
                    .unwrap_or_else(|| service_id.clone()),
                service_id,
                roles: protocol_roles,
                applications: protocol_applications,
            }
        });
        let identity_validator = self.identity.oidc.clone().map(OidcValidator::new);
        let mut read_store_kinds = BTreeMap::new();
        let mut cell_getters = BTreeMap::new();
        for (model, store) in self.read_stores {
            read_store_kinds.insert(model.clone(), store.kind());
            if let Some(getter) = store.cell_getter() {
                cell_getters.insert(model, getter);
            }
        }
        let inner = Arc::new(EngineInner {
            service_id: self.service_id,
            command_binding: self.command_binding,
            causal_storage_identity: self.causal_storage_identity,
            pool: self.pool,
            #[cfg(feature = "gateway-delivery")]
            read_routing: self.read_routing,
            #[cfg(feature = "gateway-delivery")]
            gateway_versions: self.gateway_versions,
            catalog: self.catalog,
            by_table: self.by_table,
            permissions: self.permissions,
            roles,
            anonymous_role: anonymous,
            default_limit: self.default_limit,
            max_limit: self.max_limit,
            max_depth: self.max_depth,
            max_complexity: self.max_complexity,
            max_in_list: self.max_in_list,
            max_bool_width: self.max_bool_width,
            strict_where: self.strict_where,
            introspection_for_anonymous: self.introspection_for_anonymous,
            statement_timeout: self.statement_timeout,
            graphiql: self.graphiql,
            typed_commands: self.typed_commands,
            role_surfaces,
            application_surfaces,
            schemas,
            graphiql_schemas,
            change_hub,
            dialect,
            identity: self.identity,
            identity_validator,
            protocol,
            query_protocol,
            read_stores: read_store_kinds,
            cell_getters,
        });

        Ok(GraphqlEngine { inner })
    }
}
