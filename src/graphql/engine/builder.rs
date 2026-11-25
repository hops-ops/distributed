use super::*;

impl GraphqlEngineBuilder {
    pub(crate) fn new(source: GraphqlPoolSource) -> Self {
        Self {
            service_id: None,
            protocol_token_key: None,
            protocol_namespace: None,
            client_applications: BTreeMap::new(),
            command_binding: None,
            lifecycle: crate::microsvc::LifecycleGate::always_open(),
            causal_storage_identity: source.causal_storage_identity,
            pool: source.pool,
            #[cfg(feature = "gateway")]
            read_routing: None,
            #[cfg(feature = "gateway")]
            gateway_versions: None,
            catalog: BTreeMap::new(),
            by_table: BTreeMap::new(),
            permissions: BTreeMap::new(),
            roles: None,
            anonymous_role: "anonymous".into(),
            default_limit: cost::DEFAULT_LIMIT,
            max_limit: cost::DEFAULT_MAX_LIMIT,
            max_depth: cost::DEFAULT_MAX_DEPTH,
            max_fields: cost::DEFAULT_MAX_FIELDS,
            max_rows: cost::DEFAULT_MAX_ROWS,
            max_in_list: cost::DEFAULT_MAX_IN_LIST,
            max_bool_width: cost::DEFAULT_MAX_BOOL_WIDTH,
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
            identity: None,
            websocket: WebsocketLimits::default(),
            read_stores: BTreeMap::new(),
            external: Vec::new(),
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
    #[cfg(feature = "gateway")]
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
    #[cfg(feature = "gateway")]
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
                        self.pending_errors.push(e.0);
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
            match self
                .permissions
                .entry(role.clone())
                .or_default()
                .entry(schema.model_name.clone())
            {
                Entry::Vacant(entry) => {
                    entry.insert(perm);
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
            match self
                .permissions
                .entry(role.clone())
                .or_default()
                .entry(schema.model_name.clone())
            {
                Entry::Vacant(entry) => {
                    entry.insert(permission);
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
            match self
                .permissions
                .entry(role.to_string())
                .or_default()
                .entry(model.clone())
            {
                Entry::Vacant(entry) => {
                    entry.insert(read().all_columns().aggregations());
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
        match self
            .permissions
            .entry(role.to_string())
            .or_default()
            .entry(model.clone())
        {
            Entry::Vacant(entry) => {
                entry.insert(p);
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

    /// Register one exact named application surface for generated clients,
    /// with distinct eligible and schema roles. The server still authorizes
    /// every request as its verified concrete role.
    ///
    /// - `eligible_roles`: wire/protocol role list; multi-role principals may
    ///   open the surface when any asserted role is in this set.
    /// - `schema_roles`: grant intersection for the client schema (must be a
    ///   non-empty subset of `eligible_roles`). Use e.g. eligible
    ///   `{admin,user}` + schema `{user}` so portable owner policies survive
    ///   without collapsing model permission definitions.
    pub fn client_application_surface(
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
    ///
    /// The engine also takes the service's
    /// [`LifecycleGate`](crate::microsvc::LifecycleGate) and refuses those
    /// mutations while it is closed.
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
        self.lifecycle = service.lifecycle().clone();
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

    // Query cost knobs. Each has one unit; `docs/graphql-query-cost.md`
    // describes the model.

    /// Page size of a list whose request gives no `limit` (default 100).
    pub fn default_limit(mut self, n: u64) -> Self {
        self.default_limit = n;
        self
    }
    /// Largest page size any list resolves to (default 1000). A role's
    /// `limit` may lower it further for that role's models.
    pub fn max_limit(mut self, n: u64) -> Self {
        self.max_limit = n;
        self
    }
    /// Nesting levels: fields in the document, and relationships and `where`
    /// filters in the compiled statement (default 8).
    pub fn max_depth(mut self, n: usize) -> Self {
        self.max_depth = n;
        self
    }
    /// Selected fields in one document, every field once at every level
    /// (default 500).
    pub fn max_fields(mut self, n: usize) -> Self {
        self.max_fields = n;
        self
    }
    /// Model rows one root field's statement may produce (default 200,000).
    ///
    /// Every list counts its resolved limit: a page of `limit` rows costs
    /// `limit × (1 + rows nested under one row)` and one object costs
    /// `1 + rows nested under it`, so nested lists cost about the product of
    /// their limits. A root field over budget fails with `BAD_REQUEST`
    /// `row budget exceeded` before any SQL runs.
    pub fn max_rows(mut self, n: u64) -> Self {
        self.max_rows = n;
        self
    }
    /// Entries in one `_in` / `_nin` list (default 1000).
    pub fn max_in_list(mut self, n: usize) -> Self {
        self.max_in_list = n;
        self
    }
    /// Entries in one `_and` / `_or` list in client `where` (default 256).
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
    /// Choose how requests are authenticated. Required: there is no default,
    /// so an engine can never silently trust client-supplied identity headers.
    pub fn identity(mut self, config: IdentityConfig) -> Self {
        self.identity = Some(config);
        self
    }
    /// Bound origin WebSocket connections (lifetime, idleness, live queries).
    pub fn websocket_limits(mut self, limits: WebsocketLimits) -> Self {
        self.websocket = limits;
        self
    }
    /// Serve an external read source's roots (queries, `@live`
    /// subscriptions, the client manifest); see
    /// [`ExternalReadSource`](crate::graphql::surface::ExternalReadSource).
    /// `@live` roots refresh when the change keys of their reads are
    /// published on [`GraphqlEngine::change_hub`].
    pub fn external_read_source(
        mut self,
        executor: Arc<dyn crate::graphql::surface::ExternalReadExecutor>,
    ) -> Self {
        self.external.push(executor);
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
        let identity = self.identity.take().ok_or_else(|| {
            GraphqlBuildError(
                "GraphQL engine has no identity mode; call .identity(..) with IdentityConfig::oidc_bearer, ::gateway or ::dev_headers"
                    .into(),
            )
        })?;
        if self.websocket.max_lifetime.is_zero()
            || self.websocket.idle_timeout.is_zero()
            || self.websocket.max_live_queries == 0
        {
            return Err(GraphqlBuildError(
                "GraphQL WebSocket limits must all be greater than zero".into(),
            ));
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
        let external_sources = self
            .external
            .iter()
            .map(|executor| executor.source())
            .collect::<Vec<_>>();
        for source in &external_sources {
            for role in source.roles() {
                if let Some(roles) = &declared_roles {
                    if !roles.contains(&role) && role != self.anonymous_role {
                        return Err(GraphqlBuildError(format!(
                            "{} `{}` grants undeclared role `{role}`",
                            source.section(),
                            source.name()
                        )));
                    }
                }
            }
        }
        let anonymous = self.anonymous_role.clone();
        for (role, grants) in &self.permissions {
            if let Some(roles) = &declared_roles {
                if !roles.contains(role) && role != &anonymous {
                    let model = grants.keys().next().map(String::as_str).unwrap_or("");
                    return Err(GraphqlBuildError(format!(
                        "permission for undeclared role `{role}` (model `{model}`)"
                    )));
                }
            }
            if role != &anonymous {
                continue;
            }
            // Columns, literals and relationship paths are validated once, by
            // the surface selection below. Only the engine knows which role
            // is anonymous, and an anonymous session carries no claims.
            for (model, grant) in grants {
                let mut claims = Vec::new();
                if let Some(filter) = &grant.row_filter {
                    filter.visit_claims(|claim| claims.push(claim.to_string()));
                }
                if !claims.is_empty() {
                    return Err(GraphqlBuildError(format!(
                        "claim() is not allowed in anonymous role filters (model `{model}`, claims: {})",
                        claims.join(", ")
                    )));
                }
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

        let client_execution = ClientExecutionLimits::from_runtime(
            self.max_depth,
            self.max_fields,
            self.max_rows,
            self.max_bool_width,
            self.max_in_list,
        )
        .map_err(|error| GraphqlBuildError(error.to_string()))?;
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
                .map_err(GraphqlBuildError)?
                .with_external_sources(external_sources.iter().cloned())
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

        let mut roles: BTreeSet<String> = self.permissions.keys().cloned().collect();
        roles.extend(external_sources.iter().flat_map(|source| source.roles()));
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
            let no_grants = BTreeMap::new();
            let grants = self.permissions.get(role).unwrap_or(&no_grants);
            let role_surface =
                Arc::new(surface_for_role(&full_surface, role, grants).map_err(GraphqlBuildError)?);
            let schema = dyn_schema::build_role_schema(
                &role_surface,
                self.max_depth,
                self.max_fields,
                role == &anonymous && !self.introspection_for_anonymous,
            )
            .map_err(GraphqlBuildError)?;
            if self.graphiql {
                let graphiql_schema = dyn_schema::build_role_schema(
                    &role_surface,
                    self.max_depth.max(GRAPHIQL_INTROSPECTION_MAX_DEPTH_FLOOR),
                    self.max_fields.max(GRAPHIQL_INTROSPECTION_MAX_FIELDS_FLOOR),
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
                    role_authorization_info(role, &self.permissions, &full_surface.external)?;
                let export = DistributedClientSurfaceExport::from_selected_with_execution(
                    service_id,
                    Arc::clone(&role_surface),
                    client_execution.clone(),
                )
                .map_err(|error| {
                    GraphqlBuildError(format!(
                        "failed to derive GraphQL protocol surface for role `{role}`: {error}"
                    ))
                })?;
                let manifest = export.manifest().map_err(|error| {
                    GraphqlBuildError(format!(
                        "failed to derive GraphQL protocol surface for role `{role}`: {error}"
                    ))
                })?;
                let trusted_presets = protocol_trusted_presets(&manifest)?;
                protocol_roles.insert(
                    role.clone(),
                    ProtocolRoleInfo {
                        surface: ProtocolSurfaceInfo {
                            export,
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
            for role in &registration.schema_roles {
                if !role_surfaces.contains_key(role) {
                    return Err(GraphqlBuildError(format!(
                        "client application surface `{application}` schema role `{role}` is not configured"
                    )));
                }
                self.permissions.entry(role.clone()).or_default();
            }
            for role in &registration.eligible_roles {
                if !role_surfaces.contains_key(role) {
                    return Err(GraphqlBuildError(format!(
                        "client application surface `{application}` eligible role `{role}` is not configured"
                    )));
                }
            }
            let application_surface = Arc::new(
                surface_for_application(
                    &full_surface,
                    application,
                    &registration.eligible_roles,
                    &registration.schema_roles,
                    &self.permissions,
                )
                .map_err(GraphqlBuildError)?,
            );
            if self.protocol_token_key.is_some() {
                let service_id = self
                    .service_id
                    .as_deref()
                    .expect("protocol configuration validated a service ID");
                let export = DistributedClientSurfaceExport::from_selected_with_execution(
                    service_id,
                    Arc::clone(&application_surface),
                    client_execution.clone(),
                )
                .map_err(|error| GraphqlBuildError(format!(
                    "failed to derive GraphQL protocol surface for application `{application}`: {error}"
                )))?;
                let manifest = export.manifest()
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
                        role_authorization_info(
                            &registration.schema_roles[0],
                            &self.permissions,
                            &full_surface.external,
                        )?
                    } else {
                        // Multi-privilege: fingerprint the application surface grant
                        // intersection under the synthetic privilege key.
                        role_authorization_info_for_roles(
                            &privilege_key,
                            &registration.schema_roles,
                            &self.permissions,
                            &full_surface.external,
                        )?
                    };
                // Ensure the privilege key has a role surface for projection
                // visibility and a GraphQL schema when synthetic.
                if registration.schema_roles.len() > 1 {
                    let app_schema = dyn_schema::build_role_schema(
                        &application_surface,
                        self.max_depth,
                        self.max_fields,
                        false,
                    )
                    .map_err(GraphqlBuildError)?;
                    schemas.insert(privilege_key.clone(), app_schema);
                    if self.graphiql {
                        let graphiql_schema = dyn_schema::build_role_schema(
                            &application_surface,
                            self.max_depth.max(GRAPHIQL_INTROSPECTION_MAX_DEPTH_FLOOR),
                            self.max_fields.max(GRAPHIQL_INTROSPECTION_MAX_FIELDS_FLOOR),
                            false,
                        )
                        .map_err(GraphqlBuildError)?;
                        graphiql_schemas.insert(privilege_key.clone(), graphiql_schema);
                    }
                    role_surfaces.insert(privilege_key.clone(), Arc::clone(&application_surface));
                }
                protocol_applications.insert(
                    application.clone(),
                    ProtocolApplicationInfo {
                        eligible_roles: registration.eligible_roles.clone(),
                        schema_roles: registration.schema_roles.clone(),
                        privilege_key,
                        surface: ProtocolSurfaceInfo {
                            export,
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
            lifecycle: self.lifecycle,
            causal_storage_identity: self.causal_storage_identity,
            pool: self.pool,
            #[cfg(feature = "gateway")]
            read_routing: self.read_routing,
            #[cfg(feature = "gateway")]
            gateway_versions: self.gateway_versions,
            catalog: self.catalog,
            by_table: self.by_table,
            anonymous_role: anonymous,
            default_limit: self.default_limit,
            max_limit: self.max_limit,
            max_depth: self.max_depth,
            max_rows: self.max_rows,
            max_in_list: self.max_in_list,
            max_bool_width: self.max_bool_width,
            client_execution,
            strict_where: self.strict_where,
            statement_timeout: self.statement_timeout,
            graphiql: self.graphiql,
            typed_commands: self.typed_commands,
            role_surfaces,
            application_surfaces,
            schemas,
            graphiql_schemas,
            change_hub,
            dialect,
            identity: IdentityResolver::new(identity),
            websocket: self.websocket,
            protocol,
            query_protocol,
            read_stores: read_store_kinds,
            cell_getters,
            external_executors: self
                .external
                .into_iter()
                .zip(external_sources)
                .map(|(executor, source)| {
                    (
                        (source.section().to_string(), source.name().to_string()),
                        executor,
                    )
                })
                .collect(),
        });

        Ok(GraphqlEngine { inner })
    }
}
