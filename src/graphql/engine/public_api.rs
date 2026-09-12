use super::*;

impl GraphqlEngine {
    pub fn builder(pool: impl Into<GraphqlPoolSource>) -> GraphqlEngineBuilder {
        GraphqlEngineBuilder::new(pool.into())
    }

    pub fn from_schema_catalog(
        m: &ReadModelCatalog,
        pool: impl Into<GraphqlPoolSource>,
    ) -> Result<GraphqlEngineBuilder, GraphqlBuildError> {
        let mut builder = Self::builder(pool).service_id(m.name.clone());
        for schema in &m.tables {
            if schema.kind == TableKind::ReadModel {
                builder = builder.register_schema_exposed(schema.clone())?;
            } else {
                // Operational tables never become roots, but the shared
                // Surface needs them to derive opaque m2m dependencies.
                builder = builder.table_schema(schema.clone());
            }
        }
        Ok(builder)
    }

    pub fn sdl_for_role(&self, role: &str) -> Option<String> {
        if !self.inner.roles.contains(role) && role != self.inner.anonymous_role {
            // Still allow any role that has a built schema.
        }
        self.inner.schemas.get(role).map(|s| s.sdl())
    }

    /// Dep-free **surface IR** role SDL (A11 production path).
    ///
    /// Preferred for deterministic SDL generation and schema-drift checks. Uses
    /// the same catalog grants as the engine build (A12 mapper). Runtime dump is
    /// still available via [`Self::sdl_for_role`].
    pub fn ir_sdl_for_role(&self, role: &str) -> Result<String, String> {
        let surface = self
            .inner
            .role_surfaces
            .get(role)
            .ok_or_else(|| format!("role `{role}` is not configured for GraphQL"))?;
        graphql_sdl_from_surface(surface)
    }

    /// Return the exact role Surface instance used to construct the runtime
    /// schema. This is intentionally shared rather than reconstructed.
    pub fn surface_for_role(&self, role: &str) -> Option<Arc<Surface>> {
        self.inner.role_surfaces.get(role).cloned()
    }

    /// Stable service identity retained from [`ReadModelCatalog::name`].
    ///
    /// Engines built manually return `None` unless the builder opted in with
    /// [`GraphqlEngineBuilder::service_id`].
    pub fn service_id(&self) -> Option<&str> {
        self.inner.service_id.as_deref()
    }

    pub(crate) fn typed_command_binding(&self) -> Option<&TypedServiceCommandBinding> {
        self.inner.command_binding.as_ref()
    }

    pub(crate) fn typed_command_contracts_for_service(
        &self,
    ) -> Result<Vec<crate::command::TypedCommandContract>, String> {
        Ok(self.inner.typed_commands.contracts_for_binding())
    }

    pub(crate) fn causal_storage_identity(
        &self,
    ) -> Option<crate::command_ledger::CausalStorageIdentity> {
        self.inner.causal_storage_identity
    }

    pub(crate) fn causal_protocol_configured(&self) -> bool {
        self.inner.protocol.is_some()
    }

    pub fn client_surface_for_role(
        &self,
        role: &str,
    ) -> Result<DistributedClientSurfaceExport, ClientManifestError> {
        let service_id = self.client_export_service_id()?;
        let surface = self.surface_for_role(role).ok_or_else(|| {
            ClientManifestError(format!("role `{role}` is not configured for GraphQL"))
        })?;
        DistributedClientSurfaceExport::from_selected_with_execution(
            service_id,
            surface,
            ClientExecutionLimits::from_runtime(
                self.inner.max_depth,
                self.inner.max_complexity,
                self.inner.max_bool_width,
                self.inner.max_in_list,
            )?,
        )
    }

    pub fn client_manifest_for_role(
        &self,
        role: &str,
    ) -> Result<DistributedClientManifest, ClientManifestError> {
        self.client_surface_for_role(role)?.manifest()
    }

    pub fn client_surface_for_application(
        &self,
        application: &str,
        eligible_roles: &[&str],
        schema_roles: &[&str],
    ) -> Result<DistributedClientSurfaceExport, ClientManifestError> {
        let service_id = self.client_export_service_id()?;
        let surface = self
            .inner
            .application_surfaces
            .get(application)
            .cloned()
            .ok_or_else(|| {
                ClientManifestError(format!(
                    "application surface `{application}` is not registered"
                ))
            })?;
        let mut requested_eligible_roles = eligible_roles
            .iter()
            .map(|role| (*role).to_string())
            .collect::<Vec<_>>();
        let mut requested_schema_roles = schema_roles
            .iter()
            .map(|role| (*role).to_string())
            .collect::<Vec<_>>();
        requested_eligible_roles.sort();
        requested_schema_roles.sort();
        let SurfaceSelection::Application {
            eligible_roles: registered_roles,
            schema_roles: registered_schema_roles,
            ..
        } = &surface.selection
        else {
            return Err(ClientManifestError(format!(
                "registered application surface `{application}` has invalid identity"
            )));
        };
        if requested_eligible_roles != *registered_roles
            || requested_schema_roles != *registered_schema_roles
        {
            return Err(ClientManifestError(format!(
                "application surface `{application}` is registered for eligible roles [{}] and schema roles [{}], not eligible [{}] and schema [{}]",
                registered_roles.join(", "),
                registered_schema_roles.join(", "),
                requested_eligible_roles.join(", "),
                requested_schema_roles.join(", ")
            )));
        }
        DistributedClientSurfaceExport::from_selected_with_execution(
            service_id,
            surface,
            ClientExecutionLimits::from_runtime(
                self.inner.max_depth,
                self.inner.max_complexity,
                self.inner.max_bool_width,
                self.inner.max_in_list,
            )?,
        )
    }

    pub fn client_manifest_for_application(
        &self,
        application: &str,
        eligible_roles: &[&str],
        schema_roles: &[&str],
    ) -> Result<DistributedClientManifest, ClientManifestError> {
        self.client_surface_for_application(application, eligible_roles, schema_roles)?
            .manifest()
    }

    fn client_export_service_id(&self) -> Result<String, ClientManifestError> {
        self.inner.service_id.clone().ok_or_else(|| {
            ClientManifestError(
                "client export requires a service ID; construct the engine with GraphqlEngine::from_schema_catalog or GraphqlEngineBuilder::service_id"
                    .into(),
            )
        })
    }

    pub fn graphiql_enabled(&self) -> bool {
        self.inner.graphiql
    }

    /// Identity configuration used by GraphQL HTTP handlers.
    pub fn identity_config(&self) -> &IdentityConfig {
        &self.inner.identity
    }

    pub(crate) fn identity_validator(&self) -> Option<&OidcValidator> {
        self.inner.identity_validator.as_ref()
    }

    /// Whether unknown/ungranted client `where` and `order_by` keys fail closed.
    /// Default is `true` (see [`GraphqlEngineBuilder::strict_where`]).
    pub fn strict_where(&self) -> bool {
        self.inner.strict_where
    }

    /// Hub used by live subscriptions (tests may publish directly).
    pub fn change_hub(&self) -> &crate::graphql::subscribe::ChangeHub {
        &self.inner.change_hub
    }
}
