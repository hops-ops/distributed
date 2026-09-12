use super::*;

pub(super) enum ProtocolPreparationError {
    RequiredPreset,
    Internal,
}

impl From<()> for ProtocolPreparationError {
    fn from(_: ()) -> Self {
        Self::Internal
    }
}

impl ProtocolPreparationError {
    fn into_response(self) -> Response {
        match self {
            Self::RequiredPreset => {
                let mut error =
                    ServerError::new("required session input is missing or invalid", None);
                let mut extensions = async_graphql::ErrorExtensionValues::default();
                extensions.set("code", "BAD_REQUEST");
                error.extensions = Some(extensions);
                Response::from_errors(vec![error])
            }
            Self::Internal => protocol_internal_error_response(),
        }
    }
}

impl GraphqlEngine {
    pub async fn execute(&self, session: &Session, mut request: Request) -> Response {
        #[cfg(feature = "gateway-delivery")]
        if delivery::action(&request).as_deref() == Some("validate") {
            return self.validate_delivery(session, request).await;
        }
        if selected_operation_type(&mut request)
            == Some(async_graphql::parser::types::OperationType::Mutation)
            && !crate::microsvc::lifecycle_mutations_open()
        {
            return lifecycle_mutation_rejected();
        }
        let authority = match resolve_execution_authority(&self.inner, session, &request) {
            Ok(authority) => authority,
            Err(()) => {
                return Response::from_errors(vec![ServerError::new(
                    "GraphQL execution requires a named application surface for multi-role principals, a membership-checked role surface, or an anonymous session",
                    None,
                )]);
            }
        };
        let privilege = authority.privilege_role.clone();
        let introspection = self.inner.graphiql && is_pure_introspection_request(&mut request);
        let schema = if introspection {
            self.inner
                .graphiql_schemas
                .get(&privilege)
                .or_else(|| self.inner.schemas.get(&privilege))
        } else {
            self.inner.schemas.get(&privilege)
        };
        let Some(schema) = schema else {
            return Response::from_errors(vec![ServerError::new(
                format!("privilege pack `{privilege}` is not configured for GraphQL"),
                None,
            )]);
        };
        if has_multiple_protocol_query_roots(&self.inner, &privilege, &mut request) {
            return protocol_multi_root_error_response();
        }

        let accumulator = match self.protocol_accumulator(&authority, session, &request) {
            Ok(accumulator) => accumulator,
            Err(error) => return error.into_response(),
        };
        #[cfg(feature = "gateway-delivery")]
        if let Some(accumulator) = &accumulator {
            if self
                .enable_delivery_capture(session, &request, accumulator)
                .is_err()
            {
                return protocol_internal_error_response();
            }
        }
        if introspection {
            // The relaxed schema is defense-in-depth restricted even if a
            // future classifier or request extension behaves unexpectedly.
            request = request.only_introspection();
        }
        #[cfg(feature = "gateway-delivery")]
        let read = match self.prepare_read(session, &request) {
            Ok(read) => read,
            Err(error) => return read_routing::delivery_error(error),
        };
        #[cfg(feature = "gateway-delivery")]
        let request = request.data(read.clone());
        let mut request = request
            .data(session.clone())
            .data(authority)
            .data(Arc::clone(&self.inner));
        if let Some(accumulator) = &accumulator {
            request = request.data(accumulator.clone());
        }
        let start = std::time::Instant::now();
        let response =
            attach_protocol_response(schema.execute(request).await, accumulator.as_ref());
        #[cfg(feature = "gateway-delivery")]
        let response = read_routing::enforce_minimum(response, &read);
        let status = metrics_status_for_response(&response);
        let root_field = match &response.data {
            Value::Object(map) => map.keys().next().map(|s| s.as_str()).unwrap_or("_"),
            _ => "_",
        };
        record_metrics(session, root_field, status, start.elapsed());
        response
    }

    /// Execute a GraphQL subscription document as a stream of responses.
    pub fn execute_stream(
        &self,
        session: &Session,
        mut request: Request,
    ) -> BoxStream<'static, async_graphql::Response> {
        if selected_operation_type(&mut request)
            == Some(async_graphql::parser::types::OperationType::Mutation)
            && !crate::microsvc::lifecycle_mutations_open()
        {
            return stream::once(async { lifecycle_mutation_rejected() }).boxed();
        }
        let authority = match resolve_execution_authority(&self.inner, session, &request) {
            Ok(authority) => authority,
            Err(()) => {
                return stream::once(async {
                    Response::from_errors(vec![ServerError::new(
                        "GraphQL execution requires a named application surface for multi-role principals, a membership-checked role surface, or an anonymous session",
                        None,
                    )])
                })
                .boxed();
            }
        };
        let privilege = authority.privilege_role.clone();
        let introspection = self.inner.graphiql && is_pure_introspection_request(&mut request);
        let schema = if introspection {
            self.inner
                .graphiql_schemas
                .get(&privilege)
                .or_else(|| self.inner.schemas.get(&privilege))
        } else {
            self.inner.schemas.get(&privilege)
        };
        let Some(schema) = schema.cloned() else {
            return stream::once(async move {
                Response::from_errors(vec![ServerError::new(
                    format!("privilege pack `{privilege}` is not configured for GraphQL"),
                    None,
                )])
            })
            .boxed();
        };
        if has_multiple_protocol_query_roots(&self.inner, &privilege, &mut request) {
            return stream::once(async { protocol_multi_root_error_response() }).boxed();
        }
        let accumulator = match self.protocol_accumulator(&authority, session, &request) {
            Ok(accumulator) => accumulator,
            Err(error) => {
                return stream::once(async move { error.into_response() }).boxed();
            }
        };
        if accumulator
            .as_ref()
            .is_some_and(|accumulator| accumulator.begin_stream().is_err())
        {
            return stream::once(async { protocol_internal_error_response() }).boxed();
        }
        if introspection {
            request = request.only_introspection();
        }
        #[cfg(feature = "gateway-delivery")]
        let read = match self.prepare_read(session, &request) {
            Ok(read) => read,
            Err(error) => {
                return stream::once(async move { read_routing::delivery_error(error) }).boxed()
            }
        };
        #[cfg(feature = "gateway-delivery")]
        let request = request.data(read.clone());
        let mut request = request
            .data(session.clone())
            .data(authority)
            .data(std::sync::Arc::clone(&self.inner));
        if let Some(accumulator) = &accumulator {
            request = request.data(accumulator.clone());
        }
        schema
            .execute_stream(request)
            .map(move |response| {
                let response = attach_protocol_response(response, accumulator.as_ref());
                #[cfg(feature = "gateway-delivery")]
                let response = read_routing::enforce_minimum(response, &read);
                response
            })
            .boxed()
    }

    pub(super) fn protocol_accumulator(
        &self,
        authority: &ExecutionAuthority,
        session: &Session,
        request: &Request,
    ) -> Result<Option<ProtocolResponseAccumulator>, ProtocolPreparationError> {
        let Some(runtime) = &self.inner.protocol else {
            return Ok(None);
        };
        let (surface_identity, surface_info, authorization_fingerprint, claim_keys) =
            select_protocol_surface(runtime, authority)?;
        let trusted_presets = surface_info
            .trusted_presets
            .iter()
            .map(|descriptor| {
                resolve_protocol_preset(session, descriptor)
                    .ok_or(ProtocolPreparationError::RequiredPreset)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let principal = request
            .data
            .get(&TypeId::of::<VerifiedPrincipal>())
            .and_then(|principal| principal.downcast_ref::<VerifiedPrincipal>());
        let principal_partition =
            principal.map(|principal| principal.partition_for_service(&runtime.service_id));
        let projection_principal = principal_partition
            .as_deref()
            .map(crate::command_ledger::PrincipalPartitionId::new)
            .transpose()
            .map_err(|_| ())?;
        let session_authorization_context = claim_keys
            .iter()
            .map(|key| (key.as_str(), session.get(key)))
            .collect::<Vec<_>>();

        #[derive(Serialize)]
        struct CacheScopeMaterial<'a> {
            domain: &'static str,
            version: u32,
            namespace: &'a str,
            service_id: &'a str,
            /// Privilege pack for this surface (not a primary identity role).
            privilege: &'a str,
            /// Full asserted role set for multi-role principals.
            asserted_roles: &'a [String],
            surface: &'a ClientSurfaceIdentity,
            schema_fingerprint: &'a str,
            protocol_fingerprint: &'a str,
            authorization_surface_fingerprint: &'a str,
            identity_mode: &'static str,
            verified_principal_partition: Option<&'a str>,
            session_authorization_context: Vec<(&'a str, Option<&'a str>)>,
            trusted_presets: &'a [DistributedTrustedPreset],
        }

        // Only session values that can affect authorization enter the HMAC:
        // privilege pack, asserted roles, user, and claim keys from privilege
        // policies. Ambient headers must not churn caches.
        let material = CacheScopeMaterial {
            domain: "distributed.graphql.cache-scope",
            version: 2,
            namespace: &runtime.namespace,
            service_id: &runtime.service_id,
            privilege: &authority.privilege_role,
            asserted_roles: &authority.asserted_roles,
            surface: &surface_identity,
            schema_fingerprint: &surface_info.schema_fingerprint,
            protocol_fingerprint: &surface_info.protocol_fingerprint,
            authorization_surface_fingerprint: authorization_fingerprint,
            identity_mode: identity_mode_label(self.inner.identity.mode),
            verified_principal_partition: principal_partition.as_deref(),
            session_authorization_context,
            trusted_presets: &trusted_presets,
        };
        let cache_scope = runtime
            .codec
            .issue(ProtocolTokenPurpose::CacheScope, &material)
            .map_err(|_| ())?;
        let envelope = DistributedEnvelopeV1::new(
            surface_info.schema_fingerprint.clone(),
            authorization_fingerprint.to_string(),
            cache_scope,
            // Generated artifacts submit this exact document. Hashing its
            // bytes matches manifest operation_hash and provides a useful
            // identity/drift fence without claiming APQ negotiation.
            Some(operation_fingerprint(&request.query)),
        )
        .with_trusted_presets(trusted_presets.clone());
        let accumulator = ProtocolResponseAccumulator::new(envelope, runtime.codec.clone());
        if let Some(principal_scope) = projection_principal {
            let visibility_surface = self
                .inner
                .role_surfaces
                .get(&authority.privilege_role)
                .cloned()
                .ok_or(())?;
            let selected_surface = match &surface_identity {
                ClientSurfaceIdentity::Role { name } => self.inner.role_surfaces.get(name),
                ClientSurfaceIdentity::Application { name, .. } => {
                    self.inner.application_surfaces.get(name)
                }
            }
            .cloned()
            .ok_or(())?;
            let export = DistributedClientSurfaceExport::from_selected_with_execution(
                &runtime.service_id,
                selected_surface,
                ClientExecutionLimits::from_runtime(
                    self.inner.max_depth,
                    self.inner.max_complexity,
                    self.inner.max_bool_width,
                    self.inner.max_in_list,
                )
                .map_err(|_| ())?,
            )
            .map_err(|_| ())?;
            let issued_at_unix_ms = crate::time::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| ())?
                .as_millis()
                .try_into()
                .map_err(|_| ())?;
            let projection_request =
                crate::graphql::projection_delta::runtime::ProtocolProjectionRequestSeed::new(
                    export,
                    Arc::clone(&runtime.projection_programs),
                    principal_scope,
                    authorization_fingerprint.to_string(),
                    trusted_presets,
                    issued_at_unix_ms,
                )
                .map_err(|_| ())?
                .with_visibility_surface(visibility_surface)
                .map_err(|_| ())?;
            accumulator
                .bind_projection_request(projection_request)
                .map_err(|_| ())?;
        }
        accumulator
            .set_requested_live_resume(parse_requested_live_resume(request))
            .map_err(|_| ())?;
        Ok(Some(accumulator))
    }
}

fn selected_operation_type(
    request: &mut Request,
) -> Option<async_graphql::parser::types::OperationType> {
    let requested = request.operation_name.clone();
    let document = request.parsed_query().ok()?;
    let mut operations = document.operations.iter();
    if let Some(requested) = requested.as_deref() {
        return operations
            .find(|(name, _)| name.map(|name| name.as_str()) == Some(requested))
            .map(|(_, operation)| operation.node.ty);
    }
    let operation = operations.next()?.1;
    if operations.next().is_some() {
        return None;
    }
    Some(operation.node.ty)
}

fn lifecycle_mutation_rejected() -> Response {
    Response::from_errors(vec![ServerError::new(
        "application generation is reloading; mutation dispatch is unavailable",
        None,
    )])
}

#[cfg(test)]
mod lifecycle_request_tests {
    use super::selected_operation_type;
    use async_graphql::parser::types::OperationType;
    use async_graphql::Request;

    #[test]
    fn internal_protocol_preparation_errors_are_not_request_errors() {
        let response = super::ProtocolPreparationError::from(()).into_response();
        let expected = super::protocol_internal_error_response();
        assert_eq!(
            serde_json::to_value(response).unwrap(),
            serde_json::to_value(expected).unwrap()
        );
    }

    #[test]
    fn selected_operation_type_fails_closed_for_ambiguous_documents() {
        let mut mutation = Request::new("mutation Write { __typename }");
        assert_eq!(
            selected_operation_type(&mut mutation),
            Some(OperationType::Mutation)
        );

        let mut selected = Request::new("query Read { __typename } mutation Write { __typename }")
            .operation_name("Write");
        assert_eq!(
            selected_operation_type(&mut selected),
            Some(OperationType::Mutation)
        );

        let mut ambiguous = Request::new("query Read { __typename } mutation Write { __typename }");
        assert_eq!(selected_operation_type(&mut ambiguous), None);
    }
}
