//! Causal wait-path host used by GraphQL. Local in-process or HTTP loopback.

use async_trait::async_trait;
use reqwest::redirect::Policy;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

use crate::graphql::identity::VerifiedPrincipal;
use crate::graphql::protocol::ProtocolResponseAccumulator;
use crate::microsvc::cell_host::{
    InternalHttpSecret, CELL_CAUSATION_ID_HEADER, CELL_INTERNAL_SECRET_HEADER,
    CELL_PRINCIPAL_PARTITION_HEADER, CELL_SERVICE_ID_HEADER,
};
use crate::microsvc::{
    CausalCommandPublicStatus, CausalDispatchError, CausalDispatchResult, Service, Session,
    ROLE_KEY, USER_ID_KEY,
};

/// Wait-path command host. GraphQL mutations call this instead of `Service`.
#[async_trait]
pub trait CommandHost: Send + Sync {
    async fn invoke(
        &self,
        command: &str,
        command_id: &str,
        input: Value,
        session: Session,
        principal: VerifiedPrincipal,
        protocol: Option<ProtocolResponseAccumulator>,
    ) -> Result<CausalDispatchResult, CausalDispatchError>;

    async fn status(
        &self,
        command_id: &str,
        session: &Session,
        principal: VerifiedPrincipal,
        protocol: Option<ProtocolResponseAccumulator>,
    ) -> Result<CausalCommandPublicStatus, CausalDispatchError>;
}

pub type SharedCommandHost = Arc<dyn CommandHost>;

pub(crate) fn validate_principal_session(
    session: &Session,
    principal: &VerifiedPrincipal,
) -> Result<(), CausalDispatchError> {
    let subject = session
        .user_id()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| CausalDispatchError::Rejected {
            code: "UNAUTHORIZED",
            status: 401,
            message: "durable commands require an authenticated session subject".into(),
        })?;
    if !principal.subject_matches(subject) {
        return Err(CausalDispatchError::Rejected {
            code: "UNAUTHORIZED",
            status: 401,
            message: "session subject does not match the verified principal".into(),
        });
    }
    Ok(())
}

pub(crate) fn validate_principal_session_if_present(
    session: &Session,
    principal: &VerifiedPrincipal,
) -> Result<(), CausalDispatchError> {
    match session
        .user_id()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(subject) if principal.subject_matches(subject) => Ok(()),
        Some(_) => Err(CausalDispatchError::Rejected {
            code: "UNAUTHORIZED",
            status: 401,
            message: "session subject does not match the verified principal".into(),
        }),
        None => Ok(()),
    }
}

/// In-process host wrapping a writer [`Service`].
pub struct LocalCommandHost {
    service: Arc<Service>,
}

impl LocalCommandHost {
    pub fn new(service: Arc<Service>) -> Self {
        Self { service }
    }

    pub fn service(&self) -> &Arc<Service> {
        &self.service
    }
}

#[async_trait]
impl CommandHost for LocalCommandHost {
    async fn invoke(
        &self,
        command: &str,
        command_id: &str,
        input: Value,
        session: Session,
        principal: VerifiedPrincipal,
        protocol: Option<ProtocolResponseAccumulator>,
    ) -> Result<CausalDispatchResult, CausalDispatchError> {
        validate_principal_session(&session, &principal)?;
        match protocol {
            Some(protocol) => {
                self.service
                    .dispatch_causal_with_receipt_and_protocol(
                        command, command_id, input, session, principal, protocol,
                    )
                    .await
            }
            None => {
                self.service
                    .dispatch_causal_with_receipt(command, command_id, input, session, principal)
                    .await
            }
        }
    }

    async fn status(
        &self,
        command_id: &str,
        session: &Session,
        principal: VerifiedPrincipal,
        protocol: Option<ProtocolResponseAccumulator>,
    ) -> Result<CausalCommandPublicStatus, CausalDispatchError> {
        validate_principal_session_if_present(session, &principal)?;
        match protocol {
            Some(protocol) => {
                self.service
                    .causal_command_status_with_protocol(command_id, session, principal, protocol)
                    .await
            }
            None => {
                self.service
                    .causal_command_status(command_id, session, principal)
                    .await
            }
        }
    }
}

/// HTTP wait-path client (`POST {base}/{command}` with `{ commandId, input }`).
#[derive(Clone)]
pub struct HttpCommandHost {
    base: reqwest::Url,
    client: reqwest::Client,
    internal_secret: Option<InternalHttpSecret>,
}

impl HttpCommandHost {
    const MAX_JSON_BYTES: usize = 2 * 1024 * 1024;

    pub fn new(base: impl AsRef<str>) -> Result<Self, CausalDispatchError> {
        Self::build(base.as_ref(), None)
    }

    pub fn new_internal(
        base: impl AsRef<str>,
        secret: InternalHttpSecret,
    ) -> Result<Self, CausalDispatchError> {
        Self::build(base.as_ref(), Some(secret))
    }

    fn build(
        base: &str,
        internal_secret: Option<InternalHttpSecret>,
    ) -> Result<Self, CausalDispatchError> {
        let base = reqwest::Url::parse(base.trim_end_matches('/')).map_err(|error| {
            CausalDispatchError::Internal(format!("invalid wait-path base URL: {error}"))
        })?;
        if !matches!(base.scheme(), "http" | "https")
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
            || base.host_str().is_none()
        {
            return Err(CausalDispatchError::Internal(
                "wait-path base URL must be http(s) with a host and without credentials, query, or fragment".into(),
            ));
        }
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(10))
            .redirect(Policy::none())
            .build()
            .map_err(|error| {
                CausalDispatchError::Internal(format!("wait-path HTTP client: {error}"))
            })?;
        Ok(Self {
            base,
            client,
            internal_secret,
        })
    }

    /// Same connection pool, different wait-path base (`{celld}/{shard}`).
    /// `Client::new()` loads TLS roots; do not construct one per command.
    pub fn retarget_segments(&self, segments: &[&str]) -> Result<Self, CausalDispatchError> {
        let mut base = self.base.clone();
        {
            let mut path = base.path_segments_mut().map_err(|_| {
                CausalDispatchError::Internal(
                    "wait-path base URL cannot contain path segments".into(),
                )
            })?;
            path.pop_if_empty();
            for segment in segments {
                if segment.is_empty() {
                    return Err(CausalDispatchError::Internal(
                        "wait-path URL segment must not be empty".into(),
                    ));
                }
                path.push(segment);
            }
        }
        Ok(Self {
            base,
            client: self.client.clone(),
            internal_secret: self.internal_secret.clone(),
        })
    }

    pub fn base(&self) -> &str {
        self.base.as_str()
    }

    fn request_url(&self, segment: &str) -> Result<reqwest::Url, CausalDispatchError> {
        if segment.is_empty() {
            return Err(CausalDispatchError::Internal(
                "wait-path URL segment must not be empty".into(),
            ));
        }
        let mut url = self.base.clone();
        url.path_segments_mut()
            .map_err(|_| {
                CausalDispatchError::Internal("wait-path URL cannot contain path segments".into())
            })?
            .pop_if_empty()
            .push(segment);
        Ok(url)
    }

    fn request_json(
        &self,
        segment: &str,
        body: &Value,
    ) -> Result<reqwest::RequestBuilder, CausalDispatchError> {
        let encoded = serde_json::to_vec(body).map_err(|error| {
            CausalDispatchError::Internal(format!("wait-path request JSON: {error}"))
        })?;
        if encoded.len() > Self::MAX_JSON_BYTES {
            return Err(CausalDispatchError::BadRequest(
                "wait-path request exceeds 2 MiB".into(),
            ));
        }
        let mut request = self
            .client
            .post(self.request_url(segment)?)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(encoded);
        if let Some(secret) = &self.internal_secret {
            request = request.header(CELL_INTERNAL_SECRET_HEADER, secret.header_value());
        }
        Ok(request)
    }

    async fn response_json(
        mut response: reqwest::Response,
    ) -> Result<(u16, Value), CausalDispatchError> {
        let status = response.status().as_u16();
        if response
            .content_length()
            .is_some_and(|length| length > Self::MAX_JSON_BYTES as u64)
        {
            return Err(CausalDispatchError::Internal(
                "wait-path response exceeds 2 MiB".into(),
            ));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|error| {
            CausalDispatchError::Internal(format!("wait-path HTTP body: {error}"))
        })? {
            if bytes.len().saturating_add(chunk.len()) > Self::MAX_JSON_BYTES {
                return Err(CausalDispatchError::Internal(
                    "wait-path response exceeds 2 MiB".into(),
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        let body = serde_json::from_slice(&bytes).map_err(|error| {
            CausalDispatchError::Internal(format!("wait-path HTTP body is not valid JSON: {error}"))
        })?;
        Ok((status, body))
    }

    /// POST `{base}/{path}` with a JSON body for internal host protocols.
    pub async fn post_json(
        &self,
        path: &str,
        body: Value,
    ) -> Result<(u16, Value), CausalDispatchError> {
        let response = self
            .request_json(path, &body)?
            .send()
            .await
            .map_err(|err| CausalDispatchError::Internal(format!("cell HTTP failed: {err}")))?;
        Self::response_json(response).await
    }

    /// Authenticated GET of one encoded path segment with the same transport bounds.
    pub async fn get_json(&self, path_segment: &str) -> Result<(u16, Value), CausalDispatchError> {
        let mut request = self.client.get(self.request_url(path_segment)?);
        if let Some(secret) = &self.internal_secret {
            request = request.header(CELL_INTERNAL_SECRET_HEADER, secret.header_value());
        }
        let response = request
            .send()
            .await
            .map_err(|error| CausalDispatchError::Internal(format!("cell HTTP failed: {error}")))?;
        Self::response_json(response).await
    }

    /// POST `{base}/{command}` and return status + JSON, including 4xx with
    /// cell `outbox` for drain retries.
    pub async fn post_wait_path(
        &self,
        command: &str,
        command_id: &str,
        input: Value,
        session: &Session,
    ) -> Result<(u16, Value), CausalDispatchError> {
        self.post_wait_path_inner(command, command_id, input, session, None, None)
            .await
    }

    /// POST a cell wait-path command with identity derived by the verified
    /// GraphQL host. These headers are part of the trusted internal boundary,
    /// not values copied from public request headers or command input.
    pub async fn post_cell_wait_path(
        &self,
        command: &str,
        command_id: &str,
        input: Value,
        session: &Session,
        service_id: &str,
        principal_partition: &str,
    ) -> Result<(u16, Value), CausalDispatchError> {
        self.post_cell_wait_path_with_causation(
            command,
            command_id,
            input,
            session,
            service_id,
            principal_partition,
            None,
        )
        .await
    }

    /// POST a cell wait-path command while carrying the gateway's durable
    /// causation through the authenticated internal boundary.
    pub async fn post_cell_wait_path_with_causation(
        &self,
        command: &str,
        command_id: &str,
        input: Value,
        session: &Session,
        service_id: &str,
        principal_partition: &str,
        causation_id: Option<&str>,
    ) -> Result<(u16, Value), CausalDispatchError> {
        let first = self
            .post_wait_path_inner(
                command,
                command_id,
                input.clone(),
                session,
                Some((service_id, principal_partition)),
                causation_id,
            )
            .await;
        if !matches!(&first, Ok((status, _)) if *status >= 500) {
            return first;
        }

        // Cell commands are fenced by commandId. A Worker may durably commit
        // the command and still fail while arming its outbox watchdog or
        // producing the response. One replay closes that ambiguous response
        // window without reapplying the aggregate mutation; persistent
        // failures still surface unchanged.
        self.post_wait_path_inner(
            command,
            command_id,
            input,
            session,
            Some((service_id, principal_partition)),
            causation_id,
        )
        .await
    }

    async fn post_wait_path_inner(
        &self,
        command: &str,
        command_id: &str,
        input: Value,
        session: &Session,
        cell_identity: Option<(&str, &str)>,
        causation_id: Option<&str>,
    ) -> Result<(u16, Value), CausalDispatchError> {
        let mut request = self.request_json(
            command,
            &serde_json::json!({
                "commandId": command_id,
                "input": input,
            }),
        )?;
        if let Some(user) = session.user_id() {
            request = request.header(USER_ID_KEY, user);
        }
        if let Some(roles) = session.get(ROLE_KEY) {
            request = request.header(ROLE_KEY, roles);
        }
        if let Some((service_id, principal_partition)) = cell_identity {
            if self.internal_secret.is_none() {
                return Err(CausalDispatchError::Internal(
                    "cell wait-path requires an internal HTTP secret".into(),
                ));
            }
            request = request
                .header(CELL_SERVICE_ID_HEADER, service_id)
                .header(CELL_PRINCIPAL_PARTITION_HEADER, principal_partition);
            if let Some(causation_id) = causation_id {
                request = request.header(CELL_CAUSATION_ID_HEADER, causation_id);
            }
        }
        let response = request.send().await.map_err(|err| {
            CausalDispatchError::Internal(format!("wait-path HTTP failed: {err}"))
        })?;
        Self::response_json(response).await
    }
}

#[async_trait]
impl CommandHost for HttpCommandHost {
    async fn invoke(
        &self,
        command: &str,
        command_id: &str,
        input: Value,
        session: Session,
        principal: VerifiedPrincipal,
        _protocol: Option<ProtocolResponseAccumulator>,
    ) -> Result<CausalDispatchResult, CausalDispatchError> {
        validate_principal_session(&session, &principal)?;
        let (status, body) = self
            .post_wait_path(command, command_id, input, &session)
            .await?;
        if status >= 400 {
            let message = body
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("wait-path rejected")
                .to_string();
            return Err(CausalDispatchError::Rejected {
                code: "REJECTED",
                status,
                message,
            });
        }
        CausalDispatchResult::from_wait_path_wire(body)
    }

    async fn status(
        &self,
        command_id: &str,
        session: &Session,
        principal: VerifiedPrincipal,
        _protocol: Option<ProtocolResponseAccumulator>,
    ) -> Result<CausalCommandPublicStatus, CausalDispatchError> {
        validate_principal_session_if_present(session, &principal)?;
        Ok(CausalCommandPublicStatus::unknown(command_id))
    }
}

/// Local dispatcher is a causal [`CommandHost`]. GraphQL must use this
/// trait object, not [`LocalCommandDispatcher::service`].
#[async_trait]
impl CommandHost for super::LocalCommandDispatcher {
    async fn invoke(
        &self,
        command: &str,
        command_id: &str,
        input: Value,
        session: Session,
        principal: VerifiedPrincipal,
        protocol: Option<ProtocolResponseAccumulator>,
    ) -> Result<CausalDispatchResult, CausalDispatchError> {
        LocalCommandHost::new(Arc::clone(self.service()))
            .invoke(command, command_id, input, session, principal, protocol)
            .await
    }

    async fn status(
        &self,
        command_id: &str,
        session: &Session,
        principal: VerifiedPrincipal,
        protocol: Option<ProtocolResponseAccumulator>,
    ) -> Result<CausalCommandPublicStatus, CausalDispatchError> {
        LocalCommandHost::new(Arc::clone(self.service()))
            .status(command_id, session, principal, protocol)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_host_rejects_unsafe_base_urls() {
        assert!(HttpCommandHost::new("ftp://writer.example").is_err());
        assert!(HttpCommandHost::new("https://user:pass@writer.example").is_err());
        assert!(HttpCommandHost::new("https://writer.example/path?secret=value").is_err());
        assert!(HttpCommandHost::new("https://writer.example/path#fragment").is_err());
        assert!(HttpCommandHost::new("https://writer.example/path").is_ok());
    }

    #[test]
    fn verified_principal_must_match_any_session_subject() {
        let principal =
            VerifiedPrincipal::test_oidc("https://issuer.example", "alice", &["distributed-tests"]);
        let mut session = Session::new();
        session.set(USER_ID_KEY, "mallory");
        assert!(validate_principal_session(&session, &principal).is_err());
        assert!(validate_principal_session_if_present(&session, &principal).is_err());

        session.set(USER_ID_KEY, "alice");
        assert!(validate_principal_session(&session, &principal).is_ok());
    }
}
