//! Command: `zitadel.ingress.v1` — map one provider delivery and publish the
//! user's profile. No aggregate: the profile is an external fact, committed
//! with the command's ledger receipt and projected into `auth_users`.

use axum::http::HeaderMap;
use distributed::command::{typed_command, PreparedCommand, Succeeded, TypedCommand};
use distributed::command_dispatch::deterministic_command_id;
use distributed::microsvc::{
    CausalCommandContext, CausalDispatchError, HandlerError, HttpCommandDispatch,
};
use distributed::ExternalEventSource;
use serde::Serialize;
use serde_json::Value;

use super::auth::{ingestor_principal, ingestor_session, verify_authenticity, INGESTOR_ROLE};
use super::map::{
    looks_like_action_event, map_action_delivery, normalize_ingress_body, ZitadelIngressInput,
};

/// Command name, and the path of its HTTP edge (POST `/{COMMAND}`).
pub const COMMAND: &str = "zitadel.ingress.v1";

#[derive(Debug, Serialize, distributed::CommandOutput)]
pub struct ZitadelIngressPayload {
    /// Provider event the delivery was; `None` when the event type maps to
    /// none, and nothing was published.
    pub published: Option<String>,
    /// Why nothing was published.
    pub skipped: Option<String>,
    pub event_type: Option<String>,
    pub event_id: Option<String>,
    pub provider_subject: Option<String>,
    pub user_kind: Option<String>,
    pub approval_status: Option<String>,
}

pub fn command() -> TypedCommand<ZitadelIngressInput, Succeeded<ZitadelIngressPayload>> {
    typed_command(COMMAND).roles([INGESTOR_ROLE])
}

pub async fn handle(
    ctx: &CausalCommandContext<'_, ()>,
    input: ZitadelIngressInput,
) -> Result<PreparedCommand<Succeeded<ZitadelIngressPayload>>, HandlerError> {
    let Some(mapped) = map_action_delivery(&input) else {
        return PreparedCommand::prepare(ZitadelIngressPayload {
            published: None,
            skipped: Some("unmapped_event_type".into()),
            event_type: input.event_type,
            event_id: input.delivery_id,
            provider_subject: None,
            user_kind: None,
            approval_status: None,
        })
        .map_err(|error| HandlerError::Other(Box::new(error)));
    };

    // The profile is identified by this command: a redelivery is this
    // command again, and another command never publishes the same fact.
    let source = ExternalEventSource {
        producer: COMMAND.into(),
        stream: ctx.message_id().unwrap_or_default().into(),
        position: 1,
        key: mapped.profile.user_id.clone(),
    };
    let payload = ZitadelIngressPayload {
        published: Some(mapped.message_name),
        skipped: None,
        event_type: input.event_type,
        event_id: Some(mapped.delivery_id),
        provider_subject: Some(mapped.profile.user_id.clone()),
        user_kind: Some(mapped.profile.user_kind.clone()),
        approval_status: Some(mapped.profile.approval_status.clone()),
    };
    ctx.publish_external(source, mapped.profile)
        .commit()?
        .succeeded(payload)
}

/// The ledger command id of one delivery. Zitadel delivers an event again
/// with the same aggregate, event type and sequence (a fixture, with the same
/// `delivery_id`), so a redelivery is the same command and replays its
/// receipt. A body that names no delivery is identified by its content.
pub fn ingress_command_id(input: &ZitadelIngressInput) -> String {
    match input.delivery_id.as_deref().map(str::trim) {
        Some(delivery_id) if !delivery_id.is_empty() => {
            deterministic_command_id(&[COMMAND, "delivery", delivery_id])
        }
        _ => deterministic_command_id(&[
            COMMAND,
            "content",
            &serde_json::to_string(input).unwrap_or_default(),
        ]),
    }
}

/// HTTP edge of [`COMMAND`] (`Service::with_http_command`): authenticate the
/// caller, then dispatch the delivery as the ingestor. No request header
/// reaches the command.
pub fn http_edge(
    headers: &HeaderMap,
    body: Value,
) -> Result<HttpCommandDispatch, CausalDispatchError> {
    verify_authenticity(headers, looks_like_action_event(&body))
        .map_err(CausalDispatchError::Handler)?;
    if body.is_null() {
        return Err(CausalDispatchError::BadRequest(
            "ingress body is empty".into(),
        ));
    }
    let input = normalize_ingress_body(&body);
    let command_id = ingress_command_id(&input);
    let input = serde_json::to_value(input)
        .map_err(|error| CausalDispatchError::Internal(error.to_string()))?;
    Ok(HttpCommandDispatch {
        command_id,
        input,
        session: ingestor_session(),
        principal: ingestor_principal(),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::super::auth::tests::IngestorEnv;
    use super::super::auth::SECRET_HEADER;
    use super::*;
    use distributed::microsvc::{self, Routes, Service};
    use distributed::{InMemoryRepository, OutboxStore};
    use e2e_projections::ZitadelUserProfile;
    use serde_json::json;

    /// The `zitadel.user_profile` facts in the outbox.
    async fn published_profiles(repo: &InMemoryRepository) -> Vec<ZitadelUserProfile> {
        repo.outbox_store()
            .pending(usize::MAX)
            .await
            .unwrap()
            .iter()
            .filter_map(|message| message.domain_event_occurrence().ok())
            .map(|occurrence| occurrence.decode_body().unwrap())
            .collect()
    }

    async fn post(base: &str, secret: &str, body: &Value) -> (u16, Value) {
        let response = reqwest::Client::new()
            .post(format!("{base}/{COMMAND}"))
            .header(SECRET_HEADER, secret)
            // A caller-chosen identity never reaches the command.
            .header("x-user-id", "attacker")
            .json(body)
            .send()
            .await
            .unwrap();
        (response.status().as_u16(), response.json().await.unwrap())
    }

    #[tokio::test]
    async fn a_redelivered_webhook_publishes_one_profile() {
        let _env = IngestorEnv::set(Some("s3cret"), false);
        let repo = InMemoryRepository::new();
        let service = Service::new()
            .named("identity")
            .routes(
                Routes::for_stores(repo.clone(), repo.clone())
                    .typed_command(command())
                    .handle(handle),
            )
            .with_http_command(COMMAND, http_edge);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let app = microsvc::router(Arc::new(service));
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let delivery = json!({
            "event_id": "evt-1",
            "event_type": "user.human.created",
            "user_id": "sub-1",
            "email": "ada@example.com",
            "display_name": "Ada",
        });

        // A caller without the secret runs no command.
        let (status, refused) = post(&base, "wrong", &delivery).await;
        assert_eq!(status, 401, "{refused}");
        assert!(published_profiles(&repo).await.is_empty());

        let (status, first) = post(&base, "s3cret", &delivery).await;
        assert_eq!(status, 200, "{first}");
        assert_eq!(first["payload"]["published"], super::super::HUMAN_CREATED);
        assert_eq!(first["payload"]["event_id"], "evt-1");
        let profiles = published_profiles(&repo).await;
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].user_id, "sub-1");
        assert_eq!(profiles[0].display_name, "Ada");
        assert_eq!(profiles[0].status, "active");

        // The provider delivers the event again: the stored receipt answers.
        let (_, again) = post(&base, "s3cret", &delivery).await;
        assert_eq!(again, first);
        assert_eq!(published_profiles(&repo).await.len(), 1);

        // One delivery id for another event is refused, not published.
        let other = json!({
            "event_id": "evt-1",
            "event_type": "user.human.created",
            "user_id": "sub-2",
        });
        let (status, reused) = post(&base, "s3cret", &other).await;
        assert_eq!(status, 409, "{reused}");
        assert_eq!(published_profiles(&repo).await.len(), 1);
    }
}
