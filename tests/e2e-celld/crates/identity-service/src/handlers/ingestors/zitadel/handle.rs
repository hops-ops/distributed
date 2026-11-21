//! Command: `zitadel.ingress.v1` — map one provider delivery and publish its
//! provider message. No aggregate: the message commits with the command's
//! ledger receipt.

use distributed::command::{typed_command, PreparedCommand, Succeeded, TypedCommand};
use distributed::command_dispatch::deterministic_command_id;
use distributed::microsvc::{
    CausalCommandContext, CausalDispatchError, HandlerError, Service, Session,
};
use serde::Serialize;
use serde_json::Value;

use super::auth::{ingestor_principal, ingestor_session, verify_authenticity, INGESTOR_ROLE};
use super::map::{
    looks_like_action_event, map_action_delivery, normalize_ingress_body, ZitadelIngressInput,
};
use super::publish::provider_message;

/// Command name, and the path of its HTTP edge (POST `/{COMMAND}`).
pub const COMMAND: &str = "zitadel.ingress.v1";

#[derive(Debug, Serialize, distributed::CommandOutput)]
pub struct ZitadelIngressPayload {
    /// Provider message published; `None` when the event type maps to none.
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

    // Provider envelope only — the projector maps it to auth_users.
    let command_id = ctx.message_id().unwrap_or_default();
    let message = provider_message(format!("{COMMAND}:{command_id}"), &mapped)?;
    ctx.outbox(message)
        .commit()?
        .succeeded(ZitadelIngressPayload {
            published: Some(mapped.message_name),
            skipped: None,
            event_type: input.event_type,
            event_id: Some(mapped.delivery_id),
            provider_subject: Some(mapped.payload.provider_subject),
            user_kind: Some(mapped.payload.user_kind),
            approval_status: Some(mapped.payload.approval_status),
        })
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

/// HTTP edge of [`COMMAND`]: authenticate the caller, then dispatch the
/// delivery as the ingestor. `caller` is the request's headers; none of them
/// reaches the command.
pub async fn receive(
    service: &Service,
    caller: &Session,
    body: Value,
) -> Result<Value, CausalDispatchError> {
    verify_authenticity(caller, looks_like_action_event(&body))
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
    service
        .dispatch_causal(
            COMMAND,
            &command_id,
            input,
            ingestor_session(),
            ingestor_principal(),
        )
        .await
}

#[cfg(test)]
mod tests {
    use super::super::auth::tests::IngestorEnv;
    use super::super::auth::SECRET_HEADER;
    use super::super::map::HUMAN_CREATED;
    use super::*;
    use distributed::microsvc::Routes;
    use distributed::{InMemoryRepository, OutboxStore};
    use serde_json::json;

    fn caller(secret: &str) -> Session {
        let mut caller = Session::new();
        caller.set(SECRET_HEADER, secret);
        caller
    }

    async fn provider_messages(repo: &InMemoryRepository) -> usize {
        repo.outbox_store()
            .pending(usize::MAX)
            .await
            .unwrap()
            .iter()
            .filter(|message| message.event_type == HUMAN_CREATED)
            .count()
    }

    #[tokio::test]
    async fn a_redelivered_webhook_publishes_one_provider_message() {
        let _env = IngestorEnv::set(Some("s3cret"), false);
        let repo = InMemoryRepository::new();
        let service = Service::new().named("identity").routes(
            Routes::for_stores(repo.clone(), repo.clone())
                .typed_command(command())
                .handle(handle),
        );
        let delivery = json!({
            "event_id": "evt-1",
            "event_type": "user.human.created",
            "user_id": "sub-1",
            "email": "ada@example.com",
            "display_name": "Ada",
        });

        // A caller without the secret runs no command.
        let refused = receive(&service, &caller("wrong"), delivery.clone()).await;
        assert!(
            matches!(
                refused,
                Err(CausalDispatchError::Handler(HandlerError::Unauthorized(_)))
            ),
            "{refused:?}"
        );
        assert_eq!(provider_messages(&repo).await, 0);

        let first = receive(&service, &caller("s3cret"), delivery.clone())
            .await
            .unwrap();
        assert_eq!(first["published"], HUMAN_CREATED);
        assert_eq!(first["event_id"], "evt-1");
        assert_eq!(provider_messages(&repo).await, 1);

        // The provider delivers the event again: the stored receipt answers.
        let again = receive(&service, &caller("s3cret"), delivery)
            .await
            .unwrap();
        assert_eq!(again, first);
        assert_eq!(provider_messages(&repo).await, 1);

        // One delivery id for another event is refused, not published.
        let other = json!({
            "event_id": "evt-1",
            "event_type": "user.human.created",
            "user_id": "sub-2",
        });
        let reused = receive(&service, &caller("s3cret"), other).await;
        assert!(
            matches!(reused, Err(CausalDispatchError::CommandIdReuse)),
            "{reused:?}"
        );
        assert_eq!(provider_messages(&repo).await, 1);
    }
}
