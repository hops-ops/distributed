use distributed::command::{Eventual, PreparedCommand};
use distributed::microsvc::{CausalCommandContext, HandlerError};
use distributed::portable_command;
use serde::{Deserialize, Serialize};

use crate::{domain_commands, ChatMessage, ChatMessagePostedDomainEvent, ChatMessageState};

fn rejected(err: impl std::fmt::Display) -> HandlerError {
    HandlerError::Rejected(err.to_string())
}

fn principal(ctx: &CausalCommandContext<'_, ChatMessage>) -> Result<String, HandlerError> {
    ctx.user_id().map(str::to_string)
}

fn authenticated_user(ctx: &CausalCommandContext<'_, ChatMessage>) -> bool {
    ctx.session().user_id().is_some_and(|id| !id.is_empty())
}

#[derive(Debug, Deserialize, distributed::CommandInput)]
pub struct ChatPostInput {
    pub message_id: String,
    pub body: String,
    pub room_id: String,
    /// Client-generated unix milliseconds used by the optimistic row and
    /// accepted only when it is close to server time.
    pub created_at: String,
}

#[derive(Debug, Serialize, distributed::CommandOutput)]
pub struct ChatPostPayload {
    pub message_id: String,
    pub room_id: String,
    pub author_id: String,
    pub body: String,
    pub created_at: String,
}

pub async fn handle_post(
    ctx: &CausalCommandContext<'_, ChatMessage>,
    input: ChatPostInput,
) -> Result<PreparedCommand<Eventual<ChatPostPayload>>, HandlerError> {
    let author = principal(ctx)?;
    let created_at = canonical_near_unix_millis(&input.created_at)?;
    let repo = ctx.repo();

    if repo.get(&input.message_id).await?.is_some() {
        return Err(HandlerError::Rejected(format!(
            "message {} already exists",
            input.message_id
        )));
    }

    let mut message = repo.create();
    message
        .post(
            &input.message_id,
            &input.room_id,
            &author,
            &input.body,
            &created_at,
        )
        .map_err(rejected)?;

    let state = ChatMessageState::from(&*message);
    repo.publish_events()
        .commit(message)?
        .eventual(ChatPostPayload {
            message_id: state.message_id,
            room_id: state.room_id,
            author_id: state.author_id,
            body: state.body,
            created_at: state.created_at,
        })
}

/// Accept a client timestamp only when it is canonical unix milliseconds
/// within five minutes of server time.
///
/// `wasm32-unknown-unknown` has no `SystemTime::now` (cell hosts). There the
/// value must still be canonical digits; the GraphQL wait-path already
/// accepted it on the native host.
pub fn canonical_near_unix_millis(value: &str) -> Result<String, HandlerError> {
    let millis = value
        .parse::<u128>()
        .map_err(|_| rejected("created_at must be canonical unix milliseconds"))?;
    if millis.to_string() != value {
        return Err(rejected(
            "created_at must be canonical unix milliseconds within five minutes of server time",
        ));
    }
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        if millis.abs_diff(now) > 300_000 {
            return Err(rejected(
                "created_at must be canonical unix milliseconds within five minutes of server time",
            ));
        }
    }
    Ok(value.to_string())
}

portable_command! {
    name: "chat.post",
    transition: domain_commands::Post,
    aggregate: ChatMessage,
    input: ChatPostInput,
    outcome: Eventual<ChatPostPayload>,
    shard: |input| input.message_id.clone(),
    roles: ["user", "admin"],
    field: "chat_messages_post",
    authenticated_user_field: (
        ChatMessagePostedDomainEvent,
        ChatMessageState,
        "author_id"
    ),
    guard: authenticated_user,
    handle: handle_post,
}

#[cfg(test)]
mod tests {
    use super::*;
    use distributed::microsvc::Routes;
    use distributed::{AggregateBuilder, InMemoryRepository};

    #[test]
    fn shard_is_message_id() {
        let input = ChatPostInput {
            message_id: "m1".into(),
            body: "hi".into(),
            room_id: "lobby".into(),
            created_at: "1".into(),
        };
        assert_eq!(Post::shard(&input), "m1");
    }

    #[test]
    fn post_uses_handle_escape_hatch() {
        assert_eq!(Post::COMMAND, "chat.post");
        let _ = handle_post;
        let _ = canonical_near_unix_millis;
    }

    #[test]
    fn created_at_rejects_non_canonical() {
        assert!(canonical_near_unix_millis("not-a-time").is_err());
        assert!(canonical_near_unix_millis("01").is_err());
    }

    #[test]
    fn declaration_mounts_without_host_specific_dependencies() {
        let specs = Routes::new()
            .with_repo(InMemoryRepository::new().aggregate::<ChatMessage>())
            .mount(post())
            .command_specs()
            .expect("chat command declaration compiles");
        let spec = specs
            .iter()
            .find(|spec| spec.id == "chat.post")
            .expect("chat.post");
        assert_eq!(spec.field_name, "chat_messages_post");
        assert_eq!(spec.roles, ["admin", "user"]);
    }
}
