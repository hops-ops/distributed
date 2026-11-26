use distributed::command;
use distributed::command::{Eventual, PreparedCommand};
use distributed::command_input_defaults;
use distributed::microsvc::{guards, CausalCommandContext, HandlerError};
use serde::{Deserialize, Serialize};

use crate::{domain_commands, ChatMessage, ChatMessagePostedDomainEvent, ChatMessageState};

#[derive(Debug, Deserialize, distributed::command::CommandInput)]
pub struct ChatPostInput {
    pub message_id: String,
    pub body: String,
    pub room_id: String,
    /// Unix milliseconds. Declared `now()`: the client's value only previews
    /// the optimistic row; the server stamps its own clock before this
    /// handler runs, so the stored ordering key is never the caller's.
    pub created_at: String,
}

#[derive(Debug, Serialize, distributed::command::CommandOutput)]
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
    let author = ctx.user_id()?.to_string();
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
            &input.created_at,
        )
        .map_err(HandlerError::rejected)?;

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

command! {
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
    guard: guards::authenticated,
    handle: handle_post,
    defaults: command_input_defaults! {
        input: ChatPostInput;
        default input.message_id = uuid_v7();
        default input.created_at = now();
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use distributed::microsvc::Routes;
    use distributed::{AggregateBuilder, InMemoryRepository};

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
