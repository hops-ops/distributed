use distributed::domain_event::DomainState;
use serde::{Deserialize, Serialize};

use super::ChatMessage;

/// Stable public post-transition body for `chat_message.posted`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, DomainState)]
#[domain_state(version = 1)]
pub struct ChatMessageState {
    pub message_id: String,
    pub room_id: String,
    pub author_id: String,
    pub body: String,
    pub created_at: String,
}

impl ChatMessageState {
    pub fn from_message(message: &ChatMessage) -> Self {
        Self::from(message)
    }
}

impl From<&ChatMessage> for ChatMessageState {
    fn from(message: &ChatMessage) -> Self {
        Self {
            message_id: message.message_id.clone(),
            room_id: message.room_id.clone(),
            author_id: message.author_id.clone(),
            body: message.body.clone(),
            created_at: message.created_at.clone(),
        }
    }
}
