//! Chat: mutation + projection.
//!
//! Unit partition so the lobby `@live` subscription can advertise resumable
//! index evidence (`live.mode = "resumable"`). Room isolation stays on the
//! GraphQL document (`where: { room_id: { _eq: "lobby" } }`). Expression
//! partitions are correct for multi-room worker sharding, but they make live
//! indexes incomparable, so live delivery uses authorized snapshots without
//! partition-wide resume cursors.

use chat_domain::ChatMessagePostedDomainEvent;
use distributed::mutation_file;
use distributed::projection::lower::{DirectCandidate, ProjectionDescriptor};
use distributed::Mutation;
use e2e_readmodels::ChatMessages;

/// Mutation: complete-row upsert for chat messages.
///
/// The constructor name matches the document operation: `mutation SaveChatMessage`.
#[allow(non_snake_case)]
pub fn SaveChatMessage() -> Mutation<()> {
    mutation_file!("src/mutations/save_chat_message.mutation.graphql")
}

// Event-first: on { events, mutation, input }.
// Macro is `projection!` (crate root); `distributed::projection` is the module.
distributed::projection! {
    pub const CHAT_MESSAGES: ProjectionDescriptor<DirectCandidate> = {
        name: "project_chat_messages",
        version: 1,
        epoch: "e2e-ui-chat-v2",
        model: ChatMessages,
        on {
            events: [ChatMessagePostedDomainEvent],
            mutation: SaveChatMessage,
            input: { message: body },
        },
    };
}

#[cfg(test)]
mod tests {
    use distributed::domain_event::DomainEventContract;
    use distributed::{MutationKind, RelationalReadModel, RowValue, TableMutation};

    use super::*;
    use chat_domain::{ChatMessage, ChatMessagePostedDomainEvent};

    #[test]
    fn posted_applies_save_chat_message() {
        let mut message = ChatMessage::default();
        message.post("m1", "lobby", "alice", "hello", "1").unwrap();
        let occurrence = message.entity.pending_domain_events().last().unwrap();
        assert_eq!(
            occurrence.descriptor(),
            &ChatMessagePostedDomainEvent::descriptor()
        );
        let lowered = CHAT_MESSAGES
            .server_executor()
            .unwrap()
            .plan(occurrence)
            .unwrap();
        let TableMutation::UpsertRow(row) = &lowered.write_plan.mutations[0] else {
            panic!("expected upsert");
        };
        assert_eq!(
            row.values.get("body"),
            Some(&RowValue::String("hello".into()))
        );
        assert_eq!(
            SaveChatMessage().program().operations()[0].kind(),
            MutationKind::Upsert
        );
        assert_eq!(ChatMessages::schema().table_name, "chat_messages");
    }

    #[test]
    fn mutation_is_event_free() {
        assert_eq!(SaveChatMessage().program().name(), "SaveChatMessage");
        let json = serde_json::to_value(SaveChatMessage().program())
            .unwrap()
            .to_string();
        assert!(!json.contains("event_name"));
    }
}
