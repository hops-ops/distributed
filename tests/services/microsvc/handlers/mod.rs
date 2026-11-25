use distributed::microsvc::{Session, ROLE_KEY, USER_ID_KEY};
use distributed::{
    AggregateRepository, InMemoryLockManager, InMemoryRepository, OutboxMessage, QueuedRepository,
    SourcedResult,
};
use serde::Serialize;

use crate::microsvc::models::counter::Counter;

pub type Repo =
    AggregateRepository<QueuedRepository<InMemoryRepository, InMemoryLockManager>, Counter>;

/// The role a signed-in caller of the counter commands holds. The commands
/// also declare the transport role, so a bus can deliver them.
pub const USER_ROLE: &str = "user";

/// The session of a signed-in caller.
pub fn user_session(user_id: &str) -> Session {
    let mut session = Session::new();
    session.set(USER_ID_KEY, user_id);
    session.set(ROLE_KEY, USER_ROLE);
    session
}

#[derive(Serialize)]
struct CounterState<'a> {
    id: &'a str,
    value: i64,
}

pub fn counter_state_message(counter: &Counter, event_type: &str) -> SourcedResult<OutboxMessage> {
    OutboxMessage::encode_for_entity(
        format!(
            "{}:{event_type}:{}",
            counter.entity.id(),
            counter.entity.version()
        ),
        event_type,
        &CounterState {
            id: counter.entity.id(),
            value: counter.value,
        },
        &counter.entity,
    )
}

pub mod counter_create;
pub mod counter_increment;
pub mod whoami;
