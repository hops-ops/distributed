//! `@live` refresh of a [`GraphqlEngine`]'s leaderboard roots.

use std::sync::Arc;

use distributed::bus::{Handlers, Message};
use distributed::graphql::GraphqlEngine;
use distributed::read_model::ReadModelChange;

use crate::{leaderboard_change_key, Board, LeaderboardChangeListener, LeaderboardChanged};

/// A [`LeaderboardChangeListener`] that refreshes `engine`'s `@live`
/// leaderboard subscriptions: register it on the projector writing the
/// boards (`LeaderboardProjector::with_change_listener`) when that projector
/// runs in the engine's process.
pub fn change_listener(engine: &GraphqlEngine) -> LeaderboardChangeListener {
    let hub = engine.change_hub().clone();
    Arc::new(move |board: &Board| {
        hub.publish(ReadModelChange::new([leaderboard_change_key(board.name())]));
    })
}

/// Bus handlers that refresh `engine`'s `@live` leaderboard subscriptions
/// from [`LEADERBOARD_CHANGED_EVENT`](crate::LEADERBOARD_CHANGED_EVENT)
/// notices, for a projector running in another process (it publishes them
/// with [`LeaderboardChangePublisher`](crate::LeaderboardChangePublisher)).
/// Run them with `bus.subscribe(Arc::new(handlers), RunOptions::default())`
/// under a consumer `group` of this process's own, so every GraphQL process
/// receives every notice.
pub fn change_handlers(engine: &GraphqlEngine, group: impl Into<String>) -> Handlers {
    let hub = engine.change_hub().clone();
    Handlers::new().named(group).on_event(
        crate::LEADERBOARD_CHANGED_EVENT,
        move |message: &Message| {
            let notice = LeaderboardChanged::from_message(message);
            let hub = hub.clone();
            async move {
                hub.publish(ReadModelChange::new([notice?.change_key()]));
                Ok(())
            }
        },
    )
}
