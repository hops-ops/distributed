//! Board change notices on the bus: `@live` boards across processes.
//!
//! A [`LeaderboardProjector`](super::LeaderboardProjector) calls its change
//! listeners in its own process. When the GraphQL engine serving the boards
//! runs elsewhere (a zone server projects kills into Redis; a GraphQL
//! service reads the same Redis), the projector's process publishes a
//! [`LEADERBOARD_CHANGED_EVENT`] on the application's bus and the GraphQL
//! process subscribes to it:
//!
//! ```rust,ignore
//! // projector process
//! let notices = LeaderboardChangePublisher::spawn(BusPublisher::new(Arc::new(bus)));
//! let projector = LeaderboardProjector::new(store, projection)
//!     .with_change_listener(notices.listener());
//!
//! // GraphQL process (its own consumer group, so every process gets every notice)
//! let handlers = engine.leaderboard_change_handlers("mobpit-service-boards");
//! tokio::spawn(async move { bus.subscribe(Arc::new(handlers), RunOptions::default()).await });
//! ```
//!
//! A notice names a board, nothing else: the subscriber re-reads its store
//! (hash-gated, so an unchanged top-N pushes nothing). Notices are hints, so
//! duplicates and replays are harmless; the publisher retries until the bus
//! accepts each one, so a transient bus outage delays a push but does not
//! lose it.

use serde::{Deserialize, Serialize};

use super::Board;
use crate::bus::{Message, MessageKind, TransportError};

/// Event name of a board change notice.
pub const LEADERBOARD_CHANGED_EVENT: &str = "distributed.leaderboard.changed";

/// Payload of a [`LEADERBOARD_CHANGED_EVENT`]: the board whose scores changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaderboardChanged {
    /// Board name (`Board::name`).
    pub board: String,
}

impl LeaderboardChanged {
    /// The notice for `board`.
    pub fn new(board: &Board) -> Self {
        Self {
            board: board.name().to_string(),
        }
    }

    /// The bus message of this notice (a fan-out event with a fresh id).
    pub fn to_message(&self) -> Message {
        let payload = serde_json::to_vec(self).expect("a board name serializes");
        Message::new(LEADERBOARD_CHANGED_EVENT, MessageKind::Event, payload)
            .with_id(uuid::Uuid::now_v7().to_string())
    }

    /// Decode a delivered notice. A malformed one is a permanent failure.
    pub fn from_message(message: &Message) -> Result<Self, TransportError> {
        let notice: Self = serde_json::from_slice(&message.payload).map_err(|error| {
            TransportError::permanent(format!("malformed {LEADERBOARD_CHANGED_EVENT}: {error}"))
        })?;
        Board::high_first(notice.board.clone()).map_err(|error| {
            TransportError::permanent(format!("malformed {LEADERBOARD_CHANGED_EVENT}: {error}"))
        })?;
        Ok(notice)
    }
}

#[cfg(feature = "leaderboard-background")]
pub use publisher::LeaderboardChangePublisher;

#[cfg(feature = "leaderboard-background")]
mod publisher {
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::mpsc;

    use super::super::{Board, LeaderboardChangeListener};
    use super::LeaderboardChanged;
    use crate::bus::MessagePublisher;

    const RETRY_FIRST: Duration = Duration::from_millis(100);
    const RETRY_MAX: Duration = Duration::from_secs(5);

    /// Publishes a [`LeaderboardChanged`] notice on the bus for every board a
    /// projector reports (its [`listener`](Self::listener)). Publishing runs
    /// on a Tokio task: the listener never blocks the projector. Changes to
    /// the same board that queue up while a publish is in flight coalesce
    /// into one notice; a failed publish is retried with backoff (100 ms up
    /// to 5 s) until the bus accepts it.
    #[derive(Clone, Debug)]
    pub struct LeaderboardChangePublisher {
        tx: mpsc::UnboundedSender<String>,
    }

    impl LeaderboardChangePublisher {
        /// Start publishing through `publisher` on the current Tokio runtime.
        /// The task ends when every clone of this handle (and of its
        /// listeners) is dropped and the queue is drained.
        pub fn spawn<P>(publisher: P) -> Self
        where
            P: MessagePublisher + 'static,
        {
            let (tx, mut rx) = mpsc::unbounded_channel::<String>();
            tokio::spawn(async move {
                while let Some(first) = rx.recv().await {
                    let mut boards = BTreeSet::from([first]);
                    while let Ok(next) = rx.try_recv() {
                        boards.insert(next);
                    }
                    for board in boards {
                        let message = LeaderboardChanged { board }.to_message();
                        let mut delay = RETRY_FIRST;
                        while let Err(error) = publisher.publish(message.clone()).await {
                            eprintln!("leaderboard change notice: {error}; retrying");
                            tokio::time::sleep(delay).await;
                            delay = (delay * 2).min(RETRY_MAX);
                        }
                    }
                }
            });
            Self { tx }
        }

        /// A projector change listener that queues a notice per changed board.
        pub fn listener(&self) -> LeaderboardChangeListener {
            let tx = self.tx.clone();
            Arc::new(move |board: &Board| {
                let _ = tx.send(board.name().to_string());
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notices_round_trip_and_reject_garbage() {
        let board = Board::high_first("mobpit.xp").unwrap();
        let message = LeaderboardChanged::new(&board).to_message();
        assert_eq!(message.name, LEADERBOARD_CHANGED_EVENT);
        assert_eq!(message.kind, MessageKind::Event);
        assert!(message.id.is_some());
        assert_eq!(
            LeaderboardChanged::from_message(&message).unwrap().board,
            "mobpit.xp"
        );
        let garbage = Message::new(
            LEADERBOARD_CHANGED_EVENT,
            MessageKind::Event,
            b"{}".to_vec(),
        );
        assert!(LeaderboardChanged::from_message(&garbage).is_err());
        let bad_board = Message::new(
            LEADERBOARD_CHANGED_EVENT,
            MessageKind::Event,
            br#"{"board":"bad{tag}"}"#.to_vec(),
        );
        assert!(LeaderboardChanged::from_message(&bad_board).is_err());
    }
}
