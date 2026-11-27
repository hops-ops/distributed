//! Board change notices on the bus: `@live` boards across processes.
//!
//! A [`LeaderboardProjector`](crate::LeaderboardProjector) calls its change
//! listeners in its own process. When the GraphQL engine serving the boards
//! runs elsewhere (a zone server projects kills into Redis; a GraphQL
//! service reads the same Redis), the projector's process publishes a
//! [`LEADERBOARD_CHANGED_EVENT`] on the application's bus and the GraphQL
//! process subscribes to it:
//!
//! ```rust,ignore
//! // projector process
//! let notices = LeaderboardChangePublisher::spawn(BusPublisher::new(Arc::new(bus)), 1024);
//! let projector = LeaderboardProjector::new(store, projection)
//!     .with_change_listener(notices.listener());
//!
//! // GraphQL process (its own consumer group, so every process gets every notice)
//! let handlers = distributed_leaderboard::change_handlers(&engine, "mobpit-service-boards");
//! tokio::spawn(async move { bus.subscribe(Arc::new(handlers), RunOptions::default()).await });
//! ```
//!
//! A notice names a board, or every board: the subscriber re-reads its store
//! (hash-gated, so an unchanged top-N pushes nothing). Notices are hints, so
//! duplicates and replays are harmless.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::{Board, LeaderboardChangeListener};
use distributed::bus::{Message, MessageKind, MessagePublisher, TransportError};

/// Event name of a board change notice.
pub const LEADERBOARD_CHANGED_EVENT: &str = "distributed.leaderboard.changed";

/// Change key every leaderboard read also listens on: publishing it refreshes
/// every `@live` board (a resync after notices were coalesced away).
pub const ALL_BOARDS_CHANGE_KEY: &str = "leaderboard:*";

/// Payload of a [`LEADERBOARD_CHANGED_EVENT`]: the board whose scores changed,
/// or `None` for "any board may have changed" (resync).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaderboardChanged {
    /// Board name (`Board::name`); absent for every board.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub board: Option<String>,
}

impl LeaderboardChanged {
    /// The notice for `board`.
    pub fn new(board: &Board) -> Self {
        Self {
            board: Some(board.name().to_string()),
        }
    }

    /// The notice that every board may have changed.
    pub fn all() -> Self {
        Self { board: None }
    }

    /// The change key a GraphQL engine publishes for this notice.
    pub fn change_key(&self) -> String {
        match &self.board {
            Some(board) => crate::leaderboard_change_key(board),
            None => ALL_BOARDS_CHANGE_KEY.to_string(),
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
        let malformed = |error: String| {
            TransportError::permanent(format!("malformed {LEADERBOARD_CHANGED_EVENT}: {error}"))
        };
        let notice: Self = serde_json::from_slice(&message.payload)
            .map_err(|error| malformed(error.to_string()))?;
        if let Some(board) = &notice.board {
            Board::high_first(board.clone()).map_err(|error| malformed(error.to_string()))?;
        }
        Ok(notice)
    }
}

const RETRY_FIRST: Duration = Duration::from_millis(100);
const RETRY_MAX: Duration = Duration::from_secs(5);

/// Boards waiting to be announced, at most `capacity` distinct ones.
#[derive(Debug)]
struct Pending {
    boards: BTreeSet<String>,
    /// More boards changed than fit: announce every board once instead.
    all: bool,
    capacity: usize,
}

impl Pending {
    fn add(&mut self, board: &str) {
        if self.all || self.boards.contains(board) {
            return;
        }
        if self.boards.len() < self.capacity {
            self.boards.insert(board.to_string());
        } else {
            self.boards.clear();
            self.all = true;
        }
    }

    fn take(&mut self) -> Vec<LeaderboardChanged> {
        if std::mem::take(&mut self.all) {
            self.boards.clear();
            return vec![LeaderboardChanged::all()];
        }
        std::mem::take(&mut self.boards)
            .into_iter()
            .map(|board| LeaderboardChanged { board: Some(board) })
            .collect()
    }
}

/// Publishes a [`LeaderboardChanged`] notice on the bus for every board a
/// projector reports (its [`listener`](Self::listener)).
///
/// The listener never blocks the projector and memory stays bounded: changes
/// wait in a set of at most `capacity` distinct boards, so repeated changes
/// to a board coalesce into one notice. **Overflow:** a change to one more
/// board than fits clears the set and queues a single every-board notice
/// ([`LeaderboardChanged::all`]); subscribers resync every `@live` board
/// (hash-gated) instead of the publisher growing without bound. A publish
/// that fails retryably is retried with backoff (100 ms up to 5 s) while new
/// changes keep coalescing; a permanently refused notice is dropped and
/// logged.
#[derive(Clone, Debug)]
pub struct LeaderboardChangePublisher {
    pending: Arc<Mutex<Pending>>,
    /// Wakes the task; capacity 1, a full channel means it is already awake.
    wake: mpsc::Sender<()>,
}

impl LeaderboardChangePublisher {
    /// Start publishing through `publisher` on the current Tokio runtime,
    /// holding at most `capacity` (>= 1) distinct pending boards. The task
    /// ends when every clone of this handle (and of its listeners) is
    /// dropped and the pending notices are published.
    pub fn spawn<P>(publisher: P, capacity: usize) -> Self
    where
        P: MessagePublisher + 'static,
    {
        let pending = Arc::new(Mutex::new(Pending {
            boards: BTreeSet::new(),
            all: false,
            capacity: capacity.max(1),
        }));
        let (wake, mut woken) = mpsc::channel::<()>(1);
        let queue = Arc::clone(&pending);
        tokio::spawn(async move {
            while woken.recv().await.is_some() {
                let notices = queue.lock().expect("pending boards").take();
                for notice in notices {
                    publish(&publisher, notice.to_message()).await;
                }
            }
        });
        Self { pending, wake }
    }

    /// A projector change listener that queues a notice per changed board.
    pub fn listener(&self) -> LeaderboardChangeListener {
        let handle = self.clone();
        Arc::new(move |board: &Board| {
            handle
                .pending
                .lock()
                .expect("pending boards")
                .add(board.name());
            let _ = handle.wake.try_send(());
        })
    }
}

async fn publish<P: MessagePublisher>(publisher: &P, message: Message) {
    let mut delay = RETRY_FIRST;
    loop {
        match publisher.publish(message.clone()).await {
            Ok(()) => return,
            Err(error) if error.is_permanent() => {
                tracing::warn!(%error, "leaderboard change notice refused; dropped");
                return;
            }
            Err(error) => {
                tracing::warn!(%error, "leaderboard change notice failed; retrying");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(RETRY_MAX);
            }
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
            LeaderboardChanged::from_message(&message).unwrap(),
            LeaderboardChanged::new(&board)
        );
        let all = LeaderboardChanged::all().to_message();
        assert_eq!(all.payload, b"{}");
        assert_eq!(
            LeaderboardChanged::from_message(&all).unwrap().change_key(),
            ALL_BOARDS_CHANGE_KEY
        );
        let bad_board = Message::new(
            LEADERBOARD_CHANGED_EVENT,
            MessageKind::Event,
            br#"{"board":"bad{tag}"}"#.to_vec(),
        );
        assert!(LeaderboardChanged::from_message(&bad_board).is_err());
        let unknown = Message::new(
            LEADERBOARD_CHANGED_EVENT,
            MessageKind::Event,
            br#"{"boards":"x"}"#.to_vec(),
        );
        assert!(LeaderboardChanged::from_message(&unknown).is_err());
    }

    /// Forwards each publish attempt; fails the first `fail` ones with `error`.
    struct Recording {
        sent: mpsc::UnboundedSender<Option<String>>,
        fail: Mutex<Vec<TransportError>>,
    }

    impl MessagePublisher for Recording {
        async fn publish(&self, message: Message) -> Result<(), TransportError> {
            let notice = LeaderboardChanged::from_message(&message).unwrap();
            if let Some(error) = self.fail.lock().unwrap().pop() {
                return Err(error);
            }
            self.sent.send(notice.board).unwrap();
            Ok(())
        }
    }

    fn recording(
        fail: Vec<TransportError>,
    ) -> (Recording, mpsc::UnboundedReceiver<Option<String>>) {
        let (sent, rx) = mpsc::unbounded_channel();
        (
            Recording {
                sent,
                fail: Mutex::new(fail),
            },
            rx,
        )
    }

    fn board(name: &str) -> Board {
        Board::high_first(name).unwrap()
    }

    // The current-thread runtime cannot run the publisher task until the
    // test awaits, so every listener call below lands before any publish.
    #[tokio::test(flavor = "current_thread")]
    async fn repeated_changes_coalesce_per_board() {
        let (publisher, mut sent) = recording(Vec::new());
        let notices = LeaderboardChangePublisher::spawn(publisher, 4);
        let listener = notices.listener();
        for name in ["b", "a", "b", "a", "b"] {
            listener(&board(name));
        }
        assert_eq!(sent.recv().await.unwrap().as_deref(), Some("a"));
        assert_eq!(sent.recv().await.unwrap().as_deref(), Some("b"));
        drop((notices, listener));
        assert_eq!(sent.recv().await, None, "the task ends with its handles");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn overflow_resyncs_every_board_instead_of_growing() {
        let (publisher, mut sent) = recording(Vec::new());
        let notices = LeaderboardChangePublisher::spawn(publisher, 2);
        let listener = notices.listener();
        for index in 0..1_000 {
            listener(&board(&format!("board-{index}")));
        }
        {
            let pending = notices.pending.lock().unwrap();
            assert!(pending.all && pending.boards.is_empty(), "{pending:?}");
        }
        assert_eq!(sent.recv().await.unwrap(), None, "one every-board notice");
        listener(&board("after"));
        assert_eq!(
            sent.recv().await.unwrap().as_deref(),
            Some("after"),
            "per-board notices resume after the resync"
        );
        drop((notices, listener));
        assert_eq!(sent.recv().await, None);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn retryable_failures_retry_and_permanent_ones_are_dropped() {
        let (publisher, mut sent) = recording(vec![
            TransportError::retryable("bus down"),
            TransportError::permanent("refused"),
        ]);
        let notices = LeaderboardChangePublisher::spawn(publisher, 4);
        let listener = notices.listener();
        listener(&board("a"));
        listener(&board("b"));
        // `a` is refused permanently (dropped); `b` fails once, then lands.
        assert_eq!(sent.recv().await.unwrap().as_deref(), Some("b"));
        drop((notices, listener));
        assert_eq!(sent.recv().await, None, "`a` was not retried");
    }
}
