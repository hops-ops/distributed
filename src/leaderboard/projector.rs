//! Event → leaderboard projection, with idempotent apply and rebuild.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::bus::{Message, TransportError};
use crate::DomainEventOccurrence;

use super::{ApplyKey, ApplyOutcome, Board, LeaderboardError, LeaderboardStore, ScoreSubmission};

/// One score change a projection derives from an occurrence.
#[derive(Debug, Clone, PartialEq)]
pub struct LeaderboardChange {
    /// The live board to change (the projector redirects it to staging during
    /// a rebuild).
    pub board: Board,
    /// The submission.
    pub submission: ScoreSubmission,
}

impl LeaderboardChange {
    /// Build a change.
    pub fn new(board: Board, submission: ScoreSubmission) -> Self {
        Self { board, submission }
    }
}

/// Maps domain event occurrences to leaderboard changes.
///
/// Must be deterministic: replaying an occurrence must yield the same changes
/// in the same order, because each change's dedupe key is
/// `"<occurrence id>#<index>"`. Closures
/// `Fn(&DomainEventOccurrence) -> Result<Vec<LeaderboardChange>, LeaderboardError>`
/// implement this trait and handle every event name.
pub trait LeaderboardProjection: Send + Sync {
    /// Whether this projection cares about the event name. Messages for other
    /// names are acknowledged without decoding.
    fn handles(&self, _event_name: &str) -> bool {
        true
    }

    /// Changes for one occurrence (possibly none).
    fn project(
        &self,
        event: &DomainEventOccurrence,
    ) -> Result<Vec<LeaderboardChange>, LeaderboardError>;
}

impl<F> LeaderboardProjection for F
where
    F: Fn(&DomainEventOccurrence) -> Result<Vec<LeaderboardChange>, LeaderboardError> + Send + Sync,
{
    fn project(
        &self,
        event: &DomainEventOccurrence,
    ) -> Result<Vec<LeaderboardChange>, LeaderboardError> {
        self(event)
    }
}

/// Counts from applying one or more occurrences.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProjectionReport {
    /// Changes applied.
    pub applied: u64,
    /// Changes skipped as already applied.
    pub duplicates: u64,
    /// Occurrences skipped because the projection does not handle them.
    pub ignored: u64,
}

impl ProjectionReport {
    fn merge(&mut self, other: ProjectionReport) {
        self.applied += other.applied;
        self.duplicates += other.duplicates;
        self.ignored += other.ignored;
    }
}

/// Called with each live board whose scores changed: after an applied (not
/// duplicate) change, and after a rebuild promotes a board. A GraphQL engine
/// turns this into `@live` invalidation
/// (`GraphqlEngine::leaderboard_change_listener`).
pub type LeaderboardChangeListener = Arc<dyn Fn(&Board) + Send + Sync>;

/// Applies a [`LeaderboardProjection`] to a [`LeaderboardStore`].
pub struct LeaderboardProjector<S: ?Sized, P: ?Sized> {
    store: Arc<S>,
    projection: Arc<P>,
    listeners: Vec<LeaderboardChangeListener>,
}

impl<S: ?Sized, P: ?Sized> Clone for LeaderboardProjector<S, P> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            projection: Arc::clone(&self.projection),
            listeners: self.listeners.clone(),
        }
    }
}

impl<S, P> LeaderboardProjector<S, P>
where
    S: LeaderboardStore + ?Sized,
    P: LeaderboardProjection + ?Sized,
{
    /// A projector over a store and projection.
    pub fn new(store: Arc<S>, projection: Arc<P>) -> Self {
        Self {
            store,
            projection,
            listeners: Vec::new(),
        }
    }

    /// Notify `listener` whenever a live board's scores change.
    pub fn with_change_listener(mut self, listener: LeaderboardChangeListener) -> Self {
        self.listeners.push(listener);
        self
    }

    fn notify(&self, board: &Board) {
        for listener in &self.listeners {
            listener(board);
        }
    }

    /// The store.
    pub fn store(&self) -> &Arc<S> {
        &self.store
    }

    /// Apply one occurrence to the live boards, idempotently.
    pub async fn apply_event(
        &self,
        event: &DomainEventOccurrence,
    ) -> Result<ProjectionReport, LeaderboardError> {
        self.apply_to(event, false).await
    }

    async fn apply_to(
        &self,
        event: &DomainEventOccurrence,
        staging: bool,
    ) -> Result<ProjectionReport, LeaderboardError> {
        if !self.projection.handles(&event.descriptor().name) {
            return Ok(ProjectionReport {
                ignored: 1,
                ..ProjectionReport::default()
            });
        }
        let changes = self.projection.project(event)?;
        self.apply_changes(event, changes, staging).await
    }

    async fn apply_changes(
        &self,
        event: &DomainEventOccurrence,
        changes: Vec<LeaderboardChange>,
        staging: bool,
    ) -> Result<ProjectionReport, LeaderboardError> {
        let mut report = ProjectionReport::default();
        for (index, change) in changes.into_iter().enumerate() {
            let board = if staging {
                change.board.staging()
            } else {
                change.board.live()
            };
            let key = ApplyKey::new(
                format!("{}#{index}", event.id()),
                event.occurred_at_unix_ms(),
            );
            match self.store.apply(&board, &change.submission, &key).await? {
                ApplyOutcome::Applied { .. } => {
                    report.applied += 1;
                    if !staging {
                        self.notify(&board);
                    }
                }
                ApplyOutcome::Duplicate { .. } => report.duplicates += 1,
            }
        }
        Ok(report)
    }

    /// Decode an outbox/bus message carrying a canonical
    /// [`DomainEventOccurrence`] and apply it.
    ///
    /// Suitable as a bus event handler body: an unavailable store maps to a
    /// retryable [`TransportError`] (redelivery is safe thanks to dedupe), an
    /// undecodable payload or invalid change to a permanent one.
    pub async fn apply_message(
        &self,
        message: &Message,
    ) -> Result<ProjectionReport, TransportError> {
        if !self.projection.handles(message.name()) {
            return Ok(ProjectionReport {
                ignored: 1,
                ..ProjectionReport::default()
            });
        }
        let event =
            DomainEventOccurrence::from_canonical_bytes(message.payload()).map_err(|err| {
                TransportError::permanent(format!("leaderboard: undecodable occurrence: {err}"))
            })?;
        self.apply_event(&event).await.map_err(Into::into)
    }

    /// Start a rebuild of `boards` (live variants) into their staging
    /// variants. Boards the projection touches during the rebuild but that are
    /// not listed are added (and promoted) automatically; listed boards that
    /// receive no changes are promoted empty.
    pub async fn begin_rebuild(
        &self,
        boards: &[Board],
    ) -> Result<LeaderboardRebuild<S, P>, LeaderboardError> {
        let mut rebuild = LeaderboardRebuild {
            projector: self.clone(),
            boards: BTreeMap::new(),
            report: ProjectionReport::default(),
        };
        for board in boards {
            rebuild.touch(board).await?;
        }
        Ok(rebuild)
    }

    /// Rebuild from a complete event history in one call: begin, apply every
    /// occurrence, commit.
    pub async fn rebuild<I>(
        &self,
        boards: &[Board],
        events: I,
    ) -> Result<ProjectionReport, LeaderboardError>
    where
        I: IntoIterator,
        I::Item: std::borrow::Borrow<DomainEventOccurrence>,
    {
        let mut rebuild = self.begin_rebuild(boards).await?;
        for event in events {
            rebuild
                .apply_event(std::borrow::Borrow::borrow(&event))
                .await?;
        }
        rebuild.commit().await
    }
}

/// An in-progress rebuild; feed it the event history (from any sync or async
/// source) and [`commit`](LeaderboardRebuild::commit).
pub struct LeaderboardRebuild<S: ?Sized, P: ?Sized> {
    projector: LeaderboardProjector<S, P>,
    boards: BTreeMap<String, Board>,
    report: ProjectionReport,
}

impl<S, P> LeaderboardRebuild<S, P>
where
    S: LeaderboardStore + ?Sized,
    P: LeaderboardProjection + ?Sized,
{
    async fn touch(&mut self, board: &Board) -> Result<(), LeaderboardError> {
        if !self.boards.contains_key(board.name()) {
            self.projector.store.reset(&board.staging()).await?;
            self.boards.insert(board.name().to_string(), board.live());
        }
        Ok(())
    }

    /// Apply one historical occurrence to the staging boards.
    pub async fn apply_event(
        &mut self,
        event: &DomainEventOccurrence,
    ) -> Result<(), LeaderboardError> {
        if !self.projector.projection.handles(&event.descriptor().name) {
            self.report.ignored += 1;
            return Ok(());
        }
        let changes = self.projector.projection.project(event)?;
        for change in &changes {
            self.touch(&change.board).await?;
        }
        let report = self.projector.apply_changes(event, changes, true).await?;
        self.report.merge(report);
        Ok(())
    }

    /// Boards that will be promoted.
    pub fn boards(&self) -> impl Iterator<Item = &Board> {
        self.boards.values()
    }

    /// Atomically promote every staging board over its live board.
    pub async fn commit(self) -> Result<ProjectionReport, LeaderboardError> {
        for board in self.boards.values() {
            self.projector.store.promote(board).await?;
            self.projector.notify(board);
        }
        Ok(self.report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::MessageKind;
    use crate::leaderboard::{InMemoryLeaderboard, LeaderboardErrorKind, ScorePolicy};
    use std::time::{Duration, UNIX_EPOCH};

    #[derive(serde::Serialize, serde::Deserialize, crate::DomainEvent)]
    #[domain_event(name = "test.monster_slain", version = 1)]
    struct MonsterSlain {
        player: String,
        xp: u32,
    }

    #[derive(serde::Serialize, serde::Deserialize, crate::DomainEvent)]
    #[domain_event(name = "test.chat_sent", version = 1)]
    struct ChatSent {
        text: String,
    }

    fn slain(position: u64, player: &str, xp: u32) -> DomainEventOccurrence {
        DomainEventOccurrence::capture_external(
            crate::ExternalEventSource {
                producer: "zone.sim".into(),
                stream: "zone-1".into(),
                position,
                key: "kill".into(),
            },
            UNIX_EPOCH + Duration::from_millis(position * 1_000),
            Default::default(),
            &MonsterSlain {
                player: player.into(),
                xp,
            },
        )
        .unwrap()
    }

    fn chat(position: u64) -> DomainEventOccurrence {
        DomainEventOccurrence::capture_external(
            crate::ExternalEventSource {
                producer: "zone.sim".into(),
                stream: "zone-1".into(),
                position,
                key: "chat".into(),
            },
            UNIX_EPOCH,
            Default::default(),
            &ChatSent { text: "hi".into() },
        )
        .unwrap()
    }

    fn message(event: &DomainEventOccurrence) -> Message {
        Message::new(
            event.descriptor().name.to_string(),
            MessageKind::Event,
            event.canonical_bytes().unwrap(),
        )
        .with_id(event.id())
    }

    struct XpProjection;

    impl LeaderboardProjection for XpProjection {
        fn handles(&self, name: &str) -> bool {
            name == "test.monster_slain"
        }

        fn project(
            &self,
            event: &DomainEventOccurrence,
        ) -> Result<Vec<LeaderboardChange>, LeaderboardError> {
            let body: MonsterSlain = event
                .decode_body()
                .map_err(|err| LeaderboardError::invalid(err.to_string()))?;
            Ok(vec![
                LeaderboardChange::new(
                    xp_board(),
                    ScoreSubmission::new(&body.player, f64::from(body.xp), ScorePolicy::Increment),
                ),
                LeaderboardChange::new(
                    best_board(),
                    ScoreSubmission::new(&body.player, f64::from(body.xp), ScorePolicy::Max),
                ),
            ])
        }
    }

    fn xp_board() -> Board {
        Board::high_first("xp").unwrap()
    }

    fn best_board() -> Board {
        Board::high_first("best-kill").unwrap()
    }

    fn projector() -> LeaderboardProjector<InMemoryLeaderboard, XpProjection> {
        LeaderboardProjector::new(Arc::new(InMemoryLeaderboard::new()), Arc::new(XpProjection))
    }

    #[tokio::test]
    async fn replayed_events_do_not_double_increment() {
        let projector = projector();
        let events = [
            slain(1, "ann", 10),
            slain(2, "bob", 30),
            slain(3, "ann", 25),
        ];
        for event in &events {
            let report = projector.apply_event(event).await.unwrap();
            assert_eq!(report.applied, 2);
        }
        for event in &events {
            let report = projector.apply_event(event).await.unwrap();
            assert_eq!(report.duplicates, 2);
            assert_eq!(report.applied, 0);
        }
        let store = projector.store();
        assert_eq!(store.score(&xp_board(), "ann").await.unwrap(), Some(35.0));
        assert_eq!(store.score(&best_board(), "ann").await.unwrap(), Some(25.0));
        assert_eq!(store.rank(&xp_board(), "ann").await.unwrap(), Some(0));
        assert_eq!(store.rank(&best_board(), "bob").await.unwrap(), Some(0));
    }

    #[tokio::test]
    async fn messages_decode_and_classify_errors() {
        let projector = projector();
        let first = message(&slain(1, "ann", 10));
        assert_eq!(projector.apply_message(&first).await.unwrap().applied, 2);
        assert_eq!(projector.apply_message(&first).await.unwrap().duplicates, 2);

        let ignored = projector.apply_message(&message(&chat(2))).await.unwrap();
        assert_eq!(ignored.ignored, 1);

        let garbage = Message::new("test.monster_slain", MessageKind::Event, b"nope".to_vec());
        assert!(projector
            .apply_message(&garbage)
            .await
            .unwrap_err()
            .is_permanent());

        assert!(LeaderboardError::unavailable("down")
            .into_transport_error()
            .is_retryable());
        assert_eq!(
            LeaderboardError::invalid("x").kind(),
            LeaderboardErrorKind::Invalid
        );
    }

    #[tokio::test]
    async fn rebuild_replaces_drifted_live_boards_atomically() {
        let projector = projector();
        let history = vec![
            slain(1, "ann", 10),
            slain(2, "bob", 30),
            slain(3, "ann", 25),
        ];
        for event in &history {
            projector.apply_event(event).await.unwrap();
        }
        let store = Arc::clone(projector.store());
        // Drift: a direct write and a stale board that the history never touches.
        store
            .submit(
                &xp_board(),
                &ScoreSubmission::new("cheater", 1e9, ScorePolicy::Replace),
            )
            .await
            .unwrap();
        let stale = Board::high_first("retired").unwrap();
        store
            .submit(
                &stale,
                &ScoreSubmission::new("x", 1.0, ScorePolicy::Replace),
            )
            .await
            .unwrap();

        // Boards are discovered from the history; `stale` is listed so it empties.
        let mut rebuild = projector
            .begin_rebuild(std::slice::from_ref(&stale))
            .await
            .unwrap();
        for event in &history[..2] {
            rebuild.apply_event(event).await.unwrap();
        }
        // Mid-rebuild, live reads still see the old boards.
        assert_eq!(
            store.score(&xp_board(), "cheater").await.unwrap(),
            Some(1e9)
        );
        rebuild.apply_event(&history[2]).await.unwrap();
        let names: Vec<_> = rebuild.boards().map(|b| b.name().to_string()).collect();
        assert_eq!(names, vec!["best-kill", "retired", "xp"]);
        let report = rebuild.commit().await.unwrap();
        assert_eq!(report.applied, 6);

        assert_eq!(store.score(&xp_board(), "cheater").await.unwrap(), None);
        assert_eq!(store.score(&xp_board(), "ann").await.unwrap(), Some(35.0));
        assert_eq!(store.len(&stale).await.unwrap(), 0);
        // Tail replay after commit is deduplicated by the promoted applied keys.
        let report = projector.apply_event(&history[2]).await.unwrap();
        assert_eq!(report.duplicates, 2);

        // The one-shot helper yields the same result.
        let report = projector.rebuild(&[], &history).await.unwrap();
        assert_eq!(report.applied, 6);
        assert_eq!(store.score(&xp_board(), "ann").await.unwrap(), Some(35.0));
    }

    #[tokio::test]
    async fn closures_are_projections() {
        let projection = |event: &DomainEventOccurrence| {
            Ok(vec![LeaderboardChange::new(
                Board::low_first("first-seen").unwrap(),
                ScoreSubmission::new("zone", event.occurred_at_unix_ms() as f64, ScorePolicy::Min),
            )])
        };
        let projector =
            LeaderboardProjector::new(Arc::new(InMemoryLeaderboard::new()), Arc::new(projection));
        projector.apply_event(&slain(5, "a", 1)).await.unwrap();
        projector.apply_event(&slain(2, "a", 1)).await.unwrap();
        let board = Board::low_first("first-seen").unwrap();
        assert_eq!(
            projector.store().score(&board, "zone").await.unwrap(),
            Some(2_000.0)
        );
    }

    #[tokio::test]
    async fn listeners_hear_applied_changes_and_promotions_but_not_duplicates() {
        let heard = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let log = heard.clone();
        let projector = projector().with_change_listener(Arc::new(move |board: &Board| {
            log.lock().unwrap().push(board.name().to_string());
        }));
        projector.apply_event(&slain(1, "ann", 10)).await.unwrap();
        projector.apply_event(&slain(1, "ann", 10)).await.unwrap();
        assert_eq!(
            *heard.lock().unwrap(),
            ["xp", "best-kill"],
            "one per applied change; the duplicate is silent"
        );
        let boards = [Board::high_first("xp").unwrap()];
        projector
            .rebuild(&boards, [slain(1, "ann", 10), slain(2, "bob", 5)])
            .await
            .unwrap();
        let heard = heard.lock().unwrap();
        assert_eq!(
            heard.len(),
            4,
            "one per promoted board, none for staging writes"
        );
        assert!(heard[2..].contains(&"xp".to_string()));
        assert!(heard[2..].contains(&"best-kill".to_string()));
    }
}
