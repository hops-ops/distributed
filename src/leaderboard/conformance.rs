//! Behavioural contract every [`LeaderboardStore`] must satisfy.
//!
//! Run by the in-memory unit tests and by the Redis integration test, so both
//! implementations are held to identical ordering, policy, idempotency and
//! rebuild semantics. Panics on the first violation.

use super::{
    ApplyKey, ApplyOutcome, Board, LeaderboardErrorKind, LeaderboardStore, ScorePolicy,
    ScoreSubmission,
};

fn sub(member: &str, score: f64, policy: ScorePolicy) -> ScoreSubmission {
    ScoreSubmission::new(member, score, policy)
}

fn rows(entries: &[super::LeaderboardEntry]) -> Vec<(u64, &str, f64)> {
    entries
        .iter()
        .map(|e| (e.rank, e.member.as_str(), e.score))
        .collect()
}

/// Run the whole contract using boards whose names start with `prefix`.
/// Boards are reset first, so a shared Redis is fine with a unique prefix.
pub async fn run_store_contract<S: LeaderboardStore + ?Sized>(store: &S, prefix: &str) {
    policies(store, prefix).await;
    ordering_and_queries(store, prefix).await;
    idempotent_apply(store, prefix).await;
    rebuild_and_promote(store, prefix).await;
    validation(store, prefix).await;
}

async fn fresh<S: LeaderboardStore + ?Sized>(store: &S, board: Board) -> Board {
    store.reset(&board).await.unwrap();
    store.reset(&board.staging()).await.unwrap();
    board
}

async fn policies<S: LeaderboardStore + ?Sized>(store: &S, prefix: &str) {
    let board = fresh(
        store,
        Board::high_first(format!("{prefix}-policies")).unwrap(),
    )
    .await;
    use ScorePolicy::*;
    assert_eq!(
        store.submit(&board, &sub("max", 10.0, Max)).await.unwrap(),
        10.0
    );
    assert_eq!(
        store.submit(&board, &sub("max", 5.0, Max)).await.unwrap(),
        10.0
    );
    assert_eq!(
        store.submit(&board, &sub("max", 12.5, Max)).await.unwrap(),
        12.5
    );
    assert_eq!(
        store.submit(&board, &sub("min", 10.0, Min)).await.unwrap(),
        10.0
    );
    assert_eq!(
        store.submit(&board, &sub("min", 15.0, Min)).await.unwrap(),
        10.0
    );
    assert_eq!(
        store.submit(&board, &sub("min", -3.0, Min)).await.unwrap(),
        -3.0
    );
    assert_eq!(
        store
            .submit(&board, &sub("rep", 10.0, Replace))
            .await
            .unwrap(),
        10.0
    );
    assert_eq!(
        store
            .submit(&board, &sub("rep", 1.0, Replace))
            .await
            .unwrap(),
        1.0
    );
    assert_eq!(
        store
            .submit(&board, &sub("inc", 2.5, Increment))
            .await
            .unwrap(),
        2.5
    );
    assert_eq!(
        store
            .submit(&board, &sub("inc", 4.0, Increment))
            .await
            .unwrap(),
        6.5
    );
    assert_eq!(
        store
            .submit(&board, &sub("inc", -1.5, Increment))
            .await
            .unwrap(),
        5.0
    );
    assert_eq!(store.score(&board, "max").await.unwrap(), Some(12.5));
    assert_eq!(store.score(&board, "missing").await.unwrap(), None);
    assert_eq!(store.len(&board).await.unwrap(), 4);
    assert!(store.remove(&board, "rep").await.unwrap());
    assert!(!store.remove(&board, "rep").await.unwrap());
    assert_eq!(store.len(&board).await.unwrap(), 3);
}

async fn ordering_and_queries<S: LeaderboardStore + ?Sized>(store: &S, prefix: &str) {
    let high = fresh(store, Board::high_first(format!("{prefix}-high")).unwrap()).await;
    let low = fresh(store, Board::low_first(format!("{prefix}-low")).unwrap()).await;
    for (member, score) in [
        ("a", 30.0),
        ("b", 10.0),
        ("c", 20.0),
        ("d", 20.0),
        ("e", 5.0),
    ] {
        for board in [&high, &low] {
            store
                .submit(board, &sub(member, score, ScorePolicy::Replace))
                .await
                .unwrap();
        }
    }
    // Ties break by member bytes: descending on HighFirst, ascending on LowFirst.
    assert_eq!(
        rows(&store.top(&high, 10).await.unwrap()),
        vec![
            (0, "a", 30.0),
            (1, "d", 20.0),
            (2, "c", 20.0),
            (3, "b", 10.0),
            (4, "e", 5.0)
        ]
    );
    assert_eq!(
        rows(&store.top(&low, 2).await.unwrap()),
        vec![(0, "e", 5.0), (1, "b", 10.0)]
    );
    assert!(store.top(&high, 0).await.unwrap().is_empty());
    assert_eq!(store.rank(&high, "a").await.unwrap(), Some(0));
    assert_eq!(store.rank(&high, "c").await.unwrap(), Some(2));
    assert_eq!(store.rank(&low, "a").await.unwrap(), Some(4));
    assert_eq!(store.rank(&low, "zz").await.unwrap(), None);
    assert_eq!(
        rows(&store.around(&high, "c", 1).await.unwrap()),
        vec![(1, "d", 20.0), (2, "c", 20.0), (3, "b", 10.0)]
    );
    assert_eq!(
        rows(&store.around(&high, "a", 2).await.unwrap()),
        vec![(0, "a", 30.0), (1, "d", 20.0), (2, "c", 20.0)]
    );
    assert_eq!(
        rows(&store.around(&low, "a", 1).await.unwrap()),
        vec![(3, "d", 20.0), (4, "a", 30.0)]
    );
    assert_eq!(
        rows(&store.around(&low, "b", 0).await.unwrap()),
        vec![(1, "b", 10.0)]
    );
    assert!(store.around(&high, "zz", 3).await.unwrap().is_empty());
}

async fn idempotent_apply<S: LeaderboardStore + ?Sized>(store: &S, prefix: &str) {
    let board = fresh(store, Board::high_first(format!("{prefix}-apply")).unwrap()).await;
    let inc = sub("p", 5.0, ScorePolicy::Increment);
    let k1 = ApplyKey::new("evt-1#0", 1_000);
    let k2 = ApplyKey::new("evt-2#0", 2_000);
    assert_eq!(
        store.apply(&board, &inc, &k1).await.unwrap(),
        ApplyOutcome::Applied { score: 5.0 }
    );
    assert_eq!(
        store.apply(&board, &inc, &k1).await.unwrap(),
        ApplyOutcome::Duplicate { score: Some(5.0) }
    );
    assert_eq!(
        store.apply(&board, &inc, &k2).await.unwrap(),
        ApplyOutcome::Applied { score: 10.0 }
    );
    // Duplicate detection is per board: the same key applies to another board.
    let other = fresh(
        store,
        Board::high_first(format!("{prefix}-apply-other")).unwrap(),
    )
    .await;
    assert!(!store.apply(&other, &inc, &k1).await.unwrap().is_duplicate());
    // Pruning forgets old keys only.
    assert_eq!(store.prune_applied(&board, 1_500).await.unwrap(), 1);
    assert!(store.apply(&board, &inc, &k2).await.unwrap().is_duplicate());
    assert_eq!(
        store.apply(&board, &inc, &k1).await.unwrap(),
        ApplyOutcome::Applied { score: 15.0 }
    );
    // A non-improving Max still records its key.
    let max = sub("p", 1.0, ScorePolicy::Max);
    let k3 = ApplyKey::new("evt-3#0", 3_000);
    assert_eq!(
        store.apply(&board, &max, &k3).await.unwrap(),
        ApplyOutcome::Applied { score: 15.0 }
    );
    assert!(store.apply(&board, &max, &k3).await.unwrap().is_duplicate());
    // Reset clears scores and applied keys.
    store.reset(&board).await.unwrap();
    assert_eq!(store.len(&board).await.unwrap(), 0);
    assert_eq!(
        store.apply(&board, &inc, &k1).await.unwrap(),
        ApplyOutcome::Applied { score: 5.0 }
    );
}

async fn rebuild_and_promote<S: LeaderboardStore + ?Sized>(store: &S, prefix: &str) {
    let board = fresh(
        store,
        Board::high_first(format!("{prefix}-rebuild")).unwrap(),
    )
    .await;
    let staging = board.staging();
    store
        .submit(&board, &sub("old", 99.0, ScorePolicy::Replace))
        .await
        .unwrap();
    let key = ApplyKey::new("evt-r#0", 10);
    store
        .apply(&staging, &sub("new", 7.0, ScorePolicy::Increment), &key)
        .await
        .unwrap();
    // Staging is invisible to live reads until promoted.
    assert_eq!(store.score(&board, "new").await.unwrap(), None);
    store.promote(&board).await.unwrap();
    assert_eq!(store.score(&board, "old").await.unwrap(), None);
    assert_eq!(store.score(&board, "new").await.unwrap(), Some(7.0));
    assert_eq!(store.len(&staging).await.unwrap(), 0);
    // The applied keys moved with the scores.
    assert!(store
        .apply(&board, &sub("new", 7.0, ScorePolicy::Increment), &key)
        .await
        .unwrap()
        .is_duplicate());
    // Promoting an empty staging board empties live.
    store.promote(&board).await.unwrap();
    assert_eq!(store.len(&board).await.unwrap(), 0);
}

async fn validation<S: LeaderboardStore + ?Sized>(store: &S, prefix: &str) {
    let board = fresh(
        store,
        Board::high_first(format!("{prefix}-invalid")).unwrap(),
    )
    .await;
    for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let err = store
            .submit(&board, &sub("p", bad, ScorePolicy::Replace))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), LeaderboardErrorKind::Invalid);
    }
    let err = store
        .submit(&board, &sub("", 1.0, ScorePolicy::Replace))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), LeaderboardErrorKind::Invalid);
    assert!(Board::high_first("").is_err());
    assert!(Board::high_first("a{b}").is_err());
    assert_eq!(store.len(&board).await.unwrap(), 0);
}
