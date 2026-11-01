//! Dependency-free [`LeaderboardStore`] for tests and demos.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

use super::{
    validate_member, ApplyKey, ApplyOutcome, Board, LeaderboardEntry, LeaderboardError,
    LeaderboardStore, ScoreOrder, ScorePolicy, ScoreSubmission,
};

#[derive(Debug, Default, Clone)]
struct BoardState {
    scores: HashMap<String, f64>,
    applied: HashMap<String, u64>,
}

impl BoardState {
    fn submit(&mut self, submission: &ScoreSubmission) -> Result<f64, LeaderboardError> {
        let existing = self.scores.get(&submission.member).copied();
        let score = submission.score;
        let next = match submission.policy {
            ScorePolicy::Increment => existing.unwrap_or(0.0) + score,
            ScorePolicy::Max => existing.map_or(score, |current| current.max(score)),
            ScorePolicy::Min => existing.map_or(score, |current| current.min(score)),
            ScorePolicy::Replace => score,
        };
        if !next.is_finite() {
            return Err(LeaderboardError::invalid("resulting score is not finite"));
        }
        self.scores.insert(submission.member.clone(), next);
        Ok(next)
    }

    /// Rows in rank order, matching Redis `ZRANGE`/`ZREVRANGE` tie-breaks.
    fn ordered(&self, order: ScoreOrder) -> Vec<(&String, f64)> {
        let mut rows: Vec<_> = self.scores.iter().map(|(m, s)| (m, *s)).collect();
        rows.sort_by(|a, b| {
            a.1.total_cmp(&b.1)
                .then_with(|| a.0.as_bytes().cmp(b.0.as_bytes()))
        });
        if order == ScoreOrder::HighFirst {
            rows.reverse();
        }
        rows
    }
}

fn entries(rows: &[(&String, f64)], start: usize, end: usize) -> Vec<LeaderboardEntry> {
    rows[start..end]
        .iter()
        .enumerate()
        .map(|(offset, (member, score))| LeaderboardEntry {
            rank: (start + offset) as u64,
            member: (*member).clone(),
            score: *score,
        })
        .collect()
}

/// An in-process [`LeaderboardStore`] with the same semantics as the Redis
/// implementation. Queries sort on each call, so it is meant for tests, demos
/// and small boards.
#[derive(Debug, Default)]
pub struct InMemoryLeaderboard {
    boards: Mutex<HashMap<(String, bool), BoardState>>,
}

impl InMemoryLeaderboard {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    fn with_board<R>(&self, board: &Board, f: impl FnOnce(&mut BoardState) -> R) -> R {
        let mut boards = self.boards.lock().unwrap_or_else(|e| e.into_inner());
        let state = boards
            .entry((board.name().to_string(), board.is_staging()))
            .or_default();
        f(state)
    }
}

#[async_trait]
impl LeaderboardStore for InMemoryLeaderboard {
    async fn submit(
        &self,
        board: &Board,
        submission: &ScoreSubmission,
    ) -> Result<f64, LeaderboardError> {
        submission.validate()?;
        self.with_board(board, |state| state.submit(submission))
    }

    async fn apply(
        &self,
        board: &Board,
        submission: &ScoreSubmission,
        key: &ApplyKey,
    ) -> Result<ApplyOutcome, LeaderboardError> {
        submission.validate()?;
        key.validate()?;
        self.with_board(board, |state| {
            if state.applied.contains_key(&key.id) {
                return Ok(ApplyOutcome::Duplicate {
                    score: state.scores.get(&submission.member).copied(),
                });
            }
            let score = state.submit(submission)?;
            state.applied.insert(key.id.clone(), key.at_unix_ms);
            Ok(ApplyOutcome::Applied { score })
        })
    }

    async fn score(&self, board: &Board, member: &str) -> Result<Option<f64>, LeaderboardError> {
        validate_member(member)?;
        Ok(self.with_board(board, |state| state.scores.get(member).copied()))
    }

    async fn rank(&self, board: &Board, member: &str) -> Result<Option<u64>, LeaderboardError> {
        validate_member(member)?;
        Ok(self.with_board(board, |state| {
            state
                .ordered(board.order())
                .iter()
                .position(|(m, _)| m.as_str() == member)
                .map(|rank| rank as u64)
        }))
    }

    async fn top(
        &self,
        board: &Board,
        n: usize,
    ) -> Result<Vec<LeaderboardEntry>, LeaderboardError> {
        Ok(self.with_board(board, |state| {
            let rows = state.ordered(board.order());
            entries(&rows, 0, n.min(rows.len()))
        }))
    }

    async fn around(
        &self,
        board: &Board,
        member: &str,
        radius: usize,
    ) -> Result<Vec<LeaderboardEntry>, LeaderboardError> {
        validate_member(member)?;
        Ok(self.with_board(board, |state| {
            let rows = state.ordered(board.order());
            let Some(rank) = rows.iter().position(|(m, _)| m.as_str() == member) else {
                return Vec::new();
            };
            let start = rank.saturating_sub(radius);
            let end = rank
                .saturating_add(radius)
                .saturating_add(1)
                .min(rows.len());
            entries(&rows, start, end)
        }))
    }

    async fn len(&self, board: &Board) -> Result<u64, LeaderboardError> {
        Ok(self.with_board(board, |state| state.scores.len() as u64))
    }

    async fn remove(&self, board: &Board, member: &str) -> Result<bool, LeaderboardError> {
        validate_member(member)?;
        Ok(self.with_board(board, |state| state.scores.remove(member).is_some()))
    }

    async fn reset(&self, board: &Board) -> Result<(), LeaderboardError> {
        let mut boards = self.boards.lock().unwrap_or_else(|e| e.into_inner());
        boards.remove(&(board.name().to_string(), board.is_staging()));
        Ok(())
    }

    async fn promote(&self, board: &Board) -> Result<(), LeaderboardError> {
        let mut boards = self.boards.lock().unwrap_or_else(|e| e.into_inner());
        let staged = boards
            .remove(&(board.name().to_string(), true))
            .unwrap_or_default();
        boards.insert((board.name().to_string(), false), staged);
        Ok(())
    }

    async fn prune_applied(
        &self,
        board: &Board,
        before_unix_ms: u64,
    ) -> Result<u64, LeaderboardError> {
        Ok(self.with_board(board, |state| {
            let before = state.applied.len();
            state.applied.retain(|_, at| *at >= before_unix_ms);
            (before - state.applied.len()) as u64
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn in_memory_store_satisfies_the_leaderboard_contract() {
        let store = InMemoryLeaderboard::new();
        crate::leaderboard::conformance::run_store_contract(&store, "contract").await;
    }

    #[tokio::test]
    async fn boards_are_isolated_by_name_and_variant() {
        let store = InMemoryLeaderboard::new();
        let a = Board::high_first("a").unwrap();
        let b = Board::high_first("b").unwrap();
        store
            .submit(&a, &ScoreSubmission::new("p", 1.0, ScorePolicy::Replace))
            .await
            .unwrap();
        assert_eq!(store.len(&b).await.unwrap(), 0);
        assert_eq!(store.len(&a.staging()).await.unwrap(), 0);
        assert_eq!(store.len(&a).await.unwrap(), 1);
    }
}
