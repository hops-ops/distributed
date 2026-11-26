//! [`LeaderboardRoots`]: runs a [`LeaderboardSurface`]'s GraphQL roots
//! against a [`LeaderboardStore`].

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use distributed::graphql::{
    ExternalReadError, ExternalReadExecutor, ExternalReadResult, ExternalReadSource,
};
use distributed::microsvc::Session;
use serde_json::{json, Value};

use crate::{
    leaderboard_change_key, LeaderboardEntry, LeaderboardGrant, LeaderboardOperation,
    LeaderboardStore, LeaderboardSurface, ALL_BOARDS_CHANGE_KEY,
};

/// Default `radius` of an around-me query (capped by the surface).
const DEFAULT_RADIUS: u32 = 5;

/// A leaderboard surface served from a store: register it with
/// `GraphqlEngineBuilder::external_read_source(Arc::new(LeaderboardRoots::new(surface, store)))`.
///
/// The viewer-relative roots (`…LeaderboardAround`, `…MyRank`) read the
/// member from the session claim [`LeaderboardSurface::member_claim`], never
/// from an argument.
pub struct LeaderboardRoots {
    surface: LeaderboardSurface,
    store: Arc<dyn LeaderboardStore>,
}

impl LeaderboardRoots {
    pub fn new(surface: LeaderboardSurface, store: Arc<dyn LeaderboardStore>) -> Self {
        Self { surface, store }
    }
}

fn string_arg(
    arguments: &BTreeMap<String, Value>,
    name: &str,
) -> Result<Option<String>, ExternalReadError> {
    match arguments.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(ExternalReadError::bad_request(format!(
            "`{name}` must be a string"
        ))),
    }
}

fn bounded_arg(
    arguments: &BTreeMap<String, Value>,
    name: &str,
    default: u32,
    min: u32,
    max: u32,
) -> Result<u32, ExternalReadError> {
    let value = match arguments.get(name) {
        None | Some(Value::Null) => return Ok(default),
        Some(value) => value.as_i64().ok_or_else(|| {
            ExternalReadError::bad_request(format!("`{name}` must be an integer"))
        })?,
    };
    if value < i64::from(min) || value > i64::from(max) {
        return Err(ExternalReadError::bad_request(format!(
            "`{name}` must be within {min}..={max}"
        )));
    }
    Ok(value as u32)
}

fn entry_value(entry: &LeaderboardEntry) -> Value {
    json!({ "rank": entry.rank, "member": entry.member, "score": entry.score })
}

#[async_trait]
impl ExternalReadExecutor for LeaderboardRoots {
    fn source(&self) -> Arc<dyn ExternalReadSource> {
        Arc::new(self.surface.clone())
    }

    async fn execute(
        &self,
        root: &str,
        arguments: &BTreeMap<String, Value>,
        session: &Session,
    ) -> Result<ExternalReadResult, ExternalReadError> {
        let surface = &self.surface;
        let all = LeaderboardGrant::read().rank_any_member();
        let operation = surface
            .roots_for(all)
            .into_iter()
            .find(|(_, candidate)| candidate.name == root)
            .map(|(operation, _)| operation)
            .ok_or_else(|| ExternalReadError::internal("unknown leaderboard root"))?;
        let board = surface
            .board(string_arg(arguments, "board")?.as_deref())
            .map_err(ExternalReadError::bad_request)?;
        let member = match operation {
            LeaderboardOperation::AroundMe | LeaderboardOperation::MyRank => session
                .get(surface.get_member_claim())
                .filter(|member| !member.is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    ExternalReadError::forbidden("the session has no leaderboard member claim")
                })?,
            LeaderboardOperation::Rank => string_arg(arguments, "member")?
                .filter(|member| !member.is_empty())
                .ok_or_else(|| ExternalReadError::bad_request("member is required"))?,
            LeaderboardOperation::Top => String::new(),
        };
        let count = match operation {
            LeaderboardOperation::Top => bounded_arg(
                arguments,
                "first",
                surface.default_first(),
                1,
                surface.max_first(),
            )?,
            LeaderboardOperation::AroundMe => bounded_arg(
                arguments,
                "radius",
                DEFAULT_RADIUS.min(surface.max_radius()),
                0,
                surface.max_radius(),
            )?,
            _ => 0,
        } as usize;
        let unavailable = |_| ExternalReadError::internal("leaderboard store unavailable");
        let value = match operation {
            LeaderboardOperation::Top => Value::Array(
                self.store
                    .top(&board, count)
                    .await
                    .map_err(unavailable)?
                    .iter()
                    .map(entry_value)
                    .collect(),
            ),
            LeaderboardOperation::AroundMe => Value::Array(
                self.store
                    .around(&board, &member, count)
                    .await
                    .map_err(unavailable)?
                    .iter()
                    .map(entry_value)
                    .collect(),
            ),
            LeaderboardOperation::MyRank | LeaderboardOperation::Rank => {
                let rank = self
                    .store
                    .rank(&board, &member)
                    .await
                    .map_err(unavailable)?;
                let score = self
                    .store
                    .score(&board, &member)
                    .await
                    .map_err(unavailable)?;
                match (rank, score) {
                    (Some(rank), Some(score)) => entry_value(&LeaderboardEntry {
                        rank,
                        member: member.clone(),
                        score,
                    }),
                    _ => Value::Null,
                }
            }
        };
        Ok(ExternalReadResult {
            value,
            scope: json!({
                "board": board.name(),
                "member": (!member.is_empty()).then_some(&member),
                "count": count,
            }),
            change_keys: vec![
                leaderboard_change_key(board.name()),
                ALL_BOARDS_CHANGE_KEY.to_string(),
            ],
        })
    }
}
