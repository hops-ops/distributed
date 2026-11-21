//! Redis sorted-set [`LeaderboardStore`].
//!
//! # Key layout
//!
//! Each board variant owns two keys sharing the `{<board name>}` hash tag, so
//! every key a script touches lives in one Redis Cluster slot:
//!
//! - `<prefix>:{<board>}:<live|staging>:scores` — `ZSET member → score`;
//! - `<prefix>:{<board>}:<live|staging>:applied` — `ZSET apply-key → event ms`.
//!
//! # Atomicity
//!
//! [`apply`](LeaderboardStore::apply) runs one Lua script that checks the
//! applied set, changes the score (`ZADD GT|LT`, `ZADD` or `ZINCRBY`) and
//! records the key; Redis executes scripts atomically, so concurrent or
//! redelivered applies cannot double-count. [`around`](LeaderboardStore::around)
//! reads rank and range in one script, and [`promote`](LeaderboardStore::promote)
//! renames staging over live in one script. `ZADD GT/LT` requires Redis 6.2+.
//!
//! Scores cross the Lua boundary as strings (Lua numbers would truncate to
//! integers in replies), using Redis' round-trip-exact `%.17g` formatting.

use std::time::Duration;

use async_trait::async_trait;
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::{Client, Script, Value};

use super::{
    validate_member, ApplyKey, ApplyOutcome, Board, LeaderboardEntry, LeaderboardError,
    LeaderboardStore, ScoreOrder, ScoreSubmission,
};

/// Default key prefix.
pub const DEFAULT_REDIS_LEADERBOARD_PREFIX: &str = "distributed:lb";

const APPLY_SCRIPT: &str = r#"
local id = ARGV[4]
if id ~= '' and redis.call('ZSCORE', KEYS[2], id) then
  return {0, redis.call('ZSCORE', KEYS[1], ARGV[1])}
end
local policy = ARGV[3]
local score
if policy == 'increment' then
  score = redis.call('ZINCRBY', KEYS[1], ARGV[2], ARGV[1])
else
  if policy == 'max' then
    redis.call('ZADD', KEYS[1], 'GT', ARGV[2], ARGV[1])
  elseif policy == 'min' then
    redis.call('ZADD', KEYS[1], 'LT', ARGV[2], ARGV[1])
  else
    redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
  end
  score = redis.call('ZSCORE', KEYS[1], ARGV[1])
end
if id ~= '' then
  redis.call('ZADD', KEYS[2], ARGV[5], id)
end
return {1, score}
"#;

const AROUND_SCRIPT: &str = r#"
local high = ARGV[2] == 'high'
local rank
if high then
  rank = redis.call('ZREVRANK', KEYS[1], ARGV[1])
else
  rank = redis.call('ZRANK', KEYS[1], ARGV[1])
end
if not rank then
  return {}
end
local radius = tonumber(ARGV[3])
local start = rank - radius
if start < 0 then start = 0 end
local stop = rank + radius
local rows
if high then
  rows = redis.call('ZREVRANGE', KEYS[1], start, stop, 'WITHSCORES')
else
  rows = redis.call('ZRANGE', KEYS[1], start, stop, 'WITHSCORES')
end
table.insert(rows, 1, start)
return rows
"#;

const PROMOTE_SCRIPT: &str = r#"
redis.call('DEL', KEYS[3], KEYS[4])
if redis.call('EXISTS', KEYS[1]) == 1 then
  redis.call('RENAME', KEYS[1], KEYS[3])
end
if redis.call('EXISTS', KEYS[2]) == 1 then
  redis.call('RENAME', KEYS[2], KEYS[4])
end
return 1
"#;

/// A [`LeaderboardStore`] on Redis sorted sets.
///
/// Uses a [`ConnectionManager`], which multiplexes requests and reconnects in
/// the background; [`connect`](RedisLeaderboard::connect) configures short
/// connection/response timeouts so callers fail fast (as
/// [`LeaderboardErrorKind::Unavailable`](super::LeaderboardErrorKind::Unavailable))
/// instead of hanging while Redis is down.
#[derive(Clone)]
pub struct RedisLeaderboard {
    conn: ConnectionManager,
    prefix: String,
    apply: Script,
    around: Script,
    promote: Script,
}

pub(crate) fn unavailable(context: &str, err: redis::RedisError) -> LeaderboardError {
    LeaderboardError::unavailable(format!("redis {context}: {err}"))
}

impl RedisLeaderboard {
    /// Default connection and response timeout used by [`connect`](Self::connect).
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

    /// Connect lazily to `url` (e.g. `redis://127.0.0.1:6379`). Construction
    /// does not contact Redis, so service start-up never waits on it.
    pub fn connect(url: &str) -> Result<Self, LeaderboardError> {
        let client = Client::open(url)
            .map_err(|err| LeaderboardError::invalid(format!("redis url: {err}")))?;
        let config = ConnectionManagerConfig::new()
            .set_connection_timeout(Some(Self::DEFAULT_TIMEOUT))
            .set_response_timeout(Some(Self::DEFAULT_TIMEOUT))
            .set_number_of_retries(1);
        let conn = client
            .get_connection_manager_lazy(config)
            .map_err(|err| unavailable("connect", err))?;
        Ok(Self::new(conn))
    }

    /// Wrap an existing connection manager.
    pub fn new(conn: ConnectionManager) -> Self {
        Self {
            conn,
            prefix: DEFAULT_REDIS_LEADERBOARD_PREFIX.to_string(),
            apply: Script::new(APPLY_SCRIPT),
            around: Script::new(AROUND_SCRIPT),
            promote: Script::new(PROMOTE_SCRIPT),
        }
    }

    /// Use a different key prefix (e.g. per environment or per test).
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    fn key(&self, board: &Board, staging: bool, part: &str) -> String {
        let variant = if staging { "staging" } else { "live" };
        format!("{}:{{{}}}:{variant}:{part}", self.prefix, board.name())
    }

    fn scores(&self, board: &Board) -> String {
        self.key(board, board.is_staging(), "scores")
    }

    fn applied(&self, board: &Board) -> String {
        self.key(board, board.is_staging(), "applied")
    }

    async fn run_apply(
        &self,
        board: &Board,
        submission: &ScoreSubmission,
        key: Option<&ApplyKey>,
    ) -> Result<(bool, Option<f64>), LeaderboardError> {
        submission.validate()?;
        if let Some(key) = key {
            key.validate()?;
        }
        let mut conn = self.conn.clone();
        let reply: Value = self
            .apply
            .key(self.scores(board))
            .key(self.applied(board))
            .arg(&submission.member)
            .arg(format_score(submission.score))
            .arg(submission.policy.as_str())
            .arg(key.map_or("", |k| k.id.as_str()))
            .arg(key.map_or(0, |k| k.at_unix_ms))
            .invoke_async(&mut conn)
            .await
            .map_err(|err| map_write_error("apply", err))?;
        let Value::Array(items) = reply else {
            return Err(LeaderboardError::unavailable(
                "redis apply: unexpected reply",
            ));
        };
        let applied = matches!(items.first(), Some(Value::Int(1)));
        let score = match items.get(1) {
            Some(value) => parse_optional_score(value)?,
            None => None,
        };
        Ok((applied, score))
    }

    fn parse_rows(
        &self,
        values: &[Value],
        first_rank: u64,
    ) -> Result<Vec<LeaderboardEntry>, LeaderboardError> {
        values
            .chunks(2)
            .enumerate()
            .map(|(offset, pair)| {
                let [member, score] = pair else {
                    return Err(LeaderboardError::unavailable("redis: odd WITHSCORES reply"));
                };
                Ok(LeaderboardEntry {
                    rank: first_rank + offset as u64,
                    member: parse_string(member)?,
                    score: parse_optional_score(score)?
                        .ok_or_else(|| LeaderboardError::unavailable("redis: missing score"))?,
                })
            })
            .collect()
    }
}

/// A write that Redis rejects as an error reply (e.g. an increment overflowing
/// to infinity, or a server too old for `ZADD GT`) is not transient.
fn map_write_error(context: &str, err: redis::RedisError) -> LeaderboardError {
    if err.kind() == redis::ErrorKind::Server(redis::ServerErrorKind::ResponseError) {
        LeaderboardError::invalid(format!("redis {context}: {err}"))
    } else {
        unavailable(context, err)
    }
}

fn format_score(score: f64) -> String {
    // Rust's shortest round-trip representation; Redis parses it with strtod.
    format!("{score:?}")
}

fn parse_string(value: &Value) -> Result<String, LeaderboardError> {
    match value {
        Value::BulkString(bytes) => String::from_utf8(bytes.clone())
            .map_err(|_| LeaderboardError::unavailable("redis: non-utf8 member")),
        Value::SimpleString(s) => Ok(s.clone()),
        _ => Err(LeaderboardError::unavailable(
            "redis: unexpected member reply",
        )),
    }
}

fn parse_optional_score(value: &Value) -> Result<Option<f64>, LeaderboardError> {
    match value {
        Value::Nil => Ok(None),
        Value::Boolean(false) => Ok(None),
        Value::Double(score) => Ok(Some(*score)),
        Value::Int(score) => Ok(Some(*score as f64)),
        other => {
            let text = parse_string(other)?;
            text.parse::<f64>()
                .map(Some)
                .map_err(|_| LeaderboardError::unavailable(format!("redis: bad score {text:?}")))
        }
    }
}

fn parse_rank(value: &Value) -> Result<u64, LeaderboardError> {
    match value {
        Value::Int(rank) if *rank >= 0 => Ok(*rank as u64),
        _ => Err(LeaderboardError::unavailable(
            "redis: unexpected rank reply",
        )),
    }
}

#[async_trait]
impl LeaderboardStore for RedisLeaderboard {
    async fn submit(
        &self,
        board: &Board,
        submission: &ScoreSubmission,
    ) -> Result<f64, LeaderboardError> {
        let (_, score) = self.run_apply(board, submission, None).await?;
        score.ok_or_else(|| LeaderboardError::unavailable("redis submit: missing score"))
    }

    async fn apply(
        &self,
        board: &Board,
        submission: &ScoreSubmission,
        key: &ApplyKey,
    ) -> Result<ApplyOutcome, LeaderboardError> {
        match self.run_apply(board, submission, Some(key)).await? {
            (true, Some(score)) => Ok(ApplyOutcome::Applied { score }),
            (true, None) => Err(LeaderboardError::unavailable("redis apply: missing score")),
            (false, score) => Ok(ApplyOutcome::Duplicate { score }),
        }
    }

    async fn score(&self, board: &Board, member: &str) -> Result<Option<f64>, LeaderboardError> {
        validate_member(member)?;
        let mut conn = self.conn.clone();
        let value: Value = redis::cmd("ZSCORE")
            .arg(self.scores(board))
            .arg(member)
            .query_async(&mut conn)
            .await
            .map_err(|err| unavailable("ZSCORE", err))?;
        parse_optional_score(&value)
    }

    async fn rank(&self, board: &Board, member: &str) -> Result<Option<u64>, LeaderboardError> {
        validate_member(member)?;
        let command = match board.order() {
            ScoreOrder::HighFirst => "ZREVRANK",
            ScoreOrder::LowFirst => "ZRANK",
        };
        let mut conn = self.conn.clone();
        let value: Value = redis::cmd(command)
            .arg(self.scores(board))
            .arg(member)
            .query_async(&mut conn)
            .await
            .map_err(|err| unavailable(command, err))?;
        match value {
            Value::Nil => Ok(None),
            other => parse_rank(&other).map(Some),
        }
    }

    async fn top(
        &self,
        board: &Board,
        n: usize,
    ) -> Result<Vec<LeaderboardEntry>, LeaderboardError> {
        if n == 0 {
            return Ok(Vec::new());
        }
        let command = match board.order() {
            ScoreOrder::HighFirst => "ZREVRANGE",
            ScoreOrder::LowFirst => "ZRANGE",
        };
        let stop = i64::try_from(n - 1).unwrap_or(i64::MAX);
        let mut conn = self.conn.clone();
        let value: Value = redis::cmd(command)
            .arg(self.scores(board))
            .arg(0)
            .arg(stop)
            .arg("WITHSCORES")
            .query_async(&mut conn)
            .await
            .map_err(|err| unavailable(command, err))?;
        match value {
            Value::Array(values) => self.parse_rows(&flatten_pairs(values), 0),
            _ => Err(LeaderboardError::unavailable(
                "redis: unexpected range reply",
            )),
        }
    }

    async fn around(
        &self,
        board: &Board,
        member: &str,
        radius: usize,
    ) -> Result<Vec<LeaderboardEntry>, LeaderboardError> {
        validate_member(member)?;
        let order = match board.order() {
            ScoreOrder::HighFirst => "high",
            ScoreOrder::LowFirst => "low",
        };
        // Keep the Lua arithmetic inside exact double range.
        let radius = radius.min(1 << 40);
        let mut conn = self.conn.clone();
        let value: Value = self
            .around
            .key(self.scores(board))
            .arg(member)
            .arg(order)
            .arg(radius)
            .invoke_async(&mut conn)
            .await
            .map_err(|err| unavailable("around", err))?;
        let Value::Array(values) = value else {
            return Err(LeaderboardError::unavailable(
                "redis: unexpected around reply",
            ));
        };
        let Some((first, rows)) = values.split_first() else {
            return Ok(Vec::new());
        };
        self.parse_rows(rows, parse_rank(first)?)
    }

    async fn len(&self, board: &Board) -> Result<u64, LeaderboardError> {
        let mut conn = self.conn.clone();
        let value: Value = redis::cmd("ZCARD")
            .arg(self.scores(board))
            .query_async(&mut conn)
            .await
            .map_err(|err| unavailable("ZCARD", err))?;
        parse_rank(&value)
    }

    async fn remove(&self, board: &Board, member: &str) -> Result<bool, LeaderboardError> {
        validate_member(member)?;
        let mut conn = self.conn.clone();
        let removed: i64 = redis::cmd("ZREM")
            .arg(self.scores(board))
            .arg(member)
            .query_async(&mut conn)
            .await
            .map_err(|err| unavailable("ZREM", err))?;
        Ok(removed > 0)
    }

    async fn reset(&self, board: &Board) -> Result<(), LeaderboardError> {
        let mut conn = self.conn.clone();
        let _: i64 = redis::cmd("DEL")
            .arg(self.scores(board))
            .arg(self.applied(board))
            .query_async(&mut conn)
            .await
            .map_err(|err| unavailable("DEL", err))?;
        Ok(())
    }

    async fn promote(&self, board: &Board) -> Result<(), LeaderboardError> {
        let mut conn = self.conn.clone();
        let _: i64 = self
            .promote
            .key(self.key(board, true, "scores"))
            .key(self.key(board, true, "applied"))
            .key(self.key(board, false, "scores"))
            .key(self.key(board, false, "applied"))
            .invoke_async(&mut conn)
            .await
            .map_err(|err| unavailable("promote", err))?;
        Ok(())
    }

    async fn prune_applied(
        &self,
        board: &Board,
        before_unix_ms: u64,
    ) -> Result<u64, LeaderboardError> {
        let mut conn = self.conn.clone();
        let removed: i64 = redis::cmd("ZREMRANGEBYSCORE")
            .arg(self.applied(board))
            .arg("-inf")
            .arg(format!("({before_unix_ms}"))
            .query_async(&mut conn)
            .await
            .map_err(|err| unavailable("ZREMRANGEBYSCORE", err))?;
        Ok(removed.max(0) as u64)
    }
}

/// RESP3 servers reply to `WITHSCORES` with `[[member, score], ...]`; RESP2
/// with a flat list. Normalize to flat.
fn flatten_pairs(values: Vec<Value>) -> Vec<Value> {
    if values.iter().all(|v| matches!(v, Value::Array(_))) {
        values
            .into_iter()
            .flat_map(|v| match v {
                Value::Array(pair) => pair,
                other => vec![other],
            })
            .collect()
    } else {
        values
    }
}
