//! Redis Streams [`MessagePublisher`].
//!
//! Each [`Message`] becomes one `XADD` entry with the fields `id`, `name`,
//! `kind`, `content_type`, `payload` (raw bytes) and one `meta:<key>` field per
//! metadata pair. By default the stream key is `<prefix>:{<message name>}`;
//! [`RedisStreamPublisher::with_stream`] sends everything to one stream.
//!
//! # Dedupe
//!
//! Redis Streams do not deduplicate natively, so a message with a stable id
//! is published by a Lua script that first `SET NX EX`s a marker key
//! `<stream key>:dedupe:<id>` and only `XADD`s when the marker was new — both
//! in one atomic script, with the marker sharing the stream's hash tag so it
//! works on Redis Cluster. A retried outbox row inside the dedupe window
//! therefore resolves `Ok` without a second entry. Messages without an id are
//! appended unconditionally.
//!
//! # Publish threshold
//!
//! `publish` resolves `Ok` once Redis replies to the script, i.e. the entry is
//! in the primary's memory. Durability beyond that depends on the server's
//! AOF/replication configuration; any error or timeout is retryable so the
//! outbox row stays pending.

use std::time::Duration;

use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::{Client, Script, Value};

use super::{Message, MessagePublisher, TransportError};

/// Default stream key prefix.
pub const DEFAULT_REDIS_STREAM_PREFIX: &str = "distributed:stream";
/// Default dedupe window.
pub const DEFAULT_REDIS_DEDUPE_TTL: Duration = Duration::from_secs(24 * 60 * 60);

const PUBLISH_SCRIPT: &str = r#"
local ttl = tonumber(ARGV[1])
local maxlen = tonumber(ARGV[2])
if KEYS[2] ~= nil then
  if not redis.call('SET', KEYS[2], '1', 'NX', 'EX', ttl) then
    return 0
  end
end
local args = {'XADD', KEYS[1]}
if maxlen > 0 then
  table.insert(args, 'MAXLEN')
  table.insert(args, '~')
  table.insert(args, maxlen)
end
table.insert(args, '*')
for i = 3, #ARGV do
  table.insert(args, ARGV[i])
end
redis.call(unpack(args))
return 1
"#;

/// Publishes canonical messages to Redis Streams with `XADD`.
#[derive(Clone)]
pub struct RedisStreamPublisher {
    conn: ConnectionManager,
    prefix: String,
    stream: Option<String>,
    dedupe_ttl: Duration,
    max_len: u64,
    script: Script,
}

fn retryable(context: &str, err: impl std::fmt::Display) -> TransportError {
    TransportError::retryable(format!("redis {context}: {err}"))
}

impl RedisStreamPublisher {
    /// Connect lazily to `url`; construction never contacts Redis.
    pub fn connect(url: &str) -> Result<Self, TransportError> {
        let client = Client::open(url)
            .map_err(|err| TransportError::permanent(format!("redis url: {err}")))?;
        let config = ConnectionManagerConfig::new()
            .set_connection_timeout(Some(Duration::from_secs(2)))
            .set_response_timeout(Some(Duration::from_secs(5)))
            .set_number_of_retries(1);
        let conn = client
            .get_connection_manager_lazy(config)
            .map_err(|err| retryable("connect", err))?;
        Ok(Self::new(conn))
    }

    /// Wrap an existing connection manager.
    pub fn new(conn: ConnectionManager) -> Self {
        Self {
            conn,
            prefix: DEFAULT_REDIS_STREAM_PREFIX.to_string(),
            stream: None,
            dedupe_ttl: DEFAULT_REDIS_DEDUPE_TTL,
            max_len: 0,
            script: Script::new(PUBLISH_SCRIPT),
        }
    }

    /// Key prefix for per-name streams (default [`DEFAULT_REDIS_STREAM_PREFIX`]).
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Publish every message to this exact stream key instead of one stream
    /// per message name.
    pub fn with_stream(mut self, stream: impl Into<String>) -> Self {
        self.stream = Some(stream.into());
        self
    }

    /// How long a published id suppresses re-publication (minimum 1s).
    pub fn with_dedupe_ttl(mut self, ttl: Duration) -> Self {
        self.dedupe_ttl = ttl.max(Duration::from_secs(1));
        self
    }

    /// Approximate `MAXLEN ~` trim applied on every `XADD`; 0 disables.
    pub fn with_max_len(mut self, max_len: u64) -> Self {
        self.max_len = max_len;
        self
    }

    /// The stream key a message is published to.
    pub fn stream_key(&self, message: &Message) -> String {
        match &self.stream {
            Some(stream) => stream.clone(),
            None => format!("{}:{{{}}}", self.prefix, message.name()),
        }
    }

    /// The dedupe marker key for `id` on `stream`.
    pub fn dedupe_key(stream: &str, id: &str) -> String {
        format!("{stream}:dedupe:{id}")
    }

    /// Publish and report whether a new entry was appended (`false` means the
    /// id was already published within the dedupe window).
    pub async fn publish_entry(&self, message: &Message) -> Result<bool, TransportError> {
        message
            .validate_name()
            .map_err(|err| TransportError::permanent(err.to_string()))?;
        let stream = self.stream_key(message);
        let mut invocation = self.script.prepare_invoke();
        invocation.key(&stream);
        if let Some(id) = message.id() {
            invocation.key(Self::dedupe_key(&stream, id));
        }
        invocation
            .arg(self.dedupe_ttl.as_secs())
            .arg(self.max_len)
            .arg("id")
            .arg(message.id().unwrap_or(""))
            .arg("name")
            .arg(message.name())
            .arg("kind")
            .arg(message.kind.as_str())
            .arg("content_type")
            .arg(&message.content_type)
            .arg("payload")
            .arg(message.payload());
        for (key, value) in &message.metadata {
            invocation.arg(format!("meta:{key}")).arg(value);
        }
        let mut conn = self.conn.clone();
        let reply: Value = invocation
            .invoke_async(&mut conn)
            .await
            .map_err(|err| retryable("XADD", err))?;
        Ok(matches!(reply, Value::Int(1)))
    }
}

impl MessagePublisher for RedisStreamPublisher {
    async fn publish(&self, message: Message) -> Result<(), TransportError> {
        self.publish_entry(&message).await.map(|_| ())
    }
}
