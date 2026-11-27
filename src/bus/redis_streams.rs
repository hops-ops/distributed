//! Redis Streams [`MessagePublisher`] and [`RedisStreamBus`].
//!
//! Each [`Message`] becomes one `XADD` entry laid out as a [`WireEnvelope`]:
//! the fields `name` and `payload` (raw bytes), then one `header:<key>` field
//! per envelope header (`header:x-distributed-id`, `header:x-distributed-kind`,
//! `header:x-distributed-content-type`, then the metadata in order). By default the stream key is `<prefix>:{<message name>}`;
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
//!
//! # The bus
//!
//! [`RedisStreamBus`] sends every command to the stream `<prefix>:{<ns>}:cmd`
//! and every event to `<prefix>:{<ns>}:evt`, so each kind keeps publish order
//! across names. A consumer reads with a Redis consumer group: commands use
//! one group per consumer identity on the command stream (its listeners
//! compete), events one per identity on the event stream (every identity sees
//! every event, its subscribers compete). A new group starts at the beginning
//! of the stream.
//!
//! An entry stays pending in its group until it is acknowledged (`XACK`). A
//! nack hands it back by setting its idle time so it becomes claimable once
//! its delay has passed; any consumer of the group then claims it
//! (`XAUTOCLAIM`), and later entries are not held up behind it. A consumer
//! that stops renewing an entry for the lease (a crash) loses it the same way.

use std::sync::Arc;
use std::time::Duration;

use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::{Client, Script, Value};

use super::source::{MessageSource, ReceivedMessage};
use super::{
    run_source, Bus, BusConsumer, BusTopologyConfig, Message, MessageKind, MessagePublisher,
    MessageRouter, PublishDeadLetters, RunOptions, TransportError, WireEnvelope,
};

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

/// A connection manager for `url` that connects on first use.
fn lazy_connection(
    url: &str,
    response_timeout: Duration,
) -> Result<ConnectionManager, TransportError> {
    let client =
        Client::open(url).map_err(|err| TransportError::permanent(format!("redis url: {err}")))?;
    let config = ConnectionManagerConfig::new()
        .set_connection_timeout(Some(Duration::from_secs(2)))
        .set_response_timeout(Some(response_timeout))
        .set_number_of_retries(1);
    client
        .get_connection_manager_lazy(config)
        .map_err(|err| retryable("connect", err))
}

impl RedisStreamPublisher {
    /// Connect lazily to `url`; construction never contacts Redis.
    pub fn connect(url: &str) -> Result<Self, TransportError> {
        Ok(Self::new(lazy_connection(url, Duration::from_secs(5))?))
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
    pub async fn publish_entry(&self, message: Message) -> Result<bool, TransportError> {
        let stream = self.stream_key(&message);
        self.publish_to(&stream, message).await
    }

    /// Publish to the stream `stream`, whatever the message's name; `false`
    /// when the id was already published there within the dedupe window.
    pub async fn publish_to(&self, stream: &str, message: Message) -> Result<bool, TransportError> {
        let dedupe_key = message.id().map(|id| Self::dedupe_key(stream, id));
        let envelope = WireEnvelope::encode(message)?;
        let mut invocation = self.script.prepare_invoke();
        invocation.key(stream);
        if let Some(dedupe_key) = dedupe_key {
            invocation.key(dedupe_key);
        }
        invocation
            .arg(self.dedupe_ttl.as_secs())
            .arg(self.max_len)
            .arg("name")
            .arg(&envelope.name)
            .arg("payload")
            .arg(&envelope.payload);
        for (key, value) in &envelope.headers {
            invocation.arg(format!("header:{key}")).arg(value);
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
        self.publish_entry(message).await.map(|_| ())
    }
}

/// Default lease: how long a pending entry may go unrenewed before another
/// consumer of its group claims it.
pub const DEFAULT_REDIS_LEASE: Duration = Duration::from_secs(30);
/// Default time a consumer blocks waiting for a new entry.
pub const DEFAULT_REDIS_BLOCK: Duration = Duration::from_millis(500);

/// Redis Streams [`Bus`] + [`BusConsumer`]. Cheap to clone.
#[derive(Clone)]
pub struct RedisStreamBus {
    publisher: RedisStreamPublisher,
    /// Reads block, so they get their own connection: a blocked read must
    /// not hold up publishes and acknowledgements.
    url: String,
    topology: BusTopologyConfig,
    lease: Duration,
    block: Duration,
}

impl RedisStreamBus {
    /// Connect lazily to `url`; construction never contacts Redis.
    pub fn connect(url: &str) -> Result<Self, TransportError> {
        Ok(Self {
            publisher: RedisStreamPublisher::connect(url)?,
            url: url.to_string(),
            topology: BusTopologyConfig::default(),
            lease: DEFAULT_REDIS_LEASE,
            block: DEFAULT_REDIS_BLOCK,
        })
    }

    /// Set an explicit consumer group. Service consumers can usually omit
    /// this and use [`Service::named`](crate::microsvc::Service::named).
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.topology = self.topology.group(group);
        self
    }

    /// Set the namespace that scopes the streams.
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.topology = self.topology.namespace(namespace);
        self
    }

    /// Key prefix for the streams (default [`DEFAULT_REDIS_STREAM_PREFIX`]).
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.publisher = self.publisher.with_prefix(prefix);
        self
    }

    /// How long a pending entry may go unrenewed before another consumer of
    /// its group claims it (default [`DEFAULT_REDIS_LEASE`]). It also caps a
    /// nack's delay.
    pub fn with_lease(mut self, lease: Duration) -> Self {
        self.lease = lease.max(Duration::from_millis(1));
        self
    }

    /// How long `recv` blocks waiting for a new entry (default
    /// [`DEFAULT_REDIS_BLOCK`]).
    pub fn with_block(mut self, block: Duration) -> Self {
        self.block = block.max(Duration::from_millis(1));
        self
    }

    /// The stream every message of `kind` goes to.
    pub fn stream(&self, kind: MessageKind) -> Result<String, TransportError> {
        let namespace = self.topology.namespace_for("redis")?;
        let suffix = match kind {
            MessageKind::Command => "cmd",
            MessageKind::Event => "evt",
        };
        Ok(format!(
            "{}:{{{namespace}}}:{suffix}",
            self.publisher.prefix
        ))
    }

    async fn consume<R: MessageRouter>(
        &self,
        router: Arc<R>,
        options: RunOptions,
        kind: MessageKind,
    ) -> Result<(), TransportError> {
        let plan = router.subscription_plan();
        let names = match kind {
            MessageKind::Command => plan.commands,
            MessageKind::Event => plan.events,
        };
        if names.is_empty() {
            return Ok(());
        }
        let stream = self.stream(kind)?;
        let group = self
            .topology
            .resolve_consumer_group(router.as_ref(), "redis")?;
        let mut conn = lazy_connection(&self.url, self.block + Duration::from_secs(5))?;
        let created: Result<Value, _> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&stream)
            .arg(&group)
            .arg("0")
            .arg("MKSTREAM")
            .query_async(&mut conn)
            .await;
        if let Err(err) = created {
            if err.code() != Some("BUSYGROUP") {
                return Err(retryable("XGROUP CREATE", err));
            }
        }
        let source = RedisStreamSource {
            conn,
            stream,
            group,
            consumer: uuid::Uuid::now_v7().to_string(),
            lease: self.lease,
            block: self.block,
        };
        let options = options.or_dead_letters(|| Arc::new(PublishDeadLetters(self.clone())));
        run_source(router, source, options).await
    }
}

impl Bus for RedisStreamBus {
    async fn send_message(&self, message: Message) -> Result<(), TransportError> {
        let stream = self.stream(MessageKind::Command)?;
        self.publisher
            .publish_to(&stream, message)
            .await
            .map(|_| ())
    }

    async fn publish_message(&self, message: Message) -> Result<(), TransportError> {
        let stream = self.stream(MessageKind::Event)?;
        self.publisher
            .publish_to(&stream, message)
            .await
            .map(|_| ())
    }
}

impl BusConsumer for RedisStreamBus {
    async fn listen<R: MessageRouter>(
        &self,
        router: Arc<R>,
        options: RunOptions,
    ) -> Result<(), TransportError> {
        self.consume(router, options, MessageKind::Command).await
    }

    async fn subscribe<R: MessageRouter>(
        &self,
        router: Arc<R>,
        options: RunOptions,
    ) -> Result<(), TransportError> {
        self.consume(router, options, MessageKind::Event).await
    }
}

/// One consumer of a Redis consumer group.
struct RedisStreamSource {
    conn: ConnectionManager,
    stream: String,
    group: String,
    consumer: String,
    lease: Duration,
    block: Duration,
}

impl RedisStreamSource {
    fn received(&self, entry: Entry, attempts: u32) -> RedisStreamReceived {
        let (message, decode_error) = entry.envelope.decode();
        let renewal = {
            let (conn, stream, group, consumer, id) = (
                self.conn.clone(),
                self.stream.clone(),
                self.group.clone(),
                self.consumer.clone(),
                entry.id.clone(),
            );
            let every = (self.lease / 3).max(Duration::from_millis(10));
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(every).await;
                    // Claiming an entry it holds resets its idle time.
                    if let Err(error) =
                        claim_to(&conn, &stream, &group, &consumer, &id, Duration::ZERO).await
                    {
                        tracing::warn!(%error, "renewing a redis stream entry failed; retrying");
                    }
                }
            })
        };
        RedisStreamReceived {
            conn: self.conn.clone(),
            stream: self.stream.clone(),
            group: self.group.clone(),
            consumer: self.consumer.clone(),
            id: entry.id,
            message,
            decode_error,
            attempts,
            lease: self.lease,
            renewal,
        }
    }

    /// An entry of the group that has been idle for the lease: handed back,
    /// or held by a consumer that stopped.
    async fn claim_stale(&mut self) -> Result<Option<Entry>, TransportError> {
        let reply: Value = redis::cmd("XAUTOCLAIM")
            .arg(&self.stream)
            .arg(&self.group)
            .arg(&self.consumer)
            .arg(millis(self.lease))
            .arg("0-0")
            .arg("COUNT")
            .arg(1)
            .query_async(&mut self.conn)
            .await
            .map_err(|err| retryable("XAUTOCLAIM", err))?;
        let Value::Array(parts) = reply else {
            return Err(unexpected("XAUTOCLAIM", &reply));
        };
        let entries = match parts.get(1) {
            Some(Value::Array(entries)) => entries,
            _ => return Ok(None),
        };
        Ok(entries.iter().find_map(Entry::parse))
    }

    async fn read_new(&mut self) -> Result<Option<Entry>, TransportError> {
        let reply: Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&self.group)
            .arg(&self.consumer)
            .arg("COUNT")
            .arg(1)
            .arg("BLOCK")
            .arg(millis(self.block))
            .arg("STREAMS")
            .arg(&self.stream)
            .arg(">")
            .query_async(&mut self.conn)
            .await
            .map_err(|err| retryable("XREADGROUP", err))?;
        // [[stream, [[id, [field, value, ...]], ...]]], or nil on timeout.
        let streams = match reply {
            Value::Nil => return Ok(None),
            Value::Array(streams) => streams,
            Value::Map(pairs) => pairs.into_iter().map(|(_, entries)| entries).collect(),
            other => return Err(unexpected("XREADGROUP", &other)),
        };
        Ok(streams.iter().find_map(|stream| {
            let entries = match stream {
                Value::Array(parts) => parts.get(1)?,
                entries => entries,
            };
            match entries {
                Value::Array(entries) => entries.iter().find_map(Entry::parse),
                _ => None,
            }
        }))
    }

    /// How often the group has delivered entry `id`, counting this delivery.
    async fn deliveries(&mut self, id: &str) -> Result<u32, TransportError> {
        let reply: Value = redis::cmd("XPENDING")
            .arg(&self.stream)
            .arg(&self.group)
            .arg(id)
            .arg(id)
            .arg(1)
            .query_async(&mut self.conn)
            .await
            .map_err(|err| retryable("XPENDING", err))?;
        // [[id, consumer, idle, deliveries]]
        let count = match &reply {
            Value::Array(rows) => match rows.first() {
                Some(Value::Array(row)) => match row.get(3) {
                    Some(Value::Int(count)) => *count,
                    _ => 1,
                },
                _ => 1,
            },
            _ => 1,
        };
        Ok(u32::try_from(count.max(1)).unwrap_or(u32::MAX))
    }
}

impl MessageSource for RedisStreamSource {
    type Received = RedisStreamReceived;

    fn transport_name(&self) -> &'static str {
        "redis"
    }

    async fn recv(&mut self) -> Result<Option<Self::Received>, TransportError> {
        if let Some(entry) = self.claim_stale().await? {
            let attempts = self.deliveries(&entry.id).await?;
            return Ok(Some(self.received(entry, attempts)));
        }
        Ok(self.read_new().await?.map(|entry| self.received(entry, 1)))
    }
}

/// One stream entry as a wire envelope.
struct Entry {
    id: String,
    envelope: WireEnvelope,
}

impl Entry {
    /// Read `[id, [field, value, ...]]`. Fields are `name`, `payload` and one
    /// `header:<key>` per envelope header, in order. `None` for an entry
    /// that was deleted while pending.
    fn parse(value: &Value) -> Option<Self> {
        let Value::Array(parts) = value else {
            return None;
        };
        let id = text(parts.first()?)?;
        let fields = match parts.get(1)? {
            Value::Array(fields) => fields.clone(),
            Value::Map(pairs) => pairs
                .iter()
                .flat_map(|(key, value)| [key.clone(), value.clone()])
                .collect(),
            _ => return None,
        };
        let mut envelope = WireEnvelope {
            name: String::new(),
            headers: Vec::new(),
            payload: Vec::new(),
        };
        for pair in fields.chunks(2) {
            let [field, value] = pair else { continue };
            let Some(field) = text(field) else { continue };
            match field.as_str() {
                "name" => envelope.name = text(value).unwrap_or_default(),
                "payload" => envelope.payload = bytes(value),
                header => {
                    if let Some(key) = header.strip_prefix("header:") {
                        envelope
                            .headers
                            .push((key.to_string(), text(value).unwrap_or_default()));
                    }
                }
            }
        }
        Some(Self { id, envelope })
    }
}

fn bytes(value: &Value) -> Vec<u8> {
    match value {
        Value::BulkString(bytes) => bytes.clone(),
        Value::SimpleString(text) => text.clone().into_bytes(),
        _ => Vec::new(),
    }
}

fn text(value: &Value) -> Option<String> {
    match value {
        Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
        Value::SimpleString(text) => Some(text.clone()),
        _ => None,
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn unexpected(command: &str, reply: &Value) -> TransportError {
    TransportError::retryable(format!("redis {command}: unexpected reply {reply:?}"))
}

/// Claim entry `id` for `consumer`, leaving it `idle` already. The delivery
/// count is left alone.
async fn claim_to(
    conn: &ConnectionManager,
    stream: &str,
    group: &str,
    consumer: &str,
    id: &str,
    idle: Duration,
) -> Result<(), TransportError> {
    let mut conn = conn.clone();
    let _: Value = redis::cmd("XCLAIM")
        .arg(stream)
        .arg(group)
        .arg(consumer)
        .arg(0)
        .arg(id)
        .arg("IDLE")
        .arg(millis(idle))
        .arg("JUSTID")
        .query_async(&mut conn)
        .await
        .map_err(|err| retryable("XCLAIM", err))?;
    Ok(())
}

/// A pending stream entry: `ack` acknowledges it; `nack` hands it back to
/// the group after the delay. Its idle time is reset while it is held.
pub struct RedisStreamReceived {
    conn: ConnectionManager,
    stream: String,
    group: String,
    consumer: String,
    id: String,
    message: Message,
    decode_error: Option<TransportError>,
    attempts: u32,
    lease: Duration,
    renewal: tokio::task::JoinHandle<()>,
}

impl RedisStreamReceived {
    /// Stop renewing and wait until the renewal task is gone. A renewal
    /// already sent is then ahead of whatever this delivery sends next on the
    /// shared connection, so it cannot undo a nack.
    async fn stop_renewing(&mut self) {
        self.renewal.abort();
        let _ = (&mut self.renewal).await;
    }
}

impl Drop for RedisStreamReceived {
    fn drop(&mut self) {
        self.renewal.abort();
    }
}

impl ReceivedMessage for RedisStreamReceived {
    fn message(&self) -> &Message {
        &self.message
    }

    fn attempts(&self) -> u32 {
        self.attempts
    }

    fn decode_error(&self) -> Option<&TransportError> {
        self.decode_error.as_ref()
    }

    async fn ack(mut self) -> Result<(), TransportError> {
        self.stop_renewing().await;
        let mut conn = self.conn.clone();
        let _: Value = redis::cmd("XACK")
            .arg(&self.stream)
            .arg(&self.group)
            .arg(&self.id)
            .query_async(&mut conn)
            .await
            .map_err(|err| retryable("XACK", err))?;
        Ok(())
    }

    async fn nack(mut self, delay: Duration) -> Result<(), TransportError> {
        self.stop_renewing().await;
        // Idle for `lease - delay` now, it is claimable once `delay` passes.
        claim_to(
            &self.conn,
            &self.stream,
            &self.group,
            &self.consumer,
            &self.id,
            self.lease.saturating_sub(delay),
        )
        .await
    }
}
