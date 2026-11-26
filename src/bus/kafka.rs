//! Kafka transport adapter.
//!
//! [`KafkaPublisher`] sends a canonical [`Message`] to a topic, awaiting the
//! producer ack (the durable publish threshold, per the configured `acks`).
//! As a [`MessagePublisher`] the topic is the message name; [`KafkaBus`]
//! sends every message of a kind to one topic and carries the name in the
//! [`WireEnvelope::NAME`] header. A record's key is its stream id (the
//! [`SOURCE_AGGREGATE_ID`] metadata), so one aggregate's messages share a
//! partition and keep their order; without one the key is the message id.
//! [`KafkaSource`] consumes with a consumer group (auto-commit disabled) and
//! settles by offset: ack→commit the offset, nack→seek back so the record is
//! re-read. It is idle once it has read every partition assigned to it to
//! its end.
//!
//! [`KafkaBus`]: super::KafkaBus
//! [`SOURCE_AGGREGATE_ID`]: crate::outbox_worker::SOURCE_AGGREGATE_ID
//!
//! Offset commits use `CommitMode::Async` so settling never blocks the tokio
//! worker; this is at-least-once: a crash before an async commit lands redelivers
//! the record, so consumers must tolerate duplicates.
//!
//! Requires the `kafka` feature (builds `librdkafka` via cmake). Integration-
//! tested in `tests/transports/kafka_transport` against a broker (see `compose.yaml`).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use rdkafka::message::{Header, Headers, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Message as KafkaMessageTrait, Offset, TopicPartitionList};

use super::source::{MessageSource, ReceivedMessage};
use super::{retryable, MessagePublisher, TransportError};
use super::{Message, WireEnvelope};
use crate::outbox_worker::{SOURCE_AGGREGATE_ID, SOURCE_AGGREGATE_TYPE};

/// Publishes canonical messages to a Kafka topic named by the message name.
pub struct KafkaPublisher {
    producer: FutureProducer,
    send_timeout: Duration,
}

impl KafkaPublisher {
    /// Wrap an existing producer.
    pub fn new(producer: FutureProducer) -> Self {
        Self {
            producer,
            send_timeout: Duration::from_secs(10),
        }
    }

    /// Connect a producer to `brokers` (comma-separated `host:port`), waiting for
    /// `acks=all` so a successful send is durably replicated. The producer is
    /// idempotent, so a retried send neither duplicates nor reorders records
    /// within a partition.
    pub async fn connect(brokers: &str) -> Result<Self, TransportError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("message.timeout.ms", "10000")
            .create()
            .map_err(|err| retryable("kafka producer", err))?;
        Ok(Self::new(producer))
    }
}

impl KafkaPublisher {
    /// Send `message` to `topic`, carrying its name in the
    /// [`WireEnvelope::NAME`] header, so one topic can hold many names.
    pub async fn send_to(&self, topic: &str, message: Message) -> Result<(), TransportError> {
        self.send(Some(topic), message).await
    }

    async fn send(&self, topic: Option<&str>, message: Message) -> Result<(), TransportError> {
        let key = partition_key(&message);
        let envelope = WireEnvelope::encode(message)?;
        let mut headers = OwnedHeaders::new_with_capacity(envelope.headers.len() + 1);
        if topic.is_some() {
            headers = headers.insert(Header {
                key: WireEnvelope::NAME,
                value: Some(envelope.name.as_str()),
            });
        }
        for (key, value) in &envelope.headers {
            headers = headers.insert(Header {
                key: key.as_str(),
                value: Some(value.as_str()),
            });
        }
        let record = FutureRecord::to(topic.unwrap_or(&envelope.name))
            .payload(&envelope.payload)
            .key(&key)
            .headers(headers);
        self.producer
            .send(record, self.send_timeout)
            .await
            .map_err(|(err, _)| retryable("kafka send", err))?;
        Ok(())
    }
}

/// The record key: the stream the message belongs to, so Kafka keeps one
/// stream's messages in one partition, in order. A message from no stream is
/// keyed by its id, or else its name.
fn partition_key(message: &Message) -> String {
    match message.metadata(SOURCE_AGGREGATE_ID) {
        Some(id) => match message.metadata(SOURCE_AGGREGATE_TYPE) {
            Some(kind) => format!("{kind}/{id}"),
            None => id.to_string(),
        },
        None => message.id().unwrap_or(message.name()).to_string(),
    }
}

impl MessagePublisher for KafkaPublisher {
    /// Send to the topic named by the message name.
    async fn publish(&self, message: Message) -> Result<(), TransportError> {
        self.send(None, message).await
    }
}

/// How long a [`KafkaSource`] waits on the broker at once, unless
/// [`KafkaSource::with_fetch_timeout`] says otherwise.
pub(super) const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_secs(8);

/// Consumes one topic with a consumer group, committing offsets on ack.
///
/// [`recv`](MessageSource::recv) returns `Ok(None)` (idle) once the consumer
/// has joined its group and has read every partition assigned to it to its
/// end; a group with no partition assigned, or a topic that does not exist
/// yet, is idle at once. The fetch timeout bounds one wait on the broker: for
/// the group to be joined, for the next record or end of partition, and in
/// [`ready`](MessageSource::ready) for the next record once caught up. A wait that runs out also returns `Ok(None)`,
/// so a broker that stops answering does not hang a run.
pub struct KafkaSource {
    consumer: Arc<SourceConsumer>,
    progress: Progress,
    fetch_timeout: Duration,
    /// Receive errors in a row with no record in between.
    consecutive_errors: u32,
    redelivery: Redelivery,
    /// What [`ready`](MessageSource::ready) received, for the next `recv`.
    pending: Option<KafkaResult<KafkaReceived>>,
}

/// Receive errors in a row after which `recv` stops retrying and fails.
const MAX_CONSECUTIVE_RECV_ERRORS: u32 = 10;

/// The record most recently handed back, and how often. Kafka does not count
/// deliveries, and a partition redelivers its head record until it is
/// committed, so one slot per source is enough.
type Redelivery = Arc<Mutex<Option<((String, i32, i64), u32)>>>;

type SourceConsumer = StreamConsumer<SourceContext>;

/// Where a source's consumer stands in its group: `None` until the group has
/// assigned it partitions (and again from a revocation until the next
/// assignment), else each assigned partition of the source's one topic and
/// whether it has been read to its end. A topic that does not exist is an
/// assignment of no partition.
#[derive(Clone, Default)]
struct Progress(Arc<Mutex<Option<BTreeMap<i32, bool>>>>);

impl Progress {
    fn update(&self, change: impl FnOnce(&mut Option<BTreeMap<i32, bool>>)) {
        change(&mut self.0.lock().unwrap_or_else(PoisonError::into_inner));
    }

    fn assigned(&self, partitions: impl IntoIterator<Item = i32>) {
        let partitions = partitions.into_iter().map(|p| (p, false)).collect();
        self.update(|state| *state = Some(partitions));
    }

    fn revoked(&self) {
        self.update(|state| *state = None);
    }

    /// A record came from `partition`, or its position moved back.
    fn behind(&self, partition: i32) {
        self.set_at_end(partition, false);
    }

    fn at_end(&self, partition: i32) {
        self.set_at_end(partition, true);
    }

    fn set_at_end(&self, partition: i32, at_end: bool) {
        self.update(|state| {
            if let Some(flag) = state.as_mut().and_then(|parts| parts.get_mut(&partition)) {
                *flag = at_end;
            }
        });
    }

    /// The topic does not exist, so the group has nothing to assign. Until
    /// it assigns something, there is nothing to read.
    fn no_topic(&self) {
        self.update(|state| {
            state.get_or_insert_with(BTreeMap::new);
        });
    }

    fn joined(&self) -> bool {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    }

    /// Joined, and every assigned partition read to its end.
    fn caught_up(&self) -> bool {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .is_some_and(|parts| parts.values().all(|at_end| *at_end))
    }
}

/// Tells the source's [`Progress`] what its group assigned. librdkafka calls
/// it from inside a poll of the consumer, so a rebalance is seen before the
/// records it brings.
struct SourceContext {
    progress: Progress,
}

impl ClientContext for SourceContext {}

impl ConsumerContext for SourceContext {
    fn post_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(partitions) => self
                .progress
                .assigned(partitions.elements().iter().map(|tp| tp.partition())),
            Rebalance::Revoke(_) => self.progress.revoked(),
            Rebalance::Error(error) => tracing::warn!(%error, "kafka rebalance failed"),
        }
    }
}

impl KafkaSource {
    /// Create a consumer from `config` and subscribe it to `topic`.
    ///
    /// `config` names the brokers, the group and anything else the
    /// deployment needs (credentials, `auto.offset.reset`). The source sets
    /// what it relies on: `enable.auto.commit=false` (it commits on ack) and
    /// `enable.partition.eof=true` (it knows when it has caught up).
    pub fn new(config: &ClientConfig, topic: &str) -> Result<Self, TransportError> {
        let progress = Progress::default();
        let consumer: SourceConsumer = config
            .clone()
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "true")
            .create_with_context(SourceContext {
                progress: progress.clone(),
            })
            .map_err(|err| retryable("kafka consumer", err))?;
        consumer
            .subscribe(&[topic])
            .map_err(|err| retryable("kafka subscribe", err))?;
        Ok(Self {
            consumer: Arc::new(consumer),
            progress,
            fetch_timeout: DEFAULT_FETCH_TIMEOUT,
            consecutive_errors: 0,
            redelivery: Redelivery::default(),
            pending: None,
        })
    }

    /// Bound one wait on the broker (see [`KafkaSource`]).
    pub fn with_fetch_timeout(mut self, timeout: Duration) -> Self {
        self.fetch_timeout = timeout;
        self
    }

    /// Connect a consumer (group `group_id`, earliest reset) to `brokers`
    /// and subscribe it to `topic`.
    pub async fn connect(
        brokers: &str,
        group_id: &str,
        topic: &str,
    ) -> Result<Self, TransportError> {
        Self::new(
            ClientConfig::new()
                .set("bootstrap.servers", brokers)
                .set("group.id", group_id)
                .set("auto.offset.reset", "earliest"),
            topic,
        )
    }
}

impl KafkaSource {
    /// The consumer's next record or event, or `None` when `deadline`
    /// passes first.
    async fn next_event(&self, deadline: Instant) -> Option<KafkaResult<KafkaReceived>> {
        let event = tokio::time::timeout_at(deadline.into(), self.consumer.recv())
            .await
            .ok()?;
        Some(event.map(|borrowed| {
            self.progress.behind(borrowed.partition());
            KafkaReceived::from_borrowed(
                &borrowed,
                self.consumer.clone(),
                self.progress.clone(),
                self.redelivery.clone(),
            )
        }))
    }

    /// Account for one receive error: `Err` when `recv` must fail now (an
    /// error retrying cannot fix, or [`MAX_CONSECUTIVE_RECV_ERRORS`] in a
    /// row), else a logged retry.
    fn recv_failed(&mut self, error: KafkaError) -> Result<(), TransportError> {
        if recv_error_is_permanent(&error) {
            self.consecutive_errors = 0;
            tracing::error!(%error, "kafka receive failed permanently");
            return Err(
                TransportError::permanent(format!("kafka receive: {error}")).with_source(error)
            );
        }
        self.consecutive_errors += 1;
        if self.consecutive_errors >= MAX_CONSECUTIVE_RECV_ERRORS {
            self.consecutive_errors = 0;
            tracing::error!(%error, "kafka receive kept failing");
            return Err(retryable("kafka receive", error));
        }
        tracing::warn!(%error, "kafka receive failed; retrying");
        Ok(())
    }
}

/// Whether a receive error will come back however often the consumer retries:
/// refused credentials or permissions, and librdkafka's fatal errors. An
/// unknown topic is not one: a consumer may start before the first record is
/// produced to it.
fn recv_error_is_permanent(error: &KafkaError) -> bool {
    matches!(
        error.rdkafka_error_code(),
        Some(
            RDKafkaErrorCode::Fatal
                | RDKafkaErrorCode::Authentication
                | RDKafkaErrorCode::SaslAuthenticationFailed
                | RDKafkaErrorCode::TopicAuthorizationFailed
                | RDKafkaErrorCode::GroupAuthorizationFailed
                | RDKafkaErrorCode::ClusterAuthorizationFailed
                | RDKafkaErrorCode::InvalidConfig
        )
    )
}

impl MessageSource for KafkaSource {
    type Received = KafkaReceived;

    fn transport_name(&self) -> &'static str {
        "kafka"
    }

    async fn recv(&mut self) -> Result<Option<Self::Received>, TransportError> {
        // Kafka reports transient coordination errors (group bootstrap,
        // rebalances) as receive errors and recovers by itself, so a few are
        // retried. Errors that keep coming (bad credentials) end the run.
        let deadline = Instant::now() + self.fetch_timeout;
        loop {
            let event = match self.pending.take() {
                Some(event) => event,
                None if self.progress.caught_up() => return Ok(None),
                None => match self.next_event(deadline).await {
                    Some(event) => event,
                    None => {
                        if !self.progress.joined() {
                            tracing::warn!(
                                timeout = ?self.fetch_timeout,
                                "kafka consumer has not joined its group within the fetch timeout"
                            );
                        }
                        self.consecutive_errors = 0;
                        return Ok(None);
                    }
                },
            };
            match event {
                Ok(received) => {
                    self.consecutive_errors = 0;
                    return Ok(Some(received));
                }
                Err(KafkaError::PartitionEOF(partition)) => {
                    self.consecutive_errors = 0;
                    self.progress.at_end(partition);
                }
                // librdkafka does not join a group for a topic that does not
                // exist; it reports the topic instead. Nothing to read.
                Err(KafkaError::MessageConsumption(
                    RDKafkaErrorCode::UnknownTopicOrPartition | RDKafkaErrorCode::UnknownTopic,
                )) => {
                    tracing::debug!("kafka topic does not exist yet");
                    self.consecutive_errors = 0;
                    self.progress.no_topic();
                }
                Err(error) => {
                    self.recv_failed(error)?;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    /// Wait for the consumer's next record or event (up to the fetch timeout)
    /// and keep it for `recv`, so a caught-up run waits on the broker rather
    /// than calling `recv` in a loop.
    async fn ready(&mut self) {
        if self.pending.is_some() {
            return;
        }
        let deadline = Instant::now() + self.fetch_timeout;
        self.pending = self.next_event(deadline).await;
    }
}

/// A consumed record plus the means to commit/seek its offset.
pub struct KafkaReceived {
    consumer: Arc<SourceConsumer>,
    progress: Progress,
    topic: String,
    partition: i32,
    offset: i64,
    message: Message,
    decode_error: Option<TransportError>,
    attempts: u32,
    redelivery: Redelivery,
}

impl KafkaReceived {
    fn from_borrowed(
        borrowed: &rdkafka::message::BorrowedMessage<'_>,
        consumer: Arc<SourceConsumer>,
        progress: Progress,
        redelivery: Redelivery,
    ) -> Self {
        let payload = borrowed.payload().map(|p| p.to_vec()).unwrap_or_default();
        let topic = borrowed.topic().to_string();
        // The topic names the message unless a name header does.
        let name = topic.clone();
        let headers: Vec<(String, String)> = borrowed
            .headers()
            .into_iter()
            .flat_map(|headers| headers.iter())
            .map(|header| {
                let value = header
                    .value
                    .map(|v| String::from_utf8_lossy(v).into_owned())
                    .unwrap_or_default();
                (header.key.to_string(), value)
            })
            .collect();
        let (message, decode_error) = WireEnvelope {
            name,
            headers,
            payload,
        }
        .decode();
        let key = (topic.clone(), borrowed.partition(), borrowed.offset());
        let attempts = match redelivery.lock() {
            Ok(slot) => match slot.as_ref() {
                Some((handed_back, times)) if *handed_back == key => times.saturating_add(1),
                _ => 1,
            },
            Err(_) => 1,
        };
        Self {
            consumer,
            progress,
            topic,
            partition: borrowed.partition(),
            offset: borrowed.offset(),
            message,
            decode_error,
            attempts,
            redelivery,
        }
    }

    /// Commit this record's offset (the next offset to read) without blocking the
    /// async runtime.
    ///
    /// Uses [`CommitMode::Async`]: the offset is handed to librdkafka's background
    /// thread and this returns immediately, rather than blocking the tokio worker
    /// on a broker round trip as `CommitMode::Sync` does. This keeps at-least-once
    /// semantics — the runner already acks only after handler effects complete, so
    /// a crash between effect and the async commit landing simply redelivers the
    /// record (a duplicate the consumer must tolerate, which it already does).
    fn commit_offset(&self) -> Result<(), TransportError> {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&self.topic, self.partition, Offset::Offset(self.offset + 1))
            .map_err(|err| retryable("kafka offset", err))?;
        self.consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Async)
            .map_err(|err| retryable("kafka commit", err))
    }
}

impl ReceivedMessage for KafkaReceived {
    fn message(&self) -> &Message {
        &self.message
    }

    fn attempts(&self) -> u32 {
        self.attempts
    }

    fn decode_error(&self) -> Option<&TransportError> {
        self.decode_error.as_ref()
    }

    async fn ack(self) -> Result<(), TransportError> {
        self.commit_offset()
    }

    async fn nack(self, delay: Duration) -> Result<(), TransportError> {
        if let Ok(mut slot) = self.redelivery.lock() {
            *slot = Some((
                (self.topic.clone(), self.partition, self.offset),
                self.attempts,
            ));
        }
        // A partition cannot deliver a later record first, so the delay is
        // spent here, holding the position.
        tokio::time::sleep(delay).await;
        // Do not commit; seek back so this record is re-read (redelivery).
        //
        // `seek` blocks the calling thread until it takes effect (up to the
        // timeout) and rdkafka exposes no async variant, so run it on the
        // blocking pool to avoid stalling the tokio worker. The consumer is
        // Arc-shared and `seek` takes `&self`, so a clone moves cleanly into the
        // blocking task.
        let consumer = self.consumer.clone();
        let topic = self.topic;
        let partition = self.partition;
        let offset = self.offset;
        tokio::task::spawn_blocking(move || {
            consumer.seek(
                &topic,
                partition,
                Offset::Offset(offset),
                Duration::from_secs(5),
            )
        })
        .await
        .map_err(|err| retryable("kafka seek task", err))?
        .map_err(|err| retryable("kafka seek", err))?;
        // The partition is behind its end again. (librdkafka drops an end of
        // partition it reported before the seek.)
        self.progress.behind(partition);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn consumption(code: RDKafkaErrorCode) -> KafkaError {
        KafkaError::MessageConsumption(code)
    }

    /// A consumer that never connects: creating one contacts no broker.
    fn offline_source() -> KafkaSource {
        KafkaSource::new(
            ClientConfig::new()
                .set("bootstrap.servers", "127.0.0.1:1")
                .set("group.id", "offline"),
            "offline",
        )
        .unwrap()
    }

    #[test]
    fn records_are_keyed_by_stream_then_id_then_name() {
        let base = Message::new("order.placed", crate::bus::MessageKind::Event, Vec::new());
        let streamed = base
            .clone()
            .with_id("m-1")
            .with_metadata(SOURCE_AGGREGATE_TYPE, "order")
            .with_metadata(SOURCE_AGGREGATE_ID, "o-1");
        assert_eq!(partition_key(&streamed), "order/o-1");
        assert_eq!(partition_key(&base.clone().with_id("m-1")), "m-1");
        assert_eq!(partition_key(&base), "order.placed");
    }

    #[tokio::test]
    async fn refused_credentials_fail_the_receive_at_once_as_permanent() {
        for code in [
            RDKafkaErrorCode::SaslAuthenticationFailed,
            RDKafkaErrorCode::TopicAuthorizationFailed,
            RDKafkaErrorCode::GroupAuthorizationFailed,
            RDKafkaErrorCode::Fatal,
        ] {
            let error = offline_source()
                .recv_failed(consumption(code))
                .expect_err("surfaced");
            assert!(error.is_permanent(), "{code:?}");
        }
    }

    #[tokio::test]
    async fn transient_errors_are_retried_then_surface_when_they_keep_coming() {
        let mut source = offline_source();
        for _ in 1..MAX_CONSECUTIVE_RECV_ERRORS {
            source
                .recv_failed(consumption(RDKafkaErrorCode::BrokerTransportFailure))
                .unwrap();
        }
        let error = source
            .recv_failed(consumption(RDKafkaErrorCode::UnknownTopicOrPartition))
            .expect_err("surfaced");
        assert!(error.is_retryable());
        // The count starts again after the failure is reported.
        source
            .recv_failed(consumption(RDKafkaErrorCode::BrokerTransportFailure))
            .unwrap();
    }
}
