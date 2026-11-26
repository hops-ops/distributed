//! Kafka transport adapter.
//!
//! [`KafkaPublisher`] sends a canonical [`Message`] to a topic named by the
//! message name, awaiting the producer ack (the durable publish threshold, per
//! the configured `acks`). [`KafkaSource`] consumes with a consumer group
//! (auto-commit disabled) and settles by offset: ack→commit the offset,
//! nack→seek back so the record is re-read, dead-letter/park→commit (skip).
//!
//! Offset commits use `CommitMode::Async` so settling never blocks the tokio
//! worker; this is at-least-once: a crash before an async commit lands redelivers
//! the record, so consumers must tolerate duplicates.
//!
//! Requires the `kafka` feature (builds `librdkafka` via cmake). Integration-
//! tested in `tests/transports/kafka_transport` against a broker (see `compose.yaml`).

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, Headers, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Message as KafkaMessageTrait, Offset, TopicPartitionList};

use super::source::{MessageSource, ReceivedMessage};
use super::{retryable, MessagePublisher, TransportError};
use super::{strip_address_prefix, Message, WireEnvelope};

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
    /// `acks=all` so a successful send is durably replicated.
    pub async fn connect(brokers: &str) -> Result<Self, TransportError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("acks", "all")
            .set("message.timeout.ms", "10000")
            .create()
            .map_err(|err| retryable("kafka producer", err))?;
        Ok(Self::new(producer))
    }
}

impl MessagePublisher for KafkaPublisher {
    async fn publish(&self, message: Message) -> Result<(), TransportError> {
        let key = message.id().unwrap_or(message.name()).to_string();
        let envelope = WireEnvelope::encode(message)?;
        let mut headers = OwnedHeaders::new_with_capacity(envelope.headers.len());
        for (key, value) in &envelope.headers {
            headers = headers.insert(Header {
                key: key.as_str(),
                value: Some(value.as_str()),
            });
        }
        let record = FutureRecord::to(&envelope.name)
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

/// Consumes a topic with a consumer group, committing offsets on ack.
pub struct KafkaSource {
    consumer: Arc<StreamConsumer>,
    fetch_timeout: Duration,
    strip_prefix: Option<String>,
    /// Receive errors in a row with no record in between.
    consecutive_errors: u32,
    redelivery: Redelivery,
}

/// Receive errors in a row after which `recv` stops retrying and fails.
const MAX_CONSECUTIVE_RECV_ERRORS: u32 = 10;

/// The record most recently handed back, and how often. Kafka does not count
/// deliveries, and a partition redelivers its head record until it is
/// committed, so one slot per source is enough.
type Redelivery = Arc<Mutex<Option<((String, i32, i64), u32)>>>;

impl KafkaSource {
    /// Wrap an existing subscribed consumer.
    pub fn new(consumer: Arc<StreamConsumer>) -> Self {
        Self {
            consumer,
            fetch_timeout: Duration::from_secs(5),
            strip_prefix: None,
            consecutive_errors: 0,
            redelivery: Redelivery::default(),
        }
    }

    /// How long `recv` waits for a record before reporting that none is
    /// available.
    pub fn with_fetch_timeout(mut self, timeout: Duration) -> Self {
        self.fetch_timeout = timeout;
        self
    }

    /// Strip `prefix` from each record's topic when deriving the message name, so
    /// a topic `app.cmd.account.debit` becomes the name `account.debit`. Used by
    /// [`KafkaBus`](super::KafkaBus); default: no stripping (the topic is the name).
    pub fn with_strip_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.strip_prefix = Some(prefix.into());
        self
    }

    /// Connect a consumer (group `group_id`, auto-commit off, earliest reset) and
    /// subscribe to `topics`.
    pub async fn connect(
        brokers: &str,
        group_id: &str,
        topics: &[&str],
    ) -> Result<Self, TransportError> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .map_err(|err| retryable("kafka consumer", err))?;
        consumer
            .subscribe(topics)
            .map_err(|err| retryable("kafka subscribe", err))?;
        Ok(Self::new(Arc::new(consumer)))
    }
}

impl KafkaSource {
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
        // Wait up to the fetch timeout for a record. Kafka reports transient
        // coordination errors (group bootstrap, rebalances) as receive errors
        // and recovers by itself, so a few are retried. Errors that keep
        // coming (bad credentials, a topic that does not exist) end the run.
        let deadline = Instant::now() + self.fetch_timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(None);
            }
            match tokio::time::timeout(remaining, self.consumer.recv()).await {
                Ok(Ok(borrowed)) => {
                    self.consecutive_errors = 0;
                    return Ok(Some(KafkaReceived::from_borrowed(
                        &borrowed,
                        self.consumer.clone(),
                        self.strip_prefix.as_deref(),
                        self.redelivery.clone(),
                    )));
                }
                Ok(Err(error)) => {
                    self.recv_failed(error)?;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(_elapsed) => {
                    self.consecutive_errors = 0;
                    return Ok(None);
                }
            }
        }
    }
}

/// A consumed record plus the means to commit/seek its offset.
pub struct KafkaReceived {
    consumer: Arc<StreamConsumer>,
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
        consumer: Arc<StreamConsumer>,
        strip_prefix: Option<&str>,
        redelivery: Redelivery,
    ) -> Self {
        let payload = borrowed.payload().map(|p| p.to_vec()).unwrap_or_default();
        let topic = borrowed.topic().to_string();
        let name = strip_address_prefix(topic.clone(), strip_prefix);
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
        .map_err(|err| retryable("kafka seek", err))
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
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", "127.0.0.1:1")
            .set("group.id", "offline")
            .create()
            .unwrap();
        KafkaSource::new(Arc::new(consumer))
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
