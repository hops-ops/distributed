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
//! tested in `tests/kafka_transport` against a broker (see `compose.yaml`).

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::message::{Header, Headers, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Message as KafkaMessageTrait, Offset, TopicPartitionList};

use super::source::{MessageSource, ReceivedMessage};
use super::{message_from_wire, strip_address_prefix, Message};
use super::{retryable, MessagePublisher, TransportError};

const MESSAGE_ID_HEADER: &str = "x-sourced-id";
const MESSAGE_KIND_HEADER: &str = "x-sourced-kind";

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
            .set("linger.ms", "5")
            .set("batch.num.messages", "10000")
            .set("queue.buffering.max.kbytes", "65536")
            .create()
            .map_err(|err| retryable("kafka producer", err))?;
        Ok(Self::new(producer))
    }
}

fn owned_headers(message: &Message) -> OwnedHeaders {
    let mut headers = OwnedHeaders::new().insert(Header {
        key: MESSAGE_KIND_HEADER,
        value: Some(message.kind.as_str()),
    });
    if let Some(id) = message.id() {
        headers = headers.insert(Header {
            key: MESSAGE_ID_HEADER,
            value: Some(id),
        });
    }
    for (key, value) in &message.metadata {
        headers = headers.insert(Header {
            key: key.as_str(),
            value: Some(value.as_str()),
        });
    }
    headers
}

impl MessagePublisher for KafkaPublisher {
    async fn publish(&self, message: Message) -> Result<(), TransportError> {
        let topic = message.name().to_string();
        let key = message.id().unwrap_or(message.name()).to_string();
        let headers = owned_headers(&message);
        let record = FutureRecord::to(&topic)
            .payload(&message.payload)
            .key(&key)
            .headers(headers);
        self.producer
            .send(record, self.send_timeout)
            .await
            .map_err(|(err, _)| retryable("kafka send", err))?;
        Ok(())
    }

    async fn publish_batch(&self, messages: Vec<Message>) -> Result<(), TransportError> {
        if messages.is_empty() {
            return Ok(());
        }
        let prepared: Vec<(String, String, Vec<u8>, OwnedHeaders)> = messages
            .into_iter()
            .map(|message| {
                let topic = message.name().to_string();
                let key = message.id().unwrap_or(message.name()).to_string();
                let headers = owned_headers(&message);
                (topic, key, message.payload, headers)
            })
            .collect();
        let mut pending = Vec::with_capacity(prepared.len());
        for (topic, key, payload, headers) in &prepared {
            let record = FutureRecord::to(topic)
                .payload(payload)
                .key(key)
                .headers(headers.clone());
            pending.push(self.producer.send(record, self.send_timeout));
        }
        for send in pending {
            send.await
                .map_err(|(err, _)| retryable("kafka send", err))?;
        }
        Ok(())
    }
}

/// Consumes a topic with a consumer group, committing offsets on ack.
pub struct KafkaSource {
    pub(crate) consumer: Arc<StreamConsumer>,
    fetch_timeout: Duration,
    fetch_max: usize,
    strip_prefix: Option<String>,
    buffer: VecDeque<KafkaReceived>,
    book: Arc<Mutex<KafkaOffsetBook>>,
}

/// Records successful acks and commits one high-water mark per partition
/// after a fetch batch is fully settled (or on nack).
struct KafkaOffsetBook {
    consumer: Arc<StreamConsumer>,
    high_water: HashMap<(String, i32), i64>,
    invalidate: HashSet<(String, i32)>,
    unsettled: usize,
}

impl KafkaOffsetBook {
    fn new(consumer: Arc<StreamConsumer>) -> Self {
        Self {
            consumer,
            high_water: HashMap::new(),
            invalidate: HashSet::new(),
            unsettled: 0,
        }
    }

    fn begin_batch(&mut self, count: usize) -> Result<(), TransportError> {
        self.flush()?;
        self.unsettled = count;
        Ok(())
    }

    fn record_success(
        &mut self,
        topic: String,
        partition: i32,
        offset: i64,
    ) -> Result<(), TransportError> {
        let next = offset + 1;
        self.high_water
            .entry((topic, partition))
            .and_modify(|current| *current = (*current).max(next))
            .or_insert(next);
        self.unsettled = self.unsettled.saturating_sub(1);
        if self.unsettled == 0 {
            self.flush()?;
        }
        Ok(())
    }

    fn record_nack(
        &mut self,
        topic: String,
        partition: i32,
        offset: i64,
    ) -> Result<(), TransportError> {
        if let Some(next) = self.high_water.get_mut(&(topic.clone(), partition)) {
            if *next > offset {
                *next = offset;
            }
        }
        self.unsettled = 0;
        self.invalidate.insert((topic, partition));
        self.flush()
    }

    fn take_invalidated(&mut self) -> HashSet<(String, i32)> {
        std::mem::take(&mut self.invalidate)
    }

    fn flush(&mut self) -> Result<(), TransportError> {
        if self.high_water.is_empty() {
            return Ok(());
        }
        let mut tpl = TopicPartitionList::new();
        for ((topic, partition), next) in self.high_water.drain() {
            tpl.add_partition_offset(&topic, partition, Offset::Offset(next))
                .map_err(|err| retryable("kafka offset", err))?;
        }
        self.consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Async)
            .map_err(|err| retryable("kafka commit", err))
    }
}

impl KafkaSource {
    /// Wrap an existing subscribed consumer.
    pub fn new(consumer: Arc<StreamConsumer>) -> Self {
        Self {
            fetch_timeout: Duration::from_secs(5),
            fetch_max: 32,
            strip_prefix: None,
            buffer: VecDeque::new(),
            book: Arc::new(Mutex::new(KafkaOffsetBook::new(Arc::clone(&consumer)))),
            consumer,
        }
    }

    /// How many extra records to drain after the first wait.
    pub fn with_fetch_max(mut self, max: usize) -> Self {
        self.fetch_max = max.max(1);
        self
    }

    /// How long `recv` waits for a record before returning `Ok(None)`.
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
    ///
    /// Topics are created before subscribe. A Kafka consumer that names a topic
    /// that does not exist yet is not assigned that topic until a later metadata
    /// refresh (often minutes). `listen` on several command names would then
    /// consume the first produced name and stall on every later name — the
    /// load-suite hot-increment cell after a successful initialize.
    pub async fn connect(
        brokers: &str,
        group_id: &str,
        topics: &[&str],
    ) -> Result<Self, TransportError> {
        ensure_topics(brokers, topics).await?;
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("allow.auto.create.topics", "true")
            .set("topic.metadata.refresh.interval.ms", "1000")
            .create()
            .map_err(|err| retryable("kafka consumer", err))?;
        consumer
            .subscribe(topics)
            .map_err(|err| retryable("kafka subscribe", err))?;
        Ok(Self::new(Arc::new(consumer)))
    }

    async fn fill_buffer(&mut self, first_timeout: Duration) -> Result<bool, TransportError> {
        if !self.buffer.is_empty() {
            return Ok(true);
        }
        let Some(first) = self.poll_one(first_timeout).await? else {
            return Ok(false);
        };
        self.buffer.push_back(first);
        while self.buffer.len() < self.fetch_max {
            match self.poll_one(Duration::from_millis(1)).await? {
                Some(next) => self.buffer.push_back(next),
                None => break,
            }
        }
        self.book
            .lock()
            .map_err(|_| TransportError::retryable("kafka offset book poisoned"))?
            .begin_batch(self.buffer.len())?;
        Ok(true)
    }

    async fn poll_one(&self, timeout: Duration) -> Result<Option<KafkaReceived>, TransportError> {
        if timeout.is_zero() {
            return Ok(None);
        }
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(None);
            }
            match tokio::time::timeout(remaining, self.consumer.recv()).await {
                Ok(Ok(borrowed)) => {
                    return Ok(Some(KafkaReceived::from_borrowed(
                        &borrowed,
                        self.consumer.clone(),
                        Arc::clone(&self.book),
                        self.strip_prefix.as_deref(),
                    )));
                }
                Ok(Err(_transient)) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(_elapsed) => return Ok(None),
            }
        }
    }
}

impl MessageSource for KafkaSource {
    type Received = KafkaReceived;

    fn transport_name(&self) -> &'static str {
        "kafka"
    }

    async fn recv(&mut self) -> Result<Option<Self::Received>, TransportError> {
        {
            let invalidated = self
                .book
                .lock()
                .map_err(|_| TransportError::retryable("kafka offset book poisoned"))?
                .take_invalidated();
            if !invalidated.is_empty() {
                self.buffer.retain(|message| {
                    !invalidated.contains(&(message.topic.clone(), message.partition))
                });
            }
        }
        if !self.fill_buffer(self.fetch_timeout).await? {
            return Ok(None);
        }
        Ok(self.buffer.pop_front())
    }

    async fn wait(&mut self) -> Result<(), TransportError> {
        let _ = self.fill_buffer(self.fetch_timeout).await?;
        Ok(())
    }
}

/// Create `topics` if they are missing so a consumer can be assigned immediately.
async fn ensure_topics(brokers: &str, topics: &[&str]) -> Result<(), TransportError> {
    if topics.is_empty() {
        return Ok(());
    }
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .map_err(|err| retryable("kafka admin", err))?;
    // Let the broker's topic policy choose the partition count and replication
    // factor. The transport must not silently collapse a production cluster to
    // one partition and one replica merely because it had to create a topic.
    let new_topics: Vec<NewTopic<'_>> = topics
        .iter()
        .map(|topic| NewTopic::new(topic, -1, TopicReplication::Fixed(-1)))
        .collect();
    let results = admin
        .create_topics(
            &new_topics,
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
        )
        .await
        .map_err(|err| retryable("kafka create topics", err))?;
    for result in results {
        match result {
            Ok(_) => {}
            Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((name, code)) => {
                return Err(retryable("kafka create topics", format!("{name}: {code}")));
            }
        }
    }
    Ok(())
}

/// A consumed record plus the means to commit/seek its offset.
pub struct KafkaReceived {
    consumer: Arc<StreamConsumer>,
    book: Arc<Mutex<KafkaOffsetBook>>,
    topic: String,
    partition: i32,
    offset: i64,
    message: Message,
}

impl KafkaReceived {
    fn from_borrowed(
        borrowed: &rdkafka::message::BorrowedMessage<'_>,
        consumer: Arc<StreamConsumer>,
        book: Arc<Mutex<KafkaOffsetBook>>,
        strip_prefix: Option<&str>,
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
        let message = message_from_wire(
            name,
            payload,
            Some(MESSAGE_ID_HEADER),
            MESSAGE_KIND_HEADER,
            headers,
        );
        Self {
            consumer,
            book,
            topic,
            partition: borrowed.partition(),
            offset: borrowed.offset(),
            message,
        }
    }
}

impl ReceivedMessage for KafkaReceived {
    fn message(&self) -> &Message {
        &self.message
    }

    async fn ack(self) -> Result<(), TransportError> {
        self.book
            .lock()
            .map_err(|_| TransportError::retryable("kafka offset book poisoned"))?
            .record_success(self.topic, self.partition, self.offset)
    }

    async fn nack(self, _reason: &str) -> Result<(), TransportError> {
        self.book
            .lock()
            .map_err(|_| TransportError::retryable("kafka offset book poisoned"))?
            .record_nack(self.topic.clone(), self.partition, self.offset)?;
        // Seek back so this record is re-read. `seek` is blocking; run it off
        // the tokio worker. Remaining buffered records for this partition are
        // dropped on the next recv.
        let consumer = self.consumer;
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

    async fn dead_letter(self, _reason: &str) -> Result<(), TransportError> {
        self.book
            .lock()
            .map_err(|_| TransportError::retryable("kafka offset book poisoned"))?
            .record_success(self.topic, self.partition, self.offset)
    }

    async fn park(self, _reason: &str) -> Result<(), TransportError> {
        self.book
            .lock()
            .map_err(|_| TransportError::retryable("kafka offset book poisoned"))?
            .record_success(self.topic, self.partition, self.offset)
    }
}
