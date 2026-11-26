//! RabbitMQ (AMQP 0-9-1) transport adapter.
//!
//! [`RabbitPublisher`] publishes a canonical [`Message`] to the default exchange
//! keyed by the message name, waiting for the **publisher confirm** (the durable
//! publish threshold). [`RabbitSource`] polls a queue with `basic_get` and
//! settles via AMQP ack/nack/reject (ack→ack, nack→nack+requeue,
//! dead-letter/park→reject without requeue, which routes to a dead-letter
//! exchange when one is configured on the queue).
//!
//! Requires the `rabbitmq` feature. Integration-tested in
//! `tests/transports/rabbitmq_transport` against a broker (see `compose.yaml`).

use std::time::Duration;

use lapin::message::Delivery;
use lapin::options::{
    BasicAckOptions, BasicGetOptions, BasicNackOptions, BasicPublishOptions, ConfirmSelectOptions,
    QueueDeclareOptions,
};
use lapin::types::{AMQPValue, FieldTable, ShortString};
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};

use super::source::{MessageSource, ReceivedMessage};
use super::{retryable, MessagePublisher, TransportError};
use super::{strip_address_prefix, Message, WireEnvelope};

fn settle_result(context: &str, settled: bool) -> Result<(), TransportError> {
    if settled {
        Ok(())
    } else {
        Err(TransportError::retryable(format!(
            "{context}: acker unavailable"
        )))
    }
}

pub(super) async fn connect_channel(uri: &str) -> Result<Channel, TransportError> {
    let connection = Connection::connect(uri, ConnectionProperties::default())
        .await
        .map_err(|err| retryable("amqp connect", err))?;
    connection
        .create_channel()
        .await
        .map_err(|err| retryable("amqp channel", err))
}

/// Publishes canonical messages to the default exchange, keyed by message name.
pub struct RabbitPublisher {
    channel: Channel,
}

impl RabbitPublisher {
    /// Wrap an existing channel (publisher confirms are enabled on connect).
    pub fn new(channel: Channel) -> Self {
        Self { channel }
    }

    /// Connect to an AMQP URI and enable publisher confirms.
    pub async fn connect(uri: &str) -> Result<Self, TransportError> {
        let channel = connect_channel(uri).await?;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(|err| retryable("amqp confirm_select", err))?;
        Ok(Self::new(channel))
    }
}

/// Publish `message` to `exchange` with `routing_key` and wait for the
/// publisher confirm. The message travels as a [`WireEnvelope`]: its headers
/// are the AMQP header table, its payload the body.
pub(super) async fn publish_confirmed(
    channel: &Channel,
    exchange: &str,
    routing_key: &str,
    message: Message,
) -> Result<(), TransportError> {
    let envelope = WireEnvelope::encode(message)?;
    let mut headers = FieldTable::default();
    for (key, value) in envelope.headers {
        headers.insert(ShortString::from(key), AMQPValue::LongString(value.into()));
    }
    let confirm = channel
        .basic_publish(
            ShortString::from(exchange),
            ShortString::from(routing_key),
            BasicPublishOptions::default(),
            &envelope.payload,
            BasicProperties::default().with_headers(headers),
        )
        .await
        .map_err(|err| retryable("amqp publish", err))?;
    let confirmation = confirm
        .await
        .map_err(|err| retryable("amqp publisher confirm", err))?;
    if confirmation.is_nack() {
        return Err(TransportError::retryable("amqp publisher confirm: nack"));
    }
    Ok(())
}

impl MessagePublisher for RabbitPublisher {
    async fn publish(&self, message: Message) -> Result<(), TransportError> {
        // The default exchange routes to the queue named by the routing key.
        let routing_key = message.name.clone();
        publish_confirmed(&self.channel, "", &routing_key, message).await
    }
}

/// Polls one or more queues with `basic_get`, resolving the message name from
/// the delivery's routing key (stripping a configured prefix — used by
/// [`RabbitBus`](super::RabbitBus) for its `{ns}.cmd.` command queues).
///
/// Polling is *sticky*: each `recv` starts at the queue that last yielded a
/// message, so draining a busy queue costs one `basic_get` per message instead
/// of re-polling every just-empty queue each time. `recv` reports nothing
/// available after one empty cycle over all queues; an idle consumer then
/// waits [`POLL_INTERVAL`] before the next cycle.
/// How long an idle [`RabbitSource`] waits between polls.
pub const POLL_INTERVAL: Duration = Duration::from_millis(50);

pub struct RabbitSource {
    channel: Channel,
    queues: Vec<String>,
    strip_prefix: Option<String>,
    /// Index of the queue that most recently yielded a message.
    current: usize,
}

impl RabbitSource {
    /// Wrap an existing channel bound to `queue`. With the default exchange the
    /// routing key equals the queue name, so the delivered routing key is the
    /// message name consumers subscribe to.
    pub fn new(channel: Channel, queue: impl Into<String>) -> Self {
        Self::multi(channel, vec![queue.into()], None)
    }

    /// Poll several queues on one channel, optionally stripping `strip_prefix`
    /// from each delivery's routing key when deriving the message name.
    pub(super) fn multi(
        channel: Channel,
        queues: Vec<String>,
        strip_prefix: Option<String>,
    ) -> Self {
        Self {
            channel,
            queues,
            strip_prefix,
            current: 0,
        }
    }

    /// Connect, declare a durable queue, and poll it. The default exchange routes
    /// a message published with routing key == `queue` into this queue, so the
    /// queue name is the message name consumers subscribe to.
    pub async fn connect(uri: &str, queue: &str) -> Result<Self, TransportError> {
        let channel = connect_channel(uri).await?;
        channel
            .queue_declare(
                ShortString::from(queue),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|err| retryable("amqp queue_declare", err))?;
        Ok(Self::new(channel, queue))
    }
}

impl MessageSource for RabbitSource {
    type Received = RabbitReceived;

    fn transport_name(&self) -> &'static str {
        "rabbitmq"
    }

    async fn recv(&mut self) -> Result<Option<Self::Received>, TransportError> {
        for offset in 0..self.queues.len() {
            let index = (self.current + offset) % self.queues.len();
            let got = self
                .channel
                .basic_get(
                    ShortString::from(self.queues[index].as_str()),
                    BasicGetOptions::default(),
                )
                .await
                .map_err(|err| retryable("amqp basic_get", err))?;
            if let Some(get) = got {
                self.current = index;
                let routing_key = get.delivery.routing_key.to_string();
                let name = strip_address_prefix(routing_key, self.strip_prefix.as_deref());
                return Ok(Some(RabbitReceived::from_delivery_with_name(
                    get.delivery,
                    name,
                )));
            }
        }
        Ok(None)
    }

    async fn ready(&mut self) {
        tokio::time::sleep(POLL_INTERVAL).await
    }
}

/// An AMQP delivery plus its settle handle.
pub struct RabbitReceived {
    delivery: Delivery,
    message: Message,
    decode_error: Option<TransportError>,
}

impl RabbitReceived {
    /// Build from a delivery with an explicitly resolved message name. Used by
    /// [`RabbitBus`](super::RabbitBus), which derives the name from the routing
    /// key (stripping its `{ns}.cmd.` prefix for commands) rather than the queue.
    pub(super) fn from_delivery_with_name(delivery: Delivery, name: String) -> Self {
        let payload = delivery.data.clone();
        let headers: Vec<(String, String)> = delivery
            .properties
            .headers()
            .as_ref()
            .into_iter()
            .flat_map(|headers| headers.inner())
            .map(|(key, value)| (key.to_string(), amqp_value_to_string(value)))
            .collect();
        let (message, decode_error) = WireEnvelope {
            name,
            headers,
            payload,
        }
        .decode();
        Self {
            delivery,
            message,
            decode_error,
        }
    }
}

impl ReceivedMessage for RabbitReceived {
    fn message(&self) -> &Message {
        &self.message
    }

    /// AMQP reports only whether a delivery is a redelivery, so this is 1 or
    /// 2: a retry limit above 2 never triggers on RabbitMQ.
    fn attempts(&self) -> u32 {
        if self.delivery.redelivered {
            2
        } else {
            1
        }
    }

    fn decode_error(&self) -> Option<&TransportError> {
        self.decode_error.as_ref()
    }

    async fn ack(self) -> Result<(), TransportError> {
        let settled = self
            .delivery
            .ack(BasicAckOptions::default())
            .await
            .map_err(|err| retryable("amqp ack", err))?;
        settle_result("amqp ack", settled)
    }

    async fn nack(self, delay: Duration) -> Result<(), TransportError> {
        // The delivery stays unacknowledged for the delay, then is requeued.
        // If this consumer dies first, the broker requeues it at once.
        tokio::time::sleep(delay).await;
        let settled = self
            .delivery
            .nack(BasicNackOptions {
                requeue: true,
                ..Default::default()
            })
            .await
            .map_err(|err| retryable("amqp nack", err))?;
        settle_result("amqp nack", settled)
    }
}

fn amqp_value_to_string(value: &AMQPValue) -> String {
    match value {
        AMQPValue::LongString(s) => s.to_string(),
        AMQPValue::ShortString(s) => s.to_string(),
        other => format!("{other:?}"),
    }
}
