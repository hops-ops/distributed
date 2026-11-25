//! RabbitMQ (AMQP 0-9-1) [`Bus`] + [`BusConsumer`].
//!
//! RabbitMQ shines through its exchange topologies, so the two bus surfaces map
//! onto two different exchange shapes:
//!
//! - **`send` / `listen` (point-to-point, competing):** the **default exchange**
//!   routes a message to the queue named by its routing key. `send(name)`
//!   declares a durable queue `{ns}.cmd.{name}` and publishes to it; `listen`
//!   consumes those queues. Replicas sharing a queue compete (AMQP round-robins
//!   across consumers) — point-to-point.
//! - **`publish` / `subscribe` (fan-out):** a durable **topic exchange**
//!   `{ns}.events`. `publish(name)` publishes to it with routing key `name`; each
//!   `subscribe`r (keyed by `group`) declares its **own** queue `{ns}.evt.{group}`
//!   bound to the exchange for its event names. Distinct groups get distinct
//!   queues, so every group receives every event — fan-out (replicas within a
//!   group still compete on that group's queue).
//!
//! The `group` is the logical consumer identity for event subscriptions. Command
//! queues are keyed by command name; replicas compete by consuming the same command
//! queues. `{ns}` (namespace) scopes queue and exchange names so multiple apps can
//! share a broker.
//!
//! Requires the `rabbitmq` feature. Integration-tested in `tests/transports/rabbitmq_transport`.

use std::collections::HashSet;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use lapin::options::{
    BasicPublishOptions, ConfirmSelectOptions, ExchangeDeclareOptions, QueueBindOptions,
    QueueDeclareOptions,
};
use lapin::types::{FieldTable, ShortString};
use lapin::{Channel, ExchangeKind};

use super::rabbitmq::{connect_channel, message_properties, RabbitSource};
use super::{
    retryable, run_source, Bus, BusConsumer, BusTopologyConfig, MessageRouter, PublishDeadLetters,
    RunOptions, TransportError,
};
use super::{validate_message_name, Message};

/// Reject a routing/binding name a wildcard would mis-bind. A `#`/`*` in a
/// RabbitMQ topic binding key is a subscription wildcard, so an unvalidated
/// event name here would silently widen what a consumer receives.
fn checked_name(name: &str) -> Result<(), TransportError> {
    validate_message_name(name)
        .map(|_| ())
        .map_err(|e| TransportError::permanent(format!("rabbitmq message name {e}")))
}

impl Clone for RabbitBus {
    /// The clone shares the channel and starts with an empty declaration
    /// cache, so it re-declares each name once.
    fn clone(&self) -> Self {
        Self {
            uri: self.uri.clone(),
            channel: self.channel.clone(),
            topology: self.topology.clone(),
            declared: Mutex::new(HashSet::new()),
        }
    }
}

/// RabbitMQ [`Bus`] + [`BusConsumer`].
pub struct RabbitBus {
    uri: String,
    channel: Channel,
    topology: BusTopologyConfig,
    /// Queue/exchange names this process has already declared. Declarations are
    /// idempotent but cost a broker round-trip, so `send`/`publish` declare each
    /// name once instead of on every call. A benign race (two tasks declaring
    /// the same name) just declares twice.
    declared: Mutex<HashSet<String>>,
}

/// Awaitable builder returned by [`RabbitBus::connect`].
pub struct RabbitBusConnect {
    uri: String,
    topology: BusTopologyConfig,
}

impl RabbitBusConnect {
    /// Set an explicit event subscription group. Service consumers can usually
    /// omit this and use [`Service::named`](crate::microsvc::Service::named)
    /// instead.
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.topology = self.topology.group(group);
        self
    }

    /// Set the queue/exchange namespace used on the shared RabbitMQ broker.
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.topology = self.topology.namespace(namespace);
        self
    }

    async fn connect(self) -> Result<RabbitBus, TransportError> {
        RabbitBus::connect_configured(self.uri, self.topology).await
    }
}

impl IntoFuture for RabbitBusConnect {
    type Output = Result<RabbitBus, TransportError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.connect())
    }
}

impl RabbitBus {
    /// Start building a bus connected to an AMQP URI.
    ///
    /// The returned builder is awaitable:
    ///
    /// ```ignore
    /// let bus = RabbitBus::connect("amqp://localhost:5672/%2f")
    ///     .namespace("todos-prod")
    ///     .await?;
    /// ```
    pub fn connect(uri: &str) -> RabbitBusConnect {
        RabbitBusConnect {
            uri: uri.to_string(),
            topology: BusTopologyConfig::default(),
        }
    }

    /// Connect with an explicit group and namespace for direct/low-level use.
    pub async fn connect_with(
        uri: &str,
        group: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Result<Self, TransportError> {
        Self::connect(uri).group(group).namespace(namespace).await
    }

    async fn connect_configured(
        uri: String,
        topology: BusTopologyConfig,
    ) -> Result<Self, TransportError> {
        let topology = topology.validate_for("rabbitmq")?;
        let channel = connect_channel(&uri).await?;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(|err| retryable("amqp confirm_select", err))?;
        let bus = Self {
            uri,
            channel,
            topology,
            declared: Mutex::new(HashSet::new()),
        };
        // Declare the events exchange once up front so the common publish path
        // never pays a declare round-trip.
        let exchange = bus.events_exchange()?;
        bus.declare_events_exchange(&bus.channel, &exchange).await?;
        bus.mark_declared(&exchange);
        Ok(bus)
    }

    /// Set an explicit event subscription group on an already-built bus.
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.topology = self.topology.group(group);
        self
    }

    /// Set the queue/exchange namespace used on the shared RabbitMQ broker.
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.topology = self.topology.namespace(namespace);
        self
    }

    fn validated_namespace(&self) -> Result<String, TransportError> {
        self.topology.namespace_for("rabbitmq")
    }

    fn command_queue(&self, name: &str) -> Result<String, TransportError> {
        Ok(format!("{}.cmd.{name}", self.validated_namespace()?))
    }

    fn command_prefix(&self) -> Result<String, TransportError> {
        Ok(format!("{}.cmd.", self.validated_namespace()?))
    }

    fn events_exchange(&self) -> Result<String, TransportError> {
        Ok(format!("{}.events", self.validated_namespace()?))
    }

    fn group_queue(&self, group: &str) -> Result<String, TransportError> {
        Ok(format!("{}.evt.{group}", self.validated_namespace()?))
    }

    /// Whether this process already declared `name`. Never poisoned in
    /// practice (the guarded section runs no user code); recover defensively.
    fn already_declared(&self, name: &str) -> bool {
        self.declared
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(name)
    }

    fn mark_declared(&self, name: &str) {
        self.declared
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(name.to_string());
    }

    /// Declare the command queue once per process; later sends skip the
    /// broker round-trip.
    async fn ensure_command_queue(&self, queue: &str) -> Result<(), TransportError> {
        if !self.already_declared(queue) {
            self.declare_queue(&self.channel, queue).await?;
            self.mark_declared(queue);
        }
        Ok(())
    }

    /// Declare the events exchange once per process (re-declared only when
    /// [`namespace`](Self::namespace) changes the exchange name after connect).
    async fn ensure_events_exchange(&self, exchange: &str) -> Result<(), TransportError> {
        if !self.already_declared(exchange) {
            self.declare_events_exchange(&self.channel, exchange)
                .await?;
            self.mark_declared(exchange);
        }
        Ok(())
    }

    async fn declare_queue(&self, channel: &Channel, queue: &str) -> Result<(), TransportError> {
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
        Ok(())
    }

    async fn declare_events_exchange(
        &self,
        channel: &Channel,
        exchange: &str,
    ) -> Result<(), TransportError> {
        channel
            .exchange_declare(
                ShortString::from(exchange),
                ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|err| retryable("amqp exchange_declare", err))?;
        Ok(())
    }

    async fn publish_confirmed(
        &self,
        exchange: &str,
        routing_key: &str,
        message: &Message,
    ) -> Result<(), TransportError> {
        let confirm = self
            .channel
            .basic_publish(
                ShortString::from(exchange),
                ShortString::from(routing_key),
                BasicPublishOptions::default(),
                &message.payload,
                message_properties(message),
            )
            .await
            .map_err(|err| retryable("amqp publish", err))?;
        if confirm
            .await
            .map_err(|err| retryable("amqp publisher confirm", err))?
            .is_nack()
        {
            return Err(TransportError::retryable("amqp publisher confirm: nack"));
        }
        Ok(())
    }

    /// Declare the topic exchange, this group's queue, and bind it to the
    /// service's event names — the durable setup `subscribe` needs. Exposed so a
    /// producer can ensure all subscriber bindings exist *before* publishing
    /// (RabbitMQ drops events with no matching binding).
    pub async fn ensure_subscription<R: MessageRouter>(
        &self,
        router: &R,
    ) -> Result<(), TransportError> {
        let plan = router.subscription_plan();
        if plan.events.is_empty() {
            return Ok(());
        }
        let group = self.topology.resolve_consumer_group(router, "rabbitmq")?;
        let exchange = self.events_exchange()?;
        self.ensure_events_exchange(&exchange).await?;
        let queue = self.group_queue(&group)?;
        self.declare_queue(&self.channel, &queue).await?;
        for name in &plan.events {
            checked_name(name)?;
            self.channel
                .queue_bind(
                    ShortString::from(queue.as_str()),
                    ShortString::from(exchange.as_str()),
                    ShortString::from(name.as_str()),
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await
                .map_err(|err| retryable("amqp queue_bind", err))?;
        }
        Ok(())
    }
}

impl Bus for RabbitBus {
    async fn send_message(&self, mut message: Message) -> Result<(), TransportError> {
        checked_name(message.name())?;
        // Default exchange routes by routing key == queue name; declare the queue
        // (once per process) so the command is retained until a listener consumes it.
        let queue = self.command_queue(message.name())?;
        self.ensure_command_queue(&queue).await?;
        message.name = queue.clone();
        self.publish_confirmed("", &queue, &message).await
    }

    async fn publish_message(&self, message: Message) -> Result<(), TransportError> {
        checked_name(message.name())?;
        let exchange = self.events_exchange()?;
        self.ensure_events_exchange(&exchange).await?;
        let routing_key = message.name().to_string();
        self.publish_confirmed(&exchange, &routing_key, &message)
            .await
    }
}

impl BusConsumer for RabbitBus {
    async fn listen<R: MessageRouter>(
        &self,
        router: Arc<R>,
        options: RunOptions,
    ) -> Result<(), TransportError> {
        let plan = router.subscription_plan();
        if plan.commands.is_empty() {
            return Ok(());
        }
        let channel = connect_channel(&self.uri).await?;
        let mut queues = Vec::new();
        for name in &plan.commands {
            let queue = self.command_queue(name)?;
            self.declare_queue(&channel, &queue).await?;
            queues.push(queue);
        }
        let source = RabbitSource::multi(channel, queues, Some(self.command_prefix()?));
        let options = options.or_dead_letters(|| Arc::new(PublishDeadLetters(self.clone())));
        run_source(router, source, options).await
    }

    async fn subscribe<R: MessageRouter>(
        &self,
        router: Arc<R>,
        options: RunOptions,
    ) -> Result<(), TransportError> {
        if router.subscription_plan().events.is_empty() {
            return Ok(());
        }
        self.ensure_subscription(router.as_ref()).await?;
        let group = self
            .topology
            .resolve_consumer_group(router.as_ref(), "rabbitmq")?;
        let channel = connect_channel(&self.uri).await?;
        // Events are published with routing key == the bare event name.
        let source = RabbitSource::multi(channel, vec![self.group_queue(&group)?], None);
        let options = options.or_dead_letters(|| Arc::new(PublishDeadLetters(self.clone())));
        run_source(router, source, options).await
    }
}
