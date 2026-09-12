//! Command invokers: direct dispatch, HTTP, and bus send+completion.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[cfg(feature = "kafka")]
use distributed::bus::KafkaBus;
#[cfg(feature = "rabbitmq")]
use distributed::bus::RabbitBus;
use distributed::bus::{
    Bus, BusConsumer, InMemoryBus, Message, MessageKind, MessageRouter, NatsBus, OrderedDelivery,
    PostgresBus, RunOptions, SqliteBus, SubscriptionPlan, TransportError,
};
use distributed::microsvc::grpc::{CommandServiceClient, GrpcRequest};
use distributed::microsvc::{Service, Session};
use serde_json::Value;
use tokio::sync::{oneshot, Notify};
use tonic::transport::Channel;
use uuid::Uuid;

use crate::host::{INCREMENT, INITIALIZE};

pub type InvokeError = String;

#[derive(Clone)]
pub enum Invoker {
    Direct(Arc<Service>),
    Http {
        client: reqwest::Client,
        base: String,
    },
    Grpc(CommandServiceClient<Channel>),
    Bus(BusInvoker),
}

impl Invoker {
    pub async fn invoke(&self, command: &str, body: Value) -> Result<(), InvokeError> {
        match self {
            Self::Direct(service) => service
                .dispatch(command, body, Session::new())
                .await
                .map(|_| ())
                .map_err(|e| e.to_string()),
            Self::Http { client, base } => {
                let resp = client
                    .post(format!("{base}/{command}"))
                    .json(&body)
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;
                if resp.status().is_success() {
                    Ok(())
                } else {
                    Err(format!(
                        "{} {}",
                        resp.status(),
                        resp.text().await.unwrap_or_default()
                    ))
                }
            }
            Self::Grpc(client) => {
                let mut client = client.clone();
                let resp = client
                    .dispatch(GrpcRequest {
                        command: command.to_string(),
                        input: body.to_string(),
                        session_variables: Default::default(),
                    })
                    .await
                    .map_err(|e| e.to_string())?
                    .into_inner();
                if resp.status == 200 {
                    Ok(())
                } else {
                    Err(format!("grpc status {} {}", resp.status, resp.body))
                }
            }
            Self::Bus(bus) => bus.invoke(command, body).await,
        }
    }
}

#[derive(Clone)]
pub struct BusInvoker {
    live: LiveBus,
    pending: CompletionMap,
    notify: Arc<Notify>,
    pipelined: bool,
    pub applied_ok: Arc<AtomicU64>,
    pub applied_err: Arc<AtomicU64>,
}

type CompletionMap = Arc<Mutex<HashMap<String, oneshot::Sender<Result<(), String>>>>>;

impl BusInvoker {
    async fn invoke(&self, command: &str, body: Value) -> Result<(), InvokeError> {
        let id = Uuid::now_v7().to_string();
        let rx = if self.pipelined {
            None
        } else {
            let (tx, rx) = oneshot::channel();
            self.pending
                .lock()
                .expect("completion map")
                .insert(id.clone(), tx);
            Some(rx)
        };
        let payload = serde_json::to_vec(&body).map_err(|e| e.to_string())?;
        let message = Message::new(command, MessageKind::Command, payload).with_id(id.clone());
        if let Err(e) = self.live.send(message).await {
            self.pending.lock().expect("completion map").remove(&id);
            return Err(e.to_string());
        }
        self.notify.notify_waiters();
        if self.pipelined {
            return Ok(());
        }
        let rx = rx.expect("applied mode registers a completion channel");
        match tokio::time::timeout(Duration::from_secs(15), rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                self.pending.lock().expect("completion map").remove(&id);
                Err("bus completion dropped".into())
            }
            Err(_) => {
                self.pending.lock().expect("completion map").remove(&id);
                Err("bus completion timed out".into())
            }
        }
    }
}

#[derive(Clone)]
enum LiveBus {
    Memory(InMemoryBus),
    Sqlite(SqliteBus),
    Postgres(PostgresBus),
    Nats(NatsBus),
    #[cfg(feature = "kafka")]
    Kafka(KafkaBus),
    #[cfg(feature = "rabbitmq")]
    Rabbit(std::sync::Arc<RabbitBus>),
}

impl LiveBus {
    async fn send(&self, message: Message) -> Result<(), TransportError> {
        match self {
            Self::Memory(bus) => bus.send_message(message).await,
            Self::Sqlite(bus) => bus.send_message(message).await,
            Self::Postgres(bus) => bus.send_message(message).await,
            Self::Nats(bus) => bus.send_message(message).await,
            #[cfg(feature = "kafka")]
            Self::Kafka(bus) => bus.send_message(message).await,
            #[cfg(feature = "rabbitmq")]
            Self::Rabbit(bus) => bus.send_message(message).await,
        }
    }

    async fn listen(
        &self,
        router: Arc<CompletingRouter>,
        options: RunOptions,
    ) -> Result<(), TransportError> {
        match self {
            Self::Memory(bus) => bus.listen(router, options).await,
            Self::Sqlite(bus) => bus.listen(router, options).await,
            Self::Postgres(bus) => bus.listen(router, options).await,
            Self::Nats(bus) => bus.listen(router, options).await,
            #[cfg(feature = "kafka")]
            Self::Kafka(bus) => bus.listen(router, options).await,
            #[cfg(feature = "rabbitmq")]
            Self::Rabbit(bus) => bus.listen(router, options).await,
        }
    }
}

struct CompletingRouter {
    service: Arc<Service>,
    pending: CompletionMap,
    applied_ok: Arc<AtomicU64>,
    applied_err: Arc<AtomicU64>,
}

impl MessageRouter for CompletingRouter {
    fn consumer_group(&self) -> Option<&str> {
        self.service.consumer_group()
    }

    fn handles(&self, kind: MessageKind, name: &str) -> bool {
        self.service.handles_message(kind, name)
    }

    fn subscription_plan(&self) -> SubscriptionPlan {
        self.service.subscription_plan()
    }

    async fn dispatch(&self, message: &Message) -> Result<(), TransportError> {
        let result = MessageRouter::dispatch(self.service.as_ref(), message).await;
        complete(
            &self.pending,
            &self.applied_ok,
            &self.applied_err,
            message,
            &result,
        );
        result
    }

    async fn dispatch_ordered(
        &self,
        message: &Message,
        ordered: Option<&OrderedDelivery>,
    ) -> Result<(), TransportError> {
        let result = self.service.dispatch_ordered(message, ordered).await;
        complete(
            &self.pending,
            &self.applied_ok,
            &self.applied_err,
            message,
            &result,
        );
        result
    }
}

fn complete(
    pending: &CompletionMap,
    applied_ok: &AtomicU64,
    applied_err: &AtomicU64,
    message: &Message,
    result: &Result<(), TransportError>,
) {
    match result {
        Ok(()) => {
            applied_ok.fetch_add(1, Ordering::Relaxed);
        }
        Err(_) => {
            applied_err.fetch_add(1, Ordering::Relaxed);
        }
    }
    let Some(id) = message.id() else {
        return;
    };
    if let Some(tx) = pending.lock().expect("completion map").remove(id) {
        let mapped = result.as_ref().map(|_| ()).map_err(|e| e.to_string());
        let _ = tx.send(mapped);
    }
}

pub struct BusRuntime {
    pub invoker: BusInvoker,
    consumer: tokio::task::JoinHandle<()>,
}

impl BusRuntime {
    pub fn start(live: impl Into<LiveBusWrap>, service: Arc<Service>, pipelined: bool) -> Self {
        let live = live.into().0;
        let pending: CompletionMap = Arc::new(Mutex::new(HashMap::new()));
        let notify = Arc::new(Notify::new());
        let applied_ok = Arc::new(AtomicU64::new(0));
        let applied_err = Arc::new(AtomicU64::new(0));
        let router = Arc::new(CompletingRouter {
            service,
            pending: Arc::clone(&pending),
            applied_ok: Arc::clone(&applied_ok),
            applied_err: Arc::clone(&applied_err),
        });
        let consumer_bus = live.clone();
        let consumer = tokio::spawn(async move {
            if let Err(e) = consumer_bus
                .listen(router, RunOptions::idempotent().wait_when_idle())
                .await
            {
                eprintln!("load-suite bus consumer: {e}");
            }
        });
        Self {
            invoker: BusInvoker {
                live,
                pending,
                notify,
                pipelined,
                applied_ok,
                applied_err,
            },
            consumer,
        }
    }

    pub fn stop(&self) {
        self.consumer.abort();
        self.invoker.notify.notify_waiters();
    }
}

pub struct LiveBusWrap(LiveBus);

impl From<InMemoryBus> for LiveBusWrap {
    fn from(bus: InMemoryBus) -> Self {
        Self(LiveBus::Memory(bus))
    }
}
impl From<SqliteBus> for LiveBusWrap {
    fn from(bus: SqliteBus) -> Self {
        Self(LiveBus::Sqlite(bus))
    }
}
impl From<PostgresBus> for LiveBusWrap {
    fn from(bus: PostgresBus) -> Self {
        Self(LiveBus::Postgres(bus))
    }
}
impl From<NatsBus> for LiveBusWrap {
    fn from(bus: NatsBus) -> Self {
        Self(LiveBus::Nats(bus))
    }
}
#[cfg(feature = "kafka")]
impl From<KafkaBus> for LiveBusWrap {
    fn from(bus: KafkaBus) -> Self {
        Self(LiveBus::Kafka(bus))
    }
}
#[cfg(feature = "rabbitmq")]
impl From<RabbitBus> for LiveBusWrap {
    fn from(bus: RabbitBus) -> Self {
        Self(LiveBus::Rabbit(std::sync::Arc::new(bus)))
    }
}

pub async fn setup_hot_id(invoker: &Invoker) -> Result<String, InvokeError> {
    let id = format!("hot-{}", Uuid::now_v7());
    invoker
        .invoke(INITIALIZE, serde_json::json!({ "id": id }))
        .await?;
    Ok(id)
}

pub fn command_body(scenario: crate::client::Scenario, hot_id: Option<&str>) -> (String, Value) {
    match scenario {
        crate::client::Scenario::UniqueCreate => {
            let id = Uuid::now_v7().to_string();
            (INITIALIZE.into(), serde_json::json!({ "id": id }))
        }
        crate::client::Scenario::HotIncrement => (
            INCREMENT.into(),
            serde_json::json!({ "id": hot_id.unwrap_or("hot"), "amount": 1 }),
        ),
    }
}
