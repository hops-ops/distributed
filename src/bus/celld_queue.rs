//! celld Queue producer and relay boundary.
//!
//! A celld Queue is a durable, single-consumer spool. It deliberately is not a
//! [`Bus`](super::Bus): Queue does not provide event fanout or command routing.
//! Aggregate-cell outboxes publish canonical [`Message`]s here, then a queue
//! consumer relays each message through a real bus (for example
//! `BusPublisher<NatsBus>` or `BusPublisher<KnativeBus>`) where direct commands
//! and fanout events diverge.
//!
//! Queue acceptance and aggregate-cell persistence are separate cell commits.
//! celld's output gate prevents the queue send from leaving before the
//! aggregate cell's preceding writes are durable. Duplicate delivery remains
//! possible if Queue accepts a message and the outbox settlement is interrupted;
//! the stable message id is therefore required end to end.

use crate::bus::{validate_stable_message_id, Message, MessagePublisher, TransportError};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Version of the JSON body stored in celld Queue.
pub const CELLD_QUEUE_ENVELOPE_VERSION: u16 = 1;

/// celld Queue's maximum serialized message body.
pub const CELLD_QUEUE_MAX_BODY_BYTES: usize = 128 * 1024;

/// Conventional authenticated native relay endpoint used by Queue consumers.
pub const CELLD_QUEUE_RELAY_PATH: &str = "/internal/celld-queue/relay";

/// Versioned Queue body carrying Distributed's canonical transport message.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CelldQueueEnvelope {
    pub version: u16,
    pub message: Message,
}

impl CelldQueueEnvelope {
    /// Validate and wrap a canonical message for celld Queue.
    pub fn new(message: Message) -> Result<Self, TransportError> {
        message.validate_name().map_err(|error| {
            TransportError::permanent(format!("invalid celld Queue message name: {error}"))
        })?;
        let id = message.id().ok_or_else(|| {
            TransportError::permanent("celld Queue messages require a stable message id")
        })?;
        validate_stable_message_id(Some(id)).map_err(|error| {
            TransportError::permanent(format!("invalid celld Queue message id: {error}"))
        })?;

        let envelope = Self {
            version: CELLD_QUEUE_ENVELOPE_VERSION,
            message,
        };
        envelope.validate_wire_size()?;
        Ok(envelope)
    }

    /// Validate a received envelope before it reaches the downstream bus.
    pub fn into_message(self) -> Result<Message, TransportError> {
        if self.version != CELLD_QUEUE_ENVELOPE_VERSION {
            return Err(TransportError::permanent(format!(
                "unsupported celld Queue envelope version {}",
                self.version
            )));
        }
        Self::new(self.message).map(|envelope| envelope.message)
    }

    fn validate_wire_size(&self) -> Result<(), TransportError> {
        let size = serde_json::to_vec(self)
            .map_err(|error| {
                TransportError::permanent(format!("cannot serialize celld Queue message: {error}"))
            })?
            .len();
        if size > CELLD_QUEUE_MAX_BODY_BYTES {
            return Err(TransportError::permanent(format!(
                "celld Queue message is {size} bytes; maximum is {CELLD_QUEUE_MAX_BODY_BYTES}"
            )));
        }
        Ok(())
    }
}

/// Relays Queue bodies through any Distributed publisher.
///
/// Wrap a [`crate::BusPublisher`] to recover the bus topology split: commands
/// go to `send_message`, while events go to `publish_message` for fanout.
#[derive(Clone)]
pub struct CelldQueueRelay<P> {
    publisher: P,
}

/// Type-erased native ingress handler for a celld Queue delivery.
pub type CelldQueueRelayHandler = Arc<
    dyn Fn(
            CelldQueueEnvelope,
        ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + 'static>>
        + Send
        + Sync,
>;

/// Build a native relay handler over any Distributed publisher.
///
/// Passing `BusPublisher<NatsBus>`, `BusPublisher<KafkaBus>`,
/// `BusPublisher<RabbitBus>`, or `BusPublisher<KnativeBus>` changes only the
/// final transport. The Queue ingress and envelope stay identical.
pub fn celld_queue_relay_handler<P>(publisher: P) -> CelldQueueRelayHandler
where
    P: MessagePublisher + Send + Sync + 'static,
{
    let relay = Arc::new(CelldQueueRelay::new(publisher));
    Arc::new(move |envelope| {
        let relay = Arc::clone(&relay);
        Box::pin(async move { relay.relay(envelope).await })
    })
}

impl<P> CelldQueueRelay<P>
where
    P: MessagePublisher,
{
    pub fn new(publisher: P) -> Self {
        Self { publisher }
    }

    pub fn publisher(&self) -> &P {
        &self.publisher
    }

    /// Publish one validated Queue delivery. The Queue consumer should ack only
    /// after this resolves `Ok`; errors should be retried by Queue policy.
    pub async fn relay(&self, envelope: CelldQueueEnvelope) -> Result<(), TransportError> {
        self.publisher.publish(envelope.into_message()?).await
    }
}

/// workers-rs adapter that durably accepts messages into a celld Queue.
#[cfg(feature = "workers-rs")]
#[derive(Clone)]
pub struct CelldQueuePublisher {
    queue: worker::Queue,
}

/// Worker-side HTTP hop to a native [`CelldQueueRelayHandler`].
///
/// This is useful when the final bus client cannot run in Workers WASM (native
/// NATS, Kafka, or RabbitMQ clients). A successful 2xx response is the publish
/// threshold; Queue acknowledges only after the native bus has accepted the
/// canonical message.
#[cfg(all(feature = "workers-rs", target_arch = "wasm32"))]
#[derive(Clone)]
pub struct CelldQueueHttpPublisher {
    endpoint: String,
    headers: Vec<(String, String)>,
}

#[cfg(all(feature = "workers-rs", any(target_arch = "wasm32", test)))]
#[derive(Clone, Copy)]
enum CelldQueueHttpEndpointPolicy {
    HttpsOnly,
    LocalTestHttp,
    MeshHttp,
}

#[cfg(all(feature = "workers-rs", any(target_arch = "wasm32", test)))]
fn validate_celld_queue_http_endpoint(
    endpoint: &str,
    policy: CelldQueueHttpEndpointPolicy,
    test_credential: Option<&str>,
) -> Result<(), TransportError> {
    let url = worker::Url::parse(endpoint)
        .map_err(|error| TransportError::permanent(format!("invalid Queue relay URL: {error}")))?;
    if url.host_str().is_none() || !url.username().is_empty() || url.password().is_some() {
        return Err(TransportError::permanent(
            "Queue relay URL must have a host and must not contain credentials",
        ));
    }

    match policy {
        CelldQueueHttpEndpointPolicy::HttpsOnly if url.scheme() == "https" => Ok(()),
        CelldQueueHttpEndpointPolicy::HttpsOnly => {
            Err(TransportError::permanent("Queue relay URL must use HTTPS"))
        }
        CelldQueueHttpEndpointPolicy::LocalTestHttp => {
            let host = url.host_str().unwrap_or_default();
            let local_host = matches!(host, "127.0.0.1" | "localhost" | "::1" | "[::1]");
            let test_credential = test_credential.unwrap_or_default();
            if url.scheme() != "http" || !local_host {
                return Err(TransportError::permanent(
                    "local-test Queue relay URL must use HTTP on an approved local host",
                ));
            }
            if !test_credential.starts_with("test-only-") || test_credential.len() < 24 {
                return Err(TransportError::permanent(
                    "local-test Queue relay requires an explicit test-only credential",
                ));
            }
            Ok(())
        }
        CelldQueueHttpEndpointPolicy::MeshHttp if url.scheme() == "https" => Ok(()),
        CelldQueueHttpEndpointPolicy::MeshHttp => {
            let host = url.host_str().unwrap_or_default();
            let cluster_service = host.ends_with(".svc.cluster.local") || host.ends_with(".svc");
            if url.scheme() != "http" || !cluster_service {
                return Err(TransportError::permanent(
                    "mesh Queue relay URL must use HTTPS or HTTP to a cluster Service (*.svc)",
                ));
            }
            Ok(())
        }
    }
}

#[cfg(all(feature = "workers-rs", any(target_arch = "wasm32", test)))]
fn celld_queue_http_redirect_policy() -> worker::RequestRedirect {
    // Credential-bearing relay requests never follow redirects. A redirect is
    // a transport failure; operators must configure the final HTTPS endpoint.
    worker::RequestRedirect::Error
}

#[cfg(all(feature = "workers-rs", target_arch = "wasm32"))]
impl CelldQueueHttpPublisher {
    /// Build a relay publisher that sends credentials only over HTTPS.
    pub fn new(endpoint: impl Into<String>) -> Result<Self, TransportError> {
        let endpoint = endpoint.into();
        validate_celld_queue_http_endpoint(
            &endpoint,
            CelldQueueHttpEndpointPolicy::HttpsOnly,
            None,
        )?;
        Ok(Self {
            endpoint,
            headers: Vec::new(),
        })
    }

    /// Build an HTTP publisher for explicit loopback test fixtures.
    ///
    /// This never permits arbitrary cleartext endpoints or production-looking
    /// credentials. Production callers must use [`Self::new`] with HTTPS.
    pub fn new_local_test(
        endpoint: impl Into<String>,
        test_credential: &str,
    ) -> Result<Self, TransportError> {
        let endpoint = endpoint.into();
        validate_celld_queue_http_endpoint(
            &endpoint,
            CelldQueueHttpEndpointPolicy::LocalTestHttp,
            Some(test_credential),
        )?;
        Ok(Self {
            endpoint,
            headers: Vec::new(),
        })
    }

    /// Build a publisher for a relay inside a service mesh that encrypts
    /// pod-to-pod traffic (for example Istio ambient mTLS).
    ///
    /// Cleartext is allowed only to a cluster Service host
    /// (`*.svc.cluster.local` or `*.svc`); anything else must be HTTPS. The
    /// worker cannot see the mesh: choosing this asserts that the celld pod
    /// and the relay's pods are both enrolled in it.
    pub fn new_mesh(endpoint: impl Into<String>) -> Result<Self, TransportError> {
        let endpoint = endpoint.into();
        validate_celld_queue_http_endpoint(
            &endpoint,
            CelldQueueHttpEndpointPolicy::MeshHttp,
            None,
        )?;
        Ok(Self {
            endpoint,
            headers: Vec::new(),
        })
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

#[cfg(all(feature = "workers-rs", target_arch = "wasm32"))]
impl MessagePublisher for CelldQueueHttpPublisher {
    fn publish(
        &self,
        message: Message,
    ) -> impl Future<Output = Result<(), TransportError>> + Send + '_ {
        let endpoint = self.endpoint.clone();
        let extra_headers = self.headers.clone();
        worker::send::SendFuture::new(async move {
            let envelope = CelldQueueEnvelope::new(message)?;
            let body = serde_json::to_string(&envelope).map_err(|error| {
                TransportError::permanent(format!("cannot encode Queue relay body: {error}"))
            })?;
            let headers = worker::Headers::new();
            headers
                .set("content-type", "application/json")
                .map_err(|error| {
                    TransportError::permanent(format!(
                        "cannot set Queue relay content type: {error}"
                    ))
                })?;
            for (name, value) in extra_headers {
                headers.set(&name, &value).map_err(|error| {
                    TransportError::permanent(format!(
                        "cannot set Queue relay header `{name}`: {error}"
                    ))
                })?;
            }
            let mut init = worker::RequestInit::new();
            init.with_method(worker::Method::Post)
                .with_headers(headers)
                .with_redirect(celld_queue_http_redirect_policy())
                .with_body(Some(worker::wasm_bindgen::JsValue::from_str(&body)));
            let request = worker::Request::new_with_init(&endpoint, &init).map_err(|error| {
                TransportError::permanent(format!("cannot build Queue relay request: {error}"))
            })?;
            let response = worker::Fetch::Request(request)
                .send()
                .await
                .map_err(|error| {
                    TransportError::retryable(format!("Queue relay fetch failed: {error}"))
                })?;
            if !(200..300).contains(&response.status_code()) {
                return Err(TransportError::retryable(format!(
                    "Queue relay returned HTTP {}",
                    response.status_code()
                )));
            }
            Ok(())
        })
    }
}

#[cfg(feature = "workers-rs")]
impl CelldQueuePublisher {
    pub fn new(queue: worker::Queue) -> Self {
        Self { queue }
    }

    /// Resolve a Queue binding from the Worker environment.
    pub fn from_env(env: &worker::Env, binding: &str) -> Result<Self, TransportError> {
        #[cfg(target_arch = "wasm32")]
        {
            use worker::wasm_bindgen::{JsCast, JsValue};

            // workers-rs uses `instanceof WorkerQueue`, but celld's compatible
            // Queue stub is structural and does not share Cloudflare's JS
            // constructor identity. Resolve the configured binding and perform
            // the same unchecked wrapper conversion workers-rs uses after its
            // nominal check. Queue methods still fail normally if celld ever
            // stops supplying the promised send/sendBatch surface.
            let value = js_sys::Reflect::get(env, &JsValue::from_str(binding)).map_err(|_| {
                TransportError::permanent(format!("celld Queue binding `{binding}` is unavailable"))
            })?;
            if value.is_undefined() || value.is_null() {
                return Err(TransportError::permanent(format!(
                    "celld Queue binding `{binding}` is unavailable"
                )));
            }
            Ok(Self::new(worker::Queue::unchecked_from_js(value)))
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            env.queue(binding).map(Self::new).map_err(|error| {
                TransportError::permanent(format!(
                    "celld Queue binding `{binding}` is unavailable: {error}"
                ))
            })
        }
    }
}

#[cfg(feature = "workers-rs")]
impl MessagePublisher for CelldQueuePublisher {
    fn publish(
        &self,
        message: Message,
    ) -> impl std::future::Future<Output = Result<(), TransportError>> + Send + '_ {
        let queue = self.queue.clone();
        worker::send::SendFuture::new(async move {
            let envelope = CelldQueueEnvelope::new(message)?;
            queue.send(envelope).await.map_err(|error| {
                TransportError::retryable(format!("celld Queue send was not confirmed: {error}"))
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::{Bus, MessageKind};
    use crate::outbox_worker::testing::block_on;
    use crate::BusPublisher;
    use std::sync::{Arc, Mutex};

    #[cfg(feature = "workers-rs")]
    #[test]
    fn relay_http_endpoint_requires_https_or_explicit_local_test_mode() {
        assert!(validate_celld_queue_http_endpoint(
            "https://relay.example.test/internal/celld-queue/relay",
            CelldQueueHttpEndpointPolicy::HttpsOnly,
            None,
        )
        .is_ok());
        assert!(validate_celld_queue_http_endpoint(
            "http://relay.example.test/internal/celld-queue/relay",
            CelldQueueHttpEndpointPolicy::HttpsOnly,
            None,
        )
        .is_err());
        assert!(validate_celld_queue_http_endpoint(
            "http://127.0.0.1:8791/internal/celld-queue/relay",
            CelldQueueHttpEndpointPolicy::LocalTestHttp,
            Some("test-only-internal-secret-change-me"),
        )
        .is_ok());
        assert!(validate_celld_queue_http_endpoint(
            "http://relay.example.test/internal/celld-queue/relay",
            CelldQueueHttpEndpointPolicy::LocalTestHttp,
            Some("test-only-internal-secret-change-me"),
        )
        .is_err());
        assert!(validate_celld_queue_http_endpoint(
            "http://localhost:8791/internal/celld-queue/relay",
            CelldQueueHttpEndpointPolicy::LocalTestHttp,
            Some("production-secret"),
        )
        .is_err());
    }

    #[cfg(feature = "workers-rs")]
    #[test]
    fn mesh_relay_allows_http_only_to_cluster_services() {
        let mesh = |endpoint| {
            validate_celld_queue_http_endpoint(
                endpoint,
                CelldQueueHttpEndpointPolicy::MeshHttp,
                None,
            )
        };
        assert!(mesh("http://relay.ns.svc.cluster.local:2567/internal/celld-queue/relay").is_ok());
        assert!(mesh("http://relay.ns.svc:2567/internal/celld-queue/relay").is_ok());
        assert!(mesh("https://relay.example.test/internal/celld-queue/relay").is_ok());
        assert!(mesh("http://relay.example.test/internal/celld-queue/relay").is_err());
        assert!(mesh("http://127.0.0.1:2567/internal/celld-queue/relay").is_err());
        assert!(mesh("http://svc.cluster.local.example.test/relay").is_err());
        assert!(mesh("http://user:pw@relay.ns.svc/relay").is_err());
    }

    #[derive(Clone, Default)]
    struct RecordingPublisher {
        messages: Arc<Mutex<Vec<Message>>>,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum BusCall {
        Send(String),
        Publish(String),
    }

    #[derive(Default)]
    struct RecordingBus {
        calls: Mutex<Vec<BusCall>>,
    }

    impl Bus for RecordingBus {
        async fn send_message(&self, message: Message) -> Result<(), TransportError> {
            self.calls.lock().unwrap().push(BusCall::Send(message.name));
            Ok(())
        }

        async fn publish_message(&self, message: Message) -> Result<(), TransportError> {
            self.calls
                .lock()
                .unwrap()
                .push(BusCall::Publish(message.name));
            Ok(())
        }
    }

    impl MessagePublisher for RecordingPublisher {
        async fn publish(&self, message: Message) -> Result<(), TransportError> {
            self.messages.lock().unwrap().push(message);
            Ok(())
        }
    }

    fn message(kind: MessageKind) -> Message {
        Message::new("todo.created", kind, b"{}".to_vec())
            .with_id("0190a000-0000-7000-8000-000000000201")
    }

    #[test]
    fn envelope_requires_stable_id_and_enforces_queue_limit() {
        let missing = Message::new("todo.created", MessageKind::Event, b"{}".to_vec());
        assert!(CelldQueueEnvelope::new(missing).is_err());

        let oversized = Message::new(
            "todo.created",
            MessageKind::Event,
            vec![b'x'; CELLD_QUEUE_MAX_BODY_BYTES],
        )
        .with_id("event-oversized");
        assert!(CelldQueueEnvelope::new(oversized).is_err());
    }

    #[test]
    fn envelope_round_trips_canonical_message() {
        let envelope = CelldQueueEnvelope::new(message(MessageKind::Event)).unwrap();
        let encoded = serde_json::to_vec(&envelope).unwrap();
        let decoded: CelldQueueEnvelope = serde_json::from_slice(&encoded).unwrap();
        let decoded = decoded.into_message().unwrap();
        assert_eq!(decoded.id(), Some("0190a000-0000-7000-8000-000000000201"));
        assert_eq!(decoded.name(), "todo.created");
        assert_eq!(decoded.kind, MessageKind::Event);
    }

    #[test]
    fn relay_preserves_message_for_downstream_bus_publisher() {
        let publisher = RecordingPublisher::default();
        let recorded = Arc::clone(&publisher.messages);
        let relay = CelldQueueRelay::new(publisher);
        block_on(relay.relay(CelldQueueEnvelope::new(message(MessageKind::Command)).unwrap()))
            .unwrap();

        let messages = recorded.lock().unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].kind, MessageKind::Command);
        assert_eq!(messages[0].name(), "todo.created");
    }

    #[test]
    fn generic_relay_uses_any_bus_direct_and_fanout_paths() {
        let bus = Arc::new(RecordingBus::default());
        let relay = CelldQueueRelay::new(BusPublisher::new(Arc::clone(&bus)));
        block_on(relay.relay(CelldQueueEnvelope::new(message(MessageKind::Command)).unwrap()))
            .unwrap();
        block_on(relay.relay(CelldQueueEnvelope::new(message(MessageKind::Event)).unwrap()))
            .unwrap();

        assert_eq!(
            *bus.calls.lock().unwrap(),
            vec![
                BusCall::Send("todo.created".to_string()),
                BusCall::Publish("todo.created".to_string()),
            ]
        );
    }

    #[test]
    fn relay_rejects_unknown_envelope_version() {
        let relay = CelldQueueRelay::new(RecordingPublisher::default());
        let mut envelope = CelldQueueEnvelope::new(message(MessageKind::Event)).unwrap();
        envelope.version += 1;
        assert!(block_on(relay.relay(envelope)).is_err());
    }

    #[cfg(feature = "workers-rs")]
    #[test]
    fn relay_http_requests_never_follow_redirects() {
        assert!(matches!(
            celld_queue_http_redirect_policy(),
            worker::RequestRedirect::Error
        ));
    }
}
