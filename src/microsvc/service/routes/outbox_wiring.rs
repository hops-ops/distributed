use crate::bus::{Bus, Message, MessageKind, MessagePublisher, TransportError};
use crate::microsvc::dependencies::{ConfigurableOutboxPublisher, HasOutboxStore};
use crate::outbox::OutboxPublisherConfig;
#[cfg(any(feature = "runtime-tokio", test))]
use crate::outbox_worker::{
    OutboxDispatcher, OutboxDrainRunner, OutboxPublishMailbox, DEFAULT_DRAIN_LEASE,
    DEFAULT_OUTBOX_HINT_CAPACITY,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

pub(in crate::microsvc::service) type OutboxConfigurator<D> =
    fn(&mut D, DynBusPublisher, String, Duration, u32, Option<String>);

trait DynPublish: Send + Sync {
    fn publish<'a>(
        &'a self,
        message: Message,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + 'a>>;
}

struct BusDynPublisher<B> {
    bus: Arc<B>,
}

impl<B: Bus> DynPublish for BusDynPublisher<B> {
    fn publish<'a>(
        &'a self,
        message: Message,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + 'a>> {
        Box::pin(async move {
            match message.kind {
                MessageKind::Command => self.bus.send_message(message).await,
                MessageKind::Event => self.bus.publish_message(message).await,
            }
        })
    }
}

#[derive(Clone)]
pub(crate) struct DynBusPublisher {
    inner: Arc<dyn DynPublish>,
}

impl DynBusPublisher {
    pub(crate) fn new<B>(bus: Arc<B>) -> Self
    where
        B: Bus + 'static,
    {
        Self {
            inner: Arc::new(BusDynPublisher { bus }),
        }
    }
}

impl MessagePublisher for DynBusPublisher {
    fn publish(
        &self,
        message: Message,
    ) -> impl Future<Output = Result<(), TransportError>> + Send + '_ {
        self.inner.publish(message)
    }
}

pub(in crate::microsvc::service) fn configure_outbox_for<D>(
    dependencies: &mut D,
    publisher: DynBusPublisher,
    worker_id: String,
    lease: Duration,
    max_attempts: u32,
    service_name: Option<String>,
) where
    D: HasOutboxStore + ConfigurableOutboxPublisher,
    D::OutboxStore: 'static,
{
    // One publish path either way: the commit leaves its rows pending and
    // the dispatcher claims them by id.
    #[cfg(any(feature = "runtime-tokio", test))]
    let config = {
        let _ = lease;
        let (mailbox, rx, wake) = OutboxPublishMailbox::channel(DEFAULT_OUTBOX_HINT_CAPACITY);
        let mut dispatcher = OutboxDispatcher::new(
            dependencies.outbox_store(),
            publisher,
            worker_id,
            DEFAULT_DRAIN_LEASE,
            max_attempts,
        );
        if let Some(name) = service_name {
            dispatcher = dispatcher.with_service(name);
        }
        OutboxDrainRunner::new(dispatcher)
            .with_hints(rx, wake)
            .spawn();
        // Hand the ids to the bounded worker and return at once.
        OutboxPublisherConfig::new(move |ids| {
            mailbox.try_submit(ids);
            Box::pin(async {})
        })
    };

    // No runtime to run a worker on: publish before the commit returns. A
    // publish failure leaves the rows pending for a separately run dispatcher.
    #[cfg(not(any(feature = "runtime-tokio", test)))]
    let config = {
        let mut dispatcher = crate::outbox_worker::OutboxDispatcher::new(
            dependencies.outbox_store(),
            publisher,
            worker_id,
            lease,
            max_attempts,
        );
        if let Some(name) = service_name {
            dispatcher = dispatcher.with_service(name);
        }
        let dispatcher = Arc::new(dispatcher);
        OutboxPublisherConfig::new(move |ids| {
            let dispatcher = Arc::clone(&dispatcher);
            Box::pin(async move {
                let _ = dispatcher.dispatch_ids(&ids).await;
            })
        })
    };

    dependencies.configure_outbox_publisher(config);
}
