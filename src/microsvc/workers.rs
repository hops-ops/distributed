//! Framework-owned outbox and consumer worker loops.
//!
//! Applications should not reimplement dialect-specific spawn loops.

use std::sync::Arc;
use std::time::Duration;

use crate::bus::{Bus, RunOptions};
use crate::microsvc::Service;
use crate::outbox_worker::{
    drain_worker_id, BusPublisher, OutboxDispatcher, OutboxDrainRunner, OutboxStore,
};

/// Spawn the standard outbox drain loop for a bus-backed store.
///
/// Fire-and-forget: the task lives until the process exits. Prefer
/// [`OutboxDrainRunner`] when the caller needs to stop the loop.
pub fn spawn_outbox_publish_loop<S, B>(
    store: S,
    bus: Arc<B>,
    service_name: impl Into<String>,
    lease: Duration,
    max_attempts: u32,
) where
    S: OutboxStore + 'static,
    B: Bus + Send + Sync + 'static,
{
    let dispatcher = OutboxDispatcher::new(
        store,
        BusPublisher::new(bus),
        drain_worker_id(),
        lease,
        max_attempts,
    )
    .with_service(service_name);
    let _handle = OutboxDrainRunner::new(dispatcher)
        .with_batch_size(32)
        .with_poll_interval(Duration::from_millis(25))
        .with_error_backoff(Duration::from_millis(100))
        .spawn();
}

/// Spawn the bus consumer for a long-running host.
///
/// `build_service` constructs the heavy route/projector graph **once**, then
/// again only after `run` fails. The framework waits on the source-specific
/// idle signal instead of reconstructing routes whenever the bus is quiet.
pub fn spawn_service_consumer_loop<F>(build_service: F)
where
    F: Fn() -> Service + Send + Sync + 'static,
{
    tokio::spawn(async move {
        loop {
            let service = build_service();
            match service.run(RunOptions::idempotent().wait_when_idle()).await {
                Ok(()) => tokio::time::sleep(Duration::from_millis(25)).await,
                Err(e) => {
                    eprintln!("consumer: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    });
}
