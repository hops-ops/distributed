//! Framework-owned outbox and consumer worker loops.
//!
//! Applications should not reimplement dialect-specific spawn loops.

use std::sync::Arc;
use std::time::Duration;

use crate::bus::{Bus, RetryBackoff, RunOptions, Shutdown};
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
/// The consumer runs until the process ends. `build_service` constructs the
/// route and projector graph once, and again only after a run fails; restarts
/// are spaced by the default [`RetryBackoff`], which resets after a run that
/// lasted longer than its longest delay.
pub fn spawn_service_consumer_loop<F>(build_service: F)
where
    F: Fn() -> Service + Send + Sync + 'static,
{
    spawn_service_consumer_loop_until(build_service, Shutdown::new());
}

/// [`spawn_service_consumer_loop`] that stops when `shutdown` is triggered:
/// the consumer settles the delivery it is handling and the task ends.
pub fn spawn_service_consumer_loop_until<F>(
    build_service: F,
    shutdown: Shutdown,
) -> tokio::task::JoinHandle<()>
where
    F: Fn() -> Service + Send + Sync + 'static,
{
    tokio::spawn(async move {
        let backoff = RetryBackoff::default();
        let mut failures = 0u32;
        while !shutdown.is_triggered() {
            let started = std::time::Instant::now();
            let options = RunOptions::until_shutdown(shutdown.clone());
            let Err(error) = build_service().run(options).await else {
                return;
            };
            if started.elapsed() > crate::bus::DEFAULT_RETRY_BACKOFF_MAX {
                failures = 0;
            }
            failures = failures.saturating_add(1);
            let delay = backoff.delay(failures);
            tracing::error!(%error, ?delay, "bus consumer stopped; restarting");
            tokio::time::sleep(delay).await;
        }
    })
}
