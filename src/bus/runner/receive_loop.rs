use std::collections::{HashMap, VecDeque};
use std::future::{poll_fn, Future};
use std::pin::{pin, Pin};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use futures_util::future::{select, Either};
use futures_util::stream::{FuturesUnordered, StreamExt};

use crate::bus::source::{MessageSource, ReceivedMessage};
use crate::bus::{DeadLetter, FailureAction, MessageRouter, RunOptions, RunUntil};
use crate::bus::{Message, MessageKind, OrderedDelivery, TransportError, TransportErrorKind};

/// Consume `source` with `router` until [`RunOptions::until`] says to stop.
///
/// For each delivery the runner dispatches to the router and then settles:
///
/// - success, or a message the router has no handler for: acknowledge;
/// - a retryable failure: redeliver after [`RunOptions::backoff`], or
///   dead-letter once [`RunOptions::max_attempts`] deliveries have failed;
/// - a permanent failure, including a delivery the transport could not
///   decode: [`RunOptions::failure_policy`].
///
/// Dead-lettering stores the message in [`RunOptions::dead_letters`] and then
/// acknowledges the delivery. If there is no sink, or the sink fails, the
/// delivery is handed back to the transport and the run ends with an error:
/// a message is never acknowledged without being handled or stored.
///
/// The run returns `Ok(())` when its shutdown signal is triggered (after
/// settling the delivery in hand) or, with [`RunUntil::Idle`], when the source
/// has nothing to deliver. A receive or settle error ends the run with that
/// error; effects already committed make the redelivery safe.
///
/// When the router declares several [delivery lanes](MessageRouter::delivery_lanes)
/// and the source [settles independently](MessageSource::settles_independently),
/// lanes run concurrently while each keeps receive order, and a delivery is
/// settled once every lane that received it has finished. See
/// `docs/consumer-delivery-lanes.md`.
pub async fn run_source<R, S>(
    router: Arc<R>,
    source: S,
    options: RunOptions,
) -> Result<(), TransportError>
where
    R: MessageRouter,
    S: MessageSource,
{
    if router.delivery_lanes() > 1 && source.settles_independently() {
        return LaneRun::new(router, source, options).run().await;
    }
    run_sequential(router, source, options).await
}

async fn run_sequential<R, S>(
    router: Arc<R>,
    mut source: S,
    options: RunOptions,
) -> Result<(), TransportError>
where
    R: MessageRouter,
    S: MessageSource,
{
    let service = router.consumer_group();
    let transport = source.transport_name();

    while let Some(received) =
        next_delivery(&mut source, &options.until, service, transport).await?
    {
        let result = if let Some(error) = received.decode_error() {
            Err(TransportError::new(error.kind(), error.message()))
        } else if !router.handles(received.message().kind, received.message().name()) {
            settle_ignored(service, transport, received).await?;
            continue;
        } else {
            dispatch(
                router.as_ref(),
                received.message(),
                received.ordered_delivery(),
            )
            .await
        };
        settle_result(service, transport, &options, received, result).await?;
    }
    Ok(())
}

/// The next delivery, or `None` when the run should end.
async fn next_delivery<S: MessageSource>(
    source: &mut S,
    until: &RunUntil,
    service: Option<&str>,
    transport: &str,
) -> Result<Option<S::Received>, TransportError> {
    let recv_error = |error: TransportError| {
        record_transport_failure(
            service,
            transport,
            error.kind(),
            crate::telemetry::failure_action::RECV_ERROR,
        );
        error
    };
    let shutdown = match until {
        RunUntil::Idle => return source.recv().await.map_err(recv_error),
        RunUntil::Shutdown(shutdown) => shutdown,
    };
    loop {
        if shutdown.is_triggered() {
            return Ok(None);
        }
        match select(pin!(source.recv()), shutdown.triggered()).await {
            Either::Left((Ok(Some(received)), _)) => return Ok(Some(received)),
            Either::Left((Ok(None), _)) => {}
            Either::Left((Err(error), _)) => return Err(recv_error(error)),
            Either::Right(_) => return Ok(None),
        }
        if let Either::Right(_) = select(pin!(source.ready()), shutdown.triggered()).await {
            return Ok(None);
        }
    }
}

/// No handler for this message: acknowledge it. A fan-out transport may
/// deliver events this consumer does not handle, and those are not failures.
async fn settle_ignored<M: ReceivedMessage>(
    service: Option<&str>,
    transport: &str,
    received: M,
) -> Result<(), TransportError> {
    let kind = received.message().kind;
    settle_and_record(
        service,
        transport,
        kind,
        crate::telemetry::transport_outcome::ACK,
        crate::telemetry::transport_outcome::IGNORED,
        || received.ack(),
    )
    .await
}

/// Settle one delivery according to how handling it went.
///
/// Returns `Err` when the run must end.
async fn settle_result<M: ReceivedMessage>(
    service: Option<&str>,
    transport: &str,
    options: &RunOptions,
    received: M,
    result: Result<(), TransportError>,
) -> Result<(), TransportError> {
    use crate::telemetry::transport_outcome::{ACK, DEAD_LETTER, LOG_AND_ACK, NACK};

    let kind = received.message().kind;
    let error = match result {
        Ok(()) => {
            return settle_and_record(service, transport, kind, ACK, ACK, || received.ack()).await
        }
        Err(error) => error,
    };
    let hand_back = |received: M| {
        settle_and_record(service, transport, kind, NACK, NACK, || {
            received.nack(Duration::ZERO)
        })
    };
    if error.should_retain_and_stop() {
        record_transport_failure(service, transport, error.kind(), NACK);
        hand_back(received).await?;
        return Err(error);
    }
    let attempts = received.attempts();
    let action = options
        .failure_policy
        .resolve(&error, attempts, options.max_attempts);
    record_transport_failure(service, transport, error.kind(), action);
    match action {
        FailureAction::Nack => {
            let delay = options.backoff.delay(attempts);
            settle_and_record(service, transport, kind, NACK, NACK, || {
                received.nack(delay)
            })
            .await
        }
        FailureAction::DeadLetter => {
            let Some(sink) = options.dead_letters.as_deref() else {
                hand_back(received).await?;
                return Err(TransportError::permanent(format!(
                    "no dead-letter sink is configured for a failed message: {error}"
                )));
            };
            let dead = DeadLetter {
                message: received.message().clone(),
                reason: error.to_string(),
                consumer: service.unwrap_or_default().to_owned(),
                attempts,
            };
            if let Err(sink_error) = sink.dead_letter(dead).await {
                hand_back(received).await?;
                return Err(sink_error);
            }
            settle_and_record(service, transport, kind, ACK, DEAD_LETTER, || {
                received.ack()
            })
            .await
        }
        FailureAction::LogAndAck => {
            tracing::warn!(
                message = received.message().name(),
                consumer = service.unwrap_or_default(),
                %error,
                "dropping a message after a permanent failure"
            );
            settle_and_record(service, transport, kind, ACK, LOG_AND_ACK, || {
                received.ack()
            })
            .await
        }
        FailureAction::Stop => Err(error),
    }
}

// --- delivery lanes ------------------------------------------------------

type LaneDone = (u64, usize, Result<(), TransportError>);
type LaneFuture<'r> = Pin<Box<dyn Future<Output = LaneDone> + Send + 'r>>;
type RecvOutput<S> = (
    S,
    Result<Option<<S as MessageSource>::Received>, TransportError>,
);
type RecvFuture<'s, S> = Pin<Box<dyn Future<Output = RecvOutput<S>> + Send + 's>>;

struct InFlight<M> {
    received: M,
    message: Arc<Message>,
    ordered: Option<OrderedDelivery>,
    remaining: usize,
    stop: Option<TransportError>,
    retryable: Option<TransportError>,
    permanent: Option<TransportError>,
}

enum Step<S: MessageSource> {
    Received(RecvOutput<S>),
    Lane(LaneDone),
}

/// Concurrent lanes over one ordered source. See `docs/consumer-delivery-lanes.md`.
struct LaneRun<'r, R, S: MessageSource + 'r> {
    router: Arc<R>,
    options: RunOptions,
    transport: &'static str,
    source: Option<S>,
    receiving: Option<RecvFuture<'r, S>>,
    running: FuturesUnordered<LaneFuture<'r>>,
    lane_busy: Vec<bool>,
    lane_halted: Vec<bool>,
    queues: Vec<VecDeque<u64>>,
    in_flight: HashMap<u64, InFlight<S::Received>>,
    next_sequence: u64,
    /// Set when the source drained (`Ok(None)`) or failed to receive.
    source_finished: Option<Result<(), TransportError>>,
    stop_error: Option<TransportError>,
}

impl<'r, R, S> LaneRun<'r, R, S>
where
    R: MessageRouter + 'r,
    S: MessageSource + 'r,
{
    fn new(router: Arc<R>, source: S, options: RunOptions) -> Self {
        let lanes = router.delivery_lanes().min(crate::bus::LaneSet::MAX_LANES);
        let transport = source.transport_name();
        Self {
            router,
            options,
            transport,
            source: Some(source),
            receiving: None,
            running: FuturesUnordered::new(),
            lane_busy: vec![false; lanes],
            lane_halted: vec![false; lanes],
            queues: vec![VecDeque::new(); lanes],
            in_flight: HashMap::new(),
            next_sequence: 0,
            source_finished: None,
            stop_error: None,
        }
    }

    fn service(&self) -> Option<String> {
        self.router.consumer_group().map(str::to_owned)
    }

    fn stopping(&self) -> bool {
        self.stop_error.is_some() || self.source_finished.is_some()
    }

    async fn run(mut self) -> Result<(), TransportError> {
        loop {
            self.start_idle_lanes().await;
            if self.receiving.is_none()
                && !self.stopping()
                && self.in_flight.len() < self.options.lane_window.max(1)
            {
                if let Some(mut source) = self.source.take() {
                    let until = self.options.until.clone();
                    let service = self.service();
                    let transport = self.transport;
                    self.receiving = Some(Box::pin(async move {
                        let result =
                            next_delivery(&mut source, &until, service.as_deref(), transport).await;
                        (source, result)
                    }));
                }
            }
            if self.in_flight.is_empty() && self.receiving.is_none() && self.stopping() {
                if let Some(error) = self.stop_error.take() {
                    return Err(error);
                }
                return self.source_finished.take().unwrap_or(Ok(()));
            }

            let running = &mut self.running;
            let receiving = &mut self.receiving;
            let step: Step<S> = poll_fn(|cx| {
                if let Poll::Ready(Some(done)) = running.poll_next_unpin(cx) {
                    return Poll::Ready(Step::Lane(done));
                }
                if let Some(future) = receiving.as_mut() {
                    if let Poll::Ready(output) = future.as_mut().poll(cx) {
                        return Poll::Ready(Step::Received(output));
                    }
                }
                Poll::Pending
            })
            .await;
            match step {
                Step::Received((source, result)) => {
                    self.receiving = None;
                    self.source = Some(source);
                    self.accept(result).await;
                }
                Step::Lane((sequence, lane, result)) => {
                    self.lane_busy[lane] = false;
                    self.finish_lane(sequence, lane, result).await;
                }
            }
        }
    }

    /// Start the next queued delivery on every idle lane. A halted lane NAKs
    /// its queued deliveries instead of running them.
    async fn start_idle_lanes(&mut self) {
        for lane in 0..self.queues.len() {
            while self.lane_halted[lane] {
                let Some(sequence) = self.queues[lane].pop_front() else {
                    break;
                };
                let halted = TransportError::retryable(
                    "delivery lane halted after a stop-class failure; delivery retained",
                );
                self.finish_lane(sequence, lane, Err(halted)).await;
            }
            if self.lane_busy[lane] {
                continue;
            }
            let Some(sequence) = self.queues[lane].pop_front() else {
                continue;
            };
            let Some(entry) = self.in_flight.get(&sequence) else {
                continue;
            };
            let router = Arc::clone(&self.router);
            let message = Arc::clone(&entry.message);
            let ordered = entry.ordered.clone();
            self.lane_busy[lane] = true;
            self.running.push(Box::pin(async move {
                let result = dispatch_lane(router.as_ref(), &message, ordered.as_ref(), lane).await;
                (sequence, lane, result)
            }));
        }
    }

    async fn accept(&mut self, result: Result<Option<S::Received>, TransportError>) {
        let service = self.service();
        let service = service.as_deref();
        let received = match result {
            Ok(Some(received)) => received,
            Ok(None) => {
                self.source_finished = Some(Ok(()));
                return;
            }
            Err(error) => {
                self.source_finished = Some(Err(error));
                return;
            }
        };
        if let Some(error) = received.decode_error() {
            let error = TransportError::new(error.kind(), error.message());
            let outcome =
                settle_result(service, self.transport, &self.options, received, Err(error)).await;
            self.record_stop(outcome);
            return;
        }
        let lanes = self
            .router
            .lanes_for(received.message().kind, received.message().name());
        if lanes.is_empty() {
            let outcome = settle_ignored(service, self.transport, received).await;
            self.record_stop(outcome);
            return;
        }
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        let message = Arc::new(received.message().clone());
        let ordered = received.ordered_delivery().cloned();
        let mut remaining = 0;
        let lane_count = self.queues.len();
        for lane in lanes.iter().filter(|lane| *lane < lane_count) {
            self.queues[lane].push_back(sequence);
            remaining += 1;
        }
        self.in_flight.insert(
            sequence,
            InFlight {
                received,
                message,
                ordered,
                remaining,
                stop: None,
                retryable: None,
                permanent: None,
            },
        );
        if remaining == 0 {
            // Lanes beyond this runner's range cannot run; keep it retryable.
            let error = TransportError::retryable("delivery lane out of range");
            self.settle_entry(sequence, Some(error)).await;
        }
    }

    async fn finish_lane(
        &mut self,
        sequence: u64,
        lane: usize,
        result: Result<(), TransportError>,
    ) {
        let stops = |error: &TransportError, options: &RunOptions| {
            error.should_retain_and_stop()
                || options.failure_policy.resolve(error, 1, None) == FailureAction::Stop
        };
        let Some(entry) = self.in_flight.get_mut(&sequence) else {
            return;
        };
        entry.remaining = entry.remaining.saturating_sub(1);
        if let Err(error) = result {
            if stops(&error, &self.options) {
                self.lane_halted[lane] = true;
                entry.stop.get_or_insert(error);
            } else if error.is_retryable() {
                entry.retryable.get_or_insert(error);
            } else {
                entry.permanent.get_or_insert(error);
            }
        }
        if entry.remaining == 0 {
            self.settle_entry(sequence, None).await;
        }
    }

    /// Settle a delivery whose lanes all finished. A stop-class failure wins,
    /// then any retryable failure (so a delivery that may still succeed is
    /// redelivered), then a permanent one.
    async fn settle_entry(&mut self, sequence: u64, extra: Option<TransportError>) {
        let Some(entry) = self.in_flight.remove(&sequence) else {
            return;
        };
        let error = entry.stop.or(entry.retryable).or(extra).or(entry.permanent);
        let service = self.service();
        let outcome = settle_result(
            service.as_deref(),
            self.transport,
            &self.options,
            entry.received,
            error.map_or(Ok(()), Err),
        )
        .await;
        self.record_stop(outcome);
    }

    fn record_stop(&mut self, outcome: Result<(), TransportError>) {
        if let Err(error) = outcome {
            self.stop_error.get_or_insert(error);
        }
    }
}

async fn dispatch_lane<R: MessageRouter>(
    router: &R,
    message: &Message,
    ordered: Option<&OrderedDelivery>,
    lane: usize,
) -> Result<(), TransportError> {
    #[cfg(feature = "otel")]
    {
        use tracing::Instrument as _;

        let span = transport_receive_span(message);
        crate::trace_context::set_span_parent_from_metadata_if_no_current_span(
            &span,
            &message.metadata,
        );
        return router
            .dispatch_lane(message, ordered, lane)
            .instrument(span)
            .await;
    }

    #[cfg(not(feature = "otel"))]
    {
        router.dispatch_lane(message, ordered, lane).await
    }
}

async fn settle_and_record<F, Fut>(
    service: Option<&str>,
    transport: &str,
    kind: MessageKind,
    settle_action: &'static str,
    outcome: &'static str,
    settle: F,
) -> Result<(), TransportError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<(), TransportError>>,
{
    match settle().await {
        Ok(()) => {
            record_transport_message(service, transport, kind, outcome);
            Ok(())
        }
        Err(error) => {
            record_transport_failure(
                service,
                transport,
                error.kind(),
                crate::telemetry::settle_failure_action(settle_action),
            );
            Err(error)
        }
    }
}

async fn dispatch<R: MessageRouter>(
    router: &R,
    message: &Message,
    ordered: Option<&OrderedDelivery>,
) -> Result<(), TransportError> {
    #[cfg(feature = "otel")]
    {
        use tracing::Instrument as _;

        let span = transport_receive_span(message);
        crate::trace_context::set_span_parent_from_metadata_if_no_current_span(
            &span,
            &message.metadata,
        );
        return router
            .dispatch_ordered(message, ordered)
            .instrument(span)
            .await;
    }

    #[cfg(not(feature = "otel"))]
    {
        router.dispatch_ordered(message, ordered).await
    }
}

#[cfg(feature = "otel")]
fn transport_receive_span(message: &Message) -> tracing::Span {
    crate::telemetry::transport_receive_span(message)
}

fn record_transport_message(
    service: Option<&str>,
    transport: &str,
    kind: MessageKind,
    outcome: &str,
) {
    #[cfg(feature = "metrics")]
    crate::metrics::record_transport_message(service, transport, kind, outcome);
    #[cfg(not(feature = "metrics"))]
    let _ = (service, transport, kind, outcome);
}

fn record_transport_failure<A>(
    service: Option<&str>,
    transport: &str,
    kind: TransportErrorKind,
    action: A,
) where
    A: IntoFailureActionLabel,
{
    #[cfg(feature = "metrics")]
    crate::metrics::record_transport_failure(
        service,
        transport,
        crate::telemetry::transport_failure_class(kind),
        action.into_failure_action_label(),
    );
    #[cfg(not(feature = "metrics"))]
    {
        let _ = (service, transport, kind);
        let _ = action.into_failure_action_label();
    }
}

trait IntoFailureActionLabel {
    fn into_failure_action_label(self) -> &'static str;
}

impl IntoFailureActionLabel for FailureAction {
    fn into_failure_action_label(self) -> &'static str {
        crate::telemetry::failure_action_label(self)
    }
}

impl IntoFailureActionLabel for &'static str {
    fn into_failure_action_label(self) -> &'static str {
        self
    }
}
