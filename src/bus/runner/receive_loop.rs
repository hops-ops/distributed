use std::collections::{HashMap, VecDeque};
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use futures_util::stream::{FuturesUnordered, StreamExt};

use crate::bus::source::{MessageSource, ReceivedMessage};
use crate::bus::{FailureAction, MessageRouter, RunOptions, TransportError, TransportErrorKind};
use crate::bus::{Message, MessageKind, OrderedDelivery};

/// Run the receive loop for a direct transport source.
///
/// For each message the runner:
///
/// 1. enforces the inbox stable-id contract (a no-op in idempotent mode);
/// 2. dispatches through [`MessageRouter::dispatch`];
/// 3. on success, acknowledges via the adapter;
/// 4. on failure, routes through [`RunOptions::failure_policy`] — retryable
///    failures are nacked for redelivery, permanent failures take the configured
///    action ([dead-letter](FailureAction::DeadLetter), [park](FailureAction::Park),
///    [log-and-ack](FailureAction::LogAndAck), or [stop](FailureAction::Stop)).
///
/// A message with no registered handler is **intentionally ignored**: the runner
/// acks it and moves on rather than dead-lettering it. Fan-out event transports
/// may deliver events this service does not consume, and acking matches
/// `microsvc::subscribe`; production transports should use
/// [`MessageRouter::subscription_plan`] to avoid delivering unrelated messages at all.
///
/// The runner **acks only after handler effects have completed**, never before.
/// It stops gracefully when the source returns `Ok(None)`, having fully settled
/// the in-flight message first. Receive and settle errors are propagated, not
/// swallowed: a returned `Err` ends the run and the supervisor may restart it
/// (already-committed effects make redelivery safe).
///
/// When the router declares several [delivery lanes](MessageRouter::delivery_lanes)
/// and the source [settles independently](MessageSource::settles_independently),
/// lanes run concurrently while each keeps receive order, and a delivery is
/// settled once every lane that received it has finished. The number of
/// unsettled deliveries is bounded by [`RunOptions::lane_window`]. See
/// `docs/consumer-delivery-lanes.md`. Otherwise every message runs all of its
/// routes before the next message is received.
///
/// Inbox note: until the consumer-inbox subtask lands, inbox mode enforces the
/// stable-id requirement and then dispatches like idempotent mode. The
/// receipt-commit wrapping that makes it effectively-once is added there.
///
/// `I: Send` keeps the returned future `Send` so the runner can be spawned on a
/// multi-threaded executor regardless of the inbox hook type.
pub async fn run_source<R, S, I>(
    router: Arc<R>,
    source: S,
    options: RunOptions<I>,
) -> Result<(), TransportError>
where
    R: MessageRouter,
    S: MessageSource,
    I: Send,
{
    if router.delivery_lanes() > 1 && source.settles_independently() {
        return LaneRun::new(router, source, options).run().await;
    }
    run_sequential(router, source, options).await
}

async fn run_sequential<R, S, I>(
    router: Arc<R>,
    mut source: S,
    options: RunOptions<I>,
) -> Result<(), TransportError>
where
    R: MessageRouter,
    S: MessageSource,
    I: Send,
{
    let service = router.consumer_group();
    let transport = source.transport_name();

    loop {
        let Some(received) = recv_next(&mut source, service, transport).await? else {
            break;
        };
        if received.decode_error().is_some() {
            settle_permanent_decode(service, transport, &options, received).await?;
            continue;
        }
        if !router.handles(received.message().kind, received.message().name()) {
            settle_ignored(service, transport, received).await?;
            continue;
        }
        let kind = received.message().kind;
        let result = dispatch(
            router.as_ref(),
            &options,
            received.message(),
            received.ordered_delivery(),
        )
        .await;
        settle_result(service, transport, &options, received, kind, result).await?;
    }
    Ok(())
}

/// A delivery the transport could not decode is a permanent failure: it
/// carries no valid message to dispatch, and it must NOT be treated as an
/// empty message (which would route to ack-and-ignore and silently drop a
/// corrupt row). Route it through the failure policy directly, the same as a
/// permanent dispatch failure, so it is dead-lettered/parked.
async fn settle_permanent_decode<M: ReceivedMessage, I>(
    service: Option<&str>,
    transport: &str,
    options: &RunOptions<I>,
    received: M,
) -> Result<(), TransportError> {
    let Some(error) = received.decode_error() else {
        return Ok(());
    };
    let action = options.failure_policy.resolve(error);
    record_transport_failure(service, transport, error.kind(), action);
    let kind = received.message().kind;
    let reason = error.to_string();
    match action {
        FailureAction::Nack => {
            settle_and_record(
                service,
                transport,
                kind,
                crate::telemetry::transport_outcome::NACK,
                crate::telemetry::transport_outcome::NACK,
                || received.nack(&reason),
            )
            .await
        }
        FailureAction::DeadLetter => {
            settle_and_record(
                service,
                transport,
                kind,
                crate::telemetry::transport_outcome::DEAD_LETTER,
                crate::telemetry::transport_outcome::DEAD_LETTER,
                || received.dead_letter(&reason),
            )
            .await
        }
        FailureAction::Park => {
            settle_and_record(
                service,
                transport,
                kind,
                crate::telemetry::transport_outcome::PARK,
                crate::telemetry::transport_outcome::PARK,
                || received.park(&reason),
            )
            .await
        }
        FailureAction::LogAndAck => {
            eprintln!(
                "[bus::runner] dropping undecodable message after permanent failure: {reason}"
            );
            settle_and_record(
                service,
                transport,
                kind,
                crate::telemetry::transport_outcome::ACK,
                crate::telemetry::transport_outcome::LOG_AND_ACK,
                || received.ack(),
            )
            .await
        }
        FailureAction::Stop => Err(TransportError::permanent(reason)),
    }
}

/// No handler for this message: intentionally ignore (ack) rather than
/// dead-letter, so unrelated fan-out events don't pile into the DLQ.
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

/// Settle one dispatched delivery exactly as the sequential runner always has.
///
/// Returns `Err` when the run must end: a retain-and-stop failure (after NAKing
/// the exact delivery), a `Stop` policy (without settling), or a settle error.
async fn settle_result<M: ReceivedMessage, I>(
    service: Option<&str>,
    transport: &str,
    options: &RunOptions<I>,
    received: M,
    kind: MessageKind,
    result: Result<(), TransportError>,
) -> Result<(), TransportError> {
    match result {
        Ok(()) => {
            settle_and_record(
                service,
                transport,
                kind,
                crate::telemetry::transport_outcome::ACK,
                crate::telemetry::transport_outcome::ACK,
                || received.ack(),
            )
            .await
        }
        Err(error) if error.should_retain_and_stop() => {
            record_transport_failure(
                service,
                transport,
                error.kind(),
                crate::telemetry::transport_outcome::NACK,
            );
            let reason = error.to_string();
            settle_and_record(
                service,
                transport,
                kind,
                crate::telemetry::transport_outcome::NACK,
                crate::telemetry::transport_outcome::NACK,
                || received.nack(&reason),
            )
            .await?;
            Err(error)
        }
        Err(error) => match options.failure_policy.resolve(&error) {
            action @ FailureAction::Nack => {
                record_transport_failure(service, transport, error.kind(), action);
                let reason = error.to_string();
                settle_and_record(
                    service,
                    transport,
                    kind,
                    crate::telemetry::transport_outcome::NACK,
                    crate::telemetry::transport_outcome::NACK,
                    || received.nack(&reason),
                )
                .await
            }
            action @ FailureAction::DeadLetter => {
                record_transport_failure(service, transport, error.kind(), action);
                let reason = error.to_string();
                settle_and_record(
                    service,
                    transport,
                    kind,
                    crate::telemetry::transport_outcome::DEAD_LETTER,
                    crate::telemetry::transport_outcome::DEAD_LETTER,
                    || received.dead_letter(&reason),
                )
                .await
            }
            action @ FailureAction::Park => {
                record_transport_failure(service, transport, error.kind(), action);
                let reason = error.to_string();
                settle_and_record(
                    service,
                    transport,
                    kind,
                    crate::telemetry::transport_outcome::PARK,
                    crate::telemetry::transport_outcome::PARK,
                    || received.park(&reason),
                )
                .await
            }
            FailureAction::LogAndAck => {
                record_transport_failure(
                    service,
                    transport,
                    error.kind(),
                    FailureAction::LogAndAck,
                );
                eprintln!(
                    "[bus::runner] dropping message '{}' after permanent failure: {error}",
                    received.message().name()
                );
                settle_and_record(
                    service,
                    transport,
                    kind,
                    crate::telemetry::transport_outcome::ACK,
                    crate::telemetry::transport_outcome::LOG_AND_ACK,
                    || received.ack(),
                )
                .await
            }
            FailureAction::Stop => {
                record_transport_failure(service, transport, error.kind(), FailureAction::Stop);
                Err(error)
            }
        },
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
    kind: MessageKind,
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
struct LaneRun<'r, R, S: MessageSource + 'r, I> {
    router: Arc<R>,
    options: RunOptions<I>,
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

impl<'r, R, S, I> LaneRun<'r, R, S, I>
where
    R: MessageRouter + 'r,
    S: MessageSource + 'r,
    I: Send,
{
    fn new(router: Arc<R>, source: S, options: RunOptions<I>) -> Self {
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
                    self.receiving = Some(Box::pin(async move {
                        let result = source.recv().await;
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
                record_transport_failure(
                    service,
                    self.transport,
                    error.kind(),
                    crate::telemetry::failure_action::RECV_ERROR,
                );
                self.source_finished = Some(Err(error));
                return;
            }
        };
        if received.decode_error().is_some() {
            let outcome =
                settle_permanent_decode(service, self.transport, &self.options, received).await;
            self.record_stop(outcome);
            return;
        }
        let kind = received.message().kind;
        let lanes = self.router.lanes_for(kind, received.message().name());
        if lanes.is_empty() {
            let outcome = settle_ignored(service, self.transport, received).await;
            self.record_stop(outcome);
            return;
        }
        if let Err(error) = self.options.validate_message_id(received.message()) {
            let error = TransportError::permanent(error.to_string()).with_source(error);
            let outcome = settle_result(
                service,
                self.transport,
                &self.options,
                received,
                kind,
                Err(error),
            )
            .await;
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
                kind,
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
        let stops = |error: &TransportError, options: &RunOptions<I>| {
            error.should_retain_and_stop()
                || options.failure_policy.resolve(error) == FailureAction::Stop
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
            entry.kind,
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

async fn recv_next<S: MessageSource>(
    source: &mut S,
    service: Option<&str>,
    transport: &str,
) -> Result<Option<S::Received>, TransportError> {
    match source.recv().await {
        Ok(received) => Ok(received),
        Err(error) => {
            record_transport_failure(
                service,
                transport,
                error.kind(),
                crate::telemetry::failure_action::RECV_ERROR,
            );
            Err(error)
        }
    }
}

/// Run consumer execution for one message and classify the outcome.
///
/// Enforces the inbox stable-id contract first (idempotent mode yields no key
/// and skips it), then dispatches. A failed stable-id check is a permanent
/// failure — redelivery cannot supply a missing or malformed id.
async fn dispatch<R: MessageRouter, I>(
    router: &R,
    options: &RunOptions<I>,
    message: &Message,
    ordered: Option<&crate::bus::OrderedDelivery>,
) -> Result<(), TransportError> {
    #[cfg(feature = "otel")]
    {
        use tracing::Instrument as _;

        let span = transport_receive_span(message);
        crate::trace_context::set_span_parent_from_metadata_if_no_current_span(
            &span,
            &message.metadata,
        );
        return async {
            options
                .validate_message_id(message)
                .map_err(|err| TransportError::permanent(err.to_string()).with_source(err))?;
            router.dispatch_ordered(message, ordered).await
        }
        .instrument(span)
        .await;
    }

    #[cfg(not(feature = "otel"))]
    {
        options
            .validate_message_id(message)
            .map_err(|err| TransportError::permanent(err.to_string()).with_source(err))?;
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
