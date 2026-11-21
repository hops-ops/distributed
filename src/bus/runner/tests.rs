use super::run_source;
use crate::bus::source::{MessageSource, ReceivedMessage};
use crate::bus::{
    DeadLetter, DeadLetterSink, FailurePolicy, Handlers, Message, MessageKind, RetryBackoff,
    RunOptions, Shutdown, TransportError,
};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

// --- minimal runtime-free executor -------------------------------------
// The transport module is not feature-gated, so its tests run without an
// async runtime. The fake futures never suspend, so a busy-poll with a
// no-op waker drives them to completion.
fn block_on<F: Future>(future: F) -> F::Output {
    use std::ptr;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    const VTABLE: RawWakerVTable = RawWakerVTable::new(
        |_| RawWaker::new(ptr::null(), &VTABLE),
        |_| {},
        |_| {},
        |_| {},
    );
    let raw = RawWaker::new(ptr::null(), &VTABLE);
    let waker = unsafe { Waker::from_raw(raw) };
    let mut cx = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
    }
}

// --- recorder + fakes ---------------------------------------------------
#[derive(Debug, Clone, PartialEq, Eq)]
enum Event {
    Handled(String),
    Ack,
    Nack(Duration),
    /// A dead letter reached the sink: reason, consumer, attempts.
    DeadLetter(String, String, u32),
}

struct Recorder {
    events: Mutex<Vec<Event>>,
}

impl DeadLetterSink for Recorder {
    fn dead_letter<'a>(
        &'a self,
        dead: DeadLetter,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + 'a>> {
        Box::pin(async move {
            self.push(Event::DeadLetter(dead.reason, dead.consumer, dead.attempts));
            Ok(())
        })
    }
}

/// A sink that cannot store anything.
struct BrokenSink;

impl DeadLetterSink for BrokenSink {
    fn dead_letter<'a>(
        &'a self,
        _dead: DeadLetter,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + 'a>> {
        Box::pin(async { Err(TransportError::retryable("dead-letter store is down")) })
    }
}

impl Recorder {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(Vec::new()),
        })
    }
    fn push(&self, event: Event) {
        self.events.lock().unwrap().push(event);
    }
    fn events(&self) -> Vec<Event> {
        self.events.lock().unwrap().clone()
    }
}

struct FakeReceived {
    message: Message,
    recorder: Arc<Recorder>,
    settle_ok: bool,
    decode_error: Option<TransportError>,
    attempts: u32,
}

impl FakeReceived {
    fn settle(self, event: Event) -> Result<(), TransportError> {
        self.recorder.push(event);
        if self.settle_ok {
            Ok(())
        } else {
            Err(TransportError::retryable("settle failed"))
        }
    }
}

impl ReceivedMessage for FakeReceived {
    fn message(&self) -> &Message {
        &self.message
    }
    fn decode_error(&self) -> Option<&TransportError> {
        self.decode_error.as_ref()
    }
    async fn ack(self) -> Result<(), TransportError> {
        self.settle(Event::Ack)
    }
    fn attempts(&self) -> u32 {
        self.attempts
    }
    async fn nack(self, delay: Duration) -> Result<(), TransportError> {
        self.settle(Event::Nack(delay))
    }
}

struct FakeSource {
    queue: VecDeque<Message>,
    recorder: Arc<Recorder>,
    settle_ok: bool,
    recv_error: bool,
    // When set, every received message reports this as a decode failure,
    // modeling a transport that claims a row/offset before decoding it.
    decode_error: bool,
    /// Delivery count reported for every message.
    attempts: u32,
}

impl MessageSource for FakeSource {
    type Received = FakeReceived;
    async fn recv(&mut self) -> Result<Option<FakeReceived>, TransportError> {
        if self.recv_error {
            return Err(TransportError::retryable("recv failed"));
        }
        let decode_error = self.decode_error;
        Ok(self.queue.pop_front().map(|message| FakeReceived {
            message,
            recorder: self.recorder.clone(),
            settle_ok: self.settle_ok,
            decode_error: decode_error
                .then(|| TransportError::permanent("corrupt row: name failed to decode")),
            attempts: self.attempts,
        }))
    }
}

// --- helpers ------------------------------------------------------------
fn event_message(name: &str, id: Option<&str>) -> Message {
    let mut message = Message::new(name, MessageKind::Event, b"{}".to_vec());
    if let Some(id) = id {
        message = message.with_id(id);
    }
    message
}

fn router(recorder: &Arc<Recorder>) -> Arc<Handlers> {
    let ok = recorder.clone();
    let retryable = recorder.clone();
    let permanent = recorder.clone();
    let terminal = recorder.clone();
    Arc::new(
        Handlers::new()
            .on_event("ok", move |msg: &Message| {
                let ok = ok.clone();
                let name = msg.name().to_string();
                async move {
                    ok.push(Event::Handled(name));
                    Ok(())
                }
            })
            .on_event("retryable", move |msg: &Message| {
                let retryable = retryable.clone();
                let name = msg.name().to_string();
                async move {
                    retryable.push(Event::Handled(name));
                    Err(TransportError::retryable("infra"))
                }
            })
            .on_event("permanent", move |msg: &Message| {
                let permanent = permanent.clone();
                let name = msg.name().to_string();
                async move {
                    permanent.push(Event::Handled(name));
                    Err(TransportError::permanent("nope"))
                }
            })
            .on_event("terminal", move |msg: &Message| {
                let terminal = terminal.clone();
                let name = msg.name().to_string();
                async move {
                    terminal.push(Event::Handled(name));
                    Err(TransportError::permanent("durable projection failure").retain_and_stop())
                }
            }),
    )
}

struct RunResult {
    outcome: Result<(), TransportError>,
    events: Vec<Event>,
}

fn run(messages: Vec<Message>, options: RunOptions) -> RunResult {
    run_with(messages, options, true, false)
}

fn run_with(
    messages: Vec<Message>,
    options: RunOptions,
    settle_ok: bool,
    recv_error: bool,
) -> RunResult {
    let recorder = Recorder::new();
    let svc = router(&recorder);
    let source = FakeSource {
        queue: messages.into_iter().collect(),
        recorder: recorder.clone(),
        settle_ok,
        recv_error,
        decode_error: false,
        attempts: 1,
    };
    let options = options.or_dead_letters(|| recorder.clone());
    let outcome = block_on(run_source(svc, source, options));
    RunResult {
        outcome,
        events: recorder.events(),
    }
}

// --- tests --------------------------------------------------------------
#[test]
fn application_reload_retains_exact_delivery_then_succeeds_after_activation() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let recorder = Recorder::new();
    let open = Arc::new(AtomicBool::new(false));
    let gate = open.clone();
    let effects = recorder.clone();
    let router = Arc::new(
        Handlers::new().on_event("reload", move |message: &Message| {
            let admitted = gate.load(Ordering::SeqCst);
            let effects = effects.clone();
            let id = message.id().unwrap().to_owned();
            async move {
                if !admitted {
                    return Err(crate::microsvc::HandlerError::ApplicationReloading.into());
                }
                effects.push(Event::Handled(id));
                Ok(())
            }
        }),
    );
    let message = event_message("reload", Some("retained-event"));
    let source = || FakeSource {
        queue: vec![message.clone()].into_iter().collect(),
        recorder: recorder.clone(),
        settle_ok: true,
        recv_error: false,
        decode_error: false,
        attempts: 1,
    };
    let error = block_on(run_source(
        router.clone(),
        source(),
        RunOptions::until_idle(),
    ))
    .unwrap_err();
    assert!(error.is_retryable());
    assert!(error.should_retain_and_stop());
    assert!(matches!(
        recorder.events().as_slice(),
        [Event::Nack(Duration::ZERO)]
    ));
    open.store(true, Ordering::SeqCst);
    block_on(run_source(router, source(), RunOptions::until_idle())).unwrap();
    assert_eq!(
        &recorder.events()[1..],
        &[Event::Handled("retained-event".into()), Event::Ack]
    );
}

#[test]
fn success_dispatches_then_acks_in_order() {
    let result = run(vec![event_message("ok", None)], RunOptions::until_idle());
    assert!(result.outcome.is_ok());
    // Handler effect is recorded before the ack: ack happens after success.
    assert_eq!(
        result.events,
        vec![Event::Handled("ok".to_string()), Event::Ack]
    );
}

#[test]
fn processes_every_message_then_stops_on_none() {
    let result = run(
        vec![event_message("ok", None), event_message("ok", None)],
        RunOptions::until_idle(),
    );
    assert!(result.outcome.is_ok());
    assert_eq!(
        result.events,
        vec![
            Event::Handled("ok".to_string()),
            Event::Ack,
            Event::Handled("ok".to_string()),
            Event::Ack,
        ]
    );
}

#[cfg(feature = "metrics")]
#[test]
fn metrics_record_success_and_failure_settlement_outcomes() {
    let _guard = crate::metrics::lock_for_tests();
    crate::metrics::reset_for_tests();

    let recorder = Recorder::new();
    let svc = Arc::new(
        router(&recorder)
            .as_ref()
            .clone()
            .named("metrics-runner-settlement"),
    );
    let source = FakeSource {
        queue: vec![event_message("ok", None), event_message("retryable", None)]
            .into_iter()
            .collect(),
        recorder,
        settle_ok: true,
        recv_error: false,
        decode_error: false,
        attempts: 1,
    };

    let outcome = block_on(run_source(svc, source, RunOptions::until_idle()));
    assert!(outcome.is_ok());

    let text = crate::metrics::prometheus_text();
    assert!(
            text.contains(
                "distributed_transport_messages_total{service=\"metrics-runner-settlement\",transport=\"unknown\",message_kind=\"event\",outcome=\"ack\"} 1"
            ),
            "metrics should include the ack outcome:\n{text}"
        );
    assert!(
            text.contains(
                "distributed_transport_messages_total{service=\"metrics-runner-settlement\",transport=\"unknown\",message_kind=\"event\",outcome=\"nack\"} 1"
            ),
            "metrics should include the nack outcome:\n{text}"
        );
    assert!(
            text.contains(
                "distributed_transport_failures_total{service=\"metrics-runner-settlement\",transport=\"unknown\",failure_class=\"retryable\",action=\"nack\"} 1"
            ),
            "metrics should include the retryable failure action:\n{text}"
        );
}

#[cfg(feature = "metrics")]
#[test]
fn metrics_record_settle_failures_before_propagating() {
    let _guard = crate::metrics::lock_for_tests();
    crate::metrics::reset_for_tests();

    let recorder = Recorder::new();
    let svc = Arc::new(
        router(&recorder)
            .as_ref()
            .clone()
            .named("metrics-runner-settle-failure"),
    );
    let source = FakeSource {
        queue: vec![event_message("ok", None)].into_iter().collect(),
        recorder,
        settle_ok: false,
        recv_error: false,
        decode_error: false,
        attempts: 1,
    };

    let outcome = block_on(run_source(svc, source, RunOptions::until_idle()));
    assert!(outcome
        .expect_err("settle error should propagate")
        .is_retryable());

    let text = crate::metrics::prometheus_text();
    assert!(
            text.contains(
                "distributed_transport_failures_total{service=\"metrics-runner-settle-failure\",transport=\"unknown\",failure_class=\"retryable\",action=\"settle_ack\"} 1"
            ),
            "metrics should include the settle failure:\n{text}"
        );
    assert!(
            !text.contains(
                "distributed_transport_messages_total{service=\"metrics-runner-settle-failure\",transport=\"unknown\",message_kind=\"event\",outcome=\"ack\"} 1"
            ),
            "settle failure should not record an ack outcome:\n{text}"
        );
}

#[test]
fn retryable_failure_nacks_without_acking() {
    let result = run(
        vec![event_message("retryable", None)],
        RunOptions::until_idle(),
    );
    assert!(result.outcome.is_ok());
    assert_eq!(
        result.events.first(),
        Some(&Event::Handled("retryable".to_string()))
    );
    assert!(matches!(result.events.get(1), Some(Event::Nack(_))));
    assert!(!result.events.contains(&Event::Ack));
}

#[test]
fn permanent_failure_dead_letters_under_default_policy() {
    let result = run(
        vec![event_message("permanent", None)],
        RunOptions::until_idle(),
    );
    assert!(result.outcome.is_ok());
    assert_eq!(
        result.events.first(),
        Some(&Event::Handled("permanent".to_string()))
    );
    assert!(matches!(result.events.get(1), Some(Event::DeadLetter(..))));
}

#[test]
fn permanent_failure_logs_and_acks_under_log_and_ack_policy() {
    let result = run(
        vec![event_message("permanent", None)],
        RunOptions::until_idle().with_failure_policy(FailurePolicy::LogAndAck),
    );
    assert!(result.outcome.is_ok());
    assert_eq!(result.events.get(1), Some(&Event::Ack));
}

#[test]
fn durable_terminal_failure_nacks_and_stops_regardless_of_permanent_policy() {
    for policy in [FailurePolicy::DeadLetter, FailurePolicy::LogAndAck] {
        let result = run(
            vec![event_message("terminal", None), event_message("ok", None)],
            RunOptions::until_idle().with_failure_policy(policy),
        );
        let error = result
            .outcome
            .expect_err("durable terminal failure must stop the runner");
        assert!(error.is_permanent());
        assert!(error.should_retain_and_stop());
        assert_eq!(
            result.events.first(),
            Some(&Event::Handled("terminal".to_string()))
        );
        assert!(matches!(
            result.events.get(1),
            Some(Event::Nack(Duration::ZERO))
        ));
        assert_eq!(
            result.events.len(),
            2,
            "{policy:?} must neither settle terminal input destructively nor continue"
        );
    }
}

#[test]
fn permanent_failure_nacks_under_retry_policy() {
    let result = run(
        vec![event_message("permanent", None)],
        RunOptions::until_idle().with_failure_policy(FailurePolicy::Retry),
    );
    assert!(result.outcome.is_ok());
    assert!(matches!(result.events.get(1), Some(Event::Nack(_))));
}

#[test]
fn stop_policy_returns_error_without_settling() {
    let result = run(
        vec![event_message("permanent", None), event_message("ok", None)],
        RunOptions::until_idle().with_failure_policy(FailurePolicy::Stop),
    );
    let err = result
        .outcome
        .expect_err("stop policy should surface the error");
    assert!(err.is_permanent());
    // The handler ran, but the message was not settled and the second
    // message was never processed.
    assert_eq!(result.events, vec![Event::Handled("permanent".to_string())]);
}

#[test]
fn recv_error_propagates_and_is_not_swallowed() {
    let result = run_with(
        vec![event_message("ok", None)],
        RunOptions::until_idle(),
        true,
        true,
    );
    let err = result.outcome.expect_err("recv error should propagate");
    assert!(err.is_retryable());
    assert!(result.events.is_empty());
}

#[test]
fn settle_error_propagates_and_is_not_swallowed() {
    let result = run_with(
        vec![event_message("ok", None)],
        RunOptions::until_idle(),
        false,
        false,
    );
    let err = result.outcome.expect_err("settle error should propagate");
    assert!(err.is_retryable());
    // The ack was attempted (recorded) before the error surfaced.
    assert_eq!(
        result.events,
        vec![Event::Handled("ok".to_string()), Event::Ack]
    );
}

#[test]
fn settle_error_on_failure_path_propagates() {
    // A settle failure on the nack/failure-routing branch must propagate too,
    // not just on the ack branch.
    let result = run_with(
        vec![event_message("retryable", None)],
        RunOptions::until_idle(),
        false,
        false,
    );
    let err = result
        .outcome
        .expect_err("nack settle error should propagate");
    assert!(err.is_retryable());
    assert_eq!(
        result.events.first(),
        Some(&Event::Handled("retryable".to_string()))
    );
    assert!(matches!(result.events.get(1), Some(Event::Nack(_))));
}

#[test]
fn unhandled_message_is_acked_and_ignored() {
    // No handler registered for "unrelated": ack-and-ignore, do not dispatch
    // or dead-letter.
    let result = run(
        vec![event_message("unrelated", None), event_message("ok", None)],
        RunOptions::until_idle(),
    );
    assert!(result.outcome.is_ok());
    assert_eq!(
        result.events,
        vec![Event::Ack, Event::Handled("ok".to_string()), Event::Ack]
    );
}

fn run_decode_error(messages: Vec<Message>, options: RunOptions) -> RunResult {
    let recorder = Recorder::new();
    let svc = router(&recorder);
    let source = FakeSource {
        queue: messages.into_iter().collect(),
        recorder: recorder.clone(),
        settle_ok: true,
        recv_error: false,
        decode_error: true,
        attempts: 1,
    };
    let options = options.or_dead_letters(|| recorder.clone());
    let outcome = block_on(run_source(svc, source, options));
    RunResult {
        outcome,
        events: recorder.events(),
    }
}

#[test]
fn corrupt_row_dead_letters_under_default_policy_not_acked_and_ignored() {
    // The corrupt delivery has an unhandled (empty) name, which would
    // otherwise fall into the ack-and-ignore path and silently drop it. The
    // decode error must instead route it through the failure policy.
    let result = run_decode_error(vec![event_message("", None)], RunOptions::until_idle());
    assert!(result.outcome.is_ok());
    // Stored first, acknowledged second, never handled.
    assert!(matches!(
        result.events.as_slice(),
        [Event::DeadLetter(reason, ..), Event::Ack] if reason.contains("corrupt row")
    ));
}

#[test]
fn corrupt_row_stops_under_stop_policy_with_permanent_error() {
    let result = run_decode_error(
        vec![event_message("", None)],
        RunOptions::until_idle().with_failure_policy(FailurePolicy::Stop),
    );
    let err = result
        .outcome
        .expect_err("stop policy surfaces the decode error");
    assert!(err.is_permanent());
    assert!(
        result.events.is_empty(),
        "stop does not settle the corrupt row"
    );
}

#[test]
fn run_source_future_is_send() {
    // Guards the documented multi-threaded-executor contract for the common
    // (no-inbox) path: the runner future must be Send.
    fn assert_send<T: Send>(_: &T) {}
    let recorder = Recorder::new();
    let svc = router(&recorder);
    let source = FakeSource {
        queue: VecDeque::new(),
        recorder,
        settle_ok: true,
        recv_error: false,
        decode_error: false,
        attempts: 1,
    };
    let future = run_source(svc, source, RunOptions::until_idle());
    assert_send(&future);
    // Drive it to completion (empty source -> immediate Ok).
    assert!(block_on(future).is_ok());
}

// --- the delivery contract ------------------------------------------------

fn source(recorder: &Arc<Recorder>, names: &[&str], attempts: u32) -> FakeSource {
    FakeSource {
        queue: names
            .iter()
            .map(|name| event_message(name, Some("m1")))
            .collect(),
        recorder: recorder.clone(),
        settle_ok: true,
        recv_error: false,
        decode_error: false,
        attempts,
    }
}

#[test]
fn a_retry_waits_longer_with_each_delivery() {
    let backoff = RetryBackoff::new(Duration::from_millis(10), Duration::from_millis(35));
    let delay_on = |attempts| {
        let recorder = Recorder::new();
        let options = RunOptions::until_idle().with_backoff(backoff);
        block_on(run_source(
            router(&recorder),
            source(&recorder, &["retryable"], attempts),
            options,
        ))
        .unwrap();
        match recorder.events().as_slice() {
            [Event::Handled(_), Event::Nack(delay)] => *delay,
            other => panic!("expected handle then nack, got {other:?}"),
        }
    };
    assert_eq!(delay_on(1), Duration::from_millis(10));
    assert_eq!(delay_on(2), Duration::from_millis(20));
    assert_eq!(delay_on(3), Duration::from_millis(35));
}

#[test]
fn a_retry_that_reaches_max_attempts_is_dead_lettered_then_acked() {
    let recorder = Recorder::new();
    let svc = Arc::new(router(&recorder).as_ref().clone().named("billing"));
    let options = RunOptions::until_idle()
        .with_max_attempts(3)
        .with_dead_letters(recorder.clone());
    block_on(run_source(
        svc.clone(),
        source(&recorder, &["retryable"], 2),
        options.clone(),
    ))
    .unwrap();
    assert!(matches!(
        recorder.events().as_slice(),
        [Event::Handled(_), Event::Nack(_)]
    ));

    block_on(run_source(
        svc,
        source(&recorder, &["retryable", "ok"], 3),
        options,
    ))
    .unwrap();
    // Stored with the reason, the consumer and the attempt count, and only
    // then acknowledged; the next message is still handled.
    assert_eq!(
        &recorder.events()[2..],
        &[
            Event::Handled("retryable".into()),
            Event::DeadLetter(
                "transport error (retryable): infra".into(),
                "billing".into(),
                3
            ),
            Event::Ack,
            Event::Handled("ok".into()),
            Event::Ack,
        ]
    );
}

#[test]
fn a_dead_letter_with_nowhere_to_go_is_handed_back_and_ends_the_run() {
    for sink in [None, Some(Arc::new(BrokenSink) as Arc<dyn DeadLetterSink>)] {
        let recorder = Recorder::new();
        let mut options = RunOptions::until_idle();
        options.dead_letters = sink;
        let error = block_on(run_source(
            router(&recorder),
            source(&recorder, &["permanent", "ok"], 1),
            options,
        ))
        .unwrap_err();
        assert!(
            error.message().contains("dead-letter"),
            "unexpected error: {error}"
        );
        // Never acknowledged, and nothing after it was consumed.
        assert_eq!(
            recorder.events(),
            vec![
                Event::Handled("permanent".into()),
                Event::Nack(Duration::ZERO)
            ]
        );
    }
}

/// Yields its messages, then reports idle forever. `ready` trips the shutdown
/// signal the third time the runner waits on it.
struct QuietSource {
    inner: FakeSource,
    shutdown: Shutdown,
    waits: Arc<Mutex<u32>>,
}

impl MessageSource for QuietSource {
    type Received = FakeReceived;
    async fn recv(&mut self) -> Result<Option<FakeReceived>, TransportError> {
        self.inner.recv().await
    }
    async fn ready(&mut self) {
        let mut waits = self.waits.lock().unwrap();
        *waits += 1;
        if *waits == 3 {
            self.shutdown.trigger();
        }
    }
}

#[test]
fn a_quiet_source_does_not_end_a_run_but_shutdown_does() {
    let recorder = Recorder::new();
    let shutdown = Shutdown::new();
    let waits = Arc::new(Mutex::new(0));
    let quiet = QuietSource {
        inner: source(&recorder, &["ok"], 1),
        shutdown: shutdown.clone(),
        waits: waits.clone(),
    };
    block_on(run_source(
        router(&recorder),
        quiet,
        RunOptions::until_shutdown(shutdown.clone()),
    ))
    .unwrap();
    assert_eq!(
        recorder.events(),
        vec![Event::Handled("ok".into()), Event::Ack]
    );
    assert_eq!(*waits.lock().unwrap(), 3, "the run kept waiting while idle");

    // A run that starts already shut down receives nothing.
    let untouched = Recorder::new();
    block_on(run_source(
        router(&untouched),
        source(&untouched, &["ok"], 1),
        RunOptions::until_shutdown(shutdown),
    ))
    .unwrap();
    assert!(untouched.events().is_empty());
}
