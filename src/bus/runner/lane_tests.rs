//! Delivery-lane runner contract (`docs/consumer-delivery-lanes.md`).
//!
//! Runtime-free like the sequential runner tests: a busy-poll executor with a
//! no-op waker. Gated handlers wake themselves so `FuturesUnordered` re-polls
//! them, and `block_on` fails after a bounded number of polls instead of
//! hanging, so a lane that is blocked behind another lane is a test failure.
use super::run_source;
use crate::bus::source::{MessageSource, ReceivedMessage};
use crate::bus::{
    FailurePolicy, LaneSet, Message, MessageKind, MessageRouter, RunOptions, TransportError,
};
use std::collections::VecDeque;
use std::future::{poll_fn, Future};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;

const POLL_LIMIT: usize = 200_000;

fn block_on<F: Future>(future: F) -> F::Output {
    use std::ptr;
    use std::task::{Context, RawWaker, RawWakerVTable, Waker};

    const VTABLE: RawWakerVTable = RawWakerVTable::new(
        |_| RawWaker::new(ptr::null(), &VTABLE),
        |_| {},
        |_| {},
        |_| {},
    );
    let waker = unsafe { Waker::from_raw(RawWaker::new(ptr::null(), &VTABLE)) };
    let mut cx = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    for _ in 0..POLL_LIMIT {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
    }
    panic!("runner made no progress: a lane is blocked behind another lane");
}

/// Resolve once `ready` returns true, yielding (and self-waking) until then.
async fn wait_until(ready: impl Fn() -> bool) {
    poll_fn(|cx| {
        if ready() {
            Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Event {
    Received(String),
    Ran(usize, String),
    Ack(String),
    Nack(String),
    DeadLetter(String),
}

#[derive(Default)]
struct Log(Mutex<Vec<Event>>);

impl Log {
    fn push(&self, event: Event) {
        self.0.lock().unwrap().push(event);
    }
    fn events(&self) -> Vec<Event> {
        self.0.lock().unwrap().clone()
    }
    fn ran(&self, lane: usize) -> Vec<String> {
        self.events()
            .into_iter()
            .filter_map(|event| match event {
                Event::Ran(l, id) if l == lane => Some(id),
                _ => None,
            })
            .collect()
    }
    fn position(&self, wanted: &Event) -> usize {
        self.events()
            .iter()
            .position(|event| event == wanted)
            .unwrap_or_else(|| panic!("missing {wanted:?} in {:?}", self.events()))
    }
}

struct Received {
    message: Message,
    log: Arc<Log>,
    unsettled: Arc<AtomicUsize>,
}

impl Received {
    fn id(&self) -> String {
        self.message.id().unwrap().to_owned()
    }
    fn settle(self, event: Event) -> Result<(), TransportError> {
        self.unsettled.fetch_sub(1, Ordering::SeqCst);
        self.log.push(event);
        Ok(())
    }
}

impl ReceivedMessage for Received {
    fn message(&self) -> &Message {
        &self.message
    }
    async fn ack(self) -> Result<(), TransportError> {
        let id = self.id();
        self.settle(Event::Ack(id))
    }
    async fn nack(self, _reason: &str) -> Result<(), TransportError> {
        let id = self.id();
        self.settle(Event::Nack(id))
    }
    async fn dead_letter(self, _reason: &str) -> Result<(), TransportError> {
        let id = self.id();
        self.settle(Event::DeadLetter(id))
    }
}

struct Source {
    queue: VecDeque<Message>,
    log: Arc<Log>,
    independent: bool,
    unsettled: Arc<AtomicUsize>,
    max_unsettled: Arc<AtomicUsize>,
}

impl MessageSource for Source {
    type Received = Received;

    fn settles_independently(&self) -> bool {
        self.independent
    }

    async fn recv(&mut self) -> Result<Option<Received>, TransportError> {
        let Some(message) = self.queue.pop_front() else {
            return Ok(None);
        };
        let unsettled = self.unsettled.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_unsettled.fetch_max(unsettled, Ordering::SeqCst);
        self.log
            .push(Event::Received(message.id().unwrap().to_owned()));
        Ok(Some(Received {
            message,
            log: self.log.clone(),
            unsettled: self.unsettled.clone(),
        }))
    }
}

type Behavior = Arc<dyn Fn(usize, &Message) -> Result<(), TransportError> + Send + Sync>;
type Gate = Arc<dyn Fn(usize, &Message) -> bool + Send + Sync>;

/// Lane 0 = "projection" (default), lane 1 = "process". Every message named
/// `both` reaches both lanes; `process.only` reaches lane 1; `unhandled` none.
struct LaneRouter {
    log: Arc<Log>,
    behavior: Behavior,
    gate: Gate,
}

impl MessageRouter for LaneRouter {
    fn consumer_group(&self) -> Option<&str> {
        Some("lane-test")
    }
    fn handles(&self, kind: MessageKind, name: &str) -> bool {
        !self.lanes_for(kind, name).is_empty()
    }
    fn subscription_plan(&self) -> crate::bus::SubscriptionPlan {
        crate::bus::SubscriptionPlan::default()
    }
    async fn dispatch(&self, message: &Message) -> Result<(), TransportError> {
        for lane in self.lanes_for(message.kind, message.name()).iter() {
            self.dispatch_lane(message, None, lane).await?;
        }
        Ok(())
    }
    fn delivery_lanes(&self) -> usize {
        2
    }
    fn lanes_for(&self, _kind: MessageKind, name: &str) -> LaneSet {
        match name {
            "both" => LaneSet::single(0).with(1),
            "process.only" => LaneSet::single(1),
            _ => LaneSet::EMPTY,
        }
    }
    async fn dispatch_lane(
        &self,
        message: &Message,
        _ordered: Option<&crate::bus::OrderedDelivery>,
        lane: usize,
    ) -> Result<(), TransportError> {
        let gate = self.gate.clone();
        let message_for_gate = message.clone();
        wait_until(move || gate(lane, &message_for_gate)).await;
        self.log
            .push(Event::Ran(lane, message.id().unwrap().to_owned()));
        (self.behavior)(lane, message)
    }
}

fn message(name: &str, id: &str) -> Message {
    Message::new(name, MessageKind::Event, b"{}".to_vec()).with_id(id)
}

struct Harness {
    log: Arc<Log>,
    max_unsettled: Arc<AtomicUsize>,
    outcome: Result<(), TransportError>,
}

fn run_lanes(
    messages: Vec<Message>,
    independent: bool,
    options: RunOptions,
    behavior: Behavior,
    gate: impl Fn(&Arc<Log>) -> Gate,
) -> Harness {
    let log = Arc::new(Log::default());
    let max_unsettled = Arc::new(AtomicUsize::new(0));
    let router = Arc::new(LaneRouter {
        log: log.clone(),
        behavior,
        gate: gate(&log),
    });
    let source = Source {
        queue: messages.into_iter().collect(),
        log: log.clone(),
        independent,
        unsettled: Arc::new(AtomicUsize::new(0)),
        max_unsettled: max_unsettled.clone(),
    };
    let outcome = block_on(run_source(router, source, options));
    Harness {
        log,
        max_unsettled,
        outcome,
    }
}

fn ok() -> Behavior {
    Arc::new(|_, _| Ok(()))
}

fn open() -> impl Fn(&Arc<Log>) -> Gate {
    |_| Arc::new(|_, _| true)
}

/// The slow lane-0 route for `m1` finishes only after the process lane has
/// handled every later message, which is impossible if lanes were sequential.
#[test]
fn slow_projection_lane_does_not_delay_process_lane() {
    let messages = vec![
        message("both", "m1"),
        message("both", "m2"),
        message("process.only", "m3"),
    ];
    let harness = run_lanes(messages, true, RunOptions::idempotent(), ok(), |log| {
        let log = log.clone();
        Arc::new(move |lane, message| {
            lane != 0 || message.id() != Some("m1") || log.ran(1).len() == 3
        })
    });
    harness.outcome.unwrap();
    let log = &harness.log;
    assert_eq!(log.ran(1), ["m1", "m2", "m3"], "process lane keeps order");
    assert_eq!(log.ran(0), ["m1", "m2"], "projection lane keeps order");
    // m3 (process only) settles before m1, whose slow lane was still running.
    assert!(log.position(&Event::Ack("m3".into())) < log.position(&Event::Ack("m1".into())));
    // Every delivery is acknowledged only after all of its lanes ran.
    for id in ["m1", "m2"] {
        let ack = log.position(&Event::Ack(id.into()));
        assert!(log.position(&Event::Ran(0, id.into())) < ack);
        assert!(log.position(&Event::Ran(1, id.into())) < ack);
    }
}

#[test]
fn retryable_failure_in_one_lane_nacks_after_every_lane_finished() {
    let harness = run_lanes(
        vec![message("both", "m1"), message("both", "m2")],
        true,
        RunOptions::idempotent(),
        Arc::new(|lane, message| {
            if lane == 1 && message.id() == Some("m1") {
                Err(TransportError::retryable("cell unavailable"))
            } else {
                Ok(())
            }
        }),
        open(),
    );
    harness.outcome.unwrap();
    let log = &harness.log;
    let nack = log.position(&Event::Nack("m1".into()));
    assert!(log.position(&Event::Ran(0, "m1".into())) < nack);
    assert!(log.position(&Event::Ran(1, "m1".into())) < nack);
    assert!(!log.events().contains(&Event::Ack("m1".into())));
    assert!(log.events().contains(&Event::Ack("m2".into())));
}

#[test]
fn permanent_failure_applies_failure_policy_once_per_delivery() {
    let harness = run_lanes(
        vec![message("both", "m1")],
        true,
        RunOptions::idempotent(),
        Arc::new(|lane, _| {
            if lane == 0 {
                Err(TransportError::permanent("rejected"))
            } else {
                Ok(())
            }
        }),
        open(),
    );
    harness.outcome.unwrap();
    let settled: Vec<_> = harness
        .log
        .events()
        .into_iter()
        .filter(|event| matches!(event, Event::Ack(_) | Event::Nack(_) | Event::DeadLetter(_)))
        .collect();
    assert_eq!(settled, [Event::DeadLetter("m1".into())]);
}

#[test]
fn retryable_wins_over_permanent_so_a_recoverable_delivery_is_retained() {
    let harness = run_lanes(
        vec![message("both", "m1")],
        true,
        RunOptions::idempotent(),
        Arc::new(|lane, _| {
            if lane == 0 {
                Err(TransportError::permanent("rejected"))
            } else {
                Err(TransportError::retryable("transient"))
            }
        }),
        open(),
    );
    harness.outcome.unwrap();
    assert!(harness.log.events().contains(&Event::Nack("m1".into())));
    assert!(!harness
        .log
        .events()
        .contains(&Event::DeadLetter("m1".into())));
}

#[test]
fn window_bounds_unsettled_deliveries() {
    let released = Arc::new(AtomicBool::new(false));
    let messages = (1..=6)
        .map(|n| message("process.only", &format!("m{n}")))
        .collect();
    let release = released.clone();
    let harness = run_lanes(
        messages,
        true,
        RunOptions::idempotent().with_lane_window(2),
        ok(),
        move |log| {
            let log = log.clone();
            let release = release.clone();
            Arc::new(move |_, _| {
                // Hold the lane until the runner has had every chance to
                // over-receive, then release it.
                let received = log
                    .events()
                    .iter()
                    .filter(|event| matches!(event, Event::Received(_)))
                    .count();
                if received >= 2 {
                    release.store(true, Ordering::SeqCst);
                }
                release.load(Ordering::SeqCst)
            })
        },
    );
    harness.outcome.unwrap();
    assert!(released.load(Ordering::SeqCst));
    assert_eq!(harness.max_unsettled.load(Ordering::SeqCst), 2);
    assert_eq!(harness.log.ran(1), ["m1", "m2", "m3", "m4", "m5", "m6"]);
}

#[test]
fn stop_class_failure_halts_only_its_lane_and_retains_queued_deliveries() {
    let harness = run_lanes(
        vec![
            message("both", "m1"),
            message("both", "m2"),
            message("both", "m3"),
        ],
        true,
        RunOptions::idempotent(),
        Arc::new(|lane, message| {
            if lane == 0 && message.id() == Some("m1") {
                Err(TransportError::permanent("durable projection failure").retain_and_stop())
            } else {
                Ok(())
            }
        }),
        |log| {
            // Keep lane 0 on m1 until lane 1 has queued work behind it.
            let log = log.clone();
            Arc::new(move |lane, message| {
                lane != 0 || message.id() != Some("m1") || !log.ran(1).is_empty()
            })
        },
    );
    let error = harness.outcome.unwrap_err();
    assert!(error.should_retain_and_stop());
    let log = &harness.log;
    // The halted lane never runs later deliveries...
    assert_eq!(log.ran(0), ["m1"]);
    // ...and they are NAKed (retained), never acknowledged.
    for id in ["m1", "m2", "m3"] {
        let settled = log.events().into_iter().find(|event| {
            matches!(event, Event::Ack(x) | Event::Nack(x) | Event::DeadLetter(x) if x == id)
        });
        assert!(
            matches!(settled, None | Some(Event::Nack(_))),
            "{id} must not be acknowledged after its lane halted: {settled:?}"
        );
    }
    assert!(log.events().contains(&Event::Nack("m1".into())));
}

#[test]
fn stop_policy_on_permanent_failure_stops_without_settling_that_delivery() {
    let harness = run_lanes(
        vec![message("both", "m1")],
        true,
        RunOptions::idempotent().with_failure_policy(FailurePolicy::Stop),
        Arc::new(|lane, _| {
            if lane == 1 {
                Err(TransportError::permanent("nope"))
            } else {
                Ok(())
            }
        }),
        open(),
    );
    assert!(harness.outcome.unwrap_err().is_permanent());
    assert!(!harness
        .log
        .events()
        .iter()
        .any(|event| matches!(event, Event::Ack(_) | Event::Nack(_) | Event::DeadLetter(_))));
}

#[test]
fn unhandled_messages_are_acked_without_running_lanes() {
    let harness = run_lanes(
        vec![message("unhandled", "m1")],
        true,
        RunOptions::idempotent(),
        ok(),
        open(),
    );
    harness.outcome.unwrap();
    assert_eq!(
        harness.log.events(),
        [Event::Received("m1".into()), Event::Ack("m1".into())]
    );
}

/// A source without independent settlement keeps the sequential contract:
/// one delivery at a time, every lane's routes in order.
#[test]
fn positional_sources_keep_sequential_delivery() {
    let harness = run_lanes(
        vec![message("both", "m1"), message("both", "m2")],
        false,
        RunOptions::idempotent(),
        ok(),
        open(),
    );
    harness.outcome.unwrap();
    assert_eq!(
        harness.log.events(),
        [
            Event::Received("m1".into()),
            Event::Ran(0, "m1".into()),
            Event::Ran(1, "m1".into()),
            Event::Ack("m1".into()),
            Event::Received("m2".into()),
            Event::Ran(0, "m2".into()),
            Event::Ran(1, "m2".into()),
            Event::Ack("m2".into()),
        ]
    );
    assert_eq!(harness.max_unsettled.load(Ordering::SeqCst), 1);
}

/// Redelivery after a NAK runs every lane again; handlers stay idempotent and
/// the second attempt acknowledges.
#[test]
fn redelivery_after_nack_reruns_all_lanes_then_acks() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let seen = attempts.clone();
    let log = Arc::new(Log::default());
    let router = Arc::new(LaneRouter {
        log: log.clone(),
        behavior: Arc::new(move |lane, _| {
            if lane == 1 && seen.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(TransportError::retryable("first attempt"))
            } else {
                Ok(())
            }
        }),
        gate: Arc::new(|_, _| true),
    });
    let source = |messages: Vec<Message>| Source {
        queue: messages.into_iter().collect(),
        log: log.clone(),
        independent: true,
        unsettled: Arc::new(AtomicUsize::new(0)),
        max_unsettled: Arc::new(AtomicUsize::new(0)),
    };
    block_on(run_source(
        router.clone(),
        source(vec![message("both", "m1")]),
        RunOptions::idempotent(),
    ))
    .unwrap();
    // The broker redelivers the NAKed message.
    block_on(run_source(
        router,
        source(vec![message("both", "m1")]),
        RunOptions::idempotent(),
    ))
    .unwrap();
    assert_eq!(log.ran(0), ["m1", "m1"]);
    assert_eq!(log.ran(1), ["m1", "m1"]);
    let settled: Vec<_> = log
        .events()
        .into_iter()
        .filter(|event| matches!(event, Event::Ack(_) | Event::Nack(_)))
        .collect();
    assert_eq!(settled, [Event::Nack("m1".into()), Event::Ack("m1".into())]);
}
