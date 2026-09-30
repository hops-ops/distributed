//! The consume seam: a minimal router the [`run_source`](super::run_source) loop
//! and the [`BusConsumer`](super::BusConsumer) adapters depend on instead of the
//! concrete `microsvc::Service`.
//!
//! Implemented by `microsvc::Service` (the rich registry backed by typed route
//! bundles) and the dependency-free `Handlers` builder. Splitting the
//! consume path behind this trait is what lets the bus become a standalone module
//! that does not name `Service`. See `specs/bus-module-decomposition`.

use std::future::Future;

use super::TransportError;
use super::{Message, MessageKind, OrderedDelivery, SubscriptionPlan};

/// What the receive loop and the consumer adapters need from a message consumer.
///
/// Three responsibilities, each used by a different part of the consume path:
///
/// - [`handles`](MessageRouter::handles) — the runner's ack-and-ignore vs dispatch
///   predicate;
/// - [`subscription_plan`](MessageRouter::subscription_plan) — the command/event
///   names the adapters use to build their broker topology *before* the run loop;
/// - [`dispatch`](MessageRouter::dispatch) — route and run one delivered message,
///   returning an already-classified [`TransportError`] so the bus-core runner
///   never sees a `microsvc::HandlerError`.
///
/// Not dyn-compatible (the RPITIT `dispatch`), exactly like [`Bus`](super::Bus) /
/// [`BusConsumer`](super::BusConsumer): consume it as `Arc<R>` or a generic
/// `<R: MessageRouter>`, never `Arc<dyn MessageRouter>`.
pub trait MessageRouter: Send + Sync {
    /// Stable identity for this consumer, used by broker adapters as the default
    /// durable consumer group when the bus itself was not configured with one.
    fn consumer_group(&self) -> Option<&str> {
        None
    }

    /// Whether this router has a handler for `(kind, name)`. The runner acks and
    /// ignores a delivered message it does not handle rather than dead-lettering it.
    fn handles(&self, kind: MessageKind, name: &str) -> bool;

    /// The command and event names a transport should subscribe to, derived from
    /// the router's registered handlers.
    fn subscription_plan(&self) -> SubscriptionPlan;

    /// Route and run one delivered message. `Ok(())` means the handler (and its
    /// effects) succeeded; the error is already classified retryable/permanent.
    fn dispatch(
        &self,
        message: &Message,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;

    /// Route one message together with adapter-authenticated ordering evidence.
    ///
    /// Ordinary routers and legacy event handlers keep the default behavior.
    /// A causal projector-aware router overrides this method and fails closed
    /// for its projector routes when `ordered` is absent.
    fn dispatch_ordered(
        &self,
        message: &Message,
        _ordered: Option<&OrderedDelivery>,
    ) -> impl Future<Output = Result<(), TransportError>> + Send {
        self.dispatch(message)
    }

    /// Number of independent delivery lanes (at least 1).
    ///
    /// Lanes are opt-in route groups that a receive loop may run concurrently
    /// while each lane keeps delivery order. See
    /// `docs/consumer-delivery-lanes.md`. The default single lane preserves the
    /// sequential contract.
    fn delivery_lanes(&self) -> usize {
        1
    }

    /// Lanes that have a handler for `(kind, name)`. Empty means unhandled.
    fn lanes_for(&self, kind: MessageKind, name: &str) -> LaneSet {
        if self.handles(kind, name) {
            LaneSet::single(0)
        } else {
            LaneSet::EMPTY
        }
    }

    /// Run only the routes of `lane` for one delivered message.
    ///
    /// Routers that report more than one lane must override this. The default
    /// is correct only for the single default lane.
    fn dispatch_lane(
        &self,
        message: &Message,
        ordered: Option<&OrderedDelivery>,
        _lane: usize,
    ) -> impl Future<Output = Result<(), TransportError>> + Send {
        self.dispatch_ordered(message, ordered)
    }
}

/// A set of delivery lane indexes (at most [`LaneSet::MAX_LANES`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct LaneSet(u64);

impl LaneSet {
    /// Highest supported number of lanes for one router.
    pub const MAX_LANES: usize = 64;
    /// No lanes: the router does not handle the message.
    pub const EMPTY: Self = Self(0);

    /// A set containing only `lane`.
    pub fn single(lane: usize) -> Self {
        Self::EMPTY.with(lane)
    }

    /// Add `lane` to the set.
    ///
    /// # Panics
    /// When `lane >= MAX_LANES`.
    pub fn with(self, lane: usize) -> Self {
        assert!(lane < Self::MAX_LANES, "delivery lane index out of range");
        Self(self.0 | (1 << lane))
    }

    /// Whether the set is empty.
    pub fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Number of lanes in the set.
    pub fn len(self) -> usize {
        self.0.count_ones() as usize
    }

    /// Whether `lane` is in the set.
    pub fn contains(self, lane: usize) -> bool {
        lane < Self::MAX_LANES && self.0 & (1 << lane) != 0
    }

    /// Lane indexes in ascending order.
    pub fn iter(self) -> impl Iterator<Item = usize> {
        (0..Self::MAX_LANES).filter(move |lane| self.contains(*lane))
    }
}
