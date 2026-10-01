//! `microsvc::Service` as a transport [`MessageRouter`].
//!
//! This is the bridge that lets the bus consume side run a service without the
//! bus naming `Service`. Handler-error classification into the transport's
//! retryable/permanent vocabulary happens here, on the microsvc side, so the
//! bus-core runner only ever sees an already-classified `TransportError`.

use crate::bus::{LaneSet, MessageRouter, OrderedDelivery, TransportError};
use crate::microsvc::{Message, MessageKind, Service, SubscriptionPlan};

impl MessageRouter for Service {
    fn consumer_group(&self) -> Option<&str> {
        self.name()
    }

    fn handles(&self, kind: MessageKind, name: &str) -> bool {
        self.handles_message(kind, name)
    }

    fn subscription_plan(&self) -> SubscriptionPlan {
        // Call the inherent method, not this trait method (which would recurse).
        Service::subscription_plan(self)
    }

    async fn dispatch(&self, message: &Message) -> Result<(), TransportError> {
        self.dispatch_message(message)
            .await
            .map(|_| ())
            .map_err(TransportError::from)
    }

    async fn dispatch_ordered(
        &self,
        message: &Message,
        ordered: Option<&OrderedDelivery>,
    ) -> Result<(), TransportError> {
        self.dispatch_ordered_message(message, ordered)
            .await
            .map(|_| ())
            .map_err(TransportError::from)
    }

    fn delivery_lanes(&self) -> usize {
        self.delivery_lane_names().len()
    }

    fn lanes_for(&self, kind: MessageKind, name: &str) -> LaneSet {
        self.lanes_for_message(kind, name)
    }

    async fn dispatch_lane(
        &self,
        message: &Message,
        ordered: Option<&OrderedDelivery>,
        lane: usize,
    ) -> Result<(), TransportError> {
        self.dispatch_lane_message(message, ordered, Some(lane))
            .await
            .map(|_| ())
            .map_err(TransportError::from)
    }
}
