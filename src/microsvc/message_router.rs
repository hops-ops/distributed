//! `microsvc::Service` as transport [`MessageRouter`]s.
//!
//! This is the bridge that lets the bus consume side run a service without the
//! bus naming `Service`. Handler-error classification into the transport's
//! retryable/permanent vocabulary happens here, on the microsvc side, so the
//! bus-core runner only ever sees an already-classified `TransportError`.
//!
//! A router is one durable consumer. A service is several: the [`Service`]
//! itself consumes its commands and event handlers, and every projector
//! consumes through a [`ProjectorConsumer`] of its own, so a projector that
//! stops on a terminal failure stops alone.

use std::sync::Arc;

use sha2::{Digest, Sha256};

use crate::bus::{LaneSet, MessageRouter, OrderedDelivery, TransportError};
use crate::bus::{Message, MessageKind, SubscriptionPlan};
use crate::microsvc::Service;

impl MessageRouter for Service {
    fn consumer_group(&self) -> Option<&str> {
        self.name()
    }

    fn handles(&self, kind: MessageKind, name: &str) -> bool {
        self.handles_message(kind, name)
    }

    /// The commands and the events with a handler. Events only projectors
    /// consume are subscribed to by their [`ProjectorConsumer`]s.
    fn subscription_plan(&self) -> SubscriptionPlan {
        // Call the inherent method, not this trait method (which would recurse).
        let mut plan = Service::subscription_plan(self);
        plan.events
            .retain(|event| self.handles_message(MessageKind::Event, event));
        plan
    }

    async fn dispatch(&self, message: &Message) -> Result<(), TransportError> {
        self.dispatch_message(message)
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
        _ordered: Option<&OrderedDelivery>,
        lane: usize,
    ) -> Result<(), TransportError> {
        self.dispatch_lane_message(message, Some(lane))
            .await
            .map(|_| ())
            .map_err(TransportError::from)
    }
}

/// One projector of a [`Service`] as a router of its own.
///
/// Built by [`Service::projector_consumers`]. Its durable consumer is the
/// service's group scoped by the projector, so it keeps its own cursor, retry
/// and dead-letter path on every bus.
pub struct ProjectorConsumer {
    service: Arc<Service>,
    projector: usize,
    scope: String,
}

impl ProjectorConsumer {
    pub(crate) fn new(service: Arc<Service>, projector: usize) -> Self {
        let scope = consumer_scope(&service.projector_route(projector).name);
        Self {
            service,
            projector,
            scope,
        }
    }

    /// The projector's topology name.
    pub fn projection(&self) -> &str {
        &self.service.projector_route(self.projector).name
    }
}

impl MessageRouter for ProjectorConsumer {
    fn consumer_group(&self) -> Option<&str> {
        self.service.name()
    }

    fn consumer_scope(&self) -> Option<&str> {
        Some(&self.scope)
    }

    fn handles(&self, kind: MessageKind, name: &str) -> bool {
        kind == MessageKind::Event
            && self
                .service
                .projector_route(self.projector)
                .events
                .iter()
                .any(|event| event == name)
    }

    fn subscription_plan(&self) -> SubscriptionPlan {
        SubscriptionPlan {
            commands: Vec::new(),
            events: self.service.projector_route(self.projector).events.clone(),
        }
    }

    async fn dispatch(&self, message: &Message) -> Result<(), TransportError> {
        self.dispatch_ordered(message, None).await
    }

    async fn dispatch_ordered(
        &self,
        message: &Message,
        ordered: Option<&OrderedDelivery>,
    ) -> Result<(), TransportError> {
        self.service
            .dispatch_projector(self.projector, message, ordered)
            .await
            .map_err(TransportError::from)
    }
}

/// A projector name as a consumer-group suffix every bus accepts.
///
/// A name made of ASCII letters, digits, `-` and `_` is used as it is. Any
/// other name has its other characters replaced, is cut to a bounded length,
/// and gets a digest of the whole name so two names cannot share a consumer.
fn consumer_scope(projector: &str) -> String {
    const MAX_PLAIN_BYTES: usize = 48;
    let portable: String = projector
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if portable == projector && portable.len() <= MAX_PLAIN_BYTES {
        return portable;
    }
    let digest = Sha256::digest(projector.as_bytes());
    let digest: String = digest[..6].iter().map(|b| format!("{b:02x}")).collect();
    let head = &portable[..portable.len().min(MAX_PLAIN_BYTES)];
    format!("{head}-{digest}")
}

#[cfg(test)]
mod tests {
    use super::consumer_scope;

    #[test]
    fn portable_projector_names_are_their_own_scope() {
        assert_eq!(consumer_scope("todo_board-v2"), "todo_board-v2");
    }

    #[test]
    fn other_projector_names_get_a_portable_scope_that_stays_distinct() {
        let dotted = consumer_scope("todo.board");
        let underscored = consumer_scope("todo_board");
        assert!(dotted.starts_with("todo_board-"), "{dotted}");
        assert_ne!(dotted, underscored);
        assert_ne!(dotted, consumer_scope("todo/board"));

        let long = consumer_scope(&"a".repeat(128));
        assert_eq!(long.len(), 48 + 1 + 12);
        assert_ne!(long, consumer_scope(&"a".repeat(127)));
    }
}
