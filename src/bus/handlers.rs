//! [`Handlers`] — a dependency-free inline consumer registry.
//!
//! The standalone, `Service`-free way to consume the bus: register a closure per
//! `(kind, name)` and run it with `bus.listen`/`bus.subscribe`. It is the second
//! implementation of [`MessageRouter`] (the first being `microsvc::Service`),
//! and the analog of the Node `bus.listen('seat.reserved', handler)` ergonomic.
//!
//! ```ignore
//! let handlers = Handlers::new()
//!     .on_event("seat.reserved", |msg: &Message| async move { /* ... */ Ok(()) })
//!     .on_command("place.bet",   |msg: &Message| async move { Ok(()) });
//! bus.subscribe(Arc::new(handlers), RunOptions::until_idle()).await?;
//! ```
//!
//! Deliberately minimal: handlers take `&Message` (no `Context`, dependencies,
//! guards, or sessions) and the registry is idempotent-only. The rich path —
//! typed input, guards, `Session`, dependencies, the inbox hook — stays on
//! `microsvc::Service` plus typed route bundles. A handler returns `Ok(())` to
//! ack, `Err` to nack (the runner classifies retryable vs permanent via the
//! [`TransportError`]).
//!
//! The win over unit `Routes<()>` is dropping `Context` and not depending on
//! `microsvc` at all — a `Service` facade is impossible here, because the bus
//! cannot depend up into `microsvc` where `Service` lives.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::{Message, MessageKind, SubscriptionPlan};
use super::{MessageRouter, TransportError};

type HandlerFuture<'a> = Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + 'a>>;
type HandlerFn = dyn for<'a> Fn(&'a Message) -> HandlerFuture<'a> + Send + Sync;

/// Lets an `async fn(msg: &Message) -> Result<(), TransportError>` register
/// directly as a handler. The higher-ranked bound ties the returned future's
/// lifetime to the borrowed [`Message`], which a plain generic future parameter
/// cannot express (the same shape `microsvc::Service` uses for `Context`).
pub trait MessageHandler<'a>: Send + Sync {
    /// The future returned by the handler for a message borrowed for `'a`.
    type Future: Future<Output = Result<(), TransportError>> + Send + 'a;
    /// Run the handler against the borrowed message.
    fn call(&self, message: &'a Message) -> Self::Future;
}

impl<'a, F, Fut> MessageHandler<'a> for F
where
    F: Fn(&'a Message) -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), TransportError>> + Send + 'a,
{
    type Future = Fut;
    fn call(&self, message: &'a Message) -> Fut {
        self(message)
    }
}

fn boxed_handler<F>(handler: F) -> Arc<HandlerFn>
where
    F: for<'a> MessageHandler<'a> + 'static,
{
    Arc::new(move |message| Box::pin(handler.call(message)) as HandlerFuture<'_>)
}

/// A dependency-free [`MessageRouter`]: a set of `(kind, name)` → async closure
/// bindings. Build it fluently with [`on_command`](Handlers::on_command) /
/// [`on_event`](Handlers::on_event), then run it with `bus.listen`/`bus.subscribe`.
///
/// Handlers are keyed by kind, then name, so dispatch looks up by `&str`
/// without allocating a key.
#[derive(Clone, Default)]
pub struct Handlers {
    group: Option<String>,
    handlers: HashMap<MessageKind, HashMap<String, Arc<HandlerFn>>>,
}

impl Handlers {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Assign a stable consumer identity for broker adapters that need a durable
    /// group when this standalone registry is used directly with `listen` or
    /// `subscribe`.
    pub fn named(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }

    /// Register a command handler (point-to-point / competing-consumer via `listen`).
    pub fn on_command<F>(self, name: impl Into<String>, handler: F) -> Self
    where
        F: for<'a> MessageHandler<'a> + 'static,
    {
        self.with(MessageKind::Command, name, handler)
    }

    /// Register an event handler (fan-out via `subscribe`).
    pub fn on_event<F>(self, name: impl Into<String>, handler: F) -> Self
    where
        F: for<'a> MessageHandler<'a> + 'static,
    {
        self.with(MessageKind::Event, name, handler)
    }

    fn with<F>(mut self, kind: MessageKind, name: impl Into<String>, handler: F) -> Self
    where
        F: for<'a> MessageHandler<'a> + 'static,
    {
        self.handlers
            .entry(kind)
            .or_default()
            .insert(name.into(), boxed_handler(handler));
        self
    }
}

impl MessageRouter for Handlers {
    fn consumer_group(&self) -> Option<&str> {
        self.group.as_deref()
    }

    fn handles(&self, kind: MessageKind, name: &str) -> bool {
        self.handlers
            .get(&kind)
            .is_some_and(|by_name| by_name.contains_key(name))
    }

    fn subscription_plan(&self) -> SubscriptionPlan {
        let mut plan = SubscriptionPlan::default();
        for (kind, by_name) in &self.handlers {
            let bucket = match kind {
                MessageKind::Command => &mut plan.commands,
                MessageKind::Event => &mut plan.events,
            };
            for name in by_name.keys() {
                if !bucket.iter().any(|existing| existing == name) {
                    bucket.push(name.clone());
                }
            }
        }
        plan
    }

    async fn dispatch(&self, message: &Message) -> Result<(), TransportError> {
        // Clone the handler Arc so the map is not borrowed across the awaited
        // future (mirrors `Service::invoke`).
        let handler = self
            .handlers
            .get(&message.kind)
            .and_then(|by_name| by_name.get(message.name()))
            .cloned();
        match handler {
            Some(handler) => handler(message).await,
            // No binding: the runner already filters via `handles`, so this is a
            // benign no-op for any message that slips through.
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::{Bus, BusConsumer, InMemoryBus, RunOptions};
    use std::future::Future;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn block_on<F: Future>(future: F) -> F::Output {
        use std::ptr;
        use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
        const VTABLE: RawWakerVTable = RawWakerVTable::new(
            |_| RawWaker::new(ptr::null(), &VTABLE),
            |_| {},
            |_| {},
            |_| {},
        );
        let waker = unsafe { Waker::from_raw(RawWaker::new(ptr::null(), &VTABLE)) };
        let mut cx = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);
        loop {
            if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
                return output;
            }
        }
    }

    #[test]
    fn named_handlers_expose_consumer_group() {
        let handlers = Handlers::new().named("order-projection");
        assert_eq!(
            crate::bus::MessageRouter::consumer_group(&handlers),
            Some("order-projection")
        );
    }

    #[test]
    fn subscription_plan_groups_by_kind() {
        let handlers = Handlers::new()
            .on_command("place.bet", |_: &Message| async { Ok(()) })
            .on_event("seat.reserved", |_: &Message| async { Ok(()) });
        let plan = handlers.subscription_plan();
        assert_eq!(plan.commands, vec!["place.bet".to_string()]);
        assert_eq!(plan.events, vec!["seat.reserved".to_string()]);
        assert!(handlers.handles(MessageKind::Command, "place.bet"));
        assert!(handlers.handles(MessageKind::Event, "seat.reserved"));
        assert!(!handlers.handles(MessageKind::Event, "place.bet"));
    }

    #[test]
    fn fan_out_round_trip_without_service() {
        // Full InMemoryBus publish -> subscribe round trip using only Handlers —
        // no Service, no microsvc handler machinery.
        let bus = InMemoryBus::new();
        for _ in 0..3 {
            block_on(bus.publish("seat.reserved", b"{}".to_vec())).unwrap();
        }
        let count = Arc::new(AtomicUsize::new(0));
        let seen = count.clone();
        let handlers = Arc::new(
            Handlers::new().on_event("seat.reserved", move |msg: &Message| {
                let seen = seen.clone();
                // Touch the message to prove the borrow is usable in the handler.
                let is_event = matches!(msg.kind, MessageKind::Event);
                async move {
                    if is_event {
                        seen.fetch_add(1, Ordering::SeqCst);
                    }
                    Ok(())
                }
            }),
        );
        block_on(bus.subscribe(handlers, RunOptions::until_idle())).unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn point_to_point_round_trip_without_service() {
        let bus = InMemoryBus::new();
        block_on(bus.send("place.bet", b"{}".to_vec())).unwrap();
        block_on(bus.send("place.bet", b"{}".to_vec())).unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let seen = count.clone();
        let handlers = Arc::new(Handlers::new().on_command("place.bet", move |_: &Message| {
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }));
        block_on(bus.listen(handlers, RunOptions::until_idle())).unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }
}
