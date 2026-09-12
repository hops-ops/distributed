//! Runtime-free wakeup for wait-when-idle.
//!
//! `tokio` is optional. In-memory listen still needs to park until `send`,
//! so this is a small multi-waiter notify that compiles in the default crate.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

#[derive(Clone, Default)]
pub(crate) struct Notify {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Default)]
struct Inner {
    generation: u64,
    waiters: Vec<Waker>,
}

impl Notify {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn notify_waiters(&self) {
        let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        inner.generation = inner.generation.wrapping_add(1);
        for waker in inner.waiters.drain(..) {
            waker.wake();
        }
    }

    pub(crate) fn notified(&self) -> Notified {
        let generation = self
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .generation;
        Notified {
            inner: Arc::clone(&self.inner),
            generation,
        }
    }

    #[cfg(test)]
    pub(crate) fn waiter_count(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .waiters
            .len()
    }
}

pub(crate) struct Notified {
    inner: Arc<Mutex<Inner>>,
    generation: u64,
}

impl Future for Notified {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if inner.generation != self.generation {
            return Poll::Ready(());
        }
        let waker = cx.waker();
        if !inner
            .waiters
            .iter()
            .any(|existing| existing.will_wake(waker))
        {
            inner.waiters.push(waker.clone());
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::{Wake, Waker};

    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    #[test]
    fn one_notification_releases_every_registered_waiter() {
        let notify = Notify::new();
        let mut first = Box::pin(notify.notified());
        let mut second = Box::pin(notify.notified());
        let waker = Waker::from(Arc::new(NoopWake));
        let mut context = Context::from_waker(&waker);
        assert!(matches!(first.as_mut().poll(&mut context), Poll::Pending));
        assert!(matches!(second.as_mut().poll(&mut context), Poll::Pending));

        notify.notify_waiters();

        assert!(matches!(first.as_mut().poll(&mut context), Poll::Ready(())));
        assert!(matches!(
            second.as_mut().poll(&mut context),
            Poll::Ready(())
        ));
    }

    #[test]
    fn notification_is_observed_when_future_was_created_but_not_polled() {
        let notify = Notify::new();
        let mut notified = Box::pin(notify.notified());
        notify.notify_waiters();
        let waker = Waker::from(Arc::new(NoopWake));
        let mut context = Context::from_waker(&waker);

        assert!(matches!(
            notified.as_mut().poll(&mut context),
            Poll::Ready(())
        ));
    }
}
