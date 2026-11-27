//! A runtime-free one-shot signal.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

#[derive(Default)]
struct State {
    triggered: AtomicBool,
    waiting: Mutex<Vec<Waker>>,
}

/// Tells a running consumer to stop.
///
/// Clones share one signal. [`trigger`](Self::trigger) is permanent: every
/// present and future [`triggered`](Self::triggered) future resolves. A
/// consumer finishes the delivery it is handling, settles it, and returns
/// `Ok(())`.
#[derive(Clone, Default)]
pub struct Shutdown(Arc<State>);

impl Shutdown {
    /// A signal that has not been triggered.
    pub fn new() -> Self {
        Self::default()
    }

    /// Stop every consumer holding a clone of this signal.
    pub fn trigger(&self) {
        self.0.triggered.store(true, Ordering::SeqCst);
        let waiting = match self.0.waiting.lock() {
            Ok(mut waiting) => std::mem::take(&mut *waiting),
            Err(poisoned) => std::mem::take(&mut *poisoned.into_inner()),
        };
        for waker in waiting {
            waker.wake();
        }
    }

    /// Whether [`trigger`](Self::trigger) has been called.
    pub fn is_triggered(&self) -> bool {
        self.0.triggered.load(Ordering::SeqCst)
    }

    /// Resolves once the signal is triggered.
    pub fn triggered(&self) -> Triggered {
        Triggered(Arc::clone(&self.0))
    }
}

impl std::fmt::Debug for Shutdown {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Shutdown")
            .field("triggered", &self.is_triggered())
            .finish()
    }
}

/// Future returned by [`Shutdown::triggered`].
pub struct Triggered(Arc<State>);

impl Future for Triggered {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0.triggered.load(Ordering::SeqCst) {
            return Poll::Ready(());
        }
        let mut waiting = match self.0.waiting.lock() {
            Ok(waiting) => waiting,
            Err(poisoned) => poisoned.into_inner(),
        };
        // `trigger` sets the flag before it takes the wakers, so a trigger that
        // ran between the check above and this lock is seen here.
        if self.0.triggered.load(Ordering::SeqCst) {
            return Poll::Ready(());
        }
        if !waiting.iter().any(|waker| waker.will_wake(cx.waker())) {
            waiting.push(cx.waker().clone());
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::task::Wake;

    struct Count(AtomicUsize);
    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn a_trigger_wakes_every_waiter_and_stays_triggered() {
        let shutdown = Shutdown::new();
        let count = Arc::new(Count(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&count));
        let mut cx = Context::from_waker(&waker);

        let mut first = Box::pin(shutdown.triggered());
        let mut second = Box::pin(shutdown.clone().triggered());
        assert!(first.as_mut().poll(&mut cx).is_pending());
        assert!(first.as_mut().poll(&mut cx).is_pending());
        assert!(second.as_mut().poll(&mut cx).is_pending());
        assert!(!shutdown.is_triggered());

        shutdown.trigger();
        // One task polled both futures; it is woken once, not once per poll.
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
        assert!(first.as_mut().poll(&mut cx).is_ready());
        assert!(second.as_mut().poll(&mut cx).is_ready());
        assert!(Box::pin(shutdown.triggered())
            .as_mut()
            .poll(&mut cx)
            .is_ready());
    }
}
