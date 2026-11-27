use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};

use super::{Lock, LockError, LockManager};

#[derive(Default)]
struct LockState {
    locked: bool,
    waiters: VecDeque<Waker>,
}

/// Back-reference from a manager-owned lock to its entry in the manager's map,
/// so an idle lock can evict itself on unlock instead of living forever.
struct LockRegistry {
    locks: Weak<Mutex<HashMap<String, Arc<InMemoryLock>>>>,
    key: String,
}

/// In-memory [`Lock`] backed by a `Mutex<{ locked, waiters }>`.
///
/// The std `Mutex` is held only for the brief state check/update — never across
/// an `.await` — so it never blocks the executor. Acquisition returns a future
/// that, while the lock is held, registers the task's waker and yields
/// `Pending`; `unlock` wakes all registered waiters so they re-contend (one
/// wins, the rest re-register). Runtime-agnostic: no dependency on any async
/// runtime, matching the rest of the crate's RPITIT async surface.
pub struct InMemoryLock {
    state: Mutex<LockState>,
    /// `Some` only for locks handed out by [`InMemoryLockManager`]; standalone
    /// locks never evict.
    registry: Option<LockRegistry>,
}

impl InMemoryLock {
    pub fn new() -> Self {
        InMemoryLock {
            state: Mutex::new(LockState::default()),
            registry: None,
        }
    }

    /// Synchronous core of [`try_lock`](Lock::try_lock).
    ///
    /// In-memory acquisition is pure state mutation, so the real work lives in a
    /// private synchronous helper and the public `Lock::try_lock` runs it
    /// inside its (lazy) future — there is no parallel sync *API*, only this
    /// internal detail. The synchronous core also lets the regression tests
    /// exercise acquisition from inside a `Waker`, which cannot `.await`.
    fn try_lock_core(&self) -> Result<bool, LockError> {
        let mut state = self
            .state
            .lock()
            .map_err(|err| LockError::Poisoned(err.to_string()))?;
        if state.locked {
            Ok(false)
        } else {
            state.locked = true;
            Ok(true)
        }
    }

    /// Synchronous core of [`unlock`](Lock::unlock).
    ///
    /// Drains waiters UNDER the guard (keeping register/drain mutually exclusive
    /// so no wakeup is lost), then releases the guard BEFORE waking.
    /// `Waker::wake` runs arbitrary executor code: doing it under the std
    /// `Mutex` would let a panicking waker poison (permanently brick) the lock,
    /// and a waker that synchronously re-polls would deadlock on the
    /// non-reentrant guard. Waking outside the critical section avoids both.
    ///
    /// `pub(crate)` so the SQLx locks' cancellation-safe gate guard can release
    /// the in-process gate synchronously from `Drop` (which cannot `.await`).
    pub(crate) fn unlock_core(&self) -> Result<(), LockError> {
        let woken = {
            let mut state = self
                .state
                .lock()
                .map_err(|err| LockError::Poisoned(err.to_string()))?;
            if state.locked {
                state.locked = false;
                std::mem::take(&mut state.waiters)
            } else {
                VecDeque::new()
            }
        };
        // They re-contend and one wins, the rest re-register on their next poll.
        for waker in woken {
            waker.wake();
        }
        self.evict_if_idle();
        Ok(())
    }

    /// Remove this lock from its manager's map when nothing else can reach it.
    ///
    /// Safe because a lock is only reachable through the map (`get_lock`, which
    /// serializes on the map mutex we hold here) or through an already-held
    /// `Arc`. With the map guard held, `strong_count == 2` means exactly the
    /// map's `Arc` and the one this unlock call came in through remain, so no
    /// other task can acquire this instance; the idle entry can go. Any parked
    /// waiter's future borrows a live `Arc`, which keeps the count above 2 and
    /// the entry alive.
    ///
    /// Contract note (documented on [`InMemoryLockManager::get_lock`]): callers
    /// must not reuse a `get_lock` handle after unlocking it — re-fetch instead.
    fn evict_if_idle(&self) {
        let Some(registry) = &self.registry else {
            return;
        };
        let Some(locks) = registry.locks.upgrade() else {
            return;
        };
        let Ok(mut locks) = locks.lock() else {
            return;
        };
        let Some(entry) = locks.get(&registry.key) else {
            return;
        };
        if !std::ptr::eq(Arc::as_ptr(entry), self) || Arc::strong_count(entry) != 2 {
            return;
        }
        // Re-check the lock state under the map guard: a concurrent try_lock
        // through another handle would have shown up in the strong count, but a
        // no-op unlock on a still-held lock must not evict it.
        let idle = self
            .state
            .lock()
            .map(|state| !state.locked && state.waiters.is_empty())
            .unwrap_or(false);
        if idle {
            locks.remove(&registry.key);
        }
    }
}

impl Default for InMemoryLock {
    fn default() -> Self {
        Self::new()
    }
}

/// Future returned by [`InMemoryLock::lock`].
///
/// Borrows the lock for its lifetime; resolves once the lock is acquired.
pub struct InMemoryLockFuture<'a> {
    lock: &'a InMemoryLock,
}

impl Future for InMemoryLockFuture<'_> {
    type Output = Result<(), LockError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = match self.lock.state.lock() {
            Ok(state) => state,
            Err(err) => return Poll::Ready(Err(LockError::Poisoned(err.to_string()))),
        };
        if !state.locked {
            state.locked = true;
            Poll::Ready(Ok(()))
        } else {
            // Register (or refresh) this task's waker so `unlock` can wake it.
            // Dedupe by `will_wake` so repeated polls without an intervening
            // unlock do not accumulate duplicate wakers.
            if !state
                .waiters
                .iter()
                .any(|waker| waker.will_wake(cx.waker()))
            {
                state.waiters.push_back(cx.waker().clone());
            }
            Poll::Pending
        }
    }
}

impl Lock for InMemoryLock {
    fn lock(&self) -> impl Future<Output = Result<(), LockError>> + Send + '_ {
        InMemoryLockFuture { lock: self }
    }

    // Lazy: the side effect runs when the future is polled, not at call time, so
    // a future that is dropped without being awaited is a no-op — matching the
    // I/O-backed locks (whose `async fn` bodies also only run on poll). The body
    // has no `.await`, so the returned future is trivially `Send`.
    async fn try_lock(&self) -> Result<bool, LockError> {
        self.try_lock_core()
    }

    async fn unlock(&self) -> Result<(), LockError> {
        self.unlock_core()
    }

    fn release_detached(self: Arc<Self>) {
        let _ = self.unlock_core();
    }
}

/// In-memory [`LockManager`] backed by a `HashMap<String, Arc<InMemoryLock>>`.
///
/// Lazily creates one [`InMemoryLock`] per unique key and returns the same
/// `Arc` for repeated lookups. An entry is evicted on unlock once it is idle
/// and no handle beyond the map's own remains, so the map does not grow
/// unboundedly with every aggregate id ever locked. Clones share one lock
/// table.
#[derive(Clone)]
pub struct InMemoryLockManager {
    locks: Arc<Mutex<HashMap<String, Arc<InMemoryLock>>>>,
}

impl InMemoryLockManager {
    pub fn new() -> Self {
        InMemoryLockManager {
            locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for InMemoryLockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LockManager for InMemoryLockManager {
    type Lock = InMemoryLock;

    /// Get (or create) the lock for `id`.
    ///
    /// The returned handle is good for one acquire/release cycle: unlocking may
    /// evict the idle entry, so do not cache the handle across an unlock —
    /// call `get_lock` again (as `QueuedRepository` does per operation).
    fn get_lock(&self, id: &str) -> Result<Arc<InMemoryLock>, LockError> {
        let mut locks = self
            .locks
            .lock()
            .map_err(|_| LockError::Poisoned("lock manager map poisoned".into()))?;
        Ok(locks
            .entry(id.to_string())
            .or_insert_with(|| {
                Arc::new(InMemoryLock {
                    state: Mutex::new(LockState::default()),
                    registry: Some(LockRegistry {
                        locks: Arc::downgrade(&self.locks),
                        key: id.to_string(),
                    }),
                })
            })
            .clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
    use std::time::Duration;

    /// A `Waker` whose `wake()` re-enters the given lock via `try_lock_core()`,
    /// modeling an inline-polling executor. (`wake` is synchronous, so it calls
    /// the synchronous core rather than the `async` trait method.) The data
    /// pointer is an `Arc<InMemoryLock>`.
    fn reentrant_waker(lock: Arc<InMemoryLock>) -> Waker {
        unsafe fn clone(data: *const ()) -> RawWaker {
            let arc = unsafe { Arc::from_raw(data as *const InMemoryLock) };
            let cloned = Arc::clone(&arc);
            std::mem::forget(arc);
            RawWaker::new(Arc::into_raw(cloned) as *const (), &REENTRANT_VTABLE)
        }
        unsafe fn wake(data: *const ()) {
            let arc = unsafe { Arc::from_raw(data as *const InMemoryLock) };
            let _ = arc.try_lock_core(); // re-enter from inside wake(): must not deadlock
        }
        unsafe fn wake_by_ref(data: *const ()) {
            let arc = unsafe { Arc::from_raw(data as *const InMemoryLock) };
            let _ = arc.try_lock_core();
            std::mem::forget(arc);
        }
        unsafe fn drop_fn(data: *const ()) {
            drop(unsafe { Arc::from_raw(data as *const InMemoryLock) });
        }
        static REENTRANT_VTABLE: RawWakerVTable =
            RawWakerVTable::new(clone, wake, wake_by_ref, drop_fn);
        let raw = RawWaker::new(Arc::into_raw(lock) as *const (), &REENTRANT_VTABLE);
        unsafe { Waker::from_raw(raw) }
    }

    /// A `Waker` whose `wake()` panics, modeling a misbehaving executor.
    fn panicking_waker() -> Waker {
        unsafe fn clone(_: *const ()) -> RawWaker {
            RawWaker::new(std::ptr::null(), &PANIC_VTABLE)
        }
        unsafe fn wake(_: *const ()) {
            panic!("waker panicked in wake()");
        }
        unsafe fn wake_by_ref(_: *const ()) {
            panic!("waker panicked in wake_by_ref()");
        }
        unsafe fn drop_fn(_: *const ()) {}
        static PANIC_VTABLE: RawWakerVTable =
            RawWakerVTable::new(clone, wake, wake_by_ref, drop_fn);
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &PANIC_VTABLE)) }
    }

    /// Park `waker` on the held `lock` by polling one acquire future to `Pending`.
    fn park_waker(lock: &InMemoryLock, waker: &Waker) {
        let mut cx = Context::from_waker(waker);
        let mut fut = std::pin::pin!(lock.lock());
        assert!(matches!(fut.as_mut().poll(&mut cx), Poll::Pending));
    }

    #[tokio::test]
    async fn try_lock_reflects_state() {
        let lock = InMemoryLock::new();
        assert!(lock.try_lock().await.unwrap()); // free → acquired
        assert!(!lock.try_lock().await.unwrap()); // held → fails
        lock.unlock().await.unwrap();
        assert!(lock.try_lock().await.unwrap()); // released → acquired again
    }

    #[tokio::test]
    async fn lock_resolves_immediately_when_free() {
        let lock = InMemoryLock::new();
        lock.lock().await.unwrap();
        assert!(!lock.try_lock().await.unwrap()); // now held
        lock.unlock().await.unwrap();
        assert!(lock.try_lock().await.unwrap());
    }

    #[tokio::test]
    async fn second_acquire_waits_until_unlock() {
        let lock = Arc::new(InMemoryLock::new());
        lock.lock().await.unwrap();

        let order = Arc::new(AtomicUsize::new(0));
        let waiter_lock = Arc::clone(&lock);
        let waiter_order = Arc::clone(&order);
        let waiter = tokio::spawn(async move {
            waiter_lock.lock().await.unwrap();
            // Records the order in which it acquired (must be after unlock below).
            waiter_order.fetch_add(1, Ordering::SeqCst)
        });

        // Give the waiter time to park on the held lock.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            order.load(Ordering::SeqCst),
            0,
            "waiter must still be parked"
        );

        lock.unlock().await.unwrap();
        let acquired_at = waiter.await.unwrap();
        assert_eq!(acquired_at, 0, "waiter acquired exactly once after unlock");
        assert!(!lock.try_lock().await.unwrap(), "waiter holds the lock");
    }

    #[test]
    fn manager_returns_same_arc_per_key() {
        let manager = InMemoryLockManager::new();
        let a1 = manager.get_lock("agg-1").unwrap();
        let a2 = manager.get_lock("agg-1").unwrap();
        let b = manager.get_lock("agg-2").unwrap();
        assert!(Arc::ptr_eq(&a1, &a2));
        assert!(!Arc::ptr_eq(&a1, &b));
    }

    #[tokio::test]
    async fn manager_evicts_idle_entry_on_unlock() {
        let manager = InMemoryLockManager::new();
        let lock = manager.get_lock("agg-1").unwrap();
        lock.lock().await.unwrap();
        assert_eq!(manager.locks.lock().unwrap().len(), 1);

        lock.unlock().await.unwrap();

        assert!(
            manager.locks.lock().unwrap().is_empty(),
            "an idle, otherwise-unreferenced entry is evicted on unlock"
        );
        // A later get_lock simply creates a fresh entry.
        let again = manager.get_lock("agg-1").unwrap();
        assert!(again.try_lock().await.unwrap());
    }

    #[tokio::test]
    async fn manager_keeps_entry_while_another_handle_is_held() {
        let manager = InMemoryLockManager::new();
        let a = manager.get_lock("agg-1").unwrap();
        let b = manager.get_lock("agg-1").unwrap();

        a.lock().await.unwrap();
        a.unlock().await.unwrap();

        // `b` still references the entry, so unlock must not evict it: the next
        // get_lock returns the same lock `b` points at.
        let c = manager.get_lock("agg-1").unwrap();
        assert!(Arc::ptr_eq(&b, &c));
        assert_eq!(manager.locks.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn manager_keeps_entry_for_a_parked_waiter() {
        let manager = InMemoryLockManager::new();
        let holder = manager.get_lock("agg-1").unwrap();
        holder.lock().await.unwrap();

        // A waiter parks on the held lock, keeping its own Arc alive.
        let waiter_lock = manager.get_lock("agg-1").unwrap();
        let waiter = tokio::spawn(async move {
            waiter_lock.lock().await.unwrap();
            waiter_lock
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        holder.unlock().await.unwrap();
        let waiter_lock = waiter.await.unwrap();

        // The waiter's Arc kept the entry alive across the unlock, so it now
        // holds the same lock the map still serves.
        let same = manager.get_lock("agg-1").unwrap();
        assert!(Arc::ptr_eq(&waiter_lock, &same));
        assert!(!same.try_lock().await.unwrap(), "waiter holds the lock");

        drop(holder);
        drop(same);
        waiter_lock.unlock().await.unwrap();
        assert!(
            manager.locks.lock().unwrap().is_empty(),
            "the entry is evicted once the last holder unlocks"
        );
    }

    #[tokio::test]
    async fn distinct_keys_do_not_contend() {
        let manager = InMemoryLockManager::new();
        let a = manager.get_lock("agg-1").unwrap();
        let b = manager.get_lock("agg-2").unwrap();
        a.lock().await.unwrap();
        // Different key acquires without waiting on `a`.
        b.lock().await.unwrap();
        a.unlock().await.unwrap();
        b.unlock().await.unwrap();
    }

    // Regression: `unlock` must wake waiters OUTSIDE the held guard, so a waker
    // that synchronously re-polls the lock cannot deadlock on the non-reentrant
    // std `Mutex`. Without the fix this hangs; the watchdog turns that into a
    // failure instead of wedging the suite.
    #[test]
    fn unlock_does_not_deadlock_with_reentrant_waker() {
        let lock = Arc::new(InMemoryLock::new());
        assert!(lock.try_lock_core().unwrap()); // hold the lock
        park_waker(&lock, &reentrant_waker(Arc::clone(&lock)));

        let (tx, rx) = mpsc::channel();
        let unlock_lock = Arc::clone(&lock);
        std::thread::spawn(move || {
            let _ = tx.send(unlock_lock.unlock_core());
        });
        let result = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("unlock deadlocked while waking a re-entrant waker");
        result.expect("unlock should succeed");
    }

    // Regression: a panicking waker must not poison the lock's mutex, because
    // `unlock` releases the guard before waking. After the panic the lock is
    // still usable (and was released).
    #[test]
    fn unlock_does_not_poison_when_a_waker_panics() {
        let lock = Arc::new(InMemoryLock::new());
        assert!(lock.try_lock_core().unwrap()); // hold the lock
        park_waker(&lock, &panicking_waker());

        let unlock_lock = Arc::clone(&lock);
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = unlock_lock.unlock_core();
        }))
        .is_err();
        assert!(panicked, "the panicking waker should unwind out of unlock");

        // Not poisoned: the guard was dropped before the panicking wake ran, and
        // the lock was released, so it can be acquired again.
        assert!(
            lock.try_lock_core().unwrap(),
            "lock must remain usable after a waker panic"
        );
    }
}
