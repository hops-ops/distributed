//! Several consumers that drain together.

use std::future::poll_fn;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Poll, Waker};

/// A member has not yet found its source empty.
const NEVER: u64 = u64::MAX;
/// A member's run has ended.
const LEFT: u64 = u64::MAX - 1;

#[derive(Default)]
struct State {
    /// Counts finished dispatches. A dispatch may have published to any
    /// member, so an empty source seen before it says nothing afterwards.
    activity: u64,
    /// Dispatches running now.
    dispatching: usize,
    /// Per member: the `activity` at which it last found its source empty.
    idle_at: Vec<u64>,
    waiting: Vec<Waker>,
}

impl State {
    fn quiet(&self) -> bool {
        self.dispatching == 0
            && self
                .idle_at
                .iter()
                .all(|at| *at == self.activity || *at == LEFT)
    }

    fn wake(&mut self) {
        for waker in std::mem::take(&mut self.waiting) {
            waker.wake();
        }
    }
}

/// Consumers that end a drain together: when every one of them has found its
/// source empty since the last dispatch finished, and none is dispatching.
///
/// One consumer alone ends at its own idle ([`RunUntil::Idle`]). Several
/// consumers of one service cannot: what one handles may publish to another
/// that has already found nothing to do. Give each consumer a
/// [`member`](Self::member) in [`RunUntil::AllIdle`] instead.
///
/// [`RunUntil::Idle`]: super::RunUntil::Idle
/// [`RunUntil::AllIdle`]: super::RunUntil::AllIdle
#[derive(Clone, Default)]
pub struct IdleGroup(Arc<Mutex<State>>);

impl IdleGroup {
    /// A group with no members.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a consumer. The group is not idle until it has found its source
    /// empty, or its run has ended.
    pub fn member(&self) -> IdleMember {
        let mut state = lock(&self.0);
        state.idle_at.push(NEVER);
        IdleMember {
            state: Arc::clone(&self.0),
            member: state.idle_at.len() - 1,
        }
    }
}

/// One consumer's place in an [`IdleGroup`].
#[derive(Clone)]
pub struct IdleMember {
    state: Arc<Mutex<State>>,
    member: usize,
}

impl std::fmt::Debug for IdleMember {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IdleMember")
            .field("member", &self.member)
            .finish()
    }
}

impl IdleMember {
    /// The group's activity now. Read it before asking the source, so an
    /// empty answer is known to be at least this recent.
    pub(super) fn activity(&self) -> u64 {
        lock(&self.state).activity
    }

    /// This member found its source empty after reading `activity`. Returns
    /// whether the whole group is idle, which ends this member's run.
    pub(super) fn found_empty(&self, activity: u64) -> bool {
        let mut state = lock(&self.state);
        state.idle_at[self.member] = activity;
        let quiet = state.quiet();
        if quiet {
            state.wake();
        }
        quiet
    }

    /// Resolves when a dispatch has finished since `activity` was read, or
    /// the group is idle: either way this member must look again.
    pub(super) async fn changed(&self, activity: u64) {
        poll_fn(|cx| {
            let mut state = lock(&self.state);
            if state.activity != activity || state.quiet() {
                return Poll::Ready(());
            }
            if !state
                .waiting
                .iter()
                .any(|waker| waker.will_wake(cx.waker()))
            {
                state.waiting.push(cx.waker().clone());
            }
            Poll::Pending
        })
        .await
    }

    /// Hold the returned guard while dispatching one delivery.
    pub(super) fn dispatching(&self) -> Dispatching {
        lock(&self.state).dispatching += 1;
        Dispatching(Arc::clone(&self.state))
    }

    /// This member's run has ended; the group no longer waits for it.
    pub(super) fn leave(&self) {
        let mut state = lock(&self.state);
        state.idle_at[self.member] = LEFT;
        state.wake();
    }
}

/// A dispatch in progress. Dropping it counts the dispatch as finished.
pub(super) struct Dispatching(Arc<Mutex<State>>);

impl Drop for Dispatching {
    fn drop(&mut self) {
        let mut state = lock(&self.0);
        state.dispatching -= 1;
        state.activity += 1;
        state.wake();
    }
}

fn lock(state: &Mutex<State>) -> MutexGuard<'_, State> {
    match state.lock() {
        Ok(state) => state,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_group_is_idle_only_when_every_member_saw_empty_after_the_last_dispatch() {
        let group = IdleGroup::new();
        let (a, b) = (group.member(), group.member());

        assert!(!a.found_empty(a.activity()), "b has not looked yet");
        let seen = b.activity();
        let dispatch = a.dispatching();
        assert!(!b.found_empty(seen), "a is dispatching");
        drop(dispatch);
        assert!(
            !b.found_empty(seen),
            "b looked before a's dispatch finished"
        );
        assert!(!b.found_empty(b.activity()), "a has not looked since");
        assert!(a.found_empty(a.activity()));
        assert!(b.found_empty(b.activity()));
    }

    #[test]
    fn a_member_that_left_is_not_waited_for() {
        let group = IdleGroup::new();
        let (a, b) = (group.member(), group.member());
        assert!(!a.found_empty(a.activity()));
        b.leave();
        assert!(a.found_empty(a.activity()));
    }
}
