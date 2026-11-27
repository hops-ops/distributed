//! Unique test names (temp directories, ticket nonces), the rule of
//! `tests/common/src/ids.rs` in the framework workspace, which this crate does
//! not depend on. A test binary declares this module once at its root
//! (`#[path = "../support/unique.rs"] mod unique;`) and support modules use
//! `crate::unique`, so one binary has one sequence.

#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// `{nanos:016x}{pid:08x}{seq:x}`: the process id separates live processes,
/// the sequence separates calls in one process whatever the clock's
/// resolution, and the time separates runs whose process had the same id.
pub fn token() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after UNIX epoch")
        .as_nanos();
    format!(
        "{:016x}{:08x}{:x}",
        nanos as u64,
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// `{prefix}-{token}`.
pub fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", token())
}
