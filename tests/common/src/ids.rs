//! Unique test identifiers.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Per-process sequence counter, so ids do not collide within a run.
static SEQ: AtomicU64 = AtomicU64::new(1);

/// A unique id: `{prefix}-{nanos}-{seq}`. Wall-clock nanos make ids unique
/// across runs against persistent state; the sequence makes them unique within
/// a run even under a coarse clock.
pub fn unique_id(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after UNIX epoch")
        .as_nanos();
    format!("{prefix}-{nanos}-{}", SEQ.fetch_add(1, Ordering::Relaxed))
}

/// A per-process token mixing wall-clock nanos with the pid, so names are unique
/// even across separate runs against a persistent broker.
pub fn run_token() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
        ^ u128::from(std::process::id())
}

/// A unique subject/queue/stream name: `{prefix}_{run_token:x}_{seq}`. Both the
/// run token and the sequence are included so names collide neither within a
/// run nor across runs against persistent broker state.
pub fn unique(prefix: &str) -> String {
    format!(
        "{prefix}_{:x}_{}",
        run_token(),
        SEQ.fetch_add(1, Ordering::SeqCst)
    )
}
