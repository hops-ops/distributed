//! Unique test identifiers.
//!
//! Every generated test name (schema, file, subject, queue, stream, id) is
//! built from [`token`]. A name must not collide with one made by another
//! test process running at the same time (nextest runs one process per test)
//! nor with one an earlier run left in a persistent broker or database.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Per-process sequence, so tokens made in one process differ.
static SEQ: AtomicU64 = AtomicU64::new(0);

/// A token no other call returns: `{nanos:016x}{pid:08x}{seq:x}`, lowercase
/// hex only, so it fits any identifier grammar the tests meet.
///
/// - The process id separates processes that are alive at the same time.
/// - The per-process sequence separates calls within one process, whatever the
///   clock's resolution (macOS reports microseconds).
/// - Wall-clock nanoseconds separate this run from earlier ones whose process
///   had the same id, against state that outlives a run.
///
/// The first two fields are fixed width, so no two field values concatenate to
/// the same string. 24 characters plus the sequence.
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

/// A unique id: `{prefix}-{token}`.
pub fn unique_id(prefix: &str) -> String {
    format!("{prefix}-{}", token())
}

/// A unique subject/queue/stream name: `{prefix}_{token}`.
pub fn unique(prefix: &str) -> String {
    format!("{prefix}_{}", token())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_are_distinct_hex_of_a_fixed_prefix_width() {
        let tokens: std::collections::BTreeSet<String> = (0..10_000).map(|_| token()).collect();
        assert_eq!(tokens.len(), 10_000);
        for token in &tokens {
            assert!(token.len() >= 25, "{token}");
            assert!(token
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
            assert_eq!(&token[16..24], format!("{:08x}", std::process::id()));
        }
    }
}
