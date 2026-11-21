//! Bounded, identifier-free coordinator observations. Exporters choose their own
//! transport; these counters never retain subjects, documents or variables.
use serde::Serialize;

/// Cumulative cache decisions within one coordinator lifetime. Saturating
/// counters are monotonic; restarting a coordinator starts a new series.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotMetrics {
    /// A freshly origin-admitted consumer reused a complete envelope.
    pub hits: u64,
    /// Admitted lookups with no reusable complete envelope.
    pub misses: u64,
    /// Resident/filling envelopes rejected by current version, proof or generation.
    pub stale_rejections: u64,
    /// Fills bypassed storage, including oversized or unshareable responses.
    pub fill_bypasses: u64,
    /// Explicit lost-feed/rebuild invalidations, not individual SQL writes.
    pub invalidations: u64,
}
