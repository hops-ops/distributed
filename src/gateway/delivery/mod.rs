//! Portable delivery contracts. Origin authentication establishes identity;
//! client freshness hints can strengthen reads but never authorize reuse.
mod freshness;
mod identity;
pub use freshness::*;
pub use identity::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Fail-closed outcomes of admission, routing or proof checks.
pub enum DeliveryError {
    /// Malformed or oversized client context.
    InvalidContext,
    /// Origin authority differs from the supplied context.
    ScopeChanged,
    /// Operation has no trusted reusable contract.
    Ineligible,
    /// Available snapshot cannot prove the required minimum.
    Pending,
    /// Authoritative origin is unavailable; stale fallback is forbidden.
    Unavailable,
}
impl std::fmt::Display for DeliveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for DeliveryError {}

mod snapshot;
pub use snapshot::*;

mod flight;
pub use flight::*;

mod coordinator;
pub use coordinator::*;

mod live;
pub use live::*;

mod metrics;
pub use metrics::*;
