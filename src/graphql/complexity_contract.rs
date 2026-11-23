//! Feature-neutral query complexity contract shared by runtime execution and
//! portable client-manifest generation. The weights and the cost formula are
//! [`ClientComplexityWeights`](super::client_manifest::ClientComplexityWeights).

/// Default engine budget (same as the historical builder default).
pub(crate) const DEFAULT_MAX_COMPLEXITY: usize = 500;

/// Default maximum GraphQL document depth.
pub(crate) const DEFAULT_MAX_DEPTH: usize = 8;
