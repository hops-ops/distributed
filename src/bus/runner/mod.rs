//! The direct-source runner.
//!
//! [`run_source`] is the shared receive loop for direct transports. It owns the
//! cross-cutting policy — when execution counts as successful, how retryable vs
//! permanent failures are routed — while the adapter owns how acknowledgement
//! maps back to the transport. The same dispatch/runner boundary is what the
//! Knative/HTTP ingress will call, so consumer execution stays identical across
//! ingress shapes.

mod receive_loop;

#[cfg(test)]
mod lane_tests;
#[cfg(test)]
mod tests;

pub use receive_loop::run_source;
