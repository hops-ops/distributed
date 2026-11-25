//! Prometheus exposition and OpenTelemetry export.
//!
//! A binary of its own: the metrics registry is process-global and
//! `metrics_exposition` asserts exact counter values.
//!
//! One test binary for the area: each former `tests/<name>` target is a
//! module, so `cargo test --test observability <name>::` runs what `--test <name>` ran.

#[cfg(all(feature = "http", feature = "metrics"))]
mod metrics_exposition;
#[cfg(all(feature = "http", feature = "otel"))]
mod otel_export;
