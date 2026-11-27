//! The service guard: every test that needs an external service reads its
//! address through [`require_service`]. The crate's own unit tests use it too.

/// Names the services a run must have. `1`, `true` or `all` requires every
/// service a test asks for; otherwise a comma-separated list of the env vars
/// that must be set (for example `DATABASE_URL,NATS_URL`).
pub const REQUIRE_SERVICES: &str = "DISTRIBUTED_REQUIRE_SERVICES";

/// Read the service address in `var`.
///
/// When it is unset the test cannot run. A run that declared the service
/// required (see [`REQUIRE_SERVICES`]) panics, so CI can never report a test
/// that did not execute as passed. Any other run prints a `SKIPPED` line and
/// returns `None`; the caller returns early. Rust's test harness cannot mark
/// a test ignored at run time, so a local run still counts it as passed.
pub fn require_service(var: &str, what: &str) -> Option<String> {
    if let Ok(value) = std::env::var(var) {
        return Some(value);
    }
    let required = std::env::var(REQUIRE_SERVICES).unwrap_or_default();
    if is_required(&required, var) {
        panic!(
            "{what} needs {var}, which is not set, and {REQUIRE_SERVICES}={required} requires it"
        );
    }
    eprintln!("SKIPPED {what}: {var} is not set");
    None
}

/// Whether a [`REQUIRE_SERVICES`] value names `var`.
pub fn is_required(required: &str, var: &str) -> bool {
    required
        .split(',')
        .map(str::trim)
        .any(|entry| matches!(entry, "1" | "true" | "all") || entry == var)
}

#[cfg(test)]
mod tests {
    use super::is_required;

    #[test]
    fn unset_or_empty_requires_nothing() {
        assert!(!is_required("", "DATABASE_URL"));
        assert!(!is_required("0", "DATABASE_URL"));
    }

    #[test]
    fn all_requires_every_service() {
        for value in ["1", "true", "all"] {
            assert!(is_required(value, "DATABASE_URL"));
        }
    }

    #[test]
    fn a_list_requires_only_the_named_services() {
        assert!(is_required("DATABASE_URL, NATS_URL", "NATS_URL"));
        assert!(!is_required("DATABASE_URL,NATS_URL", "CELLD_URL"));
    }
}
