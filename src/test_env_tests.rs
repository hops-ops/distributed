//! The service guard is shared by every test target; it is tested once, here.

use crate::test_env::is_required;

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
