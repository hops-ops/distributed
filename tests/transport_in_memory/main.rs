//! In-memory reference run of the async-transport conformance harness.
//!
//! Proves the outbox dispatcher contract against adapter-neutral fakes, with no external broker. Concrete adapters
//! reuse the harness in [`transport_conformance`] for their own targets.

#[path = "../transport_conformance/mod.rs"]
mod conformance;

// --- publisher / outbox dispatcher contract ---------------------------------

#[tokio::test]
async fn dispatcher_completes_only_after_publish_success() {
    conformance::dispatcher_completes_only_after_publish_success().await;
}

#[tokio::test]
async fn dispatcher_unknown_outcome_stays_retryable() {
    conformance::dispatcher_unknown_outcome_stays_retryable().await;
}

#[tokio::test]
async fn dispatcher_claims_explicit_ids_before_publish() {
    conformance::dispatcher_claims_explicit_ids_before_publish().await;
}

// --- composed at-least-once: crash between publish and complete ---------------

#[tokio::test]
async fn publish_then_crash_republishes_and_consumer_inbox_dedupes() {
    conformance::publish_then_crash_republishes_and_consumer_inbox_dedupes().await;
}
