//! Knative manifest helpers: render `Trigger` YAML and sanitize Kubernetes names.
//!
//! The CloudEvents HTTP *ingress* (the consume side) is `Service`-coupled and
//! lives on the microsvc side (`microsvc::cloud_events_router`); this module keeps
//! only the produce/manifest helpers, which need just the bus vocabulary
//! ([`SubscriptionPlan`]). Used by [`KnativeBus`](super::KnativeBus).
//!
//! Requires the `http` feature.

use std::collections::HashSet;

use super::SubscriptionPlan;

/// Sanitize a string into an RFC 1123 DNS label usable as a Kubernetes resource
/// name: lowercase, ASCII-alphanumeric or `-`, no leading/trailing `-`, ≤63
/// chars. Used for generated `Trigger` names, whose CloudEvent-type segment can
/// contain dots/uppercase/other characters that are invalid in k8s names.
pub(super) fn sanitize_k8s_name(name: &str) -> String {
    let mapped: String = name
        .chars()
        .map(|c| {
            let c = c.to_ascii_lowercase();
            if c.is_ascii_alphanumeric() {
                c
            } else {
                '-'
            }
        })
        .collect();
    let capped: String = mapped.trim_matches('-').chars().take(63).collect();
    let trimmed = capped.trim_end_matches('-');
    if trimmed.is_empty() {
        "x".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Sanitize `raw` into a Kubernetes name that is **unique** within `used`.
///
/// Distinct CloudEvent types can normalize to the same DNS label (e.g.
/// `order.created` and `order-created`, or a command and an event of the same
/// name) — which would otherwise emit two `Trigger`s with the same
/// `metadata.name`, the second silently clobbering the first on apply. On a
/// collision, append a numeric suffix, truncating the base so the result stays
/// within the 63-char label limit. The Trigger's `type:` filter still carries the
/// raw event name, so routing is unaffected; only the resource name is adjusted.
pub(super) fn unique_k8s_name(raw: &str, used: &mut HashSet<String>) -> String {
    let base = sanitize_k8s_name(raw);
    if used.insert(base.clone()) {
        return base;
    }
    let mut n = 2usize;
    loop {
        let suffix = format!("-{n}");
        let head = base.len().min(63usize.saturating_sub(suffix.len()));
        let candidate = format!("{}{suffix}", base[..head].trim_end_matches('-'));
        if used.insert(candidate.clone()) {
            return candidate;
        }
        n += 1;
    }
}

/// Render Knative `Trigger` YAML for each event a service subscribes to, derived
/// from its [`SubscriptionPlan`]. Each Trigger filters on the CloudEvent `type`
/// and routes to `subscriber_service` on `broker`.
pub fn knative_triggers(plan: &SubscriptionPlan, broker: &str, subscriber_service: &str) -> String {
    let mut out = String::new();
    let mut used = HashSet::new();
    for event in &plan.events {
        let trigger_name = unique_k8s_name(&format!("{subscriber_service}-{event}"), &mut used);
        out.push_str(&format!(
            "apiVersion: eventing.knative.dev/v1\n\
             kind: Trigger\n\
             metadata:\n\
             \x20 name: {trigger_name}\n\
             spec:\n\
             \x20 broker: {broker}\n\
             \x20 filter:\n\
             \x20   attributes:\n\
             \x20     type: {event}\n\
             \x20 subscriber:\n\
             \x20   ref:\n\
             \x20     apiVersion: serving.knative.dev/v1\n\
             \x20     kind: Service\n\
             \x20     name: {subscriber_service}\n\
             ---\n"
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One Trigger per subscribed event on the given broker. The CloudEvent
    /// type filter keeps the raw type; the resource name is sanitized.
    #[test]
    fn a_trigger_keeps_the_raw_event_type_under_a_sanitized_name() {
        struct Case {
            name: &'static str,
            event: &'static str,
            type_filter: &'static str,
            trigger_name: &'static str,
        }
        const CASES: &[Case] = &[
            Case {
                name: "a lowercase dotted type",
                event: "seat.reserved",
                type_filter: "type: seat.reserved",
                trigger_name: "name: checkout-projection-seat-reserved",
            },
            Case {
                name: "a messy type is sanitized in the name only",
                event: "Order.Created",
                type_filter: "type: Order.Created",
                trigger_name: "name: checkout-projection-order-created",
            },
        ];
        for case in CASES {
            let plan = SubscriptionPlan {
                commands: vec![],
                events: vec![case.event.to_string()],
            };
            let yaml = knative_triggers(&plan, "default", "checkout-projection");
            assert!(yaml.contains("kind: Trigger"), "{}: {yaml}", case.name);
            assert!(yaml.contains(case.type_filter), "{}: {yaml}", case.name);
            assert!(yaml.contains(case.trigger_name), "{}: {yaml}", case.name);
            assert!(yaml.contains("broker: default"), "{}: {yaml}", case.name);
        }
    }

    #[test]
    fn sanitize_k8s_name_enforces_rfc1123() {
        // Valid names pass through unchanged.
        assert_eq!(
            sanitize_k8s_name("checkout-projection-seat-reserved"),
            "checkout-projection-seat-reserved"
        );
        // Dots, uppercase, and other characters become '-' and lowercase.
        assert_eq!(sanitize_k8s_name("Order.Created!"), "order-created");
        // No leading/trailing dashes, capped at 63 chars.
        let long = "a".repeat(80);
        let out = sanitize_k8s_name(&format!(".{long}."));
        assert_eq!(out.len(), 63);
        assert!(!out.starts_with('-') && !out.ends_with('-'));
        // All-invalid degrades to a safe placeholder.
        assert_eq!(sanitize_k8s_name("..."), "x");
    }

    #[test]
    fn trigger_names_are_deduped_when_event_types_normalize_alike() {
        // `order.created` and `order-created` both sanitize to the same label;
        // the second Trigger must get a distinct name so neither clobbers the
        // other on apply, while both keep their raw `type:` filters.
        let plan = SubscriptionPlan {
            commands: vec![],
            events: vec!["order.created".to_string(), "order-created".to_string()],
        };
        let yaml = knative_triggers(&plan, "default", "svc");
        assert!(yaml.contains("name: svc-order-created\n"));
        assert!(yaml.contains("name: svc-order-created-2\n"));
        assert!(yaml.contains("type: order.created"));
        assert!(yaml.contains("type: order-created"));
    }

    #[test]
    fn unique_k8s_name_caps_suffixed_names_at_63_chars() {
        let mut used = HashSet::new();
        let long = "a".repeat(80);
        let first = unique_k8s_name(&long, &mut used);
        let second = unique_k8s_name(&long, &mut used);
        assert_eq!(first.len(), 63);
        assert_ne!(first, second);
        assert!(second.len() <= 63);
        assert!(second.ends_with("-2"));
    }
}
