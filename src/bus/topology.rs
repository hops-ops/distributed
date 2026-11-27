use super::router::MessageRouter;
use super::TransportError;

/// Default broker namespace applied when a transport is constructed without an
/// explicit one.
pub const DEFAULT_BUS_NAMESPACE: &str = "default";
/// Maximum byte length accepted for a consumer group or namespace name.
pub const MAX_TOPOLOGY_NAME_LEN: usize = 128;

/// Consumer-group and namespace topology for a broker-backed bus.
///
/// Third-party transports can reuse this to apply the same portable naming
/// rules (`group`/`namespace` validation) the built-in transports use, instead
/// of reimplementing them. Construct via [`BusTopologyConfig::default`] and
/// refine with [`group`](Self::group) / [`namespace`](Self::namespace), then
/// [`validate_for`](Self::validate_for) before touching broker topology.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BusTopologyConfig {
    group: Option<String>,
    namespace: String,
}

impl Default for BusTopologyConfig {
    fn default() -> Self {
        Self {
            group: None,
            namespace: DEFAULT_BUS_NAMESPACE.to_string(),
        }
    }
}

impl BusTopologyConfig {
    /// The default namespace used when none is set.
    pub fn default_namespace() -> &'static str {
        DEFAULT_BUS_NAMESPACE
    }

    /// Set the durable consumer group.
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }

    /// Set the broker namespace/prefix.
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// The configured namespace without re-validating it.
    pub fn namespace_unchecked(&self) -> &str {
        &self.namespace
    }

    /// Resolve the effective consumer group, falling back to the router's
    /// identity, validating the result.
    pub fn resolve_consumer_group<R: MessageRouter>(
        &self,
        router: &R,
        transport: &str,
    ) -> Result<String, TransportError> {
        resolve_consumer_group(self.group.as_deref(), router, transport)
    }

    /// Validate and return the namespace for the given transport.
    pub fn namespace_for(&self, transport: &str) -> Result<String, TransportError> {
        validate_namespace(&self.namespace, transport)
    }

    /// Validate both group and namespace, returning a checked config.
    pub fn validate_for(self, transport: &str) -> Result<Self, TransportError> {
        let group = self
            .group
            .map(|group| validate_consumer_group(&group, transport))
            .transpose()?;
        let namespace = validate_namespace(&self.namespace, transport)?;
        Ok(Self { group, namespace })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TopologyNameKind {
    ConsumerGroup,
    Namespace,
}

impl TopologyNameKind {
    fn label(self) -> &'static str {
        match self {
            Self::ConsumerGroup => "consumer group",
            Self::Namespace => "namespace",
        }
    }

    fn allows_dot(self) -> bool {
        matches!(self, Self::Namespace)
    }
}

/// Validate a consumer-group name against the portable topology rules.
pub fn validate_consumer_group(value: &str, transport: &str) -> Result<String, TransportError> {
    validate_topology_name(value, TopologyNameKind::ConsumerGroup, transport)
}

/// Validate a namespace name against the portable topology rules (dots allowed).
pub fn validate_namespace(value: &str, transport: &str) -> Result<String, TransportError> {
    validate_topology_name(value, TopologyNameKind::Namespace, transport)
}

/// Resolve a consumer group from an explicit value or the router identity,
/// validating the result.
pub fn resolve_consumer_group<R: MessageRouter>(
    explicit: Option<&str>,
    router: &R,
    transport: &str,
) -> Result<String, TransportError> {
    let Some(group) = explicit.or_else(|| router.consumer_group()) else {
        return Err(TransportError::permanent(format!(
            "{transport} bus requires a consumer group; call `Service::named(..)` \
             for service consumers or `bus.group(..)` for direct consumers"
        )));
    };

    validate_consumer_group(&scoped_group(group, router), transport)
}

/// `group`, with the router's [`consumer_scope`](MessageRouter::consumer_scope)
/// appended when it has one.
pub(super) fn scoped_group<R: MessageRouter>(group: &str, router: &R) -> String {
    match router.consumer_scope() {
        Some(scope) => format!("{group}-{scope}"),
        None => group.to_owned(),
    }
}

fn validate_topology_name(
    value: &str,
    kind: TopologyNameKind,
    transport: &str,
) -> Result<String, TransportError> {
    let label = kind.label();
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(topology_error(transport, label, "cannot be empty"));
    }
    if trimmed.len() != value.len() {
        return Err(topology_error(
            transport,
            label,
            "cannot contain leading or trailing whitespace",
        ));
    }
    if value.len() > MAX_TOPOLOGY_NAME_LEN {
        return Err(topology_error(
            transport,
            label,
            format!("cannot exceed {MAX_TOPOLOGY_NAME_LEN} bytes"),
        ));
    }

    for ch in value.chars() {
        if ch.is_control() {
            return Err(topology_error(
                transport,
                label,
                format!("cannot contain control character {}", display_char(ch)),
            ));
        }
        if ch.is_whitespace() {
            return Err(topology_error(
                transport,
                label,
                format!("cannot contain whitespace character {}", display_char(ch)),
            ));
        }
        if matches!(ch, '*' | '>') {
            return Err(topology_error(
                transport,
                label,
                format!("cannot contain NATS wildcard {}", display_char(ch)),
            ));
        }
        if matches!(ch, '/' | '\\') {
            return Err(topology_error(
                transport,
                label,
                format!("cannot contain path separator {}", display_char(ch)),
            ));
        }
        if ch == '.' && !kind.allows_dot() {
            return Err(topology_error(
                transport,
                label,
                "cannot contain `.`; use `-` or `_` for portable group names",
            ));
        }
        if !(ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') || ch == '.') {
            return Err(topology_error(
                transport,
                label,
                format!("cannot contain character {}", display_char(ch)),
            ));
        }
    }

    Ok(value.to_string())
}

fn topology_error(transport: &str, label: &str, reason: impl std::fmt::Display) -> TransportError {
    TransportError::permanent(format!("{transport} bus {label} {reason}"))
}

fn display_char(ch: char) -> String {
    if ch.is_control() {
        format!("U+{:04X}", u32::from(ch))
    } else {
        format!("`{ch}`")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::{Message, MessageKind, SubscriptionPlan};

    struct TestRouter {
        group: Option<&'static str>,
    }

    impl MessageRouter for TestRouter {
        fn consumer_group(&self) -> Option<&str> {
            self.group
        }

        fn handles(&self, _kind: MessageKind, _name: &str) -> bool {
            false
        }

        fn subscription_plan(&self) -> SubscriptionPlan {
            SubscriptionPlan::default()
        }

        async fn dispatch(&self, _message: &Message) -> Result<(), TransportError> {
            Ok(())
        }
    }

    #[test]
    fn resolve_consumer_group_takes_the_bus_group_else_the_router_identity() {
        struct Case {
            name: &'static str,
            bus_group: Option<&'static str>,
            expected: &'static str,
        }
        const CASES: &[Case] = &[
            Case {
                name: "an explicit bus group wins over the router identity",
                bus_group: Some("override"),
                expected: "override",
            },
            Case {
                name: "without a bus group the router identity is used",
                bus_group: None,
                expected: "service",
            },
        ];
        let router = TestRouter {
            group: Some("service"),
        };
        for case in CASES {
            let group = resolve_consumer_group(case.bus_group, &router, "test").unwrap();
            assert_eq!(group, case.expected, "{}", case.name);
        }
    }

    #[test]
    fn resolve_consumer_group_rejects_missing_identity() {
        let router = TestRouter { group: None };
        let err = resolve_consumer_group(None, &router, "test").unwrap_err();
        assert!(err.is_permanent());
        assert!(err.message().contains("Service::named"));
    }

    #[test]
    fn resolve_consumer_group_rejects_whitespace_identity() {
        let router = TestRouter {
            group: Some("  service"),
        };
        let err = resolve_consumer_group(None, &router, "test").unwrap_err();
        assert!(err.is_permanent());
        assert!(err.message().contains("whitespace"));
    }

    #[test]
    fn validate_consumer_group_rejects_wildcards() {
        let err = validate_consumer_group("orders>*", "nats").unwrap_err();
        assert!(err.message().contains("NATS wildcard"));
    }

    #[test]
    fn validate_consumer_group_rejects_path_separators() {
        let err = validate_consumer_group("tenant/orders", "rabbitmq").unwrap_err();
        assert!(err.message().contains("path separator"));
    }

    #[test]
    fn validate_consumer_group_rejects_overlong_names() {
        let value = "a".repeat(MAX_TOPOLOGY_NAME_LEN + 1);
        let err = validate_consumer_group(&value, "kafka").unwrap_err();
        assert!(err.message().contains("cannot exceed"));
    }

    #[test]
    fn validate_namespace_accepts_dotted_names() {
        let namespace = validate_namespace("todos-prod.v1", "kafka").unwrap();
        assert_eq!(namespace, "todos-prod.v1");
    }

    #[test]
    fn validate_namespace_rejects_control_characters() {
        let err = validate_namespace("todos\nprod", "nats").unwrap_err();
        assert!(err.message().contains("control character"));
    }
}
