#![cfg(feature = "gateway")]
mod gateway_support;
use distributed::gateway::*;
use gateway_support::*;

#[test]
fn route_ownership_matrix() {
    let cases = [
        ("GET", "/", "ui", true),
        ("POST", "/graphql?operationName=Write", "api", true),
        ("DELETE", "/graphql", "api", false),
        ("HEAD", "/graphql", "api", false),
        ("GET", "/graphql/missing", "api", true),
        ("DELETE", "/graphql/ws", "ws", true),
        ("GET", "/graphql/ws/child", "api", true),
        ("GET", "/graphqlish", "ui", true),
        ("POST", "/api/auth/callback", "auth", true),
        ("GET", "/api/%61uth/callback", "auth", true),
        ("GET", "/private/a.css", "protected", true),
        ("GET", "/privacy", "ui", true),
    ];
    let original = config();
    for reverse in [false, true] {
        let mut config = original.clone();
        if reverse {
            config.routes.reverse();
            config.bindings.reverse();
        }
        let gateway = config.build().unwrap();
        for (method, path, owner, allowed) in cases {
            let selected = gateway.select(method, path).unwrap().unwrap();
            assert_eq!(
                (selected.route().id.as_str(), selected.method_allowed()),
                (owner, allowed),
                "{method} {path}"
            );
        }
    }
}

#[test]
fn exact_owner_precedes_even_a_longer_prefix_and_prefixes_use_segment_boundaries() {
    let mut c = config();
    c.routes.push(Route::new(
        "nested",
        RoutePath::prefix("/private/images/"),
        "assets",
    ));
    c.routes.push(Route::new(
        "exact",
        RoutePath::exact("/private/images/logo"),
        "auth",
    ));
    let g = c.build().unwrap();
    for (path, owner) in [
        ("/private/images", "nested"),
        ("/private/images/", "nested"),
        ("/private/images/logo", "exact"),
        ("/private/images2", "protected"),
    ] {
        assert_eq!(g.select("GET", path).unwrap().unwrap().route().id, owner);
    }
}

#[test]
fn duplicate_and_normalized_alias_owners_fail_before_serving_even_with_disjoint_methods() {
    for path in [
        RoutePath::prefix("/graphql"),
        RoutePath::prefix("/graphql/"),
        RoutePath::prefix("/%67raphql"),
    ] {
        let mut c = config();
        let mut duplicate = Route::new("duplicate", path, "ui");
        duplicate.methods = Methods::Only(vec!["DELETE".into()]);
        c.routes.push(duplicate);
        assert!(c.build().is_err());
    }
    let mut c = config();
    c.routes
        .push(Route::new("api", RoutePath::exact("/elsewhere"), "ui"));
    assert!(c.build().is_err());
}

#[test]
fn ambiguous_paths_and_malformed_methods_fail_closed() {
    let g = config().build().unwrap();
    for path in [
        "",
        "graphql",
        "https://host/graphql",
        "//host/graphql",
        "/a//b",
        "/../graphql",
        "/a/./b",
        "/a/%2e%2e/graphql",
        "/%2fgraphql",
        "/%5Cgraphql",
        "/%252e",
        "/a\\b",
        "/%00",
        "/%7f",
        "/%ff",
        "/%c0%af",
        "/%",
        "/%2",
        "/%ZZ",
        "/a#b",
        "/a%3fb",
        "/a%23b",
    ] {
        assert!(g.select("GET", path).is_err(), "accepted {path:?}");
    }
    for method in ["", "G ET", "GET\r\n", "GÉT"] {
        assert!(g.select(method, "/").is_err());
    }
    assert!(g
        .select("GET", &format!("/{}", "a".repeat(MAX_PATH_BYTES)))
        .is_err());
    assert_eq!(
        g.select("GET", "/caf%C3%A9").unwrap().unwrap().route().id,
        "ui"
    );
}

#[test]
fn protected_assets_admit_in_order_before_execution() {
    let g = config().build().unwrap();
    for authorized in [false, true] {
        let adapter = Adapter::new(200);
        let result = run(g.dispatch(
            &adapter,
            Request {
                method: "GET",
                target: "/private/app.js",
                authorized,
            },
        ));
        assert_eq!(result.status, if authorized { 200 } else { 401 });
        let expected = if authorized {
            vec!["admit:identity", "admit:policy", "execute:assets"]
        } else {
            vec!["admit:identity"]
        };
        assert_eq!(*adapter.calls.borrow(), expected);
    }
}

#[test]
fn owned_errors_never_retry_or_reach_ui_fallback() {
    let g = config().build().unwrap();
    for target in ["/graphql/missing", "/api/auth/missing"] {
        for status in [404, 405, 500, 503, 504] {
            let adapter = Adapter::new(status);
            let response = run(g.dispatch(
                &adapter,
                Request {
                    method: "GET",
                    target,
                    authorized: true,
                },
            ));
            assert_eq!(response.status, status);
            assert_eq!(response.evidence, "opaque-causal-envelope");
            assert_eq!(adapter.calls.borrow().len(), 1);
            assert!(!adapter
                .calls
                .borrow()
                .iter()
                .any(|call| call == "execute:ui"));
        }
    }
    let adapter = Adapter::new(200);
    assert_eq!(
        run(g.dispatch(
            &adapter,
            Request {
                method: "DELETE",
                target: "/graphql",
                authorized: true
            }
        ))
        .status,
        405
    );
    assert!(adapter.calls.borrow().is_empty());
}

#[test]
fn absent_mounts_expose_nothing_and_invalid_requests_do_not_execute() {
    let g = GatewayConfig::default().build().unwrap();
    let adapter = Adapter::new(200);
    for (target, status) in [("/", 404), ("/graphql", 404), ("/%2fprivate", 400)] {
        assert_eq!(
            run(g.dispatch(
                &adapter,
                Request {
                    method: "GET",
                    target,
                    authorized: false
                }
            ))
            .status,
            status
        );
    }
    assert!(adapter.calls.borrow().is_empty());
}

#[test]
fn declarations_are_bounded_and_references_are_checked() {
    let mut c = config();
    c.routes[0].target = "missing".into();
    assert!(c.build().is_err());
    let mut c = config();
    c.routes[0].target = "identity".into();
    assert!(c.build().is_err());
    for admission in [
        vec!["missing".into()],
        vec!["ui".into()],
        vec!["identity".into(); MAX_ADMISSIONS + 1],
        vec!["identity".into(); 2],
    ] {
        let mut c = config();
        c.routes[0].admission = admission;
        assert!(c.build().is_err());
    }
    let mut c = config();
    c.routes[0].methods = Methods::Only(vec![]);
    assert!(c.build().is_err());
    let mut c = config();
    c.routes[0].methods = Methods::Only(vec!["GET".into(); 2]);
    assert!(c.build().is_err());
    let mut c = config();
    c.bindings.push(c.bindings[0].clone());
    assert!(c.build().is_err());
    let mut c = config();
    c.routes = vec![c.routes[0].clone(); MAX_ROUTES + 1];
    assert!(c.build().is_err());
    let mut c = config();
    c.bindings = vec![c.bindings[0].clone(); MAX_BINDINGS + 1];
    assert!(c.build().is_err());
}

#[test]
fn configured_origins_cannot_hide_paths_credentials_or_non_http_targets() {
    for origin in [
        "https://site.test/path",
        "https://site.test/..",
        "https://site.test//",
        "https:site.test",
        "//site.test",
        "file:///etc/passwd",
        "ftp://site.test",
        "https://user:pass@site.test",
        "https://@site.test",
        "https://site.test?url=elsewhere",
        "https://site.test#x",
        "https://site.test\\evil",
        "https://site.test\n",
    ] {
        let mut c = config();
        c.bindings[5].kind = BindingKind::UiProxy {
            origin: origin.into(),
        };
        assert!(c.build().is_err(), "accepted {origin:?}");
    }
    for origin in [
        "http://localhost:5180",
        "https://site.test/",
        "http://[::1]:5180",
    ] {
        let mut c = config();
        c.bindings[5].kind = BindingKind::UiProxy {
            origin: origin.into(),
        };
        assert!(c.build().is_ok());
    }
}

#[test]
fn serialized_configuration_still_requires_validation() {
    let c = config();
    let serialized = serde_json::to_vec(&c).unwrap();
    let roundtrip: GatewayConfig = serde_json::from_slice(&serialized).unwrap();
    assert_eq!(roundtrip, c);
    assert!(roundtrip.build().is_ok());
    assert!(serde_json::from_str::<GatewayConfig>(
        r#"{"routes":[],"bindings":[],"implicit_ui":true}"#
    )
    .is_err());
}

#[test]
fn identifiers_and_exact_inventory_limits_are_enforced() {
    let mut c = GatewayConfig {
        bindings: vec![Binding::new("assets", BindingKind::Assets)],
        routes: (0..MAX_ROUTES)
            .map(|i| {
                Route::new(
                    format!("route-{i}"),
                    RoutePath::exact(format!("/route-{i}")),
                    "assets",
                )
            })
            .collect(),
    };
    assert!(c.clone().build().is_ok());
    c.routes[0].id = "a".repeat(MAX_ID_BYTES + 1);
    assert!(c.build().is_err());
    let c = GatewayConfig {
        bindings: (0..MAX_BINDINGS)
            .map(|i| Binding::new(format!("binding-{i}"), BindingKind::Handler))
            .collect(),
        routes: vec![],
    };
    assert!(c.build().is_ok());
}
