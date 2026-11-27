#![cfg(feature = "gateway")]
mod gateway_support;
use distributed::gateway::*;
use gateway_support::*;
use std::rc::Rc;

fn graphql(executor: GraphqlExecutor, schema_extensions: Vec<String>) -> BindingKind {
    BindingKind::Graphql {
        executor,
        schema_extensions,
        capabilities: GraphqlCapabilities {
            commands: true,
            queries: true,
            live: true,
        },
        delivery: DeliveryCapabilities::default(),
    }
}

#[test]
fn custom_route_provider_and_field() {
    let mut c = config();
    c.bindings.push(Binding::new(
        "schema",
        graphql(
            GraphqlExecutor::Embedded,
            vec!["custom_health_field".into()],
        ),
    ));
    let mut custom = Route::new("custom", RoutePath::exact("/custom"), "schema");
    custom.admission = vec!["identity".into()];
    c.routes.push(custom);
    let gateway = c.build().unwrap();
    let mut adapter = Adapter::new(200);
    adapter.provider = Rc::new("replacement-provider".into());
    let response = run(gateway.dispatch(
        &adapter,
        Request {
            method: "GET",
            target: "/custom",
            authorized: true,
        },
    ));
    assert_eq!(response.body, "schema:replacement-provider");
    assert_eq!(
        *adapter.calls.borrow(),
        ["admit:identity", "execute:schema"]
    );
    let BindingKind::Graphql {
        schema_extensions,
        delivery,
        ..
    } = &gateway.binding("schema").unwrap().kind
    else {
        panic!("wrong executor kind")
    };
    assert_eq!(schema_extensions, &["custom_health_field"]);
    assert_eq!(*delivery, DeliveryCapabilities::default());
}

#[test]
fn remote_binding_requires_extensions_at_its_executor() {
    let remote = GraphqlExecutor::Remote {
        origin: "https://api.example.test".into(),
    };
    for extensions in [vec![], vec!["local_field".into()]] {
        let mut c = config();
        c.bindings.push(Binding::new(
            "schema",
            graphql(remote.clone(), extensions.clone()),
        ));
        assert_eq!(c.build().is_ok(), extensions.is_empty());
    }
    let mut c = config();
    c.bindings.push(Binding::new(
        "schema",
        graphql(GraphqlExecutor::Embedded, vec!["field".into(); 2]),
    ));
    assert!(c.build().is_err());
}

#[test]
fn delivery_mounts_are_independent_and_require_their_surface() {
    for queries in [false, true] {
        for live in [false, true] {
            for snapshots in [false, true] {
                for coalescing in [false, true] {
                    for live_sharing in [false, true] {
                        let c = GatewayConfig {
                            bindings: vec![Binding::new(
                                "schema",
                                BindingKind::Graphql {
                                    executor: GraphqlExecutor::Embedded,
                                    capabilities: GraphqlCapabilities {
                                        commands: true,
                                        queries,
                                        live,
                                    },
                                    delivery: DeliveryCapabilities {
                                        snapshots,
                                        coalescing,
                                        live_sharing,
                                    },
                                    schema_extensions: vec![],
                                },
                            )],
                            routes: vec![],
                        };
                        assert_eq!(
                            c.build().is_ok(),
                            (!(snapshots || coalescing) || queries) && (!live_sharing || live)
                        );
                    }
                }
            }
        }
    }
    let c = GatewayConfig {
        bindings: vec![Binding::new(
            "empty",
            BindingKind::Graphql {
                executor: GraphqlExecutor::Embedded,
                capabilities: GraphqlCapabilities::default(),
                delivery: DeliveryCapabilities::default(),
                schema_extensions: vec![],
            },
        )],
        routes: vec![],
    };
    assert!(c.build().is_err());
}

#[test]
fn native_adapter_can_return_a_send_dispatch_future() {
    struct Native;
    impl GatewayAdapter for Native {
        type Request = String;
        type Context = ();
        type Response = u16;
        fn method<'a>(&self, _: &'a String) -> &'a str {
            "GET"
        }
        fn target<'a>(&self, request: &'a String) -> &'a str {
            request
        }
        async fn admit(&self, _: SelectedRoute<'_>, _: &String) -> Result<(), u16> {
            Ok(())
        }
        async fn execute(&self, _: SelectedRoute<'_>, _: (), _: String) -> u16 {
            204
        }
        fn reject(&self, _: Rejection<'_>) -> u16 {
            404
        }
    }
    fn require_send<T: Send>(value: T) -> T {
        value
    }
    let gateway = config().build().unwrap();
    assert_eq!(
        run(require_send(gateway.dispatch(&Native, "/".into()))),
        204
    );
}
