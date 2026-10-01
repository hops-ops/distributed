#![cfg(feature = "gateway")]
use distributed::application::{
    compile_deployment_plan, Application, MountSelector, ProcessIntent, Runtime,
};
use distributed::gateway::*;
use std::cell::Cell;

fn ui(origin: &str) -> Gateway {
    GatewayConfig {
        bindings: vec![Binding::new(
            "ui",
            BindingKind::UiProxy {
                origin: origin.into(),
            },
        )],
        routes: vec![Route::new("ui", RoutePath::prefix("/"), "ui")],
    }
    .build()
    .unwrap()
}
#[test]
fn selected_capabilities_only() {
    let gateway = ui("http://ui.internal:3000");
    let app = Application::new("site")
        .build()
        .unwrap()
        .with_gateway("public", &gateway)
        .unwrap();
    let selected = MountSelector::gateway("public").unwrap();
    let unused = MountSelector::gateway("unused").unwrap();
    let runtime = Runtime::default()
        .mount_gateway(&app, selected.clone(), gateway)
        .unwrap();
    let allocated = Cell::new(0);
    let construct = |_: &Gateway| {
        allocated.set(allocated.get() + 1);
        Ok::<_, ()>("adapter")
    };
    assert_eq!(runtime.bind_gateway(&unused, construct), Ok(None));
    assert_eq!(allocated.get(), 0);
    assert_eq!(
        runtime.bind_gateway(&selected, construct),
        Ok(Some("adapter"))
    );
    assert_eq!(allocated.get(), 1);
    assert!(runtime.starts_gateway());
    assert!(!runtime.starts_graphql());
    assert!(!runtime.starts_outbox());
    assert!(!runtime.starts_projector_consumer());
    let plan = compile_deployment_plan(
        "edge",
        app.manifest(),
        [ProcessIntent::new("ingress").unwrap().mounts([selected])],
    )
    .unwrap();
    assert!(plan.processes[0]
        .mounts
        .iter()
        .all(|mount| matches!(mount, MountSelector::Extension { .. })));
    assert!(!plan.processes[0].capabilities.iter().any(|requirement| {
        let name = requirement.capability.as_str();
        name.contains("store") || name.contains("project") || name.contains("dispatch")
    }));
}
#[test]
fn physical_binding_changes_do_not_rewrite_application_identity() {
    let a = Application::new("site")
        .build()
        .unwrap()
        .with_gateway("public", &ui("http://a.internal:3000"))
        .unwrap();
    let b = Application::new("site")
        .build()
        .unwrap()
        .with_gateway("public", &ui("http://b.internal:4000"))
        .unwrap();
    assert_eq!(a.canonical_bytes().unwrap(), b.canonical_bytes().unwrap());
    assert!(!String::from_utf8(a.canonical_bytes().unwrap())
        .unwrap()
        .contains("internal"));
    assert!(Runtime::default()
        .mount_gateway(
            &a,
            MountSelector::command("public").unwrap(),
            ui("http://a.internal:3000")
        )
        .is_err());
    let other = GatewayConfig {
        bindings: vec![Binding::new("ui", BindingKind::Handler)],
        routes: vec![Route::new("ui", RoutePath::prefix("/"), "ui")],
    }
    .build()
    .unwrap();
    assert!(Runtime::default()
        .mount_gateway(&a, MountSelector::gateway("public").unwrap(), other)
        .is_err());
}
