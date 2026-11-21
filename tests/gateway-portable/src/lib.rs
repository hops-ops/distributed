//! UI/auth-only consumer: no GraphQL executor, SQL, listener or Worker SDK.
use distributed::gateway::*;

pub fn ui_and_auth() -> Result<Gateway, GatewayError> {
    let mut assets = Route::new("ui", RoutePath::prefix("/"), "assets");
    assets.admission = vec!["session".into()];
    GatewayConfig {
        bindings: vec![
            Binding::new("session", BindingKind::Admission),
            Binding::new("assets", BindingKind::Assets),
            Binding::new("auth", BindingKind::Handler),
        ],
        routes: vec![
            assets,
            Route::new("auth", RoutePath::prefix("/auth"), "auth"),
        ],
    }
    .build()
}
