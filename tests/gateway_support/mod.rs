#![allow(dead_code)]
use distributed::gateway::*;
use std::{
    cell::RefCell,
    future::Future,
    pin::pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

pub fn run<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("contract fixture unexpectedly requires an I/O runtime"),
    }
}

pub struct Request {
    pub method: &'static str,
    pub target: &'static str,
    pub authorized: bool,
}

#[derive(Debug, PartialEq)]
pub struct Response {
    pub status: u16,
    pub body: String,
    pub evidence: &'static str,
}

pub struct Adapter {
    pub calls: RefCell<Vec<String>>,
    pub provider: Rc<String>,
    pub status: u16,
}

impl Adapter {
    pub fn new(status: u16) -> Self {
        Self {
            calls: RefCell::new(Vec::new()),
            provider: Rc::new("default".into()),
            status,
        }
    }
}

impl GatewayAdapter for Adapter {
    type Request = Request;
    type Context = Rc<String>;
    type Response = Response;
    fn method<'a>(&self, request: &'a Request) -> &'a str {
        request.method
    }
    fn target<'a>(&self, request: &'a Request) -> &'a str {
        request.target
    }
    async fn admit(
        &self,
        selected: SelectedRoute<'_>,
        request: &Request,
    ) -> Result<Rc<String>, Response> {
        for policy in &selected.route().admission {
            self.calls.borrow_mut().push(format!("admit:{policy}"));
            if !request.authorized {
                return Err(Response {
                    status: 401,
                    body: "denied".into(),
                    evidence: "",
                });
            }
        }
        // Holding Rc across await proves that neither the contract nor dispatcher
        // forces Worker adapters into Send futures or a native async runtime.
        let context = self.provider.clone();
        std::future::ready(()).await;
        Ok(context)
    }
    async fn execute(
        &self,
        selected: SelectedRoute<'_>,
        context: Rc<String>,
        _request: Request,
    ) -> Response {
        self.calls
            .borrow_mut()
            .push(format!("execute:{}", selected.binding().id));
        std::future::ready(()).await;
        Response {
            status: self.status,
            body: format!("{}:{}", selected.binding().id, context),
            evidence: "opaque-causal-envelope",
        }
    }
    fn reject(&self, rejection: Rejection<'_>) -> Response {
        let status = match rejection {
            Rejection::BadRequest => 400,
            Rejection::NotFound => 404,
            Rejection::MethodNotAllowed(_) => 405,
        };
        Response {
            status,
            body: "rejected".into(),
            evidence: "",
        }
    }
}

pub fn config() -> GatewayConfig {
    let mut api = Route::new("api", RoutePath::prefix("/graphql"), "api");
    api.methods = Methods::Only(vec!["POST".into(), "GET".into()]);
    let auth = Route::new("auth", RoutePath::prefix("/api/auth"), "auth");
    let mut protected = Route::new("protected", RoutePath::prefix("/private"), "assets");
    protected.admission = vec!["identity".into(), "policy".into()];
    GatewayConfig {
        bindings: vec![
            Binding::new("api", BindingKind::Handler),
            Binding::new("auth", BindingKind::Handler),
            Binding::new("assets", BindingKind::Assets),
            Binding::new("identity", BindingKind::Admission),
            Binding::new("policy", BindingKind::Admission),
            Binding::new(
                "ui",
                BindingKind::UiProxy {
                    origin: "http://localhost:5180".into(),
                },
            ),
        ],
        routes: vec![
            api,
            auth,
            protected,
            Route::new("ws", RoutePath::exact("/graphql/ws"), "api"),
            Route::new("ui", RoutePath::prefix("/"), "ui"),
        ],
    }
}
