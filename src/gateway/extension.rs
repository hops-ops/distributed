use super::{Gateway, SelectedRoute};
use std::future::Future;

/// Protocol-neutral rejection for the host to render as an HTTP response.
#[derive(Clone, Copy, Debug)]
pub enum Rejection<'a> {
    /// Invalid or oversized request metadata (400).
    BadRequest,
    /// No declared route owns the path (404).
    NotFound,
    /// The selected owner does not support this method (405). Its methods can
    /// populate Allow; the adapter must not retry against a UI fallback.
    MethodNotAllowed(SelectedRoute<'a>),
}

/// Runtime execution seam. Implementations own bodies, headers, credentials,
/// streaming, cancellation and the selected binding/provider registry.
///
/// Futures deliberately have no Send bound: a Worker may hold local handles.
/// Native adapters can implement these methods with Send futures. Gateway
/// admission does not replace authorization at a remote or embedded executor.
pub trait GatewayAdapter {
    /// Complete runtime request, including the body and untrusted credentials.
    type Request;
    /// Authenticated/admitted context, defined by the adapter/provider contract.
    type Context;
    /// Complete runtime response, including streaming body and independent headers.
    type Response;

    /// The actual method of this request; it must agree with the executed request.
    fn method<'a>(&self, request: &'a Self::Request) -> &'a str;
    /// Raw origin-form path/query from this request (before decoding). Worker
    /// adapters extract this from the platform URL, never forwarded-host headers.
    fn target<'a>(&self, request: &'a Self::Request) -> &'a str;
    /// Authenticate and run every declared admission policy in order. Return a
    /// terminal denial response on failure. Public routes may return anonymous
    /// context, but must not treat an invalid credential as a valid principal.
    fn admit(
        &self,
        selected: SelectedRoute<'_>,
        request: &Self::Request,
    ) -> impl Future<Output = Result<Self::Context, Self::Response>>;
    /// Execute exactly the selected binding with admitted context. Preserve all
    /// data/errors/protocol evidence. Upstream failures are terminal responses;
    /// do not retry mutations or re-route 404/405/5xx responses to UI.
    fn execute(
        &self,
        selected: SelectedRoute<'_>,
        context: Self::Context,
        request: Self::Request,
    ) -> impl Future<Output = Self::Response>;
    /// Render a gateway rejection without resolving another route.
    fn reject(&self, rejection: Rejection<'_>) -> Self::Response;
}

impl Gateway {
    /// Select once, admit before serving (including protected assets), then
    /// execute once. The returned response/body is untouched and adapter-owned.
    pub async fn dispatch<A: GatewayAdapter>(
        &self,
        adapter: &A,
        request: A::Request,
    ) -> A::Response {
        let selected = match self.select(adapter.method(&request), adapter.target(&request)) {
            Ok(Some(selected)) => selected,
            Ok(None) => return adapter.reject(Rejection::NotFound),
            Err(_) => return adapter.reject(Rejection::BadRequest),
        };
        let context = match adapter.admit(selected, &request).await {
            Ok(context) => context,
            Err(response) => return response,
        };
        if !selected.method_allowed() {
            return adapter.reject(Rejection::MethodNotAllowed(selected));
        }
        adapter.execute(selected, context, request).await
    }
}
