use futures_channel::oneshot;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use worker::{
    wasm_bindgen::{closure::Closure, JsCast},
    web_sys::AbortSignal,
    Request, Response, Result,
};

/// Own the event listener so cancellation drops the actual in-flight Rust work.
pub(super) struct Cancelled {
    signal: AbortSignal,
    callback: Closure<dyn FnMut()>,
    receiver: oneshot::Receiver<()>,
}
impl Cancelled {
    pub fn new(signal: AbortSignal) -> Result<Self> {
        let (sender, receiver) = oneshot::channel();
        let mut sender = Some(sender);
        let callback = Closure::wrap_assert_unwind_safe(Box::new(move || {
            if let Some(sender) = sender.take() {
                let _ = sender.send(());
            }
        }) as Box<dyn FnMut()>);
        signal.add_event_listener_with_callback("abort", callback.as_ref().unchecked_ref())?;
        Ok(Self {
            signal,
            callback,
            receiver,
        })
    }
}
impl Future for Cancelled {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.signal.aborted() {
            return Poll::Ready(());
        }
        Pin::new(&mut self.receiver).poll(cx).map(|_| ())
    }
}
impl Drop for Cancelled {
    fn drop(&mut self) {
        let _ = self
            .signal
            .remove_event_listener_with_callback("abort", self.callback.as_ref().unchecked_ref());
    }
}
pub(super) async fn run(
    signal: AbortSignal,
    future: impl Future<Output = Result<Response>>,
) -> Result<Response> {
    match futures_util::future::select(Box::pin(future), Box::pin(Cancelled::new(signal)?)).await {
        futures_util::future::Either::Left((result, _)) => result,
        futures_util::future::Either::Right(_) => Response::error("request cancelled", 499),
    }
}
pub(super) fn preserve_signal(request: Request, signal: &AbortSignal) -> Result<Request> {
    let init = worker::web_sys::RequestInit::new();
    init.set_signal(Some(signal));
    Ok(worker::web_sys::Request::new_with_request_and_init(request.inner(), &init)?.into())
}

/// Dropping a WebSocket operation also cancels its HTTP/DO fetch promise.
pub(super) struct AbortOnDrop(pub worker::web_sys::AbortController);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}
