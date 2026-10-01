use std::{
    cell::{Cell, RefCell},
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};
use worker::{
    js_sys::{self, Function, Reflect},
    wasm_bindgen::{closure::Closure, JsCast, JsValue},
    Result,
};
// workers-rs 0.8 Delay requires a numeric timer ID. Recent workerd can return
// a Timeout object; retain the opaque handle and pass it back unchanged.
pub(super) struct Timer {
    millis: u64,
    callback: Option<Closure<dyn FnMut()>>,
    handle: Option<JsValue>,
    ready: Rc<Cell<bool>>,
    waker: Rc<RefCell<Option<Waker>>>,
}
impl Timer {
    pub(super) fn new(millis: u64) -> Self {
        Self {
            millis,
            callback: None,
            handle: None,
            ready: Rc::new(Cell::new(false)),
            waker: Rc::new(RefCell::new(None)),
        }
    }
}
impl Future for Timer {
    type Output = Result<()>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.ready.get() {
            return Poll::Ready(Ok(()));
        }
        *this.waker.borrow_mut() = Some(cx.waker().clone());
        if this.callback.is_none() {
            let ready = this.ready.clone();
            let waker = this.waker.clone();
            let callback = Closure::wrap_assert_unwind_safe(Box::new(move || {
                ready.set(true);
                if let Some(waker) = waker.borrow_mut().take() {
                    waker.wake();
                }
            }) as Box<dyn FnMut()>);
            let global = js_sys::global();
            let result = (|| -> Result<JsValue> {
                let function = Reflect::get(&global, &"setTimeout".into())?
                    .dyn_into::<Function>()
                    .map_err(worker::Error::from)?;
                Ok(function.call2(
                    &global,
                    callback.as_ref(),
                    &JsValue::from_f64(this.millis as f64),
                )?)
            })();
            match result {
                Ok(handle) => {
                    this.handle = Some(handle);
                    this.callback = Some(callback);
                }
                Err(error) => return Poll::Ready(Err(error)),
            }
        }
        Poll::Pending
    }
}
impl Drop for Timer {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let global = js_sys::global();
            if let Ok(function) = Reflect::get(&global, &"clearTimeout".into())
                .and_then(|value| value.dyn_into::<Function>())
            {
                let _ = function.call1(&global, &handle);
            }
        }
    }
}

// Generic runtime deadline used by cancellation-owned stream drivers.
pub(super) async fn deadline<T>(
    duration: std::time::Duration,
    future: impl Future<Output = T>,
) -> std::result::Result<T, ()> {
    match futures_util::future::select(
        Box::pin(future),
        Box::pin(Timer::new(duration.as_millis().min(u64::MAX as u128) as u64)),
    )
    .await
    {
        futures_util::future::Either::Left((value, _)) => Ok(value),
        futures_util::future::Either::Right(_) => Err(()),
    }
}
