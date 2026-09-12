use futures_channel::mpsc;
use futures_util::StreamExt;
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};
use worker::{
    js_sys::Reflect,
    wasm_bindgen::{closure::Closure, JsCast, JsValue},
    Result, WebSocket,
};
/// Runtime callbacks feed a bounded queue. workers-rs EventStream is unbounded.
type SocketCallback = (&'static str, Closure<dyn FnMut(JsValue)>);
pub(super) struct Socket {
    pub ws: WebSocket,
    receiver: mpsc::Receiver<std::result::Result<String, String>>,
    overflow: Rc<Cell<bool>>,
    queued_bytes: Rc<Cell<usize>>,
    callbacks: Vec<SocketCallback>,
}
impl Socket {
    pub fn new(ws: WebSocket, max_bytes: usize, queue: usize) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(queue);
        let sender = Rc::new(RefCell::new(sender));
        let overflow = Rc::new(Cell::new(false));
        let queued_bytes = Rc::new(Cell::new(0usize));
        let mut callbacks = Vec::new();
        for kind in ["message", "close", "error"] {
            let sender = sender.clone();
            let overflow = overflow.clone();
            let queued_bytes = queued_bytes.clone();
            let socket = ws.clone();
            let callback = Closure::wrap_assert_unwind_safe(Box::new(move |event: JsValue| {
                let value = if kind == "message" {
                    Reflect::get(&event, &"data".into())
                        .ok()
                        .and_then(|data| data.as_string())
                        .filter(|text| text.len() <= max_bytes)
                        .ok_or_else(|| "invalid or oversized GraphQL frame".to_owned())
                } else {
                    Err("socket disconnected".to_owned())
                };
                let mut sender = sender.borrow_mut();
                let bytes = value.as_ref().map_or(0, String::len);
                let total = queued_bytes.get().saturating_add(bytes);
                if total > max_bytes || sender.try_send(value).is_err() {
                    overflow.set(true);
                    sender.close_channel();
                    let _ = socket.close(Some(1013), Some("LIVE_RESET_REQUIRED"));
                } else {
                    queued_bytes.set(total);
                }
            })
                as Box<dyn FnMut(JsValue)>);
            ws.as_ref()
                .add_event_listener_with_callback(kind, callback.as_ref().unchecked_ref())?;
            callbacks.push((kind, callback));
        }
        ws.accept()?;
        Ok(Self {
            ws,
            receiver,
            overflow,
            queued_bytes,
            callbacks,
        })
    }
    pub async fn next(&mut self) -> std::result::Result<serde_json::Value, String> {
        if self.overflow.get() {
            return Err("LIVE_RESET_REQUIRED".into());
        }
        let value = self
            .receiver
            .next()
            .await
            .ok_or_else(|| "socket disconnected".to_owned())?;
        if self.overflow.get() {
            return Err("LIVE_RESET_REQUIRED".into());
        }
        let text = value?;
        self.queued_bytes
            .set(self.queued_bytes.get().saturating_sub(text.len()));
        serde_json::from_str(&text).map_err(|_| "invalid GraphQL frame".to_owned())
    }
    pub fn send(&self, value: &serde_json::Value) -> std::result::Result<(), String> {
        self.ws.send(value).map_err(|_| "socket send failed".into())
    }
}
impl Drop for Socket {
    fn drop(&mut self) {
        for (kind, callback) in self.callbacks.drain(..) {
            let _ = self
                .ws
                .as_ref()
                .remove_event_listener_with_callback(kind, callback.as_ref().unchecked_ref());
        }
        let _ = self.ws.close(Some(1012), Some("gateway connection ended"));
    }
}
