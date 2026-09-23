use futures_channel::mpsc;
use futures_util::StreamExt;
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};
use worker::{
    js_sys::{ArrayBuffer, Reflect, Uint8Array},
    wasm_bindgen::{closure::Closure, JsCast, JsValue},
    Result, WebSocket,
};

type SocketCallback = (&'static str, Closure<dyn FnMut(JsValue)>);
pub(super) enum Frame {
    Text(String),
    Bytes(Vec<u8>),
    Close(u16, String),
}
impl Frame {
    fn size(&self) -> usize {
        match self {
            Self::Text(s) => s.len(),
            Self::Bytes(b) => b.len(),
            Self::Close(_, s) => s.len(),
        }
    }
    pub fn send(&self, socket: &WebSocket) -> Result<()> {
        match self {
            Self::Text(s) => socket.send_with_str(s),
            Self::Bytes(b) => socket.send_with_bytes(b),
            Self::Close(code, reason) => socket.close(Some(*code), Some(reason)),
        }
    }
}
/// UI proxy callbacks have both frame-count and aggregate-byte bounds.
pub(super) struct RawSocket {
    pub ws: WebSocket,
    receiver: mpsc::Receiver<Option<Frame>>,
    bytes: Rc<Cell<usize>>,
    overflow: Rc<Cell<bool>>,
    callbacks: Vec<SocketCallback>,
}
impl RawSocket {
    pub fn new(ws: WebSocket, max: usize) -> Result<Self> {
        Reflect::set(ws.as_ref(), &"binaryType".into(), &"arraybuffer".into())?;
        let (sender, receiver) = mpsc::channel(16);
        let sender = Rc::new(RefCell::new(sender));
        let bytes = Rc::new(Cell::new(0usize));
        let overflow = Rc::new(Cell::new(false));
        let mut callbacks = Vec::new();
        for kind in ["message", "close", "error"] {
            let sender = sender.clone();
            let bytes = bytes.clone();
            let overflow = overflow.clone();
            let socket = ws.clone();
            let callback = Closure::wrap_assert_unwind_safe(Box::new(move |event: JsValue| {
                let frame = match kind {
                    "message" => Reflect::get(&event, &"data".into()).ok().and_then(|data| {
                        if let Some(text) = data.as_string() {
                            Some(Frame::Text(text))
                        } else if data.is_instance_of::<ArrayBuffer>() {
                            Some(Frame::Bytes(Uint8Array::new(&data).to_vec()))
                        } else {
                            None
                        }
                    }),
                    "close" => {
                        let code = Reflect::get(&event, &"code".into())
                            .ok()
                            .and_then(|c| c.as_f64())
                            .unwrap_or(1012.0) as u16;
                        let reason = Reflect::get(&event, &"reason".into())
                            .ok()
                            .and_then(|c| c.as_string())
                            .unwrap_or_default();
                        Some(Frame::Close(
                            if matches!(code, 1005 | 1006 | 1015) {
                                1012
                            } else {
                                code
                            },
                            reason,
                        ))
                    }
                    _ => None,
                };
                let total = bytes
                    .get()
                    .saturating_add(frame.as_ref().map_or(0, Frame::size));
                let mut sender = sender.borrow_mut();
                if total > max || sender.try_send(frame).is_err() {
                    overflow.set(true);
                    sender.close_channel();
                    let _ = socket.close(Some(1013), Some("proxy queue limit"));
                } else {
                    bytes.set(total);
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
            bytes,
            overflow,
            callbacks,
        })
    }
    async fn next(&mut self) -> Option<Frame> {
        if self.overflow.get() {
            return None;
        }
        let frame = self.receiver.next().await??;
        self.bytes
            .set(self.bytes.get().saturating_sub(frame.size()));
        if self.overflow.get() {
            None
        } else {
            Some(frame)
        }
    }
}
impl Drop for RawSocket {
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
pub(super) async fn bridge(client: WebSocket, origin: WebSocket, max: usize) -> Result<()> {
    let mut client = RawSocket::new(client, max)?;
    let mut origin = RawSocket::new(origin, max)?;
    let mut delivered = 0usize;
    loop {
        let (frame, from_client) =
            match futures_util::future::select(Box::pin(client.next()), Box::pin(origin.next()))
                .await
            {
                futures_util::future::Either::Left((frame, _)) => (frame, true),
                futures_util::future::Either::Right((frame, _)) => (frame, false),
            };
        let Some(frame) = frame else {
            return Ok(());
        };
        delivered = delivered.saturating_add(frame.size());
        // workerd exposes no bufferedAmount or send backpressure for arbitrary
        // UI protocols. Bound cumulative delivery, then require reconnect.
        if delivered > max.saturating_mul(8) {
            let _ = client.ws.close(Some(1013), Some("proxy delivery limit"));
            let _ = origin.ws.close(Some(1013), Some("proxy delivery limit"));
            return Ok(());
        }
        frame.send(if from_client { &origin.ws } else { &client.ws })?;
        if matches!(frame, Frame::Close(..)) {
            return Ok(());
        }
    }
}
