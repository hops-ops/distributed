//! Portable GraphQL-over-WebSocket frame codec. Every adapter's client and
//! origin loops decode and encode frames here; adapters own the sockets.
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// Most operations open at once on one connection.
pub const MAX_OPERATIONS: usize = 128;
/// Longest accepted operation id, in bytes.
pub const MAX_OPERATION_ID_BYTES: usize = 256;
/// Largest `connection_init` text read before the origin admits it.
pub const MAX_INIT_BYTES: usize = 64 * 1024;

/// Negotiated GraphQL WebSocket subprotocol.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WsProtocol {
    /// `graphql-transport-ws` (graphql-ws library protocol).
    TransportWs,
    /// Legacy `graphql-ws` (subscriptions-transport-ws protocol).
    LegacyWs,
}

/// A client frame that ends the connection: malformed, unknown for the
/// protocol, an invalid or duplicate id, or too many open operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProtocolViolation;

/// One decoded client-to-server frame.
#[derive(Clone, Debug, PartialEq)]
pub enum ClientFrame {
    /// `connection_init` with its payload (`{}` when absent).
    Init(Value),
    /// Operation start (`subscribe`, or legacy `start`).
    Subscribe {
        /// Validated operation id.
        id: String,
        /// GraphQL request.
        payload: Value,
    },
    /// Operation stop (`complete`, or legacy `stop`).
    Complete {
        /// Operation id.
        id: String,
    },
    /// `ping` (transport-ws only).
    Ping(Value),
    /// `pong` (transport-ws only).
    Pong(Value),
    /// `connection_terminate` (legacy only).
    Terminate,
}

/// One decoded server-to-client frame, as an origin sends it.
#[derive(Clone, Debug, PartialEq)]
pub enum ServerFrame {
    /// `connection_ack`.
    Ack,
    /// `connection_error`: the origin refused `connection_init`.
    ConnectionError,
    /// Operation result (`next`, or legacy `data`).
    Next {
        /// Operation id.
        id: String,
        /// Execution result.
        payload: Value,
    },
    /// Operation completed.
    Complete {
        /// Operation id.
        id: String,
    },
    /// Operation failed.
    Error {
        /// Operation id, when the origin supplied one.
        id: Option<String>,
    },
    /// `ping` with its payload.
    Ping(Value),
    /// `pong`, or legacy keep-alive `ka`.
    Pong,
    /// Any other frame.
    Other,
}

impl WsProtocol {
    /// Pick a protocol from a `Sec-WebSocket-Protocol` offer, preferring
    /// `graphql-transport-ws`.
    pub fn negotiate(offered: &str) -> Option<Self> {
        let offers = || offered.split(',').map(str::trim);
        if offers().any(|p| p == "graphql-transport-ws") {
            Some(Self::TransportWs)
        } else if offers().any(|p| p == "graphql-ws") {
            Some(Self::LegacyWs)
        } else {
            None
        }
    }
    /// Subprotocol name for the upgrade response.
    pub fn name(self) -> &'static str {
        match self {
            Self::TransportWs => "graphql-transport-ws",
            Self::LegacyWs => "graphql-ws",
        }
    }
    fn next_type(self) -> &'static str {
        match self {
            Self::TransportWs => "next",
            Self::LegacyWs => "data",
        }
    }
    /// Decode one client frame for this protocol.
    pub fn decode_client(self, mut frame: Value) -> Result<ClientFrame, ProtocolViolation> {
        let payload = frame.get_mut("payload").map(Value::take);
        let id = || {
            frame["id"]
                .as_str()
                .filter(|id| !id.is_empty() && id.len() <= MAX_OPERATION_ID_BYTES)
                .map(str::to_owned)
                .ok_or(ProtocolViolation)
        };
        let legacy = self == Self::LegacyWs;
        Ok(match frame["type"].as_str().ok_or(ProtocolViolation)? {
            "connection_init" => ClientFrame::Init(payload.unwrap_or_else(|| json!({}))),
            "subscribe" if !legacy => ClientFrame::Subscribe {
                id: id()?,
                payload: payload.ok_or(ProtocolViolation)?,
            },
            "start" if legacy => ClientFrame::Subscribe {
                id: id()?,
                payload: payload.ok_or(ProtocolViolation)?,
            },
            "complete" if !legacy => ClientFrame::Complete { id: id()? },
            "stop" if legacy => ClientFrame::Complete { id: id()? },
            "ping" if !legacy => ClientFrame::Ping(payload.unwrap_or(Value::Null)),
            "pong" if !legacy => ClientFrame::Pong(payload.unwrap_or(Value::Null)),
            "connection_terminate" if legacy => ClientFrame::Terminate,
            _ => return Err(ProtocolViolation),
        })
    }
    /// Decode a client's text frame; JSON errors are violations.
    pub fn decode_client_text(self, text: &str) -> Result<ClientFrame, ProtocolViolation> {
        self.decode_client(serde_json::from_str(text).map_err(|_| ProtocolViolation)?)
    }
    /// Encode a client frame for an origin speaking this protocol.
    pub fn encode_client(self, frame: &ClientFrame) -> Value {
        let legacy = self == Self::LegacyWs;
        match frame {
            ClientFrame::Init(payload) => json!({"type":"connection_init","payload":payload}),
            ClientFrame::Subscribe { id, payload } => {
                json!({"id":id,"type":if legacy {"start"} else {"subscribe"},"payload":payload})
            }
            ClientFrame::Complete { id } => {
                json!({"id":id,"type":if legacy {"stop"} else {"complete"}})
            }
            ClientFrame::Ping(payload) => ping(payload.clone()),
            ClientFrame::Pong(payload) => pong(payload.clone()),
            ClientFrame::Terminate => json!({"type":"connection_terminate"}),
        }
    }
    /// Operation result frame.
    pub fn next(self, id: &str, payload: Value) -> Value {
        json!({"id":id,"type":self.next_type(),"payload":payload})
    }
}

/// Decode one origin frame; both protocols share these shapes.
pub fn decode_server(mut frame: Value) -> ServerFrame {
    let id = frame["id"].as_str().map(str::to_owned);
    let payload = frame.get_mut("payload").map(Value::take);
    match (frame["type"].as_str(), id) {
        (Some("connection_ack"), _) => ServerFrame::Ack,
        (Some("connection_error"), _) => ServerFrame::ConnectionError,
        (Some("next" | "data"), Some(id)) => ServerFrame::Next {
            id,
            payload: payload.unwrap_or(Value::Null),
        },
        (Some("complete"), Some(id)) => ServerFrame::Complete { id },
        (Some("error"), id) => ServerFrame::Error { id },
        (Some("ping"), _) => ServerFrame::Ping(payload.unwrap_or(Value::Null)),
        (Some("pong" | "ka"), _) => ServerFrame::Pong,
        _ => ServerFrame::Other,
    }
}

/// The operation a server frame ends, when it is `complete` or `error`.
pub fn ended_operation(frame: &Value) -> Option<&str> {
    match frame["type"].as_str() {
        Some("complete" | "error") => frame["id"].as_str(),
        _ => None,
    }
}

/// Operation completed.
pub fn complete(id: &str) -> Value {
    json!({"id":id,"type":"complete"})
}
/// Operation failed with a gateway code (`LIVE_RESET_REQUIRED`, ...).
pub fn error(id: &str, code: &str) -> Value {
    json!({"id":id,"type":"error","payload":[{"message":code,"extensions":{"code":code}}]})
}
/// Reply to a `ping`, echoing its payload (none when it had none).
pub fn pong(payload: Value) -> Value {
    with_payload("pong", payload)
}
/// A `ping`; a null payload is omitted.
pub fn ping(payload: Value) -> Value {
    with_payload("ping", payload)
}
fn with_payload(kind: &str, payload: Value) -> Value {
    if payload.is_null() {
        json!({ "type": kind })
    } else {
        json!({ "type": kind, "payload": payload })
    }
}
/// Accept `connection_init`.
pub fn connection_ack() -> Value {
    json!({"type":"connection_ack"})
}

/// Open operations of one connection, bounded by [`MAX_OPERATIONS`], each
/// with adapter-owned state (a task handle, an acknowledgement channel).
pub struct Operations<T> {
    open: BTreeMap<String, T>,
}
impl<T> Default for Operations<T> {
    fn default() -> Self {
        Self {
            open: BTreeMap::new(),
        }
    }
}
impl<T> Operations<T> {
    /// Whether another operation with this id may open. A duplicate id or
    /// one over the bound is a violation.
    pub fn admit(&self, id: &str) -> Result<(), ProtocolViolation> {
        if self.open.len() >= MAX_OPERATIONS || self.open.contains_key(id) {
            Err(ProtocolViolation)
        } else {
            Ok(())
        }
    }
    /// Record an admitted operation.
    pub fn open(&mut self, id: String, state: T) -> Result<(), ProtocolViolation> {
        self.admit(&id)?;
        self.open.insert(id, state);
        Ok(())
    }
    /// Forget an operation, returning its state.
    pub fn close(&mut self, id: &str) -> Option<T> {
        self.open.remove(id)
    }
    /// An open operation's state.
    pub fn get_mut(&mut self, id: &str) -> Option<&mut T> {
        self.open.get_mut(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negotiation_prefers_transport_ws() {
        assert_eq!(
            WsProtocol::negotiate("graphql-ws, graphql-transport-ws"),
            Some(WsProtocol::TransportWs)
        );
        assert_eq!(
            WsProtocol::negotiate(" graphql-ws"),
            Some(WsProtocol::LegacyWs)
        );
        assert_eq!(WsProtocol::negotiate("graphql-wsx"), None);
    }

    #[test]
    fn each_protocol_accepts_only_its_own_frame_names() {
        let modern = WsProtocol::TransportWs;
        let legacy = WsProtocol::LegacyWs;
        let start = json!({"id":"1","type":"start","payload":{"query":"{a}"}});
        let subscribe = json!({"id":"1","type":"subscribe","payload":{"query":"{a}"}});
        assert_eq!(modern.decode_client(start.clone()), Err(ProtocolViolation));
        assert_eq!(
            legacy.decode_client(start),
            Ok(ClientFrame::Subscribe {
                id: "1".into(),
                payload: json!({"query":"{a}"})
            })
        );
        assert!(modern.decode_client(subscribe.clone()).is_ok());
        assert_eq!(legacy.decode_client(subscribe), Err(ProtocolViolation));
        assert_eq!(
            legacy.decode_client(json!({"type":"ping"})),
            Err(ProtocolViolation)
        );
        assert_eq!(
            modern.decode_client(json!({"type":"connection_terminate"})),
            Err(ProtocolViolation)
        );
        assert_eq!(
            legacy.decode_client(json!({"type":"connection_terminate"})),
            Ok(ClientFrame::Terminate)
        );
        assert_eq!(
            modern.decode_client(json!({"type":"connection_init"})),
            Ok(ClientFrame::Init(json!({})))
        );
        assert_eq!(
            modern.decode_client_text("not json"),
            Err(ProtocolViolation)
        );
        assert_eq!(
            modern.decode_client(json!({"type":"surprise"})),
            Err(ProtocolViolation)
        );
    }

    #[test]
    fn operation_ids_must_be_present_bounded_and_unique() {
        let modern = WsProtocol::TransportWs;
        for id in [json!(""), json!(7), json!("x".repeat(257)), Value::Null] {
            let frame = json!({"id":id,"type":"subscribe","payload":{}});
            assert_eq!(modern.decode_client(frame), Err(ProtocolViolation));
        }
        let mut operations = Operations::default();
        operations.open("a".into(), ()).unwrap();
        assert_eq!(operations.open("a".into(), ()), Err(ProtocolViolation));
        for i in 1..MAX_OPERATIONS {
            operations.open(i.to_string(), ()).unwrap();
        }
        assert_eq!(operations.admit("over"), Err(ProtocolViolation));
        assert!(operations.close("a").is_some());
        assert!(operations.admit("over").is_ok());
    }

    #[test]
    fn frames_round_trip_between_client_and_origin_shapes() {
        let legacy = WsProtocol::LegacyWs;
        assert_eq!(
            legacy.next("1", json!({"data":1})),
            json!({"id":"1","type":"data","payload":{"data":1}})
        );
        assert_eq!(
            decode_server(legacy.next("1", json!(2))),
            ServerFrame::Next {
                id: "1".into(),
                payload: json!(2)
            }
        );
        let stop = ClientFrame::Complete { id: "1".into() };
        assert_eq!(legacy.decode_client(legacy.encode_client(&stop)), Ok(stop));
        assert_eq!(
            decode_server(complete("1")),
            ServerFrame::Complete { id: "1".into() }
        );
        assert_eq!(
            decode_server(error("1", "LIVE_RESET_REQUIRED")),
            ServerFrame::Error {
                id: Some("1".into())
            }
        );
        assert_eq!(decode_server(json!({"type":"ka"})), ServerFrame::Pong);
        assert_eq!(ended_operation(&complete("1")), Some("1"));
        assert_eq!(ended_operation(&error("2", "AUTH_EXPIRED")), Some("2"));
        assert_eq!(ended_operation(&legacy.next("3", json!(null))), None);
        assert_eq!(decode_server(connection_ack()), ServerFrame::Ack);
        assert_eq!(decode_server(pong(json!(1))), ServerFrame::Pong);
        assert_eq!(decode_server(ping(json!(1))), ServerFrame::Ping(json!(1)));
        assert_eq!(pong(Value::Null), json!({"type":"pong"}));
        let ping_frame = WsProtocol::TransportWs
            .decode_client(json!({"type":"ping"}))
            .unwrap();
        assert_eq!(
            WsProtocol::TransportWs.encode_client(&ping_frame),
            json!({"type":"ping"})
        );
    }
}
