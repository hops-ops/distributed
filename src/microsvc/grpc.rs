//! gRPC transport for microsvc — maps gRPC requests to command dispatch.
//!
//! Requires the `grpc` feature. Uses tonic for the gRPC server and prost
//! for message serialization (standard protobuf wire format, no `.proto` file).
//!
//! ## RPCs
//!
//! - `Dispatch` — dispatch a typed command. Input = `GrpcRequest` whose `input`
//!   is the JSON `{ "commandId", "input" }`, output = `GrpcResponse`. An input
//!   without a `commandId` is a 400.
//! - `Health` — health check returning available commands.
//!
//! ## Example
//!
//! ```ignore
//! use std::sync::Arc;
//! use distributed::{microsvc, InMemoryRepository};
//!
//! let service = Arc::new(
//!     microsvc::Service::new().named("counters").routes(
//!         microsvc::Routes::new()
//!             .with_repo(InMemoryRepository::new().queued().aggregate::<Counter>())
//!             .typed_command(counter_create::command())
//!             .handle(counter_create::handle)
//!     )
//! );
//!
//! // Get the server to compose with other tonic routes
//! let grpc_svc = microsvc::grpc_server(service.clone());
//!
//! // Or serve directly
//! microsvc::serve_grpc(service, "[::1]:50051").await?;
//! ```

use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::net::{AddrParseError, SocketAddr};
use std::sync::Arc;

use serde_json::json;
use tonic::{Request, Response, Status};

use super::service::Service;
use super::session::Session;

// ---------------------------------------------------------------------------
// Message types (prost — standard protobuf wire format)
// ---------------------------------------------------------------------------

#[derive(Clone, prost::Message)]
pub struct GrpcRequest {
    #[prost(string, tag = "1")]
    pub command: String,
    #[prost(string, tag = "2")]
    pub input: String, // JSON string
    #[prost(map = "string, string", tag = "3")]
    pub session_variables: HashMap<String, String>,
}

#[derive(Clone, prost::Message)]
pub struct GrpcResponse {
    #[prost(uint32, tag = "1")]
    pub status: u32,
    #[prost(string, tag = "2")]
    pub body: String, // JSON string
}

#[derive(Clone, prost::Message)]
pub struct HealthRequest {}

#[derive(Clone, prost::Message)]
pub struct HealthResponse {
    #[prost(bool, tag = "1")]
    pub ok: bool,
    #[prost(string, repeated, tag = "2")]
    pub commands: Vec<String>,
}

// ---------------------------------------------------------------------------
// Generated service trait + server/client
// ---------------------------------------------------------------------------

include!(concat!(
    env!("OUT_DIR"),
    "/sourced.microsvc.CommandService.rs"
));

pub use command_service_client::CommandServiceClient;
pub use command_service_server::{CommandService, CommandServiceServer};

/// Error returned when serving the gRPC transport fails.
#[derive(Debug)]
pub enum GrpcServeError {
    /// The supplied bind address could not be parsed as a socket address.
    InvalidAddress {
        /// Original bind address string supplied by the caller.
        addr: String,
        /// Address parser error.
        source: AddrParseError,
    },
    /// The tonic transport server failed while serving.
    Transport(tonic::transport::Error),
}

impl fmt::Display for GrpcServeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GrpcServeError::InvalidAddress { addr, source } => {
                write!(f, "invalid gRPC bind address `{addr}`: {source}")
            }
            GrpcServeError::Transport(source) => write!(f, "gRPC transport error: {source}"),
        }
    }
}

impl Error for GrpcServeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            GrpcServeError::InvalidAddress { source, .. } => Some(source),
            GrpcServeError::Transport(source) => Some(source),
        }
    }
}

impl From<tonic::transport::Error> for GrpcServeError {
    fn from(source: tonic::transport::Error) -> Self {
        GrpcServeError::Transport(source)
    }
}

// ---------------------------------------------------------------------------
// Handler implementation
// ---------------------------------------------------------------------------

/// gRPC handler that wraps a `Service` and implements the generated
/// `CommandService` trait. Mirrors the HTTP transport pattern.
pub struct GrpcHandler {
    service: Arc<Service>,
}

impl GrpcHandler {
    pub fn new(service: Arc<Service>) -> Self {
        Self { service }
    }
}

#[tonic::async_trait]
impl CommandService for GrpcHandler {
    async fn dispatch(
        &self,
        request: Request<GrpcRequest>,
    ) -> Result<Response<GrpcResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();

        // Parse JSON input (strict; invalid payloads return 400)
        let input: serde_json::Value = match serde_json::from_str(&req.input) {
            Ok(value) => value,
            Err(e) => {
                return Ok(Response::new(GrpcResponse {
                    status: 400,
                    body: json!({ "error": format!("invalid JSON input: {e}") }).to_string(),
                }));
            }
        };

        // Build session: payload values first, then transport metadata wins.
        // Transport metadata is trusted (injected by a trusted proxy); the
        // payload is client-controlled and MUST NOT override it. See
        // [`build_session`] and the `Session` trust-boundary docs.
        let session = build_session(&metadata, req.session_variables);

        let (command_id, input) = match super::wait_path::parse_wait_path_body(&input) {
            Ok(wait_path) => wait_path,
            Err(error) => {
                return Ok(Response::new(GrpcResponse {
                    status: 400,
                    body: json!({ "code": "BAD_REQUEST", "error": error }).to_string(),
                }));
            }
        };
        match super::wait_path::dispatch_wait_path(
            self.service.as_ref(),
            &req.command,
            &command_id,
            input,
            session,
        )
        .await
        {
            Ok(result) => Ok(Response::new(GrpcResponse {
                status: 200,
                body: super::wait_path::wait_path_response(&result).to_string(),
            })),
            Err(e) => {
                // Mirror the HTTP transport: the error body never carries
                // internal error detail (SQL/driver text).
                let status = e.status_code();
                if status >= 500 {
                    tracing::error!(command = %req.command, error = %e, "microsvc command failed");
                }
                Ok(Response::new(GrpcResponse {
                    status: status as u32,
                    body: super::wait_path::wait_path_error_response(&e).to_string(),
                }))
            }
        }
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let commands: Vec<String> = self
            .service
            .command_names()
            .into_iter()
            .map(|s| s.to_string())
            .collect();

        Ok(Response::new(HealthResponse { ok: true, commands }))
    }
}

// ---------------------------------------------------------------------------
// Session building (same pattern as HTTP transport)
// ---------------------------------------------------------------------------

/// Build a session from gRPC metadata and payload session variables.
///
/// **Trust boundary (security-critical):** transport metadata is TRUSTED —
/// it is injected by a trusted proxy/gateway that authenticates the caller
/// and strips any client-supplied identity headers. The request payload
/// `session_variables` are CLIENT-CONTROLLED and therefore UNTRUSTED.
///
/// Precedence: **metadata wins.** Payload values are applied first, then
/// metadata overwrites any colliding key. This prevents a client from
/// spoofing identity via the request body when behind a trusted gateway.
/// See the [`Session`] docs for the framework-wide trust model.
///
/// Payload-only keys (not present in metadata) still pass through. That
/// supports gateway action/webhook shapes where verified claims arrive only
/// in the body (e.g. a query-layer action payload) and no transport metadata
/// is injected.
fn build_session(
    metadata: &tonic::metadata::MetadataMap,
    payload_vars: HashMap<String, String>,
) -> Session {
    let mut vars = HashMap::new();

    // Untrusted payload first.
    for (k, v) in payload_vars {
        vars.insert(k, v);
    }

    // Trusted metadata wins — overwrites any payload-supplied key.
    for kv in metadata.iter() {
        if let tonic::metadata::KeyAndValueRef::Ascii(key, value) = kv {
            if let Ok(v) = value.to_str() {
                vars.insert(key.as_str().to_string(), v.to_string());
            }
        }
    }

    Session::from_map(vars)
}

// ---------------------------------------------------------------------------
// Convenience constructors
// ---------------------------------------------------------------------------

/// Create a `CommandServiceServer` from a shared `Service`.
pub fn grpc_server(service: Arc<Service>) -> CommandServiceServer<GrpcHandler> {
    CommandServiceServer::new(GrpcHandler::new(service))
}

/// Bind and serve the gRPC transport at the given address (e.g. `"[::1]:50051"`).
///
/// Returns [`GrpcServeError::InvalidAddress`] when `addr` is not a valid socket
/// address, and [`GrpcServeError::Transport`] for errors returned by tonic while
/// serving.
pub async fn serve_grpc(service: Arc<Service>, addr: &str) -> Result<(), GrpcServeError> {
    let addr: SocketAddr = addr
        .parse()
        .map_err(|source| GrpcServeError::InvalidAddress {
            addr: addr.to_string(),
            source,
        })?;
    tonic::transport::Server::builder()
        .add_service(grpc_server(service))
        .serve(addr)
        .await?;
    Ok(())
}
