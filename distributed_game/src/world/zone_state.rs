//! Hibernated zone state as a cell: `ZoneState:{zone_id}`.
//!
//! A sleeping zone is a sleeping Durable Object: its last snapshot lives in
//! its own aggregate cell on celld (the in-process reference host in unit
//! tests), written and read by zone servers through
//! `celld::CellZoneStateStore`.
//!
//! | Command | Effect |
//! |---|---|
//! | `zone_state.save` | store the snapshot if `token` >= the stored token |
//! | `zone_state.delete` | drop the snapshot if `token` >= the stored token |
//!
//! Fencing equals the [`ZoneStateStore`](super::ZoneStateStore) contract: an
//! older token is rejected with `stale:{current}`, an equal one is accepted,
//! and the highest token survives a delete.
//!
//! **Size.** A snapshot up to [`ZONE_STATE_INLINE_MAX`] bytes is stored in
//! the cell itself (in the `zone_state.saved` event and the aggregate
//! snapshot). A larger one goes to object storage: the worker route
//! (`celld_worker::fetch`, feature `workers-rs`) writes the bytes to an R2 bucket
//! binding of celld (the fleet bucket; RustFS in the Hops stack) under
//! `zone-state/{zone}/{token}-{sha256}` *before* the command, and the cell
//! records only `{key, sha256, len}`. The blob a save or delete supersedes
//! is deleted after the commit; a save refused as stale deletes its own
//! upload. Loads verify the length and digest.
//!
//! Every save is one small event (inline bytes or a blob reference); cells
//! load from the aggregate snapshot, which is refreshed on every commit.
//!
//! Both commands publish the post-command [`ZoneSnapshotStatus`] (zone,
//! token, whether a snapshot is stored, its content version and size, never
//! the bytes) as `zone_state.saved` / `zone_state.deleted` domain events;
//! the [`zone_status`](super::zone_status) read model projects them, so
//! "is this zone asleep with state?" is answered without opening its cell.
//!
//! Callers must hold the `zone-host` role (an internal identity on the
//! authenticated internal path).

use distributed::cell_host::AggregateCell;
use distributed::command::{typed_command, PreparedCommand, Succeeded};
use distributed::microsvc::{
    CausalCommandContext, CausalRouteDependencies, HandlerError, PortableCommand, Routes,
};
use distributed::{sourced, Entity};
use serde::{Deserialize, Serialize};

pub use super::zone_lease::ZONE_HOST_ROLE;

/// Aggregate type (cell `ZoneState:{zone_id}`).
pub const ZONE_STATE: &str = "ZoneState";
/// Save command.
pub const SAVE: &str = "zone_state.save";
/// Delete command.
pub const DELETE: &str = "zone_state.delete";
/// Rejection prefix of a stale token (`stale:{current}`).
pub const STALE_PREFIX: &str = "stale:";
/// Largest snapshot kept inline in the cell; larger ones go to object
/// storage.
pub const ZONE_STATE_INLINE_MAX: usize = 64 * 1024;

/// A snapshot stored in object storage.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, distributed::CommandInput)]
pub struct BlobRef {
    /// Object key.
    pub key: String,
    /// Lowercase hex SHA-256 of the bytes.
    pub sha256: String,
    /// Length in bytes.
    pub len: u64,
}

/// Where a snapshot's bytes are.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotBytes {
    /// In the cell.
    Inline(Vec<u8>),
    /// In object storage.
    Blob(BlobRef),
}

impl SnapshotBytes {
    fn blob_key(&self) -> Option<&str> {
        match self {
            Self::Blob(blob) => Some(&blob.key),
            Self::Inline(_) => None,
        }
    }
}

/// The hibernated state of one zone.
#[derive(Clone, Debug, Default, Serialize, Deserialize, distributed::Snapshot)]
pub struct ZoneState {
    entity: Entity,
    token: u64,
    content_version: String,
    bytes: Option<SnapshotBytes>,
}

/// The public state of a zone's hibernation slot carried by its outward
/// events: one `zone_snapshots` read-model row (no snapshot bytes).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, distributed::DomainState)]
#[domain_state(version = 1)]
pub struct ZoneSnapshotStatus {
    /// Zone (the cell's stream id).
    pub zone_id: String,
    /// Highest token seen.
    pub token: u64,
    /// A snapshot is stored.
    pub stored: bool,
    /// Content version of the stored snapshot (empty when none).
    pub content_version: String,
    /// Size of the stored snapshot in bytes (0 when none).
    pub bytes: u64,
}

impl From<&ZoneState> for ZoneSnapshotStatus {
    fn from(state: &ZoneState) -> Self {
        let bytes = match &state.bytes {
            Some(SnapshotBytes::Inline(bytes)) => bytes.len() as u64,
            Some(SnapshotBytes::Blob(blob)) => blob.len,
            None => 0,
        };
        Self {
            zone_id: state.entity.id().to_string(),
            token: state.token,
            stored: state.bytes.is_some(),
            content_version: if state.bytes.is_some() {
                state.content_version.clone()
            } else {
                String::new()
            },
            bytes,
        }
    }
}

#[sourced(
    entity,
    aggregate_type = "ZoneState",
    events = "ZoneStateEvent",
    domain_state = ZoneSnapshotStatus
)]
impl ZoneState {
    #[event("zone_state.saved", version = 1, domain)]
    fn saved(
        &mut self,
        zone_id: String,
        token: u64,
        content_version: String,
        bytes: SnapshotBytes,
    ) {
        if self.entity.id().is_empty() {
            self.entity.set_id(zone_id);
        }
        self.token = token;
        self.content_version = content_version;
        self.bytes = Some(bytes);
    }

    #[event("zone_state.deleted", version = 1, domain)]
    fn deleted(&mut self, zone_id: String, token: u64) {
        if self.entity.id().is_empty() {
            self.entity.set_id(zone_id);
        }
        self.token = token;
        self.bytes = None;
    }
}

impl ZoneState {
    /// Highest token seen (kept after a delete).
    pub fn token(&self) -> u64 {
        self.token
    }

    /// Content version of the stored snapshot.
    pub fn content_version(&self) -> &str {
        &self.content_version
    }

    /// The stored snapshot, if any.
    pub fn bytes(&self) -> Option<&SnapshotBytes> {
        self.bytes.as_ref()
    }
}

/// `zone_state.save` input: exactly one of `inline_b64` (standard base64,
/// at most [`ZONE_STATE_INLINE_MAX`] bytes decoded) and `blob`.
#[derive(Debug, Serialize, Deserialize, distributed::CommandInput)]
pub struct SaveInput {
    pub zone_id: String,
    pub token: u64,
    pub content_version: String,
    pub inline_b64: Option<String>,
    pub blob: Option<BlobRef>,
}

/// `zone_state.delete` input.
#[derive(Debug, Serialize, Deserialize, distributed::CommandInput)]
pub struct DeleteInput {
    pub zone_id: String,
    pub token: u64,
}

/// Outcome of a save or delete.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, distributed::CommandOutput)]
pub struct StatePayload {
    pub zone_id: String,
    pub token: u64,
    /// Object key the command superseded (delete it after the commit).
    pub superseded_blob: Option<String>,
}

/// Map a rejected command's message to a stale token, if it is one.
pub fn stale_current(message: &str) -> Option<u64> {
    let message = message.strip_prefix("rejected: ").unwrap_or(message);
    message.strip_prefix(STALE_PREFIX)?.parse().ok()
}

fn rejected(message: impl std::fmt::Display) -> HandlerError {
    HandlerError::Rejected(message.to_string())
}

fn is_zone_host(ctx: &CausalCommandContext<'_, ZoneState>) -> bool {
    ctx.session().has_role(ZONE_HOST_ROLE)
}

async fn fenced(
    ctx: &CausalCommandContext<'_, ZoneState>,
    zone_id: &str,
    token: u64,
) -> Result<distributed::microsvc::AggregateCheckout<ZoneState>, HandlerError> {
    if zone_id.is_empty() {
        return Err(rejected("zone_id must not be empty"));
    }
    let repo = ctx.repo();
    let state = match repo.get(zone_id).await? {
        Some(state) => state,
        None => repo.create(),
    };
    if state.token > token {
        return Err(rejected(format!("{STALE_PREFIX}{}", state.token)));
    }
    Ok(state)
}

async fn handle_save(
    ctx: &CausalCommandContext<'_, ZoneState>,
    input: SaveInput,
) -> Result<PreparedCommand<Succeeded<StatePayload>>, HandlerError> {
    let bytes = match (input.inline_b64, input.blob) {
        (Some(inline), None) => {
            use base64::Engine as _;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(inline)
                .map_err(|_| rejected("inline_b64 is not base64"))?;
            if bytes.len() > ZONE_STATE_INLINE_MAX {
                return Err(rejected(format!(
                    "inline snapshots are at most {ZONE_STATE_INLINE_MAX} bytes"
                )));
            }
            SnapshotBytes::Inline(bytes)
        }
        (None, Some(blob)) => {
            let hex = blob.sha256.len() == 64 && blob.sha256.bytes().all(|b| b.is_ascii_hexdigit());
            if blob.key.is_empty() || !hex {
                return Err(rejected("blob needs a key and a SHA-256"));
            }
            SnapshotBytes::Blob(blob)
        }
        _ => return Err(rejected("exactly one of inline_b64 and blob")),
    };
    let mut state = fenced(ctx, &input.zone_id, input.token).await?;
    let previous = state
        .bytes
        .as_ref()
        .and_then(|b| b.blob_key())
        .map(str::to_string);
    let superseded = previous.filter(|key| Some(key.as_str()) != bytes.blob_key());
    state
        .saved(
            input.zone_id.clone(),
            input.token,
            input.content_version,
            bytes,
        )
        .map_err(rejected)?;
    ctx.repo()
        .publish_events()
        .commit(state)?
        .succeeded(StatePayload {
            zone_id: input.zone_id,
            token: input.token,
            superseded_blob: superseded,
        })
}

async fn handle_delete(
    ctx: &CausalCommandContext<'_, ZoneState>,
    input: DeleteInput,
) -> Result<PreparedCommand<Succeeded<StatePayload>>, HandlerError> {
    let mut state = fenced(ctx, &input.zone_id, input.token).await?;
    let superseded = state
        .bytes
        .as_ref()
        .and_then(|b| b.blob_key())
        .map(str::to_string);
    state
        .deleted(input.zone_id.clone(), input.token)
        .map_err(rejected)?;
    ctx.repo()
        .publish_events()
        .commit(state)?
        .succeeded(StatePayload {
            zone_id: input.zone_id,
            token: input.token,
            superseded_blob: superseded,
        })
}

struct Save;
impl<D> PortableCommand<D> for Save
where
    D: CausalRouteDependencies<Aggregate = ZoneState> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(typed_command::<SaveInput, Succeeded<StatePayload>>(SAVE))
            .guarded(is_zone_host, handle_save)
    }
}

struct Delete;
impl<D> PortableCommand<D> for Delete
where
    D: CausalRouteDependencies<Aggregate = ZoneState> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(typed_command::<DeleteInput, Succeeded<StatePayload>>(
                DELETE,
            ))
            .guarded(is_zone_host, handle_delete)
    }
}

/// Mount both zone-state commands on a `ZoneState` cell.
pub fn mount_zone_state(cell: AggregateCell<ZoneState>) -> AggregateCell<ZoneState> {
    cell.mount(Save).mount(Delete)
}

/// Lowercase hex SHA-256.
pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Object key of a large snapshot.
pub fn blob_key(zone_id: &str, token: u64, sha256: &str) -> String {
    format!("zone-state/{zone_id}/{token}-{sha256}")
}

/// celld worker glue: the `zone-state/{zone}` HTTP surface over a
/// `ZoneState` cell and an R2 bucket binding (see the module docs). The
/// caller authenticates the internal secret first.
#[cfg(all(feature = "workers-rs", target_arch = "wasm32"))]
pub mod celld_worker {
    use super::*;
    use ::worker::{Bucket, Method, Request, Response, Result};
    use base64::Engine as _;
    use distributed::cell_host::{
        CellCommandIdentity, CellDispatchError, CellWaitPathRequest,
        CELL_PRINCIPAL_PARTITION_HEADER, CELL_SERVICE_ID_HEADER,
    };
    use distributed::microsvc::Session;
    use serde_json::{json, Value};

    /// Wire input of a save from `celld::CellZoneStateStore`.
    #[derive(Deserialize)]
    struct WireSave {
        zone_id: String,
        token: u64,
        content_version: String,
        state_b64: String,
    }

    fn json_status(body: Value, status: u16) -> Result<Response> {
        Ok(Response::from_json(&body)?.with_status(status))
    }

    fn error_response(error: CellDispatchError) -> Result<Response> {
        json_status(
            json!({ "error": error.client_message(), "code": error.code() }),
            error.status_code(),
        )
    }

    /// The R2 bucket binding `name`. celld's R2 binding is not an instance
    /// of the Workers runtime's `R2Bucket` class, which `Env::bucket`
    /// checks for, but it has the same methods; take it unchecked.
    pub fn bucket(env: &::worker::Env, name: &str) -> Result<Bucket> {
        use ::worker::wasm_bindgen::{JsCast, JsValue};
        let env: &JsValue = env.as_ref();
        let binding = ::worker::js_sys::Reflect::get(env, &JsValue::from_str(name))
            .map_err(|_| ::worker::Error::RustError(format!("no binding `{name}`")))?;
        if binding.is_undefined() {
            return Err(::worker::Error::RustError(format!(
                "R2 bucket binding `{name}` is not configured"
            )));
        }
        Ok(binding.unchecked_into::<Bucket>())
    }

    /// `GET /zone-state/{zone}` → `{state: null | {token, content_version,
    /// state_b64}}`; `POST /zone-state/{zone}/zone_state.save|delete`.
    pub async fn fetch(
        cell: &AggregateCell<ZoneState>,
        bucket: &Bucket,
        mut req: Request,
    ) -> Result<Response> {
        let url = req.url()?;
        let command = url
            .path()
            .split('/')
            .filter(|part| !part.is_empty())
            .nth(2)
            .map(str::to_string);
        match (req.method(), command.as_deref()) {
            (Method::Get, None) => load(cell, bucket).await,
            (Method::Post, Some(SAVE)) | (Method::Post, Some(DELETE)) => {
                let command = command.unwrap();
                let body: Value = match req.json().await {
                    Ok(body) => body,
                    Err(_) => {
                        return error_response(CellDispatchError::BadRequest("invalid JSON".into()))
                    }
                };
                let parsed = match CellWaitPathRequest::parse(body) {
                    Ok(parsed) => parsed,
                    Err(error) => return error_response(CellDispatchError::BadRequest(error)),
                };
                let header = |name: &str| req.headers().get(name).ok().flatten();
                let identity = match (
                    header(CELL_SERVICE_ID_HEADER),
                    header(CELL_PRINCIPAL_PARTITION_HEADER),
                ) {
                    (Some(service), Some(partition)) => {
                        match CellCommandIdentity::new(service, partition, &parsed.command_id) {
                            Ok(identity) => identity,
                            Err(error) => return error_response(error),
                        }
                    }
                    _ => return error_response(CellDispatchError::Unauthorized),
                };
                let mut session = Session::new();
                for key in [distributed::USER_ID_KEY, distributed::microsvc::ROLE_KEY] {
                    if let Some(value) = header(key) {
                        session.set(key, value);
                    }
                }
                if command == SAVE {
                    save(cell, bucket, &identity, parsed.input, session).await
                } else {
                    dispatch(cell, bucket, DELETE, &identity, parsed.input, session, None).await
                }
            }
            _ => json_status(json!({ "error": "not found" }), 404),
        }
    }

    async fn load(cell: &AggregateCell<ZoneState>, bucket: &Bucket) -> Result<Response> {
        let state = cell
            .load()
            .await
            .map_err(|error| ::worker::Error::RustError(error.to_string()))?;
        let Some(state) = state else {
            return json_status(json!({ "state": null }), 200);
        };
        let bytes = match state.bytes() {
            None => return json_status(json!({ "state": null, "token": state.token() }), 200),
            Some(SnapshotBytes::Inline(bytes)) => bytes.clone(),
            Some(SnapshotBytes::Blob(blob)) => {
                let object = bucket.get(blob.key.clone()).execute().await?;
                let Some(body) = object.as_ref().and_then(|object| object.body()) else {
                    return error_response(CellDispatchError::Internal(format!(
                        "zone-state blob {} is missing",
                        blob.key
                    )));
                };
                let bytes = body.bytes().await?;
                if bytes.len() as u64 != blob.len || sha256_hex(&bytes) != blob.sha256 {
                    return error_response(CellDispatchError::Internal(format!(
                        "zone-state blob {} does not match its digest",
                        blob.key
                    )));
                }
                bytes
            }
        };
        json_status(
            json!({ "state": {
                "token": state.token(),
                "content_version": state.content_version(),
                "state_b64": base64::engine::general_purpose::STANDARD.encode(bytes),
            }}),
            200,
        )
    }

    async fn save(
        cell: &AggregateCell<ZoneState>,
        bucket: &Bucket,
        identity: &CellCommandIdentity,
        input: Value,
        session: Session,
    ) -> Result<Response> {
        let wire: WireSave = match serde_json::from_value(input) {
            Ok(wire) => wire,
            Err(error) => {
                return error_response(CellDispatchError::BadRequest(format!(
                    "zone_state.save input: {error}"
                )))
            }
        };
        let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(&wire.state_b64) else {
            return error_response(CellDispatchError::BadRequest(
                "state_b64 is not base64".into(),
            ));
        };
        let (command_input, uploaded) = if bytes.len() <= ZONE_STATE_INLINE_MAX {
            (
                json!({
                    "zone_id": wire.zone_id, "token": wire.token,
                    "content_version": wire.content_version,
                    "inline_b64": wire.state_b64, "blob": null,
                }),
                None,
            )
        } else {
            let sha256 = sha256_hex(&bytes);
            let key = blob_key(&wire.zone_id, wire.token, &sha256);
            let len = bytes.len() as u64;
            bucket.put(key.clone(), bytes).execute().await?;
            (
                json!({
                    "zone_id": wire.zone_id, "token": wire.token,
                    "content_version": wire.content_version, "inline_b64": null,
                    "blob": { "key": key, "sha256": sha256, "len": len },
                }),
                Some(key),
            )
        };
        dispatch(
            cell,
            bucket,
            SAVE,
            identity,
            command_input,
            session,
            uploaded,
        )
        .await
    }

    async fn dispatch(
        cell: &AggregateCell<ZoneState>,
        bucket: &Bucket,
        command: &str,
        identity: &CellCommandIdentity,
        input: Value,
        session: Session,
        uploaded: Option<String>,
    ) -> Result<Response> {
        match cell
            .dispatch_idempotent(command, identity, input, session)
            .await
        {
            Ok(dispatch) => {
                let superseded = dispatch
                    .payload()
                    .get("superseded_blob")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                if let Some(key) = superseded {
                    if let Err(error) = bucket.delete(key.clone()).await {
                        ::worker::console_error!("superseded zone-state blob {key}: {error}");
                    }
                }
                json_status(dispatch.wait_path_wire(), 200)
            }
            Err(error) => {
                if let CellDispatchError::Internal(message) = &error {
                    ::worker::console_error!("zone-state {command}: {message}");
                }
                // Refused (stale or invalid): the upload is nobody's.
                if let Some(key) = uploaded {
                    if let Err(error) = bucket.delete(key.clone()).await {
                        ::worker::console_error!("refused zone-state blob {key}: {error}");
                    }
                }
                error_response(error)
            }
        }
    }
}
