//! `ZoneState` cells on the in-process reference host: the zone-state
//! fencing contract (`state_store_conformance`) through the cell's
//! commands, for inline snapshots and for snapshots in object storage (an
//! in-memory blob map standing in for celld's R2 binding, split exactly as
//! the worker route splits them), plus the command's own guards.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use base64::Engine as _;
use distributed::cell_host::{
    AggregateCell, CellCommandIdentity, CellDispatchError, CellStreamStore,
};
use distributed::microsvc::{Session, ROLE_KEY, USER_ID_KEY};
use distributed::StreamIdentity;
use distributed_game::cell::command_id_for;
use distributed_game::world::zone_state::{
    blob_key, mount_zone_state, sha256_hex, stale_current, SnapshotBytes, ZoneState, DELETE, SAVE,
    ZONE_HOST_ROLE, ZONE_STATE_INLINE_MAX,
};
use distributed_game::world::{
    state_store_conformance, StateError, StoredZoneState, ZoneStateStore,
};
use distributed_game::BoxFuture;
use serde_json::{json, Value};

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

fn host_session() -> Session {
    let mut session = Session::new();
    session.set(USER_ID_KEY, "zone-server-a");
    session.set(ROLE_KEY, ZONE_HOST_ROLE);
    session
}

/// `ZoneStateStore` over one in-process cell + a blob map, as
/// `CellZoneStateStore` + the worker route are over celld.
struct InProcess {
    cell: AggregateCell<ZoneState>,
    blobs: Mutex<HashMap<String, Vec<u8>>>,
    calls: AtomicU64,
}

impl InProcess {
    fn new(zone: &str) -> Self {
        let store = CellStreamStore::for_identity(StreamIdentity::new("ZoneState", zone).unwrap());
        Self {
            cell: mount_zone_state(AggregateCell::from_store(store).unwrap()),
            blobs: Mutex::default(),
            calls: AtomicU64::new(0),
        }
    }

    async fn dispatch(
        &self,
        command: &str,
        input: Value,
        session: Session,
    ) -> Result<Value, CellDispatchError> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        let identity = CellCommandIdentity::new(
            "zone-host",
            "zone-server-a",
            command_id_for("t", &n.to_string()),
        )
        .unwrap();
        self.cell
            .dispatch_idempotent(command, &identity, input, session)
            .await
            .map(|result| result.payload().clone())
    }

    async fn command(
        &self,
        command: &str,
        input: Value,
        uploaded: Option<String>,
    ) -> Result<(), StateError> {
        let token = input["token"].as_u64().unwrap();
        match self.dispatch(command, input, host_session()).await {
            Ok(payload) => {
                if let Some(key) = payload["superseded_blob"].as_str() {
                    self.blobs.lock().unwrap().remove(key);
                }
                Ok(())
            }
            Err(error) => {
                if let Some(key) = uploaded {
                    self.blobs.lock().unwrap().remove(&key);
                }
                match error {
                    CellDispatchError::Rejected { message, .. } => match stale_current(&message) {
                        Some(current) => Err(StateError::Stale {
                            presented: token,
                            current,
                        }),
                        None => Err(StateError::Backend(message)),
                    },
                    other => Err(StateError::Backend(format!("{other:?}"))),
                }
            }
        }
    }
}

impl ZoneStateStore for InProcess {
    fn load<'a>(
        &'a self,
        _zone_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<StoredZoneState>, StateError>> {
        Box::pin(async move {
            let Some(state) = self.cell.load().await.unwrap() else {
                return Ok(None);
            };
            let bytes = match state.bytes() {
                None => return Ok(None),
                Some(SnapshotBytes::Inline(bytes)) => bytes.clone(),
                Some(SnapshotBytes::Blob(blob)) => {
                    let bytes = self
                        .blobs
                        .lock()
                        .unwrap()
                        .get(&blob.key)
                        .cloned()
                        .expect("blob");
                    assert_eq!(
                        (bytes.len() as u64, sha256_hex(&bytes)),
                        (blob.len, blob.sha256.clone())
                    );
                    bytes
                }
            };
            Ok(Some(StoredZoneState {
                token: state.token(),
                content_version: state.content_version().to_string(),
                state: bytes,
            }))
        })
    }

    fn save<'a>(
        &'a self,
        zone_id: &'a str,
        state: &'a StoredZoneState,
    ) -> BoxFuture<'a, Result<(), StateError>> {
        Box::pin(async move {
            if state.state.len() <= ZONE_STATE_INLINE_MAX {
                let input = json!({
                    "zone_id": zone_id, "token": state.token, "content_version": state.content_version,
                    "inline_b64": B64.encode(&state.state), "blob": null,
                });
                self.command(SAVE, input, None).await
            } else {
                let sha = sha256_hex(&state.state);
                let key = blob_key(zone_id, state.token, &sha);
                self.blobs
                    .lock()
                    .unwrap()
                    .insert(key.clone(), state.state.clone());
                let input = json!({
                    "zone_id": zone_id, "token": state.token, "content_version": state.content_version,
                    "inline_b64": null,
                    "blob": { "key": key, "sha256": sha, "len": state.state.len() },
                });
                self.command(SAVE, input, Some(key)).await
            }
        })
    }

    fn delete<'a>(&'a self, zone_id: &'a str, token: u64) -> BoxFuture<'a, Result<(), StateError>> {
        Box::pin(async move {
            self.command(DELETE, json!({ "zone_id": zone_id, "token": token }), None)
                .await
        })
    }
}

#[tokio::test]
async fn inline_snapshots_meet_the_zone_state_contract() {
    let store = InProcess::new("grid.0.0");
    state_store_conformance(&store, "grid.0.0", vec![3; 1_000]).await;
    assert!(
        store.blobs.lock().unwrap().is_empty(),
        "small snapshots stay in the cell"
    );
}

#[tokio::test]
async fn large_snapshots_go_to_object_storage_and_superseded_blobs_are_dropped() {
    let store = InProcess::new("grid.1.0");
    let big = (0..ZONE_STATE_INLINE_MAX + 4_096)
        .map(|i| (i % 251) as u8)
        .collect::<Vec<_>>();
    state_store_conformance(&store, "grid.1.0", big.clone()).await;
    // Only the live snapshot's object remains (the stale upload, the
    // superseded saves and the deleted one are gone).
    let blobs = store.blobs.lock().unwrap();
    assert_eq!(blobs.len(), 1, "{:?}", blobs.keys().collect::<Vec<_>>());
    assert_eq!(blobs.values().next().unwrap(), &big);
    drop(blobs);
    let state = store.cell.load().await.unwrap().unwrap();
    assert!(
        matches!(state.bytes(), Some(SnapshotBytes::Blob(blob)) if blob.len == big.len() as u64)
    );
}

#[tokio::test]
async fn commands_need_the_zone_host_role_and_one_well_formed_payload() {
    let store = InProcess::new("grid.2.0");
    let input = json!({
        "zone_id": "grid.2.0", "token": 1, "content_version": "c",
        "inline_b64": B64.encode(b"x"), "blob": null,
    });
    let mut player = Session::new();
    player.set(USER_ID_KEY, "alice");
    assert!(
        store.dispatch(SAVE, input.clone(), player).await.is_err(),
        "players cannot write zone state"
    );

    let both = json!({
        "zone_id": "grid.2.0", "token": 1, "content_version": "c", "inline_b64": B64.encode(b"x"),
        "blob": { "key": "k", "sha256": sha256_hex(b"x"), "len": 1 },
    });
    let neither = json!({ "zone_id": "grid.2.0", "token": 1, "content_version": "c", "inline_b64": null, "blob": null });
    let oversized = json!({
        "zone_id": "grid.2.0", "token": 1, "content_version": "c",
        "inline_b64": B64.encode(vec![0u8; ZONE_STATE_INLINE_MAX + 1]), "blob": null,
    });
    let bad_digest = json!({
        "zone_id": "grid.2.0", "token": 1, "content_version": "c", "inline_b64": null,
        "blob": { "key": "k", "sha256": "nope", "len": 1 },
    });
    for bad in [both, neither, oversized, bad_digest] {
        assert!(matches!(
            store.dispatch(SAVE, bad, host_session()).await,
            Err(CellDispatchError::Rejected { .. })
        ));
    }
    store.dispatch(SAVE, input, host_session()).await.unwrap();
    assert_eq!(store.load("grid.2.0").await.unwrap().unwrap().state, b"x");
}
