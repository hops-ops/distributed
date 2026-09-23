//! Commit-path subscription invalidation (live query refresh).
//!
//! Each subscription field:
//! 1. Executes the query once and yields the initial result.
//! 2. Listens on [`ChangeHub`] (fed by `change_stream` / repo broadcast).
//! 3. On dirty tables intersecting the plan footprint, debounces, re-executes,
//!    and yields only when the response hash changes (hash-gated push).

use std::collections::BTreeSet;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use async_graphql::Value;
use futures_util::Stream;
use tokio::sync::{broadcast, mpsc};

use crate::microsvc::Session;
use crate::read_model::ReadModelChange;

use super::compile::{self, RootKind, SelectionNode, SqlPlan};
use super::engine::{execute_plan, EngineInner};
use super::protocol::{ProtocolResponseAccumulator, RequestedLiveResume};

/// Fan-out hub for read-model change notifications.
#[derive(Clone, Debug)]
pub struct ChangeHub {
    tx: broadcast::Sender<ReadModelChange>,
}

impl ChangeHub {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(256);
        Self { tx }
    }

    /// Active origin live readers, excluding the external invalidation forwarder.
    pub fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ReadModelChange> {
        self.tx.subscribe()
    }

    pub fn publish(&self, change: ReadModelChange) {
        if change.is_empty() {
            // Empty set is reserved as the all-dirty lag signal; allow it only
            // when explicitly published for that purpose (forwarder).
        }
        let _ = self.tx.send(change);
    }

    /// Forward an external receiver into this hub until the source closes.
    pub fn spawn_forward_from(&self, mut rx: broadcast::Receiver<ReadModelChange>) {
        let tx = self.tx.clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(change) => {
                        let _ = tx.send(change);
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // Empty tables = all-dirty for subscribers.
                        let _ = tx.send(ReadModelChange {
                            tables: BTreeSet::new(),
                        });
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }
}

impl Default for ChangeHub {
    fn default() -> Self {
        Self::new()
    }
}

// Yield `Value` (not FieldValue<'static>) so HRTB `Into<FieldValue<'a>>` holds for any 'a.
type LiveItem = Result<Value, async_graphql::Error>;

/// Stream of GraphQL field values for one live subscription.
pub struct LiveQueryStream {
    rx: mpsc::Receiver<LiveItem>,
}

impl Stream for LiveQueryStream {
    type Item = LiveItem;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

/// Build a live-query stream for a subscription root field.
pub(crate) async fn live_query_stream(
    inner: Arc<EngineInner>,
    session: Session,
    role: String,
    model: String,
    selection: SelectionNode,
    protocol: Option<ProtocolResponseAccumulator>,
) -> Result<LiveQueryStream, String> {
    let plan: SqlPlan = match compile::compile_query(
        &inner,
        &session,
        &role,
        &model,
        RootKind::List,
        &selection,
    )? {
        compile::QueryPlan::Sql(plan) => plan,
        compile::QueryPlan::CellByKey { .. } => {
            return Err("cell-by-key store does not support @live".into());
        }
    };
    let footprint = footprint_from_tables(&plan.tables_touched);
    let mut change_rx = inner.change_hub.subscribe();
    let (tx, rx) = mpsc::channel::<LiveItem>(8);
    let debounce = Duration::from_millis(100);
    let requested_live_resume = protocol
        .as_ref()
        .map(ProtocolResponseAccumulator::requested_live_resume)
        .transpose()
        .map_err(|error| error.to_string())?
        .unwrap_or(RequestedLiveResume::Absent);

    tokio::spawn(async move {
        // 1) Initial execution + yield
        let initial_result = tokio::select! {
            _ = tx.closed() => return,
            result = execute_list(&inner, &role, &plan, protocol.as_ref(), requested_live_resume) => result,
        };
        let mut initial = match initial_result {
            Ok(executed) => executed,
            Err(e) => {
                let _ = tx.send(Err(async_graphql::Error::new(e))).await;
                return;
            }
        };
        if let Err(error) = initial.record_protocol_metadata(protocol.as_ref()) {
            let _ = tx.send(Err(async_graphql::Error::new(error))).await;
            return;
        }
        let mut last_hash = Some(initial.hash);
        let mut next_live_resume = initial.next_live_resume;
        if tx.send(Ok(initial.value)).await.is_err() {
            return;
        }

        // 2) Change loop: dirty → debounce → re-exec → hash-gate → yield
        loop {
            let change = match tokio::select! {
                _ = tx.closed() => break,
                change = change_rx.recv() => change,
            } {
                Ok(c) => c,
                Err(broadcast::error::RecvError::Lagged(_)) => ReadModelChange {
                    tables: BTreeSet::new(),
                },
                Err(broadcast::error::RecvError::Closed) => break,
            };

            if !footprint_hits(&footprint, &change) {
                continue;
            }

            // Debounce / coalesce
            tokio::select! {
                _ = tx.closed() => break,
                _ = tokio::time::sleep(debounce) => {},
            }
            loop {
                match change_rx.try_recv() {
                    Ok(more) => {
                        // Keep waiting only if still relevant; either way we re-exec once.
                        let _ = more;
                    }
                    Err(broadcast::error::TryRecvError::Empty) => break,
                    Err(broadcast::error::TryRecvError::Lagged(_)) => break,
                    Err(broadcast::error::TryRecvError::Closed) => return,
                }
            }

            let refreshed = tokio::select! {
                _ = tx.closed() => break,
                result = execute_list(&inner, &role, &plan, protocol.as_ref(), next_live_resume.clone()) => result,
            };
            match refreshed {
                Ok(mut executed) => {
                    // Advance the private replay cursor even when a redundant
                    // execution is hash-gated. Protocol frame metadata is
                    // enqueued only when the matching GraphQL value is emitted,
                    // preserving exact data/envelope FIFO ordering.
                    next_live_resume = executed.next_live_resume.clone();
                    if last_hash == Some(executed.hash) {
                        continue; // hash gate: no push on no-change
                    }
                    if let Err(error) = executed.record_protocol_metadata(protocol.as_ref()) {
                        if tx
                            .send(Err(async_graphql::Error::new(error)))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        continue;
                    }
                    last_hash = Some(executed.hash);
                    if tx.send(Ok(executed.value)).await.is_err() {
                        return;
                    }
                }
                Err(e) => {
                    if tx.send(Err(async_graphql::Error::new(e))).await.is_err() {
                        return;
                    }
                }
            }
        }
    });

    Ok(LiveQueryStream { rx })
}

struct ExecutedLiveQuery {
    value: Value,
    hash: u64,
    snapshot: Option<super::protocol::DistributedQuerySnapshot>,
    live: Option<super::protocol::DistributedLiveMetadata>,
    next_live_resume: RequestedLiveResume,
}

impl ExecutedLiveQuery {
    fn record_protocol_metadata(
        &mut self,
        protocol: Option<&ProtocolResponseAccumulator>,
    ) -> Result<(), String> {
        let Some(protocol) = protocol else {
            return Ok(());
        };
        let snapshot = self
            .snapshot
            .take()
            .ok_or_else(|| "causal live query omitted its snapshot metadata".to_string())?;
        protocol
            .record_query_metadata(snapshot, self.live.take())
            .map_err(|error| error.to_string())
    }
}

async fn execute_list(
    inner: &EngineInner,
    role: &str,
    plan: &SqlPlan,
    protocol: Option<&ProtocolResponseAccumulator>,
    requested_live_resume: RequestedLiveResume,
) -> Result<ExecutedLiveQuery, String> {
    let Some(protocol) = protocol else {
        let value = execute_plan(inner, plan).await?;
        return Ok(ExecutedLiveQuery {
            hash: response_hash(&value),
            value,
            snapshot: None,
            live: None,
            next_live_resume: RequestedLiveResume::Absent,
        });
    };

    let role_surface = inner
        .role_surfaces
        .get(role)
        .cloned()
        .ok_or_else(|| "authorized GraphQL role surface is unavailable".to_string())?;
    let executed = super::query_protocol::execute_query_with_protocol(
        inner,
        &inner.pool,
        true,
        role_surface,
        protocol.clone(),
        plan,
        Some(requested_live_resume),
    )
    .await?;
    let hash = protocol_response_hash(&executed.value, &executed.snapshot, &executed.live);
    let next_live_resume = executed
        .live
        .as_ref()
        .filter(|live| live.mode == super::protocol::DistributedLiveMode::Resumable)
        .map(|live| RequestedLiveResume::Cursors(live.cursors.clone()))
        .unwrap_or(RequestedLiveResume::Absent);
    Ok(ExecutedLiveQuery {
        value: executed.value,
        hash,
        snapshot: Some(executed.snapshot),
        live: executed.live,
        next_live_resume,
    })
}

fn footprint_hits(footprint: &BTreeSet<String>, change: &ReadModelChange) -> bool {
    // Empty tables = all-dirty (lag signal from forwarder).
    if change.tables.is_empty() {
        return true;
    }
    change.tables.iter().any(|t| footprint.contains(t))
}

/// Compute the table footprint of a compiled plan (for dirty matching).
pub fn footprint_from_tables(tables: &[String]) -> BTreeSet<String> {
    tables.iter().cloned().collect()
}

/// Hash a GraphQL JSON payload for hash-gated push (no push on no-change).
pub fn response_hash(value: &Value) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    if let Ok(json) = serde_json::to_string(value) {
        json.hash(&mut h);
    } else {
        format!("{value:?}").hash(&mut h);
    }
    h.finish()
}

fn protocol_response_hash(
    value: &Value,
    snapshot: &super::protocol::DistributedQuerySnapshot,
    live: &Option<super::protocol::DistributedLiveMetadata>,
) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hash = DefaultHasher::new();
    match serde_json::to_string(&(value, snapshot, live)) {
        Ok(encoded) => encoded.hash(&mut hash),
        Err(_) => format!("{value:?}:{snapshot:?}:{live:?}").hash(&mut hash),
    }
    hash.finish()
}

/// Forward an external change receiver into the engine hub.
pub fn spawn_change_forwarder(hub: ChangeHub, rx: broadcast::Receiver<ReadModelChange>) {
    hub.spawn_forward_from(rx);
}
