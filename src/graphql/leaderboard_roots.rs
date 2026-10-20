//! Runtime GraphQL roots for [`LeaderboardSurface`]s: queries, `@live`
//! subscriptions and the change listener that drives them.
//!
//! Authorization is structural like every other root: a role's schema holds
//! only the operations its grant allows, and the viewer-relative operations
//! (`…LeaderboardAround`, `…MyRank`) read the member from the verified
//! session claim, never from an argument.

use std::sync::Arc;
use std::time::Duration;

use async_graphql::dynamic::{
    Field, FieldFuture, InputValue, Object, ResolverContext, Subscription, SubscriptionField,
    SubscriptionFieldFuture, TypeRef,
};
use async_graphql::Value;
use futures_util::Stream;
use tokio::sync::{broadcast, mpsc};

use super::engine::{EngineInner, ExecutionAuthority, GraphqlEngine};
use super::protocol::{
    DistributedLiveMetadata, DistributedLiveMode, DistributedQuerySnapshot,
    ProtocolResponseAccumulator,
};
use super::schema::client_error;
use super::surface::{
    leaderboard_change_key, LeaderboardOperation, LeaderboardRoot, LeaderboardSurface, Surface,
    LEADERBOARD_ENTRY_TYPE,
};
use crate::leaderboard::{Board, LeaderboardChangeListener, LeaderboardEntry, LeaderboardStore};
use crate::microsvc::Session;
use crate::read_model::ReadModelChange;

/// Default `radius` of an around-me query (capped by the surface).
const DEFAULT_RADIUS: u32 = 5;

/// The `LeaderboardEntry` object type.
pub(crate) fn entry_object() -> Object {
    let field = |name: &'static str, ty: &'static str| {
        Field::new(name, TypeRef::named_nn(ty), move |ctx| {
            FieldFuture::new(async move {
                Ok(ctx.parent_value.as_value().and_then(|value| match value {
                    Value::Object(object) => object.get(name).cloned(),
                    _ => None,
                }))
            })
        })
    };
    Object::new(LEADERBOARD_ENTRY_TYPE)
        .field(field("rank", TypeRef::INT))
        .field(field("member", TypeRef::STRING))
        .field(field("score", TypeRef::FLOAT))
}

fn result_type(operation: LeaderboardOperation) -> TypeRef {
    if operation.is_list() {
        TypeRef::named_nn_list_nn(LEADERBOARD_ENTRY_TYPE)
    } else {
        TypeRef::named(LEADERBOARD_ENTRY_TYPE)
    }
}

fn argument_type(argument: &super::surface::LeaderboardArgument) -> TypeRef {
    if argument.nullable {
        TypeRef::named(argument.type_name)
    } else {
        TypeRef::named_nn(argument.type_name)
    }
}

/// Add every granted leaderboard query root of `surface`; returns whether
/// the entry type is needed.
pub(crate) fn add_query_roots(mut query: Object, surface: &Surface) -> (Object, bool) {
    let mut any = false;
    for leaderboard in &surface.leaderboards {
        for root in leaderboard.roots(leaderboard.selected_grant()) {
            any = true;
            let name = leaderboard.name().to_string();
            let operation = root.operation;
            let mut field = Field::new(root.name.clone(), result_type(operation), move |ctx| {
                let name = name.clone();
                FieldFuture::new(async move { resolve(&ctx, &name, operation).await })
            });
            for argument in &root.arguments {
                field = field.argument(InputValue::new(argument.name, argument_type(argument)));
            }
            query = query.field(field);
        }
    }
    (query, any)
}

/// Add the `@live` subscription twins; returns how many were added.
pub(crate) fn add_subscription_roots(
    mut subscription: Subscription,
    surface: &Surface,
) -> (Subscription, usize) {
    let mut added = 0;
    for leaderboard in &surface.leaderboards {
        for root in leaderboard.roots(leaderboard.selected_grant()) {
            if !root.operation.is_live() {
                continue;
            }
            added += 1;
            let name = leaderboard.name().to_string();
            let operation = root.operation;
            let mut field =
                SubscriptionField::new(root.name.clone(), result_type(operation), move |ctx| {
                    let name = name.clone();
                    let request = Request::from_context(&ctx, &name, operation);
                    SubscriptionFieldFuture::new(async move {
                        let request = request?;
                        Ok(live_stream(request))
                    })
                });
            for argument in &root.arguments {
                field = field.argument(InputValue::new(argument.name, argument_type(argument)));
            }
            subscription = subscription.field(field);
        }
    }
    (subscription, added)
}

/// One authorized leaderboard read, with everything owned.
struct Request {
    inner: Arc<EngineInner>,
    store: Arc<dyn LeaderboardStore>,
    surface: LeaderboardSurface,
    root: String,
    operation: LeaderboardOperation,
    board: Board,
    member: Option<String>,
    count: usize,
    protocol: Option<ProtocolResponseAccumulator>,
}

impl Request {
    fn from_context(
        ctx: &ResolverContext<'_>,
        name: &str,
        operation: LeaderboardOperation,
    ) -> Result<Self, async_graphql::Error> {
        let inner = ctx
            .data_opt::<Arc<EngineInner>>()
            .cloned()
            .ok_or_else(|| async_graphql::Error::new("GraphqlEngine not in request data"))?;
        let session = ctx
            .data_opt::<Session>()
            .cloned()
            .unwrap_or_else(Session::new);
        let role = privilege_role(ctx.data_opt::<ExecutionAuthority>(), &session, &inner);
        // Re-check the grant against the role surface the request runs under.
        let surface = inner
            .role_surfaces
            .get(&role)
            .and_then(|surface| {
                surface
                    .leaderboards
                    .iter()
                    .find(|leaderboard| leaderboard.name() == name)
                    .cloned()
            })
            .ok_or_else(|| client_error("FORBIDDEN", "leaderboard is not granted"))?;
        let root = surface
            .roots(surface.selected_grant())
            .into_iter()
            .find(|root: &LeaderboardRoot| root.operation == operation)
            .ok_or_else(|| client_error("FORBIDDEN", "leaderboard operation is not granted"))?;
        let store = inner
            .leaderboard_stores
            .get(name)
            .cloned()
            .ok_or_else(|| client_error("INTERNAL", "leaderboard store is not configured"))?;
        let board_key = string_arg(ctx, "board")?;
        let board = surface
            .board(board_key.as_deref())
            .map_err(|error| client_error("BAD_REQUEST", error))?;
        let member = match operation {
            LeaderboardOperation::AroundMe | LeaderboardOperation::MyRank => Some(
                session
                    .get(surface.get_member_claim())
                    .filter(|member| !member.is_empty())
                    .map(str::to_string)
                    .ok_or_else(|| {
                        client_error("FORBIDDEN", "the session has no leaderboard member claim")
                    })?,
            ),
            LeaderboardOperation::Rank => Some(
                string_arg(ctx, "member")?
                    .filter(|member| !member.is_empty())
                    .ok_or_else(|| client_error("BAD_REQUEST", "member is required"))?,
            ),
            LeaderboardOperation::Top => None,
        };
        let count = match operation {
            LeaderboardOperation::Top => bounded_arg(
                ctx,
                "first",
                surface.default_first(),
                1,
                surface.max_first(),
            )?,
            LeaderboardOperation::AroundMe => bounded_arg(
                ctx,
                "radius",
                DEFAULT_RADIUS.min(surface.max_radius()),
                0,
                surface.max_radius(),
            )?,
            _ => 0,
        };
        Ok(Self {
            protocol: ctx.data_opt::<ProtocolResponseAccumulator>().cloned(),
            inner,
            store,
            surface,
            root: root.name,
            operation,
            board,
            member,
            count: count as usize,
        })
    }

    async fn execute(&self) -> Result<Value, async_graphql::Error> {
        let unavailable = |_| client_error("INTERNAL", "leaderboard store unavailable");
        let entries = match self.operation {
            LeaderboardOperation::Top => self
                .store
                .top(&self.board, self.count)
                .await
                .map_err(unavailable)?,
            LeaderboardOperation::AroundMe => self
                .store
                .around(
                    &self.board,
                    self.member.as_deref().unwrap_or_default(),
                    self.count,
                )
                .await
                .map_err(unavailable)?,
            LeaderboardOperation::MyRank | LeaderboardOperation::Rank => {
                let member = self.member.as_deref().unwrap_or_default();
                let rank = self
                    .store
                    .rank(&self.board, member)
                    .await
                    .map_err(unavailable)?;
                let score = self
                    .store
                    .score(&self.board, member)
                    .await
                    .map_err(unavailable)?;
                return Ok(match (rank, score) {
                    (Some(rank), Some(score)) => entry_value(&LeaderboardEntry {
                        rank,
                        member: member.to_string(),
                        score,
                    }),
                    _ => Value::Null,
                });
            }
        };
        Ok(Value::List(entries.iter().map(entry_value).collect()))
    }

    /// A leaderboard result carries no record or index evidence: the
    /// protocol envelope marks it incomplete and, live, as snapshot mode.
    fn record_protocol(&self, live: bool) -> Result<(), async_graphql::Error> {
        let Some(protocol) = &self.protocol else {
            return Ok(());
        };
        let encoding = |_| client_error("INTERNAL", "query evidence encoding failed");
        let scope_token = protocol
            .bind_query_snapshot_scope(&serde_json::json!({
                "leaderboard": self.surface.name(),
                "root": self.root,
                "board": self.board.name(),
                "member": self.member,
                "count": self.count,
            }))
            .map_err(encoding)?;
        let snapshot = DistributedQuerySnapshot {
            scope_token,
            records_complete: false,
            indexes_comparable: false,
            records: Vec::new(),
            indexes: Vec::new(),
            observations: Vec::new(),
        };
        let live = live.then_some(DistributedLiveMetadata {
            mode: DistributedLiveMode::Snapshot,
            reset: true,
            cursors: Vec::new(),
        });
        protocol
            .record_query_metadata(snapshot, live)
            .map_err(encoding)
    }
}

fn entry_value(entry: &LeaderboardEntry) -> Value {
    let mut object = async_graphql::indexmap::IndexMap::new();
    object.insert(
        async_graphql::Name::new("rank"),
        Value::from(i64::try_from(entry.rank).unwrap_or(i64::MAX)),
    );
    object.insert(
        async_graphql::Name::new("member"),
        Value::from(entry.member.clone()),
    );
    object.insert(async_graphql::Name::new("score"), Value::from(entry.score));
    Value::Object(object)
}

fn string_arg(
    ctx: &ResolverContext<'_>,
    name: &str,
) -> Result<Option<String>, async_graphql::Error> {
    match ctx.args.get(name) {
        None => Ok(None),
        Some(value) if value.is_null() => Ok(None),
        Some(value) => value
            .string()
            .map(|value| Some(value.to_string()))
            .map_err(|_| client_error("BAD_REQUEST", format!("`{name}` must be a string"))),
    }
}

fn bounded_arg(
    ctx: &ResolverContext<'_>,
    name: &str,
    default: u32,
    min: u32,
    max: u32,
) -> Result<u32, async_graphql::Error> {
    let value = match ctx.args.get(name) {
        None => return Ok(default),
        Some(value) if value.is_null() => return Ok(default),
        Some(value) => value
            .i64()
            .map_err(|_| client_error("BAD_REQUEST", format!("`{name}` must be an integer")))?,
    };
    if value < i64::from(min) || value > i64::from(max) {
        return Err(client_error(
            "BAD_REQUEST",
            format!("`{name}` must be within {min}..={max}"),
        ));
    }
    Ok(value as u32)
}

fn privilege_role(
    authority: Option<&ExecutionAuthority>,
    session: &Session,
    inner: &EngineInner,
) -> String {
    if let Some(authority) = authority {
        return authority.privilege_role.clone();
    }
    session
        .roles()
        .first()
        .map(|role| (*role).to_string())
        .unwrap_or_else(|| inner.anonymous_role.clone())
}

async fn resolve(
    ctx: &ResolverContext<'_>,
    name: &str,
    operation: LeaderboardOperation,
) -> Result<Option<Value>, async_graphql::Error> {
    let request = Request::from_context(ctx, name, operation)?;
    let value = request.execute().await?;
    request.record_protocol(false)?;
    Ok(match value {
        Value::Null => None,
        value => Some(value),
    })
}

type LiveItem = Result<Value, async_graphql::Error>;

struct LeaderboardLiveStream {
    rx: mpsc::Receiver<LiveItem>,
}

impl Stream for LeaderboardLiveStream {
    type Item = LiveItem;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

/// Initial result, then a re-execution (debounced, hash-gated) whenever the
/// board's change key is published. A failed execution is terminal.
fn live_stream(request: Request) -> LeaderboardLiveStream {
    let mut changes = request.inner.change_hub.subscribe();
    let key = leaderboard_change_key(request.board.name());
    let (tx, rx) = mpsc::channel::<LiveItem>(8);
    tokio::spawn(async move {
        let mut last_hash = None;
        loop {
            let executed = tokio::select! {
                _ = tx.closed() => return,
                executed = request.execute() => executed,
            };
            match executed {
                Ok(value) => {
                    let hash = super::subscribe::response_hash(&value);
                    if last_hash != Some(hash) {
                        if let Err(error) = request.record_protocol(true) {
                            let _ = tx.send(Err(error)).await;
                            return;
                        }
                        last_hash = Some(hash);
                        if tx.send(Ok(value)).await.is_err() {
                            return;
                        }
                    }
                }
                Err(error) => {
                    let _ = tx.send(Err(error)).await;
                    return;
                }
            }
            // Wait for a change to this board (an empty set means "all").
            loop {
                let change = tokio::select! {
                    _ = tx.closed() => return,
                    change = changes.recv() => change,
                };
                match change {
                    Ok(change) if change.tables.is_empty() || change.tables.contains(&key) => break,
                    Ok(_) => continue,
                    Err(broadcast::error::RecvError::Lagged(_)) => break,
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
            tokio::select! {
                _ = tx.closed() => return,
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
            while let Ok(_) | Err(broadcast::error::TryRecvError::Lagged(_)) = changes.try_recv() {}
        }
    });
    LeaderboardLiveStream { rx }
}

impl GraphqlEngine {
    /// A [`LeaderboardChangeListener`] that refreshes `@live` leaderboard
    /// subscriptions of this engine: register it on the projector writing
    /// the boards (`LeaderboardProjector::with_change_listener`).
    pub fn leaderboard_change_listener(&self) -> LeaderboardChangeListener {
        let hub = self.inner.change_hub.clone();
        Arc::new(move |board: &Board| {
            hub.publish(ReadModelChange::new([leaderboard_change_key(board.name())]));
        })
    }
}
