//! Runtime GraphQL roots of [`ExternalReadSource`]s: queries and `@live`
//! subscriptions, executed by their [`ExternalReadExecutor`]s.
//!
//! Authorization is structural like every other root: a role's schema holds
//! only the roots its grant allows, and each request re-checks the root
//! against the role surface it runs under before the executor sees it.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_graphql::dynamic::{
    Field, FieldFuture, InputValue, Object, ResolverContext, Subscription, SubscriptionField,
    SubscriptionFieldFuture, TypeRef,
};
use async_graphql::Value;
use futures_util::Stream;
use tokio::sync::{broadcast, mpsc};

use super::engine::{EngineInner, ExecutionAuthority};
use super::protocol::{
    DistributedLiveMetadata, DistributedLiveMode, DistributedQuerySnapshot,
    ProtocolResponseAccumulator,
};
use super::schema::{client_error, execution_privilege};
use super::surface::external::external_types;
use super::surface::{
    ExternalField, ExternalReadError, ExternalReadExecutor, ExternalReadResult, ExternalRoot,
    Surface,
};
use crate::microsvc::Session;

fn type_ref(type_name: &str, nullable: bool) -> TypeRef {
    if nullable {
        TypeRef::named(type_name.to_string())
    } else {
        TypeRef::named_nn(type_name.to_string())
    }
}

fn result_type(root: &ExternalRoot) -> TypeRef {
    if root.list {
        TypeRef::named_nn_list_nn(root.type_name.clone())
    } else {
        TypeRef::named(root.type_name.clone())
    }
}

/// The object types the role's external roots return.
pub(crate) fn objects(surface: &Surface) -> Vec<Object> {
    external_types(&surface.external)
        .into_values()
        .map(|ty| {
            ty.fields
                .iter()
                .fold(Object::new(ty.name.clone()), |object, field| {
                    let name = field.name.clone();
                    object.field(Field::new(
                        field.name.clone(),
                        type_ref(&field.type_name, field.nullable),
                        move |ctx| {
                            let name = name.clone();
                            FieldFuture::new(async move {
                                Ok(ctx.parent_value.as_value().and_then(|value| match value {
                                    Value::Object(object) => object.get(name.as_str()).cloned(),
                                    _ => None,
                                }))
                            })
                        },
                    ))
                })
        })
        .collect()
}

fn with_arguments<F>(field: F, arguments: &[ExternalField], add: impl Fn(F, InputValue) -> F) -> F {
    arguments.iter().fold(field, |field, argument| {
        add(
            field,
            InputValue::new(
                argument.name.clone(),
                type_ref(&argument.type_name, argument.nullable),
            ),
        )
    })
}

/// Add every granted external query root of `surface`; returns how many.
pub(crate) fn add_query_roots(mut query: Object, surface: &Surface) -> (Object, usize) {
    let mut added = 0;
    for source in &surface.external {
        for root in source.roots() {
            added += 1;
            let key = (source.section().to_string(), source.name().to_string());
            let name = root.name.clone();
            let field = Field::new(root.name.clone(), result_type(&root), move |ctx| {
                let key = key.clone();
                let name = name.clone();
                FieldFuture::new(async move { resolve(&ctx, &key, &name).await })
            });
            query = query.field(with_arguments(field, &root.arguments, Field::argument));
        }
    }
    (query, added)
}

/// Add the `@live` subscription twins; returns how many were added.
pub(crate) fn add_subscription_roots(
    mut subscription: Subscription,
    surface: &Surface,
) -> (Subscription, usize) {
    let mut added = 0;
    for source in &surface.external {
        for root in source.roots().into_iter().filter(|root| root.live) {
            added += 1;
            let key = (source.section().to_string(), source.name().to_string());
            let name = root.name.clone();
            let field = SubscriptionField::new(root.name.clone(), result_type(&root), move |ctx| {
                let request = Request::from_context(&ctx, &key, &name);
                SubscriptionFieldFuture::new(async move {
                    let request = request?;
                    // Subscribe before the first read so no change slips between.
                    let changes = request.inner.change_hub.subscribe();
                    let first = request.execute().await?;
                    Ok(live_stream(request, first, changes))
                })
            });
            subscription = subscription.field(with_arguments(
                field,
                &root.arguments,
                SubscriptionField::argument,
            ));
        }
    }
    (subscription, added)
}

/// One authorized external read, with everything owned.
struct Request {
    inner: Arc<EngineInner>,
    executor: Arc<dyn ExternalReadExecutor>,
    key: (String, String),
    root: String,
    arguments: BTreeMap<String, serde_json::Value>,
    session: Session,
    protocol: Option<ProtocolResponseAccumulator>,
}

impl Request {
    fn from_context(
        ctx: &ResolverContext<'_>,
        key: &(String, String),
        root: &str,
    ) -> Result<Self, async_graphql::Error> {
        let inner = ctx
            .data_opt::<Arc<EngineInner>>()
            .cloned()
            .ok_or_else(|| async_graphql::Error::new("GraphqlEngine not in request data"))?;
        let session = ctx
            .data_opt::<Session>()
            .cloned()
            .unwrap_or_else(Session::new);
        let role = execution_privilege(ctx.data_opt::<ExecutionAuthority>())?;
        // Re-check the grant against the role surface the request runs under.
        let granted = inner.role_surfaces.get(&role).is_some_and(|surface| {
            surface.external.iter().any(|source| {
                source.section() == key.0
                    && source.name() == key.1
                    && source.roots().iter().any(|granted| granted.name == root)
            })
        });
        if !granted {
            return Err(client_error("FORBIDDEN", "root is not granted"));
        }
        let executor =
            inner.external_executors.get(key).cloned().ok_or_else(|| {
                client_error("INTERNAL", "external read source is not configured")
            })?;
        let arguments = ctx
            .args
            .iter()
            .map(|(name, value)| {
                value
                    .as_value()
                    .clone()
                    .into_json()
                    .map(|value| (name.to_string(), value))
                    .map_err(|_| client_error("BAD_REQUEST", format!("`{name}` is not JSON")))
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            protocol: ctx.data_opt::<ProtocolResponseAccumulator>().cloned(),
            inner,
            executor,
            key: key.clone(),
            root: root.to_string(),
            arguments,
            session,
        })
    }

    async fn execute(&self) -> Result<ExternalReadResult, async_graphql::Error> {
        self.executor
            .execute(&self.root, &self.arguments, &self.session)
            .await
            .map_err(|error: ExternalReadError| client_error(error.code(), error.message))
    }

    /// An external result carries no record or index evidence: the protocol
    /// envelope marks it incomplete and, live, as snapshot mode.
    fn record_protocol(
        &self,
        result: &ExternalReadResult,
        live: bool,
    ) -> Result<(), async_graphql::Error> {
        let Some(protocol) = &self.protocol else {
            return Ok(());
        };
        let encoding = |_| client_error("INTERNAL", "query evidence encoding failed");
        let scope_token = protocol
            .bind_query_snapshot_scope(&serde_json::json!({
                "section": self.key.0,
                "source": self.key.1,
                "root": self.root,
                "scope": result.scope,
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

fn graphql_value(value: serde_json::Value) -> Result<Value, async_graphql::Error> {
    Value::from_json(value).map_err(|_| client_error("INTERNAL", "external result is not GraphQL"))
}

async fn resolve(
    ctx: &ResolverContext<'_>,
    key: &(String, String),
    root: &str,
) -> Result<Option<Value>, async_graphql::Error> {
    let request = Request::from_context(ctx, key, root)?;
    let result = request.execute().await?;
    request.record_protocol(&result, false)?;
    Ok(match graphql_value(result.value)? {
        Value::Null => None,
        value => Some(value),
    })
}

type LiveItem = Result<Value, async_graphql::Error>;

struct ExternalLiveStream {
    rx: mpsc::Receiver<LiveItem>,
}

impl Stream for ExternalLiveStream {
    type Item = LiveItem;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

/// `first`, then a re-execution (debounced, hash-gated) whenever one of the
/// read's change keys is published. A failed execution is terminal.
fn live_stream(
    request: Request,
    first: ExternalReadResult,
    mut changes: broadcast::Receiver<crate::read_model::ReadModelChange>,
) -> ExternalLiveStream {
    let (tx, rx) = mpsc::channel::<LiveItem>(8);
    tokio::spawn(async move {
        let mut last_hash = None;
        let mut executed = Ok(first);
        loop {
            let result = match executed {
                Ok(result) => result,
                Err(error) => {
                    let _ = tx.send(Err(error)).await;
                    return;
                }
            };
            let keys = result.change_keys.clone();
            let value = match graphql_value(result.value.clone()) {
                Ok(value) => value,
                Err(error) => {
                    let _ = tx.send(Err(error)).await;
                    return;
                }
            };
            let hash = super::subscribe::response_hash(&value);
            if last_hash != Some(hash) {
                if let Err(error) = request.record_protocol(&result, true) {
                    let _ = tx.send(Err(error)).await;
                    return;
                }
                last_hash = Some(hash);
                if tx.send(Ok(value)).await.is_err() {
                    return;
                }
            }
            // Wait for a change to one of the keys (an empty set means "all").
            loop {
                let change = tokio::select! {
                    _ = tx.closed() => return,
                    change = changes.recv() => change,
                };
                match change {
                    Ok(change)
                        if change.tables.is_empty()
                            || keys.iter().any(|key| change.tables.contains(key)) =>
                    {
                        break
                    }
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
            executed = tokio::select! {
                _ = tx.closed() => return,
                executed = request.execute() => executed,
            };
        }
    });
    ExternalLiveStream { rx }
}
