use super::{response, Body, Response, StatusCode};
use crate::gateway::delivery::{
    FlightKey, FlightLimits, FlightTable, FlightTicket, FreshnessContext, OriginAdmission,
    SnapshotResponse,
};
use futures_util::{
    future::{BoxFuture, Shared, WeakShared},
    FutureExt, StreamExt,
};
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Clone)]
enum Outcome {
    Shared(Arc<SnapshotResponse>),
    Exclusive(Arc<Mutex<Option<Response>>>),
}
type Work = BoxFuture<'static, Outcome>;
pub(super) struct NativeFlights {
    table: Mutex<FlightTable<WeakShared<Work>>>,
    limits: FlightLimits,
    started: Instant,
}
struct Lease {
    owner: Arc<NativeFlights>,
    ticket: Option<FlightTicket>,
    work: Shared<Work>,
}
impl Drop for Lease {
    fn drop(&mut self) {
        // The table holds only WeakShared. Dropping the last lease cancels
        // the upstream future immediately, without a detached background task.
        if let (Some(ticket), Ok(mut table)) = (self.ticket.take(), self.owner.table.lock()) {
            table.leave(ticket);
        }
    }
}
impl NativeFlights {
    pub(super) fn new(limits: FlightLimits) -> Result<Arc<Self>, super::GatewayError> {
        Ok(Arc::new(Self {
            table: Mutex::new(
                FlightTable::new(limits)
                    .map_err(|_| super::GatewayError("invalid flight limits"))?,
            ),
            limits,
            started: Instant::now(),
        }))
    }
    pub(super) fn counts(&self) -> (usize, usize) {
        self.table.lock().map_or((0, 0), |table| table.counts())
    }
    fn join(self: &Arc<Self>, key: FlightKey, start: impl FnOnce() -> Work) -> Option<Lease> {
        let now = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let (ticket, work) = self
            .table
            .lock()
            .ok()?
            .join(
                key,
                now,
                || {
                    let work = start().shared();
                    let weak = work.downgrade().expect("unpolled shared work");
                    (work, weak)
                },
                WeakShared::upgrade,
            )
            .ok()?;
        Some(Lease {
            owner: self.clone(),
            ticket: Some(ticket),
            work,
        })
    }
    // None means this consumer executes normally without joining: the registry
    // is full, or a nonshareable (cookie/oversized/partial/error) result already
    // went to another consumer.
    pub(super) async fn execute<F, Fut>(
        self: &Arc<Self>,
        key: FlightKey,
        admission: OriginAdmission,
        freshness: Option<FreshnessContext>,
        start: F,
    ) -> Option<Response>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Response> + Send + 'static,
    {
        let limits = self.limits;
        let expires = admission.expires_at;
        // Coalescing is an optimization: with no room to join, run alone.
        let lease = self.join(key, move || {
            async move {
                let result =
                    tokio::time::timeout(Duration::from_millis(limits.deadline_ms), async move {
                        let response = start().await;
                        let (parts, body) = response.into_parts();
                        let body = match super::delivery::capture(body, limits.response_bytes).await
                        {
                            super::delivery::Captured::Bytes(bytes) => bytes,
                            super::delivery::Captured::Streaming(body) => {
                                return exclusive(Response::from_parts(parts, body))
                            }
                        };
                        let headers = parts
                            .headers
                            .iter()
                            .map(|(name, value)| {
                                value
                                    .to_str()
                                    .map(|value| (name.to_string(), value.to_owned()))
                            })
                            .collect::<Result<Vec<_>, _>>();
                        if let Ok(headers) = headers {
                            let snapshot = SnapshotResponse {
                                status: parts.status.as_u16(),
                                headers,
                                body: body.to_vec(),
                            };
                            if snapshot.shareable(&admission, freshness.as_ref()) {
                                return Outcome::Shared(Arc::new(snapshot));
                            }
                        }
                        exclusive(Response::from_parts(parts, Body::from(body)))
                    })
                    .await;
                result.unwrap_or_else(|_| exclusive(response(StatusCode::GATEWAY_TIMEOUT)))
            }
            .boxed()
        })?;
        let remaining = Duration::from_secs(expires.saturating_sub(super::now()));
        if remaining.is_zero() {
            return Some(response(StatusCode::UNAUTHORIZED));
        }
        let outcome = match tokio::time::timeout(remaining, lease.work.clone()).await {
            Ok(outcome) => outcome,
            Err(_) => return Some(response(StatusCode::UNAUTHORIZED)),
        };
        if super::now() >= expires {
            return Some(response(StatusCode::UNAUTHORIZED));
        }
        match outcome {
            Outcome::Shared(snapshot) => {
                Some(super::delivery::cached_response((*snapshot).clone()))
            }
            Outcome::Exclusive(response) => response
                .lock()
                .ok()
                .and_then(|mut response| response.take()),
        }
    }
}
fn exclusive(response: Response) -> Outcome {
    Outcome::Exclusive(Arc::new(Mutex::new(Some(response))))
}
pub(super) fn with_permit(
    response: Response,
    permit: tokio::sync::OwnedSemaphorePermit,
) -> Response {
    let (parts, body) = response.into_parts();
    let stream = body.into_data_stream().map(move |chunk| {
        let _ = &permit;
        chunk
    });
    Response::from_parts(parts, Body::from_stream(stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::delivery::{OperationKey, OriginIdentity, SnapshotPolicy};
    use std::sync::atomic::{AtomicUsize, Ordering};
    fn admitted() -> (FlightKey, OriginAdmission) {
        let request = serde_json::json!({"query":"{ rows { title } }"});
        let identity = OriginIdentity {
            application: "test".into(),
            endpoint: "origin".into(),
            schema_hash: "schema".into(),
            protocol_hash: "protocol".into(),
            authorization_generation: "policy".into(),
            cache_scope: "alice".into(),
        };
        let admission = OriginAdmission {
            key: OperationKey::from_origin(&identity, &request).unwrap(),
            identity,
            operation: "operation".into(),
            validator: "v1".into(),
            validated_at: super::super::now(),
            expires_at: super::super::now() + 30,
            policy: SnapshotPolicy::Current,
        };
        (
            FlightKey::admitted(&admission, &request, None, super::super::now()).unwrap(),
            admission,
        )
    }
    struct Dropped(Arc<AtomicUsize>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[tokio::test]
    async fn last_consumer_cancels_upstream_without_detached_owner() {
        let flights = NativeFlights::new(FlightLimits::default()).unwrap();
        let (key, admission) = admitted();
        let dropped = Arc::new(AtomicUsize::new(0));
        let counter = dropped.clone();
        let (started, ready) = tokio::sync::oneshot::channel();
        let owner = flights.clone();
        let first_key = key.clone();
        let first_admission = admission.clone();
        let first = tokio::spawn(async move {
            owner
                .execute(first_key, first_admission, None, move || async move {
                    let _guard = Dropped(counter);
                    let _ = started.send(());
                    std::future::pending::<Response>().await
                })
                .await
        });
        ready.await.unwrap();
        let owner = flights.clone();
        let second = tokio::spawn(async move {
            owner
                .execute(key, admission, None, || async {
                    panic!("joined consumer must not start work")
                })
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while flights.counts() != (1, 2) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        first.abort();
        let _ = first.await;
        assert_eq!(flights.counts(), (1, 1));
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        second.abort();
        let _ = second.await;
        assert_eq!(flights.counts(), (0, 0));
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn deadline_expiry_and_failure_release_every_group() {
        let flights = NativeFlights::new(FlightLimits {
            deadline_ms: 20,
            ..Default::default()
        })
        .unwrap();
        let (key, admission) = admitted();
        let response = flights
            .execute(key, admission, None, || std::future::pending::<Response>())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(flights.counts(), (0, 0));
        let flights = NativeFlights::new(FlightLimits {
            deadline_ms: 3000,
            ..Default::default()
        })
        .unwrap();
        let (key, mut admission) = admitted();
        admission.expires_at = super::super::now() + 1;
        let response = flights
            .execute(key, admission, None, || std::future::pending::<Response>())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(flights.counts(), (0, 0));
        let (key, admission) = admitted();
        let result = flights
            .execute(key, admission, None, || async {
                super::response(StatusCode::BAD_GATEWAY)
            })
            .await
            .unwrap();
        assert_eq!(result.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(flights.counts(), (0, 0));
    }
}
