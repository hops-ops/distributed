//! The zone's fact host on celld: commands to cells, durable outbox.

use super::*;

// ------------------------------------------------------------------ fact host

/// [`FactHost`] over the cells of the same worker: the cell wait-path
/// request (`POST /{kind}/{shard}/{command}`, internal secret, principal,
/// `x-game-*` stamp) sent straight to the cell's Durable Object stub —
/// object to object, no HTTP hop through a node's public listener. The
/// binding of kind `k` is [`cell_binding`]`(k)`.
pub struct CelldObjectHost {
    pub(super) env: SendWrapper<Env>,
    pub(super) secret: String,
    pub(super) service_id: String,
    pub(super) work: SendWrapper<Rc<RefCell<WorkCounters>>>,
}

impl CelldObjectHost {
    /// A host for `env`'s cells under command-ledger `service_id`.
    pub fn new(env: Env, secret: impl Into<String>, service_id: impl Into<String>) -> Self {
        Self {
            env: SendWrapper::new(env),
            secret: secret.into(),
            service_id: service_id.into(),
            work: SendWrapper::new(Rc::default()),
        }
    }

    /// Count the facts and requests it sends into `work`.
    pub(super) fn counting(mut self, work: Rc<RefCell<WorkCounters>>) -> Self {
        self.work = SendWrapper::new(work);
        self
    }

    pub(super) async fn post(
        &self,
        call: &FactCall<'_>,
    ) -> std::result::Result<FactReceipt, FactError> {
        let headers = Headers::new();
        let set = |name: &str, value: &str| {
            headers
                .set(name, value)
                .map_err(|e| FactError::Permanent(format!("header {name}: {e}")))
        };
        set(CELL_INTERNAL_SECRET_HEADER, &self.secret)?;
        set(CELL_SERVICE_ID_HEADER, &self.service_id)?;
        set(CELL_PRINCIPAL_PARTITION_HEADER, &call.principal)?;
        set(USER_ID_KEY, &call.principal)?;
        set("content-type", "application/json")?;
        for (name, value) in call.session.variables() {
            if name.starts_with(crate::stamp::SESSION_PREFIX) {
                set(name, value)?;
            }
        }
        let body = json!({ "commandId": call.command_id, "input": call.input });
        {
            let mut work = self.work.borrow_mut();
            work.fact_commands += 1;
            work.fact_requests += 1;
        }
        let (status, body) = self
            .send(call.kind, &call.shard, call.command, headers, body)
            .await?;
        classify_wait_path(status, &body)
    }

    /// `calls` (one aggregate) in one `POST /{kind}/{shard}/__batch`
    /// ([`super::super::cells::serve_cell`]); each entry carries its own command
    /// id, principal and stamp. A cell without the route, or one refusing
    /// the batch request as a whole (4xx), gets them one by one.
    pub(super) async fn post_batch(
        &self,
        calls: Vec<FactCall<'_>>,
    ) -> Vec<std::result::Result<FactReceipt, FactError>> {
        let Some(first) = calls.first() else {
            return Vec::new();
        };
        let (kind, shard) = (first.kind, first.shard.clone());
        let entries: Vec<Value> = calls
            .iter()
            .map(|call| {
                let mut session = serde_json::Map::new();
                session.insert(USER_ID_KEY.into(), json!(call.principal));
                for (name, value) in call.session.variables() {
                    if name.starts_with(crate::stamp::SESSION_PREFIX) {
                        session.insert(name.clone(), json!(value));
                    }
                }
                json!({
                    "command": call.command,
                    "commandId": call.command_id,
                    "input": call.input,
                    "partition": call.principal,
                    "session": session,
                })
            })
            .collect();
        let headers = Headers::new();
        let set = |name: &str, value: &str| headers.set(name, value).is_ok();
        if !(set(CELL_INTERNAL_SECRET_HEADER, &self.secret)
            && set(CELL_SERVICE_ID_HEADER, &self.service_id)
            && set("content-type", "application/json"))
        {
            return vec![Err(FactError::Permanent("batch headers".into()))];
        }
        {
            let mut work = self.work.borrow_mut();
            work.fact_commands += calls.len() as u64;
            work.fact_requests += 1;
            work.fact_batches += 1;
        }
        let sent = self
            .send(
                kind,
                &shard,
                super::super::cells::BATCH_ROUTE,
                headers,
                json!({ "entries": entries }),
            )
            .await;
        match sent {
            // No batch route (404; a cell serving only commands answers
            // `__batch` 400), or the batch itself refused: each fact gets
            // its own answer, never the first fact the batch's.
            Ok((status, _)) if (400..500).contains(&status) && !matches!(status, 408 | 429) => {
                let mut results = Vec::with_capacity(calls.len());
                for call in &calls {
                    let result = self.post(call).await;
                    let stop = matches!(result, Err(FactError::Retryable(_)));
                    results.push(result);
                    if stop {
                        break;
                    }
                }
                results
            }
            Ok((status, body)) if (200..300).contains(&status) => body
                .get("results")
                .and_then(Value::as_array)
                .map(|results| {
                    results
                        .iter()
                        .map(|result| {
                            let status = result
                                .get("status")
                                .and_then(Value::as_u64)
                                .and_then(|status| u16::try_from(status).ok())
                                .unwrap_or(500);
                            classify_wait_path(status, result.get("body").unwrap_or(&Value::Null))
                        })
                        .collect()
                })
                .unwrap_or_default(),
            Ok((status, body)) => vec![classify_wait_path(status, &body)],
            Err(error) => vec![Err(error)],
        }
    }

    /// `POST https://cell/{kind}/{shard}/{route}` to the cell's stub:
    /// `(status, JSON body)`.
    pub(super) async fn send(
        &self,
        kind: &str,
        shard: &str,
        route: &str,
        headers: Headers,
        body: Value,
    ) -> std::result::Result<(u16, Value), FactError> {
        let retry = |e: worker::Error| FactError::Retryable(e.to_string());
        let mut init = RequestInit::new();
        init.with_method(Method::Post)
            .with_headers(headers)
            .with_body(Some(body.to_string().into()));
        let url = format!(
            "https://cell/{}/{}/{}",
            encode_segment(kind),
            encode_segment(shard),
            encode_segment(route)
        );
        let request = Request::new_with_init(&url, &init).map_err(retry)?;
        let stub = stub_for(&self.env, kind, shard)
            .map_err(|e| FactError::Permanent(format!("cell `{kind}`: {e}")))?;
        let mut response = stub.fetch_with_request(request).await.map_err(retry)?;
        let status = response.status_code();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        Ok((status, body))
    }
}

impl FactHost for CelldObjectHost {
    fn dispatch_fact<'a>(
        &'a self,
        call: FactCall<'a>,
    ) -> BoxFuture<'a, std::result::Result<FactReceipt, FactError>> {
        Box::pin(SendFuture::new(async move { self.post(&call).await }))
    }

    /// One `__batch` request per cell (the feed's `ZONE_FACT_BATCH` bounds
    /// it).
    fn fact_batches(&self) -> Option<&dyn FactBatchHost> {
        Some(self)
    }
}

impl FactBatchHost for CelldObjectHost {
    fn dispatch_fact_batch<'a>(
        &'a self,
        calls: Vec<FactCall<'a>>,
    ) -> BoxFuture<'a, Vec<std::result::Result<FactReceipt, FactError>>> {
        Box::pin(SendFuture::new(async move {
            if calls.len() == 1 {
                let call = calls.into_iter().next().expect("one call");
                return vec![self.post(&call).await];
            }
            self.post_batch(calls).await
        }))
    }
}

/// The durable fact outbox around the sink: every emitted fact is written
/// to object storage right after the call that emitted it (from the fact
/// queues' journals, [`FactQueue::with_journal`]) and deleted here once it
/// settled (committed or permanently refused). The next activation (or the
/// outbox alarm) resends what is left; the cells' command ledger makes a
/// resend a replay.
#[derive(Clone)]
pub(super) struct DurableFacts {
    pub(super) sink: ObjectFactSink,
    pub(super) storage: SendWrapper<Rc<Storage>>,
    pub(super) work: SendWrapper<Rc<RefCell<WorkCounters>>>,
}

impl FactSink for DurableFacts {
    fn commit<'a>(
        &'a self,
        fact: &'a Fact,
    ) -> BoxFuture<'a, std::result::Result<FactReceipt, FactError>> {
        Box::pin(SendFuture::new(async move {
            let result = self.sink.commit(fact).await;
            match &result {
                Err(FactError::Permanent(e)) => {
                    worker::console_error!("fact {} refused: {e}", fact.key())
                }
                Err(FactError::Retryable(e)) => {
                    worker::console_warn!("fact {} will be retried: {e}", fact.key())
                }
                Ok(_) => {}
            }
            if !matches!(result, Err(FactError::Retryable(_))) {
                self.work.borrow_mut().outbox_deletes += 1;
                let _ = self
                    .storage
                    .delete(&StoredFact::storage_key(fact.key()))
                    .await;
            }
            result
        }))
    }

    /// Batches when the cells take them.
    fn batches(&self) -> Option<&dyn FactBatchSink> {
        self.sink.batches().map(|_| self as &dyn FactBatchSink)
    }
}

impl FactBatchSink for DurableFacts {
    /// One batched delete for the facts the batch settled.
    fn commit_batch<'a>(
        &'a self,
        facts: &'a [Fact],
    ) -> BoxFuture<'a, Vec<std::result::Result<FactReceipt, FactError>>> {
        Box::pin(SendFuture::new(async move {
            let results = self.sink.commit_batch(facts).await;
            let mut settled = Vec::new();
            for (fact, result) in facts.iter().zip(&results) {
                match result {
                    Err(FactError::Permanent(e)) => {
                        worker::console_error!("fact {} refused: {e}", fact.key())
                    }
                    Err(FactError::Retryable(e)) => {
                        worker::console_warn!("fact {} will be retried: {e}", fact.key());
                        continue;
                    }
                    Ok(_) => {}
                }
                settled.push(StoredFact::storage_key(fact.key()));
            }
            if !settled.is_empty() {
                self.work.borrow_mut().outbox_deletes += 1;
                let _ = self.storage.delete_multiple(settled).await;
            }
            results
        }))
    }
}

/// Facts per cell batch from the zone's options.
pub(super) fn fact_batch(options: &Option<ObjectOptions>) -> usize {
    options
        .as_ref()
        .map_or(crate::facts::DEFAULT_MAX_BATCH, |options| {
            options.fact_batch
        })
}
