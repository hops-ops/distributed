## Slice review: transports, gateway, composition

All paths are relative to `/Users/patrickleet/dev/hops-ops/hops/distributed/`. Nothing was built or run; every finding is from reading code.

### Assessment

- **Bus:** the layering is right. One `MessageSource`/`ReceivedMessage`/`MessagePublisher` contract, one `run_source` runner that owns failure policy, metrics and tracing, and adapters that do not re-implement the loop. The problem is underneath the contract: adapters differ silently on ordering, redelivery, dead-lettering and idle behaviour, and the conformance suite covers three behaviours.
- **Gateway:** the portable core is small and fail-closed. The native and worker adapters each re-implement the live/delivery state machines, and there are identity leaks at ingress.
- **application/:** half of it is a real contract artifact. The other half (about 1.6k lines of deployment plan, capability and runtime-host code) has no production consumer.
- **Three things most worth changing:**
  1. Make delivery semantics a tested contract: real dead-lettering, shared backoff, a long-running default, an adapter-level conformance suite.
  2. Close the gateway identity side channels (findings 1, 13, 14, 15).
  3. Delete the unconsumed layers: the application deployment plan, the vestigial bus API, and the `application-runtime`/`runtime` features.
- **How it was read:** I read `src/bus` (all non-test code; `sqlite_bus.rs` only by diff against `postgres_bus.rs`; `runner/lane_tests.rs` and `runner/tests.rs` by test name only), `src/metrics` (`api.rs` and the top 330 lines of `registry.rs`; `prometheus.rs`, `http.rs`, `tests.rs` not read), `telemetry.rs`, `trace_context.rs`, the `Cargo.toml` features, and `src/leaderboard` (`mod.rs`, `projector.rs`, `background.rs`, `notify.rs`; `redis.rs`, `in_memory.rs`, `conformance.rs` not read). `src/gateway` + `docs/gateway` and `src/application` + its two docs were read by two sub-reviewers; I re-opened the cited lines for findings 1, 7, 8, 9, 13 and 24, and the rest of theirs are relayed and marked as such.
- **Not read:** `docs/external-facts.md` behavioural claims (symbols checked only), `docs/gateway/load-report.json`, most `tests/gateway*` and `tests/*_transport` bodies (names plus two bodies), and the worker JS runners.

---

### 1. Client can assert `x-user-id` / `x-roles` through the gateway via the WebSocket query string
- Category: E (security)   Severity: high   Confidence: CONFIRMED by code path, not executed
- Where: `src/gateway/native/graphql.rs:412-418`; `src/graphql/http.rs:546-547, 745-767`; also `src/gateway/native/live_transport.rs:107-111`, `src/gateway/worker/live_transport.rs:22-28`, `src/gateway/worker/proxy.rs:164-169`
- What: the gateway strips identity headers (`native/proxy.rs:41-55`), but `rewrite_path` keeps the client query string. The origin's `graphql_ws_upgrade` calls `merge_identity_query_params` in every identity mode, which inserts `x-user-id`/`x-roles` from the query when the header is absent. The header is absent precisely because the gateway stripped it. The sub-reviewer traced the Hybrid-mode fallback to header claims at `src/graphql/identity/resolve.rs:465-467`; I did not re-open that file.
- Why it matters: `wss://gw/graphql?x-user-id=alice&x-roles=admin` with no token becomes alice/admin at a Hybrid or DevHeaders origin. The strip is also a denylist (`gateway/auth.rs:76-91`), while Hybrid trusts any header as a claim.
- Change: restrict `merge_identity_query_params` to DevHeaders; drop the query on gateway GraphQL WS upgrades; forward an allowlist of headers to GraphQL origins. Add a regression test with a Hybrid origin. Size S.

### 2. The default failure policy `DeadLetter` discards the message on every broker adapter
- Category: E/A (Principle 7)   Severity: high   Confidence: CONFIRMED
- Where: `src/bus/failure_policy.rs:53-61`; `src/bus/nats.rs:550-557`; `src/bus/kafka.rs:285-292`; `src/bus/rabbitmq.rs:269-289` with `src/bus/rabbitmq_bus.rs:219-232`; `src/bus/sql_bus_common.rs:606-616, 753-759`
- What: per adapter, "dead-letter" does this:
  - NATS: `AckKind::Term`.
  - Kafka: commits the offset past the record ("A DLQ-topic producer is a follow-up").
  - RabbitMQ: reject without requeue, but `RabbitBus::declare_queue` passes `FieldTable::default()`, so no dead-letter exchange exists.
  - SQL queue: deletes the row. SQL log: advances the offset.
  - In-memory: falls back to `nack`, which loses a command and redelivers an event forever.
  - Every implementation ignores `_reason`. Only `OutboxSource` records a failure (`src/outbox_worker/outbox_source.rs:164`).
- Why it matters: `failure_policy.rs:27` says `LogAndAck` is "the only action that discards a failed message", and `source.rs:100-104` says a message "is never silently dropped". In practice the default drops a permanently failed event with no log line and no stored copy.
- Change: one framework dead-letter destination above the trait: the runner publishes `{message, reason, consumer, attempts}` to a `{namespace}.dlq` destination (or a `bus_dead_letter` table for SQL) through the same `MessagePublisher`, then acks. Remove `dead_letter`/`park` from `ReceivedMessage`, and remove `Park` unless something implements it differently. Breaking trait change. Size M.

### 3. Consumers drain to idle and return `Ok(())` by default; Kafka and RabbitMQ have no long-running mode
- Category: E/F   Severity: high   Confidence: CONFIRMED
- Where: `src/bus/sql_bus_common.rs:369, 567-570, 676-679`; `src/bus/nats_bus.rs:116` and `src/bus/nats.rs:443`; `src/bus/kafka.rs:166, 180` (`DEFAULT_FETCH_TIMEOUT` 8 s, `kafka_bus.rs:33`); `src/bus/rabbitmq.rs:202`; `src/microsvc/workers.rs:62-78`; `src/bus/postgres_bus.rs:26-36`
- What: an empty source returns `Ok(None)`, the runner stops, and `listen`/`subscribe`/`Service::run` return `Ok(())`. SQL and NATS buses default `idle_poll` to zero and need `.with_idle_poll(..)` to keep running. `KafkaBus` and `RabbitBus` have no such option: Kafka stops after 8 s of quiet, RabbitMQ the moment its queues are empty. `spawn_service_consumer_loop` treats `Ok(())` as "host bug": it prints with `eprintln!` and returns, so the consumer is gone until restart.
- Why it matters: the test behaviour is the default and the production behaviour is opt-in per adapter under different names. A Kafka or RabbitMQ service cannot be hosted with the framework's own worker loop.
- Change: `recv` waits until a message or shutdown. `run_source` takes a shutdown signal and runs until cancelled. Tests call an explicit `run_until_idle`. Delete `idle_poll`, the fetch-timeout-as-drain, and `CONSUMER_IDLE_POLL`. Breaking. Size M.

### 4. Adapters differ silently on ordering, redelivery and grouping; conformance covers three behaviours
- Category: A (Principle 7)   Severity: high   Confidence: CONFIRMED
- Where: per-row citations below; `tests/transport_conformance/mod.rs:787-880`
- What:

  | Behaviour | In-memory | SQL | NATS | Kafka | RabbitMQ |
  |---|---|---|---|---|---|
  | Event order across names | grouped by name, not publish order (`in_memory_bus.rs:222`) | global `seq` | stream sequence | none: one topic per name, key = message id (`kafka.rs:83`) | one queue per group |
  | Nack | command lost (`in_memory_bus.rs:188`, `:304`); event retried at once | immediate re-read, blocks the head | delayed, later messages overtake | seek back, blocks the partition | requeue at once |
  | Two replicas, same group, events | n/a | both process every event: `log_read` takes no claim (`postgres_bus.rs:587-625`) | compete | compete | compete |
  | `ordered_delivery` (causal projectors need it, `ordered_delivery.rs:6`) | yes | yes | yes | no | no |
  | Undecodable delivery | n/a | through the failure policy (`decode_error`) | `recv` returns `Err`, run ends (`nats.rs:436-440`) | decoded lossily | decoded lossily |
  | `content_type` round-trip | yes | yes | yes | lost: `owned_headers` never writes it (`kafka.rs:60-77`) | yes |
  | Namespace isolation | none | none: `SqlBus` ignores namespace | yes | yes | yes |

  The generic suite run against real buses has three tests: point-to-point, fan-out across groups, named service as group.
- Why it matters: an app proven on the in-memory bus sees different event order, no command redelivery, and different poison handling on a broker. A projection that needs `created` before `shipped` is not safe on Kafka or in-memory.
- Change: write the contract down (per-aggregate order, at-least-once, backoff, dead-letter, competing within a group) and make it a generic conformance suite every `Bus + BusConsumer` runs. Then fix each adapter: Kafka key = aggregate/stream id with one topic per kind; in-memory as one global log with redelivery; a per-group claim on the SQL log. Size L.

### 5. The SQL bus runs a legacy schema upgrade under `ACCESS EXCLUSIVE` on every `listen` and `subscribe`
- Category: D/F   Severity: high   Confidence: CONFIRMED (Postgres read; SQLite twin located at `sqlite_bus.rs:296-375`)
- Where: `src/bus/sql_bus_common.rs:413-421, 479, 499`; `src/bus/postgres_bus.rs:299-376`
- What: `listen` and `subscribe` each call `ensure_tables()`, which runs the DDL and then `ensure_ordered_log_schema`. That function takes `LOCK TABLE bus_log IN ACCESS EXCLUSIVE MODE`, runs two `ALTER TABLE bus_offset`, deletes offsets with empty epochs, scans the whole log for duplicate `message_id`s, and creates the unique index. Its comments call this "legacy duplicate inspection" for "writers that predate this framework".
- Why it matters: every consumer start blocks all publishers and readers and scans the full log. It is a compatibility path in an unreleased framework. It is pinned by `legacy_equivalent_stable_id_duplicates_retain_the_minimum_cursor` and `legacy_conflicting_stable_id_duplicates_fail_preflight_without_mutation` in both `tests/postgres_transport` and `tests/sqlite_transport`.
- Change: put the final schema (unique index, `NOT NULL` epoch) in `SCHEMA`/migrations. Delete `ensure_ordered_log_schema` and the four legacy tests. Run `ensure_tables` once at construction or migration, not per consumer. Size S.

### 6. Retry/backoff is per-adapter and exists only on NATS; elsewhere a retryable failure is a hot loop
- Category: C/E   Severity: high   Confidence: CONFIRMED
- Where: `src/bus/nats.rs:264-301, 539-545`; `src/bus/sql_bus_common.rs:600-604, 748-751`; `src/bus/postgres_bus.rs:558, 654-666`; `src/bus/kafka.rs:260-282`; `src/bus/rabbitmq.rs:257-267`; `src/microsvc/workers.rs:74`
- What: `NackBackoff` is NATS-only. SQL queue `release_claim` clears the lock with no delay. SQL log `nack` is `Ok(())` and the next `recv` re-reads the same row. Kafka seeks back; RabbitMQ requeues. `bus_queue.attempts` is incremented but never read, and `available_at` is never set after insert. The supervisor restart sleep is a fixed 100 ms with no jitter.
- Why it matters: a downstream outage turns each consumer into a tight broker/DB loop. There is no attempt bound anywhere, so `FailurePolicy::Retry` plus a poison message is infinite.
- Change: backoff and max-attempts belong in `RunOptions` above the trait. The runner computes the delay from the delivery count and calls `nack(delay)`; adapters map it (JetStream `Nak(delay)`, `available_at = now() + delay`, a delay queue). After max attempts the message goes to the dead-letter path in finding 2. Size M.

### 7. Gateway coalescing and live sharing are not transparent: a full registry fails the request
- Category: A/E   Severity: high   Confidence: CONFIRMED (`coordinator.rs` re-opened; adapter mappings relayed from sub-review)
- Where: `src/gateway/delivery/coordinator.rs:57-59, 69-71`; `src/gateway/native/flight.rs:156-157`; `src/gateway/worker/coordinator.rs:421-450`; `src/gateway/native/live.rs:199, 219-222`
- What: `CoordinatorRegistry::join` returns `Unavailable` at 256 groups or 1024 consumers. Native maps that to 503. Live maps it, and a freshness mismatch, to `LIVE_RESET_REQUIRED`.
- Why it matters: the flight key includes the per-user cache scope, so the 257th concurrent distinct private query gets a 503 that it would not get with coalescing off. A reset subscriber reconnects into the same full registry. This breaks "optimizations must be transparent".
- Change: on a full registry or freshness mismatch, execute independently (the bypass path exists). Keep 503 for the ingress permit only. Size S–M.

### 8. The application deployment-plan / capability / topology / runtime-host layer has no production consumer
- Category: F/A (Principle 6)   Severity: high   Confidence: CONFIRMED
- Where: `src/application/plan.rs`, `capability.rs`, `topology.rs`, `runtime_host.rs`, most of `runtime.rs`, `mount.rs:77-131`
- What: outside `src/application/`, `RuntimeHost`, `CapabilityProviders`, `bind_single_process`, `TopologyIntent` and `compile_deployment_plan` appear only in `tests/application_plans.rs` and `tests/gateway_mounts.rs`. `runtime_host.rs:1` calls itself a "skeleton (task 12)". `capability.rs:376-379` computes a SHA-256 and discards it. `plan.rs:432-434` is `let _ = APPLICATION_MANIFEST_SCHEMA_VERSION;` to silence an unused import. The sub-review found the only non-test use of `Runtime` is e2e-ui using it as a selector-to-gateway map.
- Why it matters: this is the parallel topology description Principle 6 warns about, and no host runs from it. The contract half (`ApplicationManifest`, `Module`, `CommandSpec`, `ContractCompiler`) is derived from `Service` + `Surface` and is not parallel; keep it.
- Change: delete those files, `ProcessPreset`, `Runtime`/`RuntimeDialect`, and the two test files; remove the crate-root re-exports. Also remove `required_capabilities: Vec<String>` (`module.rs:700`, `manifest.rs:130`), which is hashed into identity but which the sub-review found nothing reads (not re-checked). Breaking. Size M, nearly all deletion.

### 9. `application-runtime` is evaluated in the user's crate; `runtime` gates nothing
- Category: F   Severity: high   Confidence: CONFIRMED
- Where: `Cargo.toml:37, 49`; `distributed_macros/src/command.rs:325-327, 355-401`; `distributed_macros/Cargo.toml:13-14`
- What: the `#[command]` macro emits `#[allow(unexpected_cfgs)] #[cfg(feature = "application-runtime")]` around the handler function, the mount, `register_*` and the mounted definition. That cfg resolves against the declaring crate's features. In `distributed` itself the feature gates only three test items (`src/microsvc/service/tests.rs:654, 657, 1332`). `feature = "runtime"` has zero hits in the root crate.
- Why it matters: a user who enables `distributed/application-runtime` without declaring a same-named feature in their own crate gets the handler compiled out, with the lint that would say so suppressed. It is a compile-time role flag by naming convention.
- Change: remove both features. The macro always emits the handler; contract-only is an explicit macro argument or a separate macro. Breaking. Size M.

### 10. Native and worker gateway live/delivery coordinators are copy-pasted
- Category: C   Severity: high   Confidence: relayed from sub-review, not re-verified
- Where: `src/gateway/native/live.rs:1-469` vs `worker/live.rs:1-541`; `native/delivery.rs:134-449` vs `worker/coordinator.rs:377-668`; `native/flight.rs:17-183` vs `worker/coordinator.rs:669-751`
- What: about 410 of 469 non-test lines of `native/live.rs` appear verbatim in `worker/live.rs`; the differences are `Arc<Mutex>` vs `Rc<RefCell>` and the channel type. `mark` is byte-identical in both. The GraphQL-WS protocol loop exists four times in the gateway plus the origin's own. "lifetime = min(limit, identity expiry)" is written six times.
- Drift already present: hop limits `>512 bytes, >=8` on native vs `>1024, >8` on the worker; native rejects duplicate `Authorization`, the worker joins them; the worker has no Zones binding and bypasses the `GatewayAdapter` trait, whose only production implementation is native.
- Change: move the group/consumer/history state machine and the validate→lookup→fill→install decision into `gateway/delivery/` as plain `&mut self` structs returning effects, plus one portable WS frame codec. Adapters keep spawn, timer, socket and lock. Either make the worker implement `GatewayAdapter` or delete the trait. Size L.

### 11. The Surface is stored three ways in the manifest, with a hand-written re-serializer to prove they agree
- Category: C   Severity: high   Confidence: relayed from sub-review, not re-verified
- Where: `src/application/module.rs:266-330, 377-513`; `src/application/manifest.rs:1487-1493, 1746-1881`
- What: `SurfaceSpec` carries flattened typed mirrors of the Surface IR and also `contract: serde_json::Value`, documented as the identity authority. `surface_contract_from_spec` rebuilds the contract JSON from the mirror key by key so validation can require equality. Enum identities come from `format!("{:?}", ..).to_ascii_lowercase()` (`module.rs:493, 588`). The surface fingerprint also includes the SQL dialect and physical table names, though `manifest.rs:107-110` says they are absent.
- Why it matters: each new Surface field needs three hand edits or manifests fail as non-canonical. Renaming a Rust variant changes fingerprints. The same app on Postgres has a different "placement-independent" identity.
- Change: one authority: `SurfaceSpec { id, contract, fingerprint }` with inventories derived by deserializing the contract, serde names instead of `Debug`, and no `dialect`. Breaking manifest schema. Size L.

### 12. Message↔wire encoding is written seven times and disagrees
- Category: C/E   Severity: medium   Confidence: CONFIRMED
- Where: `src/bus/nats.rs:214-232`; `src/bus/kafka.rs:60-77, 196-211`; `src/bus/rabbitmq.rs:66-85`; `src/bus/redis_streams.rs:147-162`; `src/bus/knative_bus.rs:95-107`; `src/bus/sql_bus_common.rs:107-147`; `src/bus/celld_queue.rs`
- What: only NATS filters reserved keys out of user metadata. On Kafka and RabbitMQ a metadata entry named `x-sourced-kind` (or `x-sourced-id` on Kafka) overrides the real one, because metadata is written after it and `message_from_wire` takes the last match. Name validation happens only in RabbitMQ, Redis and celld, though `message.rs:125-131` says "publish boundaries call this". Header names are `Nats-Msg-Id`/`X-Sourced-Kind`, `x-sourced-id`/`x-sourced-kind`, `ce-sourcedkind`, and newer `X-Distributed-Occurrence-Id`. `x-sourced-*` is a legacy name on the wire.
- Change: one `WireEnvelope` in bus core (reserved-key set, validation, `Message` ↔ ordered `(key, bytes)` pairs) used by every adapter. Validate name and id once in a publish wrapper above `MessagePublisher`. Rename the headers to `x-distributed-*` now. `RabbitBus::publish_confirmed` (`rabbitmq_bus.rs:251-275`) duplicates `RabbitPublisher::publish` and should reuse it. Size M.

### 13. The Zones binding forwards the caller's bearer token and cookies; docs say it forwards no credential
- Category: E (security)/B   Severity: medium-high   Confidence: CONFIRMED (code re-opened; the docs and test parts relayed)
- Where: `src/gateway/native/mod.rs:427-431`; `src/gateway/native/proxy.rs:56-62`; claims at `src/gateway/config.rs:92-95`; test `tests/gateway_ui.rs:650-661`
- What: Zones calls `proxy::forward` with the admitted context, and `prepare_headers` inserts `Authorization: Bearer <backend credential>` whenever the provider supplies one. `Cookie` is never removed. The test asserts no `authorization` header but sends an anonymous request with a fixture that always returns no credential, so it cannot fail.
- Change: Zones forwards with no backend credential and strips `Cookie`. Make the test send a valid bearer. Size S.

### 14. Worker `Handler` and `Assets` bindings receive raw client headers; native strips them
- Category: A/E   Severity: medium   Confidence: relayed from sub-review, not re-verified
- Where: `src/gateway/worker/mod.rs:301-307` vs `src/gateway/native/mod.rs:410-415`
- What: the same portable `BindingKind::Handler` runs `prepare_headers` on native and passes the original request on the worker. A handler reading `x-user-id` or `x-forwarded-host` is safe on one adapter and spoofable on the other.
- Change: one portable header-rewrite rule applied by both adapters for every binding kind. Size S.

### 15. `extensions.gatewayDelivery` is an in-band control channel a client can forge
- Category: E (security)   Severity: medium   Confidence: relayed from sub-review, not re-verified
- Where: `src/gateway/native/graphql.rs:359-378`; `src/gateway/native/delivery.rs:377-388`; `src/graphql/http.rs:336-346`
- What: native forwards a client-supplied `gatewayDelivery` unchanged when its action is not `"execute"`, and for all mutations. `mark` overwrites `action` but keeps a client-supplied `connectionInit`, from which the origin then resolves identity instead of the request headers.
- Change: strip `extensions.gatewayDelivery` from every client request at ingress and set it only from gateway code, ideally behind a gateway-to-origin secret. Size S–M.

### 16. Postgres fan-out is serialized on one row, with about 7 statements per publish and 2–3 transactions per consumed event
- Category: D   Severity: medium   Confidence: CONFIRMED
- Where: `src/bus/postgres_bus.rs:177-258, 388-470, 627-642, 668-700`; `src/bus/sql_bus_common.rs:653`
- What: every `publish` opens a transaction, runs `INSERT .. ON CONFLICT DO NOTHING` on `bus_log_identity`, `SELECT .. FOR UPDATE` on the singleton, `SELECT MAX(seq) FROM bus_log`, the insert, an `UPDATE high_water`, and commit. Every consumer `recv` runs `verify_log_epoch` (its own transaction taking `FOR SHARE` on the same row) even when rows are already buffered, and every ack runs another locked transaction.
- Why it matters: all publishers and consumers contend on one row. The singleton lock is what makes commit order equal `seq` order, so it should stay; the surrounding per-message work is the cost.
- Change: keep one lock per publish but drop the identity upsert and `MAX(seq)` from the hot path (startup only). Verify the epoch inside the `log_read` and `advance_offset` transactions that already lock it. Advance the offset once per batch. Size M.

### 17. `postgres_bus.rs` and `sqlite_bus.rs` are roughly three-quarters the same text
- Category: C   Severity: medium   Confidence: CONFIRMED (normalized diff: 309 differing lines of 1,433)
- Where: `src/bus/postgres_bus.rs` (702 lines), `src/bus/sqlite_bus.rs` (731 lines)
- What: `SqlBusDialect` is cut at whole-operation granularity, so `reconcile_log_identity`, `ensure_ordered_log_schema`, `insert_log`, `log_read` and `advance_offset` are copied with placeholder and clock changes.
- Change: the dialect supplies only placeholders, clock expression, name-list binding and claim-token minting; the control flow moves into `sql_bus_common` using `QueryBuilder`, as `sqlx_repo` already does. Removes about 500 lines. Size M.

### 18. Vestigial bus API
- Category: F   Severity: medium   Confidence: CONFIRMED
- Where and what:
  - `src/bus/capabilities.rs` (275 lines): `TransportCapabilities`, `ConsumerAckKind`, `KnativeIntegrationKind` have no reader anywhere outside their own file, the re-export, and a README table row. No adapter exposes one, and SQLite, Redis and celld are missing.
  - `src/bus/run_options.rs:14-60`: `InboxHook`, `ConsumerDeliveryMode<I>`, `NoInbox` and the `I` generic on `RunOptions` and `run_source`. The doc calls it a "placeholder shape for the future consumer inbox". Inbox mode only validates the id (`receive_loop.rs:44-46`), `BusConsumer::listen` cannot accept it, and the tests pass `()`. The real inbox is `InboxStore` on the repository.
  - `MessagePublisher::publish_batch` (`src/bus/publisher.rs:44-55`): no caller and no override outside its own two tests.
  - `Message` has public fields plus getters (`message.rs:68-140`).
  - `src/bus/mod.rs:29, 37-39` still says the Kafka adapter is a "separate slice" and the inbox is "future".
- Change: delete all of it; `RunOptions` becomes a plain struct. Breaking. Size S.

### 19. The GraphQL metric label `root_field` is the client's alias
- Category: E (cardinality)   Severity: medium   Confidence: CONFIRMED
- Where: `src/graphql/engine/request.rs:109-113`; `src/metrics/registry.rs` (`graphql_request_total`, `graphql_request_duration`)
- What: the label is the first key of `response.data`, which in GraphQL is the response key, i.e. the alias the client chose. `{ a8f3: todos { id } }` records `root_field="a8f3"`.
- Why it matters: any client can create unlimited series, each a counter plus an 11-bucket histogram, held forever in the process-global registry and exported on every scrape. The telemetry privacy list forbids ids as labels; this bypasses it.
- Change: take the label from the resolved schema field name and map anything not in the generated root set to `"_"`. Size S.

### 20. Metrics cost about ten `String` allocations and five global mutex locks per consumed message
- Category: D   Severity: medium   Confidence: CONFIRMED
- Where: `src/metrics/api.rs:19-52`; `src/metrics/registry.rs:227-252, 388-390`
- What: `record_microsvc_dispatch` builds four owned strings, clones them into separate counter and histogram keys, and locks `dispatch_total`, `dispatch_duration` and `service_info`. `record_transport_message` allocates four more and locks two more. `note_service` re-inserts the service name under a lock on every record. The registry is a process-global `OnceLock`, which is why tests need `lock_for_tests`.
- Change: keys of `&'static str` plus an interned service/message handle, resolved once per route at registration to atomic counters. Register the service once in `describe_service`. `telemetry.rs` and `trace_context.rs` do not overlap and are otherwise clean. Size M.

### 21. Leaderboard is a second projection system in the core crate, with a lossy path and an unbounded queue
- Category: A (Principles 1, 5)/F   Severity: medium   Confidence: CONFIRMED for the code facts; the placement judgement is mine
- Where: `src/leaderboard/` (2.5k lines, always compiled, `src/lib.rs:42`); `src/leaderboard/background.rs:104-115`; `src/leaderboard/notify.rs:107-121`; `Cargo.toml:59-63`
- What: it has its own projector trait, idempotency keys, rebuild/promote, change listeners, and a framework bus event `distributed.leaderboard.changed`. It reaches into `src/graphql` (seven files including a 491-line `leaderboard_roots.rs`), `src/application/{module,manifest}.rs`, and the CLI client compiler.
  - The bus-fed path (`apply_message`) respects the flow.
  - `BackgroundLeaderboard::submit` is `try_send` that drops when full and gives up after three attempts: a best-effort dual write beside the outbox.
  - `LeaderboardChangePublisher` uses an unbounded channel and retries any publish error forever, including permanent ones, so one bad notice blocks the queue while it grows.
  - `redis` bundles two unrelated things (leaderboard and `RedisStreamPublisher`) and force-enables `leaderboard-background`, a feature that only means "tokio is available" and also gates the unrelated change publisher. The Redis Streams publisher has no consumer in the repo.
- Change: move leaderboard to its own crate (`distributed_leaderboard`) that plugs into GraphQL through a generic external-read-source extension point. Delete `BackgroundLeaderboard`; bound the change publisher and drop permanent errors. Split `redis` into `redis-streams` and the leaderboard crate's own feature; delete `leaderboard-background`. Size L.

### 22. The gateway feature lattice: six names, with the bulk behind an unnamed combination
- Category: F   Severity: medium   Confidence: relayed from sub-review; the 65 `gateway-delivery` cfg hits in `src` are my count
- Where: `Cargo.toml:39-48`; `src/gateway/native/mod.rs:122, 215, 439, 467-479`
- What: `gateway-delivery = ["gateway-graphql"]` adds no dependency. `gateway-graphql` alone has no consumer and no CI job. `native/{delivery,flight,live,live_transport}.rs` (2.1k lines) compile only under `all(gateway-graphql-native, gateway-delivery)`, which is what e2e-ui and CI use. `gateway-delivery` also switches origin behaviour inside `src/graphql`.
- Change: fold `gateway-graphql` and `gateway-delivery` into `gateway`. Keep `gateway-native`, `gateway-graphql-native` (now including native delivery, still opt-in at runtime) and `gateway-worker`. The only loss is that UI-only consumers pull in `async-graphql-parser`. Breaking. Size M.

### 23. Other feature flags
- Category: F   Severity: low-medium   Confidence: CONFIRMED
- What:
  - `runtime`: alias, no gate (finding 9). Delete.
  - `metrics = []`: real (42 cfg sites), dependency-free. Keep.
  - `emitter`: gates `src/emitter` (230 lines) and `#[enqueue]`, an in-process event emitter embedded in entities. Used only by `tests/enqueue` and `tests/sourced_enqueue`. It is a side channel beside the outbox and puts an emitter inside aggregates (Principles 1, 3). Delete unless a real project uses it.
  - `workers-rs` vs `gateway-worker`: both pull `dep:worker` independently and no cfg combines them. `workers-rs` actually gates the cell host (`src/microsvc/cell_host`, 121 cfg sites) and the celld queue publisher. Rename to what it gates (for example `cell-host`).
  - `KnativeBus` and `knative_triggers` are gated on `http` (`src/bus/mod.rs:96-99`), so every GraphQL build compiles the manifest generator. Give it a `knative` feature.

### 24. `docs/application-composition.md` describes an API that does not exist and a role matrix the code rejects
- Category: F (docs)   Severity: medium   Confidence: CONFIRMED
- Where: `docs/application-composition.md:82-88, 132, 188`; `src/application/runtime.rs:70`
- What: the doc specifies `SystemSlice`, `ProcessRole::Full / CommandWriter / EventualProjector / QueryApi`, and `runtime.app(..).role(..)`. None of these names exists in any `.rs` file. `runtime.rs:70` says "There is no process-role setter." The sub-review found the doc is linked from the CLI's usage skill and the e2e-ui README. `docs/gateway/live-sharing.md:35` and `docs/gateway/worker.md:62-69` also contradict the code per the sub-review.
- Change: rewrite as what exists (`Service` + `Module`/`Application` + `ContractCompiler`, explicit hosts) or delete it. Size S.

### 25. Per-message round trips and copies on the consume path
- Category: D   Severity: medium   Confidence: CONFIRMED (bus), relayed (gateway)
- Where and what:
  - `src/bus/nats.rs:424-431`: one pull request with `max_messages(1)` per `recv`, even though lanes allow 16 in flight. `:461` copies the payload while keeping the raw message.
  - `src/bus/rabbitmq.rs:184-191`: `basic_get` polling, one round trip per message, no consumer or prefetch.
  - `src/bus/runner/receive_loop.rs:509`: lane mode clones the full `Message` into an `Arc` per delivery; `:374-376` allocates the service name per accept and per settle.
  - `src/bus/celld_queue.rs:60-72`: the envelope is serialized once to measure size and again to send; `into_message` re-validates by serializing a third time.
  - Gateway (relayed): a delivered query has its document parsed four or more times and its JSON deep-cloned five times, and the JWT is verified twice at the gateway (`native/graphql.rs:342-365`, `native/delivery.rs:189-190`). A snapshot hit parses and copies up to 1 MiB under the global cache mutex (`delivery/snapshot.rs:321-379`). Live fan-out deep-clones and re-serializes the payload per consumer (`native/live_transport.rs:548, 421`).
- Change: fetch a batch sized to `lane_window` on NATS; use `basic_consume` with prefetch on RabbitMQ; have `ReceivedMessage` hand out `Arc<Message>`. In the gateway, admit once into a struct, keep bodies as `Bytes`, and extract evidence at install. Size M.

### 26. Smaller correctness hazards in adapters
- Category: E   Severity: medium-low   Confidence: CONFIRMED
- What:
  - `src/bus/kafka.rs:176-179`: every consumer error (auth failure, unknown topic) is discarded as `_transient` with no log or metric; the source then reports "drained".
  - `src/bus/sql_bus_common.rs:52, 58, 553-566`: the queue claims 16 rows under one 30 s lease, handles them sequentially, and never renews. If the batch takes longer, tail rows are re-claimed and run twice concurrently.
  - `src/bus/sql_bus_common.rs:209`: a `seq` decode failure becomes `0`.
  - `src/bus/in_memory_bus.rs:66-73`: publish with an id scans every retained message in every topic. `:165`: each `subscribe` call starts a fresh cursor and replays the whole retained log.
  - `src/bus/postgres_bus.rs:561, 602`: `OR name IS NULL` on a `NOT NULL` column.
  - `src/bus/knative_bus.rs:132`: an unmapped event silently renders the broker `UNMAPPED-events` into the manifest.
  - `src/gateway/worker/frontend.rs:95-104, 497-501` (relayed): the worker closes every non-delivery GraphQL socket after 256 KiB cumulative bytes.
  - Library code logs with `eprintln!` (`receive_loop.rs:158, 284`, `leaderboard/notify.rs:118`, `microsvc/workers.rs:66, 73`).

### 27. Tests: duplicated runner tests, per-adapter copies, a test that proves what the library does not do
- Category: B   Severity: medium   Confidence: CONFIRMED where I name line numbers; the application items are relayed
- What:
  - `tests/transport_conformance/mod.rs:226-340`: the nine `source_*` functions restate `src/bus/runner/tests.rs` (for example `source_dispatches_before_ack` ≙ `success_dispatches_then_acks_in_order`) against a `FakeSource`. They are called only from `tests/transport_in_memory` and never touch a real adapter.
  - `publish_then_consume_round_trips_through_*`, `message_id_and_metadata_survive_the_round_trip`, `retryable_failure_is_redelivered_then_succeeds` and `undecodable_payload_dead_letters_without_blocking` are re-written in each of `tests/{kafka,rabbitmq,nats}_transport/main.rs`. The names already show the divergence: Kafka's is `permanent_failure_skips_record_without_redelivery`.
  - `tests/rabbitmq_transport/main.rs:380-431` `permanent_failure_routes_to_dead_letter_destination` declares its own dead-letter exchange in a test helper (`source_with_dlq`, `:283`). `RabbitBus` never declares one.
  - Broker tests return early when the env var is unset (`let Some(url) = amqp_url() else { return }`), so they pass without running.
  - `src/bus/capabilities.rs:186-275`: seven tests assert constants of a type nothing reads.
  - Legacy tests listed in finding 5.
  - Missing: cross-name ordering, same-group event competition, redelivery after nack on in-memory, content-type on Kafka, behaviour after idle, real dead-letter contents.
  - Gateway (relayed): no direct tests for `SnapshotCache`, `CoordinatorRegistry`, `Minimum::covers` or `LiveFrame::from_origin`, and no Rust tests for the worker coordinator.
  - Application (relayed): `no_linker_inventory_is_needed_for_explicit_application_selection`, `capability_closure_is_explained_and_schema_has_one_owner` (cannot fail), `stale_manifest_predecessor_is_retained_exactly`, and `json_contract_can_exceed_the_old_256_kib_limit`.
- Change: move every per-adapter behaviour test into the generic conformance suite from finding 4; delete the `source_*` copies; make broker tests fail (or be `#[ignore]`) when the broker is absent. Size M.

### 28. `manifest.rs` does eight jobs; primitive validators are duplicated across three files
- Category: C/F   Severity: medium   Confidence: relayed from sub-review, not re-verified
- Where: `src/application/manifest.rs` (2,266 lines, tests only at 2158-2266)
- What: jobs by range: wire types (1-182), composition (184-386), canonical encoding and fingerprints (387-470, 645-673), top-level validation (472-643), per-entity validation (675-1485), the surface re-serializer (1487-1493, 1746-1881), ownership checks (1495-1744), primitive validators (1883-2156). `validate_text`/`validate_sha256`/`validate_json_contract` in `command.rs:564-654` duplicate `manifest.rs:1948-2047`. The "zero the fingerprint, canonicalize, hash" block is written eight times. `fingerprint()` encodes, decodes, validates and re-encodes to return a field.
- Change: split into `manifest/{wire,compose,canonical,validate,ownership}.rs` plus one `application/validate.rs`; one `Fingerprinted` trait. Also reduce `CommandMount` to the typed-route token (the erased-handler path is test-only, `command.rs:687-758`). Size M.

### 29. `NatsBus::retained_domain_events` is a domain-archive reader inside one transport adapter
- Category: A (layering)   Severity: low-medium   Confidence: CONFIRMED
- Where: `src/bus/nats_bus.rs:240-327`; `src/bus/nats.rs:32-175`
- What: 90 lines on `NatsBus` read the whole JetStream stream (capped at 100k messages / 64 MiB) and decode `DomainEventOccurrence`s. `nats.rs` has about 140 lines of external-occurrence identity logic (`external_dedup_id`, `restore_external_identity`) that no other adapter has.
- Why it matters: "replay retained events" and "external fact identity" are framework features that exist on one backend. On the others, external facts get different dedup behaviour.
- Change: lift external-occurrence identity into the shared envelope (finding 12). Make archive replay a trait an adapter may implement, or source it from the event store. Size M.

### 30. Gateway public metrics are positional 7-tuples
- Category: F   Severity: low   Confidence: relayed from sub-review, not re-verified
- Where: `src/gateway/native/delivery.rs:74, 107, 120`; `src/gateway/worker/coordinator.rs:151, 164, 176`
- What: `live_counts() -> (usize, usize, u64, u64, u64, u64, u64)`, with different shapes on native and worker; tests index `.5` and `.6`.
- Change: `LiveMetrics` and `FlightMetrics` structs beside `SnapshotMetrics`, the same from both adapters. Size S.

---

### Things that are good and should not be changed

- **The bus layering.** `MessageRouter` / `MessageSource` / `ReceivedMessage` with one `run_source` that owns ack-after-effects, failure classification, metrics and tracing (`src/bus/runner/receive_loop.rs`). The bus does not depend on `microsvc`. Fix the adapters and the contract, not the shape.
- **`OrderedDelivery` is crate-private to construct** (`src/bus/ordered_delivery.rs`), so application metadata cannot mint ordering evidence. The SQL log's epoch, high-water and fenced-reset design, including the singleton lock that makes commit order equal `seq` order, is deliberate. So is fenced queue settlement by `seq` + `claim_token`.
- **Gateway per-consumer origin admission before any cache, flight or live join**, with keys hashing the origin-issued identity and install fenced by generation (`src/gateway/delivery/identity.rs:89-92`, `snapshot.rs:293-364`). Also the fail-closed config and route normalization (`config.rs:155-339`, `route.rs:180-211`) and the proxy client with redirects, retries and env proxies disabled (`native/mod.rs:261-272`).
- **The manifest's single serialized authority**: derived inventories are rebuilt on decode with `deny_unknown_fields`, encoding is byte-exact, ordering is deterministic (`BTreeMap`, sorted vecs), and registration is explicit `Application::new(..).module(..)` with no linker inventory.
- **Metrics with no SDK dependency and a tested label allowlist** (`src/telemetry.rs` `privacy_policy`); trace context carried as ordinary message metadata so it works on every transport without `otel`.
