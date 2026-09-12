# Worker ingress and Durable Object delivery

Enable `gateway-worker` to use `distributed::gateway::worker`. It compiles to
`wasm32-unknown-unknown` with workers-rs and the portable gateway/delivery
contracts. It links no Axum, Tokio runtime, SQLx, GraphQL server or domain bus.
workers-rs includes Tokio with no features for utility types. Commands
continue to execute at the configured backend; the Worker is ingress.

An application constructs `WorkerGateway` with its portable `GatewayConfig`,
explicit `WorkerBinding` resources, `WorkerOptions` and `WorkerAuth`. Mount
`gateway.fetch(request, env)` from the Worker fetch entrypoint. Bind handlers,
an asset service, UI/auth reverse proxies, or a whole remote GraphQL endpoint.
Routes use the same exact/longest-segment ownership as native ingress. A selected
handler's error is terminal. The public origin is configured, never taken from
incoming forwarding headers. The adapter retains Origin and duplicate cookies,
rewrites private-origin redirects and rejects proxy loops. Fetch requires Host
to match the fetched URL; trusted public authority reaches delegated handlers
through configured origin and forwarded headers.

`WorkerAuth::new` accepts an application-owned asynchronous session/provider
implementation returning the shared `RequestContext`. It must validate the
credential before constructing identity or a backend bearer credential.
`WorkerAuth::anonymous()` accepts opaque UI/auth cookies but rejects HTTP bearer
input. It does not turn a cookie or decoded JWT into identity. Auth.js remains
the session/callback/refresh/logout owner when delegated; no second identity
store is introduced. GraphQL connection_init credentials are forwarded to and
validated by the backend. WebSocket commands/status stay on that backend socket
identity path. HTTP query reuse for a WebSocket operation requires explicit
origin control-protocol recognition; older/custom origins remain independent.

## Optional coordination

A `WorkerDeliveryBinding` declares namespace, epoch, shard count and independent
snapshot/coalescing/live resources. With `delivery: None`, forwarding creates no
coordinator. It also works with a backend that has no delivery control protocol.
Declare a workers-rs Durable Object in application code and construct one
`Rc<WorkerCoordinator>` in its `new`. Route its fetch method through
`gateway.fetch_coordinated(request, env, coordinator)`. This is an in-process
mount; public headers cannot assert that admission already happened.

All ingress instances must use the same namespace, binding name, epoch and shard
count. Operation documents, selected operation and canonical variables select a
shard. This routing hash conveys no authorization. The DO repeats provider
admission and obtains a fresh origin validation for **every** consumer before
reuse. The actual cache/flight/live keys retain origin-resolved application,
endpoint, subject scope, schema/policy versions and freshness floors. Private
hits still cost origin validation; 100 eligible query consumers can share one
result SQL execution. WebSocket queries require a control capability check
before the HTTP reuse path and therefore have additional validation overhead.

Use Worker-sized limits, for example the explicit limits in
[`tests/gateway-worker/src/lib.rs`](../../tests/gateway-worker/src/lib.rs).
Native defaults intentionally do not fit the Worker retained-payload budget.
The adapter rejects selected configurations exceeding 16 MiB of reserved cache,
flight and live payload. Live-frame charges follow actual shared ownership,
including consumer queues retained across handoff, and force explicit reset if
the budget is exhausted. This bounds wire payload, not the entire runtime heap:
parsed JSON, credentials, active requests, sockets and the application's own
allocations need additional memory headroom. Groups, consumers, frame sizes,
queue length, response sizes and operation lifetimes have independent limits.

Modern GraphQL subscriptions use standard ping/pong payload echo as delivery
credits: the supplied JS transport echoes the payload. Full data and confirmation
proof stay unchanged. A client that does not acknowledge is reset instead of
silently accumulating data. Raw UI and legacy GraphQL sockets cannot assume that
protocol; they have bounded callback queues and cumulative delivery limits,
after which reconnect is required. `websocket_buffer_bytes` bounds a complete
frame and aggregate queued wire bytes per socket; arbitrary UI delivery is
limited to eight times this value per connection. Customize limits for the UI's
protocol. Outgoing origin sockets remain active and do not hibernate. One shared
steady-state producer does not mean one lifetime handshake: every consumer is
independently authenticated at the origin before grouping.

## Recovery and cancellation

Coordinator state is volatile. Restart starts empty and requires fresh origin
validation/replay. No invalidation feed is needed for correctness: validators
cover committed projection data and proof state, including external SQL writes.
An epoch or shard change abandons old cache/work; clients reconnect and present
origin-verifiable cursors. Cursor gaps, incompatible freshness and queue overflow
cause origin recovery or explicit reset. Last-leave drops the actual upstream
socket; one expired consumer cannot terminate another valid consumer's group.
The upstream can reconnect with a remaining consumer's current credential.

Set the `enable_request_signal` compatibility flag. The adapter preserves
Request.signal through rebuilt requests, and cancellation drops the associated
Rust work and shared-flight ownership. Plain HTTP disconnects before headers
did not produce that signal in the pinned local workerd fixture; those requests
remain bounded by the configured deadline. Explicit ingress abort signals,
response-stream cancellation and live last-leave teardown have separate actual
runtime tests. A cancelled command is never retried by the gateway; cancellation
does not imply that backend effects were rolled back.

The fixture's migration declares a new `DeliveryCoordinator` SQLite DO class;
the adapter stores no cache entries or identity grants in durable storage. For
local reset, stop the runner and change its epoch (or use fresh disposable local
state). Rollback disables delivery bindings while retaining independent remote
forwarding. Do not reuse aggregate-cell classes or namespaces. Nothing in this
fixture provisions or deploys Cloudflare resources.

Platform references: [request cancellation](https://developers.cloudflare.com/changelog/post/2025-05-22-handle-request-cancellation/),
[WebSocket API](https://developers.cloudflare.com/workers/runtime-apis/websockets/),
[outgoing socket lifecycle](https://developers.cloudflare.com/durable-objects/best-practices/websockets/).
