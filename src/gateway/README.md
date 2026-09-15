# Gateway identity boundary

`AuthProvider` validates current `Credentials` and supplies `RequestContext` for
UI, API, assets and custom routes. Run authentication before each consumer's
route admission or delivery reuse; an expired assertion fails even on a public
route. `provider_revision` changes when provider policy/configuration changes.
An application can replace the provider without changing route declarations.
Contexts are in-process assertions and have no client deserializer.

`BackendCredential` is explicit: either none or a provider-supplied bearer.
The gateway does not translate a cookie or `x-user-id` into a credential.
A configured session provider may resolve an Auth.js session and supply its
access token; the backend must still validate that token and authorize the
operation. With `graphql` enabled, `graphql::identity::OidcGatewayProvider`
reuses the existing JWT validator, JWKS cache and role mapping. This optional
adapter does not make the portable gateway depend on GraphQL.

Delegate login/callback/refresh/logout to existing auth handlers. Configure
`AUTH_URL` as the public origin. Strip incoming identity and forwarded headers,
including deployment-specific identity/secret names, before adding trusted
public-origin metadata. Preserve Origin for CSRF and every Set-Cookie header.
Credentials are redacted from Debug output and are not cache keys.

The reusable production Auth.js/OIDC/browser fixture is in
`tests/gateway-auth`. Network adapters and application mounts own transport
plumbing; merely declaring routes or providers starts no service or identity
store. Removing a gateway mount restores the application's prior entrypoints;
backend authentication stays enabled.

## Native HTTP adapter

Enable `gateway-native` and construct `NativeGateway` from a validated gateway,
`NativeOptions`, explicit named `NativeBinding` resources and `NativeAuth`.
Mount its `router()` on the application's existing listener. Local handlers
receive `RequestContext` through Axum request extensions. UI proxy targets come
from the portable declaration; callers cannot select upstream URLs.

Use the real public origin in `NativeOptions::new`. Incoming identity and
forwarded headers are stripped; Host and forwarded host/protocol are rebuilt
from this value, while Origin is preserved for CSRF. Add deployment-specific
identity/secret header names to `strip_headers`. Upstream redirects are returned
to the browser, with private-origin locations mapped to public origin. The
proxy disables retries, automatic redirects, environment proxies and automatic
response decompression. Duplicate Set-Cookie values remain separate.

`ProxyLimits` bounds request bytes, active proxy streams, connect/header wait,
read idle time and upgraded-connection lifetime. Known oversize requests return
413; over-limit streamed uploads terminate rather than being retried. Capacity
exhaustion returns 503. Response bytes stream without a whole-body buffer;
dropping the body releases capacity and cancels its upstream stream. WebSocket
upgrades require an explicit target opt-in and close on disconnect, identity
expiry or configured lifetime. Exact public-origin loops fail construction;
a bounded hop chain also detects loops through aliases at runtime.

`StaticAssets` validates an immutable preloaded path/byte inventory and memory
budget. Protected assets run normal admission before lookup. It performs no
caller-selected filesystem access. Native construction starts no listener,
projector or event consumer. Disabling this mount restores previous entrypoints.

### GraphQL executors

`gateway-graphql` adds a portable parser-based selected-operation policy; it
compiles to Wasm without an executor. `gateway-graphql-native` binds the existing
GraphQL HTTP/WS executor or a whole remote endpoint to `NativeBinding::Graphql`.
Build query-only engines without command inventory and with `subscriptions(false)`
when live is absent. Commands include status recovery; status queries are never
ordinary reusable reads. Configure custom fields on the selected executor and
register their extension IDs. Custom embedded executors must install the supplied
operation filter and retain/run `GraphqlConnectionGuard` for custom WS handlers.

Remote transport preserves operation names, variables, errors, extensions and
GraphQL WS IDs. It never retries mutations or creates receipts on transport
failure. Configured body, concurrency and connection lifetime limits apply;
backend authorization remains authoritative. Optional delivery features are
rejected until their adapters are bound. Removing the GraphQL binding restores
the existing direct executor routes; no migration is required.

### Causal read routing

`gateway-delivery` supplies bounded exact-scope identity, dependency overlap and
record/index minima contracts without a server or SQL dependency. With GraphQL,
`ReadRouting::new(replica).stale_tolerant(document, operation_name)` explicitly
registers stale-tolerant reads. Pass it to `GraphqlEngineBuilder::read_routing`;
the builder's original repository remains the authoritative primary. Schema and
SQL dialect must match. Unregistered queries, command recovery and live refreshes
use primary. SQLx pools do not certify replay progress.

Generated query artifacts carry the protocol fingerprint. The client sends
`extensions.gatewayFreshness`, bound to its server-established schema, protocol,
policy and cache scope. Generated command dependencies select affected queries;
unknown effects broaden within the authorized surface. Pending effects force
primary without confirming projection. Confirmed query/live index clocks and
Atomic record fences survive optimistic layer retirement. An origin response
that cannot cover supplied minima returns `FRESHNESS_PENDING`, including
incomparable scopes. No stale fallback occurs on primary failure. Context limits
fail explicitly rather than discarding retained evidence. This initial router
has no replica-proof adapter; any retained floor uses primary.

Applications must change the configured protocol namespace when activating a
new projection epoch/backend with incomparable evidence. Old contexts are
rejected and the existing replica scope/reset lifecycle handles rebootstrap.
Keep normal backend authentication on the executor: cacheScope and minima are
identifiers/hints, never bearer credentials. Snapshot-cache and shared-work
admission require a fresh origin identity for every consumer.

No routing migration is required. Disable replica registration to route all reads
to primary; keep client revision fences active during a deployment rollback.
The isolated physical standby fixture is documented in tests/gateway-postgres.

Snapshot caching is available through the explicit `NativeDelivery::snapshots`
resource (`gateway-graphql-native,gateway-delivery`). Origin-side
`GatewayVersionStore` supplies transactional data/proof dependency versions;
every hit authenticates and validates at the primary without result SQL.
See [activation, limits, public-age policy and rollback](../../docs/gateway/snapshot-cache.md).

Concurrent queries can share a bounded execution independently of caching via
`NativeDelivery::coalescing(FlightLimits)`. Each consumer still authenticates;
last-consumer cancellation drops the upstream future. See
[query coalescing](../../docs/gateway/query-coalescing.md).

Shared live delivery is independently selected with `NativeDelivery::live(LiveLimits)`.
See [live sharing and recovery](../../docs/gateway/live-sharing.md).
