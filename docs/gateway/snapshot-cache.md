# Origin-validated query snapshots

Enable `gateway-graphql-native,gateway-delivery` and the origin's SQL adapter.
The portable `gateway-delivery` feature alone has no SQL or native runtime.
Caching is opt-in: mount `NativeBinding::GraphqlWithDelivery` with an explicit
`NativeDelivery::snapshots(SnapshotLimits)` resource and declare `snapshots`
on that binding. An unselected capability allocates no cache.

On the origin, after framework and application migrations, install
`GatewayVersionStore::install(&pool, application_namespace, physical_tables)`
and pass the resulting store to `GraphqlEngine::builder(...).gateway_versions(store)`.
The inventory must cover every table touched by an eligible compiled query,
including relationship targets and intermediate join tables. Installed projection
proof tables are included automatically because even a no-op projection can
change the response's evidence. This is conservative across projection partitions.

Each consumer sends an authenticated validation request to the configured origin
using the existing GraphQL endpoint with the reserved `gatewayDelivery` extension.
Origin authentication, schema validation and the compiler's row/field authorization
run for every request. Validation executes a single dependency-vector SQL statement
inside a primary read snapshot; it executes no result SQL. The gateway accepts the
identity and opaque validator only from that origin response. Public request metadata
cannot grant cache scope. The existing executor still owns commands and command status.

A miss executes the query with its data, causal metadata and validator in the same
SQL snapshot. The gateway revalidates after the fill and installs only if its own
validator is still current. Every hit revalidates on the primary, so delayed or lost
invalidation notifications cannot certify old private data. Authentication/primary
failure never falls back to cached private data. Missing dependency hooks make the
operation bypass reuse; unknown/custom resolver dependencies, commands/status,
errors, partial results, cookie-setting responses, `no-store`, and `Vary: *` also
bypass storage. Required client freshness minima must be covered by the stored proof.

Defaults are 1,024 entries, 16 MiB total response bytes and 1 MiB per response.
Oversized/streaming responses continue to the consumer without truncation or storage.
Native response-header/read deadlines bound validation and capture. Entries use LRU
eviction; `invalidate_all()` also fences fills that began before the reset.
`GatewayVersionStore::metrics()` reports origin validations separately from actual
result SQL executions; these counters do not contain subjects or query text.

For explicitly public content, `store.public_snapshot(exact_document, operation_name,
max_age_seconds)` permits 1–86,400 seconds of content age. This applies to all variable
values of that exact operation. It still requires fresh origin admission and preserves
subject isolation. Age starts at the original origin validation and is never extended
by copying an entry, a new admission, or another cache layer. The default policy always
requires the current version vector.

## Activation and rollback

Migration `0006_gateway_dependency_versions` is additive and registered in the normal
SQLite/PostgreSQL migration inventory. No application data is backfilled or rewritten.
Activation installs transactional write hooks and starts a new random epoch. Normal
SQL producers, Eventual projectors, Atomic projection commits, deletes, and PostgreSQL
TRUNCATE all update versions in their data transaction. A failed transaction rolls
versions back with the data. Privileged writers that disable/drop hooks are unsupported
until coverage is restored; validation detects missing/disabled hooks and bypasses reuse.
Applications must not manually replace framework hooks with different implementations.

Install after application migrations, before enabling cache bindings. Rebuild or change
writer coverage in a controlled migration window, reinstall hooks/start a new epoch,
then invalidate gateway stores before reopening traffic. `rotate_epoch` is available
for an explicit rebuild boundary. Epochs are random identities, not inferred wall clocks
or PostgreSQL WAL positions. Disable the cache binding on rollback; leave additive
metadata in place until a separately planned cleanup. Never apply cleanup to a live DB
as part of a gateway cache rollback.
