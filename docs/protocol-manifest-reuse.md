# Immutable protocol manifest reuse

Runtime contract: a selected client surface export owns immutable service,
role/application selection, surface IR and execution limits. Its derived manifest
can be initialized once and shared across clones. Public callers still receive
independently mutable manifest copies. Errors are deterministic for that export
and may be retained; creating another export/engine creates a separate cache.

The engine must retain the exact exports already validated during protocol
construction, including distinct role and application identities. Request seeds
may borrow their compiled manifests, but must still validate principal, asserted
roles, resolved preset values, authorization generation, visibility surface,
cache-scope HMAC and issuance time for each request. No session, token, result row,
authorization decision or request seed is cached.

Observed baseline in Forge: authenticated document TTFB around 2.9–4.1 seconds,
simple authenticated GraphQL queries around 1.1 seconds. Native sampling identifies
ProtocolProjectionRequestSeed::new → export.manifest → projection manifest
lowering as repeated CPU work. Validation must cover cache/clone concurrency,
mutable-return independence, role/application/limits isolation and existing
protocol privacy/authority tests, followed by same-route runtime measurements.
No deadline, authentication, SSR or live-subscription behavior may be weakened.

## Measured local validation

Same retained Forge stack, same authenticated user and paths, September 21:

| Warm document (DOM ready) | Before | After |
| --- | ---: | ---: |
| Dashboard | 3140 ms | 116 ms |
| Personal repository, including default-ref redirect | 7445 ms | 291 ms |
| Organization People | 3350 ms | 192 ms |
| ChangeSets | 4123 ms | 97 ms |

Three direct authenticated GraphQL queries fell from 1143/1135/1072 ms to
22/15/14 ms. These are local observations, not performance assertions or an SLA.
The first repository document after the development server restart still took
18.7 seconds; the warm comparison does not hide that cold-development cost.

Framework library: 1052 passed, 3 explicit ignored. GraphQL/identity/causal HTTP
integration targets: 43 passed. Added tests exercise concurrent first use,
independent mutable returns, role/application/limit separation, rebuilt-engine
isolation, and actual repeated protocol accumulators retaining/releasing shared
metadata without retaining request authority. Existing auth/privacy checks remain.
