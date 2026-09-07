# Shared concurrent queries

Mount `NativeDelivery::coalescing(FlightLimits)` with the GraphQL binding's
`coalescing` capability. This creates no snapshot cache. To select both, use
`NativeDelivery::new(NativeDeliveryOptions { snapshots: Some(...), coalescing:
Some(...), ..Default::default() })`. All capabilities remain explicit and independently removable.
The origin uses the authenticated delivery control path and version store
described in [snapshot-cache.md](snapshot-cache.md).

Every consumer authenticates and validates at the origin before joining. A group
key binds the exact origin subject/scope, operation/variables/extensions, current
dependency validator, and exact freshness requirements. Different or stronger
floors conservatively form different groups; no minimum is weakened to improve
sharing. Mutations, status operations and unknown origin eligibility never join.
The result retains its own complete data and protocol envelope. In-flight sharing
can serve an otherwise successful admitted query that lacks future cache eligibility;
any required minima must still be proven by that response.

An operation owns one shared future. The registry retains a weak reference, and
each consumer holds a cancellation lease. Dropping one lease leaves the other
consumers' work running. Dropping the last lease immediately drops the upstream
future; no detached task remains as a hidden owner. Each response waits for its
consumer's own credential expiry deadline, and expiry is checked again before
delivery. The origin is also revalidated after result execution to detect a
changed scope/policy. A completed group is removed when its consumers finish;
with the cache disabled, a later nonoverlapping query executes normally.

Default limits are 256 active groups, 1,024 consumers per group, a 30-second
operation deadline, and 1 MiB of complete response bytes per group. Native ingress
capacity also bounds all active requests. A full registry rejects additional
joins without evicting work needed by existing consumers. Expired generation
identities cannot release a newer same-key group. Oversized streams, errors,
cookie-setting and otherwise nonshareable responses go to one consumer; other
consumers execute normally with their own admission and freshness. They are never
stored as successful shared snapshots.

`NativeDelivery::flight_counts()` exposes active groups/consumers without
identifiers. Origin metrics distinguish result SQL from per-consumer validation.
Portable `FlightKey`, `FlightLimits` and `FlightRegistry` provide the same bounded
identity, generation and refcount contracts for runtime adapters. They contain
no timers, network clients, SQL pools, or detached tasks. The Worker/DO adapter
owns its own runtime scheduling and cancellation.
