# Delivery measurements

`GatewayVersionStore::metrics()` counts actual compiled result SQL separately
from authenticated origin validation requests. `NativeDelivery::metrics()` and
`WorkerCoordinator::metrics()` expose optional `SnapshotMetrics` plus query
admission bypass counts. Cache decisions include hits, misses, resident stale
rejections, fill bypasses and explicit invalidations. These are cumulative,
identifier-free observations within a coordinator lifetime. Live counts expose
active groups/consumers, upstream source attempts, resets, received frames,
duplicates and handoffs; query counts expose active flights/consumers. Export
these through application-owned telemetry, with deployment/host/mode labels only.
No subject, token, document, variables or scope hashes are metric labels.

Run the actual load fixture after installing `tests/gateway-worker` and
worker-build as described in [worker.md](worker.md):

```sh
cargo test --no-default-features --features gateway-graphql-native,gateway-delivery,sqlite --test graphql_query_protocol separate_origin_and_client_savings -- --ignored --nocapture
```

`tests/gateway-worker/artifacts/load-report.json` records the source revision and
whether it was dirty, runtime/platform, repetitions, payload size and resource
limits. Native and workerd each run disabled, coalescing-only, snapshot-only and
live-only modes. A barrier holds actual SQL until100 coalesced consumers join.
Snapshots report warmup separately before100 hits. Every mode holds100 live
consumers during a real projection commit; result SQL, origin validation, steady
producers and all ongoing/temporary origin WebSocket connections are measured.
Different-subject controls prove private isolation. External writes, explicit
cache invalidation, cursor-gap resets and a slow Worker consumer expose recovery
costs. Native bounded-queue behavior is additionally covered by native live tests.

The metering proxy counts actual HTTP request/response bodies and WebSocket
application frames, including origin authentication/validation control traffic.
Browser/client response bytes are counted independently. Header, TLS and frame
encoding overhead are explicitly excluded. No per-browser byte savings are
inferred from producer savings. Latency distributions and full fanout completion
are reported without universal thresholds. The deterministic correctness gates
are SQL/producer counts, causal data equality, isolation and teardown.

With only live sharing selected, ordinary HTTP queries do not incur query
validation. The Worker modern WebSocket adapter may perform temporary origin
authentication handshakes even when query-only delivery is selected; those
connections and bytes are included, not hidden as steady-state producers.
Outgoing live sockets do not hibernate. Disable metric export independently of
resource bounds/correctness; coordinator restart starts a new counter series.
