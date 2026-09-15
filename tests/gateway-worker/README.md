# Local application gateway Worker

This isolated workers-rs fixture mounts the framework gateway and a **separate**
`DeliveryCoordinator` Durable Object. It does not host commands, projectors, SQL,
or aggregate cells. No Cloudflare account, deployment, or remote resources are
used by its tests. See [the adapter guide](../../docs/gateway/worker.md).

Install Node 24, a current stable Rust toolchain with `wasm32-unknown-unknown`,
and `cargo install worker-build --version 0.8.5 --locked`. From the repository:

```sh
npm ci --prefix tests/gateway-worker
npm ci --prefix tests/gateway-auth
npm exec --prefix tests/gateway-auth -- playwright install chromium
python3 tests/gateway-worker/check_dependencies.py
cargo check --manifest-path tests/gateway-worker/Cargo.toml --locked --target wasm32-unknown-unknown
node tests/gateway-worker/proxy-runtime.mjs
node tests/gateway-worker/run.mjs
cargo test --no-default-features --features gateway-graphql-native,gateway-delivery,sqlite --test graphql_query_protocol worker_ -- --ignored --nocapture --test-threads=1
```

Run these runtime commands sequentially: worker-build writes one generated
bundle. `RUSTUP_TOOLCHAIN` is passed into the isolated build environment; real
application credential files are never loaded. Lockfiles pin workers-rs 0.8.5,
Wrangler 4.129.0, Miniflare 5.20260903.0-alpha, workerd 1.20260903.1, and ws 8.21.3.

The Rust tests start actual GraphQL engines and SQLite projection stores, then
launch Node clients against workerd. They count origin validations, actual
projection SQL, and actual live producers. Query tests cover 100 consumers,
current private hits, external SQL without an invalidation feed, restart, two
separate ingress Wasm isolates using one selected DO, and explicit ingress
AbortSignal cancellation. Live tests cover 100 real JWT-authenticated sockets,
subject isolation, projection commit fanout, proof-only updates, bounded slow
consumers, old-cursor handoff, retention gaps, expiry/reconnect, last-leave
teardown, two ingress isolates and coordinator restart.

The proxy runner uses actual HTTP streams and text/binary WebSockets. The auth
runner reuses the production SvelteKit/Auth.js browser lifecycle fixture and
checks secure cookie attributes separately. Artifacts under `artifacts/` are
ignored. Fixture-only `/__coordinators`, origin control routes and distributor
cancellation headers are test instrumentation; the framework mounts none of
them.

The multi-isolate runner uses Miniflare's explicit module manifest and v4-option
converter for its pinned v5 API. A direct workerd listening socket avoids making
the Node development proxy part of the gateway contract. Plain HTTP disconnects
before response headers did not trigger Request.signal in this local runtime;
the configured operation deadline remains the cleanup bound. The explicit
AbortSignal test proves propagation through service bindings into the DO, and
response-stream cancellation and WebSocket teardown are tested separately.
