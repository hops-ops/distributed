# Native gateway fixture

This isolated consumer enables `distributed/gateway-native` without GraphQL,
SQL, domain buses or Worker SDK dependencies.

From the repository root:

```sh
cargo test --manifest-path tests/gateway-native/Cargo.toml --locked
cargo clippy --manifest-path tests/gateway-native/Cargo.toml --locked --all-targets -- -D warnings
python3 tests/gateway-native/check_dependencies.py
```

The tests use real ephemeral loopback HTTP servers and WebSocket connections.
They prove incremental streaming before an explicit completion signal, response
body drop cancelling upstream work, independent cookies, public-origin headers
and redirects, method/body/query/HEAD semantics, upgrade echo and closure,
protected assets/custom routes, owned failures, request size and concurrency
limits, timeout, and proxy loops through origin aliases. Every server belongs
to the test and is aborted on teardown. No database or external service runs.

A parent build cache can be reused with `--target-dir target` from the repository
root. The package lockfile is committed for reproducibility. Production
Auth.js browser tests are independently reusable from `tests/gateway-auth`;
the full application/Worker owners run them through their public ingress.
