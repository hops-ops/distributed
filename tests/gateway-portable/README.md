# Portable gateway contract fixture

This separate consumer avoids the parent crate's native dev-dependency feature
unification. It builds the same UI/auth-only contract on native and Wasm and
runs the parent gateway contract tests with only portable dependencies.

From the repository root:

```sh
cargo test --manifest-path tests/gateway-portable/Cargo.toml --locked
cargo check --manifest-path tests/gateway-portable/Cargo.toml --locked --target wasm32-unknown-unknown
python3 tests/gateway-portable/check_dependencies.py
```

The runtime dependency tree must exclude async-graphql, axum, sqlx, tokio,
reqwest, tonic and worker. Build dependencies run on the host and are not runtime
imports. The single `gateway` feature selects contracts; no native/Worker server
adapter feature is advertised by this fixture. Route/admission tests use local
`Rc`-holding futures without an async I/O runtime.

These tests prove portable decisions and adapter sequencing. They do not prove
real authentication, UI/network streaming, GraphQL execution or workerd/DO
behavior, which belong to the subsequent gateway implementation tasks.
