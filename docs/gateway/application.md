# Application composition

Declare a gateway alongside the typed service application:

```rust,ignore
let application = service.application("site", surface)?.with_gateway("public", &gateway)?;
let adapter = NativeGateway::new(gateway, options, bindings, auth)?;
```

The manifest records logical binding IDs and capabilities as a versioned
application extension. Physical origins, route paths, secrets and host placement
are excluded, so changing them never rewrites application identity. Declaring a
gateway constructs nothing: the host builds the adapter it serves, and a gateway
does not start domain workers, projectors, stores or an embedded GraphQL server.
Native and Wasm UI/auth-only consumers remain independent of GraphQL execution
and SQL dependencies.

The e2e-ui sample explicitly combines its existing domain host with the public
native gateway. `/graphql` owns API failures; `/auth` and `/api/auth` reach
SvelteKit, `/` serves UI, and service health/lifecycle/Zitadel ingress paths keep
explicit owners. SvelteKit is an internal upstream with no reverse API proxy.
`PUBLIC_ORIGIN` defaults to `http://localhost:8791`, `UI_INTERNAL_ORIGIN` to
`http://localhost:5180`. Configure OIDC callbacks for the public origin.
`GATEWAY_DELIVERY=none` (default) creates no delivery coordinator; `all` selects
bounded cache/flight/live resources and transactional dependency version hooks.
The logical application still uses the existing command sealing and lifecycle
protocols, including `APPLICATION_RELOADING`.

Rollback consists of reverting the sample gateway mount and public-origin
configuration together. Disabling delivery independently preserves command and
query correctness and leaves persisted domain data intact.
