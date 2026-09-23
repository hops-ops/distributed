# Delegated auth fixture

Run from this directory:

```sh
npm ci
npx playwright install chromium
npm test
```

The runner builds and launches a production SvelteKit/adapter-node server and
an isolated, in-memory `oidc-provider`, then drives Chromium through actual
OIDC authorization-code, PKCE, state, nonce, refresh and logout flows. It checks
cross-origin CSRF rejection, failed refresh admission, missing state/nonce,
HTTP session renewal, and explicitly configured Secure cookies. Production
output matters: SvelteKit disables its origin check in the Vite development
server. Dependency versions and the complete npm lockfile are committed.

`prepare.mjs` copies the application's current Auth.js configuration, claim
helpers, session admission and refresh handler from `tests/e2e-ui/ui/src` into
ignored `.generated/` files on every run. The fixture does not maintain an
independent auth implementation. A local fixture-only client secret and fresh
in-memory signing/session keys are used; no playground env file or remote IdP
is read or modified. Servers bind loopback on dynamically allocated ports and
are stopped on success or failure. Build output and node_modules are ignored.

`startFixture()` and `exerciseAuth()` are reusable by native and Worker tests.
`GATEWAY_TEST_ORIGIN` names the public origin when another fixture owns ingress;
`uiOrigin` identifies the private Auth.js upstream. Configure the gateway to
delegate `/auth`, `/login`, `/logout` and `/api/auth` to that upstream. Preserve
independent Set-Cookie fields and reconstruct forwarded host/protocol from
configured public origin. Keep the browser's Origin header for CSRF validation.

Application bindings use `OIDC_ISSUER`, `OIDC_CLIENT_ID`, `OIDC_CLIENT_SECRET`,
`AUTH_URL`, and `AUTH_SECRET`; values belong to the host secret/configuration
provider. The fixture supplies only local values through an explicit process
environment. Its memory-only provider and unconditional local consent policy
are test infrastructure, not a production identity service.
