# Public application gateway browser fixture

Run `npm ci --prefix tests/gateway-auth`, install that fixture's Playwright
Chromium, then `node tests/e2e-ui/gateway/run.mjs` from the repository root.
Rust stable, wasm32-unknown-unknown and wasm-pack are required. The runner uses
`distributed build` to generate a coherent application and production SvelteKit
bundle; `GATEWAY_SKIP_BUILD=1` reuses an already successful build for iteration.

Each delivery mode (`none`, `all`) starts a disposable SQLite backend, production
SvelteKit and an isolated standards OIDC provider on free loopback ports. There
are no external users, secrets, databases or cluster resources. Only processes
and temporary data created by this runner are stopped/removed.

Assertions cover public-origin callback, trusted UI/API identity, cookie flags,
real refresh and failed-refresh denial, logout, API ownership, Todo Eventual
optimism before its held receipt, Blob Atomic response painting without HTTP
refetch, an old HTTP/cache response arriving after another move, and a real old
live frame replayed after a confirmed Chat command. Mutation observers detect
transient regressions as well as the final state. Logs omit session payloads and
are saved under ignored `artifacts/`.
The first follow-up Todo data request is held across the fixture's refresh skew,
forcing another real credential rotation after the refresh proof was consumed.
The unchanged Todo/Chat continuity assertion must pass through this interleaving.

With `npm ci --prefix tests/e2e-ui`, set `GATEWAY_LAYOUT_TESTS=1` to additionally
run the existing authenticated and anonymous Chat layout Playwright tests against
this isolated gateway. The runner passes its temporary authenticated storage via
`E2E_USER_STORAGE_STATE`; the original test assertions remain unchanged. They
require the same subscription ID throughout child navigation and completion only
on layout exit, without browser GraphQL mount fetches.
