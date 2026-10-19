# `@hops-ops/distributed`

The generated, end-to-end typed client for
[Distributed](https://github.com/hops-ops/distributed) services.

Rust table, relationship, role, and command definitions produce one authorized
client surface. `distributed client` combines that surface with application GraphQL
documents and emits typed operations, live companions, island metadata, and
commands. This package executes those artifacts through one normalized,
causally consistent browser replica.

The intended application experience is:

```ts
const todos = Todos.use();
const commands = useCommands();
await commands.todo.create({ title });
```

Reads populate the replica on demand. Every UI consumer reads that same
replica. Generated optimistic command effects update it synchronously, and
server-issued record/index clocks reconcile the authoritative result without
application-authored cache policies.

## Install

```bash
npm install @hops-ops/distributed graphql
```

The package is ESM-only and requires Node 20 or newer for server-side use. It
also runs in modern browsers. Svelte 5 and React are optional peers used only
by the `/sveltekit` and `/react` entry points, respectively.

## Generate the client surface

The service, not the browser, owns authorization and GraphQL semantics:

```bash
distributed client-manifest > target/distributed-client.json

distributed client \
  --manifest target/distributed-client.json \
  --role user \
  --documents 'src/**/*.graphql' \
  --out src/lib/generated/distributed
```

Use `--surface <name>` for a named application surface. CI can append `--check`
to validate that committed artifacts are current without rewriting them.

A co-located route document can opt into SSR and live continuation:

Row-filtered `@live` queries remain subscribed using authorized replacement
snapshots; cursor-capable queries retain resumable delivery. Neither requires
application polling. The v5 wire protocol uses `live.mode` (`snapshot` or
`resumable`), replacing `live.supported`; upgrade client and server together.
See [live query delivery](../docs/live-query-delivery.md).

```graphql
query Todos @load @live {
  todos(order_by: [{ status: asc }, { todo_id: asc }]) {
    todo_id
    title
    status
  }
}
```

Generation validates the document against the selected role/application,
injects wire-only identity and revision fields, and emits:

- an exact typed operation and optional live companion;
- normalization, identity, relationship, filter, order, and pagination plans;
- the closed variable codec used before cache lookup or transport;
- framework-neutral island metadata plus an inspectable SvelteKit boundary plan;
- an SSR-safe SvelteKit wrapper with static operation bindings and tree-local
  client/command access;
- a nested command tree with input defaults, optimistic effects, and causal
  confirmation contracts;
- an exact schema, protocol, and client-surface binding.

Unsupported or unprovable behavior fails during generation. The runtime does
not parse GraphQL documents, guess cache keys, or infer mutation effects.

## SvelteKit

Install Svelte and describe each generated authorization surface once:

```bash
npm install svelte
```

```js
// distributed.config.js
const serviceManifestArgs = [
  'client-manifest',
  '--manifest-path',
  '../service/Cargo.toml',
  '--package',
  'service'
];

export const distributedClients = [
  {
    module: '$distributed',
    manifest: { args: serviceManifestArgs },
    surface: 'e2e-ui',
    documents: [
      'src/routes/(app)/**/*.graphql',
      'src/lib/components/**/*.graphql'
    ],
    out: 'src/lib/generated/distributed'
  },
  {
    module: '$distributed/admin',
    manifest: {
      args: [
        ...serviceManifestArgs,
        '--entrypoint',
        'service::distributed_admin_client_surface'
      ]
    },
    surface: 'e2e-ui-admin',
    documents: ['src/routes/admin/**/*.graphql'],
    out: 'src/lib/generated/distributed-admin'
  }
];

export const distributedViteOptions = {
  clients: distributedClients
};
```

The common and elevated document sets must not overlap. The route group above
keeps ordinary application documents out of the admin tree; each trust boundary
has its own Rust manifest entrypoint, generated directory, virtual module, and
request-local replica. A single-surface application can omit the second entry.

A component can own a sibling `Component.graphql` island. The adapter walks
static Svelte imports and promotes `@load` work to the nearest page/layout.
Route-local `+page.graphql` and `+layout.graphql` remain first-class. When a
surface needs its own authorization-specific copy, use a qualified route
document such as `+layout.public.graphql`; it still belongs to that layout.

Put stable values in GraphQL itself:

```graphql
query ChatMessages(
  $limit: Int! = 25
  $offset: Int! = 0
) @load @live {
  chat_messages(limit: $limit, offset: $offset) { message_id body }
}
```

The generated variable properties are optional, while the compiler-owned codec
canonicalizes the defaults before cache identity, SSR, live continuation, or
transport. An explicit `ChatMessages.use({ limit: 50, offset: 0 })` overrides
them; `ChatMessages.use()` uses the exact defaults.

Same-name route parameters are inferred. For external values, add one bounded
sidecar beside the document rather than putting screen behavior in
`distributed.config.js`:

```js
// SearchResults.graphql.bindings.js
import {
  defineGraphqlIslandBindings,
  forwardedProp,
  searchParam,
  sessionClaim
} from '@hops-ops/distributed/sveltekit';

export default defineGraphqlIslandBindings({
  query: searchParam('q', 'String'),
  offset: searchParam('offset', 'Int'),
  viewerId: sessionClaim('user', 'id'),
  filters: forwardedProp('filters')
});
```

The helper rejects unsafe shapes, generation checks the keys against the
operation, and the same binding is reused by SSR, hover prefetch, navigation,
hydration, and live work. Resolution precedence is explicit call variables,
then sidecar/route sources, then GraphQL defaults. A non-null variable without
any of those sources fails generation before transport. Central `boundaries`
registrations remain an explicit-placement escape hatch and cannot be combined
with a sidecar for the same operation.

Search bindings require an explicit built-in GraphQL scalar: `String`, `ID`,
`Int`, `Float`, or `Boolean`. Generation and runtime registration reject a
scalar or list shape that differs from the operation variable. For example,
`$offset: Int! = 0` with `searchParam('offset', 'Int')` resolves `?offset=20`
to the number `20`; an absent parameter retains the GraphQL default `0`.
`searchParam('id', 'ID', 'all')` binds repeated parameters to an `[ID!]` list
in URL order. Default `first` mode selects the first repeated value. Absent
parameters are omitted in both modes; nullable omission and required-variable
errors follow the operation codec. An empty string is a present value.

Int and Float use strict decimal syntax, with no trimming, partial parsing,
hexadecimal, or leading plus/zeroes. Float additionally accepts a fraction
and/or exponent. Boolean accepts only `true` and `false`; String and ID retain
decoded URL text. Invalid values fail before transport, and errors omit their
contents. The operation codec still enforces Int's signed 32-bit range, list
limits, defaults, and canonical identity. JSON, enum and custom-scalar decoding
are not inferred from URL text.

This is a breaking binding/adapter-plan v2 change: migrate `searchParam('q')`
to `searchParam('q', 'String')`, and the old second `all` argument to the third
argument after the scalar. Raw `search_param` sources must also include
`scalar`. Regenerate all boundary plans together with the runtime; v1 plans
and hydration fingerprints cannot be reused. Missing multi-value parameters
now preserve GraphQL defaults instead of producing an empty list.

The application lifecycle runs the client compiler before Vite starts and after
declared GraphQL, binding, or Svelte ownership inputs change. Vite consumes the
active immutable client tree and reloads its virtual modules only after atomic
activation:

```ts
// vite.config.ts
import { sveltekit } from '@sveltejs/kit/vite';
import {
  distributedGraphqlProxy,
  distributedSvelteKit
} from '@hops-ops/distributed/sveltekit/vite';
import { defineConfig } from 'vite';
import { distributedViteOptions } from './distributed.config.js';

export default defineConfig({
  plugins: [distributedSvelteKit(distributedViteOptions), sveltekit()],
  server: {
    proxy: distributedGraphqlProxy('http://127.0.0.1:8791')
  }
});
```

Give SvelteKit’s language tools the identical aliases:

```js
// svelte.config.js
import {
  distributedSvelteKitAliases
} from '@hops-ops/distributed/sveltekit/vite';
import {
  distributedClients,
  distributedViteOptions
} from './distributed.config.js';

export default {
  kit: {
    alias: distributedSvelteKitAliases({
      cwd: distributedViteOptions.cwd,
      clients: distributedClients
    })
  }
};
```

The Vite plugin and aliases consume lifecycle-owned generated clients. Run them
through `distributed dev` or `distributed build`; direct Vite execution fails
closed rather than compiling or reading a second source-tree copy.

Create one request-local server replica in the root layout:

```ts
// src/routes/+layout.server.ts
import {
  createDistributedSvelteKitServer
} from '@hops-ops/distributed/sveltekit';
import {
  DISTRIBUTED_BOUNDARY_OPERATIONS
} from '$distributed';

const distributed = createDistributedSvelteKitServer({
  boundaries: DISTRIBUTED_BOUNDARY_OPERATIONS,
  getSession: ({ locals }) => locals.auth(),
  getRole: (session) => roleFromSession(session)
});

export const load = distributed.load;
```

The browser layout installs one client in Svelte context for the current
authorization lifecycle. The generated module retains no client singleton:

```ts
// src/routes/+layout.svelte
import { browser } from '$app/environment';
import { page } from '$app/state';
import {
  createPageDataSessionSource
} from '@hops-ops/distributed/sveltekit';
import {
  DISTRIBUTED_BOUNDARY_OPERATIONS,
  provideDistributed
} from '$distributed';

let { data, children } = $props();
const pageData = createPageDataSessionSource(data);

const client = provideDistributed({
  boundaries: DISTRIBUTED_BOUNDARY_OPERATIONS,
  browser,
  session: pageData.session,
  ...(data.distributed !== undefined &&
  data.distributedAuthority !== undefined
    ? {
        hydration: data.distributed,
        authority: data.distributedAuthority
      }
    : {})
});

$effect(() => pageData.set(data));

$effect(() => {
  const retained = client.retainLocation(
    { id: 'active-page', pathname: page.url.pathname, kind: 'page' },
    {
      search: page.url.searchParams,
      session: data.session,
      props: data
    }
  );
  return () => retained.release();
});

// A delegated link-hover handler can warm any generated target without an
// operation-name switch or a second variable map.
await client.prefetchLocation(target.pathname, {
  search: target.searchParams,
  session: data.session,
  props: data
});
```

### Optional lazy command loading

For read-heavy applications, switch the root layout's generated provider import:

```ts
import { provideDistributedLazy as provideDistributed } from '$distributed';
```

Use the same provider options and `useCommands()` methods. The lazy provider
registers the generated surface authority immediately after hydration, and imports
one shared command runtime and its definitions on the first command. Queries,
SSR, hydration, and route prefetch keep their existing behavior. The default
`provideDistributed` and framework-neutral `createCommands` remain eager.

To load command code before an interaction, call `await client.preloadCommands()`
when opening an editor or focusing an editing control. This imports code without
sending a command. The first cold command waits for this import before optimistic
feedback can appear; failed imports reject and can be retried. Imports never
replay commands. Pending receipts and status recovery remain owned by the shared
client across route changes. On controlled reload, an existing
`reload.recoverPendingCommands` callback runs after lazy command code is ready;
applications still own that callback's recovery policy.

Regenerate the client with the matching CLI/runtime release to obtain
`provideDistributedLazy` and `createLazyCommands`. Framework-neutral consumers
can use `createLazyCommands` from the generated `lazy-commands.js` module and
call its `preload()` method. Directly importing `COMMANDS`, command artifacts,
`createCommands`, or command-dependent pure functions elsewhere in the browser
can keep those definitions eager. Verify the production bundle for your app.

Route components import only their generated surface. Static operation wrappers
resolve the nearest tree-local client when used:

```ts
// src/routes/todos/+page.svelte
import { Todos, useCommands } from '$distributed';

const todos = Todos.use(); // generated @live attaches automatically
const commands = useCommands();

await commands.todo.create({ title: 'Ship it' });
// $todos.data, $todos.status, $todos.pending
```

When the Rust command declaration supplies a UUIDv7, ULID, or literal input
default, generation makes that field optional and the runtime fills it exactly
once. Components do not generate IDs or maintain optimistic/cache recipes.

`@load` results are normalized on the server, dehydrated, and restored in the
browser without a duplicate first request. Hydration cannot authorize itself:
the server sends a separate authority value, and the adapter requires both
values to match. Credential changes abort old HTTP and live work. Without fresh
server evidence, they also discard the old generation. A refreshed credential
received together with a new authorized seed for the exact active scope can
keep the warm replica and pending optimism. The seed proves authorization;
its data does not overwrite newer local command results or freshness floors.
`createPageDataSessionSource`
provides that transfer automatically; custom session sources can supply
`getHydration()` alongside `getAuth()`. Never derive this authority from a JWT
or reuse an old transfer. Logout, changed scopes, and invalid hydration still
purge the generation.

The sample auth refresh endpoint returns an authorized seed for the current
route before invalidating SvelteKit page data. Follow-up data loads and ordinary
SPA navigation execute their bounded server selections too: those requests can
rotate credentials independently and need their own fresh authority. Routes
without selections still do no GraphQL work. See
[session refresh continuity](../docs/session-refresh-continuity.md).

Confirmed records and indexes under an active scope stay until auth/scope
change, stale+revalidate, or a newer authoritative write. Same-scope soft
navigation merges a route SSR seed into the warm client and does **not** wipe
keys the seed omitted (a page dehydrate is only the subset for that route).

Use a separate generated surface and replica for elevated routes. A normal
client cannot import or mix admin artifacts. Configure it as a separate virtual
module such as `$distributed/admin` and provide it only in the elevated layout.

### Game zones in SvelteKit

`distributed_game` zones get three pieces; the canvas, render loop and
entity drawing stay outside Svelte, and game state is never server-rendered.
Svelte 5.7+ is required (`svelte/reactivity`). A runnable example is
[`examples/game/sveltekit`](../examples/game/sveltekit).

**1. Generated game clients from the Vite plugin.** Add `games` to the same
plugin. Each surface runs `distributed game-manifest` → `game-client` →
`game-wasm` (the `#[predict]` step and `#[game_pure]` functions) when Vite
starts, when the zone's Rust sources change (a full reload follows only if
an output changed), and once per `vite build`. In `vite dev` a failing run
(a Rust compile error, say) is logged and shown in the error overlay — also
to overlays opened later — and the server keeps serving the last generated
client until the next change succeeds; `vite build` (and `build --watch`)
fails instead; Vite emits the WebAssembly as
an asset through the generated `new URL('./wasm/<surface>.wasm',
import.meta.url)`. Game surfaces are not lifecycle outputs, so a games-only
app runs under plain `vite dev` / `vite build`; GraphQL `clients` still need
`distributed dev` / `distributed build`.

```js
// distributed.config.js
export const distributedViteOptions = {
  games: [{
    manifest: {
      args: ['game-manifest', '--manifest-path', '../game/Cargo.toml',
        '--entrypoint', 'my_game::pit::game_surface']
    },
    out: 'src/lib/generated/pit',
    watch: ['../game-sim'] // extra crates whose changes regenerate (optional)
    // wasm: 'auto' (default) | false | { manifestPath }
    // importExtension: 'ts' (default) | 'js' | 'none'
  }]
};
// vite.config.ts: plugins: [distributedSvelteKit(distributedViteOptions), sveltekit()]
```

Commit the generated TypeScript (keep `wasm/` ignored) and check it in CI
with `checkDistributedGames(distributedViteOptions)` from
`@hops-ops/distributed/sveltekit/vite` (`game-client --check`, never writes;
`generateDistributedGames` runs the full pipeline once). `command` /
`commandArgs` choose the CLI exactly as for GraphQL clients.

**2. Session-bound tickets** (`@hops-ops/distributed/sveltekit/game/server`,
server-only). The route mints the zone server's HS256 admission ticket
(PROTOCOL.md §8) with WebCrypto, so the shared secret stays on the SvelteKit
server and no extra admission service is needed:

```ts
// src/lib/server/game.ts
import { env } from '$env/dynamic/private';
import { createGameTicketHandler } from '@hops-ops/distributed/sveltekit/game/server';

export const tickets = createGameTicketHandler({
  secret: () => env.GAME_TICKET_SECRET,          // the zone servers' TicketKey (>= 32 bytes)
  getSession: (event) => event.locals.auth(),    // the same session source as GraphQL
  zone: ({ principal, requested }) => zoneFor(principal) // decided here, never by the body
});
// src/routes/api/game/ticket/+server.ts
export const POST = tickets;
// a +page.server.ts load: const { zone, principal } = await tickets.resolve(event)
```

The principal is `session.user.id` (override with `principal(session)`;
`name(session)` sets the ticket's display name). The handler answers 403
unless the `Origin` is the app's own (`allowedOrigins` adds more) and
`Sec-Fetch-Site`, when sent, is `same-origin`; 401 without a session; 429
over the per-principal limit (default 10 per minute, in memory;
`rateLimit: { max, windowMs }`, a custom `{ take(key, now) }` returning a
boolean or a promise of one, or `false`); 400 for any body field other than
`zone` / `role` (so a body cannot name the principal, an owner or a reward);
409 with the server's zone when the requested zone differs; 403 when `zone`
returns nothing; 503 without a valid secret. Every response is
`cache-control: no-store`. Tickets live 60 s by default (`ttlSeconds` ≤ 300)
and are single use on the zone server.
`mintGameTicket({ secret, principal, zone, role, name })` signs one directly.

The default limiter counts per process, so N instances behind a load
balancer allow N× the budget. Share it through Redis (or any store that runs
Redis Lua) with `createRedisGameTicketRateLimiter`. It imports no Redis
package: pass your connected node-redis client (`sendCommand`) or ioredis
client (`call`), or `eval(script, keys, args)` for anything else (cluster
clients, a pooled wrapper):

```ts
import { createClient } from 'redis';
import {
  createGameTicketHandler,
  createRedisGameTicketRateLimiter
} from '@hops-ops/distributed/sveltekit/game/server';

const redis = await createClient({ url: env.REDIS_URL }).connect();
export const tickets = createGameTicketHandler({
  // …
  rateLimit: createRedisGameTicketRateLimiter({
    client: redis,
    prefix: 'mygame:ticket:',  // default 'distributed:game-ticket:' + principal
    limit: 10,
    windowMs: 60_000,
    timeoutMs: 1_000,          // slower store calls count as errors (default)
    onUnavailable: 'closed',   // default; 'open' = no limit while the store is down
    onError: (error) => log.warn({ error }, 'ticket limiter store')
  })
});
```

One atomic `EVAL` (`GAME_TICKET_RATE_LIMIT_SCRIPT`) keeps a sliding-window
log per principal in a sorted set: it drops entries older than the window,
refuses when `limit` remain, else adds one, and expires the key with the
window. Refused requests are not counted, matching the in-memory limiter.
The timestamp is the instance clock, so keep instances NTP-synchronized.

**Store unreachable:** a store error, a timeout or an unexpected reply makes
the limiter reject, and the handler answers **503 `rate_limit_unavailable`**
(fail closed — ticket issuance stops rather than run unlimited; the message
carries no store details, which go to `onError`). `onUnavailable: 'open'`
admits instead. The same applies to any custom limiter: a `take` that
throws or rejects is a 503, never an allow.

**3. Runes state for the UI around the game**
(`@hops-ops/distributed/sveltekit/game`, browser-safe):

```svelte
<script lang="ts">
  import { gameTicketSource, useGame } from '@hops-ops/distributed/sveltekit/game';
  import { createPitClient, EVENTS } from '$lib/generated/pit/index.ts';
  let { data } = $props();
  const game = useGame(
    () => createPitClient({ url: (z) => `${data.zonesUrl}/zones/${z}`, ticket: gameTicketSource() }),
    { zone: () => data.zone, events: EVENTS }
  );
  const profile = game.latest('profile');          // typed by the generated EVENTS
  const kills = game.events('kill', { limit: 10 }); // newest first
  $effect(() => game.client && startRenderLoop(canvas, game.client)); // your rAF loop
</script>
<p>{game.status} · {game.zone} · {game.rttMs ?? '–'} ms · {game.predictionReady ? 'predicted' : '…'}</p>
<p>XP {profile.current?.xp ?? 0} · last kills {kills.items.length}</p>
```

`useGame` builds the client on mount (browser only, nothing during SSR),
joins the zone and closes it on destroy, which includes navigation.
`createGameState(source | factory, { events })` is the same state without the
lifecycle (`connect(zone)`, `dispose()`). Getters — `status`, `connected`,
`zone` (follows handoffs), `rttMs`, `jitterMs`, `predictionReady`, `error`,
`client`, `source` — and `latest(name).current` / `events(name).items` are
reactive through `createSubscriber`: an unchanged value does not re-run
effects. `on(name, handler)` subscribes a callback (e.g. to set the
controlled entity from a `hello`). `gameTicketSource({ endpoint })` POSTs
`{ zone, role }` to the ticket route with the session cookie.

## Framework-neutral replica

Other frameworks can bind the same core directly:

```ts
import {
  createDistributedReplica,
  createReplicaGraphqlTransport
} from '@hops-ops/distributed/replica';
import { Operation_Todos } from './generated/distributed/index.js';

const transport = createReplicaGraphqlTransport({
  getUrl: () => '/graphql',
  getAuth: () => ({ accessToken: session.accessToken })
});
const replica = createDistributedReplica({ transport });
const todos = replica.watch(Operation_Todos, {}, { live: true });

const unsubscribe = todos.subscribe((snapshot) => {
  render(snapshot.data, snapshot.status);
});
```

`watch()` reads synchronously, fetches only missing or stale projections,
deduplicates work, and optionally maintains the generated live operation.
`read()` is side-effect-free. `dehydrate()` and `hydrate()` transfer confirmed
state without exposing a public storage schema. Cold `hydrate` seeds an empty
client; warm same-scope `hydrate` merges so soft navigation cannot discard
confirmed session data the next boundary did not re-dehydrate. Layout
retention survives child navigation, page retention is replaced on page exit,
and exact layout/page duplicates share one replica watch until the final owner
releases it.

The replica stores normalized records and exact argument-sensitive indexes,
not GraphQL response blobs. Generated selection metadata reconstructs each
operation result from that shared state, so a detail read, list read, live
frame, or optimistic command can update every affected view in one transaction.

## Commands and optimistic UI

Generated `createCommands` binds the service-owned command artifacts to the
same replica and GraphQL transport. A command call:

1. validates and freezes its typed input;
2. fills generated UUIDv7, ULID, or literal defaults exactly once;
3. applies the generated optimistic effect transaction (from `.applies` /
   portable mutation IR — works for **Eventual and Direct** when fields are known);
4. dispatches the exact compiler-owned mutation;
5. keeps ambiguous commits recoverable by command ID;
6. confirms or rejects only its own optimistic layer;
7. retires the layer on the path that placement allows:
   - **Eventual** — wait for projection obligations (event handler ran
     async; there is no authoritative row on the command response);
   - **Atomic / Direct** — normalize the **returned** row
     (`confirmDirectProjection`) before the call settles. The server waited in
     the command handler because it could; an event handler cannot.

An Eventual command may be terminal `succeeded` at a cell boundary while its
status envelope already carries every exact projection observation. Those
observations settle `receipt.projected` because they prove delivery of the
projection obligation; they do not retire the accepted optimistic layer. The
layer is retired only by a matching canonical query or live frame, so an
`@load` operation does not need to become `@live` merely to complete a causal
wait. A later query or navigation can still supply that canonical frame.

Applications do not provide list targets, merge functions, mutation update
callbacks, board simulators, or invalidation maps. If the compiler cannot prove
safe maintenance, the generated plan marks the affected projection stale and
the replica performs one deduplicated revalidation.

Callers may bound their own causal wait without inventing a rollback:

```ts
const receipt = await commands.todo.create(
  { title: 'Ship it' },
  { signal: AbortSignal.timeout(5_000) }
);
await receipt.projected;
```

Before acceptance the signal cancels dispatch. After finite acceptance it
rejects only that caller's `receipt.projected` wait; the optimistic layer and
internal causal tracking remain active, and `receipt.status()` stays available.

## React

Install React and use the optional adapter over an application-owned replica:

```tsx
import {
  DistributedProvider,
  useDistributedQuery
} from '@hops-ops/distributed/react';
import { Operation_Todos } from './generated/distributed/index.js';

function TodosView() {
  const todos = useDistributedQuery(Operation_Todos, {}, { live: true });
  return todos.complete
    ? todos.data.todos.map((todo) => <div key={todo.todo_id}>{todo.title}</div>)
    : null;
}

root.render(
  <DistributedProvider replica={replica}>
    <TodosView />
  </DistributedProvider>
);
```

The adapter is only a `useSyncExternalStore` bridge. It does not add another
cache, transport, auth lifecycle, or command path. For SSR, create one replica
per request and hydrate only under the same authoritative scope.

## Persistence and diagnostics

The default replica is memory-only. Optional IndexedDB persistence is explicit,
confirmed-state-only, and governed by generated/application model policy.
Optimistic layers, command inputs, credentials, cache authority, and live
connections are never persisted as replica data.

The same persistence object offers an opt-in durable command outbox for
offline use. It decorates the command transport: commands whose policy is
explicitly `{ durable: true, sensitive: false }` are written to IndexedDB
before they are sent and drained in order with their original command ids
(the server's receipts deduplicate resends). Rejections surface to the live
caller as `REPLICA_COMMAND_REJECTED` and, for commands replayed after a
reload, through `onSettled`. Only the prepared request is stored — never
optimistic state — and entries replay only under the same cache scope.

```ts
const outbox = persistence.commandOutbox({
  transport,                       // the network command transport
  scope: () => currentAuthoritativeScope,
  policy: { commands: { 'todo.create': { durable: true, sensitive: false } } },
  onSettled: ({ commandId, outcome }) => console.info(commandId, outcome)
});
const commands = createCommands(replica, outbox.transport);
addEventListener('online', () => void outbox.drain());
```

Diagnostics are also opt-in:

```ts
import {
  createReplicaDiagnostics
} from '@hops-ops/distributed/diagnostics';

const diagnostics = createReplicaDiagnostics();
const replica = createDistributedReplica({ transport, diagnostics });
const commands = createCommands(replica, transport, { diagnostics });
```

Snapshots explain operation artifacts, normalized records, index coverage,
optimistic layers, causal receipts, revalidation, response fences, and garbage
collection. Defaults pseudonymize identities and omit values, arguments,
credentials, trusted presets, raw command inputs, and cache scope. Revealing
additional development detail requires an in-process capability plus an
explicit redactor.

## Game zones

`@hops-ops/distributed/game` speaks the `distributed_game` binary protocol
(golden-tested against the Rust fixtures). In SvelteKit, see
[game zones in SvelteKit](#game-zones-in-sveltekit) for session tickets,
Vite-generated clients and runes state. The app supplies tickets — a
function, so a fresh single-use ticket is fetched whenever a reconnect token
can no longer be used — and a pure `predict` for its own entity. With a
[generated client](#generated-game-clients) that `predict` is the zone's
own Rust step compiled to WebAssembly; the hand-written form below is the
low-level API.

```ts
import { GameClient, InputPacer } from '@hops-ops/distributed/game';

const game = new GameClient<{ dx: number; dy: number }>({
  url: (zone) => `wss://game.example/zones/${zone}`,
  ticket: ({ zoneId, role }) => api.mintZoneTicket(zoneId, role),
  prediction: {
    encodeInput: ({ dx, dy }) => new Uint8Array([dx & 0xff, dy & 0xff]),
    predict: (s, i, dt) => ({ x: s.x + i.dx * 10 * dt / 1000, y: s.y + i.dy * 10 * dt / 1000 }),
    corrections: { avatar: { x: { mode: 'ease', durationMs: 120 }, hp: { mode: 'replace' } } }
  }
});
await game.connect('room-1');
game.on('event', (zone, e) => { /* e.g. announce the controlled entity */ });
game.setControlledEntity(myEntityId);
// One input per server tick, whatever the display refresh rate.
const pacer = new InputPacer({ tickRate: () => game.authority?.connection.welcome?.tickRate });
requestAnimationFrame(function frame(now) {
  pacer.update(now, (dtMs) => game.sendInput(readStick(), dtMs));
  draw(game.render());
  requestAnimationFrame(frame);
});
```

`render()` merges the authoritative zone and observed neighbours (`observe`)
sampled at `serverNow − interpolationDelay`, with the controlled entity at
its predicted state. The delay is **adaptive** per zone by default
(`AdaptiveDelay`: `rtt / 2 + 2 patch intervals + 2 × jitter`, clamped to
`[2 patches, 800 ms]`, slewed ≤ 25 ms per 100 ms; tune with
`adaptiveDelay: { minMs, maxMs, slewMs, patchIntervals, jitterFactor }`,
inspect with `client.adaptiveDelay(zone)`): the clock estimates the
server's *present*, so a fixed delay below the one-way latency would ask
for states not yet received. A fixed `interpolationDelayMs: 100` (or a
function of the zone) is still accepted. `client.setPredictionEnabled(false)`
renders the controlled entity like any other (interpolated, one RTT behind
the keys) while inputs keep being recorded, so switching back is seamless. Reconciliation rebases on
every Delta's `ack_seq`; per-field `ease`, `snap` or `replace` resolves the
divergence. A server Handoff connects to the target zone with the transfer
ticket (following `Redirect`s) and swaps authority while the previous world
keeps rendering; if the target refuses or stays silent the same Hello goes
back to the source zone, then a fresh ticket. `Kick(RELOAD)` and handshake
`VERSION_MISMATCH` emit `reloadRequired`; supply `reloadContent` (or call
`connection.resumeAfterReload(version)`) to rejoin with the new content.
`client.contentVersion` follows the reload, so later handoff targets and
observers present the new version; a client that patches content live
(e.g. on a `resume` with a new version) calls `client.setContentVersion(v)`.
`encodeInput(input, ctx)` receives the interpolation delay and view tick for
server-side lag compensation (`replica.interpolationDelayMs`,
`client.inputContext()`).
`connection.stats` exposes cumulative bytes/messages in and out (Delta bytes
separately) for bandwidth HUDs.

### Generated game clients

Rather than writing `encodeInput`, field names, event ids, correction
tables or the prediction step by hand, generate them from the zone's Rust
definitions: `distributed game-manifest` + `distributed game-client`
(see the [`distributed_game` README](../distributed_game/README.md#generated-clients))
write a typed module per zone that wraps this `GameClient` —
`createArenaClient(options)`, `playerFields(entity)`, `encodeInput`,
`sanitizeInput`, typed `on('hello', …)`. It builds on the SDK's typed
payload codec (`encodePayload`, `decodePayload`, `sanitizePayload`,
`WireType`) and helpers (`onGameEvent`, `decodeGameEvent`, `fieldNumber`,
`mergeCorrections`), which hand-written code may use directly.

**Prediction from WebAssembly.** When the zone declares its step with
`#[predict]`, `distributed game-wasm` compiles it to
`<generated>/wasm/<surface>.wasm` and the generated `create<Name>Client`
predicts with it by default (`prediction: false` turns prediction off, a
`predict` in `prediction` replaces it, `wasm` overrides where the module
is loaded from). The client exposes the step as `prediction`:

```ts
const grid = createGridClient({ url, ticket });            // loads wasm/grid.wasm
await grid.prediction?.ready;                              // instantiated
grid.prediction?.setContext({ seed, cx: 3, cy: -2 });      // steps with a context
```

Underneath, `createWasmPrediction({ source, exportName, state, context,
encodeInput })` wraps a `PredictModule` (`loadPredictModule(source)`:
URL, `file:` URL in Node, bytes, `Response` or compiled module; one
instance per URL) as a `PredictFn`. The ABI is binary: the controlled
entity's predicted fields (`PredictStateLayout`, binary32 floats), the
input payload and the context are written into the module's scratch
buffer and the next state is read back — no JSON, no allocation across the
boundary. Until the module is ready (and, for steps with a context, until
`setContext`), `predict` leaves the state unchanged; the next
reconciliation replays every pending input through the step. Cost
(Node 24 / Chrome 156, Apple silicon): ~0.8–1.2 µs per predicted step, ~8–12 µs
for a reconciliation replaying 10 pending inputs — well under 0.1 % of a
33 ms frame at 30 Hz. Bundlers emit the module as an asset through
`new URL('./wasm/<surface>.wasm', import.meta.url)` (Vite does out of the
box); with `--import-extension js` copy `wasm/` next to the emitted
JavaScript.

**Shared logic and data.** Pure functions the zone declares with
`#[game_pure]` are compiled into the same module; the generated
`create<Name>Pures()` returns one typed, synchronous wrapper per function
(plus `ready`, `loaded()`, `module()`) and shares the instance the
prediction already loaded:

```ts
const pures = createGridPures();
await pures.ready;
const cell = pures.gridCell({ seed, cx: 3, cy: -2 }); // the server's own world generation
```

Underneath, `createWasmPures(source, specs)` calls
`PredictModule.call(exportName, argBytes)` (`dg_pure_<fn>`, payload codec in
and out; calls before `ready` throw). Static data the zone exports with
`GameSurface::constant` arrives as generated constants (`constants.ts`), and
records it serves (e.g. a content pack over HTTP) get generated
`decode<Record>` / `encode<Record>` functions over `decodeValue` /
`encodeValue`.

## Protocol and security boundaries

Every accepted artifact and response is protocol v1 and carries an exact
schema/client-surface binding. Every response is also bound to a server-issued
cache scope, operation ID, and trusted-preset inventory; any supplied record
clocks or index vector are bound to that same scope. Missing, malformed,
stale-schema, or cross-surface evidence fails closed. An exact authorized
payload without a safely comparable index vector may render, but it cannot
advance index clocks/vectors, observations, live resume, or optimistic
confirmation. Independently valid record clocks remain usable.

OIDC credentials authorize transport requests; decoded client claims never
create cache authority. GraphQL remains the API and command proxy layer, while
the service's SQL read models remain authoritative.

Normative architecture and API decisions live in the Distributed GitKB,
including `specs/query-layer/v1/cache-engine`. They are intentionally not
duplicated as decision documents in this package.

## Public entry points

- `@hops-ops/distributed` — GraphQL HTTP/WebSocket, auth, and protocol
  primitives.
- `@hops-ops/distributed/replica` — replica, GraphQL transport, generated
  command runtime, query-plan helpers, and optional persistence.
- `@hops-ops/distributed/diagnostics` — redacted support snapshots and artifact
  inspection.
- `@hops-ops/distributed/sveltekit` — Svelte stores, island SSR composition,
  hydration, auth lifecycle, and tree-local generated bindings.
- `@hops-ops/distributed/sveltekit/vite` — public Vite plugin (GraphQL
  clients and `games` surfaces), virtual module aliases, GraphQL
  HTTP/WebSocket proxy helpers, `generateDistributedGames` /
  `checkDistributedGames`.
- `@hops-ops/distributed/sveltekit/game` — runes state for the UI around a
  game zone (`useGame`, `createGameState`) and `gameTicketSource`.
- `@hops-ops/distributed/sveltekit/game/server` — server-only
  `createGameTicketHandler` and `mintGameTicket` (session-bound zone tickets).
- `@hops-ops/distributed/react` — provider and query hook over the same replica.
- `@hops-ops/distributed/game` — binary `distributed_game` zone protocol
  client: codec, `GameConnection` (handshake, reconnect, clock sync),
  interpolating `EntityReplica`, `Predictor` reconciliation (with
  `createWasmPrediction` for steps compiled from Rust, and `createWasmPures`
  for pure functions compiled alongside them) and the multi-zone
  `GameClient` with handoff, `AdaptiveDelay` and `InputPacer`.
  See `distributed_game/PROTOCOL.md`.

All other subpaths are private and blocked by the package export map.

## Pre-release clean break

The earlier pilot API and persistence format are intentionally unsupported.
There is no `QueryCache`, `CacheTarget`, `ListMergeSpec`, `target/at/by`
addressing, manual cache-policy map, resource wrapper, document store, legacy
command pipeline, or package-owned codegen executable.

To move an existing pilot application:

1. rerun `distributed client` and import its operation/command artifacts;
2. compose one replica through the framework adapter or core transport;
3. remove handwritten cache targets, merge/update callbacks, and invalidation
   policies;
4. discard prior browser cache and SSR payloads rather than migrating them.

Only protocol-v1 generated artifacts and server envelopes are accepted.

## Verification and release

```bash
npm ci
npm run quality
npm run release:dry-run
```

`quality` typechecks generated consumers, runs behavior and adapter suites,
packs and installs the real tarball into clean consumers, verifies bundle
boundaries, and runs `publint`. `release:dry-run` exercises npm's publish
payload without publishing.

Version tags (`vX.Y.Z`) publish with npm provenance through GitHub Actions
trusted publishing. The package currently uses `UNLICENSED` because the
repository has no top-level license file; changing that is an explicit
maintainer decision.
