# Live queries and authorization

`@live` keeps an authorized query current. Resuming missed projection changes is
a separate capability: a partition-wide cursor can reveal activity outside a
row-filtered result, even when the rows themselves are protected.

Every live response declares `extensions.distributed.live.mode`:

- `snapshot`: a fresh authorized replacement result, with `reset: true` and
  `cursors: []`. It carries no comparable index vector or projection observations.
  The client keeps listening and reconnects with a fresh query, not a resume token.
- `resumable`: a result with matching, nonempty index and cursor vectors. Existing
  resume validation, reset, replay and causal reconciliation rules apply.

A failed execution has no result and therefore no `live` metadata. It is sent
as the terminal failure frame described below.

## Shared execution at the origin

Subscribers whose live roots are the same read share one producer. A catalog
root is the same read when its role, compiled SQL and every bind (row policies
and claim values included) match and, with the causal protocol on, its cache
scope does too; an external root when its role, root, arguments and session
match. The first subscriber's frame is the producer's first execution; a later
one opens with its own read, which honours its own resume cursors. After that,
each change that touches the read is debounced (100ms) and executed once, and
the frame goes to every subscriber, each with its own hash gate and envelope.
A subscriber whose cursors are not where a frame replays from (it joined
between frames, or skipped one while slow) catches up with one read of its
own, so replay stays exact. The producer keeps only the latest frame, so a
slow subscriber never holds the others back.

At most `GraphqlEngineBuilder::live_concurrency` live executions (default
8) run at once per engine: first reads, refreshes and catch-ups together.
When the last subscriber of a read leaves, its producer stops.
`GraphqlEngine::live_producer_count()` counts producers (distinct reads);
`GraphqlEngine::live_subscriber_count()` counts subscription streams.

## Failed live executions

A live execution that fails (for example a storage error or statement timeout)
has no result. The server sends exactly one **failure frame** and then completes
the operation. The failure ends that subscription: the server sends no more
frames for it and never attaches a later execution's metadata to the failure
frame. A failed shared execution ends every subscription that shares it. A
failure frame has:

- `data` that is `null` or absent;
- a nonempty `errors` array, where each entry is an object with a string
  `message`;
- a valid base `extensions.distributed` envelope (`protocolVersion`,
  `schemaHash`, `authorizationGeneration`, `cacheScope`, `operation`, and
  `trustedPresets` when the surface has any), with no `snapshot`, `live` or
  `command`.

The client checks the envelope's binding, schema, operation and authorization
generation in the same way as for any other response. The failure frame then
works like an HTTP error response: it admits nothing. It writes no data or
membership, advances no cursor or operation generation, takes no ownership and
confirms no command. The client shows the original GraphQL errors for that
operation and keeps any previously admitted data readable. It closes the
failed stream. After a bounded backoff (1s, doubling to at most 30s, each
delay shortened by a random share of up to half so that clients do not reopen
in step), it refreshes the operation over HTTP and opens a fresh subscription
for the operation's current watches. That subscription resumes from the last
admitted cursors, if there are any. The backoff resets when an admitted frame
arrives, and that frame also replaces the errors. This is how a live query
recovers after its storage comes back, without a page reload. Disposing the
last watch or ending the authorization generation cancels a pending reopen.

A lost connection takes the same path. The client keeps one WebSocket per
endpoint and credential and runs every live query over it as a separate
`graphql-transport-ws` subscription. When that socket errors or closes, each
of its subscriptions ends with a retryable error and is reopened as above; the
first one to reopen dials the socket again and the others share it. The
connection keeps its own backoff, so the queries of one page cause one dial
per attempt. A close that reconnecting cannot fix is not retried: a refused
`connection_init` (1002) and the 4400-4499 codes, except 4408 and 4401 with
the reason `token expired`. An `error` message for one subscription is retried
only when it is `LIVE_RESET_REQUIRED` or `FRESHNESS_SCOPE_CHANGED`.

A lost connection is not a query error. The query keeps its data, status and
`errors`; its `live` state is `reconnecting` until the reopened stream delivers
a frame, and an HTTP refresh that fails meanwhile adds no error either. The
replica reports the loss once, as `liveConnection` (`online` or
`reconnecting`, with `subscribeLiveConnection`; in SvelteKit the store
`useDistributedLiveConnection()`), so an application shows one
"reconnecting" notice instead of an error in every query.

Whichever of the HTTP refresh and the reopened stream answers first is
admitted; the other is fenced by the operation generation, so the cache never
moves back.

Only an error-only frame is a failure frame. A live frame that carries non-null
`data` (including partial data with errors) without both `snapshot` and `live`
is invalid, as are frames with `errors` that are absent, empty or malformed and
lack live metadata, and frames with a `snapshot` or `live` but not both. A
response without the `extensions.distributed` envelope is invalid as before.
Intermediaries that cannot relay a failure frame, such as shared gateway live
fan-out, end the consumer with `LIVE_RESET_REQUIRED` instead.

Snapshot delivery does not relax read permissions or invent causal evidence.
Changes affecting only denied rows must not produce activity frames. A row
leaving the authorized result disappears from that operation's membership;
absence is not a globally authoritative deletion or tombstone.

Within one current subscription, snapshot frames are ordered. The client fences
older HTTP requests when a live result takes ownership, and fences callbacks
from disposed subscriptions and previous authorization generations. Local cache
membership revisions are not server projection positions and cannot confirm
optimistic commands.

A subscription also records its local start order. Snapshot delivery can take
over shared relationships owned by queries that preceded that subscription,
including SSR seeds from a layout or another island. This lets an initially
empty result acquire rows without waiting for another page load. It does not
override an independently active live stream or query ownership acquired after
the subscription started; those results have no safe cross-stream ordering.

Independent streams often share an index without disagreeing about it. A
layout and a page may both select
`repositories(where: { id: { _eq: $id } }, limit: 1)` and then different
relationships below that row. A snapshot frame whose shared indexes have the
same membership as the independent owner's is admitted. Each such index must
contain the same normalized record keys in the same order and the same null
value, and the owner's index must be complete and not stale. The owner keeps
those shared indexes, and the frame writes only its other indexes and its
records. The resulting graph is exactly the frame's server result, so nothing
is fabricated and no cross-stream order is assumed.

The whole frame is rejected as before if any shared membership differs or
cannot be established. That includes GraphQL errors, operation-local embedded
rows and rows without record evidence. The subscription is then reopened after
another operation next writes one of those shared indexes. The fresh result is
checked against the owner's new membership, so a stream whose frame arrived
before the owner caught up does not wait for its own next server change.

When the last watch for a live operation is disposed, its local ownership is
retired at a monotonically increasing local boundary. A later live subscription
may take over an incomparable shared index only when that subscription started
after the retirement boundary. A subscription that started while the previous
stream was still active remains fenced against late frames from that stream.
Reopening the retired operation clears its retirement marker before transport
callbacks can arrive, so the reopened stream becomes an active owner again.
Retirement is local replica metadata; it does not claim that the server's
projection stopped or provide a server ordering signal.

This is a breaking v5 protocol change: `mode` replaces the ambiguous `supported`
boolean. Upgrade server and generated-client runtime together. Old or unknown
wire forms fail closed; applications do not need a polling or reload workaround.

## External read sources and leaderboard roots

Roots that are not read-model roots come from an external read source
(`distributed::graphql::surface::ExternalReadSource`, attached to a catalog
Surface with `Surface::with_external_sources` and served with
`GraphqlEngineBuilder::external_read_source(executor)`). The source declares
its roots, result object types, per-role grants, the session claims it reads,
its contract entry and its client-manifest entry; the engine renders,
authorizes, fingerprints and exports them and runs each call through the
source's `ExternalReadExecutor`, which returns the value, a scope (bound
into the protocol scope token) and the change keys a live read waits on.

Sorted-set leaderboards (the `distributed_leaderboard` crate, Redis with its
`redis` feature) are the one external read source:
`external_read_source(Arc::new(LeaderboardRoots::new(LeaderboardSurface, store)))`. For a surface
named `xp`, a role's grant (`LeaderboardGrant`) selects the roots:
`xpLeaderboard(first)`, `xpLeaderboardAround(radius)`, `xpMyRank` and, only
with `rank_any_member`, `xpRank(member)`. A board family
(`LeaderboardSurface::family`) adds `board: String!` to each. The viewer
roots take the member from the verified session claim (default `x-user-id`,
`LeaderboardSurface::member_claim`), never from an argument, and fail with
`FORBIDDEN` when the claim is absent. The same declaration feeds the static
SDL, the client manifest (`external.leaderboards`, omitted when empty so
other manifests and fingerprints are unchanged) and the generated
`leaderboards.ts` (typed documents and `@live` subscription documents).

`xpLeaderboard`, `xpLeaderboardAround` and `xpMyRank` are also subscription
roots. They re-execute when the board's change key
(`leaderboard:{board}`) or the every-board key (`leaderboard:*`) is
published, which `distributed_leaderboard::change_listener(&engine)` does
when registered on the
`LeaderboardProjector` that writes the board (applied changes and rebuild
promotions; duplicates are silent). Pushes are hash-gated like SQL live
queries, and a failed execution is terminal. With the causal protocol on,
a leaderboard result carries an incomplete snapshot
(`recordsComplete: false`, `indexesComparable: false`) and live frames use
`snapshot` mode with `reset: true`: leaderboard rows are never normalized
into the replica cache or resumed from cursors.

The listener is in-process. When the projector runs in another process
(a zone server projecting kills into Redis while a GraphQL service serves
the same boards), the change goes over the application's bus:

- the projector's process registers `LeaderboardChangePublisher::listener()`,
  which publishes a `distributed.leaderboard.changed` event
  (`{ "board": "<name>" }`) per changed board. Pending boards are a bounded
  set (`spawn(publisher, capacity)`): repeated changes to a board coalesce,
  and a change to one board more than fits replaces the set with a single
  every-board notice (`{}`), so a slow or down bus costs bounded memory and
  subscribers resync instead. Retryable publish errors are retried with
  backoff; permanently refused notices are dropped;
- the GraphQL process runs `distributed_leaderboard::change_handlers(&engine,
  group)` with `bus.subscribe(..)` under a consumer group of its own, so
  every GraphQL process receives every notice and republishes
  `leaderboard:{board}` (or `leaderboard:*`) on its `ChangeHub`.

A notice only names the board; the subscriber re-reads its store, so
duplicate or replayed notices push nothing new.
