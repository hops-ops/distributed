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

## Failed live executions

A live execution that fails (for example a storage error or statement timeout)
has no result. The server sends exactly one **failure frame** and then completes
the operation. The failure ends that subscription: the server sends no more
frames for it and never attaches a later execution's metadata to the failure
frame. A failure frame has:

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
failed stream. After a bounded backoff (1s, doubling to at most 30s), it opens
a fresh subscription for the operation's current watches. That subscription
resumes from the last admitted cursors, if there are any. The backoff resets
when an admitted frame arrives, and that frame also replaces the errors. This is
how a live query recovers after its storage comes back, without a page reload.
Disposing the last watch or ending the authorization generation cancels a
pending reopen.

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
