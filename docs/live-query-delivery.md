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

Snapshot delivery does not relax read permissions or invent causal evidence.
Changes affecting only denied rows must not produce activity frames. A row
leaving the authorized result disappears from that operation's membership;
absence is not a globally authoritative deletion or tombstone.

Within one current subscription, snapshot frames are ordered. The client fences
older HTTP requests when a live result takes ownership, and fences callbacks
from disposed subscriptions and previous authorization generations. Local cache
membership revisions are not server projection positions and cannot confirm
optimistic commands.

This is a breaking v5 protocol change: `mode` replaces the ambiguous `supported`
boolean. Upgrade server and generated-client runtime together. Old or unknown
wire forms fail closed; applications do not need a polling or reload workaround.
