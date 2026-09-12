# Shared live GraphQL

Enable `gateway-graphql-native,gateway-delivery`, select `live_sharing` on the
GraphQL declaration, and bind `NativeDelivery::live(LiveLimits::default())`.
Snapshot caching, concurrent queries and live sharing can be selected separately
with `NativeDeliveryOptions`. The origin needs the authenticated delivery control
path and version coverage described in [snapshot-cache.md](snapshot-cache.md).
Custom embedded executor routers conservatively keep independent subscriptions;
the canonical engine and eligible remote whole-operation executors support sharing.

Every consumer is admitted by the origin, including credentials supplied through
GraphQL `connection_init`. The gateway passes these credentials to the backend's
existing validator; it does not derive a subject from cookies or unverified JWTs.
A remote client initially uses a temporary origin WebSocket for the real connection
acknowledgement. It owns no subscription and closes before operation coordination.
Thus 100 clients incur 100 admissions and handshake costs but can share one steady
upstream subscription and producer. HTTP control requests do not execute result SQL.

Groups bind the exact document, variables, origin subject/cache scope, policy,
schema and protocol. Each consumer keeps its own transport ID, expiry, queue and
freshness requirements. Different resume cursors start independent replay. Handoff
requires the same operation plus an exact resumable, comparable cursor vector and
matching data; the consumer's replay frames remain queued before future shared
frames. Unknown cursors keep independent streams. Equal data alone never proves
cursor equality. Duplicate suppression hashes the whole data and protocol envelope,
so new confirmation evidence is delivered even when values are unchanged.

Cursorless `live.mode = "snapshot"` responses cannot prove shared replay handoff;
they retain independent authorized subscriptions and reconnect with fresh queries.
See [live query delivery](../live-query-delivery.md) for the wire contract.

Defaults bound a coordinator to 256 groups, 1,024 consumers per group, 16 pending
frames per consumer, 1 MiB per full frame, eight retained history frames and a
one-hour group lifetime. Native ingress bounds socket/request counts and wire
message sizes as well. Frames share immutable storage across consumer queues.
If a slow consumer cannot preserve all evidence within its queue, it receives
`LIVE_RESET_REQUIRED`; a blocked socket is closed so it must reconnect. No
latest-value replacement silently discards confirmation proof. Group deadline,
upstream loss and incomplete/invalid origin envelopes also require recovery.

Dropping a consumer releases only its lease. Last leave aborts the actual upstream
stream, socket and origin change-feed receiver, including a pending origin SQL
read. An expired credential cannot continue receiving shared frames or own other
consumers' upstream: the remaining valid consumer reconnects with its own origin
credential and the last proven resume cursor. Normal GraphQL completion drains
queued frames; unexpected transport loss requires reset. Reconnect authenticates
again. Shared outbound sockets do not hibernate for free.

`NativeDelivery::live_counts()` reports active groups, consumers, cumulative source
attempts, resets, upstream frames, exact duplicate frames and safe handoffs. Source
attempts include credential reconnects; they are not a count of active producers.
`GraphqlEngine::live_subscriber_count()` separately reports actual origin producer
subscriptions. No credentials or subject identifiers appear in these counters.

Disable `live_sharing` and remove the live delivery resource to restore independent
subscriptions. Existing client resume/reset and response-sealing behavior remains
in force. No migration or new credential is required by live coordination itself.
