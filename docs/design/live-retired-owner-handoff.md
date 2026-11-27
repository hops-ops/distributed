# Snapshot stream handoff after owner disposal

A snapshot live stream can begin while a previous route still owns a shared
relationship index. Its buffered frames must not acquire that graph merely
because the old route later disposes: their start fence predates disposal.
However, retaining that fence forever leaves the new route permanently pending.

When an incoming snapshot is blocked by a retired owner's index, reopen that
contending operation at a fresh local start fence. Reject the triggering old
frame and all subsequent callbacks from its retired transport. Only a new
server-authorized initial frame may take over the graph. A still-active owner
continues to block; normal same-scope hydration must not restart retained layout
subscriptions. No application polling, forced refresh, or optimistic-state
suppression is part of this recovery.

Runtime evidence: Forge's creation page received completed/ready rows, but the
prior NewRepository operation owned a shared topology relationship at revision
24, retired at 27, after the new subscription had already started. Entity clocks
advanced while the complete-empty root index remained fenced. The existing
pre-disposal protocol test covers safety; a fresh-receiver continuation covers
liveness after the same boundary.
