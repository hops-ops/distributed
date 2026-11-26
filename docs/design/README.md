# Design notes

Each note explains why one narrow behaviour is the way it is, usually after
an incident or a review found an edge case. They describe built behaviour
unless the note says otherwise. For how to use the framework, read the
[guides](../guides/) instead.

| Note | Status | Topic |
|---|---|---|
| [Event delivery during application reload](reloading-event-delivery.md) | Built | How consumers treat the `distributed dev` generation gate |
| [Session refresh continuity](session-refresh-continuity.md) | Built | Credential and route data stay paired across SvelteKit refreshes |
| [Snapshot stream handoff after owner disposal](live-retired-owner-handoff.md) | Built | Live snapshot streams and a retired relationship-index owner |
| [Immutable protocol manifest reuse](protocol-manifest-reuse.md) | Built | Reusing a client surface's derived manifest |
| [Process roles and system slices](process-roles.md) | **Not built: open question** | Cutting one application into writer, projector and query processes |
