# Authenticated external facts

`DomainEventOccurrence::capture_external` represents an authenticated fact from
an external ledger, webhook or other durable source. It does not create an
aggregate, command receipt or business authorization. The adapter must verify
the source before calling the typed constructor. Deserializing an occurrence
alone does not authenticate its origin.

The source identity consists of producer, stream, numeric position and member
key. Members of one external transaction share its position, with distinct keys.
The logical occurrence ID depends only on that identity. Changing its descriptor,
body, timestamp or metadata preserves the ID and must fail the existing durable
input fingerprint fence. Retries must reproduce the same canonical bytes. When
the source has no timestamp, an adapter may explicitly use the Unix epoch as an
unknown-time sentinel; it must not present adapter receipt time as source time.

An `external_snapshot` projection applies full-row source snapshots. It orders
each external stream/member independently of actual broker delivery positions;
it does not invent an aggregate sequence. Existing aggregate projection and
command APIs retain their authority and fencing rules. Derived external facts
retain their source provenance, but are not accepted as direct source snapshots.

NATS publishes external facts with a content-bound broker dedup key and a
separate reserved logical occurrence ID header. Thus identical retries within
the broker dedup window collapse, but altered bytes reach durable validation.
Different logical source facts with identical bodies never collapse. Retained
archive reads verify these headers and preserve external occurrences for replay.
Malformed or ambiguous identity headers fail permanently before dispatch and
remain unacknowledged. The supervisor sees the error; the adapter does not
silently terminate the message or advance progress past it.

An identical logical input redelivered at a new broker cursor advances only the
delivery checkpoint within its execution generation. It does not repeat row
changes, observations or business effects. A new execution generation applies
its first delivery normally. Original and alias cursor bindings remain immutable,
and one canonical topology-wide message identity arbitrates concurrent partition
claims. Migration 0009 adds delivery aliases without deleting canonical identity
or failure records. SQL mutation continues to use the existing partition lock.

Permanent input identities and their aliases must be retained for the replay
and source-conflict horizon. A bounded broker dedup window is not such a fence.
An ingress that acknowledges an external cursor must first establish its own
required durable qualification (for example, a history projection atomically
committed with the protocol fingerprint), and retain source replay until then.
Publishing is not approval, and waiting for all UI consumers is unnecessary.

Offline snapshot rebuild preserves these same identities. It distinguishes
original aggregate positions, original external stream/position/member keys,
and derived occurrence IDs; unrelated external facts cannot collide at empty
aggregate fields. Duplicate logical IDs must retain identical canonical bytes.
Aggregate projections still require a complete original aggregate sequence
prefix. External positions may be sparse or shared by different members, so
the source adapter must certify the complete retained source range instead of
inventing a contiguous aggregate history. Rebuild still rejects omitted stored
source versions, changed content at one source position, a different source
claiming an existing row, and derived facts used as snapshot authority. It does
not run business handlers, republish events, or reset delivery cursors.
