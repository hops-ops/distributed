# Bounded read-model scans

`RelationalReadModelQueryStore::scan_read_model` reads normalized rows with
declared types and row versions. It does not load aggregates, authorize commands,
or expose arbitrary SQL. Unsupported adapters return an explicit error.

```rust,ignore
use distributed::read_model::ReadModelScanRequest;
let request = ReadModelScanRequest::new::<PullRequestView>(scope, 100, cursor)?;
let page = store.scan_read_model(request).await?;
let rows = page.typed::<PullRequestView>()?;
let cursor = page.next;
```

The initial capability supports a single non-null text primary key, at most 16
typed equality filters and 1–100 returned rows. Filters support text, nullable
NULL, booleans and signed/unsigned integers within the SQL signed range. Text
filters and cursor keys are bounded to 4096 bytes; NUL is rejected. Composite
keys, joins, includes, offsets, JSON/float predicates and operational tables are
not supported. Schemas and filter types are validated before storage access.

The opaque cursor binds the exact schema and equality scope. Only page size may
change. In-memory, queued and SQLite/PostgreSQL repositories retain row versions
and use bytewise ascending key order. The SQL adapter materializes at most one
extra row to decide whether a next page exists; the in-memory adapter keeps at
most that many selected rows while scanning its test/development store.

Pages are separate current reads, not a multi-page transaction. Updates between
pages are visible; new keys before an already-consumed cursor are not revisited.
Consumers must validate command fences against the owning aggregate and provide
event-driven reconciliation for late rows. In particular, an empty first scan
does not prove a concurrently created row will never need processing. This API
does not provide an authorization bypass or replace a public GraphQL read policy.
