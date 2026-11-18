## Assessment: SQL persistence slice

**Overall health.** The Postgres/SQLite split is clean: one generic implementation over `SqlxRepoBackend` and `SqlxReadModelBackend`, with only four `DB::BACKEND == "postgres"` string branches and no per-dialect copies of logic. I found no injection surface. The weight and the risk sit in the projection protocol (7.2k non-test lines): its rules are implemented a second time for in-memory, it costs about 28 statements per projected event, and its Postgres tests never run in CI.

**Three things most worth changing:**
1. Move the protocol decisions into pure functions that both backends call, and make the tests one conformance suite that actually runs on Postgres.
2. Cut round trips and locks on the hot paths: per-event projection commit, the table ownership-fence `FOR UPDATE`, the per-row execution snapshot loop, and the unindexed causation lookup.
3. Squash the 13 migrations into one schema with a single timestamp representation; this removes about half of the dialect trait, several unused indexes and the compat wording.

**How this was read.**
- **Read in full by me:** `sqlx_repo/mod.rs`, all of `sqlx_repo/repo/`, `postgres_repo`, `sqlite_repo`, `sqlx_repo/read_model/*`, `table/sql.rs` (non-test), `table/plan.rs`, `table/mod.rs`, `projection_protocol/{mod,types,helpers,locks,partitions}.rs`, `store_impl.rs:1-700`, Postgres migrations except 0003 body (0003 reviewed by index/constraint grep), and `repository/sql.rs` + `sql/outbox.rs` for comparison.
- **Read by two sub-reviewers, spot-checked by me:** `reads.rs`, `identity.rs`, `store_impl.rs:649-1521`, `rebuild.rs`, both test files, in-memory `projection_protocol/`. Findings from them are marked "sub-review". Within that, test bodies at `tests.rs:1168-1384`, `1525-1994` and `2780-3095` and most in-memory test bodies were not read by anyone.
- **Only outlined or grepped:** `writes.rs:300-1200`, `table/{metadata,registry,mutation,catalog,error}.rs`, `read_model/{plan,workspace,in_memory,load}.rs`, SQLite migrations (diffed against Postgres only).
- **Not done:** a third sub-review of `table/` and `read_model/` could not be launched, so that area is thinner. Nothing was executed; query-plan claims are from reading SQL against DDL.

---

### 1. Postgres projection-protocol tests never run in CI
- Category: B/E   Severity: high   Confidence: CONFIRMED
- Where: `src/sqlx_repo/projection_protocol/postgres_tests.rs:32,56,645,756,1017`; `.github/workflows/integration-postgres.yaml:28,41-66`
- What: all five tests begin with `let Ok(url) = std::env::var("DISTRIBUTED_TEST_POSTGRES_URL") else { return; }`. That variable appears nowhere else in the repo. The workflow sets `DATABASE_URL` and runs named `--lib` filters that do not include these tests. They are not `#[ignore]`d, so they report green.
- Why it matters: the only Postgres coverage of commit, repair, retention, `pg_notify` and REPEATABLE READ snapshots is vacuous. That includes `postgres_cancelled_begin_does_not_poison_the_next_read_snapshot`, the regression test for the most recent fix (d9e9bd20).
- Change: use the env var CI sets, fail when it is absent in CI, and use unique per-run topology ids (sub-review: `postgres_projection_protocol_runtime_conforms` uses fixed ids and is probably not re-runnable). S.

### 2. Projection-protocol rules are implemented twice and already differ
- Category: C/A (principle 7)   Severity: high   Confidence: CONFIRMED
- Where (SQL vs in-memory):
  - `writes.rs:298-383` vs `state_impl.rs:558-644` (`next_record`; I diffed them, identical apart from indentation).
  - `identity.rs:845-1061` vs `state_impl.rs:124-362` (input classification).
  - `reads.rs:271-438` vs `direct_projection.rs:7-131`.
  - `store_impl.rs:1159-1252` vs in-memory `store_impl.rs:1124-1199` (repair).
  - Also graph budgeting, change-read window, compaction bounds and the failure preamble. `PartitionState` and `InputDisposition` are each defined twice (`sqlx types.rs:4,65`).
- What: about 750 lines of semantic decisions are interleaved with fetches in each backend. A shared home already exists (`src/projection_protocol/store/backend_helpers.rs`, `validate_ownership_batch`, `validate_snapshot_write`).
- Divergences today (sub-review):
  - Causation evidence is ordered by hashes in SQL (`reads.rs:1664-1666`) and by canonical bytes in memory (`store_impl.rs:852-867`).
  - A live record owned by another partition returns `InvalidBatch` in memory (`state_impl.rs:4-33`); SQL has no pre-check and surfaces a unique-index storage error.
  - `projection_changes` on SQL returns a corruption error for an empty or non-contiguous page (`reads.rs:2297-2315`); in-memory returns whatever is there.
- Change: pure functions over already-loaded facts (`next_record`, `classify_input(receipt_by_cursor, receipt_by_message, current_cursor, …)`, `plan_repair`, `change_read_start`, one ordering function). Backends only fetch and persist. L; start with `next_record`, which is S.

### 3. Conformance suite covers six scenarios and has no Postgres implementation
- Category: B/C   Severity: high   Confidence: CONFIRMED (sub-review; I confirmed the suite file exists)
- Where: `src/projection_protocol/store/scenario_tests.rs` (trait `ProjectionProtocolScenario`, six scenarios); impls at sqlite `tests.rs:375` and in-memory `tests.rs:544`.
- What: everything else is written per backend (17 SQLite, 28 in-memory, 5 Postgres tests).
  - Near-verbatim twins include `…coherent_execution_and_graph_snapshots…` (sqlite 2067 / pg 643 / mem 3398) and the `four_position_…_commit_failure_matrix` (2161 / 753 / 3601).
  - Fixtures such as `graph_parent_schema`, `graph_codec`, `graph_mutation`, `input`, `batch` and `matrix_*` are copy-pasted three times (sqlite 148-302, pg 198-373, mem 102-259).
  - Estimated removable: about 750 helper lines plus 600-700 twin-test lines.
  - One Postgres test is 862 lines and replays about eight SQLite scenarios serially.
- Change: move fixtures and scenarios into the suite, add hooks `fail_nth_physical_write` and `corrupt_physical_row`, and add a Postgres impl. Keep backend-specific only what is: cancelled-BEGIN, `pg_notify` on commit/rollback, REPEATABLE READ, WAL restart. L.

### 4. About 28 SQL statements per projected event
- Category: D   Severity: high   Confidence: CONFIRMED (counted from code)
- Where: `store_impl.rs:333-647` and its helpers.
- What, for one new input with one mutation and one owned model:
  - BEGIN; `verify_registered_topology_in_tx` (1).
  - `lock_partition_in_tx` (3: upsert, generation insert, generation verify; `partitions.rs:95-143`).
  - `validate_input_identity_in_tx` (3); `classify_validated_input_in_tx` (3; `identity.rs:948,981,1006`).
  - Inbox check (1); `ensure_partition_ownership_in_tx` (2 per declared model, every commit; `writes.rs:31-96`).
  - Per mutation: `record_in_tx` + `physical_row_exists_in_tx` + row write + change insert + record upsert (5).
  - Cursor (1), identity (2), receipt (1), inbox (1), head update (1), retention delete + second partition update (2; `writes.rs:1233-1270`), notify, COMMIT.
  - The same `projection_partitions` row is written three times per event.
- Why it matters: every statement is a network round trip on Postgres with the partition row lock held, which caps a partition at roughly tens of events per second. Mutations are applied one statement per row (`store_impl.rs:588-596`).
- Change:
  - Fold the partition lock, head and compacted-through into one final UPDATE.
  - Load receipts, cursor and identity in one statement.
  - Verify topology and model ownership once per process (it is immutable after registration), not per event.
  - Multi-row insert for changes and observations.
  - M-L; the pure-function refactor in #2 makes the fetch plan explicit.

### 5. Ownership-fence `FOR UPDATE` serialises every same-transaction read-model commit per table
- Category: D   Severity: high   Confidence: CONFIRMED
- Where: `projection_protocol/locks.rs:60-99`; called from `repo/commit.rs:83` and `repo/read_models.rs:28`.
- What: for each table an ordinary write plan touches, the code runs `INSERT … ON CONFLICT DO NOTHING`, then `SELECT … FOR UPDATE` on `projection_table_ownership_fences` (Postgres), then a `SELECT` on `projection_causal_tables`. That is three statements per table, and an exclusive row lock held until COMMIT.
- Why it matters: two commands on different aggregates that stage a read-model row in the same table cannot commit concurrently on Postgres. The lock is taken before the stream-version check and event insert, so the whole commit is serialised.
- Change: writers take `FOR SHARE`; only `register_projection_models` (`store_impl.rs:52`) takes `FOR UPDATE`. Batch the tables into one statement with `= ANY($1)`. S.

### 6. Causation-evidence query has no usable index and its table only grows
- Category: D   Severity: high   Confidence: CONFIRMED (query and DDL re-opened)
- Where: `reads.rs:1646-1668`; `migrations/postgres/0003_projection_protocol.sql:444-462,655-660`; caller `microsvc/dependencies.rs:131` (sub-review).
- What: the predicate is `causation_id = ? AND topology_hash IN (…)` with no `partition_hash`. The primary key and `projection_observations_causation_idx` both lead with `(topology_hash, partition_hash, causation_id)`. Observations are never deleted. The failures query at `reads.rs:1778` has the same shape.
- Change: index `(topology_hash, causation_id)` on observations and failures in both dialects; drop the current causation index, which is a strict prefix of the primary key. S.

### 7. Execution snapshot batch issues one multi-join statement per row on the projector path
- Category: D   Severity: high   Confidence: CONFIRMED
- Where: `reads.rs:1086-1097` loops `read_projection_query_snapshot_in_executor` per request; bound `MAX_PROJECTION_QUERY_BATCH_ROWS` = 4096; caller `microsvc/projector/context.rs:109` (sub-review).
- What: the same pattern is at `store_impl.rs:985`, `reads.rs:1128,1152` (graph snapshot) and `rebuild.rs:42,118` (four statements per row, up to 10,000 rows, inventory run twice).
- Change: one keyed set query per model, as `read_projection_live_record_batch_in_executor` already does. M.

### 8. Keyset scan forces `COLLATE "C"` on a key column created with the default collation
- Category: D   Severity: medium   Confidence: CONFIRMED for the mismatch, PLAUSIBLE for the plan
- Where: `sqlx_repo/read_model/scan.rs:26-61`; DDL at `table/sql.rs:140-144,251`.
- What: scans emit `WHERE "pk" COLLATE "C" > $1 ORDER BY "pk" COLLATE "C" LIMIT n`, but generated tables declare the key as plain `text`. On a database whose default collation is not C, the primary-key index cannot serve that predicate or ordering, so each page is a full scan plus sort. Equality filters get the same `COLLATE` (line 36-38), defeating declared indexes on text columns.
- Change: generate `text COLLATE "C"` for text columns on Postgres (this also makes ordering identical to SQLite and in-memory everywhere) and drop the per-query `COLLATE`. Breaking for existing table DDL. S.

### 9. Timestamps are stored three or four ways per dialect, and that is half the dialect seam
- Category: F/C   Severity: medium   Confidence: CONFIRMED
- Where: `migrations/sqlite/0001` (TEXT `secs.nanos`, with `DEFAULT CURRENT_TIMESTAMP` in a different format the codec cannot parse; `repository/sqlite_codec.rs:23-35`), `sqlite/0004` (REAL epoch for the ledger), 0010 (REAL), 0012 (ms integers). Postgres mixes `timestamptz`, `double precision` and `BIGINT` ms.
- What this forces:
  - `sqlite_repo/mod.rs:144-177` decodes "text, else real".
  - `CAST(created_at AS REAL)` in ORDER BY and WHERE (`sqlite_repo/mod.rs:67`, `repository/sql/outbox.rs:134-167`), so `outbox_messages_claimable_idx` cannot serve range or order.
  - About 13 of the roughly 27 items in `SqlxRepoBackend` exist only for timestamps (`repo/backend.rs:262-369`), plus four hand-written `EXTRACT(EPOCH …)` select lists in `postgres_repo/mod.rs:59-109`.
- Change: one representation for all framework tables in both dialects, BIGINT epoch micros (schedules already use ms). The select lists become dialect-identical and the timestamp hooks, `TimestampValue`, `ORDER_BY_CREATED_AT` and `SqlPart::TimestampCompare` go away. Breaking schema change; do it with #11. M.

### 10. Two parallel dialect abstractions
- Category: C/F   Severity: medium   Confidence: CONFIRMED
- Where: `repository/sql.rs` (`Statement`/`SqlPart`/`SqlExecutor`, used by event store, ledger, outbox and schedules; runs on SQLx and cell SQL) vs `SqlxRepoBackend` + `SqlxReadModelBackend` + `QueryBuilder<DB>` (projection protocol and read models).
- What:
  - `repo/executor.rs:119-127` mirrors eight consts from one trait to the other.
  - `stream_version` exists in both (`repo/commit.rs:520` and `repository/sql.rs` `stream_version`).
  - The Postgres claim's `RETURNING` list (`postgres_repo/mod.rs:300-318`) repeats `OUTBOX_SELECT` by hand.
  - The `QueryBuilder<DB>` style costs 587 higher-ranked `where` lines in non-test `sqlx_repo` (419 of 7,204 lines in the five big protocol files), repeated on every function.
  - The four string branches (`scan.rs:26`, `store_impl.rs:874`, `reads.rs:2169`, `locks.rs:78`) should be trait items.
- Change: pick one seam. Extending `Statement`/`SqlExecutor` to the protocol and read-model path removes the generic bounds entirely and is the only route to running the protocol on a cell executor; otherwise delete the mirrored consts and add `READ_SNAPSHOT_BEGIN`, `ROW_LOCK` and `TEXT_COLLATION` to the backend trait. L (or S for the minimal version).

### 11. Thirteen migrations for an unreleased schema, hand-mirrored per dialect
- Category: F   Severity: medium   Confidence: CONFIRMED
- Where: `migrations/{postgres,sqlite}/0001-0013` (2,237 lines).
- What:
  - 0004 rewrites `projected` to `atomic` (a PL/pgSQL constraint-drop loop on Postgres, a full table rebuild on SQLite).
  - 0005, 0007, 0008 and 0013 are `ALTER … ADD COLUMN`.
  - Comments describe data that cannot exist ("history written before program identities existed", "old cell-only rows").
  - `cell_alarms` (0011) is created in Postgres "so the inventory stays identical across dialects" although only cells use it.
  - Real divergence: the outbox status-shape `CHECK` exists only in Postgres (`postgres/0001:76-100`; SQLite stops at `CHECK (attempts >= 0)`). Sub-review found 0003 otherwise equivalent apart from type and CHECK spelling.
  - The outbox `published` status and `published_at` column are never written by SQL: completion deletes the row (`repository/sql/outbox.rs:228`).
- Change: squash to one initial migration per dialect, drop `published`, `published_at` and Postgres `cell_alarms`, and add a schema-parity test that introspects both. M.

### 12. Indexes no query uses, including two on the append path
- Category: D   Severity: medium   Confidence: CONFIRMED by rg of every `FROM`/`JOIN` on these tables in `src/`
- Where and what:
  - `aggregate_events_event_version_idx` and `aggregate_events_recorded_at_idx` (`postgres/0001:21-25`): every `aggregate_events` query filters on the primary key only (`repo/streams.rs:50`, `repo/commit.rs:533,577`, `repository/sql.rs:269,290`).
  - `outbox_messages_source_idx`: no predicate on source columns anywhere.
  - `projection_records_change_idx`, `projection_input_receipts_causation_idx`, `projection_changes_causation_idx`, `projection_changes_record_idx` (`0003:647-691`): changes are only read and deleted by primary key.
- Why it matters: two extra index writes per event appended, three per change row.
- Change: drop them in the squash. S.

### 13. Tests coupled to internals, one compat test, some weak assertions
- Category: B   Severity: medium   Confidence: CONFIRMED (sub-review; I re-opened `tests.rs:1120-1135`)
- Where:
  - `sqlite_rejects_tampered_failure_digest_before_any_protocol_write` (1490-1521): `COUNT(*)` on six internal tables plus an exact error string.
  - `four_position_sqlite_…matrix…` (2227-2342) and `sqlite_retention_prunes_exact_prefix…` (2639, 2702): physical row counts where the trait read already proves the behaviour.
  - `sqlite_modeled_projection_identity…null_history_stays_readable` (1125-1162): NULLs `program_id` to simulate pre-identity rows, which is pre-release compat.
  - `sqlite_projection_change_executor_read_uses_existing_snapshot` (2720): no concurrent writer, so it would pass with no snapshot.
  - About 32 assertions match `message.contains(…)` on `InvalidBatch`.
  - 9 of 23 SQLite tests issue raw SQL.
- No test asserts exact generated SQL for the protocol. The `table/sql.rs` DDL tests use `contains` on rendered DDL, which is reasonable for a generator.
- Gaps: no SQL twin for `concurrent_same_input_applies_once` (in-memory 2889); `projection_failure_location` (`store_impl.rs:1461`) has no direct test in any of the three files.
- Change: keep trigger and corruption setup, replace count assertions with trait reads, delete the compat test, add typed error variants. M.

### 14. Row decoding is written out by hand, column by column
- Category: C   Severity: medium   Confidence: CONFIRMED
- Where: `.try_get(` occurs 89 times in `reads.rs`, 44 in `identity.rs`, 37 in `writes.rs`, 20 in `partitions.rs`, each with its own `protocol_storage_error::<DB>("decode …")` closure (292 such calls across the five files).
- What repeats (sub-review):
  - The source-scope block is decoded five times (`identity.rs:66-119,194-257,553-631,794-838`, `reads.rs:875-975`).
  - Record metadata three times (`writes.rs:188-296`, `reads.rs:722-830,2063-2126`).
  - The `0/1` boolean match eight times.
  - `partitions.rs:374-492` is 120 lines decoding one optional joined failure.
- Change: `col::<T>(row, name)` that derives the error context, `bool01`, `decode_source_scope`, `decode_record_metadata`. Roughly 600 lines removed. M.

### 15. Oversized files doing several jobs
- Category: F   Severity: medium   Confidence: CONFIRMED
- Where and proposed split:
  - `reads.rs` (2394): tests at 3-121 belong in tests; `apply_same_transaction_projection_in_tx` (271-438) is a write and belongs in `direct_projection.rs` (as in-memory has it); the 620-line `read_projection_query_snapshot_in_executor` (448-1069) splits into build/decode pieces; then `evidence.rs` (1306-1851), `live_records.rs` (1855-2135), `changes.rs` (2198-2394).
  - `store_impl.rs` (1521): keep the trait impl as thin delegation; move `register_projection_models` (32-331, eight sequential lookups per declaration), failure recording (649-798), repair (1147-1333) and compaction (1335-1433) into their own modules.
  - `identity.rs`: row lookups vs classification (the latter mostly disappears with #2).
  - `tests.rs` and `postgres_tests.rs` are `include!`d (`mod.rs:84,87`) rather than declared as modules.
- Size: M, mostly mechanical after #2 and #14.

### 16. Single-row protocol reads use two snapshots and can report corruption that is not there
- Category: E   Severity: medium   Confidence: CONFIRMED for the code, race by reasoning
- Where: `store_impl.rs:807-830` (`projection_checkpoint`), also 851-859, 1111-1119, 1443-1457.
- What: `load_partition(self.pool())` reads the partition on one connection, then a new `begin()` reads the cursor. A commit between the two makes `verify_stored_change` (`identity.rs:21-31`) see a change position above the head and return "corrupt projection protocol storage". The transaction is never committed.
- Change: one joined statement or `with_projection_read_snapshot`. The sub-review found no non-test caller for `projection_checkpoint`, `projection_observation`, `projection_graph_snapshot`, `projection_query_snapshot_batch`, `compact_projection_changes` and `projection_live_record_batch` on the trait (PLAUSIBLE; macro callers not ruled out), so deleting them may be the better fix. S.

### 17. Corruption and caller errors share one variant
- Category: E   Severity: low   Confidence: CONFIRMED
- Where: `projection_protocol/helpers.rs:10-15` (`corrupt_storage` returns `ProjectionProtocolError::InvalidBatch`).
- Why it matters: callers cannot tell "stored data is damaged" from "your batch is wrong", and tests match on message text.
- Change: a `StorageCorruption` variant. S.

### 18. Read-snapshot helper costs three extra round trips and is duplicated
- Category: D/E   Severity: low   Confidence: CONFIRMED
- Where: `reads.rs:2151-2196`; copy at `store_impl.rs:874-962`.
- What: `begin()`, then a separate `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY`, then COMMIT, around reads of one or two statements. If rollback fails, the original error is discarded (`reads.rs:2187-2193`). `read_projection_changes_in_snapshot` carries a test-only `after_state` future in its production signature (`reads.rs:2360`; production passes `ready(())`).
- Change: one `BEGIN ISOLATION LEVEL …` via a backend hook, reuse the helper, return the original error, remove the seam. S.

### 19. Outbox claim: Postgres index does not match the predicate; SQLite claims row by row
- Category: D   Severity: medium   Confidence: PLAUSIBLE (plan not run)
- Where: `postgres_repo/mod.rs:279-291`; `postgres/0001:103-109`; `repository/sql/outbox.rs:164-209`.
- What:
  - Postgres: the candidate query is an `OR` across `pending`/`in_flight` filtered on `next_available_at` and ordered by `created_at`. `outbox_messages_pending_idx` is on `(created_at)` only, so scheduled-but-not-due rows are walked on every claim, and the `OR` blocks a single ordered index scan. Since the outbox doubles as the durable timer (comment at `outbox.rs:56`), cost grows with the number of future timers and with backlog.
  - SQLite: one candidate SELECT, then an UPDATE and a SELECT per message (1 + 2N statements).
- Change: order and index pending rows by `next_available_at`; reclaim expired leases as a separate arm (`UNION ALL` with per-arm LIMIT); use `UPDATE … RETURNING` on SQLite. S-M.

### 20. `table` vs `read_model`: the "neutral table" generalisation has no production user
- Category: F   Severity: medium   Confidence: CONFIRMED by rg across repo
- Where and what:
  - `TableModel` (`table/mod.rs:45`) has one implementor, `OutboxMessage` (`outbox/table.rs:141`). `outbox_message_schema()` is referenced only from tests (`table/sql.rs:388,417,564`, `tests/sqlite_repository/main.rs:239`). The real outbox uses migrations and `repository/sql/outbox.rs`.
  - `TableSchemaRegistryExt::register_table` has no caller.
  - `TableKind::Operational` exists for that one schema.
  - `table/` is not neutral: `table/registry.rs:21` and `table/catalog.rs` depend on `read_model::RelationalReadModel`, the catalog is named `ReadModelCatalog`, and its error text says "read model".
  - Five alias imports rename table types back (`sqlx_repo/repo/mod.rs:62-64`, `postgres_repo/mod.rs:32`, `sqlite_repo/mod.rs:35`).
- Change: keep `table::` as the schema/mutation vocabulary; delete `TableModel`, `TableSchemaRegistryExt`, `outbox/table.rs` and `TableKind::Operational` (or make the outbox genuinely use it and delete the hand DDL); move `ReadModelCatalog` and `register<M>` to `read_model`; remove the aliases. M, breaking public re-exports.

### 21. Capability and outcome types that never vary
- Category: F   Severity: medium   Confidence: CONFIRMED
- Where and what:
  - `TableAdapterCapabilities` (`table/plan.rs:6-20`) is all-true in every backend (`sqlx_repo/read_model/validation.rs:6-12`, `read_model/in_memory.rs:47`).
  - `ReadModelQueryCapabilities.relationship_includes` is true in both (`repo/read_models.rs:64`, `in_memory.rs:355`).
  - `TableCommitOutcome` is a unit struct whose `was_applied()` always returns true (`plan.rs:35-47`), yet it is branched on at `repo/read_models.rs:34`.
  - `TableSchemaAdapter` (`table/registry.rs:701-729`) has one non-test implementor, which sets `schema_verification: false, dev_bootstrap: false` (`table/sql.rs:40-46`). `verify_schema` is implemented and called only in `tests/read_model_schema_bootstrap`. `SqlxRepository::bootstrap_table_schema_for_dev` bypasses the trait.
  - `SqlxRepository` and `SqlxOutboxStore` duplicate three schema methods (`repo/types.rs:178-198` vs `223-242`).
- Change: delete both capability structs, both `*_capabilities()` trait methods and `validate_for`; make `commit_write_plan` return `()`; delete `TableSchemaAdapter`, its capabilities, verification and issue types; drop the schema methods from the outbox store. M, breaking.

### 22. Read-model helpers duplicated between in-memory and SQL
- Category: C   Severity: medium   Confidence: CONFIRMED (`validate_row_expected_version` compared line by line)
- Where: `read_model/in_memory.rs:149,163,194,217,408,472-535` vs `sqlx_repo/read_model/validation.rs:22,45,59,95`, `schema_registry.rs:41` and `load.rs`.
- What: expected-version rules, patch/key reconciliation, include resolution and relationship row selection each exist twice. The SQL module documents them as "pure helpers (no sqlx types)".
- Change: move them next to `table::mutation` and call from both. S-M.

### 23. Vestigial schema fields, one with a latent bug
- Category: F/E   Severity: low   Confidence: CONFIRMED
- Where and what:
  - `TableColumn.jsonb` (`table/metadata.rs:100`): the macro already maps it to `ColumnType::Json` (`distributed_macros/src/read_model/types.rs:12`), and `table/sql.rs:256-257` has two identical arms that both return `"jsonb"`.
  - `TableSchema.foreign_keys` (`metadata.rs:197`): the macro fills it with clones of the column-level `foreign_key` (`relational.rs:176-178`) and every other construction site passes an empty vec. For an entry not declared on a column, `table/sql.rs:202-207` emits `FOREIGN KEY (fk.column) REFERENCES table (fk.column)`, using the referenced column name as the local column.
- Change: delete both fields. S, breaking for hand-built `TableSchema` literals.

### 24. Postgres conflict recovery takes a second pool connection while holding the first
- Category: E   Severity: low   Confidence: CONFIRMED for the mechanism, PLAUSIBLE for the stall
- Where: `repo/commit.rs:452-470`.
- What: after an event-insert unique violation the transaction is aborted but still open, and the code calls `pool.acquire()` to re-read stream versions. With the default pool of five (`repo/backend.rs:295-298`) and five simultaneous losers whose winner is another process, all connections are held while each waits for one more, until the acquire timeout.
- Change: roll the transaction back before the re-read, or report `ConcurrentWrite` from the versions already read at `commit.rs:87`. S.

### 25. Dropped-BEGIN guard only applies to pools the repository opens, and costs a query per release
- Category: E/D   Severity: low   Confidence: CONFIRMED
- Where: `postgres_repo/mod.rs:122-138`; `repo/types.rs:61,126-134`.
- What: `after_release` runs `SELECT now() <> statement_timestamp()` on every connection release. It is installed in `connect()`, so `SqlxRepository::new(pool)` with a caller-built pool lacks the protection.
- Change: document or enforce that pools come from `pool_options`, for example a constructor that takes `PgPoolOptions`. S.

### 26. Dead or misleading items
- Category: F   Severity: low   Confidence: CONFIRMED by rg
- Where:
  - `commit_read_model_write_plan` (`sqlx_repo/read_model/write_plan.rs:37-63`): `#[allow(dead_code)]`, `pub(crate)`, "kept for adapter authors".
  - Stale `#[allow(dead_code)]` on used items: `partitions.rs:183` (`load_partition`), `reads.rs:2359`.
  - Single-field wrappers `StoredRecord{metadata}` and `StoredFailure{failure}` (`types.rs:55-63`).
  - `load_partition` / `_in_tx` / `_in_connection` repeat the same SQL three times (`partitions.rs:184-268`).
  - u64/i64 conversion exists four times (`sqlx_repo/mod.rs:13-54`, `projection_protocol/helpers.rs:17-35`, `repository/sql.rs` `signed`/`unsigned`).
  - `validate_sql_write_plan` runs twice per commit (`repo/commit.rs:50` and `write_plan.rs:76`).
- Change: delete or collapse. S.

### 27. Two ways to write read models in the command transaction
- Category: A (principles 1 and 5)   Severity: medium   Confidence: PLAUSIBLE (design intent is outside this slice)
- Where: `repo/commit.rs:128-135` applies handler-authored `batch.read_model_plans`; `commit.rs:137-153` applies `direct_projection` (the projection program in the same transaction); `locks.rs:11` exists to keep the first away from tables owned by the second.
- What: raw plans are used by real handlers (`tests/bomberman/handlers/*`, `tests/e2e-celld/…/project_auth_user.rs`, `microsvc/service/handlers.rs:228`). They are read-model writes not derived from events by a projection program.
- Change: decide whether raw plans survive v5. Removing them deletes the ownership fences (#5), `reject_causal_table_writes_in_tx`, `projection_table_ownership_fences` and the "legacy rows" registration check (`store_impl.rs:84-103`). L, breaking.

### 28. `with_schedule_worker` returns a tuple; schedule and rebuild SQL are gated on `graphql`
- Category: F   Severity: low   Confidence: CONFIRMED
- Where: `repo/commit.rs:765-772` returns `(Self, ScheduleWorker)`; `commit.rs:687,746`, `projection_protocol/mod.rs:61`, `store_impl.rs:16,24` use `#[cfg(feature = "graphql")]`.
- Change: split into a builder flag that returns `Self` plus `ScheduleWorker::new(&repo, config)`; gate on a feature that names the capability. S.

### 29. Migration inventory test restates the inventory
- Category: B   Severity: low   Confidence: CONFIRMED
- Where: `repo/backend.rs:37-126,130-219` (`generated_sqlite_inventory_preserves_order_descriptions_and_bytes` and the Postgres twin).
- What: 190 lines listing versions, descriptions and `include_str!` for each of 13 files, to be edited on every migration; `migrations/inventory.json` already carries sha256 per file.
- Change: delete after the squash, or replace with the parity test from #11. S.

### 30. Relationship includes are unbounded and not one snapshot
- Category: D/E   Severity: low   Confidence: CONFIRMED
- Where: `sqlx_repo/read_model/load.rs:85-93,173-190`.
- What: the root and each include are separate statements on the pool with no transaction, and a has-many include has no LIMIT.
- Change: read in one snapshot and cap or page the children. S.

---

### Things that are good and should not be changed
- One generic repository over a small backend trait, with type aliases `PostgresRepository` and `SqliteRepository`; no per-dialect copies of logic.
- No value is ever interpolated: `Statement`/`SqlPart` are structural, every identifier goes through `quote_identifier` (doubles embedded quotes; `validation.rs:186`, `table/sql.rs:268`), and DDL defaults pass an allowlist (`table/sql.rs:289-365`).
- `commit_sqlx_batch` is one transaction with the fenced ledger completion as the last statement before COMMIT (`repo/commit.rs:172-181`); event and outbox inserts are multi-row and chunked to the bind limit.
- Read-model upsert is a single `INSERT … ON CONFLICT DO UPDATE`, with re-reads only on the miss path (`write_plan.rs:110-118,405-478`).
- The generic test fixture pattern in `reads.rs:36-121` (one body, run for SQLite and Postgres) and the `ProjectionProtocolScenario` trait are the right model; extend them rather than replace.
