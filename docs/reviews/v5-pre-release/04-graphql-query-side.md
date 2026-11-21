## Assessment

The slice is in reasonable shape with one real authorization hole. One compiler serves both dialects, each root field becomes one SQL statement (no N+1), client values are always binds, and the integration tests are mostly behavioural. Nothing I read on the query path writes domain state; `delivery/versions.rs` (described at `engine/builder.rs:65` as "installing version coverage in the engine's database") was not read, so that part is unverified.

The three things most worth changing:
1. **Permissions live in two parallel representations.** The SQL compiler reads `ReadPermission`; the schema, SDL, manifest and protocol read `RoleGrant`/`SurfaceRowPolicy`. They are intersected by two different functions that disagree, and that disagreement is the hole (finding 1).
2. **Row policy is enforced by convention.** Eight hand-written sites each have to remember to pair a projection with `compile_where`; no single scan builder makes omission impossible (finding 2).
3. **The executor and result path are doubled.** There are four executor arms (plain/protocol × SQLite/Postgres), stringly-typed errors matched by substring, and several passes over the result JSON (findings 5, 6, 9, 10).

Not read, or only partly read:
- **Outline only:** `http.rs` (mostly identity/websocket, assumed part 2), `sdl.rs`, `naming.rs`, `surface/{build,commands,effects,identity,leaderboard,projections,roots,validation}.rs`.
- **Not read:** `commands.rs`, `compile/evidence.rs`, `engine/{live_resume,metrics,read_routing}.rs`, `delivery/versions.rs`, `protocol/` (assumed part 2).
- **Partial:** `leaderboard_roots.rs` (350-382), `surface/types.rs` (465-600), `filter.rs` (388-590), `query_protocol.rs` (1-1000 read, rest outlined), `engine/delivery.rs` (1-120), `engine/introspection.rs` (1-80).
- **Tests:** `engine/tests.rs` read at 860-950, 1286-1420, 1814-2065, 2295-2383, 2505-2521. `surface/tests.rs` and `engine/composite_relationship_tests.rs` by test names and assertion counts only.

Ownership calls: I took `engine/auth.rs` and `engine/protocol.rs` because they decide which grants SQL uses.

## Findings

### 1. Multi-role application surface runs SQL with the alphabetically-first role's row filter
- Category: E/A   Severity: high   Confidence: CONFIRMED (by reading; not executed)
- Where: `src/graphql/engine/auth.rs:219-227`; `engine/builder.rs:391-392, 981-1032`; `engine/protocol.rs:33-43, 128-138`; `surface/application.rs:687-692`
- What: `insert_synthetic_privilege_permissions` builds the `ReadPermission` stored under `app:{name}`. Both arms of its `if` return `first.row_filter.clone()`, and the comment admits it: "Fail closed to first role's filter is wrong … otherwise keep first for SQL". `schema_roles` is sorted, so `first` is the alphabetically first role. The surface side turns differing policies into `ServerOnly`, promising the server enforces them. Any principal holding any eligible role may open the surface and gets `privilege_role = app:{name}`.
- Why it matters: with `client_application_surface("console", ["admin","user"], ["admin","user"])`, `admin` unrestricted and `user` owner-filtered, a `user`-only principal gets SQL with no row filter. No test covers this: `named_application_protocol_selection_is_registered_exact_and_role_bound` (`engine/tests.rs:863`) uses `grant_all` for both roles.
- Change: make the synthetic filter the AND of all schema roles' filters, or reject differing policies at build. Better, delete this function via finding 3. Add a behavioural test that seeds rows and queries as `user` through the app surface. Size S (fix), M (with 3).

### 2. Row and column RBAC is re-applied per path, not enforced at one choke point
- Category: A   Severity: medium   Confidence: CONFIRMED
- Where: `compile/projection.rs:572, 642, 685, 734`; `compile/relationship.rs:84, 137, 299, 382`; `compile/filter.rs:390`; `compile/projection.rs:522-526, 866, 899, 939, 966-972`; `schema.rs:759-767`
- What: the row predicate lives in one function (`compile_where`), but nine call sites must each remember to call it next to a hand-written `SELECT … FROM "table" alias WHERE …` template. I found no site that omits it today. Other enforcement is implicit:
  - `compile_root` does not check `perm.aggregations` for `RootKind::Aggregate`; only the per-role schema hides that root.
  - Ungranted columns and relationship targets are silently `continue`d rather than rejected.
  - `execute_cell_by_key` projects `row.get(&child.field_name)` with no `allows_column` check, again relying on the schema.
  - Many-to-many joins traverse the through table without consulting any grant on it.
- Why it matters: principle 4. A new access path (sum/avg aggregates, cursor pagination) leaks rows if its author forgets the pairing.
- Change: introduce one `ModelScan { from_sql, where_sql }` constructor that takes (model, privilege) and always includes the policy. Every list, by-pk, count, nodes, relationship and EXISTS path builds from it. Turn the silent `continue`s into internal errors, and check `aggregations` in the compiler. Size M.

### 3. Parallel permission types: `ReadPermission` vs `RoleGrant`, two intersections, two validators
- Category: C/F   Severity: medium   Confidence: CONFIRMED
- Where: `permissions.rs:32-136`; `surface/application.rs:6-58`; `engine/core.rs:167-169, 241, 260`; `engine/validation.rs:63-162` vs `surface/application.rs:439-554`; `engine/builder.rs:698-714` vs `surface/application.rs:404-417`; `compile/projection.rs:1093-1108` vs `surface/types.rs:488-511`; `engine/auth.rs:93-120` vs `filter.rs:535-561`
- What:
  - `ReadPermission` and `RoleGrant` carry the same five facts and are mapped by `role_grant_from_read_permission`.
  - `EngineInner` holds both `permissions` (used by SQL) and `role_surfaces` (used by everything else).
  - The row-policy validator is written twice, near line-for-line. So is the column-allowlist check, although `build()` already calls `surface_for_role`, which runs the surface copy.
  - "Has client-normalized identity" exists twice with slightly different rules, despite the comment at `types.rs:485` saying "this single predicate".
  - The claim-key walker in `auth.rs` duplicates `FilterExpr::visit_claims`.
  - `RoleModelPerm` is a one-field wrapper.
- Why it matters: this is the owner's "no parallel types" rule, and it is the structural cause of finding 1.
- Change (breaking): one grant type (keep `ReadPermission` as the authoring API). The selected role/application `Surface` keeps the executable `FilterExpr` plus a derived "client-portable?" flag. The compiler reads `role_surfaces[privilege].models[..]`. Delete `EngineInner.permissions`, `RoleModelPerm`, `RoleGrant`, `validate_filter*`, `insert_synthetic_privilege_permissions` and the duplicate predicate. Size L.

### 4. Query cost limits ignore `limit`, so one request can ask for about a million rows
- Category: D   Severity: medium   Confidence: CONFIRMED (arithmetic from code)
- Where: `complexity.rs:89-96`; `complexity_contract.rs:32-47`; `engine/builder.rs:22-25`; `compile/projection.rs:539-543, 809-818`; `schema.rs:380-382`
- What: complexity uses a fixed `list_fanout: 5` regardless of the requested limit. `orders(limit:1000){ items(limit:1000){ id } }` costs 3 + 5×(10 + 5×1) = 78 against a budget of 500, and `max_limit` (1000) applies per level. `offset` has no cap. The budget is per root field; only async-graphql's flat field count spans the document.
- Why it matters: the only bound on result size is the 5-second statement timeout. The JSON is materialised in the database, then passed through `serde_json` and `async_graphql::Value`.
- Change: multiply by the resolved limit to get a row budget (e.g. product of limits ≤ `max_rows`, default around 10k), or give nested lists a smaller default. Collapse the three overlapping knobs into one documented cost model: async-graphql `limit_depth`/`limit_complexity`, the compiler's `max_depth` (which also counts boolean nesting, `compile/filter.rs:25, 64`), and `estimate_root_complexity`. The same `max_complexity` number currently means two different units. Size M.

### 5. Errors are `String` end to end and classified by substring
- Category: E/F   Severity: medium   Confidence: CONFIRMED
- Where: `schema.rs:901-937`; `compile/projection.rs:655`; `engine/builder.rs:88`; `execute.rs:13, 56`; `compile/filter.rs:569-579`
- What:
  - `sanitize_compile_error` picks the client message with `e.contains("max depth")`, `"_in list"`, `"cell-by-key"` and so on.
  - `client_error_for_execute_err` maps any error text containing `"timeout"` to `TIMEOUT`, which may also catch sqlx pool-acquire timeouts (not verified).
  - By-pk decides whether to AND the filter with `where_sql == "TRUE" || where_sql == "true"`.
  - The builder swallows shadow-catalog errors with `!e.0.contains("identical")`. `insert_catalog` itself never emits that word (`schema.validate()` errors, outside the slice, were not checked), so the test is effectively always true.
- Why it matters: rewording a message silently changes the client error code. Comments in `compile/filter.rs` already warn that messages "must stay free of SQL fragments so sanitize maps to a stable code".
- Change: a `CompileError` enum (`TooDeep`, `TooComplex`, `ListTooLong`, `InvalidFilter`, `UnsupportedOnCellStore`, `Internal`) and an `ExecuteError { Timeout, Database }`. Have `compile_where` return `Option<String>` instead of the `"TRUE"` sentinel. Size M.

### 6. Four executor arms; Postgres pays 4-5 round trips per root field
- Category: C/D   Severity: medium   Confidence: CONFIRMED
- Where: `execute.rs:84-127, 215-287`; `query_protocol.rs:620-669` vs `670-732`; `execute.rs:226-231` and `query_protocol.rs:683-690`
- What: plain and protocol execution are separate functions, each with a SQLite and a Postgres arm. The two protocol arms are near-identical closures of about 50 lines, even though `finish_protocol_query` is already generic over `DB`. The plain Postgres path does `BEGIN`, `SET LOCAL statement_timeout`, optionally `SELECT pg_is_in_recovery()`, the query, then `COMMIT`, for every root field. `plan.sql.clone()` runs per execution, and `plan.clone()` twice per protocol request (`query_protocol.rs:614, 625`).
- Why it matters: latency on the hottest read path, and two places to keep the timeout and primary-check logic identical.
- Change: one `execute_plan(executor, plan)` generic over the backend. The plain path becomes "protocol path with no evidence consumer". Set the timeout once per connection, or fold it into one round trip with `set_config(..., true)`. Hold the plan in an `Arc`. Size M.

### 7. `_like` / `_ilike` behave differently on SQLite and Postgres
- Category: A (principle 7)   Severity: medium   Confidence: CONFIRMED (code and repo-wide rg; standard SQLite behaviour, not executed)
- Where: `compile/filter.rs:102-103, 545-546`; `compile/dialect.rs:45`
- What: `_like` emits `LIKE` on both dialects and `_ilike` maps to `LIKE` on SQLite. SQLite's `LIKE` is ASCII-case-insensitive by default and nothing in the repo sets `case_sensitive_like`. So `_like` is case-sensitive on Postgres and insensitive on SQLite, and `_ilike` is Unicode-aware only on Postgres.
- Why it matters: the same query returns different rows per backend, with no error. `order_by` already pins `COLLATE "C"`/`BINARY` for exactly this reason.
- Change: pick one semantics. For example, `_like` on SQLite becomes `GLOB`-equivalent or the connection sets `PRAGMA case_sensitive_like=ON`; `_ilike` becomes `lower(col) LIKE lower(?)` on both. Add a cross-dialect conformance test. Size S-M.

### 8. Plans are extracted for gateway delivery by throwing a sentinel error from resolvers
- Category: A/F   Severity: medium   Confidence: CONFIRMED
- Where: `schema.rs:296-320, 804-817`; `delivery/mod.rs:35-39`; `engine/delivery.rs:109-120`
- What: under `gateway-delivery`, `validate_delivery` runs the real schema executor with a `PlanCapture` in request data. Each root resolver compiles, pushes the `SqlPlan` into a mutex-guarded `Vec`, and returns `Err("gateway query plan captured")`. The caller reads the vector back.
- Why it matters: control flow by error string, `cfg` branches inside the hot resolver, and a third execution mode beside plain and protocol.
- Change: expose `GraphqlEngine::plan(session, request) -> Result<Vec<SqlPlan>, _>` that walks validated root selections and calls `compile_query` directly. Remove `PlanCapture` and `CAPTURED`. Size M.

### 9. SQLite JSON subtype loss is patched in two post-processing layers instead of in the SQL
- Category: D/A   Severity: medium   Confidence: CONFIRMED (workaround exists); PLAUSIBLE (fix, needs a SQLite run)
- Where: `compile/projection.rs:586-589` vs `compile/relationship.rs:179, 331, 405` and `compile/projection.rs:757`; `execute.rs:180-212`; `schema.rs:714-721`
- What: only the root list wraps rows with `json(root)`. Nested aggregates emit `json_group_array(obj)` unwrapped, so SQLite returns elements as JSON strings. `deep_parse_json_strings` then re-parses any array element that looks like `{…}` or `[…]`, and `passthrough_key` re-parses a string parent on every child-field resolution (a belongs-to object with N selected fields is parsed N times).
- Why it matters: a per-request heuristic tree walk plus repeated parsing, on SQLite only, where the dialect table already has a `json_cast_fn`.
- Change: apply `json(...)` to every nested object and array expression in the SQLite dialect, then delete `deep_parse_json_strings` and the string branch of `passthrough_key`. Also check whether a `Json` column on SQLite reaches the client as a string (`column_json_expr` emits the bare column, `projection.rs:1121-1122`); I found no test asserting either way. Size S-M.

### 10. Result JSON is walked several times, and field lookup is a linear scan with deep clones
- Category: D   Severity: low-medium   Confidence: CONFIRMED
- Where: `execute.rs:130-136, 289-293`; `schema.rs:771-783`; `compile/projection.rs:846, 1015-1091`; `execute.rs:26-28`
- What: database text goes through `serde_json::from_str`, then (SQLite) `deep_parse_json_strings`, `rewrite_hex_bytes`, `extract_evidence_and_strip`, `Value::from_json`, and finally the dynamic resolvers. `lookup_key` iterates the whole map comparing keys instead of using `map.get`, and clones the value. For a relationship field that is the entire subtree, cloned again at each level. Hidden evidence columns are added to every object on every request, even when `inner.protocol` is `None` and the plain path throws the evidence away.
- Change: use `IndexMap::get`. Emit and strip evidence columns only when a protocol accumulator exists. Size S for those two; the remaining passes fall out of finding 9.

### 11. `execute` and `execute_stream` duplicate about 70 lines; operation selection is written three times
- Category: C   Severity: low-medium   Confidence: CONFIRMED
- Where: `engine/request.rs:31-102` vs `118-193`; `engine/request.rs:333-349`, `engine/protocol.rs:297-313`, `engine/introspection.rs:15-35`; fragment walkers at `engine/protocol.rs:325-393` and `engine/introspection.rs:43-80`
- What: the lifecycle check, authority resolution, schema selection, multi-root check, accumulator, read routing and data injection are repeated for the streaming entry point. "Pick the named or sole operation" appears three times, and the budgeted fragment walk twice.
- Change: one `prepare(session, request) -> Result<Prepared, Response>` used by both entry points, and one `selected_operation(&mut Request)` helper. Size S.

### 12. Privilege-role fallback written "for schema unit tests" sits on the production path, twice
- Category: E   Severity: low-medium   Confidence: CONFIRMED
- Where: `schema.rs:867-882`; `leaderboard_roots.rs:354-366`
- What: if `ExecutionAuthority` is absent from request data, both helpers authorise as `session.roles().first()`, else anonymous. `GraphqlEngine::execute` always injects authority, so this only fires if a future entry point forgets to. In that case a multi-role session gets its first role's grants while executing against whichever schema the caller picked.
- Change: make missing authority an `INTERNAL` error, keep one helper, and have tests inject an authority. Size S.

### 13. Cell-by-key read path: primary-key check only accepts strings; field lookup is inconsistent
- Category: E   Severity: low-medium   Confidence: PLAUSIBLE (depends on how sealed rows encode integer keys)
- Where: `schema.rs:742-748, 760-766`; `compile/projection.rs:158-166, 436-438`
- What: a numeric key argument is stringified, then compared with `row.get(key).and_then(Value::as_str)`. A row whose key is a JSON number always fails with "does not match request", surfaced as `INTERNAL`. The policy evaluator looks up `field_name` then `column_name`; the projection looks up only the GraphQL field name.
- Change: compare using the column type (reuse `cell_values_equal`) and use one lookup helper. Size S.

### 14. `query_protocol.rs` (1.8k) does three jobs; `schema.rs` (1.4k) does four
- Category: F   Severity: low-medium   Confidence: CONFIRMED
- Where: `query_protocol.rs:41-540` (build-time projector topology), `542-918` (snapshot execution and evidence), `920-1440` (live-resume cursor and wire assembly); `schema.rs:27-387` (schema construction), `725-865` (root and cell execution), `884-959` (error mapping), `1068-1200` (command and status resolvers)
- What: each file mixes build-time compilation with request-time execution. `query_protocol.rs` also has nine `.expect("comparable … plans retain …")` on request paths (803, 807, 966, 970, 1025, 1029, 1102, 1247, 1251), guarding an invariant held in two `Option` fields.
- Change: split into `query_protocol/{topology,execute,live}.rs` and `schema/{build,resolve,errors,commands}.rs`. Make the index plan carry a `ResumableProjector { partition, epoch }` so the `expect`s disappear. Size M, mechanical.

### 15. Aggregate compilation is copy-pasted between root and relationship; the list template appears six times
- Category: C   Severity: low   Confidence: CONFIRMED
- Where: `compile/projection.rs:668-781` vs `compile/relationship.rs:68-204`; list templates at `projection.rs:592, 757`, `relationship.rs:179, 319, 331, 405`
- What: the `aggregate`/`count`/`nodes` match (validation, where, limit/offset binds, evidence) is duplicated, differing only in FROM and join predicate. `compile_where` is re-run, and binds duplicated, for each of `count` and `nodes`.
- Change: falls out of finding 2's `ModelScan`: one `compile_aggregate(scan, selection)` and one `compile_list(scan, selection)`. Size M (with 2).

### 16. Dead code and vestigial API
- Category: F   Severity: low   Confidence: CONFIRMED (rg over the whole repo found only definitions and re-exports)
- Unused:
  - `compile_list_sql_for_test` (`compile/projection.rs:1279`)
  - `dialect_name` (`execute.rs:342`)
  - `ReadPermission::allowed_columns_for` (`permissions.rs:101`)
  - `core_sdl_for_catalog` (`engine/mod.rs:102`, `engine/validation.rs:170`)
  - `graphql_sdl_from_schemas` (`sdl.rs:635`)
  - `role_grants_for_role` (`surface/application.rs:64`)
  - `QueryProtocolRuntime::is_empty` (`query_protocol.rs:243`)
- No-ops and stale attributes:
  - `filter.visit_columns(|col| { let _ = col; })` (`engine/validation.rs:56-58`)
  - Empty `if` body in `sdl_for_role` (`engine/public_api.rs:26-28`)
  - `let _ = primary;` (`execute.rs:23`)
  - `#[allow(dead_code)]` on `max_complexity`, which is used (`engine/core.rs:247`); on `introspection_for_anonymous`, which I did not rg (`:255`); and blanket `#[allow(unused_imports)]` in `compile/mod.rs:27-37`
- Pure aliases:
  - `client_application_surface` just calls `…_with_schema_roles` with the same three arguments (`engine/builder.rs:349-356`)
  - `surface_for_application` just calls `…_contract`, used only by tests (`surface/application.rs:566-580`)
- Change: delete; keep one name per pair. Size S.

### 17. Golden SHA-256 hashes of whole SDL and manifest artefacts
- Category: B   Severity: low-medium   Confidence: CONFIRMED
- Where: `engine/tests.rs:532, 1416, 1551, 1997-2022`, asserted at `1954-1966`
- What: `sqlite_restricted_admin_full_artifact_matrix`, `postgres_restricted_admin_full_artifact_matrix`, `one_role_surface_drives_runtime_sdl_manifest_and_limits` and others compare hashes of the full manifest JSON, static SDL and runtime SDL to hard-coded constants (12 in the matrix alone).
- Why it matters: any intentional change, including async-graphql's SDL whitespace, fails with two opaque hashes and no diff. The fix is always "paste the new hash", so they detect change but not incorrectness. The structural assertions just above them (`definition_inventory` parity, root sets, field sets) are the valuable part.
- Change: drop the hashes, or replace with reviewable snapshot files. Keep one test that the fingerprint is stable across two builds. Size S.

### 18. A test that only tests SQLite
- Category: B   Severity: low   Confidence: CONFIRMED
- Where: `engine/tests.rs:2507-2520`, `sqlite_nocase_can_equate_unequal_code_unit_strings`
- What: runs `SELECT 'A' = 'a' COLLATE NOCASE` and asserts 1, then `assert_ne!("A","a")`. No framework code is exercised.
- Change: delete, or replace with a test that the engine's `order_by` and `_eq` use binary collation. Size S.

### 19. Behavioural RBAC gaps
- Category: B (inverse)   Severity: medium   Confidence: PLAUSIBLE (searched `tests/graphql_*` and in-slice tests by name and grep, not exhaustively)
- What exists: list, by-pk and root-count row isolation (`tests/graphql_harden/authz.rs:15, 57, 97`); column allowlist at root and nested (`:130, :169`); relationship-`where` applying the target policy (`engine/tests.rs:2299`); ungranted nested target (`tests/graphql_harden/residual.rs:14, 85`).
- What I could not find, executing with seeded rows:
  - Multi-role application surface with differing row filters (finding 1).
  - Nested has-many, many-to-many and belongs-to projection where the target has a row policy; only `where` is covered.
  - `<rel>_aggregate { count nodes }` under a target row policy.
  - Root `_aggregate { nodes }` under a row policy.
  - Role `limit()` actually clamping rows.
  - A cross-dialect run of the authorisation suite; it is SQLite-only and `tests/graphql_postgres/main.rs` is 63 lines.
- Change: one table-driven suite, access path × {row policy, column policy}, run on both dialects. Size M.

### 20. Static SDL emitter and dynamic schema builder are two renderings held together by parity tests
- Category: C   Severity: low   Confidence: CONFIRMED (deliberate per `mod.rs:3-6`)
- Where: `sdl.rs:148-633` vs `schema.rs:27-685`; same-named helpers `command_shape_uses_type_name`/`command_type_uses_name` at `sdl.rs:356-374` and `schema.rs:425-441` (bodies compared only on the `schema.rs` side); parity tests at `engine/tests.rs:1323-1336, 1827-1833` and `schema.rs:1306`
- What: every schema feature is implemented in the Surface IR, `sdl.rs`, `schema.rs` and the client manifest. The reason is that the CLI emits SDL without the `graphql` feature.
- Change: keep both, but move the shared naming and collision helpers into `surface` or `naming`, and express "what types and fields exist" once as an iterator over the Surface that both renderers consume. Size M. Do not merge the renderers.

### 21. Small allocations on the compile hot path
- Category: D   Severity: low   Confidence: CONFIRMED
- Where: `compile/projection.rs:524, 897, 968`; `compile/filter.rs:376`
- What: every permission lookup builds `(model.to_string(), role.to_string())` because the map key is `(String, String)`. Column lookups are linear `find`s per selected field. Each request recompiles from scratch; because the `where` shape comes from variables, plans are not trivially cacheable.
- Change: resolve once to a per-privilege `&BTreeMap<model, grant>` (free with finding 3). No plan cache. Size S.

## Answers to the slice questions

- **RBAC in one place?** No. One predicate function, nine call sites, plus implicit reliance on the per-role schema (finding 2). One confirmed bypass exists (finding 1).
- **Injection surface:** clean for client input. Aliases are checked by `validate_response_key`, values are binds, `_icontains` escapes wildcards, and order direction is mapped to fixed tokens. Identifiers are quoted without escaping and come from developer-declared `TableSchema`; `validate_generated_names` covers only exposed models (`engine/validation.rs:7`). That is acceptable, since the developer is trusted.
- **Intermediate representations:**
  - *Per request:* document → async-graphql AST and `SelectionField` → `SelectionNode` → `SqlPlan` (SQL, binds, parallel `QueryEvidencePlan`) inside `QueryPlan::{Sql, CellByKey}` → JSON → `async_graphql::Value`.
  - *Build time:* `TableSchema` → catalog `Surface` → role or application `Surface` → {dynamic schema, static SDL, manifest}.
  - *Redundant or parallel:* `ReadPermission` vs `RoleGrant`; plain vs protocol executors; the plan-capture mode. There is no separate Postgres-vs-SQLite compiler.
- **Does the query layer write?** Not in anything I read (`SELECT`, `SET LOCAL`, `pg_is_in_recovery()` only). Mutations go to `SharedCommandHost::invoke`.
- **Tests asserting SQL strings?** Essentially no, on the counts I took: 14 and 1 `contains(` assertions in the two engine test files, 69 in `surface/tests.rs` (mostly error-message and SDL field-presence checks, judged from test names and grep, not from reading bodies). The low-value pattern is the golden hashes (finding 17).

## Things that are good and should not be changed

- One compiler with a `DialectOps` table rather than per-backend compile paths, producing one statement per root field through correlated JSON subqueries.
- Client values are always bind parameters; response keys and identifiers are validated before being embedded; compile errors are sanitised before reaching the client.
- Fail-closed defaults: `strict_where: true`, deny-by-default `read()`, an always-appended primary-key tiebreak with binary collation for deterministic pagination, and SQL three-valued logic preserved in `cell_row_matches`.
- Relationship `where` compiles the full target `compile_where`, so the target's row policy applies (`compile/filter.rs:387-401`), and it has a behavioural test.
- The role `Surface` as the single input to the runtime schema, SDL and manifest, with crate-private fields and a redacting `Debug` impl; and the per-role schema selected by the same `ExecutionAuthority` that the compiler uses.
