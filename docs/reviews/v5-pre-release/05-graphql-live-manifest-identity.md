## Assessment — GraphQL layer part 2 (live delivery, deltas, manifest, identity)

**Overall health:** the security-sensitive core is careful and mostly fail-closed. The slice is weakest in three places:

1. **Identity defaults and modes.** The engine defaults to trusting client headers; `Hybrid` trusts headers whenever the bearer is absent; WS query-string identity is merged in every mode; JWKS refresh has no failure backoff.
2. **Row policy has four interpreters.** The one used for command deltas has wrong NULL semantics.
3. **The client wire contract is hand-maintained in five or more places.** The CLI does not depend on the core crate, so ~4k lines of Rust mirror types plus ~3k lines of TS validators restate it.

Live queries share the one-shot RBAC path (compile once, full re-execution), so they cannot leak rows or columns. The cost is one full query per subscriber per table-level change, with no sharing and no caps.

**Read fully:** `subscribe.rs`, `http.rs`, `execute.rs`, `commands.rs`, `identity/{mod,gateway,claims,resolve}.rs`, `identity/oidc.rs` 1–760, `engine/{auth,delivery,live_resume,request}.rs`, `protocol/token.rs`, `delivery/mod.rs`, `projection_delta/{mod,authorization}.rs`, `client_manifest/{mod,capabilities}.rs`, the four docs.

**Read in part:**
- `projection_delta/runtime.rs`: 1–420, 1440–1570, 1795–2085, plus the `manifest()` call sites
- `projection_delta/types.rs`: type declarations and `DeltaValue::validate` only
- `projection_delta/{lower.rs 36–130, canonical.rs 1–80}`
- `protocol/accumulator.rs`: 80–300, 920–1060
- `delivery/versions.rs`: 1–330
- `client_manifest/projections.rs`: 584–700, 1265–1372
- `client_manifest/export.rs`: 1–200; `validation.rs`: 1–48
- `leaderboard_roots.rs`: 255–491; `read_store.rs`: 1–140
- `schema.rs`: 300–345, 1067–1205; `compile/projection.rs`: 178–200, 326–500; `compile/filter.rs`: 70–135
- CLI `projection_delta/wire.rs` (parts) and `manifest/types.rs` (head); TS `projection-delta/{types,validate}.ts` (outline and float code)

**Not read:**
- `runtime.rs` 420–1440 and 1570–1795; `lower.rs` 130–1209; `canonical.rs` 80–724
- `protocol/{types,projection_metadata}.rs`
- `client_manifest/{build,codec,commands,identity,limits,types}.rs` and the rest of `validation.rs`
- `complexity*.rs`, `filter.rs`, `naming.rs`, `permissions.rs`, `sdl.rs`
- `leaderboard_roots.rs` 1–255; `oidc.rs` 760–929
- Test bodies beyond the samples cited below
- `docs/session-refresh-continuity.md` and `docs/compact-projection-contracts.md` describe `js/sveltekit` and `src/application`, which are outside this directory; neither was verified against code.

---

### 1. WS identity query params are merged into headers in every identity mode
- Category: E   Severity: high   Confidence: CONFIRMED
- Where: `src/graphql/http.rs:546-547`, `745-767`; `http.rs:627-642`; `identity/resolve.rs:351-357`, `465-467`
- What: `graphql_ws_upgrade` always calls `merge_identity_query_params`, which copies `?x-user-id=` and `?x-roles=` from the URL into the header map when those headers are absent. In `Hybrid`, `resolve_ws_identity` clones those headers and, with no bearer in `connection_init`, falls into `hybrid_proxy_session`, which builds the session from all headers. The doc comment at `http.rs:483` says query params are a DevHeaders convenience.
- Why it matters: a gateway that strips client `x-user-id`/`x-roles` headers but forwards the URL lets any client assert identity and roles for live reads via `wss://…/graphql/ws?x-user-id=admin&x-roles=admin`.
- Change: apply the merge only when `mode == DevHeaders` (or delete it; GraphiQL already uses `wsConnectionParams`). Size S.

### 2. `Hybrid` trusts all headers when no bearer is present; `TrustedProxy` can never carry identity
- Category: F/E   Severity: high   Confidence: CONFIRMED
- Where: `identity/resolve.rs:81-87`, `340-357`, `10-17`; `tests/graphql_identity/main.rs:367` and `431`
- What: `IdentityConfig::hybrid(oidc)` uses `TrustedProxyConfig::with_defaults()`, whose `gateway_secret_header` is `None`. A request without `Authorization` therefore gets `session_from_all_headers`. Test `f6_hybrid_missing_bearer_trusts_proxy_headers` pins this. Conversely `TrustedProxy` checks the optional secret and then strips `x-user-id`/`x-roles` even from the proxy, so it always yields an anonymous session. The enum doc ("configure gateway secret then trust") describes behaviour the code does not have.
- Why it matters: `Hybrid` is DevHeaders for anyone who omits the token unless the operator knows to set a secret. `TrustedProxy` is misnamed, and its secret buys nothing.
- Change (breaking): collapse to three modes — `OidcBearer`, `Gateway { secret }` with the secret required, and `DevHeaders`. Remove `Hybrid`'s secretless path and the strip-list configuration. Size M.

### 3. Engine default identity is `DevHeaders`
- Category: E/F   Severity: high   Confidence: CONFIRMED
- Where: `engine/builder.rs:40`; `identity/resolve.rs:31-32`, `61-69`
- What: a `GraphqlEngine` built without `.identity(..)` trusts client `x-user-id`/`x-roles` for read RBAC. Only the generated scaffold opts into OIDC. Leaderboard "viewer" roots read the member from this same session.
- Why it matters: fail-open default on the RBAC boundary. Mutations are safe because they require a `VerifiedPrincipal`.
- Change (breaking): no `Default` for `IdentityMode`/`IdentityConfig`. Either make `.identity()` mandatory at build, or default to OIDC with the unset placeholder that `public_oidc_identity_from_env_vars` already uses. Size S.

### 4. Row policy is interpreted four ways; the delta one has wrong NULL and numeric semantics
- Category: C/E (principle 4)   Severity: high   Confidence: CONFIRMED
- Where: `projection_delta/runtime.rs:1963-2067`; `compile/projection.rs:334-496`; `compile/filter.rs:84-118`; `js/src/replica/query-plan/filter.ts:500-518`
- What: the same `FilterExpr` is evaluated by the SQL compiler, by `evaluate_cell_filter` (typed, three-valued, NULL is unknown), by `evaluate_filter` for command deltas, and by the TS replica. The delta evaluator uses raw JSON equality: `CmpOp::Neq => Some(left != &right)`.
  - A row whose policy column is NULL under `status <> 'x'` is denied by SQL but `Authorized` here.
  - `Not(Eq)` on a NULL column and `Eq` against `Lit(Null)` also evaluate to true.
  - A `Float` column against an `I64` literal never compares equal, so `Neq` is always true.
- Why it matters: an `Authorized` after-state lowers to an `Upsert` carrying all granted columns. A command can therefore push into the caller's replica a row that the one-shot query would never return.
- Change: one `FilterExpr::evaluate(schema, row, claims) -> Truth` in `filter.rs`, typed by column like the cell version. Delete `evaluate_filter`/`policy_operand`. Add a parity test table run against SQL and this evaluator, ideally exported as vectors for the TS one. Size M.

### 5. JWKS refresh has no failure backoff or stale-if-error
- Category: E/D   Severity: high   Confidence: CONFIRMED
- Where: `identity/oidc.rs:373-379`, `415-445`, `476-488`
- What: once the 300s TTL passes, `ensure_jwks` calls `refresh_jwks` under an async mutex. On HTTP failure nothing is recorded (`refreshed_at` and `generation` are unchanged) and the request returns `Err(Jwks)`, which becomes 401. The next waiter takes the lock and repeats its own fetch with a 5s timeout, plus another 5s if discovery is not cached.
- Why it matters: during an IdP outage every authenticated request queues serially behind 5–10s timeouts and then fails, even though valid cached keys exist. Single-flight only deduplicates successes.
- Change: on failure keep serving the cached keys and record `last_failed_refresh` with a backoff. Fail only when there are no keys at all. Size S.

### 6. Origin WebSocket subscriptions outlive the token and have no keepalive or caps
- Category: E/D   Severity: medium   Confidence: CONFIRMED
- Where: `http.rs:581-613`; `subscribe.rs:107-139`; `identity/oidc.rs:182`
- What: session and principal are fixed at `connection_init`, and the plan (with claim binds) is compiled once. `VerifiedPrincipal::expires_at` is consulted only in `engine/delivery.rs:105` and under `src/gateway`. Without a gateway `GraphqlConnectionGuard`, `serve.await` runs unbounded. No `keepalive_timeout` is set and nothing limits subscriptions per connection or globally.
- Why it matters: a revoked or expired user keeps receiving live frames with stale claims indefinitely. Half-open sockets keep producers alive, because a hash-gated producer never writes. One connection can open unlimited live queries.
- Change: bound the socket by `min(exp, max lifetime)` on the origin as well. Set a keepalive. Add `max_live_queries_per_connection`. Size S–M.

### 7. Live delivery costs one full query per subscriber per table-level change
- Category: D   Severity: medium   Confidence: CONFIRMED
- Where: `subscribe.rs:128-224`, `305-311`, `319-344`; `execute.rs:215-240`; `leaderboard_roots.rs:402-446`
- What:
  - Footprint is a set of table names, so a write to another tenant's `todos` row wakes every subscriber on `todos`.
  - All subscribers sleep the same fixed 100ms and then re-execute at the same instant, with no concurrency limit and no sharing between identical (plan, binds) subscriptions.
  - On Postgres each refresh is `BEGIN`, `SET LOCAL`, the query, `COMMIT`, plus `pg_is_in_recovery()` under `gateway-delivery`.
  - The result is serialized to a JSON string solely to hash it.
- Why it matters: N subscribers produce an N-query burst on the shared pool per write burst, starving one-shot queries. Buffers themselves are bounded (mpsc 8, broadcast 256 with lag treated as all-dirty) and tasks do stop on `tx.closed()`.
- Change: a registry keyed by (role, plan SQL, binds) that executes once and fans out; a semaphore on live refreshes; hash the value directly. Size M.

### 8. The client wire contract is re-described in at least five places
- Category: C (principle 5)   Severity: high   Confidence: CONFIRMED
- Where:
  - `src/graphql/projection_delta/types.rs` (1256 lines)
  - `distributed_cli/src/client_compiler/projection_delta/wire.rs` (1015 lines; header says "compiler-owned mirror of the frozen wire contract" and carries `#![cfg_attr(not(test), allow(dead_code))]`; only the version constant is used, at `preview.rs:747`)
  - `js/src/replica/projection-delta/types.ts` (425) and `validate.ts` (2589)
  - `src/graphql/client_manifest/types.rs` (74 types) versus `distributed_cli/src/client_compiler/manifest/types.rs` (90 types, 1149 lines) plus `manifest/model_validation.rs` (1609)
- What: `distributed_cli/Cargo.toml` has no dependency on `distributed`, so every shape, bound and canonical-number rule is hand-copied. The F64 canonical check exists at `types.rs:709-721`, `wire.rs:400-413` and as a re-implementation of serde_json float formatting in `validate.ts:2520` (`rustRyuFloat`). The only cross-check is one golden file, `tests/fixtures/projection-delta-v1.json`.
- Why it matters: every wire change is a three-language edit guarded by a single fixture, and the CLI mirror is dead code outside tests.
- Change (breaking for the build graph): move the pool-free contract types (`client_manifest`, `projection_delta::types`, `protocol` types) into a small crate that the CLI depends on, and delete the CLI mirrors. Generate the TS types and validators from those types, or ship generated vectors for every variant instead of one golden. Size L.

### 9. The projection value/kind enums are copied 1:1 inside the core crate
- Category: C/F   Severity: medium   Confidence: CONFIRMED
- Where: `client_manifest/projections.rs:1269-1343`; `projection_delta/types.rs:629-676`; `projection_delta/runtime.rs:1458-1500`
- What: `ProjectionValue` is converted to `ClientProjectionValue`, `DeltaValue` and `serde_json::Value` by three hand-written matches. `ProjectionMutationKind` to `ClientProjectionMutationKind` is an identity mapping over eight variants. `ProjectionValueType` and `ProjectionEnvelopeField` have the same treatment, the latter with `unreachable!` arms.
- Why it matters: these are parallel types that exist only to attach serde attributes. Adding a variant means editing each copy.
- Change: give the core projection enums the wire serde representation and use them directly in the manifest and delta. Size M.

### 10. Request paths still deep-clone the whole client manifest
- Category: D   Severity: medium   Confidence: CONFIRMED
- Where: `client_manifest/export.rs:122-124`; `projection_delta/runtime.rs:534`, `586`, `769`, `1546`, `1636`, `1649`, `1738`; `projection_delta/lower.rs:39`, `67-69`
- What: `manifest()` is `manifest_ref().cloned()`. `try_new` at 1546 clones and immediately drops it, just to check `is_ok`. `verify_partition_token` at 1738 clones it to compare the surface identity. `ProjectionDeltaAuthority` stores an owned copy per lowering.
- Why it matters: this is the same hot spot `docs/protocol-manifest-reuse.md` set out to remove ("request seeds may borrow their compiled manifests"). Only line 108 borrows.
- Change: use `manifest_ref()` at all eight sites and hold `&DistributedClientManifest` in `ProjectionDeltaAuthority`. Size S.

### 11. Gateway version triggers serialize all writers on one counter row per table
- Category: D   Severity: medium   Confidence: PLAUSIBLE (read from SQL, not measured)
- Where: `delivery/versions.rs:196-228`, `13-28`
- What: `install_sql` adds a per-statement trigger running `UPDATE distributed_gateway_versions SET version=version+1 WHERE namespace=… AND table_name=…` on every covered table and on all 14 `PROOF_TABLES` (including `projection_changes` and `projection_records`).
- Why it matters: concurrent transactions touching the same table block on that row until commit. Since every projector writes the proof tables, enabling gateway caching likely serializes projection commits.
- Change: derive versions from an existing monotonic source such as the projection change cursor, or use a sequence. Size M.

### 12. `PROOF_TABLES` hard-codes the projection store's schema inside `graphql/delivery`
- Category: A/C   Severity: medium   Confidence: CONFIRMED
- Where: `delivery/versions.rs:13-28`, `197-201`; `engine/delivery.rs:130`, `143`
- What: the GraphQL layer keeps a literal list of `projection_*` table names. A table added by the projection store is silently uncovered and cached envelopes go stale. The same module also includes a file under `migrations/postgres/` for SQLite, detects plan capture by comparing an error message to a sentinel string, and classifies errors with `error.starts_with("dependency ")`.
- Change: have the projection store export its evidence table list; return the captured plan through a typed channel; use an error enum. Size S–M.

### 13. Two copies of the live producer loop
- Category: C   Severity: medium   Confidence: CONFIRMED
- Where: `subscribe.rs:91-228`; `leaderboard_roots.rs:385-446`
- What: both define `LiveItem`, an mpsc(8) stream wrapper, "initial, wait for matching change, sleep 100ms, drain, re-execute, hash-gate, terminal on error". They have already drifted: the SQL loop returns on `TryRecvError::Closed` while the leaderboard loop does not.
- Change: one `live_refresh_stream(hub, matches, execute)` generic over the executor. Size S.

### 14. `test_oidc` mints production principals and can panic
- Category: E/F   Severity: medium   Confidence: CONFIRMED
- Where: `identity/oidc.rs:133-168`; callers `src/microsvc/wait_path.rs:70`, `src/schedule/worker.rs:192`
- What: `from_trusted_transport` calls `test_oidc`, which is documented as test-only and contains three `assert!`s. It stamps a fake issuer `https://distributed.local/wait-path`.
- Why it matters: a constructor described as impossible ("no constructor from a Session or header map") exists and is used at runtime. It also contradicts `resolve.rs:490`'s test that ambient identity never mints a principal.
- Change: an explicit `VerifiedPrincipal::internal(subject) -> Result` variant with its own provenance; gate `test_oidc` behind `cfg(test)` or a test feature. Size S.

### 15. Claim mapping special-cases Zitadel and cannot map custom claims
- Category: A/F   Severity: medium   Confidence: CONFIRMED
- Where: `identity/claims.rs:63-72`, `84-89`, `97-107`, `115-126`; `identity/oidc.rs:751`
- What: the generic mapper always scans `urn:zitadel:iam:org:project:*:roles`, silently grants `user` when no role matched, and copies only `email` and `org_id`/`orgId` to `x-email`/`x-org-id`. There is no configurable claim-to-session mapping, so a row policy such as `claim("x-tenant-id")` cannot be satisfied in OIDC mode. `claim_path` is duplicated as `claim_at_path`.
- Change: `ClaimMapConfig { session_claims: Vec<(claim_path, session_key)>, default_role: Option<String> }`; move the Zitadel scan into a preset; one path helper. Size S.

### 16. Secret comparison is not constant-time, and it leaks into the session
- Category: E   Severity: low-medium   Confidence: CONFIRMED
- Where: `identity/resolve.rs:333` (`if got != expected`); `identity/mod.rs:25-33`
- What: the secret is compared with string inequality. `session_from_all_headers` then copies every header into `Session`, including the gateway secret, `authorization` and `cookie`, and that session is passed on to command dispatch.
- Change: `subtle::ConstantTimeEq`; drop credential headers when building the session. Size S.

### 17. Public identity helpers rebuild the validator per call
- Category: D/F   Severity: low-medium   Confidence: CONFIRMED
- Where: `identity/resolve.rs:359-367`, `434-441`, `479-482`; `identity/oidc.rs:34`, `498-513`, `279-281`
- What: `resolve_session` builds a new `IdentityResolver`, validator and `reqwest::Client` on each call, so there is no JWKS cache at all. `resolve_session_sync` re-parses the static JWKS each call. `alg_allowlist: Vec<String>` can only ever narrow RS256/ES256 because other algorithms are rejected by the match. `is_anonymous_identity` and the JWK `use_` field are `#[allow(dead_code)]`.
- Change: keep `IdentityResolver` as the only entry point; remove both free functions and the dead items. Size S.

### 18. HTTP glue triplication and dead statements
- Category: C/F   Severity: low   Confidence: CONFIRMED
- Where: `http.rs:378-461`, `228-301`, `398`, `449-451`, `692-717`, `769-803`; `subscribe.rs:54-60`; `execute.rs:23`, `341`
- What:
  - Three near-identical POST handlers and five router constructors; `graphql_router_with_dispatcher` only forwards.
  - `let _ = engine.identity_config().mode == IdentityMode::DevHeaders;` does nothing.
  - `.expect("graphql route mounted without engine")` on a request path.
  - The non-DevHeaders branch of `session_from_connection_init` is reachable only for `TrustedProxy` and sets a session key `authorization`.
  - A hand-rolled percent decoder decodes bytes as Latin-1.
  - `ChangeHub::publish` has an empty `if` body; `execute_sql_on_pool` has `let _ = primary;`; `dialect_name` is dead.
- Change: one handler over `(engine, Option<host>, Option<filter>)` and one router builder. Size S.

### 19. Live frame metadata travels in a side FIFO separate from the value
- Category: E/F   Severity: low-medium   Confidence: CONFIRMED (design hazard, no bug found)
- Where: `protocol/accumulator.rs:95`, `926-947`, `1018-1036`; `subscribe.rs:9-14`
- What: the producer pushes an envelope onto `stream_frames` and then sends the value through mpsc. `attach` pops by position and falls back to the base envelope when the queue is empty. The module comment explains failures must be terminal precisely because of this coupling. The accumulator also has seven mutexes and five copies of "lock seed, clone, clone cache_scope" (135–270).
- Change: carry the envelope with the value (for example, wrap the yielded value and unwrap it in the `execute_stream` map). Size M.

### 20. No behavioural test for the non-`Eq` paths of delta row policy or for SQL parity
- Category: B   Severity: medium   Confidence: CONFIRMED
- Where: `projection_delta/tests.rs:485` (`production_predicate_authorizes_only_complete_actual_after_rows`)
- What: only `owner = claim` is exercised. A search of the file for `Neq`, `IsNull` and `evaluate_filter` finds nothing. No test covers the expired-token WS case (#6) or the query-param merge under non-Dev modes (#1); `http.rs` tests only cover DevHeaders parsing.
- Change: add them with the fixes for #1, #4 and #6.

### 21. Test duplication and weak negative assertions
- Category: B   Severity: low   Confidence: CONFIRMED
- Where: `projection_delta/tests.rs` versus `vector_tests.rs`; `client_manifest/tests.rs`
- What:
  - Four tests in `tests.rs` are strict subsets of `vector_tests.rs`: `strict_decode_rejects_unknown_wire_fields` (65) ⊂ vector 240; `authoritative_zero_actual_occurrences_is_valid_and_deterministic` (74) ⊂ 218; `tagged_values_distinguish_null_absent_and_unset_and_bound_numbers` (131) ⊂ 381; `value_depth_key_partition_and_body_limits_fail_closed` (165) ⊂ 544.
  - Several tests are 290–330 lines each, e.g. `wait_path_observation_proof_requires_the_persisted_semantic_program_identity` (1155), `active_metadata_revalidates_but_remains_queryable_while_projection_is_draining` (1718) and `lifecycle_status_rejects_changed_deployment_identity_and_mixed_fanout_tampering` (2019).
  - `users()`/`todos()` table fixtures are redefined in both test files.
  - Leak checks are `!json.contains("user_id")`-style at `client_manifest/tests.rs:603-605`, `945-947`, `2419-2421`; they pass vacuously if a field is renamed.
- What is fine: both suites otherwise assert behaviour (authorization matrices, tamper rejection, determinism), not exact JSON. Exact bytes are asserted only for the deliberate golden vector.
- Change: delete the four subsumed tests; share fixtures; pair each negative `contains` with a positive assertion that the public counterpart is present. Size S.

### 22. Work-in-progress scaffolding left in the delta module
- Category: F   Severity: low   Confidence: CONFIRMED
- Where: `projection_delta/mod.rs:14-18`; `authorization.rs:123-134`; `lower.rs:46`; `protocol/accumulator.rs:408`, `461`, `505`, `972`; `client_manifest/build.rs:5`, `mod.rs:52`
- What: `#[allow(dead_code, reason = "Task 11 …")]` markers and four dead-code-suppressed accumulator methods. `ProjectionAuthorization`, `ProjectionPartitionScopeEncoder` and `ProjectionDeltaRequestAuthority` each have exactly one production implementation, and `SelectedSurfaceAuthorization<'a, R, R>` is instantiated with the same type twice. `SelectedSurfacePolicyVisibility` survives only to serve `relationship_transition`.
- Change: collapse the three traits into the concrete `ProtocolProjectionDeltaRequestAuthority` (tests construct it with a test codec) and delete the suppressed items. Size M.

### 23. Audience check accepts `azp`/`client_id` when `aud` is absent or `"account"`
- Category: E   Severity: low   Confidence: CONFIRMED (deliberate, documented in code)
- Where: `identity/oidc.rs:636-704`
- What: any token issued to the configured client is accepted regardless of its intended resource server. Everything else in the validator checks out: HS* and `none` are rejected, the key type is tied to the algorithm, `iss`/`exp`/`nbf` are checked with 60s leeway, and `token_use` and `sub` are enforced.
- Change: make the fallback opt-in (`accept_authorized_party: bool`, default false). Size S.

### 24. Multi-role application surface keeps the first role's row filter when filters differ
- Category: E   Severity: low   Confidence: CONFIRMED (in part 1's directory; flagged because it is authorization)
- Where: `engine/auth.rs:219-227`
- What: both branches of the `if` return `first.row_filter.clone()`, and the comment contradicts itself. Columns are intersected across roles, but rows follow whichever role sorts first.
- Change: AND the filters, matching the intersection semantics used for columns, or reject differing filters at build. Size S.

---

### Answers to the slice questions not covered above
- **RBAC on live frames:** same code path as one-shot. `compile::compile_query` runs with the session at subscribe time and every refresh is a full re-execution, so there are no row-level live deltas and nothing can leak when a row leaves the permitted set. Resumable cursors are offered only when every owning projector's models are `Unrestricted` (`client_manifest/capabilities.rs:62-96`), which matches `docs/live-query-delivery.md`. The gap is claims changing mid-session (#6).
- **Command proxy:** clean. `resolve_command` (`schema.rs:1067-1138`) reads the input and `commandId`, requires a `VerifiedPrincipal`, calls `host.invoke`, records the receipt and returns the payload. No writes or side channels.
- **Protocol tokens:** HMAC-SHA256 with domain separation and `verify_slice`, so comparison is constant-time (`protocol/token.rs:202-206`).

### Where code and docs disagree
- `docs/protocol-manifest-reuse.md` says seeds borrow compiled manifests; eight runtime sites still deep-clone (#10).
- `http.rs:483` and the `IdentityMode` docs describe query-param identity as DevHeaders-only and `TrustedProxy` as "configure secret then trust"; the code does neither (#1, #2).
- `docs/live-query-delivery.md` calls the leaderboard member claim "verified"; under the default identity mode it is a client header (#3).

### Things that are good and should not be changed
- Mutations as pure dispatch through `SharedCommandHost`, with `Arc<Service>` kept out of request data and durable commands requiring a `VerifiedPrincipal`.
- Opaque, purpose-separated HMAC protocol tokens with no plaintext payload, and the strict bounded parsers (`deny_unknown_fields`, canonical-bytes round trip, 128-item and 64-depth limits).
- Live queries as hash-gated full re-execution through the one-shot compiler, with terminal failure frames and lag treated as all-dirty. Optimise by sharing executions, not by adding a second delta path.
- `DistributedClientSurfaceExport` deriving identity from the selected Surface and caching the manifest in a `OnceLock` shared across clones.
- Fail-closed visibility lowering (`Unknown` becomes model recovery, never keyed operations) and the cross-language golden vector; extend the vectors, do not drop them.
