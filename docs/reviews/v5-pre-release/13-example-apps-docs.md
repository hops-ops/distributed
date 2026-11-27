# Review: reference apps and docs (the framework as users experience it)

All paths are relative to `/Users/patrickleet/dev/hops-ops/hops/distributed`.

## Assessment

The domain layer in the reference app is good: `todo-domain/src/models/todo.rs` is a plain struct with rules and solid behavioural unit tests, and the Todos page really is `Todos.use()` plus `commands.todo.*`. Everything around that core is heavy. One new Todo command touches about 12 hand-edited places, and the "copyable template" contains a dual write, a fake aggregate, hand-rolled HTTP endpoints, TypeScript twins of domain rules, and a 300-line security-hardened config loader in user code. No build outputs are tracked, and the three skill directories are symlinks, not copies.

Three things most worth changing:
1. **One command authoring form.** There are five today. The flagship app uses the verbose one for every command, while `#[command]` on the aggregate method would cover 5 of 7 Todo commands.
2. **Make the service/host layer disappear.** The `routes()` generic wall, `bounds.rs`, `projection_owners()`, `MODULE_IDS`, the hand-built `application_manifest()`, and the sqlite/postgres host forks are restated framework knowledge. `docs/application-composition.md` already calls this the target, and it is not built.
3. **e2e-celld should be a different `main`, not a second app.** About 3,300 of its 3,583 Rust lines have an e2e-ui twin, most byte-identical.

Not read, or only skimmed:
- README.md lines 700–1557, 1800–2054, 2175–2264, 2625–2713 were not read line by line; they were covered only by the path/link and symbol scripts.
- `docs/`: `coordination-primitives`, `consumer-delivery-lanes`, `external-facts`, `live-query-delivery`, `unsigned-command-inputs`, `compact-projection-contracts` were symbol-checked only.
- e2e-ui UI: `auth.ts`, `lib/server/*`, admin/session/login/signup routes, all `components/`, CSS, `ui/tests/*.mjs` not read. `demos.ts` was sampled, not checked snippet by snippet.
- e2e-ui Rust: `zitadel/map.rs` not read; `zitadel/scrape.rs` only lines 95–180; `projections/src/blob.rs` tests by name only; `suite/tests/behavioral.rs` only lines 1–125 and test names; blob/chat domain models not read.
- Playwright: `todos.user.spec.ts` partly read; `blob`, `chat`, `admin`, `unauth` specs by grep only.
- Ops: all `.gitops/` charts, `docker/`, `gateway/`, `scripts/up.sh` beyond line 60, `helm-contract-test.sh`, `lifecycle-*.mjs`, and the e2e-ui Makefile body.
- e2e-celld: `graphql-service/src/http.rs` (diff count only), `blob-service`, Makefile. `tests/celld/worker` is outside my slice.
- Repo scripts: `oidc-*.sh`, `ci-bootstrap-graphql-oidc.sh`, `check-wasm32-domains.sh` not read.
- Nothing was compiled or run. Every "what breaks when they disagree" statement is from reading and is marked PLAUSIBLE unless I saw the check.

## Findings

### 1. Adding one command touches about 12 hand-edited sites, most restating what `#[sourced]` knows
- Category: C/F   Severity: high   Confidence: CONFIRMED
- Where: `tests/e2e-ui/crates/todo-domain/src/commands/complete.rs:1-41`, `commands/mod.rs:6-24,54-62`, `lib.rs:12-24`, `models/mod.rs:8-13`, `projections/src/todos.rs:7-11,39-47,71`, `service/src/modules/todo.rs:41-47`, `tests/e2e-celld/crates/todo-service/src/routes.rs:39-45`, `todo-service/src/host.rs:8-16,47-80`, `service/src/modules/graphql.rs:632-641`, `suite/src/lib.rs:287-408`, `e2e/helpers/optimism.ts:11-20`
- What: `Todo::reassign` already has a domain method, an event and a projection arm (`todo.rs:155-178`) but no command. To expose it an author writes:
  1. A command file: Input struct, Payload struct, and a 12-key `portable_command!`.
  2. `mod` plus a four-name `pub use` in `commands/mod.rs`.
  3. The same names again in `lib.rs`, and the generated `…DomainEvent` type by name in `models/mod.rs` and `lib.rs`.
  4. The event import and an entry in `events: [...]` in the projection.
  5. `.mount(...)` in `modules/todo.rs`.
  6. `.mount(...)` again in e2e-celld.
  7. The name in `TODO_CELL_COMMANDS` and a `match` arm in `graphql_todo_payload`.
  8. Test inventories: `arms().len() == 8`, the subscription list, a suite helper, the `MutationNeedle` union.
  9. The page handler.

  Only the domain method (already written) and the page call carry new information.
- Why it matters: the pitch is "write the aggregate method; the rest is generated". The template teaches the opposite. Every list is a place to forget; a missed projection `events:` entry means the command succeeds and the read model silently never updates.
- Change (L, breaking):
  - Make `#[command]` on the method the only form. Default `field` from the command name and model, `shard` from the aggregate id (`#[snapshot(id = "todo_id")]` already names it), `transition` from the method, and input from the parameters.
  - Have `#[sourced]` emit `Todo::commands()` so hosts write `.mount(Todo::commands())`.
  - Let a projection arm say `events: state_events(Todo)`, expanding to every `domain` event carrying `TodoState`, with deletion events routed to the delete arm.
  - Glob-export generated event types.

### 2. Five ways to declare a command; the reference app uses the long one for all 11
- Category: F   Severity: high   Confidence: CONFIRMED
- Where: README.md:301-338, 1558-1612, 1618-1638, 1663-1700, 1757-1764, 2125-2146; `distributed_macros/src/portable_command.rs:60-125`; `distributed_macros/src/aggregate_command.rs:1-22`
- What: the README documents five forms:
  - `#[command]` on the method.
  - `portable_command!`.
  - `typed_command::<I,K>()` / `command_transition` builder with `.emits().applies()`.
  - Untyped `.command(name).handle(|ctx| json!)`.
  - `routes!(command handlers::x)` with the `COMMAND`/`guard`/`handle` file convention.

  The two macros have diverging option sets. `#[command]` has `load = or_create` but no `defaults`, `preview_reduce_known_record`, `authenticated_user_field`, or `Atomic`. `portable_command!` has those but rejects `or_create` (`portable_command.rs:101-108`). The repo-wide grep finds `#[command(` only in `examples/game` and `tests/aggregate_command`; `tests/e2e-ui` has none.
- Why it matters: a new user cannot tell which form is canonical. The README calls `#[command]` "the shortest form" and then shows the template not using it.
- Change (L, breaking): keep `#[command]` (with the missing options added) plus one explicit `handle:` escape hatch. Delete `portable_command!`'s thin form, the untyped JSON route style and `routes!` from the public authoring surface, or mark them integration-only. Convert e2e-ui.

### 3. `todo.create` and `todo.force_archive` use `handle:` only because the thin path lacks `or_create`, payload-from-principal and a server default
- Category: A/F   Severity: medium   Confidence: CONFIRMED (code); PLAUSIBLE that `or_create` fully replaces `handle_create`
- Where: `todo-domain/src/commands/create.rs:24-65`, `force_archive.rs:22-55`, `chat-domain/src/commands/post.rs:39-75`
- What: `handle_create` does `repo.get(..).is_some()` → reject, `repo.create()`, call the domain, build the payload. The aggregate already rejects double create (`todo.rs:70-72`). `handle_force_archive` exists only to put `archived_by: admin` in the payload. Each test asserts nothing: `create_uses_handle_escape_hatch` is `assert_eq!(Create::COMMAND, …); let _ = handle_create;`.
- Why it matters: principle 3 (thin handlers). The existence rule is written twice, once in the handler and once in the domain.
- Change (M): add `load: or_create` plus `invoke` with principal, and let `payload` see the principal. Delete both handlers and the two `*_uses_handle_escape_hatch` tests.

### 4. The module `routes()` wall: 18 lines of trait bounds per module, a copied `bounds.rs`, and four positional args each
- Category: C/F   Severity: high   Confidence: CONFIRMED
- Where: `service/src/modules/todo.rs:16-39`, `chat.rs:16-39`, `blob.rs:15-38`, `compose.rs:21-39`, `service/src/bounds.rs:1-40` (byte-identical in 5 e2e-celld crates), `blob.rs:23,39`
- What: every module repeats the same `where R: EventStore, L: Locks, S: ReadStore, QueuedRepository<R,L>: …, AggregateRepository<…, A>: HasRepo + HasOutboxStore + ConfigurableOutboxPublisher…` block. `compose.rs` repeats it once per aggregate. `bounds.rs` exists only to shorten it and is copied six times. `blob::routes` takes `_blob_direct: SurfaceDirectProjection` and immediately discards it.
- Why it matters: this is the first Rust a template user reads after the domain, and it is entirely about framework internals. Adding an aggregate adds a bounds line to `build_service`.
- Change (L, breaking): ship one framework trait or struct, for example `Stores { events, locks, read_models }`, with the blanket bounds inside. Module signature becomes `fn routes(stores: &Stores) -> Routes`. Delete `bounds.rs` and the dead parameter.

### 5. Projection name, epoch and model are each stated three times and mounted through a pass-through handler
- Category: C/F   Severity: high   Confidence: CONFIRMED (redundancy); PLAUSIBLE (effect of mismatch)
- Where: `projections/src/todos.rs:32-37`, `service/src/modules/projections.rs:21-41`, `service/src/modules/todo.rs:48-49`, `handlers/events/project_todos.rs:1-11`, `src/projection/local_mounts.rs:131-160`
- What: `projection!` declares `name: "project_todos"`, `epoch: "e2e-ui-todos-v2"`, `model: Todos`. `projection_owners()` then calls `.eventual_model::<Todos,_>("project_todos", TODOS, "e2e-ui-todos-v2")` and looks it up again with `.projector("project_todos")`. The projector is passed to the module, which does `.modeled_projector(p).handle(project_todos::handle)`, where the handler is an 11-line file calling `projection.apply(TODOS, &context)`. `eventual_model` takes `owner` and `epoch` as free strings and does not compare them with the descriptor.
- Why it matters: three copies of name and epoch with no check I could find. A bumped epoch in one place plausibly creates a second binding identity at startup or silently keeps the old one. Six `.expect()`s sit on a path that is pure restatement.
- Change (M, breaking): `LocalProjectionMounts::from([TODOS.into(), CHAT_MESSAGES.into(), BLOB_GAMES.into()])` reading name, epoch and model from the descriptor, and `Routes::project(TODOS)` replacing `modeled_projector` + `handle` + the handler file.

### 6. `MODULE_IDS`, `MODULE_DECLARATIONS` and a hand-built `application_manifest()` duplicate `Service::application`
- Category: C/F   Severity: medium   Confidence: CONFIRMED
- Where: `service/src/modules/compose.rs:15`, `application.rs:20-28`, `modules/graphql.rs:170-213,826-833`, `service/src/http.rs:141-145`, `src/microsvc/service/runtime.rs:663-702`, README.md:2541
- What: the README says "Command namespaces … supply module names; there is no second module list to maintain", and `Service::application` implements that. The template still keeps `MODULE_IDS` (with a bare `"identity"` string) and `MODULE_DECLARATIONS`, documented as "compile-time proof" although it only pairs constants with prose. `application_manifest()` re-implements the prefix grouping by hand, while `http.rs` in the same crate calls `service.application(...)`. The "identity" module has zero commands because its commands are named `zitadel.*`.
- Why it matters: two different manifests of one application (CLI versus runtime gateway). The whole block is copied into e2e-celld. The test `module_inventory_lists_todo_chat_blob_identity` asserts a constant equals itself.
- Change (S): `application_manifest()` becomes `pool_free_service().application(NAME, surface)?.manifest()`. Delete `MODULE_ID`, `MODULE_IDS`, `MODULE_DECLARATIONS`, `E2E_UI_MODULE_IDS` and that test.

### 7. The read-model list is spelled in six places and the role and surface lists in four
- Category: C   Severity: medium   Confidence: CONFIRMED
- Where: `readmodels/src/lib.rs:15-19,46-49`, `service/src/modules/graphql.rs:84-99,101-105,149-153,217-225`, `service/src/host.rs:74-79,141-146`
- What: Todos/ChatMessages/BlobGames/AuthUsers are listed as `.table_schema(...)` ×4, `add(&mut grants, X::permissions())` ×4, `.model::<X>(X::permissions())` ×4, and twice as table-name strings (`"todos".into()…`) for `GatewayVersionStore::install`. Projection owners are listed twice. The surface role triples (`"e2e-ui", ["admin","user"], ["user"]`) are stated in `build_engine` and again in `distributed_client_surface()`. `application_grants()` is 30 lines of user code compiling `ModelPermissions` into a nested `BTreeMap`.
- Why it matters: principle 4 says permissions are declared next to the model. They are, and then re-registered by hand in three shapes. A model missing from `application_grants()` but present in `.model::<>()` would plausibly make generated-client and runtime fingerprints diverge. The test `graphiql_does_not_change_the_postgres_runtime_client_manifest` exists to catch exactly that class.
- Change (M, breaking): put `permissions()` on the `ReadModel` trait via `#[readmodel(permissions = …)]`. One `read_models![Todos, ChatMessages, BlobGames, AuthUsers]` catalog should feed schema bootstrap, grants, the engine, the version store and client export. Declare surfaces once as values used by both the engine builder and the export.

### 8. The host forks SQLite and Postgres into two 65-line copies
- Category: C   Severity: medium   Confidence: CONFIRMED
- Where: `service/src/host.rs:53-118` vs `120-185`; `94,161`; `docs/application-composition.md:177,201`
- What: `run_sqlite` and `run_postgres` differ only in type names. Both spawn the outbox loop with magic `Duration::from_secs(30), 5`, build the service twice (once for serving, once inside the consumer closure), and create an unused `_dispatcher`. The design doc lists "Runner has one path for sqlite/postgres" as an unchecked acceptance item.
- Why it matters: this is the "copyable" host. Principle 7 (interchangeable backends) is not visible to the user.
- Change (M): either make `run` generic over a `Backend` trait pairing repo, locks and bus, or ship `distributed::host::run(build_service, HostOptions)`. Delete `_dispatcher`.

### 9. Dual write in the reference app: scrape upserts `auth_users` directly and also publishes the event that projects the same row
- Category: A/E   Severity: high   Confidence: CONFIRMED
- Where: `service/src/handlers/ingestors/zitadel/scrape.rs:117-176`, `handlers/events/project_auth_user.rs:28-46` (both byte-identical in e2e-celld)
- What: `scrape_users_to_outbox` calls `materialize_auth_user` (a direct `ReadModelWritePlanBuilder` upsert) and then `publish_mapped_delivery` (outbox) as two separate commits. The comment says the direct write was added because "duplicate outbox ids used to skip this upsert". Duplicate detection is `err.to_lowercase().contains("unique") || contains("already exists")`. `project_auth_user` is a hand-written projector that selects behaviour with `name.contains("deactivated")`, so `auth_users` has no mutation program and no replica optimism.
- Why it matters: this violates principles 1 and 5 inside the template. The writes are non-atomic: the row can exist with no event, or a later stale event can overwrite a newer scrape. The string-matched error swallows any unrelated error containing "unique".
- Framework gap: no first-class integration-event → `projection!` path, and no typed "duplicate outbox id" error or idempotent publish.
- Change (M): let `projection!` accept integration-event contracts so `AUTH_USERS` is declared like the others. Add `OutboxCommitError::Duplicate` or `publish_idempotent`. Delete `materialize_auth_user`.

### 10. Non-aggregate routes need a fake aggregate
- Category: A/F   Severity: medium   Confidence: CONFIRMED
- Where: `service/src/deps.rs:7-12`, `service/src/modules/chat.rs:44-60`, `tests/e2e-celld/crates/identity-service/src/aggregate.rs:1-29`, `identity-service/src/routes.rs:32`
- What: Zitadel ingress, scrape and the `auth_users` projector have no aggregate. e2e-ui mounts them on the chat module with `type AuthDeps = ChatDeps` ("share the chat aggregate repo for outbox/leaf access"). e2e-celld invents `struct Identity` with a no-op `replay_event` purely to call `Routes::for_aggregate::<_,_,Identity,_>`.
- Why it matters: the workaround is in both example apps, so every real app with a webhook will copy it.
- Change (S–M): add `Routes::for_stores(stores)` giving the outbox and read-model store without an aggregate type.

### 11. Hand-written HTTP endpoints re-implement crate-private framework code
- Category: A/C   Severity: medium   Confidence: CONFIRMED
- Where: `service/src/http.rs:20-59,84-111,181-197`, `src/microsvc/http.rs:224`, `src/microsvc/service/runtime.rs:100-107`
- What: `with_http_command_routes()` is all-or-nothing, so the template writes its own `session_from_headers` (the framework's is `pub(crate)`), `status_for_error`, `dispatch_named` and two axum routes for `zitadel.ingress.v1` and `zitadel.scrape.v1`. It also loops over `command_specs()` to register "closed" routes so old command URLs do not fall through to the UI proxy. The copied `session_from_headers` puts every request header into the `Session`; safety depends entirely on the handler's own secret check.
- Why it matters: README principle 05 says "you do not write … a REST endpoint". The route names are strings repeated three times in one file (lines 85-86, 185-195), plus `COMMAND` constants elsewhere.
- Change (S): add `Service::with_http_commands(["zitadel.ingress.v1", …])` or a per-route `.http()`, and let the gateway own the default-closed rule for command paths.

### 12. e2e-celld `graphql_todo_payload` is a JSON twin of the typed payloads, with domain defaults in it
- Category: A/C   Severity: high   Confidence: CONFIRMED
- Where: `tests/e2e-celld/crates/todo-service/src/host.rs:8-82`, `chat-service/src/host.rs:9-33`
- What: `CelldRoute::new(TODO_CELL_COMMANDS, "todo", todo_shard, graphql_todo_payload)` takes a hand-kept command-name list, the aggregate type as a string, and a shard function re-deriving `input.todo_id` (already `shard: |input| input.todo_id.clone()` in each declaration). It also takes a 50-line `match command { "todo.create" => json!({...}) … }` that rebuilds each payload from `remote`/`input`/`session`, with fallbacks such as `"todo.complete" => "completed"` and `owner_id` from the session.
- Why it matters: this is a second copy of `TodoCreatePayload` and the others, plus state-transition knowledge in a host. If the cell response lacks a field, the GraphQL payload is plausibly fabricated instead of failing.
- Change (M, breaking): `CelldCommandHost::route_aggregate::<Todo>()` derived from mounted specs (kind = `aggregate_type`, shard = the declaration's shard, payload = the cell's typed response as-is). Delete both `host.rs` files.

### 13. e2e-celld shares the domain, read-model, projection and UI crates but copies the entire service layer
- Category: C   Severity: high   Confidence: CONFIRMED
- Where: `tests/e2e-celld/crates/**` vs `tests/e2e-ui/crates/service/src/**`
- What:
  - Shared by path dependency: the three domain crates, `e2e-readmodels`, `e2e-projections`, and the UI.
  - Copied: `modules/graphql.rs` (803 lines; the diff is the missing delivery variant and "e2e-ui"→"e2e-celld" in `expect` strings, including about 500 lines of tests).
  - Copied with 0 differing lines: the identity ingestor `zitadel/{auth,handle,map,mod,publish}.rs`, `zitadel_scrape.rs`, `util.rs`, `project_auth_user.rs`.
  - Copied near-identically: `scrape.rs` (7 differing lines), `projections.rs`, `bounds.rs` ×5, the Todo mount list, `application.rs`, `compose.rs`, `spawn_zitadel_scrape`.
  - Genuinely celld-specific: about 130 lines in the two `host.rs` route files plus `graphql-service/host.rs`.
- Why it matters: the README says the sibling "mounts the same declarations". The declarations yes, but an author cannot retarget a module without forking the service crate. Every fix to the ingestor or surface code must be made twice.
- Change (M): e2e-celld becomes one runner crate depending on `e2e-service` (or on shared `todo-module`, `chat-module`, `identity-module` crates), adding only a `CommandHost` and bus choice. This is also the real test of principle 6.

### 14. `blob-domain` depends on the read-model crate and carries a duplicate mutation file
- Category: A/C   Severity: medium   Confidence: CONFIRMED
- Where: `blob-domain/Cargo.toml:15,25`, `blob-domain/src/commands/support.rs:25-34`, `blob-domain/src/mutations/save_blob_game.mutation.graphql` (identical to the one in `projections/src/mutations/`), `move_dir.rs:37-53`, `wasm.rs:29-38`
- What: for `Atomic<BlobGames>` the domain crate imports `e2e_readmodels::BlobGames` and keeps its own private `SaveBlobGame()` because depending on `e2e-projections` would be circular. The WASM preview is wired by four strings (`"blob.simulate_move"`, `"blob/pkg/blob_wasm"`, `"blobSimulateMove"`, `"BlobGames"`) plus a string list of assigned columns that `wasm.rs` re-types by hand as JSON keys.
- Why it matters: a domain → read-model dependency is a layering inversion. Two copies of one mutation program can diverge between the command seal and the projector. A typo in the assign list or a WASM key plausibly fails closed at runtime (no preview), not at build.
- Change (M):
  - Commands with Atomic outcomes belong in the crate that owns the projection, or `Atomic` should be keyed by the projection descriptor (`outcome: Atomic<BLOB_GAMES>`) so the row and program come from one place.
  - Generate the WASM export and `CommandProjectionPureReduce` from one annotated Rust function, for example `#[pure_reduce(model = BlobGames)] fn simulate_move(row: &BlobGames, args: MoveArgs) -> BlobGamesPatch`.

### 15. `TodoState` and `Todos` are the same struct twice, with `status` degraded to `String`
- Category: C/F   Severity: medium   Confidence: CONFIRMED
- Where: `todo-domain/src/models/todo_state.rs:12-40`, `readmodels/src/models/todos.rs:10-20`, `todo_status.rs:3-10`, `ui/src/routes/todos/+page.svelte:35-37`
- What: `Todo` → `TodoState` (hand `From` mapping the enum to `"open"|"completed"|"archived"`) → `Todos` (same five columns plus the join) → TypeScript `todo.status === 'open'`. The enum exists and derives serde `snake_case`, but neither the domain state nor the read model uses it. The allowed values live in doc comments (`/// open | completed | archived`).
- Why it matters: field renames must be made in three structs with nothing tying them together except the upsert at program-build time (PLAUSIBLE: startup error). Status typos on the UI side are unchecked.
- Change (M): support enum columns in `ReadModel`, `DomainState` and the generated GraphQL/TypeScript. Allow `#[derive(DomainState)]` to project from the aggregate (`#[domain_state(from = Todo, skip(purged, snapshot_generation))]`). Consider letting the read model reuse the state struct plus relationships.

### 16. Stringly-typed coupling the compiler cannot check
- Category: F   Severity: medium   Confidence: CONFIRMED (sites); failure modes as marked
- Where and what:
  - **Command name, GraphQL field, client method:** `"todo.complete"` / `"todos_complete"` in `commands/complete.rs:30,38`. Re-typed in the `commands/mod.rs:54-62` test, celld `host.rs:8-16,43-77`, `suite/src/lib.rs:296-300`, `e2e/helpers/optimism.ts:11-20`, `graphql.rs:462-464`. The TypeScript client is generated, so the UI is checked. The celld route list silently falls back to local dispatch if a name is missing (PLAUSIBLE).
  - **Roles:** `["user","admin"]` in 11 command declarations, `ModelPermissions.grant("user", …)` ×4, `.roles(&[…])` at `graphql.rs:84`, surface triples at `graphql.rs:85-95,217-225`, OIDC `engine_roles` at `graphql.rs:291`, `has_role("admin")` at `support.rs:26`, and TypeScript `ui/src/lib/roles.ts:8-12`. A grant for an undeclared role is a startup error (CONFIRMED, `src/graphql/engine/builder.rs:689`). A command `roles:` typo plausibly yields a command nobody can call.
  - **`roles.ts` already diverges:** it treats group `admins` as admin, while the Rust `engine_roles` list has only `admin`.
  - **Claim `"x-user-id"`:** `claim("x-user-id")` in `todos.rs:30` and `blob_games.rs:33`, although `USER_ID_KEY` exists (used in `util.rs:103`). A typo plausibly matches zero rows silently.
  - **Mutation IR:** `upsert_todos`, `delete_todos_by_pk(todo_id:)`, `$input.todo` versus `input: { todo: body }`. The table name is parsed out of the field name (`distributed_macros/src/mutation.rs:259-301`). Model existence and key agreement are checked when the program is built (PLAUSIBLE: first `TODOS.program()`, guarded by `.expect` at startup).
  - **Projection name and epoch:** see finding 5.
  - **`aggregate_type = "todo"`** (`todo.rs:59`) re-typed as the cell kind `"todo"` in celld `host.rs:20`. A mismatch gives a wrong cell address at runtime.
  - **Zitadel event names:** five strings in `project_auth_user.rs:11-17` plus `contains("deactivated")` dispatch.
- Change (M): role constants or an app `Role` enum accepted by `roles:`, `grant` and `.roles`, exported to TypeScript. A `claim::user_id()` helper. Mutation operations checked against the `model:` type at macro time (`upsert Todos from input.todo` already has the type).

### 17. `ui/distributed.config.js` is 300 lines of security-hardened JSON loading in user code
- Category: C/F   Severity: high   Confidence: CONFIRMED
- Where: `tests/e2e-ui/ui/distributed.config.js:21-259,263-306`; also present at `examples/game/sveltekit/distributed.config.js`; `distributed_cli/src/lifecycle/project.rs` also reads the inventory
- What: the app-owned config implements symlink rejection, `O_NOFOLLOW`, a 1 MiB cap, a JSON nesting-depth scanner, a secret-looking-string regex, and path-portability checks for a 30-line `distributed.clients.json`. It then hand-builds `cargo run … -p distributed_cli … client-manifest --package e2e-service --distributed-path ../../..` argument arrays. A dedicated test (`ui/tests/client-inventory-parity.test.mjs`) exists to keep the config in line.
- Why it matters: a copyable template makes every app own framework validation code. The CLI already parses the same file, so there are two validators.
- Change (M): `distributedSvelteKit()` with no arguments reads `distributed.clients.json` and locates the CLI. Validation lives in `@hops-ops/distributed` and the CLI only. An app's `distributed.config.js` should be absent or under 10 lines.

### 18. The chat page hand-rolls live pagination, de-duplication and ordering
- Category: A   Severity: high   Confidence: CONFIRMED
- Where: `ui/src/routes/chat/+page.svelte` script lines 51-134, 250-306; `ui/src/lib/chat/lobby-log.ts:74-108`; `routes/chat/+layout.graphql:2-8`
- What: the page keeps its own `history` array, `historyOffset`, `hasMoreHistory` and `historyExhaustedAtNewestId`. It opens ad-hoc `chat.use({limit, offset}, {live:false})` stores and calls `refetch()`, `get()`, `destroy()`. It merges pages by `message_id`, re-sorts by `created_at` in TypeScript, re-absorbs cached windows on soft navigation, and filters "sparse incomplete rows … possible mid-flight" with a type guard at lines 107-112 and 271-276. Pagination is offset-based under a live head, so every new message shifts the window, which is why the de-dup exists.
- Why it matters: README principle 08 says the page "does not patch arrays". The framework has keyset scans server-side (`docs/bounded-read-model-scans.md`), but I found no client pagination primitive in `js/src/sveltekit` (grep for `fetchMore|loadMore|paginate|cursor` returned nothing). Rows that do not match the generated type are leaking to app code.
- Change (L): a generated `Operation.useWindow()` / `fetchMore()` with keyset cursors that returns one ordered, de-duplicated list. The replica should never surface partially shaped rows through a typed operation.

### 19. The client supplies the chat `created_at` and a millisecond-derived primary key
- Category: A/E   Severity: medium   Confidence: CONFIRMED (code); PLAUSIBLE (collision scenario)
- Where: `ui/src/routes/chat/+page.svelte` script 312-329, `chat-domain/src/commands/post.rs:25-27,44,77-106`
- What: the page sends `message_id = "m-" + Date.now().toString(16)` and `created_at: String(now)`. The server accepts the client timestamp if it is within five minutes, and skips even that check on wasm cells (`post.rs:92`). Two users posting in the same millisecond produce the same aggregate id. The second is rejected server-side ("already exists"), after its optimistic upsert has overwritten the other user's row in that replica.
- Why it matters: the ordering key of a shared room is client-controlled. The reason is that optimism needs the value before the server answers and `command_input_defaults!` offers only `uuid_v7()`.
- Framework gap: no `now()` default or server-stamped column that the optimistic row can hold provisionally.
- Change (M): add `default input.created_at = now_ms()`, where the client generates it for preview and the server overwrites it authoritatively. Use the existing `uuid_v7()` default for `message_id`, as Todo does. Remove `canonical_near_unix_millis`.

### 20. Three pages, three id strategies, and hand-tracked pending state
- Category: A   Severity: medium   Confidence: CONFIRMED (code); PLAUSIBLE that existing `pending` receipts could replace it
- Where: `ui/src/routes/todos/+page.svelte:26,44-55`; `blob/[[gameId]]/+page.svelte` script 24-26,62-68,97-108; `chat/+page.svelte` script 51,320-343; README.md:410
- What:
  - **Ids:** Todos calls `createReplicaUuidV7()` and passes `todo_id` explicitly, although the command has `default input.todo_id = uuid_v7()` and the README shows `commands.todo.create({ title })`. Blob builds `blob-<12 hex>` from `crypto.randomUUID`. Chat uses the millisecond id from finding 19.
  - **Pending state:** each page keeps its own (`pendingCreateIds` set, `pendingCommandCount`, `deliveryById`). On Todos a failed create never removes its id from `pendingCreateIds`, because the delete happens only on the success path.
- Why it matters: the page needs the id only to mark the optimistic row as "Saving…", so the framework does not expose "this row is optimistic or pending" on query rows. `sveltekit/replica.ts:159` has a `pending` receipts list, but the template does not use it.
- Change (M): expose per-row provenance (for example `$query.pending.has(row)`) and return the generated id from the command call synchronously. Then make the pages match the README.

### 21. TypeScript twins of blob domain rules, and a test hook in page code
- Category: A/C   Severity: medium   Confidence: CONFIRMED
- Where: `ui/src/routes/blob/[[gameId]]/+page.svelte` script 111-137, 193-207; `ui/src/lib/blob/board.ts:6-13`; `blob-domain/src/core/tile.rs:3-8`; `readmodels/src/models/blob_games.rs:18`
- What:
  - `canMove(direction)` re-implements the board-edge rule in TypeScript ("don't dispatch a predictably rejected command"), although the WASM pure already knows it.
  - `TILE = {hole:0,…,player:9}` duplicates `tile.rs`, whose own comment says "client board + domain parity".
  - The board is stored as `map_json: String`, parsed with `JSON.parse` and hand-validated on the client.
  - The page calls `ensurePureFunctionsReady()` itself and installs `window.__distributedBlobRefetch` for Playwright.
  - Todos has a milder case: `if (target.status !== 'open') return` (lines 58-91).
- Why it matters: principle 5 says "do not write a TypeScript twin", and the showcase for it has two.
- Change (M): JSON/array columns typed end to end. Enum or const export from Rust into the generated client. The generated command exposes `commands.blob.move.preview(input)` so the page can ask the same pure whether a move is a no-op. Warm WASM automatically. Drop the window hook.

### 22. Every surface needs its own copy of the same GraphQL document
- Category: C   Severity: low   Confidence: CONFIRMED
- Where: `ui/src/routes/chat/+layout.graphql` vs `+layout.public.graphql` (identical bodies), `chat/+page.svelte` script 14-18,42-43, `chat/+layout.server.ts:18-37`, `ui/distributed.clients.json:4-28`
- What: user and anonymous surfaces need separate files ("authorization surfaces own distinct source files even when … intentionally equal"), two generated modules, a runtime `signedIn ? UserChatMessages : PublicChatMessages`, and a second `createDistributedSvelteKitServer`. Each protected page also has a `+page.server.ts` whose only job is `requireAuth`.
- Change (S–M): let one document be listed under several clients in `distributed.clients.json` (or a `# @surfaces(user, public)` pragma), and offer a route-level auth declaration.

### 23. `docs/application-composition.md` describes an API that does not exist and contradicts the README
- Category: F   Severity: high   Confidence: CONFIRMED
- Where: `docs/application-composition.md:3,47-57,63-75,84-96,104-137,172-190`; README.md:347,377,551
- What: it is marked "accepted direction". It documents `CommandDef`, `SystemSlice`, `ProcessRole::{Full,Writer,Projector,QueryApi}`, `Runtime::sqlite()`, `runtime.app().role().run()` and `projection_inventory!`, none of which my symbol script found in `src/`, `distributed_macros/` or `distributed_cli/src/`. Item 6 is marked "Done" while items 1–5 are not built. Its core concept is a process-role flag; the README says three times "You do not flip a `Runtime::role` flag".
- Why it matters: its "Target shape of e2e-ui" table is the same list as findings 4–8, so the diagnosis is right. The owner should decide between a role enum and explicit composition, then delete or rewrite the doc.
- Change (S): move it to GitKB as a spec, or rewrite it against the chosen design. Do not ship it under `docs/`.

### 24. README: stale or wrong statements
- Category: F   Severity: medium   Confidence: CONFIRMED
- Where and what:
  - **516-517, 2318:** "UI http://localhost:5180 / API http://127.0.0.1:8791". The app's own README (`tests/e2e-ui/README.md:49-51`) says the public UI is `http://localhost:8791` and 5180 is internal; the OIDC cookie only works on `localhost`, not `127.0.0.1` (`scripts/up.sh:11-12`).
  - **1768-1790:** "Every registered command becomes a `POST /:command` endpoint", with a curl example. This contradicts 380-382 and `runtime.rs:100-104` (off unless `with_http_command_routes()`).
  - **1640-1645:** a migration note ("This is a breaking Rust API change. Replace `GraphqlInput`/`GraphqlOutput` …") for types that no longer exist, in a pre-release README. Similar wire-version history sits at 2549-2552 and 2586-2591.
  - **593-600:** the crate layout lists `readmodels/ # projections + read_model_catalog` and `service/ # thin command handlers…`. Projections are a separate crate, command handlers live in the domain crates, and `blob-domain`, `projections` and `suite` are missing.
  - **402-413:** `commands.todo.create({ title: text })` does not match the page (finding 20).
  - **2125-2165:** the "Mount on a service" example uses `typed_command(...).emits().applies(/* state_preview! */)` and `GraphqlEngine::from_schema_catalog(...).permission::<TodoView>()`. The APIs exist, but this is not how the template does it (`GraphqlEngine::builder(pool).model::<Todos>(Todos::permissions())`), and e2e tests are named `…auto_derive_optimism_without_applies`.
  - **607-620:** `distributed = "0.1"` and `cargo install distributed_cli` (2567) while the license section (2842-2845) says there is no licence file and npm is `UNLICENSED`.
  - **Clean:** all 54 relative links and 51 backticked repo paths in README and docs resolve. The "Project Structure" tree matches the paths I spot-checked.

### 25. README structure: pitch, operations runbook and API reference in one 2,845-line file
- Category: F   Severity: medium   Confidence: CONFIRMED
- Where: README.md:202-299, 646-2713
- What: the "06 · Projections" principle contains about 100 lines on source-version fencing, migration `0005`, the `SnapshotProjectionRebuild` runbook and its 10,000-record / 64 MiB limits before it returns to "Handlers stay thin". The CLI section contains manifest byte budgets and NATS header notes (2541-2565). Nine of the twelve `docs/*.md` files are not linked from the README.
- Change (M):
  - README: principles 01–10 at one screen each, "See it run", "Use as a dependency", a feature table, and links. Target about 400 lines.
  - Move reference material to `docs/` as guides: aggregates, commands (one form), projections (including snapshots and rebuild), read models and GraphQL, bus/outbox, microsvc, CLI, testing.
  - Move design notes that are incident write-ups from a downstream app (`live-retired-owner-handoff`, `protocol-manifest-reuse` with dated "Forge" timings, `session-refresh-continuity`, `reloading-event-delivery`) to `docs/design/` or GitKB.
  - Delete migration notes.

### 26. First run needs Docker, Zitadel and a 637-line bootstrap before a todo can be created
- Category: F   Severity: medium   Confidence: CONFIRMED (steps); PLAUSIBLE (no login-free UI path; `auth.ts` not read)
- Where: README.md:511-524, `tests/e2e-ui/README.md:16-47`, `tests/e2e-ui/Makefile:29,53-68`, `tests/e2e-ui/scripts/up.sh:1-60`, `crates/runner/src/main.rs:16-18`, `tests/e2e-celld/README.md:63-65`
- What:
  - The README says `make up` then `distributed dev`. `distributed` is not on PATH from a checkout; the Makefile actually runs `cargo run --manifest-path ../../Cargo.toml -p distributed_cli --bin distributed -- dev .` via `make run`, which the root README does not mention. The README also says the CLI, crate and npm versions must match exactly, so `cargo install distributed_cli` cannot serve a checkout.
  - `make up` needs docker, jq, curl and openssl, and bootstraps Zitadel plus Postgres.
  - The runner itself defaults to SQLite and DevHeaders, but the UI goes through Auth.js/OIDC, so the zero-dependency backend is not reachable from the UI.
  - "Option A" comes first in the app README and needs the external `hops` CLI plus kind and Crossplane.
  - A cold compile can exceed the `distributed dev` five-minute readiness window (e2e-celld README).
  - The only zero-dependency example is `examples/graphiql.rs`, which hand-builds a `TableSchema` literal and seeds with raw `CREATE TABLE`/`INSERT` (lines 23-70): no aggregate, command or projection.
- Change (M): add one no-Docker quickstart (`cargo run` → SQLite + DevHeaders + a dev login in the UI, or a tiny `examples/todo` using `#[sourced]`, `#[command]`, `projection!`, `ReadModel` and GraphiQL). Document `make run` as the checkout path. Put Option B before Option A. Replace the `graphiql.rs` seeding with real commands.

### 27. Framework-internals tests and can't-fail tests inside the template
- Category: B   Severity: medium   Confidence: CONFIRMED (names and bodies cited); `blob.rs` test bodies not fully read
- Where and what:
  - `projections/src/blob.rs:62-480`: about 400 lines of fixtures (`patch_blob_program`, `delete_blob_program`, `multi_row_blob_program`, a `BlobGameAudits` model) driving `lower_model_mutation`/`finish_lowering` for `patch_delete_and_multi_row_descriptors_are_not_direct_candidates`, plus `compile_fail` doctests for "removed authoring surfaces" (lines 44-60). These are framework lowering tests living in an app crate.
  - `projections/src/todos.rs:116-142` `graphql_file_matches_inline_graphql_looking_form`: asserts three macro syntaxes share IR. A macro test, not an app test.
  - `todos.rs:69-74` asserts `program.arms().len() == 8`.
  - `shard_is_todo_id` ×7 and `shard_is_game_id` (for example `complete.rs:47-53`) restate the one-line closure.
  - `create_uses_handle_escape_hatch`, `force_archive_uses_handle_escape_hatch`: cannot fail meaningfully.
  - `blob-domain/src/commands/mod.rs:54-59` `client_preview_wasm_stays_in_blob_domain_wasm_module`: `include_str!("../wasm.rs").contains("blobSimulateMove")`.
  - `graphql.rs:826-833` `module_inventory_lists_todo_chat_blob_identity`: a constant equals a literal.
  - `service/src/handlers/util.rs:100-150`: five tests for helpers with no callers.
  - All `graphql.rs` tests are duplicated in e2e-celld.
- Good and worth keeping: `models/todo.rs` tests (rules, idempotent no-ops, replay), `todo_commands_auto_derive_optimism_without_applies`, `application_todos_keep_portable_owner_row_policy…`, `public_surface_opens_and_queries_chat_without_identity`, and the `suite/tests/behavioral.rs` t0–t10 cases.
- Change (S): move the lowering and macro tests to the framework's own tests. Delete the restating ones.

### 28. Dead and duplicated helpers in the template
- Category: C/F   Severity: low   Confidence: CONFIRMED
- Where: `service/src/handlers/util.rs:33-98`, `todo-domain/src/commands/support.rs`, `blob-domain/src/commands/support.rs:7-23`, `chat-domain/src/commands/post.rs:8-18`, `service/src/handlers/commands/mod.rs`, `modules/graphql.rs:25-34`, `util.rs:16-31`
- What:
  - `require_user`, `require_admin`, `session_has_user`, `session_is_admin`, `causal_has_user`, `causal_is_admin`, `principal`, `rejected` in `util.rs` have no callers outside the file; only `decode_payload` and `read_model_error` are imported.
  - `rejected`, `principal` and `authenticated_user` are defined identically in three domain crates.
  - `handlers/commands/mod.rs` is an empty placeholder.
  - `ClientSurfaceLocks` is a newtype around `Arc<InMemoryLockManager>` used only to get `Clone`.
  - `decode_payload` sniffs JSON versus bitcode by first byte.
- Change (S): the framework should provide `HandlerError::rejected(e)` and built-in guards (`guard: authenticated` / `guard: role("admin")`). `ctx.input::<T>()` should decode by content type. `InMemoryLockManager` should be `Clone`. Then delete all of the above.

### 29. Playwright suite: behavioural, but timing- and CSS-coupled and gated on the full stack
- Category: B   Severity: low   Confidence: CONFIRMED (todos spec, helpers, config); other specs by grep only
- Where: `tests/e2e-ui/e2e/todos.user.spec.ts:4,136-159,362,389,414,434,537`; `e2e/helpers/optimism.ts:34-57`; `playwright.config.ts:35-48`; `crates/suite/tests/behavioral.rs:102-120`
- What:
  - **Strengths:** the tests assert real behaviour. Optimistic paint is proven by holding the mutation request; a per-animation-frame trace checks no intermediate mis-ordered frame; the mutation is counted as sent once.
  - **Weaknesses:** five `page.waitForTimeout(750)` waits to "prove nothing regresses"; class selectors (`.panel`, `.item`, `.ch-msg`, `.ad-table tbody tr`) coupled to styling; `expectBinarySorted` assumes ids sort lexicographically; substring matching on POST bodies; `workers: 1`, 90 s timeouts.
  - **Not the only whole-system check:** `suite/tests/behavioral.rs` boots SQLite, InMemoryBus, projector and HTTP in-process with no Docker and covers isolation, RBAC, purge and Atomic blob. It polls for projections with `sleep(30ms)` ×100 because a Rust caller has no "await projected".
  - **Gap:** the browser/replica path (optimism, live, SSR) is verified only with Postgres and Zitadel up. There is no browser run against the in-process SQLite + DevHeaders stack.
- Change (M): add `data-testid`/role selectors. Replace fixed waits with waiting on the command receipt's `projected` promise (exposed for tests). Give the Rust suite a causal wait helper. Add one Playwright project against the no-Docker stack.

### 30. Repo hygiene
- Category: F   Severity: low   Confidence: CONFIRMED
- Where and what:
  - **Skill directories:** `.claude/skills/*` and `.codex/skills/*` are symlinks into `.kb/skills/` and `.agents/skills/`, not copies. Only `.codex/skills/rust-best-practices` is a real directory, and it is not exposed to `.claude`. `skills-lock.json` pins two third-party skills, one visible to each agent.
  - **Stale script:** `scripts/graphql-skill-dry-run.sh:22` tests for `docs/graphql.md`, which does not exist, so the script fails. It references a task slug and `GROK_SCRATCH`, and nothing references it.
  - **Unreferenced wrapper:** `scripts/setup-graphql-oidc-zitadel.sh` is a five-line `exec` of `oidc-zitadel-up.sh`.
  - **AGENTS.md / CLAUDE.md:** identical five-line stubs pointing at `.kb/AGENTS.md`. That file is the older GitKB guide (`.kb/workspace/`, `git-kb commit --all`, mandatory banner) and disagrees with the parent repo's newer one. Neither says how to build or test the framework. The parent CLAUDE.md asks for the Crossplane/Upbound reminder to be copied into each sub-repo AGENTS.md; it is not here (and is not relevant).
  - **Tracked files:** no build outputs, `node_modules`, generated clients, `.wasm` or `.db` files. The e2e-ui workspace has no tracked `Cargo.lock` while `examples/game/server` and `tests/gateway-*` do, so the template is not reproducible.
  - **`walkthrough/demos.ts`:** 1,597 lines holding 84 hand-copied code snippets as strings (for example lines 709-720 re-type `MODULE_IDS`). This is a third copy of the source after code and README, and I found no test that references the walkthrough.
  - **`tests/e2e-ui/PROJECTION_ROLLOUT.md`:** a production cut-over runbook ("name one accountable person or on-call rotation") inside a test fixture.
  - **Location:** the flagship "copyable template" lives under `tests/`.
- Change (S): delete the two scripts. Write a real AGENTS.md (build, test, layout). Track `tests/e2e-ui/Cargo.lock`. Generate walkthrough snippets from source regions or drop them. Move the template to `examples/app` or `templates/`.

## Things that are good and should not be changed
- **Domain crates as plain structs.** `todo-domain/src/models/todo.rs` puts rules in methods, uses `#[event(.., domain)]` recorders, has behavioural tests, and compiles with `default-features = false` for wasm.
- **Permissions next to the model.** `Todos::permissions()` with `col("owner_id").eq(claim(..))` is used for both server RLS and client-portable row policy. Keep the shape; only remove the re-registration.
- **The `projection!` + mutation IR model.** One program serves the server projector and the browser replica, and optimism is auto-derived from the transition (no `.applies`). The `todo_commands_auto_derive_optimism_without_applies` test is the right kind of test.
- **Lifecycle-owned client generation.** `distributed build`/`dev` owns it, nothing generated is tracked, clients are per surface, and colocated `.graphql` islands use GraphQL defaults as bindings.
- **The in-process behavioural suite and the held-mutation Playwright helper.** `crates/suite/tests/behavioral.rs` (GraphQL-only edge, owner isolation, HTTP command routes closed by default) and `expectOptimisticPaint` prove behaviour, not structure.
