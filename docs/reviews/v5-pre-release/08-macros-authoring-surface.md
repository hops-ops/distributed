# Slice review: procedural macros and the authoring surface (v5)

## Assessment

The newer macros are in good shape: `distributed_game_macros` and `aggregate_command.rs`, `command_types.rs`, `domain_state.rs` and `domain_event.rs` expand thinly, resolve crate paths and span their errors. The older core (`sourced.rs`, `aggregate.rs`, `snapshot.rs`, `read_model/*`, `mutation.rs`) hard-codes `distributed::` paths, duplicates runtime logic and carries three generations of authoring surface at once.

The three things most worth changing:
1. **One command surface, not four.** `#[command(id = ..)]` on a handler fn, `#[command("a.b", ..)]` on an aggregate method, `portable_command!` and raw `routes.command_transition` overlap. `module!`, `application!`, `aggregate!`, `#[digest]` and `#[enqueue]` have no users outside framework tests and fixtures.
2. **Two correctness hazards in `#[sourced]`.** The event is recorded before a fallible recorder body runs (finding 1). Optimistic "known values" are inferred syntactically from recorder assignments and can be wrong (finding 2).
3. **Macros that should expand to one runtime call.** `projection!` recompiles its program on every event, `mutation!` re-implements program builders inline with drift, and `mutation_file!` does not track its input file.

Principle 3 is only partly honoured (finding 8).

Nothing was compiled or run; every finding is from reading code and `rg`.

Not read or only skimmed:
- `distributed_macros/src/read_model/{attrs,relational,types}.rs`: attribute key list and grep only.
- `distributed_macros/src/command_types.rs`: lines 100–620 not read.
- `distributed_macros/src/command_input_defaults.rs`: symbol list only.
- `distributed_macros/src/mutation.rs`: parser section (lines 67–636) not read.
- `distributed_macros/src/snapshot.rs`: tests after line 330 not read.
- `distributed_game_macros/src/{replicated,event,input,pure}.rs`: grep only.
- `.rs` fixtures under the `compile_fail` dirs: sampled about 8; all 75 `.stderr` files classified by script.
- Root `tests/sourced/*`, `tests/enqueue/*`, `tests/aggregate_command/*`: grep only.
- `examples/`: only `examples/game/server/crawler`.
- `macro_rules!` other than `projection!`, `graphql_models!` and `impl_aggregate!` (`routes!`, `events!`, `state_preview!`, `event_preview!`): usage-counted, not read.

## Authoring surface inventory

Usage counts are from `rg` over the repo, excluding the macro crates themselves.

| Macro | Arguments | Usage outside macro crates |
|---|---|---|
| `#[sourced(entity, ..)]` | `events = "Name"`, `aggregate_type = "x"`, `domain_state = Type`, `enqueue[(field)]`, `upcasters(..)` | 79 |
| `#[event("name", ..)]` | `version`, `when = expr`, `domain` / `domain = state\|event\|deleted\|with(T, f)` | `domain = with`: 3 (tests only); `domain = state` spelled out: 1 |
| `#[command("a.b", ..)]` on an aggregate method, with `#[principal]` | `shard`, `load`, `outcome`, `payload`, `roles`, `guard`, `field`, `input`, `transition`, `mount` | 1 example (crawler), plus `tests/aggregate_command` |
| `#[command(id = "..", ..)]` on an async fn | `roles(a, b)`, `emits(..)`, `applies(..)`, `defaults`/`input_defaults`, `default(f = gen)`, `field`/`field_name`, `input`, `outcome` | 3 test/fixture sites, 0 apps |
| `portable_command! { .. }` | 17 keys: `name, transition, aggregate, input, outcome, shard, field, roles, load, invoke, payload, handle, guard, defaults, constructor, authenticated_user_field, preview_reduce_known_record` | 17 (11 in e2e and examples) |
| `module!`, `application!` | `id, commands, projections, surfaces, capabilities`/`required_capabilities, modules, extensions` | fixtures only |
| `aggregate!`, `#[digest]`, `#[enqueue]`, `impl_aggregate!` | — | `tests/` only; `impl_aggregate!` has 0 uses |
| `mutation!` (two syntaxes), `mutation_file!` | — | `mutation!`: 2 (one test); `mutation_file!`: 18 |
| `command_input_defaults!` | — | 4 |
| `projection!` (two arms) | — | 17 |
| `graphql_models!` | — | 0 call sites (one CLI-generated comment) |
| derive `Snapshot` | `#[snapshot(entity = "f", id = "f", version = N)]` | 43 |
| derive `DomainState` / `DomainEvent` | `#[domain_state(version)]` / `#[domain_event(name, version)]` | 25 / 25 |
| derive `CommandInput` / `CommandOutput` | — | 76 / 43 |
| derive `ReadModel` | `#[readmodel(..)]` plus bare `#[table] #[collection] #[column] #[id] #[index] #[unique]` | 128 |
| Game: `Replicated, GameInput, GameEvent, GameRecord, FactCommand, TicketExtension`, `#[predict]`, `#[game_pure]` | — | coherent, see end |

## Findings

### 1. `#[event]` records the event before a fallible recorder body runs
- Category: E   Severity: high   Confidence: CONFIRMED
- Where: `distributed_macros/src/shared.rs:145-198` (arms at 165-175 and 187-196); prelude built at `distributed_macros/src/sourced.rs:816-828`
- What: For a recorder that returns `Result`, the expansion is `{ #prepend (|| #original_block)()?; #append Ok(()) }`. `#prepend` is the `enqueue` call plus `self.entity.digest_v(..)?`. The event is pushed onto `entity.events` (and enqueued on the emitter) before the body runs. If the body returns `Err`, the event stays pending with the state change not applied.
- Why it matters: A caller that handles the error and still commits persists an event whose handler fails; replay then fails forever at `replay_event`. Domain-marked recorders are protected by compile errors (`sourced.rs:761-771`); plain `#[event]`, `#[digest]` and `#[enqueue]` are not. The test `explicit_digest_validates_before_recording_events` in `tests/sourced/main.rs` shows the hand-written workaround. No test covers a failing macro recorder body.
- Change: Make recorders infallible, as domain recorders already are: reject `-> Result` on `#[event]` methods. Validation belongs in the public decision method. Breaking. Size S.

### 2. `#[sourced]` infers optimistic "known state values" by syntax, and can be wrong
- Category: A/E   Severity: high   Confidence: CONFIRMED (code path); client-visible impact PLAUSIBLE
- Where: `distributed_macros/src/sourced.rs:1102-1180` (inference), `1470-1507` (emission); consumed at `src/command/projections.rs:715-753` and `805-815`
- What: The macro scans top-level `self.field = <expr>` statements of a state-domain recorder. It treats literals, `None`, `Some(lit)`, `std::string::String::from("..")` and any path of two or more segments as a proven value. The value is published as the predicted value of the `DomainState` field with the same Rust name, serialised with serde from the aggregate-side expression. Three things break it:
  - A later `if c { self.status = X }` or a helper call such as `self.advance_snapshot_generation()` does not invalidate an earlier assignment, because non-assign statements are skipped (`1106-1108`).
  - The real body is `From<&Aggregate> for State`, which is arbitrary code. `TodoState.status` is a `String` built by a hand-written `match` (`tests/e2e-ui/crates/todo-domain/src/models/todo_state.rs`), while the macro predicts `serde(TodoStatus::Completed)`. They agree only because `TodoStatus` carries `#[serde(rename_all = "snake_case")]`.
  - The same field name on aggregate and state is assumed to mean the same thing.
- Why it matters: The command preview says "this field will be X" while the server writes Y, so the optimistic UI flickers or shows wrong data. Nothing fails at compile time or in tests. It is a second, heuristic implementation of the state mapping (Principle 5). The rules are invisible to authors: `String::from("x")` is not known but `std::string::String::from("x")` is (see `expand_sourced_transition_exports_unconditional_known_state_values` in `src/entry_tests.rs:210`).
- Change: Delete the inference (`KnownStateValue*`, `infer_unconditional_known_state_values`, `flat_literal`, `command_event_known_values`). Authors who want a preview declare it with the existing `state_preview!` / `event_preview!`. Roughly 330 lines removed. Breaking. Size M.

### 3. `projection!` recompiles the whole projection program for every event
- Category: D   Severity: high   Confidence: CONFIRMED
- Where: `src/lib.rs:231-237` and `283-289` (`__resolve`), `196-229` (`__handlers`); hot caller `src/projection/lower.rs:529-535`, used at `src/microsvc/projector/runtime.rs:211` and `src/microsvc/causal.rs:782`
- What: `__resolve(occurrence)` is `resolve_mutation_program(&__handlers()?, occurrence)`. `__handlers()` rebuilds every mutation (`$mutation().program().clone()`), re-reads `RelationalReadModel::schema()`, re-binds every event and runs `compile_projection`. I found no caching in `src/mutation/descriptor.rs`.
- Why it matters: Projection apply is the hottest read-side path. Each event pays allocation and validation proportional to (events × columns) of the projection.
- Change: Build the program once: a `static OnceLock<Result<ProjectionProgram, _>>` in the expansion, or have `ProjectionDescriptor` own a lazily built program and take `resolve(&program, occurrence)`. Size S.

### 4. `mutation_file!` does not register its input file with Cargo
- Category: E   Severity: high   Confidence: CONFIRMED (code); stale-build symptom PLAUSIBLE
- Where: `distributed_macros/src/mutation.rs:35-66`, expansion at `637-668`
- What: The macro reads `CARGO_MANIFEST_DIR/<path>` with `std::fs::read_to_string` and emits only the compiled program. There is no `include_str!` or tracked path anywhere in the crate, and the e2e projections crate has no `build.rs`.
- Why it matters: Editing only `save_todo.mutation.graphql` leaves rustc's dependency info unchanged, so the old program keeps running. I did not run a build to confirm.
- Change: Emit `const _: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", #path));` in the expansion. Size S.

### 5. Four overlapping ways to declare a command
- Category: F/C   Severity: high   Confidence: CONFIRMED
- Where: `distributed_macros/src/command.rs` (588 lines), `aggregate_command.rs` (630), `portable_command.rs` (344); builder at `routes.command_transition::<T, I, O>`
- What: Each form produces a typed command plus a mount. They disagree on spelling and validation:
  - Roles: `roles(user, admin)` with dedupe and non-empty checks (`command.rs:35-78`); `roles = ["a"]` non-empty (`aggregate_command.rs:206-235`); `roles: ["a"]` with no validation (`portable_command.rs:153`).
  - Name: `validate_command_id` allows `-` and caps at 128 bytes (`command.rs:151`); `validate_name` allows no `-` and has no cap (`aggregate_command.rs:161`); `portable_command!` checks only the last segment (`portable_command.rs:176`).
  - GraphQL field: required in `portable_command!`, optional in the method form, defaulted from the id in the fn form.
  - Load: `required|create` versus `required|or_create|create`.
  - Outcome: detected by the last path segment being `"Eventual"`/`"Succeeded"` (`portable_command.rs:323-343`, `aggregate_command.rs:180-204`); a type alias defeats it.
- Why it matters: A new author cannot tell which form is canonical. The README shows none of the method forms in Principle 3. The e2e app uses `portable_command!` with 9–13 keys per command, where `aggregate`, `transition` and, for thin commands, `shard` and `payload` restate what the aggregate method already says.
- Change: Keep `#[command("agg.verb", ..)]` on the aggregate method as the one declarative form. Give it the missing options from `portable_command!` (`defaults`, `authenticated_user_field`, `preview_reduce_known_record`, `load = create`). Keep the plain `Routes` builder as the escape hatch for hand-written handlers. Delete `portable_command!` and the fn-form `#[command(id = ..)]`. Put role, name and outcome validation in one runtime function that both paths call. Breaking. Size L.

### 6. `#[command(id = ..)]` gates the user's handler on a feature of the user's crate
- Category: A/F   Severity: high   Confidence: CONFIRMED
- Where: `distributed_macros/src/command.rs:325-327`, also `354-413`
- What: The expansion emits `#[allow(unexpected_cfgs)] #[cfg(feature = "application-runtime")] #function`. The cfg is evaluated in the caller's crate. A user crate without a feature literally named `application-runtime` silently loses its handler, mount and register fn, with the warning suppressed. Each command also emits four `LazyLock` statics and seven functions; two of them panic at first access (`:346`, `:398`, `:408`).
- Why it matters: This is a hidden runtime matrix (Principle 6). It is used only by `tests/fixtures/application-contract-only`, `tests/fixtures/renamed-dependency-trybuild`, `distributed_macros/tests/application.rs` and one test in `src/microsvc/service/tests.rs`. No example or the e2e app uses it; the e2e app composes by hand in `tests/e2e-ui/crates/service/src/application.rs`.
- Change: Remove with finding 5, together with `module!`, `application!`, the `*_DEFINITION`/`*_COMMAND_ID` naming protocol (`module.rs:139-165` rewrites identifier suffixes) and the `application-runtime`/`runtime` features of `distributed_macros`. If contract-only builds are still wanted, gate inside the framework crate. Breaking. Size M.

### 7. Vestigial first- and second-generation authoring macros are still public and documented
- Category: F   Severity: medium   Confidence: CONFIRMED (rg over the repo)
- Where: `distributed_macros/src/{digest.rs, enqueue.rs}`, `aggregate.rs:187-475`, `src/aggregate/aggregate.rs:34-95` (`impl_aggregate!`), `src/lib.rs:480` (`graphql_models!`), `mutation.rs` classic syntax (`435-636`), README lines 1017-1090
- What:
  - `aggregate!`, `#[digest]`, `#[enqueue]` and `#[sourced(.., enqueue)]` are used only under `tests/` (`upcasting`, `enqueue`, `sourced_enqueue`, `replay_event_version`).
  - `impl_aggregate!` has no call site.
  - `graphql_models!` has no call site and assumes a `$m::Model` layout the e2e app does not use; it is referenced only in a CLI-generated comment and one skill doc.
  - Inline `mutation!` has two grammars; the only non-macro-crate use is one test in `tests/e2e-ui/crates/projections/src/todos.rs:122-132`.
  - Alias keys exist for the same option: `defaults|input_defaults`, `field|field_name` (`command.rs:91`, `:118`), `capabilities|required_capabilities` (`module.rs:53`, `application.rs:46`), `skip_query|skip|private` (`read_model/attrs.rs:157-159`). `module.rs:45` keeps a tombstone error for a removed `mounts` key.
- Why it matters: This is roughly 900 lines of macro source plus about 300 README lines for surface the README itself says `#[sourced]` replaces.
- Change: Delete all of the above and the tests that exist only for them. Keep one mutation grammar (the GraphQL-looking one) and one spelling per key. Breaking. Size M.

### 8. Principle 3 is only partly honoured: framework plumbing lives in the domain struct
- Category: A   Severity: medium   Confidence: CONFIRMED
- Where: `tests/e2e-ui/crates/todo-domain/src/models/todo.rs:8-10` and `:96`; `distributed_macros/src/aggregate_command.rs:476-486`; `distributed_macros/src/snapshot.rs:173-187`
- What: An aggregate must carry `#[serde(skip, default)] pub entity: Entity`, name that field in `#[sourced(entity, ..)]`, and call `self.entity.set_id(&id)` inside its creation recorder (11 such calls across e2e and examples). The generated handler knows the id: in `load = or_create` it computes `__id` and then calls `ctx.create()` without it, relying on the domain method to set it. `#[snapshot(id = "todo_id")]` then tells the snapshot derive which domain field equals the entity id so it can call `set_id` again on restore. Domain unit tests reach into `t.entity.events()`.
- Why it matters: Forgetting `set_id` is a silent domain bug that the framework has the information to prevent. The README's "plain types" example shows `self.entity.set_id(..)` in its first snippet.
- Change: Have the framework own identity: `ctx.create(id)` or `repo.create_with_id(id)` sets it, and replay and snapshot restore set it from the stream id. Then drop `#[snapshot(id = ..)]` and the `id` field synthesised into `XSnapshot`. Keeping an `entity: Entity` field is a reasonable minimum; default the field name to `entity` so `#[sourced]` needs no positional argument. Breaking. Size M.

### 9. Stringly-typed and redundant arguments
- Category: F   Severity: medium   Confidence: CONFIRMED
- Where: `distributed_macros/src/sourced.rs:49-56` and `697-701`; `snapshot.rs:210-216`; `portable_command.rs:17-32`
- What:
  - `events = "TodoEvent"` is a string turned into an identifier, and the default is already `{Struct}Event`. 25 of the 27 uses spell out exactly the default; the other two are test renames.
  - `#[snapshot(entity = "state", id = "todo_id")]` takes field names as strings, while `#[sourced(entity)]` and `#[enqueue(.., entity = f)]` take bare identifiers.
  - `aggregate_type = "todo"` is repeated on `#[sourced]` and consumed as a string again in every capture call.
  - `authenticated_user_field: (Event, State, "author_id")` names a field by string.
  - `#[readmodel(primary_key = ["todo_id"])]` plus `#[readmodel(id)]` on the same field (`tests/e2e-ui/crates/readmodels/src/models/todos.rs`) says the same thing twice.
  - Every ReadModel option has two spellings (`#[table("x")]` 67 uses versus `#[readmodel(table = "x")]` 38; `#[id]` 19 versus `#[readmodel(id)]` 11). The derive claims the generic helper names `id`, `index`, `table`, `column`, `unique` and `collection` (`lib.rs:113-116`), which collide with other derives.
- Change: Remove `events =` (or make it an identifier); use identifiers in `#[snapshot]`; keep only namespaced `#[readmodel(..)]`; a single-column `primary_key` implies `id`. Breaking. Size S–M.

### 10. Generated `…Event` enum and `TryFrom<&EventRecord>` are a second, unused decode path
- Category: C/F   Severity: medium   Confidence: CONFIRMED (generation); "unused" by rg
- Where: `distributed_macros/src/sourced.rs:940-1017` (enum, `event_name()`, `TryFrom`), versus replay arms at `1029-1058`
- What: Every `#[sourced]` impl emits a public enum with one variant per event, `event_name()`, and a `TryFrom` that repeats the tuple decode of each replay arm. `rg` for `Event::try_from(` and for uses of the enum finds only `tests/sourced*` and the `domain = with(T, adapter)` adapter signature (`:617-618`), which itself has three uses, all in tests.
- Why it matters: Every aggregate pays compile time for it, there are two decode implementations per event, and the pascal-cased last name segment causes a variant-collision error class (`:710-759`) that exists only because of this enum.
- Change: Either route replay through it (decode once to the enum, then one `match`), or drop the enum and give `domain = with` adapters the typed parameters. I would drop `domain = with` as well: `domain = event` and derive `DomainEvent` cover it. Breaking. Size M.

### 11. Generated code hard-codes `distributed::`, `serde::` and `std::`
- Category: E   Severity: medium   Confidence: CONFIRMED
- Where: `sourced.rs` (52 bare `distributed::` sites, e.g. `:293`, `:379`, `:1008`), `aggregate.rs:30-66`, `snapshot.rs:162-187`, `read_model/{mod,relational,types,attrs}.rs` (about 77 sites), `mutation.rs:657-664` and every operation (`::distributed::`); `serde::Serialize` at `sourced.rs:430` and `:923`, `snapshot.rs:162`
- What: `framework_path()` exists (`shared.rs:17-32`) and `sourced.rs:675` even calls it, but uses the result only for `SourcedResult`. The renamed-dependency fixture exercises only `command`, `module!`, `application!`, `CommandInput`/`CommandOutput` and `DomainEvent`. So `framework = { package = "distributed" }` works for the newest macros and fails for `#[sourced]`, `Snapshot`, `ReadModel` and `mutation!`. `command.rs:382` shows the right pattern for serde: `#framework::__private::serde`.
- Related: `distributed_macros` maps `FoundCrate::Itself` to `crate` (`shared.rs:19`), while the game crate maps it to `::distributed_game` (`distributed_game_macros/src/shared.rs:14`). `src/lib.rs:12` already has `extern crate self as distributed`, so `::distributed` would work in both cases.
- Change: Use one `framework_path()` everywhere and re-export serde through `__private`. Extend the renamed-dependency fixture to cover `#[sourced]`, `Snapshot` and `ReadModel`. Size M, mechanical.

### 12. `mutation!` inlines program-building logic that already exists in the runtime, with drift
- Category: C   Severity: medium   Confidence: CONFIRMED (duplication); behavioural impact PLAUSIBLE
- Where: `distributed_macros/src/mutation.rs:670-920`; runtime twins `src/mutation/descriptor.rs:324-380` (`state_upsert_program_for_model`) and `:387` (`delete_by_pk_program_for_model`)
- What: Each upsert, insert, delete or patch expands to 40–60 lines that loop over `RelationalReadModel::schema()` at run time, with about 20 `.expect("key path")`-style panics whose messages name neither model nor field. The runtime helper types each field with `projection_value_type_for_column(&column.column_type)`. The macro copy uses `ProjectionValueType::String` for every key and field (`mutation.rs:705`, `:719`, `:768`, `:782`, …). I did not trace whether a later bind step corrects the types.
- Why it matters: The same semantics are built twice, and the copies have already diverged. Bad schemas panic with opaque messages instead of returning `Result`.
- Change: The macro should emit data only: `MutationProgram::from_ops(name, version, [MutationOpSpec::Upsert { model: schema::<M>, root: &[..] }, ..])`, with one fallible builder in `src/mutation`. Have the `Mutation` constructor return `Result`, or keep one panic with the model name. Size M.

### 13. `Snapshot` generates a parallel struct and leaves layout versioning to memory
- Category: A/E   Severity: medium   Confidence: CONFIRMED (code); corruption scenario PLAUSIBLE
- Where: `distributed_macros/src/snapshot.rs:44-187`; contract at `src/snapshot/snapshottable.rs:15-36`
- What: The derive emits `XSnapshot` with every non-skipped field cloned, `fn snapshot()`, and a restore that assigns field by field. Skip detection (`:263`) recognises only `#[serde(skip)]`. `SNAPSHOT_VERSION` defaults to 1, and the trait docs state the encoding is positional bitcode, so reordering two same-typed fields "decodes successfully into the wrong state". The macro has the field names and types and already fingerprints schemas for `DomainState` (`shared.rs:223-259`, `:564`).
- Why it matters: This is a parallel type of the kind the owner's preferences rule out. An optimisation that needs a manual version bump to stay correct is not transparent.
- Change: Emit an automatic layout fingerprint (hash of ordered field names and types) as the version, or as an extra component alongside a manual bump. Consider snapshotting the aggregate itself, since it already derives `Serialize`/`Deserialize` with `entity` skipped. Breaking. Size M.

### 14. `domain = event` re-implements `#[derive(DomainEvent)]`; deletion descriptor is built three times
- Category: C   Severity: medium   Confidence: CONFIRMED
- Where: `distributed_macros/src/sourced.rs:429-473` versus `domain_event.rs:60-104`; `sourced.rs:502-519`, `552-562`, `894-912`
- What: The generated flat body struct plus its five impls are a hand-copied version of the derive's output, with bare paths. The `DomainDeletion<Identity>` schema, fingerprint and descriptor literal are computed three times in the same file.
- Change: Share one codegen function between derive and `domain = event`. Add a runtime `DomainEventDescriptor::deletion::<K>(name, version)` and call it from all three sites. Size S.

### 15. Helpers reinvented within and across the macro crates
- Category: C   Severity: low   Confidence: CONFIRMED
- Where:
  - snake-to-Pascal: `sourced.rs:1435`, `aggregate_command.rs:384`, `portable_command.rs:176`, `mutation.rs:335`, `aggregate.rs:81`, `shared.rs:471-487`.
  - serde `rename_all`: `shared.rs:471-509` and `command_types.rs:37-99`. The two differ on `lowercase` (one lowercases, the other returns the name unchanged).
  - `named_fields`: `domain_state.rs`, `domain_event.rs`, `read_model/mod.rs:79`, and the game `shared.rs`.
  - `parse_expr_array` and the whole parse skeleton: `module.rs` and `application.rs` are near-identical files.
  - `enqueue.rs:26-48` re-implements `shared::generate_enqueue_call` (`shared.rs:201-220`).
  - "ident-or-string first, then `key = value` loop" is hand-written four times (`sourced.rs:29-97`, `113-197`, `digest.rs`, `enqueue.rs`).
- Change: One `names.rs` and one `serde_attrs.rs`; use `syn::meta::parser` or `parse_nested_meta` for key/value lists. Most of this disappears with findings 5–7. Size S–M.

### 16. Type-name heuristics stand in for trait bounds
- Category: E   Severity: low   Confidence: CONFIRMED
- Where: `distributed_macros/src/shared.rs:64-73` (`Result`/`SourcedResult` by last segment), `:511-528` (`portable_projection_kind` by identifier), `command.rs:416-446` (`CausalCommandContext` by identifier), `command.rs:548` (`same_type` by token string), `aggregate_command.rs:364-378`
- What: A type alias, a user type named `Value` or `Vec`, or `std::string::String` versus `String` changes the result. The `DomainState` fingerprint hashes type tokens (`shared.rs:234`), so `String` and `std::string::String` produce different contract fingerprints for the same wire shape.
- Change: Where a trait exists (`CommandOutcome`, a `PortableKind` trait implemented for primitives), emit `<T as Trait>::CONST` and let rustc resolve it. Size M; the `portable_projection_kind` case first.

### 17. Compile-fail suite: mostly valuable, with 18 rustc-pinned snapshots
- Category: B   Severity: medium   Confidence: CONFIRMED
- Where: `distributed_macros/tests/{compile_fail, command_types_compile_fail, command_input_defaults_compile_fail}/`, `distributed_game_macros/tests/compile_fail/`
- What: There are 75 `.stderr` snapshots (39 + 6 + 4 in core, 26 in game). 57 contain only the macro's own `error:` text; these are short (5–8 lines) and worth keeping. 18 are rustc `error[E…]` output.
  - Brittle, with standard-library spans, "and $N others" lists or trait-solver wording: `application_command_duplicate_id.stderr` (prints `$RUST/core/src/panic.rs` internals), `input_default_nullable.stderr`, `input_default_list.stderr`, `input_default_wrong_type.stderr`, `game_pure_unsupported_output.stderr`, `input_buttons_on_float.stderr`, `input_unsupported_field.stderr`, `replicated_unsupported_type.stderr`, `replicated_quant_on_integer.stderr`, `predict_returns_state.stderr`.
  - Short rustc errors that test a deliberate const-panic or type check; these are acceptable: `domain_adapter_contract_{name,version}_mismatch`, `aggregate_command_principal_input_field`, `domain_adapter_{mismatch,output_mismatch}`, `replicated_from_{missing_field,wrong_type}`.
  - `enqueue_renamed_entity.stderr` pins rustc's "no field `entity`" for a macro slated for removal.
  - Maintenance cost shows in the tooling: `trybuild` is pinned to `=1.0.121` in both macro crates and `=1.0.118` in `tests/fixtures/renamed-dependency-trybuild/Cargo.toml:15`, and `compile_fail.rs` says regeneration needs `rust-src`.
  - Fixtures repeat a 10–15 line aggregate/input/handler preamble across the 11 `application_command_*.rs` files (188 lines in total).
  - Two files under `distributed_macros/tests/compile_fail/` belong to no documented macro mode after the removals above: `enqueue_renamed_entity`, `aggregate_unknown_keyword`.
- Change: Keep the 57 macro-diagnostic snapshots. For the 10 brittle ones, either assert that the build fails with a substring (a small harness over `cargo check --message-format=json`) or move the check into the macro so the error is its own. Share one fixture prelude via `include!`. Size M.

### 18. Expansion-string tests assert token text instead of behaviour
- Category: B   Severity: medium   Confidence: CONFIRMED
- Where: `distributed_macros/src/entry_tests.rs` (35 tests, 75 `contains(..)` assertions), `src/read_model/tests.rs` (31 tests, 49), `src/snapshot.rs` (13 tests, 18)
- What: Tests assert on `quote!` output text with token spacing, for example:
  - `expand_sourced_generates_enum_and_aggregate` checks `out.contains("impl distributed :: Aggregate for Todo")`.
  - `expand_sourced_places_state_capture_after_transition_body` compares the string offsets of `digest_v`, `self . completed = true` and `capture_domain_state`.
  - `expand_portable_command_emits_thin_mount` checks `contains("load_by")`.
  - `expand_snapshot_accepts_named_struct` and `expand_read_model_accepts_named_id_field` check `contains("fn id")`.
  - `expand_digest_inserts_digest_call` checks `out.contains("digest")`, which is true of the input attribute name.
  
  These break on any refactor, including finding 11, and pass when the generated code is wrong. The `*_rejects_*` tests on error messages duplicate trybuild fixtures: `expand_sourced_rejects_duplicate_event_names` and `duplicate_event_names.rs`; `expand_sourced_rejects_variant_ident_collisions` and `sourced_variant_collision.rs`; `expand_snapshot_rejects_tuple_structs` and `snapshot_tuple_struct.rs`.
  
  Low-value tests in the authoring examples:
  - `create_uses_handle_escape_hatch` and `post_uses_handle_escape_hatch` (todo-domain `commands/create.rs`, chat-domain `commands/post.rs`) are `assert_eq!(X::COMMAND, "..."); let _ = handle;` and cannot fail meaningfully.
  - `shard_is_todo_id` is copied once per command file to test a one-line closure.
  - `tests/legacy_authoring_absence.rs` greps `src/lib.rs` for substrings to prove removed macros stay removed.
- Missing behavioural tests: a failing fallible `#[event]` body (finding 1); a conditional overwrite after a "known" assignment (finding 2); a renamed dependency with `#[sourced]`, `Snapshot` or `ReadModel` (finding 11); staleness of `mutation_file!` (finding 4).
- Change: Keep parser unit tests such as `parse_*_args_*` and the fingerprint-freeze tests (`expansion_freezes_domain_state_schema_and_fingerprint` pins a wire contract). Replace `contains` tests with the existing behavioural suites under `tests/sourced` and `tests/aggregate_command`, and delete the duplicated rejects. Size M.

### 19. Generated panics on runtime paths
- Category: E   Severity: low   Confidence: CONFIRMED
- Where: `distributed_macros/src/command.rs:346`, `:398`, `:408`; `module.rs:129`; `application.rs:116`; `mutation.rs:664` and the `.expect` calls from `696` to `915`
- What: These are `LazyLock` initialisers with `unwrap_or_else(|e| panic!(..))`. The first access panics, possibly while serving a request, and the lock is poisoned afterwards. The mutation `.expect("field")` messages carry no model or column.
- Why it matters: `sourced.rs` and `aggregate_command.rs` are clean here (errors map to `EventRecordError` and `HandlerError`), so this is confined to code that findings 5, 6 and 12 remove or rewrite.
- Change: Covered by 5, 6 and 12; whatever remains should return `Result` from a function validated at `Service` build time.

### 20. `FactCommand` and `TicketExtension` reject field names by a deny list
- Category: F   Severity: low   Confidence: CONFIRMED
- Where: `distributed_game_macros/src/trusted.rs` (`FORBIDDEN`, 24 names including `user`, `amount`, `cost`, `credit`, `grant`, `sub`), mirrored in `distributed_game/tests/fixtures/trusted_field_denylist.json`
- What: The derive fails compilation when a field or serde rename normalises to a listed word. It is a lint, not a security boundary (`gold_amt` passes), and it blocks legitimate names such as `amount` of damage or `cost` of a path. The test `deny_list_matches_the_shared_fixture` parses the JSON by string slicing.
- Change: Keep it if the owner values the guard rail, but add an opt-out (`#[fact(allow_name)]`) and say in the error that it is a lint. Otherwise leave it alone.

## Things that are good and should not be changed
- **`distributed_game_macros` as the template.** Expansions are thin calls into generic runtime functions (`#rt::__private::wasm_export::<Step>(..)`, `<T as Wire>::encode`). Crate paths are resolved once, items use `::core`/`::std` paths, errors are spanned at the user's token with `quote_spanned!`, and multiple errors are combined (`trusted.rs` `reject_forbidden`).
- **`aggregate_command.rs`.** The `#[principal]` parameter is bound only from the verified session, with a compile-time check that the input has no field of the same name (`:520-534`). The generated handler is small and panic-free. This is the right canonical command form.
- **Domain-recorder restrictions in `#[sourced]`.** No return type, no `?`, no `return` (`sourced.rs:257-289`, `761-771`), each with a pointed error. Finding 1 extends this rule; it does not relax it.
- **Compile-time schema fingerprints and typed descriptors** for `DomainState` and `DomainEvent`, and keeping the state DTO separate from the aggregate. The freeze tests on these fingerprints are worth keeping.
- **The 57 short compile-fail snapshots of the macros' own diagnostics**, and the duplicate-name and unknown-key checks behind them.
