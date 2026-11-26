# Process roles and system slices

**Status: not built. Open question.** Nothing on this page exists in the
code. It records a direction that was proposed for
[application composition](../application-composition.md) and the reasons it
is still open.

## The idea

Package each bounded context once, then cut deployables from it without
touching the packages:

- **System slice.** A context's commands and projections as one value, from
  which a deployable takes all of it, only the commands, or only the
  projectors.
- **Process role.** A selector that decides what one process spawns:

  | Role | Would run |
  |---|---|
  | Full | Commands, eventual projectors, outbox drain, consumer, GraphQL |
  | CommandWriter | Commands and outbox publish, no consumer |
  | EventualProjector | Bus consumer and projector mounts, no public writes |
  | QueryApi | GraphQL queries and surfaces only |

- **Atomic mount check.** Refuse an `Atomic` command, or a `.direct(..)`
  projection, in a process that has no write path for it, because the row
  can only be sealed in the command's own transaction.
- **Command definitions.** A `CommandDef` value next to each handler,
  registered with one line.

## What exists instead

- Commands are declared next to their aggregate (`#[command]`, `command!`)
  and registered with `.mount(..)`, which covers the last bullet.
- A process is the `Service` it composes. A writer-only or projector-only
  process is composed by hand: mount the commands without
  `.consume_projection(..)`, or mount only the projectors.
- README principle 07 deliberately avoids a role flag: the application is the
  list of modules in one `Service`, and a role flag would be a second way to
  say what a process runs.
- An earlier application `Runtime` with `ProcessPreset` and `MountSelector`
  was removed in v5 for the same reason.

## Open questions

- Is a split deployment common enough to justify a framework concept, or is
  composing two `Service`s by hand enough?
- If it is built, can it avoid a second source of truth for what a process
  mounts?
- Should the Atomic-placement check exist without process roles, for example
  as validation when a `Service` mounts an Atomic command but no store write
  path?
- Can a dual-process smoke test (writer plus projector, sharing crates with
  the Full e2e-ui process) be added first, to find out what is missing?
