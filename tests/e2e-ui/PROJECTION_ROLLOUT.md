# Changing a projection

The read model is an ordinary SQL database. Projections write updates to it.
A projection declares no epoch and nothing at startup compares the running
program with the stored data, so a rollout resets nothing and refuses nothing.

## What a change does

A changed projection applies to the events delivered after it is deployed.
Rows already written stay as they are. During a rollout old and new nodes run
different programs against the same tables; each event is projected by the
program of the node that received it.

Knowing the program ahead of time is used for one thing: the browser applies
the same mutation for optimism. That never gates a server-side write.

## Additive change: same projection

Add an optional or defaulted column.

1. Ship the column as the application's own migration. The framework creates
   tables for development and never alters one.
2. Deploy the projection that writes it.

Old rows keep the default until a later event rewrites them. Backfill them
with ordinary SQL if the application needs the value sooner.

## Breaking change: a new table and a new projection

Anything that is not additive (a renamed or retyped column, a different key,
a different meaning for existing rows) is a new read model.

1. Add the new table and a new `projection!` with its own name and model. It
   runs next to the old one; two projections never share a table.
2. Fill the new table for existing data: the application's own backfill, or
   an explicit snapshot rebuild where the projection supports one
   (see [Projections and read models](../../docs/guides/projections-and-read-models.md)).
3. Switch readers to the new model.
4. Remove the old projection and drop its table.

## When a projector stops

Each projector has its own consumer. One that cannot accept an input records
a durable failure and stops alone; the others keep projecting. Its error
names the projection and carries a `ProjectionRepairHandle`. Fix the cause,
call `Service::repair_projection(&handle)`, and restart: it resumes from the
failed input.

## Fixture projections

| Projection | Model | Placement |
|---|---|---|
| `project_todos` | `Todos` | eventual |
| `project_chat_messages` | `ChatMessages` | eventual |
| `project_blob` | `BlobGames` | direct |
| `project_auth_users` | `AuthUsers` | eventual |
