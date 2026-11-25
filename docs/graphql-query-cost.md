# GraphQL query cost

What one GraphQL request may ask the database for, and where each limit is
enforced. The engine compiles every root field to one SQL statement; the
limits below bound that statement before it runs, and the statement timeout
bounds it while it runs.

## The knobs

Each knob has one unit and is checked in one place. All are set on
`GraphqlEngineBuilder`; the defaults live in `src/graphql/cost.rs`.

| Builder method | Default | Unit | Enforced by |
|---|---|---|---|
| `max_depth` | 8 | nesting levels: fields in the document; relationships and `where` (boolean and relationship nesting) in the compiler | async-graphql `limit_depth`; the compiler |
| `max_fields` | 500 | selected fields in one document, each field once at every level | async-graphql `limit_complexity` |
| `max_rows` | 200,000 | model rows one root field's statement can produce (below) | the compiler, per root field |
| `default_limit` | 100 | page size of a list whose request gives no `limit` | the compiler, per list |
| `max_limit` | 1,000 | largest page size any list resolves to; a role's `limit` can lower it | the compiler, per list |
| `max_in_list` | 1,000 | entries in one `_in` / `_nin` list | the compiler |
| `max_bool_width` | 256 | entries in one `_and` / `_or` list | the compiler |
| `statement_timeout` | 5 s | wall clock per root field | PostgreSQL `statement_timeout`, plus an executor deadline on every backend |

A request over a limit fails with `extensions.code` `BAD_REQUEST` and a fixed
message (`max depth exceeded`, `row budget exceeded`, `list too long`, …)
before any SQL runs. A statement that times out fails with `TIMEOUT`
(`statement timeout`).

## Rows

`max_rows` bounds how many model rows one root field can return, counted from
each list's *resolved* page size: the client's `limit`, else `default_limit`,
capped by the role's `limit` and by `max_limit`.

- A page of `limit` rows costs `limit × (1 + rows nested under one of its rows)`.
- One object (a by-primary-key root, a belongs-to relationship) costs
  `1 + rows nested under it`.
- Columns, `__typename` and `aggregate { count }` cost nothing. An aggregate's
  `nodes` are a page.
- Siblings add. Arithmetic saturates, so an overflowing cost is refused, never
  wrapped.

For one chain of nested lists the cost is about the product of their limits:

| Request | Rows | Default budget |
|---|---|---|
| `orders { id }` | 100 | admitted |
| `orders { id items { id } }` | 100 × (1 + 100) = 10,100 | admitted |
| `orders { items { parts { id } } }` | 100 × (1 + 100 × (1 + 100)) = 1,010,100 | refused |
| `orders(limit: 1000) { items(limit: 1000) { id } }` | 1000 × (1 + 1000) = 1,001,000 | refused |
| `orders(limit: 1000) { items { id } }` | 1000 × (1 + 100) = 101,000 | admitted |
| `orders { a { id } b { id } c { id } }` | 100 × (1 + 100 + 100 + 100) = 30,100 | admitted |
| `orders(limit: 1000) { items(limit: 199) { id } }` | 1000 × (1 + 199) = 200,000 | admitted |
| `orders_by_pk(id: 1) { customer { name } }` | 1 + 1 = 2 | admitted |

`offset` is not part of the cost: skipped rows are scanned, not returned, and
the statement timeout bounds the scan.

The budget is per root field. A document with several root fields is bounded
across fields by `max_fields`; a causal (protocol) request may carry only one
read root.

## Client manifests

Client manifests export the limits a generated client must respect as
`execution`:

```json
{ "max_depth": 8, "max_fields": 500, "max_rows": 200000, "max_bool_width": 256, "max_in_list": 1000 }
```

The client compiler (`distributed client compile`) computes rows with the same
functions (`distributed::graphql::row_cost`) and refuses an operation whose
literal or default page sizes already exceed `max_rows`. A variable `limit` is
unknown at build time and counts as the field's default page size; the engine
checks the actual value on every request.
