//! GraphQL harden / red-team suite.
//!
//! Modules map to threat categories (S/A/D/E/T). Every test drives shipped
//! `GraphqlEngine::execute` and/or microsvc HTTP — no SQL reimplementation.
//!
//! | Module | Coverage |
//! |---|---|
//! | `access_paths` | Row + column policy on every read access path (one table of cases) |
//! | `authz` | A* AuthZ (claim list/by_pk/aggregate, column allowlists, nested deny) |
//! | `inject` | S* injection / key safety |
//! | `dos` | D* depth, in-list, bool-width, concurrency |
//! | `errors` | E* leak + TIMEOUT |
//! | `transport` | T* introspection HTTP, mutation grants, GraphiQL policy |
//! | `softskip` | Fail-closed where/order contracts (strict_where default) |
//! | `residual` | A8/A12/S9/E4 residual post-quality-1 review |
//! | `dialect` | Dialect-honest comparison ops (no PG JSON ops on SQLite) |

#![cfg(all(feature = "graphql", feature = "sqlite"))]

mod access_paths;
mod authz;
mod common;
mod dialect;
mod dos;
mod errors;
mod inject;
mod residual;
mod softskip;
mod transport;
