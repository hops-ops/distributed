//! Fixtures and conformance suites shared by the `distributed` integration
//! tests.
//!
//! Integration tests depend on this crate as `common` (a dev-dependency of
//! `distributed`). Modules that need a backend are compiled only when the test
//! build enables it: `distributed`'s `sqlite`, `postgres` and `graphql`
//! features forward to this crate's features of the same name.
//!
//! # Conformance suites
//!
//! A contract (repository, lease store, lock manager, schedule worker) is a
//! list of scenarios written once as `async fn`s over a backend trait, plus a
//! macro that expands the identical case list for one backend:
//!
//! ```ignore
//! mod sqlite {
//!     common::lease_conformance_tests!(
//!         common::lease::Sqlite,
//!         controlled_clock: false,
//!         standalone_migration: true,
//!     );
//! }
//! ```
//!
//! A scenario a backend cannot run needs a capability: a trait the backend
//! implements, named by a `capability: true | false` argument the macro
//! requires for every backend. Leaving a case out is therefore always a
//! visible, named decision, and claiming a capability the backend does not
//! implement does not compile.
//!
//! Each macro emits one `#[tokio::test]` per case; the invoking crate must
//! depend on `tokio` with the `macros` and `rt-multi-thread` features.

// Backend traits are only ever used generically, never as `dyn`.
#![allow(async_fn_in_trait)]

/// Connections in each SQL test pool (`sqlite::TempDb::pool`,
/// `postgres::PostgresTestSchema::repository`). A race over one pool puts
/// at most this many contenders in the database at once; any more wait in the
/// pool's queue, where a slow commit can outlast the pool's acquire timeout.
pub const POOL_CONNECTIONS: u32 = 5;

pub mod commands;
pub mod env;
pub mod ids;
pub mod lease;
pub mod outbox;
pub mod read_models;
pub mod repository;
pub mod session;
pub mod todo;
pub mod transport;

#[cfg(all(feature = "graphql", feature = "sqlite"))]
pub mod graphql;
#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub mod lock;
#[cfg(all(feature = "graphql", feature = "sqlite"))]
pub mod oidc;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub mod schedule;
#[cfg(feature = "sqlite")]
pub mod sqlite;

/// The case expander behind every `*_conformance_tests!` macro.
///
/// `$trait` is the backend trait (brought into scope for the case bodies)
/// and `$open` its constructor. Each case
/// is `#[test attribute] name(backend) => future;`: the test opens the
/// backend, skips when `open` returns `None`, and awaits the future.
/// `@if false` expands nothing, which is how a capability leaves cases out.
#[doc(hidden)]
#[macro_export]
macro_rules! conformance_cases {
    (@if false $($rest:tt)*) => {};
    (@if true $($rest:tt)*) => {
        $crate::conformance_cases! { $($rest)* }
    };
    (@unless true $($rest:tt)*) => {};
    (@unless false $($rest:tt)*) => {
        $crate::conformance_cases! { $($rest)* }
    };
    ($trait:path, $backend:ty, $open:ident;
        $( $(#[$meta:meta])* $name:ident($b:ident) => $body:expr; )*
    ) => {
        $(
            $(#[$meta])*
            async fn $name() {
                #[allow(unused_imports)]
                use $trait as _;
                let Some($b) = <$backend>::$open(stringify!($name)).await
                else {
                    return;
                };
                $body.await;
            }
        )*
    };
}
