//! Todo service crate for the celld example.
//!
//! Domain commands stay in `todo-domain`. Every Todo aggregate transition is
//! wait-dispatched to celld through [`distributed::cell_host::CelldCommandHost`].

mod host;
mod routes;

pub use host::celld_route;
pub use routes::routes;
