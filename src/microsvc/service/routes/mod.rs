//! Route bundles: what a handler declares ([`spec`]), how one is built
//! ([`builder`]) and registered ([`registry`]), the typed command pipeline
//! ([`execute`]) behind the type-erased seams ([`erased`]), and the outbox
//! publisher a bus installs ([`outbox_wiring`]).

mod builder;
mod erased;
mod execute;
mod outbox_wiring;
mod registry;
mod spec;

pub use builder::{
    PortableCommand, RouteBuilder, ThinCommandBuilder, ThinCommandInvoked, ThinCommandLoaded,
    TypedRouteBuilder,
};
pub(crate) use erased::LedgerCompaction;
pub(super) use erased::{CausalCommandPolicy, ErasedRoutes};
pub(super) use outbox_wiring::configure_outbox_for;
pub(crate) use outbox_wiring::DynBusPublisher;
pub use registry::Routes;
pub use spec::{DeliveryKind, HandlerNames, HandlerSpec};
