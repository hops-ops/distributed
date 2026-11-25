//! Command guards for the `guard:` slot of `command!` and
//! [`TypedRouteBuilder::guarded`](crate::microsvc::TypedRouteBuilder::guarded).
//!
//! A guard admits a session before the handler runs; a refused command is
//! answered without loading its aggregate.

use crate::microsvc::CausalCommandContext;

/// The session names a user (`x-user-id`, from dev headers or an OIDC
/// subject).
pub fn authenticated<A>(ctx: &CausalCommandContext<'_, A>) -> bool {
    ctx.user_id().is_ok()
}

/// An [`authenticated`] user holding the engine role `role`.
pub fn role<A: 'static>(
    role: &'static str,
) -> impl Fn(&CausalCommandContext<'_, A>) -> bool + Send + Sync + 'static {
    move |ctx| authenticated(ctx) && ctx.session().has_role(role)
}
