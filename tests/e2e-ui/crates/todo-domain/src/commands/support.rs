use distributed::microsvc::CausalCommandContext;
use distributed::Aggregate;

pub(super) fn authenticated_user<A>(ctx: &CausalCommandContext<'_, A>) -> bool
where
    A: Aggregate + Send + Sync + 'static,
{
    ctx.session().user_id().is_some_and(|id| !id.is_empty())
}

pub(super) fn admin_user<A>(ctx: &CausalCommandContext<'_, A>) -> bool
where
    A: Aggregate + Send + Sync + 'static,
{
    authenticated_user(ctx) && ctx.session().has_role("admin")
}
