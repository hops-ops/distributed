//! Project `zitadel.user.*.v1` → `auth_users` (join target for chat + blob games).

use distributed::microsvc::{Context, HandlerError};
use distributed::read_model::ReadModelWritePlanBuilder;
use e2e_projections::{map_zitadel_user_status, map_zitadel_user_upsert, ZitadelUserPayload};
use serde_json::{json, Value};

use crate::deps::AuthDeps;
use crate::handlers::util::{decode_payload, read_model_error};

pub const EVENTS: &[&str] = &[
    "zitadel.user.human.created.v1",
    "zitadel.user.human.updated.v1",
    "zitadel.user.human.deactivated.v1",
    "zitadel.user.human.reactivated.v1",
    "zitadel.user.machine.created.v1",
];

pub fn guard<R, S>(_ctx: &Context<AuthDeps<R, S>>) -> bool
where
    R: crate::bounds::EventStore,
    S: crate::bounds::ReadStore,
{
    true
}

pub async fn handle<R, S>(ctx: &Context<'_, AuthDeps<R, S>>) -> Result<Value, HandlerError>
where
    R: crate::bounds::EventStore,
    S: crate::bounds::ReadStore,
{
    let payload: ZitadelUserPayload = decode_payload(ctx.message())?;
    let name = ctx.message().name();
    let row = if name.contains("deactivated") || name.contains("reactivated") {
        map_zitadel_user_status(name, &payload)
    } else {
        map_zitadel_user_upsert(name, &payload)
    };

    let store = ctx.read_model_store();
    let mut plan = ReadModelWritePlanBuilder::new();
    plan.upsert(&row).map_err(read_model_error)?;
    plan.commit(store).await.map_err(read_model_error)?;

    Ok(json!({
        "event": name,
        "user_id": row.user_id,
        "status": row.status,
        "display_name": row.display_name,
    }))
}
