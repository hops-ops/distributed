use super::*;

pub const EVENT: &str = "order.initialized";

pub fn guard(ctx: &Context<Repo>) -> bool {
    ctx.has_fields(&["saga_id", "order_id"])
}

pub async fn handle(ctx: &Context<'_, Repo>) -> Result<Value, HandlerError> {
    let input = ctx.input::<OrderInitializedMsg>()?;

    let mut saga = ctx
        .repo()
        .get(&input.saga_id)
        .await?
        .ok_or_else(|| HandlerError::NotFound(input.saga_id.clone()))?;

    let sku = saga.items()[0].sku.clone();
    let quantity = saga.items()[0].quantity;

    let msg = json_outbox_to(
        &format!("{}-reserve-inventory", input.saga_id),
        "inventory.reserve",
        "inventory",
        &ReserveInventoryMsg {
            saga_id: input.saga_id.clone(),
            order_id: input.order_id,
            sku,
            quantity,
        },
    )?;

    ctx.repo().outbox(msg).commit(&mut saga).await?;
    Ok(json!({ "next": "inventory.reserve" }))
}
