use super::*;

pub const EVENT: &str = "inventory.reserved";

pub fn guard(ctx: &Context<Repo>) -> bool {
    ctx.has_fields(&["saga_id", "order_id"])
}

pub async fn handle(ctx: &Context<'_, Repo>) -> Result<Value, HandlerError> {
    let input = ctx.input::<InventoryReservedMsg>()?;

    let mut saga = ctx
        .repo()
        .get(&input.saga_id)
        .await?
        .ok_or_else(|| HandlerError::NotFound(input.saga_id.clone()))?;
    saga.inventory_reserved()?;

    let msg = json_outbox_to(
        &format!("{}-process-payment", input.saga_id),
        "payment.process",
        "payments",
        &ProcessPaymentMsg {
            saga_id: input.saga_id.clone(),
            order_id: input.order_id,
            amount_cents: saga.total_cents(),
        },
    )?;

    ctx.repo().outbox(msg).commit(&mut saga).await?;
    Ok(json!({ "next": "payment.process" }))
}
