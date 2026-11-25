use super::*;

pub const EVENT: &str = "payment.succeeded";

pub fn guard(ctx: &Context<Repo>) -> bool {
    ctx.has_fields(&["saga_id", "order_id"])
}

pub async fn handle(ctx: &Context<'_, Repo>) -> Result<Value, HandlerError> {
    let input = ctx.input::<PaymentSucceededMsg>()?;

    let mut saga = ctx
        .repo()
        .get(&input.saga_id)
        .await?
        .ok_or_else(|| HandlerError::NotFound(input.saga_id.clone()))?;
    saga.payment_succeeded()?;

    let msg = json_outbox_to(
        &format!("{}-complete-order", input.saga_id),
        "order.complete",
        "orders",
        &CompleteOrderMsg {
            saga_id: input.saga_id.clone(),
            order_id: input.order_id,
        },
    )?;

    ctx.repo().outbox(msg).commit(&mut saga).await?;
    Ok(json!({ "next": "order.complete" }))
}
