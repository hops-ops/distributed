use super::*;

pub const EVENT: &str = "order.completed";

pub fn guard(ctx: &Context<Repo>) -> bool {
    ctx.has_fields(&["saga_id", "order_id"])
}

pub async fn handle(ctx: &Context<'_, Repo>) -> Result<Value, HandlerError> {
    let input = ctx.input::<OrderCompletedMsg>()?;

    let mut saga = ctx
        .repo()
        .get(&input.saga_id)
        .await?
        .ok_or_else(|| HandlerError::NotFound(input.saga_id.clone()))?;
    saga.complete()?;

    ctx.repo().commit(&mut saga).await?;
    Ok(json!({ "saga_id": input.saga_id, "status": "completed" }))
}
