use super::*;

pub const COMMAND: &str = "order.complete";

#[derive(Debug, Serialize, distributed::CommandOutput)]
pub struct Output {
    pub order_id: String,
}

/// Delivered by the saga over the bus, so it declares the transport role.
pub fn command() -> TypedCommand<CompleteOrderMsg, Succeeded<Output>> {
    typed_command::<CompleteOrderMsg, Succeeded<Output>>(COMMAND).roles([TRANSPORT_ROLE])
}

pub async fn handle(
    ctx: &CausalCommandContext<'_, Order>,
    input: CompleteOrderMsg,
) -> Result<PreparedCommand<Succeeded<Output>>, HandlerError> {
    let mut order = ctx
        .repo()
        .get(&input.order_id)
        .await?
        .ok_or_else(|| HandlerError::NotFound(input.order_id.clone()))?;
    order.mark_inventory_reserved()?;
    order.mark_payment_processed()?;
    order.complete()?;

    let msg = json_outbox_to(
        &format!("{}-order-completed", input.order_id),
        "order.completed",
        "saga",
        &OrderCompletedMsg {
            saga_id: input.saga_id,
            order_id: input.order_id.clone(),
        },
    )?;

    ctx.repo().outbox(msg).commit(order)?.succeeded(Output {
        order_id: input.order_id,
    })
}
