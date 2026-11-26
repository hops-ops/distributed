use super::*;

pub const COMMAND: &str = "order.initialize";

#[derive(Debug, Serialize, distributed::command::CommandOutput)]
pub struct Output {
    pub order_id: String,
}

/// Delivered by the saga over the bus, so it declares the transport role.
pub fn command() -> TypedCommand<CreateOrderMsg, Succeeded<Output>> {
    typed_command::<CreateOrderMsg, Succeeded<Output>>(COMMAND).roles([TRANSPORT_ROLE])
}

pub async fn handle(
    ctx: &CausalCommandContext<'_, Order>,
    input: CreateOrderMsg,
) -> Result<PreparedCommand<Succeeded<Output>>, HandlerError> {
    let mut order = ctx.repo().create();
    order.create(input.order_id.clone(), input.customer_id, input.items)?;

    let msg = json_outbox_to(
        &format!("{}-order-created", input.order_id),
        "order.initialized",
        "saga",
        &OrderInitializedMsg {
            saga_id: input.saga_id,
            order_id: input.order_id.clone(),
        },
    )?;

    ctx.repo().outbox(msg).commit(order)?.succeeded(Output {
        order_id: input.order_id,
    })
}
