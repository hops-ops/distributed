use super::*;

pub const COMMAND: &str = "saga.start";

#[derive(Debug, Serialize, distributed::command::CommandOutput)]
pub struct Output {
    pub saga_id: String,
}

pub fn command() -> TypedCommand<StartSagaInput, Succeeded<Output>> {
    typed_command::<StartSagaInput, Succeeded<Output>>(COMMAND)
}

pub async fn handle(
    ctx: &CausalCommandContext<'_, OrderFulfillmentSaga>,
    input: StartSagaInput,
) -> Result<PreparedCommand<Succeeded<Output>>, HandlerError> {
    let mut saga = ctx.repo().create();
    saga.start(
        input.saga_id.clone(),
        input.order_id.clone(),
        input.customer_id.clone(),
        input.items.clone(),
        input.total_cents,
    )?;

    let msg = json_outbox_to(
        &format!("{}-create-order", input.saga_id),
        "order.initialize",
        "orders",
        &CreateOrderMsg {
            saga_id: input.saga_id.clone(),
            order_id: input.order_id,
            customer_id: input.customer_id,
            items: input.items,
            total_cents: input.total_cents,
        },
    )?;

    ctx.repo().outbox(msg).commit(saga)?.succeeded(Output {
        saga_id: input.saga_id,
    })
}
