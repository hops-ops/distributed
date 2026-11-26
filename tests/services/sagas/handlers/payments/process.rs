use super::*;

pub const COMMAND: &str = "payment.process";

#[derive(Debug, Serialize, distributed::command::CommandOutput)]
pub struct Output {
    pub payment_id: String,
}

/// Delivered by the saga over the bus, so it declares the transport role.
pub fn command() -> TypedCommand<ProcessPaymentMsg, Succeeded<Output>> {
    typed_command::<ProcessPaymentMsg, Succeeded<Output>>(COMMAND).roles([TRANSPORT_ROLE])
}

pub async fn handle(
    ctx: &CausalCommandContext<'_, Payment>,
    input: ProcessPaymentMsg,
) -> Result<PreparedCommand<Succeeded<Output>>, HandlerError> {
    let payment_id = format!("pay-{}", input.order_id);
    let mut payment = ctx.repo().create();
    payment.initiate(
        payment_id.clone(),
        input.order_id.clone(),
        input.amount_cents,
    )?;
    payment.authorize("txn-distributed-001".to_string())?;
    payment.capture()?;

    let msg = json_outbox_to(
        &format!("{}-payment-succeeded", input.order_id),
        "payment.succeeded",
        "saga",
        &PaymentSucceededMsg {
            saga_id: input.saga_id,
            order_id: input.order_id,
        },
    )?;

    ctx.repo()
        .outbox(msg)
        .commit(payment)?
        .succeeded(Output { payment_id })
}
