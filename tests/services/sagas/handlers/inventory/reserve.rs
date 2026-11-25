use super::*;

pub const COMMAND: &str = "inventory.reserve";

#[derive(Debug, Serialize, distributed::CommandOutput)]
pub struct Output {
    pub reserved: u32,
}

/// Delivered by the saga over the bus, so it declares the transport role.
pub fn command() -> TypedCommand<ReserveInventoryMsg, Succeeded<Output>> {
    typed_command::<ReserveInventoryMsg, Succeeded<Output>>(COMMAND).roles([TRANSPORT_ROLE])
}

pub async fn handle(
    ctx: &CausalCommandContext<'_, Inventory>,
    input: ReserveInventoryMsg,
) -> Result<PreparedCommand<Succeeded<Output>>, HandlerError> {
    let mut inv = ctx
        .repo()
        .get(&input.sku)
        .await?
        .ok_or_else(|| HandlerError::NotFound(input.sku.clone()))?;

    if !inv.can_reserve(input.quantity) {
        return Err(HandlerError::Rejected("insufficient stock".into()));
    }
    inv.reserve(input.order_id.clone(), input.quantity)?;

    let msg = json_outbox_to(
        &format!("{}-inventory-reserved", input.order_id),
        "inventory.reserved",
        "saga",
        &InventoryReservedMsg {
            saga_id: input.saga_id,
            order_id: input.order_id,
        },
    )?;

    ctx.repo().outbox(msg).commit(inv)?.succeeded(Output {
        reserved: input.quantity,
    })
}
