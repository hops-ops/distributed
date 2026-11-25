use super::*;

pub const COMMAND: &str = "inventory.initialize";

#[derive(Debug, Serialize, distributed::CommandOutput)]
pub struct Output {
    pub sku: String,
    pub stock: u32,
}

pub fn command() -> TypedCommand<InitInventoryInput, Succeeded<Output>> {
    typed_command::<InitInventoryInput, Succeeded<Output>>(COMMAND)
}

pub async fn handle(
    ctx: &CausalCommandContext<'_, Inventory>,
    input: InitInventoryInput,
) -> Result<PreparedCommand<Succeeded<Output>>, HandlerError> {
    let mut inv = ctx.repo().create();
    inv.initialize(input.sku.clone(), input.stock)?;

    ctx.repo().commit(inv)?.succeeded(Output {
        sku: input.sku,
        stock: input.stock,
    })
}
