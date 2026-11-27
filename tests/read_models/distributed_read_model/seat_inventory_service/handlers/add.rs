use distributed::command::{typed_command, PreparedCommand, Succeeded, TypedCommand};
use distributed::microsvc::{CausalCommandContext, HandlerError};
use serde::Serialize;

use crate::distributed_read_model::checkout::{
    json_outbox_event, seat_command, seat_event, AddSeat, SeatAdded,
};
use crate::distributed_read_model::seat_inventory_service::Seat;

pub const COMMAND: &str = seat_command::ADD;

#[derive(Debug, Serialize, distributed::command::CommandOutput)]
pub struct Output {
    pub seat_id: String,
}

pub fn command() -> TypedCommand<AddSeat, Succeeded<Output>> {
    typed_command::<AddSeat, Succeeded<Output>>(COMMAND)
}

pub async fn handle(
    ctx: &CausalCommandContext<'_, Seat>,
    msg: AddSeat,
) -> Result<PreparedCommand<Succeeded<Output>>, HandlerError> {
    let mut seat = ctx.repo().create();
    seat.add(msg.seat_id.clone(), msg.category.clone())?;

    let event = SeatAdded {
        seat_id: msg.seat_id.clone(),
        category: msg.category.clone(),
    };
    let out = json_outbox_event(&msg.seat_id, seat_event::ADDED, &event)?;
    ctx.repo().outbox(out).commit(seat)?.succeeded(Output {
        seat_id: msg.seat_id,
    })
}
