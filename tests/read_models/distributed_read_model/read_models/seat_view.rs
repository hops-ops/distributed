use distributed::ReadModel;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("seats")]
pub struct SeatView {
    #[id("seat_id")]
    pub seat_id: String,
    pub category: String,
    pub status: String,
    pub checkout_id: String,
}
