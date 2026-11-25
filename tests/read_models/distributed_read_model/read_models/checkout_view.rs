use distributed::ReadModel;
use serde::{Deserialize, Serialize};

use super::{CheckoutStepView, SeatView};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("checkouts")]
pub struct CheckoutView {
    #[id("checkout_id")]
    pub checkout_id: String,
    pub seat_id: String,
    pub seat_category: String,
    pub status: String,
    pub screen_message: String,
    #[readmodel(has_many = "CheckoutStepView", foreign_key = "checkout_id")]
    pub steps: Vec<CheckoutStepView>,
    #[readmodel(belongs_to = "SeatView", foreign_key = "seat_id")]
    pub seat: Option<SeatView>,
}
