use distributed::ReadModel;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("checkout_steps")]
#[readmodel(primary_key = ["checkout_id", "step"])]
pub struct CheckoutStepView {
    #[readmodel(
        foreign_key = "checkouts.checkout_id",
        delegated_from = "CheckoutView.checkout_id"
    )]
    pub checkout_id: String,
    pub step: String,
    pub detail: String,
}
