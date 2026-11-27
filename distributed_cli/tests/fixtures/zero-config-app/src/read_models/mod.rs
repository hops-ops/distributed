use distributed::read_model::ReadModel;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("zero_config_app_views")]
pub struct ZeroConfigAppView {
    #[id("id")]
    pub id: String,
    pub name: String,
    pub status: String,
}
