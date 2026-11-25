//! The shared read-model set: orders across tenants, and a parent/child pair
//! for relationship scenarios.

use distributed::ReadModel;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("orders")]
pub struct OrderView {
    #[id("order_id")]
    pub order_id: String,
    pub customer_id: String,
    pub status: String,
    pub total_cents: i64,
    /// May look like JSON; must remain a GraphQL String.
    pub note: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("parents")]
pub struct ParentView {
    #[id("parent_id")]
    pub parent_id: String,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("children")]
pub struct ChildView {
    #[id("child_id")]
    pub child_id: String,
    pub parent_id: String,
    pub name: String,
}

pub fn order(
    order_id: &str,
    customer_id: &str,
    status: &str,
    total_cents: i64,
    note: &str,
) -> OrderView {
    OrderView {
        order_id: order_id.into(),
        customer_id: customer_id.into(),
        status: status.into(),
        total_cents,
        note: note.into(),
    }
}

pub fn parent(parent_id: &str, name: &str) -> ParentView {
    ParentView {
        parent_id: parent_id.into(),
        name: name.into(),
    }
}

pub fn child(child_id: &str, parent_id: &str, name: &str) -> ChildView {
    ChildView {
        child_id: child_id.into(),
        parent_id: parent_id.into(),
        name: name.into(),
    }
}
