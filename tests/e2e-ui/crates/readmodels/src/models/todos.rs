use distributed::graphql::{claim, col, read, AuthorizedReadModel, ModelPermissions};
use distributed::ReadModel;
use serde::{Deserialize, Serialize};

use super::AuthUsers;

/// Query-oriented Todo row. The natural plural name infers table `todos`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ReadModel)]
#[readmodel(primary_key = ["todo_id"])]
pub struct Todos {
    #[readmodel(id)]
    pub todo_id: String,
    pub owner_id: String,
    pub title: String,
    /// `open` | `completed` | `archived`
    pub status: String,
    pub assignee_id: Option<String>,
    #[readmodel(belongs_to = "AuthUsers", foreign_key = "owner_id")]
    pub owner: Option<AuthUsers>,
}

impl AuthorizedReadModel for Todos {
    /// Read authorization attached to the Todo query model.
    fn permissions() -> ModelPermissions<Self> {
        ModelPermissions::new()
            .grant(
                "user",
                read()
                    .all_columns()
                    .rows(col("owner_id").eq(claim("x-user-id"))),
            )
            .grant("admin", read().all_columns())
    }
}
