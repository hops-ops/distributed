//! e2e-ui projection mounts — product declaration only.
//!
//! Topology, catalog activation, and Surface packaging come from
//! [`distributed::LocalProjectionMountsBuilder`].

use distributed::graphql::{SurfaceDirectProjection, SurfaceProjector};
use distributed::LocalProjectionMountsBuilder;
use e2e_projections::{AUTH_USERS, BLOB_GAMES, CHAT_MESSAGES, TODOS};
use e2e_readmodels::{AuthUsers, BlobGames, ChatMessages, Todos};

/// Projection surface mounts used by compose + GraphQL.
#[derive(Clone)]
pub struct ProjectionOwners {
    pub todo: SurfaceProjector,
    pub chat: SurfaceProjector,
    pub blob: SurfaceDirectProjection,
    pub auth: SurfaceProjector,
}

/// Compile local projection mounts for the e2e-ui application.
pub fn projection_owners() -> ProjectionOwners {
    let mounts = LocalProjectionMountsBuilder::new("e2e-ui", "ordered-domain-events")
        .expect("projection source")
        .eventual_model::<Todos, _>("project_todos", TODOS, "e2e-ui-todos-v2")
        .expect("todo mount")
        .eventual_model::<ChatMessages, _>("project_chat_messages", CHAT_MESSAGES, "e2e-ui-chat-v2")
        .expect("chat mount")
        .direct_model::<BlobGames>("project_blob", BLOB_GAMES, "e2e-ui-blob-v2")
        .expect("blob mount")
        .eventual_model::<AuthUsers, _>("project_auth_users", AUTH_USERS, "e2e-ui-auth-users-v1")
        .expect("auth users mount")
        .build()
        .expect("projection catalog");

    ProjectionOwners {
        todo: mounts.projector("project_todos").expect("todo projector"),
        chat: mounts
            .projector("project_chat_messages")
            .expect("chat projector"),
        blob: mounts
            .direct_projection("project_blob")
            .expect("blob direct"),
        auth: mounts
            .projector("project_auth_users")
            .expect("auth users projector"),
    }
}
