//! Domain-event handlers over read-model mutations for the e2e-ui fixture.

mod auth_users;
mod blob;
mod chat;
mod todos;

pub use auth_users::{
    map_zitadel_user_status, map_zitadel_user_upsert, ZitadelEmail, ZitadelUserPayload,
};
pub use blob::{SaveBlobGame, BLOB_GAMES};
pub use chat::{SaveChatMessage, CHAT_MESSAGES};
pub use todos::{DeleteTodo, SaveTodo, TODOS};
