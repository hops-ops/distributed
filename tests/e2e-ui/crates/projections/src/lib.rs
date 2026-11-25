//! Domain-event handlers over read-model mutations for the e2e-ui fixture.

mod auth_users;
mod blob;
mod chat;
mod todos;

pub use auth_users::{SaveAuthUser, ZitadelUserProfile, AUTH_USERS};
pub use blob::{SaveBlobGame, BLOB_GAMES};
pub use chat::{SaveChatMessage, CHAT_MESSAGES};
pub use todos::{DeleteTodo, SaveTodo, TODOS};
