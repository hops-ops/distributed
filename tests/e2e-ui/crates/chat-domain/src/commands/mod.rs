//! Portable Chat command declarations.
//!
//! Zitadel ingest stays in the service module rather than becoming a cell
//! class method. Each domain command and its GraphQL types live in one module.

mod post;

pub use post::{handle_post, post, ChatPostInput, ChatPostPayload, Post};
