use std::sync::Arc;

use distributed::microsvc::{Routes, Service};

use super::{handlers, BoardRepo};

pub fn model_service(repo: BoardRepo) -> Arc<Service> {
    Arc::new(
        Service::new().named("boards").routes(
            Routes::new()
                .with_repo(repo)
                .typed_command(handlers::board_open::command())
                .handle(handlers::board_open::handle)
                .typed_command(handlers::board_add_card::command())
                .handle(handlers::board_add_card::handle)
                .typed_command(handlers::board_move_card::command())
                .handle(handlers::board_move_card::handle)
                .typed_command(handlers::board_remove_card::command())
                .handle(handlers::board_remove_card::handle),
        ),
    )
}
