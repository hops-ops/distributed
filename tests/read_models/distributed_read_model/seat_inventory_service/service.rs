use std::sync::Arc;

use distributed::microsvc::{Routes, Service};

use super::{handlers, SeatRepo};

pub fn service(repo: SeatRepo) -> Arc<Service> {
    Arc::new(
        Service::new()
            .named("seat-inventory")
            .routes(distributed::routes!(
                Routes::new()
                    .with_repo(repo)
                    .typed_command(handlers::add::command())
                    .handle(handlers::add::handle),
                event handlers::reserve_started_checkout_seat,
            )),
    )
}
