use std::sync::Arc;

use distributed::microsvc::{Routes, Service};
use distributed::InMemoryReadModelStore;

use super::handlers;

pub type ProjectionDependencies = InMemoryReadModelStore;

pub fn service(store: InMemoryReadModelStore) -> Arc<Service> {
    Arc::new(Service::new().routes(distributed::routes!(
        Routes::new().with_read_model_store(store),
        events handlers::checkout,
        events handlers::seat,
    )))
}
