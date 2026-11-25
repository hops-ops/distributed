//! Todo command mounts + eventual projection.

use distributed::microsvc::{AggregateDependencies, Routes, ServiceStores};
use distributed::LocalProjectionMounts;
use e2e_projections::TODOS;
use todo_domain::Todo;

/// Mount todo commands and the todo projection.
pub fn routes<St: ServiceStores>(
    stores: &St,
    projections: &LocalProjectionMounts,
) -> Routes<AggregateDependencies<St, Todo>> {
    Routes::for_aggregate(stores)
        .mount(todo_domain::commands::create())
        .mount(todo_domain::commands::rename())
        .mount(todo_domain::commands::complete())
        .mount(todo_domain::commands::reopen())
        .mount(todo_domain::commands::archive())
        .mount(todo_domain::commands::force_archive())
        .mount(todo_domain::commands::purge())
        .consume_projection(projections.projector(TODOS))
}
