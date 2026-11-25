//! Chat room messages + eventual projection.

use chat_domain::ChatMessage;
use distributed::microsvc::{AggregateDependencies, Routes, ServiceStores};
use distributed::LocalProjectionMounts;
use e2e_projections::CHAT_MESSAGES;

/// Mount chat commands and the chat projection.
pub fn routes<St: ServiceStores>(
    stores: &St,
    projections: &LocalProjectionMounts,
) -> Routes<AggregateDependencies<St, ChatMessage>> {
    Routes::for_aggregate(stores)
        .mount(chat_domain::commands::post())
        .consume_projection(projections.projector(CHAT_MESSAGES))
}
