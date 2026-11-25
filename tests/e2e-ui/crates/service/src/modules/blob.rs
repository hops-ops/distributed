//! Blob game module: Atomic command mounts. Their `BLOB_GAMES` projection is
//! direct: each command seals its row in its own transaction.

use blob_domain::BlobGame;
use distributed::microsvc::{AggregateDependencies, Routes, ServiceStores};

/// Mount blob Atomic commands from blob-domain.
pub fn routes<St: ServiceStores>(stores: &St) -> Routes<AggregateDependencies<St, BlobGame>> {
    Routes::for_aggregate(stores)
        .mount(blob_domain::commands::start())
        .mount(blob_domain::commands::move_dir())
        .mount(blob_domain::commands::start_level())
}
