//! The `boroughs` read model: one row per borough — bosses down, weather,
//! tier and the **world event** running in it — the [`BoroughState`] every
//! borough domain event carries (a hood seen, an event started or ended).
//! `mobpit_service` projects it from the bus and serves it over GraphQL
//! `@live` (the SvelteKit page's active events).

use distributed::graphql::SurfaceProjector;
use distributed::mutation::Mutation;
use distributed::mutation_file;
use distributed::projection::lower::{EventualOnly, ProjectionDescriptor};
use distributed::projection::LocalProjectionMounts;
use distributed::read_model::{ReadModel, RelationalReadModel};
use distributed::table::ReadModelCatalog;
use serde::{Deserialize, Serialize};

#[allow(unused_imports)]
use crate::BoroughState;
use crate::{
    BoroughChangedDomainEvent, BoroughEventEndedDomainEvent, BoroughEventStartedDomainEvent,
};

/// One borough as its cell last published it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ReadModel)]
#[table("boroughs")]
pub struct Boroughs {
    /// `bx.by`.
    #[id("borough")]
    pub borough: String,
    /// Bosses down among its 9 hoods.
    pub bosses_down: i64,
    /// Index into `WEATHER`.
    pub weather: i64,
    /// Sum of its hoods' tiers.
    pub tier: i64,
    /// The world event running (`invasion`, `frenzy`, `blessing`; empty when none).
    pub event: String,
    /// When it ends (Unix ms, 0 when none).
    pub event_until_ms: i64,
}

/// Upsert one `boroughs` row from the domain event's state body.
#[allow(non_snake_case)]
pub fn SaveBorough() -> Mutation<()> {
    mutation_file!("src/mutations/save_borough.mutation.graphql")
}

distributed::projection! {
    pub const BOROUGHS: ProjectionDescriptor<EventualOnly> = {
        name: "project_boroughs",
        version: 1,
        model: Boroughs,
        on {
            events: [
                BoroughChangedDomainEvent,
                BoroughEventStartedDomainEvent,
                BoroughEventEndedDomainEvent
            ],
            mutation: SaveBorough,
            input: { borough: body },
        },
    };
}

/// Physical read-model catalog (`boroughs`) under `application`.
pub fn read_model_catalog(application: &str) -> ReadModelCatalog {
    ReadModelCatalog::new(application).table_schema(Boroughs::schema().clone())
}

/// The modeled projector mount for [`BOROUGHS`], owned by `application`.
pub fn projector(application: &str) -> SurfaceProjector {
    LocalProjectionMounts::builder(application, "ordered-domain-events")
        .eventual(BOROUGHS)
        .build()
        .expect("boroughs projection")
        .projector(BOROUGHS)
}
