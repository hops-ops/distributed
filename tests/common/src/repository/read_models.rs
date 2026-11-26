use distributed::read_model::ReadModelWritePlanBuilder;
use distributed::read_model::{ReadModel, RelationalReadModel, Versioned};
use distributed::table::{RowKey, RowValue, TableSchemaRegistry};
use distributed::{ReadModelWritePlanStore, RelationalReadModelQueryStore};
use serde::{Deserialize, Serialize};

use super::scenario::unique_id;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("conformance_seat_views")]
struct SeatView {
    #[id]
    id: String,
    status: String,
}

/// The tables the conformance scenarios write, which SQL backends bootstrap
/// in `open`; the in-memory repository needs no schema.
pub fn conformance_table_registry() -> TableSchemaRegistry {
    let mut registry = TableSchemaRegistry::new();
    registry
        .register::<SeatView>()
        .expect("SeatView schema should register");
    registry
        .register::<super::storage::CounterView>()
        .expect("CounterView schema should register");
    registry
}

fn seat_view_key(id: &str) -> RowKey {
    RowKey::new([("id", RowValue::String(id.into()))])
}

async fn load_seat_view<R>(repo: &R, id: &str) -> Option<Versioned<SeatView>>
where
    R: RelationalReadModelQueryStore + Send + Sync,
{
    let request = ReadModelWritePlanBuilder::new()
        .load::<SeatView>(seat_view_key(id))
        .expect("load request should build");
    let graph = repo
        .load_graph(request)
        .await
        .expect("read model should load");
    graph.root.map(|root| Versioned {
        data: SeatView::from_row(root.data).expect("row should hydrate"),
        version: root.version,
    })
}

pub async fn standalone_relational_write_plan_persists_row<R>(repo: R)
where
    R: ReadModelWritePlanStore + RelationalReadModelQueryStore + Send + Sync,
{
    let view = SeatView {
        id: unique_id("read-model-view"),
        status: "available".into(),
    };
    let mut read_models = ReadModelWritePlanBuilder::new();
    read_models
        .upsert(&view)
        .expect("row mutation should serialize");

    read_models
        .commit(&repo)
        .await
        .expect("read model write plan should commit");

    let loaded = load_seat_view(&repo, &view.id)
        .await
        .expect("read model row should exist");
    assert_eq!(loaded.version, 1);
    assert_eq!(loaded.data, view);
}
