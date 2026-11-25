use distributed::read_model::ReadModelWritePlanBuilder;
use distributed::{
    sourced, Entity, InMemoryRepository, ReadModel, ReadModelWorkspaceExt,
    ReadModelWritePlanCommitExt, RowKey, RowValue,
};
use serde::{Deserialize, Serialize};

#[derive(Default)]
struct TestAggregate {
    entity: Entity,
}

#[sourced(entity)]
impl TestAggregate {
    #[event("touched")]
    fn touch(&mut self) {
        if self.entity.id().is_empty() {
            self.entity.set_id("agg-1");
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("bridge_views")]
struct BridgeView {
    #[id]
    id: String,
    value: i32,
}

#[tokio::test]
async fn repo_first_read_models_session_commit_form_is_available() {
    let repo = InMemoryRepository::new();
    let view = BridgeView {
        id: "view-1".into(),
        value: 42,
    };
    let mut session = ReadModelWritePlanBuilder::new();
    session.upsert(&view).unwrap();
    let mut aggregate = TestAggregate::default();
    aggregate.touch().unwrap();

    repo.read_models(session)
        .commit(&mut aggregate)
        .await
        .unwrap();

    let loaded = repo
        .workspace()
        .load::<BridgeView>(RowKey::new([("id", RowValue::String("view-1".into()))]))
        .one()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.data, view);
}
