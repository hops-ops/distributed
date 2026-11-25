use distributed::read_model::{ReadModelScanRequest, ReadModelWritePlanBuilder};
use distributed::{
    ReadModel, ReadModelWritePlanStore, RelationalReadModelQueryStore, RowValue, RowValues,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[readmodel(primary_key = ["id"])]
struct ScanCandidate {
    #[readmodel(id)]
    id: String,
    scope: String,
    source: Option<String>,
    enabled: bool,
    generation: u64,
}
fn filter(scope: &str) -> RowValues {
    let mut values = RowValues::new();
    values.insert("scope", RowValue::String(scope.into()));
    values
}
async fn exercise<S: ReadModelWritePlanStore + RelationalReadModelQueryStore>(store: &S) {
    let mut plan = ReadModelWritePlanBuilder::new();
    for index in 0..230 {
        plan.upsert(&ScanCandidate {
            id: format!("candidate-{index:03}"),
            scope: if index < 205 { "repo" } else { "foreign" }.into(),
            source: (index % 2 == 0).then(|| "storage".into()),
            enabled: index % 2 == 0,
            generation: 1,
        })
        .unwrap();
    }
    plan.commit(store).await.unwrap();
    let mut cursor = None;
    let mut ids = Vec::new();
    loop {
        let page = store
            .scan_read_model(
                ReadModelScanRequest::new::<ScanCandidate>(filter("repo"), 100, cursor).unwrap(),
            )
            .await
            .unwrap();
        assert!(page.rows.len() <= 100);
        assert!(page.rows.iter().all(|r| r.version == 1));
        ids.extend(
            page.typed::<ScanCandidate>()
                .unwrap()
                .into_iter()
                .map(|r| r.data.id),
        );
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(
        ids,
        (0..205)
            .map(|i| format!("candidate-{i:03}"))
            .collect::<Vec<_>>()
    );
    let first = store
        .scan_read_model(
            ReadModelScanRequest::new::<ScanCandidate>(filter("repo"), 100, None).unwrap(),
        )
        .await
        .unwrap();
    let cursor = first.next.unwrap();
    assert_eq!(cursor.after(), "candidate-099");
    assert!(ReadModelScanRequest::new::<ScanCandidate>(
        filter("foreign"),
        100,
        Some(cursor.clone())
    )
    .is_err());
    let mut update = ReadModelWritePlanBuilder::new();
    update
        .upsert(&ScanCandidate {
            id: "candidate-100".into(),
            scope: "repo".into(),
            source: None,
            enabled: false,
            generation: 2,
        })
        .unwrap();
    update
        .upsert(&ScanCandidate {
            id: "candidate-099-new".into(),
            scope: "repo".into(),
            source: None,
            enabled: false,
            generation: 2,
        })
        .unwrap();
    update.commit(store).await.unwrap();
    let second = store
        .scan_read_model(
            ReadModelScanRequest::new::<ScanCandidate>(filter("repo"), 2, Some(cursor)).unwrap(),
        )
        .await
        .unwrap();
    let rows = second.typed::<ScanCandidate>().unwrap();
    assert_eq!(rows[0].data.id, "candidate-099-new");
    assert_eq!(rows[1].data.id, "candidate-100");
    assert_eq!(rows[1].version, 2);
    let mut null = filter("repo");
    null.insert("source", RowValue::Null);
    null.insert("generation", RowValue::U64(2));
    let page = store
        .scan_read_model(ReadModelScanRequest::new::<ScanCandidate>(null, 100, None).unwrap())
        .await
        .unwrap();
    assert_eq!(page.rows.len(), 2);
    assert!(page.next.is_none());
    let empty = store
        .scan_read_model(
            ReadModelScanRequest::new::<ScanCandidate>(filter("absent"), 100, None).unwrap(),
        )
        .await
        .unwrap();
    assert!(empty.rows.is_empty());
    assert!(empty.next.is_none());
    for limit in [0, 101, u16::MAX] {
        assert!(ReadModelScanRequest::new::<ScanCandidate>(filter("repo"), limit, None).is_err());
    }
    for (name, value) in [
        ("missing", RowValue::String("x".into())),
        ("generation", RowValue::String("1".into())),
        ("scope", RowValue::Null),
        ("generation", RowValue::U64(u64::MAX)),
        ("enabled", RowValue::I64(1)),
    ] {
        let mut bad = RowValues::new();
        bad.insert(name, value);
        assert!(ReadModelScanRequest::new::<ScanCandidate>(bad, 1, None).is_err());
    }
}
#[tokio::test]
async fn bounded_keyset_in_memory_and_queued_repository() {
    let store = distributed::InMemoryRepository::new();
    store
        .model_store()
        .register_schema::<ScanCandidate>()
        .unwrap();
    exercise(&store).await;
    let inner = distributed::InMemoryRepository::new();
    inner
        .model_store()
        .register_schema::<ScanCandidate>()
        .unwrap();
    let queued = distributed::QueuedRepository::new(inner);
    exercise(&queued).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn bounded_keyset_sqlite_preserves_scope_cursor_and_row_versions() {
    let store = distributed::SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .unwrap();
    let mut registry = distributed::TableSchemaRegistry::new();
    registry.register::<ScanCandidate>().unwrap();
    store
        .bootstrap_table_schema_for_dev(&registry)
        .await
        .unwrap();
    exercise(&store).await;
}
