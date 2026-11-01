//! Schedule rows on the shared SQL repository: stored in the command's
//! transaction, owned by the batch's one stream, relative due times resolved
//! by the database clock, re-arming bumps the generation, cancelling
//! disarms. Dispatching them is the schedule worker's job
//! (`tests/schedule_worker`).

#![cfg(feature = "sqlite")]

use std::time::{Duration, SystemTime};

use distributed::repository::{CommitBatch, StreamWrite};
use distributed::schedule::{Schedule, ScheduleWrite};
use distributed::{Entity, SqliteRepository, StreamIdentity, TransactionalCommit};

fn event(entity: &mut Entity, id: &str) {
    if entity.id().is_empty() {
        entity.set_id(id);
    }
    entity.digest_empty("ticker.ticked").expect("record event");
}

#[tokio::test]
async fn sqlite_stores_schedules_with_their_owner_in_the_commit() {
    let repo = SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .unwrap();
    let owner = StreamIdentity::new("Ticker", "t1").unwrap();
    let mut entity = Entity::new();
    event(&mut entity, "t1");
    let mut batch = CommitBatch::new(vec![StreamWrite::new(owner.clone(), &mut entity)]);
    batch.schedules.push(ScheduleWrite::Arm(
        Schedule::after(
            "tick",
            Duration::from_secs(30),
            "ticker.tick",
            &serde_json::json!({ "id": "t1" }),
        )
        .unwrap(),
    ));
    let before = SystemTime::now();
    repo.commit_batch(batch).await.unwrap();
    let armed = repo.armed_schedules(&owner).await.unwrap();
    assert_eq!(armed.len(), 1);
    assert_eq!(armed[0].command, "ticker.tick");
    assert_eq!(armed[0].generation, 1);
    let due = armed[0].due();
    assert!(
        due >= before + Duration::from_secs(29)
            && due <= SystemTime::now() + Duration::from_secs(31),
        "relative due resolved by the database clock: {due:?}"
    );

    // Re-arm (generation 2) and cancel in later commits of the same owner.
    event(&mut entity, "t1");
    let mut batch = CommitBatch::new(vec![StreamWrite::new(owner.clone(), &mut entity)]);
    batch.schedules.push(ScheduleWrite::Arm(
        Schedule::at(
            "tick",
            SystemTime::UNIX_EPOCH + Duration::from_secs(2_000_000_000),
            "ticker.tick",
            &serde_json::json!({ "id": "t1" }),
        )
        .unwrap(),
    ));
    repo.commit_batch(batch).await.unwrap();
    assert_eq!(repo.armed_schedules(&owner).await.unwrap()[0].generation, 2);
    event(&mut entity, "t1");
    let mut batch = CommitBatch::new(vec![StreamWrite::new(owner.clone(), &mut entity)]);
    batch.schedules.push(ScheduleWrite::Cancel("tick".into()));
    repo.commit_batch(batch).await.unwrap();
    assert!(repo.armed_schedules(&owner).await.unwrap().is_empty());

    // Another owner's table is separate; a schedule needs exactly one owner.
    let other = StreamIdentity::new("Ticker", "t2").unwrap();
    assert!(repo.armed_schedules(&other).await.unwrap().is_empty());
    let mut orphan = CommitBatch::empty();
    orphan.schedules.push(ScheduleWrite::Cancel("tick".into()));
    let error = repo.commit_batch(orphan).await.unwrap_err();
    assert!(
        error.to_string().contains("exactly one aggregate"),
        "{error}"
    );
}
