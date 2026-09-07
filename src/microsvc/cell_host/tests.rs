use super::{
    instance_name, parent_cell_name, AggregateCell, CellCommandIdentity, CellDispatchError,
    CellNamespace, CellStreamStore, DURABLE_AGGREGATE_CELL_STATE_VERSION,
};
use crate::aggregate::{Aggregate, AggregateRepository};
use crate::command::{typed_command, PreparedCommand, Succeeded};
use crate::entity::Entity;
use crate::microsvc::service::{CausalCommandContext, PortableCommand, Routes};
use crate::microsvc::session::{Session, USER_ID_KEY};
use crate::microsvc::HandlerError;
use crate::repository::{
    CommitBatch, GetStream, RepositoryError, StreamIdentity, StreamWrite, TransactionalCommit,
};
use crate::sourced;
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::super::causal::{CausalWorkspace, CausalWorkspaceError};

#[derive(Clone, Default, Serialize, Deserialize, crate::Snapshot)]
struct CellItem {
    entity: Entity,
    title: String,
    done: bool,
}

#[sourced(entity, aggregate_type = "CellItem")]
impl CellItem {
    #[event("cell_item.created", version = 1)]
    fn create(&mut self, id: String, title: String) {
        self.entity.set_id(id);
        self.title = title;
        self.done = false;
    }

    #[event("cell_item.completed", version = 1)]
    fn complete(&mut self) {
        self.done = true;
    }
}

#[derive(Debug, Deserialize, crate::CommandInput)]
struct CreateInput {
    id: String,
    title: String,
}

#[derive(Debug, Serialize, crate::CommandOutput)]
struct CreatePayload {
    id: String,
}

#[derive(Debug, Deserialize, crate::CommandInput)]
struct CompleteInput {
    id: String,
}

#[derive(Debug, Serialize, crate::CommandOutput)]
struct CompletePayload {
    id: String,
    done: bool,
}

struct Create;

impl Create {
    const COMMAND: &'static str = "cell_item.create";
}

impl<D> PortableCommand<D> for Create
where
    D: crate::microsvc::CausalRouteDependencies<Aggregate = CellItem> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(typed_command::<CreateInput, Succeeded<CreatePayload>>(
                Self::COMMAND,
            ))
            .guarded(
                |ctx: &CausalCommandContext<'_, CellItem>| ctx.session().user_id().is_some(),
                handle_create,
            )
    }
}

struct Complete;

impl Complete {
    const COMMAND: &'static str = "cell_item.complete";

    fn shard(input: &CompleteInput) -> String {
        input.id.clone()
    }
}

impl<D> PortableCommand<D> for Complete
where
    D: crate::microsvc::CausalRouteDependencies<Aggregate = CellItem> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(typed_command::<CompleteInput, Succeeded<CompletePayload>>(
                Self::COMMAND,
            ))
            .load_by(|input: &CompleteInput| Complete::shard(input))
            .invoke(|item, _input, _owner| item.complete())
            .succeeded(|item| CompletePayload {
                id: item.entity().id().to_string(),
                done: item.done,
            })
    }
}

async fn handle_create(
    ctx: &CausalCommandContext<'_, CellItem>,
    input: CreateInput,
) -> Result<PreparedCommand<Succeeded<CreatePayload>>, HandlerError> {
    let repo = ctx.repo();
    if repo.get(&input.id).await?.is_some() {
        return Err(HandlerError::Rejected(format!(
            "cell item {} already exists",
            input.id
        )));
    }
    let mut item = repo.create();
    item.create(input.id.clone(), input.title)
        .map_err(|error| HandlerError::Rejected(error.to_string()))?;
    repo.commit(item)?.succeeded(CreatePayload { id: input.id })
}

fn owner_session() -> Session {
    let mut session = Session::new();
    session.set(USER_ID_KEY, "user-1");
    session
}

fn fn_send_sync<T: Send + Sync>(_: &T) {}

#[tokio::test]
async fn workspace_adapter_loads_and_commits_one_stream_without_sqlx() {
    let store = CellStreamStore::new("CellItem", "item-1").expect("identity");
    let repository = AggregateRepository::<_, CellItem>::new(store.clone());
    let workspace = CausalWorkspace::new(&repository);

    let mut item = workspace.create();
    item.create("item-1".into(), "write".into()).unwrap();
    workspace.stage(item).unwrap();

    let mut parts = workspace.into_parts().unwrap();
    parts.prepare_domain_publications("causation-1").unwrap();
    let batch = parts.prepare_commit_batch().unwrap();
    TransactionalCommit::commit_batch(&store, batch)
        .await
        .unwrap();

    let repository = AggregateRepository::<_, CellItem>::new(store.clone());
    let workspace = CausalWorkspace::new(&repository);
    let loaded = workspace.load("item-1").await.unwrap().unwrap();
    assert_eq!(loaded.entity().id(), "item-1");
    assert_eq!(loaded.title, "write");

    match workspace.load("item-2").await {
        Err(CausalWorkspaceError::Repository(RepositoryError::Model(message))) => {
            assert!(
                message.contains("cannot access stream"),
                "unexpected message: {message}"
            );
        }
        other => panic!(
            "expected shard fence, got {}",
            match other {
                Ok(_) => "Ok(checkout)".to_string(),
                Err(error) => error.to_string(),
            }
        ),
    }
}

#[tokio::test]
async fn cell_rejects_commit_of_a_foreign_stream() {
    let store = CellStreamStore::new("CellItem", "item-1").expect("identity");
    let repository = AggregateRepository::<_, CellItem>::new(store.clone());
    let workspace = CausalWorkspace::new(&repository);
    let mut item = workspace.create();
    item.create("item-2".into(), "other".into()).unwrap();
    workspace.stage(item).unwrap();
    let mut parts = workspace.into_parts().unwrap();
    parts.prepare_domain_publications("causation-1").unwrap();
    let batch = parts.prepare_commit_batch().unwrap();
    let error = TransactionalCommit::commit_batch(&store, batch)
        .await
        .unwrap_err();
    assert!(
        matches!(error, RepositoryError::Model(message) if message.contains("cannot access stream"))
    );
}

#[tokio::test]
async fn cell_dispatches_complete_with_the_same_portable_command_as_soa() {
    let cell = AggregateCell::<CellItem>::new_with_snapshots("item-1", 1)
        .unwrap()
        .mount(Create)
        .mount(Complete);
    assert_eq!(cell.instance_name(), "CellItem:item-1");
    assert_eq!(instance_name::<CellItem>("item-1"), "CellItem:item-1");
    let names = cell.command_names();
    assert!(names.iter().any(|name| name == "cell_item.create"));
    assert!(names.iter().any(|name| name == "cell_item.complete"));
    assert!(cell.is_command_only());
    fn_send_sync(&cell);

    let created = cell
        .dispatch(
            "cell_item.create",
            json!({ "id": "item-1", "title": "ship" }),
            owner_session(),
        )
        .await
        .expect("create");
    assert_eq!(created["id"], "item-1");
    let loaded = cell.load().await.expect("load");
    assert_eq!(loaded.expect("resident").title, "ship");

    let completed = cell
        .dispatch(
            "cell_item.complete",
            json!({ "id": "item-1" }),
            owner_session(),
        )
        .await
        .expect("complete");
    assert_eq!(completed["id"], "item-1");
    assert_eq!(completed["done"], true);

    let snap = cell
        .cached_snapshot()
        .await
        .expect("snapshot")
        .expect("snapshot after complete");
    assert_eq!(snap.version, 2);

    let sealed = json!({ "id": "item-1", "title": "ship", "done": true });
    cell.replace_sealed_row(sealed.clone())
        .expect("seal row after complete");
    assert_eq!(cell.sealed_row().expect("read seal"), Some(sealed.clone()));

    let state = cell.durable_state().expect("export durable state");
    assert_eq!(state.version, DURABLE_AGGREGATE_CELL_STATE_VERSION);
    assert!(!state.events.is_empty());
    assert!(!state.snapshots.is_empty());
    let mut unsupported = state.clone();
    unsupported.version += 1;
    let rejected = AggregateCell::<CellItem>::new_with_snapshots("item-1", 1)
        .unwrap()
        .restore_durable_state(unsupported);
    assert!(rejected.is_err());
    let restored = AggregateCell::<CellItem>::new_with_snapshots("item-1", 1)
        .unwrap()
        .mount(Create)
        .mount(Complete);
    restored
        .restore_durable_state(state)
        .expect("restore durable state");
    assert_eq!(restored.sealed_row().expect("restored seal"), Some(sealed));
    let loaded = restored
        .load()
        .await
        .expect("load restored")
        .expect("durable");
    assert_eq!(loaded.title, "ship");
    assert!(loaded.done);
    assert_eq!(loaded.entity.snapshot_version(), 2);
}

#[tokio::test]
async fn cell_wait_path_replays_the_same_command_without_new_domain_effects() {
    let cell = AggregateCell::<CellItem>::new("item-ledger")
        .unwrap()
        .mount(Create)
        .mount(Complete);
    let identity = CellCommandIdentity::new(
        "cell-test-service",
        "principal-alice",
        "0190a000-0000-7000-8000-000000000401",
    )
    .unwrap();
    let input = json!({ "id": "item-ledger", "title": "once" });

    let first = cell
        .dispatch_idempotent(
            "cell_item.create",
            &identity,
            input.clone(),
            owner_session(),
        )
        .await
        .expect("first dispatch");
    let replay = cell
        .dispatch_idempotent("cell_item.create", &identity, input, owner_session())
        .await
        .expect("same-input replay");

    assert!(!first.replayed());
    assert!(replay.replayed());
    assert_eq!(replay.payload(), first.payload());
    assert_eq!(replay.causation_id(), first.causation_id());
    let events = cell.durable_events().unwrap();
    assert_eq!(
        events
            .iter()
            .map(|stream| stream.events.len())
            .sum::<usize>(),
        1,
        "replay must not invoke the handler or append another event"
    );
    assert_eq!(
        events[0].events[0].causation_id(),
        Some(first.causation_id())
    );

    let durable_commands = cell.durable_commands().expect("export command ledger");
    assert_eq!(durable_commands.len(), 1);
    let restored = AggregateCell::<CellItem>::new("item-ledger")
        .unwrap()
        .mount(Create)
        .mount(Complete);
    restored
        .restore_durable_events(events)
        .expect("restore domain events");
    restored
        .restore_durable_commands(durable_commands)
        .expect("restore command ledger");
    let replay_after_restart = restored
        .dispatch_idempotent(
            "cell_item.create",
            &identity,
            json!({ "id": "item-ledger", "title": "once" }),
            owner_session(),
        )
        .await
        .expect("durable replay after restart");
    assert!(replay_after_restart.replayed());
    assert_eq!(replay_after_restart.causation_id(), first.causation_id());
    assert_eq!(
        restored
            .durable_events()
            .unwrap()
            .iter()
            .map(|stream| stream.events.len())
            .sum::<usize>(),
        1
    );
}

#[tokio::test]
async fn cell_wait_path_rejects_command_id_reuse_with_different_input() {
    let cell = AggregateCell::<CellItem>::new("item-conflict")
        .unwrap()
        .mount(Create)
        .mount(Complete);
    let identity = CellCommandIdentity::new(
        "cell-test-service",
        "principal-alice",
        "0190a000-0000-7000-8000-000000000402",
    )
    .unwrap();
    cell.dispatch_idempotent(
        "cell_item.create",
        &identity,
        json!({ "id": "item-conflict", "title": "first" }),
        owner_session(),
    )
    .await
    .unwrap();

    let error = cell
        .dispatch_idempotent(
            "cell_item.create",
            &identity,
            json!({ "id": "item-conflict", "title": "different" }),
            owner_session(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, CellDispatchError::CommandIdReuse));
    assert_eq!(error.code(), "COMMAND_ID_REUSE");
    assert_eq!(error.status_code(), 409);
}

#[tokio::test]
async fn cell_complete_rejects_a_different_shard_id() {
    let cell = AggregateCell::<CellItem>::new("item-1")
        .unwrap()
        .mount(Create)
        .mount(Complete);
    cell.dispatch(
        "cell_item.create",
        json!({ "id": "item-1", "title": "ship" }),
        owner_session(),
    )
    .await
    .unwrap();

    let error = cell
        .dispatch(
            "cell_item.complete",
            json!({ "id": "item-2" }),
            owner_session(),
        )
        .await
        .unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("cannot access stream") || message.contains("not found"),
        "unexpected error: {message}"
    );
}

#[tokio::test]
async fn namespace_get_by_name_addresses_type_and_shard() {
    let mut namespace = CellNamespace::<CellItem>::new();
    namespace
        .get_or_create("item-7", |cell| cell.mount(Create).mount(Complete))
        .unwrap();
    let cell = namespace
        .get_by_name("CellItem:item-7")
        .expect("named cell");
    assert_eq!(cell.shard_id(), "item-7");
    assert!(namespace.get_by_name("CellItem:missing").is_none());
}

#[tokio::test]
async fn parent_cell_commits_sibling_streams_in_one_batch() {
    let store = CellStreamStore::for_parent_shard("game", "game-1", |identity| {
        matches!(identity.aggregate_type(), "GameMap" | "Player" | "Bomb")
    })
    .expect("parent shard");
    assert_eq!(store.instance_name(), "game:game-1");
    assert_eq!(parent_cell_name("game", "game-1"), "game:game-1");
    assert_ne!(parent_cell_name("game", "game-1"), "player:player-1");

    let mut map = Entity::with_id("game-1");
    map.digest_empty("initialized").unwrap();
    let mut player = Entity::with_id("player:1");
    player.digest_empty("joined").unwrap();
    let mut bomb = Entity::with_id("bomb:1");
    bomb.digest_empty("placed").unwrap();

    let map_id = StreamIdentity::new("GameMap", "game-1").unwrap();
    let player_id = StreamIdentity::new("Player", "player:1").unwrap();
    let bomb_id = StreamIdentity::new("Bomb", "bomb:1").unwrap();
    let batch = CommitBatch::new(vec![
        StreamWrite::new(map_id.clone(), &mut map),
        StreamWrite::new(player_id.clone(), &mut player),
        StreamWrite::new(bomb_id.clone(), &mut bomb),
    ]);
    TransactionalCommit::commit_batch(&store, batch)
        .await
        .expect("sibling streams commit on one parent cell");

    assert!(GetStream::get_stream(&store, &map_id)
        .await
        .unwrap()
        .is_some());
    assert!(GetStream::get_stream(&store, &player_id)
        .await
        .unwrap()
        .is_some());
    assert!(GetStream::get_stream(&store, &bomb_id)
        .await
        .unwrap()
        .is_some());

    let foreign = StreamIdentity::new("Foreign", "foreign-1").unwrap();
    assert!(GetStream::get_stream(&store, &foreign).await.is_err());
}

#[tokio::test]
async fn parent_cells_are_isolated_and_have_no_cross_cell_commit() {
    let game_1 = CellStreamStore::for_parent_shard("game", "g1", |identity| {
        identity.aggregate_id().starts_with("g1:")
    })
    .unwrap();
    let game_2 = CellStreamStore::for_parent_shard("game", "g2", |identity| {
        identity.aggregate_id().starts_with("g2:")
    })
    .unwrap();

    let mut player = Entity::with_id("g1:player:1");
    player.digest_empty("joined").unwrap();
    let player_id = StreamIdentity::new("Player", "g1:player:1").unwrap();
    let batch = CommitBatch::new(vec![StreamWrite::new(player_id.clone(), &mut player)]);
    TransactionalCommit::commit_batch(&game_1, batch)
        .await
        .unwrap();

    assert!(GetStream::get_stream(&game_1, &player_id)
        .await
        .unwrap()
        .is_some());
    assert!(GetStream::get_stream(&game_2, &player_id).await.is_err());
}

#[test]
fn cargo_features_keep_sqlite_and_do_not_add_celld() {
    let manifest = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"));
    assert!(
        manifest
            .lines()
            .any(|line| line.trim_start().starts_with("sqlite =")),
        "sqlite feature must remain next to postgres"
    );
    assert!(
        manifest
            .lines()
            .any(|line| line.trim_start().starts_with("postgres =")),
        "postgres feature must remain next to sqlite"
    );
    let features = manifest
        .split("[features]")
        .nth(1)
        .and_then(|rest| rest.split("\n[").next())
        .expect("features table");
    assert!(
        !features
            .lines()
            .any(|line| line.trim_start().starts_with("celld")),
        "PCH-DEC-005: do not add a celld Cargo feature beside sqlite/postgres"
    );
}
