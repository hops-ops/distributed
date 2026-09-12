use std::future::Future;

use distributed::read_model::{
    ReadModelLoadGraph, ReadModelLoadRequest, ReadModelQueryCapabilities,
};
use distributed::{
    InMemoryReadModelStore, ReadModel, ReadModelWorkspaceExt, ReadModelWritePlanStore,
    RelationalReadModelQueryStore, RowKey, RowValue, TableAdapterCapabilities, TableCommitOutcome,
    TableStoreError, TableWritePlan,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ReadModel)]
struct Account {
    id: String,
    #[readmodel(belongs_to = "Profile", foreign_key = "id")]
    profile: Option<Box<Profile>>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ReadModel)]
struct Profile {
    id: String,
    #[readmodel(belongs_to = "Account", foreign_key = "id")]
    account: Option<Account>,
}

#[test]
fn cyclic_singular_relationships_hydrate_and_serialize_without_box_metadata() {
    use distributed::{RelationalReadModel, RelationalReadModelIncludes};
    let mut account = Account {
        id: "a".into(),
        profile: None,
    };
    let profile = Profile {
        id: "a".into(),
        account: None,
    };
    let row = profile.to_row().unwrap();
    account
        .hydrate_include("profile", vec![row.clone()])
        .unwrap();
    assert_eq!(account.profile.as_deref(), Some(&profile));
    assert_eq!(account.include_rows("profile").unwrap(), vec![row.clone()]);
    assert_eq!(
        Account::include_target_schema("profile").unwrap(),
        Profile::schema()
    );
    assert!(!Account::schema()
        .columns
        .iter()
        .any(|column| column.field_name == "profile"));
    account.hydrate_include("profile", vec![]).unwrap();
    assert_eq!(account.profile, None);
    assert!(account
        .hydrate_include("profile", vec![row.clone(), row])
        .is_err());
    let mut profile = profile;
    profile
        .hydrate_include(
            "account",
            vec![Account {
                id: "a".into(),
                profile: None,
            }
            .to_row()
            .unwrap()],
        )
        .unwrap();
    assert_eq!(profile.account.as_ref().unwrap().id, "a");
}

fn block_on<F: Future>(future: F) -> F::Output {
    use std::ptr;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
    const VTABLE: RawWakerVTable = RawWakerVTable::new(
        |_| RawWaker::new(ptr::null(), &VTABLE),
        |_| {},
        |_| {},
        |_| {},
    );
    let waker = unsafe { Waker::from_raw(RawWaker::new(ptr::null(), &VTABLE)) };
    let mut cx = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("players")]
struct Player {
    #[id("player_id")]
    player_id: String,
    display_name: String,
    #[readmodel(has_many = "PlayerWeapon", foreign_key = "player_id")]
    weapons: Vec<PlayerWeapon>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("player_weapons")]
#[readmodel(primary_key = ["player_id", "weapon_id"])]
struct PlayerWeapon {
    #[readmodel(foreign_key = "players.player_id", delegated_from = "Player.player_id")]
    player_id: String,
    weapon_id: String,
    acquired_at: String,
    #[readmodel(belongs_to = "Player", foreign_key = "player_id")]
    player: Option<Player>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("players_with_many")]
struct PlayerWithMany {
    #[id("player_id")]
    player_id: String,
    #[readmodel(
        many_to_many = "Weapon",
        through = "player_weapon_links",
        foreign_key = "player_id"
    )]
    weapons: Vec<Weapon>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("weapons")]
struct Weapon {
    #[id("weapon_id")]
    weapon_id: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("weapon_label_refs")]
struct WeaponLabelRef {
    #[id("ref_id")]
    ref_id: String,
    player_id: String,
    #[readmodel(belongs_to = "CompositeWeaponLabel", foreign_key = "player_id")]
    label: Option<CompositeWeaponLabel>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("weapon_labels")]
#[readmodel(primary_key = ["player_id", "weapon_id"])]
struct CompositeWeaponLabel {
    player_id: String,
    weapon_id: String,
    label: String,
}

struct NoIncludeStore {
    inner: InMemoryReadModelStore,
}

impl NoIncludeStore {
    fn new(inner: InMemoryReadModelStore) -> Self {
        Self { inner }
    }
}

impl ReadModelWritePlanStore for NoIncludeStore {
    fn read_model_capabilities(&self) -> TableAdapterCapabilities {
        self.inner.read_model_capabilities()
    }

    fn commit_write_plan(
        &self,
        plan: TableWritePlan,
    ) -> impl Future<Output = Result<TableCommitOutcome, TableStoreError>> + Send + '_ {
        self.inner.commit_write_plan(plan)
    }
}

impl RelationalReadModelQueryStore for NoIncludeStore {
    fn read_model_query_capabilities(&self) -> ReadModelQueryCapabilities {
        ReadModelQueryCapabilities::default()
    }

    async fn load_graph(
        &self,
        request: ReadModelLoadRequest,
    ) -> Result<ReadModelLoadGraph, TableStoreError> {
        request.validate_for_query_capabilities(&self.read_model_query_capabilities())?;
        self.inner.load_graph(request).await
    }
}

fn player_key(player_id: &str) -> RowKey {
    RowKey::new([("player_id", RowValue::String(player_id.into()))])
}

fn weapon_key(player_id: &str, weapon_id: &str) -> RowKey {
    RowKey::new([
        ("player_id", RowValue::String(player_id.into())),
        ("weapon_id", RowValue::String(weapon_id.into())),
    ])
}

fn player(player_id: &str, display_name: &str) -> Player {
    Player {
        player_id: player_id.into(),
        display_name: display_name.into(),
        weapons: Vec::new(),
    }
}

fn weapon(player_id: &str, weapon_id: &str, acquired_at: &str) -> PlayerWeapon {
    PlayerWeapon {
        player_id: player_id.into(),
        weapon_id: weapon_id.into(),
        acquired_at: acquired_at.into(),
        player: None,
    }
}

fn store_with_player_and_weapons(
    weapons: impl IntoIterator<Item = PlayerWeapon>,
) -> InMemoryReadModelStore {
    let store = InMemoryReadModelStore::new();
    store.register_schema::<Player>().unwrap();
    store.register_schema::<PlayerWeapon>().unwrap();

    let mut session = distributed::read_model::ReadModelWritePlanBuilder::new();
    session.upsert(&player("player-1", "Ada")).unwrap();
    for weapon in weapons {
        session.upsert(&weapon).unwrap();
    }
    block_on(session.commit(&store)).unwrap();
    store
}

#[test]
fn friendly_session_loads_one_root_by_primary_key_without_includes() {
    let store = store_with_player_and_weapons([]);
    let mut read_models = store.workspace();

    let loaded = block_on(read_models.load::<Player>(player_key("player-1")).one()).unwrap();

    assert_eq!(loaded.unwrap().data.display_name, "Ada");
}

#[test]
fn friendly_session_hydrates_has_many_include() {
    let store = store_with_player_and_weapons([weapon("player-1", "sword", "2026-05-23")]);
    let mut read_models = store.workspace();

    let loaded = block_on(
        read_models
            .load::<Player>(player_key("player-1"))
            .include("weapons")
            .one(),
    )
    .unwrap()
    .unwrap();

    assert_eq!(loaded.data.weapons[0].weapon_id, "sword");
}

#[test]
fn friendly_session_hydrates_belongs_to_include() {
    let store = store_with_player_and_weapons([weapon("player-1", "sword", "2026-05-23")]);
    let mut read_models = store.workspace();

    let loaded = block_on(
        read_models
            .load::<PlayerWeapon>(weapon_key("player-1", "sword"))
            .include("player")
            .one(),
    )
    .unwrap()
    .unwrap();

    assert_eq!(loaded.data.player.unwrap().display_name, "Ada");
}

#[test]
fn sync_persists_loaded_scalar_field_without_manual_patch() {
    let store = store_with_player_and_weapons([]);
    let mut read_models = store.workspace();
    let mut loaded = block_on(read_models.load::<Player>(player_key("player-1")).one())
        .unwrap()
        .unwrap()
        .data;
    loaded.display_name = "Ada Lovelace".into();

    read_models.sync(loaded).unwrap();
    block_on(read_models.commit()).unwrap();

    let mut check = store.workspace();
    let reloaded = block_on(check.load::<Player>(player_key("player-1")).one())
        .unwrap()
        .unwrap();
    assert_eq!(reloaded.data.display_name, "Ada Lovelace");
}

#[test]
fn sync_refreshes_loaded_root_baseline_between_calls() {
    let store = store_with_player_and_weapons([]);
    let mut read_models = store.workspace();
    let mut loaded = block_on(read_models.load::<Player>(player_key("player-1")).one())
        .unwrap()
        .unwrap()
        .data;

    loaded.display_name = "Ada Lovelace".into();
    read_models.sync(loaded.clone()).unwrap();
    loaded.display_name = "Countess Lovelace".into();
    read_models.sync(loaded).unwrap();
    block_on(read_models.commit()).unwrap();

    let mut check = store.workspace();
    let reloaded = block_on(check.load::<Player>(player_key("player-1")).one())
        .unwrap()
        .unwrap();
    assert_eq!(reloaded.data.display_name, "Countess Lovelace");
}

#[test]
fn sync_persists_added_and_modified_related_rows() {
    let store = store_with_player_and_weapons([weapon("player-1", "sword", "2026-05-23")]);
    let mut read_models = store.workspace();
    let mut loaded = block_on(
        read_models
            .load::<Player>(player_key("player-1"))
            .include("weapons")
            .one(),
    )
    .unwrap()
    .unwrap()
    .data;
    loaded.weapons[0].acquired_at = "2026-05-24".into();
    loaded.weapons.push(weapon("", "shield", "2026-05-25"));

    read_models.sync(loaded).unwrap();
    block_on(read_models.commit()).unwrap();

    let mut check = store.workspace();
    let mut reloaded = block_on(
        check
            .load::<Player>(player_key("player-1"))
            .include("weapons")
            .one(),
    )
    .unwrap()
    .unwrap()
    .data;
    reloaded
        .weapons
        .sort_by(|left, right| left.weapon_id.cmp(&right.weapon_id));
    assert_eq!(reloaded.weapons[0].player_id, "player-1");
    assert_eq!(reloaded.weapons[0].weapon_id, "shield");
    assert_eq!(reloaded.weapons[1].acquired_at, "2026-05-24");
}

#[test]
fn sync_refreshes_loaded_include_baseline_between_calls() {
    let store = store_with_player_and_weapons([weapon("player-1", "sword", "2026-05-23")]);
    let mut read_models = store.workspace();
    let mut loaded = block_on(
        read_models
            .load::<Player>(player_key("player-1"))
            .include("weapons")
            .one(),
    )
    .unwrap()
    .unwrap()
    .data;

    loaded.weapons[0].acquired_at = "2026-05-24".into();
    read_models.sync(loaded.clone()).unwrap();
    loaded.weapons[0].acquired_at = "2026-05-25".into();
    read_models.sync(loaded).unwrap();
    block_on(read_models.commit()).unwrap();

    let mut check = store.workspace();
    let reloaded = block_on(
        check
            .load::<Player>(player_key("player-1"))
            .include("weapons")
            .one(),
    )
    .unwrap()
    .unwrap()
    .data;
    assert_eq!(reloaded.weapons[0].acquired_at, "2026-05-25");
}

#[test]
fn sync_deletes_removed_related_rows() {
    let store = store_with_player_and_weapons([
        weapon("player-1", "shield", "2026-05-24"),
        weapon("player-1", "sword", "2026-05-23"),
    ]);
    let mut read_models = store.workspace();
    let mut loaded = block_on(
        read_models
            .load::<Player>(player_key("player-1"))
            .include("weapons")
            .one(),
    )
    .unwrap()
    .unwrap()
    .data;
    loaded.weapons.retain(|weapon| weapon.weapon_id == "sword");

    read_models.sync(loaded).unwrap();
    block_on(read_models.commit()).unwrap();

    let mut check = store.workspace();
    let reloaded = block_on(
        check
            .load::<Player>(player_key("player-1"))
            .include("weapons")
            .one(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(reloaded.data.weapons.len(), 1);
    assert_eq!(reloaded.data.weapons[0].weapon_id, "sword");
}

#[test]
fn sync_clearing_belongs_to_does_not_delete_target() {
    let store = store_with_player_and_weapons([weapon("player-1", "sword", "2026-05-23")]);
    let mut read_models = store.workspace();
    let mut loaded = block_on(
        read_models
            .load::<PlayerWeapon>(weapon_key("player-1", "sword"))
            .include("player")
            .one(),
    )
    .unwrap()
    .unwrap()
    .data;
    assert!(loaded.player.is_some());
    loaded.player = None;

    read_models.sync(loaded).unwrap();
    block_on(read_models.commit()).unwrap();

    let mut check = store.workspace();
    let player = block_on(check.load::<Player>(player_key("player-1")).one()).unwrap();
    assert_eq!(player.unwrap().data.display_name, "Ada");
}

#[test]
fn missing_root_returns_none_without_include_loading() {
    let store = store_with_player_and_weapons([weapon("player-1", "sword", "2026-05-23")]);
    let mut read_models = store.workspace();

    let loaded = block_on(
        read_models
            .load::<Player>(player_key("missing"))
            .include("weapons")
            .one(),
    )
    .unwrap();

    assert!(loaded.is_none());
}

#[test]
fn unregistered_relationship_target_fails_before_loading() {
    let store = InMemoryReadModelStore::new();
    store.register_schema::<Player>().unwrap();
    let mut session = distributed::read_model::ReadModelWritePlanBuilder::new();
    session.upsert(&player("player-1", "Ada")).unwrap();
    block_on(session.commit(&store)).unwrap();
    let mut read_models = store.workspace();

    let err = block_on(
        read_models
            .load::<Player>(player_key("player-1"))
            .include("weapons")
            .one(),
    )
    .unwrap_err();

    assert!(
        matches!(err, TableStoreError::Metadata(message) if message.contains("unregistered model `PlayerWeapon`"))
    );
}

#[test]
fn unregistered_root_schema_can_load_primary_key_without_includes() {
    let store = InMemoryReadModelStore::new();
    let mut session = distributed::read_model::ReadModelWritePlanBuilder::new();
    session.upsert(&player("player-1", "Ada")).unwrap();
    block_on(session.commit(&store)).unwrap();
    let mut read_models = store.workspace();

    let loaded = block_on(read_models.load::<Player>(player_key("player-1")).one())
        .unwrap()
        .unwrap();

    assert_eq!(loaded.data.display_name, "Ada");
}

#[test]
fn adapter_without_include_capability_rejects_includes() {
    let inner = store_with_player_and_weapons([weapon("player-1", "sword", "2026-05-23")]);
    let store = NoIncludeStore::new(inner);
    let mut read_models = store.workspace();

    let err = block_on(
        read_models
            .load::<Player>(player_key("player-1"))
            .include("weapons")
            .one(),
    )
    .unwrap_err();

    assert!(
        matches!(err, TableStoreError::Metadata(message) if message.contains("relationship includes"))
    );
}

#[test]
fn nested_query_style_include_paths_are_not_a_public_query_dsl() {
    let store = store_with_player_and_weapons([weapon("player-1", "sword", "2026-05-23")]);
    let mut read_models = store.workspace();

    let err = block_on(
        read_models
            .load::<Player>(player_key("player-1"))
            .include("weapons.owner")
            .one(),
    )
    .unwrap_err();

    assert!(
        matches!(err, TableStoreError::Metadata(message) if message.contains("has no relationship"))
    );
}

#[test]
fn many_to_many_include_fails_until_join_metadata_is_rich_enough() {
    let store = InMemoryReadModelStore::new();
    store.register_schema::<PlayerWithMany>().unwrap();
    let mut read_models = store.workspace();

    let err = block_on(
        read_models
            .load::<PlayerWithMany>(player_key("player-1"))
            .include("weapons")
            .one(),
    )
    .unwrap_err();

    assert!(
        matches!(err, TableStoreError::Metadata(message) if message.contains("many-to-many relationship"))
    );
}

#[test]
fn belongs_to_include_rejects_composite_target_primary_key() {
    let store = InMemoryReadModelStore::new();
    store.register_schema::<WeaponLabelRef>().unwrap();
    store.register_schema::<CompositeWeaponLabel>().unwrap();
    let mut session = distributed::read_model::ReadModelWritePlanBuilder::new();
    session
        .upsert(&WeaponLabelRef {
            ref_id: "ref-1".into(),
            player_id: "player-1".into(),
            label: None,
        })
        .unwrap()
        .upsert(&CompositeWeaponLabel {
            player_id: "player-1".into(),
            weapon_id: "sword".into(),
            label: "Sword".into(),
        })
        .unwrap();
    block_on(session.commit(&store)).unwrap();
    let mut read_models = store.workspace();

    let err = block_on(
        read_models
            .load::<WeaponLabelRef>(RowKey::new([("ref_id", RowValue::String("ref-1".into()))]))
            .include("label")
            .one(),
    )
    .unwrap_err();

    assert!(
        matches!(err, TableStoreError::Metadata(message) if message.contains("CompositeWeaponLabel")
            && message.contains("player_id")
            && message.contains("single-column primary key"))
    );
}

// --- Workspace parity ------------------------------------------------------
//
// `InMemoryReadModelStore` implements the async store traits, so the same
// workspace ergonomic is available over `workspace()` /
// `load()` / `commit()`.

#[tokio::test]
async fn workspace_hydrates_has_many_include() {
    let store = store_with_player_and_weapons([weapon("player-1", "sword", "2026-05-23")]);
    let mut read_models = store.workspace();

    let loaded = read_models
        .load::<Player>(player_key("player-1"))
        .include("weapons")
        .one()
        .await
        .unwrap()
        .unwrap();

    assert_eq!(loaded.data.weapons[0].weapon_id, "sword");
}

#[tokio::test]
async fn workspace_sync_persists_loaded_scalar_field_without_manual_patch() {
    let store = store_with_player_and_weapons([]);
    let mut read_models = store.workspace();
    let mut loaded = read_models
        .load::<Player>(player_key("player-1"))
        .one()
        .await
        .unwrap()
        .unwrap()
        .data;
    loaded.display_name = "Ada Lovelace".into();

    read_models.sync(loaded).unwrap();
    read_models.commit().await.unwrap();

    let mut check = store.workspace();
    let reloaded = check
        .load::<Player>(player_key("player-1"))
        .one()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reloaded.data.display_name, "Ada Lovelace");
}
