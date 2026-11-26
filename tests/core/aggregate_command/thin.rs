//! `command!` (the thin, aggregate-external form) against the same `Hero`:
//! each `load:` kind, `guard:`, `constructor:` and
//! `preview_reduce_known_record:` behave as declared at dispatch time.

use super::*;
use distributed::command;
use distributed::command::CommandProjectionPureReduce;

fn not_mallory(ctx: &CausalCommandContext<'_, Hero>) -> bool {
    ctx.session().user_id() != Some("mallory")
}

/// The row a hero's client preview reads (a subset of [`Heroes`]).
#[derive(Deserialize, distributed::table::JsonType)]
pub struct HeroRow {
    pub items: Vec<String>,
}

#[derive(Deserialize, distributed::table::JsonType)]
pub struct HeroArgs {
    pub item: String,
}

#[derive(Serialize, distributed::table::JsonType)]
pub struct HeroPatch {
    pub items: Vec<String>,
}

pub fn equip_preview(row: HeroRow, args: HeroArgs) -> Option<HeroPatch> {
    let mut items = row.items;
    items.push(args.item);
    Some(HeroPatch { items })
}

distributed::wasm_pure! {
    pub const EQUIP_PREVIEW = equip_preview(HeroRow, HeroArgs) -> HeroPatch;
}

#[derive(Clone, Default, Serialize, Deserialize, distributed::read_model::ReadModel)]
#[readmodel(primary_key = ["hero_id"])]
pub struct Heroes {
    pub hero_id: String,
    pub items: Vec<String>,
}

fn hero_preview() -> CommandProjectionPureReduce {
    CommandProjectionPureReduce::known_record::<Heroes>(EQUIP_PREVIEW)
}

command! {
    name: "hero.enlist",
    transition: domain_commands::Create,
    aggregate: Hero,
    input: CreateHero,
    outcome: Eventual<HeroPayload>,
    shard: |input| input.hero_id.clone(),
    load: create,
    roles: ["player"],
    field: "heroes_enlist",
    invoke: |hero, input, principal| hero
        .create(principal, CreateHero { hero_id: input.hero_id.clone(), name: input.name.clone() })
        .map(|_| ()),
    payload: |hero, _principal| hero_payload(hero),
}

command! {
    name: "hero.outfit",
    transition: domain_commands::Equip,
    aggregate: Hero,
    input: EquipInput,
    outcome: Eventual<HeroPayload>,
    shard: |input| input.hero_id.clone(),
    load: required,
    roles: ["player"],
    field: "heroes_outfit",
    constructor: outfit_command,
    preview_reduce_known_record: hero_preview(),
    guard: not_mallory,
    invoke: |hero, input, principal| hero.equip(input.slot.clone(), principal, &input.item),
    payload: |hero, _principal| hero_payload(hero),
}

command! {
    name: "hero.doze",
    transition: domain_commands::Nap,
    aggregate: Hero,
    input: NapInput,
    outcome: Eventual<HeroPayload>,
    shard: |_input| "cy".to_string(),
    load: or_create,
    roles: ["player"],
    field: "heroes_doze",
    invoke: |hero, input, principal| hero
        .nap(principal.to_string(), NapInput { minutes: input.minutes })
        .map(|_| ()),
    payload: |hero, _principal| hero_payload(hero),
}

// `Succeeded`, on a transition that captures the `hero.napped` domain event.
command! {
    name: "hero.siesta",
    transition: domain_commands::Nap,
    aggregate: Hero,
    input: NapInput,
    outcome: Succeeded<HeroPayload>,
    shard: |_input| "dee".to_string(),
    load: or_create,
    roles: ["player"],
    field: "heroes_siesta",
    invoke: |hero, input, principal| hero
        .nap(principal.to_string(), NapInput { minutes: input.minutes })
        .map(|_| ()),
    payload: |hero, _principal| hero_payload(hero),
}

fn thin_cell(id: &str) -> AggregateCell<Hero> {
    AggregateCell::<Hero>::new(id)
        .unwrap()
        .mount(enlist())
        .mount(outfit_command())
        .mount(doze())
        .mount(siesta())
}

/// The cell's outbox rows as `(source sequence, event type)`, in stream
/// order (the outbox itself is unordered).
fn outbox_events(cell: &AggregateCell<Hero>) -> Vec<(Option<u64>, String)> {
    let mut rows: Vec<_> = cell
        .durable_outbox()
        .unwrap()
        .into_iter()
        .map(|message| (message.source_sequence, message.event_type))
        .collect();
    rows.sort();
    rows
}

/// An aggregate whose events are history-only: nothing to publish.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Tally {
    entity: distributed::Entity,
    count: u32,
}

#[sourced(entity, aggregate_type = "tally")]
impl Tally {
    #[event("tally.bumped")]
    fn bumped(&mut self, id: String) {
        self.entity.set_id(id);
        self.count += 1;
    }
}

/// The witness of a transition with no outward domain events.
pub enum Bumped {}

impl distributed::command::CommandEventSet for Bumped {
    fn command_event_set() -> distributed::command::CommandProjectionEventSet {
        Default::default()
    }
}

#[derive(Debug, Deserialize, distributed::command::CommandInput)]
pub struct BumpInput {
    pub tally_id: String,
}

#[derive(Debug, Serialize, distributed::command::CommandOutput)]
pub struct TallyPayload {
    pub count: u32,
}

command! {
    name: "tally.bump",
    transition: Bumped,
    aggregate: Tally,
    input: BumpInput,
    outcome: Succeeded<TallyPayload>,
    shard: |input| input.tally_id.clone(),
    load: or_create,
    roles: ["player"],
    field: "tallies_bump",
    invoke: |tally, input, _principal| tally.bumped(input.tally_id.clone()),
    payload: |tally, _principal| TallyPayload { count: tally.count },
}

#[tokio::test]
async fn succeeded_commits_an_aggregate_without_domain_events() {
    let cell = AggregateCell::<Tally>::new("t").unwrap().mount(bump());
    for count in 1..=2 {
        let identity = CellCommandIdentity::new(
            "tallies",
            "ana",
            format!("0190a000-0000-7000-8001-{count:012}"),
        )
        .unwrap();
        let result = cell
            .dispatch_idempotent(
                "tally.bump",
                &identity,
                json!({ "tally_id": "t" }),
                session(Some("ana")),
            )
            .await
            .unwrap();
        assert_eq!(result.payload(), &json!({ "count": count }));
    }
    assert_eq!(cell.history().await.unwrap().len(), 2);
    assert!(cell.durable_outbox().unwrap().is_empty());
}

#[tokio::test]
async fn succeeded_publishes_the_captured_domain_events() {
    let cell = thin_cell("dee");
    for naps in 1..=2 {
        let payload = dispatch(
            &cell,
            "hero.siesta",
            json!({ "minutes": 5 }),
            session(Some("dee")),
        )
        .await
        .unwrap();
        assert_eq!(
            payload,
            json!({ "hero_id": "dee", "name": "dee", "items": 0 })
        );
        assert_eq!(cell.load().await.unwrap().unwrap().naps, naps);
    }
    assert_eq!(
        outbox_events(&cell),
        [
            (Some(1), "hero.created".to_string()),
            (Some(2), "hero.napped".to_string()),
            (Some(3), "hero.napped".to_string()),
        ]
    );
}

#[tokio::test]
async fn create_load_starts_a_new_aggregate_and_rejects_an_existing_one() {
    let cell = thin_cell("t1");
    let created = dispatch(
        &cell,
        "hero.enlist",
        json!({ "hero_id": "t1", "name": "Ana" }),
        session(Some("ana")),
    )
    .await
    .unwrap();
    assert_eq!(
        created,
        json!({ "hero_id": "t1", "name": "Ana", "items": 0 })
    );

    let again = dispatch(
        &cell,
        "hero.enlist",
        json!({ "hero_id": "t1", "name": "Ana" }),
        session(Some("ana")),
    )
    .await;
    assert!(
        matches!(again, Err(CausalDispatchError::Rejected { .. })),
        "{again:?}"
    );
    assert_eq!(cell.history().await.unwrap().len(), 1);
}

#[tokio::test]
async fn required_load_and_guard_gate_the_transition() {
    let missing = dispatch(
        &thin_cell("t2"),
        "hero.outfit",
        json!({ "hero_id": "t2", "slot": "hand", "item": "axe" }),
        session(Some("ana")),
    )
    .await;
    assert!(
        matches!(
            missing,
            Err(CausalDispatchError::Rejected { status: 404, .. })
        ),
        "{missing:?}"
    );

    let cell = thin_cell("t3");
    dispatch(
        &cell,
        "hero.enlist",
        json!({ "hero_id": "t3", "name": "Ana" }),
        session(Some("ana")),
    )
    .await
    .unwrap();
    let outfitted = dispatch(
        &cell,
        "hero.outfit",
        json!({ "hero_id": "t3", "slot": "hand", "item": "sword" }),
        session(Some("ana")),
    )
    .await
    .unwrap();
    assert_eq!(outfitted["items"], 1);

    // Mallory owns this hero, so only the guard can refuse her.
    let guarded = thin_cell("t4");
    dispatch(
        &guarded,
        "hero.enlist",
        json!({ "hero_id": "t4", "name": "Mal" }),
        session(Some("mallory")),
    )
    .await
    .unwrap();
    let refused = dispatch(
        &guarded,
        "hero.outfit",
        json!({ "hero_id": "t4", "slot": "head", "item": "hat" }),
        session(Some("mallory")),
    )
    .await;
    assert!(refused.is_err(), "the guard must refuse: {refused:?}");
    assert!(guarded.load().await.unwrap().unwrap().slots.is_empty());
}

#[tokio::test]
async fn or_create_load_reuses_the_shard_aggregate() {
    let cell = thin_cell("cy");
    for naps in 1..=2 {
        dispatch(
            &cell,
            "hero.doze",
            json!({ "minutes": 5 }),
            session(Some("cy")),
        )
        .await
        .unwrap();
        assert_eq!(cell.load().await.unwrap().unwrap().naps, naps);
    }
    // created + two naps on one aggregate
    assert_eq!(cell.history().await.unwrap().len(), 3);
}

#[test]
fn declarations_register_their_fields_and_the_declaring_package() {
    let specs = distributed::microsvc::Routes::new()
        .with_repo(distributed::AggregateRepository::<_, Hero>::new(
            distributed::InMemoryRepository::new(),
        ))
        .mount(enlist())
        .mount(outfit_command())
        .mount(doze())
        .command_specs()
        .unwrap();
    let field = |id: &str| {
        specs
            .iter()
            .find(|spec| spec.id == id)
            .unwrap_or_else(|| panic!("missing {id}"))
            .field_name
            .clone()
    };
    assert_eq!(field("hero.enlist"), "heroes_enlist");
    assert_eq!(field("hero.outfit"), "heroes_outfit");
    assert_eq!(field("hero.doze"), "heroes_doze");

    let outfit = specs.iter().find(|spec| spec.id == "hero.outfit").unwrap();
    let contract = &outfit.projection_contract;
    let pure = &contract["pure_reduces"][0];
    let package = env!("CARGO_PKG_NAME");
    assert_eq!(
        pure["fn_name"],
        format!("{package}::equip_preview"),
        "{contract}"
    );
    assert_eq!(pure["wasm_rust_package"], package, "{contract}");
    assert_eq!(pure["wasm_export"], "equip_preview", "{contract}");
    assert_eq!(pure["model"], "Heroes", "{contract}");
    assert_eq!(pure["key"][0]["name"], "hero_id", "{contract}");
    assert_eq!(pure["args"][0]["name"], "item", "{contract}");
    assert_eq!(pure["assign"], serde_json::json!(["items"]), "{contract}");
    assert_eq!(
        distributed::command::__run_wasm_pure(
            r#"{"items":["a"],"hero_id":"h"}"#,
            r#"{"item":"b"}"#,
            equip_preview
        )
        .as_deref(),
        Some(r#"{"items":["a","b"]}"#)
    );
}
