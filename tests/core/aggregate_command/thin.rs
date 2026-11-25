//! `command!` (the thin, aggregate-external form) against the same `Hero`:
//! each `load:` kind, `guard:`, `constructor:` and
//! `preview_reduce_known_record:` behave as declared at dispatch time.

use super::*;
use distributed::command;
use distributed::command::CommandProjectionPureReduce;

fn not_mallory(ctx: &CausalCommandContext<'_, Hero>) -> bool {
    ctx.session().user_id() != Some("mallory")
}

fn hero_preview() -> CommandProjectionPureReduce {
    CommandProjectionPureReduce::wasm(
        "hero.preview",
        "hero/pkg/hero_wasm",
        "heroPreview",
        "Heroes",
    )
    .key_input("hero_id", ["hero_id"])
    .arg_input("item", ["item"])
    .assign(["items"])
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

fn thin_cell(id: &str) -> AggregateCell<Hero> {
    AggregateCell::<Hero>::new(id)
        .unwrap()
        .mount(enlist())
        .mount(outfit_command())
        .mount(doze())
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
    assert_eq!(pure["fn_name"], "hero.preview", "{contract}");
    assert_eq!(
        pure["wasm_rust_package"],
        env!("CARGO_PKG_NAME"),
        "{contract}"
    );
}
