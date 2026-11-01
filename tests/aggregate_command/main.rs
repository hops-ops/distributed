//! `#[command]` on aggregate methods inside `#[sourced]`: the generated
//! handler binds the principal from the session only, loads / creates the
//! aggregate, maps errors to rejections, builds the payload and commits; the
//! generated registration matches the hand-written form (command specs,
//! GraphQL SDL and the client manifest).

use distributed::cell_host::AggregateCell;
use distributed::command::{Eventual, PreparedCommand, Succeeded};
use distributed::microsvc::{
    require_loaded, CausalCommandContext, HandlerError, Session, USER_ID_KEY,
};
use distributed::{sourced, DomainState, Entity};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Hero {
    entity: Entity,
    owner: String,
    name: String,
    slots: Vec<(String, String)>,
    naps: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, DomainState)]
#[domain_state(version = 1)]
pub struct HeroState {
    pub hero_id: String,
    pub owner: String,
    pub name: String,
    pub naps: u32,
}

impl From<&Hero> for HeroState {
    fn from(hero: &Hero) -> Self {
        Self {
            hero_id: hero.entity.id().to_string(),
            owner: hero.owner.clone(),
            name: hero.name.clone(),
            naps: hero.naps,
        }
    }
}

#[derive(Debug, Deserialize, distributed::CommandInput)]
#[serde(deny_unknown_fields)]
pub struct CreateHero {
    pub hero_id: String,
    pub name: String,
}

#[derive(Debug, Deserialize, distributed::CommandInput)]
#[serde(deny_unknown_fields)]
pub struct EquipInput {
    pub hero_id: String,
    pub slot: String,
    pub item: String,
}

#[derive(Debug, Deserialize, distributed::CommandInput)]
#[serde(deny_unknown_fields)]
pub struct NapInput {
    pub minutes: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, distributed::CommandOutput)]
pub struct HeroPayload {
    pub hero_id: String,
    pub name: String,
    pub items: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, distributed::CommandOutput)]
pub struct NapReceipt {
    pub hero_id: String,
    pub naps: u32,
    pub sequence: u64,
    pub at_ms: u64,
}

#[derive(Debug)]
pub enum HeroError {
    NotOwner,
    Exists,
    EmptySlot,
    Sourced(String),
}

impl std::fmt::Display for HeroError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotOwner => f.write_str("not your hero"),
            Self::Exists => f.write_str("hero exists"),
            Self::EmptySlot => f.write_str("slot is empty"),
            Self::Sourced(e) => f.write_str(e),
        }
    }
}

fn sourced_error(e: impl std::fmt::Display) -> HeroError {
    HeroError::Sourced(e.to_string())
}

pub fn hero_payload(hero: &Hero) -> HeroPayload {
    HeroPayload {
        hero_id: hero.entity.id().to_string(),
        name: hero.name.clone(),
        items: hero.slots.len() as u32,
    }
}

pub fn signed_in(ctx: &CausalCommandContext<'_, Hero>) -> bool {
    ctx.session().user_id().is_some()
}

#[sourced(
    entity,
    aggregate_type = "hero",
    events = "HeroEvent",
    domain_state = HeroState
)]
impl Hero {
    /// Whole input, new aggregate, the method's `Ok` value is the payload.
    #[command("hero.create", load = create, outcome = eventual, roles = ["player"])]
    pub fn create(
        &mut self,
        #[principal] owner: &str,
        input: CreateHero,
    ) -> Result<HeroPayload, HeroError> {
        if !self.entity.id().is_empty() {
            return Err(HeroError::Exists);
        }
        self.created(input.hero_id, owner.to_string(), input.name)
            .map_err(sourced_error)?;
        Ok(hero_payload(self))
    }

    /// Parameters mapped to input fields by name (`&str` borrows), a payload
    /// function, a guard and a custom GraphQL field.
    #[command(
        "hero.equip",
        shard = hero_id,
        load = required,
        input = EquipInput,
        outcome = Eventual<HeroPayload>,
        payload = hero_payload,
        roles = ["player"],
        field = "heroes_equip",
        guard = signed_in
    )]
    pub fn equip(
        &mut self,
        slot: String,
        #[principal] actor: &str,
        item: &str,
    ) -> Result<(), HeroError> {
        if actor != self.owner {
            return Err(HeroError::NotOwner);
        }
        if slot.is_empty() {
            return Err(HeroError::EmptySlot);
        }
        self.equipped(slot, item.to_string()).map_err(sourced_error)
    }

    /// Sharded by the session principal (`String` parameter), or-created,
    /// receipt from `Entity::last_recorded`.
    #[command("hero.nap", shard = sleeper, load = or_create, outcome = succeeded)]
    pub fn nap(
        &mut self,
        #[principal] sleeper: String,
        input: NapInput,
    ) -> Result<NapReceipt, HeroError> {
        let _ = input.minutes;
        if self.entity.id().is_empty() {
            self.created(sleeper.clone(), sleeper.clone(), sleeper.clone())
                .map_err(sourced_error)?;
        }
        self.napped().map_err(sourced_error)?;
        let recorded = self.entity.last_recorded().expect("recorded");
        Ok(NapReceipt {
            hero_id: sleeper,
            naps: self.naps,
            sequence: recorded.sequence,
            at_ms: recorded.unix_ms(),
        })
    }

    #[event("hero.created", version = 1, domain)]
    fn created(&mut self, hero_id: String, owner: String, name: String) {
        self.entity.set_id(hero_id);
        self.owner = owner;
        self.name = name;
    }

    #[event("hero.equipped", version = 1, domain)]
    fn equipped(&mut self, slot: String, item: String) {
        self.slots.push((slot, item));
    }

    #[event("hero.napped", version = 1, domain)]
    fn napped(&mut self) {
        self.naps += 1;
    }
}

fn session(user: Option<&str>) -> Session {
    let mut session = Session::new();
    if let Some(user) = user {
        session.set(USER_ID_KEY, user);
        session.set("x-roles", "player");
    }
    session
}

fn cell(id: &str) -> AggregateCell<Hero> {
    AggregateCell::<Hero>::new(id)
        .unwrap()
        .mount(Create)
        .mount(Equip)
        .mount(Nap)
}

#[tokio::test]
async fn generated_handlers_bind_the_session_principal_and_commit() {
    let cell = cell("h1");
    let created = cell
        .dispatch(
            "hero.create",
            json!({ "hero_id": "h1", "name": "Ana" }),
            session(Some("ana")),
        )
        .await
        .unwrap();
    assert_eq!(
        created,
        json!({ "hero_id": "h1", "name": "Ana", "items": 0 })
    );
    let equipped = cell
        .dispatch(
            "hero.equip",
            json!({ "hero_id": "h1", "slot": "hand", "item": "sword" }),
            session(Some("ana")),
        )
        .await
        .unwrap();
    assert_eq!(equipped["items"], 1);
    // Domain errors become rejections; the owner check sees the session's
    // principal, never the input.
    let stolen = cell
        .dispatch(
            "hero.equip",
            json!({ "hero_id": "h1", "slot": "hand", "item": "axe" }),
            session(Some("bo")),
        )
        .await;
    assert!(
        matches!(&stolen, Err(HandlerError::Rejected(m)) if m == "not your hero"),
        "{stolen:?}"
    );
    // No session principal: refused before the aggregate is touched.
    let anonymous = cell
        .dispatch(
            "hero.equip",
            json!({ "hero_id": "h1", "slot": "hand", "item": "axe" }),
            session(None),
        )
        .await;
    assert!(anonymous.is_err(), "{anonymous:?}");
    // Unknown fields (an attempt to pass identity) are refused by the input.
    let forged = cell
        .dispatch(
            "hero.equip",
            json!({ "hero_id": "h1", "slot": "hand", "item": "axe", "actor": "ana" }),
            session(Some("bo")),
        )
        .await;
    assert!(forged.is_err(), "{forged:?}");
    let hero = cell.load().await.unwrap().unwrap();
    assert_eq!(hero.slots, [("hand".to_string(), "sword".to_string())]);
    assert_eq!(hero.owner, "ana");

    // `load = required` on a missing aggregate.
    let missing = AggregateCell::<Hero>::new("nobody")
        .unwrap()
        .mount(Equip)
        .dispatch(
            "hero.equip",
            json!({ "hero_id": "nobody", "slot": "hand", "item": "axe" }),
            session(Some("ana")),
        )
        .await;
    assert!(missing.is_err(), "{missing:?}");
}

#[tokio::test]
async fn or_create_sharded_by_principal_returns_the_recorded_position() {
    let cell = AggregateCell::<Hero>::new("cy").unwrap().mount(Nap);
    let first = cell
        .dispatch("hero.nap", json!({ "minutes": 5 }), session(Some("cy")))
        .await
        .unwrap();
    // created (1) + napped (2)
    assert_eq!(first["sequence"], 2);
    assert_eq!(first["naps"], 1);
    assert!(first["at_ms"].as_u64().unwrap() > 1_600_000_000_000);
    let second = cell
        .dispatch("hero.nap", json!({ "minutes": 5 }), session(Some("cy")))
        .await
        .unwrap();
    assert_eq!(second["sequence"], 3);
    assert_eq!(second["hero_id"], "cy");
    let history = cell.history().await.unwrap();
    assert_eq!(history.len(), 3);
    assert_eq!(history[2].sequence, 3);
}

#[test]
fn mounts_expose_command_names_and_shards() {
    assert_eq!(Create::COMMAND, "hero.create");
    assert_eq!(Equip::COMMAND, "hero.equip");
    assert_eq!(Nap::COMMAND, "hero.nap");
    let input = EquipInput {
        hero_id: "h9".into(),
        slot: "s".into(),
        item: "i".into(),
    };
    assert_eq!(Equip::shard(&input), "h9");
    assert_eq!(
        <EquipInput as distributed::command::CommandInputType>::FIELD_NAMES,
        ["hero_id", "slot", "item"]
    );
}

// ---- golden: the generated registration equals the hand-written form -----

mod handwritten {
    use super::*;

    pub async fn create(
        ctx: &CausalCommandContext<'_, Hero>,
        input: CreateHero,
    ) -> Result<PreparedCommand<Eventual<HeroPayload>>, HandlerError> {
        let actor = ctx.user_id()?.to_owned();
        let mut hero = ctx.create();
        let payload = hero
            .aggregate_mut()
            .create(&actor, input)
            .map_err(|e| HandlerError::Rejected(e.to_string()))?;
        ctx.publish_events().commit(hero)?.eventual(payload)
    }

    pub async fn equip(
        ctx: &CausalCommandContext<'_, Hero>,
        input: EquipInput,
    ) -> Result<PreparedCommand<Eventual<HeroPayload>>, HandlerError> {
        let actor = ctx.user_id()?.to_owned();
        let mut hero = require_loaded(ctx.get(&input.hero_id).await?, input.hero_id.clone())?;
        hero.aggregate_mut()
            .equip(input.slot, &actor, &input.item)
            .map_err(|e| HandlerError::Rejected(e.to_string()))?;
        let payload = hero_payload(hero.aggregate());
        ctx.publish_events().commit(hero)?.eventual(payload)
    }

    pub async fn nap(
        ctx: &CausalCommandContext<'_, Hero>,
        input: NapInput,
    ) -> Result<PreparedCommand<Succeeded<NapReceipt>>, HandlerError> {
        let actor = ctx.user_id()?.to_owned();
        let mut hero = match ctx.get(&actor).await? {
            Some(hero) => hero,
            None => ctx.create(),
        };
        let receipt = hero
            .aggregate_mut()
            .nap(actor, input)
            .map_err(|e| HandlerError::Rejected(e.to_string()))?;
        ctx.publish_events().commit(hero)?.succeeded(receipt)
    }

    pub fn install<D>(routes: distributed::microsvc::Routes<D>) -> distributed::microsvc::Routes<D>
    where
        D: distributed::microsvc::CausalRouteDependencies<Aggregate = Hero> + Send + Sync + 'static,
    {
        routes
            .command_transition::<domain_commands::Create, CreateHero, Eventual<HeroPayload>>(
                "hero.create",
            )
            .roles(["player"])
            .handle(create)
            .command_transition::<domain_commands::Equip, EquipInput, Eventual<HeroPayload>>(
                "hero.equip",
            )
            .field_name("heroes_equip")
            .roles(["player"])
            .guarded(signed_in, equip)
            .command_transition::<domain_commands::Nap, NapInput, Succeeded<NapReceipt>>("hero.nap")
            .handle(nap)
    }
}

#[test]
fn generated_command_specs_equal_the_hand_written_registration() {
    let generated = AggregateCell::<Hero>::new("x")
        .unwrap()
        .mount(Create)
        .mount(Equip)
        .mount(Nap);
    let handwritten = AggregateCell::<Hero>::new("x")
        .unwrap()
        .mount(handwritten::install);
    assert_eq!(generated.command_names(), handwritten.command_names());
    let specs = |routes: distributed::microsvc::Routes<_>| {
        serde_json::to_value(routes.command_specs().unwrap()).unwrap()
    };
    let repo =
        || distributed::AggregateRepository::<_, Hero>::new(distributed::InMemoryRepository::new());
    let generated = specs(
        distributed::microsvc::Routes::new()
            .with_repo(repo())
            .mount(Create)
            .mount(Equip)
            .mount(Nap),
    );
    let handwritten = specs(handwritten::install(
        distributed::microsvc::Routes::new().with_repo(repo()),
    ));
    assert_eq!(generated, handwritten);
    // The domain transition witness carries each command's outward events.
    let text = generated.to_string();
    assert!(text.contains("hero.equipped"), "{text}");
}

#[cfg(feature = "graphql")]
#[test]
fn generated_graphql_sdl_and_client_manifest_equal_the_hand_written_form() {
    use distributed::graphql::{
        build_surface, graphql_sdl_from_surface, surface_for_role, DistributedClientSurfaceExport,
        SurfaceOptions,
    };
    use distributed::microsvc::{Routes, Service};

    let render = |generated: bool| {
        let routes = Routes::new().with_repo(distributed::AggregateRepository::<_, Hero>::new(
            distributed::InMemoryRepository::new(),
        ));
        let routes = if generated {
            routes.mount(Create).mount(Equip).mount(Nap)
        } else {
            handwritten::install(routes)
        };
        let service = Service::new().named("heroes").routes(routes);
        let surface = build_surface(&[], &SurfaceOptions::postgres())
            .unwrap()
            .with_service(&service)
            .unwrap();
        let selected =
            surface_for_role(&surface, "player", &std::collections::BTreeMap::new()).unwrap();
        let sdl = graphql_sdl_from_surface(&selected).unwrap();
        let manifest = DistributedClientSurfaceExport::from_selected("heroes", selected)
            .unwrap()
            .manifest()
            .unwrap();
        (sdl, serde_json::to_value(manifest).unwrap())
    };
    let (generated_sdl, generated_manifest) = render(true);
    let (handwritten_sdl, handwritten_manifest) = render(false);
    assert!(generated_sdl.contains("heroes_equip"), "{generated_sdl}");
    assert_eq!(generated_sdl, handwritten_sdl);
    assert_eq!(generated_manifest, handwritten_manifest);
}
