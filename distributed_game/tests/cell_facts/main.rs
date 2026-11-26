//! Fact batches committed into a real `distributed` AggregateCell with a
//! server-built trusted Session.

use distributed::command::{typed_command, PreparedCommand, Succeeded};
use distributed::microsvc::cell_host::AggregateCell;
use distributed::microsvc::{
    CausalCommandContext, CausalRouteDependencies, HandlerError, PortableCommand, Routes,
};
use distributed::{sourced, Entity};
use std::collections::HashMap;

use distributed_game::cell::{
    command_id_for, fact_session_from_headers, run_fact_batch, session_stamp,
};
use distributed_game::{stamp_metadata, FactStamp};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Default, Serialize, Deserialize, distributed::Snapshot)]
struct Hero {
    entity: Entity,
    xp: u32,
}

#[sourced(entity, aggregate_type = "Hero", events = "HeroEvent")]
impl Hero {
    #[event("hero.xp_credited", version = 1)]
    fn credit(&mut self, id: String, amount: u32) {
        self.entity.set_id(id);
        self.xp += amount;
    }
}

#[derive(Debug, Deserialize, distributed::command::CommandInput)]
struct CreditInput {
    amount: u32,
}

#[derive(Debug, Serialize, distributed::command::CommandOutput)]
struct CreditPayload {
    hero: String,
    xp: u32,
    tick: String,
    /// The stamp the handler read from the trusted session, after a trip
    /// through event metadata.
    emitted_at_ms: u64,
    activation: u64,
    config_version: String,
}

struct CreditXp;

impl<D> PortableCommand<D> for CreditXp
where
    D: CausalRouteDependencies<Aggregate = Hero> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(typed_command::<CreditInput, Succeeded<CreditPayload>>(
                "hero.credit_xp",
            ))
            .guarded(
                |ctx: &CausalCommandContext<'_, Hero>| ctx.session().user_id().is_some(),
                handle_credit,
            )
    }
}

async fn handle_credit(
    ctx: &CausalCommandContext<'_, Hero>,
    input: CreditInput,
) -> Result<PreparedCommand<Succeeded<CreditPayload>>, HandlerError> {
    // Identity comes from the trusted session, never from the input.
    let principal = ctx.session().user_id().unwrap_or_default().to_string();
    let tick = ctx.session().get("x-game-tick").unwrap_or("").to_string();
    let stamp = session_stamp(ctx.session())
        .ok_or_else(|| HandlerError::Rejected("fact sessions carry a stamp".into()))?;
    let mut metadata = HashMap::new();
    stamp_metadata(&mut metadata, &stamp);
    let stamp = FactStamp::from_metadata(&metadata).expect("metadata round trip");
    let repo = ctx.repo();
    let mut hero = match repo.get(&principal).await? {
        Some(hero) => hero,
        None => repo.create(),
    };
    hero.credit(principal.clone(), input.amount)
        .map_err(|e| HandlerError::Rejected(e.to_string()))?;
    let xp = hero.xp;
    repo.commit(hero)?.succeeded(CreditPayload {
        hero: principal,
        xp,
        tick,
        emitted_at_ms: stamp.emitted_at_ms,
        activation: stamp.activation,
        config_version: stamp.config_version,
    })
}

#[test]
fn command_ids_are_deterministic_uuidv7_shaped() {
    let a = command_id_for("svc", "coin:z1:7");
    assert_eq!(a, command_id_for("svc", "coin:z1:7"));
    assert_ne!(a, command_id_for("svc", "coin:z1:8"));
    assert_eq!(&a[14..15], "7");
    assert!(matches!(&a[19..20], "8" | "9" | "a" | "b"));
}

/// A `__batch` body of `amounts` for alice, one entry per amount; `None`
/// is an entry without a partition.
fn batch_body(amounts: &[Option<u32>]) -> serde_json::Value {
    let entries: Vec<_> = amounts
        .iter()
        .enumerate()
        .map(|(i, amount)| {
            let mut session = serde_json::Map::new();
            session.insert("x-user-id".into(), json!("alice"));
            let stamp = FactStamp {
                zone_id: "z1".into(),
                tick: i as u64,
                ..FactStamp::default()
            };
            for (name, value) in stamp.session_vars() {
                session.insert(name, json!(value));
            }
            let mut entry = json!({
                "command": "hero.credit_xp",
                "commandId": command_id_for("game-svc", &format!("kill:z1:{i}")),
                "input": { "amount": amount.unwrap_or(1) },
                "session": session,
            });
            if amount.is_some() {
                entry["partition"] = json!("alice");
            }
            entry
        })
        .collect();
    json!({ "entries": entries })
}

#[tokio::test]
async fn a_replayed_batch_answers_only_replays() {
    let cell = AggregateCell::<Hero>::new("alice").unwrap().mount(CreditXp);
    let sessions = |header: &dyn Fn(&str) -> Option<String>| fact_session_from_headers(header);
    let body = batch_body(&[Some(10), Some(20), None, Some(30)]);

    let first = run_fact_batch(&cell, "game-svc", &body, sessions)
        .await
        .unwrap();
    let first = first["results"].as_array().unwrap().clone();
    let statuses: Vec<_> = first
        .iter()
        .map(|r| r["status"].as_u64().unwrap())
        .collect();
    // The entry without a partition is refused in its place; the rest commit.
    assert_eq!(statuses, [200, 200, 400, 200]);
    let xp: Vec<_> = first
        .iter()
        .map(|r| r["body"]["payload"]["xp"].clone())
        .collect();
    assert_eq!(xp, [json!(10), json!(30), json!(null), json!(60)]);
    assert!(first
        .iter()
        .all(|r| r["body"]["receipt"]["replayed"] != json!(true)));

    // The whole batch again, same command ids: replays only, nothing new.
    let replay = run_fact_batch(&cell, "game-svc", &body, sessions)
        .await
        .unwrap();
    let replay = replay["results"].as_array().unwrap().clone();
    assert_eq!(replay.len(), 4);
    for (i, (replayed, original)) in replay.iter().zip(&first).enumerate() {
        assert_eq!(replayed["status"], original["status"]);
        if i == 2 {
            continue;
        }
        assert_eq!(replayed["body"]["receipt"]["replayed"], json!(true));
        assert_eq!(replayed["body"]["payload"], original["body"]["payload"]);
    }
    assert_eq!(cell.load().await.unwrap().unwrap().xp, 60, "credited once");
    assert_eq!(cell.history().await.unwrap().len(), 3);
}
