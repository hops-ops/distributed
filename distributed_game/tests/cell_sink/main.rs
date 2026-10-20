//! Facts committed into a real `distributed` AggregateCell through
//! `dispatch_idempotent` with a server-built trusted Session.

use std::sync::Arc;
use std::time::Duration;

use distributed::cell_host::AggregateCell;
use distributed::command::{typed_command, PreparedCommand, Succeeded};
use distributed::microsvc::{
    CausalCommandContext, CausalRouteDependencies, HandlerError, PortableCommand, Routes,
};
use distributed::{sourced, Entity};
use std::collections::HashMap;

use distributed_game::cell::{command_id_for, session_stamp, CellFactSink};
use distributed_game::{
    fact_channel, spawn_fact_worker, stamp_metadata, Fact, FactOutcome, FactStamp, RetryPolicy,
};
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

#[derive(Debug, Deserialize, distributed::CommandInput)]
struct CreditInput {
    amount: u32,
}

#[derive(Debug, Serialize, distributed::CommandOutput)]
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

#[tokio::test]
async fn facts_dispatch_idempotently_with_trusted_principal() {
    let cell = Arc::new(AggregateCell::<Hero>::new("alice").unwrap().mount(CreditXp));
    let sink = CellFactSink::new(cell.clone(), "game-svc", "zone-system");
    let (mut queue, feed) = fact_channel(16, RetryPolicy::default());
    let worker = spawn_fact_worker(feed, sink);

    let stamp = FactStamp {
        zone_id: "z1".into(),
        tick: 99,
        content_version: "c1".into(),
        emitted_at_ms: 1_700_000_000_456,
        activation: 77,
        config_version: "combat@12".into(),
    };
    let fact = Fact::trusted(
        "coin:z1:7",
        "hero.credit_xp",
        json!({ "amount": 10 }),
        Some("alice".into()),
        stamp.clone(),
    );
    queue.try_send(fact.clone()).unwrap();
    let first = wait_one(&mut queue).await;
    queue.try_send(fact).unwrap();
    let replay = wait_one(&mut queue).await;

    let FactOutcome::Committed(first) = first.outcome else {
        panic!("first commit failed: {first:?}")
    };
    let FactOutcome::Committed(replay) = replay.outcome else {
        panic!("replay failed: {replay:?}")
    };
    assert!(!first.replayed);
    assert!(
        replay.replayed,
        "same key replays through the command ledger"
    );
    assert_eq!(first.payload["hero"], "alice");
    assert_eq!(first.payload["xp"], 10);
    assert_eq!(first.payload["tick"], "99");
    assert_eq!(first.payload["emitted_at_ms"], 1_700_000_000_456u64);
    assert_eq!(first.payload["activation"], 77);
    assert_eq!(first.payload["config_version"], "combat@12");
    assert_eq!(cell.load().await.unwrap().unwrap().xp, 10, "credited once");

    // Payload-supplied identity is not a declared input: permanent failure.
    let forged = Fact::trusted(
        "coin:z1:8",
        "hero.credit_xp",
        json!({ "amount": 10, "principal": "mallory" }),
        Some("alice".into()),
        stamp,
    );
    queue.try_send(forged).unwrap();
    let forged = wait_one(&mut queue).await;
    assert!(matches!(
        forged.outcome,
        FactOutcome::Failed {
            error: distributed_game::FactError::Permanent(_),
            ..
        }
    ));
    assert_eq!(cell.load().await.unwrap().unwrap().xp, 10);

    drop(queue);
    worker.await.unwrap();
}

async fn wait_one(queue: &mut distributed_game::FactQueue) -> distributed_game::FactSettlement {
    for _ in 0..500 {
        if let Some(s) = queue.drain().pop() {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("no settlement")
}
