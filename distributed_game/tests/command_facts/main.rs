//! The fact routes a zone server uses with celld (`tests/celld_facts`), on
//! an ordinary service instead: `CommandFactSink<LocalCommandHost>` over an
//! in-memory repository. Kills land in the killer's stream, match results in
//! the match's stream, lifecycle facts in the player's — no cell involved —
//! with the trusted principal and stamp, exactly once per fact key.

use std::sync::Arc;

use distributed::command::{typed_command, PreparedCommand, Succeeded};
use distributed::microsvc::{
    CausalCommandContext, CausalRouteDependencies, HandlerError, PortableCommand, Routes, Service,
};
use distributed::{sourced, AggregateRepository, Entity, InMemoryRepository, LocalCommandHost};
use distributed_game::cell::session_stamp;
use distributed_game::command_sink::{CommandFactSink, FactRoute};
use distributed_game::lifecycle::{JoinVia, PlayerJoined, LIFECYCLE_COMMANDS};
use distributed_game::{Fact, FactCommand, FactError, FactSink, FactStamp};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Serialize, FactCommand)]
#[fact(command = "kills.record")]
struct Kill {
    mob: String,
}

#[derive(Serialize, FactCommand)]
#[fact(command = "match.record_result")]
struct MatchResult {
    match_id: String,
    winner_slot: u32,
}

/// One stream per routed shard: a player's kills and lifecycle, a match.
#[derive(Clone, Default, Serialize, Deserialize, distributed::Snapshot)]
struct Log {
    entity: Entity,
    entries: Vec<String>,
}

#[sourced(entity, aggregate_type = "Log", events = "LogEvent")]
impl Log {
    #[event("log.recorded", version = 1)]
    fn recorded(&mut self, id: String, entry: String) {
        if self.entity.id().is_empty() {
            self.entity.set_id(id);
        }
        self.entries.push(entry);
    }
}

#[derive(Debug, Deserialize, distributed::CommandInput)]
struct KillInput {
    mob: String,
}

#[derive(Debug, Deserialize, distributed::CommandInput)]
struct ResultInput {
    match_id: String,
    winner_slot: u32,
}

#[derive(Debug, Deserialize, distributed::CommandInput)]
#[allow(dead_code)]
struct LifecycleInput {
    zone: String,
    via: Option<String>,
    from_zone: Option<String>,
    reason: Option<String>,
    grace_ms: Option<u64>,
    cause: Option<String>,
}

#[derive(Debug, Serialize, distributed::CommandOutput)]
struct Recorded {
    stream: String,
    entries: usize,
}

fn rejected(error: impl std::fmt::Display) -> HandlerError {
    HandlerError::Rejected(error.to_string())
}

/// Append `entry` to stream `id` (the shard the route would address).
async fn record(
    ctx: &CausalCommandContext<'_, Log>,
    id: String,
    entry: String,
) -> Result<PreparedCommand<Succeeded<Recorded>>, HandlerError> {
    let repo = ctx.repo();
    let mut log = match repo.get(&id).await? {
        Some(log) => log,
        None => repo.create(),
    };
    log.recorded(id.clone(), entry).map_err(rejected)?;
    let entries = log.entries.len();
    repo.commit(log)?.succeeded(Recorded {
        stream: id,
        entries,
    })
}

async fn handle_kill(
    ctx: &CausalCommandContext<'_, Log>,
    input: KillInput,
) -> Result<PreparedCommand<Succeeded<Recorded>>, HandlerError> {
    // Same shard as `FactRoute::by_principal`: the trusted session's user.
    let principal = ctx.user_id()?.to_string();
    let zone = session_stamp(ctx.session())
        .ok_or_else(|| rejected("no stamp"))?
        .zone_id;
    record(
        ctx,
        format!("kills/{principal}"),
        format!("{}@{zone}", input.mob),
    )
    .await
}

async fn handle_result(
    ctx: &CausalCommandContext<'_, Log>,
    input: ResultInput,
) -> Result<PreparedCommand<Succeeded<Recorded>>, HandlerError> {
    // Same shard as `FactRoute::by_input_field("match_id")`.
    record(
        ctx,
        format!("match/{}", input.match_id),
        format!("winner {}", input.winner_slot),
    )
    .await
}

async fn handle_joined(
    ctx: &CausalCommandContext<'_, Log>,
    input: LifecycleInput,
) -> Result<PreparedCommand<Succeeded<Recorded>>, HandlerError> {
    let principal = ctx.user_id()?.to_string();
    record(
        ctx,
        format!("presence/{principal}"),
        format!("joined {}", input.zone),
    )
    .await
}

struct Commands;
impl<D> PortableCommand<D> for Commands
where
    D: CausalRouteDependencies<Aggregate = Log> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(typed_command::<KillInput, Succeeded<Recorded>>(
                Kill::COMMAND,
            ))
            .handle(handle_kill)
            .typed_command(typed_command::<ResultInput, Succeeded<Recorded>>(
                MatchResult::COMMAND,
            ))
            .handle(handle_result)
            .typed_command(typed_command::<LifecycleInput, Succeeded<Recorded>>(
                LIFECYCLE_COMMANDS[0],
            ))
            .handle(handle_joined)
    }
}

fn stamp(tick: u64) -> FactStamp {
    FactStamp {
        zone_id: "pit-1".into(),
        tick,
        content_version: "c1".into(),
        emitted_at_ms: 1_700_000_000_000 + tick,
        activation: 1,
        config_version: "cfg-1".into(),
        seq: tick,
    }
}

fn fact<C: FactCommand + Serialize>(
    key: &str,
    input: &C,
    principal: Option<&str>,
    tick: u64,
) -> Fact {
    Fact::trusted(
        key,
        C::COMMAND,
        serde_json::to_value(input).unwrap(),
        principal.map(str::to_string),
        stamp(tick),
    )
}

#[tokio::test]
async fn the_celld_fact_routes_land_on_a_service_over_any_repository() {
    let repo = InMemoryRepository::new();
    let routes = Commands.install(Routes::new().with_repo(AggregateRepository::new(repo.clone())));
    let service = Arc::new(Service::new().named("zone-server-test").routes(routes));
    // The same routes as the celld fact-routing test.
    let sink = CommandFactSink::new(
        LocalCommandHost::new(service),
        "zone-server-test",
        "zone:pit-1",
    )
    .unwrap()
    .route(FactRoute::by_principal::<Kill>("kills"))
    .route(FactRoute::by_input_field::<MatchResult>(
        "match", "match_id",
    ))
    .lifecycle_route("presence");

    let kill = fact("kill:1", &Kill { mob: "m1".into() }, Some("alice"), 1);
    let first = sink.commit(&kill).await.unwrap();
    assert!(!first.replayed);
    assert_eq!(
        first.payload,
        json!({ "stream": "kills/alice", "entries": 1 })
    );
    let again = sink.commit(&kill).await.unwrap();
    assert!(again.replayed, "same fact key replays through the ledger");
    assert_eq!(again.payload, first.payload);
    let second = sink
        .commit(&fact(
            "kill:2",
            &Kill { mob: "m2".into() },
            Some("alice"),
            2,
        ))
        .await
        .unwrap();
    assert_eq!(second.payload["entries"], 2, "credited once per key");

    let result = sink
        .commit(&fact(
            "result:1",
            &MatchResult {
                match_id: "m-7".into(),
                winner_slot: 1,
            },
            None,
            3,
        ))
        .await
        .unwrap();
    assert_eq!(result.payload["stream"], "match/m-7");

    let joined = PlayerJoined {
        zone: "pit-1".into(),
        via: JoinVia::Ticket,
        from_zone: None,
    };
    let lifecycle = Fact::trusted(
        "joined:1",
        LIFECYCLE_COMMANDS[0],
        serde_json::to_value(&joined).unwrap(),
        Some("alice".into()),
        stamp(4),
    );
    let presence = sink.commit(&lifecycle).await.unwrap();
    assert_eq!(presence.payload["stream"], "presence/alice");

    // Unrouted commands are permanent; so is a command the service rejects.
    let unrouted = Fact::trusted(
        "x",
        "guild.whatever",
        json!({}),
        Some("alice".into()),
        stamp(5),
    );
    assert!(matches!(
        sink.commit(&unrouted).await,
        Err(FactError::Permanent(_))
    ));
    let forged = Fact::trusted(
        "kill:3",
        Kill::COMMAND,
        json!({ "mob": "m3", "xp": 9999 }),
        Some("alice".into()),
        stamp(6),
    );
    assert!(matches!(
        sink.commit(&forged).await,
        Err(FactError::Permanent(_))
    ));
}
