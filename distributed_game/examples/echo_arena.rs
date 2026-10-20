//! Tiny WebSocket zone server for client end-to-end tests (feature `net`).
//!
//! Every player owns one avatar moved by 2-byte inputs `[dx, dy]` (i8,
//! clamped to -1..=1) at 10 units/s. On join the player receives Event type
//! 1 carrying its avatar entity id (u32 LE) so a client can bind prediction.
//! Zones `arena-*` are created on demand; tickets are HS256 with the secret
//! from `DISTRIBUTED_GAME_SECRET` (default `dev-secret`).
//!
//! Prints `LISTENING ws://127.0.0.1:<port>` once bound, then serves until
//! stdin closes (so a parent test process can stop it by exiting).
//!
//! ```sh
//! cargo run -p distributed_game --features net --example echo_arena
//! ```

use std::collections::BTreeMap;
use std::io::{BufRead, Write};

use distributed_game::net::{GameServer, ServerConfig, TicketKey, ZoneFactory};
use distributed_game::{
    ConsumePolicy, Ctx, EntitySchema, EntityState, FieldType, FieldValue, KindDef, LeaveReason,
    PlayerId, View, WorldView, Zone,
};

const AVATAR: u16 = 1;
const SPEED: f32 = 10.0;

struct EchoArena {
    avatars: BTreeMap<PlayerId, (u32, f32, f32)>,
}

impl Zone for EchoArena {
    type Config = ();
    type Input = (i8, i8);
    type Timer = ();
    type Message = distributed_game::NoMessages;

    fn schema() -> EntitySchema {
        let pos = FieldType::QuantF32 {
            min: -500.0,
            max: 500.0,
            bits: 20,
        };
        EntitySchema::new().kind(
            KindDef::new(AVATAR, "avatar")
                .field("x", pos)
                .field("y", pos)
                .field("seq_hint", FieldType::U32),
        )
    }

    fn on_create(_ctx: &mut Ctx<'_, Self>, _config: ()) -> Self {
        Self {
            avatars: BTreeMap::new(),
        }
    }

    fn on_join(&mut self, ctx: &mut Ctx<'_, Self>, player: PlayerId) {
        let entity = 100 + player.0;
        self.avatars.insert(player, (entity, 0.0, 0.0));
        ctx.set_view(player, View::new(0.0, 0.0, 1000.0));
        ctx.send_event(player, 1, entity.to_le_bytes().to_vec());
    }

    fn on_leave(&mut self, _ctx: &mut Ctx<'_, Self>, player: PlayerId, _reason: LeaveReason) {
        self.avatars.remove(&player);
    }

    fn decode_input(&self, _player: PlayerId, payload: &[u8]) -> Option<(i8, i8)> {
        match payload {
            [dx, dy] => Some(((*dx as i8).clamp(-1, 1), (*dy as i8).clamp(-1, 1))),
            _ => None,
        }
    }

    fn on_tick(&mut self, ctx: &mut Ctx<'_, Self>) {
        let dt = ctx.dt_secs();
        let players: Vec<PlayerId> = self.avatars.keys().copied().collect();
        for player in players {
            let inputs = ctx.take_inputs(player, ConsumePolicy::Drain);
            if let Some(avatar) = self.avatars.get_mut(&player) {
                for input in inputs {
                    avatar.1 += f32::from(input.input.0) * SPEED * dt;
                    avatar.2 += f32::from(input.input.1) * SPEED * dt;
                }
            }
        }
    }

    fn replicate(&self, out: &mut WorldView) {
        for (entity, x, y) in self.avatars.values() {
            out.push(EntityState::new(
                *entity,
                AVATAR,
                *x,
                *y,
                vec![FieldValue::F(*x), FieldValue::F(*y), FieldValue::U(0)],
            ));
        }
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let secret = std::env::var("DISTRIBUTED_GAME_SECRET").unwrap_or_else(|_| "dev-secret".into());
    let mut config = ServerConfig::new(TicketKey::new(secret));
    config.host.tick_rate = 30;
    config.host.patch_rate = 20;
    let server = GameServer::<EchoArena>::new(
        config,
        ZoneFactory::sync(|a| a.zone_id.starts_with("arena-").then_some(())),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    println!("LISTENING ws://{addr}");
    std::io::stdout().flush()?;
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    std::thread::spawn(move || {
        // Stop when the parent closes our stdin (or sends a line).
        let mut line = String::new();
        let _ = std::io::stdin().lock().read_line(&mut line);
        let _ = stop_tx.send(());
    });
    server
        .serve(listener, async move {
            let _ = stop_rx.await;
        })
        .await
}
