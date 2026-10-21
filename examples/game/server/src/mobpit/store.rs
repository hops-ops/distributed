//! Mob Pit's durable state lives in celld cells (`examples/game/cells`),
//! next to the rooms that feed them: a room's kill and lifecycle facts go
//! to the killer's `crawler/{principal}` and `presence/{principal}` cells
//! object to object (`mobpit_rooms`, `distributed_game::celld_zone`).
//!
//! Each kill commit writes the `crawler.mob_killed` domain event to the
//! cell's outbox in the same cell transaction. celld drains it to the
//! `game-demo-outbox` Queue; the worker's consumer relays every message to
//! this server's authenticated `/internal/celld-queue/relay`
//! (`crate::common::relay`), which publishes it onto the projection bus.
//! `mobpit-service` and this server's [`super::directory`] project it (the
//! crawler list, which feeds the leaderboard). [`CrawlerCells`] reads one
//! cell by principal (the board rebuild's kill histories, presence).

use std::time::Duration;

use distributed::cell_host::{InternalHttpSecret, CELL_INTERNAL_SECRET_HEADER};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::crawler::{RecordKillInput, CRAWLER_KIND, PRESENCE_KIND};
use distributed_game::celld::{CelldFactSink, FactRoute};

/// Kill facts into the Crawler cells over celld's internal HTTP path, the
/// route a room uses object to object (tests and tools; rooms send theirs
/// from inside celld). Command-ledger service `mobpit-tools`.
pub fn kill_sink(celld: &CelldOptions) -> Result<CelldFactSink, String> {
    Ok(CelldFactSink::celld(
        celld.url.clone(),
        celld.secret.clone(),
        "mobpit-tools",
        "mobpit-tools",
    )?
    .route(FactRoute::by_principal::<RecordKillInput>(CRAWLER_KIND)))
}

pub use crate::common::CelldOptions;

/// A Crawler cell's state as the cells worker reports it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CrawlerSummary {
    /// Display name at the last kill.
    pub name: String,
    /// Total XP.
    pub xp: u64,
    /// Mobs killed.
    pub kills: u32,
    /// Level for `xp`.
    pub level: u32,
    /// Committed stream version.
    pub version: u64,
}

/// One `crawler.mob_killed` event of a Crawler cell's history.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KillRecord {
    /// Stream position (the leaderboard occurrence).
    pub sequence: u64,
    /// Cell time of the commit (Unix ms).
    pub at_ms: u64,
    /// Principal.
    pub crawler: String,
    /// Display name at the kill.
    pub name: String,
    /// Mob instance.
    pub mob: String,
    /// Mob tier.
    pub tier: u8,
    /// XP credited.
    pub xp: u32,
}

/// `GET crawler/{principal}`: state (None before the first kill) and the
/// kill history, oldest first.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct CrawlerView {
    /// Current state.
    pub state: Option<CrawlerSummary>,
    /// Kill events.
    pub kills: Vec<KillRecord>,
}

/// Read side of one Crawler or presence cell, over the internal path.
#[derive(Clone)]
pub struct CrawlerCells {
    client: reqwest::Client,
    base: String,
    secret: InternalHttpSecret,
}

impl CrawlerCells {
    /// Reader for the cells at `celld`.
    pub fn new(celld: &CelldOptions) -> Result<Self, String> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| format!("celld client: {error}"))?;
        Ok(Self {
            client,
            base: celld.url.clone(),
            secret: celld.secret.clone(),
        })
    }

    /// celld base URL.
    pub fn url(&self) -> &str {
        &self.base
    }

    async fn get(&self, kind: &str, shard: &str) -> Result<Value, String> {
        let url = format!("{}/{}/{}", self.base, kind, encode_segment(shard));
        let response = self
            .client
            .get(&url)
            .header(CELL_INTERNAL_SECRET_HEADER, self.secret.header_value())
            .send()
            .await
            .map_err(|error| format!("celld unreachable at {url}: {error}"))?;
        let status = response.status();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            return Err(format!("GET {url}: HTTP {status}: {body}"));
        }
        Ok(body)
    }

    /// The durable presence entry of `principal` (`null` when none).
    pub async fn presence(&self, principal: &str) -> Result<Value, String> {
        Ok(self.get(PRESENCE_KIND, principal).await?["current"].take())
    }

    /// State and kill history of `principal`'s cell.
    pub async fn crawler(&self, principal: &str) -> Result<CrawlerView, String> {
        let body = self.get(CRAWLER_KIND, principal).await?;
        serde_json::from_value(body).map_err(|error| format!("crawler {principal}: {error}"))
    }
}

/// Like `distributed_game::celld`: a shard cannot add path segments.
fn encode_segment(segment: &str) -> String {
    let mut encoded = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~' | b':') {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}
