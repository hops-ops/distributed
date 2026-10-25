//! What a zone running as a celld object is doing, as a cell:
//! `ZoneActivity:{zone_id}` (cell kind [`ZONE_ACTIVITY_KIND`]).
//!
//! A zone hosted by [`celld_zone`](crate::celld_zone) reports its own
//! lifecycle — woke up (`active`, `restored` when it came back from a
//! snapshot), emptied (`dormant`), slept (`hibernated`) — as facts
//! ([`RecordActivity`], command [`RECORD`]) through its fact sink, so the
//! transitions are durable and exactly once like any other zone fact.
//!
//! | Command | Effect | Outward event |
//! |---|---|---|
//! | `zone_activity.record` | the reported status, if emitted after the one stored | `zone.activity_changed` |
//!
//! Facts can settle out of order (retries, concurrent commits): a report
//! emitted before the stored one ([`FactStamp::emission_order`]) is
//! ignored. The outward event carries the post-transition
//! [`ZoneActivityStatus`]; the [`zone_status`](super::zone_status) read model
//! projects it into `zone_activity` rows (minimaps, dashboards).
//!
//! Only the zone itself may report: the fact's principal must be the
//! zone's system principal [`zone_principal`]`(zone)`, and the stamp's zone
//! is the shard.
//!
//! `node` and `isolate` are debug details (which celld node and V8 isolate
//! ran the activation, when known); nothing may route by them.

use distributed::cell_host::AggregateCell;
use distributed::command::{typed_command, PreparedCommand, Succeeded};
use distributed::microsvc::{
    CausalCommandContext, CausalRouteDependencies, HandlerError, PortableCommand, Routes,
};
use distributed::{sourced, Entity};
use serde::{Deserialize, Serialize};

use crate::cell::session_stamp;
use crate::FactStamp;

/// Aggregate type (cell `ZoneActivity:{zone_id}`).
pub const ZONE_ACTIVITY: &str = "ZoneActivity";
/// Cell kind (URL segment / Durable Object routing) of zone-activity cells.
pub const ZONE_ACTIVITY_KIND: &str = "zone-activity";
/// The one command (a zone fact).
pub const RECORD: &str = "zone_activity.record";

/// Status: ticking with players (or just woke up).
pub const ACTIVITY_ACTIVE: &str = "active";
/// Status: empty, still in memory, snapshot written.
pub const ACTIVITY_DORMANT: &str = "dormant";
/// Status: asleep; the next join restores the snapshot.
pub const ACTIVITY_HIBERNATED: &str = "hibernated";

/// The system principal a zone reports its own activity under.
pub fn zone_principal(zone_id: &str) -> String {
    format!("zone:{zone_id}")
}

/// One zone's activity.
#[derive(Clone, Debug, Default, Serialize, Deserialize, distributed::Snapshot)]
pub struct ZoneActivity {
    entity: Entity,
    status: String,
    activation: String,
    restored: bool,
    players: u32,
    node: String,
    isolate: String,
    activations: u64,
    changed_at_ms: u64,
    /// Ordering key of the report that set the current status.
    last: Option<StampKey>,
}

/// The emission-order key of the last applied report.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StampKey {
    /// Zone.
    pub zone_id: String,
    /// Activation epoch.
    pub activation: u64,
    /// Tick.
    pub tick: u64,
    /// Emission wall time.
    pub emitted_at_ms: u64,
    /// Emission sequence.
    pub seq: u64,
}

impl StampKey {
    fn of(stamp: &FactStamp) -> Self {
        Self {
            zone_id: stamp.zone_id.clone(),
            activation: stamp.activation,
            tick: stamp.tick,
            emitted_at_ms: stamp.emitted_at_ms,
            seq: stamp.seq,
        }
    }

    fn stamp(&self) -> FactStamp {
        FactStamp {
            zone_id: self.zone_id.clone(),
            activation: self.activation,
            tick: self.tick,
            emitted_at_ms: self.emitted_at_ms,
            seq: self.seq,
            ..FactStamp::default()
        }
    }
}

/// The public state carried by `zone.activity_changed`: one
/// `zone_activity` read-model row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, distributed::DomainState)]
#[domain_state(version = 1)]
pub struct ZoneActivityStatus {
    /// Zone (the cell's stream id).
    pub zone_id: String,
    /// [`ACTIVITY_ACTIVE`], [`ACTIVITY_DORMANT`] or [`ACTIVITY_HIBERNATED`].
    pub status: String,
    /// Id of the object activation that reported it.
    pub activation: String,
    /// That activation restored a snapshot.
    pub restored: bool,
    /// Players in the zone when reported.
    pub players: u32,
    /// celld node running the activation (debug; empty when unknown).
    pub node: String,
    /// V8 isolate running the activation (debug).
    pub isolate: String,
    /// Activations seen so far.
    pub activations: u64,
    /// When the zone reported it (zone clock, Unix ms).
    pub changed_at_ms: u64,
}

impl From<&ZoneActivity> for ZoneActivityStatus {
    fn from(activity: &ZoneActivity) -> Self {
        Self {
            zone_id: activity.entity.id().to_string(),
            status: activity.status.clone(),
            activation: activity.activation.clone(),
            restored: activity.restored,
            players: activity.players,
            node: activity.node.clone(),
            isolate: activity.isolate.clone(),
            activations: activity.activations,
            changed_at_ms: activity.changed_at_ms,
        }
    }
}

#[sourced(
    entity,
    aggregate_type = "ZoneActivity",
    events = "ZoneActivityEvent",
    domain_state = ZoneActivityStatus
)]
impl ZoneActivity {
    #[event("zone.activity_changed", version = 1, domain)]
    fn changed(&mut self, zone_id: String, report: RecordActivity, stamp: StampKey) {
        if self.entity.id().is_empty() {
            self.entity.set_id(zone_id);
        }
        if report.activation != self.activation {
            self.activations += 1;
        }
        self.status = report.status;
        self.activation = report.activation;
        self.restored = report.restored;
        self.players = report.players;
        self.node = report.node;
        self.isolate = report.isolate;
        self.changed_at_ms = stamp.emitted_at_ms;
        self.last = Some(stamp);
    }
}

impl ZoneActivity {
    /// Current status (empty before the first report).
    pub fn status(&self) -> &str {
        &self.status
    }

    /// The public row.
    pub fn row(&self) -> ZoneActivityStatus {
        self.into()
    }
}

/// `zone_activity.record` input: what the zone reports about itself.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    distributed::CommandInput,
    crate::FactCommand,
)]
#[fact(command = "zone_activity.record")]
pub struct RecordActivity {
    /// [`ACTIVITY_ACTIVE`], [`ACTIVITY_DORMANT`] or [`ACTIVITY_HIBERNATED`].
    pub status: String,
    /// Object activation id.
    pub activation: String,
    /// The activation restored a snapshot.
    pub restored: bool,
    /// Players present.
    pub players: u32,
    /// celld node (debug; empty when unknown).
    pub node: String,
    /// V8 isolate (debug).
    pub isolate: String,
}

/// The applied (or ignored) report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, distributed::CommandOutput)]
pub struct ActivityApplied {
    /// False when a later-emitted report already set the status.
    pub applied: bool,
    /// The status after the command.
    pub status: String,
}

fn rejected(message: impl std::fmt::Display) -> HandlerError {
    HandlerError::Rejected(message.to_string())
}

async fn handle_record(
    ctx: &CausalCommandContext<'_, ZoneActivity>,
    input: RecordActivity,
) -> Result<PreparedCommand<Succeeded<ActivityApplied>>, HandlerError> {
    if ![ACTIVITY_ACTIVE, ACTIVITY_DORMANT, ACTIVITY_HIBERNATED].contains(&input.status.as_str()) {
        return Err(rejected(format!("unknown activity `{}`", input.status)));
    }
    let stamp = session_stamp(ctx.session())
        .ok_or_else(|| rejected("activity reports are zone facts and need a fact stamp"))?;
    if ctx.user_id()? != zone_principal(&stamp.zone_id) {
        return Err(rejected("only the zone itself reports its activity"));
    }
    let repo = ctx.repo();
    let mut activity = match repo.get(&stamp.zone_id).await? {
        Some(activity) => activity,
        None => repo.create(),
    };
    let newer = activity
        .last
        .as_ref()
        .is_none_or(|last| last.stamp().emission_order(&stamp) == std::cmp::Ordering::Less);
    if !newer {
        let status = activity.status.clone();
        return repo.commit(activity)?.succeeded(ActivityApplied {
            applied: false,
            status,
        });
    }
    let status = input.status.clone();
    activity
        .changed(stamp.zone_id.clone(), input, StampKey::of(&stamp))
        .map_err(rejected)?;
    repo.publish_events()
        .commit(activity)?
        .succeeded(ActivityApplied {
            applied: true,
            status,
        })
}

struct Record;
impl<D> PortableCommand<D> for Record
where
    D: CausalRouteDependencies<Aggregate = ZoneActivity> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(typed_command::<RecordActivity, Succeeded<ActivityApplied>>(
                RECORD,
            ))
            .handle(handle_record)
    }
}

/// Mount the zone-activity command on a `ZoneActivity` cell.
pub fn mount_zone_activity(cell: AggregateCell<ZoneActivity>) -> AggregateCell<ZoneActivity> {
    cell.mount(Record)
}
