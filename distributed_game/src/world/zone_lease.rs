//! Zone ownership as a cell: `ZoneLease:{zone_id}`.
//!
//! The lease of a zone lives in its own aggregate cell (celld in deployment,
//! the in-process reference host in tests) instead of a store private to the
//! zone server, so services learn when a zone server dies:
//!
//! | Command | Effect | Outward event |
//! |---|---|---|
//! | `zone_lease.acquire` | new holder, token + 1, expiry scheduled | `zone.activated` |
//! | `zone_lease.renew` | later expiry, expiry rescheduled | none (no heartbeat events) |
//! | `zone_lease.release` | no holder, expiry cancelled | `zone.released` |
//! | `zone_lease.expire` (schedule only) | no holder, if still expired | `zone.lease_expired` |
//!
//! Outward events carry the lease's post-transition [`ZoneLeaseStatus`]
//! (zone, holder, token, `active` / `released` / `expired`); the
//! [`zone_status`](super::zone_status) read model projects them.
//!
//! Fencing tokens increase on every acquisition and are never reset. Expiry
//! is declared by a schedule ([`distributed::schedule::Schedule`]): the
//! expiry command re-reads the lease and expires it only when the holder's
//! expiry has passed by the host clock, so a renewal that races it never
//! produces a false expiry. All times are the host's clock (`ctx.now()`),
//! never the caller's. The commands declare `Capability::Scheduling` and
//! `Capability::Clock`: they mount on aggregate cells (celld; the reference
//! host in tests) and on any backend that dispatches schedules.
//!
//! Callers must hold the `zone-host` role (an internal identity; the
//! commands are not routed through the public GraphQL edge).

use std::time::{Duration, SystemTime};

use distributed::cell_host::AggregateCell;
use distributed::command::{typed_command, PreparedCommand, Succeeded};
use distributed::microsvc::{
    CausalCommandContext, CausalRouteDependencies, HandlerError, PortableCommand, Routes,
};
use distributed::schedule::{schedule_guard, Capability, Schedule};
use distributed::{sourced, Entity};
use serde::{Deserialize, Serialize};

/// Aggregate type (cell `ZoneLease:{zone_id}`).
pub const ZONE_LEASE: &str = "ZoneLease";
/// Role a zone server's internal session carries.
pub const ZONE_HOST_ROLE: &str = "zone-host";
/// Acquire command.
pub const ACQUIRE: &str = "zone_lease.acquire";
/// Renew command.
pub const RENEW: &str = "zone_lease.renew";
/// Release command.
pub const RELEASE: &str = "zone_lease.release";
/// Alarm-only expiry command.
pub const EXPIRE: &str = "zone_lease.expire";
/// Rejection message prefix when another holder has the lease.
pub const HELD_PREFIX: &str = "held:";
/// Rejection message when a claim is no longer the live one.
pub const LOST: &str = "lost";
/// The expiry schedule's name.
pub const EXPIRY_ALARM: &str = "expiry";

/// [`ZoneLeaseStatus::status`] of a held lease.
pub const STATUS_ACTIVE: &str = "active";
/// [`ZoneLeaseStatus::status`] after `zone.released`.
pub const STATUS_RELEASED: &str = "released";
/// [`ZoneLeaseStatus::status`] after `zone.lease_expired`.
pub const STATUS_EXPIRED: &str = "expired";

/// The lease of one zone.
#[derive(Clone, Debug, Default, Serialize, Deserialize, distributed::Snapshot)]
pub struct ZoneLease {
    entity: Entity,
    holder: Option<String>,
    token: u64,
    expires_at_ms: u64,
    /// Holder of the last claim (kept after release and expiry).
    #[serde(default)]
    last_holder: String,
    /// How the last claim ended (`released` / `expired`; empty while held
    /// or never claimed).
    #[serde(default)]
    ended: String,
}

/// The public state of a lease carried by its outward events (versioned
/// independently of the replay events): one `zone_leases` read-model row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, distributed::DomainState)]
#[domain_state(version = 1)]
pub struct ZoneLeaseStatus {
    /// Zone (the cell's stream id).
    pub zone_id: String,
    /// Holder of the current or, once ended, the last claim.
    pub holder: String,
    /// Fencing token of that claim.
    pub token: u64,
    /// [`STATUS_ACTIVE`], [`STATUS_RELEASED`] or [`STATUS_EXPIRED`].
    pub status: String,
}

impl From<&ZoneLease> for ZoneLeaseStatus {
    fn from(lease: &ZoneLease) -> Self {
        Self {
            zone_id: lease.entity.id().to_string(),
            holder: lease.last_holder.clone(),
            token: lease.token,
            status: if lease.holder.is_some() {
                STATUS_ACTIVE.to_string()
            } else {
                lease.ended.clone()
            },
        }
    }
}

#[sourced(
    entity,
    aggregate_type = "ZoneLease",
    events = "ZoneLeaseEvent",
    domain_state = ZoneLeaseStatus
)]
impl ZoneLease {
    #[event("zone.activated", version = 1, domain)]
    fn activated(&mut self, zone_id: String, holder: String, token: u64, expires_at_ms: u64) {
        if self.entity.id().is_empty() {
            self.entity.set_id(zone_id);
        }
        self.last_holder = holder.clone();
        self.holder = Some(holder);
        self.token = token;
        self.expires_at_ms = expires_at_ms;
        self.ended.clear();
    }

    #[event("zone_lease.renewed", version = 1)]
    fn renewed(&mut self, expires_at_ms: u64) {
        self.expires_at_ms = expires_at_ms;
    }

    #[event("zone.released", version = 1, domain)]
    fn released(&mut self, zone_id: String, holder: String, token: u64) {
        let _ = (zone_id, holder, token);
        self.holder = None;
        self.ended = STATUS_RELEASED.to_string();
    }

    #[event("zone.lease_expired", version = 1, domain)]
    fn lease_expired(&mut self, zone_id: String, holder: String, token: u64) {
        let _ = (zone_id, holder, token);
        self.holder = None;
        self.ended = STATUS_EXPIRED.to_string();
    }
}

impl ZoneLease {
    /// Current holder, if any.
    pub fn holder(&self) -> Option<&str> {
        self.holder.as_deref()
    }

    /// Latest fencing token (kept after release and expiry).
    pub fn token(&self) -> u64 {
        self.token
    }

    /// Expiry of the current holder (epoch ms, cell clock).
    pub fn expires_at_ms(&self) -> u64 {
        self.expires_at_ms
    }
}

/// `zone_lease.acquire` input.
#[derive(Debug, Serialize, Deserialize, distributed::CommandInput)]
pub struct AcquireInput {
    pub zone_id: String,
    pub holder: String,
    pub ttl_ms: u64,
}

/// `zone_lease.renew` input.
#[derive(Debug, Serialize, Deserialize, distributed::CommandInput)]
pub struct RenewInput {
    pub zone_id: String,
    pub holder: String,
    pub token: u64,
    pub ttl_ms: u64,
}

/// `zone_lease.release` input.
#[derive(Debug, Serialize, Deserialize, distributed::CommandInput)]
pub struct ReleaseInput {
    pub zone_id: String,
    pub holder: String,
    pub token: u64,
}

/// `zone_lease.expire` input (scheduled by the lease itself).
#[derive(Debug, Serialize, Deserialize, distributed::CommandInput)]
pub struct ExpireInput {
    pub zone_id: String,
}

/// A held (or released) claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, distributed::CommandOutput)]
pub struct ClaimPayload {
    pub zone_id: String,
    pub holder: String,
    pub token: u64,
    pub expires_at_ms: u64,
}

/// What an expiry schedule did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, distributed::CommandOutput)]
pub struct ExpiryPayload {
    pub expired: bool,
}

/// Map a rejected lease command's message to an
/// [`OwnershipError`](super::OwnershipError).
pub fn ownership_error(message: &str) -> super::OwnershipError {
    let message = message.strip_prefix("rejected: ").unwrap_or(message);
    if let Some(holder) = message.strip_prefix(HELD_PREFIX) {
        super::OwnershipError::Held {
            holder: holder.to_string(),
        }
    } else if message == LOST {
        super::OwnershipError::Lost
    } else {
        super::OwnershipError::Backend(message.to_string())
    }
}

fn rejected(message: impl std::fmt::Display) -> HandlerError {
    HandlerError::Rejected(message.to_string())
}

fn is_zone_host(ctx: &CausalCommandContext<'_, ZoneLease>) -> bool {
    ctx.session().has_role(ZONE_HOST_ROLE)
}

fn now_ms(ctx: &CausalCommandContext<'_, ZoneLease>) -> Result<u64, HandlerError> {
    ctx.now()?
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|now| now.as_millis() as u64)
        .map_err(|_| rejected("host clock is before the Unix epoch"))
}

fn ttl(ttl_ms: u64) -> Result<u64, HandlerError> {
    if ttl_ms == 0 || ttl_ms > 24 * 60 * 60 * 1_000 {
        return Err(rejected("ttl_ms must be within 1..=86400000"));
    }
    Ok(ttl_ms)
}

fn validate(zone_id: &str, holder: &str) -> Result<(), HandlerError> {
    if zone_id.is_empty() || holder.is_empty() {
        return Err(rejected("zone_id and holder must not be empty"));
    }
    Ok(())
}

fn expiry_schedule(zone_id: &str, expires_at_ms: u64) -> Result<Schedule, HandlerError> {
    Schedule::at(
        EXPIRY_ALARM,
        SystemTime::UNIX_EPOCH + Duration::from_millis(expires_at_ms),
        EXPIRE,
        &ExpireInput {
            zone_id: zone_id.to_string(),
        },
    )
    .map_err(rejected)
}

async fn handle_acquire(
    ctx: &CausalCommandContext<'_, ZoneLease>,
    input: AcquireInput,
) -> Result<PreparedCommand<Succeeded<ClaimPayload>>, HandlerError> {
    validate(&input.zone_id, &input.holder)?;
    let ttl = ttl(input.ttl_ms)?;
    let now = now_ms(ctx)?;
    let repo = ctx.repo();
    let mut lease = match repo.get(&input.zone_id).await? {
        Some(lease) => lease,
        None => repo.create(),
    };
    match lease.holder() {
        // The holder re-acquiring its live claim gets it back unchanged.
        Some(holder) if holder == input.holder => {
            let claim = ClaimPayload {
                zone_id: input.zone_id,
                holder: input.holder,
                token: lease.token,
                expires_at_ms: lease.expires_at_ms,
            };
            return repo.commit(lease)?.succeeded(claim);
        }
        // Held until the expiry schedule declares otherwise.
        Some(holder) => return Err(rejected(format!("{HELD_PREFIX}{holder}"))),
        None => {}
    }
    let token = lease.token + 1;
    let expires_at_ms = now + ttl;
    lease
        .activated(
            input.zone_id.clone(),
            input.holder.clone(),
            token,
            expires_at_ms,
        )
        .map_err(rejected)?;
    ctx.schedule(expiry_schedule(&input.zone_id, expires_at_ms)?)?;
    repo.publish_events()
        .commit(lease)?
        .succeeded(ClaimPayload {
            zone_id: input.zone_id,
            holder: input.holder,
            token,
            expires_at_ms,
        })
}

async fn handle_renew(
    ctx: &CausalCommandContext<'_, ZoneLease>,
    input: RenewInput,
) -> Result<PreparedCommand<Succeeded<ClaimPayload>>, HandlerError> {
    validate(&input.zone_id, &input.holder)?;
    let ttl = ttl(input.ttl_ms)?;
    let now = now_ms(ctx)?;
    let repo = ctx.repo();
    let mut lease = repo
        .get(&input.zone_id)
        .await?
        .ok_or_else(|| rejected(LOST))?;
    if lease.holder() != Some(input.holder.as_str()) || lease.token != input.token {
        return Err(rejected(LOST));
    }
    let expires_at_ms = now + ttl;
    lease.renewed(expires_at_ms).map_err(rejected)?;
    ctx.schedule(expiry_schedule(&input.zone_id, expires_at_ms)?)?;
    // Internal state only: no outward event, so no outbox row.
    repo.commit(lease)?.succeeded(ClaimPayload {
        zone_id: input.zone_id,
        holder: input.holder,
        token: input.token,
        expires_at_ms,
    })
}

async fn handle_release(
    ctx: &CausalCommandContext<'_, ZoneLease>,
    input: ReleaseInput,
) -> Result<PreparedCommand<Succeeded<ClaimPayload>>, HandlerError> {
    validate(&input.zone_id, &input.holder)?;
    let repo = ctx.repo();
    let mut lease = repo
        .get(&input.zone_id)
        .await?
        .ok_or_else(|| rejected(LOST))?;
    let claim = ClaimPayload {
        zone_id: input.zone_id.clone(),
        holder: input.holder.clone(),
        token: input.token,
        expires_at_ms: lease.expires_at_ms,
    };
    if lease.holder() != Some(input.holder.as_str()) || lease.token != input.token {
        // Releasing a claim that is already gone is a no-op.
        return repo.commit(lease)?.succeeded(claim);
    }
    lease
        .released(input.zone_id, input.holder, input.token)
        .map_err(rejected)?;
    ctx.cancel_schedule(EXPIRY_ALARM)?;
    repo.publish_events().commit(lease)?.succeeded(claim)
}

async fn handle_expire(
    ctx: &CausalCommandContext<'_, ZoneLease>,
    input: ExpireInput,
) -> Result<PreparedCommand<Succeeded<ExpiryPayload>>, HandlerError> {
    let now = now_ms(ctx)?;
    let repo = ctx.repo();
    let Some(mut lease) = repo.get(&input.zone_id).await? else {
        return Err(rejected("no lease"));
    };
    let expired = match lease.holder.clone() {
        Some(holder) if now >= lease.expires_at_ms => {
            let token = lease.token;
            lease
                .lease_expired(input.zone_id, holder, token)
                .map_err(rejected)?;
            true
        }
        // Renewed (or released) after this schedule was read: not expired.
        _ => false,
    };
    if expired {
        repo.publish_events()
            .commit(lease)?
            .succeeded(ExpiryPayload { expired })
    } else {
        repo.commit(lease)?.succeeded(ExpiryPayload { expired })
    }
}

macro_rules! lease_command {
    ($name:ident, $command:expr, $input:ty, $output:ty, $guard:expr, $handler:expr) => {
        struct $name;
        impl<D> PortableCommand<D> for $name
        where
            D: CausalRouteDependencies<Aggregate = ZoneLease> + Send + Sync + 'static,
        {
            fn install(self, routes: Routes<D>) -> Routes<D> {
                routes
                    .typed_command(typed_command::<$input, Succeeded<$output>>($command))
                    .requires(Capability::Scheduling)
                    .requires(Capability::Clock)
                    .guarded($guard, $handler)
            }
        }
    };
}

lease_command!(
    Acquire,
    ACQUIRE,
    AcquireInput,
    ClaimPayload,
    is_zone_host,
    handle_acquire
);
lease_command!(
    Renew,
    RENEW,
    RenewInput,
    ClaimPayload,
    is_zone_host,
    handle_renew
);
lease_command!(
    Release,
    RELEASE,
    ReleaseInput,
    ClaimPayload,
    is_zone_host,
    handle_release
);
lease_command!(
    Expire,
    EXPIRE,
    ExpireInput,
    ExpiryPayload,
    schedule_guard::<ZoneLease>,
    handle_expire
);

/// Mount every zone-lease command on a `ZoneLease` cell.
pub fn mount_zone_lease(cell: AggregateCell<ZoneLease>) -> AggregateCell<ZoneLease> {
    cell.mount(Acquire)
        .mount(Renew)
        .mount(Release)
        .mount(Expire)
}
