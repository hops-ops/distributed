//! Schedule rows of a shared SQL repository (`aggregate_schedules`): one
//! table for every aggregate, keyed by the owning stream and the schedule
//! name, written in the command's transaction. Cells keep their own
//! single-owner `cell_alarms` table.

use super::{SqlExecutor, SqlRow, Statement};
use crate::repository::RepositoryError;
use crate::schedule::{
    ArmedSchedule, ScheduleError, ScheduleSettlement, ScheduleWrite, MAX_ARMED_SCHEDULES,
};
use crate::StreamIdentity;

/// Apply one command's resolved schedule writes for `owner`.
pub(crate) async fn apply(
    executor: &mut impl SqlExecutor,
    owner: &StreamIdentity,
    writes: &[ScheduleWrite],
) -> Result<(), RepositoryError> {
    for write in writes {
        match write {
            ScheduleWrite::Arm(schedule) => {
                let input = schedule.input_json()?;
                executor
                    .execute(
                        Statement::new("INSERT INTO aggregate_schedules (aggregate_type, aggregate_id, name, command, input, due_at_ms, generation, attempts, armed) VALUES (")
                            .bind(owner.aggregate_type().into())
                            .sql(", ")
                            .bind(owner.aggregate_id().into())
                            .sql(", ")
                            .bind(schedule.name().into())
                            .sql(", ")
                            .bind(schedule.command().into())
                            .sql(", ")
                            .bind(input.as_str().into())
                            .sql(", ")
                            .bind(schedule.due_at_ms()?.into())
                            .sql(", 1, 0, 1) ON CONFLICT (aggregate_type, aggregate_id, name) DO UPDATE SET command = excluded.command, input = excluded.input, due_at_ms = excluded.due_at_ms, generation = aggregate_schedules.generation + 1, attempts = 0, armed = 1, claimed_until_ms = NULL"),
                    )
                    .await?;
            }
            ScheduleWrite::Cancel(name) => {
                executor
                    .execute(
                        Statement::new(
                            "UPDATE aggregate_schedules SET armed = 0, attempts = 0, claimed_until_ms = NULL WHERE aggregate_type = ",
                        )
                        .bind(owner.aggregate_type().into())
                        .sql(" AND aggregate_id = ")
                        .bind(owner.aggregate_id().into())
                        .sql(" AND name = ")
                        .bind(name.as_str().into()),
                    )
                    .await?;
            }
        }
    }
    let rows = executor
        .query(
            Statement::new(
                "SELECT COUNT(*) AS armed FROM aggregate_schedules WHERE armed = 1 AND aggregate_type = ",
            )
            .bind(owner.aggregate_type().into())
            .sql(" AND aggregate_id = ")
            .bind(owner.aggregate_id().into()),
        )
        .await?;
    let armed = rows
        .first()
        .map(|row| row.integer("armed"))
        .transpose()?
        .unwrap_or(0);
    if armed > MAX_ARMED_SCHEDULES as i64 {
        return Err(ScheduleError::TooManyArmed.into());
    }
    Ok(())
}

/// Armed schedules of `owner`, earliest first.
pub(crate) async fn armed(
    executor: &mut impl SqlExecutor,
    owner: &StreamIdentity,
) -> Result<Vec<ArmedSchedule>, RepositoryError> {
    let rows = executor
        .query(
            Statement::new(
                "SELECT name, command, input, due_at_ms, generation, attempts FROM aggregate_schedules WHERE armed = 1 AND aggregate_type = ",
            )
            .bind(owner.aggregate_type().into())
            .sql(" AND aggregate_id = ")
            .bind(owner.aggregate_id().into())
            .sql(" ORDER BY due_at_ms, name"),
        )
        .await?;
    rows.iter().map(armed_from_row).collect()
}

fn armed_from_row(row: &impl SqlRow) -> Result<ArmedSchedule, RepositoryError> {
    let input = row.text("input")?;
    Ok(ArmedSchedule {
        name: row.text("name")?,
        command: row.text("command")?,
        input: serde_json::from_str(&input).map_err(|error| {
            RepositoryError::Model(format!("schedule input is invalid JSON: {error}"))
        })?,
        due_at_ms: row.integer("due_at_ms")?,
        generation: u64::try_from(row.integer("generation")?)
            .map_err(|_| RepositoryError::Model("schedule generation is negative".into()))?,
        attempts: u32::try_from(row.integer("attempts")?).unwrap_or(u32::MAX),
    })
}

/// Claim up to `limit` armed schedules due at `now_ms` that no live claim
/// holds, until `claim_until_ms`, in one statement: concurrent workers get
/// disjoint rows (`FOR UPDATE SKIP LOCKED` on Postgres; SQLite serializes
/// the write). Returned earliest first, with their owners.
#[cfg_attr(not(feature = "graphql"), allow(dead_code))]
pub(crate) async fn claim_due<E: SqlExecutor>(
    executor: &mut E,
    now_ms: i64,
    claim_until_ms: i64,
    limit: usize,
) -> Result<Vec<(StreamIdentity, ArmedSchedule)>, RepositoryError> {
    let limit = i64::try_from(limit).unwrap_or(i64::MAX);
    let rows = executor
        .query(
            Statement::new("UPDATE aggregate_schedules SET claimed_until_ms = ")
                .bind(claim_until_ms.into())
                .sql(" WHERE (aggregate_type, aggregate_id, name) IN (SELECT aggregate_type, aggregate_id, name FROM aggregate_schedules WHERE armed = 1 AND due_at_ms <= ")
                .bind(now_ms.into())
                .sql(" AND (claimed_until_ms IS NULL OR claimed_until_ms <= ")
                .bind(now_ms.into())
                .sql(") ORDER BY due_at_ms, aggregate_type, aggregate_id, name LIMIT ")
                .bind(limit.into())
                .sql(E::SCHEDULE_CLAIM_LOCK_SUFFIX)
                .sql(") RETURNING aggregate_type, aggregate_id, name, command, input, due_at_ms, generation, attempts"),
        )
        .await?;
    let mut claimed = rows
        .iter()
        .map(|row| {
            let owner =
                StreamIdentity::new(row.text("aggregate_type")?, row.text("aggregate_id")?)?;
            Ok((owner, armed_from_row(row)?))
        })
        .collect::<Result<Vec<_>, RepositoryError>>()?;
    claimed.sort_by(|(a_owner, a), (b_owner, b)| {
        (a.due_at_ms, a_owner.storage_key(), &a.name).cmp(&(
            b.due_at_ms,
            b_owner.storage_key(),
            &b.name,
        ))
    });
    Ok(claimed)
}

/// Settle one fired arm. Only the generation that fired is touched: a
/// handler that re-armed its own name produced a newer generation, which
/// stays armed; a stale or already-settled arm is a no-op.
#[cfg_attr(not(feature = "graphql"), allow(dead_code))]
pub(crate) async fn settle(
    executor: &mut impl SqlExecutor,
    owner: &StreamIdentity,
    name: &str,
    generation: u64,
    settlement: &ScheduleSettlement,
) -> Result<(), RepositoryError> {
    let generation = i64::try_from(generation)
        .map_err(|_| RepositoryError::Model("schedule generation exceeds SQL storage".into()))?;
    let statement = match settlement {
        ScheduleSettlement::Done => Statement::new(
            "UPDATE aggregate_schedules SET armed = 0, attempts = 0, claimed_until_ms = NULL WHERE aggregate_type = ",
        ),
        ScheduleSettlement::Retry { due_at_ms } => Statement::new(
            "UPDATE aggregate_schedules SET attempts = attempts + 1, claimed_until_ms = NULL, due_at_ms = ",
        )
        .bind((*due_at_ms).into())
        .sql(" WHERE aggregate_type = "),
    };
    executor
        .execute(
            statement
                .bind(owner.aggregate_type().into())
                .sql(" AND aggregate_id = ")
                .bind(owner.aggregate_id().into())
                .sql(" AND name = ")
                .bind(name.into())
                .sql(" AND generation = ")
                .bind(generation.into())
                .sql(" AND armed = 1"),
        )
        .await?;
    Ok(())
}
