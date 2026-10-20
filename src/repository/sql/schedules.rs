//! Schedule rows of a shared SQL repository (`aggregate_schedules`): one
//! table for every aggregate, keyed by the owning stream and the schedule
//! name, written in the command's transaction. Cells keep their own
//! single-owner `cell_alarms` table.

use super::{SqlExecutor, SqlRow, Statement};
use crate::repository::RepositoryError;
use crate::schedule::{ArmedSchedule, ScheduleError, ScheduleWrite, MAX_ARMED_SCHEDULES};
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
                            .sql(", 1, 0, 1) ON CONFLICT (aggregate_type, aggregate_id, name) DO UPDATE SET command = excluded.command, input = excluded.input, due_at_ms = excluded.due_at_ms, generation = aggregate_schedules.generation + 1, attempts = 0, armed = 1"),
                    )
                    .await?;
            }
            ScheduleWrite::Cancel(name) => {
                executor
                    .execute(
                        Statement::new(
                            "UPDATE aggregate_schedules SET armed = 0, attempts = 0 WHERE aggregate_type = ",
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
    rows.iter()
        .map(|row| {
            let input = row.text("input")?;
            Ok(ArmedSchedule {
                name: row.text("name")?,
                command: row.text("command")?,
                input: serde_json::from_str(&input).map_err(|error| {
                    RepositoryError::Model(format!("schedule input is invalid JSON: {error}"))
                })?,
                due_at_ms: row.integer("due_at_ms")?,
                generation: u64::try_from(row.integer("generation")?).map_err(|_| {
                    RepositoryError::Model("schedule generation is negative".into())
                })?,
                attempts: u32::try_from(row.integer("attempts")?).unwrap_or(u32::MAX),
            })
        })
        .collect()
}
