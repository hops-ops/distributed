//! Saga - event-sourced process managers for cross-cell workflows.
//!
//! A cell has one atomic boundary. Anything that must change state in two
//! cells (moving a player between zones, transferring a corpse's inventory,
//! settling an encounter across participants) is a **saga**: a sequence of
//! steps, each a command to one cell, with a compensating command per step
//! that undoes it if a later step fails.
//!
//! ## Model
//!
//! - A [`SagaDefinition`] is a pure description: ordered [`SagaStep`]s, the
//!   [`SagaCommand`] for each step's action and (optional) compensation, and
//!   how to fold reply payloads into the saga's `Data`.
//! - [`Saga<D>`] is an ordinary event-sourced [`Aggregate`](crate::Aggregate)
//!   (one stream per saga instance). Its events record every dispatch, reply,
//!   failure and compensation, so a saga survives restarts and its history is
//!   auditable.
//! - Every transition returns the outbox messages to commit **with** the
//!   saga's events: the participant command plus a durable timeout timer
//!   (an [`OutboxMessage`](crate::OutboxMessage) with
//!   [`deliver_at`](crate::OutboxMessage::deliver_at())). A crash can never
//!   lose a dispatched command or its timer.
//! - Participants reply with [`SagaReply`] (`succeeded` / `failed`), routed to
//!   [`SagaDefinition::REPLY_TO`]. Replies and timers arrive as
//!   [`SagaMessage`]s and are fed to [`Saga::handle`] / [`SagaRunner::handle`].
//!
//! ## Semantics
//!
//! - **Forward**: step *n* succeeds → step *n+1* is dispatched; after the last
//!   step the saga is [`Completed`](SagaStatus::Completed).
//! - **Fan-out**: a step whose [`SagaDefinition::action`] returns
//!   [`SagaAction::FanOut`] dispatches up to [`MAX_SAGA_FAN_OUT`] commands at
//!   once, each with its own timer and a command id per `(step, member)`. It
//!   succeeds when every member succeeds. It settles only after every member
//!   replied or timed out; on any failure the members that applied (or timed
//!   out, outcome unknown) are compensated one by one, highest index first,
//!   and members that rejected are not. Late replies are ignored.
//! - **Rejection**: a `failed` reply means "definitely not applied"; the saga
//!   compensates the previously applied steps in reverse order.
//! - **Timeout**: the outcome is unknown, so the timed-out step is
//!   compensated *as well*. Compensations therefore must be idempotent and
//!   must tolerate arriving before (or instead of) their action — the
//!   participant records a tombstone keyed by saga id so a late action is
//!   rejected. The worked example in `tests/saga_transfer` shows this.
//! - **Compensation retries**: a failed/timed-out compensation is
//!   re-dispatched (new attempt, new timer) up to
//!   [`SagaDefinition::MAX_COMPENSATION_ATTEMPTS`], then the saga is
//!   [`Stuck`](SagaStatus::Stuck) for an operator.
//! - **Idempotency**: message ids that changed state are stored in the saga
//!   ([`SagaOutcome::Duplicate`] on redelivery); late replies and superseded
//!   timers are [`SagaOutcome::Ignored`]. Command ids are deterministic
//!   (`type:id:step:phase:attempt`, plus `:member` on fan-out steps), so
//!   participants can dedupe with consumer
//!   inbox receipts keyed by the command id, and reply ids derive from them.
//! - **Concurrency**: the saga stream's optimistic version is the fence;
//!   [`SagaRunner`] reloads and retries on `ConcurrentWrite`.
//!
//! ## Example
//!
//! ```ignore
//! struct Transfer;
//! impl SagaDefinition for Transfer {
//!     const SAGA_TYPE: &'static str = "bank.transfer";
//!     const REPLY_TO: &'static str = "transfer-saga";
//!     type Data = TransferData;
//!     fn steps() -> &'static [SagaStep] {
//!         const STEPS: &[SagaStep] = &[
//!             SagaStep::new("debit", Duration::from_secs(10)),
//!             SagaStep::new("credit", Duration::from_secs(10)),
//!         ];
//!         STEPS
//!     }
//!     fn action(step: &str, id: &str, d: &TransferData) -> Result<SagaAction, SagaError> {
//!         Ok(match step {
//!             "debit" => SagaCommand::json(&d.from_cell, "wallet.debit", &Move::new(id, d))?.into(),
//!             _ => SagaCommand::json(&d.to_cell, "wallet.credit", &Move::new(id, d))?.into(),
//!         })
//!     }
//!     fn compensation(step: &str, member: usize, id: &str, d: &TransferData)
//!         -> Result<Option<SagaCommand>, SagaError> { /* refund / reverse */ }
//! }
//!
//! let runner = SagaRunner::<_, Transfer>::new(repo);
//! runner.start("t-1", data).await?;                 // commits saga + debit cmd + timer
//! runner.handle(&SagaMessage::from_bus(&msg)?).await?; // replies and timers
//! ```
//!
//! See `docs/coordination-primitives.md` for the full walkthrough.

mod definition;
mod error;
#[cfg(test)]
mod fan_out_tests;
mod message;
mod runner;
mod state;

pub use definition::{SagaAction, SagaCommand, SagaDefinition, SagaStep, MAX_SAGA_FAN_OUT};
pub use error::SagaError;
pub use message::{
    SagaMessage, SagaPhase, SagaReply, SagaSignal, META_ATTEMPT, META_COMMAND_ID, META_MEMBER,
    META_OUTCOME, META_PHASE, META_REPLY_TO, META_SAGA_ID, META_SAGA_TYPE, META_STEP, SAGA_REPLY,
    SAGA_TIMEOUT,
};
pub use runner::SagaRunner;
pub use state::{
    Saga, SagaCursor, SagaFailure, SagaFanOut, SagaOutcome, SagaStatus, SagaTransition,
};
