//! The [`Saga`] aggregate: an event-sourced process manager.

use std::collections::BTreeSet;
use std::fmt;
use std::marker::PhantomData;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use super::message::{
    SagaMessage, SagaPhase, SagaSignal, META_ATTEMPT, META_COMMAND_ID, META_PHASE, META_REPLY_TO,
    META_SAGA_ID, META_SAGA_TYPE, META_STEP, SAGA_TIMEOUT,
};
use super::{SagaDefinition, SagaError};
use crate::aggregate::Aggregate;
use crate::entity::{Entity, EventRecord};
use crate::outbox::OutboxMessage;

/// Lifecycle of a saga.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SagaStatus {
    /// No `started` event yet.
    #[default]
    NotStarted,
    /// Executing forward steps.
    Running,
    /// A step failed or timed out; undoing completed steps in reverse.
    Compensating,
    /// Every step succeeded.
    Completed,
    /// A step failed and every applied step was compensated.
    Compensated,
    /// A compensation exhausted its attempts; needs operator attention.
    Stuck,
}

impl SagaStatus {
    /// Completed, compensated or stuck: no further messages are acted on.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            SagaStatus::Completed | SagaStatus::Compensated | SagaStatus::Stuck
        )
    }
}

/// Why the forward path stopped.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SagaFailure {
    pub step: String,
    pub reason: String,
    pub timed_out: bool,
}

/// The command currently awaited.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SagaCursor {
    pub step: u32,
    pub phase: SagaPhase,
    pub attempt: u32,
    pub deadline: SystemTime,
}

/// Persisted saga events. Event names are `saga.<variant>`; payloads are the
/// bitcode-encoded variant.
#[derive(Clone, Debug, Serialize, Deserialize)]
enum SagaEvent<T> {
    Started {
        data: T,
    },
    Dispatched {
        cursor: SagaCursor,
    },
    StepSucceeded {
        step: u32,
        message_id: String,
        output: Vec<u8>,
    },
    StepFailed {
        step: u32,
        message_id: String,
        reason: String,
        timed_out: bool,
    },
    CompensationSucceeded {
        step: u32,
        message_id: String,
    },
    CompensationFailed {
        step: u32,
        message_id: String,
        reason: String,
    },
    Completed,
    Compensated,
    Stuck {
        reason: String,
    },
}

impl<T> SagaEvent<T> {
    fn name(&self) -> &'static str {
        match self {
            SagaEvent::Started { .. } => "saga.started",
            SagaEvent::Dispatched { .. } => "saga.dispatched",
            SagaEvent::StepSucceeded { .. } => "saga.step_succeeded",
            SagaEvent::StepFailed { .. } => "saga.step_failed",
            SagaEvent::CompensationSucceeded { .. } => "saga.compensation_succeeded",
            SagaEvent::CompensationFailed { .. } => "saga.compensation_failed",
            SagaEvent::Completed => "saga.completed",
            SagaEvent::Compensated => "saga.compensated",
            SagaEvent::Stuck { .. } => "saga.stuck",
        }
    }
}

/// How a message was handled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SagaOutcome {
    /// New events were recorded (and possibly commands/timers emitted).
    Applied,
    /// This message id was already processed: nothing to do, ack it.
    Duplicate,
    /// Stale or irrelevant (late reply, superseded timer, terminal saga): ack it.
    Ignored,
}

/// Result of a saga transition: commit the aggregate together with
/// `messages` (commands to participants and the saga's own timers) in one
/// transaction.
#[derive(Debug)]
pub struct SagaTransition {
    pub outcome: SagaOutcome,
    pub messages: Vec<OutboxMessage>,
}

impl SagaTransition {
    fn unchanged(outcome: SagaOutcome) -> Self {
        Self {
            outcome,
            messages: Vec::new(),
        }
    }
}

/// An event-sourced saga / process manager for definition `D`.
///
/// `Saga<D>` is an ordinary [`Aggregate`]: load and commit it with any
/// repository (`repo.aggregate::<Saga<D>>()`). Its methods record events
/// and *return* the outbox messages to commit alongside them; commit both in
/// one batch (`repo.outbox(..).commit(&mut saga)`, or use
/// [`SagaRunner`](super::SagaRunner) which does exactly that).
///
/// Idempotency: every reply/timeout message id that changed state is stored
/// in the saga, so a redelivery is reported as [`SagaOutcome::Duplicate`]
/// and emits nothing. Optimistic stream versions fence concurrent handlers.
pub struct Saga<D: SagaDefinition> {
    entity: Entity,
    data: D::Data,
    status: SagaStatus,
    cursor: Option<SagaCursor>,
    /// Steps that took (or may have taken) effect, oldest first — the
    /// compensation stack.
    applied: Vec<u32>,
    failure: Option<SagaFailure>,
    processed: BTreeSet<String>,
    _definition: PhantomData<fn() -> D>,
}

impl<D: SagaDefinition> Default for Saga<D> {
    fn default() -> Self {
        Self {
            entity: Entity::default(),
            data: D::Data::default(),
            status: SagaStatus::NotStarted,
            cursor: None,
            applied: Vec::new(),
            failure: None,
            processed: BTreeSet::new(),
            _definition: PhantomData,
        }
    }
}

impl<D: SagaDefinition> fmt::Debug for Saga<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Saga")
            .field("type", &D::SAGA_TYPE)
            .field("id", &self.entity.id())
            .field("status", &self.status)
            .field("cursor", &self.cursor)
            .field("applied", &self.applied)
            .field("failure", &self.failure)
            .finish()
    }
}

impl<D: SagaDefinition> Aggregate for Saga<D> {
    type ReplayError = SagaError;

    fn aggregate_type() -> &'static str {
        D::SAGA_TYPE
    }

    fn entity(&self) -> &Entity {
        &self.entity
    }

    fn entity_mut(&mut self) -> &mut Entity {
        &mut self.entity
    }

    fn replay_event(&mut self, event: &EventRecord) -> Result<(), SagaError> {
        let event: SagaEvent<D::Data> = event.decode()?;
        self.apply(event);
        Ok(())
    }
}

impl<D: SagaDefinition> Saga<D> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn id(&self) -> &str {
        self.entity.id()
    }

    pub fn status(&self) -> SagaStatus {
        self.status
    }

    pub fn data(&self) -> &D::Data {
        &self.data
    }

    /// The command currently awaited, if any.
    pub fn cursor(&self) -> Option<&SagaCursor> {
        self.cursor.as_ref()
    }

    /// Why the forward path stopped, once it has.
    pub fn failure(&self) -> Option<&SagaFailure> {
        self.failure.as_ref()
    }

    /// Names of the steps currently on the compensation stack.
    pub fn applied_steps(&self) -> Vec<&'static str> {
        self.applied
            .iter()
            .filter_map(|index| D::steps().get(*index as usize).map(|step| step.name))
            .collect()
    }

    /// Whether `message_id` has already been processed.
    pub fn has_processed(&self, message_id: &str) -> bool {
        self.processed.contains(message_id)
    }

    // ---------------------------------------------------------------------
    // Commands
    // ---------------------------------------------------------------------

    /// Start the saga and dispatch its first step. Starting an already
    /// started saga is a no-op [`SagaOutcome::Duplicate`].
    pub fn start(
        &mut self,
        saga_id: impl Into<String>,
        data: D::Data,
        now: SystemTime,
    ) -> Result<SagaTransition, SagaError> {
        if self.status != SagaStatus::NotStarted {
            return Ok(SagaTransition::unchanged(SagaOutcome::Duplicate));
        }
        let saga_id = saga_id.into();
        if saga_id.is_empty() {
            return Err(SagaError::Definition("saga id must not be empty".into()));
        }
        self.entity.set_id(&saga_id);
        self.entity.set_correlation_id(saga_id);
        let mut messages = Vec::new();
        self.record(SagaEvent::Started { data })?;
        self.advance_from(0, now, &mut messages)?;
        Ok(SagaTransition {
            outcome: SagaOutcome::Applied,
            messages,
        })
    }

    /// React to a reply or timeout. See [`SagaOutcome`] for the no-op cases.
    pub fn handle(
        &mut self,
        message: &SagaMessage,
        now: SystemTime,
    ) -> Result<SagaTransition, SagaError> {
        if self.status == SagaStatus::NotStarted {
            return Err(SagaError::NotStarted);
        }
        if message.saga_type != D::SAGA_TYPE || message.saga_id != self.id() {
            return Err(SagaError::Malformed(format!(
                "message for {}/{} delivered to {}/{}",
                message.saga_type,
                message.saga_id,
                D::SAGA_TYPE,
                self.id()
            )));
        }
        if self.processed.contains(&message.message_id) {
            return Ok(SagaTransition::unchanged(SagaOutcome::Duplicate));
        }
        if self.status.is_terminal() {
            return Ok(SagaTransition::unchanged(SagaOutcome::Ignored));
        }
        let Some(cursor) = self.cursor.clone() else {
            return Ok(SagaTransition::unchanged(SagaOutcome::Ignored));
        };
        let Some(step) = step_index::<D>(&message.step) else {
            return Err(SagaError::Malformed(format!(
                "unknown saga step `{}`",
                message.step
            )));
        };
        if step != cursor.step || message.phase != cursor.phase {
            // A late reply/timer for a step we already moved past.
            return Ok(SagaTransition::unchanged(SagaOutcome::Ignored));
        }
        let current_attempt = message.attempt == cursor.attempt;
        let mut messages = Vec::new();
        let id = message.message_id.clone();
        match (cursor.phase, &message.signal) {
            (SagaPhase::Action, SagaSignal::Succeeded { output }) => {
                self.record(SagaEvent::StepSucceeded {
                    step,
                    message_id: id,
                    output: output.clone(),
                })?;
                self.advance_from(step as usize + 1, now, &mut messages)?;
            }
            (SagaPhase::Action, SagaSignal::Failed { reason }) if current_attempt => {
                self.record(SagaEvent::StepFailed {
                    step,
                    message_id: id,
                    reason: reason.clone(),
                    timed_out: false,
                })?;
                self.compensate_next(now, &mut messages)?;
            }
            (SagaPhase::Action, SagaSignal::TimedOut) if current_attempt => {
                self.record(SagaEvent::StepFailed {
                    step,
                    message_id: id,
                    reason: format!("step `{}` timed out", message.step),
                    timed_out: true,
                })?;
                self.compensate_next(now, &mut messages)?;
            }
            (SagaPhase::Compensation, SagaSignal::Succeeded { .. }) => {
                self.record(SagaEvent::CompensationSucceeded {
                    step,
                    message_id: id,
                })?;
                self.compensate_next(now, &mut messages)?;
            }
            (SagaPhase::Compensation, SagaSignal::Failed { reason }) if current_attempt => {
                self.compensation_failed(step, id, reason.clone(), &cursor, now, &mut messages)?;
            }
            (SagaPhase::Compensation, SagaSignal::TimedOut) if current_attempt => {
                let reason = format!("compensation of `{}` timed out", message.step);
                self.compensation_failed(step, id, reason, &cursor, now, &mut messages)?;
            }
            // Failure/timeout of a superseded attempt.
            _ => return Ok(SagaTransition::unchanged(SagaOutcome::Ignored)),
        }
        Ok(SagaTransition {
            outcome: SagaOutcome::Applied,
            messages,
        })
    }

    // ---------------------------------------------------------------------
    // Transitions
    // ---------------------------------------------------------------------

    /// Dispatch step `next`, or complete when there is none.
    fn advance_from(
        &mut self,
        next: usize,
        now: SystemTime,
        messages: &mut Vec<OutboxMessage>,
    ) -> Result<(), SagaError> {
        let Some(step) = D::steps().get(next) else {
            return self.record(SagaEvent::Completed);
        };
        let command = D::action(step.name, self.id(), &self.data)?;
        self.dispatch(next as u32, SagaPhase::Action, 1, command, now, messages)
    }

    /// Dispatch the next compensation from the stack, or finish compensating.
    fn compensate_next(
        &mut self,
        now: SystemTime,
        messages: &mut Vec<OutboxMessage>,
    ) -> Result<(), SagaError> {
        for &index in self.applied.clone().iter().rev() {
            let step = &D::steps()[index as usize];
            if let Some(command) = D::compensation(step.name, self.id(), &self.data)? {
                return self.dispatch(index, SagaPhase::Compensation, 1, command, now, messages);
            }
        }
        self.record(SagaEvent::Compensated)
    }

    fn compensation_failed(
        &mut self,
        step: u32,
        message_id: String,
        reason: String,
        cursor: &SagaCursor,
        now: SystemTime,
        messages: &mut Vec<OutboxMessage>,
    ) -> Result<(), SagaError> {
        self.record(SagaEvent::CompensationFailed {
            step,
            message_id,
            reason: reason.clone(),
        })?;
        if cursor.attempt >= D::MAX_COMPENSATION_ATTEMPTS {
            return self.record(SagaEvent::Stuck { reason });
        }
        let name = D::steps()[step as usize].name;
        let command = D::compensation(name, self.id(), &self.data)?.ok_or_else(|| {
            SagaError::Definition(format!("compensation for `{name}` disappeared"))
        })?;
        self.dispatch(
            step,
            SagaPhase::Compensation,
            cursor.attempt + 1,
            command,
            now,
            messages,
        )
    }

    /// Record a dispatch and emit the command plus its durable timeout timer.
    fn dispatch(
        &mut self,
        step: u32,
        phase: SagaPhase,
        attempt: u32,
        command: super::SagaCommand,
        now: SystemTime,
        messages: &mut Vec<OutboxMessage>,
    ) -> Result<(), SagaError> {
        let definition = &D::steps()[step as usize];
        let timeout = match phase {
            SagaPhase::Action => definition.timeout,
            SagaPhase::Compensation => definition.compensation_timeout,
        };
        let deadline = now
            .checked_add(timeout)
            .ok_or_else(|| SagaError::Definition("saga step timeout overflows".into()))?;
        let command_id = format!(
            "{}:{}:{}:{}:{}",
            D::SAGA_TYPE,
            self.id(),
            definition.name,
            phase.as_str(),
            attempt
        );
        let saga_id = self.id().to_string();
        let metadata = [
            (META_SAGA_TYPE, D::SAGA_TYPE.to_string()),
            (META_SAGA_ID, saga_id.clone()),
            (META_STEP, definition.name.to_string()),
            (META_PHASE, phase.as_str().to_string()),
            (META_ATTEMPT, attempt.to_string()),
            (META_REPLY_TO, D::REPLY_TO.to_string()),
        ];

        let mut outbound = OutboxMessage::create_to(
            command_id.clone(),
            command.command_type,
            command.destination,
            command.payload,
        )?;
        let mut timer = OutboxMessage::create_to(
            format!("{command_id}:timeout"),
            SAGA_TIMEOUT,
            D::REPLY_TO,
            Vec::new(),
        )?
        .deliver_at(deadline);
        for message in [&mut outbound, &mut timer] {
            for (key, value) in &metadata {
                message.set_meta(*key, value.clone());
            }
            message.set_correlation_id(saga_id.clone());
        }
        timer.set_meta(META_COMMAND_ID, command_id);

        self.record(SagaEvent::Dispatched {
            cursor: SagaCursor {
                step,
                phase,
                attempt,
                deadline,
            },
        })?;
        messages.push(outbound);
        messages.push(timer);
        Ok(())
    }

    // ---------------------------------------------------------------------
    // Event recording / folding
    // ---------------------------------------------------------------------

    fn record(&mut self, event: SagaEvent<D::Data>) -> Result<(), SagaError> {
        self.entity.digest(event.name(), &event)?;
        self.apply(event);
        Ok(())
    }

    /// Pure state fold, shared by live recording and replay.
    fn apply(&mut self, event: SagaEvent<D::Data>) {
        match event {
            SagaEvent::Started { data } => {
                self.data = data;
                self.status = SagaStatus::Running;
            }
            SagaEvent::Dispatched { cursor } => {
                if cursor.phase == SagaPhase::Compensation {
                    self.status = SagaStatus::Compensating;
                    // Steps above this one had no compensation; drop them.
                    if let Some(position) = self.applied.iter().rposition(|s| *s == cursor.step) {
                        self.applied.truncate(position + 1);
                    }
                }
                self.cursor = Some(cursor);
            }
            SagaEvent::StepSucceeded {
                step,
                message_id,
                output,
            } => {
                if let Some(definition) = D::steps().get(step as usize) {
                    D::on_step_succeeded(definition.name, &mut self.data, &output);
                }
                self.applied.push(step);
                self.processed.insert(message_id);
                self.cursor = None;
            }
            SagaEvent::StepFailed {
                step,
                message_id,
                reason,
                timed_out,
            } => {
                // A timed-out step may still have applied: compensate it too.
                if timed_out {
                    self.applied.push(step);
                }
                self.failure = Some(SagaFailure {
                    step: D::steps()
                        .get(step as usize)
                        .map(|s| s.name.to_string())
                        .unwrap_or_default(),
                    reason,
                    timed_out,
                });
                self.processed.insert(message_id);
                self.status = SagaStatus::Compensating;
                self.cursor = None;
            }
            SagaEvent::CompensationSucceeded { step, message_id } => {
                if let Some(position) = self.applied.iter().rposition(|s| *s == step) {
                    self.applied.truncate(position);
                }
                self.processed.insert(message_id);
                self.cursor = None;
            }
            SagaEvent::CompensationFailed { message_id, .. } => {
                self.processed.insert(message_id);
            }
            SagaEvent::Completed => {
                self.status = SagaStatus::Completed;
                self.cursor = None;
            }
            SagaEvent::Compensated => {
                self.status = SagaStatus::Compensated;
                self.applied.clear();
                self.cursor = None;
            }
            SagaEvent::Stuck { reason } => {
                self.status = SagaStatus::Stuck;
                if let Some(failure) = &mut self.failure {
                    failure.reason = format!("{}; {reason}", failure.reason);
                }
                self.cursor = None;
            }
        }
    }
}

fn step_index<D: SagaDefinition>(name: &str) -> Option<u32> {
    D::steps()
        .iter()
        .position(|step| step.name == name)
        .map(|index| index as u32)
}
