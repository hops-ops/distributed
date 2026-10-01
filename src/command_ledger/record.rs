use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::projection_protocol::{ResolvedProjectionObligation, SameTransactionProjectionEvidence};

use super::{
    ids::COMMAND_REPLAY_VERSION, state::validate_projection_obligation_semantics, AttemptFence,
    AttemptToken, CanonicalInputHash, CausationId, CommandAttempt, CommandCompletion,
    CommandContractFingerprint, CommandLedgerError, CommandLedgerKey, CommandLedgerState,
    CommandLookup, CommandLookupScope, CommandReplay, CommandReservation,
    ExternalCommandCompletion, ExternalDispatchBinding, ReservationOutcome,
};

/// Storage-neutral row representation shared by built-in adapters.
#[derive(Clone, Debug)]
pub(crate) struct CommandLedgerRecord {
    pub(crate) key: CommandLedgerKey,
    pub(crate) command_name: String,
    pub(crate) contract_fingerprint: CommandContractFingerprint,
    pub(crate) input_hash: CanonicalInputHash,
    pub(crate) state: CommandLedgerState,
    pub(crate) causation_id: CausationId,
    pub(crate) attempt_token: Option<AttemptToken>,
    pub(crate) attempt_number: u64,
    pub(crate) external_binding: Option<ExternalDispatchBinding>,
    pub(crate) lease_expires_at: Option<SystemTime>,
    pub(crate) outcome_json: Option<String>,
    #[allow(dead_code)]
    pub(crate) created_at: SystemTime,
    pub(crate) updated_at: SystemTime,
    pub(crate) completed_at: Option<SystemTime>,
    pub(crate) retention_expires_at: SystemTime,
    pub(crate) compacted_at: Option<SystemTime>,
}

impl CommandLedgerRecord {
    pub(crate) fn initial(
        reservation: &CommandReservation,
        now: SystemTime,
    ) -> Result<Self, CommandLedgerError> {
        Ok(Self {
            key: reservation.key.clone(),
            command_name: reservation.command_name.clone(),
            contract_fingerprint: reservation.contract_fingerprint,
            input_hash: reservation.input_hash,
            state: CommandLedgerState::InProgress,
            causation_id: reservation.candidate_causation.clone(),
            attempt_token: Some(reservation.candidate_attempt.clone()),
            attempt_number: 1,
            external_binding: reservation.external_binding.clone(),
            lease_expires_at: Some(checked_deadline(now, reservation.lease, "attempt lease")?),
            outcome_json: None,
            created_at: now,
            updated_at: now,
            completed_at: None,
            retention_expires_at: checked_deadline(
                now,
                reservation.retention,
                "command retention",
            )?,
            compacted_at: None,
        })
    }

    /// Stable opaque key used by a cell host's private SQLite table.
    pub(crate) fn durable_cell_key(&self) -> String {
        let material = format!(
            "{}\0{}\0{}",
            self.key.service_id(),
            self.key.principal_partition(),
            self.key.command_id()
        );
        format!("v1.{}", URL_SAFE_NO_PAD.encode(material))
    }

    /// Versioned cell-storage representation. This is deliberately separate
    /// from the public command receipt and retains the complete fenced row.
    pub(crate) fn durable_cell_json(&self) -> Result<String, CommandLedgerError> {
        self.validate_stored_shape()?;
        let wire = DurableCellCommandRecordV1 {
            version: 1,
            service_id: self.key.service_id().to_string(),
            principal_partition: self.key.principal_partition().to_string(),
            command_id: self.key.command_id().to_string(),
            command_name: self.command_name.clone(),
            external_binding: self
                .external_binding
                .as_ref()
                .map(ExternalDispatchBinding::to_storage)
                .transpose()?,
            contract_fingerprint: self.contract_fingerprint.as_bytes().to_vec(),
            input_hash: self.input_hash.as_bytes().to_vec(),
            state: self.state.as_str().to_string(),
            causation_id: self.causation_id.as_str().to_string(),
            attempt_token: self
                .attempt_token
                .as_ref()
                .map(|token| token.as_str().to_string()),
            attempt_number: self.attempt_number,
            lease_expires_at_ms: self
                .lease_expires_at
                .map(system_time_to_unix_millis)
                .transpose()?,
            outcome_json: self.outcome_json.clone(),
            created_at_ms: system_time_to_unix_millis(self.created_at)?,
            updated_at_ms: system_time_to_unix_millis(self.updated_at)?,
            completed_at_ms: self
                .completed_at
                .map(system_time_to_unix_millis)
                .transpose()?,
            retention_expires_at_ms: system_time_to_unix_millis(self.retention_expires_at)?,
            compacted_at_ms: self
                .compacted_at
                .map(system_time_to_unix_millis)
                .transpose()?,
        };
        serde_json::to_string(&wire).map_err(|error| {
            CommandLedgerError::Corrupt(format!(
                "cell command ledger row could not be encoded: {error}"
            ))
        })
    }

    pub(crate) fn from_durable_cell_json(body: &str) -> Result<Self, CommandLedgerError> {
        let wire: DurableCellCommandRecordV1 = serde_json::from_str(body).map_err(|error| {
            CommandLedgerError::Corrupt(format!("cell command ledger row is invalid JSON: {error}"))
        })?;
        if wire.version != 1 {
            return Err(CommandLedgerError::Corrupt(format!(
                "cell command ledger row version `{}` is unsupported",
                wire.version
            )));
        }
        let key = CommandLedgerKey::new(
            wire.service_id,
            super::PrincipalPartitionId::new(wire.principal_partition)?,
            super::CommandId::parse(wire.command_id)?,
        )?;
        let record = Self {
            key,
            command_name: wire.command_name,
            external_binding: wire
                .external_binding
                .as_deref()
                .map(ExternalDispatchBinding::from_storage)
                .transpose()?,
            contract_fingerprint: CommandContractFingerprint::try_from_slice(
                &wire.contract_fingerprint,
            )?,
            input_hash: CanonicalInputHash::try_from_slice(&wire.input_hash)?,
            state: CommandLedgerState::parse(&wire.state)?,
            causation_id: CausationId::parse_stored(wire.causation_id)?,
            attempt_token: wire
                .attempt_token
                .map(AttemptToken::parse_stored)
                .transpose()?,
            attempt_number: wire.attempt_number,
            lease_expires_at: wire
                .lease_expires_at_ms
                .map(system_time_from_unix_millis)
                .transpose()?,
            outcome_json: wire.outcome_json,
            created_at: system_time_from_unix_millis(wire.created_at_ms)?,
            updated_at: system_time_from_unix_millis(wire.updated_at_ms)?,
            completed_at: wire
                .completed_at_ms
                .map(system_time_from_unix_millis)
                .transpose()?,
            retention_expires_at: system_time_from_unix_millis(wire.retention_expires_at_ms)?,
            compacted_at: wire
                .compacted_at_ms
                .map(system_time_from_unix_millis)
                .transpose()?,
        };
        record.validate_stored_shape()?;
        if record.state.is_replayable() {
            record.replay()?;
        }
        Ok(record)
    }

    pub(crate) fn acquired_attempt(&self) -> Result<CommandAttempt, CommandLedgerError> {
        let token = self.attempt_token.as_ref().ok_or_else(|| {
            CommandLedgerError::Corrupt(format!(
                "in-progress command `{}` has no attempt token",
                self.key.command_id()
            ))
        })?;
        Ok(CommandAttempt {
            key: self.key.clone(),
            contract_fingerprint: self.contract_fingerprint,
            input_hash: self.input_hash,
            causation_id: self.causation_id.clone(),
            attempt_token: token.clone(),
            attempt_number: self.attempt_number,
            external_binding: self.external_binding.clone(),
        })
    }

    pub(crate) fn classify_reservation(
        &self,
        reservation: &CommandReservation,
        now: SystemTime,
    ) -> Result<ReservationDecision, CommandLedgerError> {
        if self.state == CommandLedgerState::Expired || self.retention_expires_at <= now {
            return Ok(ReservationDecision::Expire);
        }
        if self.command_name != reservation.command_name
            || self.contract_fingerprint != reservation.contract_fingerprint
            || self.input_hash != reservation.input_hash
            || self.external_binding != reservation.external_binding
        {
            return Ok(ReservationDecision::Conflict);
        }
        match self.state {
            CommandLedgerState::InProgress => {
                let lease = self.lease_expires_at.ok_or_else(|| {
                    CommandLedgerError::Corrupt(format!(
                        "in-progress command `{}` has no lease deadline",
                        self.key.command_id()
                    ))
                })?;
                if lease <= now {
                    Ok(ReservationDecision::Reclaim)
                } else {
                    Ok(ReservationDecision::InProgress)
                }
            }
            CommandLedgerState::RetryableUnknown => Ok(ReservationDecision::Reclaim),
            state if state.is_replayable() => Ok(ReservationDecision::Replay),
            CommandLedgerState::Expired => Ok(ReservationDecision::Expire),
            other => Err(CommandLedgerError::Corrupt(format!(
                "command `{}` has unsupported state `{}`",
                self.key.command_id(),
                other.as_str()
            ))),
        }
    }

    pub(crate) fn reclaim(
        &mut self,
        reservation: &CommandReservation,
        now: SystemTime,
    ) -> Result<(), CommandLedgerError> {
        let attempt_number = self.attempt_number.checked_add(1).ok_or_else(|| {
            CommandLedgerError::Corrupt(format!(
                "command `{}` attempt counter overflowed",
                self.key.command_id()
            ))
        })?;
        let lease_expires_at = checked_deadline(now, reservation.lease, "attempt lease")?;
        let retention_expires_at =
            checked_deadline(now, reservation.retention, "command retention")?;

        self.state = CommandLedgerState::InProgress;
        self.attempt_token = Some(reservation.candidate_attempt.clone());
        self.attempt_number = attempt_number;
        self.lease_expires_at = Some(lease_expires_at);
        self.retention_expires_at = retention_expires_at;
        self.updated_at = now;
        Ok(())
    }

    pub(crate) fn validate_stored_shape(&self) -> Result<(), CommandLedgerError> {
        if self.command_name.trim().is_empty() || self.attempt_number == 0 {
            return Err(CommandLedgerError::Corrupt(format!(
                "command `{}` has invalid invariant fields",
                self.key.command_id()
            )));
        }
        let valid = match self.state {
            CommandLedgerState::InProgress => {
                self.attempt_token.is_some()
                    && self.lease_expires_at.is_some()
                    && self.outcome_json.is_none()
                    && self.completed_at.is_none()
                    && self.compacted_at.is_none()
            }
            CommandLedgerState::RetryableUnknown => {
                self.attempt_token.is_none()
                    && self.lease_expires_at.is_none()
                    && self.outcome_json.is_none()
                    && self.completed_at.is_none()
                    && self.compacted_at.is_none()
            }
            state if state.is_replayable() => {
                self.attempt_token.is_none()
                    && self.lease_expires_at.is_none()
                    && self.outcome_json.is_some()
                    && self.completed_at.is_some()
                    && self.compacted_at.is_none()
            }
            CommandLedgerState::Expired => {
                self.attempt_token.is_none()
                    && self.lease_expires_at.is_none()
                    && self.outcome_json.is_none()
                    && self.compacted_at.is_some()
            }
            _ => false,
        };
        if !valid {
            return Err(CommandLedgerError::Corrupt(format!(
                "command `{}` state `{}` has inconsistent nullable fields",
                self.key.command_id(),
                self.state.as_str()
            )));
        }
        Ok(())
    }

    pub(crate) fn expire(&mut self, now: SystemTime) {
        self.state = CommandLedgerState::Expired;
        self.attempt_token = None;
        self.lease_expires_at = None;
        self.outcome_json = None;
        self.updated_at = now;
        self.compacted_at = Some(now);
    }

    pub(crate) fn matches_fence(&self, attempt: &AttemptFence) -> bool {
        self.key == attempt.key
            && self.contract_fingerprint == attempt.contract_fingerprint
            && self.input_hash == attempt.input_hash
            && self.state == CommandLedgerState::InProgress
            && self.causation_id == attempt.causation_id
            && self.attempt_token.as_ref() == Some(&attempt.attempt_token)
            && self.attempt_number == attempt.attempt_number
    }

    pub(crate) fn matches_lookup_scope(&self, scope: CommandLookupScope<'_>) -> bool {
        match scope {
            CommandLookupScope::CommandName(expected) => self.command_name == expected,
            CommandLookupScope::CommandContract {
                command_name,
                contract_fingerprint,
            } => {
                self.command_name == command_name
                    && self.contract_fingerprint.as_bytes() == contract_fingerprint
            }
            CommandLookupScope::Attempt(attempt) => {
                self.key == attempt.key
                    && self.contract_fingerprint == attempt.contract_fingerprint
                    && self.input_hash == attempt.input_hash
                    && self.causation_id == attempt.causation_id
                    && self.attempt_number == attempt.attempt_number
                    && self
                        .attempt_token
                        .as_ref()
                        .is_none_or(|token| token == &attempt.attempt_token)
            }
        }
    }

    /// Prove that a locked ledger row still belongs to this live attempt
    /// without inspecting or mutating its eventual completion payload.
    ///
    /// SQL adapters use this as an early transaction preflight before any
    /// domain or projection writes. The final conditional completion remains
    /// the authoritative last statement and repeats the same fence check.
    pub(crate) fn validate_live_attempt(
        &self,
        attempt: &AttemptFence,
        now: SystemTime,
    ) -> Result<(), CommandLedgerError> {
        let lease_is_live = self
            .lease_expires_at
            .is_some_and(|lease_expires_at| lease_expires_at > now);
        if self.matches_fence(attempt) && lease_is_live {
            Ok(())
        } else {
            Err(CommandLedgerError::AttemptFenced {
                command_id: attempt.key.command_id().to_string(),
            })
        }
    }

    pub(crate) fn mark_retryable_unknown(
        &mut self,
        attempt: &AttemptFence,
        now: SystemTime,
    ) -> Result<(), CommandLedgerError> {
        if !self.matches_fence(attempt) {
            return Err(CommandLedgerError::AttemptFenced {
                command_id: attempt.key.command_id().to_string(),
            });
        }
        self.state = CommandLedgerState::RetryableUnknown;
        self.attempt_token = None;
        self.lease_expires_at = None;
        self.updated_at = now;
        Ok(())
    }

    pub(crate) fn complete(
        &mut self,
        completion: &CommandCompletion,
        now: SystemTime,
    ) -> Result<(), CommandLedgerError> {
        if self.external_binding.is_some() {
            return Err(CommandLedgerError::Invalid(
                "local causal completion cannot complete an externally bound reservation".into(),
            ));
        }
        completion.validate_direct_projection()?;
        self.validate_live_attempt(&completion.attempt.fence(), now)?;
        let retention_expires_at = match completion.retention_expires_at() {
            Some(deadline) if deadline > now => deadline,
            Some(_) => {
                return Err(CommandLedgerError::Invalid(
                    "command retention deadline must remain live at commit".into(),
                ));
            }
            None => checked_deadline(now, completion.retention, "command retention")?,
        };
        self.state = completion.state.into();
        self.attempt_token = None;
        self.lease_expires_at = None;
        self.outcome_json = Some(completion.replay.clone());
        self.updated_at = now;
        self.completed_at = Some(now);
        self.retention_expires_at = retention_expires_at;
        Ok(())
    }

    pub(crate) fn complete_external(
        &mut self,
        completion: &ExternalCommandCompletion,
        now: SystemTime,
    ) -> Result<(), CommandLedgerError> {
        if self.external_binding.as_ref() != Some(completion.binding()) {
            return Err(CommandLedgerError::Invalid(
                "external completion binding does not match the reserved route".into(),
            ));
        }
        if completion.attempt().external_binding() != Some(completion.binding()) {
            return Err(CommandLedgerError::Invalid(
                "external completion attempt does not carry the reserved route".into(),
            ));
        }
        // Check the complete attempt fence before any other mutable-row
        // invariant. A late completion must report the generation race even
        // when its retention deadline has also elapsed.
        self.validate_live_attempt(&completion.attempt_fence(), now)?;
        let retention_expires_at = match completion.retention_expires_at() {
            Some(deadline) if deadline > now => deadline,
            Some(_) => {
                return Err(CommandLedgerError::Invalid(
                    "command retention deadline must remain live at commit".into(),
                ));
            }
            None => checked_deadline(now, completion.retention(), "command retention")?,
        };
        self.state = completion.state().into();
        self.attempt_token = None;
        self.lease_expires_at = None;
        self.outcome_json = Some(completion.replay_json().to_string());
        self.updated_at = now;
        self.completed_at = Some(now);
        self.retention_expires_at = retention_expires_at;
        Ok(())
    }

    pub(crate) fn replay(&self) -> Result<CommandReplay, CommandLedgerError> {
        if !self.state.is_replayable() {
            return Err(CommandLedgerError::Corrupt(format!(
                "command `{}` state `{}` is not replayable",
                self.key.command_id(),
                self.state.as_str()
            )));
        }
        let payload = self.outcome_json.as_deref().ok_or_else(|| {
            CommandLedgerError::Corrupt(format!(
                "replayable command `{}` has no outcome",
                self.key.command_id()
            ))
        })?;
        let envelope: Value = serde_json::from_str(payload).map_err(|error| {
            CommandLedgerError::Corrupt(format!(
                "command `{}` outcome is invalid JSON: {error}",
                self.key.command_id()
            ))
        })?;
        let Value::Object(mut envelope) = envelope else {
            return Err(CommandLedgerError::Corrupt(format!(
                "command `{}` replay envelope is not an object",
                self.key.command_id()
            )));
        };
        let version = envelope
            .remove("version")
            .and_then(|value| value.as_u64())
            .ok_or_else(|| {
                CommandLedgerError::Corrupt(format!(
                    "command `{}` replay envelope has no numeric version",
                    self.key.command_id()
                ))
            })?;
        if version != u64::from(COMMAND_REPLAY_VERSION) {
            return Err(CommandLedgerError::Corrupt(format!(
                "command `{}` replay envelope version `{version}` is unsupported",
                self.key.command_id()
            )));
        }
        let outcome = envelope.remove("outcome").ok_or_else(|| {
            CommandLedgerError::Corrupt(format!(
                "command `{}` replay envelope has no outcome",
                self.key.command_id()
            ))
        })?;
        let projection_obligations_value =
            envelope.remove("projection_obligations").ok_or_else(|| {
                CommandLedgerError::Corrupt(format!(
                    "command `{}` replay envelope has no projection obligations",
                    self.key.command_id()
                ))
            })?;
        let projection_obligations: Vec<ResolvedProjectionObligation> =
            serde_json::from_value(projection_obligations_value.clone()).map_err(|error| {
                CommandLedgerError::Corrupt(format!(
                    "command `{}` replay projection obligations are invalid: {error}",
                    self.key.command_id()
                ))
            })?;
        let canonical_projection_obligations = serde_json::to_value(&projection_obligations)
            .map_err(|error| {
                CommandLedgerError::Corrupt(format!(
                    "command `{}` replay projection obligations cannot be normalized: {error}",
                    self.key.command_id()
                ))
            })?;
        if canonical_projection_obligations != projection_obligations_value {
            return Err(CommandLedgerError::Corrupt(format!(
                "command `{}` replay projection obligations contain unknown or non-canonical fields",
                self.key.command_id()
            )));
        }
        let projection_metadata = envelope
            .remove("projection_metadata")
            .map(|value| {
                let encoded = value.as_str().ok_or_else(|| {
                    CommandLedgerError::Corrupt(format!(
                        "command `{}` replay projection metadata is not an opaque string",
                        self.key.command_id()
                    ))
                })?;
                let bytes = URL_SAFE_NO_PAD.decode(encoded).map_err(|_| {
                    CommandLedgerError::Corrupt(format!(
                        "command `{}` replay projection metadata is not canonical base64url",
                        self.key.command_id()
                    ))
                })?;
                if URL_SAFE_NO_PAD.encode(&bytes) != encoded {
                    return Err(CommandLedgerError::Corrupt(format!(
                        "command `{}` replay projection metadata uses noncanonical base64url",
                        self.key.command_id()
                    )));
                }
                super::reservation::validate_projection_metadata_bytes(self.state, Some(&bytes))
                    .map_err(|error| {
                        CommandLedgerError::Corrupt(format!(
                            "command `{}` replay projection metadata is invalid: {error}",
                            self.key.command_id()
                        ))
                    })?;
                Ok(bytes)
            })
            .transpose()?;
        if projection_metadata.is_some() && !projection_obligations.is_empty() {
            return Err(CommandLedgerError::Corrupt(format!(
                "command `{}` replay mixes legacy and modeled projection obligations",
                self.key.command_id()
            )));
        }
        if projection_metadata.is_none() {
            validate_projection_obligation_semantics(self.state, &projection_obligations).map_err(
                |error| {
                    CommandLedgerError::Corrupt(format!(
                        "command `{}` replay projection obligations are inconsistent: {error}",
                        self.key.command_id()
                    ))
                },
            )?;
        }
        let direct_projection = envelope.remove("direct_projection");
        match (&direct_projection, self.state) {
            (Some(value), CommandLedgerState::Atomic) => {
                SameTransactionProjectionEvidence::validate_replay_value(value).map_err(
                    |error| {
                        CommandLedgerError::Corrupt(format!(
                            "command `{}` replay direct projection is invalid: {error}",
                            self.key.command_id()
                        ))
                    },
                )?;
            }
            (None, CommandLedgerState::Atomic) => {
                return Err(CommandLedgerError::Corrupt(format!(
                    "command `{}` projected replay has no exact direct projection evidence",
                    self.key.command_id()
                )));
            }
            (Some(_), _) => {
                return Err(CommandLedgerError::Corrupt(format!(
                    "command `{}` non-projected replay contains direct projection evidence",
                    self.key.command_id()
                )));
            }
            (None, _) => {}
        }
        if !envelope.is_empty() {
            return Err(CommandLedgerError::Corrupt(format!(
                "command `{}` replay envelope has unknown fields",
                self.key.command_id()
            )));
        }
        Ok(CommandReplay {
            command_id: self.key.command_id.clone(),
            command_name: self.command_name.clone(),
            state: self.state,
            causation_id: self.causation_id.clone(),
            outcome,
            projection_obligations,
            projection_metadata,
            direct_projection,
        })
    }

    pub(crate) fn lookup(&self) -> Result<CommandLookup, CommandLedgerError> {
        match self.state {
            CommandLedgerState::InProgress => Ok(CommandLookup::InProgress {
                causation_id: self.causation_id.clone(),
            }),
            CommandLedgerState::RetryableUnknown => Ok(CommandLookup::RetryableUnknown {
                causation_id: self.causation_id.clone(),
            }),
            CommandLedgerState::Expired => Ok(CommandLookup::Expired),
            state if state.is_replayable() => Ok(CommandLookup::Replay(self.replay()?)),
            other => Err(CommandLedgerError::Corrupt(format!(
                "command `{}` has unsupported lookup state `{}`",
                self.key.command_id(),
                other.as_str()
            ))),
        }
    }

    pub(crate) fn reservation_outcome(
        &self,
        decision: ReservationDecision,
    ) -> Result<ReservationOutcome, CommandLedgerError> {
        match decision {
            ReservationDecision::Conflict => Ok(ReservationOutcome::Conflict),
            ReservationDecision::Expire => Ok(ReservationOutcome::Expired),
            ReservationDecision::InProgress => Ok(ReservationOutcome::InProgress {
                causation_id: self.causation_id.clone(),
            }),
            ReservationDecision::Replay => Ok(ReservationOutcome::Replay(self.replay()?)),
            ReservationDecision::Reclaim => {
                Ok(ReservationOutcome::Acquired(self.acquired_attempt()?))
            }
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DurableCellCommandRecordV1 {
    version: u16,
    service_id: String,
    principal_partition: String,
    command_id: String,
    command_name: String,
    #[serde(default)]
    external_binding: Option<String>,
    contract_fingerprint: Vec<u8>,
    input_hash: Vec<u8>,
    state: String,
    causation_id: String,
    attempt_token: Option<String>,
    attempt_number: u64,
    lease_expires_at_ms: Option<u64>,
    outcome_json: Option<String>,
    created_at_ms: u64,
    updated_at_ms: u64,
    completed_at_ms: Option<u64>,
    retention_expires_at_ms: u64,
    compacted_at_ms: Option<u64>,
}

fn system_time_to_unix_millis(value: SystemTime) -> Result<u64, CommandLedgerError> {
    let millis = value
        .duration_since(UNIX_EPOCH)
        .map_err(|_| {
            CommandLedgerError::Corrupt(
                "cell command ledger timestamp precedes the Unix epoch".into(),
            )
        })?
        .as_millis();
    u64::try_from(millis).map_err(|_| {
        CommandLedgerError::Corrupt("cell command ledger timestamp exceeds u64 millis".into())
    })
}

fn system_time_from_unix_millis(value: u64) -> Result<SystemTime, CommandLedgerError> {
    UNIX_EPOCH
        .checked_add(Duration::from_millis(value))
        .ok_or_else(|| {
            CommandLedgerError::Corrupt("cell command ledger timestamp is out of range".into())
        })
}

fn checked_deadline(
    now: SystemTime,
    duration: Duration,
    label: &str,
) -> Result<SystemTime, CommandLedgerError> {
    now.checked_add(duration).ok_or_else(|| {
        CommandLedgerError::Invalid(format!("{label} deadline exceeds SystemTime range"))
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReservationDecision {
    Conflict,
    Expire,
    InProgress,
    Replay,
    Reclaim,
}
