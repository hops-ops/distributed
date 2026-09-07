use super::*;

/// Exact evidence allocated for a same-transaction projected command.
///
/// The command ledger stores the canonical replay form produced by
/// [`replay_value`](Self::replay_value), so response-loss recovery returns the
/// revision and change allocated by the original transaction rather than the
/// record's later head.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SameTransactionProjectionEvidence {
    pub(crate) records: Vec<ProjectionRecordMetadata>,
    pub(crate) changes: Vec<ProjectionChange>,
    pub(crate) observations: Vec<ProjectionObservation>,
}

impl SameTransactionProjectionEvidence {
    pub(crate) fn replay_value(&self) -> serde_json::Value {
        serde_json::to_value(SameTransactionReplayEnvelope::from(self))
            .expect("same-transaction projection evidence contains only serializable primitives")
    }

    pub(crate) fn from_replay_value(value: &serde_json::Value) -> Result<Self, String> {
        let decoded: SameTransactionReplayEnvelope = serde_json::from_value(value.clone())
            .map_err(|error| format!("direct projection evidence is invalid: {error}"))?;
        let evidence = decoded.into_evidence()?;
        let canonical = serde_json::to_value(SameTransactionReplayEnvelope::from(&evidence))
            .map_err(|error| format!("direct projection evidence cannot be normalized: {error}"))?;
        if canonical != *value {
            return Err(
                "direct projection evidence contains unknown or non-canonical fields".into(),
            );
        }
        Ok(evidence)
    }

    pub(crate) fn validate_replay_value(value: &serde_json::Value) -> Result<(), String> {
        Self::from_replay_value(value).map(|_| ())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SameTransactionReplayEnvelope {
    version: u16,
    records: Vec<ReplayRecord>,
    changes: Vec<ReplayChange>,
    observations: Vec<ReplayObservation>,
}

impl From<&SameTransactionProjectionEvidence> for SameTransactionReplayEnvelope {
    fn from(evidence: &SameTransactionProjectionEvidence) -> Self {
        Self {
            version: 1,
            records: evidence.records.iter().map(ReplayRecord::from).collect(),
            changes: evidence.changes.iter().map(ReplayChange::from).collect(),
            observations: evidence
                .observations
                .iter()
                .map(ReplayObservation::from)
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayScope {
    topology_version: u32,
    topology_name: String,
    topology_digest: String,
    partition: String,
    partition_digest: String,
    model: String,
    key: String,
    key_digest: String,
}

impl From<&ProjectionRecordScope> for ReplayScope {
    fn from(scope: &ProjectionRecordScope) -> Self {
        Self {
            topology_version: scope.topology().version(),
            topology_name: scope.topology().name().to_string(),
            topology_digest: digest_hex(&scope.topology().digest()),
            partition: bytes_hex(scope.projection_partition().canonical_bytes()),
            partition_digest: digest_hex(&scope.projection_partition().digest()),
            model: scope.model().to_string(),
            key: bytes_hex(scope.canonical_key_bytes()),
            key_digest: digest_hex(&scope.key_digest()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayRevision {
    scope: ReplayScope,
    incarnation: u64,
    revision: u64,
}

impl From<&RecordRevision> for ReplayRevision {
    fn from(revision: &RecordRevision) -> Self {
        Self {
            scope: ReplayScope::from(revision.scope()),
            incarnation: revision.incarnation(),
            revision: revision.revision(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayCursor {
    topology_version: u32,
    topology_name: String,
    topology_digest: String,
    partition: String,
    partition_digest: String,
    epoch: String,
    position: u64,
}

impl From<&ProjectionChangeCursor> for ReplayCursor {
    fn from(cursor: &ProjectionChangeCursor) -> Self {
        Self {
            topology_version: cursor.topology().version(),
            topology_name: cursor.topology().name().to_string(),
            topology_digest: digest_hex(&cursor.topology().digest()),
            partition: bytes_hex(cursor.projection_partition().canonical_bytes()),
            partition_digest: digest_hex(&cursor.projection_partition().digest()),
            epoch: cursor.epoch().as_str().to_string(),
            position: cursor.position(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayRecord {
    revision: ReplayRevision,
    tombstone: bool,
    change: ReplayCursor,
}

impl From<&ProjectionRecordMetadata> for ReplayRecord {
    fn from(record: &ProjectionRecordMetadata) -> Self {
        Self {
            revision: ReplayRevision::from(&record.revision),
            tombstone: record.tombstone,
            change: ReplayCursor::from(&record.change),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayChange {
    cursor: ReplayCursor,
    kind: String,
    causation_id: String,
    observation_kind: Option<String>,
    scope: Option<ReplayScope>,
    revision: Option<ReplayRevision>,
    failure_id: Option<String>,
}

impl From<&ProjectionChange> for ReplayChange {
    fn from(change: &ProjectionChange) -> Self {
        Self {
            cursor: ReplayCursor::from(&change.cursor),
            kind: change.kind.as_storage_str().to_string(),
            causation_id: change.causation_id.clone(),
            observation_kind: change
                .observation_kind
                .map(|kind| kind.as_storage_str().to_string()),
            scope: change.scope.as_ref().map(ReplayScope::from),
            revision: change.revision.as_ref().map(ReplayRevision::from),
            failure_id: change.failure_id.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayObservation {
    causation_id: String,
    kind: String,
    revision: Option<ReplayRevision>,
    scope: ReplayScope,
    change: ReplayCursor,
}

impl From<&ProjectionObservation> for ReplayObservation {
    fn from(observation: &ProjectionObservation) -> Self {
        Self {
            causation_id: observation.causation_id.clone(),
            kind: observation.kind.as_storage_str().to_string(),
            revision: observation.revision.as_ref().map(ReplayRevision::from),
            scope: ReplayScope::from(&observation.scope),
            change: ReplayCursor::from(&observation.change),
        }
    }
}

impl SameTransactionReplayEnvelope {
    fn into_evidence(self) -> Result<SameTransactionProjectionEvidence, String> {
        if self.version != 1 {
            return Err(format!(
                "direct projection evidence version {} is unsupported",
                self.version
            ));
        }
        let evidence = SameTransactionProjectionEvidence {
            records: self
                .records
                .into_iter()
                .map(ReplayRecord::into_record)
                .collect::<Result<_, _>>()?,
            changes: self
                .changes
                .into_iter()
                .map(ReplayChange::into_change)
                .collect::<Result<_, _>>()?,
            observations: self
                .observations
                .into_iter()
                .map(ReplayObservation::into_observation)
                .collect::<Result<_, _>>()?,
        };
        validate_same_transaction_replay_evidence(&evidence)?;
        Ok(evidence)
    }
}

impl ReplayScope {
    fn into_scope(self) -> Result<ProjectionRecordScope, String> {
        let topology = decode_replay_topology(
            self.topology_version,
            self.topology_name,
            self.topology_digest,
        )?;
        let partition = decode_replay_partition(self.partition, self.partition_digest)?;
        let key = decode_replay_hex("record key", &self.key, MAX_PROJECTION_RECORD_KEY_BYTES)?;
        let scope = ProjectionRecordScope::new(topology, partition, self.model, key)
            .map_err(|error| replay_validation_error("record scope", error))?;
        if digest_hex(&scope.key_digest()) != self.key_digest {
            return Err(
                "direct projection evidence record key digest does not match its canonical bytes"
                    .into(),
            );
        }
        Ok(scope)
    }
}

impl ReplayRevision {
    fn into_revision(self) -> Result<RecordRevision, String> {
        RecordRevision::new(self.scope.into_scope()?, self.incarnation, self.revision)
            .map_err(|error| replay_validation_error("record revision", error))
    }
}

impl ReplayCursor {
    fn into_cursor(self) -> Result<ProjectionChangeCursor, String> {
        ProjectionChangeCursor::new(
            decode_replay_topology(
                self.topology_version,
                self.topology_name,
                self.topology_digest,
            )?,
            decode_replay_partition(self.partition, self.partition_digest)?,
            ProjectionEpoch::new(self.epoch)
                .map_err(|error| replay_validation_error("change epoch", error))?,
            self.position,
        )
        .map_err(|error| replay_validation_error("change cursor", error))
    }
}

impl ReplayRecord {
    fn into_record(self) -> Result<ProjectionRecordMetadata, String> {
        Ok(ProjectionRecordMetadata {
            source_snapshot: None,
            revision: self.revision.into_revision()?,
            tombstone: self.tombstone,
            change: self.change.into_cursor()?,
        })
    }
}

impl ReplayChange {
    fn into_change(self) -> Result<ProjectionChange, String> {
        Ok(ProjectionChange {
            cursor: self.cursor.into_cursor()?,
            kind: decode_replay_change_kind(&self.kind)?,
            causation_id: bounded_opaque(
                "projection causation ID",
                self.causation_id,
                MAX_CAUSATION_ID_BYTES,
            )
            .map_err(|error| replay_validation_error("change causation", error))?,
            observation_kind: self
                .observation_kind
                .as_deref()
                .map(decode_replay_observation_kind)
                .transpose()?,
            scope: self.scope.map(ReplayScope::into_scope).transpose()?,
            revision: self
                .revision
                .map(ReplayRevision::into_revision)
                .transpose()?,
            failure_id: self
                .failure_id
                .map(|failure_id| {
                    bounded_opaque("projection failure ID", failure_id, MAX_FAILURE_ID_BYTES)
                        .map_err(|error| replay_validation_error("change failure ID", error))
                })
                .transpose()?,
        })
    }
}

impl ReplayObservation {
    fn into_observation(self) -> Result<ProjectionObservation, String> {
        Ok(ProjectionObservation {
            causation_id: bounded_opaque(
                "projection causation ID",
                self.causation_id,
                MAX_CAUSATION_ID_BYTES,
            )
            .map_err(|error| replay_validation_error("observation causation", error))?,
            kind: decode_replay_observation_kind(&self.kind)?,
            revision: self
                .revision
                .map(ReplayRevision::into_revision)
                .transpose()?,
            scope: self.scope.into_scope()?,
            change: self.change.into_cursor()?,
        })
    }
}

fn decode_replay_topology(
    version: u32,
    name: String,
    digest: String,
) -> Result<ProjectorTopologyId, String> {
    ProjectorTopologyId::new(
        version,
        name,
        decode_replay_digest("topology digest", &digest)?,
    )
    .map_err(|error| replay_validation_error("topology", error))
}

fn decode_replay_partition(
    canonical: String,
    digest: String,
) -> Result<ProjectionPartition, String> {
    let partition = ProjectionPartition::new(decode_replay_hex(
        "partition",
        &canonical,
        MAX_PROJECTION_PARTITION_BYTES,
    )?)
    .map_err(|error| replay_validation_error("partition", error))?;
    if digest_hex(&partition.digest()) != digest {
        return Err(
            "direct projection evidence partition digest does not match its canonical bytes".into(),
        );
    }
    Ok(partition)
}

fn decode_replay_digest(field: &str, value: &str) -> Result<[u8; 32], String> {
    decode_replay_hex(field, value, 32)?
        .try_into()
        .map_err(|_| format!("direct projection evidence {field} must contain exactly 32 bytes"))
}

fn decode_replay_hex(field: &str, value: &str, max_bytes: usize) -> Result<Vec<u8>, String> {
    if !value.len().is_multiple_of(2) || value.len() > max_bytes.saturating_mul(2) {
        return Err(format!(
            "direct projection evidence {field} is not bounded canonical hexadecimal"
        ));
    }
    let mut decoded = Vec::with_capacity(value.len() / 2);
    for pair in value.as_bytes().chunks_exact(2) {
        let high = decode_replay_hex_nibble(pair[0]).ok_or_else(|| {
            format!("direct projection evidence {field} is not lowercase hexadecimal")
        })?;
        let low = decode_replay_hex_nibble(pair[1]).ok_or_else(|| {
            format!("direct projection evidence {field} is not lowercase hexadecimal")
        })?;
        decoded.push((high << 4) | low);
    }
    Ok(decoded)
}

fn decode_replay_hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn decode_replay_change_kind(value: &str) -> Result<ProjectionChangeKind, String> {
    ProjectionChangeKind::from_storage_str(value)
        .ok_or_else(|| format!("direct projection evidence has unknown change kind `{value}`"))
}

fn decode_replay_observation_kind(value: &str) -> Result<ProjectionObservationKind, String> {
    ProjectionObservationKind::from_storage_str(value)
        .ok_or_else(|| format!("direct projection evidence has unknown observation kind `{value}`"))
}

fn validate_same_transaction_replay_evidence(
    evidence: &SameTransactionProjectionEvidence,
) -> Result<(), String> {
    let [record] = evidence.records.as_slice() else {
        return Err("direct projection evidence must contain exactly one projected record".into());
    };
    let [change] = evidence.changes.as_slice() else {
        return Err("direct projection evidence must contain exactly one record change".into());
    };
    let [observation] = evidence.observations.as_slice() else {
        return Err(
            "direct projection evidence must contain exactly one record observation".into(),
        );
    };
    if record.tombstone {
        return Err("direct projection evidence cannot replay a tombstone".into());
    }
    if change.kind != ProjectionChangeKind::RecordUpsert
        || change.observation_kind.is_some()
        || change.failure_id.is_some()
    {
        return Err("direct projection evidence change must be a plain record upsert".into());
    }
    if change.scope.as_ref() != Some(record.revision.scope())
        || change.revision.as_ref() != Some(&record.revision)
    {
        return Err(
            "direct projection evidence change does not match the projected record revision".into(),
        );
    }
    if change.cursor.topology() != record.revision.scope().topology()
        || change.cursor.projection_partition() != record.revision.scope().projection_partition()
    {
        return Err(
            "direct projection evidence change cursor does not match its record scope".into(),
        );
    }
    if record.change != change.cursor {
        return Err("direct projection evidence record and change cursors do not match".into());
    }
    if observation.kind != ProjectionObservationKind::Record
        || observation.scope != *record.revision.scope()
        || observation.revision.as_ref() != Some(&record.revision)
    {
        return Err(
            "direct projection evidence observation does not match the projected record revision"
                .into(),
        );
    }
    if observation.change != change.cursor || observation.causation_id != change.causation_id {
        return Err(
            "direct projection evidence observation does not match its record change".into(),
        );
    }
    Ok(())
}

fn replay_validation_error(field: &str, error: impl fmt::Display) -> String {
    format!("direct projection evidence {field} is invalid: {error}")
}

fn bytes_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
