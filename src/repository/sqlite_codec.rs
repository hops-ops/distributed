//! SQLite timestamp representation shared by SQLx and Durable Object SQL.

use super::RepositoryError;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(crate) const EVENT_SELECT: &str = "event_name, event_version, payload, payload_codec, payload_codec_version, metadata, sequence, recorded_at";
pub(crate) const SNAPSHOT_SELECT: &str = "aggregate_type, aggregate_id, version, snapshot_version, payload, payload_codec, payload_codec_version, metadata, recorded_at";
pub(crate) const OUTBOX_SELECT: &str = "message_id, event_type, payload, payload_codec, payload_codec_version, metadata, status, created_at, claimed_by, claimed_until, attempts, last_error, destination, source_aggregate_type, source_aggregate_id, source_sequence, correlation_id, causation_id";

pub(crate) fn encode(timestamp: SystemTime) -> Result<String, RepositoryError> {
    let duration = timestamp.duration_since(UNIX_EPOCH).map_err(|error| {
        RepositoryError::Model(format!(
            "event timestamp before UNIX epoch cannot be stored in sqlite: {error}"
        ))
    })?;
    Ok(format!(
        "{}.{:09}",
        duration.as_secs(),
        duration.subsec_nanos()
    ))
}

pub(crate) fn decode(value: &str) -> Result<SystemTime, RepositoryError> {
    let invalid =
        || RepositoryError::Model(format!("sqlite stored timestamp `{value}` is invalid"));
    let (secs, nanos) = value.split_once('.').ok_or_else(invalid)?;
    let secs = secs.parse::<u64>().map_err(|_| invalid())?;
    let nanos = nanos.parse::<u32>().map_err(|_| invalid())?;
    if nanos >= 1_000_000_000 {
        return Err(invalid());
    }
    UNIX_EPOCH
        .checked_add(Duration::new(secs, nanos))
        .ok_or_else(invalid)
}

pub(crate) fn decode_epoch(value: f64) -> Result<SystemTime, RepositoryError> {
    let invalid =
        || RepositoryError::Model(format!("sqlite timestamp epoch value {value} is invalid"));
    let duration = Duration::try_from_secs_f64(value).map_err(|_| invalid())?;
    UNIX_EPOCH.checked_add(duration).ok_or_else(invalid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_round_trip_preserves_nanoseconds() {
        let value = UNIX_EPOCH + Duration::new(1_700_000_000, 123_456_789);
        assert_eq!(encode(value).unwrap(), "1700000000.123456789");
        assert_eq!(decode(&encode(value).unwrap()).unwrap(), value);
        assert_eq!(
            decode_epoch(42.25).unwrap(),
            UNIX_EPOCH + Duration::new(42, 250_000_000)
        );
    }

    #[test]
    fn invalid_timestamps_return_errors_without_panicking() {
        for value in [
            "",
            "not-a-time",
            "-1.000000000",
            "1.1000000000",
            "1.2.3",
            "18446744073709551615.000000000",
        ] {
            assert!(decode(value).is_err(), "accepted {value}");
        }
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -1.0, f64::MAX] {
            assert!(decode_epoch(value).is_err(), "accepted {value}");
        }
        assert!(encode(UNIX_EPOCH - Duration::from_secs(1)).is_err());
    }
}
pub(crate) const COMMAND_LEDGER_SELECT: &str = "command_name, command_contract_hash, input_hash, state, causation_id, attempt_token, attempt_number, external_binding, lease_expires_at, outcome, created_at, updated_at, completed_at, retention_expires_at, compacted_at";
