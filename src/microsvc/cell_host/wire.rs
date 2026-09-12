//! Strict, size-bounded wire values shared by cell workers and command hosts.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::OutboxMessage;

pub const MAX_CELL_PROJECTION_EVENTS: usize = 256;
pub const MAX_CELL_PROJECTION_EVENT_PAYLOAD_BYTES: usize = 1024 * 1024;
pub const MAX_CELL_PROJECTION_EVENT_WIRE_BYTES: usize = 1536 * 1024;
const MAX_IDENTIFIER_BYTES: usize = 512;
const MAX_CODEC_BYTES: usize = 128;
const MAX_METADATA_ENTRIES: usize = 64;
const MAX_METADATA_VALUE_BYTES: usize = 1024;

/// Domain-event evidence returned by a cell wait-path.
///
/// This is not a delivery record. Delivery status, leases, attempts, and
/// settlement belong exclusively to the cell's durable outbox and Queue.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CellProjectionEventWireItem {
    pub id: String,
    pub event_type: String,
    pub payload: Vec<u8>,
    pub payload_codec: String,
    pub payload_codec_version: u16,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
    pub source_aggregate_type: Option<String>,
    pub source_aggregate_id: Option<String>,
    pub source_sequence: Option<u64>,
}

impl CellProjectionEventWireItem {
    pub fn from_message(message: &OutboxMessage) -> Self {
        Self {
            id: message.id.clone(),
            event_type: message.event_type.clone(),
            payload: message.payload.clone(),
            payload_codec: message.payload_codec.clone(),
            payload_codec_version: message.payload_codec_version,
            metadata: message.metadata.clone(),
            source_aggregate_type: message.source_aggregate_type.clone(),
            source_aggregate_id: message.source_aggregate_id.clone(),
            source_sequence: message.source_sequence,
        }
    }

    pub fn try_into_message(self) -> Result<OutboxMessage, String> {
        validate_identifier("projection event id", &self.id)?;
        validate_identifier("event type", &self.event_type)?;
        if self.payload.len() > MAX_CELL_PROJECTION_EVENT_PAYLOAD_BYTES {
            return Err("cell projection event payload exceeds 1 MiB".into());
        }
        if self.payload_codec.is_empty()
            || self.payload_codec.len() > MAX_CODEC_BYTES
            || self.payload_codec_version == 0
        {
            return Err("cell projection event payload codec is invalid".into());
        }
        validate_metadata(&self.metadata)?;
        if let Some(value) = &self.source_aggregate_type {
            validate_identifier("source aggregate type", value)?;
        }
        if let Some(value) = &self.source_aggregate_id {
            validate_identifier("source aggregate id", value)?;
        }
        let source_fields = [
            self.source_aggregate_type.is_some(),
            self.source_aggregate_id.is_some(),
            self.source_sequence.is_some(),
        ];
        if source_fields.iter().any(|present| *present)
            && source_fields.iter().any(|present| !*present)
        {
            return Err("cell projection event source identity must be complete or absent".into());
        }
        if self.source_sequence == Some(0) {
            return Err("cell projection event source sequence must be nonzero".into());
        }
        let mut message = OutboxMessage::create_with_metadata(
            self.id,
            self.event_type,
            self.payload,
            self.metadata,
        )
        .map_err(|error| error.to_string())?;
        message.payload_codec = self.payload_codec;
        message.payload_codec_version = self.payload_codec_version;
        message.source_aggregate_type = self.source_aggregate_type;
        message.source_aggregate_id = self.source_aggregate_id;
        message.source_sequence = self.source_sequence;
        Ok(message)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CellWaitPathRequest {
    pub command_id: String,
    #[serde(default)]
    pub input: serde_json::Value,
}

impl CellWaitPathRequest {
    pub fn parse(value: serde_json::Value) -> Result<Self, String> {
        let request: Self = serde_json::from_value(value)
            .map_err(|error| format!("invalid cell wait-path envelope: {error}"))?;
        if request.command_id.trim().is_empty()
            || request.command_id.len() > MAX_IDENTIFIER_BYTES
            || request.command_id.chars().any(char::is_control)
        {
            return Err("cell wait-path commandId is invalid".into());
        }
        Ok(request)
    }
}

pub fn parse_cell_projection_events(
    value: &serde_json::Value,
) -> Result<Vec<OutboxMessage>, String> {
    let Some(items) = value.get("events") else {
        return Ok(Vec::new());
    };
    let items: Vec<CellProjectionEventWireItem> = serde_json::from_value(items.clone())
        .map_err(|error| format!("cell projection events: {error}"))?;
    if items.len() > MAX_CELL_PROJECTION_EVENTS {
        return Err(format!(
            "cell response contains more than {MAX_CELL_PROJECTION_EVENTS} projection events"
        ));
    }
    let mut total_payload = 0_usize;
    let mut ids = std::collections::HashSet::new();
    items
        .into_iter()
        .map(|item| {
            if !ids.insert(item.id.clone()) {
                return Err("cell projection event response contains duplicate ids".into());
            }
            total_payload = total_payload.saturating_add(item.payload.len());
            if total_payload > MAX_CELL_PROJECTION_EVENT_PAYLOAD_BYTES {
                return Err("cell projection event payloads exceed 1 MiB in total".into());
            }
            item.try_into_message()
        })
        .collect()
}

/// Select the exact events committed by one causal command and encode them as
/// delivery-neutral projection evidence before commit. Store the result with
/// the command receipt; never recover it by retaining delivered outbox rows.
pub fn cell_projection_event_evidence(
    messages: &[OutboxMessage],
    causation_id: &str,
) -> Result<Vec<CellProjectionEventWireItem>, String> {
    validate_identifier("projection event causation id", causation_id)?;
    let selected = messages
        .iter()
        .filter(|message| message.causation_id() == Some(causation_id))
        .cloned()
        .collect::<Vec<_>>();
    validate_cell_projection_events(&selected)?;
    Ok(selected
        .iter()
        .map(CellProjectionEventWireItem::from_message)
        .collect())
}

/// Reject a cell command before commit when its domain-event evidence cannot
/// fit in the bounded wait-path response used to seal optimistic projections.
pub(crate) fn validate_cell_projection_events(messages: &[OutboxMessage]) -> Result<(), String> {
    let value = serde_json::json!({
        "events": messages
            .iter()
            .map(CellProjectionEventWireItem::from_message)
            .collect::<Vec<_>>()
    });
    parse_cell_projection_events(&value)?;
    let encoded = serde_json::to_vec(&value)
        .map_err(|error| format!("cell projection events could not be encoded: {error}"))?;
    if encoded.len() > MAX_CELL_PROJECTION_EVENT_WIRE_BYTES {
        return Err("cell projection event envelope exceeds 1.5 MiB".into());
    }
    Ok(())
}

fn validate_identifier(label: &str, value: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > MAX_IDENTIFIER_BYTES || value.chars().any(char::is_control)
    {
        return Err(format!("{label} is invalid"));
    }
    Ok(())
}

fn validate_metadata(metadata: &HashMap<String, String>) -> Result<(), String> {
    if metadata.len() > MAX_METADATA_ENTRIES {
        return Err("cell projection event metadata has too many entries".into());
    }
    for (key, value) in metadata {
        if key.is_empty()
            || key.len() > MAX_METADATA_VALUE_BYTES
            || value.len() > MAX_METADATA_VALUE_BYTES
            || key.chars().any(char::is_control)
            || value.chars().any(char::is_control)
        {
            return Err("cell projection event metadata is invalid".into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn valid_item() -> serde_json::Value {
        json!({
            "id": "event-1",
            "eventType": "todo.created",
            "payload": [0, 127, 255],
            "payloadCodec": "bytes",
            "payloadCodecVersion": 1,
            "metadata": {},
            "sourceAggregateType": "todo",
            "sourceAggregateId": "todo-1",
            "sourceSequence": 1
        })
    }

    #[test]
    fn parses_strict_projection_event_without_numeric_truncation() {
        let rows =
            parse_cell_projection_events(&json!({ "events": [valid_item()] })).expect("valid");
        assert_eq!(rows[0].payload, vec![0, 127, 255]);

        let mut invalid = valid_item();
        invalid["payload"] = json!([256]);
        assert!(parse_cell_projection_events(&json!({ "events": [invalid] })).is_err());
    }

    #[test]
    fn rejects_unknown_and_delivery_fields() {
        let mut unknown = valid_item();
        unknown["forged"] = json!(true);
        assert!(parse_cell_projection_events(&json!({ "events": [unknown] })).is_err());

        let mut delivery = valid_item();
        delivery["status"] = json!("published");
        assert!(parse_cell_projection_events(&json!({ "events": [delivery] })).is_err());

        assert!(CellWaitPathRequest::parse(json!({
            "commandId": "command-1",
            "input": {},
            "roles": ["admin"]
        }))
        .is_err());
    }

    #[test]
    fn published_queue_rows_remain_delivery_neutral_projection_evidence() {
        let mut selected = OutboxMessage::create_with_metadata(
            "event-1",
            "todo.created",
            vec![1, 2, 3],
            HashMap::new(),
        )
        .expect("selected");
        selected.set_causation_id("causation-1");
        selected.status = crate::OutboxMessageStatus::Published;
        selected.attempts = 7;
        selected.worker_id = Some("queue-worker".into());

        let mut unrelated = selected.clone();
        unrelated.id = "event-2".into();
        unrelated.set_causation_id("causation-2");

        let evidence =
            cell_projection_event_evidence(&[selected.clone(), unrelated], "causation-1")
                .expect("evidence");
        assert_eq!(evidence.len(), 1);
        let wire = serde_json::to_value(&evidence[0]).expect("wire");
        assert!(wire.get("status").is_none());
        assert!(wire.get("attempts").is_none());
        assert!(wire.get("workerId").is_none());

        let parsed = parse_cell_projection_events(&json!({ "events": evidence }))
            .expect("projection events");
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].id, selected.id);
        assert_eq!(parsed[0].causation_id(), Some("causation-1"));
    }

    #[test]
    fn enforces_row_and_total_payload_limits() {
        let items = (0..=MAX_CELL_PROJECTION_EVENTS)
            .map(|index| {
                let mut item = valid_item();
                item["id"] = json!(format!("event-{index}"));
                item
            })
            .collect::<Vec<_>>();
        assert!(parse_cell_projection_events(&json!({ "events": items })).is_err());

        let mut oversized = valid_item();
        oversized["payload"] = json!(vec![0_u8; MAX_CELL_PROJECTION_EVENT_PAYLOAD_BYTES + 1]);
        assert!(parse_cell_projection_events(&json!({ "events": [oversized] })).is_err());
    }

    #[test]
    fn precommit_validation_enforces_the_encoded_wire_budget() {
        let rows = (0..MAX_CELL_PROJECTION_EVENTS)
            .map(|index| {
                OutboxMessage::create_with_metadata(
                    format!("event-{index}"),
                    "todo.created",
                    vec![255; 4_096],
                    HashMap::new(),
                )
                .expect("message")
            })
            .collect::<Vec<_>>();
        assert!(validate_cell_projection_events(&rows).is_err());
    }
}
