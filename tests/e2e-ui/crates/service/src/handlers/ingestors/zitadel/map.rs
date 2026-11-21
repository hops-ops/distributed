//! Map Zitadel Action / fixture payloads → provider bus subjects + envelopes.

use e2e_projections::{ZitadelEmail, ZitadelUserPayload};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const HUMAN_CREATED: &str = "zitadel.user.human.created.v1";
pub const HUMAN_UPDATED: &str = "zitadel.user.human.updated.v1";
pub const HUMAN_DEACTIVATED: &str = "zitadel.user.human.deactivated.v1";
pub const HUMAN_REACTIVATED: &str = "zitadel.user.human.reactivated.v1";
pub const MACHINE_CREATED: &str = "zitadel.user.machine.created.v1";

/// Input of `zitadel.ingress.v1`: one provider delivery, normalized by the
/// HTTP edge from a Zitadel Action event or a local fixture body.
///
/// It holds nothing that differs between two deliveries of one event, so a
/// redelivery is the same command.
#[derive(Debug, Clone, Serialize, Deserialize, distributed::CommandInput)]
pub struct ZitadelIngressInput {
    pub delivery_id: Option<String>,
    pub event_type: Option<String>,
    pub provider_subject: Option<String>,
    pub user_kind: Option<String>,
    pub email: Option<String>,
    pub emails: Option<Vec<ZitadelIngressEmail>>,
    pub display_name: Option<String>,
    pub approval_status: Option<String>,
    pub grants: Option<Vec<String>>,
    pub roles: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, distributed::CommandInput)]
pub struct ZitadelIngressEmail {
    pub address: String,
    /// Absent means not primary.
    pub primary: Option<bool>,
    /// Absent means verified.
    pub verified: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct MappedDelivery {
    pub message_name: String,
    pub delivery_id: String,
    pub payload: ZitadelUserPayload,
}

pub fn looks_like_action_event(raw: &Value) -> bool {
    raw.get("aggregateID").is_some()
        || raw.get("aggregateId").is_some()
        || (raw.get("aggregateType").is_some() && raw.get("sequence").is_some())
}

/// Normalize a webhook body into the command input. A fixture body may name
/// a field by any of the spellings listed here.
pub fn normalize_ingress_body(raw: &Value) -> ZitadelIngressInput {
    if looks_like_action_event(raw) {
        return action_event_to_delivery(raw);
    }
    let text = |names: &[&str]| {
        names
            .iter()
            .find_map(|name| raw.get(name)?.as_str())
            .map(str::to_string)
    };
    let list = |name: &str| serde_json::from_value(raw.get(name)?.clone()).ok();
    ZitadelIngressInput {
        delivery_id: text(&["delivery_id", "event_id", "id"]),
        event_type: text(&["event_type", "action_event", "type"]),
        provider_subject: text(&["provider_subject", "user_id", "userId"]),
        user_kind: text(&["user_kind", "kind"]),
        email: text(&["email"]),
        emails: raw
            .get("emails")
            .and_then(|emails| serde_json::from_value(emails.clone()).ok()),
        display_name: text(&["display_name", "displayName"]),
        approval_status: text(&["approval_status"]),
        grants: list("grants"),
        roles: list("roles"),
    }
}

fn action_event_to_delivery(raw: &Value) -> ZitadelIngressInput {
    let aggregate_id = raw
        .get("aggregateID")
        .or_else(|| raw.get("aggregateId"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let event_type = raw
        .get("type")
        .or_else(|| raw.get("eventType"))
        .or_else(|| raw.get("event_type"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let sequence = raw
        .get("sequence")
        .map(|v| match v {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            _ => String::new(),
        })
        .filter(|s| !s.is_empty());
    let delivery_id = match (&aggregate_id, &event_type, &sequence) {
        (Some(a), Some(t), Some(s)) => Some(format!("zitadel-action:{t}:{a}:{s}")),
        (Some(a), Some(t), None) => Some(format!("zitadel-action:{t}:{a}")),
        // A sequence alone does not name an event.
        _ => None,
    };

    let event_payload = raw
        .get("event_payload")
        .or_else(|| raw.get("eventPayload"))
        .or_else(|| raw.get("payload"))
        .cloned()
        .unwrap_or(Value::Null);

    let email = event_payload
        .get("emailAddress")
        .or_else(|| event_payload.get("email"))
        .or_else(|| event_payload.pointer("/email/email"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let display_name = event_payload
        .get("displayName")
        .or_else(|| event_payload.get("display_name"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let user_name = event_payload
        .get("userName")
        .or_else(|| event_payload.get("user_name"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let grants = event_payload
        .get("roleKeys")
        .or_else(|| event_payload.get("role_keys"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty());

    let subject = event_payload
        .get("userId")
        .or_else(|| event_payload.get("userID"))
        .or_else(|| event_payload.get("user_id"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or(aggregate_id);

    let kind = if event_type
        .as_deref()
        .unwrap_or("")
        .to_ascii_lowercase()
        .contains("machine")
    {
        Some("machine".into())
    } else {
        Some("human".into())
    };

    ZitadelIngressInput {
        delivery_id,
        event_type,
        provider_subject: subject,
        user_kind: kind,
        email: email.or(user_name),
        emails: None,
        display_name,
        approval_status: None,
        grants,
        roles: None,
    }
}

pub fn map_action_delivery(input: &ZitadelIngressInput) -> Option<MappedDelivery> {
    let subject = input
        .provider_subject
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())?;
    let event_type = input
        .event_type
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())?;

    let message_name = resolve_message_name(event_type, input.user_kind.as_deref())?;
    let user_kind = if message_name == MACHINE_CREATED {
        "machine".to_string()
    } else {
        match input.user_kind.as_deref().map(str::to_ascii_lowercase) {
            Some(k) if k == "machine" || k == "service" => "machine".into(),
            _ => "human".into(),
        }
    };

    let delivery_id = input
        .delivery_id
        .clone()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| format!("zitadel:{message_name}:{subject}"));

    let emails = normalize_emails(input);
    let approval_status = derive_approval(input, &user_kind);

    let payload = ZitadelUserPayload {
        schema_version: 1,
        source: "zitadel".into(),
        delivery_id: delivery_id.clone(),
        provider: "zitadel".into(),
        provider_subject: subject.to_string(),
        user_kind,
        emails,
        display_name: input.display_name.clone().filter(|s| !s.trim().is_empty()),
        approval_status,
        ingested_at: now_rfc3339ish(),
    };

    Some(MappedDelivery {
        message_name: message_name.to_string(),
        delivery_id,
        payload,
    })
}

fn resolve_message_name(event_type: &str, user_kind: Option<&str>) -> Option<&'static str> {
    let t = event_type.to_ascii_lowercase().replace('_', ".");
    match t.as_str() {
        HUMAN_CREATED | "zitadel.user.human.created" => return Some(HUMAN_CREATED),
        HUMAN_UPDATED | "zitadel.user.human.updated" => return Some(HUMAN_UPDATED),
        HUMAN_DEACTIVATED | "zitadel.user.human.deactivated" => return Some(HUMAN_DEACTIVATED),
        HUMAN_REACTIVATED | "zitadel.user.human.reactivated" => return Some(HUMAN_REACTIVATED),
        MACHINE_CREATED | "zitadel.user.machine.created" => return Some(MACHINE_CREATED),
        _ => {}
    }

    let kind_machine = matches!(
        user_kind.map(str::to_ascii_lowercase).as_deref(),
        Some("machine") | Some("service")
    );

    if t.contains("machine") && (t.contains("created") || t.ends_with(".added")) {
        return Some(MACHINE_CREATED);
    }
    if t.contains("deactivat") || t.contains(".locked") || t.ends_with(".locked") {
        return Some(HUMAN_DEACTIVATED);
    }
    if t.contains("reactivat") || t.contains(".unlocked") || t.ends_with(".unlocked") {
        return Some(HUMAN_REACTIVATED);
    }
    if t.contains("human") && (t.contains("added") || t.contains("created")) {
        return Some(HUMAN_CREATED);
    }
    if t.contains("created")
        || t.contains("create")
        || (t.ends_with(".added") && !t.contains("grant"))
    {
        return if kind_machine {
            Some(MACHINE_CREATED)
        } else {
            Some(HUMAN_CREATED)
        };
    }
    if t.contains("updated")
        || t.contains("update")
        || t.contains("changed")
        || t.contains("grant")
        || t.contains("role")
        || t.contains("profile")
        || t.contains("email")
    {
        return Some(HUMAN_UPDATED);
    }
    None
}

fn normalize_emails(input: &ZitadelIngressInput) -> Vec<ZitadelEmail> {
    if let Some(list) = &input.emails {
        if !list.is_empty() {
            return list
                .iter()
                .map(|e| ZitadelEmail {
                    address: e.address.clone(),
                    primary: e.primary.unwrap_or(false),
                    verified: e.verified.unwrap_or(true),
                })
                .collect();
        }
    }
    if let Some(email) = input.email.as_ref().filter(|s| !s.trim().is_empty()) {
        return vec![ZitadelEmail {
            address: email.clone(),
            primary: true,
            verified: true,
        }];
    }
    Vec::new()
}

fn derive_approval(input: &ZitadelIngressInput, user_kind: &str) -> String {
    if user_kind == "machine" {
        return "approved".into();
    }
    if let Some(status) = input
        .approval_status
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        return status.to_ascii_lowercase();
    }
    let has_approved = input
        .grants
        .iter()
        .flatten()
        .chain(input.roles.iter().flatten())
        .any(|g| g.eq_ignore_ascii_case("approved"));
    if has_approved {
        "approved".into()
    } else {
        "pending".into()
    }
}

fn now_rfc3339ish() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    // Sortable timestamp without chrono dependency.
    format!("{}", d.as_millis())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn maps_human_created_with_waitlist_pending() {
        let input = ZitadelIngressInput {
            delivery_id: Some("d1".into()),
            event_type: Some("user.human.created".into()),
            provider_subject: Some("sub-1".into()),
            user_kind: Some("human".into()),
            email: Some("ada@example.com".into()),
            emails: None,
            display_name: Some("Ada".into()),
            approval_status: None,
            grants: None,
            roles: None,
        };
        let m = map_action_delivery(&input).expect("mapped");
        assert_eq!(m.message_name, HUMAN_CREATED);
        assert_eq!(m.delivery_id, "d1");
        assert_eq!(m.payload.approval_status, "pending");
        assert_eq!(m.payload.user_kind, "human");
        assert_eq!(m.payload.emails[0].address, "ada@example.com");
    }

    #[test]
    fn maps_updated_with_approved_grant() {
        let input = ZitadelIngressInput {
            delivery_id: Some("d2".into()),
            event_type: Some("user.human.updated".into()),
            provider_subject: Some("sub-1".into()),
            user_kind: None,
            email: Some("ada@example.com".into()),
            emails: None,
            display_name: Some("Ada".into()),
            approval_status: None,
            grants: Some(vec!["approved".into()]),
            roles: None,
        };
        let m = map_action_delivery(&input).unwrap();
        assert_eq!(m.message_name, HUMAN_UPDATED);
        assert_eq!(m.payload.approval_status, "approved");
    }

    #[test]
    fn unmapped_type_returns_none() {
        let input = ZitadelIngressInput {
            delivery_id: Some("d3".into()),
            event_type: Some("org.metadata.set".into()),
            provider_subject: Some("sub-1".into()),
            user_kind: None,
            email: None,
            emails: None,
            display_name: None,
            approval_status: None,
            grants: None,
            roles: None,
        };
        assert!(map_action_delivery(&input).is_none());
    }

    #[test]
    fn maps_native_action_event_human_added() {
        let raw = json!({
            "aggregateID": "user-99",
            "aggregateType": "user",
            "sequence": 7,
            "type": "user.human.added",
            "event_payload": {
                "userName": "ada@example.com",
                "emailAddress": "ada@example.com",
                "displayName": "Ada"
            }
        });
        let d = normalize_ingress_body(&raw);
        assert_eq!(d.provider_subject.as_deref(), Some("user-99"));
        let m = map_action_delivery(&d).expect("mapped");
        assert_eq!(m.message_name, HUMAN_CREATED);
        assert_eq!(m.payload.provider_subject, "user-99");
    }
}
