use super::DeliveryError;
use crate::gateway::graphql::{operation_kind, OperationKind, MAX_DOCUMENT_BYTES};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Values established by the authenticated origin, never by a browser scope
/// claim. The cache scope includes subject and all result-affecting authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OriginIdentity {
    /// Origin-selected application surface.
    pub application: String,
    /// Configured origin service identity.
    pub endpoint: String,
    /// Exact schema generation.
    pub schema_hash: String,
    /// Exact protocol generation.
    pub protocol_hash: String,
    /// Origin policy generation.
    pub authorization_generation: String,
    /// Origin-issued subject and authorization scope.
    pub cache_scope: String,
}
impl OriginIdentity {
    /// Validate bounded contract values before accepting external data.
    pub fn validate(&self) -> Result<(), DeliveryError> {
        for part in [
            &self.application,
            &self.endpoint,
            &self.schema_hash,
            &self.protocol_hash,
            &self.authorization_generation,
            &self.cache_scope,
        ] {
            if part.is_empty() || part.len() > 1024 || part.chars().any(char::is_control) {
                return Err(DeliveryError::InvalidContext);
            }
        }
        Ok(())
    }
}

/// Exact operation identity bound to an authenticated origin response.
/// Different HTTP/query and live documents intentionally have different keys.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OperationKey(String);
impl OperationKey {
    /// Call only after origin admission and trusted operation eligibility.
    pub fn from_origin(
        identity: &OriginIdentity,
        request: &serde_json::Value,
    ) -> Result<Self, DeliveryError> {
        identity.validate()?;
        let document = request
            .get("query")
            .and_then(|v| v.as_str())
            .ok_or(DeliveryError::Ineligible)?;
        let name = match request.get("operationName") {
            None | Some(serde_json::Value::Null) => None,
            Some(serde_json::Value::String(name)) => Some(name.as_str()),
            _ => return Err(DeliveryError::Ineligible),
        };
        if !matches!(
            operation_kind(document, name),
            Ok(OperationKind::Query | OperationKind::Subscription)
        ) {
            return Err(DeliveryError::Ineligible);
        }
        let variables = request
            .get("variables")
            .filter(|v| !v.is_null())
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        if !variables.is_object() {
            return Err(DeliveryError::Ineligible);
        }
        let mut extensions = request
            .get("extensions")
            .filter(|v| !v.is_null())
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        // Freshness is checked separately for each consumer; it is not identity.
        let object = extensions
            .as_object_mut()
            .ok_or(DeliveryError::Ineligible)?;
        object.remove("gatewayFreshness");
        object.remove("gatewayDelivery");
        let bytes = canonical_json(&serde_json::json!([
            identity, document, name, variables, extensions
        ]))?;
        Ok(Self(format!("{:x}", Sha256::digest(bytes))))
    }
    /// Stable digest used only after admission.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Bounded canonical JSON: objects sort by key, arrays preserve order.
/// Used for keys only, never a rewrite of the executed GraphQL request.
pub fn canonical_json(value: &serde_json::Value) -> Result<Vec<u8>, DeliveryError> {
    fn ordered(
        value: &serde_json::Value,
        depth: usize,
        budget: &mut usize,
    ) -> Result<serde_json::Value, DeliveryError> {
        if depth > 64 || *budget == 0 {
            return Err(DeliveryError::InvalidContext);
        }
        *budget -= 1;
        Ok(match value {
            serde_json::Value::Object(map) => {
                let sorted: std::collections::BTreeMap<_, _> = map.iter().collect();
                let mut result = serde_json::Map::new();
                for (key, value) in sorted {
                    result.insert(key.clone(), ordered(value, depth + 1, budget)?);
                }
                serde_json::Value::Object(result)
            }
            serde_json::Value::Array(values) => serde_json::Value::Array(
                values
                    .iter()
                    .map(|v| ordered(v, depth + 1, budget))
                    .collect::<Result<_, _>>()?,
            ),
            value => value.clone(),
        })
    }
    let bytes = serde_json::to_vec(&ordered(value, 0, &mut 16384)?)
        .map_err(|_| DeliveryError::InvalidContext)?;
    if bytes.len() > MAX_DOCUMENT_BYTES * 2 {
        return Err(DeliveryError::InvalidContext);
    }
    Ok(bytes)
}
