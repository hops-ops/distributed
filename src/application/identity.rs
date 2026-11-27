use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::error::{ApplicationError, ApplicationResult};

/// A portable logical identity.
///
/// Logical identities intentionally reject path separators, whitespace,
/// control characters, and environment-like syntax. They are names in a
/// manifest, never filesystem paths or runtime endpoints.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct LogicalId(String);

impl LogicalId {
    pub fn try_new(kind: &'static str, value: impl Into<String>) -> ApplicationResult<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(ApplicationError::InvalidIdentity {
                kind,
                value,
                reason: "must not be empty",
            });
        }
        if value.trim() != value {
            return Err(ApplicationError::InvalidIdentity {
                kind,
                value,
                reason: "must not have leading or trailing whitespace",
            });
        }
        if value.starts_with('.') || value.ends_with('.') || value.contains("..") {
            return Err(ApplicationError::InvalidIdentity {
                kind,
                value,
                reason: "must not start, end, or repeat a separator",
            });
        }
        if !value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-' | ':')
        }) {
            return Err(ApplicationError::InvalidIdentity {
                kind,
                value,
                reason: "may contain only ASCII letters, digits, '.', '_', '-', and ':'",
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl AsRef<str> for LogicalId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for LogicalId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Recursively sort JSON objects so an IR value has one portable encoding.
pub fn canonical_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(canonical_json).collect())
        }
        serde_json::Value::Object(values) => {
            let mut sorted = serde_json::Map::new();
            for (key, value) in values {
                sorted.insert(key.clone(), canonical_json(value));
            }
            serde_json::Value::Object(sorted)
        }
        other => other.clone(),
    }
}

/// Compute a domain-separated SHA-256 fingerprint for portable bytes.
pub fn sha256_fingerprint(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"distributed.application-artifact.v1\0");
    digest.update(bytes);
    format!("sha256:{:x}", digest.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_ids_accept_portable_names_and_reject_each_other_shape() {
        for value in ["todo", "todo.create", "a-b_c:d", "V2"] {
            let id = LogicalId::try_new("command", value).unwrap();
            assert_eq!(id.as_str(), value);
            assert_eq!(id.to_string(), value);
            assert_eq!(id.as_ref(), value);
        }
        let cases = [
            ("empty", "", "must not be empty"),
            (
                "padded",
                " todo",
                "must not have leading or trailing whitespace",
            ),
            (
                "leading separator",
                ".todo",
                "must not start, end, or repeat a separator",
            ),
            (
                "trailing separator",
                "todo.",
                "must not start, end, or repeat a separator",
            ),
            (
                "repeated separator",
                "todo..create",
                "must not start, end, or repeat a separator",
            ),
            (
                "path",
                "todo/create",
                "may contain only ASCII letters, digits, '.', '_', '-', and ':'",
            ),
            (
                "environment syntax",
                "${TODO}",
                "may contain only ASCII letters, digits, '.', '_', '-', and ':'",
            ),
            (
                "inner space",
                "todo create",
                "may contain only ASCII letters, digits, '.', '_', '-', and ':'",
            ),
        ];
        for (name, value, reason) in cases {
            assert_eq!(
                LogicalId::try_new("command", value).unwrap_err(),
                ApplicationError::InvalidIdentity {
                    kind: "command",
                    value: value.into(),
                    reason,
                },
                "{name}"
            );
        }
    }
}
