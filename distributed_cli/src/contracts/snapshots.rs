//! Canonical semantic snapshots for lifecycle review.
//!
//! Snapshots capture behavior-affecting material as reviewable paths and values
//! rather than opaque hashes alone. Absolute paths, timestamps, environment
//! values, and secrets are excluded.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Maximum number of snapshot paths retained for one artifact.
pub const MAX_SNAPSHOT_PATHS: usize = 16_384;
/// Maximum depth when walking nested JSON.
pub const MAX_SNAPSHOT_DEPTH: usize = 32;
/// Maximum string value bytes retained in a snapshot entry.
pub const MAX_SNAPSHOT_VALUE_BYTES: usize = 4_096;

/// One canonical semantic path and its reviewable value.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotEntry {
    /// Dot-path into the semantic material (stable, sorted).
    pub path: String,
    /// JSON value at the path (objects already flattened into child paths).
    pub value: Value,
}

/// Deterministic semantic snapshot for one artifact owner.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticSnapshot {
    /// Semantic owner identity (spec/source path or catalog entry id).
    pub owner: String,
    /// Artifact kind label (surface, application_manifest, deployment_plan, …).
    pub kind: String,
    /// Sorted path inventory.
    pub entries: Vec<SnapshotEntry>,
    /// Content digest of the canonical snapshot bytes.
    pub digest: String,
}

/// Diff between two snapshots.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotDiff {
    pub owner: String,
    pub kind: String,
    pub changes: Vec<SnapshotChange>,
}

/// One path-level change requiring a lifecycle decision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotChange {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<Value>,
}

/// Build a semantic snapshot from arbitrary JSON by flattening into sorted paths.
pub fn snapshot_from_json(
    owner: impl Into<String>,
    kind: impl Into<String>,
    value: &Value,
) -> Result<SemanticSnapshot, String> {
    let owner = owner.into();
    let kind = kind.into();
    let mut paths = BTreeMap::new();
    flatten_value("", value, 0, &mut paths)?;
    if paths.len() > MAX_SNAPSHOT_PATHS {
        return Err(format!(
            "snapshot for `{owner}` exceeds max path count {MAX_SNAPSHOT_PATHS}"
        ));
    }
    let entries = paths
        .into_iter()
        .map(|(path, value)| SnapshotEntry { path, value })
        .collect::<Vec<_>>();
    let digest = digest_entries(&entries);
    Ok(SemanticSnapshot {
        owner,
        kind,
        entries,
        digest,
    })
}

/// Diff two snapshots of the same owner/kind.
pub fn diff_snapshots(before: &SemanticSnapshot, after: &SemanticSnapshot) -> SnapshotDiff {
    let mut changes = Vec::new();
    let mut before_map = before
        .entries
        .iter()
        .map(|entry| (entry.path.as_str(), &entry.value))
        .collect::<BTreeMap<_, _>>();
    for entry in &after.entries {
        match before_map.remove(entry.path.as_str()) {
            Some(previous) if previous == &entry.value => {}
            Some(previous) => changes.push(SnapshotChange {
                path: entry.path.clone(),
                before: Some(previous.clone()),
                after: Some(entry.value.clone()),
            }),
            None => changes.push(SnapshotChange {
                path: entry.path.clone(),
                before: None,
                after: Some(entry.value.clone()),
            }),
        }
    }
    for (path, previous) in before_map {
        changes.push(SnapshotChange {
            path: path.to_string(),
            before: Some(previous.clone()),
            after: None,
        });
    }
    changes.sort_by(|left, right| left.path.cmp(&right.path));
    SnapshotDiff {
        owner: after.owner.clone(),
        kind: after.kind.clone(),
        changes,
    }
}

fn flatten_value(
    prefix: &str,
    value: &Value,
    depth: usize,
    out: &mut BTreeMap<String, Value>,
) -> Result<(), String> {
    if depth > MAX_SNAPSHOT_DEPTH {
        return Err(format!(
            "snapshot depth exceeds maximum {MAX_SNAPSHOT_DEPTH} at `{prefix}`"
        ));
    }
    match value {
        Value::Object(fields) => {
            // Skip volatile / non-semantic keys.
            for (key, child) in fields {
                if is_volatile_key(key) {
                    continue;
                }
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                flatten_value(&path, child, depth + 1, out)?;
            }
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                let path = format!("{prefix}[{index}]");
                flatten_value(&path, child, depth + 1, out)?;
            }
        }
        Value::String(text) if text.len() > MAX_SNAPSHOT_VALUE_BYTES => {
            out.insert(
                prefix.to_string(),
                Value::String(format!("<redacted:string:len={}>", text.len())),
            );
        }
        other => {
            out.insert(prefix.to_string(), other.clone());
        }
    }
    Ok(())
}

fn is_volatile_key(key: &str) -> bool {
    matches!(
        key,
        "generated_at"
            | "timestamp"
            | "wall_time"
            | "absolute_path"
            | "cwd"
            | "hostname"
            | "env"
            | "environment"
            | "secret"
            | "password"
            | "token"
            | "connection_string"
    )
}

fn digest_entries(entries: &[SnapshotEntry]) -> String {
    let bytes = serde_json::to_vec(entries).unwrap_or_default();
    let mut digest = Sha256::new();
    digest.update(b"distributed.contract.snapshot.v1\0");
    digest.update(&bytes);
    format!("sha256:{:x}", digest.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn canonical_snapshot_ignores_unordered_input_but_detects_nullability() {
        let left = snapshot_from_json(
            "surface/web",
            "surface_client_manifest",
            &json!({
                "models": {
                    "TodoView": { "fields": { "title": { "nullable": false }, "id": { "nullable": false } } }
                },
                "generated_at": "2026-01-01T00:00:00Z"
            }),
        )
        .unwrap();
        let right = snapshot_from_json(
            "surface/web",
            "surface_client_manifest",
            &json!({
                "models": {
                    "TodoView": { "fields": { "id": { "nullable": false }, "title": { "nullable": false } } }
                }
            }),
        )
        .unwrap();
        assert_eq!(left.digest, right.digest);
        assert!(diff_snapshots(&left, &right).changes.is_empty());

        let drifted = snapshot_from_json(
            "surface/web",
            "surface_client_manifest",
            &json!({
                "models": {
                    "TodoView": { "fields": { "id": { "nullable": false }, "title": { "nullable": true } } }
                }
            }),
        )
        .unwrap();
        let changes = diff_snapshots(&left, &drifted).changes;
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].path, "models.TodoView.fields.title.nullable");
        assert_eq!(changes[0].before, Some(json!(false)));
        assert_eq!(changes[0].after, Some(json!(true)));
    }
}
