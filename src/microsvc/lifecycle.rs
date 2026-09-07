//! Supervisor-owned membership, independent of a process's startup generation.
//! No compatibility-only shortcut: a retained process must be explicitly named
//! by its launch identity in the active cohort. This is dev coordination, not
//! a security boundary against an operator who can edit the lifecycle directory.
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ActiveGeneration {
    pub generation_id: String,
    pub release_id: String,
    pub topology_id: String,
    pub compatibility_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Member {
    instance_id: String,
    generation_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct State {
    schema_version: u32,
    phase: String,
    active: ActiveGeneration,
    members: BTreeMap<String, Member>,
}

pub(crate) struct Membership {
    pub generation: ActiveGeneration,
    pub mutations_open: bool,
}

fn identity(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 512
        && value == value.trim()
        && !value.chars().any(char::is_control)
}

fn resolve(source: &[u8], member_id: &str, instance_id: &str, startup: &str) -> Option<Membership> {
    if source.len() > 1024 * 1024 || ![member_id, instance_id, startup].into_iter().all(identity) {
        return None;
    }
    let state: State = serde_json::from_slice(source).ok()?;
    if state.schema_version != 1
        || !matches!(state.phase.as_str(), "active" | "preparing")
        || state.members.len() > 64
    {
        return None;
    }
    let member = state.members.get(member_id)?;
    if member.instance_id != instance_id || member.generation_id != startup {
        return None;
    }
    let generation = state.active;
    if ![
        &generation.generation_id,
        &generation.release_id,
        &generation.topology_id,
        &generation.compatibility_id,
    ]
    .into_iter()
    .all(|value| identity(value))
    {
        return None;
    }
    Some(Membership {
        generation,
        mutations_open: state.phase == "active",
    })
}

pub(crate) fn membership_from_environment() -> Option<Membership> {
    let root = std::path::PathBuf::from(std::env::var_os("DISTRIBUTED_LIFECYCLE_DIR")?);
    if !root.is_absolute() {
        return None;
    }
    let state = root.join("dev.json");
    let metadata = std::fs::symlink_metadata(&state).ok()?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > 1024 * 1024 {
        return None;
    }
    // Bound the actual read too, even if the file changes after metadata lookup.
    use std::io::Read;
    let mut source = Vec::new();
    std::fs::File::open(state)
        .ok()?
        .take(1024 * 1024 + 1)
        .read_to_end(&mut source)
        .ok()?;
    resolve(
        &source,
        &std::env::var("DISTRIBUTED_MEMBER_ID").ok()?,
        &std::env::var("DISTRIBUTED_PROCESS_INSTANCE_ID").ok()?,
        &std::env::var("DISTRIBUTED_GENERATION_ID").ok()?,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn state(phase: &str, active: &str, instance: &str, startup: &str) -> serde_json::Value {
        json!({"schemaVersion":1,"phase":phase,"active":{
            "generationId":active,"releaseId":format!("release:{active}"),
            "topologyId":"topology","compatibilityId":"compatibility"},
            "members":{"api":{"instanceId":instance,"generationId":startup}}})
    }
    fn check(value: &serde_json::Value, instance: &str, startup: &str) -> Option<Membership> {
        resolve(
            &serde_json::to_vec(value).unwrap(),
            "api",
            instance,
            startup,
        )
    }
    #[test]
    fn retained_member_follows_active_generation_without_restart() {
        for active in ["one", "two", "three"] {
            let resolved =
                check(&state("active", active, "api-one", "one"), "api-one", "one").unwrap();
            assert!(resolved.mutations_open);
            assert_eq!(resolved.generation.generation_id, active);
            assert_eq!(resolved.generation.release_id, format!("release:{active}"));
        }
    }
    #[test]
    fn preparing_preserves_old_reader_identity_but_fences_all_writes() {
        let value = state("preparing", "one", "api-one", "one");
        let old = check(&value, "api-one", "one").unwrap();
        assert!(!old.mutations_open);
        assert_eq!(old.generation.generation_id, "one");
        assert!(check(&value, "candidate", "two").is_none());
    }
    #[test]
    fn replacement_and_rollback_retire_old_launches_even_with_identical_generations() {
        for (active, instance, startup) in [("two", "api-two", "two"), ("one", "rollback", "one")] {
            let value = state("active", active, instance, startup);
            assert!(check(&value, instance, startup).unwrap().mutations_open);
            assert!(check(&value, "api-one", "one").is_none());
            assert!(check(&value, "api-two", "one").is_none());
        }
    }
    #[test]
    fn missing_malformed_unknown_and_retired_members_fail_closed() {
        let original = state("active", "one", "api-one", "one");
        for key in ["schemaVersion", "phase", "active", "members"] {
            let mut value = original.clone();
            value.as_object_mut().unwrap().remove(key);
            assert!(check(&value, "api-one", "one").is_none());
        }
        for phase in ["stopped", "failed", ""] {
            assert!(check(&state(phase, "one", "api-one", "one"), "api-one", "one").is_none());
        }
        let mut retired = original.clone();
        retired["members"] = json!({});
        assert!(check(&retired, "api-one", "one").is_none());
        let mut malformed = original;
        malformed["active"]["releaseId"] = json!("\ninvalid");
        assert!(check(&malformed, "api-one", "one").is_none());
        assert!(resolve(b"not json", "api", "api-one", "one").is_none());
    }
}
