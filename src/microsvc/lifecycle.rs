//! Supervisor-owned membership, independent of a process's startup generation.
//! No compatibility-only shortcut: a retained process must be explicitly named
//! by its launch identity in the active cohort. This is dev coordination, not
//! a security boundary against an operator who can edit the lifecycle directory.
use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

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
    #[cfg(any(feature = "graphql", test))]
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
        #[cfg(any(feature = "graphql", test))]
        generation,
        mutations_open: state.phase == "active",
    })
}

/// The supervisor's membership file and this process's launch identity in it.
struct MembershipFile {
    state: std::path::PathBuf,
    member_id: String,
    instance_id: String,
    startup: String,
}

impl MembershipFile {
    /// `None` unless the supervisor's directory and this process's identity
    /// are all present and well formed.
    fn from_env() -> Option<Self> {
        let root = std::path::PathBuf::from(std::env::var_os("DISTRIBUTED_LIFECYCLE_DIR")?);
        if !root.is_absolute() {
            return None;
        }
        Some(Self {
            state: root.join("dev.json"),
            member_id: std::env::var("DISTRIBUTED_MEMBER_ID").ok()?,
            instance_id: std::env::var("DISTRIBUTED_PROCESS_INSTANCE_ID").ok()?,
            startup: std::env::var("DISTRIBUTED_GENERATION_ID").ok()?,
        })
    }

    fn read(&self) -> Option<Membership> {
        let metadata = std::fs::symlink_metadata(&self.state).ok()?;
        if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > 1024 * 1024
        {
            return None;
        }
        // Bound the actual read too, even if the file changes after metadata lookup.
        use std::io::Read;
        let mut source = Vec::new();
        std::fs::File::open(&self.state)
            .ok()?
            .take(1024 * 1024 + 1)
            .read_to_end(&mut source)
            .ok()?;
        resolve(&source, &self.member_id, &self.instance_id, &self.startup)
    }

    fn mutations_open(&self) -> bool {
        self.read().is_some_and(|member| member.mutations_open)
    }
}

#[cfg(feature = "graphql")]
pub(crate) fn membership_from_environment() -> Option<Membership> {
    MembershipFile::from_env()?.read()
}

/// How often a supervised [`LifecycleGate`] re-reads the membership file, and
/// so how stale its answer may be.
const REFRESH: Duration = Duration::from_millis(50);

/// Whether this process may accept writes.
///
/// `distributed dev` runs an application as generations and tells each
/// process, through a membership file, whether it belongs to the active one.
/// A process outside it (a generation being replaced, or a candidate not yet
/// activated) must not write. Resolve the gate once where the process
/// composes its services and hand it to each with
/// [`Service::with_lifecycle`](crate::microsvc::Service::with_lifecycle):
///
/// ```ignore
/// let lifecycle = LifecycleGate::from_env();
/// let service = build_service().with_lifecycle(lifecycle.clone());
/// ```
///
/// The gate answers from memory. A supervised gate follows the membership
/// file from a background thread every 50 ms, so a demoted process may
/// accept a write for up to that long after the supervisor fenced it, and a
/// promoted one refuses for up to that long. The supervisor waits at least
/// 500 ms after fencing before it replaces a process.
///
/// This is dev coordination, not a security boundary.
#[derive(Clone, Debug, Default)]
pub struct LifecycleGate {
    /// `None` when no supervisor governs this process.
    open: Option<Arc<AtomicBool>>,
}

impl LifecycleGate {
    /// A gate that never closes: the process is not run by a supervisor.
    pub fn always_open() -> Self {
        Self::default()
    }

    /// The gate the environment asks for. Without `DISTRIBUTED_LIFECYCLE_DIR`
    /// (anywhere but under `distributed dev`) it is [`Self::always_open`].
    /// With it, the gate follows `dev.json` in that directory for the member
    /// named by `DISTRIBUTED_MEMBER_ID`, `DISTRIBUTED_PROCESS_INSTANCE_ID`
    /// and `DISTRIBUTED_GENERATION_ID`, and stays closed if any of them is
    /// missing or malformed.
    ///
    /// Reads the environment and the file once, here; call it once per
    /// process and clone the result.
    pub fn from_env() -> Self {
        if std::env::var_os("DISTRIBUTED_LIFECYCLE_DIR").is_none() {
            return Self::always_open();
        }
        match MembershipFile::from_env() {
            Some(file) => Self::watch(file, REFRESH),
            None => Self::closed(),
        }
    }

    /// A gate that never opens.
    pub(crate) fn closed() -> Self {
        Self {
            open: Some(Arc::new(AtomicBool::new(false))),
        }
    }

    /// Follow `file` every `refresh` until the last clone is dropped.
    fn watch(file: MembershipFile, refresh: Duration) -> Self {
        let open = Arc::new(AtomicBool::new(file.mutations_open()));
        let cached = Arc::downgrade(&open);
        let follower = std::thread::Builder::new()
            .name("distributed-lifecycle".into())
            .spawn(move || loop {
                std::thread::sleep(refresh);
                let Some(open) = cached.upgrade() else {
                    return;
                };
                open.store(file.mutations_open(), Ordering::Relaxed);
            });
        if follower.is_err() {
            // Nothing follows the supervisor: an open gate would never close.
            open.store(false, Ordering::Relaxed);
        }
        Self { open: Some(open) }
    }

    /// Whether writes are admitted. Never blocks and never touches the
    /// filesystem.
    pub fn mutations_open(&self) -> bool {
        self.open
            .as_ref()
            .is_none_or(|open| open.load(Ordering::Relaxed))
    }

    /// Whether `other` is a clone of this gate.
    #[cfg(feature = "graphql")]
    pub(crate) fn is(&self, other: &Self) -> bool {
        match (&self.open, &other.open) {
            (None, None) => true,
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }
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

    /// A supervisor's lifecycle directory holding `dev.json` for member
    /// `api`, launched as `api-one` in generation `one`.
    struct Supervisor(std::path::PathBuf);

    impl Supervisor {
        fn new(phase: &str) -> Self {
            let root = std::env::temp_dir().join(format!("lifecycle-{}", uuid::Uuid::now_v7()));
            std::fs::create_dir_all(&root).unwrap();
            let supervisor = Self(root);
            supervisor.write(phase);
            supervisor
        }

        /// Replace `dev.json` the way the supervisor does: write, then rename.
        fn write(&self, phase: &str) {
            let staged = self.0.join("dev.json.next");
            let value = state(phase, "one", "api-one", "one");
            std::fs::write(&staged, serde_json::to_vec(&value).unwrap()).unwrap();
            std::fs::rename(staged, self.0.join("dev.json")).unwrap();
        }

        fn gate(&self, refresh: Duration) -> LifecycleGate {
            LifecycleGate::watch(
                MembershipFile {
                    state: self.0.join("dev.json"),
                    member_id: "api".into(),
                    instance_id: "api-one".into(),
                    startup: "one".into(),
                },
                refresh,
            )
        }
    }

    impl Drop for Supervisor {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn wait_until(gate: &LifecycleGate, open: bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while gate.mutations_open() != open {
            assert!(
                std::time::Instant::now() < deadline,
                "the gate never became open={open}"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn a_process_without_a_supervisor_is_always_open() {
        assert!(LifecycleGate::always_open().mutations_open());
        assert!(LifecycleGate::default().mutations_open());
        assert!(!LifecycleGate::closed().mutations_open());
    }

    #[test]
    fn a_supervised_gate_follows_the_membership_file() {
        let supervisor = Supervisor::new("active");
        let gate = supervisor.gate(Duration::from_millis(2));
        let clone = gate.clone();
        assert!(gate.mutations_open(), "the first read is synchronous");

        supervisor.write("preparing");
        wait_until(&gate, false);
        assert!(!clone.mutations_open(), "clones share one answer");

        supervisor.write("active");
        wait_until(&gate, true);

        std::fs::remove_file(supervisor.0.join("dev.json")).unwrap();
        wait_until(&gate, false);
    }

    #[test]
    fn a_gate_starts_closed_for_a_process_that_is_not_a_member() {
        let supervisor = Supervisor::new("preparing");
        assert!(!supervisor.gate(Duration::from_secs(3600)).mutations_open());
    }

    /// The refresh interval is an hour, so nothing re-reads the file during
    /// the test: every answer comes from memory. A dispatch that consulted
    /// the filesystem would see the file the test has just rewritten.
    #[tokio::test]
    async fn a_closed_gate_refuses_a_write_without_reading_the_membership_file() {
        use crate::command::{typed_command, PreparedCommand, Succeeded};
        use crate::command_dispatch::VerifiedPrincipal;
        use crate::microsvc::{
            CausalCommandContext, CausalDispatchError, HandlerError, Routes, Service, Session,
        };

        #[derive(serde::Deserialize, crate::command::CommandInput)]
        struct Ping {}
        #[derive(serde::Serialize, crate::command::CommandOutput)]
        struct Pong {
            pong: bool,
        }

        let service = |gate: &LifecycleGate| {
            let repository = crate::InMemoryRepository::new();
            Service::new()
                .named("lifecycle")
                .routes(
                    Routes::for_stores(repository.clone(), repository)
                        .typed_command(typed_command::<Ping, Succeeded<Pong>>("ping"))
                        .handle(|_ctx: &CausalCommandContext<'_, ()>, _input: Ping| async {
                            Ok(PreparedCommand::prepare(Pong { pong: true }).unwrap())
                        }),
                )
                .with_lifecycle(gate.clone())
        };
        let ping = |service: Service| async move {
            service
                .dispatch_causal(
                    "ping",
                    &uuid::Uuid::now_v7().hyphenated().to_string(),
                    json!({}),
                    Session::new(),
                    VerifiedPrincipal::internal("lifecycle-test").unwrap(),
                )
                .await
        };
        let never = Duration::from_secs(3600);

        let demoted = Supervisor::new("preparing");
        let gate = demoted.gate(never);
        demoted.write("active");
        let refused = ping(service(&gate)).await;
        assert!(matches!(
            refused,
            Err(CausalDispatchError::Handler(
                HandlerError::ApplicationReloading
            ))
        ));

        let active = Supervisor::new("active");
        let gate = active.gate(never);
        active.write("preparing");
        let accepted = ping(service(&gate)).await;
        assert_eq!(accepted.unwrap(), json!({ "pong": true }));
    }
}
