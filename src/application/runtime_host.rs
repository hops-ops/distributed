//! Framework runtime host skeleton (task 12).
//!
//! Realizes one process entry from a validated [`DeploymentPlan`] by requiring
//! the explained capability set and an explicit [`CommandDispatcher`]. Full
//! adapter bootstrap (stores, workers, supervision) continues to grow here;
//! application code must not hand-pair dialect runners.

use super::capability::Capability;
use super::error::{ApplicationError, ApplicationResult};
use super::plan::{DeploymentPlan, ProcessPlan};
use crate::command_dispatch::SharedCommandDispatcher;
use std::collections::BTreeSet;

/// Provider-backed capabilities available to the host at bind time.
#[derive(Clone, Debug, Default)]
pub struct CapabilityProviders {
    pub available: BTreeSet<Capability>,
}

impl CapabilityProviders {
    pub fn with(mut self, capability: Capability) -> Self {
        self.available.insert(capability);
        self
    }

    pub fn contains(&self, capability: Capability) -> bool {
        self.available.contains(&capability)
    }
}

/// One bound process ready for supervision / serve (task 12 expansion point).
pub struct RuntimeHost {
    pub plan_name: String,
    pub process: ProcessPlan,
    pub dispatcher: Option<SharedCommandDispatcher>,
    pub providers: CapabilityProviders,
}

impl RuntimeHost {
    /// Bind one process from a validated plan against available providers.
    ///
    /// Missing required capabilities fail closed before serve.
    pub fn bind(
        plan: &DeploymentPlan,
        process_id: &str,
        providers: CapabilityProviders,
        dispatcher: Option<SharedCommandDispatcher>,
    ) -> ApplicationResult<Self> {
        plan.validate()?;
        let process = plan
            .processes
            .iter()
            .find(|process| process.id == process_id)
            .cloned()
            .ok_or_else(|| ApplicationError::Missing {
                kind: "process",
                identity: process_id.to_string(),
            })?;

        for requirement in &process.capabilities {
            if !providers.contains(requirement.capability) {
                return Err(ApplicationError::InvalidSpec(format!(
                    "process `{process_id}` requires capability `{}` but no provider is bound",
                    requirement.capability.as_str()
                )));
            }
        }

        let needs_dispatch = process.mounts.iter().any(|mount| {
            matches!(
                mount,
                super::mount::MountSelector::Command { .. }
                    | super::mount::MountSelector::Surface { .. }
            )
        });
        if needs_dispatch && dispatcher.is_none() {
            return Err(ApplicationError::InvalidSpec(format!(
                "process `{process_id}` requires a CommandDispatcher for command/surface mounts"
            )));
        }

        Ok(Self {
            plan_name: plan.name.clone(),
            process,
            dispatcher,
            providers,
        })
    }

    pub fn process_id(&self) -> &str {
        &self.process.id
    }

    pub fn dispatcher(&self) -> Option<&SharedCommandDispatcher> {
        self.dispatcher.as_ref()
    }
}

/// Convenience: bind the sole process when a plan has exactly one entry.
pub fn bind_single_process(
    plan: &DeploymentPlan,
    providers: CapabilityProviders,
    dispatcher: Option<SharedCommandDispatcher>,
) -> ApplicationResult<RuntimeHost> {
    if plan.processes.len() != 1 {
        return Err(ApplicationError::InvalidSpec(
            "bind_single_process requires exactly one process in the plan".into(),
        ));
    }
    RuntimeHost::bind(plan, &plan.processes[0].id, providers, dispatcher)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::{
        compile_deployment_plan, Application, CommandDefinition, CommandSpec, CommandTypeSpec,
        ModelFieldSpec, ModelSpec, Module, ProcessIntent, ProcessPreset, ProjectionSpec,
    };
    use crate::command::CommandConsistency;
    use std::sync::Arc;

    fn manifest() -> crate::application::ApplicationManifest {
        let model = ModelSpec::try_new(
            "TodoView",
            "todos",
            [ModelFieldSpec {
                name: "todo_id".into(),
                scalar: "String".into(),
                nullable: false,
            }],
            ["todo_id"],
        )
        .unwrap();
        let command = CommandSpec::try_new(
            "todo.create",
            "todo_create",
            CommandTypeSpec {
                name: "In".into(),
                fields: vec![],
            },
            CommandTypeSpec {
                name: "Out".into(),
                fields: vec![],
            },
            CommandConsistency::Eventual,
        )
        .unwrap();
        let module = Module::new("todo")
            .command_definitions([CommandDefinition::contract(command)])
            .models([model])
            .projections([
                ProjectionSpec::try_new("project_todos", ["todo.created"], ["TodoView"]).unwrap(),
            ])
            .build()
            .unwrap();
        Application::new("todo-app")
            .module(module)
            .build()
            .unwrap()
            .manifest()
            .clone()
    }

    #[test]
    fn host_fails_closed_without_required_capability() {
        let manifest = manifest();
        let plan = compile_deployment_plan(
            "local",
            &manifest,
            [ProcessIntent::with_preset("full", &manifest, ProcessPreset::Writer).unwrap()],
        )
        .unwrap();
        let err = match RuntimeHost::bind(&plan, "full", CapabilityProviders::default(), None) {
            Ok(_) => panic!("expected missing capability failure"),
            Err(error) => error,
        };
        assert!(err.to_string().contains("capability"));
    }

    #[test]
    fn host_binds_when_capabilities_and_dispatcher_present() {
        use crate::command_dispatch::{CommandDispatchError, CommandDispatcher};
        use crate::microsvc::{CommandRequest, CommandResponse};
        use async_trait::async_trait;

        struct Stub;
        #[async_trait]
        impl CommandDispatcher for Stub {
            async fn dispatch(
                &self,
                _request: &CommandRequest,
            ) -> Result<CommandResponse, CommandDispatchError> {
                Ok(CommandResponse {
                    status: 200,
                    body: serde_json::json!({}),
                })
            }
            fn kind(&self) -> &'static str {
                "stub"
            }
        }

        let manifest = manifest();
        let plan = compile_deployment_plan(
            "local",
            &manifest,
            [ProcessIntent::with_preset("full", &manifest, ProcessPreset::Writer).unwrap()],
        )
        .unwrap();
        let mut providers = CapabilityProviders::default();
        for requirement in &plan.processes[0].capabilities {
            providers = providers.with(requirement.capability);
        }
        let host = RuntimeHost::bind(
            &plan,
            "full",
            providers,
            Some(Arc::new(Stub) as SharedCommandDispatcher),
        )
        .unwrap();
        assert_eq!(host.process_id(), "full");
        assert!(host.dispatcher().is_some());
    }
}
