use std::any::TypeId;
use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;

use serde::de::DeserializeOwned;
use serde::Serialize;
use sha2::{Digest, Sha256};

use super::direct_projection::{
    CommandDirectProjectionTarget, CommandProjectedModel, CompiledDirectProjectionTarget,
    DirectProjectionTargetResolutionError, ResolvedDirectProjectionTarget,
};
use super::effect_wire::{CommandInputDefault, CompiledInputDefaults};
use super::outcomes::{Atomic, CommandConsistency, CommandOutcome};
use super::projection_proof::{canonical_json, CommandCommitProofError};
use super::projections::{
    CommandProjectionEvents, CommandProjectionPreview, CommandProjectionPureReduce,
};
use super::types::{CommandInputType, CommandTypeDef};
use crate::microsvc::Session;
use crate::projection_protocol::ProjectionEpoch;
use crate::read_model::RelationalReadModel;

#[derive(Clone, Debug)]
pub(crate) struct TypedCommandContract {
    pub name: String,
    pub field_name: String,
    pub roles: Vec<String>,
    pub input: CommandTypeDef,
    pub output: CommandTypeDef,
    pub input_type_id: TypeId,
    pub output_type_id: TypeId,
    pub consistency: CommandConsistency,
    pub input_defaults: Vec<CommandInputDefault>,
    /// Present automatically for `Atomic<M>` before Surface ownership is
    /// resolved. This never requires an application declaration.
    pub projected_model: Option<CommandProjectedModel>,
    pub direct_projection: Option<CommandDirectProjectionTarget>,
    /// Exact outward event contracts, independent of whichever projectors
    /// happen to consume them in this deployment.
    pub projections: CommandProjectionEvents,
}

impl TypedCommandContract {
    /// Stable per-route identity used in the command ledger fingerprint.
    ///
    /// This is intentionally distinct from the service inventory digest: a
    /// deployment may add unrelated routes without invalidating safe retries
    /// for this command. The explicit domain/version prevents this byte digest
    /// from aliasing any other SHA-256 use in the framework.
    pub(crate) fn fingerprint_bytes(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"distributed.typed-command-contract.v1");
        digest.update([0]);
        digest.update(
            serde_json::to_vec(&self.canonical_value())
                .expect("canonical typed command contract serialization cannot fail"),
        );
        digest.finalize().into()
    }

    pub(crate) fn canonical_value(&self) -> serde_json::Value {
        let mut roles = self.roles.clone();
        roles.sort();
        roles.dedup();
        let mut input_defaults = self.input_defaults.clone();
        input_defaults.sort_by(|left, right| left.path.cmp(&right.path));
        let direct_projection = self
            .direct_projection
            .as_ref()
            .map(CommandDirectProjectionTarget::canonical_value);
        let projected_model = self
            .projected_model
            .as_ref()
            .map(CommandProjectedModel::canonical_value);
        let mut projections = self.projections.clone();
        projections
            .canonicalize_and_validate(&self.name)
            .expect("validated command projection declarations are canonical");
        canonical_json(&serde_json::json!({
            "name": self.name,
            "field_name": self.field_name,
            "roles": roles,
            "input": canonical_command_type(&self.input),
            "output": canonical_command_type(&self.output),
            "consistency": self.consistency,
            "input_defaults": input_defaults,
            "projected_model": projected_model,
            "direct_projection": direct_projection,
            "projections": projections,
        }))
    }

    pub(crate) fn resolve_direct_projection_target_from_session(
        &self,
        canonical_wire_input: &serde_json::Value,
        session: Option<&Session>,
    ) -> Result<Option<ResolvedDirectProjectionTarget>, DirectProjectionTargetResolutionError> {
        self.direct_projection
            .as_ref()
            .map(|target| target.resolve(canonical_wire_input, session))
            .transpose()
    }

    /// An eventual command must declare the outward events its projections
    /// are driven by.
    pub(super) fn validate_declares_projection_events(
        &self,
    ) -> Result<(), CommandCommitProofError> {
        if self.consistency == CommandConsistency::Eventual && self.projections.selectors.is_empty()
        {
            return Err(CommandCommitProofError::EventualHasNoProjectionSelectors);
        }
        Ok(())
    }
}

fn canonical_command_type(definition: &CommandTypeDef) -> serde_json::Value {
    let mut fields = definition.fields.iter().collect::<Vec<_>>();
    fields.sort_by(|left, right| left.name.cmp(&right.name));
    serde_json::json!({
        "name": definition.name,
        "fields": fields.into_iter().map(|field| {
            let mut value = serde_json::json!({
            "name": field.name,
            "type_name": field.type_name,
            "nullable": field.nullable,
            "list": field.list,
            "item_nullable": field.item_nullable,
            "nested": field.nested.as_deref().map(canonical_command_type),
            });
            if let Some(unsigned) = field.unsigned_integer {
                value["unsigned_integer"] = serde_json::json!(unsigned);
            }
            value
        }).collect::<Vec<_>>(),
    })
}

/// Stable command inventory identity shared by a service and GraphQL engine.
///
/// The digest covers canonical wire structure while the non-serializable
/// `TypeId` pairs prove that both sides were built from the exact Rust input
/// and output types, not merely lookalike GraphQL shapes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TypedServiceCommandBinding {
    pub service_id: String,
    pub structural_fingerprint: String,
    pub types: BTreeMap<String, (TypeId, TypeId)>,
}

impl TypedServiceCommandBinding {
    pub(crate) fn from_contracts(
        service_id: &str,
        contracts: &[TypedCommandContract],
    ) -> Result<Self, String> {
        if service_id.trim().is_empty() {
            return Err("typed command inventory requires a non-empty service ID".into());
        }

        let mut seen = BTreeSet::new();
        let mut ordered = contracts.iter().collect::<Vec<_>>();
        ordered.sort_by(|left, right| left.name.cmp(&right.name));
        let mut types = BTreeMap::new();
        let mut canonical = Vec::with_capacity(ordered.len());
        for contract in ordered {
            let mut projections = contract.projections.clone();
            projections.canonicalize_and_validate(&contract.name)?;
            if contract.name.trim().is_empty() {
                return Err("typed command id must not be empty".into());
            }
            if !seen.insert(contract.name.clone()) {
                return Err(format!(
                    "duplicate typed command declaration for `{}`",
                    contract.name
                ));
            }
            if contract.input.type_id != Some(contract.input_type_id) {
                return Err(format!(
                    "typed command `{}` input command metadata is missing or has a different Rust TypeId",
                    contract.name
                ));
            }
            if contract.output.type_id != Some(contract.output_type_id) {
                return Err(format!(
                    "typed command `{}` output command metadata is missing or has a different Rust TypeId",
                    contract.name
                ));
            }
            match contract.consistency {
                CommandConsistency::Eventual if contract.projections.selectors.is_empty() => {
                    return Err(format!(
                        "typed causal command `{}` must declare at least one expected projector confirmation",
                        contract.name
                    ));
                }
                CommandConsistency::Atomic if contract.projected_model.is_none() => {
                    return Err(format!(
                        "typed projected command `{}` is missing its compiler-retained relational model",
                        contract.name
                    ));
                }
                CommandConsistency::Succeeded | CommandConsistency::Eventual
                    if contract.projected_model.is_some()
                        || contract.direct_projection.is_some() =>
                {
                    return Err(format!(
                        "typed non-projected command `{}` cannot carry direct projection metadata",
                        contract.name
                    ));
                }
                CommandConsistency::Eventual
                | CommandConsistency::Succeeded
                | CommandConsistency::Atomic => {}
            }
            if let Some(projected) = &contract.projected_model {
                if projected.output_type_id != contract.output_type_id {
                    return Err(format!(
                        "typed projected command `{}` retained model has a different Rust output type",
                        contract.name
                    ));
                }
                if projected.model != contract.output.name {
                    return Err(format!(
                        "typed projected command `{}` retained model `{}` differs from output `{}`",
                        contract.name, projected.model, contract.output.name
                    ));
                }
            }
            if let Some(target) = &contract.direct_projection {
                if target.output_type_id != contract.output_type_id {
                    return Err(format!(
                        "typed projected command `{}` direct target has a different Rust output type",
                        contract.name
                    ));
                }
                if target.model != contract.output.name {
                    return Err(format!(
                        "typed projected command `{}` direct target model `{}` differs from output `{}`",
                        contract.name, target.model, contract.output.name
                    ));
                }
                let Some(change_epoch) = target.change_epoch.as_deref() else {
                    return Err(format!(
                        "typed projected command `{}` direct target has no registered change-log epoch",
                        contract.name
                    ));
                };
                ProjectionEpoch::new(change_epoch).map_err(|error| {
                    format!(
                        "typed projected command `{}` direct target change epoch is invalid: {error}",
                        contract.name
                    )
                })?;
            }
            types.insert(
                contract.name.clone(),
                (contract.input_type_id, contract.output_type_id),
            );
            canonical.push(contract.canonical_value());
        }

        let material = canonical_json(&serde_json::json!({
            "service_id": service_id,
            "commands": canonical,
        }));
        let bytes = serde_json::to_vec(&material)
            .expect("serializing canonical command inventory cannot fail");
        Ok(Self {
            service_id: service_id.to_string(),
            structural_fingerprint: format!("sha256:{:x}", Sha256::digest(bytes)),
            types,
        })
    }
}

/// A typed command declaration registered together with its executable handler.
pub struct TypedCommand<I, K: CommandOutcome> {
    route_name: &'static str,
    contract: TypedCommandContract,
    _types: PhantomData<fn(I) -> K>,
}

impl<I, K: CommandOutcome> Clone for TypedCommand<I, K> {
    fn clone(&self) -> Self {
        Self {
            route_name: self.route_name,
            contract: self.contract.clone(),
            _types: PhantomData,
        }
    }
}

/// Begin a typed command declaration.
pub fn typed_command<I, K>(name: &'static str) -> TypedCommand<I, K>
where
    I: CommandInputType + DeserializeOwned + Send + 'static,
    K: CommandOutcome,
{
    let route_name = name;
    let name = route_name.to_string();
    let field_name = name
        .chars()
        .map(|character| match character {
            '.' | '-' => '_',
            other => other,
        })
        .collect();
    let input = I::command_type();
    let output = K::__command_output_type();
    let projected_model = K::__projected_model()
        .map(|(output_type_id, schema)| CommandProjectedModel::new(output_type_id, schema));
    TypedCommand {
        route_name,
        contract: TypedCommandContract {
            name,
            field_name,
            roles: Vec::new(),
            input,
            output,
            input_type_id: TypeId::of::<I>(),
            output_type_id: TypeId::of::<K::Payload>(),
            consistency: K::CONSISTENCY,
            input_defaults: Vec::new(),
            projected_model,
            direct_projection: None,
            projections: CommandProjectionEvents::default(),
        },
        _types: PhantomData,
    }
}

/// Begin a typed command whose outward emit set is owned by a domain transition.
///
/// `S` is typically a `#[sourced]` `domain_commands::*` witness (for example
/// `domain_commands::Create`) or a domain-event marker. The emit set is filled
/// automatically — callers do not also call [`.emits`](TypedCommand::emits) /
/// [`.emits_events`](TypedCommand::emits_events) unless they intentionally
/// extend the set.
///
/// Prefer this over [`typed_command`] plus a hand-written event list when the
/// domain already defines the transition.
pub fn command_transition<S, I, K>(name: &'static str) -> TypedCommand<I, K>
where
    S: super::CommandEventSet,
    I: CommandInputType + DeserializeOwned + Send + 'static,
    K: CommandOutcome,
{
    typed_command::<I, K>(name).emits_events::<S>()
}

impl<I, K: CommandOutcome> TypedCommand<I, K> {
    pub fn field_name(mut self, field_name: impl Into<String>) -> Self {
        self.contract.field_name = field_name.into();
        self
    }

    pub fn roles(mut self, roles: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.contract.roles = roles.into_iter().map(Into::into).collect();
        self.contract.roles.sort();
        self.contract.roles.dedup();
        self
    }

    /// Declare values generated once into the canonical command input before
    /// dispatch. Generators are referenced from `.preview` / mutation inputs
    /// rather than separately authored command effects.
    pub fn input_defaults(mut self, defaults: CompiledInputDefaults<I>) -> Self {
        self.contract.input_defaults = defaults.0;
        self.contract
            .input_defaults
            .sort_by(|left, right| left.path.cmp(&right.path));
        self
    }

    /// Declare an exact outward domain-event set this command may emit.
    ///
    /// This declaration is intentionally independent of projector ownership:
    /// one occurrence can fan out to zero, one, or many modeled programs.
    ///
    /// Prefer [`Self::emits_events`] when the set is a domain event marker or a
    /// `#[sourced]` `domain_commands::*` transition witness.
    #[must_use]
    pub fn emits(mut self, events: super::CommandProjectionEventSet) -> Self {
        self.contract.projections.add_event_set(events);
        self
    }

    /// Declare outward domain events from a type-level [`super::CommandEventSet`].
    ///
    /// Equivalent to [`.emits`](Self::emits)`(events![...])` for the same
    /// domain-event contracts. A generated `domain_commands` witness also
    /// contributes recorder values the compiler can prove; role-visible
    /// projection arms decide their optimistic cache consequences.
    #[must_use]
    pub fn emits_events<S: super::CommandEventSet>(mut self) -> Self {
        self.contract
            .projections
            .add_event_set(S::command_event_set());
        for values in S::command_event_known_values() {
            self.contract.projections.add_inferred_values(values);
        }
        self
    }

    /// Override known mutation-input fields for client cache application.
    ///
    /// Maps command-known values (and unknowns) onto the emitted domain-event
    /// body shape that projections bind into mutation IR. The client
    /// applies that mutation to the cache only; the server applies the same
    /// mutation for real (eventual projector or handler-owned projected row).
    ///
    /// Generated domain transitions normally supply these values automatically.
    /// This lower-level escape hatch exists for event producers the compiler
    /// cannot inspect; it is not required for ordinary `command_transition`
    /// registration.
    ///
    /// Service registration rejects a mapping whose exact event selector is
    /// not also present through [`Self::emits`].
    #[must_use]
    pub fn applies(mut self, mapping: CommandProjectionPreview) -> Self {
        self.contract.projections.add_preview(mapping);
        self
    }

    /// Declare that one emitted event-body field is the authenticated user id.
    ///
    /// `B` is the serialized body: a `DomainState` for state capture, or the
    /// `DomainEvent` type itself for a flat event. Both use the same binding.
    ///
    /// This supplies the missing trusted `x-user-id` provenance to automatic
    /// event→mutation optimism when the projected model is not owner-filtered
    /// (for example, a public-readable chat lobby). It does not authorize the
    /// command or rewrite handler input: the mount must still require a user
    /// and the handler must bind the same principal from its session.
    #[must_use]
    pub fn authenticated_user_field<E, B>(mut self, rust_field: &'static str) -> Self
    where
        E: crate::domain_event::DomainEventBodyContract<B>,
        B: super::CommandProjectionBody,
    {
        let values = super::projections::authenticated_user_field_preview::<E, B>(rust_field);
        self.contract
            .projections
            .add_authenticated_user_field(rust_field, values);
        self
    }

    /// Declare a pure reducer over a known cache row for client auto-optimism.
    ///
    /// The pure function is domain-owned (e.g. `blob_domain::simulate_move`);
    /// `client_module` / `client_export` name the TypeScript twin shipped with
    /// the generated client and registered as `pureFunctions[fn_name]`.
    #[must_use]
    pub fn preview_reduce_known_record(mut self, reduce: CommandProjectionPureReduce) -> Self {
        self.contract.projections.add_pure_reduce(reduce);
        self
    }

    pub fn name(&self) -> &str {
        &self.contract.name
    }

    pub fn consistency(&self) -> CommandConsistency {
        self.contract.consistency
    }

    #[cfg(test)]
    pub(crate) fn into_contract(self) -> TypedCommandContract {
        self.contract
    }

    pub(crate) fn into_parts(self) -> (&'static str, TypedCommandContract) {
        (self.route_name, self.contract)
    }
}

impl<I, M> TypedCommand<I, Atomic<M>>
where
    I: CommandInputType + DeserializeOwned + Send + 'static,
    M: RelationalReadModel + Serialize + Send + Sync + 'static,
{
    /// Attach compiler-generated direct projection ownership metadata.
    ///
    /// Application handlers do not call this; generated service inventory
    /// binds it from the registered projector declaration and handlers use the
    /// fluent direct-projection commit API.
    #[doc(hidden)]
    pub fn __direct_projection(mut self, target: CompiledDirectProjectionTarget<I, M>) -> Self {
        if let Some(projected) = &mut self.contract.projected_model {
            projected.partition = target.0.partition.clone();
        }
        self.contract.direct_projection = Some(target.0);
        self
    }
}
