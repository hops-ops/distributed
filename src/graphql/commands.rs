//! Crate-private GraphQL command inventory.
//!
//! GraphQL mutations are derived exclusively from the executable service's
//! typed causal command contracts. There is deliberately no second public
//! registry, JSON-shaped escape hatch, or client policy catalog.

#[cfg(feature = "graphql")]
use std::collections::BTreeMap;
use std::collections::BTreeSet;

#[cfg(feature = "graphql")]
use super::surface::{
    compile_projection_owner_topology, validate_direct_modeled_owner_compatibility,
    SurfaceProjectionOwner,
};
use super::surface::{SurfaceCommand, SurfaceCommandShape, SurfaceTypeDef, SurfaceTypeField};
#[cfg(feature = "graphql")]
use crate::command::CommandConsistency;
use crate::command::CommandTypeDef;
use crate::command::TypedCommandContract;

#[derive(Clone, Debug, Default)]
pub(crate) struct TypedCommandInventory {
    contracts: Vec<TypedCommandContract>,
}

fn surface_type(definition: &CommandTypeDef) -> SurfaceTypeDef {
    SurfaceTypeDef {
        name: definition.name.clone(),
        fields: definition
            .fields
            .iter()
            .map(|field| SurfaceTypeField {
                name: field.name.clone(),
                type_name: field.type_name.clone(),
                nullable: field.nullable,
                list: field.list,
                item_nullable: field.item_nullable,
                nested: field.nested.as_deref().map(surface_type).map(Box::new),
            })
            .collect(),
    }
}

impl TypedCommandInventory {
    #[cfg(any(feature = "graphql", test))]
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    pub(crate) fn from_contracts(contracts: &[TypedCommandContract]) -> Result<Self, String> {
        let mut seen = BTreeSet::new();
        let mut contracts = contracts.to_vec();
        for contract in &contracts {
            if contract.name.trim().is_empty() {
                return Err("typed command id must not be empty".into());
            }
            if !seen.insert(contract.name.clone()) {
                return Err(format!(
                    "duplicate typed command declaration for `{}`",
                    contract.name
                ));
            }
        }
        contracts.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(Self { contracts })
    }

    pub(crate) fn surface_commands(&self) -> Vec<SurfaceCommand> {
        self.contracts
            .iter()
            .map(|contract| {
                let mut roles = contract.roles.clone();
                roles.sort();
                roles.dedup();
                SurfaceCommand {
                    command_name: contract.name.clone(),
                    field_name: contract.field_name.clone(),
                    roles,
                    input: SurfaceCommandShape::Typed(surface_type(&contract.input)),
                    output: SurfaceCommandShape::Typed(surface_type(&contract.output)),
                    consistency: contract.consistency,
                    input_defaults: contract.input_defaults.clone(),
                    effects: Some(contract.effects.clone()),
                    confirmations: contract.confirmations.clone(),
                    projected_model: contract.projected_model.clone(),
                    direct_projection: contract.direct_projection.clone(),
                    projections: contract.projections.clone(),
                    confirmation_unavailable: false,
                }
            })
            .collect()
    }

    #[cfg(feature = "graphql")]
    pub(crate) fn contracts_for_binding(&self) -> Vec<TypedCommandContract> {
        self.contracts.clone()
    }

    /// Bind every confirmation and ordinary `Atomic<M>` target to the exact
    /// compiled projector registry. Runtime lowering never reconstructs
    /// authority from projector/model strings.
    #[cfg(feature = "graphql")]
    pub(crate) fn bind_direct_projection_targets(
        &mut self,
        projectors: &[SurfaceProjectionOwner],
        model_schemas: &BTreeMap<String, crate::table::TableSchema>,
    ) -> Result<(), String> {
        validate_direct_modeled_owner_compatibility(projectors)?;
        let mut compiled_projectors = BTreeMap::new();
        for projector in projectors {
            let binding_models = projector.binding_models();
            let schemas = binding_models
                .iter()
                .map(|model| {
                    model_schemas.get(model).ok_or_else(|| {
                        format!(
                            "projector `{}` references unknown model `{model}`",
                            projector.name
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let compiled =
                compile_projection_owner_topology(projector, schemas).map_err(|error| {
                    format!(
                        "projector `{}` has invalid compiled topology: {error}",
                        projector.name
                    )
                })?;
            compiled_projectors.insert(projector.name.clone(), compiled);
        }

        for contract in &mut self.contracts {
            let name = &contract.name;
            for confirmation in &mut contract.confirmations {
                let projector = projectors
                    .iter()
                    .find(|projector| projector.name == confirmation.projector)
                    .ok_or_else(|| {
                        format!(
                            "typed command `{name}` expects unknown projector `{}`",
                            confirmation.projector
                        )
                    })?;
                let binding_facts = projector.binding_facts();
                let binding_models = projector.binding_models();
                if !confirmation.topology_matches(
                    &projector.name,
                    &binding_facts,
                    &binding_models,
                    &projector.partition,
                ) {
                    return Err(format!(
                        "typed command `{name}` captured projector `{}` topology identity does not match the registered projector facts/models",
                        confirmation.projector
                    ));
                }
                if !confirmation.partition_matches(&projector.partition) {
                    return Err(format!(
                        "typed command `{name}` confirmation for projector `{}` does not provide the partition mapping required by its declaration",
                        confirmation.projector
                    ));
                }
                if !binding_models
                    .iter()
                    .any(|model| model == &confirmation.model)
                {
                    return Err(format!(
                        "typed command `{name}` expects projector `{}` to confirm model `{}`, but that model is not in the projector topology",
                        confirmation.projector, confirmation.model
                    ));
                }
                let (topology, _) = compiled_projectors
                    .get(&projector.name)
                    .expect("every registered projector was compiled above");
                confirmation.bind_protocol_topology(topology.clone());
            }

            if contract.consistency != CommandConsistency::Atomic {
                continue;
            }
            let projected = contract.projected_model.as_ref().ok_or_else(|| {
                format!(
                    "typed projected command `{name}` is missing its compiler-retained relational model"
                )
            })?;
            let owners = projectors
                .iter()
                .filter(|projector| {
                    projector
                        .binding_models()
                        .iter()
                        .any(|model| model == &projected.model)
                })
                .collect::<Vec<_>>();
            let projector = match owners.as_slice() {
                [projector] => *projector,
                [] => {
                    return Err(format!(
                        "typed projected command `{name}` output model `{}` has no registered SurfaceProjector owner",
                        projected.model
                    ));
                }
                _ => {
                    return Err(format!(
                        "typed projected command `{name}` output model `{}` has ambiguous SurfaceProjector ownership: {}",
                        projected.model,
                        owners
                            .iter()
                            .map(|owner| owner.name.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
            };
            let binding_change_epoch = projector.binding_change_epoch();
            if binding_change_epoch.is_none() {
                return Err(format!(
                    "typed projected command `{name}` owner `{}` has no registered change-log epoch",
                    projector.name
                ));
            }
            let registered_schema = model_schemas
                .get(&projected.model)
                .expect("projector ownership above requires a registered model schema");
            if !projected
                .schema
                .has_same_storage_contract(registered_schema)
            {
                return Err(format!(
                    "typed projected command `{name}` retained schema for `{}` differs from the registered full table schema",
                    projected.model
                ));
            }
            if !projected.partition_matches(&projector.partition) {
                return Err(format!(
                    "typed projected command `{name}` does not provide the partition mapping required by projector `{}`",
                    projector.name
                ));
            }
            let (protocol_topology, ownership) = compiled_projectors
                .get(&projector.name)
                .expect("every registered projector was compiled above");
            let binding_facts = projector.binding_facts();
            let binding_models = projector.binding_models();
            contract.direct_projection = Some(projected.bind(
                &projector.name,
                &binding_facts,
                &binding_models,
                &projector.partition,
                binding_change_epoch.as_deref(),
                ownership.clone(),
                Some(protocol_topology.clone()),
                projector.active_modeled_program_id_for(&projected.model),
            ));
        }
        Ok(())
    }
}
