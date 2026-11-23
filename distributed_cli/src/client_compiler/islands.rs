use serde::Serialize;

use super::graphql::{Cardinality, CompiledOperation};
use super::manifest::{hash_bytes, ClientManifest, ClientSurfaceIdentity};
use super::{
    GeneratedIslandDirectives, GeneratedIslandLiveCoverage, GeneratedIslandPlan,
    GeneratedIslandSource, GeneratedIslandVariable, GeneratedIslandVariableSchema,
};

pub(crate) const ISLAND_PLAN_VERSION: u32 = 1;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GeneratedIslandInventory<'a> {
    pub(crate) version: u32,
    pub(crate) schema_fingerprint: &'a str,
    pub(crate) protocol_fingerprint: &'a str,
    pub(crate) surface: &'a ClientSurfaceIdentity,
    pub(crate) islands: &'a [GeneratedIslandPlan],
}

pub(crate) fn island_plan(
    manifest: &ClientManifest,
    operation: &CompiledOperation,
) -> GeneratedIslandPlan {
    let surface = serde_json::to_string(&manifest.surface)
        .expect("validated manifest surfaces always serialize");
    let identity = format!(
        "distributed-island-v{ISLAND_PLAN_VERSION}\n{}\n{surface}\n{}",
        manifest.schema_fingerprint, operation.query_hash
    );
    let coverage = operation.root.coverage.as_ref();
    let finite = match operation.root.cardinality {
        Cardinality::One => true,
        Cardinality::Many => coverage
            .is_some_and(|coverage| coverage.kind == "complete" || coverage.max_limit.is_some()),
    };
    let variables = operation
        .variables
        .iter()
        .map(|variable| GeneratedIslandVariable {
            name: variable.name.clone(),
            graphql_type: variable.graphql_type.to_string(),
            default_value: variable
                .default
                .as_ref()
                .map(|default| default.value.clone()),
        })
        .collect();

    GeneratedIslandPlan {
        version: ISLAND_PLAN_VERSION,
        id: hash_bytes(identity.as_bytes()),
        operation: operation.name.clone(),
        operation_hash: operation.query_hash.clone(),
        module_path: operation.module_path.clone(),
        export_name: operation.export_name.clone(),
        source: GeneratedIslandSource {
            path: operation.source_path.clone(),
            line: operation.source_line,
            column: operation.source_column,
        },
        directives: GeneratedIslandDirectives {
            load: operation.load,
            live: operation.live.is_some(),
        },
        variable_schema: GeneratedIslandVariableSchema {
            reference: format!(
                "{}#variable-codec-v{}",
                operation.query_hash, operation.variable_codec.version
            ),
            codec_version: operation.variable_codec.version,
            variables,
        },
        live_coverage: GeneratedIslandLiveCoverage {
            requested: operation.live.is_some(),
            finite,
            kind: coverage
                .map(|coverage| coverage.kind.clone())
                .unwrap_or_else(|| "unknown".to_string()),
            max_items: coverage.and_then(|coverage| coverage.max_limit),
        },
    }
}
