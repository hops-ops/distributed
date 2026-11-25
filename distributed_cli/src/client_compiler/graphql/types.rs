use super::*;

pub(super) const MAX_SOURCE_BYTES: usize = 1024 * 1024;
pub(super) const MAX_VARIABLES: usize = 256;
pub(super) const MAX_OBJECT_DEPTH: usize = 64;
pub(super) const MAX_EXPANDED_SELECTIONS: usize = 10_000;

#[derive(Clone, Debug)]
pub(crate) struct CompiledOperation {
    pub(crate) name: String,
    pub(crate) source_path: String,
    pub(crate) source_line: usize,
    pub(crate) source_column: usize,
    pub(crate) module_path: String,
    pub(crate) export_name: String,
    pub(crate) query_document: String,
    pub(crate) query_hash: String,
    pub(crate) load: bool,
    pub(crate) live: Option<CompiledLiveOperation>,
    pub(crate) variables: Vec<CompiledVariable>,
    pub(crate) variable_codec: CompiledVariableCodec,
    pub(crate) root: CompiledRoot,
}

#[derive(Clone, Debug)]
pub(crate) struct CompiledLiveOperation {
    pub(crate) document: String,
    pub(crate) hash: String,
}

#[derive(Clone, Debug)]
pub(crate) struct CompiledVariable {
    pub(crate) name: String,
    pub(crate) graphql_type: Type,
    pub(crate) default: Option<CompiledVariableDefault>,
}

#[derive(Clone, Debug)]
pub(crate) struct CompiledVariableDefault {
    pub(crate) value: JsonValue,
    pub(crate) wire: String,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct CompiledVariableCodec {
    pub(crate) version: u32,
    pub(crate) limits: CompiledVariableCodecLimits,
    pub(crate) variables: BTreeMap<String, CompiledInputType>,
    pub(crate) defaults: BTreeMap<String, JsonValue>,
    pub(crate) inputs: BTreeMap<String, CompiledInputDefinition>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct CompiledVariableCodecLimits {
    #[serde(rename = "maxDepth")]
    pub(crate) max_depth: u64,
    #[serde(rename = "maxBoolWidth")]
    pub(crate) max_bool_width: u64,
    #[serde(rename = "maxInList")]
    pub(crate) max_in_list: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum CompiledInputType {
    Scalar {
        scalar: String,
        codec: String,
        nullable: bool,
    },
    Enum {
        name: String,
        values: Vec<String>,
        nullable: bool,
    },
    Input {
        name: String,
        nullable: bool,
        #[serde(rename = "filterBaseDepth", skip_serializing_if = "Option::is_none")]
        filter_base_depth: Option<u64>,
    },
    List {
        nullable: bool,
        #[serde(rename = "maxItems", skip_serializing_if = "Option::is_none")]
        max_items: Option<u64>,
        item: Box<CompiledInputType>,
    },
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum CompiledInputDefinition {
    Filter {
        model: String,
        fields: Vec<CompiledFilterInputField>,
        relationships: Vec<CompiledFilterInputRelationship>,
    },
    Order {
        model: String,
        fields: Vec<CompiledOrderInputField>,
        values: Vec<String>,
    },
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct CompiledFilterInputField {
    pub(crate) field: String,
    pub(crate) scalar: String,
    pub(crate) codec: String,
    pub(crate) nullable: bool,
    pub(crate) operators: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct CompiledOrderInputField {
    pub(crate) field: String,
    pub(crate) scalar: String,
    pub(crate) codec: String,
    pub(crate) nullable: bool,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct CompiledFilterInputRelationship {
    pub(crate) field: String,
    pub(crate) target: CompiledFilterInputTarget,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum CompiledFilterInputTarget {
    Input { name: String },
    Opaque,
}

#[derive(Clone, Debug)]
pub(crate) struct CompiledRoot {
    pub(crate) response_key: String,
    pub(crate) field: String,
    pub(crate) cardinality: Cardinality,
    pub(crate) nullable: bool,
    pub(crate) arguments: BTreeMap<String, CompiledArgument>,
    pub(crate) dependencies: Vec<String>,
    pub(crate) coverage: Option<CompiledCoverage>,
    pub(crate) filter: Option<CompiledFilterPlan>,
    pub(crate) order: Option<CompiledOrderPlan>,
    pub(crate) pagination: Option<CompiledPaginationPlan>,
    pub(crate) selection: CompiledObject,
}

#[derive(Clone, Debug)]
pub(crate) struct CompiledObject {
    pub(crate) typename: String,
    pub(crate) storage: CompiledStorage,
    pub(crate) members: Vec<CompiledMember>,
}

#[derive(Clone, Debug)]
pub(crate) enum CompiledMember {
    Scalar(CompiledScalar),
    Branch(Box<CompiledBranch>),
}

#[derive(Clone, Debug)]
pub(crate) enum CompiledStorage {
    Normalized {
        model_id: String,
        identity_fields: Vec<String>,
    },
    Embedded,
}

#[derive(Clone, Debug)]
pub(crate) struct CompiledBranch {
    pub(crate) semantic: CompiledBranchSemantic,
    pub(crate) response_key: String,
    pub(crate) field: String,
    pub(crate) cardinality: Cardinality,
    pub(crate) nullable: bool,
    pub(crate) arguments: BTreeMap<String, CompiledArgument>,
    pub(crate) dependencies: Vec<String>,
    pub(crate) coverage: Option<CompiledCoverage>,
    pub(crate) filter: Option<CompiledFilterPlan>,
    pub(crate) order: Option<CompiledOrderPlan>,
    pub(crate) pagination: Option<CompiledPaginationPlan>,
    pub(crate) relationship: Option<CompiledRelationshipPlan>,
    pub(crate) selection: CompiledObject,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CompiledBranchSemantic {
    Relationship,
    Aggregate,
    AggregateFields,
    AggregateNodes,
}

pub(super) struct MergedField<'a> {
    pub(super) first: &'a Positioned<Field>,
    pub(super) selection_sets: Vec<&'a Positioned<SelectionSet>>,
    pub(super) canonical_arguments: Vec<(String, String)>,
}

pub(super) struct FragmentExpander<'ast, 'source> {
    pub(super) fragments: &'ast HashMap<Name, Positioned<FragmentDefinition>>,
    pub(super) document: &'source ClientDocument,
    pub(super) state: ExpansionState,
}

#[derive(Default)]
pub(super) struct ExpansionState {
    pub(super) used_fragments: BTreeSet<String>,
    pub(super) active_fragments: Vec<String>,
    pub(super) expanded_units: usize,
}

#[derive(Default)]
pub(super) struct FragmentGraphState {
    pub(super) active_fragments: Vec<String>,
    pub(super) completed_fragments: BTreeSet<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Cardinality {
    One,
    Many,
}

#[derive(Clone, Debug)]
pub(crate) enum CompiledArgument {
    Literal { value: JsonValue, wire: String },
    Variable(String),
    List(Vec<CompiledArgument>),
    Object(BTreeMap<String, CompiledArgument>),
}

#[derive(Clone, Debug)]
pub(crate) struct CompiledCoverage {
    pub(crate) kind: String,
    pub(crate) offset_argument: Option<String>,
    pub(crate) limit_argument: Option<String>,
    pub(crate) default_limit: Option<u64>,
    pub(crate) max_limit: Option<u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct CompiledFilterPlan {
    pub(crate) input: Option<CompiledArgument>,
    pub(crate) fields: Vec<CompiledFilterField>,
    pub(crate) relationships: Vec<CompiledRelationshipPlan>,
    pub(crate) row_policy: ClientRowPolicy,
    pub(super) variable_constraints: BTreeMap<String, VariableUseConstraint>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct VariableUseConstraint {
    pub(super) filter_base_depth: Option<u64>,
    pub(super) max_items: Option<u64>,
    pub(super) item: Option<Box<VariableUseConstraint>>,
}

impl VariableUseConstraint {
    pub(super) fn filter(base_depth: u64) -> Self {
        Self {
            filter_base_depth: Some(base_depth),
            ..Self::default()
        }
    }

    pub(super) fn list(max_items: u64, item: Option<Self>) -> Self {
        Self {
            max_items: Some(max_items),
            item: item.map(Box::new),
            ..Self::default()
        }
    }

    pub(super) fn intersect(&mut self, other: &Self) {
        self.filter_base_depth = match (self.filter_base_depth, other.filter_base_depth) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (left, right) => left.or(right),
        };
        self.max_items = match (self.max_items, other.max_items) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        };
        match (&mut self.item, &other.item) {
            (Some(left), Some(right)) => left.intersect(right),
            (None, Some(right)) => self.item = Some(right.clone()),
            _ => {}
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CompiledFilterField {
    pub(crate) name: String,
    pub(crate) scalar: String,
    pub(crate) codec: String,
    pub(crate) nullable: bool,
    pub(crate) operators: Vec<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct CompiledRelationshipPlan {
    pub(crate) field: String,
    pub(crate) target_model: String,
    pub(crate) kind: ClientRelationshipKind,
    pub(crate) key_mapping: RelationshipKeyMapping,
    pub(crate) maintenance: ClientRelationshipMaintenance,
    pub(crate) dependencies: Vec<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct CompiledOrderPlan {
    pub(crate) input: Option<CompiledArgument>,
    pub(crate) fields: Vec<CompiledOrderField>,
    pub(crate) identity: Vec<CompiledOrderField>,
}

#[derive(Clone, Debug)]
pub(crate) struct CompiledOrderField {
    pub(crate) name: String,
    pub(crate) scalar: String,
    pub(crate) codec: String,
    pub(crate) nullable: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct CompiledPaginationPlan {
    pub(crate) kind: String,
    pub(crate) insert: String,
    pub(crate) delete: String,
    pub(crate) reorder: String,
    pub(crate) stable_update: String,
}

pub(super) type CompiledQueryPlans = (
    Option<CompiledFilterPlan>,
    Option<CompiledOrderPlan>,
    Option<CompiledPaginationPlan>,
);

#[derive(Clone, Debug)]
pub(crate) struct CompiledScalar {
    pub(crate) response_key: String,
    pub(crate) field: String,
    pub(crate) codec: String,
    pub(crate) nullable: bool,
    pub(crate) expose: bool,
    /// The exact shape of a `json` field's values, when the model states it.
    pub(crate) json_type: Option<distributed::JsonShape>,
}
