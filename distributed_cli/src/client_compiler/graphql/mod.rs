use std::collections::{BTreeMap, BTreeSet, HashMap};

use async_graphql_parser::types::{
    BaseType, Directive, DocumentOperations, Field, FragmentDefinition, OperationDefinition,
    OperationType, Selection, SelectionSet, Type,
};
use async_graphql_parser::{parse_query, Pos, Positioned};
use async_graphql_value::{Name, Value};
use base64::Engine as _;
use serde::Serialize;
use serde_json::{Map as JsonMap, Value as JsonValue};

use super::manifest::{
    hash_bytes, row_cost, ClientAggregateSemantics, ClientArgument, ClientArgumentKind,
    ClientExecutionLimits, ClientField, ClientFilterField, ClientFilterInput,
    ClientFilterSemantics, ClientManifest, ClientModel, ClientOrderSemantics,
    ClientPaginationSemantics, ClientRelationship, ClientRelationshipKind,
    ClientRelationshipMaintenance, ClientRoot, ClientRootKind, ClientRootOperation,
    ClientRowPolicy, FilterExpr, RelationshipKeyMapping,
};
use super::{ClientCompileError, ClientDocument};

mod arguments;
mod fragments;
mod objects;
mod operation;
mod query_plan;
mod types;
mod validation;
mod variables;

pub(crate) use operation::{compile_document, typescript_scalar};
pub(crate) use types::*;

use arguments::*;
use fragments::*;
use objects::*;
use operation::*;
use query_plan::*;
use validation::*;
use variables::*;
