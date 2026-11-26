//! A mutation says which input fills which column of a read-model row.

use std::marker::PhantomData;

use serde::Serialize;

use crate::projection::{
    MutationKind, ProjectionAssignment, ProjectionField, ProjectionKeyField, ProjectionOperation,
    ProjectionProgramError, ProjectionTarget, ProjectionValueType,
    MAX_PROJECTION_OPERATIONS_PER_OCCURRENCE,
};
use crate::table::{ColumnType, TableSchema};

use super::bind::{bind_input, MutationInputBinding};

/// One column and the mutation input path that fills it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct MutationColumn {
    name: String,
    value_type: ProjectionValueType,
    input: Vec<String>,
}

impl MutationColumn {
    /// Fill column `name` from the mutation input at `input`.
    ///
    /// `value_type` types the value when the path is reached through a bound
    /// input object rather than bound itself.
    ///
    /// # Errors
    ///
    /// Rejects an empty name, an empty path and empty path segments.
    pub fn try_new<I, S>(
        name: impl Into<String>,
        value_type: ProjectionValueType,
        input: I,
    ) -> Result<Self, ProjectionProgramError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let name = name.into();
        let input = input.into_iter().map(Into::into).collect::<Vec<_>>();
        if name.is_empty() {
            return Err(ProjectionProgramError::EmptyName("mutation column"));
        }
        if input.is_empty() || input.iter().any(String::is_empty) {
            return Err(ProjectionProgramError::EmptyName("mutation input path"));
        }
        Ok(Self {
            name,
            value_type,
            input,
        })
    }

    /// Return the column name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the mutation input path that fills the column.
    pub fn input(&self) -> &[String] {
        &self.input
    }
}

/// One row change of a mutation: its kind, target, key and written columns.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct MutationOperation {
    operation_id: String,
    kind: MutationKind,
    target: ProjectionTarget,
    key: Vec<MutationColumn>,
    fields: Vec<MutationColumn>,
}

impl MutationOperation {
    /// Declare one row change. Key and field order is the declared order.
    ///
    /// # Errors
    ///
    /// Rejects an empty operation identifier or key. Everything else is
    /// checked when the operation is bound to an event.
    pub fn try_new(
        operation_id: impl Into<String>,
        kind: MutationKind,
        target: ProjectionTarget,
        key: Vec<MutationColumn>,
        fields: Vec<MutationColumn>,
    ) -> Result<Self, ProjectionProgramError> {
        let operation_id = operation_id.into();
        if operation_id.is_empty() {
            return Err(ProjectionProgramError::EmptyName("mutation operation ID"));
        }
        if key.is_empty() {
            return Err(ProjectionProgramError::InvalidOperation {
                operation: operation_id,
                reason: "a complete logical key is required".to_owned(),
            });
        }
        Ok(Self {
            operation_id,
            kind,
            target,
            key,
            fields,
        })
    }

    /// Return the stable operation identifier.
    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }

    /// Return the logical mutation kind.
    pub fn kind(&self) -> MutationKind {
        self.kind
    }

    /// Return the target read model.
    pub fn target(&self) -> &ProjectionTarget {
        &self.target
    }

    /// Return the key columns in declared order.
    pub fn key(&self) -> &[MutationColumn] {
        &self.key
    }

    /// Return the written columns in declared order.
    pub fn fields(&self) -> &[MutationColumn] {
        &self.fields
    }

    /// The projection operation this mutation is for an event whose inputs
    /// are bound by `inputs`.
    pub(super) fn bind(
        &self,
        staging_ordinal: u32,
        inputs: &[MutationInputBinding],
    ) -> Result<ProjectionOperation, ProjectionProgramError> {
        let expression =
            |column: &MutationColumn| bind_input(inputs, &column.input, &column.value_type);
        let key = self
            .key
            .iter()
            .zip(0u32..)
            .map(|(column, ordinal)| {
                ProjectionKeyField::try_new(ordinal, column.name.clone(), expression(column)?)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let fields = self
            .fields
            .iter()
            .zip(0u32..)
            .map(|(column, ordinal)| {
                ProjectionField::try_new(
                    ordinal,
                    column.name.clone(),
                    ProjectionAssignment::Set(expression(column)?),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        ProjectionOperation::try_new(
            self.operation_id.clone(),
            staging_ordinal,
            self.kind,
            self.target.clone(),
            key,
            fields,
            Vec::new(),
            Vec::new(),
        )
    }
}

/// An event-independent mutation: a name, a version and its row changes in
/// staging order. It names no event; [`crate::projection!`] binds events to it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct MutationProgram {
    name: String,
    version: u64,
    operations: Vec<MutationOperation>,
}

impl MutationProgram {
    /// Construct a mutation from its row changes.
    ///
    /// # Errors
    ///
    /// Rejects an empty name, a zero version, no operations, more operations
    /// than one occurrence may stage, and repeated operation identifiers.
    pub fn try_new(
        name: impl Into<String>,
        version: u64,
        operations: Vec<MutationOperation>,
    ) -> Result<Self, ProjectionProgramError> {
        let name = name.into();
        if name.is_empty() {
            return Err(ProjectionProgramError::EmptyName("mutation name"));
        }
        if version == 0 {
            return Err(ProjectionProgramError::ZeroVersion("mutation version"));
        }
        if operations.is_empty() {
            return Err(ProjectionProgramError::InvalidOperation {
                operation: name,
                reason: "a mutation requires at least one operation".to_owned(),
            });
        }
        if operations.len() > MAX_PROJECTION_OPERATIONS_PER_OCCURRENCE {
            return Err(ProjectionProgramError::TooManyOperations {
                count: operations.len(),
                max: MAX_PROJECTION_OPERATIONS_PER_OCCURRENCE,
            });
        }
        for (index, operation) in operations.iter().enumerate() {
            if operations[..index]
                .iter()
                .any(|earlier| earlier.operation_id == operation.operation_id)
            {
                return Err(ProjectionProgramError::DuplicateName {
                    kind: "mutation operation",
                    name: operation.operation_id.clone(),
                });
            }
        }
        Ok(Self {
            name,
            version,
            operations,
        })
    }

    /// Build the mutation a [`mutation!`](macro@crate::mutation) declaration states.
    ///
    /// # Errors
    ///
    /// Returns the first invalid model, column or operation.
    pub fn build(
        name: &str,
        version: u64,
        steps: &[MutationStep],
    ) -> Result<Self, ProjectionProgramError> {
        let operations = steps
            .iter()
            .enumerate()
            .map(|(index, step)| step.operation(index))
            .collect::<Result<Vec<_>, _>>()?;
        Self::try_new(name, version, operations)
    }

    /// Return the stable mutation name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the declaration version.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Return the row changes in staging order.
    pub fn operations(&self) -> &[MutationOperation] {
        &self.operations
    }

    /// The projection operations this mutation is for an event whose inputs
    /// are bound by `inputs`.
    pub(super) fn bind(
        &self,
        inputs: &[MutationInputBinding],
    ) -> Result<Vec<ProjectionOperation>, ProjectionProgramError> {
        self.operations
            .iter()
            .zip(0u32..)
            .map(|(operation, ordinal)| operation.bind(ordinal, inputs))
            .collect()
    }
}

/// A column filled from a mutation input path, as a declaration states it.
#[derive(Clone, Copy, Debug)]
pub struct MutationInput {
    /// Column name.
    pub column: &'static str,
    /// Mutation input path.
    pub path: &'static [&'static str],
}

/// One statement of a [`mutation!`](macro@crate::mutation) declaration. The macros
/// emit these as data; [`MutationProgram::build`] turns them into operations.
#[derive(Clone, Copy, Debug)]
pub enum MutationStep {
    /// Replace or create the complete row from the input object at `object`.
    Upsert {
        /// Schema of the target model.
        model: fn() -> &'static TableSchema,
        /// Input path of the row object.
        object: &'static [&'static str],
    },
    /// Create the complete row from the input object at `object`.
    Insert {
        /// Schema of the target model.
        model: fn() -> &'static TableSchema,
        /// Input path of the row object.
        object: &'static [&'static str],
    },
    /// Remove the row with the given key.
    Delete {
        /// Schema of the target model.
        model: fn() -> &'static TableSchema,
        /// Key columns.
        key: &'static [MutationInput],
    },
    /// Set the given columns of the row with the given key.
    Patch {
        /// Schema of the target model.
        model: fn() -> &'static TableSchema,
        /// Key columns.
        key: &'static [MutationInput],
        /// Written columns.
        set: &'static [MutationInput],
    },
}

impl MutationStep {
    fn operation(&self, index: usize) -> Result<MutationOperation, ProjectionProgramError> {
        match *self {
            Self::Upsert { model, object } => complete_row(
                format!("upsert-{index}"),
                MutationKind::Upsert,
                model(),
                object,
            ),
            Self::Insert { model, object } => complete_row(
                format!("insert-{index}"),
                MutationKind::Insert,
                model(),
                object,
            ),
            Self::Delete { model, key } => MutationOperation::try_new(
                format!("delete-{index}"),
                MutationKind::Delete,
                target(model())?,
                declared(model(), key)?,
                Vec::new(),
            ),
            Self::Patch { model, key, set } => MutationOperation::try_new(
                format!("patch-{index}"),
                MutationKind::Patch,
                target(model())?,
                declared(model(), key)?,
                declared(model(), set)?,
            ),
        }
    }
}

fn target(schema: &TableSchema) -> Result<ProjectionTarget, ProjectionProgramError> {
    ProjectionTarget::try_new(schema.model_name.clone(), schema.table_name.clone())
}

/// The portable value type of one physical column type.
pub fn projection_value_type_for_column(column_type: &ColumnType) -> ProjectionValueType {
    match column_type {
        ColumnType::Boolean => ProjectionValueType::Boolean,
        ColumnType::Integer => ProjectionValueType::I64,
        ColumnType::UnsignedInteger => ProjectionValueType::U64,
        ColumnType::Float => ProjectionValueType::F64,
        ColumnType::Text | ColumnType::Timestamp => ProjectionValueType::String,
        ColumnType::Bytes | ColumnType::Json | ColumnType::Unsupported(_) => {
            ProjectionValueType::Json
        }
    }
}

fn value_type(schema: &TableSchema, column: &str) -> ProjectionValueType {
    schema
        .columns
        .iter()
        .find(|candidate| candidate.field_name == column)
        .map_or(ProjectionValueType::String, |column| {
            projection_value_type_for_column(&column.column_type)
        })
}

fn declared(
    schema: &TableSchema,
    columns: &[MutationInput],
) -> Result<Vec<MutationColumn>, ProjectionProgramError> {
    columns
        .iter()
        .map(|input| {
            MutationColumn::try_new(
                input.column,
                value_type(schema, input.column),
                input.path.iter().copied(),
            )
        })
        .collect()
}

/// Every non-skipped column of the model, each read from the same-named member
/// of the input object at `object`. The key is the model's primary key.
pub(super) fn complete_row(
    operation_id: String,
    kind: MutationKind,
    schema: &TableSchema,
    object: &[&str],
) -> Result<MutationOperation, ProjectionProgramError> {
    let column = |name: &str| {
        MutationColumn::try_new(
            name,
            value_type(schema, name),
            object.iter().copied().chain([name]),
        )
    };
    let key = schema
        .primary_key
        .columns
        .iter()
        .map(|name| column(name))
        .collect::<Result<Vec<_>, _>>()?;
    let fields = schema
        .columns
        .iter()
        .filter(|candidate| !candidate.skipped)
        .map(|candidate| column(&candidate.field_name))
        .collect::<Result<Vec<_>, _>>()?;
    MutationOperation::try_new(operation_id, kind, target(schema)?, key, fields)
}

/// A declared mutation: what [`mutation!`](macro@crate::mutation) and
/// [`mutation_file!`](crate::mutation_file) expand to. `I` marks the input
/// type the author declared.
#[derive(Debug)]
pub struct Mutation<I> {
    name: &'static str,
    version: u64,
    steps: &'static [MutationStep],
    marker: PhantomData<fn() -> I>,
}

impl<I> Clone for Mutation<I> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<I> Copy for Mutation<I> {}

impl<I> Mutation<I> {
    /// State a mutation as data. Nothing is validated until [`Self::program`].
    pub const fn declare(name: &'static str, version: u64, steps: &'static [MutationStep]) -> Self {
        Self {
            name,
            version,
            steps,
            marker: PhantomData,
        }
    }

    /// Return the declared mutation name.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Build the declared mutation.
    ///
    /// # Errors
    ///
    /// Returns the first invalid model, column or operation.
    pub fn program(&self) -> Result<MutationProgram, ProjectionProgramError> {
        MutationProgram::build(self.name, self.version, self.steps)
    }

    /// Materialize a complete read-model row from known domain/state input.
    ///
    /// Used by handler-owned projected commits: apply the same field shape the
    /// mutation would write (`upsert M from input.root`) from a post-transition
    /// state DTO. Relationship/query-only fields on `M` that are absent on
    /// `state` deserialize as their serde defaults (`None`, etc.).
    ///
    /// # Errors
    ///
    /// Returns encoding failures when `state` cannot be converted into `M`.
    pub fn from_state<S, M>(&self, state: &S) -> Result<M, ProjectionProgramError>
    where
        S: serde::Serialize,
        M: serde::de::DeserializeOwned,
    {
        let invalid = |reason: String| ProjectionProgramError::InvalidOperation {
            operation: self.name.to_owned(),
            reason,
        };
        let value = serde_json::to_value(state)
            .map_err(|error| invalid(format!("could not encode known state: {error}")))?;
        serde_json::from_value(value).map_err(|error| {
            invalid(format!(
                "could not materialize row from known state: {error}"
            ))
        })
    }
}
