use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::DomainEventOccurrence;

use super::ProjectionProgramError;

/// Maximum nesting of a portable expression or literal value.
pub const MAX_PROJECTION_EXPRESSION_DEPTH: usize = 64;

/// Maximum number of segments in a portable body path.
pub const MAX_PROJECTION_PATH_SEGMENTS: usize = 32;

/// Stable occurrence fields that portable expressions may read.
///
/// Delivery metadata, clocks, tracing, and workflow headers are intentionally
/// excluded so retries and execution placement cannot change a result.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionEnvelopeField {
    /// Canonical occurrence-envelope version.
    OccurrenceVersion,
    /// Retry-stable domain-event occurrence ID.
    OccurrenceId,
    /// Semantic domain-event name.
    EventName,
    /// Semantic domain-event version.
    EventVersion,
    /// Canonical event-body schema fingerprint.
    BodyFingerprint,
    /// Event-body semantic kind (`state`, `event`, or `deletion`).
    BodyKind,
    /// Stable event-body type name.
    BodyTypeName,
    /// Independently evolving event-body schema version.
    BodyVersion,
    /// Canonical event-body schema identity.
    BodySchema,
    /// Canonical body codec.
    BodyCodec,
    /// Canonical body codec version.
    BodyCodecVersion,
    /// Stable aggregate type.
    AggregateType,
    /// Stable aggregate stream ID.
    AggregateId,
    /// Aggregate event sequence that caused publication.
    AggregateSequence,
    /// Publication position within the aggregate sequence.
    PublicationOrdinal,
}

impl ProjectionEnvelopeField {
    /// Whether a client knows this field from the command alone (the
    /// envelope version and the event's name and version), so a client
    /// manifest may carry it as an envelope expression. Every other field is
    /// known only once the server writes the event, and a manifest carries it
    /// as an opaque typed slot.
    pub fn is_client_intrinsic(self) -> bool {
        matches!(
            self,
            Self::OccurrenceVersion | Self::EventName | Self::EventVersion
        )
    }
}

/// Version-one deterministic scalar transforms.
///
/// This deliberately small set has direct JavaScript equivalents. New
/// transforms require an operation-semantics version change and cross-runtime
/// golden vectors.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionScalarTransform {
    /// Concatenate one or more strings in declared argument order.
    StringConcat,
    /// Return the first non-absent argument; explicit null is a value.
    FirstPresent,
}

/// Declared portable result codec for an event-body path.
///
/// The type is part of the program digest. In particular, a positive JSON
/// integer cannot silently collapse an `i64` field into a `u64` field.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "name",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ProjectionValueType {
    /// Boolean scalar.
    Boolean,
    /// Signed 64-bit integer encoded as a decimal string.
    I64,
    /// Unsigned 64-bit integer encoded as a decimal string.
    U64,
    /// Finite IEEE-754 double encoded as a canonical decimal string.
    F64,
    /// UTF-8 string.
    String,
    /// Typed enum whose body representation is its string variant.
    Enum(String),
    /// Recursively tagged arbitrary JSON.
    Json,
}

/// One canonical literal used by a portable projection expression.
///
/// Its serde form is the client wire's tagged value
/// (`{"type":"u64","value":"7"}`). Deserializing accepts only canonical
/// values: decimal numbers as Rust prints them, object members sorted and
/// unique, nesting within [`MAX_PROJECTION_EXPRESSION_DEPTH`].
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct ProjectionValue(ProjectionValueKind);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum ProjectionValueKind {
    Null,
    Boolean(bool),
    I64(String),
    U64(String),
    F64(String),
    String(String),
    Enum { enum_type: String, variant: String },
    List(Vec<ProjectionValue>),
    Object(Vec<ProjectionObjectValueField>),
}

/// One canonical object member of a resolved portable value.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionObjectValueField {
    name: String,
    value: ProjectionValue,
}

impl ProjectionObjectValueField {
    /// Return the canonical object-member name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the member value.
    pub fn value(&self) -> &ProjectionValue {
        &self.value
    }
}

/// Borrowed exhaustive view of a validated portable value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionValueRef<'a> {
    /// Explicit null.
    Null,
    /// Boolean scalar.
    Boolean(bool),
    /// Signed 64-bit decimal text.
    I64(&'a str),
    /// Unsigned 64-bit decimal text.
    U64(&'a str),
    /// Finite canonical IEEE-754 decimal text.
    F64(&'a str),
    /// UTF-8 string.
    String(&'a str),
    /// Typed enum scalar.
    Enum {
        /// Stable enum type.
        enum_type: &'a str,
        /// Stable variant.
        variant: &'a str,
    },
    /// Ordered portable list.
    List(&'a [ProjectionValue]),
    /// Lexicographically ordered portable object.
    Object(&'a [ProjectionObjectValueField]),
}

impl<'de> Deserialize<'de> for ProjectionValue {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Children deserialize through this impl first, so each node checks
        // only itself and the depth of the subtree below it.
        let value = Self(ProjectionValueKind::deserialize(deserializer)?);
        value
            .validate_canonical()
            .map_err(serde::de::Error::custom)?;
        Ok(value)
    }
}

impl ProjectionValue {
    fn validate_canonical(&self) -> Result<(), ProjectionProgramError> {
        let canonical = match &self.0 {
            ProjectionValueKind::I64(text) => text
                .parse::<i64>()
                .is_ok_and(|value| value.to_string() == *text),
            ProjectionValueKind::U64(text) => text
                .parse::<u64>()
                .is_ok_and(|value| value.to_string() == *text),
            ProjectionValueKind::F64(text) => text.parse::<f64>().is_ok_and(|value| {
                Self::try_float(value).is_ok_and(|canonical| canonical == *self)
            }),
            ProjectionValueKind::Object(fields) => {
                fields.windows(2).all(|pair| pair[0].name < pair[1].name)
            }
            _ => true,
        };
        if !canonical {
            return Err(ProjectionProgramError::CanonicalJson(
                "projection value numbers must be canonical decimal text and object members \
                 sorted and unique"
                    .to_owned(),
            ));
        }
        let depth = portable_value_depth(self, 1);
        if depth > MAX_PROJECTION_EXPRESSION_DEPTH {
            return Err(ProjectionProgramError::ExpressionTooDeep {
                depth,
                max: MAX_PROJECTION_EXPRESSION_DEPTH,
            });
        }
        Ok(())
    }

    /// Validate and construct a canonical JSON-compatible literal.
    ///
    /// # Errors
    ///
    /// Returns [`ProjectionProgramError::ExpressionTooDeep`] when nested
    /// arrays or objects exceed the portable limit.
    pub fn try_from_json(value: Value) -> Result<Self, ProjectionProgramError> {
        validate_value_depth(&value, 1)?;
        Self::from_json(value)
    }

    /// Construct a null literal.
    pub fn null() -> Self {
        Self(ProjectionValueKind::Null)
    }

    /// Construct a boolean literal.
    pub fn boolean(value: bool) -> Self {
        Self(ProjectionValueKind::Boolean(value))
    }

    /// Construct a signed integer literal.
    pub fn signed(value: i64) -> Self {
        Self(ProjectionValueKind::I64(value.to_string()))
    }

    /// Construct an unsigned integer literal.
    pub fn unsigned(value: u64) -> Self {
        Self(ProjectionValueKind::U64(value.to_string()))
    }

    /// Construct a finite floating-point literal.
    ///
    /// # Errors
    ///
    /// Returns [`ProjectionProgramError::NonFiniteFloat`] for NaN or infinity.
    pub fn try_float(value: f64) -> Result<Self, ProjectionProgramError> {
        let normalized = if value == 0.0 { 0.0 } else { value };
        serde_json::Number::from_f64(normalized)
            .map(|number| Self(ProjectionValueKind::F64(number.to_string())))
            .ok_or(ProjectionProgramError::NonFiniteFloat)
    }

    /// Construct a string literal.
    pub fn string(value: impl Into<String>) -> Self {
        Self(ProjectionValueKind::String(value.into()))
    }

    /// Return whether this is an explicit null value.
    pub fn is_null(&self) -> bool {
        matches!(self.0, ProjectionValueKind::Null)
    }

    /// Borrow the exact tagged value without reparsing canonical JSON.
    pub fn as_ref(&self) -> ProjectionValueRef<'_> {
        match &self.0 {
            ProjectionValueKind::Null => ProjectionValueRef::Null,
            ProjectionValueKind::Boolean(value) => ProjectionValueRef::Boolean(*value),
            ProjectionValueKind::I64(value) => ProjectionValueRef::I64(value),
            ProjectionValueKind::U64(value) => ProjectionValueRef::U64(value),
            ProjectionValueKind::F64(value) => ProjectionValueRef::F64(value),
            ProjectionValueKind::String(value) => ProjectionValueRef::String(value),
            ProjectionValueKind::Enum { enum_type, variant } => {
                ProjectionValueRef::Enum { enum_type, variant }
            }
            ProjectionValueKind::List(values) => ProjectionValueRef::List(values),
            ProjectionValueKind::Object(fields) => ProjectionValueRef::Object(fields),
        }
    }

    /// Whether this value may be one component of a record key: a non-null
    /// scalar.
    pub fn valid_key_component(&self) -> bool {
        matches!(
            self.0,
            ProjectionValueKind::Boolean(_)
                | ProjectionValueKind::I64(_)
                | ProjectionValueKind::U64(_)
                | ProjectionValueKind::F64(_)
                | ProjectionValueKind::String(_)
                | ProjectionValueKind::Enum { .. }
        )
    }

    fn as_string(&self) -> Option<&str> {
        match &self.0 {
            ProjectionValueKind::String(value) => Some(value),
            _ => None,
        }
    }

    /// Construct a typed enum literal.
    pub fn enum_variant(enum_type: impl Into<String>, variant: impl Into<String>) -> Self {
        Self(ProjectionValueKind::Enum {
            enum_type: enum_type.into(),
            variant: variant.into(),
        })
    }

    fn list(values: Vec<Self>) -> Self {
        Self(ProjectionValueKind::List(values))
    }

    fn object(fields: Vec<ProjectionObjectValueField>) -> Self {
        Self(ProjectionValueKind::Object(fields))
    }

    fn from_json(value: Value) -> Result<Self, ProjectionProgramError> {
        match value {
            Value::Null => Ok(Self::null()),
            Value::Bool(value) => Ok(Self::boolean(value)),
            Value::String(value) => Ok(Self::string(value)),
            Value::Number(number) => {
                if let Some(value) = number.as_i64().filter(|value| *value < 0) {
                    Ok(Self::signed(value))
                } else if let Some(value) = number.as_u64() {
                    Ok(Self::unsigned(value))
                } else {
                    let value = number.as_f64().ok_or_else(|| {
                        ProjectionProgramError::CanonicalJson(
                            "JSON number has no portable scalar representation".to_owned(),
                        )
                    })?;
                    Self::try_float(value)
                }
            }
            Value::Array(values) => Ok(Self::list(
                values
                    .into_iter()
                    .map(Self::from_json)
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            Value::Object(values) => {
                let mut fields = values
                    .into_iter()
                    .map(|(name, value)| {
                        Ok(ProjectionObjectValueField {
                            name,
                            value: Self::from_json(value)?,
                        })
                    })
                    .collect::<Result<Vec<_>, ProjectionProgramError>>()?;
                fields.sort_by(|left, right| left.name.cmp(&right.name));
                Ok(Self::object(fields))
            }
        }
    }

    fn try_from_typed_json(
        value: Value,
        value_type: &ProjectionValueType,
    ) -> Result<Self, ProjectionProgramError> {
        if value.is_null() {
            return Ok(Self::null());
        }
        match value_type {
            ProjectionValueType::Boolean => value
                .as_bool()
                .map(Self::boolean)
                .ok_or_else(|| type_mismatch("boolean", &value)),
            ProjectionValueType::I64 => value
                .as_i64()
                .map(Self::signed)
                .ok_or_else(|| type_mismatch("i64", &value)),
            ProjectionValueType::U64 => value
                .as_u64()
                .map(Self::unsigned)
                .ok_or_else(|| type_mismatch("u64", &value)),
            ProjectionValueType::F64 => value
                .as_f64()
                .ok_or_else(|| type_mismatch("f64", &value))
                .and_then(Self::try_float),
            ProjectionValueType::String => value
                .as_str()
                .map(Self::string)
                .ok_or_else(|| type_mismatch("string", &value)),
            ProjectionValueType::Enum(enum_type) => value
                .as_str()
                .map(|variant| Self::enum_variant(enum_type.clone(), variant.to_owned()))
                .ok_or_else(|| type_mismatch("enum string", &value)),
            ProjectionValueType::Json => Self::try_from_json(value),
        }
    }
}

/// Result of evaluating a portable expression or assignment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "state", content = "value", rename_all = "snake_case")]
pub enum ResolvedProjectionValue {
    /// A concrete value; JSON null remains a concrete value.
    Value(ProjectionValue),
    /// The selected optional input did not exist.
    Absent,
    /// The projection explicitly removes this field.
    Unset,
}

/// A bounded, deterministic expression over a domain-event occurrence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct ProjectionExpression {
    expression: ProjectionExpressionKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ProjectionExpressionKind {
    BodyPath {
        path: Vec<String>,
        value_type: ProjectionValueType,
    },
    Envelope {
        field: ProjectionEnvelopeField,
    },
    Constant {
        value: ProjectionValue,
    },
    Enum {
        enum_type: String,
        variant: String,
    },
    List {
        values: Vec<ProjectionExpression>,
    },
    Object {
        fields: Vec<ProjectionExpressionObjectField>,
    },
    Transform {
        transform: ProjectionScalarTransform,
        arguments: Vec<ProjectionExpression>,
    },
}

/// One canonical object member in a portable projection expression.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ProjectionExpressionObjectField {
    name: String,
    value: ProjectionExpression,
}

impl ProjectionExpressionObjectField {
    /// Return the canonical member name.
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Return the member expression.
    pub(crate) fn value(&self) -> &ProjectionExpression {
        &self.value
    }
}

/// Exhaustive borrowed view of one portable projection expression.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProjectionExpressionRef<'a> {
    /// Read one typed outward-event body path.
    BodyPath {
        /// Raw body path, retained only on the server.
        path: &'a [String],
        /// Portable body value type.
        value_type: &'a ProjectionValueType,
    },
    /// Read one retry-stable occurrence-envelope field.
    Envelope {
        /// Selected field.
        field: ProjectionEnvelopeField,
    },
    /// Embed one portable typed constant.
    Constant {
        /// Constant value.
        value: &'a ProjectionValue,
    },
    /// Embed one typed enum variant.
    Enum {
        /// Enum type.
        enum_type: &'a str,
        /// Enum variant.
        variant: &'a str,
    },
    /// Construct an ordered list.
    List {
        /// Child expressions.
        values: &'a [ProjectionExpression],
    },
    /// Construct a canonical object.
    Object {
        /// Canonical object members.
        fields: &'a [ProjectionExpressionObjectField],
    },
    /// Apply one portable scalar transform.
    Transform {
        /// Closed transform.
        transform: ProjectionScalarTransform,
        /// Ordered arguments.
        arguments: &'a [ProjectionExpression],
    },
}

impl ProjectionExpression {
    /// Select a nested property from the canonical event body.
    ///
    /// A missing property resolves to [`ResolvedProjectionValue::Absent`];
    /// an explicitly null property resolves to a concrete null value.
    ///
    /// # Errors
    ///
    /// Rejects empty path segments and paths beyond the portable limit.
    pub fn body_path<I, S>(
        value_type: ProjectionValueType,
        segments: I,
    ) -> Result<Self, ProjectionProgramError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let path = segments.into_iter().map(Into::into).collect::<Vec<_>>();
        if path.len() > MAX_PROJECTION_PATH_SEGMENTS {
            return Err(ProjectionProgramError::PathTooDeep {
                segments: path.len(),
                max: MAX_PROJECTION_PATH_SEGMENTS,
            });
        }
        if path.iter().any(|segment| segment.is_empty()) {
            return Err(ProjectionProgramError::EmptyName("body path segment"));
        }
        if matches!(&value_type, ProjectionValueType::Enum(name) if name.is_empty()) {
            return Err(ProjectionProgramError::EmptyName("body enum type"));
        }
        Ok(Self {
            expression: ProjectionExpressionKind::BodyPath { path, value_type },
        })
    }

    /// Read one stable semantic field from the occurrence envelope.
    pub fn envelope(field: ProjectionEnvelopeField) -> Self {
        Self {
            expression: ProjectionExpressionKind::Envelope { field },
        }
    }

    /// Embed a validated literal.
    pub fn constant(value: ProjectionValue) -> Self {
        Self {
            expression: ProjectionExpressionKind::Constant { value },
        }
    }

    /// Embed a typed enum variant.
    ///
    /// # Errors
    ///
    /// Rejects an empty enum type or variant.
    pub fn enum_variant(
        enum_type: impl Into<String>,
        variant: impl Into<String>,
    ) -> Result<Self, ProjectionProgramError> {
        let enum_type = non_empty(enum_type.into(), "enum type")?;
        let variant = non_empty(variant.into(), "enum variant")?;
        Ok(Self {
            expression: ProjectionExpressionKind::Enum { enum_type, variant },
        })
    }

    /// Construct a deterministic list from child expressions.
    ///
    /// # Errors
    ///
    /// Rejects expression trees beyond the portable depth limit.
    pub fn list(values: Vec<Self>) -> Result<Self, ProjectionProgramError> {
        let expression = Self {
            expression: ProjectionExpressionKind::List { values },
        };
        expression.validate_depth()?;
        Ok(expression)
    }

    /// Construct an object whose keys are canonicalized lexicographically.
    ///
    /// # Errors
    ///
    /// Rejects empty or duplicate fields and trees beyond the depth limit.
    pub fn object<I, S>(fields: I) -> Result<Self, ProjectionProgramError>
    where
        I: IntoIterator<Item = (S, Self)>,
        S: Into<String>,
    {
        let mut fields = fields
            .into_iter()
            .map(|(name, value)| {
                Ok(ProjectionExpressionObjectField {
                    name: non_empty(name.into(), "object field")?,
                    value,
                })
            })
            .collect::<Result<Vec<_>, ProjectionProgramError>>()?;
        fields.sort_by(|left, right| left.name.cmp(&right.name));
        for pair in fields.windows(2) {
            if pair[0].name == pair[1].name {
                return Err(ProjectionProgramError::DuplicateName {
                    kind: "object field",
                    name: pair[0].name.clone(),
                });
            }
        }
        let expression = Self {
            expression: ProjectionExpressionKind::Object { fields },
        };
        expression.validate_depth()?;
        Ok(expression)
    }

    /// Apply one closed, versioned scalar transform.
    ///
    /// # Errors
    ///
    /// Rejects an empty argument list and trees beyond the depth limit.
    pub fn transform(
        transform: ProjectionScalarTransform,
        arguments: Vec<Self>,
    ) -> Result<Self, ProjectionProgramError> {
        if arguments.is_empty() {
            return Err(ProjectionProgramError::InvalidOperation {
                operation: "portable expression".to_owned(),
                reason: "scalar transforms require at least one argument".to_owned(),
            });
        }
        let expression = Self {
            expression: ProjectionExpressionKind::Transform {
                transform,
                arguments,
            },
        };
        expression.validate_depth()?;
        Ok(expression)
    }

    /// Borrow the complete portable expression without JSON re-parsing.
    pub(crate) fn as_ref(&self) -> ProjectionExpressionRef<'_> {
        match &self.expression {
            ProjectionExpressionKind::BodyPath { path, value_type } => {
                ProjectionExpressionRef::BodyPath { path, value_type }
            }
            ProjectionExpressionKind::Envelope { field } => {
                ProjectionExpressionRef::Envelope { field: *field }
            }
            ProjectionExpressionKind::Constant { value } => {
                ProjectionExpressionRef::Constant { value }
            }
            ProjectionExpressionKind::Enum { enum_type, variant } => {
                ProjectionExpressionRef::Enum { enum_type, variant }
            }
            ProjectionExpressionKind::List { values } => ProjectionExpressionRef::List { values },
            ProjectionExpressionKind::Object { fields } => {
                ProjectionExpressionRef::Object { fields }
            }
            ProjectionExpressionKind::Transform {
                transform,
                arguments,
            } => ProjectionExpressionRef::Transform {
                transform: *transform,
                arguments,
            },
        }
    }

    pub(crate) fn resolve(
        &self,
        occurrence: &DomainEventOccurrence,
        body: &Value,
    ) -> Result<ResolvedProjectionValue, ProjectionProgramError> {
        match &self.expression {
            ProjectionExpressionKind::BodyPath { path, value_type } => {
                let mut current = body;
                for segment in path {
                    let Value::Object(object) = current else {
                        return Ok(ResolvedProjectionValue::Absent);
                    };
                    let Some(next) = object.get(segment) else {
                        return Ok(ResolvedProjectionValue::Absent);
                    };
                    current = next;
                }
                Ok(ResolvedProjectionValue::Value(
                    ProjectionValue::try_from_typed_json(current.clone(), value_type)?,
                ))
            }
            ProjectionExpressionKind::Envelope { field } => {
                let value = match field {
                    ProjectionEnvelopeField::OccurrenceVersion => {
                        ProjectionValue::unsigned(occurrence.occurrence_version().into())
                    }
                    ProjectionEnvelopeField::OccurrenceId => {
                        ProjectionValue::string(occurrence.id())
                    }
                    ProjectionEnvelopeField::EventName => {
                        ProjectionValue::string(occurrence.descriptor().name.to_string())
                    }
                    ProjectionEnvelopeField::EventVersion => {
                        ProjectionValue::unsigned(occurrence.descriptor().version)
                    }
                    ProjectionEnvelopeField::BodyFingerprint => ProjectionValue::string(
                        occurrence.descriptor().body.fingerprint.to_string(),
                    ),
                    ProjectionEnvelopeField::BodyKind => ProjectionValue::string(
                        match occurrence.descriptor().body.kind {
                            crate::DomainEventBodyKind::State => "state",
                            crate::DomainEventBodyKind::Event => "event",
                            crate::DomainEventBodyKind::Deletion => "deletion",
                        }
                        .to_owned(),
                    ),
                    ProjectionEnvelopeField::BodyTypeName => {
                        ProjectionValue::string(occurrence.descriptor().body.type_name.to_string())
                    }
                    ProjectionEnvelopeField::BodyVersion => {
                        ProjectionValue::unsigned(occurrence.descriptor().body.version)
                    }
                    ProjectionEnvelopeField::BodySchema => {
                        ProjectionValue::string(occurrence.descriptor().body.schema.to_string())
                    }
                    ProjectionEnvelopeField::BodyCodec => {
                        ProjectionValue::string(occurrence.descriptor().body.codec.to_string())
                    }
                    ProjectionEnvelopeField::BodyCodecVersion => {
                        ProjectionValue::unsigned(occurrence.descriptor().body.codec_version.into())
                    }
                    ProjectionEnvelopeField::AggregateType => {
                        ProjectionValue::string(occurrence.aggregate_type())
                    }
                    ProjectionEnvelopeField::AggregateId => {
                        ProjectionValue::string(occurrence.aggregate_id())
                    }
                    ProjectionEnvelopeField::AggregateSequence => {
                        ProjectionValue::unsigned(occurrence.aggregate_sequence())
                    }
                    ProjectionEnvelopeField::PublicationOrdinal => {
                        ProjectionValue::unsigned(occurrence.publication_ordinal().into())
                    }
                };
                Ok(ResolvedProjectionValue::Value(value))
            }
            ProjectionExpressionKind::Constant { value } => {
                Ok(ResolvedProjectionValue::Value(value.clone()))
            }
            ProjectionExpressionKind::Enum { enum_type, variant } => {
                Ok(ResolvedProjectionValue::Value(
                    ProjectionValue::enum_variant(enum_type.clone(), variant.clone()),
                ))
            }
            ProjectionExpressionKind::List { values } => {
                let mut resolved = Vec::with_capacity(values.len());
                for value in values {
                    match value.resolve(occurrence, body)? {
                        ResolvedProjectionValue::Value(value) => resolved.push(value),
                        ResolvedProjectionValue::Absent => {
                            return Ok(ResolvedProjectionValue::Absent);
                        }
                        ResolvedProjectionValue::Unset => {
                            return Err(ProjectionProgramError::UnsetNotAllowed {
                                field: "list element".to_owned(),
                            });
                        }
                    }
                }
                Ok(ResolvedProjectionValue::Value(ProjectionValue::list(
                    resolved,
                )))
            }
            ProjectionExpressionKind::Object { fields } => {
                let mut resolved = Vec::with_capacity(fields.len());
                for field in fields {
                    match field.value.resolve(occurrence, body)? {
                        ResolvedProjectionValue::Value(value) => {
                            resolved.push(ProjectionObjectValueField {
                                name: field.name.clone(),
                                value,
                            });
                        }
                        ResolvedProjectionValue::Absent => {
                            return Ok(ResolvedProjectionValue::Absent);
                        }
                        ResolvedProjectionValue::Unset => {
                            return Err(ProjectionProgramError::UnsetNotAllowed {
                                field: field.name.clone(),
                            });
                        }
                    }
                }
                Ok(ResolvedProjectionValue::Value(ProjectionValue::object(
                    resolved,
                )))
            }
            ProjectionExpressionKind::Transform {
                transform,
                arguments,
            } => match transform {
                ProjectionScalarTransform::StringConcat => {
                    let mut output = String::new();
                    for argument in arguments {
                        match argument.resolve(occurrence, body)? {
                            ResolvedProjectionValue::Value(value) => {
                                let Some(value) = value.as_string() else {
                                    return Err(ProjectionProgramError::InvalidOperation {
                                        operation: "string_concat expression".to_owned(),
                                        reason: "all arguments must resolve to strings".to_owned(),
                                    });
                                };
                                output.push_str(value);
                            }
                            ResolvedProjectionValue::Absent => {
                                return Ok(ResolvedProjectionValue::Absent);
                            }
                            ResolvedProjectionValue::Unset => {
                                return Err(ProjectionProgramError::UnsetNotAllowed {
                                    field: "string_concat argument".to_owned(),
                                });
                            }
                        }
                    }
                    Ok(ResolvedProjectionValue::Value(ProjectionValue::string(
                        output,
                    )))
                }
                ProjectionScalarTransform::FirstPresent => {
                    for argument in arguments {
                        let value = argument.resolve(occurrence, body)?;
                        if value != ResolvedProjectionValue::Absent {
                            return Ok(value);
                        }
                    }
                    Ok(ResolvedProjectionValue::Absent)
                }
            },
        }
    }

    fn validate_depth(&self) -> Result<(), ProjectionProgramError> {
        fn depth(expression: &ProjectionExpression, level: usize) -> usize {
            match &expression.expression {
                ProjectionExpressionKind::List { values } => values
                    .iter()
                    .map(|value| depth(value, level + 1))
                    .max()
                    .unwrap_or(level),
                ProjectionExpressionKind::Object { fields } => fields
                    .iter()
                    .map(|field| depth(&field.value, level + 1))
                    .max()
                    .unwrap_or(level),
                ProjectionExpressionKind::Transform { arguments, .. } => arguments
                    .iter()
                    .map(|argument| depth(argument, level + 1))
                    .max()
                    .unwrap_or(level),
                ProjectionExpressionKind::Constant { value } => {
                    level + portable_value_depth(value, 1).saturating_sub(1)
                }
                _ => level,
            }
        }
        let depth = depth(self, 1);
        if depth > MAX_PROJECTION_EXPRESSION_DEPTH {
            return Err(ProjectionProgramError::ExpressionTooDeep {
                depth,
                max: MAX_PROJECTION_EXPRESSION_DEPTH,
            });
        }
        Ok(())
    }
}

/// A field assignment that preserves the difference between null and removal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "expression", rename_all = "snake_case")]
pub enum ProjectionAssignment {
    /// Evaluate and assign a concrete value.
    Set(ProjectionExpression),
    /// Explicitly remove the field.
    Unset,
}

/// Exhaustive borrowed view of a portable field assignment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProjectionAssignmentRef<'a> {
    /// Evaluate and assign an expression.
    Set(&'a ProjectionExpression),
    /// Explicitly remove the field.
    Unset,
}

impl ProjectionAssignment {
    /// Borrow set versus unset without serializing the assignment.
    pub(crate) fn as_ref(&self) -> ProjectionAssignmentRef<'_> {
        match self {
            Self::Set(expression) => ProjectionAssignmentRef::Set(expression),
            Self::Unset => ProjectionAssignmentRef::Unset,
        }
    }

    pub(crate) fn resolve(
        &self,
        occurrence: &DomainEventOccurrence,
        body: &Value,
    ) -> Result<ResolvedProjectionValue, ProjectionProgramError> {
        match self {
            Self::Set(expression) => expression.resolve(occurrence, body),
            Self::Unset => Ok(ResolvedProjectionValue::Unset),
        }
    }
}

fn validate_value_depth(value: &Value, level: usize) -> Result<(), ProjectionProgramError> {
    let depth = json_depth(value, level);
    if depth > MAX_PROJECTION_EXPRESSION_DEPTH {
        return Err(ProjectionProgramError::ExpressionTooDeep {
            depth,
            max: MAX_PROJECTION_EXPRESSION_DEPTH,
        });
    }
    Ok(())
}

fn json_depth(value: &Value, level: usize) -> usize {
    match value {
        Value::Array(values) => values
            .iter()
            .map(|value| json_depth(value, level + 1))
            .max()
            .unwrap_or(level),
        Value::Object(fields) => fields
            .values()
            .map(|value| json_depth(value, level + 1))
            .max()
            .unwrap_or(level),
        _ => level,
    }
}

fn portable_value_depth(value: &ProjectionValue, level: usize) -> usize {
    match &value.0 {
        ProjectionValueKind::List(values) => values
            .iter()
            .map(|value| portable_value_depth(value, level + 1))
            .max()
            .unwrap_or(level),
        ProjectionValueKind::Object(fields) => fields
            .iter()
            .map(|field| portable_value_depth(&field.value, level + 1))
            .max()
            .unwrap_or(level),
        _ => level,
    }
}

fn type_mismatch(expected: &'static str, value: &Value) -> ProjectionProgramError {
    let actual = match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(number) if number.is_i64() => "integer",
        Value::Number(number) if number.is_u64() => "unsigned integer",
        Value::Number(_) => "float",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    };
    ProjectionProgramError::ValueTypeMismatch { expected, actual }
}

pub(crate) fn validate_named_ordinals<T>(
    values: &[T],
    kind: &'static str,
    ordinal: impl Fn(&T) -> u32,
    name: impl Fn(&T) -> &str,
) -> Result<(), ProjectionProgramError> {
    let mut names = BTreeSet::new();
    for (expected, value) in values.iter().enumerate() {
        let actual = ordinal(value);
        let expected = expected as u32;
        if actual != expected {
            return if values
                .iter()
                .take(expected as usize)
                .any(|prior| ordinal(prior) == actual)
            {
                Err(ProjectionProgramError::DuplicateOrdinal {
                    kind,
                    ordinal: actual,
                })
            } else {
                Err(ProjectionProgramError::NonContiguousOrdinal {
                    kind,
                    expected,
                    actual,
                })
            };
        }
        let candidate = name(value);
        if !names.insert(candidate) {
            return Err(ProjectionProgramError::DuplicateName {
                kind,
                name: candidate.to_owned(),
            });
        }
    }
    Ok(())
}

pub(crate) fn validate_ordinals<T>(
    values: &[T],
    kind: &'static str,
    ordinal: impl Fn(&T) -> u32,
) -> Result<(), ProjectionProgramError> {
    for (expected, value) in values.iter().enumerate() {
        let actual = ordinal(value);
        let expected = expected as u32;
        if actual != expected {
            return if values
                .iter()
                .take(expected as usize)
                .any(|prior| ordinal(prior) == actual)
            {
                Err(ProjectionProgramError::DuplicateOrdinal {
                    kind,
                    ordinal: actual,
                })
            } else {
                Err(ProjectionProgramError::NonContiguousOrdinal {
                    kind,
                    expected,
                    actual,
                })
            };
        }
    }
    Ok(())
}

pub(crate) fn expressions_statically_distinct(
    left: &ProjectionExpression,
    right: &ProjectionExpression,
) -> bool {
    match (&left.expression, &right.expression) {
        (
            ProjectionExpressionKind::Constant { value: left },
            ProjectionExpressionKind::Constant { value: right },
        ) => left != right,
        (
            ProjectionExpressionKind::Enum {
                enum_type: left_type,
                variant: left_variant,
            },
            ProjectionExpressionKind::Enum {
                enum_type: right_type,
                variant: right_variant,
            },
        ) => left_type != right_type || left_variant != right_variant,
        _ => false,
    }
}

pub(crate) fn non_empty(
    value: String,
    kind: &'static str,
) -> Result<String, ProjectionProgramError> {
    if value.is_empty() {
        Err(ProjectionProgramError::EmptyName(kind))
    } else {
        Ok(value)
    }
}

#[cfg(test)]
mod borrowed_view_tests {
    use super::*;

    fn kind(expression: &ProjectionExpression) -> &'static str {
        match expression.as_ref() {
            ProjectionExpressionRef::BodyPath { .. } => "body_path",
            ProjectionExpressionRef::Envelope { .. } => "envelope",
            ProjectionExpressionRef::Constant { .. } => "constant",
            ProjectionExpressionRef::Enum { .. } => "enum",
            ProjectionExpressionRef::List { .. } => "list",
            ProjectionExpressionRef::Object { .. } => "object",
            ProjectionExpressionRef::Transform { .. } => "transform",
        }
    }

    #[test]
    fn borrowed_expression_and_assignment_views_are_exhaustive() {
        let body =
            ProjectionExpression::body_path(ProjectionValueType::String, ["state", "todo_id"])
                .unwrap();
        let expressions = [
            body.clone(),
            ProjectionExpression::envelope(ProjectionEnvelopeField::OccurrenceId),
            ProjectionExpression::constant(ProjectionValue::string("fixed")),
            ProjectionExpression::enum_variant("TodoStatus", "Completed").unwrap(),
            ProjectionExpression::list(vec![body.clone()]).unwrap(),
            ProjectionExpression::object([("id", body.clone())]).unwrap(),
            ProjectionExpression::transform(ProjectionScalarTransform::FirstPresent, vec![body])
                .unwrap(),
        ];
        assert_eq!(
            expressions.iter().map(kind).collect::<Vec<_>>(),
            [
                "body_path",
                "envelope",
                "constant",
                "enum",
                "list",
                "object",
                "transform"
            ]
        );
        assert!(matches!(
            ProjectionAssignment::Set(expressions[0].clone()).as_ref(),
            ProjectionAssignmentRef::Set(_)
        ));
        assert_eq!(
            ProjectionAssignment::Unset.as_ref(),
            ProjectionAssignmentRef::Unset
        );
        let ProjectionExpressionRef::Object { fields } = expressions[5].as_ref() else {
            panic!("expected object view");
        };
        assert_eq!(fields[0].name(), "id");
        assert!(matches!(
            fields[0].value().as_ref(),
            ProjectionExpressionRef::BodyPath { .. }
        ));
    }
}

#[cfg(test)]
mod value_wire_tests {
    use super::*;

    fn parse(json: &str) -> Result<ProjectionValue, serde_json::Error> {
        serde_json::from_str(json)
    }

    #[test]
    fn canonical_values_round_trip_through_the_wire_form() {
        let value = ProjectionValue::try_from_json(serde_json::json!({
            "b": [1, -2, 1.5, "x", null, true],
            "a": {}
        }))
        .unwrap();
        let wire = serde_json::to_string(&value).unwrap();
        assert_eq!(parse(&wire).unwrap(), value);
        assert_eq!(
            parse(r#"{"type":"enum","value":{"enum_type":"Status","variant":"OPEN"}}"#).unwrap(),
            ProjectionValue::enum_variant("Status", "OPEN")
        );
    }

    #[test]
    fn non_canonical_values_are_refused() {
        for json in [
            r#"{"type":"u64","value":"01"}"#,
            r#"{"type":"u64","value":"-1"}"#,
            r#"{"type":"i64","value":"+1"}"#,
            r#"{"type":"f64","value":"1.50"}"#,
            r#"{"type":"f64","value":"-0.0"}"#,
            r#"{"type":"f64","value":"NaN"}"#,
            r#"{"type":"object","value":[{"name":"b","value":{"type":"null"}},{"name":"a","value":{"type":"null"}}]}"#,
            r#"{"type":"object","value":[{"name":"a","value":{"type":"null"}},{"name":"a","value":{"type":"null"}}]}"#,
            r#"{"type":"null","extra":1}"#,
        ] {
            assert!(parse(json).is_err(), "{json} must be refused");
        }
    }

    #[test]
    fn value_depth_is_bounded() {
        let nested = |depth: usize| {
            let mut json = r#"{"type":"null"}"#.to_owned();
            for _ in 1..depth {
                json = format!(r#"{{"type":"list","value":[{json}]}}"#);
            }
            json
        };
        assert!(parse(&nested(MAX_PROJECTION_EXPRESSION_DEPTH)).is_ok());
        assert!(parse(&nested(MAX_PROJECTION_EXPRESSION_DEPTH + 1)).is_err());
    }

    #[test]
    fn only_command_known_envelope_fields_are_client_intrinsic() {
        let intrinsic = [
            ProjectionEnvelopeField::OccurrenceVersion,
            ProjectionEnvelopeField::EventName,
            ProjectionEnvelopeField::EventVersion,
        ];
        for field in intrinsic {
            assert!(field.is_client_intrinsic());
        }
        assert!(!ProjectionEnvelopeField::OccurrenceId.is_client_intrinsic());
        assert!(!ProjectionEnvelopeField::AggregateSequence.is_client_intrinsic());
    }
}
