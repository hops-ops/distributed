use std::fmt;

use crate::projection_protocol::ProjectionProtocolValidationError;

/// A projection identity could not be encoded without ambiguity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ProjectionScopeCodecError {
    BlankModelRegistration,
    InvalidModelRegistration {
        model: String,
        reason: String,
    },
    ModelRegistrationMismatch {
        declared: String,
        schema: String,
    },
    DuplicateModelRegistration {
        model: String,
    },
    ProjectorMismatch {
        expected: String,
        actual: String,
    },
    UnknownModel {
        projector: String,
        model: String,
    },
    #[cfg(any(feature = "graphql", test))]
    DuplicateKeyField {
        model: String,
        field: String,
    },
    #[cfg(any(feature = "graphql", test))]
    ExtraKeyField {
        model: String,
        field: String,
    },
    #[cfg(any(feature = "graphql", test))]
    MissingKeyField {
        model: String,
        field: String,
    },
    ExtraKeyColumn {
        model: String,
        column: String,
    },
    MissingKeyColumn {
        model: String,
        column: String,
    },
    NullPrimaryKey {
        model: String,
        field: String,
    },
    #[cfg(any(feature = "graphql", test))]
    WrongJsonShape {
        model: String,
        field: String,
        expected: &'static str,
        actual: &'static str,
    },
    WrongRowValueShape {
        model: String,
        column: String,
        expected: &'static str,
        actual: &'static str,
    },
    #[cfg(any(feature = "graphql", test))]
    IntegerOutOfRange {
        model: String,
        field: String,
        expected: &'static str,
    },
    NonFiniteFloat {
        model: String,
        field: String,
    },
    #[cfg(any(feature = "graphql", test))]
    InvalidBytes {
        model: String,
        field: String,
    },
    CanonicalEncodingTooLong {
        target: &'static str,
        max: usize,
    },
    Protocol(ProjectionProtocolValidationError),
}

impl fmt::Display for ProjectionScopeCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BlankModelRegistration => {
                formatter.write_str("projection model registration must not be blank")
            }
            Self::InvalidModelRegistration { model, reason } => {
                write!(formatter, "invalid projection model `{model}`: {reason}")
            }
            Self::ModelRegistrationMismatch { declared, schema } => write!(
                formatter,
                "projection model registration `{declared}` does not match schema model `{schema}`"
            ),
            Self::DuplicateModelRegistration { model } => {
                write!(
                    formatter,
                    "projection model `{model}` is already registered"
                )
            }
            Self::ProjectorMismatch { expected, actual } => write!(
                formatter,
                "projection scope belongs to projector `{expected}`, not `{actual}`"
            ),
            Self::UnknownModel { projector, model } => write!(
                formatter,
                "projector `{projector}` does not register projection model `{model}`"
            ),
            #[cfg(any(feature = "graphql", test))]
            Self::DuplicateKeyField { model, field } => {
                write!(
                    formatter,
                    "projection key for `{model}` repeats field `{field}`"
                )
            }
            #[cfg(any(feature = "graphql", test))]
            Self::ExtraKeyField { model, field } => write!(
                formatter,
                "projection key for `{model}` contains non-key field `{field}`"
            ),
            #[cfg(any(feature = "graphql", test))]
            Self::MissingKeyField { model, field } => {
                write!(
                    formatter,
                    "projection key for `{model}` is missing field `{field}`"
                )
            }
            Self::ExtraKeyColumn { model, column } => write!(
                formatter,
                "projector row key for `{model}` contains non-key column `{column}`"
            ),
            Self::MissingKeyColumn { model, column } => write!(
                formatter,
                "projector row key for `{model}` is missing column `{column}`"
            ),
            Self::NullPrimaryKey { model, field } => {
                write!(
                    formatter,
                    "projection key `{model}.{field}` must not be null"
                )
            }
            #[cfg(any(feature = "graphql", test))]
            Self::WrongJsonShape {
                model,
                field,
                expected,
                actual,
            } => write!(
                formatter,
                "projection key `{model}.{field}` must be {expected}, got {actual}"
            ),
            Self::WrongRowValueShape {
                model,
                column,
                expected,
                actual,
            } => write!(
                formatter,
                "projector row key `{model}.{column}` must be {expected}, got {actual}"
            ),
            #[cfg(any(feature = "graphql", test))]
            Self::IntegerOutOfRange {
                model,
                field,
                expected,
            } => write!(
                formatter,
                "projection key `{model}.{field}` is outside the {expected} range"
            ),
            Self::NonFiniteFloat { model, field } => write!(
                formatter,
                "projection key `{model}.{field}` must be a finite float"
            ),
            #[cfg(any(feature = "graphql", test))]
            Self::InvalidBytes { model, field } => write!(
                formatter,
                "projection key `{model}.{field}` must be canonical standard base64"
            ),
            Self::CanonicalEncodingTooLong { target, max } => {
                write!(formatter, "{target} canonical encoding exceeds {max} bytes")
            }
            Self::Protocol(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ProjectionScopeCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Protocol(error) => Some(error),
            _ => None,
        }
    }
}

impl From<ProjectionProtocolValidationError> for ProjectionScopeCodecError {
    fn from(error: ProjectionProtocolValidationError) -> Self {
        Self::Protocol(error)
    }
}
