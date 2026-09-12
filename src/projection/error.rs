use std::fmt;

/// A typed validation or resolution failure for a projection program.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProjectionProgramError {
    /// Derived facts are not new authoritative aggregate snapshots.
    DerivedSourceSnapshot,
    /// A stable name, identifier, field, or storage name was empty.
    EmptyName(&'static str),
    /// A declared version must be non-zero.
    ZeroVersion(&'static str),
    /// An event body fingerprint was not canonical lowercase SHA-256.
    InvalidBodyFingerprint,
    /// A program ID was not in canonical form.
    InvalidProgramId,
    /// A body path exceeded the portable segment limit.
    PathTooDeep {
        /// Observed segment count.
        segments: usize,
        /// Supported segment ceiling.
        max: usize,
    },
    /// A portable expression or constant exceeded the nesting limit.
    ExpressionTooDeep {
        /// Observed nesting depth.
        depth: usize,
        /// Supported nesting ceiling.
        max: usize,
    },
    /// An event arm declared too many portable operations.
    TooManyOperations {
        /// Observed operation count.
        count: usize,
        /// Supported operation ceiling.
        max: usize,
    },
    /// A key, partition, or other bounded canonical value was too large.
    ValueTooLarge {
        /// Kind of bounded logical encoding.
        kind: &'static str,
        /// Observed encoded byte length.
        len: usize,
        /// Supported byte ceiling.
        max: usize,
    },
    /// Two declarations used the same explicit ordinal.
    DuplicateOrdinal {
        /// Kind of ordered declaration.
        kind: &'static str,
        /// Repeated ordinal.
        ordinal: u32,
    },
    /// Ordinals did not form a contiguous zero-based sequence.
    NonContiguousOrdinal {
        /// Kind of ordered declaration.
        kind: &'static str,
        /// Required next ordinal.
        expected: u32,
        /// Observed ordinal.
        actual: u32,
    },
    /// Two declarations used the same stable name.
    DuplicateName {
        /// Kind of named declaration.
        kind: &'static str,
        /// Repeated name.
        name: String,
    },
    /// Two arms selected the same exact event contract.
    DuplicateSelector,
    /// No arm accepted the supplied event occurrence.
    NoMatchingArm,
    /// More than one arm accepted the supplied event occurrence.
    MultipleMatchingArms,
    /// A typed template marker did not exactly describe the program selectors.
    EventSetMismatch,
    /// A required expression evaluated as absent.
    RequiredValueAbsent {
        /// Stable expression or field description.
        path: String,
    },
    /// `unset` appeared where the operation cannot represent it.
    UnsetNotAllowed {
        /// Field or expression position.
        field: String,
    },
    /// A key component was null, absent, unset, or structurally invalid.
    InvalidKeyValue {
        /// Key component name.
        field: String,
    },
    /// An operation shape was incompatible with its semantic kind.
    InvalidOperation {
        /// Stable operation identifier.
        operation: String,
        /// Validation reason.
        reason: String,
    },
    /// Two operations could not deterministically become one final mutation.
    AmbiguousMutation {
        /// Affected logical model.
        model: String,
        /// Ambiguity reason.
        reason: String,
    },
    /// A constant used a non-finite floating-point value.
    NonFiniteFloat,
    /// A typed source path resolved to a value of another scalar type.
    ValueTypeMismatch {
        /// Declared portable value codec.
        expected: &'static str,
        /// Observed JSON value category.
        actual: &'static str,
    },
    /// Canonical JSON could not be encoded or decoded.
    CanonicalJson(String),
}

impl fmt::Display for ProjectionProgramError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DerivedSourceSnapshot => {
                formatter.write_str("derived facts cannot establish an aggregate source snapshot")
            }
            Self::EmptyName(kind) => write!(formatter, "{kind} must not be empty"),
            Self::ZeroVersion(kind) => write!(formatter, "{kind} must be non-zero"),
            Self::InvalidBodyFingerprint => formatter.write_str(
                "body fingerprint must be `sha256:` followed by 64 lowercase hex digits",
            ),
            Self::InvalidProgramId => formatter.write_str(
                "projection program ID must be `pp1:sha256:` followed by 64 lowercase hex digits",
            ),
            Self::PathTooDeep { segments, max } => {
                write!(formatter, "path has {segments} segments; maximum is {max}")
            }
            Self::ExpressionTooDeep { depth, max } => {
                write!(formatter, "expression depth {depth} exceeds maximum {max}")
            }
            Self::TooManyOperations { count, max } => {
                write!(formatter, "arm has {count} operations; maximum is {max}")
            }
            Self::ValueTooLarge { kind, len, max } => {
                write!(formatter, "{kind} is {len} bytes; maximum is {max}")
            }
            Self::DuplicateOrdinal { kind, ordinal } => {
                write!(formatter, "duplicate {kind} ordinal {ordinal}")
            }
            Self::NonContiguousOrdinal {
                kind,
                expected,
                actual,
            } => write!(
                formatter,
                "{kind} ordinals must be contiguous: expected {expected}, found {actual}"
            ),
            Self::DuplicateName { kind, name } => {
                write!(formatter, "duplicate {kind} name `{name}`")
            }
            Self::DuplicateSelector => formatter.write_str("duplicate exact event selector"),
            Self::NoMatchingArm => formatter.write_str("no projection arm matched the occurrence"),
            Self::MultipleMatchingArms => {
                formatter.write_str("multiple projection arms matched the occurrence")
            }
            Self::EventSetMismatch => formatter.write_str(
                "projection event-set marker does not exactly match the program selectors",
            ),
            Self::RequiredValueAbsent { path } => {
                write!(formatter, "required projection value `{path}` is absent")
            }
            Self::UnsetNotAllowed { field } => {
                write!(
                    formatter,
                    "field `{field}` cannot be unset by this operation"
                )
            }
            Self::InvalidKeyValue { field } => {
                write!(formatter, "field `{field}` is not a valid key value")
            }
            Self::InvalidOperation { operation, reason } => {
                write!(
                    formatter,
                    "invalid projection operation `{operation}`: {reason}"
                )
            }
            Self::AmbiguousMutation { model, reason } => {
                write!(
                    formatter,
                    "ambiguous mutation for model `{model}`: {reason}"
                )
            }
            Self::NonFiniteFloat => formatter.write_str("projection floats must be finite"),
            Self::ValueTypeMismatch { expected, actual } => {
                write!(
                    formatter,
                    "expected portable {expected} value, found {actual}"
                )
            }
            Self::CanonicalJson(message) => {
                write!(formatter, "canonical projection JSON failed: {message}")
            }
        }
    }
}

impl std::error::Error for ProjectionProgramError {}
