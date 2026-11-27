//! Why a selection did not compile.
//!
//! The variant, not the message, decides what the client sees: each one maps
//! to a fixed `extensions.code` and a fixed short message
//! ([`CompileError::client_code`], [`CompileError::client_message`]). The
//! detail strings are for logs and tests; they never reach the client, so they
//! may name SQL, columns or limits.

use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompileError {
    /// Selection, relationship or filter nesting beyond `max_depth`.
    TooDeep,
    /// The statement could produce more rows than `max_rows` (see the cost
    /// model in `docs/graphql-query-cost.md`).
    TooManyRows { rows: u64, max_rows: u64 },
    /// An `_in`/`_nin` list longer than `max_in_list`, or an `_and`/`_or`
    /// list wider than `max_bool_width`.
    ListTooLong(String),
    /// A response key (alias) that is not a GraphQL name.
    InvalidResponseKey(String),
    /// An unknown, ungranted or ambiguous `where` / `order_by` entry, or a
    /// comparison the dialect does not support.
    InvalidFilter(String),
    /// A read the model's cell-by-key store cannot serve.
    UnsupportedOnCellStore(String),
    /// Any other request the compiler refuses (a malformed argument value, a
    /// missing primary key, a claim the session lacks).
    BadRequest(String),
    /// The compiler reached something the per-privilege schema should never
    /// have offered: a server fault, not the request's.
    Internal(String),
}

impl CompileError {
    pub(crate) fn internal(detail: impl Into<String>) -> Self {
        Self::Internal(detail.into())
    }

    /// GraphQL `extensions.code`.
    pub fn client_code(&self) -> &'static str {
        match self {
            Self::Internal(_) => "INTERNAL",
            _ => "BAD_REQUEST",
        }
    }

    /// The client message: stable per variant and free of request detail.
    pub fn client_message(&self) -> &'static str {
        match self {
            Self::TooDeep => "max depth exceeded",
            Self::TooManyRows { .. } => "row budget exceeded",
            Self::ListTooLong(_) => "list too long",
            Self::InvalidResponseKey(_) => "invalid response key",
            Self::InvalidFilter(_) => "invalid filter",
            Self::UnsupportedOnCellStore(_) => "unsupported on cell store",
            Self::BadRequest(_) => "bad request",
            Self::Internal(_) => "internal error",
        }
    }
}

impl fmt::Display for CompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooDeep => f.write_str("max depth exceeded"),
            Self::TooManyRows { rows, max_rows } => {
                write!(f, "query may return {rows} rows, max_rows is {max_rows}")
            }
            Self::ListTooLong(detail)
            | Self::InvalidResponseKey(detail)
            | Self::InvalidFilter(detail)
            | Self::UnsupportedOnCellStore(detail)
            | Self::BadRequest(detail) => f.write_str(detail),
            Self::Internal(detail) => write!(f, "internal: {detail}"),
        }
    }
}

impl std::error::Error for CompileError {}

/// Helpers below the compiler (bind conversion, join keys, evidence plans)
/// report plain strings: a request they cannot express.
impl From<String> for CompileError {
    fn from(detail: String) -> Self {
        Self::BadRequest(detail)
    }
}
