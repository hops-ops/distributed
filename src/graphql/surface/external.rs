//! External read sources: GraphQL roots served by something other than the
//! read-model catalog (a sorted-set store, a search index, …).
//!
//! Core owns everything around such roots — name collisions, per-role and
//! per-application selection, SDL, the runtime schema, authorization
//! fingerprints, the application contract, the client manifest, protocol
//! envelopes and `@live` refresh. A source only says what its roots are
//! ([`ExternalReadSource`], part of the Surface IR so pool-free exports see
//! it) and how to run one ([`ExternalReadExecutor`], registered on the
//! engine with `GraphqlEngineBuilder::external_read_source`).
//!
//! External results carry no record or index evidence: protocol envelopes
//! mark them incomplete and, live, as snapshot mode. A live root re-executes
//! (debounced, hash-gated) whenever one of its read's change keys is
//! published on the engine's `ChangeHub`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use crate::microsvc::Session;

/// Scalars an external argument or object field may use.
const SCALARS: &[&str] = &["Int", "Float", "String", "Boolean", "ID"];

/// A typed name: an argument of an external root or a field of an external
/// object type. `type_name` is a built-in scalar.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalField {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
}

impl ExternalField {
    pub fn new(name: impl Into<String>, type_name: impl Into<String>, nullable: bool) -> Self {
        Self {
            name: name.into(),
            type_name: type_name.into(),
            nullable,
        }
    }

    /// `type_name`, with `!` unless nullable.
    pub fn graphql_type(&self) -> String {
        if self.nullable {
            self.type_name.clone()
        } else {
            format!("{}!", self.type_name)
        }
    }
}

/// An object type external roots return. Sources may share one by name if
/// their definitions are identical.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalType {
    pub name: String,
    pub fields: Vec<ExternalField>,
}

/// One external root field.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalRoot {
    pub name: String,
    pub arguments: Vec<ExternalField>,
    /// The result object type (one of the source's [`ExternalReadSource::types`]).
    pub type_name: String,
    /// `[T!]!` when true, a nullable `T` otherwise.
    pub list: bool,
    /// Also a same-named subscription root (`@live`) where the surface has
    /// subscriptions.
    pub live: bool,
}

impl ExternalRoot {
    /// The result type reference.
    pub fn graphql_type(&self) -> String {
        if self.list {
            format!("[{}!]!", self.type_name)
        } else {
            self.type_name.clone()
        }
    }
}

/// The declaration of an external read source.
pub trait ExternalReadSource: Send + Sync + fmt::Debug {
    /// Section of the application contract and client manifest this source
    /// is listed under (one per kind of source).
    fn section(&self) -> &str;
    /// Unique within its section.
    fn name(&self) -> &str;
    /// Every role this declaration grants anything to.
    fn roles(&self) -> Vec<String>;
    /// What every one of `roles` may run, granted to `grantee` (a role
    /// selection passes itself; an application its name and schema roles).
    /// `None` when nothing is common.
    fn select(&self, grantee: &str, roles: &[String]) -> Option<Arc<dyn ExternalReadSource>>;
    /// Object types the roots return.
    fn types(&self) -> Vec<ExternalType>;
    /// Roots of this declaration (the union of every grant before selection).
    fn roots(&self) -> Vec<ExternalRoot>;
    /// Session claims the reads depend on (authorization claim keys).
    fn claims(&self) -> Vec<String>;
    /// Canonical declaration: the application contract entry and
    /// authorization fingerprint material.
    fn contract(&self) -> Value;
    /// The client-manifest entry; `subscriptions` says whether live roots
    /// have subscription twins.
    fn client_manifest(&self, subscriptions: bool) -> Value;
    /// Validate the declaration itself.
    fn validate(&self) -> Result<(), String>;
}

/// Why an external read failed; mapped to `extensions.code`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExternalReadErrorKind {
    /// `BAD_REQUEST`: invalid arguments.
    BadRequest,
    /// `FORBIDDEN`: the session may not run it (e.g. a missing claim).
    Forbidden,
    /// `INTERNAL`: the backing store failed.
    Internal,
}

/// A failed external read; `message` reaches the client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalReadError {
    pub kind: ExternalReadErrorKind,
    pub message: String,
}

impl ExternalReadError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            kind: ExternalReadErrorKind::BadRequest,
            message: message.into(),
        }
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self {
            kind: ExternalReadErrorKind::Forbidden,
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            kind: ExternalReadErrorKind::Internal,
            message: message.into(),
        }
    }

    /// The engine error code.
    pub fn code(&self) -> &'static str {
        match self.kind {
            ExternalReadErrorKind::BadRequest => "BAD_REQUEST",
            ExternalReadErrorKind::Forbidden => "FORBIDDEN",
            ExternalReadErrorKind::Internal => "INTERNAL",
        }
    }
}

/// One executed external read.
#[derive(Clone, Debug, PartialEq)]
pub struct ExternalReadResult {
    /// The root's value (`null` for an absent nullable result).
    pub value: Value,
    /// What was read (bound into the protocol scope token).
    pub scope: Value,
    /// `ChangeHub` keys after which a live read re-executes.
    pub change_keys: Vec<String>,
}

/// Runs the roots of one [`ExternalReadSource`].
#[async_trait]
pub trait ExternalReadExecutor: Send + Sync {
    /// The declaration this executor serves.
    fn source(&self) -> Arc<dyn ExternalReadSource>;
    /// Run `root` for the viewer in `session`. The engine has checked that
    /// the request's role is granted `root`; `arguments` are the GraphQL
    /// arguments as JSON (absent or `null` when omitted). A live root runs
    /// this again after each matching change.
    async fn execute(
        &self,
        root: &str,
        arguments: &BTreeMap<String, Value>,
        session: &Session,
    ) -> Result<ExternalReadResult, ExternalReadError>;
}

/// Validate a set of sources against each other and the catalog's root and
/// type names.
pub(crate) fn validate_external_sources(
    sources: &[Arc<dyn ExternalReadSource>],
    existing_roots: &[&str],
    existing_types: &[&str],
) -> Result<(), String> {
    let mut names = BTreeSet::new();
    let mut roots = existing_roots
        .iter()
        .map(|root| root.to_string())
        .collect::<BTreeSet<_>>();
    let mut types: BTreeMap<String, ExternalType> = BTreeMap::new();
    for source in sources {
        source.validate()?;
        let id = format!("{} `{}`", source.section(), source.name());
        if source.section().is_empty()
            || !names.insert((source.section().to_string(), source.name().to_string()))
        {
            return Err(format!("duplicate or unnamed external read source {id}"));
        }
        for ty in source.types() {
            if !crate::graphql::naming::is_valid_graphql_name(&ty.name)
                || existing_types.contains(&ty.name.as_str())
            {
                return Err(format!(
                    "{id} type `{}` is invalid or collides with a model type",
                    ty.name
                ));
            }
            for field in &ty.fields {
                validate_field(&id, field)?;
            }
            match types.get(&ty.name) {
                Some(existing) if existing != &ty => {
                    return Err(format!("{id} redefines type `{}`", ty.name));
                }
                _ => {
                    types.insert(ty.name.clone(), ty);
                }
            }
        }
        for root in source.roots() {
            if !crate::graphql::naming::is_valid_graphql_name(&root.name)
                || !roots.insert(root.name.clone())
            {
                return Err(format!(
                    "{id} root `{}` is invalid or collides with another root",
                    root.name
                ));
            }
            if !types.contains_key(&root.type_name) {
                return Err(format!(
                    "{id} root `{}` returns undeclared type `{}`",
                    root.name, root.type_name
                ));
            }
            for argument in &root.arguments {
                validate_field(&id, argument)?;
            }
        }
    }
    Ok(())
}

fn validate_field(id: &str, field: &ExternalField) -> Result<(), String> {
    if !crate::graphql::naming::is_valid_graphql_name(&field.name)
        || !SCALARS.contains(&field.type_name.as_str())
    {
        return Err(format!(
            "{id} field `{}: {}` must be a valid name of a built-in scalar",
            field.name, field.type_name
        ));
    }
    Ok(())
}

/// Every distinct object type the selected sources' roots need, by name.
pub(crate) fn external_types(
    sources: &[Arc<dyn ExternalReadSource>],
) -> BTreeMap<String, ExternalType> {
    let mut types = BTreeMap::new();
    for source in sources {
        let declared = source.types();
        for root in source.roots() {
            if let Some(ty) = declared.iter().find(|ty| ty.name == root.type_name) {
                types.entry(ty.name.clone()).or_insert_with(|| ty.clone());
            }
        }
    }
    types
}
