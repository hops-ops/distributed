//! Ticket extension claims: game-declared facts the ticket issuer vouches
//! for (match id, team, party, mode), carried next to the fixed
//! [`TicketClaims`](crate::TicketClaims) fields.
//!
//! The issuer (the web backend, a matchmaker) signs them under the ticket's
//! `ext` object; the zone reads them from
//! [`TicketClaims::extensions`](crate::TicketClaims::extensions) in
//! [`Zone::on_auth`](crate::Zone::on_auth) and from
//! [`PlayerInfo::extensions`](crate::PlayerInfo::extensions) afterwards, typed
//! with [`TicketExtensions::typed`]. Handoff transfer tickets carry them to
//! the next zone unchanged.
//!
//! A game declares the schema once with `#[derive(TicketExtension)]`:
//!
//! ```rust,ignore
//! #[derive(Serialize, Deserialize, TicketExtension)]
//! pub struct MatchClaims { pub match_id: String, pub team: u8 }
//! // net: ServerConfig { ticket_extensions: Some(ExtensionSchema::of::<MatchClaims>()), .. }
//! ```
//!
//! The derive refuses identity-, reward- and economy-looking names (the
//! same deny list as `FactCommand`): identity is only ever the verified
//! `sub`, and amounts are decided by command handlers. The `net` server
//! refuses a ticket whose extensions do not match the declared schema
//! (unknown names, wrong types), and any ticket with extensions when no
//! schema is declared.

use std::collections::BTreeMap;
use std::fmt;

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{Map, Value};

/// Largest accepted extension object, as compact JSON (bytes).
pub const MAX_EXTENSION_BYTES: usize = 2048;

/// A game-declared extension claims type (`#[derive(TicketExtension)]`).
pub trait TicketExtension: Serialize + DeserializeOwned {
    /// Claim names as serialized (serde renames applied).
    const CLAIMS: &'static [&'static str];
}

/// Why extension claims were refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtensionError {
    /// `ext` is not a JSON object.
    NotAnObject,
    /// Larger than [`MAX_EXTENSION_BYTES`].
    TooLarge,
    /// A name the declared schema does not know.
    Unknown(String),
    /// Extensions present but the server declares no schema.
    Undeclared,
    /// The values do not decode as the declared type.
    Invalid(String),
}

impl fmt::Display for ExtensionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAnObject => f.write_str("extension claims must be an object"),
            Self::TooLarge => write!(f, "extension claims exceed {MAX_EXTENSION_BYTES} bytes"),
            Self::Unknown(name) => write!(f, "undeclared extension claim `{name}`"),
            Self::Undeclared => f.write_str("this server declares no ticket extension claims"),
            Self::Invalid(why) => write!(f, "invalid extension claims: {why}"),
        }
    }
}

impl std::error::Error for ExtensionError {}

/// Verified extension claims (name → JSON value), empty for a ticket
/// without any.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TicketExtensions(BTreeMap<String, Value>);

impl TicketExtensions {
    /// No extensions.
    pub fn new() -> Self {
        Self::default()
    }

    /// From a typed declaration.
    pub fn from_typed<E: TicketExtension>(claims: &E) -> Result<Self, ExtensionError> {
        let value =
            serde_json::to_value(claims).map_err(|e| ExtensionError::Invalid(e.to_string()))?;
        Self::from_json(value)
    }

    /// From a JSON object (the ticket's `ext`), size-checked.
    pub fn from_json(value: Value) -> Result<Self, ExtensionError> {
        let Value::Object(map) = value else {
            return Err(ExtensionError::NotAnObject);
        };
        let extensions = Self(map.into_iter().collect());
        if extensions.encoded_len() > MAX_EXTENSION_BYTES {
            return Err(ExtensionError::TooLarge);
        }
        Ok(extensions)
    }

    /// As a JSON object (what the ticket signs under `ext`).
    pub fn to_json(&self) -> Value {
        Value::Object(self.0.clone().into_iter().collect::<Map<_, _>>())
    }

    fn encoded_len(&self) -> usize {
        self.to_json().to_string().len()
    }

    /// True when the ticket carried none.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// One raw claim.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.0.get(name)
    }

    /// All claims, by name.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Decode as the declared type. Names `E` does not declare are refused.
    pub fn typed<E: TicketExtension>(&self) -> Result<E, ExtensionError> {
        if let Some(unknown) = self
            .0
            .keys()
            .find(|name| !E::CLAIMS.contains(&name.as_str()))
        {
            return Err(ExtensionError::Unknown(unknown.clone()));
        }
        serde_json::from_value(self.to_json()).map_err(|e| ExtensionError::Invalid(e.to_string()))
    }
}

/// The extension schema a server admits ([`ExtensionSchema::of`]).
#[derive(Clone, Copy)]
pub struct ExtensionSchema {
    claims: &'static [&'static str],
    check: fn(&TicketExtensions) -> Result<(), ExtensionError>,
}

impl fmt::Debug for ExtensionSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExtensionSchema")
            .field("claims", &self.claims)
            .finish()
    }
}

impl ExtensionSchema {
    /// Admit extensions that decode as `E` (missing optional fields are
    /// fine; unknown names are not).
    pub fn of<E: TicketExtension>() -> Self {
        Self {
            claims: E::CLAIMS,
            check: |ext| ext.typed::<E>().map(drop),
        }
    }

    /// Declared claim names.
    pub fn claims(&self) -> &'static [&'static str] {
        self.claims
    }

    /// Check verified extensions against the schema.
    pub fn validate(&self, extensions: &TicketExtensions) -> Result<(), ExtensionError> {
        if extensions.is_empty() {
            return Ok(());
        }
        (self.check)(extensions)
    }
}

/// Check extensions against an optional server schema: none declared means
/// none admitted.
pub fn validate_extensions(
    schema: Option<&ExtensionSchema>,
    extensions: &TicketExtensions,
) -> Result<(), ExtensionError> {
    match schema {
        Some(schema) => schema.validate(extensions),
        None if extensions.is_empty() => Ok(()),
        None => Err(ExtensionError::Undeclared),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use serde_json::json;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct MatchClaims {
        match_id: String,
        #[serde(default)]
        team: Option<u8>,
    }

    impl TicketExtension for MatchClaims {
        const CLAIMS: &'static [&'static str] = &["match_id", "team"];
    }

    #[test]
    fn typed_round_trip_and_schema_checks() {
        let claims = MatchClaims {
            match_id: "m-1".into(),
            team: Some(2),
        };
        let ext = TicketExtensions::from_typed(&claims).unwrap();
        assert_eq!(ext.get("team"), Some(&json!(2)));
        assert_eq!(ext.typed::<MatchClaims>().unwrap(), claims);
        let schema = ExtensionSchema::of::<MatchClaims>();
        schema.validate(&ext).unwrap();
        schema.validate(&TicketExtensions::new()).unwrap();

        let unknown = TicketExtensions::from_json(json!({"match_id": "m", "gold": 9})).unwrap();
        assert_eq!(
            schema.validate(&unknown),
            Err(ExtensionError::Unknown("gold".into()))
        );
        let mistyped = TicketExtensions::from_json(json!({"match_id": 7})).unwrap();
        assert!(matches!(
            schema.validate(&mistyped),
            Err(ExtensionError::Invalid(_))
        ));
        assert_eq!(
            TicketExtensions::from_json(json!(["x"])),
            Err(ExtensionError::NotAnObject)
        );
        let big = "x".repeat(MAX_EXTENSION_BYTES);
        assert_eq!(
            TicketExtensions::from_json(json!({ "match_id": big })),
            Err(ExtensionError::TooLarge)
        );
        assert_eq!(
            validate_extensions(None, &ext),
            Err(ExtensionError::Undeclared)
        );
        validate_extensions(None, &TicketExtensions::new()).unwrap();
    }
}
