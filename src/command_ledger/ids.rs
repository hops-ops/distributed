use std::fmt;

use sha2::{Digest, Sha256};
use uuid::{Uuid, Variant};

use super::CommandLedgerError;

pub(crate) const SHA256_BYTES: usize = 32;
pub(super) const COMMAND_REPLAY_VERSION: u16 = 3;

/// Opaque identity for one concrete leaf repository instance.
///
/// Clones copy this value. Constructing a new leaf repository over even the
/// same underlying pool creates a different identity, so GraphQL command
/// binding must retain the repository handle that owns the causal committer.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct CausalStorageIdentity(Uuid);

impl CausalStorageIdentity {
    pub(crate) fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl fmt::Debug for CausalStorageIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CausalStorageIdentity([opaque])")
    }
}

/// Canonical client-created UUIDv7 command identity.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct CommandId(String);

impl CommandId {
    pub(crate) fn parse(value: impl AsRef<str>) -> Result<Self, CommandLedgerError> {
        let value = value.as_ref();
        let parsed = Uuid::parse_str(value).map_err(|_| {
            CommandLedgerError::Invalid(format!("command ID `{value}` must be a valid UUIDv7"))
        })?;
        if parsed.get_version_num() != 7 || parsed.get_variant() != Variant::RFC4122 {
            return Err(CommandLedgerError::Invalid(format!(
                "command ID `{value}` must be an RFC 4122 UUIDv7"
            )));
        }
        Ok(Self(parsed.hyphenated().to_string()))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// The command ID of work the host starts itself (a fired schedule, a
/// delivered transport message), derived from a stable key the caller hashed.
///
/// The ID is the first 16 bytes of the digest with the RFC 4122 version-7 and
/// variant bits set: the shape [`CommandId::parse`] requires. One key always
/// yields one ID, so the ledger replays a repeated dispatch instead of
/// running it again. Callers start the key with their own domain tag so two
/// sources cannot derive the same ID.
pub(crate) fn deterministic_command_id(key: Sha256) -> String {
    let digest = key.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes).hyphenated().to_string()
}

impl fmt::Debug for CommandId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("CommandId").field(&self.0).finish()
    }
}

/// Versioned server-derived verified-principal partition.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct PrincipalPartitionId(String);

impl PrincipalPartitionId {
    pub(crate) fn new(value: impl Into<String>) -> Result<Self, CommandLedgerError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(CommandLedgerError::Invalid(
                "principal partition must not be empty".into(),
            ));
        }
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for PrincipalPartitionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PrincipalPartitionId([redacted])")
    }
}

/// Complete non-forgeable ledger key.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct CommandLedgerKey {
    service_id: String,
    principal_partition: PrincipalPartitionId,
    pub(super) command_id: CommandId,
}

impl CommandLedgerKey {
    pub(crate) fn new(
        service_id: impl Into<String>,
        principal_partition: PrincipalPartitionId,
        command_id: CommandId,
    ) -> Result<Self, CommandLedgerError> {
        let service_id = service_id.into();
        if service_id.trim().is_empty() {
            return Err(CommandLedgerError::Invalid(
                "command ledger service ID must not be empty".into(),
            ));
        }
        Ok(Self {
            service_id,
            principal_partition,
            command_id,
        })
    }

    pub(crate) fn service_id(&self) -> &str {
        &self.service_id
    }

    pub(crate) fn principal_partition(&self) -> &str {
        self.principal_partition.as_str()
    }

    pub(crate) fn command_id(&self) -> &str {
        self.command_id.as_str()
    }
}

impl fmt::Debug for CommandLedgerKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommandLedgerKey")
            .field("service_id", &self.service_id)
            .field("principal_partition", &"[redacted]")
            .field("command_id", &self.command_id)
            .finish()
    }
}

macro_rules! fixed_hash {
    ($name:ident, $description:literal) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub(crate) struct $name([u8; SHA256_BYTES]);

        impl $name {
            pub(crate) fn new(bytes: [u8; SHA256_BYTES]) -> Self {
                Self(bytes)
            }

            pub(crate) fn try_from_slice(bytes: &[u8]) -> Result<Self, CommandLedgerError> {
                let bytes: [u8; SHA256_BYTES] = bytes.try_into().map_err(|_| {
                    CommandLedgerError::Invalid(format!(
                        "{} must contain exactly {SHA256_BYTES} bytes",
                        $description
                    ))
                })?;
                Ok(Self(bytes))
            }

            /// Parse the canonical wire spelling emitted by command-input and
            /// command-contract code (`sha256:` followed by 64 lowercase hex
            /// digits). Keeping this checked seam here prevents dispatch code
            /// from hand-decoding identity material differently at each call
            /// site.
            #[cfg(test)]
            pub(crate) fn parse_sha256(value: &str) -> Result<Self, CommandLedgerError> {
                parse_sha256(value, $description).map(Self)
            }

            pub(crate) fn as_bytes(&self) -> &[u8; SHA256_BYTES] {
                &self.0
            }
        }
    };
}

fixed_hash!(CanonicalInputHash, "canonical command input hash");
fixed_hash!(CommandContractFingerprint, "command contract fingerprint");

#[cfg(test)]
fn parse_sha256(
    value: &str,
    description: &'static str,
) -> Result<[u8; SHA256_BYTES], CommandLedgerError> {
    let encoded = value.strip_prefix("sha256:").ok_or_else(|| {
        CommandLedgerError::Invalid(format!(
            "{description} must use the canonical `sha256:<lowercase-hex>` format"
        ))
    })?;
    if encoded.len() != SHA256_BYTES * 2 {
        return Err(CommandLedgerError::Invalid(format!(
            "{description} must contain exactly {} hexadecimal digits",
            SHA256_BYTES * 2
        )));
    }

    fn nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            _ => None,
        }
    }

    let encoded = encoded.as_bytes();
    let mut digest = [0; SHA256_BYTES];
    for (index, target) in digest.iter_mut().enumerate() {
        let high = nibble(encoded[index * 2]);
        let low = nibble(encoded[index * 2 + 1]);
        let (Some(high), Some(low)) = (high, low) else {
            return Err(CommandLedgerError::Invalid(format!(
                "{description} must contain only lowercase hexadecimal digits"
            )));
        };
        *target = (high << 4) | low;
    }
    Ok(digest)
}

/// Stable causation allocated exactly once when a ledger identity is inserted.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct CausationId(String);

impl CausationId {
    pub(crate) fn new() -> Self {
        Self(Uuid::now_v7().hyphenated().to_string())
    }

    pub(crate) fn parse_stored(value: String) -> Result<Self, CommandLedgerError> {
        parse_stored_uuid_v7("causation ID", value).map(Self)
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for CausationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("CausationId").field(&self.0).finish()
    }
}

/// Generation fence for one speculative handler attempt.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct AttemptToken(pub(super) String);

impl AttemptToken {
    pub(super) fn new() -> Self {
        Self(Uuid::now_v7().hyphenated().to_string())
    }

    pub(crate) fn parse_stored(value: String) -> Result<Self, CommandLedgerError> {
        parse_stored_uuid_v7("attempt token", value).map(Self)
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for AttemptToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AttemptToken([redacted])")
    }
}

fn parse_stored_uuid_v7(label: &str, value: String) -> Result<String, CommandLedgerError> {
    let parsed = Uuid::parse_str(&value)
        .map_err(|_| CommandLedgerError::Corrupt(format!("stored {label} is not a UUID")))?;
    if parsed.get_version_num() != 7 || parsed.get_variant() != Variant::RFC4122 {
        return Err(CommandLedgerError::Corrupt(format!(
            "stored {label} is not an RFC 4122 UUIDv7"
        )));
    }
    Ok(parsed.hyphenated().to_string())
}
