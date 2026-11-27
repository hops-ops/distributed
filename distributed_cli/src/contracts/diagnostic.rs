use std::fmt;

/// Stable classifications for catalog and client-inventory validation errors.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ContractDiagnosticCode {
    CatalogInvalid,
    CatalogInputLimit,
    CatalogPath,
    CatalogSymlinkEscape,
    CatalogSpecialFile,
    CatalogDuplicateScope,
    CatalogDuplicateOwner,
    CatalogDuplicateOutput,
    CatalogUnboundedGlob,
    EnvironmentValue,
    ChainMissingPredecessor,
    ChainKindMismatch,
    ChainIdentityMismatch,
    ChainCycle,
}

impl ContractDiagnosticCode {
    /// The stable code string used in error messages.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CatalogInvalid => "CTL-CATALOG-INVALID",
            Self::CatalogInputLimit => "CTL-CATALOG-LIMIT",
            Self::CatalogPath => "CTL-CATALOG-PATH",
            Self::CatalogSymlinkEscape => "CTL-CATALOG-SYMLINK",
            Self::CatalogSpecialFile => "CTL-CATALOG-FILE",
            Self::CatalogDuplicateScope => "CTL-CATALOG-DUPLICATE-SCOPE",
            Self::CatalogDuplicateOwner => "CTL-CATALOG-DUPLICATE-OWNER",
            Self::CatalogDuplicateOutput => "CTL-CATALOG-DUPLICATE-OUTPUT",
            Self::CatalogUnboundedGlob => "CTL-CATALOG-GLOB",
            Self::EnvironmentValue => "CTL-ENV-VALUE",
            Self::ChainMissingPredecessor => "CTL-CHAIN-MISSING",
            Self::ChainKindMismatch => "CTL-CHAIN-KIND",
            Self::ChainIdentityMismatch => "CTL-CHAIN-IDENTITY",
            Self::ChainCycle => "CTL-CHAIN-CYCLE",
        }
    }
}

impl fmt::Display for ContractDiagnosticCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

pub(crate) fn is_secret_like(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    lower.contains("postgres://")
        || lower.contains("postgresql://")
        || lower.contains("mysql://")
        || lower.contains("mongodb://")
        || lower.contains("bearer ")
        || lower.contains("password=")
        || lower.contains("token=")
        || lower.contains("secret=")
        || lower.contains("-----begin ")
}
