use std::collections::BTreeSet;

use crate::client_compiler::manifest::ManifestCommand;
use crate::client_compiler::ClientCompileError;

pub(super) fn graphql_name(value: &str, label: &str) -> Result<(), ClientCompileError> {
    if !crate::client_compiler::is_graphql_name(value) || value.starts_with("__") {
        Err(invalid(
            "client.manifest.graphql_name",
            format!("{label} `{value}` must be a valid GraphQL name"),
        ))
    } else {
        Ok(())
    }
}

pub(super) fn unique_nonempty(values: &[String], label: &str) -> Result<(), ClientCompileError> {
    let mut seen = BTreeSet::new();
    for value in values {
        nonempty(value, label)?;
        if !seen.insert(value) {
            return Err(invalid(
                "client.manifest.duplicate_entry",
                format!("{label} entries must be unique"),
            ));
        }
    }
    Ok(())
}

pub(super) fn nonempty(value: &str, label: &str) -> Result<(), ClientCompileError> {
    if value.trim().is_empty() {
        Err(invalid(
            "client.manifest.empty",
            format!("{label} must not be empty"),
        ))
    } else {
        Ok(())
    }
}

pub(super) fn command_error(
    command: &ManifestCommand,
    code: &'static str,
    message: impl std::fmt::Display,
) -> ClientCompileError {
    invalid(
        code,
        format!("manifest command `{}` {message}", command.name),
    )
}

pub(super) fn invalid(code: &'static str, message: impl Into<String>) -> ClientCompileError {
    ClientCompileError::manifest(code, message)
}
