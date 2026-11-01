mod artifact;
mod commands;
mod common;
mod leaderboards;
mod operation;
mod project;
mod typescript;

pub(crate) use project::render_project;

#[cfg(test)]
pub(crate) fn render_operation_artifact_json(
    operation: &super::graphql::CompiledOperation,
    manifest: &super::manifest::ClientManifest,
) -> Result<String, super::ClientCompileError> {
    artifact::render_operation_artifact_json(operation, manifest)
}

#[cfg(test)]
pub(crate) fn render_operation_module(
    operation: &super::graphql::CompiledOperation,
    manifest: &super::manifest::ClientManifest,
) -> Result<String, super::ClientCompileError> {
    operation::render_operation_module(operation, manifest)
}
