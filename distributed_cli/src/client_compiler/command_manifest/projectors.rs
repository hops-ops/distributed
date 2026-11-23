use std::collections::{BTreeMap, BTreeSet};

use crate::client_compiler::manifest::{ClientModel, ClientProjector};
use crate::client_compiler::ClientCompileError;

use super::support::{invalid, nonempty};

pub(super) fn validate_projector_inventory(
    projectors: &[ClientProjector],
    models: &BTreeMap<String, ClientModel>,
) -> Result<(), ClientCompileError> {
    let mut names = BTreeSet::new();
    for projector in projectors {
        if projector.version != 1 {
            return Err(invalid(
                "client.manifest.projector_version",
                format!("projector `{}` must use version 1", projector.name),
            ));
        }
        nonempty(&projector.name, "projector name")?;
        for model in &projector.models {
            if !models.contains_key(model) {
                return Err(invalid(
                    "client.manifest.projector_model",
                    format!(
                        "projector `{}` references unknown model `{model}`",
                        projector.name
                    ),
                ));
            }
        }
        if !names.insert(projector.name.as_str()) {
            return Err(invalid(
                "client.manifest.duplicate_projector",
                format!("duplicate projector `{}`", projector.name),
            ));
        }
    }
    Ok(())
}
