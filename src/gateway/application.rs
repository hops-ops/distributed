use super::{BindingKind, Gateway};
use crate::application::{ApplicationExtension, ApplicationResult};

impl Gateway {
    /// Logical capability declaration for Application/Service composition.
    /// Changing physical origins, paths or embedded/remote placement does not
    /// change the portable application identity.
    pub fn application_extension(
        &self,
        id: impl Into<String>,
    ) -> ApplicationResult<ApplicationExtension> {
        let bindings = self.bindings().map(|binding| {
            let capability = match &binding.kind {
                BindingKind::Handler => serde_json::json!({"kind":"handler"}),
                BindingKind::Admission => serde_json::json!({"kind":"admission"}),
                BindingKind::Assets => serde_json::json!({"kind":"assets"}),
                BindingKind::UiProxy { .. } => serde_json::json!({"kind":"ui"}),
                BindingKind::Graphql { capabilities, delivery, schema_extensions, .. } => serde_json::json!({"kind":"graphql","operations":capabilities,"delivery":delivery,"schemaExtensions":schema_extensions}),
            };
            serde_json::json!({"id":binding.id,"capability":capability})
        }).collect::<Vec<_>>();
        ApplicationExtension::try_new(
            id,
            1,
            serde_json::json!({"kind":"application_gateway","bindings":bindings}),
        )
    }
}
