//! Acceptance checks: every module, model, projection and surface, and the
//! bounds on portable text and JSON.

use super::*;

impl ApplicationManifest {
    pub fn validate(&self) -> ApplicationResult<()> {
        self.validate_inner(true, false)
    }

    pub(super) fn validate_inner(
        &self,
        require_nested_fingerprints: bool,
        require_manifest_fingerprints: bool,
    ) -> ApplicationResult<()> {
        if self.schema_version != APPLICATION_MANIFEST_SCHEMA_VERSION {
            return Err(ApplicationError::UnsupportedVersion {
                expected: APPLICATION_MANIFEST_SCHEMA_VERSION,
                actual: self.schema_version,
            });
        }
        LogicalId::try_new("application", self.name.clone())?;
        self.validate_inventory_expansion()?;
        validate_collection_len("modules", self.modules.len())?;
        validate_collection_len("commands", self.commands.len())?;
        validate_collection_len("events", self.events.len())?;
        validate_collection_len("projections", self.projections.len())?;
        validate_collection_len("models", self.models.len())?;
        validate_collection_len("surfaces", self.surfaces.len())?;
        validate_collection_len("extensions", self.extensions.len())?;
        validate_unique_ids(
            "module",
            self.modules.iter().map(|module| module.id.clone()),
        )?;
        validate_unique_ids(
            "command",
            self.commands.iter().map(|command| command.id.clone()),
        )?;
        validate_unique_ids("event", self.events.iter().map(|event| event.name.clone()))?;
        validate_unique_ids(
            "projection",
            self.projections
                .iter()
                .map(|projection| projection.id.clone()),
        )?;
        validate_unique_ids("model", self.models.iter().map(|model| model.id.clone()))?;
        validate_unique_ids(
            "surface",
            self.surfaces.iter().map(|surface| surface.id.clone()),
        )?;

        let model_ids = self
            .models
            .iter()
            .map(|model| model.id.as_str())
            .collect::<HashSet<_>>();
        let projection_ids = self
            .projections
            .iter()
            .map(|projection| projection.id.as_str())
            .collect::<HashSet<_>>();
        for module in &self.modules {
            validate_module(module, require_nested_fingerprints)?;
        }
        for command in &self.commands {
            command.validate()?;
            validate_fingerprint(
                "command",
                &command.id,
                &command.fingerprint,
                command,
                require_nested_fingerprints,
            )?;
            if let Some(model) = &command.projected_model {
                require_reference("model", model, &model_ids)?;
            }
        }
        for event in &self.events {
            validate_event(event)?;
        }
        for projection in &self.projections {
            validate_projection(
                projection,
                &model_ids,
                &projection_ids,
                require_nested_fingerprints,
            )?;
        }
        for model in &self.models {
            validate_model(model, &model_ids, require_nested_fingerprints)?;
        }
        for surface in &self.surfaces {
            validate_surface(
                surface,
                &model_ids,
                &projection_ids,
                require_nested_fingerprints,
            )?;
        }
        let mut extension_ids = BTreeSet::new();
        for extension in &self.extensions {
            if !extension_ids.insert(extension.id.clone()) {
                return Err(ApplicationError::Duplicate {
                    kind: "application extension",
                    identity: extension.id.clone(),
                });
            }
            LogicalId::try_new("application extension", extension.id.clone())?;
            if extension.version == 0 {
                return Err(ApplicationError::InvalidSpec(
                    "application extension version must be non-zero".into(),
                ));
            }
            validate_json_contract("application extension", &extension.value)?;
        }
        let compatibility = self.framework_compatibility()?;
        if compatibility != FrameworkCompatibility::current() {
            return Err(ApplicationError::InvalidSpec(format!(
                "application manifest framework release `{}` does not match compiling framework `{}`",
                compatibility.release,
                env!("CARGO_PKG_VERSION")
            )));
        }
        validate_portable_text("manifest generator", &self.provenance.generator)?;
        if let Some(revision) = &self.provenance.source_revision {
            validate_artifact_text("source revision", revision)?;
        }
        for source in &self.provenance.sources {
            validate_artifact_text("manifest source", source)?;
        }

        let expected = expected_fingerprints(self)?;
        if require_manifest_fingerprints
            && (self.fingerprints.logical.is_empty() || self.fingerprints.canonical.is_empty())
        {
            return Err(ApplicationError::NonCanonical(
                "application manifest fingerprint material",
            ));
        }
        if (!self.fingerprints.logical.is_empty() && self.fingerprints.logical != expected.logical)
            || (!self.fingerprints.canonical.is_empty()
                && self.fingerprints.canonical != expected.canonical)
        {
            return Err(ApplicationError::NonCanonical(
                "application manifest fingerprint material",
            ));
        }
        validate_manifest_ownership(self)?;
        Ok(())
    }
}

pub(super) fn validate_module(
    module: &ModuleManifest,
    require_nested_fingerprints: bool,
) -> ApplicationResult<()> {
    LogicalId::try_new("module", module.id.clone())?;
    validate_collection_len("module commands", module.commands.len())?;
    validate_collection_len("module events", module.events.len())?;
    validate_collection_len("module projections", module.projections.len())?;
    validate_collection_len("module models", module.models.len())?;
    validate_collection_len("module surfaces", module.surfaces.len())?;
    validate_fingerprint(
        "module",
        &module.id,
        &module.fingerprint,
        module,
        require_nested_fingerprints,
    )?;
    validate_unique_ids(
        "module command",
        module.commands.iter().map(|item| item.id.clone()),
    )?;
    validate_unique_ids(
        "module projection",
        module.projections.iter().map(|item| item.id.clone()),
    )?;
    validate_unique_ids(
        "module event",
        module.events.iter().map(|item| item.name.clone()),
    )?;
    validate_unique_ids(
        "module model",
        module.models.iter().map(|item| item.id.clone()),
    )?;
    validate_unique_ids(
        "module surface",
        module.surfaces.iter().map(|item| item.id.clone()),
    )?;
    validate_sorted_unique(
        "module commands",
        &module
            .commands
            .iter()
            .map(|item| item.id.clone())
            .collect::<Vec<_>>(),
    )?;
    validate_sorted_unique(
        "module events",
        &module
            .events
            .iter()
            .map(|item| item.name.clone())
            .collect::<Vec<_>>(),
    )?;
    validate_sorted_unique(
        "module projections",
        &module
            .projections
            .iter()
            .map(|item| item.id.clone())
            .collect::<Vec<_>>(),
    )?;
    validate_sorted_unique(
        "module models",
        &module
            .models
            .iter()
            .map(|item| item.id.clone())
            .collect::<Vec<_>>(),
    )?;
    validate_sorted_unique(
        "module surfaces",
        &module
            .surfaces
            .iter()
            .map(|item| item.id.clone())
            .collect::<Vec<_>>(),
    )?;
    let module_model_ids = module
        .models
        .iter()
        .map(|model| model.id.as_str())
        .chain(module.surfaces.iter().flat_map(|surface| {
            surface
                .contract
                .models
                .iter()
                .map(|model| model.id.as_str())
        }))
        .collect::<HashSet<_>>();
    let module_projection_ids = module
        .projections
        .iter()
        .map(|projection| projection.id.as_str())
        .chain(module.surfaces.iter().flat_map(|surface| {
            surface
                .contract
                .projections
                .iter()
                .map(|projection| projection.id.as_str())
        }))
        .collect::<HashSet<_>>();
    for command in &module.commands {
        command.validate()?;
        validate_fingerprint(
            "command",
            &command.id,
            &command.fingerprint,
            command,
            require_nested_fingerprints,
        )?;
    }
    let emitted_events = dedup_events(
        module
            .commands
            .iter()
            .flat_map(|command| command.emits.iter().cloned())
            .collect(),
    )?;
    if emitted_events != module.events {
        return Err(ApplicationError::Collision {
            kind: "event",
            identity: module.id.clone(),
            reason: "module event inventory is not closed over command declarations".into(),
        });
    }
    for event in &module.events {
        validate_event(event)?;
    }
    for projection in &module.projections {
        validate_projection(
            projection,
            &module_model_ids,
            &module_projection_ids,
            require_nested_fingerprints,
        )?;
    }
    for surface in &module.surfaces {
        validate_surface(
            surface,
            &module_model_ids,
            &module_projection_ids,
            require_nested_fingerprints,
        )?;
    }
    for model in &module.models {
        validate_model(model, &module_model_ids, require_nested_fingerprints)?;
    }
    Ok(())
}

pub(super) fn validate_event(
    event: &crate::application::command::EventSpec,
) -> ApplicationResult<()> {
    LogicalId::try_new("event", event.name.clone())?;
    if event.version == 0 || event.body_version == 0 || event.body_codec_version == 0 {
        return Err(ApplicationError::InvalidSpec(format!(
            "event `{}` versions must be non-zero",
            event.name
        )));
    }
    validate_portable_text("event body type", &event.body_type)?;
    validate_portable_text("event body schema", &event.body_schema)?;
    validate_sha256_text("event body fingerprint", &event.body_fingerprint)?;
    validate_portable_text("event body codec", &event.body_codec)?;
    Ok(())
}

pub(super) fn validate_projection(
    projection: &ProjectionSpec,
    model_ids: &HashSet<&str>,
    projection_ids: &HashSet<&str>,
    require_nested_fingerprints: bool,
) -> ApplicationResult<()> {
    LogicalId::try_new("projection", projection.id.clone())?;
    validate_fingerprint(
        "projection",
        &projection.id,
        &projection.fingerprint,
        projection,
        require_nested_fingerprints,
    )?;
    validate_collection_len("projection facts", projection.facts.len())?;
    validate_collection_len("projection models", projection.models.len())?;
    validate_collection_len("projection dependencies", projection.dependencies.len())?;
    validate_collection_len("modeled projections", projection.modeled.len())?;
    validate_sorted_unique("projection facts", &projection.facts)?;
    validate_sorted_unique("projection models", &projection.models)?;
    validate_sorted_unique("projection dependencies", &projection.dependencies)?;
    validate_sorted_unique(
        "modeled projection program IDs",
        &projection.modeled_programs,
    )?;
    for fact in &projection.facts {
        validate_portable_text("projection fact", fact)?;
    }
    for model in &projection.models {
        validate_portable_text("projection model", model)?;
        require_reference("model", model, model_ids)?;
    }
    validate_json_contract("projection partition", &projection.partition)?;
    let mut modeled_ids = Vec::with_capacity(projection.modeled.len());
    for modeled in &projection.modeled {
        validate_json_contract("modeled projection", modeled)?;
        let fields = modeled.as_object().ok_or_else(|| {
            ApplicationError::InvalidSpec("modeled projection must be an object".into())
        })?;
        if let Some(program) = fields.get("program") {
            // Validate each shared value independently without retaining the
            // expanded copies in the manifest or increasing the wire budget.
            crate::application::expand_projection_program_contract(program)?;
        }
        let program_id = fields
            .get("program_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                ApplicationError::InvalidSpec(
                    "modeled projection must retain its program identity".into(),
                )
            })?;
        validate_portable_text("modeled projection program ID", program_id)?;
        modeled_ids.push(program_id.to_owned());
        let output_models = fields
            .get("output_models")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| {
                ApplicationError::InvalidSpec(
                    "modeled projection must retain its output model inventory".into(),
                )
            })?;
        for output_model in output_models {
            let output_model = output_model.as_str().ok_or_else(|| {
                ApplicationError::InvalidSpec(
                    "modeled projection output model identities must be strings".into(),
                )
            })?;
            require_reference("model", output_model, model_ids)?;
        }
    }
    modeled_ids.sort();
    if modeled_ids != projection.modeled_programs {
        return Err(ApplicationError::Collision {
            kind: "projection",
            identity: projection.id.clone(),
            reason: "modeled projection program identities are stale or incomplete".into(),
        });
    }
    if let Some(catalog) = &projection.catalog_fingerprint {
        validate_sha256_text("projection catalog fingerprint", catalog)?;
    }
    for dependency in &projection.dependencies {
        if let Some(identity) = dependency.strip_prefix("projection:") {
            LogicalId::try_new("projection", identity.to_owned())?;
            require_reference("projection", identity, projection_ids)?;
        } else {
            validate_portable_text("projection dependency", dependency)?;
        }
    }
    Ok(())
}

pub(super) fn validate_model(
    model: &ModelSpec,
    model_ids: &HashSet<&str>,
    require_nested_fingerprints: bool,
) -> ApplicationResult<()> {
    LogicalId::try_new("model", model.id.clone())?;
    validate_fingerprint(
        "model",
        &model.id,
        &model.fingerprint,
        model,
        require_nested_fingerprints,
    )?;
    validate_portable_text("model table", &model.table)?;
    validate_portable_text("model object", &model.object)?;
    validate_collection_len("model fields", model.fields.len())?;
    validate_collection_len("model relationships", model.relationships.len())?;
    if model.role_limit.is_some_and(|limit| limit == 0) {
        return Err(ApplicationError::InvalidSpec(format!(
            "model `{}` has a zero role limit",
            model.id
        )));
    }
    validate_row_policy(&model.row_policy)?;
    let field_names = model
        .fields
        .iter()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    validate_sorted_unique("model fields", &field_names)?;
    let relationship_names = model
        .relationships
        .iter()
        .map(|relationship| relationship.name.clone())
        .collect::<Vec<_>>();
    validate_sorted_unique("model relationships", &relationship_names)?;
    if model.primary_key.is_empty() {
        return Err(ApplicationError::InvalidSpec(format!(
            "model `{}` must declare a primary key",
            model.id
        )));
    }
    validate_sorted_unique("model primary key", &model.primary_key)?;
    let field_names = field_names
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    for field in &model.fields {
        validate_portable_text("model field", &field.name)?;
        validate_portable_text("model field scalar", &field.scalar)?;
    }
    for key in &model.primary_key {
        validate_portable_text("model primary key field", key)?;
        if !field_names.contains(key.as_str()) {
            return Err(ApplicationError::Missing {
                kind: "model field",
                identity: key.clone(),
            });
        }
    }
    for relationship in &model.relationships {
        validate_portable_text("relationship name", &relationship.name)?;
        validate_portable_text("relationship target model", &relationship.target_model)?;
        validate_portable_text("relationship target object", &relationship.target_object)?;
        require_reference("model", &relationship.target_model, model_ids)?;
        validate_collection_len("relationship arguments", relationship.arguments.len())?;
        let argument_names = relationship
            .arguments
            .iter()
            .map(|argument| argument.name.clone())
            .collect::<Vec<_>>();
        validate_unique("relationship arguments", &argument_names)?;
        for argument in &relationship.arguments {
            validate_surface_argument(argument)?;
        }
        for dependency in &relationship.dependencies {
            validate_portable_text("relationship dependency", dependency)?;
        }
        validate_sorted_unique("relationship dependencies", &relationship.dependencies)?;
        validate_json_contract("relationship keys", &relationship.keys)?;
        validate_relationship_keys(&relationship.keys)?;
        if let Some(aggregate) = &relationship.aggregate {
            validate_portable_text("relationship aggregate", &aggregate.name)?;
            validate_portable_text("relationship aggregate type", &aggregate.type_name)?;
            validate_collection_len(
                "relationship aggregate arguments",
                aggregate.arguments.len(),
            )?;
            let argument_names = aggregate
                .arguments
                .iter()
                .map(|argument| argument.name.clone())
                .collect::<Vec<_>>();
            validate_unique("relationship aggregate arguments", &argument_names)?;
            for argument in &aggregate.arguments {
                validate_surface_argument(argument)?;
            }
            for dependency in &aggregate.dependencies {
                validate_portable_text("relationship aggregate dependency", dependency)?;
            }
            validate_sorted_unique(
                "relationship aggregate dependencies",
                &aggregate.dependencies,
            )?;
        }
    }
    Ok(())
}

pub(super) fn validate_surface(
    surface: &SurfaceSpec,
    model_ids: &HashSet<&str>,
    projection_ids: &HashSet<&str>,
    require_nested_fingerprints: bool,
) -> ApplicationResult<()> {
    LogicalId::try_new("surface", surface.id.clone())?;
    validate_fingerprint(
        "surface",
        &surface.id,
        &surface.fingerprint,
        surface,
        require_nested_fingerprints,
    )?;
    let contract = &surface.contract;
    validate_collection_len("surface models", contract.models.len())?;
    validate_collection_len("surface roots", contract.roots.len())?;
    validate_collection_len("surface commands", contract.commands.len())?;
    validate_collection_len("surface projections", contract.projections.len())?;
    validate_sorted_unique(
        "surface models",
        &contract
            .models
            .iter()
            .map(|model| model.id.clone())
            .collect::<Vec<_>>(),
    )?;
    let root_ids = contract
        .roots
        .iter()
        .map(|root| format!("{}:{}", root.operation, root.name))
        .collect::<Vec<_>>();
    validate_sorted_unique("surface roots", &root_ids)?;
    validate_sorted_unique(
        "surface commands",
        &contract
            .commands
            .iter()
            .map(|command| command.id.clone())
            .collect::<Vec<_>>(),
    )?;
    validate_sorted_unique(
        "surface projections",
        &contract
            .projections
            .iter()
            .map(|projection| projection.id.clone())
            .collect::<Vec<_>>(),
    )?;
    validate_surface_selection(surface)?;
    if contract.max_limit == 0 || contract.default_limit > contract.max_limit {
        return Err(ApplicationError::InvalidSpec(format!(
            "surface `{}` has invalid pagination bounds",
            surface.id
        )));
    }
    let surface_models = contract
        .models
        .iter()
        .map(|model| model.id.as_str())
        .collect::<HashSet<_>>();
    let model_scope = if model_ids.is_empty() {
        &surface_models
    } else {
        model_ids
    };
    for model in &contract.models {
        validate_model(model, model_scope, require_nested_fingerprints)?;
    }
    for root in &contract.roots {
        if !matches!(root.operation.as_str(), "query" | "subscription") {
            return Err(ApplicationError::InvalidSpec(format!(
                "surface root `{}` has unsupported operation `{}`",
                root.name, root.operation
            )));
        }
        validate_portable_text("surface root operation", &root.operation)?;
        validate_portable_text("surface root name", &root.name)?;
        validate_portable_text("surface root object", &root.object)?;
        if !matches!(root.kind.as_str(), "list" | "by_pk" | "aggregate") {
            return Err(ApplicationError::InvalidSpec(format!(
                "surface root `{}` has unknown kind `{}`",
                root.name, root.kind
            )));
        }
        LogicalId::try_new("surface root model", root.model.clone())?;
        require_reference("model", &root.model, model_scope)?;
        validate_collection_len("surface root arguments", root.arguments.len())?;
        let argument_names = root
            .arguments
            .iter()
            .map(|argument| argument.name.clone())
            .collect::<Vec<_>>();
        validate_unique("surface root arguments", &argument_names)?;
        for argument in &root.arguments {
            validate_surface_argument(argument)?;
        }
        for dependency in &root.dependencies {
            validate_portable_text("surface root dependency", dependency)?;
        }
        if let Some(max) = root.max_limit {
            if max == 0 || root.default_limit.is_some_and(|default| default > max) {
                return Err(ApplicationError::InvalidSpec(format!(
                    "surface root `{}` has invalid pagination bounds",
                    root.name
                )));
            }
        }
    }
    for command in &contract.commands {
        LogicalId::try_new("command", command.id.clone())?;
        validate_portable_text("surface command field", &command.field_name)?;
        validate_roles("surface command role", &command.roles)?;
        if let Some(input) = &command.input {
            validate_command_type_spec(input)?;
        }
        if let Some(output) = &command.output {
            validate_command_type_spec(output)?;
        }
        validate_json_contract("surface command defaults", &command.defaults)?;
        validate_json_contract(
            "surface command projection contract",
            &command.projection_contract,
        )?;
        if let Some(model) = &command.projected_model {
            require_reference("model", model, model_scope)?;
        }
        if let Some(direct_projection) = &command.direct_projection {
            validate_json_contract("surface command direct projection", direct_projection)?;
        }
    }
    for projection in &contract.projections {
        validate_projection(
            projection,
            model_scope,
            projection_ids,
            require_nested_fingerprints,
        )?;
    }
    for source in contract.external.values().flatten() {
        validate_json_contract("surface external read source", source)?;
    }
    Ok(())
}

pub(super) fn validate_surface_selection(surface: &SurfaceSpec) -> ApplicationResult<()> {
    let selection = &surface.contract.selection;
    match selection {
        SurfaceSelectionSpec::Catalog => {}
        SurfaceSelectionSpec::Role { name } => {
            validate_roles("surface role", std::slice::from_ref(name))?;
        }
        SurfaceSelectionSpec::Application {
            name,
            eligible_roles,
            schema_roles,
        } => {
            LogicalId::try_new("surface application", name.clone())?;
            if eligible_roles.is_empty() {
                return Err(ApplicationError::InvalidSpec(format!(
                    "application surface `{}` must expose at least one eligible role",
                    surface.id
                )));
            }
            if schema_roles.is_empty() {
                return Err(ApplicationError::InvalidSpec(format!(
                    "application surface `{}` must expose at least one schema role",
                    surface.id
                )));
            }
            if schema_roles
                .iter()
                .any(|role| !eligible_roles.iter().any(|eligible| eligible == role))
            {
                return Err(ApplicationError::InvalidSpec(format!(
                    "application surface `{}` schema roles must be a subset of eligible roles",
                    surface.id
                )));
            }
            validate_roles("surface eligible role", eligible_roles)?;
            validate_roles("surface schema role", schema_roles)?;
        }
    }
    Ok(())
}

pub(super) fn validate_roles(kind: &'static str, roles: &[String]) -> ApplicationResult<()> {
    let mut previous: Option<&str> = None;
    for role in roles {
        LogicalId::try_new(kind, role.clone())?;
        if previous.is_some_and(|previous| previous >= role.as_str()) {
            return Err(ApplicationError::NonCanonical("role ordering"));
        }
        previous = Some(role);
    }
    Ok(())
}

pub(super) fn validate_surface_argument(
    argument: &crate::application::surface::SurfaceArgumentSpec,
) -> ApplicationResult<()> {
    validate_portable_text("surface argument", &argument.name)?;
    validate_portable_text("surface argument kind", &argument.kind)?;
    validate_portable_text("surface argument type", &argument.type_name)?;
    if !matches!(
        argument.kind.as_str(),
        "filter" | "order" | "limit" | "offset" | "primary_key"
    ) {
        return Err(ApplicationError::InvalidSpec(format!(
            "surface argument `{}` has unknown kind `{}`",
            argument.name, argument.kind
        )));
    }
    Ok(())
}

pub(super) fn validate_row_policy(value: &serde_json::Value) -> ApplicationResult<()> {
    let Some(fields) = value.as_object() else {
        return Err(ApplicationError::InvalidSpec(
            "row policy must be a tagged object".into(),
        ));
    };
    let Some(kind) = fields.get("kind").and_then(serde_json::Value::as_str) else {
        return Err(ApplicationError::InvalidSpec(
            "row policy must declare its kind".into(),
        ));
    };
    match kind {
        "unrestricted" | "server_only" if fields.len() == 1 => Ok(()),
        "predicate" if fields.len() == 2 => {
            let expression = fields.get("expression").ok_or_else(|| {
                ApplicationError::InvalidSpec(
                    "predicate row policy must retain its expression".into(),
                )
            })?;
            validate_json_contract("row policy expression", expression)
        }
        _ => Err(ApplicationError::InvalidSpec(
            "row policy contains unknown or redundant material".into(),
        )),
    }
}

pub(super) fn validate_relationship_keys(value: &serde_json::Value) -> ApplicationResult<()> {
    let Some(fields) = value.as_object() else {
        return Err(ApplicationError::InvalidSpec(
            "relationship keys must be a tagged object".into(),
        ));
    };
    let Some(kind) = fields.get("kind").and_then(serde_json::Value::as_str) else {
        return Err(ApplicationError::InvalidSpec(
            "relationship keys must declare their kind".into(),
        ));
    };
    if kind == "embedded" {
        if fields.len() != 1 {
            return Err(ApplicationError::InvalidSpec(
                "embedded relationship keys contain redundant material".into(),
            ));
        }
        return Ok(());
    }
    if !matches!(kind, "direct" | "through" | "through_opaque") {
        return Err(ApplicationError::InvalidSpec(format!(
            "relationship keys have unknown kind `{kind}`"
        )));
    }
    let local = fields
        .get("local")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            ApplicationError::InvalidSpec("relationship keys need local columns".into())
        })?;
    let remote = fields
        .get("remote")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            ApplicationError::InvalidSpec("relationship keys need remote columns".into())
        })?;
    if local.is_empty() || local.len() != remote.len() {
        return Err(ApplicationError::InvalidSpec(
            "relationship key columns must be non-empty and paired".into(),
        ));
    }
    for column in local.iter().chain(remote) {
        let name = column.as_str().ok_or_else(|| {
            ApplicationError::InvalidSpec("relationship key columns must be strings".into())
        })?;
        validate_portable_text("relationship key column", name)?;
    }
    let expected_len = match kind {
        "direct" => 3,
        "through" => 6,
        "through_opaque" => 4,
        _ => unreachable!(),
    };
    if fields.len() != expected_len {
        return Err(ApplicationError::InvalidSpec(
            "relationship keys contain missing or redundant material".into(),
        ));
    }
    if kind == "through" {
        for field in ["table", "source_foreign_key", "target_foreign_key"] {
            if !fields.contains_key(field) {
                return Err(ApplicationError::InvalidSpec(format!(
                    "through relationship keys need `{field}`"
                )));
            }
        }
        let table = fields
            .get("table")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                ApplicationError::InvalidSpec("relationship key `table` must be a string".into())
            })?;
        validate_portable_text("relationship key identity", table)?;
        validate_through_key_list(fields, "source_foreign_key", local.len())?;
        validate_through_key_list(fields, "target_foreign_key", remote.len())?;
    }
    if kind == "through_opaque" {
        if !fields.contains_key("dependency") {
            return Err(ApplicationError::InvalidSpec(
                "opaque relationship keys need a dependency identity".into(),
            ));
        }
        let value = fields
            .get("dependency")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                ApplicationError::InvalidSpec(
                    "relationship key `dependency` must be a string".into(),
                )
            })?;
        validate_portable_text("relationship key identity", value)?;
    }
    Ok(())
}

pub(super) fn validate_through_key_list(
    fields: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    expected_len: usize,
) -> ApplicationResult<()> {
    let columns = fields
        .get(field)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            ApplicationError::InvalidSpec(format!(
                "relationship key `{field}` must be a string list"
            ))
        })?;
    if columns.len() != expected_len || columns.is_empty() {
        return Err(ApplicationError::InvalidSpec(format!(
            "relationship key `{field}` must list one through column per identity column"
        )));
    }
    for column in columns {
        let name = column.as_str().ok_or_else(|| {
            ApplicationError::InvalidSpec(format!(
                "relationship key `{field}` columns must be strings"
            ))
        })?;
        validate_portable_text("relationship key identity", name)?;
    }
    Ok(())
}

pub(super) fn validate_command_type_spec(
    definition: &crate::application::command::CommandTypeSpec,
) -> ApplicationResult<()> {
    validate_command_type_spec_at_depth(definition, 0)
}

pub(super) fn validate_command_type_spec_at_depth(
    definition: &crate::application::command::CommandTypeSpec,
    depth: usize,
) -> ApplicationResult<()> {
    if depth > MAX_MANIFEST_JSON_DEPTH {
        return Err(ApplicationError::InvalidSpec(format!(
            "surface command type exceeds nesting depth {MAX_MANIFEST_JSON_DEPTH}"
        )));
    }
    validate_portable_text("surface command type", &definition.name)?;
    validate_collection_len("surface command type fields", definition.fields.len())?;
    let field_names = definition
        .fields
        .iter()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    validate_unique("surface command type fields", &field_names)?;
    for field in &definition.fields {
        validate_portable_text("surface command field", &field.name)?;
        validate_portable_text("surface command field type", &field.type_name)?;
        if let Some(nested) = &field.nested {
            validate_command_type_spec_at_depth(nested, depth + 1)?;
        }
    }
    Ok(())
}

pub(super) fn require_reference<T: std::hash::BuildHasher>(
    kind: &'static str,
    identity: &str,
    references: &HashSet<&str, T>,
) -> ApplicationResult<()> {
    if !references.contains(identity) {
        return Err(ApplicationError::Missing {
            kind,
            identity: identity.into(),
        });
    }
    Ok(())
}

pub(super) fn validate_collection_len(kind: &'static str, length: usize) -> ApplicationResult<()> {
    if length > MAX_MANIFEST_COLLECTION_ITEMS {
        return Err(ApplicationError::InvalidSpec(format!(
            "{kind} count exceeds {MAX_MANIFEST_COLLECTION_ITEMS}"
        )));
    }
    Ok(())
}

pub(super) fn validate_portable_text(kind: &'static str, value: &str) -> ApplicationResult<()> {
    if value.trim().is_empty()
        || value.trim() != value
        || value.len() > MAX_MANIFEST_STRING_BYTES
        || value.contains('\0')
    {
        return Err(ApplicationError::InvalidIdentity {
            kind,
            value: value.into(),
            reason: "must be a bounded portable logical value",
        });
    }
    Ok(())
}

pub(super) fn validate_artifact_text(kind: &'static str, value: &str) -> ApplicationResult<()> {
    if value.trim().is_empty()
        || value.trim() != value
        || value.len() > MAX_MANIFEST_STRING_BYTES
        || value.contains('\0')
    {
        return Err(ApplicationError::InvalidSpec(format!(
            "{kind} must be a bounded artifact provenance value"
        )));
    }
    Ok(())
}

pub(super) fn validate_sha256_text(kind: &'static str, value: &str) -> ApplicationResult<()> {
    validate_portable_text(kind, value)?;
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(ApplicationError::InvalidSpec(format!(
            "{kind} must use the sha256:<64 lowercase hex> form"
        )));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ApplicationError::InvalidSpec(format!(
            "{kind} must use the sha256:<64 lowercase hex> form"
        )));
    }
    Ok(())
}

pub(super) fn validate_json_contract(
    kind: &'static str,
    value: &serde_json::Value,
) -> ApplicationResult<()> {
    validate_json_contract_with_budget(kind, value, MAX_MANIFEST_JSON_BYTES)
}

pub(super) fn validate_json_contract_with_budget(
    kind: &'static str,
    value: &serde_json::Value,
    max_bytes: usize,
) -> ApplicationResult<()> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > max_bytes {
        return Err(ApplicationError::InvalidSpec(format!(
            "{kind} exceeds {max_bytes} JSON bytes"
        )));
    }
    fn walk(kind: &'static str, value: &serde_json::Value, depth: usize) -> ApplicationResult<()> {
        if depth > MAX_MANIFEST_JSON_DEPTH {
            return Err(ApplicationError::InvalidSpec(format!(
                "{kind} exceeds JSON depth {MAX_MANIFEST_JSON_DEPTH}"
            )));
        }
        match value {
            serde_json::Value::String(value) => {
                if value.len() > MAX_MANIFEST_STRING_BYTES || value.contains('\0') {
                    return Err(ApplicationError::InvalidSpec(format!(
                        "{kind} contains oversized or NUL string material"
                    )));
                }
            }
            serde_json::Value::Array(values) => {
                validate_collection_len(kind, values.len())?;
                for value in values {
                    walk(kind, value, depth + 1)?;
                }
            }
            serde_json::Value::Object(fields) => {
                validate_collection_len(kind, fields.len())?;
                for (key, value) in fields {
                    if key.len() > MAX_MANIFEST_STRING_BYTES || key.contains('\0') {
                        return Err(ApplicationError::InvalidSpec(format!(
                            "{kind} contains oversized or NUL object-key material"
                        )));
                    }
                    walk(kind, value, depth + 1)?;
                }
            }
            serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {
            }
        }
        Ok(())
    }
    walk(kind, value, 0)
}

pub(super) fn validate_unique(kind: &'static str, identities: &[String]) -> ApplicationResult<()> {
    let mut seen = BTreeSet::new();
    for identity in identities {
        if !seen.insert(identity) {
            return Err(ApplicationError::Duplicate {
                kind,
                identity: identity.clone(),
            });
        }
    }
    Ok(())
}

pub(super) fn validate_sorted_unique(
    kind: &'static str,
    identities: &[String],
) -> ApplicationResult<()> {
    validate_unique(kind, identities)?;
    if identities.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(ApplicationError::NonCanonical(kind));
    }
    Ok(())
}

#[cfg(test)]
mod size_limit_tests {
    use super::*;

    #[test]
    fn structured_surface_budget_preserves_nested_validation() {
        let validate = |value: &serde_json::Value| {
            validate_json_contract_with_budget(
                "surface canonical contract",
                value,
                MAX_APPLICATION_MANIFEST_BYTES,
            )
        };
        assert!(validate(&serde_json::json!({"bad": "nul\u{0}value"})).is_err());
        assert!(
            validate(&serde_json::json!({"bad": "x".repeat(MAX_MANIFEST_STRING_BYTES + 1)}))
                .is_err()
        );
        assert!(validate(&serde_json::json!(vec![
            0;
            MAX_MANIFEST_COLLECTION_ITEMS + 1
        ]))
        .is_err());
        let mut deep = serde_json::Value::Null;
        for _ in 0..=MAX_MANIFEST_JSON_DEPTH {
            deep = serde_json::json!([deep]);
        }
        assert!(validate(&deep).is_err());
        let oversized = serde_json::json!(vec![
            "x".repeat(MAX_MANIFEST_STRING_BYTES);
            MAX_MANIFEST_COLLECTION_ITEMS
        ]);
        assert!(validate(&oversized)
            .unwrap_err()
            .to_string()
            .contains(&format!(
                "exceeds {MAX_APPLICATION_MANIFEST_BYTES} JSON bytes"
            )));
    }

    #[test]
    fn json_contract_can_exceed_the_old_256_kib_limit() {
        let chunk = "x".repeat(MAX_MANIFEST_STRING_BYTES);
        let within_budget = serde_json::json!(vec![chunk.clone(); 96]);
        assert!(serde_json::to_vec(&within_budget).unwrap().len() > 256 * 1024);
        validate_json_contract("surface canonical contract", &within_budget).unwrap();

        let over_budget = serde_json::json!(vec![chunk; 257]);
        assert!(serde_json::to_vec(&over_budget).unwrap().len() > MAX_MANIFEST_JSON_BYTES);
        assert!(validate_json_contract("surface canonical contract", &over_budget).is_err());
    }

    #[test]
    fn manifest_accepts_multiple_bounded_contracts_beyond_four_mib() {
        let value = serde_json::json!(vec!["x".repeat(MAX_MANIFEST_STRING_BYTES); 200]);
        assert!(serde_json::to_vec(&value).unwrap().len() < MAX_MANIFEST_JSON_BYTES);
        let mut manifest = ApplicationManifest::new("large-app");
        for index in 0..6 {
            manifest.extensions.push(
                ApplicationExtension::try_new(format!("large.contract.{index}"), 1, value.clone())
                    .unwrap(),
            );
        }

        let bytes = manifest.canonical_bytes().unwrap();
        assert!(bytes.len() > 4 * 1024 * 1024);
        assert!(bytes.len() < MAX_APPLICATION_MANIFEST_BYTES);
        let decoded = ApplicationManifest::from_canonical_bytes(&bytes).unwrap();
        assert_eq!(decoded.canonical_bytes().unwrap(), bytes);
        for index in 0..6 {
            assert_eq!(
                decoded
                    .extensions
                    .iter()
                    .find(|extension| extension.id == format!("large.contract.{index}"))
                    .unwrap()
                    .value,
                value,
            );
        }
    }

    #[test]
    fn manifest_rejects_multiple_contracts_beyond_aggregate_budget() {
        let value = serde_json::json!(vec!["x".repeat(MAX_MANIFEST_STRING_BYTES); 230]);
        assert!(serde_json::to_vec(&value).unwrap().len() < MAX_MANIFEST_JSON_BYTES);
        let mut manifest = ApplicationManifest::new("oversized-app");
        let contract_bytes = serde_json::to_vec(&value).unwrap().len();
        for index in 0..=MAX_APPLICATION_MANIFEST_BYTES / contract_bytes {
            manifest.extensions.push(
                ApplicationExtension::try_new(format!("large.contract.{index}"), 1, value.clone())
                    .unwrap(),
            );
        }

        let error = manifest.canonical_bytes().unwrap_err().to_string();
        assert!(error.contains(&format!(
            "application manifest exceeds {MAX_APPLICATION_MANIFEST_BYTES} bytes"
        )));
        let oversized_bytes = serde_json::to_vec(&manifest).unwrap();
        assert!(oversized_bytes.len() > MAX_APPLICATION_MANIFEST_BYTES);
        let error = ApplicationManifest::from_canonical_bytes(&oversized_bytes)
            .unwrap_err()
            .to_string();
        assert!(error.contains(&format!(
            "application manifest bytes must be between 1 and {MAX_APPLICATION_MANIFEST_BYTES}"
        )));
    }
}
