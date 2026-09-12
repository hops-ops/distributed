use serde::Serialize;

use crate::domain_event::{DomainEventBodyContract, DomainEventContract};
use crate::projection::lower::{ProjectionBodyMetadata, ProjectionPortableType};
use crate::{
    DomainEvent, DomainEventBodyKind, DomainEventDescriptor, DomainState, ProjectionEnvelopeField,
    ProjectionEventSelector, ProjectionValue,
};

/// One preview-time source for an emitted domain-event body field.
///
/// The source is retained only in the server-side command contract. Client
/// manifests lower body paths to program-scoped opaque slots and never expose
/// the outward event schema's private source paths.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CommandProjectionPreviewSource {
    /// Read a value from canonical GraphQL command input.
    InputPath { path: Vec<String> },
    /// Read a value generated into canonical command input before dispatch.
    GeneratedDefaultPath { path: Vec<String> },
    /// Read a framework-authenticated scoped preset by public descriptor.
    TrustedPreset { name: String, codec: String },
    /// Use a portable typed constant.
    Constant { value: ProjectionValue },
    /// Use explicit null.
    Null,
    /// The body property is known to be omitted.
    Absent,
    /// This one field cannot be predicted; other fields remain usable.
    Unknown,
    /// Deliberately non-portable server-only source.
    ///
    /// Registration rejects this variant. It exists as a fail-closed sentinel
    /// for generated declarations, not as a client escape hatch.
    ServerOnly,
}

impl CommandProjectionPreviewSource {
    /// Construct a canonical input-path source.
    pub fn input(path: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self::InputPath {
            path: path.into_iter().map(Into::into).collect(),
        }
    }

    /// Construct a generated-default path source.
    pub fn generated_default(path: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self::GeneratedDefaultPath {
            path: path.into_iter().map(Into::into).collect(),
        }
    }

    /// Construct a trusted scoped-preset source without retaining its value.
    pub fn trusted(name: impl Into<String>, codec: impl Into<String>) -> Self {
        Self::TrustedPreset {
            name: name.into(),
            codec: codec.into(),
        }
    }

    /// Construct a portable constant source.
    pub fn constant(value: ProjectionValue) -> Self {
        Self::Constant { value }
    }
}

/// One emitted-body path and its preview provenance.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CommandProjectionPreviewField {
    pub(crate) body_path: Vec<String>,
    pub(crate) envelope: Option<ProjectionEnvelopeField>,
    #[serde(skip)]
    pub(crate) body_type: Option<ProjectionPortableType>,
    #[serde(skip)]
    pub(crate) body_rust_type: Option<&'static str>,
    #[serde(skip)]
    pub(crate) body_nullable: Option<bool>,
    #[serde(skip)]
    pub(crate) body_always_present: Option<bool>,
    pub(crate) source: CommandProjectionPreviewSource,
}

/// Partial, field-by-field preview for one exact emitted event set.
///
/// Missing fields are unknown by definition. Unknown fields do not poison
/// known fields: manifest lowering can still produce a safe partial patch and
/// attach narrow recovery for the unresolved remainder.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct CommandProjectionPreview {
    pub(crate) selectors: Vec<ProjectionEventSelector>,
    pub(crate) declaration_errors: Vec<String>,
    pub(crate) fields: Vec<CommandProjectionPreviewField>,
}

impl CommandProjectionPreview {
    /// Begin an empty, intentionally partial preview.
    pub fn new() -> Self {
        Self::default()
    }

    /// Bind this preview to the exact outward event-set value also passed to
    /// [`crate::command::TypedCommand::emits`].
    #[must_use]
    pub fn events(mut self, events: CommandProjectionEventSet) -> Self {
        self.selectors = events.selectors;
        self.declaration_errors = events.declaration_errors;
        self
    }

    /// Bind one emitted-event body path to preview provenance.
    #[must_use]
    pub fn field(
        mut self,
        body_path: impl IntoIterator<Item = impl Into<String>>,
        source: CommandProjectionPreviewSource,
    ) -> Self {
        self.fields.push(CommandProjectionPreviewField {
            body_path: body_path.into_iter().map(Into::into).collect(),
            envelope: None,
            body_type: None,
            body_rust_type: None,
            body_nullable: None,
            body_always_present: None,
            source,
        });
        self
    }

    /// Bind one non-intrinsic occurrence-envelope field to command provenance.
    #[must_use]
    pub fn envelope(
        mut self,
        field: ProjectionEnvelopeField,
        source: CommandProjectionPreviewSource,
    ) -> Self {
        self.fields.push(CommandProjectionPreviewField {
            body_path: Vec::new(),
            envelope: Some(field),
            body_type: None,
            body_rust_type: None,
            body_nullable: None,
            body_always_present: None,
            source,
        });
        self
    }
}

/// Preview declaration for one exact emitted event selector.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct CommandProjectionEventPreview {
    pub selector: ProjectionEventSelector,
    pub preview: CommandProjectionPreview,
}

/// Pure reducer over a known cache row for client auto-optimism.
///
/// Domain owns pure semantics. Client delivery is either:
/// - **WASM** ([`Self::wasm`]): gen-client emits a `createWasmJsonPure` host in
///   `pures.ts` — no app TypeScript pure file required.
/// - **Hand module** ([`Self::client_module`]): gen-client imports a named export
///   from `$lib/<module>` (escape hatch).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CommandProjectionPureReduce {
    /// Stable pure id, e.g. `blob.simulate_move`.
    pub fn_name: String,
    /// Path under app `$lib` without extension for a hand-written pure (empty if WASM).
    pub client_module: String,
    /// Named export in that hand module (empty if WASM).
    pub client_export: String,
    /// wasm-pack package under `$lib` without extension, e.g. `blob/pkg/blob_wasm`.
    pub wasm_package: String,
    /// Named WASM export `(recordJson, argsJson) -> assignJson | undefined`.
    pub wasm_export: String,
    /// Cargo package that declared this WASM pure. `portable_command!` fills
    /// this from `CARGO_PKG_NAME`; application authors do not maintain a
    /// parallel build inventory.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub wasm_rust_package: String,
    /// Projection model id (e.g. `BlobGames`).
    pub model: String,
    /// Record key fields: `name` is the model field; `source` is input/default/preset.
    pub key: Vec<CommandProjectionPureArg>,
    /// Pure function arguments (resolved like preview values).
    pub args: Vec<CommandProjectionPureArg>,
    /// Fields taken from the pure result and patched onto the known row.
    pub assign: Vec<String>,
}

/// One named pure-reduce binding (key field or pure arg).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CommandProjectionPureArg {
    pub name: String,
    pub source: CommandProjectionPreviewSource,
}

impl CommandProjectionPureReduce {
    /// Hand-written pure under `$lib/<module>` exporting `client_export`.
    pub fn client_module(
        fn_name: impl Into<String>,
        client_module: impl Into<String>,
        client_export: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            fn_name: fn_name.into(),
            client_module: client_module.into(),
            client_export: client_export.into(),
            wasm_package: String::new(),
            wasm_export: String::new(),
            wasm_rust_package: String::new(),
            model: model.into(),
            key: Vec::new(),
            args: Vec::new(),
            assign: Vec::new(),
        }
    }

    /// Domain pure shipped as wasm-pack under `$lib/<package>`; gen-client hosts it.
    pub fn wasm(
        fn_name: impl Into<String>,
        wasm_package: impl Into<String>,
        wasm_export: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            fn_name: fn_name.into(),
            client_module: String::new(),
            client_export: String::new(),
            wasm_package: wasm_package.into(),
            wasm_export: wasm_export.into(),
            wasm_rust_package: String::new(),
            model: model.into(),
            key: Vec::new(),
            args: Vec::new(),
            assign: Vec::new(),
        }
    }

    /// Attach the Cargo package that owns a declared WASM pure.
    ///
    /// Normal application code receives this through `portable_command!`.
    /// It is public only so generated declarations can remain ordinary Rust.
    #[doc(hidden)]
    #[must_use]
    pub fn declared_in_rust_package(mut self, package: impl Into<String>) -> Self {
        if !self.wasm_package.is_empty() || !self.wasm_export.is_empty() {
            self.wasm_rust_package = package.into();
        }
        self
    }

    /// Deprecated alias for [`Self::client_module`].
    #[deprecated(note = "use client_module() or wasm()")]
    pub fn new(
        fn_name: impl Into<String>,
        client_module: impl Into<String>,
        client_export: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self::client_module(fn_name, client_module, client_export, model)
    }

    #[must_use]
    pub fn key_input(
        mut self,
        field: impl Into<String>,
        path: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.key.push(CommandProjectionPureArg {
            name: field.into(),
            source: CommandProjectionPreviewSource::input(path),
        });
        self
    }

    #[must_use]
    pub fn arg_input(
        mut self,
        name: impl Into<String>,
        path: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.args.push(CommandProjectionPureArg {
            name: name.into(),
            source: CommandProjectionPreviewSource::input(path),
        });
        self
    }

    #[must_use]
    pub fn assign(mut self, fields: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.assign.extend(fields.into_iter().map(Into::into));
        self.assign.sort();
        self.assign.dedup();
        self
    }
}

/// Exact outward events a command may emit, independent of any projector.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct CommandProjectionEvents {
    pub selectors: Vec<ProjectionEventSelector>,
    pub previews: Vec<CommandProjectionEventPreview>,
    /// Values proved by a generated domain transition. Projector arms decide
    /// whether these event values have any client-visible consequence.
    #[serde(skip)]
    pub inferred_values: Vec<CommandProjectionEventPreview>,
    /// Pure reducers over known cache rows (client auto-optimism).
    pub pure_reduces: Vec<CommandProjectionPureReduce>,
    pub declaration_errors: Vec<String>,
}

impl CommandProjectionEvents {
    pub(crate) fn add_event_set(&mut self, events: CommandProjectionEventSet) {
        self.selectors.extend(events.selectors);
        self.declaration_errors.extend(events.declaration_errors);
    }

    pub(crate) fn add_preview(&mut self, preview: CommandProjectionPreview) {
        self.declaration_errors
            .extend(preview.declaration_errors.clone());
        if preview.selectors.is_empty() {
            self.declaration_errors
                .push("projection preview must bind exactly one emitted event variant".to_owned());
            return;
        }
        if preview.selectors.len() != 1 {
            self.declaration_errors.push(
                "projection preview must bind one exact event variant, not a multi-event set"
                    .to_owned(),
            );
            return;
        }
        self.previews
            .extend(preview.selectors.iter().cloned().map(|selector| {
                CommandProjectionEventPreview {
                    selector,
                    preview: preview.clone(),
                }
            }));
    }

    pub(crate) fn add_inferred_values(&mut self, values: CommandProjectionPreview) {
        self.declaration_errors
            .extend(values.declaration_errors.clone());
        if values.selectors.len() != 1 {
            self.declaration_errors.push(
                "inferred transition values must bind exactly one emitted event variant".to_owned(),
            );
            return;
        }
        let selector = values.selectors[0].clone();
        if let Some(existing) = self
            .inferred_values
            .iter_mut()
            .find(|candidate| candidate.selector == selector)
        {
            existing.preview.fields.extend(values.fields);
            existing
                .preview
                .declaration_errors
                .extend(values.declaration_errors);
        } else {
            self.inferred_values.push(CommandProjectionEventPreview {
                selector,
                preview: values,
            });
        }
    }

    pub(crate) fn add_authenticated_user_field(
        &mut self,
        rust_field: &str,
        values: CommandProjectionPreview,
    ) {
        if values.fields.len() != 1 {
            self.declaration_errors.push(
                format!(
                    "authenticated-user inference field `{rust_field}` is not one exact emitted-event body field"
                ),
            );
            return;
        }
        self.add_inferred_values(values);
    }

    pub(crate) fn add_pure_reduce(&mut self, reduce: CommandProjectionPureReduce) {
        self.pure_reduces.push(reduce);
    }

    pub(crate) fn canonicalize_and_validate(&mut self, command: &str) -> Result<(), String> {
        if let Some(error) = self.declaration_errors.first() {
            return Err(format!(
                "typed command `{command}` has an invalid domain-event declaration: {error}"
            ));
        }
        self.selectors
            .sort_by(ProjectionEventSelector::canonical_cmp);
        if self.selectors.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(format!(
                "typed command `{command}` repeats an exact emitted domain event selector"
            ));
        }
        for pair in self.selectors.windows(2) {
            if pair[0].event_name() == pair[1].event_name()
                && pair[0].event_version() == pair[1].event_version()
                && pair[0] != pair[1]
            {
                return Err(format!(
                    "typed command `{command}` declares conflicting schemas for domain event `{}` v{}",
                    pair[0].event_name(),
                    pair[0].event_version()
                ));
            }
        }
        // Preview declarations are synthetic occurrences, not another view of
        // the allowed event set. Preserve their declaration order so generated
        // clients can apply them deterministically until the authoritative
        // ordered command delta replaces the optimistic overlay.
        canonicalize_preview_declarations(command, &self.selectors, &mut self.previews, false)?;
        canonicalize_preview_declarations(
            command,
            &self.selectors,
            &mut self.inferred_values,
            true,
        )?;
        for reduce in &mut self.pure_reduces {
            if reduce.fn_name.trim().is_empty() || reduce.model.trim().is_empty() {
                return Err(format!(
                    "typed command `{command}` pure reduce requires non-empty fn and model"
                ));
            }
            let hand =
                !reduce.client_module.trim().is_empty() || !reduce.client_export.trim().is_empty();
            let wasm =
                !reduce.wasm_package.trim().is_empty() || !reduce.wasm_export.trim().is_empty();
            if hand == wasm {
                return Err(format!(
                    "typed command `{command}` pure reduce `{}` must declare either client_module+client_export or wasm_package+wasm_export (not both, not neither)",
                    reduce.fn_name
                ));
            }
            if hand
                && (reduce.client_module.trim().is_empty()
                    || reduce.client_export.trim().is_empty())
            {
                return Err(format!(
                    "typed command `{command}` pure reduce `{}` client module requires non-empty client_module and client_export",
                    reduce.fn_name
                ));
            }
            if wasm
                && (reduce.wasm_package.trim().is_empty() || reduce.wasm_export.trim().is_empty())
            {
                return Err(format!(
                    "typed command `{command}` pure reduce `{}` wasm package requires non-empty wasm_package and wasm_export",
                    reduce.fn_name
                ));
            }
            if reduce.key.is_empty() {
                return Err(format!(
                    "typed command `{command}` pure reduce `{}` requires at least one key field",
                    reduce.fn_name
                ));
            }
            if reduce.assign.is_empty() {
                return Err(format!(
                    "typed command `{command}` pure reduce `{}` requires at least one assign field",
                    reduce.fn_name
                ));
            }
            reduce.key.sort_by(|a, b| a.name.cmp(&b.name));
            reduce.args.sort_by(|a, b| a.name.cmp(&b.name));
            reduce.assign.sort();
            reduce.assign.dedup();
            for arg in reduce.key.iter().chain(reduce.args.iter()) {
                match &arg.source {
                    CommandProjectionPreviewSource::InputPath { path }
                    | CommandProjectionPreviewSource::GeneratedDefaultPath { path } => {
                        validate_path(command, "pure reduce", path)?;
                    }
                    CommandProjectionPreviewSource::TrustedPreset { name, codec } => {
                        if name.trim().is_empty() || codec.trim().is_empty() {
                            return Err(format!(
                                "typed command `{command}` pure reduce trusted preset name and codec must not be empty"
                            ));
                        }
                    }
                    other => {
                        return Err(format!(
                            "typed command `{command}` pure reduce `{}` arg `{}` uses unsupported source {other:?}",
                            reduce.fn_name, arg.name
                        ));
                    }
                }
            }
        }
        self.pure_reduces
            .sort_by(|left, right| left.fn_name.cmp(&right.fn_name));
        if self
            .pure_reduces
            .windows(2)
            .any(|pair| pair[0].fn_name == pair[1].fn_name)
        {
            return Err(format!(
                "typed command `{command}` repeats pure reduce fn name"
            ));
        }
        Ok(())
    }
}

fn canonicalize_preview_declarations(
    command: &str,
    selectors: &[ProjectionEventSelector],
    previews: &mut [CommandProjectionEventPreview],
    inferred: bool,
) -> Result<(), String> {
    for preview in previews {
        if selectors
            .binary_search_by(|selector| selector.canonical_cmp(&preview.selector))
            .is_err()
        {
            let source = if inferred {
                "infers transition values"
            } else {
                "declares preview provenance"
            };
            return Err(format!(
                "typed command `{command}` {source} outside its exact emitted event set"
            ));
        }
        preview.preview.fields.sort_by_key(preview_field_key);
        for pair in preview.preview.fields.windows(2) {
            if preview_field_key(&pair[0]) == preview_field_key(&pair[1]) {
                let source = if inferred {
                    "inferred transition value"
                } else {
                    "preview provenance"
                };
                return Err(format!(
                    "typed command `{command}` repeats {source} for one event value"
                ));
            }
        }
        for field in &preview.preview.fields {
            if field.envelope.is_none() {
                validate_path(command, "emitted body", &field.body_path)?;
            }
            match &field.source {
                CommandProjectionPreviewSource::InputPath { path }
                | CommandProjectionPreviewSource::GeneratedDefaultPath { path } => {
                    validate_path(command, "preview input", path)?;
                }
                CommandProjectionPreviewSource::TrustedPreset { name, codec } => {
                    if name.trim().is_empty() || codec.trim().is_empty() {
                        return Err(format!(
                            "typed command `{command}` preview trusted preset name and codec must not be empty"
                        ));
                    }
                }
                CommandProjectionPreviewSource::ServerOnly => {
                    return Err(format!(
                        "typed command `{command}` cannot expose server-only preview provenance"
                    ));
                }
                CommandProjectionPreviewSource::Constant { .. }
                | CommandProjectionPreviewSource::Null
                | CommandProjectionPreviewSource::Absent
                | CommandProjectionPreviewSource::Unknown => {}
            }
        }
    }
    Ok(())
}

/// Sealed exact event-set value produced by [`events!`](crate::events).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CommandProjectionEventSet {
    selectors: Vec<ProjectionEventSelector>,
    declaration_errors: Vec<String>,
}

/// Build the sealed command event-set value used by `events!`.
#[doc(hidden)]
pub fn __command_projection_events(
    descriptors: impl IntoIterator<Item = Result<DomainEventDescriptor, String>>,
) -> CommandProjectionEventSet {
    let mut events = CommandProjectionEventSet::default();
    for descriptor in descriptors {
        let descriptor = match descriptor {
            Ok(descriptor) => descriptor,
            Err(error) => {
                events.declaration_errors.push(error);
                continue;
            }
        };
        match ProjectionEventSelector::try_from_descriptor(&descriptor) {
            Ok(selector) => events.selectors.push(selector),
            Err(error) => events.declaration_errors.push(error.to_string()),
        }
    }
    events
}

/// Resolve one exact typed command event descriptor for `events!`.
#[doc(hidden)]
pub fn __command_projection_event_descriptor<E: DomainEventContract>(
) -> Result<DomainEventDescriptor, String> {
    let descriptor = E::descriptor();
    if descriptor.name != E::EVENT_NAME {
        return Err(format!(
            "event contract name `{}` differs from descriptor name `{}`",
            E::EVENT_NAME,
            descriptor.name
        ));
    }
    if descriptor.version != E::EVENT_VERSION {
        return Err(format!(
            "event contract `{}` version {} differs from descriptor version {}",
            E::EVENT_NAME,
            E::EVENT_VERSION,
            descriptor.version
        ));
    }
    Ok(descriptor)
}

/// Build a structured state preview from generated body metadata.
#[doc(hidden)]
pub fn __command_projection_state_preview<E, S>(
    fields: Vec<(&'static str, CommandProjectionPreviewSource)>,
) -> CommandProjectionPreview
where
    E: DomainEventBodyContract<S>,
    S: DomainState + ProjectionBodyMetadata,
{
    let descriptor = __command_projection_event_descriptor::<E>().and_then(|descriptor| {
        let expected = DomainEventDescriptor::state::<S>(E::EVENT_NAME, E::EVENT_VERSION);
        if descriptor != expected || descriptor.body.kind != DomainEventBodyKind::State {
            return Err(format!(
                "state preview event contract `{}` does not exactly describe `{}` state",
                E::EVENT_NAME,
                std::any::type_name::<S>()
            ));
        }
        Ok(descriptor)
    });
    structured_preview::<S>(__command_projection_events([descriptor]), fields)
}

/// Build compiler-inferred state values from a sourced transition.
///
/// Unlike the application-authored preview helper, aggregate fields absent
/// from the outward `DomainState` are ignored. This lets the sourced macro
/// inspect recorder assignments without coupling private aggregate storage to
/// the public event schema.
#[doc(hidden)]
pub fn __command_projection_state_known_values<E, S>(
    fields: Vec<(&'static str, CommandProjectionPreviewSource)>,
) -> CommandProjectionPreview
where
    E: DomainEventBodyContract<S>,
    S: DomainState + ProjectionBodyMetadata,
{
    let descriptor = __command_projection_event_descriptor::<E>().and_then(|descriptor| {
        let expected = DomainEventDescriptor::state::<S>(E::EVENT_NAME, E::EVENT_VERSION);
        if descriptor != expected || descriptor.body.kind != DomainEventBodyKind::State {
            return Err(format!(
                "inferred transition event contract `{}` does not exactly describe `{}` state",
                E::EVENT_NAME,
                std::any::type_name::<S>()
            ));
        }
        Ok(descriptor)
    });
    let mut values =
        CommandProjectionPreview::new().events(__command_projection_events([descriptor]));
    for (rust_name, source) in fields {
        let Some(field) = S::PROJECTION_FIELDS
            .iter()
            .find(|field| field.rust_name == rust_name && field.present)
        else {
            continue;
        };
        values.fields.push(CommandProjectionPreviewField {
            body_path: vec![field.wire_name.to_owned()],
            envelope: None,
            body_type: Some(field.portable_type),
            body_rust_type: Some(field.rust_type),
            body_nullable: Some(field.nullable),
            body_always_present: Some(field.always_present),
            source,
        });
    }
    values
}

/// Build a structured sparse-event preview from generated body metadata.
#[doc(hidden)]
pub fn __command_projection_event_preview<E, B>(
    fields: Vec<(&'static str, CommandProjectionPreviewSource)>,
) -> CommandProjectionPreview
where
    E: DomainEventBodyContract<B>,
    B: DomainEvent + ProjectionBodyMetadata,
{
    let descriptor = __command_projection_event_descriptor::<E>().and_then(|descriptor| {
        if descriptor != B::DESCRIPTOR || descriptor.body.kind != DomainEventBodyKind::Event {
            return Err(format!(
                "event preview contract `{}` differs from its exact typed body descriptor",
                E::EVENT_NAME
            ));
        }
        Ok(descriptor)
    });
    structured_preview::<B>(__command_projection_events([descriptor]), fields)
}

fn structured_preview<B: ProjectionBodyMetadata>(
    events: CommandProjectionEventSet,
    fields: Vec<(&'static str, CommandProjectionPreviewSource)>,
) -> CommandProjectionPreview {
    let mut preview = CommandProjectionPreview::new().events(events);
    for (rust_name, source) in fields {
        match B::PROJECTION_FIELDS
            .iter()
            .find(|field| field.rust_name == rust_name && field.present)
        {
            Some(field) => preview.fields.push(CommandProjectionPreviewField {
                body_path: vec![field.wire_name.to_owned()],
                envelope: None,
                body_type: Some(field.portable_type),
                body_rust_type: Some(field.rust_type),
                body_nullable: Some(field.nullable),
                body_always_present: Some(field.always_present),
                source,
            }),
            None => preview.declaration_errors.push(format!(
                "state preview references unknown body field `{rust_name}`"
            )),
        }
    }
    preview
}

/// Convert a typed constant into the portable preview value lattice.
#[doc(hidden)]
pub fn __command_projection_preview_constant(
    value: impl Serialize,
) -> CommandProjectionPreviewSource {
    match serde_json::to_value(value)
        .map_err(|error| error.to_string())
        .and_then(|value| ProjectionValue::try_from_json(value).map_err(|error| error.to_string()))
    {
        Ok(value) => CommandProjectionPreviewSource::Constant { value },
        Err(_) => CommandProjectionPreviewSource::Unknown,
    }
}

/// Type-level source of a command's outward domain-event set.
///
/// Implemented for:
/// - every [`DomainEventContract`] marker (for example `TodoCreatedDomainEvent`)
/// - tuples of those markers
/// - `#[sourced]`-generated `domain_commands::*` transition witnesses (public
///   aggregate methods that call domain-marked `#[event]` recorders)
///
/// Prefer [`crate::command::TypedCommand::emits_events`] with these types over
/// hand-maintaining a parallel event list when the domain already owns the
/// transition.
pub trait CommandEventSet {
    /// Build the sealed event-set value used by command registration.
    fn command_event_set() -> CommandProjectionEventSet;

    /// Return values proved by a generated domain transition.
    ///
    /// Ordinary event markers have none. `#[sourced]` transition witnesses
    /// override this with portable unconditional recorder values; projection
    /// arms remain the sole definition of their cache consequences.
    fn command_event_known_values() -> Vec<CommandProjectionPreview> {
        Vec::new()
    }
}

impl<E: DomainEventContract> CommandEventSet for E {
    fn command_event_set() -> CommandProjectionEventSet {
        __command_projection_events([__command_projection_event_descriptor::<E>()])
    }
}

macro_rules! impl_command_event_set_tuple {
    ($($E:ident),+) => {
        impl<$($E: DomainEventContract),+> CommandEventSet for ($($E,)+) {
            fn command_event_set() -> CommandProjectionEventSet {
                __command_projection_events([
                    $(__command_projection_event_descriptor::<$E>()),+
                ])
            }
        }
    };
}

impl_command_event_set_tuple!(E1, E2);
impl_command_event_set_tuple!(E1, E2, E3);
impl_command_event_set_tuple!(E1, E2, E3, E4);
impl_command_event_set_tuple!(E1, E2, E3, E4, E5);
impl_command_event_set_tuple!(E1, E2, E3, E4, E5, E6);
impl_command_event_set_tuple!(E1, E2, E3, E4, E5, E6, E7);
impl_command_event_set_tuple!(E1, E2, E3, E4, E5, E6, E7, E8);

/// Declare an exact, type-checked outward domain-event set.
#[macro_export]
macro_rules! events {
    ($($event:ty),+ $(,)?) => {
        $crate::command::__command_projection_events([
            $($crate::command::__command_projection_event_descriptor::<$event>()),+
        ])
    };
}

/// Build partial preview provenance for an exact outward event set.
///
/// Body paths remain server-only. Manifest lowering replaces body-path leaves
/// in authoritative projection expressions with program-scoped opaque slots.
#[macro_export]
macro_rules! state_preview {
    (
        $event:ty => $state:ty { $($fields:tt)* }
    ) => {{
        $crate::command::__command_projection_state_preview::<$event, $state>(
            $crate::__distributed_state_preview_fields!(@collect [] ; $($fields)*)
        )
    }};
}

/// Build partial preview provenance for one exact sparse outward event.
#[macro_export]
macro_rules! event_preview {
    (
        $event:ty => $body:ty { $($fields:tt)* }
    ) => {{
        $crate::command::__command_projection_event_preview::<$event, $body>(
            $crate::__distributed_state_preview_fields!(@collect [] ; $($fields)*)
        )
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __distributed_state_preview_fields {
    (@collect [$($out:expr,)*] ; ..unknown $(,)?) => {
        vec![$($out,)*]
    };
    (@collect [$($out:expr,)*] ; ) => {
        vec![$($out,)*]
    };
    (@collect [$($out:expr,)*] ;
        $field:ident : input.$first:ident $(.$rest:ident)*,
        $($tail:tt)*
    ) => {
        $crate::__distributed_state_preview_fields!(
            @collect [
                $($out,)*
                (
                    stringify!($field),
                    $crate::command::CommandProjectionPreviewSource::input([
                        stringify!($first) $(, stringify!($rest))*
                    ])
                ),
            ];
            $($tail)*
        )
    };
    (@collect [$($out:expr,)*] ;
        $field:ident : generated.$first:ident $(.$rest:ident)*,
        $($tail:tt)*
    ) => {
        $crate::__distributed_state_preview_fields!(
            @collect [
                $($out,)*
                (
                    stringify!($field),
                    $crate::command::CommandProjectionPreviewSource::generated_default([
                        stringify!($first) $(, stringify!($rest))*
                    ])
                ),
            ];
            $($tail)*
        )
    };
    (@collect [$($out:expr,)*] ;
        $field:ident : trusted($name:expr, $codec:expr),
        $($tail:tt)*
    ) => {
        $crate::__distributed_state_preview_fields!(
            @collect [
                $($out,)*
                (
                    stringify!($field),
                    $crate::command::CommandProjectionPreviewSource::trusted($name, $codec)
                ),
            ];
            $($tail)*
        )
    };
    (@collect [$($out:expr,)*] ; $field:ident : unknown, $($tail:tt)*) => {
        $crate::__distributed_state_preview_fields!(
            @collect [$($out,)* (stringify!($field), $crate::command::CommandProjectionPreviewSource::Unknown),];
            $($tail)*
        )
    };
    (@collect [$($out:expr,)*] ; $field:ident : absent, $($tail:tt)*) => {
        $crate::__distributed_state_preview_fields!(
            @collect [$($out,)* (stringify!($field), $crate::command::CommandProjectionPreviewSource::Absent),];
            $($tail)*
        )
    };
    (@collect [$($out:expr,)*] ; $field:ident : null, $($tail:tt)*) => {
        $crate::__distributed_state_preview_fields!(
            @collect [$($out,)* (stringify!($field), $crate::command::CommandProjectionPreviewSource::Null),];
            $($tail)*
        )
    };
    (@collect [$($out:expr,)*] ; $field:ident : $constant:path, $($tail:tt)*) => {
        $crate::__distributed_state_preview_fields!(
            @collect [
                $($out,)*
                (
                    stringify!($field),
                    $crate::command::__command_projection_preview_constant($constant)
                ),
            ];
            $($tail)*
        )
    };
    (@collect [$($out:expr,)*] ; $field:ident : $constant:literal, $($tail:tt)*) => {
        $crate::__distributed_state_preview_fields!(
            @collect [
                $($out,)*
                (
                    stringify!($field),
                    $crate::command::__command_projection_preview_constant($constant)
                ),
            ];
            $($tail)*
        )
    };
}

fn validate_path(command: &str, label: &str, path: &[String]) -> Result<(), String> {
    if path.is_empty() || path.iter().any(|segment| segment.trim().is_empty()) {
        return Err(format!(
            "typed command `{command}` {label} path must contain only non-empty segments"
        ));
    }
    Ok(())
}

fn preview_field_key(field: &CommandProjectionPreviewField) -> (u8, Vec<String>) {
    match field.envelope {
        Some(envelope) => (
            1,
            vec![serde_json::to_string(&envelope)
                .expect("projection envelope field serialization cannot fail")],
        ),
        None => (0, field.body_path.clone()),
    }
}
