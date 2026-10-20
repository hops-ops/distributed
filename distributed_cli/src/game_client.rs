//! `distributed game-client`: typed TypeScript for one `distributed_game`
//! zone surface (`GameSurface` JSON, schema v3, from `distributed
//! game-manifest`).
//!
//! The generated files import only `@hops-ops/distributed/game` and are
//! deterministic (same surface → same bytes):
//!
//! | file | contents |
//! |---|---|
//! | `schema.ts` | `KIND` ids, per-kind field interfaces + accessors over rendered entities, field wire types, `CORRECTIONS` defaults |
//! | `input.ts` | the input interface, `BUTTON` bits, `encodeInput` / `decodeInput` / `sanitizeInput` |
//! | `events.ts` | record + event payload interfaces, `EVENT` ids, `EVENTS` codecs, `decodeEvent`, `onEvent`; `encode<Record>` / `decode<Record>` for the surface's standalone codecs; with zone inbox messages, `<Name>Message` interfaces, `MESSAGE` ids, `MESSAGES` codecs and `encodeMessage` (for services posting to `/admin/zone-message`) |
//! | `client.ts` | `create<Surface>Client`: a `GameClient` with the input encoder and correction defaults wired in, plus typed `on`; with a predicted step, prediction defaults to the compiled WebAssembly step |
//! | `predict.ts` | only with a `#[predict]` step: its state/context layouts, the module URL (`wasm/<surface>.wasm`, built by `distributed game-wasm`) and `create<Surface>Prediction` |
//! | `pures.ts` | only with `#[game_pure]` functions: `create<Surface>Pures`, one typed wrapper per function over the same module |
//! | `constants.ts` | only with constants: each value as a typed TypeScript literal (exact binary32 floats) |
//! | `index.ts` | re-exports |
//! | `surface.json` | the surface it was generated from (so `--check` also catches manifest drift) |
//!
//! This is intentionally not the GraphQL client manifest: that contract is
//! strict (`deny_unknown_fields`, fingerprints) and describes a different
//! protocol.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use serde::Deserialize;

use crate::client_compiler::GeneratedClientFile;

/// The only surface schema version this generator understands.
pub const GAME_SURFACE_SCHEMA_VERSION: u32 = 3;

/// The only prediction ABI version this generator (and the SDK) speaks.
pub const PREDICT_ABI_VERSION: u32 = 1;

const SDK: &str = "@hops-ops/distributed/game";

/// Import specifier style for imports between generated files.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImportExtension {
    /// `./schema.ts` (Node type stripping, Vite, `allowImportingTsExtensions`).
    Ts,
    /// `./schema.js` (tsc `NodeNext` emit).
    Js,
    /// `./schema` (bundler resolution).
    None,
}

impl ImportExtension {
    fn suffix(self) -> &'static str {
        match self {
            Self::Ts => ".ts",
            Self::Js => ".js",
            Self::None => "",
        }
    }
}

// ---- surface mirror (schema v3) -------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Surface {
    schema_version: u32,
    name: String,
    protocol_version: u32,
    kinds: Vec<Kind>,
    input: Option<Input>,
    events: Vec<Event>,
    records: Vec<Record>,
    #[serde(default)]
    predict: Option<Predict>,
    #[serde(default)]
    pures: Vec<Pure>,
    #[serde(default)]
    constants: Vec<Constant>,
    #[serde(default)]
    codecs: Vec<String>,
    #[serde(default)]
    messages: Vec<Event>,
}

/// A pure function compiled into the surface's module (`#[game_pure]`).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Pure {
    pub(crate) export: String,
    pub(crate) package: String,
    #[serde(default)]
    arg: Option<Wire>,
    output: Wire,
    pub(crate) abi: u32,
}

/// A constant emitted as TypeScript data.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Constant {
    name: String,
    #[serde(rename = "type")]
    ty: Wire,
    value: serde_json::Value,
}

/// What `distributed game-wasm` compiles for a surface: one module from one
/// Cargo package exporting the predicted step and the pure functions.
#[derive(Debug)]
pub(crate) struct ModulePlan {
    /// The declaring Cargo package.
    pub(crate) package: String,
    /// Required function exports (`dg_predict_<step>`, `dg_pure_<fn>`).
    pub(crate) exports: Vec<String>,
    /// ABI version.
    pub(crate) abi: u32,
}
/// The zone's client-predicted step (`#[predict]`).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Predict {
    pub(crate) kind: String,
    pub(crate) package: String,
    pub(crate) export: String,
    pub(crate) state: String,
    #[serde(default)]
    pub(crate) context: Option<String>,
    pub(crate) abi: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Kind {
    id: u16,
    name: String,
    priority: f32,
    fields: Vec<EntityField>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EntityField {
    name: String,
    #[serde(rename = "type")]
    ty: Wire,
    priority: f32,
    correction: Option<Correction>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum Correction {
    Ease {
        #[serde(rename = "durationMs")]
        duration_ms: u32,
    },
    Snap,
    Replace,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Field {
    name: String,
    #[serde(rename = "type")]
    ty: Wire,
    #[serde(default)]
    range: Option<[f64; 2]>,
    #[serde(default)]
    buttons: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Input {
    name: String,
    fields: Vec<Field>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Event {
    id: u32,
    name: String,
    payload: Payload,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Payload {
    Fields {
        fields: Vec<Field>,
    },
    Value {
        #[serde(rename = "type")]
        ty: Wire,
    },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Record {
    name: String,
    fields: Vec<Field>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Wire {
    Bool,
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    F32,
    F64,
    Quant { min: f32, max: f32, bits: u8 },
    Angle { bits: u8 },
    ShortStr,
    Str,
    Bytes,
    List { item: Box<Wire> },
    Optional { item: Box<Wire> },
    Record { name: String },
}

impl Wire {
    fn is_entity_type(&self) -> bool {
        matches!(
            self,
            Self::Bool
                | Self::U8
                | Self::U16
                | Self::U32
                | Self::I32
                | Self::F32
                | Self::Quant { .. }
                | Self::Angle { .. }
                | Self::ShortStr
                | Self::Bytes
        )
    }

    fn is_float(&self) -> bool {
        matches!(self, Self::F32 | Self::F64)
    }

    fn is_integer(&self) -> bool {
        matches!(
            self,
            Self::U8 | Self::I8 | Self::U16 | Self::I16 | Self::U32 | Self::I32 | Self::U64
        )
    }
}

// ---- naming ---------------------------------------------------------------

fn words(name: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut prev_lower = false;
    for c in name.chars() {
        if c == '_' || c == '-' || c == ' ' {
            if !current.is_empty() {
                out.push(std::mem::take(&mut current));
            }
            prev_lower = false;
            continue;
        }
        if c.is_uppercase() && prev_lower && !current.is_empty() {
            out.push(std::mem::take(&mut current));
        }
        prev_lower = c.is_lowercase() || c.is_ascii_digit();
        current.push(c);
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

fn capitalize(word: &str) -> String {
    let mut chars = word.chars();
    match chars.next() {
        Some(first) => first
            .to_uppercase()
            .chain(chars.flat_map(char::to_lowercase))
            .collect(),
        None => String::new(),
    }
}

/// `view_radius` → `viewRadius`.
fn camel(name: &str) -> String {
    let words = words(name);
    let mut out = String::new();
    for (i, word) in words.iter().enumerate() {
        if i == 0 {
            out.push_str(&word.to_lowercase());
        } else {
            out.push_str(&capitalize(word));
        }
    }
    out
}

/// `board_snapshot` → `BoardSnapshot`.
fn pascal(name: &str) -> String {
    words(name).iter().map(|w| capitalize(w)).collect()
}

/// `slow_ms` → `SLOW_MS`.
fn constant(name: &str) -> String {
    words(name)
        .iter()
        .map(|w| w.to_uppercase())
        .collect::<Vec<_>>()
        .join("_")
}

fn is_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_' || c == '$')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
}

fn key(name: &str) -> String {
    if is_identifier(name) {
        name.to_string()
    } else {
        format!("'{}'", name.replace('\\', "\\\\").replace('\'', "\\'"))
    }
}

/// Single-quoted TypeScript string literal.
fn string(value: &str) -> String {
    let mut out = String::from("'");
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            '\n' => out.push_str("\\n"),
            c if c.is_control() => {
                let _ = write!(out, "\\u{{{:x}}}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('\'');
    out
}

fn f32_literal(v: f32) -> String {
    if v.is_finite() {
        format!("{v}")
    } else {
        "Number.NaN".into()
    }
}

/// The exact binary32 value as a JS number (`Math.fround` of it is itself).
fn f32_exact(v: f32) -> String {
    if v.is_finite() {
        format!("{}", f64::from(v))
    } else {
        "Number.NaN".into()
    }
}

fn f64_literal(v: f64) -> String {
    format!("{v}")
}

// ---- generation -----------------------------------------------------------

struct Gen<'a> {
    surface: &'a Surface,
    records: BTreeMap<&'a str, &'a Record>,
    ext: ImportExtension,
}

type GenResult<T> = Result<T, String>;

/// Generate the client files for a surface manifest (JSON text).
pub fn generate_game_client(
    manifest_json: &str,
    ext: ImportExtension,
) -> GenResult<Vec<GeneratedClientFile>> {
    let value: serde_json::Value =
        serde_json::from_str(manifest_json).map_err(|e| format!("parse game surface: {e}"))?;
    let version = value
        .get("schemaVersion")
        .and_then(serde_json::Value::as_u64);
    if version != Some(u64::from(GAME_SURFACE_SCHEMA_VERSION)) {
        return Err(format!(
            "unsupported game surface schemaVersion {version:?}; this CLI generates schema version {GAME_SURFACE_SCHEMA_VERSION}"
        ));
    }
    let surface: Surface =
        serde_json::from_value(value.clone()).map_err(|e| format!("invalid game surface: {e}"))?;
    let gen = Gen::new(&surface, ext)?;
    let mut files = vec![
        file("schema.ts", gen.schema_ts()?),
        file("input.ts", gen.input_ts()?),
        file("events.ts", gen.events_ts()?),
        file("client.ts", gen.client_ts()),
        file("index.ts", gen.index_ts()),
        file(
            "surface.json",
            serde_json::to_string_pretty(&value).expect("surface serializes") + "\n",
        ),
    ];
    if let Some(predict) = gen.predict_ts()? {
        files.push(file("predict.ts", predict));
    }
    if let Some(pures) = gen.pures_ts()? {
        files.push(file("pures.ts", pures));
    }
    if let Some(constants) = gen.constants_ts()? {
        files.push(file("constants.ts", constants));
    }
    files.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(files)
}

/// What `distributed game-wasm` builds for a surface: its name and the
/// validated module plan (`None` when the zone compiles nothing: no
/// predicted step and no pure functions).
pub(crate) fn surface_module(manifest_json: &str) -> GenResult<(String, Option<ModulePlan>)> {
    validate_game_surface(manifest_json)?;
    let surface: Surface =
        serde_json::from_str(manifest_json).map_err(|e| format!("invalid game surface: {e}"))?;
    let package = surface
        .predict
        .as_ref()
        .map(|p| p.package.clone())
        .or_else(|| surface.pures.first().map(|p| p.package.clone()));
    let plan = package.map(|package| {
        let mut exports: Vec<String> = surface
            .predict
            .iter()
            .map(|p| format!("dg_predict_{}", p.export))
            .collect();
        exports.extend(
            surface
                .pures
                .iter()
                .map(|p| format!("dg_pure_{}", p.export)),
        );
        ModulePlan {
            package,
            exports,
            abi: PREDICT_ABI_VERSION,
        }
    });
    Ok((surface.name, plan))
}

/// Module path of a surface's compiled step, relative to the generated
/// client directory.
pub(crate) fn predict_wasm_path(surface: &str) -> String {
    format!("wasm/{surface}.wasm")
}

/// Validate a manifest (as `game-manifest` prints it).
pub fn validate_game_surface(manifest_json: &str) -> GenResult<()> {
    generate_game_client(manifest_json, ImportExtension::Ts).map(|_| ())
}

fn file(path: &str, contents: String) -> GeneratedClientFile {
    GeneratedClientFile {
        path: path.to_string(),
        contents,
    }
}

impl<'a> Gen<'a> {
    fn new(surface: &'a Surface, ext: ImportExtension) -> GenResult<Self> {
        let _ = (surface.schema_version, surface.protocol_version);
        if !surface
            .name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
            || surface.name.is_empty()
        {
            return Err(format!("invalid surface name {:?}", surface.name));
        }
        let mut records = BTreeMap::new();
        for record in &surface.records {
            if !is_identifier(&record.name) {
                return Err(format!(
                    "record name {:?} is not an identifier",
                    record.name
                ));
            }
            if records.insert(record.name.as_str(), record).is_some() {
                return Err(format!("duplicate record {}", record.name));
            }
        }
        let gen = Self {
            surface,
            records,
            ext,
        };
        gen.validate()?;
        Ok(gen)
    }

    fn validate(&self) -> GenResult<()> {
        let mut kind_ids = BTreeSet::new();
        let mut kind_names = BTreeSet::new();
        for kind in &self.surface.kinds {
            if !kind_ids.insert(kind.id) {
                return Err(format!("duplicate kind id {}", kind.id));
            }
            if pascal(&kind.name).is_empty() || !kind_names.insert(pascal(&kind.name)) {
                return Err(format!("duplicate or empty kind name {:?}", kind.name));
            }
            let mut props = BTreeSet::new();
            for field in &kind.fields {
                if !field.ty.is_entity_type() {
                    return Err(format!(
                        "kind {} field {} has a non-entity wire type",
                        kind.name, field.name
                    ));
                }
                if !props.insert(camel(&field.name)) {
                    return Err(format!(
                        "kind {} fields collide as `{}`",
                        kind.name,
                        camel(&field.name)
                    ));
                }
            }
        }
        let mut event_ids = BTreeSet::new();
        let mut event_names = BTreeSet::new();
        for event in &self.surface.events {
            if !event_ids.insert(event.id) || !event_names.insert(camel(&event.name)) {
                return Err(format!("duplicate event {} / {}", event.id, event.name));
            }
            match &event.payload {
                Payload::Fields { fields } => self.check_fields(fields, &event.name)?,
                Payload::Value { ty } => self.check_type(ty, &event.name)?,
            }
        }
        for record in &self.surface.records {
            self.check_fields(&record.fields, &record.name)?;
        }
        if let Some(input) = &self.surface.input {
            self.check_fields(&input.fields, &input.name)?;
            let mut buttons = BTreeSet::new();
            for field in &input.fields {
                for b in &field.buttons {
                    if !buttons.insert(camel(b)) {
                        return Err(format!("duplicate input button {b}"));
                    }
                }
            }
        }
        self.record_order()?;
        self.check_predict()?;
        self.check_pures()?;
        self.check_constants()?;
        for codec in &self.surface.codecs {
            if !self.records.contains_key(codec.as_str()) {
                return Err(format!("codec record {codec} is missing"));
            }
        }
        self.check_exports()
    }

    /// Pure functions: valid unique exports, one Cargo package (the
    /// predicted step's, if any), known types, the ABI this generator speaks.
    fn check_pures(&self) -> GenResult<()> {
        let mut exports = BTreeSet::new();
        let mut wrappers = BTreeSet::new();
        let package = self.surface.predict.as_ref().map(|p| p.package.as_str());
        for pure in &self.surface.pures {
            if pure.abi != PREDICT_ABI_VERSION {
                return Err(format!(
                    "pure function {} uses ABI v{}; this CLI generates ABI v{PREDICT_ABI_VERSION}",
                    pure.export, pure.abi
                ));
            }
            if pure.export.is_empty()
                || !pure
                    .export
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                return Err(format!("invalid pure function export {:?}", pure.export));
            }
            if !exports.insert(pure.export.as_str()) || !wrappers.insert(camel(&pure.export)) {
                return Err(format!("duplicate pure function {}", pure.export));
            }
            if matches!(camel(&pure.export).as_str(), "ready" | "loaded" | "module") {
                return Err(format!(
                    "pure function name {} is reserved by the generated handle",
                    pure.export
                ));
            }
            if pure.package.is_empty() {
                return Err(format!(
                    "pure function {} names no Cargo package",
                    pure.export
                ));
            }
            let expected = package.unwrap_or(self.surface.pures[0].package.as_str());
            if pure.package != expected {
                return Err(format!(
                    "pure function {} is declared in Cargo package {}, but the surface's module is built from {expected}",
                    pure.export, pure.package
                ));
            }
            if let Some(arg) = &pure.arg {
                self.check_type(arg, &pure.export)?;
            }
            self.check_type(&pure.output, &pure.export)?;
        }
        Ok(())
    }

    /// Constants: unique `snake_case` names, values that match their type.
    fn check_constants(&self) -> GenResult<()> {
        let mut names = BTreeSet::new();
        for constant in &self.surface.constants {
            let valid = constant.name.starts_with(|c: char| c.is_ascii_lowercase())
                && constant
                    .name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
            if !valid || !names.insert(constant.name.as_str()) {
                return Err(format!(
                    "constant name {:?} is not snake_case or is declared twice",
                    constant.name
                ));
            }
            self.check_type(&constant.ty, &constant.name)?;
            self.literal(&constant.ty, &constant.value, 0)
                .map_err(|e| format!("constant {}: {e}", constant.name))?;
        }
        Ok(())
    }

    /// The predicted step names a kind of this surface, a state record whose
    /// fields are fields of that kind (plain scalar types), an optional
    /// context record, and the ABI this generator speaks.
    fn check_predict(&self) -> GenResult<()> {
        let Some(predict) = &self.surface.predict else {
            return Ok(());
        };
        if predict.abi != PREDICT_ABI_VERSION {
            return Err(format!(
                "predicted step uses ABI v{}; this CLI generates ABI v{PREDICT_ABI_VERSION}",
                predict.abi
            ));
        }
        if predict.export.is_empty()
            || !predict
                .export
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(format!(
                "invalid predicted step export {:?}",
                predict.export
            ));
        }
        if predict.package.is_empty() {
            return Err("predicted step names no Cargo package".into());
        }
        if self.surface.input.is_none() {
            return Err("a predicted step needs the zone's input type".into());
        }
        let kind = self
            .surface
            .kinds
            .iter()
            .find(|k| k.name == predict.kind)
            .ok_or_else(|| format!("predicted kind {:?} is not replicated", predict.kind))?;
        let state = self
            .records
            .get(predict.state.as_str())
            .ok_or_else(|| format!("predicted state record {} is missing", predict.state))?;
        if state.fields.is_empty() {
            return Err(format!("predicted state {} has no fields", state.name));
        }
        for field in &state.fields {
            let entity = kind
                .fields
                .iter()
                .find(|f| f.name == field.name)
                .ok_or_else(|| {
                    format!(
                        "predicted state {} field {} is not a field of kind {}",
                        state.name, field.name, kind.name
                    )
                })?;
            let matches = match field.ty {
                Wire::F32 => matches!(
                    entity.ty,
                    Wire::F32 | Wire::Quant { .. } | Wire::Angle { .. }
                ),
                Wire::Bool => matches!(entity.ty, Wire::Bool),
                Wire::U8 => matches!(entity.ty, Wire::U8),
                Wire::U16 => matches!(entity.ty, Wire::U16),
                Wire::U32 => matches!(entity.ty, Wire::U32),
                Wire::I32 => matches!(entity.ty, Wire::I32),
                _ => false,
            };
            if !matches {
                return Err(format!(
                    "predicted state {} field {} does not match the type of kind {} field {}",
                    state.name, field.name, kind.name, entity.name
                ));
            }
        }
        if let Some(context) = &predict.context {
            if !self.records.contains_key(context.as_str()) {
                return Err(format!("predicted context record {context} is missing"));
            }
        }
        Ok(())
    }

    /// Every name the generated modules export, so `index.ts` re-exports
    /// cannot collide.
    fn check_exports(&self) -> GenResult<()> {
        let prefix = pascal(&self.surface.name);
        let mut names: Vec<String> = [
            "KIND",
            "KindName",
            "FIELD_TYPES",
            "CORRECTIONS",
            "Input",
            "EVENT",
            "Events",
            "EVENTS",
            "AnyEvent",
            "decodeEvent",
            "onEvent",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        names.push(format!("create{prefix}Client"));
        names.push(format!("{prefix}Client"));
        names.push(format!("{prefix}ClientOptions"));
        names.push(format!("{prefix}PredictionOptions"));
        for kind in &self.surface.kinds {
            names.push(format!("{}Fields", pascal(&kind.name)));
            names.push(format!("is{}", pascal(&kind.name)));
            names.push(format!("{}Fields", camel(&kind.name)));
        }
        if let Some(input) = &self.surface.input {
            names.extend(
                [
                    "BUTTON",
                    "INPUT_LAYOUT",
                    "encodeInput",
                    "decodeInput",
                    "sanitizeInput",
                ]
                .map(String::from),
            );
            names.push(input.name.clone());
        }
        for record in &self.surface.records {
            names.push(record.name.clone());
        }
        for event in &self.surface.events {
            names.push(format!("{}Event", pascal(&event.name)));
        }
        if !self.surface.pures.is_empty() {
            names.extend(["PURES", "PURES_WASM_URL"].map(String::from));
            names.push(format!("create{prefix}Pures"));
            names.push(format!("{prefix}Pures"));
            names.push(format!("{prefix}PureFns"));
        }
        for c in &self.surface.constants {
            names.push(constant(&c.name));
        }
        for codec in &self.surface.codecs {
            names.push(format!("encode{codec}"));
            names.push(format!("decode{codec}"));
        }
        if self.surface.predict.is_some() {
            names.extend(
                [
                    "PREDICT",
                    "PREDICT_STATE",
                    "PREDICT_CONTEXT",
                    "PREDICT_WASM_URL",
                    "PredictContext",
                ]
                .map(String::from),
            );
            names.push(format!("create{prefix}Prediction"));
            names.push(format!("{prefix}Prediction"));
        }
        let mut seen = BTreeSet::new();
        for name in names {
            if !seen.insert(name.clone()) {
                return Err(format!(
                    "generated export `{name}` would be declared twice; rename the record, input, kind or event"
                ));
            }
        }
        Ok(())
    }

    fn check_fields(&self, fields: &[Field], owner: &str) -> GenResult<()> {
        let mut props = BTreeSet::new();
        for field in fields {
            if !props.insert(camel(&field.name)) || camel(&field.name).is_empty() {
                return Err(format!(
                    "{owner}: field `{}` is empty or collides with another as `{}`",
                    field.name,
                    camel(&field.name)
                ));
            }
            self.check_type(&field.ty, owner)?;
        }
        Ok(())
    }

    fn check_type(&self, ty: &Wire, owner: &str) -> GenResult<()> {
        match ty {
            Wire::List { item } | Wire::Optional { item } => self.check_type(item, owner),
            Wire::Record { name } if !self.records.contains_key(name.as_str()) => {
                Err(format!("{owner}: unknown record {name}"))
            }
            _ => Ok(()),
        }
    }

    /// Records in dependency order (a record after every record it uses).
    fn record_order(&self) -> GenResult<Vec<&'a Record>> {
        fn deps(ty: &Wire, out: &mut Vec<String>) {
            match ty {
                Wire::List { item } | Wire::Optional { item } => deps(item, out),
                Wire::Record { name } => out.push(name.clone()),
                _ => {}
            }
        }
        let mut order = Vec::new();
        let mut done = BTreeSet::new();
        let mut visiting = BTreeSet::new();
        fn visit<'r>(
            name: &str,
            records: &BTreeMap<&'r str, &'r Record>,
            done: &mut BTreeSet<String>,
            visiting: &mut BTreeSet<String>,
            order: &mut Vec<&'r Record>,
        ) -> GenResult<()> {
            if done.contains(name) {
                return Ok(());
            }
            if !visiting.insert(name.to_string()) {
                return Err(format!(
                    "record {name} is recursive; recursive records are not supported"
                ));
            }
            let record = records[name];
            let mut needs = Vec::new();
            for field in &record.fields {
                deps(&field.ty, &mut needs);
            }
            for dep in needs {
                visit(&dep, records, done, visiting, order)?;
            }
            visiting.remove(name);
            done.insert(name.to_string());
            order.push(record);
            Ok(())
        }
        for name in self.records.keys() {
            visit(name, &self.records, &mut done, &mut visiting, &mut order)?;
        }
        Ok(order)
    }

    fn header(&self, what: &str) -> String {
        format!(
            "// @generated by `distributed game-client` from game surface `{}` (schema v{}, protocol v{}).\n// {what}\n// Do not edit: change the Rust definitions and regenerate.\n",
            self.surface.name,
            self.surface.schema_version,
            self.surface.protocol_version
        )
    }

    fn local(&self, module: &str) -> String {
        format!("./{module}{}", self.ext.suffix())
    }

    fn ts_type(&self, ty: &Wire) -> String {
        match ty {
            Wire::Bool => "boolean".into(),
            Wire::ShortStr | Wire::Str => "string".into(),
            Wire::Bytes => "Uint8Array".into(),
            Wire::List { item } => {
                let inner = self.ts_type(item);
                if inner.contains(' ') {
                    format!("readonly ({inner})[]")
                } else {
                    format!("readonly {inner}[]")
                }
            }
            Wire::Optional { item } => format!("{} | undefined", self.ts_type(item)),
            Wire::Record { name } => name.clone(),
            _ => "number".into(),
        }
    }

    /// A `WireType` literal; records refer to their `*_TYPE` constants.
    fn wire_literal(&self, ty: &Wire) -> String {
        match ty {
            Wire::Quant { min, max, bits } => format!(
                "{{ type: 'quant', min: {}, max: {}, bits: {bits} }}",
                f32_exact(*min),
                f32_exact(*max)
            ),
            Wire::Angle { bits } => format!("{{ type: 'angle', bits: {bits} }}"),
            Wire::List { item } => format!("{{ type: 'list', item: {} }}", self.wire_literal(item)),
            Wire::Optional { item } => {
                format!("{{ type: 'optional', item: {} }}", self.wire_literal(item))
            }
            Wire::Record { name } => format!("{}_TYPE", constant(name)),
            other => format!("{{ type: '{}' }}", simple_tag(other)),
        }
    }

    fn layout(&self, fields: &[Field]) -> String {
        if fields.is_empty() {
            return "[]".into();
        }
        let mut out = String::from("[\n");
        for field in fields {
            let _ = writeln!(
                out,
                "\t[{}, {}],",
                string(&camel(&field.name)),
                self.wire_literal(&field.ty)
            );
        }
        out.push(']');
        out
    }

    fn interface(&self, name: &str, doc: &str, fields: &[Field]) -> String {
        let mut out = format!("/** {doc} */\nexport interface {name} {{\n");
        for field in fields {
            let _ = writeln!(
                out,
                "\treadonly {}: {};",
                key(&camel(&field.name)),
                self.ts_type(&field.ty)
            );
        }
        out.push_str("}\n");
        out
    }

    fn schema_ts(&self) -> GenResult<String> {
        let kinds = &self.surface.kinds;
        let mut readers = BTreeSet::new();
        for kind in kinds {
            for field in &kind.fields {
                readers.insert(reader(&field.ty));
            }
        }
        let mut imports: Vec<String> = readers.into_iter().map(str::to_string).collect();
        imports.push("type CorrectionPolicies".into());
        imports.push("type EntityFields".into());
        imports.push("type WireType".into());
        let mut out =
            self.header("Replicated kinds: ids, typed fields, wire types, correction defaults.");
        let _ = writeln!(out, "import {{ {} }} from '{SDK}';\n", imports.join(", "));

        out.push_str("/** Replicated kind ids by name. */\nexport const KIND = {\n");
        for kind in kinds {
            let _ = writeln!(out, "\t{}: {},", key(&camel(&kind.name)), kind.id);
        }
        out.push_str("} as const;\n\nexport type KindName = keyof typeof KIND;\n");

        for kind in kinds {
            let pascal_name = pascal(&kind.name);
            let camel_name = camel(&kind.name);
            let _ = write!(
                out,
                "\n/** `{}` (kind {}, priority {}). */\nexport interface {pascal_name}Fields {{\n",
                kind.name,
                kind.id,
                f32_literal(kind.priority)
            );
            for field in &kind.fields {
                let doc = field_doc(&field.ty, field.priority);
                let _ = writeln!(
                    out,
                    "\t/** {doc} */\n\treadonly {}: {};",
                    key(&camel(&field.name)),
                    self.ts_type(&field.ty)
                );
            }
            out.push_str("}\n\n");
            let _ = writeln!(
                out,
                "/** True for a `{}` entity. */\nexport function is{pascal_name}(entity: {{ readonly kind: number }}): boolean {{\n\treturn entity.kind === {};\n}}\n",
                kind.name, kind.id
            );
            let _ = writeln!(
                out,
                "/** Typed fields of a `{}` entity (rendered or replica). */\nexport function {camel_name}Fields(entity: {{ readonly fields: EntityFields }}): {pascal_name}Fields {{\n\tconst f = entity.fields;\n\treturn {{",
                kind.name
            );
            for field in &kind.fields {
                let _ = writeln!(
                    out,
                    "\t\t{}: {}(f, {}),",
                    key(&camel(&field.name)),
                    reader(&field.ty),
                    string(&field.name)
                );
            }
            out.push_str("\t};\n}\n");
        }

        out.push_str("\n/** Wire type of every replicated field, by kind and wire field name. */\nexport const FIELD_TYPES = {\n");
        for kind in kinds {
            let _ = writeln!(out, "\t{}: {{", key(&camel(&kind.name)));
            for field in &kind.fields {
                let _ = writeln!(
                    out,
                    "\t\t{}: {},",
                    key(&field.name),
                    self.wire_literal(&field.ty)
                );
            }
            out.push_str("\t},\n");
        }
        out.push_str("} as const satisfies Record<string, Record<string, WireType>>;\n");

        out.push_str("\n/** Client correction defaults (`#[correct(..)]`), by kind and wire field name. */\nexport const CORRECTIONS: CorrectionPolicies = {\n");
        for kind in kinds {
            let corrected: Vec<_> = kind
                .fields
                .iter()
                .filter_map(|f| f.correction.as_ref().map(|c| (f, c)))
                .collect();
            if corrected.is_empty() {
                continue;
            }
            let _ = writeln!(out, "\t{}: {{", key(&kind.name));
            for (field, correction) in corrected {
                let policy = match correction {
                    Correction::Ease { duration_ms } => {
                        format!("{{ mode: 'ease', durationMs: {duration_ms} }}")
                    }
                    Correction::Snap => "{ mode: 'snap' }".into(),
                    Correction::Replace => "{ mode: 'replace' }".into(),
                };
                let _ = writeln!(out, "\t\t{}: {policy},", key(&field.name));
            }
            out.push_str("\t},\n");
        }
        out.push_str("};\n");
        Ok(out)
    }

    fn input_ts(&self) -> GenResult<String> {
        let mut out =
            self.header("Input frame: interface, button bits, encoder, decoder, sanitize.");
        let Some(input) = &self.surface.input else {
            out.push_str("\n/** This zone takes no typed input. */\nexport type Input = never;\n");
            return Ok(out);
        };
        let can_reject = input.fields.iter().any(|f| f.ty.is_float());
        let _ = writeln!(
            out,
            "import {{ decodePayload, encodePayload, sanitizePayload, type InputRule, type PayloadLayout }} from '{SDK}';\n"
        );
        out.push_str(&self.interface(
            &input.name,
            &format!("One `{}` input frame (`#[derive(GameInput)]`).", input.name),
            &input.fields,
        ));
        let _ = writeln!(out, "\nexport type Input = {};", input.name);
        let flag_fields: Vec<&Field> = input
            .fields
            .iter()
            .filter(|f| !f.buttons.is_empty())
            .collect();
        if !flag_fields.is_empty() {
            out.push_str("\n/** Bits of each bit-flag field (`#[buttons(..)]`), by field. */\nexport const BITS = {\n");
            for field in &flag_fields {
                let _ = writeln!(out, "\t{}: {{", key(&camel(&field.name)));
                for (bit, name) in field.buttons.iter().enumerate() {
                    let _ = writeln!(out, "\t\t{}: {},", key(&camel(name)), 1u32 << bit);
                }
                out.push_str("\t},\n");
            }
            out.push_str("} as const;\n");
            if let [only] = flag_fields.as_slice() {
                let _ = writeln!(
                    out,
                    "\n/** Bits of `{}`, the only bit-flag field. */\nexport const BUTTON = BITS.{};",
                    camel(&only.name),
                    camel(&only.name)
                );
            }
        }
        let _ = writeln!(
            out,
            "\n/** Wire layout, in field order. */\nexport const INPUT_LAYOUT: PayloadLayout = {};\n",
            self.layout(&input.fields)
        );
        out.push_str("const INPUT_RULES: readonly InputRule[] = [\n");
        for field in &input.fields {
            let rule = if !field.buttons.is_empty() {
                let mask: u64 = (1u64 << field.buttons.len()) - 1;
                format!("{{ buttons: {mask} }}")
            } else if let Some([min, max]) = field.range {
                let (min, max) = if field.ty.is_integer() {
                    (min.trunc(), max.trunc())
                } else {
                    (min, max)
                };
                format!("{{ range: [{}, {}] }}", f64_literal(min), f64_literal(max))
            } else {
                "undefined".into()
            };
            let _ = writeln!(out, "\t{rule},");
        }
        out.push_str("];\n\n");
        let name = &input.name;
        let _ = writeln!(
            out,
            "/** Payload bytes of one input frame. */\nexport function encodeInput(input: {name}): Uint8Array {{\n\treturn encodePayload(INPUT_LAYOUT, input);\n}}\n"
        );
        let _ = writeln!(
            out,
            "/** Decode one input frame (no sanitizing). */\nexport function decodeInput(payload: Uint8Array): {name} {{\n\treturn decodePayload(INPUT_LAYOUT, payload) as unknown as {name};\n}}\n"
        );
        if can_reject {
            let _ = writeln!(
                out,
                "/**\n * What the zone simulates for `input` (`#[range]` clamps, `#[buttons]` masks,\n * wire precision); `undefined` when the server would drop it (non-finite float).\n */\nexport function sanitizeInput(input: {name}): {name} | undefined {{\n\treturn sanitizePayload(INPUT_LAYOUT, INPUT_RULES, input);\n}}"
            );
        } else {
            let _ = writeln!(
                out,
                "/** What the zone simulates for `input` (`#[range]` clamps, `#[buttons]` masks, wire precision). */\nexport function sanitizeInput(input: {name}): {name} {{\n\treturn sanitizePayload(INPUT_LAYOUT, INPUT_RULES, input)!;\n}}"
            );
        }
        Ok(out)
    }

    fn events_ts(&self) -> GenResult<String> {
        let mut out = self
            .header("Events and records: payload interfaces, ids, codecs, typed subscriptions.");
        let events = &self.surface.events;
        let records = self.record_order()?;
        let mut sdk = vec![
            "decodeGameEvent",
            "onGameEvent",
            "type DecodedGameEvent",
            "type GameEventCodecs",
            "type GameEventSource",
        ];
        let messages = &self.surface.messages;
        let uses_fields = events
            .iter()
            .chain(messages)
            .any(|e| matches!(e.payload, Payload::Fields { .. }));
        let uses_value = events
            .iter()
            .chain(messages)
            .any(|e| matches!(e.payload, Payload::Value { .. }));
        if uses_fields {
            sdk.extend(["decodePayload", "encodePayload", "type PayloadLayout"]);
        }
        let has_codecs = !self.surface.codecs.is_empty();
        if uses_value || has_codecs {
            sdk.extend(["decodeValue", "encodeValue"]);
        }
        if uses_value
            || has_codecs
            || records
                .iter()
                .any(|r| self.encoded_records().contains(r.name.as_str()))
        {
            sdk.push("type WireType");
        }
        sdk.sort_by_key(|s| (s.starts_with("type "), s.to_string()));
        let _ = writeln!(out, "import {{ {} }} from '{SDK}';", sdk.join(", "));

        let encoded = self.encoded_records();
        for record in &records {
            out.push('\n');
            out.push_str(&self.interface(
                &record.name,
                &format!("Record `{}` (`#[derive(GameRecord)]`).", record.name),
                &record.fields,
            ));
            // Records only the predicted step uses (its state and context)
            // are encoded by `predict.ts` layouts, not by an event codec.
            if encoded.contains(record.name.as_str()) {
                let _ = writeln!(
                    out,
                    "\nconst {}_TYPE: WireType = {{\n\ttype: 'record',\n\tfields: {}\n}};",
                    constant(&record.name),
                    indent(&self.layout(&record.fields))
                );
            }
        }

        for codec in &self.surface.codecs {
            let c = constant(codec);
            let _ = writeln!(
                out,
                "\n/** Encode a `{codec}` record (standalone codec, e.g. served over HTTP). */\nexport function encode{codec}(value: {codec}): Uint8Array {{\n\treturn encodeValue({c}_TYPE, value);\n}}\n\n/** Decode a whole `{codec}` record (throws on trailing or missing bytes). */\nexport function decode{codec}(bytes: Uint8Array): {codec} {{\n\treturn decodeValue({c}_TYPE, bytes) as {codec};\n}}"
            );
        }

        self.payload_types(&mut out, events, "Event", "");
        if !messages.is_empty() {
            self.payload_types(&mut out, messages, "Message", "MESSAGE_");
        }

        out.push_str("\n/** Event type ids by name. */\nexport const EVENT = {\n");
        for event in events {
            let _ = writeln!(out, "\t{}: {},", key(&camel(&event.name)), event.id);
        }
        out.push_str(
            "} as const;\n\n/** Event payload types by name. */\nexport interface Events {\n",
        );
        for event in events {
            let _ = writeln!(
                out,
                "\t{}: {}Event;",
                key(&camel(&event.name)),
                pascal(&event.name)
            );
        }
        out.push_str("}\n\n/** Codecs of every event. */\nexport const EVENTS: GameEventCodecs<Events> = {\n");
        for event in events {
            let type_name = format!("{}Event", pascal(&event.name));
            let c = constant(&event.name);
            let (encode, decode) = match &event.payload {
                Payload::Fields { .. } => (
                    format!("(value) => encodePayload({c}_LAYOUT, value)"),
                    format!(
                        "(payload) => decodePayload({c}_LAYOUT, payload) as unknown as {type_name}"
                    ),
                ),
                Payload::Value { .. } => (
                    format!("(value) => encodeValue({c}_PAYLOAD, value)"),
                    format!("(payload) => decodeValue({c}_PAYLOAD, payload) as {type_name}"),
                ),
            };
            let _ = writeln!(
                out,
                "\t{}: {{\n\t\tid: {},\n\t\tname: {},\n\t\tencode: {encode},\n\t\tdecode: {decode}\n\t}},",
                key(&camel(&event.name)),
                event.id,
                string(&event.name)
            );
        }
        out.push_str("};\n\n");
        out.push_str("/** A decoded event, discriminated by `name`. */\nexport type AnyEvent = DecodedGameEvent<Events>;\n\n");
        out.push_str("/** Decode an Event message (`undefined` for ids this surface does not declare). */\nexport function decodeEvent(event: { readonly typeId: number; readonly payload: Uint8Array }): AnyEvent | undefined {\n\treturn decodeGameEvent(EVENTS, event);\n}\n\n");
        out.push_str("/** Subscribe to one typed event on every zone of `client`; returns the unsubscribe. */\nexport function onEvent<K extends keyof Events>(\n\tclient: GameEventSource,\n\tname: K,\n\thandler: (value: Events[K], zoneId: string) => void\n): () => void {\n\treturn onGameEvent(client, EVENTS, name, handler);\n}\n");
        if !messages.is_empty() {
            self.messages_ts(&mut out, messages);
        }
        Ok(out)
    }

    /// Zone inbox message ids, codecs and `encodeMessage` (the body of a
    /// `/admin/zone-message` `message`).
    fn messages_ts(&self, out: &mut String, messages: &[Event]) {
        out.push_str("\n/** Zone inbox message type ids by name. */\nexport const MESSAGE = {\n");
        for m in messages {
            let _ = writeln!(out, "\t{}: {},", key(&camel(&m.name)), m.id);
        }
        out.push_str("} as const;\n\n/** Zone inbox message payload types by name. */\nexport interface Messages {\n");
        for m in messages {
            let _ = writeln!(
                out,
                "\t{}: {}Message;",
                key(&camel(&m.name)),
                pascal(&m.name)
            );
        }
        out.push_str("}\n\n/** Codecs of every zone inbox message. */\nexport const MESSAGES: GameEventCodecs<Messages> = {\n");
        for m in messages {
            let type_name = format!("{}Message", pascal(&m.name));
            let c = format!("MESSAGE_{}", constant(&m.name));
            let (encode, decode) = match &m.payload {
                Payload::Fields { .. } => (
                    format!("(value) => encodePayload({c}_LAYOUT, value)"),
                    format!(
                        "(payload) => decodePayload({c}_LAYOUT, payload) as unknown as {type_name}"
                    ),
                ),
                Payload::Value { .. } => (
                    format!("(value) => encodeValue({c}_PAYLOAD, value)"),
                    format!("(payload) => decodeValue({c}_PAYLOAD, payload) as {type_name}"),
                ),
            };
            let _ = writeln!(
                out,
                "\t{}: {{\n\t\tid: {},\n\t\tname: {},\n\t\tencode: {encode},\n\t\tdecode: {decode}\n\t}},",
                key(&camel(&m.name)),
                m.id,
                string(&m.name)
            );
        }
        out.push_str("};\n\n/** Encode a zone inbox message: `typeId` and `payload` of a `/admin/zone-message` body (base64url the payload). */\nexport function encodeMessage<K extends keyof Messages>(\n\tname: K,\n\tvalue: Messages[K]\n): { readonly typeId: number; readonly payload: Uint8Array } {\n\tconst codec = MESSAGES[name];\n\treturn { typeId: codec.id, payload: codec.encode(value) };\n}\n");
    }

    /// Payload interfaces and layouts of events (`suffix` `Event`) or zone
    /// inbox messages (`Message`, constants prefixed `MESSAGE_`).
    fn payload_types(&self, out: &mut String, items: &[Event], suffix: &str, prefix: &str) {
        for event in items {
            let type_name = format!("{}{suffix}", pascal(&event.name));
            out.push('\n');
            match &event.payload {
                Payload::Fields { fields } if fields.is_empty() => {
                    let _ = writeln!(
                            out,
                            "/** {suffix} `{}` (id {}): no payload. */\nexport type {type_name} = Readonly<Record<string, never>>;",
                            event.name, event.id
                        );
                    let _ = writeln!(
                        out,
                        "\nconst {prefix}{}_LAYOUT: PayloadLayout = [];",
                        constant(&event.name)
                    );
                }
                Payload::Fields { fields } => {
                    out.push_str(&self.interface(
                        &type_name,
                        &format!("{suffix} `{}` (id {}).", event.name, event.id),
                        fields,
                    ));
                    let _ = writeln!(
                        out,
                        "\nconst {prefix}{}_LAYOUT: PayloadLayout = {};",
                        constant(&event.name),
                        self.layout(fields)
                    );
                }
                Payload::Value { ty } => {
                    let _ = writeln!(
                        out,
                        "/** {suffix} `{}` (id {}). */\nexport type {type_name} = {};",
                        event.name,
                        event.id,
                        self.ts_type(ty)
                    );
                    let _ = writeln!(
                        out,
                        "\nconst {prefix}{}_PAYLOAD: WireType = {};",
                        constant(&event.name),
                        self.wire_literal(ty)
                    );
                }
            }
        }
    }

    /// Records an event payload (or the input) encodes, directly or nested.
    fn encoded_records(&self) -> BTreeSet<&'a str> {
        fn walk<'r>(ty: &'r Wire, out: &mut Vec<&'r str>) {
            match ty {
                Wire::List { item } | Wire::Optional { item } => walk(item, out),
                Wire::Record { name } => out.push(name),
                _ => {}
            }
        }
        let mut pending = Vec::new();
        for event in self.surface.events.iter().chain(&self.surface.messages) {
            match &event.payload {
                Payload::Fields { fields } => fields.iter().for_each(|f| walk(&f.ty, &mut pending)),
                Payload::Value { ty } => walk(ty, &mut pending),
            }
        }
        if let Some(input) = &self.surface.input {
            input.fields.iter().for_each(|f| walk(&f.ty, &mut pending));
        }
        pending.extend(self.surface.codecs.iter().map(String::as_str));
        let mut seen = BTreeSet::new();
        while let Some(name) = pending.pop() {
            if seen.insert(name) {
                if let Some(record) = self.records.get(name) {
                    record.fields.iter().for_each(|f| walk(&f.ty, &mut pending));
                }
            }
        }
        seen
    }

    fn client_ts(&self) -> String {
        let prefix = pascal(&self.surface.name);
        let mut out = self.header("Typed client factory.");
        let input = self.surface.input.as_ref().map(|i| i.name.as_str());
        match input {
            Some(name) if self.surface.predict.is_some() => {
                out.push_str(&self.predicted_client_ts(name));
            }
            Some(name) => {
                let _ = writeln!(
                    out,
                    "import {{ GameClient, mergeCorrections, type GameClientOptions, type GamePredictionOptions }} from '{SDK}';\n\nimport {{ EVENTS, type Events }} from '{}';\nimport {{ encodeInput, type {name} }} from '{}';\nimport {{ CORRECTIONS }} from '{}';\n",
                    self.local("events"),
                    self.local("input"),
                    self.local("schema")
                );
                let _ = writeln!(
                    out,
                    "/** Prediction options; `encodeInput` and the correction defaults are filled in. */\nexport type {prefix}PredictionOptions = Omit<GamePredictionOptions<{name}>, 'encodeInput'> &\n\tPartial<Pick<GamePredictionOptions<{name}>, 'encodeInput'>>;\n\nexport type {prefix}ClientOptions = Omit<GameClientOptions<{name}>, 'prediction'> &\n\tReadonly<{{ prediction?: {prefix}PredictionOptions }}>;\n"
                );
                let _ = writeln!(
                    out,
                    "export type {prefix}Client = Readonly<{{\n\tclient: GameClient<{name}>;\n\t/** Typed event subscription (every zone); returns the unsubscribe. */\n\ton<K extends keyof Events>(name: K, handler: (value: Events[K], zoneId: string) => void): () => void;\n\t/** Send one input: predicted when prediction is configured, raw otherwise. */\n\tsend(input: {name}): number | undefined;\n}}>;\n"
                );
                let _ = writeln!(
                    out,
                    "/** A `GameClient` for the `{surface}` surface with its input encoder and correction defaults. */\nexport function create{prefix}Client(options: {prefix}ClientOptions): {prefix}Client {{\n\tconst prediction = options.prediction;\n\tconst client = new GameClient<{name}>({{\n\t\t...options,\n\t\tprediction:\n\t\t\tprediction === undefined\n\t\t\t\t? undefined\n\t\t\t\t: {{\n\t\t\t\t\t\t...prediction,\n\t\t\t\t\t\tencodeInput: prediction.encodeInput ?? ((input) => encodeInput(input)),\n\t\t\t\t\t\tcorrections: mergeCorrections(CORRECTIONS, prediction.corrections)\n\t\t\t\t\t}}\n\t}});\n\treturn {{\n\t\tclient,\n\t\ton: (name, handler) =>\n\t\t\tclient.on('event', (zoneId, event) => {{\n\t\t\t\tconst codec = EVENTS[name];\n\t\t\t\tif (event.typeId === codec.id) handler(codec.decode(event.payload), zoneId);\n\t\t\t}}),\n\t\tsend: (input) =>\n\t\t\tprediction === undefined ? client.sendRawInput(encodeInput(input)) : client.sendInput(input)\n\t}};\n}}",
                    surface = self.surface.name
                );
            }
            None => {
                let _ = writeln!(
                    out,
                    "import {{ GameClient, type GameClientOptions }} from '{SDK}';\n\nimport {{ EVENTS, type Events }} from '{}';\n",
                    self.local("events")
                );
                let _ = writeln!(
                    out,
                    "export type {prefix}ClientOptions = Omit<GameClientOptions<never>, 'prediction'>;\n\nexport type {prefix}Client = Readonly<{{\n\tclient: GameClient<never>;\n\t/** Typed event subscription (every zone); returns the unsubscribe. */\n\ton<K extends keyof Events>(name: K, handler: (value: Events[K], zoneId: string) => void): () => void;\n}}>;\n"
                );
                let _ = writeln!(
                    out,
                    "/** A `GameClient` for the `{surface}` surface (no typed input). */\nexport function create{prefix}Client(options: {prefix}ClientOptions): {prefix}Client {{\n\tconst client = new GameClient<never>(options);\n\treturn {{\n\t\tclient,\n\t\ton: (name, handler) =>\n\t\t\tclient.on('event', (zoneId, event) => {{\n\t\t\t\tconst codec = EVENTS[name];\n\t\t\t\tif (event.typeId === codec.id) handler(codec.decode(event.payload), zoneId);\n\t\t\t}})\n\t}};\n}}",
                    surface = self.surface.name
                );
            }
        }
        out
    }

    /// `client.ts` of a surface with a `#[predict]` step: prediction defaults
    /// to the compiled WebAssembly step.
    fn predicted_client_ts(&self, name: &str) -> String {
        let prefix = pascal(&self.surface.name);
        let mut out = String::new();
        let _ = writeln!(
            out,
            "import {{ GameClient, mergeCorrections, type GameClientOptions, type GamePredictionOptions, type WasmSource }} from '{SDK}';\n\nimport {{ EVENTS, type Events }} from '{}';\nimport {{ encodeInput, type {name} }} from '{}';\nimport {{ create{prefix}Prediction, type {prefix}Prediction }} from '{}';\nimport {{ CORRECTIONS }} from '{}';\n",
            self.local("events"),
            self.local("input"),
            self.local("predict"),
            self.local("schema")
        );
        let _ = writeln!(
            out,
            "/**\n * Prediction tuning; `encodeInput`, the correction defaults and `predict` (the\n * compiled `#[predict]` step) are filled in. A `predict` given here replaces\n * the WebAssembly step.\n */\nexport type {prefix}PredictionOptions = Omit<GamePredictionOptions<{name}>, 'encodeInput' | 'predict'> &\n\tPartial<Pick<GamePredictionOptions<{name}>, 'encodeInput' | 'predict'>>;\n\nexport type {prefix}ClientOptions = Omit<GameClientOptions<{name}>, 'prediction'> &\n\tReadonly<{{\n\t\t/** Prediction tuning, or `false` for none. Default: the compiled step. */\n\t\tprediction?: {prefix}PredictionOptions | false;\n\t\t/** Where to load the compiled step from (default `PREDICT_WASM_URL`). */\n\t\twasm?: WasmSource;\n\t}}>;\n"
        );
        let _ = writeln!(
            out,
            "export type {prefix}Client = Readonly<{{\n\tclient: GameClient<{name}>;\n\t/** The compiled step (`ready`, `setContext`); undefined when prediction is off or custom. */\n\tprediction: {prefix}Prediction | undefined;\n\t/** Typed event subscription (every zone); returns the unsubscribe. */\n\ton<K extends keyof Events>(name: K, handler: (value: Events[K], zoneId: string) => void): () => void;\n\t/** Send one input: predicted when prediction is on, raw otherwise. */\n\tsend(input: {name}): number | undefined;\n}}>;\n"
        );
        let _ = write!(
            out,
            "/**\n * A `GameClient` for the `{surface}` surface with its input encoder, correction\n * defaults and the compiled predicted step wired in.\n */\nexport function create{prefix}Client(options: {prefix}ClientOptions): {prefix}Client {{\n\tconst {{ prediction: tuning, wasm, ...rest }} = options;\n\tconst prediction =\n\t\ttuning === false || tuning?.predict !== undefined ? undefined : create{prefix}Prediction(wasm);\n\tconst client = new GameClient<{name}>({{\n\t\t...rest,\n\t\tprediction:\n\t\t\ttuning === false\n\t\t\t\t? undefined\n\t\t\t\t: {{\n\t\t\t\t\t\t...tuning,\n\t\t\t\t\t\tpredict: tuning?.predict ?? prediction!.predict,\n\t\t\t\t\t\tencodeInput: tuning?.encodeInput ?? ((input) => encodeInput(input)),\n\t\t\t\t\t\tcorrections: mergeCorrections(CORRECTIONS, tuning?.corrections)\n\t\t\t\t\t}}\n\t}});\n\treturn {{\n\t\tclient,\n\t\tprediction,\n\t\ton: (name, handler) =>\n\t\t\tclient.on('event', (zoneId, event) => {{\n\t\t\t\tconst codec = EVENTS[name];\n\t\t\t\tif (event.typeId === codec.id) handler(codec.decode(event.payload), zoneId);\n\t\t\t}}),\n\t\tsend: (input) =>\n\t\t\ttuning === false ? client.sendRawInput(encodeInput(input)) : client.sendInput(input)\n\t}};\n}}\n",
            surface = self.surface.name
        );
        out
    }

    fn index_ts(&self) -> String {
        let mut out = self.header("Everything generated for this surface.");
        let mut modules = vec!["client"];
        if !self.surface.constants.is_empty() {
            modules.push("constants");
        }
        modules.extend(["events", "input"]);
        if self.surface.predict.is_some() {
            modules.push("predict");
        }
        if !self.surface.pures.is_empty() {
            modules.push("pures");
        }
        modules.push("schema");
        for module in modules {
            let _ = writeln!(out, "export * from '{}';", self.local(module));
        }
        out
    }

    fn predict_ts(&self) -> GenResult<Option<String>> {
        let Some(predict) = &self.surface.predict else {
            return Ok(None);
        };
        let input = self
            .surface
            .input
            .as_ref()
            .ok_or("a predicted step needs the zone's input type")?;
        let state = self.records[predict.state.as_str()];
        let prefix = pascal(&self.surface.name);
        let mut out = self.header(
            "Client-predicted step: the Rust `#[predict]` fn compiled to WebAssembly (binary ABI).",
        );
        let mut sdk = vec![
            "createWasmPrediction",
            "type PredictStateLayout",
            "type WasmPrediction",
            "type WasmSource",
        ];
        if predict.context.is_some() {
            sdk.push("type PayloadLayout");
        }
        let _ = writeln!(out, "import {{ {} }} from '{SDK}';\n", sdk.join(", "));
        if let Some(context) = &predict.context {
            let _ = writeln!(
                out,
                "import type {{ {context} }} from '{}';",
                self.local("events")
            );
        }
        let _ = writeln!(
            out,
            "import {{ encodeInput, type {} }} from '{}';\n",
            input.name,
            self.local("input")
        );
        let _ = writeln!(
            out,
            "/** The predicted step: `#[predict]` fn `{}` of Cargo package `{}`, predicting kind `{}`. */\nexport const PREDICT = {{\n\tkind: {},\n\texport: {},\n\tpackage: {},\n\tstate: {},\n\tabi: {},\n}} as const;\n",
            predict.export,
            predict.package,
            predict.kind,
            string(&predict.kind),
            string(&predict.export),
            string(&predict.package),
            string(&predict.state),
            predict.abi
        );
        let _ = writeln!(
            out,
            "/** Predicted `{}` fields (record `{}`): wire field name and codec type, in record order. */\nexport const PREDICT_STATE: PredictStateLayout = [",
            predict.kind, state.name
        );
        for field in &state.fields {
            let _ = writeln!(
                out,
                "\t[{}, {}],",
                string(&field.name),
                self.wire_literal(&field.ty)
            );
        }
        out.push_str("];\n\n");
        match &predict.context {
            Some(context) => {
                let record = self.records[context.as_str()];
                let _ = writeln!(
                    out,
                    "/** What the step reads besides state and input (`prediction.setContext`). */\nexport type PredictContext = {context};\n\n/** Layout of the context record `{context}`. */\nexport const PREDICT_CONTEXT: PayloadLayout | undefined = {};\n",
                    self.layout(&record.fields)
                );
            }
            None => {
                out.push_str("/** The step takes no context. */\nexport type PredictContext = undefined;\n\n/** The step takes no context. */\nexport const PREDICT_CONTEXT = undefined;\n\n");
            }
        }
        let _ = writeln!(
            out,
            "/** The compiled step (`distributed game-wasm`); bundlers emit it as an asset. */\nexport const PREDICT_WASM_URL: URL = new URL({}, import.meta.url);\n",
            string(&format!("./{}", predict_wasm_path(&self.surface.name)))
        );
        let _ = writeln!(
            out,
            "/** The `{surface}` step as `GameClient` prediction (see `createWasmPrediction`). */\nexport type {prefix}Prediction = WasmPrediction<{input}, PredictContext>;\n",
            surface = self.surface.name,
            input = input.name
        );
        let _ = writeln!(
            out,
            "/**\n * Load the compiled step (default `PREDICT_WASM_URL`) as prediction: `predict`\n * leaves the state unchanged until `ready` resolves{}.\n */\nexport function create{prefix}Prediction(source: WasmSource = PREDICT_WASM_URL): {prefix}Prediction {{\n\treturn createWasmPrediction<{input}, PredictContext>({{\n\t\tsource,\n\t\texportName: PREDICT.export,\n\t\tstate: PREDICT_STATE,\n\t\tcontext: PREDICT_CONTEXT,\n\t\tencodeInput\n\t}});\n}}",
            if predict.context.is_some() {
                " and a context is set"
            } else {
                ""
            },
            input = input.name
        );
        Ok(Some(out))
    }
}

impl Gen<'_> {
    /// Records reachable from `roots` (nested ones included).
    fn reachable<'t>(&self, roots: impl IntoIterator<Item = &'t Wire>) -> BTreeSet<&str> {
        let mut pending = Vec::new();
        for root in roots {
            top_records(root, &mut pending);
        }
        let mut seen = BTreeSet::new();
        while let Some(name) = pending.pop() {
            if let Some((name, record)) = self.records.get_key_value(name.as_str()) {
                if seen.insert(*name) {
                    for field in &record.fields {
                        top_records(&field.ty, &mut pending);
                    }
                }
            }
        }
        seen
    }

    /// `const <RECORD>_TYPE: WireType` for `names`, dependencies first.
    fn type_consts(&self, names: &BTreeSet<&str>) -> GenResult<String> {
        let mut out = String::new();
        for record in self.record_order()? {
            if names.contains(record.name.as_str()) {
                let _ = writeln!(
                    out,
                    "const {}_TYPE: WireType = {{\n\ttype: 'record',\n\tfields: {}\n}};\n",
                    constant(&record.name),
                    indent(&self.layout(&record.fields))
                );
            }
        }
        Ok(out)
    }

    /// `import type { A, B } from './events'` for the records `types`
    /// mention directly.
    fn import_records<'t>(&self, types: impl IntoIterator<Item = &'t Wire>) -> String {
        let mut names = Vec::new();
        for ty in types {
            top_records(ty, &mut names);
        }
        let names: BTreeSet<String> = names.into_iter().collect();
        if names.is_empty() {
            return String::new();
        }
        format!(
            "import type {{ {} }} from '{}';\n",
            names.into_iter().collect::<Vec<_>>().join(", "),
            self.local("events")
        )
    }

    fn pures_ts(&self) -> GenResult<Option<String>> {
        let pures = &self.surface.pures;
        let Some(first) = pures.first() else {
            return Ok(None);
        };
        let prefix = pascal(&self.surface.name);
        let mut out = self.header(
            "Pure functions: the Rust `#[game_pure]` fns compiled to WebAssembly (binary ABI).",
        );
        let types: Vec<&Wire> = pures
            .iter()
            .flat_map(|p| p.arg.iter().chain([&p.output]))
            .collect();
        let records = self.reachable(types.iter().copied());
        let _ = writeln!(
            out,
            "import {{ createWasmPures, type WasmPures, type WasmSource{} }} from '{SDK}';\n",
            if records.is_empty() && !types.iter().any(|t| !matches!(t, Wire::Record { .. })) {
                ""
            } else {
                ", type WireType"
            }
        );
        let imports = self.import_records(types.iter().copied());
        if !imports.is_empty() {
            out.push_str(&imports);
            out.push('\n');
        }
        out.push_str(&self.type_consts(&records)?);
        let _ = writeln!(
            out,
            "/** The pure functions (`#[game_pure]`) of Cargo package `{}`: wrapper name → module export. */\nexport const PURES = {{",
            first.package
        );
        for pure in pures {
            let _ = writeln!(
                out,
                "\t{}: {{ export: {}, abi: {} }},",
                key(&camel(&pure.export)),
                string(&pure.export),
                pure.abi
            );
        }
        out.push_str("} as const;\n\n");
        let _ = writeln!(
            out,
            "/** The compiled module (`distributed game-wasm`; shared with the predicted step); bundlers emit it as an asset. */\nexport const PURES_WASM_URL: URL = new URL({}, import.meta.url);\n",
            string(&format!("./{}", predict_wasm_path(&self.surface.name)))
        );
        let _ = writeln!(
            out,
            "/** Typed calls of the compiled functions (synchronous; they throw until `ready` resolves). */\nexport interface {prefix}PureFns {{"
        );
        for pure in pures {
            let signature = match &pure.arg {
                Some(arg) => format!("arg: {}", self.ts_type(arg)),
                None => String::new(),
            };
            let _ = writeln!(
                out,
                "\t/** `{}` (`dg_pure_{}`). */\n\t{}({signature}): {};",
                pure.export,
                pure.export,
                camel(&pure.export),
                self.ts_type(&pure.output)
            );
        }
        out.push_str("}\n\n");
        let _ = writeln!(
            out,
            "/** The `{surface}` pure functions plus `ready` / `loaded()` / `module()`. */\nexport type {prefix}Pures = WasmPures<{prefix}PureFns>;\n",
            surface = self.surface.name
        );
        let _ = writeln!(
            out,
            "/**\n * Load the compiled pure functions (default `PURES_WASM_URL`; a URL loaded by\n * the predicted step too is instantiated once).\n */\nexport function create{prefix}Pures(source: WasmSource = PURES_WASM_URL): {prefix}Pures {{\n\treturn createWasmPures<{prefix}PureFns>(source, {{"
        );
        for pure in pures {
            let arg = match &pure.arg {
                Some(arg) => format!(", arg: {}", self.wire_literal(arg)),
                None => String::new(),
            };
            let _ = writeln!(
                out,
                "\t\t{}: {{ exportName: {}{arg}, output: {} }},",
                key(&camel(&pure.export)),
                string(&pure.export),
                self.wire_literal(&pure.output)
            );
        }
        out.push_str("\t});\n}\n");
        Ok(Some(out))
    }

    fn constants_ts(&self) -> GenResult<Option<String>> {
        let constants = &self.surface.constants;
        if constants.is_empty() {
            return Ok(None);
        }
        let mut out =
            self.header("Constants: static data defined once in Rust (`GameSurface::constant`).");
        let imports = self.import_records(constants.iter().map(|c| &c.ty));
        if !imports.is_empty() {
            out.push_str(&imports);
        }
        for c in constants {
            let _ = writeln!(
                out,
                "\n/** `{}`, as the client decodes it. */\nexport const {}: {} = {};",
                c.name,
                constant(&c.name),
                self.ts_type(&c.ty),
                self.literal(&c.ty, &c.value, 0)
                    .map_err(|e| format!("constant {}: {e}", c.name))?
            );
        }
        Ok(Some(out))
    }

    /// A TypeScript literal of `value` (JSON as `GameSurface::constant`
    /// stores it) of type `ty`: records keyed by camelCase field name, floats
    /// as exact binary32 values, `null` as `undefined`.
    fn literal(&self, ty: &Wire, value: &serde_json::Value, depth: usize) -> GenResult<String> {
        use serde_json::Value;
        let pad = "\t".repeat(depth + 1);
        let close = "\t".repeat(depth);
        let mismatch = || format!("value {value} does not match {}", self.ts_type(ty));
        let int = |min: i128, max: i128| -> GenResult<String> {
            let v = value
                .as_i64()
                .map(i128::from)
                .or(value.as_u64().map(i128::from))
                .ok_or_else(mismatch)?;
            if v < min || v > max {
                return Err(format!("{v} is out of range"));
            }
            Ok(v.to_string())
        };
        Ok(match ty {
            Wire::Bool => value.as_bool().ok_or_else(mismatch)?.to_string(),
            Wire::U8 => int(0, 255)?,
            Wire::I8 => int(-128, 127)?,
            Wire::U16 => int(0, 65_535)?,
            Wire::I16 => int(-32_768, 32_767)?,
            Wire::U32 => int(0, u32::MAX.into())?,
            Wire::I32 => int(i32::MIN.into(), i32::MAX.into())?,
            Wire::U64 => int(0, (1 << 53) - 1)?,
            Wire::F32 | Wire::Quant { .. } | Wire::Angle { .. } => {
                let v = value.as_f64().ok_or_else(mismatch)?;
                if f64::from(v as f32) != v {
                    return Err(format!("{v} is not a binary32 value"));
                }
                f32_exact(v as f32)
            }
            Wire::F64 => f64_literal(value.as_f64().ok_or_else(mismatch)?),
            Wire::ShortStr | Wire::Str => string(value.as_str().ok_or_else(mismatch)?),
            Wire::Bytes => {
                let bytes = value.as_array().ok_or_else(mismatch)?;
                let mut items = Vec::new();
                for b in bytes {
                    items.push(self.literal(&Wire::U8, b, depth)?);
                }
                format!("new Uint8Array([{}])", items.join(", "))
            }
            Wire::Optional { item } => match value {
                Value::Null => "undefined".into(),
                other => self.literal(item, other, depth)?,
            },
            Wire::List { item } => {
                let items = value.as_array().ok_or_else(mismatch)?;
                if items.is_empty() {
                    return Ok("[]".into());
                }
                let parts = items
                    .iter()
                    .map(|v| self.literal(item, v, depth + 1))
                    .collect::<GenResult<Vec<_>>>()?;
                let inline = format!("[{}]", parts.join(", "));
                if is_scalar(item) && inline.len() <= 100 {
                    inline
                } else {
                    format!("[\n{pad}{}\n{close}]", parts.join(&format!(",\n{pad}")))
                }
            }
            Wire::Record { name } => {
                let record = self.records[name.as_str()];
                let object = value.as_object().ok_or_else(mismatch)?;
                if object.len() != record.fields.len() {
                    return Err(format!(
                        "record {name} value has fields {:?}",
                        object.keys().collect::<Vec<_>>()
                    ));
                }
                let mut parts = Vec::new();
                let mut simple = true;
                for field in &record.fields {
                    let v = object
                        .get(&field.name)
                        .ok_or_else(|| format!("record {name} value lacks field {}", field.name))?;
                    simple &= is_scalar(&field.ty);
                    parts.push(format!(
                        "{}: {}",
                        key(&camel(&field.name)),
                        self.literal(&field.ty, v, depth + 1)?
                    ));
                }
                let inline = format!("{{ {} }}", parts.join(", "));
                if simple && inline.len() <= 100 {
                    inline
                } else {
                    format!("{{\n{pad}{}\n{close}}}", parts.join(&format!(",\n{pad}")))
                }
            }
        })
    }
}

/// Records a type names directly (through lists and options, not through
/// other records).
fn top_records(ty: &Wire, out: &mut Vec<String>) {
    match ty {
        Wire::List { item } | Wire::Optional { item } => top_records(item, out),
        Wire::Record { name } => out.push(name.clone()),
        _ => {}
    }
}

fn is_scalar(ty: &Wire) -> bool {
    match ty {
        Wire::List { .. } | Wire::Record { .. } | Wire::Bytes => false,
        Wire::Optional { item } => is_scalar(item),
        _ => true,
    }
}

fn simple_tag(ty: &Wire) -> &'static str {
    match ty {
        Wire::Bool => "bool",
        Wire::U8 => "u8",
        Wire::I8 => "i8",
        Wire::U16 => "u16",
        Wire::I16 => "i16",
        Wire::U32 => "u32",
        Wire::I32 => "i32",
        Wire::U64 => "u64",
        Wire::F32 => "f32",
        Wire::F64 => "f64",
        Wire::ShortStr => "short_str",
        Wire::Str => "str",
        Wire::Bytes => "bytes",
        _ => unreachable!("composite types have their own literal"),
    }
}

fn reader(ty: &Wire) -> &'static str {
    match ty {
        Wire::Bool => "fieldBool",
        Wire::ShortStr => "fieldString",
        Wire::Bytes => "fieldBytes",
        _ => "fieldNumber",
    }
}

fn field_doc(ty: &Wire, priority: f32) -> String {
    let wire = match ty {
        Wire::Quant { min, max, bits } => format!(
            "quant [{}, {}] / {bits} bits",
            f32_literal(*min),
            f32_literal(*max)
        ),
        Wire::Angle { bits } => format!("angle / {bits} bits"),
        other => simple_tag(other).to_string(),
    };
    if priority == 1.0 {
        format!("`{wire}`")
    } else {
        format!("`{wire}`, priority {}", f32_literal(priority))
    }
}

fn indent(block: &str) -> String {
    block.replace("\n\t", "\n\t\t").replace("\n]", "\n\t]")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_convert() {
        assert_eq!(camel("view_radius"), "viewRadius");
        assert_eq!(camel("slowMs"), "slowMs");
        assert_eq!(camel("hp"), "hp");
        assert_eq!(pascal("board_snapshot"), "BoardSnapshot");
        assert_eq!(pascal("player"), "Player");
        assert_eq!(constant("BoardRow"), "BOARD_ROW");
        assert_eq!(constant("slow_ms"), "SLOW_MS");
        assert_eq!(key("type"), "type");
        assert_eq!(key("a-b"), "'a-b'");
    }

    #[test]
    fn rejects_other_schema_versions_and_unknown_fields() {
        let err = generate_game_client(r#"{"schemaVersion": 1}"#, ImportExtension::Ts).unwrap_err();
        assert!(err.contains("schemaVersion"), "{err}");
        let err = generate_game_client(
            r#"{"schemaVersion":3,"name":"x","protocolVersion":1,"kinds":[],"input":null,"events":[],"records":[],"extra":1}"#,
            ImportExtension::Ts,
        )
        .unwrap_err();
        assert!(err.contains("unknown field"), "{err}");
    }

    fn sample() -> serde_json::Value {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../distributed_game/tests/fixtures/authoring.json"
        ))
        .unwrap();
        fixture["surface"].clone()
    }

    fn generate(surface: &serde_json::Value) -> GenResult<Vec<GeneratedClientFile>> {
        generate_game_client(&surface.to_string(), ImportExtension::Ts)
    }

    #[test]
    fn validates_the_predicted_step() {
        let ok = sample();
        let files = generate(&ok).unwrap();
        let predict = files.iter().find(|f| f.path == "predict.ts").unwrap();
        assert!(predict
            .contents
            .contains("new URL('./wasm/sample.wasm', import.meta.url)"));
        assert!(predict.contents.contains("['dead', { type: 'bool' }]"));

        type Mutate = fn(&mut serde_json::Value);
        let cases: [(&str, Mutate); 6] = [
            ("ABI v9", |s| s["predict"]["abi"] = 9.into()),
            ("not replicated", |s| s["predict"]["kind"] = "ghost".into()),
            ("state record Nope is missing", |s| {
                s["predict"]["state"] = "Nope".into()
            }),
            ("context record Nope is missing", |s| {
                s["predict"]["context"] = "Nope".into()
            }),
            ("invalid predicted step export", |s| {
                s["predict"]["export"] = "a-b".into()
            }),
            ("does not match the type", |s| {
                let records = s["records"].as_array_mut().unwrap();
                let state = records
                    .iter_mut()
                    .find(|r| r["name"] == "SampleState")
                    .unwrap();
                state["fields"][2]["type"] = serde_json::json!({"type": "u16"});
            }),
        ];
        for (expected, mutate) in cases {
            let mut surface = ok.clone();
            mutate(&mut surface);
            let err = generate(&surface).unwrap_err();
            assert!(err.contains(expected), "{expected}: {err}");
        }

        let mut none = ok.clone();
        none.as_object_mut().unwrap().remove("predict");
        let files = generate(&none).unwrap();
        assert!(files.iter().all(|f| f.path != "predict.ts"));
        let client = files.iter().find(|f| f.path == "client.ts").unwrap();
        assert!(!client.contents.contains("Prediction("));
    }

    #[test]
    fn validates_pures_constants_and_codecs() {
        type Mutate = fn(&mut serde_json::Value);
        let cases: [(&str, Mutate); 9] = [
            ("ABI v2", |s| s["pures"][0]["abi"] = 2.into()),
            ("module is built from distributed_game", |s| {
                s["pures"][1]["package"] = "other".into()
            }),
            ("reserved", |s| s["pures"][0]["export"] = "ready".into()),
            ("duplicate pure function", |s| {
                s["pures"][1]["export"] = "sample_mirror".into()
            }),
            ("unknown record Nope", |s| {
                s["pures"][0]["output"] = serde_json::json!({"type": "record", "name": "Nope"})
            }),
            ("not a binary32 value", |s| {
                s["constants"][1]["value"][0] = 0.1.into()
            }),
            ("out of range", |s| {
                s["constants"][0]["value"]["version"] = (-1).into()
            }),
            ("lacks field label", |s| {
                let spot = s["constants"][2]["value"].as_object_mut().unwrap();
                spot.remove("label");
                spot.insert("other".into(), "x".into());
            }),
            ("codec record Nope is missing", |s| {
                s["codecs"][0] = "Nope".into()
            }),
        ];
        for (expected, mutate) in cases {
            let mut surface = sample();
            mutate(&mut surface);
            let err = generate(&surface).unwrap_err();
            assert!(err.contains(expected), "{expected}: {err}");
        }
        // Without pures, constants or codecs: no extra files or functions.
        let mut plain = sample();
        for key in ["pures", "constants", "codecs"] {
            plain.as_object_mut().unwrap().remove(key);
        }
        let files = generate(&plain).unwrap();
        assert!(files
            .iter()
            .all(|f| f.path != "pures.ts" && f.path != "constants.ts"));
        let events = files.iter().find(|f| f.path == "events.ts").unwrap();
        assert!(!events.contents.contains("decodeSampleWind"));
        // The module plan lists the step and every pure function.
        let (name, plan) = surface_module(&sample().to_string()).unwrap();
        let plan = plan.unwrap();
        assert_eq!(name, "sample");
        assert_eq!(
            plan.exports,
            [
                "dg_predict_sample_step",
                "dg_pure_sample_mirror",
                "dg_pure_sample_rows"
            ]
        );
        plain.as_object_mut().unwrap().remove("predict");
        assert!(surface_module(&plain.to_string()).unwrap().1.is_none());
    }

    #[test]
    fn rejects_recursive_records() {
        let json = r#"{"schemaVersion":3,"name":"x","protocolVersion":1,"kinds":[],"input":null,"events":[],
            "records":[{"name":"Node","fields":[{"name":"next","type":{"type":"optional","item":{"type":"record","name":"Node"}}}]}]}"#;
        let err = generate_game_client(json, ImportExtension::Ts).unwrap_err();
        assert!(err.contains("recursive"), "{err}");
    }
}
