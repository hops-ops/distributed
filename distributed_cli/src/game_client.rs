//! `distributed game-client`: typed TypeScript for one `distributed_game`
//! zone surface (`GameSurface` JSON, schema v1, from `distributed
//! game-manifest`).
//!
//! The generated files import only `@hops-ops/distributed/game` and are
//! deterministic (same surface → same bytes):
//!
//! | file | contents |
//! |---|---|
//! | `schema.ts` | `KIND` ids, per-kind field interfaces + accessors over rendered entities, field wire types, `CORRECTIONS` defaults |
//! | `input.ts` | the input interface, `BUTTON` bits, `encodeInput` / `decodeInput` / `sanitizeInput` |
//! | `events.ts` | record + event payload interfaces, `EVENT` ids, `EVENTS` codecs, `decodeEvent`, `onEvent` |
//! | `client.ts` | `create<Surface>Client`: a `GameClient` with the input encoder and correction defaults wired in, plus typed `on` |
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
pub const GAME_SURFACE_SCHEMA_VERSION: u32 = 1;

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

// ---- surface mirror (schema v1) -------------------------------------------

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
    files.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(files)
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
        self.check_exports()
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
                f32_literal(*min),
                f32_literal(*max)
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
        let buttons: Vec<(String, u32)> = input
            .fields
            .iter()
            .flat_map(|f| {
                f.buttons
                    .iter()
                    .enumerate()
                    .map(|(bit, b)| (camel(b), 1u32 << bit))
            })
            .collect();
        if !buttons.is_empty() {
            out.push_str("\n/** Button bits (`#[buttons(..)]`). */\nexport const BUTTON = {\n");
            for (name, bit) in &buttons {
                let _ = writeln!(out, "\t{}: {bit},", key(name));
            }
            out.push_str("} as const;\n");
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
        let uses_fields = events
            .iter()
            .any(|e| matches!(e.payload, Payload::Fields { .. }));
        let uses_value = events
            .iter()
            .any(|e| matches!(e.payload, Payload::Value { .. }));
        if uses_fields {
            sdk.extend(["decodePayload", "encodePayload", "type PayloadLayout"]);
        }
        if uses_value {
            sdk.extend(["decodeValue", "encodeValue"]);
        }
        if uses_value || !records.is_empty() {
            sdk.push("type WireType");
        }
        sdk.sort_by_key(|s| (s.starts_with("type "), s.to_string()));
        let _ = writeln!(out, "import {{ {} }} from '{SDK}';", sdk.join(", "));

        for record in &records {
            out.push('\n');
            out.push_str(&self.interface(
                &record.name,
                &format!("Record `{}` (`#[derive(GameRecord)]`).", record.name),
                &record.fields,
            ));
            let _ = writeln!(
                out,
                "\nconst {}_TYPE: WireType = {{\n\ttype: 'record',\n\tfields: {}\n}};",
                constant(&record.name),
                indent(&self.layout(&record.fields))
            );
        }

        for event in events {
            let type_name = format!("{}Event", pascal(&event.name));
            out.push('\n');
            match &event.payload {
                Payload::Fields { fields } if fields.is_empty() => {
                    let _ = writeln!(
                        out,
                        "/** Event `{}` (id {}): no payload. */\nexport type {type_name} = Readonly<Record<string, never>>;",
                        event.name, event.id
                    );
                    let _ = writeln!(
                        out,
                        "\nconst {}_LAYOUT: PayloadLayout = [];",
                        constant(&event.name)
                    );
                }
                Payload::Fields { fields } => {
                    out.push_str(&self.interface(
                        &type_name,
                        &format!("Event `{}` (id {}).", event.name, event.id),
                        fields,
                    ));
                    let _ = writeln!(
                        out,
                        "\nconst {}_LAYOUT: PayloadLayout = {};",
                        constant(&event.name),
                        self.layout(fields)
                    );
                }
                Payload::Value { ty } => {
                    let _ = writeln!(
                        out,
                        "/** Event `{}` (id {}). */\nexport type {type_name} = {};",
                        event.name,
                        event.id,
                        self.ts_type(ty)
                    );
                    let _ = writeln!(
                        out,
                        "\nconst {}_PAYLOAD: WireType = {};",
                        constant(&event.name),
                        self.wire_literal(ty)
                    );
                }
            }
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
        Ok(out)
    }

    fn client_ts(&self) -> String {
        let prefix = pascal(&self.surface.name);
        let mut out = self.header("Typed client factory.");
        let input = self.surface.input.as_ref().map(|i| i.name.as_str());
        match input {
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

    fn index_ts(&self) -> String {
        let mut out = self.header("Everything generated for this surface.");
        for module in ["client", "events", "input", "schema"] {
            let _ = writeln!(out, "export * from '{}';", self.local(module));
        }
        out
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
        let err = generate_game_client(r#"{"schemaVersion": 2}"#, ImportExtension::Ts).unwrap_err();
        assert!(err.contains("schemaVersion"), "{err}");
        let err = generate_game_client(
            r#"{"schemaVersion":1,"name":"x","protocolVersion":1,"kinds":[],"input":null,"events":[],"records":[],"extra":1}"#,
            ImportExtension::Ts,
        )
        .unwrap_err();
        assert!(err.contains("unknown field"), "{err}");
    }

    #[test]
    fn rejects_recursive_records() {
        let json = r#"{"schemaVersion":1,"name":"x","protocolVersion":1,"kinds":[],"input":null,"events":[],
            "records":[{"name":"Node","fields":[{"name":"next","type":{"type":"optional","item":{"type":"record","name":"Node"}}}]}]}"#;
        let err = generate_game_client(json, ImportExtension::Ts).unwrap_err();
        assert!(err.contains("recursive"), "{err}");
    }
}
