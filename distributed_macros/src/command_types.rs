//! Transport-neutral command data-shape derives.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{
    punctuated::Punctuated, Attribute, Data, DeriveInput, Expr, Fields, GenericArgument, Lit,
    LitStr, Meta, PathArguments, Token, Type,
};

#[derive(Clone, Copy)]
enum SerdeDirection {
    Deserialize,
    Serialize,
}

impl SerdeDirection {
    fn attribute_name(self) -> &'static str {
        match self {
            Self::Deserialize => "deserialize",
            Self::Serialize => "serialize",
        }
    }

    fn derive_name(self) -> &'static str {
        match self {
            Self::Deserialize => "CommandInput",
            Self::Serialize => "CommandOutput",
        }
    }
}

#[derive(Clone, Copy)]
#[expect(
    clippy::enum_variant_names,
    reason = "variant names intentionally mirror serde's public rename_all terminology"
)]
enum RenameRule {
    LowerCase,
    UpperCase,
    PascalCase,
    CamelCase,
    SnakeCase,
    ScreamingSnakeCase,
    KebabCase,
    ScreamingKebabCase,
}

impl RenameRule {
    fn parse(value: &LitStr) -> syn::Result<Self> {
        match value.value().as_str() {
            "lowercase" => Ok(Self::LowerCase),
            "UPPERCASE" => Ok(Self::UpperCase),
            "PascalCase" => Ok(Self::PascalCase),
            "camelCase" => Ok(Self::CamelCase),
            "snake_case" => Ok(Self::SnakeCase),
            "SCREAMING_SNAKE_CASE" => Ok(Self::ScreamingSnakeCase),
            "kebab-case" => Ok(Self::KebabCase),
            "SCREAMING-KEBAB-CASE" => Ok(Self::ScreamingKebabCase),
            other => Err(syn::Error::new_spanned(
                value,
                format!(
                    "unsupported serde rename_all rule `{other}` for CommandInput/CommandOutput"
                ),
            )),
        }
    }

    fn apply(self, field: &str) -> String {
        match self {
            Self::KebabCase => field.replace('_', "-"),
            Self::ScreamingKebabCase => field.replace('_', "-").to_ascii_uppercase(),
            Self::LowerCase | Self::SnakeCase => field.to_string(),
            Self::UpperCase | Self::ScreamingSnakeCase => field.to_ascii_uppercase(),
            Self::PascalCase => {
                let mut renamed = String::new();
                let mut capitalize = true;
                for ch in field.chars() {
                    if ch == '_' {
                        capitalize = true;
                    } else if capitalize {
                        renamed.push(ch.to_ascii_uppercase());
                        capitalize = false;
                    } else {
                        renamed.push(ch);
                    }
                }
                renamed
            }
            Self::CamelCase => {
                let pascal = Self::PascalCase.apply(field);
                let mut chars = pascal.chars();
                chars
                    .next()
                    .map(|first| first.to_ascii_lowercase().to_string() + chars.as_str())
                    .unwrap_or_default()
            }
        }
    }
}

pub fn expand_command_input(input: DeriveInput) -> syn::Result<TokenStream> {
    let framework = crate::shared::framework_path()?;
    expand(
        input,
        quote! { #framework::command::CommandInputType },
        quote! { #framework::command::CommandInputType },
        framework,
        SerdeDirection::Deserialize,
    )
}

pub fn expand_command_output(input: DeriveInput) -> syn::Result<TokenStream> {
    let framework = crate::shared::framework_path()?;
    expand(
        input,
        quote! { #framework::command::CommandOutputType },
        quote! { #framework::command::CommandOutputType },
        framework,
        SerdeDirection::Serialize,
    )
}

fn expand(
    input: DeriveInput,
    trait_path: TokenStream,
    nested_trait: TokenStream,
    framework: TokenStream,
    serde_direction: SerdeDirection,
) -> syn::Result<TokenStream> {
    let method = quote! { command_type };
    let type_def = quote! { #framework::command::CommandTypeDef };
    let type_field = quote! { #framework::command::CommandTypeField };
    let name = &input.ident;
    let visibility = &input.vis;
    validate_serde_container_shape(&input.attrs, serde_direction)?;
    let rename_all = serde_rename_all(&input.attrs, serde_direction)?;
    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input,
            "CommandInput/CommandOutput only support structs with named fields",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(syn::Error::new_spanned(
            &input,
            "CommandInput/CommandOutput require named fields",
        ));
    };

    let mut field_tokens = Vec::new();
    let mut effect_input_markers = Vec::new();
    for field in &fields.named {
        validate_serde_field_shape(&field.attrs, serde_direction)?;
        let field_name = field
            .ident
            .as_ref()
            .ok_or_else(|| syn::Error::new_spanned(field, "field must be named"))?;
        let rust_field_name = field_name.to_string();
        let rust_field_name = rust_field_name
            .strip_prefix("r#")
            .unwrap_or(&rust_field_name);
        let field_name_str =
            serde_field_rename(&field.attrs, serde_direction)?.unwrap_or_else(|| {
                rename_all
                    .map(|rule| rule.apply(rust_field_name))
                    .unwrap_or_else(|| rust_field_name.to_string())
            });
        let (type_name, nullable, list, item_nullable, nested) =
            map_type(&field.ty, field, &nested_trait, &method)?;
        let effect_path_kind = if !list && nested.is_some() {
            quote! { #framework::command::EffectInputObjectKind }
        } else {
            quote! { #framework::command::EffectInputTerminalKind }
        };
        let effect_wire = effect_input_wire_tokens(&framework, &type_name, list, nested.is_some());
        let nested_tokens = match nested {
            Some(tokens) => quote! { Some(::std::boxed::Box::new(#tokens)) },
            None => quote! { None },
        };
        field_tokens.push(quote! {
            #type_field {
                name: #field_name_str.to_string(),
                type_name: #type_name.to_string(),
                nullable: #nullable,
                list: #list,
                item_nullable: #item_nullable,
                nested: #nested_tokens,
            }
        });
        if matches!(serde_direction, SerdeDirection::Deserialize) {
            let marker = format_ident!("__Distributed{}EffectInputField_{}", name, field_name);
            let field_ty = &field.ty;
            let nested_ty = effect_nested_type(field_ty);
            let non_null_ty = effect_non_null_type(field_ty);
            let nullability = if extract_path_arg(field_ty, "Option").is_some() {
                quote! { #framework::command::EffectNullable }
            } else {
                quote! { #framework::command::EffectRequired }
            };
            effect_input_markers.push(quote! {
                #[doc(hidden)]
                #[allow(non_camel_case_types)]
                #visibility struct #marker;

                impl #framework::command::EffectInputFieldMarker for #marker {
                    type Input = #name;
                    type Value = #field_ty;
                    type NonNullValue = #non_null_ty;
                    type Nullability = #nullability;
                    type PathKind = #effect_path_kind;
                    type Wire = #effect_wire;
                    type Nested = #nested_ty;
                    fn path() -> ::std::vec::Vec<&'static str> {
                        vec![#field_name_str]
                    }
                }
            });
        }
    }

    let type_name_str = name.to_string();
    Ok(quote! {
        impl #trait_path for #name {
            fn #method() -> #type_def {
                #type_def::new(
                    #type_name_str,
                    vec![#(#field_tokens),*],
                ).with_type_id(::std::any::TypeId::of::<#name>())
            }
        }

        #(#effect_input_markers)*
    })
}

fn effect_input_wire_tokens(
    framework: &TokenStream,
    type_name: &str,
    list: bool,
    nested: bool,
) -> proc_macro2::TokenStream {
    if list {
        return quote! { #framework::command::EffectWireList };
    }
    if nested {
        return quote! { #framework::command::EffectWireObject };
    }
    match type_name {
        "String" | "ID" => quote! { #framework::command::EffectWireString },
        "Boolean" => quote! { #framework::command::EffectWireBoolean },
        "BigInt" | "Int" => quote! { #framework::command::EffectWireBigInt },
        "Float" => quote! { #framework::command::EffectWireFloat },
        "JSON" => quote! { #framework::command::EffectWireJson },
        "Bytea" => quote! { #framework::command::EffectWireBytea },
        "Timestamptz" => quote! { #framework::command::EffectWireTimestamp },
        _ => quote! { #framework::command::EffectWireUnsupported },
    }
}

fn effect_non_null_type(mut ty: &Type) -> &Type {
    while let Some(inner) = extract_path_arg(ty, "Option") {
        ty = inner;
    }
    ty
}

fn effect_nested_type(mut ty: &Type) -> &Type {
    while let Some(inner) = extract_path_arg(ty, "Option") {
        ty = inner;
    }
    if let Some(inner) = extract_path_arg(ty, "Vec") {
        ty = inner;
        while let Some(inner) = extract_path_arg(ty, "Option") {
            ty = inner;
        }
    }
    ty
}

fn map_type(
    ty: &Type,
    span: &syn::Field,
    nested_trait: &TokenStream,
    method: &TokenStream,
) -> syn::Result<(String, bool, bool, bool, Option<TokenStream>)> {
    let mut current = ty;
    let mut nullable = false;
    while let Some(inner) = extract_path_arg(current, "Option") {
        nullable = true;
        current = inner;
    }

    let mut list = false;
    let mut item_nullable = false;
    if let Some(inner) = extract_path_arg(current, "Vec") {
        list = true;
        current = inner;
        while let Some(inner) = extract_path_arg(current, "Option") {
            item_nullable = true;
            current = inner;
        }
        if extract_path_arg(current, "Vec").is_some() {
            return Err(syn::Error::new_spanned(
                ty,
                "nested lists are not supported for CommandInput/CommandOutput fields",
            ));
        }
    }

    let path = match current {
        Type::Path(p) => p,
        _ => {
            return Err(syn::Error::new_spanned(
                span,
                "unsupported field type for CommandInput/CommandOutput",
            ));
        }
    };
    let last = path
        .path
        .segments
        .last()
        .ok_or_else(|| syn::Error::new_spanned(span, "empty type path"))?;
    let ident = last.ident.to_string();

    let scalar = match ident.as_str() {
        "String" | "str" => Some("String"),
        "bool" => Some("Boolean"),
        "i8" | "i16" | "i32" | "i64" | "u8" | "u16" | "u32" | "u64" | "isize" | "usize" => {
            Some("BigInt")
        }
        "f32" | "f64" => Some("Float"),
        "Value" => Some("JSON"),
        _ => None,
    };

    if let Some(s) = scalar {
        return Ok((s.to_string(), nullable, list, item_nullable, None));
    }

    let nested = quote! { <#current as #nested_trait>::#method() };
    Ok((ident, nullable, list, item_nullable, Some(nested)))
}

fn extract_path_arg<'a>(ty: &'a Type, wrapper: &str) -> Option<&'a Type> {
    let Type::Path(path) = ty else {
        return None;
    };
    let seg = path.path.segments.last()?;
    if seg.ident != wrapper {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &seg.arguments else {
        return None;
    };
    args.args.iter().find_map(|arg| match arg {
        GenericArgument::Type(t) => Some(t),
        _ => None,
    })
}

fn serde_rename_all(
    attrs: &[Attribute],
    direction: SerdeDirection,
) -> syn::Result<Option<RenameRule>> {
    let value = serde_name_value(attrs, "rename_all", direction)?;
    value.as_ref().map(RenameRule::parse).transpose()
}

fn validate_serde_field_shape(attrs: &[Attribute], direction: SerdeDirection) -> syn::Result<()> {
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("serde")) {
        let metas = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
        for meta in metas {
            let unsupported = match &meta {
                Meta::Path(path) if path.is_ident("skip") => Some("skip"),
                Meta::Path(path)
                    if path.is_ident("skip_deserializing")
                        && matches!(direction, SerdeDirection::Deserialize) =>
                {
                    Some("skip_deserializing")
                }
                Meta::Path(path)
                    if path.is_ident("skip_serializing")
                        && matches!(direction, SerdeDirection::Serialize) =>
                {
                    Some("skip_serializing")
                }
                Meta::NameValue(name_value)
                    if name_value.path.is_ident("skip_serializing_if")
                        && matches!(direction, SerdeDirection::Serialize) =>
                {
                    Some("skip_serializing_if")
                }
                Meta::Path(path) if path.is_ident("flatten") => Some("flatten"),
                Meta::Path(path)
                    if path.is_ident("default")
                        && matches!(direction, SerdeDirection::Deserialize) =>
                {
                    Some("default")
                }
                Meta::NameValue(name_value)
                    if name_value.path.is_ident("default")
                        && matches!(direction, SerdeDirection::Deserialize) =>
                {
                    Some("default")
                }
                Meta::NameValue(name_value) if name_value.path.is_ident("with") => Some("with"),
                Meta::NameValue(name_value)
                    if name_value.path.is_ident("deserialize_with")
                        && matches!(direction, SerdeDirection::Deserialize) =>
                {
                    Some("deserialize_with")
                }
                Meta::NameValue(name_value)
                    if name_value.path.is_ident("serialize_with")
                        && matches!(direction, SerdeDirection::Serialize) =>
                {
                    Some("serialize_with")
                }
                Meta::NameValue(name_value)
                    if name_value.path.is_ident("alias")
                        && matches!(direction, SerdeDirection::Deserialize) =>
                {
                    Some("alias")
                }
                _ => None,
            };
            if let Some(attribute) = unsupported {
                return Err(syn::Error::new_spanned(
                    meta,
                    format!(
                        "#[serde({attribute})] is not supported by {} because it changes the declared command field shape; define a separate wire type",
                        direction.derive_name(),
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn validate_serde_container_shape(
    attrs: &[Attribute],
    direction: SerdeDirection,
) -> syn::Result<()> {
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("serde")) {
        let metas = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
        for meta in metas {
            let unsupported = match &meta {
                Meta::Path(path) if path.is_ident("transparent") => Some("transparent"),
                Meta::Path(path) if path.is_ident("untagged") => Some("untagged"),
                Meta::NameValue(name_value)
                    if name_value.path.is_ident("tag") || name_value.path.is_ident("content") =>
                {
                    Some(if name_value.path.is_ident("tag") {
                        "tag"
                    } else {
                        "content"
                    })
                }
                Meta::Path(path)
                    if path.is_ident("default")
                        && matches!(direction, SerdeDirection::Deserialize) =>
                {
                    Some("default")
                }
                Meta::NameValue(name_value)
                    if name_value.path.is_ident("default")
                        && matches!(direction, SerdeDirection::Deserialize) =>
                {
                    Some("default")
                }
                Meta::NameValue(name_value)
                    if (name_value.path.is_ident("from")
                        || name_value.path.is_ident("try_from"))
                        && matches!(direction, SerdeDirection::Deserialize) =>
                {
                    Some(if name_value.path.is_ident("from") {
                        "from"
                    } else {
                        "try_from"
                    })
                }
                Meta::NameValue(name_value)
                    if name_value.path.is_ident("into")
                        && matches!(direction, SerdeDirection::Serialize) =>
                {
                    Some("into")
                }
                _ => None,
            };
            if let Some(attribute) = unsupported {
                return Err(syn::Error::new_spanned(
                    meta,
                    format!(
                        "#[serde({attribute})] is not supported by {} because it changes the declared command object shape; define a separate wire type",
                        direction.derive_name(),
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn serde_field_rename(
    attrs: &[Attribute],
    direction: SerdeDirection,
) -> syn::Result<Option<String>> {
    Ok(serde_name_value(attrs, "rename", direction)?.map(|value| value.value()))
}

fn serde_name_value(
    attrs: &[Attribute],
    key: &str,
    direction: SerdeDirection,
) -> syn::Result<Option<LitStr>> {
    let mut found = None;
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("serde")) {
        let metas = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
        for meta in metas {
            match meta {
                Meta::NameValue(name_value) if name_value.path.is_ident(key) => {
                    set_serde_name(&mut found, string_literal(&name_value.value, key)?, key)?;
                }
                Meta::List(list) if list.path.is_ident(key) => {
                    let directional =
                        list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
                    for meta in directional {
                        if let Meta::NameValue(name_value) = meta {
                            if name_value.path.is_ident(direction.attribute_name()) {
                                set_serde_name(
                                    &mut found,
                                    string_literal(&name_value.value, key)?,
                                    key,
                                )?;
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
    Ok(found)
}

fn set_serde_name(found: &mut Option<LitStr>, value: LitStr, key: &str) -> syn::Result<()> {
    if found.is_some() {
        return Err(syn::Error::new_spanned(
            value,
            format!("duplicate serde `{key}` rule for CommandInput/CommandOutput"),
        ));
    }
    *found = Some(value);
    Ok(())
}

fn string_literal(value: &Expr, key: &str) -> syn::Result<LitStr> {
    match value {
        Expr::Lit(expr) => match &expr.lit {
            Lit::Str(value) => Ok(value.clone()),
            _ => Err(syn::Error::new_spanned(
                value,
                format!("serde `{key}` must be a string literal"),
            )),
        },
        _ => Err(syn::Error::new_spanned(
            value,
            format!("serde `{key}` must be a string literal"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rename_all_rules_match_serde_field_rules() {
        let field = "long_field_name";
        let cases = [
            ("lowercase", "long_field_name"),
            ("UPPERCASE", "LONG_FIELD_NAME"),
            ("PascalCase", "LongFieldName"),
            ("camelCase", "longFieldName"),
            ("snake_case", "long_field_name"),
            ("SCREAMING_SNAKE_CASE", "LONG_FIELD_NAME"),
            ("kebab-case", "long-field-name"),
            ("SCREAMING-KEBAB-CASE", "LONG-FIELD-NAME"),
        ];
        for (rule, expected) in cases {
            let literal = LitStr::new(rule, proc_macro2::Span::call_site());
            assert_eq!(RenameRule::parse(&literal).unwrap().apply(field), expected);
        }
    }

    #[test]
    fn directional_serde_names_follow_wire_direction() {
        let input: DeriveInput = syn::parse_quote! {
            #[serde(rename_all(deserialize = "camelCase", serialize = "SCREAMING_SNAKE_CASE"))]
            struct CommandInput {
                regular_field: String,
                #[serde(rename(deserialize = "inputID", serialize = "OUTPUT_ID"))]
                custom_id: String,
            }
        };
        let input_tokens = expand_command_input(input).unwrap().to_string();
        assert!(input_tokens.contains("\"regularField\""));
        assert!(input_tokens.contains("\"inputID\""));

        let output: DeriveInput = syn::parse_quote! {
            #[serde(rename_all(deserialize = "camelCase", serialize = "SCREAMING_SNAKE_CASE"))]
            struct CommandOutput {
                regular_field: String,
                #[serde(rename(deserialize = "inputID", serialize = "OUTPUT_ID"))]
                custom_id: String,
            }
        };
        let output_tokens = expand_command_output(output).unwrap().to_string();
        assert!(output_tokens.contains("\"REGULAR_FIELD\""));
        assert!(output_tokens.contains("\"OUTPUT_ID\""));
    }

    #[test]
    fn nested_lists_and_shape_changing_serde_attrs_fail_closed() {
        let nested: DeriveInput = syn::parse_quote! {
            struct Nested { values: Option<Vec<Option<Vec<String>>>> }
        };
        let error = expand_command_input(nested).unwrap_err().to_string();
        assert!(error.contains("nested lists are not supported"), "{error}");

        let skipped: DeriveInput = syn::parse_quote! {
            struct Skipped {
                #[serde(skip_deserializing)]
                value: String,
            }
        };
        let error = expand_command_input(skipped).unwrap_err().to_string();
        assert!(
            error.contains("changes the declared command field shape"),
            "{error}"
        );

        let defaulted: DeriveInput = syn::parse_quote! {
            struct Defaulted {
                #[serde(default)]
                value: String,
            }
        };
        let error = expand_command_input(defaulted).unwrap_err().to_string();
        assert!(error.contains("#[serde(default)]"), "{error}");

        let custom: DeriveInput = syn::parse_quote! {
            struct Custom {
                #[serde(deserialize_with = "decode_value")]
                value: String,
            }
        };
        let error = expand_command_input(custom).unwrap_err().to_string();
        assert!(error.contains("#[serde(deserialize_with)]"), "{error}");

        let transparent: DeriveInput = syn::parse_quote! {
            #[serde(transparent)]
            struct Transparent { value: String }
        };
        let error = expand_command_output(transparent).unwrap_err().to_string();
        assert!(error.contains("#[serde(transparent)]"), "{error}");

        let container_default: DeriveInput = syn::parse_quote! {
            #[serde(default = "default_input")]
            struct ContainerDefault { value: String }
        };
        let error = expand_command_input(container_default)
            .unwrap_err()
            .to_string();
        assert!(error.contains("#[serde(default)]"), "{error}");
    }
}
