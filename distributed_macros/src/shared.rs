use proc_macro_crate::{crate_name, FoundCrate};
use quote::{quote, ToTokens};
use sha2::{Digest, Sha256};
use syn::{
    punctuated::Punctuated, Attribute, Expr, Field, FnArg, GenericArgument, Ident, LitStr, Meta,
    MetaNameValue, Pat, PathArguments, ReturnType, Token, Type,
};

// Shared helpers
// ============================================================================

/// Resolve the framework crate as it is named by the consuming package.
///
/// Proc-macro output is compiled in the caller's crate, so a literal
/// `::distributed` path is wrong when the dependency is renamed or re-exported.
/// `FoundCrate::Itself` also keeps framework-internal macro tests hygienic.
/// [`framework_path`] for a helper below a macro's entry point. The entry
/// point has already resolved the dependency (and reported a missing one), so
/// the fallback is never what the user sees.
pub(crate) fn framework() -> proc_macro2::TokenStream {
    framework_path().unwrap_or_else(|_| quote!(::distributed))
}

pub(crate) fn framework_path() -> syn::Result<proc_macro2::TokenStream> {
    match crate_name("distributed") {
        Ok(FoundCrate::Itself) => Ok(quote!(crate)),
        Ok(FoundCrate::Name(name)) => {
            let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
            Ok(quote!(::#ident))
        }
        Err(error) => Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            format!(
                "unable to resolve the `distributed` dependency for generated code: {}; add the framework dependency or re-export it under that package",
                error
            ),
        )),
    }
}

/// Extract parameter names and types from a method signature (excludes `self`).
///
/// Every parameter must be a plain identifier: its name is recorded in the
/// event payload and used to call the method again on replay. A pattern like
/// `(a, b): (u8, u8)` or `_: String` has no single name, so silently skipping
/// it would drop the parameter from the payload and make the generated replay
/// arm call the method with too few arguments — an arity error pointing at
/// generated code, far from the cause. Reject it here with a spanned error.
pub(crate) fn extract_params_with_types(
    sig: &syn::Signature,
    attr_name: &str,
) -> syn::Result<Vec<(Ident, syn::Type)>> {
    sig.inputs
        .iter()
        .filter_map(|arg| match arg {
            FnArg::Typed(pat_type) => Some(pat_type),
            FnArg::Receiver(_) => None,
        })
        .map(|pat_type| match &*pat_type.pat {
            Pat::Ident(pat_ident) => Ok((pat_ident.ident.clone(), (*pat_type.ty).clone())),
            other => Err(syn::Error::new_spanned(
                other,
                format!(
                    "unsupported parameter pattern in #[{attr_name}] method — use a plain identifier"
                ),
            )),
        })
        .collect()
}

/// A recorder returns nothing: the macro gives it `SourcedResult<()>` for the
/// digest. A recorder that could fail after the event is recorded would leave
/// an event whose state change never happened, so validation belongs in the
/// method that calls the recorder.
pub(crate) fn ensure_infallible_recorder_signature(
    sig: &mut syn::Signature,
    framework: &proc_macro2::TokenStream,
) -> syn::Result<()> {
    if let ReturnType::Type(_, ty) = &sig.output {
        return Err(syn::Error::new_spanned(
            ty,
            "#[event] methods must omit the return type: the event is recorded before the body runs, so the body cannot fail. Validate in the method that calls the recorder",
        ));
    }
    sig.output = syn::parse_quote!(-> #framework::SourcedResult<()>);
    Ok(())
}

/// Generate a digest call token stream.
pub(crate) fn generate_digest_call(
    entity_field: &Ident,
    event_name: &LitStr,
    param_names: &[&Ident],
    version: Option<&syn::LitInt>,
) -> proc_macro2::TokenStream {
    match version {
        Some(ver) => {
            if param_names.is_empty() {
                quote! { self.#entity_field.digest_v(#event_name, #ver, &())?; }
            } else if param_names.len() == 1 {
                let param = param_names[0];
                quote! { self.#entity_field.digest_v(#event_name, #ver, &(#param.clone(),))?; }
            } else {
                quote! { self.#entity_field.digest_v(#event_name, #ver, &(#(#param_names.clone()),*))?; }
            }
        }
        None => {
            if param_names.is_empty() {
                quote! { self.#entity_field.digest_empty(#event_name)?; }
            } else if param_names.len() == 1 {
                let param = param_names[0];
                quote! { self.#entity_field.digest(#event_name, &(#param.clone(),))?; }
            } else {
                quote! { self.#entity_field.digest(#event_name, &(#(#param_names.clone()),*))?; }
            }
        }
    }
}

/// Wrap a recorder body: the prelude (digest), the body, then the postlude,
/// all behind the optional `when` guard.
pub(crate) fn wrap_recorder_body(
    guard: Option<&Expr>,
    prepend: proc_macro2::TokenStream,
    original_block: &syn::Block,
    append: proc_macro2::TokenStream,
) -> syn::Block {
    match guard {
        Some(guard) => syn::parse_quote! {
            {
                if #guard {
                    #prepend
                    #original_block;
                    #append
                }
                Ok(())
            }
        },
        None => syn::parse_quote! {
            {
                #prepend
                #original_block;
                #append
                Ok(())
            }
        },
    }
}

/// Build a deterministic schema string for one named JSON object.
pub(crate) fn canonical_object_schema(
    role: &str,
    type_name: &str,
    version: u64,
    container_attrs: &[Attribute],
    fields: impl IntoIterator<Item = (String, Type, Vec<Attribute>)>,
) -> String {
    let serde_container = canonical_serde_attrs(container_attrs);
    let fields = fields
        .into_iter()
        .map(|(name, ty, attrs)| {
            let ty = compact_tokens(&ty);
            let serde = canonical_serde_attrs(&attrs);
            format!(
                "{}:{}|{}:{}|{}:{}",
                name.len(),
                name,
                ty.len(),
                ty,
                serde.len(),
                serde
            )
        })
        .collect::<Vec<_>>();

    format!(
        "distributed.schema/v1|role={}:{}|type={}:{}|version={version}|serde={}:{}|fields={}:{}",
        role.len(),
        role,
        type_name.len(),
        type_name,
        serde_container.len(),
        serde_container,
        fields.len(),
        fields.join("|")
    )
}

fn canonical_serde_attrs(attrs: &[Attribute]) -> String {
    attrs
        .iter()
        .filter(|attr| attr.path().is_ident("serde"))
        .map(compact_tokens)
        .collect::<Vec<_>>()
        .join(",")
}

pub(crate) fn compact_tokens(tokens: &impl ToTokens) -> String {
    tokens
        .to_token_stream()
        .to_string()
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect()
}

/// Portable flat-field metadata shared by outward body and relational model
/// derives. This is deliberately independent of the table codec metadata.
pub(crate) struct ProjectionFieldMetadata<'a> {
    pub(crate) rust_name: String,
    pub(crate) wire_name: String,
    pub(crate) inner_type: &'a Type,
    pub(crate) rust_type: String,
    pub(crate) portable_kind: &'static str,
    pub(crate) nullable: bool,
    pub(crate) present: bool,
    pub(crate) always_present: bool,
}

pub(crate) fn projection_field_metadata(field: &Field) -> syn::Result<ProjectionFieldMetadata<'_>> {
    projection_field_metadata_with_rename(field, None)
}

fn projection_field_metadata_with_rename<'a>(
    field: &'a Field,
    rename_all: Option<&str>,
) -> syn::Result<ProjectionFieldMetadata<'a>> {
    let ident = field.ident.as_ref().ok_or_else(|| {
        syn::Error::new_spanned(field, "portable projection metadata requires named fields")
    })?;
    let rust_name = ident.to_string();
    let mut wire_name = rename_all
        .map(|rule| apply_serde_rename_all(&rust_name, rule))
        .transpose()?
        .unwrap_or_else(|| rust_name.clone());
    let mut present = true;
    let mut always_present = true;
    for attr in &field.attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        let metas = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
        for meta in metas {
            match meta {
                Meta::NameValue(value) if value.path.is_ident("rename") => {
                    wire_name = string_value(&value)?;
                }
                Meta::List(list) if list.path.is_ident("rename") => {
                    let nested = list.parse_args_with(
                        Punctuated::<MetaNameValue, Token![,]>::parse_terminated,
                    )?;
                    if let Some(value) =
                        nested.iter().find(|value| value.path.is_ident("serialize"))
                    {
                        wire_name = string_value(value)?;
                    }
                }
                Meta::Path(path) if path.is_ident("skip") || path.is_ident("skip_serializing") => {
                    present = false;
                    always_present = false;
                }
                Meta::NameValue(value) if value.path.is_ident("skip_serializing_if") => {
                    always_present = false;
                }
                Meta::Path(path) if path.is_ident("flatten") => {
                    return Err(syn::Error::new_spanned(
                        path,
                        "projection flat-field metadata does not support #[serde(flatten)]; use an explicit projection mapping",
                    ));
                }
                _ => {}
            }
        }
    }
    let (inner_type, nullable) = option_inner_type(&field.ty)
        .map(|inner| (inner, true))
        .unwrap_or((&field.ty, false));
    let rust_type = compact_tokens(inner_type);
    let portable_kind = portable_projection_kind(inner_type);
    Ok(ProjectionFieldMetadata {
        rust_name,
        wire_name,
        inner_type,
        rust_type,
        portable_kind,
        nullable,
        present,
        always_present,
    })
}

pub(crate) fn projection_body_metadata_tokens(
    framework: &proc_macro2::TokenStream,
    role: &str,
    type_name: &str,
    version: u64,
    container_attrs: &[Attribute],
    fields: &Punctuated<Field, Token![,]>,
) -> syn::Result<proc_macro2::TokenStream> {
    let rename_all = serde_serialize_rename_all(container_attrs)?;
    let metadata = fields
        .iter()
        .map(|field| projection_field_metadata_with_rename(field, rename_all.as_deref()))
        .collect::<syn::Result<Vec<_>>>()?;
    let canonical = metadata
        .iter()
        .map(|field| {
            format!(
                "{}:{}:{}:{}:{}:{}:{}",
                field.rust_name,
                field.wire_name,
                field.rust_type,
                field.portable_kind,
                field.nullable,
                field.present,
                field.always_present
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    let fingerprint = schema_fingerprint(&format!(
        "distributed.projection-body/v1|role={role}|type={type_name}|version={version}|{canonical}"
    ));
    let fingerprint = LitStr::new(&fingerprint, proc_macro2::Span::call_site());
    let entries = metadata.iter().map(|field| {
        let rust_name = &field.rust_name;
        let wire_name = &field.wire_name;
        let rust_type = &field.rust_type;
        let portable_kind = Ident::new(field.portable_kind, proc_macro2::Span::call_site());
        let nullable = field.nullable;
        let present = field.present;
        let always_present = field.always_present;
        quote! {
            #framework::projection::lower::ProjectionBodyFieldMetadata {
                rust_name: #rust_name,
                wire_name: #wire_name,
                rust_type: #rust_type,
                portable_type:
                    #framework::projection::lower::ProjectionPortableType::#portable_kind,
                nullable: #nullable,
                present: #present,
                always_present: #always_present,
            }
        }
    });
    Ok(quote! {
        const PROJECTION_FIELDS: &'static [
            #framework::projection::lower::ProjectionBodyFieldMetadata
        ] = &[#(#entries),*];
        const PROJECTION_SCHEMA_FINGERPRINT: &'static str = #fingerprint;
    })
}

fn serde_serialize_rename_all(attrs: &[Attribute]) -> syn::Result<Option<String>> {
    let mut rename_all = None;
    for attr in attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        let metas = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
        for meta in metas {
            match meta {
                Meta::NameValue(value) if value.path.is_ident("rename_all") => {
                    rename_all = Some(string_value(&value)?);
                }
                Meta::List(list) if list.path.is_ident("rename_all") => {
                    let nested = list.parse_args_with(
                        Punctuated::<MetaNameValue, Token![,]>::parse_terminated,
                    )?;
                    if let Some(value) =
                        nested.iter().find(|value| value.path.is_ident("serialize"))
                    {
                        rename_all = Some(string_value(value)?);
                    }
                }
                _ => {}
            }
        }
    }
    Ok(rename_all)
}

fn string_value(value: &MetaNameValue) -> syn::Result<String> {
    let Expr::Lit(literal) = &value.value else {
        return Err(syn::Error::new_spanned(
            &value.value,
            "serde rename value must be a string literal",
        ));
    };
    let syn::Lit::Str(value) = &literal.lit else {
        return Err(syn::Error::new_spanned(
            &literal.lit,
            "serde rename value must be a string literal",
        ));
    };
    Ok(value.value())
}

fn apply_serde_rename_all(field: &str, rule: &str) -> syn::Result<String> {
    let words = field
        .split('_')
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>();
    let pascal = || {
        words
            .iter()
            .map(|word| {
                let mut chars = word.chars();
                chars
                    .next()
                    .map(|first| first.to_uppercase().collect::<String>() + chars.as_str())
                    .unwrap_or_default()
            })
            .collect::<String>()
    };
    match rule {
        "lowercase" => Ok(field.to_lowercase()),
        "UPPERCASE" => Ok(field.to_uppercase()),
        "PascalCase" => Ok(pascal()),
        "camelCase" => {
            let pascal = pascal();
            let mut chars = pascal.chars();
            Ok(chars
                .next()
                .map(|first| first.to_lowercase().collect::<String>() + chars.as_str())
                .unwrap_or_default())
        }
        "snake_case" => Ok(field.to_owned()),
        "SCREAMING_SNAKE_CASE" => Ok(field.to_uppercase()),
        "kebab-case" => Ok(field.replace('_', "-")),
        "SCREAMING-KEBAB-CASE" => Ok(field.replace('_', "-").to_uppercase()),
        other => Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("unsupported serde rename_all rule `{other}` in projection metadata"),
        )),
    }
}

pub(crate) fn portable_projection_kind(ty: &Type) -> &'static str {
    let Type::Path(path) = ty else {
        return "Custom";
    };
    let Some(segment) = path.path.segments.last() else {
        return "Custom";
    };
    match segment.ident.to_string().as_str() {
        "bool" => "Boolean",
        "i8" | "i16" | "i32" | "i64" | "isize" => "I64",
        "u8" | "u16" | "u32" | "u64" | "usize" => "U64",
        "f32" | "f64" => "F64",
        "String" | "str" => "String",
        "Vec" if vec_inner_type(ty).is_some_and(is_u8) => "Bytes",
        "Vec" | "HashMap" | "BTreeMap" | "Value" => "Json",
        _ => "Custom",
    }
}

fn is_u8(ty: &Type) -> bool {
    matches!(
        ty,
        Type::Path(path)
            if path.path.segments.last().is_some_and(|segment| segment.ident == "u8")
    )
}

fn option_inner_type(ty: &Type) -> Option<&Type> {
    generic_inner_type(ty, "Option")
}

fn vec_inner_type(ty: &Type) -> Option<&Type> {
    generic_inner_type(ty, "Vec")
}

fn generic_inner_type<'a>(ty: &'a Type, wrapper: &str) -> Option<&'a Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let segment = type_path.path.segments.last()?;
    if segment.ident != wrapper {
        return None;
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    arguments.args.iter().find_map(|argument| match argument {
        GenericArgument::Type(inner) => Some(inner),
        _ => None,
    })
}

/// Return a lowercase SHA-256 descriptor fingerprint.
pub(crate) fn schema_fingerprint(schema: &str) -> String {
    let digest = Sha256::digest(schema.as_bytes());
    format!("sha256:{digest:x}")
}

pub(crate) fn validate_domain_event_name_literal(name: &LitStr) -> syn::Result<()> {
    const MAX_MESSAGE_NAME_BYTES: usize = 256;

    let value = name.value();
    if value.trim().is_empty() {
        return Err(syn::Error::new(
            name.span(),
            "domain-event name must not be empty",
        ));
    }
    if value.len() > MAX_MESSAGE_NAME_BYTES {
        return Err(syn::Error::new(
            name.span(),
            format!(
                "domain-event name is {} bytes, exceeding the maximum of {MAX_MESSAGE_NAME_BYTES}",
                value.len()
            ),
        ));
    }
    if value.chars().any(char::is_control) {
        return Err(syn::Error::new(
            name.span(),
            "domain-event name must not contain control characters",
        ));
    }
    if let Some(wildcard) = value
        .chars()
        .find(|character| matches!(character, '*' | '#' | '>'))
    {
        return Err(syn::Error::new(
            name.span(),
            format!(
                "domain-event name contains broker wildcard `{wildcard}`; `*`, `#`, and `>` are reserved routing operators"
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod schema_tests {
    use super::{schema_fingerprint, validate_domain_event_name_literal};
    use syn::LitStr;

    #[test]
    fn schema_fingerprint_matches_sha256_reference_vector() {
        assert_eq!(
            schema_fingerprint("abc"),
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn domain_event_name_validation_matches_runtime_boundaries() {
        let boundary = LitStr::new(&"a".repeat(256), proc_macro2::Span::call_site());
        let over = LitStr::new(&"a".repeat(257), proc_macro2::Span::call_site());

        assert!(validate_domain_event_name_literal(&boundary).is_ok());
        assert!(validate_domain_event_name_literal(&over).is_err());
    }
}
