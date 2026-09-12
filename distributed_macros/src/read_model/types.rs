use quote::{quote, ToTokens};
use syn::{Field, GenericArgument, PathArguments, Type};

pub(super) fn option_string_tokens(value: Option<&str>) -> proc_macro2::TokenStream {
    match value {
        Some(value) => quote! { Some(#value.to_string()) },
        None => quote! { None },
    }
}

pub(super) fn column_type_tokens(ty: &Type, jsonb: bool, text: bool) -> proc_macro2::TokenStream {
    if jsonb {
        return quote! { distributed::ColumnType::Json };
    }
    if text {
        return quote! { distributed::ColumnType::Text };
    }

    let ty = option_inner_type(ty).unwrap_or(ty);
    if let Some(last) = last_type_segment(ty) {
        let ident = last.ident.to_string();
        return match ident.as_str() {
            "String" | "str" => quote! { distributed::ColumnType::Text },
            "bool" => quote! { distributed::ColumnType::Boolean },
            "i8" | "i16" | "i32" | "i64" | "isize" => {
                quote! { distributed::ColumnType::Integer }
            }
            "u8" | "u16" | "u32" | "u64" | "usize" => {
                quote! { distributed::ColumnType::UnsignedInteger }
            }
            "f32" | "f64" => quote! { distributed::ColumnType::Float },
            "Vec" => {
                if vec_inner_is_u8(last) {
                    quote! { distributed::ColumnType::Bytes }
                } else {
                    quote! { distributed::ColumnType::Json }
                }
            }
            "HashMap" | "BTreeMap" | "Value" => quote! { distributed::ColumnType::Json },
            _ => {
                let type_name = ty.to_token_stream().to_string();
                quote! { distributed::ColumnType::Unsupported(#type_name.to_string()) }
            }
        };
    }

    let type_name = ty.to_token_stream().to_string();
    quote! { distributed::ColumnType::Unsupported(#type_name.to_string()) }
}

pub(super) fn effect_model_wire_tokens(
    ty: &Type,
    jsonb: bool,
    text: bool,
) -> proc_macro2::TokenStream {
    if jsonb {
        return quote! { distributed::command::EffectWireJson };
    }
    if text {
        return quote! { distributed::command::EffectWireString };
    }
    let ty = option_inner_type(ty).unwrap_or(ty);
    let Some(last) = last_type_segment(ty) else {
        return quote! { distributed::command::EffectWireUnsupported };
    };
    match last.ident.to_string().as_str() {
        "String" | "str" => quote! { distributed::command::EffectWireString },
        "bool" => quote! { distributed::command::EffectWireBoolean },
        "i8" | "i16" | "i32" | "i64" | "isize" | "u8" | "u16" | "u32" | "u64" | "usize" => {
            quote! { distributed::command::EffectWireBigInt }
        }
        "f32" | "f64" => quote! { distributed::command::EffectWireFloat },
        "Vec" if vec_inner_is_u8(last) => quote! { distributed::command::EffectWireBytea },
        "Vec" | "HashMap" | "BTreeMap" | "Value" => {
            quote! { distributed::command::EffectWireJson }
        }
        _ => quote! { distributed::command::EffectWireUnsupported },
    }
}

pub(super) fn bytes_row_value_tokens(
    ty: &Type,
    value: proc_macro2::TokenStream,
) -> Option<proc_macro2::TokenStream> {
    let option_inner = option_inner_type(ty);
    let ty = option_inner.unwrap_or(ty);
    let segment = last_type_segment(ty)?;
    if segment.ident != "Vec" || !vec_inner_is_u8(segment) {
        return None;
    }

    if option_inner.is_some() {
        Some(quote! {
            match &#value {
                Some(value) => distributed::RowValue::Bytes(value.clone()),
                None => distributed::RowValue::Null,
            }
        })
    } else {
        Some(quote! {
            distributed::RowValue::Bytes(#value.clone())
        })
    }
}

pub(super) fn option_inner_type(ty: &Type) -> Option<&Type> {
    let segment = last_type_segment(ty)?;
    if segment.ident != "Option" {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    args.args.iter().find_map(|arg| match arg {
        GenericArgument::Type(ty) => Some(ty),
        _ => None,
    })
}

pub(super) fn vec_inner_type(ty: &Type) -> Option<&Type> {
    let segment = last_type_segment(ty)?;
    if segment.ident != "Vec" {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    args.args.iter().find_map(|arg| match arg {
        GenericArgument::Type(ty) => Some(ty),
        _ => None,
    })
}

/// One allocation wrapper for a singular relationship, not a model or column.
pub(super) fn box_inner_type(ty: &Type) -> Option<&Type> {
    let segment = last_type_segment(ty)?;
    if segment.ident != "Box" {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    if args.args.len() != 1 {
        return None;
    }
    match args.args.first()? {
        GenericArgument::Type(ty) => Some(ty),
        _ => None,
    }
}

pub(super) fn validate_relationship_target_type(
    field: &Field,
    ty: &Type,
    target_model: &str,
    field_name: &str,
) -> syn::Result<()> {
    let Some(segment) = last_type_segment(ty) else {
        return Err(syn::Error::new_spanned(
            field,
            format!("relationship `{field_name}` target type must be a named read model"),
        ));
    };
    if segment.ident != target_model {
        return Err(syn::Error::new_spanned(
            field,
            format!(
                "relationship `{field_name}` targets `{target_model}` but the field stores `{}`",
                segment.ident
            ),
        ));
    }
    Ok(())
}

pub(super) fn last_type_segment(ty: &Type) -> Option<&syn::PathSegment> {
    match ty {
        Type::Path(path) => path.path.segments.last(),
        Type::Reference(reference) => last_type_segment(&reference.elem),
        _ => None,
    }
}

pub(super) fn vec_inner_is_u8(segment: &syn::PathSegment) -> bool {
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return false;
    };
    args.args.iter().any(|arg| match arg {
        GenericArgument::Type(Type::Path(path)) => path
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "u8"),
        _ => false,
    })
}

pub(super) fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.extend(ch.to_lowercase());
        } else {
            result.push(ch);
        }
    }
    result
}

/// Infers a read model's default physical storage name.
///
/// A snake-cased model name ending in `s` is treated as plural. Singular nouns
/// that also end in `s` must use an explicit `#[table("...")]` or
/// `#[collection("...")]` override when their storage name differs.
pub(super) fn default_storage_name(model_name: &str) -> String {
    let mut storage_name = to_snake_case(model_name);
    if !storage_name.ends_with('s') {
        storage_name.push('s');
    }
    storage_name
}
