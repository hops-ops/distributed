//! `#[derive(JsonType)]`: the serialized JSON shape of a named-field struct or
//! a unit-only enum, following its Serde names.
//!
//! Only Serde attributes that keep the shape exact are accepted. One that
//! changes or conditions the serialized form (`flatten`, `skip*`, `with`,
//! `untagged`, `tag`, `transparent`, `into`, …) is a compile error: the shape
//! would no longer describe the JSON.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{punctuated::Punctuated, Attribute, Data, DeriveInput, Fields, Meta, Token};

use crate::command_types::{serde_field_rename, serde_rename_all, RenameRule, SerdeDirection};

pub fn expand(input: DeriveInput) -> syn::Result<TokenStream> {
    let framework = crate::shared::framework_path()?;
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "JsonType cannot be derived for a generic type",
        ));
    }
    reject_shape_changing(&input.attrs, CONTAINER)?;
    let rename_all = serde_rename_all(&input.attrs, SerdeDirection::Serialize)?;
    let name = &input.ident;
    let shape = match &input.data {
        Data::Struct(data) => {
            let Fields::Named(fields) = &data.fields else {
                return Err(syn::Error::new_spanned(
                    &data.fields,
                    "JsonType requires named fields (a tuple or unit struct has no field names)",
                ));
            };
            let fields = fields
                .named
                .iter()
                .map(|field| {
                    reject_shape_changing(&field.attrs, FIELD)?;
                    let ident = field.ident.as_ref().expect("named field");
                    let rust_name = ident.to_string();
                    let rust_name = rust_name.strip_prefix("r#").unwrap_or(&rust_name);
                    let wire = match serde_field_rename(&field.attrs, SerdeDirection::Serialize)? {
                        Some(name) => name,
                        None => rename_all
                            .map_or_else(|| rust_name.to_string(), |rule| rule.apply(rust_name)),
                    };
                    let ty = &field.ty;
                    Ok(quote! {
                        #framework::table::JsonField {
                            name: ::std::string::String::from(#wire),
                            shape: <#ty as #framework::table::JsonType>::json_shape(),
                        }
                    })
                })
                .collect::<syn::Result<Vec<_>>>()?;
            quote! { #framework::table::JsonShape::Object { fields: ::std::vec![#(#fields),*] } }
        }
        Data::Enum(data) => {
            if data.variants.is_empty() {
                return Err(syn::Error::new_spanned(
                    name,
                    "JsonType requires at least one enum variant",
                ));
            }
            let values = data
                .variants
                .iter()
                .map(|variant| {
                    if !matches!(variant.fields, Fields::Unit) {
                        return Err(syn::Error::new_spanned(
                            variant,
                            "JsonType enums must be unit-only: they serialize as one string",
                        ));
                    }
                    reject_shape_changing(&variant.attrs, VARIANT)?;
                    let rust_name = variant.ident.to_string();
                    Ok(
                        match serde_field_rename(&variant.attrs, SerdeDirection::Serialize)? {
                            Some(name) => name,
                            None => rename_all
                                .map_or(rust_name.clone(), |rule| rename_variant(rule, &rust_name)),
                        },
                    )
                })
                .collect::<syn::Result<Vec<_>>>()?;
            quote! {
                #framework::table::JsonShape::Enum {
                    values: ::std::vec![#(::std::string::String::from(#values)),*],
                }
            }
        }
        Data::Union(data) => {
            return Err(syn::Error::new_spanned(
                data.union_token,
                "JsonType cannot be derived for a union",
            ));
        }
    };
    Ok(quote! {
        impl #framework::table::JsonType for #name {
            fn json_shape() -> #framework::table::JsonShape {
                #shape
            }
        }
    })
}

/// Serde's variant rule: `rename_all` renames the PascalCase variant name.
fn rename_variant(rule: RenameRule, variant: &str) -> String {
    match rule {
        RenameRule::LowerCase => variant.to_ascii_lowercase(),
        RenameRule::UpperCase => variant.to_ascii_uppercase(),
        RenameRule::PascalCase => variant.to_string(),
        rule => {
            let mut snake = String::new();
            for (index, ch) in variant.char_indices() {
                if index > 0 && ch.is_ascii_uppercase() {
                    snake.push('_');
                }
                snake.push(ch.to_ascii_lowercase());
            }
            rule.apply(&snake)
        }
    }
}

const CONTAINER: &[&str] = &[
    "transparent",
    "untagged",
    "tag",
    "content",
    "into",
    "remote",
];
const FIELD: &[&str] = &[
    "flatten",
    "skip",
    "skip_serializing",
    "skip_serializing_if",
    "with",
    "serialize_with",
    "getter",
];
const VARIANT: &[&str] = &[
    "skip",
    "skip_serializing",
    "with",
    "serialize_with",
    "other",
];

fn reject_shape_changing(attrs: &[Attribute], rejected: &[&str]) -> syn::Result<()> {
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("serde")) {
        let metas = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
        for meta in metas {
            if let Some(name) = rejected.iter().find(|name| meta.path().is_ident(name)) {
                return Err(syn::Error::new_spanned(
                    meta,
                    format!(
                        "#[serde({name})] is not supported by JsonType because it changes the serialized shape; define a separate wire type"
                    ),
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variant_rules_follow_serde() {
        assert_eq!(
            rename_variant(RenameRule::SnakeCase, "DeadByHole"),
            "dead_by_hole"
        );
        assert_eq!(
            rename_variant(RenameRule::KebabCase, "DeadByHole"),
            "dead-by-hole"
        );
        assert_eq!(
            rename_variant(RenameRule::LowerCase, "DeadByHole"),
            "deadbyhole"
        );
        assert_eq!(
            rename_variant(RenameRule::CamelCase, "DeadByHole"),
            "deadByHole"
        );
        assert_eq!(
            rename_variant(RenameRule::ScreamingSnakeCase, "DeadByHole"),
            "DEAD_BY_HOLE"
        );
    }

    #[test]
    fn shape_changing_serde_attributes_fail_closed() {
        let flatten: DeriveInput = syn::parse_quote! {
            struct Outer { #[serde(flatten)] inner: Inner }
        };
        let error = expand(flatten).unwrap_err().to_string();
        assert!(error.contains("serde(flatten)"), "{error}");
        let tagged: DeriveInput = syn::parse_quote! {
            #[serde(tag = "kind")]
            enum Kind { A }
        };
        assert!(expand(tagged).is_err());
        let data: DeriveInput = syn::parse_quote! {
            enum Kind { A(u8) }
        };
        assert!(expand(data).unwrap_err().to_string().contains("unit-only"));
    }
}
