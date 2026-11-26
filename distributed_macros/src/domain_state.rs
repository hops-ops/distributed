use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{Data, DeriveInput, Fields, LitInt, LitStr};

use crate::shared::{
    canonical_object_schema, framework_path, projection_body_metadata_tokens, schema_fingerprint,
};

pub(crate) fn derive_domain_state(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as DeriveInput);
    expand_domain_state(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

pub(crate) fn expand_domain_state(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let framework = framework_path()?;
    let version = parse_domain_state_version(&input)?;
    let fields = named_fields(&input)?;
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "DomainState derive does not support generic state DTOs in version one",
        ));
    }

    let name = &input.ident;
    let type_name = name.to_string();
    let schema_fields = fields
        .named
        .iter()
        .map(|field| {
            let name = field.ident.as_ref().ok_or_else(|| {
                syn::Error::new_spanned(field, "domain-state field must be named")
            })?;
            Ok((name.to_string(), field.ty.clone(), field.attrs.clone()))
        })
        .collect::<syn::Result<Vec<_>>>()?;
    let schema = canonical_object_schema(
        "domain_state",
        &type_name,
        version,
        &input.attrs,
        schema_fields,
    );
    let fingerprint = schema_fingerprint(&schema);
    let projection_metadata = projection_body_metadata_tokens(
        &framework,
        "domain_state",
        &type_name,
        version,
        &input.attrs,
        &fields.named,
    )?;
    let type_name = LitStr::new(&type_name, Span::call_site());
    let schema = LitStr::new(&schema, Span::call_site());
    let fingerprint = LitStr::new(&fingerprint, Span::call_site());

    Ok(quote! {
        impl #framework::domain_event::DomainState for #name {
            const DESCRIPTOR: #framework::domain_event::DomainStateDescriptor =
                #framework::domain_event::DomainStateDescriptor::distributed_json(
                    #type_name,
                    #version,
                    #schema,
                    #fingerprint,
                );
        }

        impl #framework::projection::lower::ProjectionBodyMetadata for #name {
            #projection_metadata
        }
    })
}

fn parse_domain_state_version(input: &DeriveInput) -> syn::Result<u64> {
    let mut version = None;
    let mut seen_attribute = false;

    for attr in &input.attrs {
        if !attr.path().is_ident("domain_state") {
            continue;
        }
        if seen_attribute {
            return Err(syn::Error::new_spanned(
                attr,
                "duplicate #[domain_state(...)] attribute",
            ));
        }
        seen_attribute = true;
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("version") {
                if version.is_some() {
                    return Err(meta.error("duplicate `version` in #[domain_state(...)]"));
                }
                let value: LitInt = meta.value()?.parse()?;
                let parsed = value.base10_parse::<u64>()?;
                if parsed == 0 {
                    return Err(syn::Error::new(
                        value.span(),
                        "domain-state version must be greater than zero",
                    ));
                }
                version = Some(parsed);
                Ok(())
            } else if meta.path.is_ident("name") {
                Err(meta.error(
                    "domain state is a body schema, not an event; remove `name` and keep the semantic name on #[event(...)]",
                ))
            } else {
                Err(meta.error(
                    "unsupported key in #[domain_state(...)]; expected only `version`",
                ))
            }
        })?;
    }

    if !seen_attribute {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "DomainState derive requires #[domain_state(version = N)]",
        ));
    }
    version.ok_or_else(|| {
        syn::Error::new_spanned(&input.ident, "#[domain_state(...)] requires `version = N`")
    })
}

fn named_fields(input: &DeriveInput) -> syn::Result<&syn::FieldsNamed> {
    match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => Ok(fields),
            other => Err(syn::Error::new_spanned(
                other,
                "DomainState derive requires a struct with named fields",
            )),
        },
        _ => Err(syn::Error::new_spanned(
            input,
            "DomainState derive can only be used on structs with named fields",
        )),
    }
}
