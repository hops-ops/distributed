use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{Data, DeriveInput, Fields, LitInt, LitStr};

use crate::shared::{
    canonical_object_schema, framework_path, projection_body_metadata_tokens, schema_fingerprint,
    validate_domain_event_name_literal,
};

pub(crate) fn derive_domain_event(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as DeriveInput);
    expand_domain_event(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

pub(crate) fn expand_domain_event(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let framework = framework_path()?;
    let (event_name, version) = parse_domain_event_descriptor(&input)?;
    let fields = named_fields(&input)?;
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "DomainEvent derive does not support generic event DTOs in version one",
        ));
    }

    let name = &input.ident;
    let type_name = name.to_string();
    let schema_fields = fields
        .named
        .iter()
        .map(|field| {
            let name = field.ident.as_ref().ok_or_else(|| {
                syn::Error::new_spanned(field, "domain-event field must be named")
            })?;
            Ok((name.to_string(), field.ty.clone(), field.attrs.clone()))
        })
        .collect::<syn::Result<Vec<_>>>()?;
    let schema = canonical_object_schema(
        "domain_event",
        &type_name,
        version,
        &input.attrs,
        schema_fields,
    );
    let fingerprint = schema_fingerprint(&schema);
    let projection_metadata = projection_body_metadata_tokens(
        &framework,
        "domain_event",
        &type_name,
        version,
        &input.attrs,
        &fields.named,
    )?;
    let type_name = LitStr::new(&type_name, Span::call_site());
    let schema = LitStr::new(&schema, Span::call_site());
    let fingerprint = LitStr::new(&fingerprint, Span::call_site());

    Ok(quote! {
        impl #framework::domain_event::DomainEvent for #name {
            const DESCRIPTOR: #framework::domain_event::DomainEventDescriptor =
                #framework::domain_event::DomainEventDescriptor {
                    name: std::borrow::Cow::Borrowed(#event_name),
                    version: #version,
                    body: #framework::domain_event::DomainEventBodyDescriptor::distributed_json(
                        #framework::domain_event::DomainEventBodyKind::Event,
                        #type_name,
                        #version,
                        #schema,
                        #fingerprint,
                    ),
                };
        }

        impl #framework::domain_event::DomainEventContract for #name {
            const EVENT_NAME: &'static str = #event_name;
            const EVENT_VERSION: u64 = #version;

            fn descriptor() -> #framework::domain_event::DomainEventDescriptor {
                <Self as #framework::domain_event::DomainEvent>::DESCRIPTOR.clone()
            }
        }

        impl #framework::domain_event::DomainEventBodyContract<Self> for #name {}

        impl #framework::command::CommandProjectionBody for #name {
            fn command_projection_descriptor(
                _: &'static str,
                _: u64,
            ) -> #framework::domain_event::DomainEventDescriptor {
                <Self as #framework::domain_event::DomainEvent>::DESCRIPTOR.clone()
            }
        }

        impl #framework::projection::lower::ProjectionBodyMetadata for #name {
            #projection_metadata
        }
    })
}

fn parse_domain_event_descriptor(input: &DeriveInput) -> syn::Result<(LitStr, u64)> {
    let mut name = None;
    let mut version = None;
    let mut seen_attribute = false;

    for attr in &input.attrs {
        if !attr.path().is_ident("domain_event") {
            continue;
        }
        if seen_attribute {
            return Err(syn::Error::new_spanned(
                attr,
                "duplicate #[domain_event(...)] attribute",
            ));
        }
        seen_attribute = true;
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                if name.is_some() {
                    return Err(meta.error("duplicate `name` in #[domain_event(...)]"));
                }
                let value: LitStr = meta.value()?.parse()?;
                validate_domain_event_name_literal(&value)?;
                name = Some(value);
                Ok(())
            } else if meta.path.is_ident("version") {
                if version.is_some() {
                    return Err(meta.error("duplicate `version` in #[domain_event(...)]"));
                }
                let value: LitInt = meta.value()?.parse()?;
                let parsed = value.base10_parse::<u64>()?;
                if parsed == 0 {
                    return Err(syn::Error::new(
                        value.span(),
                        "domain-event version must be greater than zero",
                    ));
                }
                version = Some(parsed);
                Ok(())
            } else {
                Err(meta
                    .error("unsupported key in #[domain_event(...)]; expected `name` or `version`"))
            }
        })?;
    }

    if !seen_attribute {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "DomainEvent derive requires #[domain_event(name = \"...\", version = N)]",
        ));
    }
    let name = name.ok_or_else(|| {
        syn::Error::new_spanned(
            &input.ident,
            "#[domain_event(...)] requires `name = \"...\"`",
        )
    })?;
    let version = version.ok_or_else(|| {
        syn::Error::new_spanned(&input.ident, "#[domain_event(...)] requires `version = N`")
    })?;
    Ok((name, version))
}

fn named_fields(input: &DeriveInput) -> syn::Result<&syn::FieldsNamed> {
    match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => Ok(fields),
            other => Err(syn::Error::new_spanned(
                other,
                "DomainEvent derive requires a struct with named fields",
            )),
        },
        _ => Err(syn::Error::new_spanned(
            input,
            "DomainEvent derive can only be used on structs with named fields",
        )),
    }
}
