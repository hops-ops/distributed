use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    parse::{Parse, ParseStream},
    Ident, LitStr, Token,
};

/// Emit the `impl distributed::Aggregate` block of `#[sourced]`: the version
/// fence, then the replay match. The arms are built by the caller.
///
/// It emits only the `impl` block; callers still place `#upcaster_wrappers`
/// (the free upcaster fns) where they already do.
pub(crate) fn aggregate_impl_tokens(
    framework: &TokenStream2,
    type_name: &Ident,
    entity_field: &Ident,
    aggregate_type_method: &Option<TokenStream2>,
    version_arms: &[TokenStream2],
    replay_arms: &[TokenStream2],
    upcasters_method: &TokenStream2,
) -> TokenStream2 {
    quote! {
        impl #framework::Aggregate for #type_name {
            type ReplayError = String;

            #aggregate_type_method

            fn entity(&self) -> &#framework::Entity {
                &self.#entity_field
            }

            fn entity_mut(&mut self) -> &mut #framework::Entity {
                &mut self.#entity_field
            }

            fn replay_event(
                &mut self,
                event: &#framework::EventRecord,
            ) -> Result<(), Self::ReplayError> {
                // Hydration runs upcasters first. Payload compatibility alone
                // cannot establish that an event has the handler's semantics.
                let expected_version: u64 = match event.event_name.as_str() {
                    #(#version_arms)*
                    _ => return Err(format!("Unknown event: {}", event.event_name)),
                };
                if event.event_version != expected_version {
                    return Err(format!(
                        "Unsupported event version for {}: expected {}, got {}",
                        event.event_name, expected_version, event.event_version,
                    ));
                }
                match event.event_name.as_str() {
                    #(#replay_arms)*
                    _ => return Err(format!("Unknown event: {}", event.event_name)),
                }
                Ok(())
            }

            #upcasters_method
        }
    }
}

fn upcaster_wrapper_prefix(owner: &Ident) -> String {
    owner
        .to_string()
        .trim_start_matches("r#")
        .to_ascii_lowercase()
}

pub(crate) fn event_variant_ident(event_name: &LitStr) -> Ident {
    let value = event_name.value();
    let segment = value
        .rsplit('.')
        .find(|part| !part.is_empty())
        .unwrap_or(&value);
    let mut ident = String::new();
    let mut capitalize_next = true;

    for ch in segment.chars() {
        if ch.is_ascii_alphanumeric() {
            if ident.is_empty() && ch.is_ascii_digit() {
                ident.push_str("Event");
            }

            if capitalize_next {
                ident.push(ch.to_ascii_uppercase());
                capitalize_next = false;
            } else {
                ident.push(ch);
            }
        } else {
            capitalize_next = true;
        }
    }

    if ident.is_empty() {
        ident.push_str("Event");
    }

    format_ident!("{}", ident)
}

pub(crate) fn generate_upcaster_tokens(
    framework: &TokenStream2,
    owner: &Ident,
    upcasters: &[UpcasterDef],
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    if upcasters.is_empty() {
        return (quote! {}, quote! {});
    }

    let prefix = upcaster_wrapper_prefix(owner);
    let wrapper_names: Vec<_> = upcasters
        .iter()
        .enumerate()
        .map(|(idx, _)| format_ident!("__sourced_upcast_{}_{}", prefix, idx))
        .collect();

    let wrapper_defs = upcasters
        .iter()
        .zip(wrapper_names.iter())
        .map(|(u, wrapper)| {
            let source_type = &u.source_type;
            let target_type = &u.target_type;
            let to_version = &u.to_version;
            let transform_fn = &u.transform_fn;
            quote! {
                fn #wrapper(
                    event: &#framework::EventRecord,
                ) -> Result<Vec<u8>, #framework::UpcastError> {
                    #framework::upcast_payload::<#source_type, #target_type>(
                        event,
                        #to_version,
                        #transform_fn,
                    )
                }
            }
        });

    let upcaster_entries = upcasters
        .iter()
        .zip(wrapper_names.iter())
        .map(|(u, wrapper)| {
            let event_name = &u.event_name;
            let from_version = &u.from_version;
            let to_version = &u.to_version;
            quote! {
                #framework::EventUpcaster {
                    event_type: #event_name,
                    from_version: #from_version,
                    to_version: #to_version,
                    transform: #owner::#wrapper,
                }
            }
        });

    let upcasters_method = quote! {
        fn upcasters() -> &'static [#framework::EventUpcaster] {
            static UPCASTERS: &[#framework::EventUpcaster] = &[
                #(#upcaster_entries),*
            ];
            UPCASTERS
        }
    };

    (
        quote! {
            impl #owner {
                #(#wrapper_defs)*
            }
        },
        upcasters_method,
    )
}

pub(crate) struct UpcasterDef {
    pub(crate) event_name: LitStr,
    pub(crate) from_version: syn::LitInt,
    pub(crate) to_version: syn::LitInt,
    source_type: syn::Type,
    target_type: syn::Type,
    transform_fn: syn::Path,
}

impl Parse for UpcasterDef {
    /// Parses one upcaster entry:
    /// `("event.name", from => to, SourceType => TargetType, transform_fn)`.
    ///
    /// Shared by `aggregate!` and `#[sourced(..., upcasters(...))]` so the
    /// upcaster grammar cannot drift between the two entry points.
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let inner;
        syn::parenthesized!(inner in input);

        let event_name: LitStr = inner.parse()?;
        inner.parse::<Token![,]>()?;
        let from_version: syn::LitInt = inner.parse()?;
        inner.parse::<Token![=>]>()?;
        let to_version: syn::LitInt = inner.parse()?;
        inner.parse::<Token![,]>()?;
        let source_type: syn::Type = inner.parse()?;
        inner.parse::<Token![=>]>()?;
        let target_type: syn::Type = inner.parse()?;
        inner.parse::<Token![,]>()?;
        let transform_fn: syn::Path = inner.parse()?;

        Ok(UpcasterDef {
            event_name,
            from_version,
            to_version,
            source_type,
            target_type,
            transform_fn,
        })
    }
}

/// Parse a sequence of parenthesized upcaster entries with optional trailing
/// commas, until `content` is exhausted.
pub(crate) fn parse_upcaster_list(content: ParseStream) -> syn::Result<Vec<UpcasterDef>> {
    let mut upcasters = Vec::new();
    while !content.is_empty() {
        upcasters.push(content.parse::<UpcasterDef>()?);
        // Optional trailing comma between upcaster entries
        if content.peek(Token![,]) {
            content.parse::<Token![,]>()?;
        }
    }
    Ok(upcasters)
}

pub(crate) fn validate_aggregate_type_literal(lit: &LitStr) -> syn::Result<()> {
    let value = lit.value();
    if value.trim().is_empty() {
        return Err(syn::Error::new_spanned(
            lit,
            "`aggregate_type` must not be empty or whitespace",
        ));
    }
    if value.contains('\u{1f}') {
        return Err(syn::Error::new_spanned(
            lit,
            "`aggregate_type` must not contain the reserved stream delimiter (U+001F)",
        ));
    }
    Ok(())
}
