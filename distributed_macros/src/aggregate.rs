use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    braced,
    parse::{Parse, ParseStream},
    Ident, LitStr, Token,
};

/// Emit the `impl distributed::Aggregate` block shared by `aggregate!` and
/// `#[sourced]`.
///
/// Both entry points produce a byte-identical impl: same associated
/// `ReplayError = String`, same `entity`/`entity_mut`/`replay_event` bodies, and
/// the same optional `aggregate_type` and upcasters methods. The version and
/// replay match arms are built upstream, so this helper takes them (already
/// rendered) along with the type name and entity field. Keeping the version
/// fence here prevents the replay semantics of the two macros from drifting.
///
/// It emits only the `impl` block; callers still place `#upcaster_wrappers`
/// (the free upcaster fns) where they already do.
pub(crate) fn aggregate_impl_tokens(
    type_name: &Ident,
    entity_field: &Ident,
    aggregate_type_method: &Option<TokenStream2>,
    version_arms: &[TokenStream2],
    replay_arms: &[TokenStream2],
    upcasters_method: &TokenStream2,
) -> TokenStream2 {
    quote! {
        impl distributed::Aggregate for #type_name {
            type ReplayError = String;

            #aggregate_type_method

            fn entity(&self) -> &distributed::Entity {
                &self.#entity_field
            }

            fn entity_mut(&mut self) -> &mut distributed::Entity {
                &mut self.#entity_field
            }

            fn replay_event(
                &mut self,
                event: &distributed::EventRecord,
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

// ============================================================================
// aggregate! proc-macro
// ============================================================================
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
                    event: &distributed::EventRecord,
                ) -> Result<Vec<u8>, distributed::UpcastError> {
                    distributed::upcast_payload::<#source_type, #target_type>(
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
                distributed::EventUpcaster {
                    event_type: #event_name,
                    from_version: #from_version,
                    to_version: #to_version,
                    transform: #owner::#wrapper,
                }
            }
        });

    let upcasters_method = quote! {
        fn upcasters() -> &'static [distributed::EventUpcaster] {
            static UPCASTERS: &[distributed::EventUpcaster] = &[
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

// ============================================================================
// #[enqueue] attribute macro
// ============================================================================
pub(crate) fn expand_aggregate(input: TokenStream2) -> syn::Result<TokenStream2> {
    let input = syn::parse2::<AggregateInput>(input)?;

    let agg_name = &input.agg_name;
    let entity_field = &input.entity_field;
    let version_arms: Vec<_> = input
        .events
        .iter()
        .map(|event| {
            let name = &event.event_name;
            let version = &event.version;
            quote! { #name => #version, }
        })
        .collect();

    // Generate replay match arms - deserialize and call method directly
    let replay_arms: Vec<_> = input
        .events
        .iter()
        .map(|evt| {
            let event_name = &evt.event_name;
            let method_name = &evt.method_name;
            let args = &evt.args;

            // Determine what args to pass to the method
            let call_args: Vec<_> = match &evt.method_args {
                Some(method_args) => method_args.clone(),
                None => args.clone(),
            };

            if args.is_empty() {
                // No payload
                quote! {
                    #event_name => {
                        self.#method_name().map_err(|e| e.to_string())?;
                    }
                }
            } else if call_args.is_empty() {
                // Event has payload but method takes no args
                quote! {
                    #event_name => {
                        self.#method_name().map_err(|e| e.to_string())?;
                    }
                }
            } else if args.len() == 1 {
                // Single-element tuple needs trailing comma: (x,) not (x)
                let arg = &args[0];
                let call_arg = &call_args[0];
                quote! {
                    #event_name => {
                        let (#arg,) = event.decode().map_err(|e| e.to_string())?;
                        self.#method_name(#call_arg).map_err(|e| e.to_string())?;
                    }
                }
            } else {
                // Multi-element tuple
                quote! {
                    #event_name => {
                        let (#(#args),*) = event.decode().map_err(|e| e.to_string())?;
                        self.#method_name(#(#call_args),*).map_err(|e| e.to_string())?;
                    }
                }
            }
        })
        .collect();

    let (upcaster_wrappers, upcasters_method) =
        generate_upcaster_tokens(agg_name, &input.upcasters);
    let aggregate_type_method = input.aggregate_type.as_ref().map(|aggregate_type| {
        quote! {
            fn aggregate_type() -> &'static str {
                #aggregate_type
            }
        }
    });

    // ReplayError stays `String`: replay errors are flattened from
    // heterogeneous sources (per-event `decode()` errors, user method errors of
    // arbitrary `E`, and unknown-event messages) via `e.to_string()`. A typed
    // error would have to be generic over each method's error type or erase
    // them anyway, so `String` is the smaller, honest representation here.
    let aggregate_impl = aggregate_impl_tokens(
        agg_name,
        entity_field,
        &aggregate_type_method,
        &version_arms,
        &replay_arms,
        &upcasters_method,
    );
    let expanded = quote! {
        #upcaster_wrappers

        #aggregate_impl
    };

    Ok(expanded)
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

struct AggregateInput {
    agg_name: Ident,
    entity_field: Ident,
    aggregate_type: Option<LitStr>,
    events: Vec<EventDef>,
    upcasters: Vec<UpcasterDef>,
}

struct EventDef {
    event_name: LitStr,
    version: syn::LitInt,
    args: Vec<Ident>,
    method_name: Ident,
    method_args: Option<Vec<Ident>>, // None = use event args, Some([]) = no args, Some([x,y]) = specific args
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

impl Parse for AggregateInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let agg_name: Ident = input.parse()?;
        input.parse::<Token![,]>()?;
        let entity_field: Ident = input.parse()?;
        let mut aggregate_type = None;

        if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            let kw: Ident = input.parse()?;
            if kw != "aggregate_type" {
                return Err(syn::Error::new(kw.span(), "expected `aggregate_type`"));
            }
            input.parse::<Token![=]>()?;
            let lit = input.parse::<LitStr>()?;
            validate_aggregate_type_literal(&lit)?;
            aggregate_type = Some(lit);
        }

        let content;
        braced!(content in input);

        let mut events = Vec::new();
        while !content.is_empty() {
            let event_name: LitStr = content.parse()?;

            // Parse (arg1, arg2, ...)
            let args_content;
            syn::parenthesized!(args_content in content);
            let args: syn::punctuated::Punctuated<Ident, Token![,]> =
                args_content.parse_terminated(Ident::parse, Token![,])?;
            let args: Vec<Ident> = args.into_iter().collect();

            // `"renamed"(name), version = 2 => rename`; omitted versions
            // have the same v1 default as #[digest] and #[event].
            let version = if content.peek(Token![,]) {
                content.parse::<Token![,]>()?;
                let keyword: Ident = content.parse()?;
                if keyword != "version" {
                    return Err(syn::Error::new(keyword.span(), "expected `version`"));
                }
                content.parse::<Token![=]>()?;
                content.parse::<syn::LitInt>()?
            } else {
                syn::LitInt::new("1", event_name.span())
            };
            content.parse::<Token![=>]>()?;
            let method_name: Ident = content.parse()?;

            // Check for optional method args: method() or method(a, b)
            let method_args = if content.peek(syn::token::Paren) {
                let method_args_content;
                syn::parenthesized!(method_args_content in content);
                let method_args: syn::punctuated::Punctuated<Ident, Token![,]> =
                    method_args_content.parse_terminated(Ident::parse, Token![,])?;
                Some(method_args.into_iter().collect())
            } else {
                None // Use event args
            };

            events.push(EventDef {
                event_name,
                version,
                args,
                method_name,
                method_args,
            });

            // Optional trailing comma
            if content.peek(Token![,]) {
                content.parse::<Token![,]>()?;
            }
        }

        // Parse optional `upcasters [...]` block
        let mut upcasters = Vec::new();
        if input.peek(syn::Ident) {
            let kw: Ident = input.parse()?;
            if kw != "upcasters" {
                return Err(syn::Error::new(kw.span(), "expected `upcasters`"));
            }

            let upcaster_content;
            syn::bracketed!(upcaster_content in input);
            upcasters = parse_upcaster_list(&upcaster_content)?;
        }

        Ok(AggregateInput {
            agg_name,
            entity_field,
            aggregate_type,
            events,
            upcasters,
        })
    }
}

// ============================================================================
// #[sourced] attribute macro
// ============================================================================
