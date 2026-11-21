use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    parse::{ParseStream, Parser},
    visit::Visit,
    Expr, FnArg, Ident, ItemImpl, LitStr, Path, Token, Type,
};

use crate::aggregate::{
    aggregate_impl_tokens, event_variant_ident, generate_upcaster_tokens, parse_upcaster_list,
    validate_aggregate_type_literal, UpcasterDef,
};
use crate::shared::{
    canonical_object_schema, ensure_infallible_recorder_signature, extract_params_with_types,
    framework_path, generate_digest_call, projection_body_metadata_tokens, schema_fingerprint,
    validate_domain_event_name_literal, wrap_recorder_body,
};

pub(crate) struct SourcedArgs {
    entity_field: Ident,
    enum_name: Option<LitStr>,
    aggregate_type: Option<LitStr>,
    domain_state: Option<Type>,
    pub(crate) upcasters: Vec<UpcasterDef>,
}

pub(crate) fn parse_sourced_args(input: ParseStream) -> syn::Result<SourcedArgs> {
    if input.is_empty() {
        return Err(
            input.error("#[sourced] requires the entity field name, e.g. `#[sourced(entity)]`")
        );
    }
    let entity_field: Ident = input.parse()?;
    let mut enum_name = None;
    let mut aggregate_type = None;
    let mut domain_state = None;
    let mut upcasters = Vec::new();

    while input.peek(Token![,]) {
        input.parse::<Token![,]>()?;
        // Allow (and ignore) a trailing comma.
        if input.is_empty() {
            break;
        }
        let kw: Ident = input.parse()?;
        if kw == "events" {
            input.parse::<Token![=]>()?;
            enum_name = Some(input.parse::<LitStr>()?);
        } else if kw == "aggregate_type" {
            input.parse::<Token![=]>()?;
            let lit = input.parse::<LitStr>()?;
            validate_aggregate_type_literal(&lit)?;
            aggregate_type = Some(lit);
        } else if kw == "domain_state" {
            if domain_state.is_some() {
                return Err(syn::Error::new_spanned(
                    &kw,
                    "duplicate `domain_state` in #[sourced(...)]",
                ));
            }
            input.parse::<Token![=]>()?;
            domain_state = Some(input.parse::<Type>()?);
        } else if kw == "upcasters" {
            let upcaster_content;
            syn::parenthesized!(upcaster_content in input);
            upcasters = parse_upcaster_list(&upcaster_content)?;
        } else {
            return Err(syn::Error::new_spanned(
                &kw,
                format!(
                    "unsupported key `{kw}` in #[sourced(...)]; expected `events`, `aggregate_type`, `domain_state`, or `upcasters`"
                ),
            ));
        }
    }

    Ok(SourcedArgs {
        entity_field,
        enum_name,
        aggregate_type,
        domain_state,
        upcasters,
    })
}

pub(crate) struct EventAttr {
    event_name: LitStr,
    guard: Option<Expr>,
    version: Option<syn::LitInt>,
    domain: Option<DomainMode>,
    /// `known(field: value, ..)`: state fields this recorder always leaves at
    /// a fixed value, in the field grammar of `state_preview!`.
    known: Option<TokenStream2>,
}

enum DomainMode {
    State,
    Event,
    Deleted,
    With { output: Box<Type>, adapter: Path },
}

pub(crate) fn parse_event_args(input: ParseStream) -> syn::Result<EventAttr> {
    let event_name: LitStr = input.parse()?;
    let mut guard = None;
    let mut version: Option<syn::LitInt> = None;
    let mut domain = None;
    let mut known: Option<(Ident, TokenStream2)> = None;

    while input.peek(Token![,]) {
        input.parse::<Token![,]>()?;
        // Allow (and ignore) a trailing comma.
        if input.is_empty() {
            break;
        }
        let ident: Ident = input.parse()?;
        if ident == "when" {
            input.parse::<Token![=]>()?;
            guard = Some(input.parse()?);
        } else if ident == "version" {
            input.parse::<Token![=]>()?;
            version = Some(input.parse()?);
        } else if ident == "domain" {
            if domain.is_some() {
                return Err(syn::Error::new_spanned(
                    &ident,
                    "duplicate `domain` mode in #[event(...)]",
                ));
            }
            domain = Some(if input.peek(Token![=]) {
                input.parse::<Token![=]>()?;
                let mode: Ident = input.parse()?;
                if mode == "state" {
                    DomainMode::State
                } else if mode == "event" {
                    DomainMode::Event
                } else if mode == "deleted" {
                    DomainMode::Deleted
                } else if mode == "with" {
                    let content;
                    syn::parenthesized!(content in input);
                    let output = Box::new(content.parse::<Type>()?);
                    content.parse::<Token![,]>()?;
                    let adapter = content.parse::<Path>()?;
                    if !content.is_empty() {
                        return Err(content.error(
                            "`domain = with(...)` expects exactly `(OutputType, adapter_path)`",
                        ));
                    }
                    DomainMode::With { output, adapter }
                } else {
                    return Err(syn::Error::new_spanned(
                        mode,
                        "invalid domain mode; expected `state`, `event`, `deleted`, or `with(OutputType, adapter_path)`",
                    ));
                }
            } else {
                DomainMode::State
            });
        } else if ident == "known" {
            if known.is_some() {
                return Err(syn::Error::new_spanned(
                    &ident,
                    "duplicate `known(...)` in #[event(...)]",
                ));
            }
            let content;
            syn::parenthesized!(content in input);
            // `state_preview!` wants every field followed by one comma.
            let mut fields: Vec<proc_macro2::TokenTree> =
                content.parse::<TokenStream2>()?.into_iter().collect();
            if matches!(fields.last(), Some(proc_macro2::TokenTree::Punct(p)) if p.as_char() == ',')
            {
                fields.pop();
            }
            if fields.is_empty() {
                return Err(syn::Error::new_spanned(
                    &ident,
                    "`known(...)` needs at least one `field: value`",
                ));
            }
            known = Some((ident, quote!(#(#fields)* ,)));
        } else {
            return Err(syn::Error::new_spanned(
                &ident,
                format!(
                    "unsupported key `{ident}` in #[event(...)]; expected `when`, `version`, `domain`, or `known`"
                ),
            ));
        }
    }

    if let Some((key, _)) = &known {
        if !matches!(domain, Some(DomainMode::State)) {
            return Err(syn::Error::new_spanned(
                key,
                "`known(...)` names fields of the domain state, so it needs `domain` (state mode) on the same #[event]",
            ));
        }
    }

    if domain.is_some() {
        validate_domain_event_name_literal(&event_name)?;
        if let Some(version) = &version {
            if version.base10_parse::<u64>()? == 0 {
                return Err(syn::Error::new_spanned(
                    version,
                    "domain event version must be greater than zero",
                ));
            }
        }
    }

    Ok(EventAttr {
        event_name,
        guard,
        version,
        domain,
        known: known.map(|(_, fields)| fields),
    })
}

fn find_and_remove_event_attr(
    attrs: &mut Vec<syn::Attribute>,
) -> Result<Option<EventAttr>, syn::Error> {
    let idx = attrs.iter().position(|a| a.path().is_ident("event"));
    match idx {
        Some(idx) => {
            let attr = attrs.remove(idx);
            let event_attr = attr.parse_args_with(parse_event_args)?;
            Ok(Some(event_attr))
        }
        None => Ok(None),
    }
}

struct EventMethodInfo {
    event_name: LitStr,
    version: syn::LitInt,
    method_name: Ident,
    params: Vec<(Ident, syn::Type)>,
    /// Present when this recorder has `domain` and therefore an exact outward
    /// event contract (a generated marker or the explicitly declared body).
    command_event: Option<DomainCommandEvent>,
}

#[derive(Clone)]
struct DomainCommandEvent {
    domain_event_type: Type,
    domain_state: Option<Type>,
    known: Option<TokenStream2>,
}

/// Public aggregate method that may capture one or more domain events.
struct DomainCommandTransition {
    method_name: Ident,
    events: Vec<DomainCommandEvent>,
}

struct DomainExpansion {
    prepare: TokenStream2,
    capture: TokenStream2,
    generated_type: Option<TokenStream2>,
    uses_deletion_identity: bool,
}

fn ensure_recorder_body_has_no_early_exit(block: &syn::Block) -> syn::Result<()> {
    #[derive(Default)]
    struct EarlyExitVisitor {
        error: Option<syn::Error>,
    }

    impl<'ast> Visit<'ast> for EarlyExitVisitor {
        fn visit_expr_try(&mut self, expression: &'ast syn::ExprTry) {
            if self.error.is_none() {
                self.error = Some(syn::Error::new_spanned(
                    expression,
                    "#[event] methods cannot use `?`: the event is already recorded when the body runs, so the body cannot fail",
                ));
            }
        }

        fn visit_expr_return(&mut self, expression: &'ast syn::ExprReturn) {
            if self.error.is_none() {
                self.error = Some(syn::Error::new_spanned(
                    expression,
                    "#[event] methods cannot use `return`: it would skip the rest of the recorded transition",
                ));
            }
        }
    }

    let mut visitor = EarlyExitVisitor::default();
    visitor.visit_block(block);
    match visitor.error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn domain_capture_error(framework: &TokenStream2, expression: TokenStream2) -> TokenStream2 {
    quote! {
        #expression.map_err(|error| #framework::EventRecordError {
            message: format!("domain-event capture failed: {error}"),
        })?;
    }
}

fn event_version(version: Option<&syn::LitInt>) -> syn::LitInt {
    version.cloned().unwrap_or_else(|| syn::parse_quote!(1_u64))
}

fn identity_domain_event_type(aggregate: &Ident, event_name: &LitStr) -> syn::Result<Ident> {
    let variant = event_variant_ident(event_name);
    syn::parse_str::<Ident>(&format!("{aggregate}{variant}DomainEvent")).map_err(|error| {
        syn::Error::new_spanned(
            event_name,
            format!("could not derive a domain-event DTO name: {error}"),
        )
    })
}

fn expand_domain_capture(
    aggregate: &Ident,
    event_enum: &Ident,
    entity_field: &Ident,
    aggregate_type: Option<&LitStr>,
    domain_state: Option<&Type>,
    event_attr: &EventAttr,
    params: &[(Ident, Type)],
) -> syn::Result<DomainExpansion> {
    let Some(mode) = event_attr.domain.as_ref() else {
        return Ok(DomainExpansion {
            prepare: TokenStream2::new(),
            capture: TokenStream2::new(),
            generated_type: None,
            uses_deletion_identity: false,
        });
    };

    let aggregate_type = match (mode, aggregate_type) {
        (DomainMode::Deleted, None) => {
            return Err(syn::Error::new_spanned(
                &event_attr.event_name,
                "`domain = deleted` requires `aggregate_type = \"...\"` in #[sourced(...)] so the deletion has stable typed identity",
            ));
        }
        (_, None) => {
            return Err(syn::Error::new_spanned(
                &event_attr.event_name,
                "domain-event capture requires `aggregate_type = \"...\"` in #[sourced(...)]",
            ));
        }
        (_, Some(aggregate_type)) => aggregate_type,
    };
    let version = event_version(event_attr.version.as_ref());
    let event_name = &event_attr.event_name;
    let framework = framework_path()?;

    match mode {
        DomainMode::State => {
            let state = domain_state.ok_or_else(|| {
                syn::Error::new_spanned(
                    event_name,
                    "bare `domain` and `domain = state` require `domain_state = StateType` in #[sourced(...)]",
                )
            })?;
            let event_type = identity_domain_event_type(aggregate, event_name)?;
            let capture = domain_capture_error(
                &framework,
                quote! {
                    self.#entity_field.capture_domain_state(
                        #aggregate_type,
                        #framework::DomainEventDescriptor::state::<#state>(#event_name, #version),
                        &__distributed_domain_state,
                    )
                },
            );
            Ok(DomainExpansion {
                prepare: TokenStream2::new(),
                capture: quote! {
                    if !self.#entity_field.is_replaying() {
                        let __distributed_domain_state: #state =
                            <#state as From<&#aggregate>>::from(&*self);
                        #capture
                    }
                },
                generated_type: Some(quote! {
                    /// Exact outward event marker for this state-backed transition.
                    pub enum #event_type {}

                    impl #framework::domain_event::DomainEventContract for #event_type {
                        const EVENT_NAME: &'static str = #event_name;
                        const EVENT_VERSION: u64 = #version;

                        fn descriptor() -> #framework::DomainEventDescriptor {
                            #framework::DomainEventDescriptor::state::<#state>(
                                #event_name,
                                #version,
                            )
                        }
                    }

                    impl #framework::domain_event::DomainEventBodyContract<#state>
                        for #event_type
                    {
                    }
                }),
                uses_deletion_identity: false,
            })
        }
        DomainMode::Event => {
            let body_type = identity_domain_event_type(aggregate, event_name)?;
            let body_type_name = body_type.to_string();
            let schema = canonical_object_schema(
                "domain_event",
                &body_type_name,
                version.base10_parse::<u64>()?,
                &[],
                params
                    .iter()
                    .map(|(name, ty)| (name.to_string(), ty.clone(), Vec::new())),
            );
            let fingerprint = schema_fingerprint(&schema);
            let projection_field_definitions = params.iter().map(|(name, ty)| quote!(#name: #ty));
            let projection_fields: syn::FieldsNamed = syn::parse2(quote!({
                #(#projection_field_definitions),*
            }))?;
            let projection_metadata = projection_body_metadata_tokens(
                &framework,
                "domain_event",
                &body_type_name,
                version.base10_parse::<u64>()?,
                &[],
                &projection_fields.named,
            )?;
            let body_type_name = LitStr::new(&body_type_name, event_name.span());
            let schema = LitStr::new(&schema, event_name.span());
            let fingerprint = LitStr::new(&fingerprint, event_name.span());
            let field_definitions = params.iter().map(|(name, ty)| quote!(pub #name: #ty));
            let field_values = params.iter().map(|(name, _)| quote!(#name: #name.clone()));
            let generated_type = quote! {
                #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
                pub struct #body_type {
                    #(#field_definitions),*
                }

                impl #framework::DomainEvent for #body_type {
                    const DESCRIPTOR: #framework::DomainEventDescriptor =
                        #framework::DomainEventDescriptor {
                            name: std::borrow::Cow::Borrowed(#event_name),
                            version: #version,
                            body: #framework::DomainEventBodyDescriptor::distributed_json(
                                #framework::DomainEventBodyKind::Event,
                                #body_type_name,
                                #version,
                                #schema,
                                #fingerprint,
                            ),
                        };
                }

                impl #framework::domain_event::DomainEventContract for #body_type {
                    const EVENT_NAME: &'static str = #event_name;
                    const EVENT_VERSION: u64 = #version;

                    fn descriptor() -> #framework::DomainEventDescriptor {
                        <Self as #framework::DomainEvent>::DESCRIPTOR.clone()
                    }
                }

                impl #framework::domain_event::DomainEventBodyContract<Self> for #body_type {}

                impl #framework::command::CommandProjectionBody for #body_type {
                    fn command_projection_descriptor(
                        _: &'static str,
                        _: u64,
                    ) -> #framework::DomainEventDescriptor {
                        <Self as #framework::DomainEvent>::DESCRIPTOR.clone()
                    }
                }

                impl #framework::projection::lower::ProjectionBodyMetadata for #body_type {
                    #projection_metadata
                }
            };
            let capture = domain_capture_error(
                &framework,
                quote! {
                    self.#entity_field.capture_domain_event(
                        #aggregate_type,
                        &__distributed_domain_event_body,
                    )
                },
            );
            Ok(DomainExpansion {
                prepare: quote! {
                    let __distributed_domain_event_body =
                        (!self.#entity_field.is_replaying()).then(|| #body_type {
                            #(#field_values),*
                        });
                },
                capture: quote! {
                    if let Some(__distributed_domain_event_body) =
                        __distributed_domain_event_body
                    {
                        #capture
                    }
                },
                generated_type: Some(generated_type),
                uses_deletion_identity: false,
            })
        }
        DomainMode::Deleted => {
            let identity = format_ident!("{aggregate}DomainIdentity");
            let event_type = identity_domain_event_type(aggregate, event_name)?;
            let deletion_type_name = format!("DomainDeletion<{identity}>");
            let schema = canonical_object_schema(
                "domain_deletion",
                &deletion_type_name,
                1,
                &[],
                [
                    ("key".to_string(), syn::parse_quote!(#identity), Vec::new()),
                    (
                        "incarnation".to_string(),
                        syn::parse_quote!(u64),
                        Vec::new(),
                    ),
                ],
            );
            let fingerprint = schema_fingerprint(&schema);
            let deletion_type_name = LitStr::new(&deletion_type_name, event_name.span());
            let schema = LitStr::new(&schema, event_name.span());
            let fingerprint = LitStr::new(&fingerprint, event_name.span());
            let generated_type = quote! {
                /// Exact outward event marker for this deletion transition.
                pub enum #event_type {}

                impl #framework::domain_event::DomainEventContract for #event_type {
                    const EVENT_NAME: &'static str = #event_name;
                    const EVENT_VERSION: u64 = #version;

                    fn descriptor() -> #framework::DomainEventDescriptor {
                        #framework::DomainEventDescriptor {
                            name: std::borrow::Cow::Borrowed(#event_name),
                            version: #version,
                            body: #framework::DomainEventBodyDescriptor::distributed_json(
                                #framework::DomainEventBodyKind::Deletion,
                                #deletion_type_name,
                                1,
                                #schema,
                                #fingerprint,
                            ),
                        }
                    }
                }

                impl #framework::domain_event::DomainEventBodyContract<
                    #framework::DomainDeletion<#identity>
                > for #event_type
                {
                }
            };
            let capture = domain_capture_error(
                &framework,
                quote! {
                    self.#entity_field.capture_domain_deletion(
                        #aggregate_type,
                        #framework::DomainEventDescriptor {
                            name: std::borrow::Cow::Borrowed(#event_name),
                            version: #version,
                            body: #framework::DomainEventBodyDescriptor::distributed_json(
                                #framework::DomainEventBodyKind::Deletion,
                                #deletion_type_name,
                                1,
                                #schema,
                                #fingerprint,
                            ),
                        },
                        &__distributed_domain_deletion,
                    )
                },
            );
            Ok(DomainExpansion {
                prepare: TokenStream2::new(),
                capture: quote! {
                    if !self.#entity_field.is_replaying() {
                        let __distributed_domain_deletion =
                            #framework::DomainDeletion::new(
                                #identity {
                                    aggregate_id: self.#entity_field.id().to_owned(),
                                },
                                self.#entity_field.version(),
                            )
                            .map_err(|error| #framework::EventRecordError {
                                message: format!("domain-event capture failed: {error}"),
                            })?;
                        #capture
                    }
                },
                generated_type: Some(generated_type),
                uses_deletion_identity: true,
            })
        }
        DomainMode::With { output, adapter } => {
            let variant = event_variant_ident(event_name);
            let source = if params.is_empty() {
                quote!(#event_enum::#variant)
            } else {
                let fields = params.iter().map(|(name, _)| quote!(#name: #name.clone()));
                quote!(#event_enum::#variant { #(#fields),* })
            };
            let capture = domain_capture_error(
                &framework,
                quote! {
                    self.#entity_field.capture_domain_event(
                        #aggregate_type,
                        &__distributed_domain_event_body,
                    )
                },
            );
            Ok(DomainExpansion {
                prepare: quote! {
                    let __distributed_domain_event_source =
                        (!self.#entity_field.is_replaying()).then(|| #source);
                },
                capture: quote! {
                    if let Some(__distributed_domain_event_source) =
                        __distributed_domain_event_source.as_ref()
                    {
                        fn __distributed_assert_domain_event_contract<T>()
                        where
                            T: #framework::DomainEvent
                                + #framework::domain_event::DomainEventBodyContract<T>,
                        {
                        }
                        __distributed_assert_domain_event_contract::<#output>();
                        let __distributed_domain_event_adapter:
                            fn(&#aggregate, &#event_enum) -> #output = #adapter;
                        let __distributed_domain_event_body =
                            __distributed_domain_event_adapter(
                                &*self,
                                __distributed_domain_event_source,
                            );
                        if <#output as #framework::domain_event::DomainEventContract>::descriptor()
                            != <#output as #framework::DomainEvent>::DESCRIPTOR
                        {
                            return Err(#framework::EventRecordError {
                                message: "domain-event capture failed: adapter output contract differs from its DomainEvent descriptor".to_owned(),
                            });
                        }
                        #capture
                    }
                },
                generated_type: Some(quote! {
                    const _: () = {
                        const fn __distributed_same_str(
                            left: &str,
                            right: &str,
                        ) -> bool {
                            let left = left.as_bytes();
                            let right = right.as_bytes();
                            if left.len() != right.len() {
                                return false;
                            }
                            let mut index = 0;
                            while index < left.len() {
                                if left[index] != right[index] {
                                    return false;
                                }
                                index += 1;
                            }
                            true
                        }

                        if !__distributed_same_str(
                            <#output as #framework::domain_event::DomainEventContract>::EVENT_NAME,
                            #event_name,
                        ) {
                            panic!("domain adapter output event name differs from #[event]");
                        }
                        if <#output as #framework::domain_event::DomainEventContract>::EVENT_VERSION
                            != #version
                        {
                            panic!("domain adapter output event version differs from #[event]");
                        }
                    };
                }),
                uses_deletion_identity: false,
            })
        }
    }
}

pub(crate) fn expand_sourced(attr: TokenStream2, item: TokenStream2) -> syn::Result<TokenStream2> {
    let framework = framework_path()?;
    let args = parse_sourced_args.parse2(attr)?;
    let mut impl_block = syn::parse2::<ItemImpl>(item)?;

    // Extract struct name from self type
    let struct_name = match &*impl_block.self_ty {
        syn::Type::Path(type_path) => match type_path.path.segments.last() {
            Some(segment) => segment.ident.clone(),
            None => {
                return Err(syn::Error::new_spanned(
                    &impl_block.self_ty,
                    "#[sourced] requires a named type",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                &impl_block.self_ty,
                "#[sourced] requires a named type",
            ));
        }
    };
    let enum_name = if let Some(ref custom) = args.enum_name {
        format_ident!("{}", custom.value())
    } else {
        format_ident!("{}Event", struct_name)
    };

    // Collect event info and modify methods
    let mut event_methods: Vec<EventMethodInfo> = Vec::new();
    let mut generated_domain_types = Vec::new();
    let mut uses_deletion_identity = false;
    // Detect duplicate event names so the conflict points at the offending
    // attribute instead of surfacing as a confusing duplicate match arm later.
    let mut seen_events: std::collections::HashSet<String> = std::collections::HashSet::new();
    // Distinct event names can still derive the same enum variant because only
    // the last `.`-segment is PascalCased (`user.completed` and
    // `admin.completed` both become `Completed`). Track the derived idents so
    // the collision is reported here, naming both event strings, instead of as
    // a duplicate-variant error inside the generated enum.
    let mut seen_variants: std::collections::HashMap<String, LitStr> =
        std::collections::HashMap::new();

    let mut commands = Vec::new();
    for item in &mut impl_block.items {
        if let syn::ImplItem::Fn(method) = item {
            if let Some(command) = crate::aggregate_command::take(method)? {
                commands.push(command);
                continue;
            }
            match find_and_remove_event_attr(&mut method.attrs) {
                Ok(Some(event_attr)) => {
                    // Event methods are replayed as `self.method(...)`, so they
                    // must take a `self` receiver. Reject free associated
                    // functions up front with a pointed message.
                    if !matches!(
                        method.sig.inputs.first(),
                        Some(FnArg::Receiver(receiver))
                            if receiver.reference.is_some() && receiver.mutability.is_some()
                    ) {
                        return Err(syn::Error::new_spanned(
                            &method.sig,
                            "#[event] methods must take a `&mut self` receiver",
                        ));
                    }

                    let event_key = event_attr.event_name.value();
                    if !seen_events.insert(event_key.clone()) {
                        return Err(syn::Error::new_spanned(
                            &event_attr.event_name,
                            format!("duplicate #[event] name `{event_key}` in this #[sourced] impl block"),
                        ));
                    }

                    let variant = event_variant_ident(&event_attr.event_name);
                    if let Some(prev) = seen_variants.get(&variant.to_string()) {
                        return Err(syn::Error::new_spanned(
                            &event_attr.event_name,
                            format!(
                                "#[event] names `{}` and `{event_key}` both derive the enum variant `{variant}`; rename one so the variant names are distinct",
                                prev.value()
                            ),
                        ));
                    }
                    seen_variants.insert(variant.to_string(), event_attr.event_name.clone());

                    ensure_infallible_recorder_signature(&mut method.sig, &framework)?;
                    ensure_recorder_body_has_no_early_exit(&method.block)?;
                    let state_domain =
                        matches!(event_attr.domain.as_ref(), Some(DomainMode::State));
                    let params = extract_params_with_types(&method.sig, "event")?;
                    let param_name_refs: Vec<&Ident> =
                        params.iter().map(|(name, _)| name).collect();

                    let digest_call = generate_digest_call(
                        &args.entity_field,
                        &event_attr.event_name,
                        &param_name_refs,
                        event_attr.version.as_ref(),
                    );
                    let domain = expand_domain_capture(
                        &struct_name,
                        &enum_name,
                        &args.entity_field,
                        args.aggregate_type.as_ref(),
                        args.domain_state.as_ref(),
                        &event_attr,
                        &params,
                    )?;
                    if let Some(generated_type) = domain.generated_type {
                        generated_domain_types.push(generated_type);
                    }
                    uses_deletion_identity |= domain.uses_deletion_identity;
                    let domain_prepare = domain.prepare;
                    let domain_capture = domain.capture;
                    let prepend = quote! {
                        #digest_call
                        #domain_prepare
                    };

                    let new_body = wrap_recorder_body(
                        event_attr.guard.as_ref(),
                        prepend,
                        &method.block,
                        domain_capture,
                    );
                    method.block = new_body;

                    let command_event = if let Some(mode) = event_attr.domain.as_ref() {
                        let domain_event_type = match mode {
                            DomainMode::With { output, .. } => (**output).clone(),
                            _ => {
                                let marker = identity_domain_event_type(
                                    &struct_name,
                                    &event_attr.event_name,
                                )?;
                                syn::parse_quote!(#marker)
                            }
                        };
                        Some(DomainCommandEvent {
                            domain_event_type,
                            domain_state: state_domain.then(|| args.domain_state.clone()).flatten(),
                            known: event_attr.known.clone(),
                        })
                    } else {
                        None
                    };

                    event_methods.push(EventMethodInfo {
                        version: event_version(event_attr.version.as_ref()),
                        event_name: event_attr.event_name,
                        method_name: method.sig.ident.clone(),
                        params,
                        command_event,
                    });
                }
                Ok(None) => { /* not an event method, skip */ }
                Err(err) => return Err(err),
            }
        }
    }

    let command_methods: std::collections::BTreeSet<String> = commands
        .iter()
        .map(|command| command.method().to_string())
        .collect();
    let domain_command_transitions =
        discover_domain_command_transitions(&impl_block, &event_methods, &command_methods);
    let self_ty = (*impl_block.self_ty).clone();
    let command_mounts = commands
        .iter()
        .map(|command| {
            let witness = domain_command_transitions
                .iter()
                .find(|transition| transition.method_name == *command.method())
                .map(|transition| {
                    let ty = method_name_to_type_ident(&transition.method_name);
                    quote!(domain_commands::#ty)
                });
            crate::aggregate_command::expand(command, &self_ty, witness, &framework)
        })
        .collect::<syn::Result<Vec<_>>>()?;

    let deletion_identity = if uses_deletion_identity {
        let identity = format_ident!("{struct_name}DomainIdentity");
        let deletion_type_name = format!("DomainDeletion<{identity}>");
        let deletion_schema = canonical_object_schema(
            "domain_deletion",
            &deletion_type_name,
            1,
            &[],
            [
                ("key".to_string(), syn::parse_quote!(#identity), Vec::new()),
                (
                    "incarnation".to_string(),
                    syn::parse_quote!(u64),
                    Vec::new(),
                ),
            ],
        );
        let deletion_fingerprint = schema_fingerprint(&deletion_schema);
        let deletion_type_name = LitStr::new(&deletion_type_name, struct_name.span());
        let deletion_schema = LitStr::new(&deletion_schema, struct_name.span());
        let deletion_fingerprint = LitStr::new(&deletion_fingerprint, struct_name.span());
        quote! {
            /// Stable aggregate identity carried by deletion domain events.
            ///
            /// Version one uses the causing aggregate sequence as the
            /// [`distributed::DomainDeletion`] incarnation.
            #[derive(
                Clone,
                Debug,
                PartialEq,
                Eq,
                serde::Serialize,
                serde::Deserialize,
            )]
            pub struct #identity {
                pub aggregate_id: String,
            }

            impl #framework::projection::lower::ProjectionDeletionMetadata for #identity {
                const BODY_TYPE_NAME: &'static str = #deletion_type_name;
                const BODY_SCHEMA: &'static str = #deletion_schema;
                const BODY_FINGERPRINT: &'static str = #deletion_fingerprint;
            }
        }
    } else {
        TokenStream2::new()
    };

    // Generate event enum
    let enum_variants = event_methods.iter().map(|e| {
        let variant_name = event_variant_ident(&e.event_name);
        if e.params.is_empty() {
            quote! { #variant_name }
        } else {
            let fields = e.params.iter().map(|(name, ty)| quote! { #name: #ty });
            quote! { #variant_name { #(#fields),* } }
        }
    });

    let enum_def = quote! {
        #[allow(clippy::enum_variant_names)]
        #[derive(Debug, Clone, PartialEq)]
        pub enum #enum_name {
            #(#enum_variants),*
        }
    };

    // Generate event_name() method on the enum
    let event_name_arms = event_methods.iter().map(|e| {
        let variant_name = event_variant_ident(&e.event_name);
        let name_str = &e.event_name;
        if e.params.is_empty() {
            quote! { #enum_name::#variant_name => #name_str }
        } else {
            quote! { #enum_name::#variant_name { .. } => #name_str }
        }
    });

    let event_name_impl = quote! {
        impl #enum_name {
            pub fn event_name(&self) -> &'static str {
                match self {
                    #(#event_name_arms),*
                }
            }
        }
    };

    // Generate TryFrom<&EventRecord>
    let try_from_arms = event_methods.iter().map(|e| {
        let variant_name = event_variant_ident(&e.event_name);
        let event_name_str = &e.event_name;
        if e.params.is_empty() {
            quote! {
                #event_name_str => Ok(#enum_name::#variant_name),
            }
        } else if e.params.len() == 1 {
            let (name, _) = &e.params[0];
            quote! {
                #event_name_str => {
                    let (#name,) = event.decode().map_err(|e| e.to_string())?;
                    Ok(#enum_name::#variant_name { #name })
                }
            }
        } else {
            let names: Vec<_> = e.params.iter().map(|(n, _)| n).collect();
            quote! {
                #event_name_str => {
                    let (#(#names),*) = event.decode().map_err(|e| e.to_string())?;
                    Ok(#enum_name::#variant_name { #(#names),* })
                }
            }
        }
    });

    let try_from_impl = quote! {
        impl TryFrom<&#framework::EventRecord> for #enum_name {
            type Error = String;
            fn try_from(event: &#framework::EventRecord) -> Result<Self, Self::Error> {
                match event.event_name.as_str() {
                    #(#try_from_arms)*
                    _ => Err(format!("Unknown event: {}", event.event_name)),
                }
            }
        }
    };

    // Generate impl Aggregate
    let entity_field = &args.entity_field;
    let version_arms: Vec<_> = event_methods
        .iter()
        .map(|event| {
            let name = &event.event_name;
            let version = &event.version;
            quote! { #name => #version, }
        })
        .collect();
    let replay_arms: Vec<_> = event_methods
        .iter()
        .map(|e| {
            let event_name_str = &e.event_name;
            let method_name = &e.method_name;
            if e.params.is_empty() {
                quote! {
                    #event_name_str => {
                        self.#method_name().map_err(|e| e.to_string())?;
                    }
                }
            } else if e.params.len() == 1 {
                let (name, _) = &e.params[0];
                quote! {
                    #event_name_str => {
                        let (#name,) = event.decode().map_err(|e| e.to_string())?;
                        self.#method_name(#name).map_err(|e| e.to_string())?;
                    }
                }
            } else {
                let names: Vec<_> = e.params.iter().map(|(n, _)| n).collect();
                quote! {
                    #event_name_str => {
                        let (#(#names),*) = event.decode().map_err(|e| e.to_string())?;
                        self.#method_name(#(#names),*).map_err(|e| e.to_string())?;
                    }
                }
            }
        })
        .collect();

    let (upcaster_wrappers, upcasters_method) =
        generate_upcaster_tokens(&framework, &struct_name, &args.upcasters);
    let aggregate_type_method = args.aggregate_type.as_ref().map(|aggregate_type| {
        quote! {
            fn aggregate_type() -> &'static str {
                #aggregate_type
            }
        }
    });

    // ReplayError stays `String` — see the rationale on `expand_aggregate`.
    let aggregate_impl = aggregate_impl_tokens(
        &framework,
        &struct_name,
        entity_field,
        &aggregate_type_method,
        &version_arms,
        &replay_arms,
        &upcasters_method,
    );

    let domain_commands_module =
        expand_domain_commands_module(&framework, &struct_name, &domain_command_transitions);

    let expanded = quote! {
        #impl_block
        #enum_def
        #event_name_impl
        #try_from_impl
        #(#generated_domain_types)*
        #deletion_identity
        #domain_commands_module
        #(#command_mounts)*
        #upcaster_wrappers
        #aggregate_impl
    };

    Ok(expanded)
}

fn discover_domain_command_transitions(
    impl_block: &ItemImpl,
    event_methods: &[EventMethodInfo],
    command_methods: &std::collections::BTreeSet<String>,
) -> Vec<DomainCommandTransition> {
    let recorders: std::collections::BTreeMap<String, DomainCommandEvent> = event_methods
        .iter()
        .filter_map(|event| {
            event
                .command_event
                .clone()
                .map(|command_event| (event.method_name.to_string(), command_event))
        })
        .collect();
    if recorders.is_empty() {
        return Vec::new();
    }

    // Only decision methods belong in the helper graph. A recorder's rewritten
    // implementation includes replay/capture plumbing, not another decision.
    let methods = impl_block
        .items
        .iter()
        .filter_map(|item| match item {
            syn::ImplItem::Fn(method)
                if !event_methods
                    .iter()
                    .any(|event| event.method_name == method.sig.ident) =>
            {
                Some((method.sig.ident.to_string(), method))
            }
            _ => None,
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut transitions = Vec::new();
    for (name, method) in &methods {
        // Public methods are transitions; so is every `#[command]` method
        // (its generated mount needs the witness whatever its visibility).
        if !matches!(method.vis, syn::Visibility::Public(_)) && !command_methods.contains(name) {
            continue;
        }

        let mut finder = DomainEventCallFinder {
            recorders: &recorders,
            found: std::collections::BTreeMap::new(),
            calls: std::collections::BTreeSet::new(),
        };
        let mut pending = vec![name.clone()];
        let mut visited = std::collections::BTreeSet::new();
        while let Some(name) = pending.pop() {
            if !visited.insert(name.clone()) {
                continue;
            }
            let Some(callee) = methods.get(&name) else {
                continue;
            };
            finder.visit_block(&callee.block);
            pending.extend(std::mem::take(&mut finder.calls));
        }
        if finder.found.is_empty() {
            continue;
        }
        transitions.push(DomainCommandTransition {
            method_name: method.sig.ident.clone(),
            events: finder.found.into_values().collect(),
        });
    }
    transitions
}

struct DomainEventCallFinder<'a> {
    recorders: &'a std::collections::BTreeMap<String, DomainCommandEvent>,
    found: std::collections::BTreeMap<String, DomainCommandEvent>,
    calls: std::collections::BTreeSet<String>,
}

impl<'ast> Visit<'ast> for DomainEventCallFinder<'_> {
    // A nested function/impl has its own receiver scope. Its `self` must not
    // be confused with the aggregate, even when a method name happens to match.
    fn visit_item(&mut self, _: &'ast syn::Item) {}

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        if is_self_receiver(&node.receiver) {
            if let Some(command_event) = self.recorders.get(&node.method.to_string()) {
                let event_type = &command_event.domain_event_type;
                self.found
                    .entry(quote!(#event_type).to_string())
                    .or_insert_with(|| command_event.clone());
            } else {
                self.calls.insert(node.method.to_string());
            }
        }
        syn::visit::visit_expr_method_call(self, node);
    }
}

fn is_self_receiver(expression: &Expr) -> bool {
    match expression {
        Expr::Path(path) => path.path.is_ident("self"),
        Expr::Paren(paren) => is_self_receiver(&paren.expr),
        Expr::Group(group) => is_self_receiver(&group.expr),
        _ => false,
    }
}

fn method_name_to_type_ident(method_name: &Ident) -> Ident {
    let pascal = method_name
        .to_string()
        .split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect::<String>();
    format_ident!("{}", pascal)
}

fn expand_domain_commands_module(
    framework: &TokenStream2,
    aggregate: &Ident,
    transitions: &[DomainCommandTransition],
) -> TokenStream2 {
    if transitions.is_empty() {
        return TokenStream2::new();
    }

    let mut transition_items = Vec::new();
    let mut transition_impls = Vec::new();
    for transition in transitions {
        let type_name = method_name_to_type_ident(&transition.method_name);
        let method_name = transition.method_name.to_string();
        let aggregate_name = aggregate.to_string();
        let event_types = transition
            .events
            .iter()
            .map(|event| &event.domain_event_type)
            .collect::<Vec<_>>();
        // `known(..)` on a state recorder: declared once next to the
        // transition, checked against the state's fields when the command is
        // registered.
        let known_value_items = transition.events.iter().filter_map(|event| {
            let (fields, state) = (event.known.as_ref()?, event.domain_state.as_ref()?);
            let event_type = &event.domain_event_type;
            Some(quote! {
                #framework::state_preview! { #event_type => #state { #fields } }
            })
        });
        let has_known_values = transition.events.iter().any(|event| event.known.is_some());
        let known_values_method = has_known_values.then(|| {
            quote! {
                fn command_event_known_values(
                ) -> Vec<#framework::command::CommandProjectionPreview> {
                    vec![#(#known_value_items),*]
                }
            }
        });
        let doc = format!(
            "Outward domain-event set for `{aggregate_name}::{method_name}`.\n\n\
             Derived from `self.<recorder>()` calls to `#[event(..., domain)]` \
             methods, including through decision helpers in this `#[sourced]` impl. Use with \
             [`distributed::command::TypedCommand::emits_events`]."
        );
        transition_items.push(quote! {
            #[doc = #doc]
            pub enum #type_name {}
        });
        // Keep authored types in the aggregate's lexical scope. Moving an
        // explicit `with(super::facts::Body, ...)` path inside domain_commands
        // would silently change its meaning (and generated aliases do not exist
        // for custom bodies). The public witness still lives in that module.
        transition_impls.push(quote! {
            impl #framework::command::CommandEventSet for domain_commands::#type_name {
                fn command_event_set() -> #framework::command::CommandProjectionEventSet {
                    #framework::command::__command_projection_events([
                        #(
                            #framework::command::__command_projection_event_descriptor::<
                                #event_types,
                            >()
                        ),*
                    ])
                }

                #known_values_method
            }
        });
    }

    let aggregate_name = aggregate.to_string();
    let module_doc = format!(
        "Domain command transitions for `{aggregate_name}`.\n\n\
         Each type is a zero-sized witness for the outward domain events a public \
         aggregate method may capture. Prefer \
         `typed_command(...).emits_events::<domain_commands::Create>()` over a \
         hand-duplicated `events![...]` list when the domain method already owns \
         the transition."
    );

    quote! {
        #[doc = #module_doc]
        pub mod domain_commands {
            #(#transition_items)*
        }
        #(#transition_impls)*
    }
}

// ============================================================================
// #[derive(ReadModel)] derive macro
// ============================================================================
