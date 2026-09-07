use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    parse::{ParseStream, Parser},
    visit::Visit,
    Expr, FnArg, Ident, ItemImpl, LitStr, Member, Path, Stmt, Token, Type,
};

use crate::aggregate::{
    aggregate_impl_tokens, event_variant_ident, generate_upcaster_tokens, parse_upcaster_list,
    validate_aggregate_type_literal, UpcasterDef,
};
use crate::shared::{
    canonical_object_schema, ensure_sourced_result_signature, extract_params_with_types,
    framework_path, generate_digest_call, generate_enqueue_call, projection_body_metadata_tokens,
    schema_fingerprint, validate_domain_event_name_literal,
    wrap_result_body_with_guard_and_postlude,
};

pub(crate) struct SourcedArgs {
    entity_field: Ident,
    enum_name: Option<LitStr>,
    aggregate_type: Option<LitStr>,
    domain_state: Option<Type>,
    enqueue: Option<Ident>, // Some(emitter_field) if enqueue enabled
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
    let mut enqueue = None;
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
        } else if kw == "enqueue" {
            // Optional custom emitter field: enqueue(my_emitter)
            if input.peek(syn::token::Paren) {
                let inner;
                syn::parenthesized!(inner in input);
                enqueue = Some(inner.parse::<Ident>()?);
            } else {
                enqueue = Some(format_ident!("emitter"));
            }
        } else if kw == "upcasters" {
            let upcaster_content;
            syn::parenthesized!(upcaster_content in input);
            upcasters = parse_upcaster_list(&upcaster_content)?;
        } else {
            return Err(syn::Error::new_spanned(
                &kw,
                format!(
                    "unsupported key `{kw}` in #[sourced(...)]; expected `events`, `aggregate_type`, `domain_state`, `enqueue`, or `upcasters`"
                ),
            ));
        }
    }

    Ok(SourcedArgs {
        entity_field,
        enum_name,
        aggregate_type,
        domain_state,
        enqueue,
        upcasters,
    })
}

pub(crate) struct EventAttr {
    event_name: LitStr,
    guard: Option<Expr>,
    version: Option<syn::LitInt>,
    domain: Option<DomainMode>,
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
        } else {
            return Err(syn::Error::new_spanned(
                &ident,
                format!(
                    "unsupported key `{ident}` in #[event(...)]; expected `when`, `version`, or `domain`"
                ),
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
    method_name: Ident,
    params: Vec<(Ident, syn::Type)>,
    /// Present when this recorder has `domain` and therefore a generated
    /// outward domain-event marker type.
    command_event: Option<DomainCommandEvent>,
}

#[derive(Clone)]
struct DomainCommandEvent {
    domain_event_type: Ident,
    domain_state: Option<Type>,
    known_state_values: Vec<KnownStateValue>,
}

#[derive(Clone)]
struct KnownStateValue {
    field: Ident,
    source: KnownStateValueSource,
}

#[derive(Clone)]
enum KnownStateValueSource {
    Constant(Expr),
    Null,
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

fn ensure_domain_body_has_no_early_exit(block: &syn::Block) -> syn::Result<()> {
    #[derive(Default)]
    struct EarlyExitVisitor {
        error: Option<syn::Error>,
    }

    impl<'ast> Visit<'ast> for EarlyExitVisitor {
        fn visit_expr_try(&mut self, expression: &'ast syn::ExprTry) {
            if self.error.is_none() {
                self.error = Some(syn::Error::new_spanned(
                    expression,
                    "domain-marked #[event] methods cannot use `?` in version one because it may skip post-transition capture",
                ));
            }
        }

        fn visit_expr_return(&mut self, expression: &'ast syn::ExprReturn) {
            if self.error.is_none() {
                self.error = Some(syn::Error::new_spanned(
                    expression,
                    "domain-marked #[event] methods cannot use `return` in version one because it skips post-transition capture",
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

fn domain_capture_error(expression: TokenStream2) -> TokenStream2 {
    quote! {
        #expression.map_err(|error| distributed::EventRecordError {
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
            let capture = domain_capture_error(quote! {
                self.#entity_field.capture_domain_state(
                    #aggregate_type,
                    distributed::DomainEventDescriptor::state::<#state>(#event_name, #version),
                    &__distributed_domain_state,
                )
            });
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

                    impl distributed::domain_event::DomainEventContract for #event_type {
                        const EVENT_NAME: &'static str = #event_name;
                        const EVENT_VERSION: u64 = #version;

                        fn descriptor() -> distributed::DomainEventDescriptor {
                            distributed::DomainEventDescriptor::state::<#state>(
                                #event_name,
                                #version,
                            )
                        }
                    }

                    impl distributed::domain_event::DomainEventBodyContract<#state>
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

                impl distributed::DomainEvent for #body_type {
                    const DESCRIPTOR: distributed::DomainEventDescriptor =
                        distributed::DomainEventDescriptor {
                            name: std::borrow::Cow::Borrowed(#event_name),
                            version: #version,
                            body: distributed::DomainEventBodyDescriptor::distributed_json(
                                distributed::DomainEventBodyKind::Event,
                                #body_type_name,
                                #version,
                                #schema,
                                #fingerprint,
                            ),
                        };
                }

                impl distributed::domain_event::DomainEventContract for #body_type {
                    const EVENT_NAME: &'static str = #event_name;
                    const EVENT_VERSION: u64 = #version;

                    fn descriptor() -> distributed::DomainEventDescriptor {
                        <Self as distributed::DomainEvent>::DESCRIPTOR.clone()
                    }
                }

                impl distributed::domain_event::DomainEventBodyContract<Self> for #body_type {}

                impl distributed::projection::lower::ProjectionBodyMetadata for #body_type {
                    #projection_metadata
                }
            };
            let capture = domain_capture_error(quote! {
                self.#entity_field.capture_domain_event(
                    #aggregate_type,
                    &__distributed_domain_event_body,
                )
            });
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

                impl distributed::domain_event::DomainEventContract for #event_type {
                    const EVENT_NAME: &'static str = #event_name;
                    const EVENT_VERSION: u64 = #version;

                    fn descriptor() -> distributed::DomainEventDescriptor {
                        distributed::DomainEventDescriptor {
                            name: std::borrow::Cow::Borrowed(#event_name),
                            version: #version,
                            body: distributed::DomainEventBodyDescriptor::distributed_json(
                                distributed::DomainEventBodyKind::Deletion,
                                #deletion_type_name,
                                1,
                                #schema,
                                #fingerprint,
                            ),
                        }
                    }
                }

                impl distributed::domain_event::DomainEventBodyContract<
                    distributed::DomainDeletion<#identity>
                > for #event_type
                {
                }
            };
            let capture = domain_capture_error(quote! {
                self.#entity_field.capture_domain_deletion(
                    #aggregate_type,
                    distributed::DomainEventDescriptor {
                        name: std::borrow::Cow::Borrowed(#event_name),
                        version: #version,
                        body: distributed::DomainEventBodyDescriptor::distributed_json(
                            distributed::DomainEventBodyKind::Deletion,
                            #deletion_type_name,
                            1,
                            #schema,
                            #fingerprint,
                        ),
                    },
                    &__distributed_domain_deletion,
                )
            });
            Ok(DomainExpansion {
                prepare: TokenStream2::new(),
                capture: quote! {
                    if !self.#entity_field.is_replaying() {
                        let __distributed_domain_deletion =
                            distributed::DomainDeletion::new(
                                #identity {
                                    aggregate_id: self.#entity_field.id().to_owned(),
                                },
                                self.#entity_field.version(),
                            )
                            .map_err(|error| distributed::EventRecordError {
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
            let capture = domain_capture_error(quote! {
                self.#entity_field.capture_domain_event(
                    #aggregate_type,
                    &__distributed_domain_event_body,
                )
            });
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
                            T: distributed::DomainEvent
                                + distributed::domain_event::DomainEventBodyContract<T>,
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
                        if <#output as distributed::domain_event::DomainEventContract>::descriptor()
                            != <#output as distributed::DomainEvent>::DESCRIPTOR
                        {
                            return Err(distributed::EventRecordError {
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
                            <#output as distributed::domain_event::DomainEventContract>::EVENT_NAME,
                            #event_name,
                        ) {
                            panic!("domain adapter output event name differs from #[event]");
                        }
                        if <#output as distributed::domain_event::DomainEventContract>::EVENT_VERSION
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

    for item in &mut impl_block.items {
        if let syn::ImplItem::Fn(method) = item {
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

                    if event_attr.domain.is_some()
                        && !matches!(method.sig.output, syn::ReturnType::Default)
                    {
                        return Err(syn::Error::new_spanned(
                            &method.sig.output,
                            "domain-marked #[event] methods must omit the return type in version one; fallible recorders cannot prove post-transition capture safety",
                        ));
                    }
                    if event_attr.domain.is_some() {
                        ensure_domain_body_has_no_early_exit(&method.block)?;
                    }
                    let state_domain =
                        matches!(event_attr.domain.as_ref(), Some(DomainMode::State));
                    let known_state_values = state_domain
                        .then(|| infer_unconditional_known_state_values(&method.block))
                        .unwrap_or_default();
                    let signature_synthesized =
                        ensure_sourced_result_signature(&mut method.sig, "event", &framework)?;

                    let params = extract_params_with_types(&method.sig, "event")?;
                    let param_name_refs: Vec<&Ident> =
                        params.iter().map(|(name, _)| name).collect();

                    // Build prepend: optional enqueue + digest
                    let enqueue_call = args.enqueue.as_ref().map(|emitter_field| {
                        generate_enqueue_call(
                            &args.entity_field,
                            emitter_field,
                            &event_attr.event_name,
                            &param_name_refs,
                        )
                    });
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
                        #enqueue_call
                        #digest_call
                        #domain_prepare
                    };

                    let new_body = wrap_result_body_with_guard_and_postlude(
                        event_attr.guard.as_ref(),
                        prepend,
                        &method.block,
                        domain_capture,
                        signature_synthesized,
                    );
                    method.block = new_body;

                    let command_event = if event_attr.domain.is_some() {
                        Some(DomainCommandEvent {
                            domain_event_type: identity_domain_event_type(
                                &struct_name,
                                &event_attr.event_name,
                            )?,
                            domain_state: state_domain.then(|| args.domain_state.clone()).flatten(),
                            known_state_values,
                        })
                    } else {
                        None
                    };

                    event_methods.push(EventMethodInfo {
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

    let domain_command_transitions =
        discover_domain_command_transitions(&impl_block, &event_methods);

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

            impl distributed::projection::lower::ProjectionDeletionMetadata for #identity {
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
        impl TryFrom<&distributed::EventRecord> for #enum_name {
            type Error = String;
            fn try_from(event: &distributed::EventRecord) -> Result<Self, Self::Error> {
                match event.event_name.as_str() {
                    #(#try_from_arms)*
                    _ => Err(format!("Unknown event: {}", event.event_name)),
                }
            }
        }
    };

    // Generate impl Aggregate
    let entity_field = &args.entity_field;
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
        generate_upcaster_tokens(&struct_name, &args.upcasters);
    let aggregate_type_method = args.aggregate_type.as_ref().map(|aggregate_type| {
        quote! {
            fn aggregate_type() -> &'static str {
                #aggregate_type
            }
        }
    });

    // ReplayError stays `String` — see the rationale on `expand_aggregate`.
    let aggregate_impl = aggregate_impl_tokens(
        &struct_name,
        entity_field,
        &aggregate_type_method,
        &replay_arms,
        &upcasters_method,
    );

    let domain_commands_module =
        expand_domain_commands_module(&struct_name, &domain_command_transitions);

    let expanded = quote! {
        #impl_block
        #enum_def
        #event_name_impl
        #try_from_impl
        #(#generated_domain_types)*
        #deletion_identity
        #domain_commands_module
        #upcaster_wrappers
        #aggregate_impl
    };

    Ok(expanded)
}

/// Collect only straight-line recorder assignments whose serialized value is
/// independent of command input and current aggregate state. Nested control
/// flow and arbitrary Rust expressions deliberately remain unknown.
fn infer_unconditional_known_state_values(block: &syn::Block) -> Vec<KnownStateValue> {
    let mut values =
        std::collections::BTreeMap::<String, (Ident, Option<KnownStateValueSource>)>::new();
    for statement in &block.stmts {
        let Stmt::Expr(Expr::Assign(assign), _) = statement else {
            continue;
        };
        let Expr::Field(field) = ungroup_expr(&assign.left) else {
            continue;
        };
        if !is_self_receiver(&field.base) {
            continue;
        }
        let Member::Named(name) = &field.member else {
            continue;
        };
        values.insert(
            name.to_string(),
            (name.clone(), known_state_value_source(&assign.right)),
        );
    }
    values
        .into_values()
        .filter_map(|(field, source)| source.map(|source| KnownStateValue { field, source }))
        .collect()
}

fn known_state_value_source(expression: &Expr) -> Option<KnownStateValueSource> {
    let expression = ungroup_expr(expression);
    match expression {
        Expr::Path(path) if path.path.is_ident("None") => Some(KnownStateValueSource::Null),
        // Qualified paths cover enum variants and associated constants without
        // treating a local variable as a compile-time value.
        Expr::Path(path) if path.path.segments.len() >= 2 => {
            Some(KnownStateValueSource::Constant(expression.clone()))
        }
        Expr::Lit(literal)
            if matches!(
                literal.lit,
                syn::Lit::Bool(_) | syn::Lit::Str(_) | syn::Lit::Char(_)
            ) =>
        {
            Some(KnownStateValueSource::Constant(expression.clone()))
        }
        Expr::Call(call)
            if call.args.len() == 1
                && matches!(
                    ungroup_expr(&call.func),
                    Expr::Path(path)
                        if path.path.segments.len() == 4
                            && path.path.segments[0].ident == "std"
                            && path.path.segments[1].ident == "string"
                            && path.path.segments[2].ident == "String"
                            && path.path.segments[3].ident == "from"
                )
                && matches!(
                    call.args.first().map(ungroup_expr),
                    Some(Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(_),
                        ..
                    }))
                ) =>
        {
            Some(KnownStateValueSource::Constant(expression.clone()))
        }
        Expr::Call(call)
            if call.args.len() == 1
                && matches!(
                    ungroup_expr(&call.func),
                    Expr::Path(path) if path.path.is_ident("Some")
                )
                && known_state_value_source(call.args.first().expect("one Some argument"))
                    .is_some() =>
        {
            Some(KnownStateValueSource::Constant(expression.clone()))
        }
        _ => None,
    }
}

fn ungroup_expr(expression: &Expr) -> &Expr {
    match expression {
        Expr::Paren(paren) => ungroup_expr(&paren.expr),
        Expr::Group(group) => ungroup_expr(&group.expr),
        other => other,
    }
}

fn discover_domain_command_transitions(
    impl_block: &ItemImpl,
    event_methods: &[EventMethodInfo],
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

    let mut transitions = Vec::new();
    for item in &impl_block.items {
        let syn::ImplItem::Fn(method) = item else {
            continue;
        };
        // Domain recorders themselves are not command transitions.
        if recorders.contains_key(&method.sig.ident.to_string()) {
            continue;
        }
        if !matches!(method.vis, syn::Visibility::Public(_)) {
            continue;
        }

        let mut finder = DomainEventCallFinder {
            recorders: &recorders,
            found: std::collections::BTreeMap::new(),
        };
        finder.visit_block(&method.block);
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
}

impl<'ast> Visit<'ast> for DomainEventCallFinder<'_> {
    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        if is_self_receiver(&node.receiver) {
            if let Some(command_event) = self.recorders.get(&node.method.to_string()) {
                self.found
                    .entry(command_event.domain_event_type.to_string())
                    .or_insert_with(|| command_event.clone());
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
    aggregate: &Ident,
    transitions: &[DomainCommandTransition],
) -> TokenStream2 {
    if transitions.is_empty() {
        return TokenStream2::new();
    }

    let transition_items = transitions.iter().map(|transition| {
        let type_name = method_name_to_type_ident(&transition.method_name);
        let method_name = transition.method_name.to_string();
        let aggregate_name = aggregate.to_string();
        let event_types = transition
            .events
            .iter()
            .map(|event| &event.domain_event_type)
            .collect::<Vec<_>>();
        let known_value_items = transition.events.iter().filter_map(|event| {
            let state = event.domain_state.as_ref()?;
            if event.known_state_values.is_empty() {
                return None;
            }
            let event_type = &event.domain_event_type;
            let fields = event.known_state_values.iter().map(|value| {
                let field = value.field.to_string();
                let source = match &value.source {
                    KnownStateValueSource::Constant(expression) => quote! {
                        distributed::command::__command_projection_preview_constant(#expression)
                    },
                    KnownStateValueSource::Null => quote! {
                        distributed::command::CommandProjectionPreviewSource::Null
                    },
                };
                quote! { (#field, #source) }
            });
            Some(quote! {
                distributed::command::__command_projection_state_known_values::<
                    super::#event_type,
                    #state,
                >(vec![#(#fields),*])
            })
        });
        let has_known_values = transition
            .events
            .iter()
            .any(|event| event.domain_state.is_some() && !event.known_state_values.is_empty());
        let known_values_method = has_known_values.then(|| {
            quote! {
                fn command_event_known_values(
                ) -> Vec<distributed::command::CommandProjectionPreview> {
                    #[allow(unused_imports)]
                    use super::*;
                    vec![#(#known_value_items),*]
                }
            }
        });
        let doc = format!(
            "Outward domain-event set for `{aggregate_name}::{method_name}`.\n\n\
             Derived from direct `self.<recorder>()` calls to `#[event(..., domain)]` \
             methods in this `#[sourced]` impl. Use with \
             [`distributed::command::TypedCommand::emits_events`]."
        );
        quote! {
            #[doc = #doc]
            pub enum #type_name {}

            impl distributed::command::CommandEventSet for #type_name {
                fn command_event_set() -> distributed::command::CommandProjectionEventSet {
                    distributed::command::__command_projection_events([
                        #(
                            distributed::command::__command_projection_event_descriptor::<
                                super::#event_types,
                            >()
                        ),*
                    ])
                }

                #known_values_method
            }
        }
    });

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
    }
}

// ============================================================================
// #[derive(ReadModel)] derive macro
// ============================================================================
