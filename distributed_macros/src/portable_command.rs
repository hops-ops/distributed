//! `portable_command!` — domain command mounts (`PCH-DEC-001`).
//!
//! The handler attribute is already `#[command]`, so this function-like form
//! uses a distinct name. Spec sketches called it `command!`.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{parenthesized, Expr, Ident, LitStr, Token, Type};

struct AuthenticatedUserField {
    event: Type,
    state: Type,
    field: LitStr,
}

impl Parse for AuthenticatedUserField {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let values;
        parenthesized!(values in input);
        let event = values.parse()?;
        values.parse::<Token![,]>()?;
        let state = values.parse()?;
        values.parse::<Token![,]>()?;
        let field = values.parse()?;
        if !values.is_empty() {
            return Err(values
                .error("authenticated_user_field expects (EventType, StateType, \"field_name\")"));
        }
        Ok(Self {
            event,
            state,
            field,
        })
    }
}

struct PortableCommandArgs {
    name: LitStr,
    transition: Type,
    aggregate: Type,
    input: Type,
    outcome: Type,
    shard: Expr,
    field: LitStr,
    roles: Vec<LitStr>,
    load: LoadKind,
    invoke: Option<Expr>,
    payload: Option<Expr>,
    handle: Option<Expr>,
    guard: Option<Expr>,
    defaults: Option<Expr>,
    constructor: Option<Ident>,
    authenticated_user_field: Option<AuthenticatedUserField>,
    preview_reduce_known_record: Option<Expr>,
}

enum LoadKind {
    Required,
    Create,
    None,
}

impl Parse for PortableCommandArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut name = None;
        let mut transition = None;
        let mut aggregate = None;
        let mut input_ty = None;
        let mut outcome = None;
        let mut shard = None;
        let mut field = None;
        let mut roles = Vec::new();
        let mut load = LoadKind::None;
        let mut invoke = None;
        let mut payload = None;
        let mut handle = None;
        let mut guard = None;
        let mut defaults = None;
        let mut constructor = None;
        let mut authenticated_user_field = None;
        let mut preview_reduce_known_record = None;

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![:]>()?;
            match key.to_string().as_str() {
                "name" => name = Some(input.parse()?),
                "transition" => transition = Some(input.parse()?),
                "aggregate" => aggregate = Some(input.parse()?),
                "input" => input_ty = Some(input.parse()?),
                "outcome" => outcome = Some(input.parse()?),
                "shard" => shard = Some(input.parse()?),
                "field" => field = Some(input.parse()?),
                "invoke" => invoke = Some(input.parse()?),
                "payload" => payload = Some(input.parse()?),
                "handle" => handle = Some(input.parse()?),
                "guard" => guard = Some(input.parse()?),
                "defaults" => defaults = Some(input.parse()?),
                "constructor" => constructor = Some(input.parse()?),
                "authenticated_user_field" => authenticated_user_field = Some(input.parse()?),
                "preview_reduce_known_record" => preview_reduce_known_record = Some(input.parse()?),
                "load" => {
                    let ident: Ident = input.parse()?;
                    load = match ident.to_string().as_str() {
                        "required" => LoadKind::Required,
                        "create" => LoadKind::Create,
                        other => {
                            return Err(syn::Error::new(
                                ident.span(),
                                format!("load must be `required` or `create`, not `{other}`"),
                            ))
                        }
                    };
                }
                "roles" => {
                    let expr: Expr = input.parse()?;
                    roles = lit_strs_from_array(&expr)?;
                }
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!("unknown portable_command key `{other}`"),
                    ))
                }
            }
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }

        let name = name.ok_or_else(|| input.error("portable_command requires `name: \"...\"`"))?;
        Ok(Self {
            name,
            transition: transition
                .ok_or_else(|| input.error("portable_command requires `transition:`"))?,
            aggregate: aggregate
                .ok_or_else(|| input.error("portable_command requires `aggregate:`"))?,
            input: input_ty.ok_or_else(|| input.error("portable_command requires `input:`"))?,
            outcome: outcome.ok_or_else(|| input.error("portable_command requires `outcome:`"))?,
            shard: shard.ok_or_else(|| input.error("portable_command requires `shard:`"))?,
            field: field.ok_or_else(|| input.error("portable_command requires `field:`"))?,
            roles,
            load,
            invoke,
            payload,
            handle,
            guard,
            defaults,
            constructor,
            authenticated_user_field,
            preview_reduce_known_record,
        })
    }
}

fn lit_strs_from_array(expr: &Expr) -> syn::Result<Vec<LitStr>> {
    let Expr::Array(array) = expr else {
        return Err(syn::Error::new_spanned(
            expr,
            "roles must be an array of string literals, e.g. [\"user\", \"admin\"]",
        ));
    };
    array
        .elems
        .iter()
        .map(|elem| match elem {
            Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(value),
                ..
            }) => Ok(value.clone()),
            other => Err(syn::Error::new_spanned(
                other,
                "roles entries must be string literals",
            )),
        })
        .collect()
}

fn names_from_command(name: &LitStr) -> syn::Result<(Ident, Ident)> {
    let value = name.value();
    let last = value.rsplit('.').next().unwrap_or(value.as_str());
    if last.is_empty() || !last.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(syn::Error::new(
            name.span(),
            "command name must end in a snake_case ident (e.g. todo.complete)",
        ));
    }
    let pascal = last
        .split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
            }
        })
        .collect::<String>();
    Ok((format_ident!("{pascal}"), Ident::new(last, name.span())))
}

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let framework = crate::shared::framework_path()?;
    let args = syn::parse2::<PortableCommandArgs>(input)?;
    let (ty, default_ctor) = names_from_command(&args.name)?;
    let ctor = args.constructor.as_ref().unwrap_or(&default_ctor);
    let name = &args.name;
    let transition = &args.transition;
    let aggregate = &args.aggregate;
    let input_ty = &args.input;
    let outcome = &args.outcome;
    let shard = &args.shard;
    let field = &args.field;
    let roles = &args.roles;
    let defaults = args.defaults.as_ref().map(|defaults| {
        quote! { .input_defaults(#defaults) }
    });
    let authenticated_user_field = args.authenticated_user_field.as_ref().map(|value| {
        let event = &value.event;
        let state = &value.state;
        let field = &value.field;
        quote! { .authenticated_user_field::<#event, #state>(#field) }
    });
    let preview_reduce_known_record = args.preview_reduce_known_record.as_ref().map(|preview| {
        quote! {
            .preview_reduce_known_record(
                (#preview).declared_in_rust_package(env!("CARGO_PKG_NAME"))
            )
        }
    });

    let install_body = if let Some(handle) = &args.handle {
        if args.invoke.is_some() || args.payload.is_some() {
            return Err(syn::Error::new_spanned(
                handle,
                "handle: is the escape hatch; do not also set invoke:/payload:",
            ));
        }
        let finish = match &args.guard {
            Some(guard) => quote! { .guarded(#guard, #handle) },
            None => quote! { .handle(#handle) },
        };
        quote! {
            routes
                .command_transition::<#transition, #input_ty, #outcome>(Self::COMMAND)
                .field_name(#field)
                .roles([#(#roles),*].into_iter())
                #defaults
                #authenticated_user_field
                #preview_reduce_known_record
                #finish
        }
    } else {
        let invoke = args.invoke.as_ref().ok_or_else(|| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                "thin portable_command requires `invoke:` (or `handle:` as the escape hatch)",
            )
        })?;
        let payload = args.payload.as_ref().ok_or_else(|| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                "thin portable_command requires `payload:` (or `handle:` as the escape hatch)",
            )
        })?;
        if args.guard.is_some() {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                "guard: is only valid with handle:; thin commands admit via roles",
            ));
        }
        let load = match args.load {
            LoadKind::Required => quote! { .load_by(|input: &#input_ty| Self::shard(input)) },
            LoadKind::Create => quote! { .create() },
            LoadKind::None => {
                return Err(syn::Error::new(
                    proc_macro2::Span::call_site(),
                    "thin portable_command requires `load: required` or `load: create`",
                ))
            }
        };
        let finish = thin_finish(outcome, payload)?;
        quote! {
            routes
                .command_transition::<#transition, #input_ty, #outcome>(Self::COMMAND)
                .field_name(#field)
                .roles([#(#roles),*].into_iter())
                #defaults
                #authenticated_user_field
                #preview_reduce_known_record
                #load
                .invoke(#invoke)
                #finish
        }
    };

    Ok(quote! {
        pub struct #ty;

        pub fn #ctor() -> #ty {
            #ty
        }

        impl #ty {
            pub const COMMAND: &'static str = #name;

            pub fn shard(input: &#input_ty) -> String {
                let shard: fn(&#input_ty) -> String = #shard;
                shard(input)
            }
        }

        impl<D> #framework::microsvc::PortableCommand<D> for #ty
        where
            D: #framework::microsvc::CausalRouteDependencies<Aggregate = #aggregate>
                + Send
                + Sync
                + 'static,
        {
            fn install(self, routes: #framework::microsvc::Routes<D>) -> #framework::microsvc::Routes<D> {
                #install_body
            }
        }
    })
}

fn thin_finish(outcome: &Type, payload: &Expr) -> syn::Result<TokenStream> {
    let last = match outcome {
        Type::Path(path) => path
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string()),
        _ => None,
    };
    match last.as_deref() {
        Some("Eventual") => Ok(quote! { .eventual(#payload) }),
        Some("Succeeded") => Ok(quote! { .succeeded(#payload) }),
        Some("Atomic") => Err(syn::Error::new_spanned(
            outcome,
            "thin portable_command does not yet support Atomic; use handle:",
        )),
        _ => Err(syn::Error::new_spanned(
            outcome,
            "thin portable_command outcome must be Eventual<...> or Succeeded<...>",
        )),
    }
}
