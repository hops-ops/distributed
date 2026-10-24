//! `#[command("aggregate.verb", ..)]` on an aggregate method inside a
//! `#[sourced]` impl: the method is the command. `#[sourced]` strips the
//! attribute and emits a portable mount (`pub struct Verb`) whose generated
//! handler binds the principal from the verified session, loads (or
//! creates) the aggregate, calls the method, maps its error to `Rejected`,
//! builds the payload and commits with publication.
//!
//! ```ignore
//! #[command("crawler.record_kill", shard = principal, load = or_create, outcome = succeeded)]
//! pub fn record_kill(&mut self, #[principal] principal: &str, input: RecordKillInput)
//!     -> Result<KillReceipt, CrawlerError> { .. }
//! ```
//!
//! Options: `shard = <input field | principal parameter>` (required unless
//! `load = create`), `load = required | or_create | create`,
//! `outcome = succeeded | eventual | Succeeded<P> | Eventual<P>`,
//! `payload = path` (`fn(&Aggregate) -> P`; otherwise the method's `Ok`
//! value is the payload), `roles = ["player", ..]`, `guard = expr`,
//! `field = "graphql_field"`, `input = Type` (map the remaining method
//! parameters to the input's fields by name instead of passing the whole
//! input), `transition = path` (override the domain transition witness),
//! `mount = Ident` (name of the generated struct).

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote, quote_spanned};
use syn::parse::{Parse, ParseStream};
use syn::{Expr, FnArg, Ident, ImplItemFn, LitStr, Pat, Path, ReturnType, Token, Type};

#[derive(Clone, Copy, PartialEq)]
enum Load {
    Required,
    OrCreate,
    Create,
}

enum Outcome {
    /// `succeeded` / `eventual`: the payload type is the method's `Ok` type.
    Keyword { eventual: bool, span: Span },
    /// `Succeeded<P>` / `Eventual<P>`.
    Type { ty: Box<Type>, eventual: bool },
}

struct Args {
    name: LitStr,
    shard: Option<Ident>,
    load: Load,
    outcome: Outcome,
    payload: Option<Path>,
    roles: Option<Vec<LitStr>>,
    guard: Option<Expr>,
    field: Option<LitStr>,
    input: Option<Type>,
    transition: Option<Path>,
    mount: Option<Ident>,
}

impl Parse for Args {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let name: LitStr = input.parse().map_err(|_| {
            input.error("#[command] on an aggregate method starts with the command name: #[command(\"aggregate.verb\", ..)]")
        })?;
        validate_name(&name)?;
        let mut shard = None;
        let mut load = None;
        let mut outcome = None;
        let mut payload = None;
        let mut roles = None;
        let mut guard = None;
        let mut field = None;
        let mut input_ty = None;
        let mut transition = None;
        let mut mount = None;
        let mut seen = std::collections::HashSet::new();
        while !input.is_empty() {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
            let key: Ident = input.parse()?;
            if !seen.insert(key.to_string()) {
                return Err(syn::Error::new(
                    key.span(),
                    format!("duplicate #[command] option `{key}`"),
                ));
            }
            input.parse::<Token![=]>()?;
            match key.to_string().as_str() {
                "shard" => shard = Some(input.parse::<Ident>()?),
                "load" => {
                    let value: Ident = input.parse()?;
                    load = Some(match value.to_string().as_str() {
                        "required" => Load::Required,
                        "or_create" => Load::OrCreate,
                        "create" => Load::Create,
                        other => {
                            return Err(syn::Error::new(
                                value.span(),
                                format!("load must be `required`, `or_create` or `create`, not `{other}`"),
                            ))
                        }
                    });
                }
                "outcome" => {
                    let ty: Type = input.parse()?;
                    outcome = Some(parse_outcome(ty)?);
                }
                "payload" => payload = Some(input.parse::<Path>()?),
                "roles" => {
                    let expr: Expr = input.parse()?;
                    roles = Some(role_list(&expr)?);
                }
                "guard" => guard = Some(input.parse::<Expr>()?),
                "field" => field = Some(input.parse::<LitStr>()?),
                "input" => input_ty = Some(input.parse::<Type>()?),
                "transition" => transition = Some(input.parse::<Path>()?),
                "mount" => mount = Some(input.parse::<Ident>()?),
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!(
                            "unknown #[command] option `{other}`; expected `shard`, `load`, `outcome`, `payload`, `roles`, `guard`, `field`, `input`, `transition` or `mount`"
                        ),
                    ))
                }
            }
        }
        let load = load.ok_or_else(|| {
            syn::Error::new(
                name.span(),
                "#[command] needs `load = required | or_create | create`",
            )
        })?;
        let outcome = outcome.ok_or_else(|| {
            syn::Error::new(
                name.span(),
                "#[command] needs `outcome = succeeded | eventual` (or `Succeeded<P>` / `Eventual<P>`)",
            )
        })?;
        if load != Load::Create && shard.is_none() {
            return Err(syn::Error::new(
                name.span(),
                "#[command] needs `shard = <input field or principal parameter>` to load the aggregate",
            ));
        }
        Ok(Self {
            name,
            shard,
            load,
            outcome,
            payload,
            roles,
            guard,
            field,
            input: input_ty,
            transition,
            mount,
        })
    }
}

fn validate_name(name: &LitStr) -> syn::Result<()> {
    let value = name.value();
    let segments: Vec<&str> = value.split('.').collect();
    if segments.len() < 2
        || segments.iter().any(|segment| {
            segment.is_empty()
                || !segment
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        })
    {
        return Err(syn::Error::new(
            name.span(),
            "command name must be 2+ dot-separated snake_case segments (e.g. \"character.equip\")",
        ));
    }
    Ok(())
}

fn parse_outcome(ty: Type) -> syn::Result<Outcome> {
    let Type::Path(path) = &ty else {
        return Err(syn::Error::new_spanned(
            &ty,
            "outcome must be `succeeded`, `eventual`, `Succeeded<P>` or `Eventual<P>`",
        ));
    };
    let last = path.path.segments.last().expect("non-empty path");
    let ident = last.ident.to_string();
    let bare = path.path.segments.len() == 1 && last.arguments.is_empty();
    match ident.as_str() {
        "succeeded" | "eventual" if bare => Ok(Outcome::Keyword {
            eventual: ident == "eventual",
            span: last.ident.span(),
        }),
        "Succeeded" | "Eventual" if !last.arguments.is_empty() => Ok(Outcome::Type {
            eventual: ident == "Eventual",
            ty: Box::new(ty.clone()),
        }),
        _ => Err(syn::Error::new_spanned(
            &ty,
            "outcome must be `succeeded`, `eventual`, `Succeeded<P>` or `Eventual<P>`",
        )),
    }
}

fn role_list(expr: &Expr) -> syn::Result<Vec<LitStr>> {
    let Expr::Array(array) = expr else {
        return Err(syn::Error::new_spanned(
            expr,
            "roles must be an array of string literals, e.g. [\"player\"]",
        ));
    };
    let mut roles = Vec::new();
    for elem in &array.elems {
        match elem {
            Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(value),
                ..
            }) if !value.value().trim().is_empty() => roles.push(value.clone()),
            other => {
                return Err(syn::Error::new_spanned(
                    other,
                    "roles entries must be non-empty string literals",
                ))
            }
        }
    }
    if roles.is_empty() {
        return Err(syn::Error::new_spanned(
            expr,
            "roles must name at least one role (omit `roles` for an internal command)",
        ));
    }
    Ok(roles)
}

/// One parameter after `self`.
struct Param {
    name: Ident,
    ty: Type,
}

/// A `#[command]` aggregate method, collected by `#[sourced]`.
pub(crate) struct CommandMethod {
    args: Args,
    method: Ident,
    principal: Param,
    /// Whole-input parameter (no `input = T`), or the parameters mapped to
    /// input fields by name.
    params: Vec<Param>,
    /// Every parameter name after `self`, in declaration order.
    order: Vec<Ident>,
    ok_type: Option<Type>,
}

impl CommandMethod {
    pub(crate) fn method(&self) -> &Ident {
        &self.method
    }
}

/// Strip `#[command(..)]` (and `#[principal]`) from `method`; `None` when it
/// is not a command.
pub(crate) fn take(method: &mut ImplItemFn) -> syn::Result<Option<CommandMethod>> {
    let Some(index) = method
        .attrs
        .iter()
        .position(|attr| attr.path().is_ident("command"))
    else {
        return Ok(None);
    };
    let attr = method.attrs.remove(index);
    let args: Args = attr.parse_args()?;
    if method.sig.asyncness.is_some() {
        return Err(syn::Error::new_spanned(
            method.sig.asyncness,
            "#[command] aggregate methods are synchronous decisions; the generated handler does the I/O",
        ));
    }
    if !matches!(
        method.sig.inputs.first(),
        Some(FnArg::Receiver(receiver)) if receiver.reference.is_some() && receiver.mutability.is_some()
    ) {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "#[command] methods take `&mut self`",
        ));
    }
    let mut principal = None;
    let mut params = Vec::new();
    let mut order = Vec::new();
    for arg in method.sig.inputs.iter_mut().skip(1) {
        let FnArg::Typed(typed) = arg else { continue };
        let Pat::Ident(pat) = &*typed.pat else {
            return Err(syn::Error::new_spanned(
                &typed.pat,
                "#[command] parameters must be plain identifiers",
            ));
        };
        let is_principal = typed
            .attrs
            .iter()
            .any(|attr| attr.path().is_ident("principal"));
        typed
            .attrs
            .retain(|attr| !attr.path().is_ident("principal"));
        let param = Param {
            name: pat.ident.clone(),
            ty: (*typed.ty).clone(),
        };
        order.push(param.name.clone());
        if is_principal {
            if principal.is_some() {
                return Err(syn::Error::new_spanned(
                    &typed.pat,
                    "only one #[principal] parameter",
                ));
            }
            principal = Some(param);
        } else {
            params.push(param);
        }
    }
    let principal = principal.ok_or_else(|| {
        syn::Error::new_spanned(
            &method.sig.inputs,
            "a #[command] method needs a `#[principal]` parameter (`&str` or `String`): the actor comes from the verified session, never from the input",
        )
    })?;
    if args.input.is_none() && params.len() != 1 {
        return Err(syn::Error::new_spanned(
            &method.sig.inputs,
            "a #[command] method takes `#[principal]` and one input parameter; with `input = Type` its other parameters are the input's fields by name",
        ));
    }
    let ok_type = match &method.sig.output {
        ReturnType::Type(_, ty) => result_ok_type(ty),
        ReturnType::Default => None,
    };
    if ok_type.is_none() {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "#[command] methods return `Result<T, E>` (`E: Display` becomes the rejection; `T` is the payload unless `payload = ..` is set)",
        ));
    }
    if let (Outcome::Keyword { span, .. }, Some(_)) = (&args.outcome, &args.payload) {
        return Err(syn::Error::new(
            *span,
            "with `payload = ..` name the payload type: `outcome = Succeeded<P>` or `Eventual<P>`",
        ));
    }
    Ok(Some(CommandMethod {
        args,
        method: method.sig.ident.clone(),
        principal,
        params,
        order,
        ok_type,
    }))
}

/// `T` of `Result<T, E>` (any path ending in `Result` with two arguments, or
/// one for an alias like `SourcedResult<T>`); `()` for a bare alias.
fn result_ok_type(ty: &Type) -> Option<Type> {
    let Type::Path(path) = ty else { return None };
    let last = path.path.segments.last()?;
    if !last.ident.to_string().ends_with("Result") {
        return None;
    }
    match &last.arguments {
        syn::PathArguments::AngleBracketed(args) => match args.args.first()? {
            syn::GenericArgument::Type(ok) => Some(ok.clone()),
            _ => None,
        },
        syn::PathArguments::None => Some(syn::parse_quote!(())),
        syn::PathArguments::Parenthesized(_) => None,
    }
}

fn is_reference(ty: &Type) -> bool {
    matches!(ty, Type::Reference(_))
}

fn pascal(segment: &str) -> String {
    segment
        .split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

/// The mount, its handler and its registration. `transition` is the domain
/// transition witness `#[sourced]` discovered for the method, if any.
pub(crate) fn expand(
    command: &CommandMethod,
    aggregate: &Type,
    transition: Option<TokenStream>,
    framework: &TokenStream,
) -> syn::Result<TokenStream> {
    let fw = framework;
    let args = &command.args;
    let name = &args.name;
    let name_value = name.value();
    let last = name_value.rsplit('.').next().expect("validated");
    let mount = args
        .mount
        .clone()
        .unwrap_or_else(|| format_ident!("{}", pascal(last), span = name.span()));
    let method = &command.method;
    let principal = &command.principal;
    let principal_name = principal.name.to_string();

    let input_ty: Type = match &args.input {
        Some(ty) => ty.clone(),
        None => command.params[0].ty.clone(),
    };

    // Payload type.
    let payload_ty: Type = match &args.outcome {
        Outcome::Type { ty, .. } => {
            let Type::Path(path) = &**ty else {
                unreachable!()
            };
            let last = path.path.segments.last().expect("path");
            match &last.arguments {
                syn::PathArguments::AngleBracketed(a) => match a.args.first() {
                    Some(syn::GenericArgument::Type(p)) => p.clone(),
                    _ => return Err(syn::Error::new_spanned(ty, "expected `Succeeded<P>`")),
                },
                _ => return Err(syn::Error::new_spanned(ty, "expected `Succeeded<P>`")),
            }
        }
        Outcome::Keyword { .. } => command.ok_type.clone().expect("checked"),
    };
    let eventual = match &args.outcome {
        Outcome::Keyword { eventual, .. } | Outcome::Type { eventual, .. } => *eventual,
    };
    let outcome_ty = if eventual {
        quote!(#fw::command::Eventual<#payload_ty>)
    } else {
        quote!(#fw::command::Succeeded<#payload_ty>)
    };
    let terminal = if eventual {
        quote!(eventual)
    } else {
        quote!(succeeded)
    };

    // Shard.
    let shard_by_principal = args.shard.as_ref().is_some_and(|s| *s == principal.name);
    let shard_fn = match &args.shard {
        Some(field) if !shard_by_principal => Some(quote! {
            /// Aggregate id (shard) of a command input.
            pub fn shard(input: &#input_ty) -> ::std::string::String {
                ::std::string::ToString::to_string(&input.#field)
            }
        }),
        _ => None,
    };
    let shard_expr = if shard_by_principal {
        quote!(::std::clone::Clone::clone(&__principal))
    } else {
        quote!(Self::shard(&input))
    };
    let load = match args.load {
        Load::Required => quote! {
            let __id = #shard_expr;
            let mut __checkout = #fw::microsvc::require_loaded(ctx.get(&__id).await?, __id)?;
        },
        Load::OrCreate => quote! {
            let __id = #shard_expr;
            let mut __checkout = match ctx.get(&__id).await? {
                ::std::option::Option::Some(checkout) => checkout,
                ::std::option::Option::None => ctx.create(),
            };
        },
        Load::Create => quote! {
            let mut __checkout = ctx.create();
        },
    };

    // Call arguments.
    let principal_arg = if is_reference(&principal.ty) {
        quote!(&__principal)
    } else {
        quote!(::std::convert::Into::into(::std::clone::Clone::clone(
            &__principal
        )))
    };
    let call_args: Vec<TokenStream> = if args.input.is_some() {
        command
            .params
            .iter()
            .map(|param| {
                let field = &param.name;
                if is_reference(&param.ty) {
                    quote_spanned!(field.span()=> &input.#field)
                } else {
                    quote_spanned!(field.span()=> input.#field)
                }
            })
            .collect()
    } else {
        vec![quote!(input)]
    };
    let payload = match &args.payload {
        Some(payload) => quote! {
            let _ = __out;
            let __payload: #payload_ty = #payload(__checkout.aggregate());
        },
        None => quote! { let __payload: #payload_ty = __out; },
    };

    // Compile-time principal check: no input field may be named like the
    // principal parameter (the session is its only source).
    let message = format!(
        "#[command(\"{name_value}\")]: the input has a field named `{principal_name}`, the #[principal] parameter; the principal comes from the verified session only, so it can never be an input field"
    );
    let principal_check = quote_spanned! {principal.name.span()=>
        const _: () = {
            if #fw::command::__input_has_field(
                <#input_ty as #fw::command::CommandInputType>::FIELD_NAMES,
                #principal_name,
            ) {
                ::core::panic!(#message);
            }
        };
    };

    let transition = match &args.transition {
        Some(path) => Some(quote!(#path)),
        None => transition,
    };
    let registration = match &transition {
        Some(transition) => quote! {
            routes.command_transition::<#transition, #input_ty, #outcome_ty>(Self::COMMAND)
        },
        None => quote! {
            routes.typed_command(#fw::command::typed_command::<#input_ty, #outcome_ty>(Self::COMMAND))
        },
    };
    let field = args.field.as_ref().map(|field| quote!(.field_name(#field)));
    let roles = args
        .roles
        .as_ref()
        .map(|roles| quote!(.roles([#(#roles),*])));
    let finish = match &args.guard {
        Some(guard) => quote!(.guarded(#guard, Self::handle)),
        None => quote!(.handle(Self::handle)),
    };
    let doc = format!(
        "Mounts `{name_value}` (`{}::{method}`): principal from the verified session, then load, call, commit.",
        quote!(#aggregate).to_string().replace(' ', "")
    );

    let args_in_order = order_args(command, &principal_arg, &call_args);

    Ok(quote! {
        #[doc = #doc]
        #[derive(Debug, Clone, Copy, Default)]
        pub struct #mount;

        #principal_check

        impl #mount {
            /// Command name.
            pub const COMMAND: &'static str = #name;

            #shard_fn

            /// The generated handler (also usable from a hand-written mount).
            pub async fn handle(
                ctx: &#fw::microsvc::CausalCommandContext<'_, #aggregate>,
                input: #input_ty,
            ) -> ::std::result::Result<
                #fw::command::PreparedCommand<#outcome_ty>,
                #fw::microsvc::HandlerError,
            > {
                // The principal: the verified session only.
                let __principal: ::std::string::String = ctx.user_id()?.to_owned();
                #load
                let __out = __checkout
                    .aggregate_mut()
                    .#method(#(#args_in_order),*)
                    .map_err(|error| #fw::microsvc::HandlerError::Rejected(error.to_string()))?;
                #payload
                ctx.publish_events().commit(__checkout)?.#terminal(__payload)
            }
        }

        impl<D> #fw::microsvc::PortableCommand<D> for #mount
        where
            D: #fw::microsvc::CausalRouteDependencies<Aggregate = #aggregate>
                + ::std::marker::Send
                + ::std::marker::Sync
                + 'static,
        {
            fn install(self, routes: #fw::microsvc::Routes<D>) -> #fw::microsvc::Routes<D> {
                #registration
                    #field
                    #roles
                    #finish
            }
        }
    })
}

/// Arguments in the method's declaration order.
fn order_args(
    command: &CommandMethod,
    principal_arg: &TokenStream,
    call_args: &[TokenStream],
) -> Vec<TokenStream> {
    let mut out = Vec::new();
    let mut rest = call_args.iter();
    for name in &command.order {
        if *name == command.principal.name {
            out.push(principal_arg.clone());
        } else if let Some(arg) = rest.next() {
            out.push(arg.clone());
        }
    }
    out
}
