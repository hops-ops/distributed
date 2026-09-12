use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use std::collections::HashSet;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{Expr, FnArg, Ident, ItemFn, LitStr, PathArguments, ReturnType, Token, Type};

#[derive(Default)]
struct CommandArgs {
    id: Option<LitStr>,
    field_name: Option<LitStr>,
    input: Option<Type>,
    outcome: Option<Type>,
    roles: Option<Vec<LitStr>>,
    emits: Vec<Type>,
    applies: Option<Expr>,
    defaults: Option<Expr>,
    generated_defaults: Vec<(Ident, Ident)>,
}

impl Parse for CommandArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut args = Self::default();
        let mut seen_options = HashSet::new();
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            let option = key.to_string();
            if !seen_options.insert(option.clone()) {
                return Err(syn::Error::new(
                    key.span(),
                    format!("duplicate command option {option}"),
                ));
            }
            match key.to_string().as_str() {
                "roles" => {
                    let content;
                    syn::parenthesized!(content in input);
                    let values = Punctuated::<syn::Expr, Token![,]>::parse_terminated(&content)?;
                    let mut roles = Vec::new();
                    let mut seen_roles = HashSet::new();
                    for value in values {
                        let role = match value {
                            syn::Expr::Path(path) if path.path.segments.len() == 1 => LitStr::new(
                                &path.path.segments[0].ident.to_string(),
                                path.path.segments[0].ident.span(),
                            ),
                            syn::Expr::Lit(syn::ExprLit {
                                lit: syn::Lit::Str(value),
                                ..
                            }) => value,
                            other => {
                                return Err(syn::Error::new_spanned(
                                    other,
                                    "command roles must be identifiers or string literals",
                                ))
                            }
                        };
                        if role.value().trim().is_empty() {
                            return Err(syn::Error::new(
                                role.span(),
                                "command roles must not be empty",
                            ));
                        }
                        if !seen_roles.insert(role.value()) {
                            return Err(syn::Error::new(
                                role.span(),
                                "command roles must not contain duplicates",
                            ));
                        }
                        roles.push(role);
                    }
                    if roles.is_empty() {
                        return Err(content.error(
                            "command roles must declare at least one role; use an explicit anonymous role when intended",
                        ));
                    }
                    args.roles = Some(roles);
                }
                "emits" => {
                    let content;
                    syn::parenthesized!(content in input);
                    args.emits = Punctuated::<Type, Token![,]>::parse_terminated(&content)?
                        .into_iter()
                        .collect();
                }
                "applies" => {
                    let content;
                    syn::parenthesized!(content in input);
                    args.applies = Some(content.parse()?);
                }
                "defaults" | "input_defaults" => {
                    input.parse::<Token![=]>()?;
                    args.defaults = Some(input.parse()?);
                }
                "default" => {
                    let content;
                    syn::parenthesized!(content in input);
                    let target: Ident = content.parse()?;
                    content.parse::<Token![=]>()?;
                    let generator: Ident = content.parse()?;
                    if !content.is_empty() {
                        let arguments;
                        syn::parenthesized!(arguments in content);
                        if !arguments.is_empty() {
                            return Err(
                                arguments.error("input-default generators take no arguments")
                            );
                        }
                    }
                    if !matches!(generator.to_string().as_str(), "uuid_v7" | "ulid") {
                        return Err(syn::Error::new(
                            generator.span(),
                            "unknown input-default generator; expected uuid_v7() or ulid()",
                        ));
                    }
                    args.generated_defaults.push((target, generator));
                }
                "id" | "field" | "field_name" => {
                    input.parse::<Token![=]>()?;
                    let value: LitStr = input.parse()?;
                    if key == "id" {
                        args.id = Some(value);
                    } else {
                        args.field_name = Some(value);
                    }
                }
                "input" | "outcome" => {
                    input.parse::<Token![=]>()?;
                    let value: Type = input.parse()?;
                    if key == "input" {
                        args.input = Some(value);
                    } else {
                        args.outcome = Some(value);
                    }
                }
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!("unknown command option `{other}`"),
                    ))
                }
            }
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }
        Ok(args)
    }
}

fn validate_command_id(id: &LitStr) -> syn::Result<()> {
    let value = id.value();
    let segments = value.split('.').collect::<Vec<_>>();
    if value.len() > 128
        || segments.len() < 2
        || segments.iter().any(|segment| {
            segment.is_empty()
                || !segment.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || byte == b'_'
                        || byte == b'-'
                })
        })
    {
        return Err(syn::Error::new(
            id.span(),
            "command id must be 2+ dot-separated lowercase ASCII identifier segments",
        ));
    }
    Ok(())
}

pub fn expand(
    attr: proc_macro2::TokenStream,
    item: proc_macro2::TokenStream,
) -> syn::Result<TokenStream> {
    let framework = crate::shared::framework_path()?;
    let args = syn::parse2::<CommandArgs>(attr)?;
    let function = syn::parse2::<ItemFn>(item)?;
    let id = args.id.ok_or_else(|| {
        syn::Error::new(
            function.sig.ident.span(),
            "command declaration requires `id = \"...\"`",
        )
    })?;
    validate_command_id(&id)?;
    let roles = args.roles.ok_or_else(|| {
        syn::Error::new(
            function.sig.ident.span(),
            "command declaration requires explicit roles(...)",
        )
    })?;
    if !function.sig.asyncness.is_some() {
        return Err(syn::Error::new_spanned(
            function.sig.fn_token,
            "typed command handlers must be async",
        ));
    }
    let typed_args = function
        .sig
        .inputs
        .iter()
        .filter_map(|argument| match argument {
            FnArg::Typed(argument) => Some(argument),
            FnArg::Receiver(_) => None,
        })
        .collect::<Vec<_>>();
    if function
        .sig
        .inputs
        .iter()
        .any(|argument| matches!(argument, FnArg::Receiver(_)))
        || typed_args.len() != 2
    {
        return Err(syn::Error::new_spanned(
            &function.sig.inputs,
            "typed command handler must have exactly `(context: &CausalCommandContext<'_, Aggregate>, input: Input)` parameters",
        ));
    }
    let context_ty = &typed_args[0].ty;
    if !is_causal_context_type(context_ty) {
        return Err(syn::Error::new_spanned(
            &typed_args[0].ty,
            "first typed command parameter must be `&CausalCommandContext<'_, Aggregate>`",
        ));
    }
    let aggregate = causal_context_aggregate_type(context_ty).ok_or_else(|| {
        syn::Error::new_spanned(
            context_ty,
            "first typed command parameter must name the aggregate in `CausalCommandContext`",
        )
    })?;
    let inferred_input = (*typed_args[1].ty).clone();
    let declared_input = args.input.clone();
    let input = args.input.unwrap_or(inferred_input);
    let outcome = args
        .outcome
        .or_else(|| infer_prepared_outcome(&function.sig.output))
        .ok_or_else(|| {
            syn::Error::new_spanned(
                &function.sig.output,
                "command outcome is not inferable; provide `outcome = PreparedCommand<Outcome>`",
            )
        })?;
    validate_prepared_return(&function.sig.output, &outcome)?;
    if let Some(input) = declared_input.as_ref() {
        if !same_type(input, &typed_args[1].ty) {
            return Err(syn::Error::new_spanned(
                &typed_args[1].ty,
                "handler input parameter does not match the declared `input = ...` type",
            ));
        }
    }
    let field_name = args.field_name.unwrap_or_else(|| {
        LitStr::new(
            &id.value()
                .chars()
                .map(|character| {
                    if matches!(character, '.' | '-') {
                        '_'
                    } else {
                        character
                    }
                })
                .collect::<String>(),
            id.span(),
        )
    });
    let function_name = &function.sig.ident;
    let command_name = format_ident!("{}_command", function_name);
    let spec_name = format_ident!("{}_spec", function_name);
    let command_static = format_ident!("{}_COMMAND", function_name.to_string().to_uppercase());
    let spec_static = format_ident!("{}_SPEC", function_name.to_string().to_uppercase());
    let mount_name = format_ident!("{}_mount", function_name);
    let mount_static = format_ident!("{}_MOUNT", function_name.to_string().to_uppercase());
    let definition_name = format_ident!("{}_definition", function_name);
    let definition_static =
        format_ident!("{}_DEFINITION", function_name.to_string().to_uppercase());
    let command_id_static =
        format_ident!("{}_COMMAND_ID", function_name.to_string().to_uppercase());
    let accessor_name = format_ident!("{}_application_command", function_name);
    let register_name = format_ident!("{}_register", function_name);
    let visibility = &function.vis;
    let emits = args.emits;
    let applies = args.applies;
    let defaults = args.defaults;
    let generated_defaults = args.generated_defaults;
    let mut builder = quote! {
        #framework::command::typed_command::<#input, #outcome>(#id)
            .field_name(#field_name)
    };
    builder.extend(quote! { .roles([#(#roles),*]) });
    if !emits.is_empty() {
        builder.extend(quote! { .emits(#framework::events!(#(#emits),*)) });
    }
    if let Some(applies) = applies {
        builder.extend(quote! { .applies(#applies) });
    }
    if let Some(defaults) = defaults {
        builder.extend(quote! { .input_defaults(#defaults) });
    } else if !generated_defaults.is_empty() {
        let defaults = generated_defaults.iter().map(|(field, generator)| {
            quote! { default input.#field = #generator(); }
        });
        builder.extend(quote! {
            .input_defaults(#framework::command_input_defaults! {
                input: #input;
                #(#defaults)*
            })
        });
    }
    Ok(quote! {
        #[allow(unexpected_cfgs)]
        #[cfg(feature = "application-runtime")]
        #function

        #visibility fn #command_name() -> #framework::command::TypedCommand<#input, #outcome> {
            #builder
        }

        #visibility static #command_static: ::std::sync::LazyLock<
            #framework::command::TypedCommand<#input, #outcome>
        > = ::std::sync::LazyLock::new(#command_name);

        #visibility fn #spec_name() -> #framework::application::ApplicationResult<
            #framework::application::CommandSpec
        > {
            (#command_static).spec()
        }

        #visibility static #spec_static: ::std::sync::LazyLock<
            #framework::application::CommandSpec
        > = ::std::sync::LazyLock::new(||
            #spec_name().unwrap_or_else(|error| panic!("invalid generated command spec: {error}"))
        );

        #visibility fn #accessor_name() -> &'static #framework::application::CommandSpec {
            &#spec_static
        }

        #visibility const #command_id_static: &str = #id;

        #[allow(unexpected_cfgs)]
        #[cfg(feature = "application-runtime")]
        #visibility fn #mount_name() -> #framework::application::CommandMount {
            #framework::application::CommandMount::from_typed_route(
                (#spec_static).clone(),
                #id,
            )
        }

        #[allow(unexpected_cfgs)]
        #[cfg(feature = "application-runtime")]
        #visibility static #mount_static: ::std::sync::LazyLock<
            #framework::application::CommandMount
        > = ::std::sync::LazyLock::new(#mount_name);

        #[allow(unexpected_cfgs)]
        #[cfg(feature = "application-runtime")]
        #visibility fn #register_name<D>(
            routes: #framework::microsvc::Routes<D>,
        ) -> #framework::microsvc::Routes<D>
        where
            D: #framework::microsvc::CausalRouteDependencies<Aggregate = #aggregate>
                + Send
                + Sync
                + 'static,
            #aggregate: #framework::Aggregate + Send + Sync + 'static,
            #input: #framework::__private::serde::de::DeserializeOwned + Send + 'static,
            #outcome: #framework::command::CommandOutcome,
        {
            routes
                .typed_command((#command_static).clone())
                .mount((#mount_static).clone())
                .handle(#function_name)
        }

        #[allow(unexpected_cfgs)]
        #[cfg(feature = "application-runtime")]
        #visibility fn #definition_name() -> #framework::application::CommandDefinition {
            #framework::application::CommandDefinition::from_typed_command(
                (#command_static).clone(),
                Some((#mount_static).clone()),
            )
            .unwrap_or_else(|error| panic!("invalid generated command definition: {error}"))
        }

        #[allow(unexpected_cfgs)]
        #[cfg(not(feature = "application-runtime"))]
        #visibility fn #definition_name() -> #framework::application::CommandDefinition {
            #framework::application::CommandDefinition::from_typed_command(
                (#command_static).clone(),
                None,
            )
            .unwrap_or_else(|error| panic!("invalid generated command definition: {error}"))
        }

        #visibility static #definition_static: ::std::sync::LazyLock<
            #framework::application::CommandDefinition
        > = ::std::sync::LazyLock::new(#definition_name);
    })
}

fn is_causal_context_type(ty: &Type) -> bool {
    let Type::Reference(reference) = ty else {
        return false;
    };
    let Type::Path(path) = reference.elem.as_ref() else {
        return false;
    };
    path.path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == "CausalCommandContext")
}

fn causal_context_aggregate_type(ty: &Type) -> Option<Type> {
    let Type::Reference(reference) = ty else {
        return None;
    };
    let Type::Path(path) = reference.elem.as_ref() else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != "CausalCommandContext" {
        return None;
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    arguments.args.iter().find_map(|argument| match argument {
        syn::GenericArgument::Type(ty) => Some(ty.clone()),
        _ => None,
    })
}

fn validate_prepared_return(output: &ReturnType, outcome: &Type) -> syn::Result<()> {
    let Some((_, output)) = (match output {
        ReturnType::Type(arrow, output) => Some((arrow, output.as_ref())),
        ReturnType::Default => None,
    }) else {
        return Err(syn::Error::new_spanned(
            output,
            "typed command handler must return `Result<PreparedCommand<Outcome>, HandlerError>`",
        ));
    };
    let Type::Path(result) = output else {
        return Err(syn::Error::new_spanned(
            output,
            "typed command handler must return `Result<PreparedCommand<Outcome>, HandlerError>`",
        ));
    };
    let Some(result_segment) = result.path.segments.last() else {
        return Err(syn::Error::new_spanned(
            output,
            "missing Result return type",
        ));
    };
    if result_segment.ident != "Result" {
        return Err(syn::Error::new_spanned(
            output,
            "typed command handler return type must be Result<PreparedCommand<Outcome>, HandlerError>",
        ));
    }
    let PathArguments::AngleBracketed(arguments) = &result_segment.arguments else {
        return Err(syn::Error::new_spanned(
            output,
            "typed command handler Result must declare PreparedCommand and HandlerError types",
        ));
    };
    let mut types = arguments.args.iter().filter_map(|argument| match argument {
        syn::GenericArgument::Type(ty) => Some(ty),
        _ => None,
    });
    let Some(prepared) = types.next() else {
        return Err(syn::Error::new_spanned(
            output,
            "missing PreparedCommand return type",
        ));
    };
    let Some(error) = types.next() else {
        return Err(syn::Error::new_spanned(
            output,
            "missing HandlerError return type",
        ));
    };
    let Type::Path(prepared) = prepared else {
        return Err(syn::Error::new_spanned(
            prepared,
            "first Result type must be PreparedCommand<Outcome>",
        ));
    };
    let Some(prepared_segment) = prepared.path.segments.last() else {
        return Err(syn::Error::new_spanned(
            prepared,
            "missing PreparedCommand type",
        ));
    };
    if prepared_segment.ident != "PreparedCommand" {
        return Err(syn::Error::new_spanned(
            prepared,
            "first Result type must be PreparedCommand<Outcome>",
        ));
    }
    let PathArguments::AngleBracketed(prepared_args) = &prepared_segment.arguments else {
        return Err(syn::Error::new_spanned(
            prepared,
            "PreparedCommand must declare its outcome type",
        ));
    };
    let Some(syn::GenericArgument::Type(actual_outcome)) = prepared_args.args.first() else {
        return Err(syn::Error::new_spanned(
            prepared,
            "PreparedCommand must declare its outcome type",
        ));
    };
    if !same_type(actual_outcome, outcome) {
        return Err(syn::Error::new_spanned(
            actual_outcome,
            "PreparedCommand outcome does not match the declared `outcome = ...` type",
        ));
    }
    let Type::Path(error) = error else {
        return Err(syn::Error::new_spanned(
            error,
            "second Result type must be HandlerError",
        ));
    };
    if error
        .path
        .segments
        .last()
        .is_none_or(|segment| segment.ident != "HandlerError")
    {
        return Err(syn::Error::new_spanned(
            error,
            "second Result type must be HandlerError",
        ));
    }
    Ok(())
}

fn same_type(left: &Type, right: &Type) -> bool {
    quote!(#left).to_string().replace(' ', "") == quote!(#right).to_string().replace(' ', "")
}

fn infer_prepared_outcome(output: &ReturnType) -> Option<Type> {
    let ReturnType::Type(_, output) = output else {
        return None;
    };
    let Type::Path(path) = output.as_ref() else {
        return None;
    };
    let result = path.path.segments.last()?;
    if result.ident != "Result" {
        return None;
    }
    let PathArguments::AngleBracketed(arguments) = &result.arguments else {
        return None;
    };
    let first = arguments.args.first()?;
    let syn::GenericArgument::Type(Type::Path(prepared)) = first else {
        return None;
    };
    let prepared = prepared.path.segments.last()?;
    if prepared.ident != "PreparedCommand" {
        return None;
    }
    let PathArguments::AngleBracketed(arguments) = &prepared.arguments else {
        return None;
    };
    match arguments.args.first()? {
        syn::GenericArgument::Type(output) => Some(output.clone()),
        _ => None,
    }
}
