//! `#[predict(StepType, kind = "player")]` on a step function
//! `fn(state: &S, input: &I, context: &C, dt: f32) -> S`.

use proc_macro2::TokenStream;
use quote::{format_ident, quote, quote_spanned};
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{FnArg, Ident, ItemFn, LitStr, Pat, ReturnType, Token, Type};

use crate::shared::runtime;

pub(crate) struct Args {
    step: Ident,
    kind: LitStr,
}

impl Parse for Args {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let step: Ident = input.parse().map_err(|e| {
            syn::Error::new(
                e.span(),
                "expected `#[predict(StepType, kind = \"kind_name\")]`",
            )
        })?;
        let mut kind = None;
        while !input.is_empty() {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            match key.to_string().as_str() {
                "kind" => kind = Some(input.parse::<LitStr>()?),
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!("unknown `#[predict]` option `{other}` (expected `kind`)"),
                    ))
                }
            }
        }
        let kind = kind.ok_or_else(|| {
            syn::Error::new(
                step.span(),
                "`#[predict]` needs the predicted kind: `kind = \"player\"`",
            )
        })?;
        if kind.value().is_empty() {
            return Err(syn::Error::new(kind.span(), "kind must not be empty"));
        }
        Ok(Self { step, kind })
    }
}

fn referenced(arg: &FnArg, what: &str) -> syn::Result<Type> {
    let FnArg::Typed(typed) = arg else {
        return Err(syn::Error::new(
            arg.span(),
            "a predicted step is a free function, not a method",
        ));
    };
    if !matches!(&*typed.pat, Pat::Ident(_) | Pat::Wild(_)) {
        return Err(syn::Error::new(
            typed.pat.span(),
            "use a plain binding for step arguments",
        ));
    }
    match &*typed.ty {
        Type::Reference(reference) if reference.mutability.is_none() => {
            Ok((*reference.elem).clone())
        }
        other => Err(syn::Error::new(
            other.span(),
            format!("the {what} argument must be a shared reference (`&T`)"),
        )),
    }
}

pub(crate) fn expand(args: Args, function: ItemFn) -> syn::Result<TokenStream> {
    let root = runtime()?;
    let sig = &function.sig;
    if !sig.generics.params.is_empty() || sig.asyncness.is_some() || sig.unsafety.is_some() {
        return Err(syn::Error::new(
            sig.span(),
            "a predicted step must be a plain, non-generic, synchronous fn",
        ));
    }
    let inputs: Vec<&FnArg> = sig.inputs.iter().collect();
    let [state_arg, input_arg, context_arg, dt_arg] = inputs.as_slice() else {
        return Err(syn::Error::new(
            sig.inputs.span(),
            "a predicted step takes `(state: &State, input: &Input, context: &Context, dt: f32)`",
        ));
    };
    let state = referenced(state_arg, "state")?;
    let input = referenced(input_arg, "input")?;
    let context = referenced(context_arg, "context")?;
    match dt_arg {
        FnArg::Typed(typed) if matches!(&*typed.ty, Type::Path(p) if p.path.is_ident("f32")) => {}
        other => {
            return Err(syn::Error::new(
                other.span(),
                "the last argument is the frame length `dt: f32` (seconds)",
            ))
        }
    }
    let ReturnType::Type(_, output) = &sig.output else {
        return Err(syn::Error::new(
            sig.span(),
            "a predicted step returns the next state",
        ));
    };
    let name = &sig.ident;
    let export = name.to_string();
    let export = export.strip_prefix("r#").unwrap_or(&export).to_string();
    let symbol = format_ident!("dg_predict_{}", export);
    let step = &args.step;
    let kind = &args.kind;
    let vis = &function.vis;
    let doc = format!(
        "The predicted step [`{export}`]: kind `{}`, exported to WebAssembly as `dg_predict_{export}`.",
        kind.value()
    );
    // Spanned at the return type: a step that does not return its state
    // fails there.
    let call = quote_spanned! {output.span()=>
        #name(state, input, context, dt)
    };
    Ok(quote! {
        #function

        #[doc = #doc]
        #[derive(Debug, Clone, Copy, Default)]
        #vis struct #step;

        impl #root::prediction::PredictStep for #step {
            const KIND: &'static str = #kind;
            const PACKAGE: &'static str = ::core::env!("CARGO_PKG_NAME");
            const EXPORT: &'static str = #export;
            type State = #state;
            type Input = #input;
            type Context = #context;

            fn step(state: &#state, input: &#input, context: &#context, dt: f32) -> #state {
                #call
            }
        }

        #[cfg(target_arch = "wasm32")]
        const _: () = {
            #[unsafe(no_mangle)]
            extern "C" fn #symbol(state_len: u32, input_len: u32, context_len: u32, dt: f32) -> i32 {
                #root::__private::wasm_export::<#step>(state_len, input_len, context_len, dt)
            }
        };
    })
}
