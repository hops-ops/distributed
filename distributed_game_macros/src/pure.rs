//! `#[game_pure(FnType)]` on a pure function `fn(arg: &A) -> R` or
//! `fn() -> R`.

use proc_macro2::TokenStream;
use quote::{format_ident, quote, quote_spanned};
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{FnArg, Ident, ItemFn, Pat, ReturnType, Type};

use crate::shared::runtime;

pub(crate) struct Args {
    name: Ident,
}

impl Parse for Args {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let name: Ident = input
            .parse()
            .map_err(|e| syn::Error::new(e.span(), "expected `#[game_pure(FnType)]`"))?;
        if !input.is_empty() {
            return Err(input.error("`#[game_pure]` takes only the generated type name"));
        }
        Ok(Self { name })
    }
}

pub(crate) fn expand(args: Args, function: ItemFn) -> syn::Result<TokenStream> {
    let root = runtime()?;
    let sig = &function.sig;
    if !sig.generics.params.is_empty() || sig.asyncness.is_some() || sig.unsafety.is_some() {
        return Err(syn::Error::new(
            sig.span(),
            "a game pure function must be a plain, non-generic, synchronous fn",
        ));
    }
    let arg = match sig.inputs.iter().collect::<Vec<_>>().as_slice() {
        [] => None,
        [FnArg::Typed(typed)] => {
            if !matches!(&*typed.pat, Pat::Ident(_) | Pat::Wild(_)) {
                return Err(syn::Error::new(
                    typed.pat.span(),
                    "use a plain binding for the argument",
                ));
            }
            match &*typed.ty {
                Type::Reference(reference) if reference.mutability.is_none() => {
                    Some((*reference.elem).clone())
                }
                other => {
                    return Err(syn::Error::new(
                        other.span(),
                        "the argument must be a shared reference (`&T`, T: Wire)",
                    ))
                }
            }
        }
        [FnArg::Receiver(receiver)] => {
            return Err(syn::Error::new(
                receiver.span(),
                "a game pure function is a free function, not a method",
            ))
        }
        _ => {
            return Err(syn::Error::new(
                sig.inputs.span(),
                "a game pure function takes nothing or one argument `&T` (bundle several values in a #[derive(GameRecord)] struct)",
            ))
        }
    };
    let ReturnType::Type(_, output) = &sig.output else {
        return Err(syn::Error::new(
            sig.span(),
            "a game pure function returns a value (any `Wire` type)",
        ));
    };
    let fn_name = &sig.ident;
    let export = fn_name.to_string();
    let export = export.strip_prefix("r#").unwrap_or(&export).to_string();
    let symbol = format_ident!("dg_pure_{}", export);
    let name = &args.name;
    let vis = &function.vis;
    let doc =
        format!("The pure function [`{export}`], exported to WebAssembly as `dg_pure_{export}`.");
    let (arg_ty, call) = match &arg {
        Some(ty) => (quote!(#ty), quote_spanned! {output.span()=> #fn_name(arg) }),
        None => (
            quote!(()),
            quote_spanned! {output.span()=> { let _ = arg; #fn_name() } },
        ),
    };
    Ok(quote! {
        #function

        #[doc = #doc]
        #[derive(Debug, Clone, Copy, Default)]
        #vis struct #name;

        impl #root::pure::GamePure for #name {
            const PACKAGE: &'static str = ::core::env!("CARGO_PKG_NAME");
            const EXPORT: &'static str = #export;
            type Arg = #arg_ty;
            type Output = #output;

            fn call(arg: &#arg_ty) -> #output {
                #call
            }
        }

        #[cfg(target_arch = "wasm32")]
        const _: () = {
            #[unsafe(no_mangle)]
            extern "C" fn #symbol(arg_len: u32) -> i32 {
                #root::__private::pure_wasm_export::<#name>(arg_len)
            }
        };
    })
}
