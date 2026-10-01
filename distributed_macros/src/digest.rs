use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{parse::Parser, Expr, Ident, ItemFn, LitStr, Token};

use crate::shared::{
    ensure_sourced_result_signature, extract_params_with_types, framework_path,
    generate_digest_call, wrap_result_body_with_guard,
};

pub(crate) fn expand_digest(attr: TokenStream2, item: TokenStream2) -> syn::Result<TokenStream2> {
    let args = parse_digest_args.parse2(attr)?;
    let mut func = syn::parse2::<ItemFn>(item)?;

    let framework = framework_path()?;
    let signature_synthesized =
        ensure_sourced_result_signature(&mut func.sig, "digest", &framework)?;

    let params = extract_params_with_types(&func.sig, "digest")?;
    let param_names: Vec<&Ident> = params.iter().map(|(name, _)| name).collect();
    let digest_call = generate_digest_call(
        &args.entity_field,
        &args.event_name,
        &param_names,
        args.version.as_ref(),
    );

    let new_body = wrap_result_body_with_guard(
        args.guard.as_ref(),
        digest_call,
        &func.block,
        signature_synthesized,
    );
    *func.block = new_body;

    Ok(quote! { #func })
}

pub(crate) struct DigestArgs {
    pub(crate) entity_field: syn::Ident,
    event_name: LitStr,
    pub(crate) guard: Option<Expr>,
    pub(crate) version: Option<syn::LitInt>,
}

pub(crate) fn parse_digest_args(input: syn::parse::ParseStream) -> syn::Result<DigestArgs> {
    // Check if first token is an identifier (potential entity field) or a string literal (event name)
    let (entity_field, event_name) = if input.peek(LitStr) {
        // No entity field specified, use default "entity"
        let event_name: LitStr = input.parse()?;
        (format_ident!("entity"), event_name)
    } else {
        // First token is an identifier - could be entity field or event name follows
        let first_ident: syn::Ident = input.parse()?;
        input.parse::<Token![,]>()?;
        let event_name: LitStr = input.parse()?;
        (first_ident, event_name)
    };

    let mut guard = None;
    let mut version = None;

    // Parse optional keyword arguments: `when = condition`, `version = N`
    while input.peek(Token![,]) {
        input.parse::<Token![,]>()?;
        // Allow (and ignore) a trailing comma.
        if input.is_empty() {
            break;
        }
        let ident: syn::Ident = input.parse()?;
        if ident == "when" {
            input.parse::<Token![=]>()?;
            guard = Some(input.parse()?);
        } else if ident == "version" {
            input.parse::<Token![=]>()?;
            version = Some(input.parse()?);
        } else {
            return Err(syn::Error::new_spanned(
                &ident,
                format!(
                    "unsupported key `{ident}` in #[digest(...)]; expected `when` or `version`"
                ),
            ));
        }
    }

    Ok(DigestArgs {
        entity_field,
        event_name,
        guard,
        version,
    })
}
