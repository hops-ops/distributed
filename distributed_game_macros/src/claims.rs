//! `#[derive(TicketExtension)]`: declare the extension claims a ticket
//! issuer may vouch for (match id, team, party…).

use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, LitStr};

use crate::shared::{field_name, named_fields, no_generics, runtime};
use crate::trusted::reject_forbidden;

pub(crate) fn expand(input: DeriveInput) -> syn::Result<TokenStream> {
    no_generics(&input, "TicketExtension")?;
    let rt = runtime()?;
    // Container-wide renames would change the names the schema admits
    // behind the deny list's back.
    for attr in input.attrs.iter().filter(|a| a.path().is_ident("serde")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename_all") || meta.path.is_ident("rename_all_fields") {
                return Err(meta.error(
                    "#[derive(TicketExtension)] does not support `rename_all`; rename fields individually",
                ));
            }
            if meta.input.peek(syn::Token![=]) {
                let _ = meta.value()?.parse::<syn::Expr>()?;
            } else if meta.input.peek(syn::token::Paren) {
                meta.parse_nested_meta(|_| Ok(()))?;
            }
            Ok(())
        })?;
    }
    let fields = named_fields(&input, "TicketExtension")?;
    reject_forbidden(
        &fields,
        "ticket claim",
        "identity is only ever the verified `sub`, and rewards and economy values are decided by command handlers, never vouched for in a ticket",
    )?;
    let mut names = Vec::new();
    for field in &fields {
        let mut name = field_name(field.ident.as_ref().expect("named"));
        for attr in field.attrs.iter().filter(|a| a.path().is_ident("serde")) {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("flatten") {
                    return Err(meta.error(
                        "#[derive(TicketExtension)] does not support `flatten`; declare every claim as a field",
                    ));
                }
                if meta.path.is_ident("rename") {
                    if meta.input.peek(syn::Token![=]) {
                        name = meta.value()?.parse::<LitStr>()?.value();
                    } else {
                        return Err(meta.error("use `rename = \"..\"` (one name for both directions)"));
                    }
                } else if meta.input.peek(syn::Token![=]) {
                    let _ = meta.value()?.parse::<syn::Expr>()?;
                } else if meta.input.peek(syn::token::Paren) {
                    meta.parse_nested_meta(|_| Ok(()))?;
                }
                Ok(())
            })?;
        }
        names.push(name);
    }
    let ident = &input.ident;
    Ok(quote! {
        impl #rt::TicketExtension for #ident {
            const CLAIMS: &'static [&'static str] = &[#(#names),*];
        }
    })
}
