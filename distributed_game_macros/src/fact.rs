//! `#[derive(FactCommand)]`: bind a cell command input type to its command
//! name so a zone can `ctx.emit(player, input)`.

use proc_macro2::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::{DeriveInput, Expr, Ident};

use crate::shared::{find_attr, named_fields, no_generics, runtime};
use crate::trusted::reject_forbidden;

pub(crate) fn expand(input: DeriveInput) -> syn::Result<TokenStream> {
    no_generics(&input, "FactCommand")?;
    let rt = runtime()?;
    let Some(attr) = find_attr(&input.attrs, "fact") else {
        return Err(syn::Error::new(
            input.ident.span(),
            "#[derive(FactCommand)] needs #[fact(command = COMMAND_NAME)] (the constant the cell mounts the command under, or a string literal)",
        ));
    };
    let (mut command, mut key): (Option<Expr>, Option<Vec<Ident>>) = (None, None);
    attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("command") {
            command = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("key") {
            let mut parts = Vec::new();
            meta.parse_nested_meta(|part| {
                let Some(ident) = part.path.get_ident() else {
                    return Err(part.error("key parts are field names"));
                };
                parts.push(ident.clone());
                Ok(())
            })?;
            key = Some(parts);
        } else {
            return Err(meta.error("expected `command = ..` or `key(field, ..)` in #[fact(..)]"));
        }
        Ok(())
    })?;
    let Some(command) = command else {
        return Err(syn::Error::new(
            attr.path().span(),
            "#[fact(..)] needs `command = ..`",
        ));
    };

    let fields = named_fields(&input, "FactCommand")?;
    reject_forbidden(
        &fields,
        "fact input",
        "a zone fact reports what happened; the host binds the principal from the verified ticket and the command handler decides rewards and economy values",
    )?;

    let parts = match &key {
        None => quote!(::std::vec::Vec::new()),
        Some(parts) => {
            for part in parts {
                if !fields.iter().any(|f| f.ident.as_ref() == Some(part)) {
                    return Err(syn::Error::new(
                        part.span(),
                        format!("no field `{part}` in this struct"),
                    ));
                }
            }
            quote!(::std::vec![#(::std::string::ToString::to_string(&self.#parts)),*])
        }
    };
    let ident = &input.ident;
    Ok(quote! {
        impl #rt::FactCommand for #ident {
            const COMMAND: &'static str = #command;

            fn dedupe_parts(&self) -> ::std::vec::Vec<::std::string::String> {
                #parts
            }
        }
    })
}
