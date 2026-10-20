//! The deny list for fields a game must not let untrusted data carry.
//!
//! Zone facts (`FactCommand`) and ticket extension claims share it: identity
//! is bound by the host from the verified ticket, and rewards and economy
//! amounts are decided by the deterministic command handler, never by a
//! field the zone or the browser filled in.

use proc_macro2::Span;
use syn::{Expr, Field, LitStr};

use crate::shared::field_name;

/// Denied names and what they look like. Matching ignores `_`, `-` and case,
/// so `user_id`, `userId`, `USER-ID` and `UserID` all hit `user_id`.
const FORBIDDEN: &[(&str, &str)] = &[
    ("principal", "identity"),
    ("user", "identity"),
    ("user_id", "identity"),
    ("owner", "identity"),
    ("owner_id", "identity"),
    ("account", "identity"),
    ("account_id", "identity"),
    ("subject", "identity"),
    ("sub", "identity"),
    ("xp", "a reward"),
    ("experience", "a reward"),
    ("reward", "a reward"),
    ("rewards", "a reward"),
    ("amount", "an economy value"),
    ("coins", "an economy value"),
    ("gold", "an economy value"),
    ("currency", "an economy value"),
    ("price", "an economy value"),
    ("cost", "an economy value"),
    ("balance", "an economy value"),
    ("grant", "an economy value"),
    ("payout", "an economy value"),
    ("credit", "an economy value"),
    ("debit", "an economy value"),
];

fn normalized(name: &str) -> String {
    name.chars()
        .filter(|c| *c != '_' && *c != '-')
        .flat_map(char::to_lowercase)
        .collect()
}

/// What `name` looks like, if it is denied.
pub(crate) fn forbidden(name: &str) -> Option<&'static str> {
    let n = normalized(name);
    FORBIDDEN
        .iter()
        .find(|(bad, _)| normalized(bad) == n)
        .map(|(_, why)| *why)
}

/// Every name a field is seen under by a serde consumer: the Rust name plus
/// any `#[serde(rename = ..)]` / `#[serde(alias = ..)]`.
fn seen_names(field: &Field) -> Vec<(String, Span)> {
    let ident = field.ident.as_ref().expect("named");
    let mut names = vec![(field_name(ident), ident.span())];
    for a in field.attrs.iter().filter(|a| a.path().is_ident("serde")) {
        let _ = a.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") || meta.path.is_ident("alias") {
                if let Ok(value) = meta.value() {
                    if let Ok(lit) = value.parse::<LitStr>() {
                        names.push((lit.value(), lit.span()));
                    }
                }
            } else if meta.input.peek(syn::Token![=]) {
                let _ = meta.value()?.parse::<Expr>();
            } else if meta.input.peek(syn::token::Paren) {
                let _ = meta.parse_nested_meta(|_| Ok(()));
            }
            Ok(())
        });
    }
    names
}

/// Reject every denied field name (one spanned error each, combined).
/// `what` names the declaration ("fact input", "ticket claim") and `rule`
/// ends the message with why the value must come from elsewhere.
pub(crate) fn reject_forbidden(fields: &[&Field], what: &str, rule: &str) -> syn::Result<()> {
    let mut errors: Option<syn::Error> = None;
    for field in fields {
        for (name, span) in seen_names(field) {
            if let Some(why) = forbidden(&name) {
                let error = syn::Error::new(
                    span,
                    format!("{what} field `{name}` looks like {why}: {rule}"),
                );
                match &mut errors {
                    Some(all) => all.combine(error),
                    None => errors = Some(error),
                }
            }
        }
    }
    errors.map_or(Ok(()), Err)
}

#[cfg(test)]
mod tests {
    use super::forbidden;

    #[test]
    fn matches_ignore_separators_and_case() {
        for name in ["userId", "USER-ID", "user_id", "UserID", "Owner_Id"] {
            assert_eq!(forbidden(name), Some("identity"), "{name}");
        }
        for name in ["Gold", "GOLD", "pay_out", "Pay-Out", "c_o_i_n_s", "Balance"] {
            assert_eq!(forbidden(name), Some("an economy value"), "{name}");
        }
        for name in ["mob", "generation", "amounts", "coin", "costs", "kills"] {
            assert_eq!(forbidden(name), None, "{name}");
        }
    }
}
