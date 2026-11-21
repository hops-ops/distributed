//! Attribute parsing and codegen helpers shared by the derives.

use proc_macro2::{Span, TokenStream};
use proc_macro_crate::{crate_name, FoundCrate};
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;
use syn::{Attribute, Data, DeriveInput, Expr, Field, Fields, Ident, LitStr, Type};

/// `distributed_game` as named by the consuming crate (renames and the
/// crate itself included).
pub(crate) fn runtime() -> syn::Result<TokenStream> {
    let root = match crate_name("distributed_game") {
        // `::distributed_game`, not `crate`: doctests of the crate itself
        // report `Itself` but compile as a separate crate. The lib declares
        // `extern crate self as distributed_game` so the path also works
        // inside it.
        Ok(FoundCrate::Itself) => quote!(::distributed_game),
        Ok(FoundCrate::Name(name)) => {
            let ident = Ident::new(&name, Span::call_site());
            quote!(::#ident)
        }
        Err(error) => {
            let message =
                format!("unable to resolve `distributed_game` for generated code: {error}");
            return Err(syn::Error::new(Span::call_site(), message));
        }
    };
    Ok(root)
}

/// Rust field name without a raw prefix.
pub(crate) fn field_name(ident: &Ident) -> String {
    let name = ident.to_string();
    name.strip_prefix("r#").map(str::to_string).unwrap_or(name)
}

/// `BoardSnapshot` → `board_snapshot`.
pub(crate) fn snake_case(name: &str) -> String {
    let mut out = String::new();
    for (i, c) in name.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// Named fields of a struct, or a spanned error naming the derive.
pub(crate) fn named_fields<'a>(
    input: &'a DeriveInput,
    derive: &str,
) -> syn::Result<Vec<&'a Field>> {
    match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => Ok(fields.named.iter().collect()),
            Fields::Unit => Ok(Vec::new()),
            Fields::Unnamed(_) => Err(syn::Error::new(
                input.ident.span(),
                format!("#[derive({derive})] needs a struct with named fields"),
            )),
        },
        _ => Err(syn::Error::new(
            input.ident.span(),
            format!("#[derive({derive})] only supports structs"),
        )),
    }
}

/// Reject generics (descriptors are per concrete type).
pub(crate) fn no_generics(input: &DeriveInput, derive: &str) -> syn::Result<()> {
    if input.generics.params.is_empty() {
        Ok(())
    } else {
        Err(syn::Error::new(
            input.generics.span(),
            format!("#[derive({derive})] does not support generic types"),
        ))
    }
}

/// Wire-level override of one field, from `#[quant]`, `#[angle]`,
/// `#[field_type]` or `#[long]`.
pub(crate) enum Special {
    /// `#[quant(min, max, bits)]` / `#[quant(centered, bits)]` / `#[angle(bits)]`
    /// / `#[field_type(EXPR)]`: an `f32` encoded as this `FieldType` expression.
    FieldType(TokenStream),
    /// `#[long]`: a varuint-length string.
    Long,
}

/// Every per-field attribute a derive may accept. Each derive rejects the
/// ones it does not support.
#[derive(Default)]
pub(crate) struct FieldAttrs {
    pub special: Option<(Special, Span)>,
    pub priority: Option<(Expr, Span)>,
    pub correct: Option<(TokenStream, Span)>,
    pub range: Option<(Expr, Expr, Span)>,
    pub buttons: Option<(Vec<Ident>, Span)>,
}

const FIELD_ATTRS: &[&str] = &[
    "quant",
    "angle",
    "field_type",
    "long",
    "priority",
    "correct",
    "range",
    "buttons",
];

fn set_special(attrs: &mut FieldAttrs, special: Special, span: Span) -> syn::Result<()> {
    if attrs.special.is_some() {
        return Err(syn::Error::new(
            span,
            "only one of #[quant], #[angle], #[field_type] or #[long] per field",
        ));
    }
    attrs.special = Some((special, span));
    Ok(())
}

pub(crate) fn parse_field_attrs(field: &Field, rt: &TokenStream) -> syn::Result<FieldAttrs> {
    let mut out = FieldAttrs::default();
    for attr in &field.attrs {
        let Some(name) = attr.path().get_ident().map(Ident::to_string) else {
            continue;
        };
        if !FIELD_ATTRS.contains(&name.as_str()) {
            continue;
        }
        let span = attr.path().span();
        match name.as_str() {
            "quant" => {
                let (mut min, mut max, mut centered, mut bits) = (None, None, None, None);
                attr.parse_nested_meta(|meta| {
                    let key = meta
                        .path
                        .get_ident()
                        .map(Ident::to_string)
                        .unwrap_or_default();
                    let value: Expr = meta.value()?.parse()?;
                    match key.as_str() {
                        "min" => min = Some(value),
                        "max" => max = Some(value),
                        "centered" => centered = Some(value),
                        "bits" => bits = Some(value),
                        _ => {
                            return Err(meta.error(
                                "expected `min`, `max`, `bits` or `centered` in #[quant(..)]",
                            ))
                        }
                    }
                    Ok(())
                })?;
                let Some(bits) = bits else {
                    return Err(syn::Error::new(span, "#[quant(..)] needs `bits = 1..=32`"));
                };
                let ty = match (min, max, centered) {
                    (Some(min), Some(max), None) => quote! {
                        #rt::__private::FieldType::QuantF32 {
                            min: (#min) as f32,
                            max: (#max) as f32,
                            bits: (#bits) as u8,
                        }
                    },
                    (None, None, Some(range)) => quote! {
                        #rt::__private::FieldType::quant_centered((#range) as f32, (#bits) as u8)
                    },
                    _ => {
                        return Err(syn::Error::new(
                            span,
                            "#[quant(..)] needs either `min` and `max`, or `centered = RANGE` (zero exactly representable)",
                        ))
                    }
                };
                set_special(&mut out, Special::FieldType(ty), span)?;
            }
            "angle" => {
                let mut bits = None;
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("bits") {
                        bits = Some(meta.value()?.parse::<Expr>()?);
                        Ok(())
                    } else {
                        Err(meta.error("expected `bits` in #[angle(bits = ..)]"))
                    }
                })?;
                let Some(bits) = bits else {
                    return Err(syn::Error::new(span, "#[angle(..)] needs `bits = 1..=32`"));
                };
                let ty = quote!(#rt::__private::FieldType::Angle { bits: (#bits) as u8 });
                set_special(&mut out, Special::FieldType(ty), span)?;
            }
            "field_type" => {
                let expr: Expr = attr.parse_args()?;
                set_special(&mut out, Special::FieldType(quote!((#expr))), span)?;
            }
            "long" => {
                attr.meta.require_path_only()?;
                set_special(&mut out, Special::Long, span)?;
            }
            "priority" => {
                if out.priority.is_some() {
                    return Err(syn::Error::new(span, "duplicate #[priority]"));
                }
                out.priority = Some((attr.parse_args()?, span));
            }
            "correct" => {
                if out.correct.is_some() {
                    return Err(syn::Error::new(span, "duplicate #[correct]"));
                }
                let mut policy = None;
                let mut ease_ms: Option<Expr> = None;
                let mut snap_above: Option<Expr> = None;
                attr.parse_nested_meta(|meta| {
                    let key = meta
                        .path
                        .get_ident()
                        .map(Ident::to_string)
                        .unwrap_or_default();
                    if key == "snap_above" {
                        if snap_above.is_some() {
                            return Err(meta.error("duplicate `snap_above`"));
                        }
                        snap_above = Some(meta.value()?.parse()?);
                        return Ok(());
                    }
                    if policy.is_some() {
                        return Err(meta.error("#[correct(..)] takes one policy"));
                    }
                    policy = Some(match key.as_str() {
                        "ease" => {
                            ease_ms = Some(if meta.input.peek(syn::Token![=]) {
                                meta.value()?.parse()?
                            } else {
                                syn::parse_quote!(100)
                            });
                            quote!()
                        }
                        "snap" => quote!(#rt::__private::Correction::Snap),
                        "replace" => quote!(#rt::__private::Correction::Replace),
                        _ => return Err(meta.error(
                            "expected `ease`, `ease = MS`, `snap` or `replace` (with `ease`: `snap_above = M`) in #[correct(..)]",
                        )),
                    });
                    Ok(())
                })?;
                let Some(mut policy) = policy else {
                    return Err(syn::Error::new(span, "#[correct(..)] needs a policy"));
                };
                match (ease_ms, snap_above) {
                    (Some(ms), above) => {
                        let above = match above {
                            Some(m) => quote!(::core::option::Option::Some((#m) as f32)),
                            None => quote!(::core::option::Option::None),
                        };
                        policy = quote!(#rt::__private::Correction::Ease {
                            duration_ms: (#ms) as u32,
                            snap_above: #above,
                        });
                    }
                    (None, Some(_)) => {
                        return Err(syn::Error::new(
                            span,
                            "`snap_above` only applies to `ease` in #[correct(..)]",
                        ))
                    }
                    (None, None) => {}
                }
                out.correct = Some((policy, span));
            }
            "range" => {
                if out.range.is_some() {
                    return Err(syn::Error::new(span, "duplicate #[range]"));
                }
                let (mut min, mut max) = (None, None);
                attr.parse_nested_meta(|meta| {
                    let value: Expr = meta.value()?.parse()?;
                    if meta.path.is_ident("min") {
                        min = Some(value);
                    } else if meta.path.is_ident("max") {
                        max = Some(value);
                    } else {
                        return Err(meta.error("expected `min` and `max` in #[range(..)]"));
                    }
                    Ok(())
                })?;
                match (min, max) {
                    (Some(min), Some(max)) => out.range = Some((min, max, span)),
                    _ => {
                        return Err(syn::Error::new(
                            span,
                            "#[range(..)] needs both `min` and `max`",
                        ))
                    }
                }
            }
            "buttons" => {
                if out.buttons.is_some() {
                    return Err(syn::Error::new(span, "duplicate #[buttons]"));
                }
                let mut names = Vec::new();
                attr.parse_nested_meta(|meta| {
                    let Some(ident) = meta.path.get_ident() else {
                        return Err(meta.error("button names are identifiers"));
                    };
                    if names.iter().any(|n: &Ident| n == ident) {
                        return Err(meta.error("duplicate button name"));
                    }
                    names.push(ident.clone());
                    Ok(())
                })?;
                if names.is_empty() || names.len() > 32 {
                    return Err(syn::Error::new(span, "#[buttons(..)] names 1..=32 bits"));
                }
                out.buttons = Some((names, span));
            }
            _ => unreachable!(),
        }
    }
    Ok(out)
}

/// Error for an attribute a derive does not accept on fields.
pub(crate) fn reject(span: Option<Span>, what: &str, derive: &str) -> syn::Result<()> {
    match span {
        Some(span) => Err(syn::Error::new(
            span,
            format!("{what} is not supported by #[derive({derive})]"),
        )),
        None => Ok(()),
    }
}

/// One payload field's encode / decode / descriptor tokens (inputs, events,
/// records).
pub(crate) struct PayloadCodec {
    pub encode: TokenStream,
    pub decode: TokenStream,
    pub wire_type: TokenStream,
    pub records: TokenStream,
}

/// Codec tokens for `value` (an expression of type `&ty`) of a payload field.
pub(crate) fn payload_codec(
    ty: &Type,
    value: TokenStream,
    special: Option<&Special>,
    rt: &TokenStream,
) -> PayloadCodec {
    let p = quote!(#rt::__private);
    let span = ty.span();
    match special {
        // `Vec<u8>` encodes like any list of bytes (varuint count + bytes) —
        // identical to `bytes` — but is described as `bytes` so generated
        // clients get a `Uint8Array`.
        None if is_byte_vec(ty) => PayloadCodec {
            encode: quote_spanned!(span=> <#ty as #p::Wire>::encode(#value, w);),
            decode: quote_spanned!(span=> <#ty as #p::Wire>::decode(r)?),
            wire_type: quote!(#p::WireType::Bytes),
            records: quote!(),
        },
        None => PayloadCodec {
            encode: quote_spanned!(span=> <#ty as #p::Wire>::encode(#value, w);),
            decode: quote_spanned!(span=> <#ty as #p::Wire>::decode(r)?),
            wire_type: quote_spanned!(span=> <#ty as #p::Wire>::wire_type()),
            records: quote_spanned!(span=> <#ty as #p::Wire>::records(records);),
        },
        Some(Special::FieldType(ft)) => PayloadCodec {
            encode: quote_spanned!(span=>
                #p::special::encode_f32(w, #ft, <#ty as #p::QuantizedF32>::get(#value));
            ),
            decode: quote_spanned!(span=>
                <#ty as #p::QuantizedF32>::from_f32(#p::special::decode_f32(r, #ft)?)
            ),
            wire_type: quote!(#p::WireType::of_field(#ft)),
            records: quote!(),
        },
        Some(Special::Long) => PayloadCodec {
            encode: quote_spanned!(span=>
                #p::special::encode_long(w, <#ty as #p::LongString>::text(#value));
            ),
            decode: quote_spanned!(span=>
                <#ty as #p::LongString>::from_text(#p::special::decode_long(r)?)
            ),
            wire_type: quote!(#p::WireType::Str),
            records: quote!(),
        },
    }
}

/// `Vec<u8>` written as such (aliases are not seen).
fn is_byte_vec(ty: &Type) -> bool {
    let Type::Path(path) = ty else { return false };
    let Some(last) = path.path.segments.last() else {
        return false;
    };
    if last.ident != "Vec" {
        return false;
    }
    let syn::PathArguments::AngleBracketed(args) = &last.arguments else {
        return false;
    };
    matches!(
        args.args.first(),
        Some(syn::GenericArgument::Type(Type::Path(inner))) if inner.path.is_ident("u8")
    ) && args.args.len() == 1
}

/// A `name = "..."` / `id = N` style container attribute.
pub(crate) fn find_attr<'a>(attrs: &'a [Attribute], name: &str) -> Option<&'a Attribute> {
    attrs.iter().find(|a| a.path().is_ident(name))
}

/// Validate a wire name (non-empty, ASCII identifier-ish).
pub(crate) fn check_name(lit: &LitStr) -> syn::Result<String> {
    let value = lit.value();
    let ok = !value.is_empty()
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    if ok {
        Ok(value)
    } else {
        Err(syn::Error::new(
            lit.span(),
            "names are non-empty ASCII letters, digits, `_` or `-`",
        ))
    }
}
