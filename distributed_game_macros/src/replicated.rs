//! `#[derive(Replicated)]`: one struct = one kind's schema + field encoding.

use proc_macro2::{Span, TokenStream};
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;
use syn::{DeriveInput, Expr, Ident, LitInt, LitStr};

use crate::shared::{
    check_name, field_name, find_attr, named_fields, no_generics, parse_field_attrs, reject,
    runtime, Special,
};

const MAX_FIELDS: usize = 32;

struct KindAttrs {
    kind: String,
    id: u16,
    priority: Option<Expr>,
    global: bool,
    x: Option<Ident>,
    y: Option<Ident>,
}

fn parse_kind_attrs(input: &DeriveInput) -> syn::Result<KindAttrs> {
    let Some(attr) = find_attr(&input.attrs, "replicated") else {
        return Err(syn::Error::new(
            input.ident.span(),
            "#[derive(Replicated)] needs #[replicated(kind = \"name\", id = N)]",
        ));
    };
    let (mut kind, mut id, mut priority, mut global, mut x, mut y) =
        (None, None, None, false, None, None);
    attr.parse_nested_meta(|meta| {
        let key = meta
            .path
            .get_ident()
            .map(Ident::to_string)
            .unwrap_or_default();
        match key.as_str() {
            "kind" => kind = Some(check_name(&meta.value()?.parse::<LitStr>()?)?),
            "id" => {
                let lit: LitInt = meta.value()?.parse()?;
                id = Some(lit.base10_parse::<u16>()?);
            }
            "priority" => priority = Some(meta.value()?.parse::<Expr>()?),
            "global" => global = true,
            "x" => x = Some(meta.value()?.parse::<Ident>()?),
            "y" => y = Some(meta.value()?.parse::<Ident>()?),
            _ => {
                return Err(meta.error(
                    "expected `kind`, `id`, `priority`, `global`, `x` or `y` in #[replicated(..)]",
                ))
            }
        }
        Ok(())
    })?;
    let span = attr.path().span();
    Ok(KindAttrs {
        kind: kind
            .ok_or_else(|| syn::Error::new(span, "#[replicated(..)] needs `kind = \"name\"`"))?,
        id: id.ok_or_else(|| {
            syn::Error::new(span, "#[replicated(..)] needs a literal `id = N` (u16)")
        })?,
        priority,
        global,
        x,
        y,
    })
}

pub(crate) fn expand(input: DeriveInput) -> syn::Result<TokenStream> {
    no_generics(&input, "Replicated")?;
    let rt = runtime()?;
    let p = quote!(#rt::__private);
    let attrs = parse_kind_attrs(&input)?;
    let fields = named_fields(&input, "Replicated")?;
    if fields.is_empty() {
        return Err(syn::Error::new(
            input.ident.span(),
            "a replicated kind needs at least one field",
        ));
    }
    if fields.len() > MAX_FIELDS {
        return Err(syn::Error::new(
            fields[MAX_FIELDS].span(),
            "at most 32 fields per kind (update masks are 32-bit)",
        ));
    }

    let mut defs = Vec::new();
    let mut values = Vec::new();
    for field in &fields {
        let ident = field.ident.as_ref().expect("named");
        let name = field_name(ident);
        let ty = &field.ty;
        let fa = parse_field_attrs(field, &rt)?;
        reject(fa.range.as_ref().map(|r| r.2), "#[range]", "Replicated")?;
        reject(fa.buttons.as_ref().map(|b| b.1), "#[buttons]", "Replicated")?;
        let span = ty.span();
        let (field_type, value) = match fa.special.as_ref().map(|s| &s.0) {
            None => (
                quote_spanned!(span=> <#ty as #p::EntityField>::FIELD_TYPE),
                quote_spanned!(span=> <#ty as #p::EntityField>::to_field_value(&self.#ident)),
            ),
            Some(Special::FieldType(ft)) => (
                ft.clone(),
                quote_spanned!(span=> #p::FieldValue::F(<#ty as #p::QuantizedF32>::get(&self.#ident))),
            ),
            Some(Special::Long) => return Err(syn::Error::new(
                fa.special.as_ref().map_or(Span::call_site(), |s| s.1),
                "#[long] strings cannot be replicated (entity strings are short_str, <= 255 bytes)",
            )),
        };
        let priority = fa
            .priority
            .as_ref()
            .map(|(e, _)| quote!((#e) as f32))
            .unwrap_or(quote!(1.0));
        let correct = fa
            .correct
            .as_ref()
            .map(|(c, _)| quote!(.correct(#c)))
            .unwrap_or_default();
        defs.push(quote! { .field_with_priority(#name, #field_type, #priority) #correct });
        values.push(value);
    }

    let has = |n: &str| {
        fields
            .iter()
            .find(|f| f.ident.as_ref().is_some_and(|i| field_name(i) == n))
            .and_then(|f| f.ident.clone())
    };
    let check_named = |ident: &Ident| -> syn::Result<()> {
        if fields.iter().any(|f| f.ident.as_ref() == Some(ident)) {
            Ok(())
        } else {
            Err(syn::Error::new(
                ident.span(),
                format!("no field `{ident}` in this struct"),
            ))
        }
    };
    if let Some(x) = &attrs.x {
        check_named(x)?;
    }
    if let Some(y) = &attrs.y {
        check_named(y)?;
    }
    let pos_x = attrs.x.clone().or_else(|| has("x"));
    let pos_y = attrs.y.clone().or_else(|| has("y"));
    let position = match (pos_x, pos_y) {
        (Some(x), Some(y)) => quote! { (self.#x as f32, self.#y as f32) },
        _ if attrs.global => quote! { (0.0, 0.0) },
        _ => {
            return Err(syn::Error::new(
                input.ident.span(),
                "a non-global kind needs a position: name fields `x` and `y`, point at them with #[replicated(x = .., y = ..)], or mark the kind `global`",
            ))
        }
    };

    let ident = &input.ident;
    let kind = &attrs.kind;
    let id = attrs.id;
    let global = attrs.global;
    let kind_priority = attrs
        .priority
        .as_ref()
        .map(|e| quote!(.priority((#e) as f32)))
        .unwrap_or_default();
    Ok(quote! {
        impl #rt::Replicated for #ident {
            const KIND_ID: u16 = #id;
            const KIND_NAME: &'static str = #kind;
            const GLOBAL: bool = #global;

            fn kind_def() -> #p::KindDef {
                #p::KindDef::new(#id, #kind)
                    #(#defs)*
                    #kind_priority
            }

            #[allow(clippy::unnecessary_cast)]
            fn position(&self) -> (f32, f32) {
                #position
            }

            fn field_values(&self) -> #p::std::vec::Vec<#p::FieldValue> {
                #p::std::vec![#(#values),*]
            }
        }
    })
}
