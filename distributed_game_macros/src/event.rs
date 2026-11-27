//! `#[derive(GameEvent)]` and `#[derive(GameRecord)]`: typed payloads with
//! the shared value codec.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Field, Fields, Ident, LitInt, LitStr};

use crate::shared::{
    check_name, field_name, find_attr, named_fields, no_generics, parse_field_attrs, payload_codec,
    reject, runtime, snake_case,
};

/// Encode/decode/surface tokens for a list of named fields read through
/// `access(ident)` (e.g. `&self.x` or a bound pattern variable).
struct FieldsCodec {
    encodes: Vec<TokenStream>,
    decodes: Vec<TokenStream>,
    surfaces: Vec<TokenStream>,
    records: Vec<TokenStream>,
}

fn fields_codec(
    fields: &[&Field],
    derive: &str,
    rt: &TokenStream,
    access: impl Fn(&Ident) -> TokenStream,
) -> syn::Result<FieldsCodec> {
    let p = quote!(#rt::__private);
    let mut out = FieldsCodec {
        encodes: Vec::new(),
        decodes: Vec::new(),
        surfaces: Vec::new(),
        records: Vec::new(),
    };
    for field in fields {
        let fid = field.ident.as_ref().expect("named");
        let fa = parse_field_attrs(field, rt)?;
        reject(fa.priority.as_ref().map(|x| x.1), "#[priority]", derive)?;
        reject(fa.correct.as_ref().map(|x| x.1), "#[correct]", derive)?;
        reject(fa.range.as_ref().map(|x| x.2), "#[range]", derive)?;
        reject(fa.buttons.as_ref().map(|x| x.1), "#[buttons]", derive)?;
        let codec = payload_codec(
            &field.ty,
            access(fid),
            fa.special.as_ref().map(|s| &s.0),
            rt,
        );
        let name = field_name(fid);
        out.encodes.push(codec.encode);
        let decode = codec.decode;
        out.decodes.push(quote!(#fid: #decode));
        let wire_type = codec.wire_type;
        out.surfaces
            .push(quote!(#p::PayloadField::new(#name, #wire_type)));
        out.records.push(codec.records);
    }
    Ok(out)
}

pub(crate) fn expand_record(input: DeriveInput) -> syn::Result<TokenStream> {
    no_generics(&input, "GameRecord")?;
    let rt = runtime()?;
    let p = quote!(#rt::__private);
    let fields = named_fields(&input, "GameRecord")?;
    let c = fields_codec(&fields, "GameRecord", &rt, |f| quote!(&self.#f))?;
    let ident = &input.ident;
    let name = ident.to_string();
    let (encodes, decodes, surfaces, records) = (c.encodes, c.decodes, c.surfaces, c.records);
    Ok(quote! {
        impl #p::Wire for #ident {
            fn wire_type() -> #p::WireType {
                #p::WireType::Record { name: #p::std::string::String::from(#name) }
            }

            fn encode(&self, w: &mut #p::Writer) {
                let _ = &w;
                #(#encodes)*
            }

            fn decode(r: &mut #p::Reader<'_>) -> #p::std::result::Result<Self, #p::DecodeError> {
                let _ = &r;
                #p::std::result::Result::Ok(Self { #(#decodes),* })
            }

            fn records(records: &mut #p::std::vec::Vec<#p::RecordSurface>) {
                if records.iter().any(|r| r.name == #name) {
                    return;
                }
                records.push(#p::RecordSurface {
                    name: #p::std::string::String::from(#name),
                    fields: #p::std::vec![#(#surfaces),*],
                });
                #(#records)*
            }
        }
    })
}

struct EventAttr {
    id: u32,
    id_span: proc_macro2::Span,
    name: Option<String>,
}

fn parse_event_attr(
    attrs: &[syn::Attribute],
    span: proc_macro2::Span,
    what: &str,
) -> syn::Result<EventAttr> {
    let Some(attr) = find_attr(attrs, "game_event") else {
        return Err(syn::Error::new(
            span,
            format!("{what} needs #[game_event(id = N)] (a stable u32 type id)"),
        ));
    };
    let (mut id, mut name) = (None, None);
    attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("id") {
            let lit: LitInt = meta.value()?.parse()?;
            id = Some((lit.base10_parse::<u32>()?, lit.span()));
        } else if meta.path.is_ident("name") {
            name = Some(check_name(&meta.value()?.parse::<LitStr>()?)?);
        } else {
            return Err(meta.error("expected `id` or `name` in #[game_event(..)]"));
        }
        Ok(())
    })?;
    let Some((id, id_span)) = id else {
        return Err(syn::Error::new(
            attr.path().span(),
            "#[game_event(..)] needs a literal `id = N`",
        ));
    };
    Ok(EventAttr { id, id_span, name })
}

pub(crate) fn expand_event(input: DeriveInput) -> syn::Result<TokenStream> {
    no_generics(&input, "GameEvent")?;
    match &input.data {
        Data::Struct(_) => expand_struct_event(&input),
        Data::Enum(_) => expand_enum_event(&input),
        Data::Union(_) => Err(syn::Error::new(
            input.ident.span(),
            "#[derive(GameEvent)] supports structs and enums",
        )),
    }
}

fn expand_struct_event(input: &DeriveInput) -> syn::Result<TokenStream> {
    let rt = runtime()?;
    let p = quote!(#rt::__private);
    let attr = parse_event_attr(&input.attrs, input.ident.span(), "a struct event")?;
    let fields = named_fields(input, "GameEvent")?;
    let c = fields_codec(&fields, "GameEvent", &rt, |f| quote!(&self.#f))?;
    let ident = &input.ident;
    let id = attr.id;
    let name = attr.name.unwrap_or_else(|| snake_case(&ident.to_string()));
    let (encodes, decodes, surfaces, records) = (c.encodes, c.decodes, c.surfaces, c.records);
    Ok(quote! {
        impl #ident {
            /// Stable event type id.
            pub const EVENT_ID: u32 = #id;
        }

        impl #rt::GameEvent for #ident {
            fn event_id(&self) -> u32 {
                #id
            }

            fn encode_payload(&self, w: &mut #p::Writer) {
                let _ = &w;
                #(#encodes)*
            }

            fn decode_event(
                type_id: u32,
                payload: &[u8],
            ) -> #p::std::result::Result<#p::std::option::Option<Self>, #p::DecodeError> {
                if type_id != #id {
                    return #p::std::result::Result::Ok(#p::std::option::Option::None);
                }
                #p::decode_whole(payload, |r| {
                    let _ = &r;
                    #p::std::result::Result::Ok(Self { #(#decodes),* })
                })
                .map(#p::std::option::Option::Some)
            }

            fn event_surfaces(
                records: &mut #p::std::vec::Vec<#p::RecordSurface>,
            ) -> #p::std::vec::Vec<#p::EventSurface> {
                #(#records)*
                let _ = &records;
                #p::std::vec![#p::EventSurface {
                    id: #id,
                    name: #p::std::string::String::from(#name),
                    payload: #p::EventPayload::Fields { fields: #p::std::vec![#(#surfaces),*] },
                }]
            }
        }
    })
}

fn expand_enum_event(input: &DeriveInput) -> syn::Result<TokenStream> {
    let rt = runtime()?;
    let p = quote!(#rt::__private);
    let Data::Enum(data) = &input.data else {
        unreachable!()
    };
    if data.variants.is_empty() {
        return Err(syn::Error::new(
            input.ident.span(),
            "an event enum needs at least one variant",
        ));
    }
    let ident = &input.ident;
    let mut seen: Vec<(u32, String)> = Vec::new();
    let (mut ids, mut encodes, mut decodes, mut surfaces, mut consts) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for variant in &data.variants {
        let vid = &variant.ident;
        let attr = parse_event_attr(&variant.attrs, vid.span(), "every event variant")?;
        let id = attr.id;
        let name = attr.name.unwrap_or_else(|| snake_case(&vid.to_string()));
        if let Some((_, other)) = seen.iter().find(|(i, n)| *i == id || *n == name) {
            return Err(syn::Error::new(
                attr.id_span,
                format!("event id {id} / name `{name}` already used by `{other}`"),
            ));
        }
        seen.push((id, name.clone()));
        let const_name = format_ident!("{}", snake_case(&vid.to_string()).to_uppercase());
        let doc = format!("Type id of [`{ident}::{vid}`].");
        consts.push(quote!(#[doc = #doc] pub const #const_name: u32 = #id;));
        match &variant.fields {
            Fields::Unit => {
                ids.push(quote!(Self::#vid => #id));
                encodes.push(quote!(Self::#vid => {}));
                decodes.push(quote!(#id => #p::decode_whole(payload, |_| #p::std::result::Result::Ok(Self::#vid))));
                surfaces.push(quote! {
                    #p::EventSurface {
                        id: #id,
                        name: #p::std::string::String::from(#name),
                        payload: #p::EventPayload::Fields { fields: #p::std::vec::Vec::new() },
                    }
                });
            }
            Fields::Named(named) => {
                let fields: Vec<&Field> = named.named.iter().collect();
                let bind: Vec<&Ident> = fields
                    .iter()
                    .map(|f| f.ident.as_ref().expect("named"))
                    .collect();
                let c = fields_codec(&fields, "GameEvent", &rt, |f| quote!(#f))?;
                let (enc, dec, surf, recs) = (c.encodes, c.decodes, c.surfaces, c.records);
                ids.push(quote!(Self::#vid { .. } => #id));
                encodes.push(quote!(Self::#vid { #(#bind),* } => { #(#enc)* }));
                decodes.push(quote!(#id => #p::decode_whole(payload, |r| {
                    let _ = &r;
                    #p::std::result::Result::Ok(Self::#vid { #(#dec),* })
                })));
                surfaces.push(quote! {{
                    #(#recs)*
                    #p::EventSurface {
                        id: #id,
                        name: #p::std::string::String::from(#name),
                        payload: #p::EventPayload::Fields { fields: #p::std::vec![#(#surf),*] },
                    }
                }});
            }
            Fields::Unnamed(unnamed) => {
                if unnamed.unnamed.len() != 1 {
                    return Err(syn::Error::new(
                        variant.fields.span(),
                        "a tuple event variant carries exactly one value (use named fields for more)",
                    ));
                }
                let ty = &unnamed.unnamed[0].ty;
                let codec = payload_codec(ty, quote!(value), None, &rt);
                let (enc, dec, wt, recs) =
                    (codec.encode, codec.decode, codec.wire_type, codec.records);
                ids.push(quote!(Self::#vid(..) => #id));
                encodes.push(quote!(Self::#vid(value) => { #enc }));
                decodes.push(quote!(#id => #p::decode_whole(payload, |r| #p::std::result::Result::Ok(Self::#vid(#dec)))));
                surfaces.push(quote! {{
                    #recs
                    #p::EventSurface {
                        id: #id,
                        name: #p::std::string::String::from(#name),
                        payload: #p::EventPayload::Value { ty: #wt },
                    }
                }});
            }
        }
    }
    Ok(quote! {
        impl #ident {
            #(#consts)*
        }

        impl #rt::GameEvent for #ident {
            fn event_id(&self) -> u32 {
                match self {
                    #(#ids),*
                }
            }

            fn encode_payload(&self, w: &mut #p::Writer) {
                let _ = &w;
                match self {
                    #(#encodes)*
                }
            }

            fn decode_event(
                type_id: u32,
                payload: &[u8],
            ) -> #p::std::result::Result<#p::std::option::Option<Self>, #p::DecodeError> {
                match type_id {
                    #(#decodes.map(#p::std::option::Option::Some),)*
                    _ => #p::std::result::Result::Ok(#p::std::option::Option::None),
                }
            }

            fn event_surfaces(
                records: &mut #p::std::vec::Vec<#p::RecordSurface>,
            ) -> #p::std::vec::Vec<#p::EventSurface> {
                let _ = &records;
                #p::std::vec![#(#surfaces),*]
            }
        }
    })
}
