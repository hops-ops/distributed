//! `#[derive(GameInput)]`: compact input codec + sanitize from one struct.

use proc_macro2::TokenStream;
use quote::{format_ident, quote, quote_spanned};
use syn::spanned::Spanned;
use syn::DeriveInput;

use crate::shared::{
    field_name, named_fields, no_generics, parse_field_attrs, payload_codec, reject, runtime,
    Special,
};

pub(crate) fn expand(input: DeriveInput) -> syn::Result<TokenStream> {
    no_generics(&input, "GameInput")?;
    let rt = runtime()?;
    let p = quote!(#rt::__private);
    let fields = named_fields(&input, "GameInput")?;
    let ident = &input.ident;
    let type_name = ident.to_string();

    let mut surfaces = Vec::new();
    let mut encodes = Vec::new();
    let mut decodes = Vec::new();
    let mut sanitizes = Vec::new();
    let mut consts = Vec::new();
    let mut const_names: Vec<syn::Ident> = Vec::new();
    for field in &fields {
        let fid = field.ident.as_ref().expect("named");
        let name = field_name(fid);
        let ty = &field.ty;
        let span = ty.span();
        let fa = parse_field_attrs(field, &rt)?;
        reject(
            fa.priority.as_ref().map(|x| x.1),
            "#[priority]",
            "GameInput",
        )?;
        reject(fa.correct.as_ref().map(|x| x.1), "#[correct]", "GameInput")?;
        if let Some((Special::Long, span)) = &fa.special {
            return Err(syn::Error::new(
                *span,
                "inputs carry no strings; #[long] is not supported by #[derive(GameInput)]",
            ));
        }
        let codec = payload_codec(
            ty,
            quote!(&self.#fid),
            fa.special.as_ref().map(|s| &s.0),
            &rt,
        );
        encodes.push(codec.encode);
        let decode = codec.decode;
        decodes.push(quote!(#fid: #decode));

        let range = match &fa.range {
            Some((min, max, _)) => quote!(Some([(#min) as f64, (#max) as f64])),
            None => quote!(None),
        };
        let mut buttons = Vec::new();
        let sanitize = if let Some((names, bspan)) = &fa.buttons {
            if fa.range.is_some() || fa.special.is_some() {
                return Err(syn::Error::new(
                    *bspan,
                    "a #[buttons] field takes no #[range] / #[quant] / #[angle]",
                ));
            }
            let mask: u64 = (1u64 << names.len()) - 1;
            let mask = mask as u32;
            for (bit, button) in names.iter().enumerate() {
                let name = field_name(button);
                buttons.push(quote!(#p::std::string::String::from(#name)));
                let cname = format_ident!("{}", name.to_uppercase(), span = button.span());
                if const_names.iter().any(|c| c == &cname) {
                    return Err(syn::Error::new(button.span(), "duplicate button constant"));
                }
                const_names.push(cname.clone());
                let doc = format!("`{name}` bit of `{}`.", field_name(fid));
                let bit = bit as u32;
                consts.push(quote_spanned!(span=>
                    #[doc = #doc]
                    pub const #cname: #ty = (1u32 << #bit) as #ty;
                ));
            }
            quote_spanned!(span=> <#ty as #p::ButtonBits>::mask(self.#fid, #mask))
        } else if let Some((min, max, _)) = &fa.range {
            quote_spanned!(span=>
                <#ty as #p::InputField>::sanitize_range(self.#fid, (#min) as f64, (#max) as f64)?
            )
        } else {
            quote_spanned!(span=> <#ty as #p::InputField>::sanitize_plain(self.#fid)?)
        };
        sanitizes.push(quote!(#fid: #sanitize));
        let wire_type = codec.wire_type;
        let records = codec.records;
        surfaces.push(quote! {{
            #records
            #p::PayloadField {
                name: #p::std::string::String::from(#name),
                ty: #wire_type,
                range: #range,
                buttons: #p::std::vec![#(#buttons),*],
            }
        }});
    }

    let consts = if consts.is_empty() {
        quote!()
    } else {
        quote! {
            impl #ident {
                #(#consts)*
            }
        }
    };
    Ok(quote! {
        #consts

        impl #rt::GameInput for #ident {
            fn input_surface(
                records: &mut #p::std::vec::Vec<#p::RecordSurface>,
            ) -> #p::InputSurface {
                let _ = &records;
                #p::InputSurface {
                    name: #p::std::string::String::from(#type_name),
                    fields: #p::std::vec![#(#surfaces),*],
                }
            }

            fn encode_input(&self, w: &mut #p::Writer) {
                let _ = &w;
                #(#encodes)*
            }

            fn decode_input(r: &mut #p::Reader<'_>) -> #p::std::result::Result<Self, #p::DecodeError> {
                let _ = &r;
                #p::std::result::Result::Ok(Self { #(#decodes),* })
            }

            #[allow(clippy::unnecessary_cast)]
            fn sanitize(self) -> #p::std::option::Option<Self> {
                #p::std::option::Option::Some(Self { #(#sanitizes),* })
            }
        }
    })
}
