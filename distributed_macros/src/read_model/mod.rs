mod attrs;
mod relational;
mod types;

use proc_macro::TokenStream;
use quote::quote;
use syn::{punctuated::Punctuated, Data, DeriveInput, Field, Fields, Token};

use attrs::{FieldAttrs, StructAttrs};
use relational::expand_relational_read_model;
use types::default_storage_name;

pub fn derive_read_model(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as DeriveInput);
    match expand_read_model(input) {
        Ok(expanded) => TokenStream::from(expanded),
        Err(err) => err.to_compile_error().into(),
    }
}

pub(crate) fn expand_read_model(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let framework = crate::shared::framework_path()?;
    let name = &input.ident;
    let visibility = &input.vis;
    let struct_attrs = StructAttrs::from_input(&input)?;
    let fields = named_fields(&input)?;
    let field_attrs = fields
        .named
        .iter()
        .map(FieldAttrs::from_field)
        .collect::<syn::Result<Vec<_>>>()?;
    let relational =
        struct_attrs.is_relational() || field_attrs.iter().any(FieldAttrs::is_relational);

    let id_field = find_id_field(&fields.named, &field_attrs)?;
    let collection = struct_attrs
        .collection
        .clone()
        .or_else(|| struct_attrs.table.clone())
        .unwrap_or_else(|| default_storage_name(&name.to_string()));

    let read_model_impl = if let Some(id_field) = &id_field {
        Some(quote! {
            impl #framework::ReadModel for #name {
                const COLLECTION: &'static str = #collection;

                fn id(&self) -> &str {
                    &self.#id_field
                }
            }
        })
    } else if relational {
        None
    } else {
        return Err(syn::Error::new_spanned(
            input,
            "ReadModel derive requires a field named `id` or a field marked with #[readmodel(id)]",
        ));
    };

    let relational_impl = if relational {
        Some(expand_relational_read_model(
            name,
            visibility,
            &struct_attrs,
            &fields.named,
            &field_attrs,
            id_field.as_ref(),
        )?)
    } else {
        None
    };

    Ok(quote! {
        #read_model_impl
        #relational_impl
    })
}

fn named_fields(input: &DeriveInput) -> syn::Result<&FieldsNamed> {
    let Data::Struct(data_struct) = &input.data else {
        return Err(syn::Error::new_spanned(
            input,
            "ReadModel derive can only be used on structs with named fields",
        ));
    };

    let Fields::Named(fields) = &data_struct.fields else {
        return Err(syn::Error::new_spanned(
            &data_struct.fields,
            "ReadModel derive requires named fields",
        ));
    };

    Ok(fields)
}

type FieldsNamed = syn::FieldsNamed;

fn find_id_field(
    fields: &Punctuated<Field, Token![,]>,
    field_attrs: &[FieldAttrs],
) -> syn::Result<Option<syn::Ident>> {
    let mut explicit_id: Option<syn::Ident> = None;
    for (field, attrs) in fields.iter().zip(field_attrs) {
        if attrs.id {
            let ident = field.ident.clone().ok_or_else(|| {
                syn::Error::new_spanned(field, "ReadModel id field must be named")
            })?;
            if let Some(previous) = &explicit_id {
                return Err(syn::Error::new_spanned(
                    field,
                    format!(
                        "Multiple #[readmodel(id)] fields found: `{}` and `{}`",
                        previous, ident
                    ),
                ));
            }
            explicit_id = Some(ident);
        }
    }

    if explicit_id.is_some() {
        return Ok(explicit_id);
    }

    Ok(fields
        .iter()
        .filter_map(|field| field.ident.clone())
        .find(|ident| ident == "id"))
}

#[cfg(test)]
mod tests;
