use proc_macro2::Span;
use quote::{format_ident, quote};
use syn::{punctuated::Punctuated, Field, Token};

use crate::shared::{
    compact_tokens, portable_projection_kind, projection_field_metadata, schema_fingerprint,
};

use super::attrs::{foreign_key_tokens, FieldAttrs, RelationshipKindAttr, StructAttrs};
use super::types::{
    box_inner_type, bytes_row_value_tokens, column_type_tokens, default_storage_name,
    effect_model_wire_tokens, json_type_tokens, option_inner_type, option_string_tokens,
    vec_inner_type,
};

pub(super) fn expand_relational_read_model(
    name: &syn::Ident,
    visibility: &syn::Visibility,
    struct_attrs: &StructAttrs,
    fields: &Punctuated<Field, Token![,]>,
    field_attrs: &[FieldAttrs],
    id_field: Option<&syn::Ident>,
) -> syn::Result<proc_macro2::TokenStream> {
    let framework = crate::shared::framework();
    let rust_name = name.to_string();
    let model_name = struct_attrs
        .name
        .clone()
        .unwrap_or_else(|| rust_name.clone());
    let table_name = struct_attrs
        .table
        .clone()
        .or_else(|| struct_attrs.collection.clone())
        .unwrap_or_else(|| default_storage_name(&rust_name));

    let primary_key_fields =
        relational_primary_key_fields(struct_attrs, fields, field_attrs, id_field);
    let primary_key_columns = primary_key_fields
        .iter()
        .map(|column| quote! { #column.to_string() })
        .collect::<Vec<_>>();

    for primary_key_column in &primary_key_fields {
        let has_field = fields.iter().zip(field_attrs).any(|(field, attrs)| {
            if attrs.relationship.is_some() || attrs.skip_query {
                return false;
            }
            field.ident.as_ref().is_some_and(|ident| {
                let field_name = ident.to_string();
                attrs.column.as_deref().unwrap_or(&field_name) == primary_key_column
            })
        });
        if !has_field {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                format!("primary-key column `{primary_key_column}` has no field on `{model_name}`"),
            ));
        }
    }

    let mut column_defs = Vec::new();
    let mut row_inserts = Vec::new();
    let mut row_fields = Vec::new();
    let mut key_inserts = Vec::new();
    let mut indexes = Vec::new();
    let mut relationships = Vec::new();
    let mut hydrate_include_arms = Vec::new();
    let mut include_rows_arms = Vec::new();
    let mut include_schema_arms = Vec::new();
    let mut effect_markers = Vec::new();
    let mut projection_fields = Vec::new();
    let mut projection_field_schema = Vec::new();

    for (field, attrs) in fields.iter().zip(field_attrs) {
        let ident = field
            .ident
            .as_ref()
            .ok_or_else(|| syn::Error::new_spanned(field, "ReadModel fields must be named"))?;
        let field_name = ident.to_string();

        if let Some(relationship) = attrs.relationship_tokens(&field_name)? {
            relationships.push(relationship);
            let (hydrate_arm, include_rows_arm, include_schema_arm) =
                attrs.relationship_include_tokens(field, &field_name)?;
            hydrate_include_arms.push(hydrate_arm);
            include_rows_arms.push(include_rows_arm);
            include_schema_arms.push(include_schema_arm);
            let relationship_attr = attrs
                .relationship
                .as_ref()
                .expect("relationship tokens require relationship metadata");
            let target_ty = match relationship_attr.kind {
                RelationshipKindAttr::HasMany | RelationshipKindAttr::ManyToMany => {
                    vec_inner_type(&field.ty).expect("relationship shape was validated")
                }
                RelationshipKindAttr::BelongsTo => {
                    let inner =
                        option_inner_type(&field.ty).expect("relationship shape was validated");
                    box_inner_type(inner).unwrap_or(inner)
                }
            };
            let marker = format_ident!("__Distributed{}EffectRelationship_{}", name, ident);
            effect_markers.push(quote! {
                #[doc(hidden)]
                #[allow(non_camel_case_types)]
                #visibility struct #marker;

                impl #framework::command::EffectRelationshipMarker for #marker {
                    type Source = #name;
                    type Target = #target_ty;
                    const FIELD: &'static str = #field_name;
                }

                impl #framework::projection::lower::ProjectionRelationshipMetadata for #marker {
                    type Source = #name;
                    type Target = #target_ty;
                    const FIELD: &'static str = #field_name;
                }
            });
            row_fields.push(quote! { #ident: ::core::default::Default::default() });
            continue;
        }

        if attrs.skip_query {
            row_fields.push(quote! { #ident: ::core::default::Default::default() });
            continue;
        }

        let column_name = attrs.column.clone().unwrap_or_else(|| field_name.clone());
        let field_ty = &field.ty;
        let effect_wire = effect_model_wire_tokens(field_ty, attrs.jsonb, attrs.text);
        let effect_marker = format_ident!("__Distributed{}EffectModelField_{}", name, ident);
        effect_markers.push(quote! {
            #[doc(hidden)]
            #[allow(non_camel_case_types)]
            #visibility struct #effect_marker;

            impl #framework::command::EffectModelFieldMarker for #effect_marker {
                type Model = #name;
                type Value = #field_ty;
                type Wire = #effect_wire;
                const FIELD: &'static str = #column_name;
            }
        });
        let primary_key = primary_key_fields
            .iter()
            .any(|pk| pk == &field_name || pk == &column_name);
        let nullable = attrs.nullable || option_inner_type(&field.ty).is_some();
        let column_type = column_type_tokens(&field.ty, attrs.jsonb, attrs.text);
        let json_type = json_type_tokens(&field.ty, attrs.jsonb, attrs.text);
        let default_tokens = option_string_tokens(attrs.default.as_deref());
        let foreign_key = attrs
            .foreign_key
            .as_ref()
            .map(foreign_key_tokens)
            .map(|foreign_key| quote! { Some(#foreign_key) })
            .unwrap_or_else(|| quote! { None });
        let delegated_from = option_string_tokens(attrs.delegated_from.as_deref());
        let has_default = attrs.has_default;
        let projection_metadata = projection_field_metadata(field)?;
        let rust_type = projection_metadata.rust_type;
        let portable_kind = syn::Ident::new(
            portable_projection_kind(projection_metadata.inner_type),
            Span::call_site(),
        );
        let required = !nullable && !has_default;
        projection_field_schema.push(format!(
            "{}:{}:{}:{}:{}:{}:{}",
            field_name, column_name, rust_type, portable_kind, nullable, required, primary_key
        ));
        projection_fields.push(quote! {
            #framework::projection::lower::ProjectionModelFieldMetadata {
                field_name: #field_name,
                column_name: #column_name,
                rust_type: #rust_type,
                portable_type:
                    #framework::projection::lower::ProjectionPortableType::#portable_kind,
                nullable: #nullable,
                required: #required,
                primary_key: #primary_key,
            }
        });

        column_defs.push(quote! {
            #framework::table::TableColumn {
                field_name: #field_name.to_string(),
                column_name: #column_name.to_string(),
                column_type: #column_type,
                nullable: #nullable,
                has_default: #has_default,
                default: #default_tokens,
                primary_key: #primary_key,
                foreign_key: #foreign_key,
                delegated_from: #delegated_from,
                skipped: false,
                json_type: #json_type,
            }
        });

        if attrs.text {
            row_inserts.push(quote! {
                row.insert(
                    #column_name,
                    #framework::table::RowValue::from_text_serde(
                        &self.#ident,
                        #nullable,
                        #column_name,
                    )?,
                );
            });
        } else if let Some(value) = bytes_row_value_tokens(&field.ty, quote! { self.#ident }) {
            row_inserts.push(quote! {
                row.insert(#column_name, #value);
            });
        } else {
            row_inserts.push(quote! {
                row.insert_serde(#column_name, &self.#ident)?;
            });
        }
        row_fields.push(quote! {
            #ident: row.get_serde(#column_name)?
        });

        if primary_key {
            if attrs.text {
                key_inserts.push(quote! {
                    key.values.insert(
                        #column_name.to_string(),
                        #framework::table::RowValue::from_text_serde(
                            &self.#ident,
                            #nullable,
                            #column_name,
                        )?,
                    );
                });
            } else if let Some(value) = bytes_row_value_tokens(&field.ty, quote! { self.#ident }) {
                key_inserts.push(quote! {
                    key.values.insert(#column_name.to_string(), #value);
                });
            } else {
                key_inserts.push(quote! {
                    key.values.insert(
                        #column_name.to_string(),
                        #framework::table::RowValue::from_serde(&self.#ident)?,
                    );
                });
            }
        }

        if attrs.indexed || attrs.unique {
            let index_columns = vec![column_name.clone()];
            let index_name = attrs
                .index_name
                .clone()
                .unwrap_or_else(|| default_index_name(&table_name, &index_columns, attrs.unique));
            let unique = attrs.unique;
            indexes.push(index_def_tokens(index_name, index_columns, unique));
        }
    }

    for index in &struct_attrs.indexes {
        let index_columns = index
            .columns
            .iter()
            .map(|column| resolve_column_reference(column, fields, field_attrs))
            .collect::<Vec<_>>();
        let index_name = index
            .name
            .clone()
            .unwrap_or_else(|| default_index_name(&table_name, &index_columns, index.unique));
        indexes.push(index_def_tokens(index_name, index_columns, index.unique));
    }

    let relationship_schema = relationships
        .iter()
        .map(compact_tokens)
        .collect::<Vec<_>>()
        .join("|");
    let projection_schema_fingerprint = schema_fingerprint(&format!(
        "distributed.projection-model/v1|model={model_name}|table={table_name}|fields={}|relationships={relationship_schema}",
        projection_field_schema.join("|")
    ));
    let projection_schema_fingerprint =
        syn::LitStr::new(&projection_schema_fingerprint, Span::call_site());

    Ok(quote! {
        #(#effect_markers)*

        impl #framework::read_model::RelationalReadModel for #name {
            fn schema() -> &'static #framework::table::TableSchema {
                static SCHEMA: ::std::sync::LazyLock<#framework::table::TableSchema> =
                    ::std::sync::LazyLock::new(|| #framework::table::TableSchema {
                        model_name: #model_name.to_string(),
                        table_name: #table_name.to_string(),
                        columns: vec![#(#column_defs),*],
                        primary_key: #framework::table::PrimaryKey {
                            columns: vec![#(#primary_key_columns),*],
                        },
                        version_column: Some(#framework::table::DEFAULT_TABLE_VERSION_COLUMN.to_string()),
                        indexes: vec![#(#indexes),*],
                        relationships: vec![#(#relationships),*],
                        kind: #framework::table::TableKind::ReadModel,
                    });
                &SCHEMA
            }

            fn primary_key(&self) -> Result<#framework::table::RowKey, #framework::table::TableStoreError> {
                let mut key = #framework::table::RowKey::default();
                #(#key_inserts)*
                Ok(key)
            }

            fn to_row(&self) -> Result<#framework::table::RowValues, #framework::table::TableStoreError> {
                let mut row = #framework::table::RowValues::new();
                #(#row_inserts)*
                Ok(row)
            }

            fn from_row(row: #framework::table::RowValues) -> Result<Self, #framework::table::TableStoreError> {
                Ok(Self {
                    #(#row_fields),*
                })
            }
        }

        impl #framework::projection::lower::ProjectionReadModelMetadata for #name {
            const PROJECTION_FIELDS: &'static [
                #framework::projection::lower::ProjectionModelFieldMetadata
            ] = &[#(#projection_fields),*];
            const PROJECTION_SCHEMA_FINGERPRINT: &'static str =
                #projection_schema_fingerprint;
        }

        impl #framework::read_model::RelationalReadModelIncludes for #name {
            fn hydrate_include(
                &mut self,
                include: &str,
                rows: Vec<#framework::table::RowValues>,
            ) -> Result<(), #framework::table::TableStoreError> {
                match include {
                    #(#hydrate_include_arms,)*
                    _ => Err(#framework::table::TableStoreError::Metadata(format!(
                        "read model `{}` has no hydratable relationship `{}`",
                        #model_name,
                        include
                    ))),
                }
            }

            fn include_rows(
                &self,
                include: &str,
            ) -> Result<Vec<#framework::table::RowValues>, #framework::table::TableStoreError> {
                match include {
                    #(#include_rows_arms,)*
                    _ => Err(#framework::table::TableStoreError::Metadata(format!(
                        "read model `{}` has no tracked relationship `{}`",
                        #model_name,
                        include
                    ))),
                }
            }

            fn include_target_schema(
                include: &str,
            ) -> Result<&'static #framework::table::TableSchema, #framework::table::TableStoreError> {
                match include {
                    #(#include_schema_arms,)*
                    _ => Err(#framework::table::TableStoreError::Metadata(format!(
                        "read model `{}` has no tracked relationship `{}`",
                        #model_name,
                        include
                    ))),
                }
            }
        }
    })
}

fn relational_primary_key_fields(
    struct_attrs: &StructAttrs,
    fields: &Punctuated<Field, Token![,]>,
    field_attrs: &[FieldAttrs],
    id_field: Option<&syn::Ident>,
) -> Vec<String> {
    if !struct_attrs.primary_key.is_empty() {
        return struct_attrs
            .primary_key
            .iter()
            .map(|key| resolve_column_reference(key, fields, field_attrs))
            .collect();
    }

    id_field
        .map(|id| {
            let id_name = id.to_string();
            resolve_column_reference(&id_name, fields, field_attrs)
        })
        .into_iter()
        .collect()
}

fn resolve_column_reference(
    reference: &str,
    fields: &Punctuated<Field, Token![,]>,
    field_attrs: &[FieldAttrs],
) -> String {
    fields
        .iter()
        .zip(field_attrs)
        .find_map(|(field, attrs)| {
            let field_name = field.ident.as_ref()?.to_string();
            if field_name == reference {
                Some(attrs.column.clone().unwrap_or(field_name))
            } else {
                None
            }
        })
        .unwrap_or_else(|| reference.to_string())
}

fn default_index_name(table_name: &str, columns: &[String], unique: bool) -> String {
    let prefix = if unique { "uq" } else { "idx" };
    format!("{prefix}_{table_name}_{}", columns.join("_"))
}

fn index_def_tokens(
    index_name: String,
    index_columns: Vec<String>,
    unique: bool,
) -> proc_macro2::TokenStream {
    let framework = crate::shared::framework();
    let columns = index_columns
        .iter()
        .map(|column| quote! { #column.to_string() })
        .collect::<Vec<_>>();

    quote! {
        #framework::table::TableIndex {
            name: Some(#index_name.to_string()),
            columns: vec![#(#columns),*],
            unique: #unique,
        }
    }
}
