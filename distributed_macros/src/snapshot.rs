use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields, LitInt, LitStr};

pub fn derive_snapshot(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as DeriveInput);
    match expand_snapshot(input) {
        Ok(expanded) => TokenStream::from(expanded),
        Err(err) => err.to_compile_error().into(),
    }
}

fn expand_snapshot(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let framework = crate::shared::framework_path()?;
    let name = &input.ident;
    let snapshot_name = format_ident!("{}Snapshot", name);

    // Parse struct-level #[snapshot(...)] attributes
    let SnapshotAttrs {
        entity_field,
        custom_id,
        version,
    } = parse_struct_attrs(&input)?;

    // Collect eligible fields (exclude entity field and #[serde(skip)] fields)
    let fields = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields) => &fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    &data_struct.fields,
                    "Snapshot derive requires named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                &input,
                "Snapshot derive can only be used on structs with named fields",
            ));
        }
    };

    let mut snapshot_fields = Vec::new();
    let mut all_field_names = Vec::new();
    let mut entity_field_found = false;

    if custom_id.as_ref() == Some(&entity_field) {
        return Err(syn::Error::new_spanned(
            &entity_field,
            format!("snapshot entity field `{entity_field}` cannot also be the snapshot id field"),
        ));
    }

    for field in fields {
        let ident = field
            .ident
            .as_ref()
            .ok_or_else(|| syn::Error::new_spanned(field, "Snapshot field must be named"))?;

        all_field_names.push(ident.clone());

        // Skip the entity field
        if *ident == entity_field {
            entity_field_found = true;
            continue;
        }

        // Skip fields with #[serde(skip)]
        if has_serde_skip(&field.attrs) {
            continue;
        }

        snapshot_fields.push((ident.clone(), field.ty.clone()));
    }

    if !entity_field_found {
        return Err(syn::Error::new_spanned(
            &entity_field,
            format!(
                "snapshot entity field `{}` must match a named struct field; available fields: {}",
                entity_field,
                format_field_names(&all_field_names)
            ),
        ));
    }

    if let Some(id_field) = &custom_id {
        if !snapshot_fields.iter().any(|(name, _)| name == id_field) {
            let snapshot_field_names = snapshot_fields
                .iter()
                .map(|(name, _)| name.clone())
                .collect::<Vec<_>>();
            return Err(syn::Error::new_spanned(
                id_field,
                format!(
                    "snapshot id field `{}` must match a retained snapshot field; available snapshot fields: {}",
                    id_field,
                    format_field_names(&snapshot_field_names)
                ),
            ));
        }
    }

    // Determine if custom_id matches an existing field
    let has_custom_id = custom_id.is_some();
    let id_field_name = custom_id.clone().unwrap_or_else(|| format_ident!("id"));

    // Check if id_field_name already exists in snapshot_fields
    let id_already_in_fields = snapshot_fields.iter().any(|(n, _)| *n == id_field_name);

    // Build the snapshot struct fields
    let struct_field_defs: Vec<_> = if has_custom_id || id_already_in_fields {
        // Custom ID key or "id" already present - just use existing fields
        snapshot_fields
            .iter()
            .map(|(n, ty)| quote! { pub #n: #ty })
            .collect()
    } else {
        // Default case: prepend `pub id: String`
        let mut defs = vec![quote! { pub id: String }];
        defs.extend(snapshot_fields.iter().map(|(n, ty)| quote! { pub #n: #ty }));
        defs
    };

    // Build snapshot() field assignments
    let field_assignments: Vec<_> = if has_custom_id || id_already_in_fields {
        // All fields just clone
        snapshot_fields
            .iter()
            .map(|(n, _)| quote! { #n: self.#n.clone() })
            .collect()
    } else {
        // Default: id from entity + clone all fields
        let mut assignments = vec![quote! { id: self.#entity_field.id().to_string() }];
        assignments.extend(
            snapshot_fields
                .iter()
                .map(|(n, _)| quote! { #n: self.#n.clone() }),
        );
        assignments
    };

    // Build restore_from_snapshot field assignments
    let restore_assignments: Vec<_> = snapshot_fields
        .iter()
        .map(|(n, _)| quote! { self.#n = snapshot.#n; })
        .collect();

    // Only emit the `SNAPSHOT_VERSION` const override when the user opted in via
    // `#[snapshot(version = N)]`; otherwise inherit the trait default of 1.
    // Bump the version whenever the snapshot struct's layout changes so old
    // cached snapshots are ignored and rebuilt from events.
    let snapshot_version_const = version.map(|v| {
        quote! {
            const SNAPSHOT_VERSION: u64 = #v;
        }
    });

    Ok(quote! {
        #[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
        pub struct #snapshot_name {
            #(#struct_field_defs),*
        }

        impl #name {
            pub fn snapshot(&self) -> #snapshot_name {
                #snapshot_name {
                    #(#field_assignments),*
                }
            }
        }

        impl #framework::Snapshottable for #name {
            type Snapshot = #snapshot_name;

            #snapshot_version_const

            fn create_snapshot(&self) -> #snapshot_name {
                self.snapshot()
            }

            fn restore_from_snapshot(&mut self, snapshot: #snapshot_name) {
                let __id_value = snapshot.#id_field_name.clone();
                #(#restore_assignments)*
                self.#entity_field.set_id(&__id_value);
            }
        }
    })
}

/// Parsed `#[snapshot(...)]` struct-level attributes.
struct SnapshotAttrs {
    entity_field: syn::Ident,
    custom_id: Option<syn::Ident>,
    /// `version = N` if specified; `None` inherits the trait default of 1.
    version: Option<u64>,
}

fn parse_struct_attrs(input: &DeriveInput) -> syn::Result<SnapshotAttrs> {
    let mut entity_field = format_ident!("entity");
    let mut custom_id: Option<syn::Ident> = None;
    let mut version: Option<u64> = None;

    for attr in &input.attrs {
        if !attr.path().is_ident("snapshot") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("entity") {
                let value: LitStr = meta.value()?.parse()?;
                entity_field = parse_snapshot_ident(&value, "entity")?;
            } else if meta.path.is_ident("id") {
                let value: LitStr = meta.value()?.parse()?;
                custom_id = Some(parse_snapshot_ident(&value, "id")?);
            } else if meta.path.is_ident("version") {
                let value: LitInt = meta.value()?.parse()?;
                let parsed: u64 = value.base10_parse()?;
                if parsed == 0 {
                    return Err(syn::Error::new(
                        value.span(),
                        "snapshot version must be greater than zero",
                    ));
                }
                version = Some(parsed);
            } else {
                return Err(meta.error(
                    "unsupported key in #[snapshot(...)]; expected `entity`, `id`, or `version`",
                ));
            }
            Ok(())
        })?;
    }

    Ok(SnapshotAttrs {
        entity_field,
        custom_id,
        version,
    })
}

fn parse_snapshot_ident(value: &LitStr, attr_name: &str) -> syn::Result<syn::Ident> {
    value.parse().map_err(|err| {
        syn::Error::new(
            value.span(),
            format!("expected identifier for snapshot {attr_name} attribute: {err}"),
        )
    })
}

fn format_field_names(names: &[syn::Ident]) -> String {
    if names.is_empty() {
        return "<none>".to_string();
    }

    names
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

fn has_serde_skip(attrs: &[syn::Attribute]) -> bool {
    for attr in attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }

        let mut found_skip = false;
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("skip") {
                found_skip = true;
            }
            Ok(())
        });

        if found_skip {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expand_snapshot_accepts_named_struct() {
        let input: DeriveInput = syn::parse_quote! {
            struct Todo {
                entity: distributed::Entity,
                task: String,
            }
        };

        let expanded = expand_snapshot(input).unwrap().to_string();

        assert!(expanded.contains("pub struct TodoSnapshot"));
        assert!(expanded.contains("impl :: distributed :: Snapshottable for Todo"));
    }

    #[test]
    fn expand_snapshot_rejects_tuple_structs() {
        let input: DeriveInput = syn::parse_quote! {
            struct Todo(String);
        };

        let err = expand_snapshot(input).expect_err("tuple struct should return an error");

        assert!(
            err.to_string().contains("requires named fields"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn expand_snapshot_rejects_enums() {
        let input: DeriveInput = syn::parse_quote! {
            enum Todo {
                Created,
            }
        };

        let err = expand_snapshot(input).expect_err("enum should return an error");

        assert!(
            err.to_string().contains("can only be used on structs"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn expand_snapshot_rejects_invalid_id_identifiers() {
        let input: DeriveInput = syn::parse_quote! {
            #[snapshot(id = "not-valid")]
            struct Todo {
                entity: distributed::Entity,
                task: String,
            }
        };

        let err = expand_snapshot(input).expect_err("invalid id should return an error");

        assert!(
            err.to_string().contains("expected identifier"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn expand_snapshot_rejects_unknown_attribute_keys() {
        let input: DeriveInput = syn::parse_quote! {
            #[snapshot(collection = "todos")]
            struct Todo {
                entity: distributed::Entity,
                task: String,
            }
        };

        let err = expand_snapshot(input).expect_err("unknown key should return an error");

        assert!(
            err.to_string().contains("unsupported key"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn expand_snapshot_rejects_missing_entity_field() {
        let input: DeriveInput = syn::parse_quote! {
            #[snapshot(entity = "state")]
            struct Todo {
                entity: distributed::Entity,
                task: String,
            }
        };

        let err = expand_snapshot(input).expect_err("missing entity field should return an error");

        assert!(
            err.to_string().contains("snapshot entity field `state`"),
            "unexpected error: {err}"
        );
        assert!(
            err.to_string().contains("entity, task"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn expand_snapshot_rejects_missing_custom_id_field() {
        let input: DeriveInput = syn::parse_quote! {
            #[snapshot(id = "sku")]
            struct Todo {
                entity: distributed::Entity,
                task: String,
            }
        };

        let err = expand_snapshot(input).expect_err("missing id field should return an error");

        assert!(
            err.to_string().contains("snapshot id field `sku`"),
            "unexpected error: {err}"
        );
        assert!(err.to_string().contains("task"), "unexpected error: {err}");
    }

    #[test]
    fn expand_snapshot_rejects_skipped_custom_id_field() {
        let input: DeriveInput = syn::parse_quote! {
            #[snapshot(id = "sku")]
            struct Inventory {
                entity: distributed::Entity,
                #[serde(skip)]
                sku: String,
                quantity: i32,
            }
        };

        let err = expand_snapshot(input).expect_err("skipped id field should return an error");

        assert!(
            err.to_string().contains("retained snapshot field"),
            "unexpected error: {err}"
        );
        assert!(
            err.to_string().contains("quantity"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn expand_snapshot_without_version_attr_inherits_default() {
        let input: DeriveInput = syn::parse_quote! {
            struct Todo {
                entity: distributed::Entity,
                task: String,
            }
        };

        let expanded = expand_snapshot(input).unwrap().to_string();

        // No explicit const is emitted; the trait default (1) is inherited.
        assert!(
            !expanded.contains("const SNAPSHOT_VERSION"),
            "default case must not emit a SNAPSHOT_VERSION const: {expanded}"
        );
    }

    #[test]
    fn expand_snapshot_emits_version_const() {
        let input: DeriveInput = syn::parse_quote! {
            #[snapshot(version = 3)]
            struct Todo {
                entity: distributed::Entity,
                task: String,
            }
        };

        let expanded = expand_snapshot(input).unwrap().to_string();

        assert!(
            expanded.contains("const SNAPSHOT_VERSION : u64 = 3"),
            "expected emitted version const: {expanded}"
        );
    }

    #[test]
    fn expand_snapshot_rejects_zero_version() {
        let input: DeriveInput = syn::parse_quote! {
            #[snapshot(version = 0)]
            struct Todo {
                entity: distributed::Entity,
                task: String,
            }
        };

        let err = expand_snapshot(input).expect_err("zero version should return an error");

        assert!(
            err.to_string()
                .contains("version must be greater than zero"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn expand_snapshot_rejects_non_integer_version() {
        let input: DeriveInput = syn::parse_quote! {
            #[snapshot(version = "two")]
            struct Todo {
                entity: distributed::Entity,
                task: String,
            }
        };

        let err = expand_snapshot(input).expect_err("non-integer version should return an error");

        // `LitInt` parse failure surfaces syn's "expected integer literal".
        assert!(
            err.to_string().contains("expected integer literal")
                || err.to_string().contains("integer"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn expand_snapshot_rejects_entity_field_as_custom_id() {
        let input: DeriveInput = syn::parse_quote! {
            #[snapshot(id = "entity")]
            struct Todo {
                entity: distributed::Entity,
                task: String,
            }
        };

        let err = expand_snapshot(input).expect_err("entity id should return an error");

        assert!(
            err.to_string()
                .contains("cannot also be the snapshot id field"),
            "unexpected error: {err}"
        );
    }
}
