use quote::quote;
use syn::{Attribute, DeriveInput, Expr, ExprArray, ExprLit, Field, Lit, LitStr, Meta, Token};

use super::types::{
    box_inner_type, option_inner_type, option_string_tokens, validate_relationship_target_type,
    vec_inner_type,
};

#[derive(Default)]
pub(super) struct StructAttrs {
    pub(super) name: Option<String>,
    pub(super) collection: Option<String>,
    pub(super) table: Option<String>,
    pub(super) primary_key: Vec<String>,
    pub(super) indexes: Vec<IndexAttr>,
}

pub(super) struct IndexAttr {
    pub(super) name: Option<String>,
    pub(super) columns: Vec<String>,
    pub(super) unique: bool,
}

impl StructAttrs {
    pub(super) fn from_input(input: &DeriveInput) -> syn::Result<Self> {
        let mut attrs = Self::default();
        for attr in &input.attrs {
            if attr.path().is_ident("collection") {
                attrs.collection = Some(parse_direct_string_attr(attr, "collection")?);
                continue;
            }

            if attr.path().is_ident("table") {
                attrs.table = Some(parse_direct_string_attr(attr, "table")?);
                continue;
            }

            if attr.path().is_ident("index") {
                attrs
                    .indexes
                    .push(parse_direct_index_attr(attr, "index", false)?);
                continue;
            }

            if attr.path().is_ident("unique") {
                attrs
                    .indexes
                    .push(parse_direct_index_attr(attr, "unique", true)?);
                continue;
            }

            if !attr.path().is_ident("readmodel") {
                continue;
            }

            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("name") {
                    attrs.name = Some(meta.value()?.parse::<LitStr>()?.value());
                } else if meta.path.is_ident("collection") {
                    attrs.collection = Some(meta.value()?.parse::<LitStr>()?.value());
                } else if meta.path.is_ident("table") {
                    attrs.table = Some(meta.value()?.parse::<LitStr>()?.value());
                } else if meta.path.is_ident("primary_key") {
                    let expr = meta.value()?.parse::<Expr>()?;
                    attrs.primary_key = parse_string_list(expr)?;
                } else {
                    return Err(meta.error("unknown readmodel struct attribute"));
                }
                Ok(())
            })?;
        }
        Ok(attrs)
    }

    pub(super) fn is_relational(&self) -> bool {
        self.table.is_some() || !self.primary_key.is_empty() || !self.indexes.is_empty()
    }
}

#[derive(Default)]
pub(super) struct FieldAttrs {
    pub(super) id: bool,
    pub(super) column: Option<String>,
    pub(super) indexed: bool,
    pub(super) index_name: Option<String>,
    pub(super) unique: bool,
    pub(super) jsonb: bool,
    pub(super) text: bool,
    pub(super) skip_query: bool,
    pub(super) nullable: bool,
    pub(super) has_default: bool,
    pub(super) default: Option<String>,
    pub(super) foreign_key: Option<ForeignKeyParts>,
    pub(super) delegated_from: Option<String>,
    pub(super) relationship: Option<RelationshipAttr>,
}

impl FieldAttrs {
    pub(super) fn from_field(field: &Field) -> syn::Result<Self> {
        let mut attrs = Self::default();
        let mut pending_foreign_key: Option<String> = None;
        let mut pending_references: Option<String> = None;
        let mut pending_through: Option<String> = None;
        let mut pending_target_foreign_key: Option<String> = None;
        for attr in &field.attrs {
            if attr.path().is_ident("id") {
                attrs.id = true;
                if let Some(column) = parse_optional_direct_string_attr(attr)? {
                    attrs.column = Some(column);
                }
                continue;
            }

            if attr.path().is_ident("column") {
                attrs.column = Some(parse_direct_string_attr(attr, "column")?);
                continue;
            }

            if attr.path().is_ident("index") {
                attrs.indexed = true;
                if let Some(index_name) = parse_optional_direct_string_attr(attr)? {
                    attrs.index_name = Some(index_name);
                }
                continue;
            }

            if attr.path().is_ident("unique") {
                attrs.unique = true;
                attrs.indexed = true;
                if let Some(index_name) = parse_optional_direct_string_attr(attr)? {
                    attrs.index_name = Some(index_name);
                }
                continue;
            }

            if !attr.path().is_ident("readmodel") {
                continue;
            }

            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("id") {
                    attrs.id = true;
                } else if meta.path.is_ident("column") {
                    attrs.column = Some(meta.value()?.parse::<LitStr>()?.value());
                } else if meta.path.is_ident("index") {
                    attrs.indexed = true;
                    if meta.input.peek(Token![=]) {
                        attrs.index_name = Some(meta.value()?.parse::<LitStr>()?.value());
                    }
                } else if meta.path.is_ident("unique") {
                    attrs.unique = true;
                    attrs.indexed = true;
                } else if meta.path.is_ident("jsonb") {
                    attrs.jsonb = true;
                } else if meta.path.is_ident("text") {
                    attrs.text = true;
                } else if meta.path.is_ident("skip_query")
                    || meta.path.is_ident("skip")
                    || meta.path.is_ident("private")
                {
                    attrs.skip_query = true;
                } else if meta.path.is_ident("nullable") {
                    attrs.nullable = true;
                } else if meta.path.is_ident("default") {
                    attrs.has_default = true;
                    if meta.input.peek(Token![=]) {
                        attrs.default = Some(meta.value()?.parse::<LitStr>()?.value());
                    }
                } else if meta.path.is_ident("references") {
                    if pending_references.is_some() {
                        return Err(meta.error("relationship references declared more than once"));
                    }
                    pending_references = Some(meta.value()?.parse::<LitStr>()?.value());
                } else if meta.path.is_ident("foreign_key") {
                    let value = meta.value()?.parse::<LitStr>()?.value();
                    if attrs.relationship.is_some() {
                        let relationship = relationship_mut(&mut attrs, "foreign_key")?;
                        if relationship.foreign_key.is_some() {
                            return Err(
                                meta.error("relationship foreign_key declared more than once")
                            );
                        }
                        relationship.foreign_key = Some(value);
                    } else if pending_foreign_key.is_some() {
                        return Err(meta.error("relationship foreign_key declared more than once"));
                    } else {
                        pending_foreign_key = Some(value);
                    }
                } else if meta.path.is_ident("delegated_from") {
                    attrs.delegated_from = Some(meta.value()?.parse::<LitStr>()?.value());
                } else if meta.path.is_ident("has_many") {
                    let target = meta.value()?.parse::<LitStr>()?.value();
                    attrs.relationship = Some(RelationshipAttr {
                        references: None,
                        kind: RelationshipKindAttr::HasMany,
                        target_model: target,
                        foreign_key: None,
                        through: None,
                        target_foreign_key: None,
                    });
                } else if meta.path.is_ident("belongs_to") {
                    let target = meta.value()?.parse::<LitStr>()?.value();
                    attrs.relationship = Some(RelationshipAttr {
                        references: None,
                        kind: RelationshipKindAttr::BelongsTo,
                        target_model: target,
                        foreign_key: None,
                        through: None,
                        target_foreign_key: None,
                    });
                } else if meta.path.is_ident("many_to_many") {
                    let target = meta.value()?.parse::<LitStr>()?.value();
                    attrs.relationship = Some(RelationshipAttr {
                        references: None,
                        kind: RelationshipKindAttr::ManyToMany,
                        target_model: target,
                        foreign_key: None,
                        through: None,
                        target_foreign_key: None,
                    });
                } else if meta.path.is_ident("through") {
                    let through = meta.value()?.parse::<LitStr>()?.value();
                    if attrs.relationship.is_some() {
                        let relationship = relationship_mut(&mut attrs, "through")?;
                        if relationship.through.is_some() {
                            return Err(meta.error("relationship through declared more than once"));
                        }
                        relationship.through = Some(through);
                    } else if pending_through.is_some() {
                        return Err(meta.error("relationship through declared more than once"));
                    } else {
                        pending_through = Some(through);
                    }
                } else if meta.path.is_ident("target_foreign_key") {
                    let value = meta.value()?.parse::<LitStr>()?.value();
                    if attrs.relationship.is_some() {
                        let relationship = relationship_mut(&mut attrs, "target_foreign_key")?;
                        if relationship.target_foreign_key.is_some() {
                            return Err(meta
                                .error("relationship target_foreign_key declared more than once"));
                        }
                        relationship.target_foreign_key = Some(value);
                    } else if pending_target_foreign_key.is_some() {
                        return Err(
                            meta.error("relationship target_foreign_key declared more than once")
                        );
                    } else {
                        pending_target_foreign_key = Some(value);
                    }
                } else {
                    return Err(meta.error("unknown readmodel field attribute"));
                }
                Ok(())
            })?;
        }

        if let Some(value) = pending_foreign_key {
            if let Some(relationship) = attrs.relationship.as_mut() {
                if relationship.foreign_key.is_some() {
                    return Err(syn::Error::new_spanned(
                        field,
                        "relationship foreign_key declared more than once",
                    ));
                }
                relationship.foreign_key = Some(value);
            } else {
                attrs.foreign_key = Some(parse_foreign_key(&value)?);
            }
        }

        if let Some(through) = pending_through {
            if let Some(relationship) = attrs.relationship.as_mut() {
                if relationship.through.is_some() {
                    return Err(syn::Error::new_spanned(
                        field,
                        "relationship through declared more than once",
                    ));
                }
                relationship.through = Some(through);
            } else {
                return Err(syn::Error::new_spanned(
                    field,
                    "`through` must be declared with a relationship attribute",
                ));
            }
        }

        if let Some(target_fk) = pending_target_foreign_key {
            if let Some(relationship) = attrs.relationship.as_mut() {
                if relationship.target_foreign_key.is_some() {
                    return Err(syn::Error::new_spanned(
                        field,
                        "relationship target_foreign_key declared more than once",
                    ));
                }
                relationship.target_foreign_key = Some(target_fk);
            } else {
                return Err(syn::Error::new_spanned(
                    field,
                    "`target_foreign_key` must be declared with a relationship attribute",
                ));
            }
        }

        if let Some(references) = pending_references {
            let relationship = attrs.relationship.as_mut().ok_or_else(|| {
                syn::Error::new_spanned(field, "`references` requires a direct relationship")
            })?;
            if matches!(relationship.kind, RelationshipKindAttr::ManyToMany) {
                return Err(syn::Error::new_spanned(
                    field,
                    "`references` requires a direct relationship",
                ));
            }
            relationship.references = Some(references);
        }

        if attrs.jsonb && attrs.text {
            return Err(syn::Error::new_spanned(
                field,
                "readmodel field cannot be both `jsonb` and text-backed",
            ));
        }

        Ok(attrs)
    }

    pub(super) fn is_relational(&self) -> bool {
        self.column.is_some()
            || self.indexed
            || self.unique
            || self.jsonb
            || self.text
            || self.skip_query
            || self.nullable
            || self.has_default
            || self.foreign_key.is_some()
            || self.delegated_from.is_some()
            || self.relationship.is_some()
    }

    pub(super) fn relationship_tokens(
        &self,
        field_name: &str,
    ) -> syn::Result<Option<proc_macro2::TokenStream>> {
        let Some(relationship) = &self.relationship else {
            return Ok(None);
        };
        let Some(foreign_key) = relationship.foreign_key.as_deref() else {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                format!("relationship `{field_name}` must declare `foreign_key = \"...\"`"),
            ));
        };
        let target_model = &relationship.target_model;
        let references = option_string_tokens(relationship.references.as_deref());
        let through = option_string_tokens(relationship.through.as_deref());
        let target_foreign_key = option_string_tokens(relationship.target_foreign_key.as_deref());
        let kind = match relationship.kind {
            RelationshipKindAttr::HasMany => quote! { distributed::RelationshipKind::HasMany },
            RelationshipKindAttr::BelongsTo => quote! { distributed::RelationshipKind::BelongsTo },
            RelationshipKindAttr::ManyToMany => {
                quote! { distributed::RelationshipKind::ManyToMany }
            }
        };
        Ok(Some(quote! {
            distributed::RelationshipDef {
                references: #references,
                field_name: #field_name.to_string(),
                kind: #kind,
                target_model: #target_model.to_string(),
                foreign_key: Some(#foreign_key.to_string()),
                through: #through,
                target_foreign_key: #target_foreign_key,
            }
        }))
    }

    pub(super) fn relationship_include_tokens(
        &self,
        field: &Field,
        field_name: &str,
    ) -> syn::Result<(
        proc_macro2::TokenStream,
        proc_macro2::TokenStream,
        proc_macro2::TokenStream,
    )> {
        let relationship = self.relationship.as_ref().ok_or_else(|| {
            syn::Error::new_spanned(field, "field is not a read-model relationship")
        })?;
        let ident = field
            .ident
            .as_ref()
            .ok_or_else(|| syn::Error::new_spanned(field, "ReadModel fields must be named"))?;

        match relationship.kind {
            RelationshipKindAttr::HasMany | RelationshipKindAttr::ManyToMany => {
                let inner = vec_inner_type(&field.ty).ok_or_else(|| {
                    syn::Error::new_spanned(
                        field,
                        format!("relationship `{field_name}` must be shaped as `Vec<T>`"),
                    )
                })?;
                validate_relationship_target_type(
                    field,
                    inner,
                    &relationship.target_model,
                    field_name,
                )?;
                let hydrate = quote! {
                    #field_name => {
                        self.#ident = rows
                            .into_iter()
                            .map(<#inner as distributed::RelationalReadModel>::from_row)
                            .collect::<Result<Vec<_>, distributed::TableStoreError>>()?;
                        Ok(())
                    }
                };
                let include_rows = quote! {
                    #field_name => self
                        .#ident
                        .iter()
                        .map(distributed::RelationalReadModel::to_row)
                        .collect::<Result<Vec<_>, distributed::TableStoreError>>()
                };
                let include_schema = quote! {
                    #field_name => Ok(<#inner as distributed::RelationalReadModel>::schema())
                };
                Ok((hydrate, include_rows, include_schema))
            }
            RelationshipKindAttr::BelongsTo => {
                let inner = option_inner_type(&field.ty).ok_or_else(|| {
                    syn::Error::new_spanned(
                        field,
                        format!(
                            "belongs_to relationship `{field_name}` must be shaped as `Option<T>`"
                        ),
                    )
                })?;
                let boxed = box_inner_type(inner);
                let inner = boxed.unwrap_or(inner);
                validate_relationship_target_type(
                    field,
                    inner,
                    &relationship.target_model,
                    field_name,
                )?;
                let hydrated_value = if boxed.is_some() {
                    quote! { ::std::boxed::Box::new(<#inner as distributed::RelationalReadModel>::from_row(row)?) }
                } else {
                    quote! { <#inner as distributed::RelationalReadModel>::from_row(row)? }
                };
                let included_value = if boxed.is_some() {
                    quote! { value.as_ref() }
                } else {
                    quote! { value }
                };
                let hydrate = quote! {
                    #field_name => {
                        let mut rows = rows.into_iter();
                        self.#ident = match rows.next() {
                            Some(row) => Some(#hydrated_value),
                            None => None,
                        };
                        if rows.next().is_some() {
                            return Err(distributed::TableStoreError::Metadata(format!(
                                "belongs_to relationship `{}` returned more than one row",
                                #field_name
                            )));
                        }
                        Ok(())
                    }
                };
                let include_rows = quote! {
                    #field_name => {
                        let mut rows = Vec::new();
                        if let Some(value) = &self.#ident {
                            rows.push(distributed::RelationalReadModel::to_row(#included_value)?);
                        }
                        Ok(rows)
                    }
                };
                let include_schema = quote! {
                    #field_name => Ok(<#inner as distributed::RelationalReadModel>::schema())
                };
                Ok((hydrate, include_rows, include_schema))
            }
        }
    }
}

fn relationship_mut<'a>(
    attrs: &'a mut FieldAttrs,
    name: &str,
) -> syn::Result<&'a mut RelationshipAttr> {
    attrs.relationship.as_mut().ok_or_else(|| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("`{name}` must be declared after a relationship attribute"),
        )
    })
}

#[derive(Clone)]
pub(super) struct ForeignKeyParts {
    table: String,
    column: String,
}

#[derive(Clone)]
pub(super) struct RelationshipAttr {
    pub(super) references: Option<String>,
    pub(super) kind: RelationshipKindAttr,
    pub(super) target_model: String,
    pub(super) foreign_key: Option<String>,
    pub(super) through: Option<String>,
    pub(super) target_foreign_key: Option<String>,
}

#[derive(Clone, Copy)]
pub(super) enum RelationshipKindAttr {
    HasMany,
    BelongsTo,
    ManyToMany,
}
fn parse_string_list(expr: Expr) -> syn::Result<Vec<String>> {
    match expr {
        Expr::Array(ExprArray { elems, .. }) => elems.into_iter().map(parse_string_expr).collect(),
        expr => parse_string_expr(expr).map(|value| vec![value]),
    }
}

fn parse_string_expr(expr: Expr) -> syn::Result<String> {
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Str(value),
            ..
        }) => Ok(value.value()),
        other => Err(syn::Error::new_spanned(
            other,
            "expected string literal in readmodel attribute",
        )),
    }
}

fn parse_direct_string_attr(attr: &Attribute, attr_name: &str) -> syn::Result<String> {
    parse_optional_direct_string_attr(attr)?.ok_or_else(|| {
        syn::Error::new_spanned(attr, format!("#[{attr_name}] requires a string literal"))
    })
}

fn parse_optional_direct_string_attr(attr: &Attribute) -> syn::Result<Option<String>> {
    match &attr.meta {
        Meta::List(list) => Ok(Some(list.parse_args::<LitStr>()?.value())),
        Meta::NameValue(name_value) => parse_string_expr(name_value.value.clone()).map(Some),
        Meta::Path(_) => Ok(None),
    }
}

fn parse_direct_index_attr(
    attr: &Attribute,
    attr_name: &str,
    unique: bool,
) -> syn::Result<IndexAttr> {
    let mut name = None;
    let mut columns = None;

    match &attr.meta {
        Meta::List(_) => {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("name") {
                    name = Some(meta.value()?.parse::<LitStr>()?.value());
                } else if meta.path.is_ident("columns") {
                    let expr = meta.value()?.parse::<Expr>()?;
                    columns = Some(parse_string_list(expr)?);
                } else {
                    return Err(meta.error(format!("unknown {attr_name} attribute")));
                }
                Ok(())
            })?;
        }
        Meta::NameValue(name_value) => {
            columns = Some(parse_string_list(name_value.value.clone())?);
        }
        Meta::Path(_) => {}
    }

    let columns = columns.ok_or_else(|| {
        syn::Error::new_spanned(attr, format!("#[{attr_name}] requires columns = [\"...\"]"))
    })?;
    if columns.is_empty() {
        return Err(syn::Error::new_spanned(
            attr,
            format!("#[{attr_name}] requires at least one column"),
        ));
    }

    Ok(IndexAttr {
        name,
        columns,
        unique,
    })
}

fn parse_foreign_key(value: &str) -> syn::Result<ForeignKeyParts> {
    let Some((table, column)) = value.split_once('.') else {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "foreign_key must use `table.column` syntax",
        ));
    };
    Ok(ForeignKeyParts {
        table: table.to_string(),
        column: column.to_string(),
    })
}

pub(super) fn foreign_key_tokens(foreign_key: &ForeignKeyParts) -> proc_macro2::TokenStream {
    let table = &foreign_key.table;
    let column = &foreign_key.column;
    quote! {
        distributed::ForeignKey {
            table: #table.to_string(),
            column: #column.to_string(),
        }
    }
}
