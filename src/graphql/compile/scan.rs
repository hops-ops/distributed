//! The one way the compiler reads a model.
//!
//! A [`ModelScan`] is a model as one privilege may read it. It is the only
//! source of a grant (columns, limit, aggregations) and, through
//! [`ModelScan::visible_rows`], the only source of a `FROM … WHERE`: the row
//! policy is part of the scan, so an access path cannot be written without it.

use async_graphql::Value;

use crate::graphql::surface::{model_has_client_normalized_identity, SurfaceModel};
use crate::microsvc::Session;
use crate::table::{
    resolve_direct_join_keys, resolve_m2m_join_keys, ColumnType, RelationshipDef, RelationshipKind,
    TableSchema,
};

use super::super::engine::EngineInner;
use super::dialect::{join_predicate_direct, join_predicate_m2m_pairs, SqlDialect};

/// Ties a scan to what encloses it: an optional `JOIN` (many-to-many through
/// table) and the predicate selecting the enclosing row's related rows, or one
/// primary key. Empty for a root scan.
#[derive(Default)]
pub(super) struct ScanLink {
    pub(super) join: String,
    pub(super) on: String,
}

#[derive(Clone, Copy)]
pub(super) struct ModelScan<'a> {
    pub(super) inner: &'a EngineInner,
    pub(super) session: &'a Session,
    pub(super) role: &'a str,
    /// Complete catalog schema: a row policy may reference columns and
    /// relationships the privilege cannot select.
    pub(super) schema: &'a TableSchema,
    pub(super) grant: &'a SurfaceModel,
}

impl<'a> ModelScan<'a> {
    pub(super) fn open(
        inner: &'a EngineInner,
        session: &'a Session,
        role: &'a str,
        model_name: &str,
    ) -> Result<Self, String> {
        let entry = inner
            .catalog
            .get(model_name)
            .ok_or_else(|| format!("unknown model `{model_name}`"))?;
        let grant = inner
            .role_surfaces
            .get(role)
            .and_then(|surface| surface.models.get(model_name))
            .ok_or_else(|| format!("role `{role}` has no permission on `{model_name}`"))?;
        Ok(Self {
            inner,
            session,
            role,
            schema: &entry.schema,
            grant,
        })
    }

    /// The relationship's target as this privilege may read it, or `None`
    /// when the privilege has no grant on it.
    pub(super) fn target(&self, rel: &RelationshipDef) -> Option<ModelScan<'a>> {
        Self::open(self.inner, self.session, self.role, &rel.target_model).ok()
    }

    /// The target scan and its link to this scan's row at `alias`.
    ///
    /// The per-privilege schema never offers a relationship to an ungranted
    /// model, so reaching one here is a schema/compiler disagreement.
    pub(super) fn related(
        &self,
        rel: &RelationshipDef,
        alias: &str,
        target_alias: &str,
        join_alias: &str,
        tables: &mut Vec<String>,
    ) -> Result<(ModelScan<'a>, ScanLink), String> {
        let target = self.target(rel).ok_or_else(|| {
            format!(
                "internal: relationship `{}` reached ungranted model `{}`",
                rel.field_name, rel.target_model
            )
        })?;
        let link = relationship_link(
            self.inner,
            self.schema,
            rel,
            target.schema,
            alias,
            target_alias,
            join_alias,
            tables,
        )?;
        Ok((target, link))
    }

    pub(super) fn allows_column(&self, column: &str) -> bool {
        self.grant.allows_column(column)
    }

    pub(super) fn aggregations(&self) -> bool {
        self.grant.aggregations
    }

    /// Whether rows of this scan carry an identity the client can address.
    pub(super) fn has_client_normalized_identity(&self) -> bool {
        model_has_client_normalized_identity(self.grant)
    }

    /// Page size: the client's request, capped by the grant and the engine.
    pub(super) fn limit(&self, client: Option<&Value>) -> u64 {
        resolve_limit(
            client,
            self.grant.role_limit,
            self.inner.default_limit,
            self.inner.max_limit,
        )
    }

    pub(super) fn order_by(
        &self,
        order_arg: Option<&Value>,
        alias: &str,
    ) -> Result<String, String> {
        compile_order_by(
            self.schema,
            order_arg,
            alias,
            |column| self.allows_column(column),
            self.inner.strict_where,
            self.inner.dialect,
        )
    }
}

/// `source` row at `source_alias` → its `rel` rows in `target` at
/// `target_alias`. Records every table the link reads.
pub(super) fn relationship_link(
    inner: &EngineInner,
    source: &TableSchema,
    rel: &RelationshipDef,
    target: &TableSchema,
    source_alias: &str,
    target_alias: &str,
    join_alias: &str,
    tables: &mut Vec<String>,
) -> Result<ScanLink, String> {
    tables.push(target.table_name.clone());
    match rel.kind {
        RelationshipKind::HasMany | RelationshipKind::BelongsTo => {
            let pairs =
                resolve_direct_join_keys(source, rel, target).map_err(|error| error.to_string())?;
            Ok(ScanLink {
                join: String::new(),
                on: join_predicate_direct(rel.kind.clone(), source_alias, target_alias, &pairs)?,
            })
        }
        RelationshipKind::ManyToMany => {
            let through = rel
                .through
                .as_deref()
                .ok_or_else(|| "m2m missing through".to_string())?;
            let through_schema = inner
                .by_table
                .get(through)
                .and_then(|model| inner.catalog.get(model))
                .map(|entry| &entry.schema)
                .ok_or_else(|| format!("through table `{through}` not in catalog"))?;
            let keys = resolve_m2m_join_keys(source, rel, through_schema, target)
                .map_err(|error| error.to_string())?;
            tables.push(through.to_string());
            let on_target = join_predicate_m2m_pairs(join_alias, target_alias, &keys.target)?;
            Ok(ScanLink {
                join: format!(" JOIN \"{through}\" {join_alias} ON {on_target}"),
                on: join_predicate_m2m_pairs(join_alias, source_alias, &keys.parent)?,
            })
        }
    }
}

pub(super) fn resolve_limit(
    client: Option<&Value>,
    role_limit: Option<u64>,
    default_limit: u64,
    max_limit: u64,
) -> u64 {
    let client = client.and_then(value_as_u64).unwrap_or(default_limit);
    let with_role = role_limit.map(|r| client.min(r)).unwrap_or(client);
    with_role.min(max_limit)
}

pub(super) fn value_as_u64(v: &Value) -> Option<u64> {
    match v {
        Value::Number(n) => n
            .as_u64()
            .or_else(|| n.as_i64().and_then(|i| u64::try_from(i).ok())),
        _ => None,
    }
}

pub(super) fn compile_order_by(
    schema: &TableSchema,
    order_arg: Option<&Value>,
    alias: &str,
    allows_column: impl Fn(&str) -> bool,
    strict: bool,
    dialect: SqlDialect,
) -> Result<String, String> {
    let mut parts = Vec::new();
    if let Some(Value::List(items)) = order_arg {
        for item in items {
            if let Value::Object(map) = item {
                if map.len() > 1 {
                    return Err(
                        "ambiguous order_by entry: use one field per list entry to declare priority"
                            .into(),
                    );
                }
                for (col, dir) in map {
                    if !schema.columns.iter().any(|c| c.column_name == *col) {
                        if strict {
                            return Err(format!("unknown order_by column `{col}`"));
                        }
                        continue;
                    }
                    if !allows_column(col) {
                        if strict {
                            return Err(format!("ungranted order_by column `{col}`"));
                        }
                        continue;
                    }
                    let dir_s = match dir {
                        Value::Enum(e) => e.as_str(),
                        Value::String(s) => s.as_str(),
                        _ => "asc",
                    };
                    let sql_dir = match dir_s {
                        "desc" | "desc_nulls_first" | "desc_nulls_last" => "DESC",
                        _ => "ASC",
                    };
                    let nulls = match dir_s {
                        "asc_nulls_first" | "desc_nulls_first" => " NULLS FIRST",
                        "asc_nulls_last" | "desc_nulls_last" => " NULLS LAST",
                        _ => "",
                    };
                    let collation = schema
                        .columns
                        .iter()
                        .find(|column| column.column_name == *col)
                        .filter(|column| column.column_type == ColumnType::Text)
                        .map(|_| format!(" COLLATE {}", dialect.ops().binary_collation))
                        .unwrap_or_default();
                    parts.push(format!("{alias}.\"{col}\"{collation} {sql_dir}{nulls}"));
                }
            }
        }
    }
    // Always append PK asc tiebreaker.
    for pk in &schema.primary_key.columns {
        let collation = schema
            .columns
            .iter()
            .find(|column| column.column_name == *pk)
            .filter(|column| column.column_type == ColumnType::Text)
            .map(|_| format!(" COLLATE {}", dialect.ops().binary_collation))
            .unwrap_or_default();
        parts.push(format!("{alias}.\"{pk}\"{collation} ASC"));
    }
    if parts.is_empty() {
        Ok(String::new())
    } else {
        Ok(format!("ORDER BY {}", parts.join(", ")))
    }
}

#[cfg(test)]
mod strict_order_by_tests {
    use super::*;
    use crate::table::{ColumnType, PrimaryKey, TableColumn, TableKind, TableSchema};
    use async_graphql::indexmap::IndexMap;
    use async_graphql::Value as GqlValue;

    fn item_schema() -> TableSchema {
        TableSchema {
            model_name: "Item".into(),
            table_name: "items".into(),
            columns: vec![
                TableColumn {
                    primary_key: true,
                    ..TableColumn::new("id", "id", ColumnType::Text)
                },
                TableColumn::new("name", "name", ColumnType::Text),
                TableColumn::new("secret", "secret", ColumnType::Text),
            ],
            primary_key: PrimaryKey::new(["id"]),
            version_column: None,
            indexes: Vec::new(),
            relationships: Vec::new(),
            kind: TableKind::ReadModel,
        }
    }

    fn order_list(entries: Vec<(&str, &str)>) -> GqlValue {
        let mut items = Vec::new();
        for (col, dir) in entries {
            let mut map = IndexMap::new();
            map.insert(
                async_graphql::Name::new(col),
                GqlValue::Enum(async_graphql::Name::new(dir)),
            );
            items.push(GqlValue::Object(map));
        }
        GqlValue::List(items)
    }

    #[test]
    fn strict_rejects_unknown_order_column() {
        let schema = item_schema();
        let perm = |_: &str| true;
        let arg = order_list(vec![("nope", "asc")]);
        let err = compile_order_by(&schema, Some(&arg), "t0", perm, true, SqlDialect::Sqlite)
            .unwrap_err();
        assert!(err.contains("unknown order_by"), "{err}");
    }

    #[test]
    fn strict_rejects_ungranted_order_column() {
        let schema = item_schema();
        let perm = |column: &str| matches!(column, "id" | "name");
        let arg = order_list(vec![("secret", "asc")]);
        let err = compile_order_by(&schema, Some(&arg), "t0", perm, true, SqlDialect::Sqlite)
            .unwrap_err();
        assert!(err.contains("ungranted order_by"), "{err}");
    }

    #[test]
    fn soft_skip_ignores_unknown_and_ungranted_order() {
        let schema = item_schema();
        let perm = |column: &str| matches!(column, "id" | "name");
        let arg = order_list(vec![("secret", "asc"), ("nope", "desc"), ("name", "desc")]);
        let sql =
            compile_order_by(&schema, Some(&arg), "t0", perm, false, SqlDialect::Sqlite).unwrap();
        assert!(sql.contains(r#"t0."name" COLLATE BINARY DESC"#), "{sql}");
        assert!(!sql.contains("secret"), "{sql}");
        assert!(!sql.contains("nope"), "{sql}");
        assert!(
            sql.contains(r#"t0."id" COLLATE BINARY ASC"#),
            "pk tiebreak: {sql}"
        );
    }

    #[test]
    fn strict_accepts_granted_order_with_pk_tiebreak() {
        let schema = item_schema();
        let perm = |_: &str| true;
        let arg = order_list(vec![("name", "desc")]);
        let sql =
            compile_order_by(&schema, Some(&arg), "t0", perm, true, SqlDialect::Sqlite).unwrap();
        assert!(sql.contains(r#"t0."name" COLLATE BINARY DESC"#), "{sql}");
        assert!(sql.contains(r#"t0."id" COLLATE BINARY ASC"#), "{sql}");
    }

    #[test]
    fn multi_field_order_object_is_rejected_even_in_soft_mode() {
        let schema = item_schema();
        let perm = |_: &str| true;
        let mut entry = IndexMap::new();
        entry.insert(
            async_graphql::Name::new("name"),
            GqlValue::Enum(async_graphql::Name::new("desc")),
        );
        entry.insert(
            async_graphql::Name::new("id"),
            GqlValue::Enum(async_graphql::Name::new("asc")),
        );
        let arg = GqlValue::List(vec![GqlValue::Object(entry)]);

        let error = compile_order_by(&schema, Some(&arg), "t0", perm, false, SqlDialect::Sqlite)
            .unwrap_err();
        assert!(error.contains("ambiguous order_by"), "{error}");
        assert!(error.contains("one field per list entry"), "{error}");
    }

    #[test]
    fn separate_order_entries_preserve_declared_priority() {
        let schema = item_schema();
        let perm = |_: &str| true;
        let arg = order_list(vec![("name", "desc"), ("secret", "asc")]);

        let sql =
            compile_order_by(&schema, Some(&arg), "t0", perm, true, SqlDialect::Postgres).unwrap();
        assert!(sql.contains(r#"t0."name" COLLATE "C" DESC"#), "{sql}");
        assert!(sql.contains(r#"t0."secret" COLLATE "C" ASC"#), "{sql}");
        let name_position = sql
            .find(r#"t0."name" COLLATE "C" DESC"#)
            .expect("name ordering");
        let secret_position = sql
            .find(r#"t0."secret" COLLATE "C" ASC"#)
            .expect("secret ordering");
        assert!(name_position < secret_position, "{sql}");
    }
}
