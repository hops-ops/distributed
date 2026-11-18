use crate::table::{DirectJoinPair, JoinColumnPair, RelationshipKind};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlDialect {
    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    Postgres,
    Sqlite,
}

/// Dialect-specific SQL fragment table (dedup-4).
///
/// Prefer `dialect.ops()` over ad-hoc match arms for JSON aggregate / object /
/// empty-array / ILIKE strings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DialectOps {
    pub json_agg: &'static str,
    pub empty_array: &'static str,
    pub build_object: &'static str,
    /// Deterministic UTF-8 byte order shared with the generated client replica.
    pub binary_collation: &'static str,
    /// SQLite wraps list roots with `json(...)`; Postgres leaves this empty.
    pub json_cast_fn: Option<&'static str>,
    /// Case-insensitive LIKE operator (`ILIKE` on PG; `LIKE` on SQLite).
    pub ilike_op: &'static str,
}

impl SqlDialect {
    pub fn ops(self) -> DialectOps {
        match self {
            SqlDialect::Postgres => DialectOps {
                json_agg: "jsonb_agg",
                empty_array: "'[]'::jsonb",
                build_object: "jsonb_build_object",
                binary_collation: r#""C""#,
                json_cast_fn: None,
                ilike_op: "ILIKE",
            },
            SqlDialect::Sqlite => DialectOps {
                json_agg: "json_group_array",
                empty_array: "'[]'",
                build_object: "json_object",
                binary_collation: "BINARY",
                // Ensures json_object TEXT is treated as JSON, not a JSON string.
                json_cast_fn: Some("json"),
                ilike_op: "LIKE",
            },
        }
    }
}

impl DialectOps {
    /// A JSON-valued subquery embedded as a member of an enclosing object.
    ///
    /// SQLite marks JSON with a value subtype that a scalar subquery does not
    /// reliably carry out (it depends on the SQLite version and on how the
    /// subquery is evaluated). Without the subtype the enclosing
    /// `json_object` stores the nested JSON as a string. `json()` restores
    /// it, and maps SQL NULL to NULL, so an absent row stays `null`.
    pub(super) fn nested_json(&self, subquery: String) -> String {
        match self.json_cast_fn {
            None => subquery,
            Some(cast) => format!("{cast}({subquery})"),
        }
    }
}

pub(super) fn placeholder(dialect: SqlDialect, n: usize) -> String {
    match dialect {
        SqlDialect::Postgres => format!("${n}"),
        SqlDialect::Sqlite => "?".into(),
    }
}

/// AND of `fk = pk` equalities for a direct HasMany / BelongsTo join.
pub(crate) fn join_predicate_direct(
    kind: RelationshipKind,
    parent_alias: &str,
    child_alias: &str,
    pairs: &[DirectJoinPair],
) -> Result<String, String> {
    if pairs.is_empty() {
        return Err("direct join requires at least one key column".into());
    }
    match kind {
        RelationshipKind::HasMany => Ok(pairs
            .iter()
            .map(|pair| {
                format!(
                    "{child_alias}.\"{}\" = {parent_alias}.\"{}\"",
                    pair.foreign_key_column, pair.primary_key_column
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ")),
        RelationshipKind::BelongsTo => Ok(pairs
            .iter()
            .map(|pair| {
                format!(
                    "{child_alias}.\"{}\" = {parent_alias}.\"{}\"",
                    pair.primary_key_column, pair.foreign_key_column
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ")),
        RelationshipKind::ManyToMany => {
            Err("m2m relationships use join_predicate_m2m_pairs, not join_predicate_direct".into())
        }
    }
}

/// AND of `through.col = end.pk` equalities for one m2m side.
pub(crate) fn join_predicate_m2m_pairs(
    through_alias: &str,
    end_alias: &str,
    pairs: &[JoinColumnPair],
) -> Result<String, String> {
    if pairs.is_empty() {
        return Err("m2m join requires at least one key column".into());
    }
    Ok(pairs
        .iter()
        .map(|pair| {
            format!(
                "{through_alias}.\"{}\" = {end_alias}.\"{}\"",
                pair.through_column, pair.end_column
            )
        })
        .collect::<Vec<_>>()
        .join(" AND "))
}

#[cfg(test)]
mod dialect_ops_tests {
    use super::*;

    #[test]
    fn postgres_ops_table() {
        let ops = SqlDialect::Postgres.ops();
        assert_eq!(ops.json_agg, "jsonb_agg");
        assert_eq!(ops.empty_array, "'[]'::jsonb");
        assert_eq!(ops.build_object, "jsonb_build_object");
        assert_eq!(ops.json_cast_fn, None);
        assert_eq!(ops.ilike_op, "ILIKE");
        assert_eq!(placeholder(SqlDialect::Postgres, 3), "$3");
    }

    #[test]
    fn sqlite_ops_table() {
        let ops = SqlDialect::Sqlite.ops();
        assert_eq!(ops.json_agg, "json_group_array");
        assert_eq!(ops.empty_array, "'[]'");
        assert_eq!(ops.build_object, "json_object");
        assert_eq!(ops.json_cast_fn, Some("json"));
        assert_eq!(ops.ilike_op, "LIKE");
        assert_eq!(placeholder(SqlDialect::Sqlite, 1), "?");
    }
}

#[cfg(test)]
mod join_predicate_tests {
    use super::*;

    #[test]
    fn has_many_join() {
        let sql = join_predicate_direct(
            RelationshipKind::HasMany,
            "t0",
            "t1",
            &[DirectJoinPair::new("order_id", "order_id")],
        )
        .unwrap();
        assert_eq!(sql, r#"t1."order_id" = t0."order_id""#);
    }

    #[test]
    fn belongs_to_join() {
        let sql = join_predicate_direct(
            RelationshipKind::BelongsTo,
            "t0",
            "t1",
            &[DirectJoinPair::new("customer_id", "customer_id")],
        )
        .unwrap();
        assert_eq!(sql, r#"t1."customer_id" = t0."customer_id""#);
    }

    #[test]
    fn direct_composite_join_ands_equalities() {
        let sql = join_predicate_direct(
            RelationshipKind::HasMany,
            "t0",
            "t1",
            &[
                DirectJoinPair::new("workspace_id", "workspace_id"),
                DirectJoinPair::new("path", "path"),
            ],
        )
        .unwrap();
        assert_eq!(
            sql,
            r#"t1."workspace_id" = t0."workspace_id" AND t1."path" = t0."path""#
        );
    }

    #[test]
    fn m2m_rejects_direct_helper() {
        let err = join_predicate_direct(
            RelationshipKind::ManyToMany,
            "t0",
            "t1",
            &[DirectJoinPair::new("a", "b")],
        )
        .unwrap_err();
        assert!(err.contains("m2m"), "{err}");
    }

    #[test]
    fn m2m_fragments() {
        assert_eq!(
            join_predicate_m2m_pairs("j1", "t1", &[JoinColumnPair::new("post_id", "id")]).unwrap(),
            r#"j1."post_id" = t1."id""#
        );
        assert_eq!(
            join_predicate_m2m_pairs("j1", "t0", &[JoinColumnPair::new("user_id", "id")]).unwrap(),
            r#"j1."user_id" = t0."id""#
        );
        assert_eq!(
            join_predicate_m2m_pairs(
                "j1",
                "t1",
                &[
                    JoinColumnPair::new("workspace_id", "workspace_id"),
                    JoinColumnPair::new("path", "path"),
                ]
            )
            .unwrap(),
            r#"j1."workspace_id" = t1."workspace_id" AND j1."path" = t1."path""#
        );
        assert!(join_predicate_m2m_pairs("j1", "t1", &[])
            .unwrap_err()
            .contains("at least one key column"));
    }
}
