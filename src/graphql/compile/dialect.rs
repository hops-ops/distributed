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
/// empty-array strings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DialectOps {
    pub json_agg: &'static str,
    pub empty_array: &'static str,
    pub build_object: &'static str,
    /// SQLite wraps list roots with `json(...)`; Postgres leaves this empty.
    pub json_cast_fn: Option<&'static str>,
}

impl SqlDialect {
    pub fn ops(self) -> DialectOps {
        match self {
            SqlDialect::Postgres => DialectOps {
                json_agg: "jsonb_agg",
                empty_array: "'[]'::jsonb",
                build_object: "jsonb_build_object",
                json_cast_fn: None,
            },
            SqlDialect::Sqlite => DialectOps {
                json_agg: "json_group_array",
                empty_array: "'[]'",
                build_object: "json_object",
                // Ensures json_object TEXT is treated as JSON, not a JSON string.
                json_cast_fn: Some("json"),
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

/// `column` matched against a text `pattern`, with one meaning on both
/// dialects:
///
/// - `%` matches any run of characters and `_` exactly one;
/// - `escape` makes the character after it literal, and may not end the pattern;
/// - `case_sensitive` compares characters exactly; otherwise ASCII letters
///   fold and every other character still compares exactly.
///
/// Returns the predicate and the value to bind at `placeholder`.
///
/// The dialects' own operators disagree, which is why none is used bare:
/// SQLite `LIKE` folds ASCII and has no default escape, Postgres `LIKE` is
/// exact with `\` as its default escape, and Postgres `ILIKE` folds by the
/// column's collation. So the case-sensitive form is `GLOB` on SQLite, with
/// the pattern translated, and the folding form relies on generated Postgres
/// text columns being `COLLATE "C"`, under which `ILIKE` folds ASCII only.
pub(super) fn text_pattern(
    dialect: SqlDialect,
    column: &str,
    placeholder: &str,
    pattern: &str,
    escape: char,
    case_sensitive: bool,
) -> Result<(String, String), String> {
    let mut glob = String::with_capacity(pattern.len());
    let mut chars = pattern.chars();
    while let Some(ch) = chars.next() {
        let literal = if ch == escape {
            chars
                .next()
                .ok_or("a pattern must not end with its escape character")?
        } else if ch == '%' {
            glob.push('*');
            continue;
        } else if ch == '_' {
            glob.push('?');
            continue;
        } else {
            ch
        };
        match literal {
            '*' | '?' | '[' => {
                glob.push('[');
                glob.push(literal);
                glob.push(']');
            }
            other => glob.push(other),
        }
    }
    Ok(match (dialect, case_sensitive) {
        (SqlDialect::Sqlite, true) => (format!("{column} GLOB {placeholder}"), glob),
        (SqlDialect::Sqlite, false) | (SqlDialect::Postgres, true) => (
            format!("{column} LIKE {placeholder} ESCAPE '{escape}'"),
            pattern.to_string(),
        ),
        (SqlDialect::Postgres, false) => (
            format!("{column} ILIKE {placeholder} ESCAPE '{escape}'"),
            pattern.to_string(),
        ),
    })
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
        assert_eq!(placeholder(SqlDialect::Postgres, 3), "$3");
    }

    #[test]
    fn sqlite_ops_table() {
        let ops = SqlDialect::Sqlite.ops();
        assert_eq!(ops.json_agg, "json_group_array");
        assert_eq!(ops.empty_array, "'[]'");
        assert_eq!(ops.build_object, "json_object");
        assert_eq!(ops.json_cast_fn, Some("json"));
        assert_eq!(placeholder(SqlDialect::Sqlite, 1), "?");
    }

    #[test]
    fn case_sensitive_sqlite_patterns_become_glob() {
        let glob = |pattern| {
            text_pattern(SqlDialect::Sqlite, "c", "?", pattern, '\\', true)
                .unwrap()
                .1
        };
        assert_eq!(glob("a%b_c"), "a*b?c");
        assert_eq!(glob(r"100\%"), "100%");
        assert_eq!(glob(r"a\_b"), "a_b");
        assert_eq!(glob("a*b?c[d]"), "a[*]b[?]c[[]d]");
        assert_eq!(glob(r"a\\b"), r"a\b");
        assert!(text_pattern(SqlDialect::Sqlite, "c", "?", r"a\", '\\', true).is_err());
        assert!(text_pattern(SqlDialect::Postgres, "c", "$1", r"a\", '\\', false).is_err());
    }
}

#[cfg(test)]
mod join_predicate_tests {
    use super::*;

    #[test]
    fn has_many_join_equates_every_key_pair() {
        struct Case {
            name: &'static str,
            pairs: &'static [(&'static str, &'static str)],
            expected: &'static str,
        }
        const CASES: &[Case] = &[
            Case {
                name: "one key",
                pairs: &[("order_id", "order_id")],
                expected: r#"t1."order_id" = t0."order_id""#,
            },
            Case {
                name: "a composite key ands the equalities",
                pairs: &[("workspace_id", "workspace_id"), ("path", "path")],
                expected: r#"t1."workspace_id" = t0."workspace_id" AND t1."path" = t0."path""#,
            },
        ];
        for case in CASES {
            let pairs: Vec<_> = case
                .pairs
                .iter()
                .map(|&(foreign_key, primary_key)| DirectJoinPair::new(foreign_key, primary_key))
                .collect();
            let sql = join_predicate_direct(RelationshipKind::HasMany, "t0", "t1", &pairs)
                .unwrap_or_else(|err| panic!("{}: {err}", case.name));
            assert_eq!(sql, case.expected, "{}", case.name);
        }
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
