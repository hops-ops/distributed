//! GraphQL harden / red-team suite.
//!
//! The modules listed in `on_each_dialect!` hold scenarios that take a `Db`;
//! each runs once on SQLite and once on Postgres (`DATABASE_URL`). The other
//! modules are SQLite-only by nature (dialect operators, microsvc HTTP).
//!
//! Modules map to threat categories (S/A/D/E/T). Every test drives shipped
//! `GraphqlEngine::execute` and/or microsvc HTTP — no SQL reimplementation.
//!
//! | Module | Coverage |
//! |---|---|
//! | `access_paths` | Row + column policy on every read access path (one table of cases) |
//! | `authz` | A* AuthZ (claim list/by_pk/aggregate, column allowlists, nested deny) |
//! | `inject` | S* injection / key safety |
//! | `dos` | D* depth, in-list, bool-width, concurrency |
//! | `errors` | E* leak + TIMEOUT |
//! | `transport` | T* introspection HTTP, mutation grants, GraphiQL policy |
//! | `softskip` | Fail-closed where/order contracts (strict_where default) |
//! | `residual` | A8/A12/S9/E4 residual post-quality-1 review |
//! | `json_columns` | A `Json` column is JSON, not a string, on every read path |
//! | `text_patterns` | `_like` / `_ilike` / `_icontains` mean the same on both dialects |
//! | `dialect` | Dialect-honest comparison ops (no PG JSON ops on SQLite) |

#![cfg(all(feature = "graphql", feature = "sqlite"))]

mod access_paths;
mod authz;
mod dialect;
mod dos;
mod errors;
mod inject;
mod json_columns;
mod residual;
mod softskip;
mod text_patterns;
mod transport;

use common::on_each_dialect;

on_each_dialect! {
    access_paths::{
        row_policy_holds_on_every_access_path,
        ungranted_columns_models_and_aggregates_are_refused_on_every_access_path,
    }
    authz::{
        claim_row_filter_isolates_tenants,
        a2_claim_filter_on_by_pk_isolates_tenants,
        a3_claim_filter_on_aggregate_count,
        column_allowlist_denies_ungranted_fields,
        a5_nested_relationship_column_allowlist_denies,
        nested_has_many_relationship_e2e,
        by_pk_with_nested_children_returns_row,
        json_looking_string_column_stays_string,
        application_surface_with_differing_row_policies_applies_all_of_them,
    }
    dos::{
        where_max_depth_rejected,
        graphql_depth_counts_root_and_leaf_fields,
        max_in_list_rejected,
        limit_clamped_by_max_limit,
        d5_wide_or_list_rejected_by_max_bool_width,
        d5_wide_and_list_rejected_by_max_bool_width,
        d7_concurrent_queries_complete,
        d7_concurrent_with_timeout_bound_terminates,
        d8_two_level_default_nest_exceeds_row_budget,
        d8_shallow_nested_has_many_within_budget,
        d8_low_max_rows_rejects_single_nest,
        d8_explicit_limits_multiply_into_the_row_budget,
    }
    errors::{
        compile_errors_do_not_leak_sql,
        e2_execute_failure_is_internal_without_sql_leak,
    }
    inject::{
        s1_response_key_field_selection_safe_roundtrip,
        s2_unknown_where_key_fails_closed_without_sql_leak,
        s3_like_wildcards_are_bound_and_safe,
        s4_json_looking_string_stays_string,
        s5_order_by_junk_direction_is_safe,
        s6_pk_type_confusion_does_not_leak_sql,
        s2_denied_where_column_fails_closed,
    }
    json_columns::{
        json_columns_are_json_on_every_read_path,
        json_column_in_a_wide_row_is_json,
    }
    residual::{
        a8_nested_has_many_without_child_grant_is_unknown_field,
        a12_rel_where_without_target_grant_is_unknown_field,
        s9_json_contains_on_a_text_column_is_rejected_without_sql_leak,
        e4_missing_claim_header_is_stable_without_sql_leak,
    }
    softskip::{
        strict_unknown_where_key_errors,
        strict_denied_where_column_errors,
        strict_junk_order_by_column_errors,
        strict_valid_where_and_order_still_work,
        soft_skip_mode_is_opt_in_only,
    }
    text_patterns::{
        string_operators_match_the_same_rows_on_every_dialect,
        row_policy_patterns_match_the_same_rows_on_every_dialect,
        pattern_ending_in_its_escape_is_a_bad_request,
    }
}
