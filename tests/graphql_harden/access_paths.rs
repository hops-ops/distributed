//! Row and column policy on every read access path.
//!
//! One fixture, one table of cases: each access path (list, by-pk, root
//! aggregate, has-many, belongs-to, many-to-many, relationship aggregate,
//! relationship `where`) is driven through `GraphqlEngine::execute` against
//! seeded rows, once for the row policy and once for the column policy.

use async_graphql::Request;
use distributed::graphql::{claim, col, read, GraphqlEngine, ReadPermission};
use distributed::{
    ReadModel, ReadModelCatalog, RelationalReadModel, RelationshipDef, RelationshipKind,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::common::{exec_json, session};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("authors")]
struct AuthorView {
    #[id("author_id")]
    author_id: String,
    tenant: String,
    name: String,
    secret: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("posts")]
struct PostView {
    #[id("post_id")]
    post_id: String,
    author_id: String,
    tenant: String,
    title: String,
    secret: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("tags")]
struct TagView {
    #[id("tag_id")]
    tag_id: String,
    tenant: String,
    label: String,
    secret: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("post_tags")]
struct PostTagView {
    #[id("link_id")]
    link_id: String,
    post_id: String,
    tag_id: String,
}

/// Rows the tenant `t1` must never see carry `HIDDEN` in a selectable column,
/// and every row carries `S3CR3T` in the column no role is granted.
const SEED: &str = "
    CREATE TABLE authors (author_id TEXT PRIMARY KEY, tenant TEXT NOT NULL, name TEXT NOT NULL, secret TEXT NOT NULL);
    CREATE TABLE posts (post_id TEXT PRIMARY KEY, author_id TEXT NOT NULL, tenant TEXT NOT NULL, title TEXT NOT NULL, secret TEXT NOT NULL);
    CREATE TABLE tags (tag_id TEXT PRIMARY KEY, tenant TEXT NOT NULL, label TEXT NOT NULL, secret TEXT NOT NULL);
    CREATE TABLE post_tags (link_id TEXT PRIMARY KEY, post_id TEXT NOT NULL, tag_id TEXT NOT NULL);
    INSERT INTO authors VALUES ('a1', 't1', 'Ann', 'S3CR3T'), ('a2', 't2', 'HIDDEN', 'S3CR3T');
    INSERT INTO posts VALUES
        ('p1', 'a1', 't1', 'mine', 'S3CR3T'),
        ('p2', 'a1', 't2', 'HIDDEN', 'S3CR3T'),
        ('p3', 'a2', 't2', 'HIDDEN', 'S3CR3T'),
        ('p4', 'a2', 't1', 'orphan', 'S3CR3T');
    INSERT INTO tags VALUES ('g1', 't1', 'ours', 'S3CR3T'), ('g2', 't2', 'HIDDEN', 'S3CR3T');
    INSERT INTO post_tags VALUES ('l1', 'p1', 'g1'), ('l2', 'p1', 'g2'), ('l3', 'p2', 'g1'), ('l4', 'p4', 'g1');
";

fn relationship(
    field: &str,
    kind: RelationshipKind,
    target: &str,
    foreign_key: &str,
) -> RelationshipDef {
    RelationshipDef {
        references: None,
        field_name: field.into(),
        kind,
        target_model: target.into(),
        foreign_key: Some(foreign_key.into()),
        through: None,
        target_foreign_key: None,
    }
}

/// Own-tenant rows, every column but `secret`.
fn tenant(columns: &[&str]) -> ReadPermission {
    read()
        .columns(columns.iter().copied())
        .aggregations()
        .rows(col("tenant").eq(claim("x-user-id")))
}

async fn engine() -> GraphqlEngine {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::query(SEED).execute(&pool).await.unwrap();

    let mut authors = AuthorView::schema().clone();
    authors.relationships = vec![relationship(
        "posts",
        RelationshipKind::HasMany,
        "PostView",
        "author_id",
    )];
    let mut posts = PostView::schema().clone();
    posts.relationships = vec![
        relationship(
            "author",
            RelationshipKind::BelongsTo,
            "AuthorView",
            "author_id",
        ),
        RelationshipDef {
            through: Some("post_tags".into()),
            target_foreign_key: Some("tag_id".into()),
            ..relationship("tags", RelationshipKind::ManyToMany, "TagView", "post_id")
        },
    ];
    let catalog = ReadModelCatalog::new("access-paths")
        .table_schema(authors)
        .table_schema(posts)
        .table_schema(TagView::schema().clone())
        .table_schema(PostTagView::schema().clone());

    let author_columns = ["author_id", "tenant", "name"];
    let post_columns = ["post_id", "author_id", "tenant", "title"];
    let tag_columns = ["tag_id", "tenant", "label"];
    GraphqlEngine::from_schema_catalog(&catalog, pool)
        .unwrap()
        .roles(&["reader", "capped", "narrow", "counter"])
        // reader: the policy under test on all three models.
        .permission::<AuthorView>("reader", tenant(&author_columns))
        .permission::<PostView>("reader", tenant(&post_columns))
        .permission::<TagView>("reader", tenant(&tag_columns))
        // capped: a role page limit.
        .permission::<PostView>("capped", read().columns(post_columns).limit(1))
        // narrow: authors only; posts are not granted at all.
        .permission::<AuthorView>("narrow", tenant(&author_columns))
        // counter: both models, but no aggregations on posts.
        .permission::<AuthorView>("counter", read().columns(author_columns))
        .permission::<PostView>("counter", read().columns(post_columns))
        .build()
        .unwrap()
}

#[tokio::test]
async fn row_policy_holds_on_every_access_path() {
    let engine = engine().await;
    let cases = [
        (
            "list",
            "reader",
            "{ posts(order_by: [{post_id: asc}]) { post_id } }",
            json!({ "posts": [{ "post_id": "p1" }, { "post_id": "p4" }] }),
        ),
        (
            "by-pk, own row",
            "reader",
            r#"{ posts_by_pk(post_id: "p1") { post_id } }"#,
            json!({ "posts_by_pk": { "post_id": "p1" } }),
        ),
        (
            "by-pk, another tenant's row",
            "reader",
            r#"{ posts_by_pk(post_id: "p2") { post_id title } }"#,
            json!({ "posts_by_pk": null }),
        ),
        (
            "root aggregate count",
            "reader",
            "{ posts_aggregate { aggregate { count } } }",
            json!({ "posts_aggregate": { "aggregate": { "count": 2 } } }),
        ),
        (
            "root aggregate nodes",
            "reader",
            "{ posts_aggregate { nodes { post_id } } }",
            json!({ "posts_aggregate": { "nodes": [{ "post_id": "p1" }, { "post_id": "p4" }] } }),
        ),
        (
            "has-many",
            "reader",
            "{ authors { author_id posts { post_id } } }",
            json!({ "authors": [{ "author_id": "a1", "posts": [{ "post_id": "p1" }] }] }),
        ),
        (
            "belongs-to",
            "reader",
            "{ posts(order_by: [{post_id: asc}]) { post_id author { author_id } } }",
            json!({ "posts": [
                { "post_id": "p1", "author": { "author_id": "a1" } },
                { "post_id": "p4", "author": null },
            ]}),
        ),
        (
            "many-to-many",
            "reader",
            "{ posts(order_by: [{post_id: asc}]) { post_id tags { tag_id } } }",
            json!({ "posts": [
                { "post_id": "p1", "tags": [{ "tag_id": "g1" }] },
                { "post_id": "p4", "tags": [{ "tag_id": "g1" }] },
            ]}),
        ),
        (
            "has-many aggregate",
            "reader",
            "{ authors { posts_aggregate { aggregate { count } nodes { post_id } } } }",
            json!({ "authors": [{ "posts_aggregate": {
                "aggregate": { "count": 1 },
                "nodes": [{ "post_id": "p1" }],
            }}]}),
        ),
        (
            "many-to-many aggregate",
            "reader",
            r#"{ posts_by_pk(post_id: "p1") { tags_aggregate { aggregate { count } nodes { tag_id } } } }"#,
            json!({ "posts_by_pk": { "tags_aggregate": {
                "aggregate": { "count": 1 },
                "nodes": [{ "tag_id": "g1" }],
            }}}),
        ),
        (
            "where through has-many",
            "reader",
            r#"{ authors(where: {posts: {title: {_eq: "HIDDEN"}}}) { author_id } }"#,
            json!({ "authors": [] }),
        ),
        (
            "where through belongs-to",
            "reader",
            r#"{ posts(where: {author: {name: {_eq: "HIDDEN"}}}) { post_id } }"#,
            json!({ "posts": [] }),
        ),
        (
            "where through many-to-many",
            "reader",
            r#"{ posts(where: {tags: {label: {_eq: "HIDDEN"}}}) { post_id } }"#,
            json!({ "posts": [] }),
        ),
        (
            "role limit caps the page",
            "capped",
            "{ posts(order_by: [{post_id: asc}], limit: 50) { post_id } }",
            json!({ "posts": [{ "post_id": "p1" }] }),
        ),
    ];
    for (path, role, query, expected) in cases {
        let data = exec_json(&engine, &session(role, "t1"), query).await;
        assert_eq!(data, expected, "{path}: {query}");
    }
}

#[tokio::test]
async fn ungranted_columns_models_and_aggregates_are_refused_on_every_access_path() {
    let engine = engine().await;
    let cases = [
        ("list", "reader", "{ posts { secret } }"),
        (
            "by-pk",
            "reader",
            r#"{ posts_by_pk(post_id: "p1") { secret } }"#,
        ),
        (
            "root aggregate nodes",
            "reader",
            "{ posts_aggregate { nodes { secret } } }",
        ),
        ("has-many", "reader", "{ authors { posts { secret } } }"),
        ("belongs-to", "reader", "{ posts { author { secret } } }"),
        ("many-to-many", "reader", "{ posts { tags { secret } } }"),
        (
            "has-many aggregate nodes",
            "reader",
            "{ authors { posts_aggregate { nodes { secret } } } }",
        ),
        (
            "where",
            "reader",
            r#"{ posts(where: {secret: {_eq: "S3CR3T"}}) { post_id } }"#,
        ),
        (
            "where through a relationship",
            "reader",
            r#"{ authors(where: {posts: {secret: {_eq: "S3CR3T"}}}) { author_id } }"#,
        ),
        (
            "order_by",
            "reader",
            "{ posts(order_by: [{secret: asc}]) { post_id } }",
        ),
        ("ungranted model root", "narrow", "{ posts { post_id } }"),
        (
            "ungranted model nested",
            "narrow",
            "{ authors { posts { post_id } } }",
        ),
        (
            "ungranted model in where",
            "narrow",
            r#"{ authors(where: {posts: {title: {_eq: "mine"}}}) { author_id } }"#,
        ),
        (
            "ungranted model aggregate",
            "narrow",
            "{ authors { posts_aggregate { aggregate { count } } } }",
        ),
        (
            "root aggregate without the grant",
            "counter",
            "{ posts_aggregate { aggregate { count } } }",
        ),
        (
            "relationship aggregate without the grant",
            "counter",
            "{ authors { posts_aggregate { aggregate { count } } } }",
        ),
    ];
    for (path, role, query) in cases {
        let response = engine
            .execute(&session(role, "t1"), Request::new(query))
            .await;
        assert!(
            !response.errors.is_empty(),
            "{path} must be refused: {query}"
        );
        let data = serde_json::to_string(&response.data).unwrap();
        assert!(
            !data.contains("S3CR3T") && !data.contains("p1"),
            "{path} leaked data: {data}"
        );
    }
}
