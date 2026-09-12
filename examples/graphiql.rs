//! Local GraphiQL playground for the GraphQL query service.
//!
//! Boots an in-memory SQLite read model, seeds sample orders, mounts GraphQL
//! with GraphiQL enabled, and serves on `http://127.0.0.1:4000/graphql`.
//!
//! ```bash
//! cargo run --example graphiql --features "graphql,sqlite"
//! ```
//!
//! Open the URL in a browser. GraphiQL ships default headers `x-roles: user`
//! and `x-user-id: demo` (edit them in the Headers panel for other roles).
//!
//! Override bind address with `GRAPHIQL_ADDR` (default `127.0.0.1:4000`).

#![cfg(all(feature = "graphql", feature = "sqlite"))]

use std::sync::Arc;

use distributed::graphql::GraphqlEngine;
use distributed::microsvc::{serve, Service};
use distributed::{ColumnType, PrimaryKey, ReadModelCatalog, TableColumn, TableKind, TableSchema};
use sqlx::sqlite::SqlitePoolOptions;

fn orders_schema() -> TableSchema {
    TableSchema {
        model_name: "OrderView".into(),
        table_name: "orders".into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                ..TableColumn::new("order_id", "order_id", ColumnType::Text)
            },
            TableColumn::new("customer_id", "customer_id", ColumnType::Text),
            TableColumn::new("status", "status", ColumnType::Text),
            TableColumn {
                column_type: ColumnType::Integer,
                ..TableColumn::new("total_cents", "total_cents", ColumnType::Integer)
            },
        ],
        primary_key: PrimaryKey::new(["order_id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    }
}

async fn seed_pool() -> sqlx::SqlitePool {
    let pool = SqlitePoolOptions::new()
        .connect("sqlite::memory:")
        .await
        .expect("connect sqlite");
    sqlx::query(
        "CREATE TABLE orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            status TEXT NOT NULL,
            total_cents INTEGER NOT NULL
        );
        INSERT INTO orders VALUES
            ('o1', 'c1', 'open', 1000),
            ('o2', 'c1', 'shipped', 2000),
            ('o3', 'c2', 'open', 500),
            ('o4', 'demo', 'open', 4200);",
    )
    .execute(&pool)
    .await
    .expect("seed orders");
    pool
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = std::env::var("GRAPHIQL_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".into());
    let pool = seed_pool().await;
    let manifest = ReadModelCatalog::new("graphiql-demo").table_schema(orders_schema());

    let engine = GraphqlEngine::from_schema_catalog(&manifest, pool)?
        .roles(&["user", "anonymous"])
        .grant_all("user")
        .graphiql(true)
        .build()?;

    let service = Arc::new(Service::new().named("graphiql-demo").with_graphql(engine));

    println!();
    println!("  GraphiQL  →  http://{addr}/graphql");
    println!("  Health    →  http://{addr}/health");
    println!();
    println!("  Default headers (already set in GraphiQL):");
    println!("    x-roles: user");
    println!("    x-user-id: demo");
    println!();
    println!("  Try:");
    println!("    {{ orders {{ order_id customer_id status total_cents }} }}");
    println!("    {{ orders(where: {{ status: {{ _eq: \"open\" }} }}) {{ order_id status }} }}");
    println!("    {{ orders_by_pk(order_id: \"o1\") {{ order_id status total_cents }} }}");
    println!("    {{ orders_aggregate {{ aggregate {{ count }} }} }}");
    println!();

    serve(service, &addr).await?;
    Ok(())
}
