//! A throwaway schema of a Postgres test database, for tests whose tables and
//! topology have fixed names. Projection registrations are insert-only, so
//! such a test run twice against one schema meets its own earlier run.

use common::postgres::PostgresTestSchema;

/// A fresh schema of the database at `var`, and that database's URL with the
/// schema as its `search_path`, so every connection opened from the URL,
/// including a restarted repository's, resolves tables in it. Dropping the
/// returned schema drops it, also when the test panics.
pub(crate) async fn postgres_test_schema(
    var: &str,
    what: &str,
    prefix: &str,
) -> Option<(PostgresTestSchema, String)> {
    let url = crate::test_env::require_service(var, what)?;
    let schema = PostgresTestSchema::create(&url, prefix)
        .await
        .expect("Postgres test schema should create");
    // Generated schema names are lowercase ASCII, digits and `_`, so they need
    // no escaping inside the startup `options` parameter.
    let separator = if url.contains('?') { '&' } else { '?' };
    let url = format!(
        "{url}{separator}options=-c%20search_path%3D{}",
        schema.schema_name()
    );
    Some((schema, url))
}
