//! Atlas Operator schema-resource generation. Pure: wraps desired-state schema
//! SQL into an `AtlasSchema` custom resource (YAML) for the ariga
//! [atlas-operator]. The result is plain text the caller prints/writes wherever
//! it wants (e.g. stdout → any file, or a separate schema repo) — this crate
//! intentionally does **not** decide a `.gitops/` location for it.
//!
//! The desired-state SQL (e.g. `ReadModelCatalog::sql_statements`) goes
//! into `spec.schema.sql`; the operator diffs the live database against it and
//! applies the change.
//!
//! [atlas-operator]: https://github.com/ariga/atlas-operator

/// An `AtlasSchema` rendering error (bad spec value).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AtlasSchemaError(pub String);

impl std::fmt::Display for AtlasSchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for AtlasSchemaError {}

impl AtlasSchemaError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

/// How the generated `AtlasSchema` reaches its target database URL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AtlasDatabaseUrl {
    /// Reference a key in a Kubernetes `Secret` (`spec.urlFrom.secretKeyRef`).
    /// The GitOps-friendly choice — no credentials in the manifest.
    SecretKeyRef {
        /// Secret name.
        name: String,
        /// Key within the secret holding the connection URL.
        key: String,
    },
    /// Inline connection URL (`spec.url`). Convenient for dev; avoid committing
    /// real credentials this way.
    Inline(String),
}

/// Input for [`render_atlas_schema`]. Plain data — the caller maps its flags onto
/// this and supplies the desired-state schema SQL.
#[derive(Clone, Debug)]
pub struct AtlasSchemaSpec {
    /// `metadata.name` of the AtlasSchema resource.
    pub name: String,
    /// Optional `metadata.namespace`.
    pub namespace: Option<String>,
    /// Target database connection.
    pub database: AtlasDatabaseUrl,
    /// Optional `spec.devURL` — a scratch database Atlas uses to plan changes.
    pub dev_url: Option<String>,
    /// Desired-state schema SQL placed verbatim into `spec.schema.sql`.
    pub sql: String,
}

/// Render an `AtlasSchema` (`db.atlasgo.io/v1alpha1`) resource as YAML.
///
/// Returns an error for an empty name or empty schema SQL (nothing to apply), or
/// an incompletely specified database reference.
pub fn render_atlas_schema(spec: &AtlasSchemaSpec) -> Result<String, AtlasSchemaError> {
    validate_k8s_name(&spec.name, "AtlasSchema name")?;
    if let Some(namespace) = trimmed_non_empty(spec.namespace.as_deref()) {
        validate_k8s_name(namespace, "AtlasSchema namespace")?;
    }
    let name = spec.name.trim();
    if spec.sql.trim().is_empty() {
        return Err(AtlasSchemaError::new(
            "AtlasSchema has no schema SQL to apply (no tables registered?)",
        ));
    }

    let mut out = String::new();
    out.push_str("apiVersion: db.atlasgo.io/v1alpha1\n");
    out.push_str("kind: AtlasSchema\n");
    out.push_str("metadata:\n");
    out.push_str(&format!("  name: {name}\n"));
    if let Some(namespace) = trimmed_non_empty(spec.namespace.as_deref()) {
        out.push_str(&format!("  namespace: {namespace}\n"));
    }

    out.push_str("spec:\n");
    match &spec.database {
        AtlasDatabaseUrl::SecretKeyRef { name: secret, key } => {
            let secret = secret.trim();
            let key = key.trim();
            if secret.is_empty() || key.is_empty() {
                return Err(AtlasSchemaError::new(
                    "AtlasSchema secret reference needs both a secret name and a key",
                ));
            }
            out.push_str("  urlFrom:\n");
            out.push_str("    secretKeyRef:\n");
            out.push_str(&format!("      name: {secret}\n"));
            out.push_str(&format!("      key: {key}\n"));
        }
        AtlasDatabaseUrl::Inline(url) => {
            let url = url.trim();
            if url.is_empty() {
                return Err(AtlasSchemaError::new(
                    "AtlasSchema inline database URL is empty",
                ));
            }
            out.push_str(&format!("  url: {}\n", yaml_quote(url)));
        }
    }

    if let Some(dev_url) = trimmed_non_empty(spec.dev_url.as_deref()) {
        out.push_str(&format!("  devURL: {}\n", yaml_quote(dev_url)));
    }

    out.push_str("  schema:\n");
    out.push_str("    sql: |\n");
    for line in spec.sql.trim_end().lines() {
        if line.is_empty() {
            out.push('\n');
        } else {
            out.push_str("      ");
            out.push_str(line);
            out.push('\n');
        }
    }

    Ok(out)
}

fn trimmed_non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

/// Validate a Kubernetes object name (RFC 1123 label: lowercase ASCII letters,
/// digits, and `-`, not starting or ending with `-`). Fails at generation time
/// with a clear message rather than emitting YAML the API server would reject —
/// and rejects names with characters (newlines, colons, quotes) that would also
/// break the document itself.
fn validate_k8s_name(value: &str, field: &str) -> Result<(), AtlasSchemaError> {
    let name = value.trim();
    if name.is_empty() {
        return Err(AtlasSchemaError::new(format!("{field} must not be empty")));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Err(AtlasSchemaError::new(format!(
            "{field} `{name}` must contain only lowercase letters, digits, and hyphens"
        )));
    }
    if name.starts_with('-') || name.ends_with('-') {
        return Err(AtlasSchemaError::new(format!(
            "{field} `{name}` must not start or end with a hyphen"
        )));
    }
    Ok(())
}

/// Quote a value as a double-quoted YAML scalar. A JSON string literal is valid
/// YAML, so this safely escapes URLs that contain `:`, `@`, etc.
fn yaml_quote(value: &str) -> String {
    serde_json::to_string(value).expect("string serialization should succeed")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secret_spec() -> AtlasSchemaSpec {
        AtlasSchemaSpec {
            name: "orders".to_string(),
            namespace: None,
            database: AtlasDatabaseUrl::SecretKeyRef {
                name: "orders-db".to_string(),
                key: "url".to_string(),
            },
            dev_url: None,
            sql: "CREATE TABLE orders (id text PRIMARY KEY);".to_string(),
        }
    }

    #[test]
    fn renders_secret_ref_resource_with_indented_sql() {
        let yaml = render_atlas_schema(&secret_spec()).unwrap();
        assert!(yaml.contains("apiVersion: db.atlasgo.io/v1alpha1\n"));
        assert!(yaml.contains("kind: AtlasSchema\n"));
        assert!(yaml.contains("  name: orders\n"));
        assert!(
            yaml.contains("  urlFrom:\n    secretKeyRef:\n      name: orders-db\n      key: url\n")
        );
        // SQL is a literal block scalar, each line indented under `sql: |`.
        assert!(yaml.contains("    sql: |\n      CREATE TABLE orders (id text PRIMARY KEY);\n"));
        // No namespace / devURL emitted when unset.
        assert!(!yaml.contains("namespace:"));
        assert!(!yaml.contains("devURL:"));
    }

    #[test]
    fn namespace_and_dev_url_are_optional_and_quoted() {
        let mut spec = secret_spec();
        spec.namespace = Some("data".to_string());
        spec.dev_url = Some("docker://postgres/16/dev".to_string());
        let yaml = render_atlas_schema(&spec).unwrap();
        assert!(yaml.contains("  namespace: data\n"));
        assert!(yaml.contains("  devURL: \"docker://postgres/16/dev\"\n"));
    }

    #[test]
    fn inline_url_is_quoted_to_survive_special_characters() {
        let mut spec = secret_spec();
        spec.database =
            AtlasDatabaseUrl::Inline("postgres://u:p@host:5432/db?sslmode=disable".to_string());
        let yaml = render_atlas_schema(&spec).unwrap();
        assert!(yaml.contains("  url: \"postgres://u:p@host:5432/db?sslmode=disable\"\n"));
        assert!(!yaml.contains("urlFrom:"));
    }

    #[test]
    fn multi_statement_sql_keeps_every_line_indented() {
        let mut spec = secret_spec();
        spec.sql = "CREATE TABLE a (id text);\nCREATE TABLE b (id text);".to_string();
        let yaml = render_atlas_schema(&spec).unwrap();
        assert!(yaml.contains("      CREATE TABLE a (id text);\n      CREATE TABLE b (id text);\n"));
    }

    #[test]
    fn empty_name_or_sql_is_rejected() {
        let mut blank_name = secret_spec();
        blank_name.name = "  ".to_string();
        assert!(render_atlas_schema(&blank_name).is_err());

        let mut blank_sql = secret_spec();
        blank_sql.sql = "\n  \n".to_string();
        assert!(render_atlas_schema(&blank_sql).is_err());
    }

    #[test]
    fn invalid_kubernetes_names_are_rejected() {
        for bad in [
            "Orders",
            "orders_db",
            "orders.db",
            "-orders",
            "orders-",
            "a b",
        ] {
            let mut spec = secret_spec();
            spec.name = bad.to_string();
            assert!(
                render_atlas_schema(&spec).is_err(),
                "expected `{bad}` to be rejected"
            );
        }

        let mut bad_ns = secret_spec();
        bad_ns.namespace = Some("Data".to_string());
        assert!(render_atlas_schema(&bad_ns).is_err());

        // A valid RFC-1123 name still renders.
        let mut ok = secret_spec();
        ok.name = "orders-2".to_string();
        assert!(render_atlas_schema(&ok).is_ok());
    }

    #[test]
    fn incomplete_secret_ref_is_rejected() {
        let mut spec = secret_spec();
        spec.database = AtlasDatabaseUrl::SecretKeyRef {
            name: "orders-db".to_string(),
            key: "  ".to_string(),
        };
        assert!(render_atlas_schema(&spec).is_err());
    }
}
