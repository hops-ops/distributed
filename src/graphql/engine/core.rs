use super::*;

#[derive(Clone)]
pub enum GraphqlPool {
    #[cfg(feature = "postgres")]
    Postgres(sqlx::PgPool),
    #[cfg(feature = "sqlite")]
    Sqlite(sqlx::SqlitePool),
}

#[cfg(feature = "postgres")]
impl From<sqlx::PgPool> for GraphqlPool {
    fn from(pool: sqlx::PgPool) -> Self {
        GraphqlPool::Postgres(pool)
    }
}

#[cfg(feature = "sqlite")]
impl From<sqlx::SqlitePool> for GraphqlPool {
    fn from(pool: sqlx::SqlitePool) -> Self {
        GraphqlPool::Sqlite(pool)
    }
}

/// Database source for a GraphQL engine.
///
/// Passing a Distributed repository handle instead of its raw pool preserves
/// the opaque storage identity required to prove that `Projected` commands
/// update the same database read by GraphQL.
#[derive(Clone)]
pub struct GraphqlPoolSource {
    pub(crate) pool: GraphqlPool,
    pub(crate) causal_storage_identity: Option<crate::command_ledger::CausalStorageIdentity>,
}

impl From<GraphqlPool> for GraphqlPoolSource {
    fn from(pool: GraphqlPool) -> Self {
        Self {
            pool,
            causal_storage_identity: None,
        }
    }
}

#[cfg(feature = "postgres")]
impl From<sqlx::PgPool> for GraphqlPoolSource {
    fn from(pool: sqlx::PgPool) -> Self {
        GraphqlPool::from(pool).into()
    }
}

#[cfg(feature = "sqlite")]
impl From<sqlx::SqlitePool> for GraphqlPoolSource {
    fn from(pool: sqlx::SqlitePool) -> Self {
        GraphqlPool::from(pool).into()
    }
}

#[cfg(feature = "postgres")]
impl From<&crate::PostgresRepository> for GraphqlPoolSource {
    fn from(repository: &crate::PostgresRepository) -> Self {
        Self {
            pool: GraphqlPool::Postgres(repository.pool().clone()),
            causal_storage_identity: Some(repository.causal_storage_identity()),
        }
    }
}

#[cfg(feature = "sqlite")]
impl From<&crate::SqliteRepository> for GraphqlPoolSource {
    fn from(repository: &crate::SqliteRepository) -> Self {
        Self {
            pool: GraphqlPool::Sqlite(repository.pool().clone()),
            causal_storage_identity: Some(repository.causal_storage_identity()),
        }
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod graphql_pool_source_identity_tests {
    use super::GraphqlPoolSource;

    fn pool() -> sqlx::SqlitePool {
        sqlx::sqlite::SqlitePoolOptions::new()
            .connect_lazy("sqlite::memory:")
            .expect("lazy SQLite pool")
    }

    #[tokio::test]
    async fn raw_pool_has_no_causal_storage_identity() {
        let source = GraphqlPoolSource::from(pool());

        assert!(source.causal_storage_identity.is_none());
    }

    #[tokio::test]
    async fn repository_reference_carries_its_opaque_storage_identity() {
        let repository = crate::SqliteRepository::new(pool());
        let source = GraphqlPoolSource::from(&repository);

        assert_eq!(
            source.causal_storage_identity,
            Some(repository.causal_storage_identity())
        );
    }

    #[tokio::test]
    async fn repository_and_pool_source_clones_preserve_storage_identity() {
        let repository = crate::SqliteRepository::new(pool());
        let repository_clone = repository.clone();
        let source = GraphqlPoolSource::from(&repository);
        let source_clone = source.clone();

        assert_eq!(
            GraphqlPoolSource::from(&repository_clone).causal_storage_identity,
            source.causal_storage_identity
        );
        assert_eq!(
            source_clone.causal_storage_identity,
            source.causal_storage_identity
        );
    }

    #[tokio::test]
    async fn independent_repositories_over_the_same_pool_have_distinct_identities() {
        let pool = pool();
        let first = crate::SqliteRepository::new(pool.clone());
        let second = crate::SqliteRepository::new(pool);

        assert_ne!(
            GraphqlPoolSource::from(&first).causal_storage_identity,
            GraphqlPoolSource::from(&second).causal_storage_identity
        );
    }
}

#[derive(Debug)]
pub struct GraphqlBuildError(pub String);

impl std::fmt::Display for GraphqlBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for GraphqlBuildError {}

impl From<String> for GraphqlBuildError {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for GraphqlBuildError {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

#[derive(Clone)]
pub(crate) struct CatalogEntry {
    pub schema: TableSchema,
    pub exposed: bool,
}

#[derive(Clone)]
pub(crate) struct RoleModelPerm {
    pub permission: ReadPermission,
}

#[derive(Clone)]
pub(crate) struct ProtocolSurfaceInfo {
    pub(crate) schema_fingerprint: String,
    pub(crate) protocol_fingerprint: String,
    pub(crate) trusted_presets: Vec<ClientTrustedPresetDescriptor>,
}

#[derive(Clone)]
pub(crate) struct ProtocolRoleInfo {
    pub(crate) surface: ProtocolSurfaceInfo,
    pub(crate) authorization_fingerprint: String,
    pub(crate) claim_keys: Vec<String>,
}

#[derive(Clone)]
pub(crate) struct ProtocolApplicationInfo {
    /// Eligible wire roles (who may open).
    pub(crate) eligible_roles: Vec<String>,
    /// Privilege packs for portable schema + server execution grants.
    #[allow(dead_code)] // inspected by tests / future multi-privilege diagnostics
    pub(crate) schema_roles: Vec<String>,
    /// Privilege role key used for SQL grants / prebuilt schema lookup.
    /// Single-privilege apps use that role name; multi-privilege apps use
    /// `app:{name}` with synthetic grants.
    pub(crate) privilege_key: String,
    pub(crate) surface: ProtocolSurfaceInfo,
    pub(crate) authorization_fingerprint: String,
    pub(crate) claim_keys: Vec<String>,
}

/// Request-scoped execution authority (surface privilege, not identity primary).
///
/// Injected into GraphQL request data so compile/live paths authorize with the
/// opened surface's privilege pack rather than a session primary role.
#[derive(Clone, Debug)]
pub struct ExecutionAuthority {
    /// Role pack key for `permissions` / `schemas` / `role_surfaces`.
    pub privilege_role: String,
    /// Asserted identity set (`x-roles`).
    pub asserted_roles: Vec<String>,
    /// Named client surface when the request opened one via protocol extensions.
    pub surface: ClientSurfaceIdentity,
}

#[derive(Clone)]
pub(crate) struct ProtocolRuntime {
    pub(crate) codec: ProtocolTokenCodec,
    pub(crate) projection_programs:
        Arc<crate::graphql::projection_delta::runtime::ProtocolProjectionProgramRegistry>,
    pub(crate) namespace: String,
    pub(crate) service_id: String,
    pub(crate) roles: BTreeMap<String, ProtocolRoleInfo>,
    pub(crate) applications: BTreeMap<String, ProtocolApplicationInfo>,
}

pub(crate) struct EngineInner {
    /// Stable service identity used by client manifest hashes and cache scopes.
    /// Manifest-built engines populate this automatically; manual builders may
    /// opt in with [`GraphqlEngineBuilder::service_id`].
    pub service_id: Option<String>,
    pub command_binding: Option<TypedServiceCommandBinding>,
    pub causal_storage_identity: Option<crate::command_ledger::CausalStorageIdentity>,
    pub pool: GraphqlPool,
    #[cfg(feature = "gateway-delivery")]
    pub read_routing: Option<super::ReadRouting>,
    #[cfg(feature = "gateway-delivery")]
    pub gateway_versions: Option<crate::graphql::delivery::GatewayVersionStore>,
    pub catalog: BTreeMap<String, CatalogEntry>,
    pub by_table: BTreeMap<String, String>,
    pub permissions: BTreeMap<(String, String), RoleModelPerm>,
    pub roles: BTreeSet<String>,
    pub anonymous_role: String,
    pub default_limit: u64,
    pub max_limit: u64,
    pub max_depth: usize,
    #[allow(dead_code)]
    pub max_complexity: usize,
    pub max_in_list: usize,
    /// Max length of a single `_and` / `_or` list in client `where` (breadth DoS).
    pub max_bool_width: usize,
    /// When true (default), unknown/ungranted client `where` and `order_by`
    /// keys fail the request instead of soft-skipping.
    pub strict_where: bool,
    #[allow(dead_code)]
    pub introspection_for_anonymous: bool,
    pub statement_timeout: Duration,
    pub graphiql: bool,
    pub(crate) typed_commands: TypedCommandInventory,
    pub role_surfaces: BTreeMap<String, Arc<Surface>>,
    pub application_surfaces: BTreeMap<String, Arc<Surface>>,
    pub schemas: HashMap<String, async_graphql::dynamic::Schema>,
    /// Relaxed schemas selected only for pure introspection while GraphiQL is
    /// enabled. Application operations always use `schemas` and its exact
    /// manifest-fingerprinted execution limits.
    pub graphiql_schemas: HashMap<String, async_graphql::dynamic::Schema>,
    pub change_hub: crate::graphql::subscribe::ChangeHub,
    pub dialect: SqlDialect,
    /// Identity mode for HTTP session construction (see `identity` module).
    pub identity: IdentityConfig,
    pub(crate) identity_validator: Option<OidcValidator>,
    pub(crate) protocol: Option<ProtocolRuntime>,
    pub(crate) query_protocol: QueryProtocolRuntime,
    pub(crate) read_stores: BTreeMap<String, crate::graphql::read_store::ReadStoreKind>,
    pub(crate) cell_getters:
        BTreeMap<String, std::sync::Arc<dyn crate::graphql::read_store::CellByKeyGetter>>,
}

pub struct GraphqlEngine {
    pub(crate) inner: Arc<EngineInner>,
}

/// Registration for one named application surface.
///
/// `eligible_roles` is the wire/protocol role list (who may open the contract).
/// `schema_roles` is the grant-intersection set used to build the portable
/// client schema; it must be a non-empty subset of `eligible_roles`.
#[derive(Clone, Debug)]
pub(crate) struct ClientApplicationRegistration {
    pub(crate) eligible_roles: Vec<String>,
    pub(crate) schema_roles: Vec<String>,
}

pub struct GraphqlEngineBuilder {
    pub(crate) service_id: Option<String>,
    pub(crate) protocol_token_key: Option<[u8; 32]>,
    pub(crate) protocol_namespace: Option<String>,
    pub(crate) client_applications: BTreeMap<String, ClientApplicationRegistration>,
    pub(crate) command_binding: Option<TypedServiceCommandBinding>,
    pub(crate) causal_storage_identity: Option<crate::command_ledger::CausalStorageIdentity>,
    pub(crate) pool: GraphqlPool,
    #[cfg(feature = "gateway-delivery")]
    pub(crate) read_routing: Option<super::ReadRouting>,
    #[cfg(feature = "gateway-delivery")]
    pub(crate) gateway_versions: Option<crate::graphql::delivery::GatewayVersionStore>,
    pub(crate) catalog: BTreeMap<String, CatalogEntry>,
    pub(crate) by_table: BTreeMap<String, String>,
    pub(crate) permissions: BTreeMap<(String, String), RoleModelPerm>,
    pub(crate) roles: Option<BTreeSet<String>>,
    pub(crate) anonymous_role: String,
    pub(crate) default_limit: u64,
    pub(crate) max_limit: u64,
    pub(crate) max_depth: usize,
    pub(crate) max_complexity: usize,
    pub(crate) max_in_list: usize,
    pub(crate) max_bool_width: usize,
    pub(crate) strict_where: bool,
    pub(crate) introspection_for_anonymous: bool,
    pub(crate) statement_timeout: Duration,
    pub(crate) graphiql: bool,
    pub(crate) subscriptions: bool,
    pub(crate) typed_commands: TypedCommandInventory,
    pub(crate) projectors: Vec<SurfaceProjectionOwner>,
    pub(crate) change_rx: Option<tokio::sync::broadcast::Receiver<ReadModelChange>>,
    pub(crate) pending_errors: Vec<String>,
    pub(crate) identity: IdentityConfig,
    pub(crate) read_stores: BTreeMap<String, crate::graphql::read_store::ReadStore>,
}
