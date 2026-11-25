use distributed::microsvc::RepoReadModelDependencies;
use distributed::AggregateRepository;

/// Zitadel ingress/scrape + the auth_users projection: stores, no aggregate.
pub type AuthDeps<R, S> = RepoReadModelDependencies<AggregateRepository<R, ()>, S>;
