use distributed::microsvc::RepoReadModelDependencies;
use distributed::AggregateRepository;

/// Zitadel ingress/scrape + auth_users projector: stores, no aggregate.
pub type AuthDeps<R, S> = RepoReadModelDependencies<AggregateRepository<R, ()>, S>;
