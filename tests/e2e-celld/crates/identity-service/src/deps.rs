use distributed::microsvc::RepoReadModelDependencies;
use distributed::AggregateRepository;

/// Stores, no aggregate: the commands publish user profiles, which the
/// auth_users projection writes.
pub type AuthDeps<R, S> = RepoReadModelDependencies<AggregateRepository<R, ()>, S>;
