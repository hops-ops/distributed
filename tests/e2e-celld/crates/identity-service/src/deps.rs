use distributed::microsvc::RepoReadModelDependencies;
use distributed::AggregateRepository;

/// Stores, no aggregate: the commands publish provider messages only.
pub type AuthDeps<R, S> = RepoReadModelDependencies<AggregateRepository<R, ()>, S>;
