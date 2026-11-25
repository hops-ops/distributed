#[path = "../persistent_repository_conformance/mod.rs"]
mod conformance;

use distributed::InMemoryRepository;

async fn repository() -> Option<((), InMemoryRepository)> {
    Some(((), InMemoryRepository::new()))
}

conformance::repository_conformance_tests!();
