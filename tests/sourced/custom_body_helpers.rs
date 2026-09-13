use distributed::{command::CommandEventSet, AggregateBuilder, DomainEvent};

mod facts {
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, distributed::DomainEvent)]
    #[domain_event(name = "repository.branch_created", version = 2)]
    pub struct BranchCreated {
        pub repository_id: String,
        pub name: String,
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, distributed::DomainEvent)]
    #[domain_event(name = "repository.tag_created", version = 1)]
    pub struct TagCreated {
        pub repository_id: String,
        pub name: String,
    }
}

mod repository {
    use std::cell::Cell;

    #[derive(Default)]
    pub struct Repository {
        pub entity: distributed::Entity,
        pub branch: String,
        pub tag: String,
        pub adapter_calls: Cell<usize>,
    }

    #[distributed::sourced(entity, events = "RepositoryEvent", aggregate_type = "repository")]
    impl Repository {
        pub fn create(&mut self, id: String) -> distributed::SourcedResult {
            self.create_refs(id)
        }

        fn create_refs(&mut self, id: String) -> distributed::SourcedResult {
            self.create_branch(id.clone())?;
            self.record_branch(id, "main".into())?;
            self.record_tag("v1".into())?;
            Ok(())
        }

        fn create_branch(&mut self, id: String) -> distributed::SourcedResult {
            self.record_branch(id, "initial".into())?;
            Ok(())
        }

        #[event(
            "repository.branch_created",
            version = 2,
            domain = with(crate::custom_body_helpers::facts::BranchCreated, branch_created)
        )]
        fn record_branch(&mut self, id: String, name: String) {
            self.entity.set_id(id);
            self.branch = name;
        }

        #[event(
            "repository.tag_created",
            version = 1,
            domain = with(super::facts::TagCreated, tag_created)
        )]
        fn record_tag(&mut self, name: String) {
            self.tag = name;
        }
    }

    fn branch_created(
        repository: &Repository,
        _event: &RepositoryEvent,
    ) -> super::facts::BranchCreated {
        repository
            .adapter_calls
            .set(repository.adapter_calls.get() + 1);
        super::facts::BranchCreated {
            repository_id: repository.entity.id().to_owned(),
            name: repository.branch.clone(),
        }
    }

    fn tag_created(repository: &Repository, _event: &RepositoryEvent) -> super::facts::TagCreated {
        repository
            .adapter_calls
            .set(repository.adapter_calls.get() + 1);
        super::facts::TagCreated {
            repository_id: repository.entity.id().to_owned(),
            name: repository.tag.clone(),
        }
    }
}

#[tokio::test]
async fn helper_custom_bodies_preserve_paths_descriptors_and_replay() {
    // Neither RepositoryBranchCreatedDomainEvent nor RepositoryTagCreatedDomainEvent
    // exists: custom bodies, not naming-convention aliases, own these contracts.
    assert_eq!(
        repository::domain_commands::Create::command_event_set(),
        distributed::events![facts::BranchCreated, facts::TagCreated],
    );
    // Adapter output cannot be inferred from recorder arguments, even literals.
    assert!(repository::domain_commands::Create::command_event_known_values().is_empty());

    let mut aggregate = repository::Repository::default();
    aggregate.create("repo-1".into()).unwrap();
    let events = aggregate.entity.pending_domain_events();
    assert_eq!(events.len(), 3);
    assert_eq!(aggregate.adapter_calls.get(), 3);
    assert_eq!(events[0].descriptor(), &facts::BranchCreated::DESCRIPTOR);
    assert_eq!(events[1].descriptor(), &facts::BranchCreated::DESCRIPTOR);
    assert_eq!(events[2].descriptor(), &facts::TagCreated::DESCRIPTOR);
    let branch: facts::BranchCreated = events[1].decode_body().unwrap();
    assert_eq!(branch.repository_id, "repo-1");
    assert_eq!(branch.name, "main");
    let tag: facts::TagCreated = events[2].decode_body().unwrap();
    assert_eq!(tag.repository_id, "repo-1");
    assert_eq!(tag.name, "v1");

    let store = distributed::InMemoryRepository::new().aggregate::<repository::Repository>();
    store.commit(&mut aggregate).await.unwrap();
    let loaded = store.get("repo-1").await.unwrap().unwrap();
    assert_eq!(loaded.branch, "main");
    assert_eq!(loaded.tag, "v1");
    assert_eq!(
        loaded.adapter_calls.get(),
        0,
        "replay must not invoke adapters"
    );
    assert!(loaded.entity.pending_domain_events().is_empty());
}
