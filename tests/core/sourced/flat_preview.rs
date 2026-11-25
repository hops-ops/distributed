use distributed::{command::CommandEventSet, Entity};

#[derive(Default)]
struct Review {
    entity: Entity,
    status: String,
}

#[distributed::sourced(entity, events = "ReviewEvent", aggregate_type = "review")]
impl Review {
    pub fn approve_via_helper(&mut self, id: String) -> distributed::SourcedResult {
        self.recursive_helper(id, 2)
    }

    fn recursive_helper(&mut self, id: String, remaining: u8) -> distributed::SourcedResult {
        if remaining > 0 {
            self.recursive_helper(id, remaining - 1)
        } else {
            self.approve(id)
        }
    }

    pub fn conflicting_helpers(&mut self, id: String) -> distributed::SourcedResult {
        self.approve(id.clone())?;
        self.reject(id)
    }

    pub fn forwarded_literal(&mut self, id: String) -> distributed::SourcedResult {
        self.status_helper(id, "approved".into())
    }

    fn status_helper(&mut self, id: String, status: String) -> distributed::SourcedResult {
        self.record_status(id, status, true, 25, None, None)?;
        Ok(())
    }

    pub fn approve_then_leave(
        &mut self,
        id: String,
        user_id: String,
    ) -> distributed::SourcedResult {
        self.approve(id.clone())?;
        self.leave(id, user_id)
    }

    pub fn leave(&mut self, id: String, user_id: String) -> distributed::SourcedResult {
        self.remove_member_decision(id, user_id)
    }

    pub fn remove_member(&mut self, id: String, user_id: String) -> distributed::SourcedResult {
        self.remove_member_decision(id, user_id)
    }

    fn remove_member_decision(
        &mut self,
        id: String,
        user_id: String,
    ) -> distributed::SourcedResult {
        self.record_member_removed(id, user_id)?;
        Ok(())
    }

    #[event("review.member_removed", version = 1, domain = event)]
    fn record_member_removed(&mut self, id: String, user_id: String) {
        self.entity.set_id(id);
        self.status = user_id;
    }

    pub fn approve(&mut self, id: String) -> distributed::SourcedResult {
        self.record_status(
            id,
            "approved".into(),
            true,
            25,
            ::core::option::Option::Some(0.1),
            ::core::option::Option::None,
        )?;
        Ok(())
    }

    pub fn reject(&mut self, id: String) -> distributed::SourcedResult {
        self.record_status(
            id,
            "rejected".to_owned(),
            false,
            0,
            ::std::option::Option::None,
            ::std::option::Option::Some('x'),
        )?;
        Ok(())
    }

    pub fn archive(&mut self, id: String) -> distributed::SourcedResult {
        self.record_archived(id, "archived".into())?;
        Ok(())
    }

    /// One outcome, one event: its constant body field is declared, not inferred.
    #[event("review.archived", version = 1, domain = event, known(status: "archived"))]
    fn record_archived(&mut self, id: String, status: String) {
        self.entity.set_id(id);
        self.status = status;
    }

    #[allow(clippy::too_many_arguments)]
    #[event("review.status_recorded", version = 1, domain = event)]
    fn record_status(
        &mut self,
        id: String,
        status: String,
        approved: bool,
        count: u64,
        score: Option<f32>,
        mark: Option<char>,
    ) {
        self.entity.set_id(id);
        self.status = status;
        let _ = (approved, count, score, mark);
    }
}

#[tokio::test]
async fn shared_recursive_helpers_preserve_contracts_captures_and_replay() {
    use distributed::AggregateBuilder;

    assert_eq!(
        domain_commands::ApproveViaHelper::command_event_set(),
        domain_commands::Approve::command_event_set(),
    );
    assert_eq!(
        domain_commands::Leave::command_event_set(),
        domain_commands::RemoveMember::command_event_set(),
    );
    assert_eq!(
        domain_commands::Leave::command_event_set(),
        <ReviewMemberRemovedDomainEvent as CommandEventSet>::command_event_set(),
    );
    assert_eq!(
        domain_commands::ApproveThenLeave::command_event_set(),
        distributed::events![
            ReviewMemberRemovedDomainEvent,
            ReviewStatusRecordedDomainEvent
        ],
    );
    // Nothing is predicted from what a decision method passes its recorder.
    assert!(domain_commands::Approve::command_event_known_values().is_empty());

    let mut review = Review::default();
    review.approve_via_helper("r1".into()).unwrap();
    review.conflicting_helpers("r1".into()).unwrap();
    review.forwarded_literal("r1".into()).unwrap();
    review
        .approve_then_leave("r1".into(), "member-0".into())
        .unwrap();
    review.leave("r1".into(), "member-1".into()).unwrap();
    review
        .remove_member("r1".into(), "member-2".into())
        .unwrap();
    let body: ReviewMemberRemovedDomainEvent = review
        .entity
        .pending_domain_events()
        .last()
        .unwrap()
        .decode_body()
        .unwrap();
    assert_eq!(body.id, "r1");
    assert_eq!(body.user_id, "member-2");
    let repository = distributed::InMemoryRepository::new().aggregate::<Review>();
    repository
        .publish_events()
        .commit(&mut review)
        .await
        .unwrap();
    let loaded = repository.get("r1").await.unwrap().unwrap();
    assert_eq!(loaded.status, "member-2");
    assert!(loaded.entity.pending_domain_events().is_empty());
}

#[cfg(feature = "graphql")]
mod client_contract {
    use super::*;
    use distributed::command::{typed_command, Eventual, PreparedCommand};
    use distributed::graphql::{
        build_surface, read, surface_for_role, ClientProjectionPreviewSource,
        DistributedClientSurfaceExport, SurfaceOptions,
    };
    use distributed::microsvc::{CausalCommandContext, HandlerError, Routes, Service};
    use distributed::projection::lower::{EventualOnly, ProjectionDescriptor};
    use distributed::{
        AggregateRepository, InMemoryRepository, LocalProjectionMounts, Mutation,
        RelationalReadModel,
    };
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Default, Serialize, Deserialize, distributed::ReadModel)]
    #[readmodel(table = "flat_reviews", primary_key = ["id"])]
    struct FlatReviews {
        id: String,
        status: String,
    }

    #[derive(Clone, Default, Serialize, Deserialize, distributed::ReadModel)]
    #[readmodel(table = "flat_members", primary_key = ["id", "user_id"])]
    struct FlatMembers {
        id: String,
        user_id: String,
    }

    #[derive(Deserialize, distributed::CommandInput)]
    struct Input {
        id: String,
    }
    #[derive(Serialize, distributed::CommandOutput)]
    struct Output {
        id: String,
    }

    #[allow(non_snake_case)]
    fn SaveFlatReview() -> Mutation<()> {
        distributed::mutation_file!("tests/fixtures/flat_review_save.graphql")
    }
    distributed::projection! {
        const REVIEWS: ProjectionDescriptor<EventualOnly> = {
            name: "flat_reviews", version: 1, epoch: "flat-reviews-v1",
            model: FlatReviews, source: aggregate_snapshot,
            on { events: [ReviewStatusRecordedDomainEvent, ReviewArchivedDomainEvent], mutation: SaveFlatReview,
                input: {review: body}, },
        };
    }

    #[test]
    fn mutation_file_and_inline_mutation_compile_to_the_same_program() {
        let inline: Mutation<()> = distributed::mutation! {
            mutation SaveFlatReview {
                upsert_flat_reviews(object: $input.review)
            }
        };
        assert_eq!(
            SaveFlatReview().program().unwrap(),
            inline.program().unwrap()
        );
    }

    #[allow(non_snake_case)]
    fn DeleteFlatMember() -> Mutation<()> {
        distributed::mutation_file!("tests/fixtures/flat_member_delete.graphql")
    }

    distributed::projection! {
        const MEMBERS: ProjectionDescriptor<EventualOnly> = {
            name: "flat_members", version: 1, epoch: "flat-members-v1",
            model: FlatMembers, source: aggregate_snapshot,
            on { events: [ReviewMemberRemovedDomainEvent], mutation: DeleteFlatMember,
                input: {member: body}, },
        };
    }

    distributed::command! {
        name: "review.leave",
        transition: domain_commands::Leave,
        aggregate: Review,
        input: Input,
        outcome: Eventual<Output>,
        shard: |input| input.id.clone(),
        roles: ["user"],
        field: "review_leave",
        authenticated_user_field: (
            ReviewMemberRemovedDomainEvent, ReviewMemberRemovedDomainEvent, "user_id"
        ),
        guard: |ctx| ctx.session().user_id().is_some(),
        handle: metadata_only,
    }

    async fn metadata_only(
        _ctx: &CausalCommandContext<'_, Review>,
        _input: Input,
    ) -> Result<PreparedCommand<Eventual<Output>>, HandlerError> {
        let _ = _input.id;
        panic!("manifest compilation must not execute a command")
    }

    #[test]
    fn authenticated_flat_delete_compiles_from_typed_and_portable_commands() {
        use distributed::graphql::{
            ClientCommandShape, ClientProjectionExpression, ClientProjectionMutationKind,
        };
        use distributed_cli::{
            compile_client, ClientCompileInput, ClientDocument, ClientSurfaceIdentity,
        };

        let mounts = LocalProjectionMounts::builder("members", "events")
            .eventual(MEMBERS)
            .build()
            .unwrap();
        let mut previous = None;
        for portable in [false, true] {
            let routes = Routes::new().with_repo(AggregateRepository::<_, Review>::new(
                InMemoryRepository::new(),
            ));
            let routes = if portable {
                routes.mount(leave())
            } else {
                routes.typed_command(
                    distributed::command::command_transition::<domain_commands::Leave, Input, Eventual<Output>>("review.leave")
                        .roles(["user"])
                        .field_name("review_leave")
                        .authenticated_user_field::<ReviewMemberRemovedDomainEvent, ReviewMemberRemovedDomainEvent>("user_id")
                ).guarded(|ctx| ctx.session().user_id().is_some(), metadata_only)
            };
            let service = Service::new().named("members").routes(routes);
            let surface = build_surface(
                &[FlatMembers::schema().clone()],
                &SurfaceOptions::postgres(),
            )
            .unwrap()
            .with_projectors([mounts.projector(MEMBERS)])
            .unwrap()
            .with_service(&service)
            .unwrap();
            let selected = surface_for_role(
                &surface,
                "user",
                &std::collections::BTreeMap::from([("FlatMembers".into(), read().all_columns())]),
            )
            .unwrap();
            let manifest = DistributedClientSurfaceExport::from_selected("members", selected)
                .unwrap()
                .manifest()
                .unwrap();
            let command = &manifest.commands[0];
            let ClientCommandShape::Object { definition } = &command.input else {
                panic!("input object")
            };
            assert_eq!(
                definition
                    .fields
                    .iter()
                    .map(|field| field.name.as_str())
                    .collect::<Vec<_>>(),
                ["id"]
            );
            let projection = command.extensions.projection.as_ref().unwrap();
            assert_eq!(projection.preview_occurrences.len(), 1);
            let operation = &manifest.projection_programs[0].arms[0].operations[0];
            assert_eq!(operation.kind, ClientProjectionMutationKind::Delete);
            let key = operation
                .key
                .iter()
                .find(|field| field.name == "user_id")
                .unwrap();
            let ClientProjectionExpression::Slot { slot, .. } = &key.expression else {
                panic!("user key slot")
            };
            let value = projection.preview_occurrences[0]
                .values
                .iter()
                .find(|value| &value.slot == slot)
                .unwrap();
            assert_eq!(
                value.source,
                ClientProjectionPreviewSource::TrustedPreset {
                    name: "x-user-id".into(),
                    codec: "string".into()
                }
            );
            assert_eq!(command.extensions.trusted_presets.len(), 1);
            assert_eq!(command.extensions.trusted_presets[0].name, "x-user-id");

            let compiled = compile_client(ClientCompileInput::new(
                manifest.clone(),
                ClientSurfaceIdentity::role("user"),
                vec![ClientDocument::new(
                    "src/routes/members/+page.graphql",
                    "query Members @load { flat_members { id user_id } }",
                )],
            ))
            .unwrap();
            assert_eq!(compiled.operations.len(), 1);
            if let Some(previous) = &previous {
                assert_eq!(
                    &compiled, previous,
                    "typed and portable registration must compile identically"
                );
            }
            previous = Some(compiled);
        }
    }

    #[test]
    fn an_event_mode_known_constant_reaches_projection_slots_without_applies() {
        let mounts = LocalProjectionMounts::builder("reviews", "events")
            .eventual(REVIEWS)
            .build()
            .unwrap();
        let service = Service::new().named("reviews").routes(
            Routes::new()
                .with_repo(AggregateRepository::<_, Review>::new(
                    InMemoryRepository::new(),
                ))
                .typed_command(
                    typed_command::<Input, Eventual<Output>>("review.archive")
                        .emits_events::<domain_commands::Archive>(),
                )
                .handle(metadata_only),
        );
        let surface = build_surface(
            &[FlatReviews::schema().clone()],
            &SurfaceOptions::postgres(),
        )
        .unwrap()
        .with_projectors([mounts.projector(REVIEWS)])
        .unwrap()
        .with_service(&service)
        .unwrap();
        let selected = surface_for_role(
            &surface,
            "anonymous",
            &std::collections::BTreeMap::from([("FlatReviews".into(), read().all_columns())]),
        )
        .unwrap();
        let manifest = DistributedClientSurfaceExport::from_selected("reviews", selected)
            .unwrap()
            .manifest()
            .unwrap();
        let projection = manifest.commands[0].extensions.projection.as_ref().unwrap();
        assert!(projection
            .preview_occurrences
            .iter()
            .any(|occurrence| occurrence.values.iter().any(
                |field| matches!(&field.source, ClientProjectionPreviewSource::Constant { value }
                    if value.as_ref() == distributed::projection::ProjectionValueRef::String("archived"))
            )));
    }

    #[test]
    fn a_declared_event_constant_reaches_generated_projection_slots() {
        let mounts = LocalProjectionMounts::builder("reviews", "events")
            .eventual(REVIEWS)
            .build()
            .unwrap();
        let service = Service::new().named("reviews").routes(
            Routes::new()
                .with_repo(AggregateRepository::<_, Review>::new(
                    InMemoryRepository::new(),
                ))
                .typed_command(
                    typed_command::<Input, Eventual<Output>>("review.approve")
                        .emits_events::<domain_commands::Approve>()
                        .applies(distributed::event_preview! {
                            ReviewStatusRecordedDomainEvent => ReviewStatusRecordedDomainEvent {
                                id: input.id,
                                status: "approved",
                                ..unknown
                            }
                        }),
                )
                .handle(metadata_only),
        );
        let surface = build_surface(
            &[FlatReviews::schema().clone()],
            &SurfaceOptions::postgres(),
        )
        .unwrap()
        .with_projectors([mounts.projector(REVIEWS)])
        .unwrap()
        .with_service(&service)
        .unwrap();
        let selected = surface_for_role(
            &surface,
            "anonymous",
            &std::collections::BTreeMap::from([("FlatReviews".into(), read().all_columns())]),
        )
        .unwrap();
        let manifest = DistributedClientSurfaceExport::from_selected("reviews", selected)
            .unwrap()
            .manifest()
            .unwrap();
        let projection = manifest.commands[0].extensions.projection.as_ref().unwrap();
        assert_eq!(projection.preview_occurrences.len(), 1);
        assert!(projection.preview_occurrences[0].values.iter().any(
            |field| matches!(&field.source, ClientProjectionPreviewSource::Constant { value }
                    if value.as_ref() == distributed::projection::ProjectionValueRef::String("approved"))
        ));
        assert!(projection.preview_occurrences[0].values.iter().any(|field|
            matches!(&field.source, ClientProjectionPreviewSource::Input { path } if path == &["id"])));
    }
}
