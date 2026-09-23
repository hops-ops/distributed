use distributed::{command::CommandEventSet, Entity};
use serde_json::{json, Value};

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

    pub fn conflicting(&mut self, id: String, approve: bool) -> distributed::SourcedResult {
        if approve {
            self.record_status(id, "approved".into(), true, 1, None, None)?;
        } else {
            self.record_status(id, "rejected".into(), false, 1, None, None)?;
        }
        Ok(())
    }

    pub fn dynamic(&mut self, id: String, status: String) -> distributed::SourcedResult {
        self.record_status(id.clone(), "approved".into(), true, 1, None, None)?;
        self.record_status(id, status, false, 1, None, None)?;
        Ok(())
    }

    pub fn executable(&mut self, id: String) -> distributed::SourcedResult {
        self.record_status(id, never_execute(), true, 1, None, None)?;
        Ok(())
    }

    pub fn shadowed_constructor(&mut self, id: String) -> distributed::SourcedResult {
        #[allow(non_snake_case)]
        fn Some(_score: f32) -> Option<f32> {
            panic!("not an Option constructor")
        }
        self.record_status(id, "approved".into(), true, 1, Some(0.1), None)?;
        Ok(())
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

fn never_execute() -> String {
    panic!("building metadata must not execute transition expressions")
}

fn fields<T: CommandEventSet>() -> Value {
    let previews = serde_json::to_value(T::command_event_known_values()).unwrap();
    let mut fields = serde_json::Map::new();
    for preview in previews.as_array().unwrap() {
        for field in preview["fields"].as_array().unwrap() {
            fields.insert(
                field["body_path"][0].as_str().unwrap().into(),
                field["source"].clone(),
            );
        }
    }
    Value::Object(fields)
}

#[tokio::test]
async fn shared_recursive_helpers_preserve_contracts_captures_and_replay() {
    use distributed::AggregateBuilder;

    assert_eq!(
        domain_commands::ApproveViaHelper::command_event_set(),
        domain_commands::Approve::command_event_set(),
    );
    assert_eq!(
        fields::<domain_commands::ApproveViaHelper>(),
        fields::<domain_commands::Approve>(),
    );
    assert!(fields::<domain_commands::ConflictingHelpers>()
        .get("status")
        .is_none());
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
    assert!(fields::<domain_commands::ForwardedLiteral>()
        .get("status")
        .is_none());
    assert!(domain_commands::Leave::command_event_known_values().is_empty());

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
    repository.commit(&mut review).await.unwrap();
    let loaded = repository.get("r1").await.unwrap().unwrap();
    assert_eq!(loaded.status, "member-2");
    assert!(loaded.entity.pending_domain_events().is_empty());
}

#[test]
fn flat_constants_match_recorded_body_without_running_the_command() {
    let values = fields::<domain_commands::Approve>();
    assert_eq!(
        values["status"],
        json!({"kind":"constant", "value":{"type":"string","value":"approved"}})
    );
    assert_eq!(values["approved"]["value"]["value"], true);
    assert_eq!(values["count"]["value"]["value"], "25");
    assert!(
        values.get("id").is_none(),
        "server-computed/input identity is not a constant"
    );
    let mut review = Review::default();
    review.approve("r1".into()).unwrap();
    let body: Value = review.entity.pending_domain_events()[0]
        .decode_body()
        .unwrap();
    for (name, source) in values.as_object().unwrap() {
        if source["kind"] == "constant" {
            let typed = &source["value"];
            let value = if matches!(typed["type"].as_str(), Some("u64" | "i64" | "f64")) {
                serde_json::from_str(typed["value"].as_str().unwrap()).unwrap()
            } else {
                typed["value"].clone()
            };
            assert_eq!(body[name], value, "wire value for {name}");
        } else {
            assert_eq!(source["kind"], "null");
            assert!(body[name].is_null());
        }
    }
    let rejected = fields::<domain_commands::Reject>();
    assert_eq!(rejected["status"]["value"]["value"], "rejected");
    assert_eq!(rejected["mark"]["value"]["value"], "x");
}

#[test]
fn conflicting_dynamic_and_executable_values_are_not_constants() {
    for values in [
        fields::<domain_commands::Conflicting>(),
        fields::<domain_commands::Dynamic>(),
    ] {
        assert!(values.get("status").is_none());
        assert!(values.get("approved").is_none());
        assert_eq!(values["count"]["value"]["value"], "1");
    }
    assert!(fields::<domain_commands::Executable>()
        .get("status")
        .is_none());
    let _never_call: fn(&mut Review, String) -> distributed::SourcedResult = Review::executable;
    assert!(fields::<domain_commands::ShadowedConstructor>()
        .get("score")
        .is_none());
    let _shadowed: fn(&mut Review, String) -> distributed::SourcedResult =
        Review::shadowed_constructor;
    // Exercise the commands too: inference does not change their real behavior.
    let mut review = Review::default();
    review.conflicting("r1".into(), false).unwrap();
    review.dynamic("r1".into(), "custom".into()).unwrap();
    review.reject("r1".into()).unwrap();
    assert_eq!(review.status, "rejected");
}

#[cfg(feature = "graphql")]
mod client_contract {
    use super::*;
    use distributed::command::{typed_command, Eventual, PreparedCommand};
    use distributed::graphql::{
        build_surface, surface_for_role, ClientProjectionPreviewSource, ClientProjectionValue,
        DistributedClientSurfaceExport, RoleGrant, SurfaceOptions,
    };
    use distributed::microsvc::{CausalCommandContext, HandlerError, Routes, Service};
    use distributed::projection::lower::{EventualOnly, ProjectionDescriptor};
    use distributed::{
        AggregateRepository, InMemoryRepository, LocalProjectionMountsBuilder, Mutation,
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
            on { events: [ReviewStatusRecordedDomainEvent], mutation: SaveFlatReview,
                input: {review: body}, },
        };
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

    distributed::portable_command! {
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
            compile_client, ClientCompileInput, ClientDocument, ClientSurfaceSelector,
        };

        let mounts = LocalProjectionMountsBuilder::new("members", "events")
            .unwrap()
            .eventual_model::<FlatMembers, _>("flat_members", MEMBERS, MEMBERS.epoch())
            .unwrap()
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
            .with_projectors([mounts.projector("flat_members").unwrap()])
            .unwrap()
            .with_service(&service)
            .unwrap();
            let selected = surface_for_role(
                &surface,
                "user",
                &std::collections::BTreeMap::from([(
                    "FlatMembers".into(),
                    RoleGrant::all_columns(),
                )]),
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
                serde_json::to_value(&manifest).unwrap(),
                ClientSurfaceSelector::role("user"),
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
    fn flat_domain_constant_reaches_generated_projection_slots() {
        let mounts = LocalProjectionMountsBuilder::new("reviews", "events")
            .unwrap()
            .eventual_model::<FlatReviews, _>("flat_reviews", REVIEWS, REVIEWS.epoch())
            .unwrap()
            .build()
            .unwrap();
        let service = Service::new().named("reviews").routes(
            Routes::new()
                .with_repo(AggregateRepository::<_, Review>::new(
                    InMemoryRepository::new(),
                ))
                .typed_command(
                    typed_command::<Input, Eventual<Output>>("review.approve")
                        .emits_events::<domain_commands::Approve>(),
                )
                .handle(metadata_only),
        );
        let surface = build_surface(
            &[FlatReviews::schema().clone()],
            &SurfaceOptions::postgres(),
        )
        .unwrap()
        .with_projectors([mounts.projector("flat_reviews").unwrap()])
        .unwrap()
        .with_service(&service)
        .unwrap();
        let selected = surface_for_role(
            &surface,
            "anonymous",
            &std::collections::BTreeMap::from([("FlatReviews".into(), RoleGrant::all_columns())]),
        )
        .unwrap();
        let manifest = DistributedClientSurfaceExport::from_selected("reviews", selected)
            .unwrap()
            .manifest()
            .unwrap();
        let projection = manifest.commands[0].extensions.projection.as_ref().unwrap();
        assert_eq!(projection.preview_occurrences.len(), 1);
        assert!(projection.preview_occurrences[0].values.iter().any(
            |field| matches!(&field.source, ClientProjectionPreviewSource::Constant {
                value: ClientProjectionValue::String(status) } if status == "approved")
        ));
        assert!(projection.preview_occurrences[0].values.iter().any(|field|
            matches!(&field.source, ClientProjectionPreviewSource::Input { path } if path == &["id"])));
    }
}
