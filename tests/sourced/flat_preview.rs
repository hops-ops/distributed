use distributed::{command::CommandEventSet, Entity};
use serde_json::{json, Value};

#[derive(Default)]
struct Review {
    entity: Entity,
    status: String,
}

#[distributed::sourced(entity, events = "ReviewEvent", aggregate_type = "review")]
impl Review {
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

    async fn metadata_only(
        _ctx: &CausalCommandContext<'_, Review>,
        _input: Input,
    ) -> Result<PreparedCommand<Eventual<Output>>, HandlerError> {
        let _ = _input.id;
        panic!("manifest compilation must not execute a command")
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
