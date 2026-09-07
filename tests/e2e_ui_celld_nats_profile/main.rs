//! Optional celld+NATS e2e-ui profile.
//!
//! Fixture checks always run (default host stays one-process). Live smoke
//! runs only when `CELLD_URL` and `NATS_URL` are set.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

#[path = "../support/env.rs"]
mod env_support;

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn default_e2e_ui_host_stays_one_process() {
    let host = std::fs::read_to_string(repo_root().join("tests/e2e-ui/crates/service/src/host.rs"))
        .expect("host.rs");
    assert!(
        host.contains("single backend process"),
        "default host.rs must remain the one-process playground"
    );
    assert!(
        host.contains("celld-nats-profile"),
        "host.rs should point at the optional profile, not implement it"
    );
    assert!(
        !host.contains("NatsBus"),
        "optional NATS profile must not replace SqliteBus/PostgresBus in host.rs"
    );
}

#[test]
fn optional_profile_is_named_and_not_the_playground() {
    let readme =
        std::fs::read_to_string(repo_root().join("tests/e2e-ui/celld-nats-profile/README.md"))
            .expect("profile README");
    assert!(readme.contains("optional"), "{readme}");
    assert!(readme.contains("make run"), "{readme}");
    assert!(readme.contains("make up-celld-nats"), "{readme}");
    assert!(readme.contains("make test-celld-nats"), "{readme}");
    assert!(readme.contains("CELLD_URL"), "{readme}");
    assert!(readme.contains("NATS_URL"), "{readme}");
    assert!(
        readme.contains("not") && readme.contains("cell class"),
        "projectors must stay off cells"
    );

    let compose = std::fs::read_to_string(
        repo_root().join("tests/e2e-ui/celld-nats-profile/docker-compose.yml"),
    )
    .expect("profile compose");
    assert!(compose.contains("e2e-ui-celld-nats-optional"), "{compose}");
    assert!(compose.contains("nats:2-alpine"), "{compose}");
    assert!(
        !compose
            .lines()
            .any(|line| line.trim_start().starts_with("image:") && line.contains("minio")),
        "do not run MinIO"
    );

    let worker = std::fs::read_to_string(repo_root().join("tests/celld/worker/src/lib.rs"))
        .expect("todo cell worker");
    assert!(
        worker.contains("projectors are not methods on this class"),
        "cells stay command-only"
    );
}

#[cfg(all(feature = "graphql", feature = "http", feature = "sqlite"))]
mod live {
    use super::*;
    use async_graphql::Request;
    use distributed::cell_host::{
        CelldCommandHost, CelldRoute, InternalHttpSecret, CELL_INTERNAL_SECRET_ENV,
        CELL_INTERNAL_SECRET_HEADER,
    };
    use distributed::command::{
        typed_command, CommandInputType, CommandOutputType, CommandTypeDef, CommandTypeField,
        Succeeded,
    };
    use distributed::command_dispatch::SharedCommandHost;
    use distributed::graphql::{read, GraphqlEngine, ModelPermissions, VerifiedPrincipal};
    use distributed::microsvc::{Session, ROLE_KEY, USER_ID_KEY};
    use distributed::{
        Aggregate, AggregateBuilder, Entity, InMemoryRepository, ReadModel, Snapshot,
    };
    use serde::{Deserialize, Serialize};
    use serde_json::{json, Value};

    const OPTIONAL_TODO_COMMANDS: &[&str] = &["todo.create"];

    fn optional_todo_shard(input: &Value) -> Option<String> {
        input.get("id").and_then(Value::as_str).map(str::to_owned)
    }

    fn optional_todo_payload(
        _command: &str,
        input: &Value,
        remote: &Value,
        _session: &Session,
    ) -> Value {
        json!({
            "id": remote
                .get("id")
                .or_else(|| input.get("id"))
                .cloned()
                .unwrap_or(Value::Null)
        })
    }

    #[derive(Default, Snapshot)]
    struct SchemaAgg {
        entity: Entity,
    }

    impl SchemaAgg {
        fn record(&mut self, id: String) -> distributed::SourcedResult {
            self.entity.set_id(id);
            self.entity.digest_empty("todo.recorded")
        }
    }

    impl Aggregate for SchemaAgg {
        type ReplayError = std::convert::Infallible;
        fn aggregate_type() -> &'static str {
            "optional-profile-todo"
        }
        fn entity(&self) -> &Entity {
            &self.entity
        }
        fn entity_mut(&mut self) -> &mut Entity {
            &mut self.entity
        }
        fn replay_event(
            &mut self,
            _event: &distributed::EventRecord,
        ) -> Result<(), Self::ReplayError> {
            Ok(())
        }
    }

    #[derive(Clone, Deserialize, Serialize, ReadModel)]
    #[readmodel(primary_key = ["id"])]
    struct Todos {
        #[readmodel(id)]
        id: String,
        title: String,
    }

    #[derive(Deserialize)]
    #[allow(dead_code)]
    struct CreateInput {
        id: String,
        title: String,
    }

    impl CommandInputType for CreateInput {
        fn command_type() -> CommandTypeDef {
            CommandTypeDef::new(
                "CreateInput",
                vec![
                    CommandTypeField {
                        name: "id".into(),
                        type_name: "String".into(),
                        nullable: false,
                        list: false,
                        item_nullable: false,
                        nested: None,
                    },
                    CommandTypeField {
                        name: "title".into(),
                        type_name: "String".into(),
                        nullable: false,
                        list: false,
                        item_nullable: false,
                        nested: None,
                    },
                ],
            )
            .with_type_id(std::any::TypeId::of::<Self>())
        }
    }

    #[derive(Serialize)]
    struct IdPayload {
        id: String,
    }

    impl CommandOutputType for IdPayload {
        fn command_type() -> CommandTypeDef {
            CommandTypeDef::new(
                "IdPayload",
                vec![CommandTypeField {
                    name: "id".into(),
                    type_name: "String".into(),
                    nullable: false,
                    list: false,
                    item_nullable: false,
                    nested: None,
                }],
            )
            .with_type_id(std::any::TypeId::of::<Self>())
        }
    }

    fn schema_service() -> distributed::microsvc::Service {
        distributed::microsvc::Service::new()
            .named("optional-profile")
            .routes(
                distributed::microsvc::Routes::new()
                    .with_repo(InMemoryRepository::new().aggregate::<SchemaAgg>())
                    .typed_command(
                        typed_command::<CreateInput, Succeeded<IdPayload>>("todo.create")
                            .roles(["user"]),
                    )
                    .create()
                    .invoke(|aggregate, input, _owner| {
                        aggregate.record(input.id.clone())?;
                        Ok::<_, distributed::EventRecordError>(())
                    })
                    .succeeded(|aggregate| IdPayload {
                        id: aggregate.entity().id().to_string(),
                    }),
            )
    }

    #[tokio::test]
    async fn optional_profile_smoke_graphql_wait_path_and_sql_list() {
        let Some(celld) = env_support::broker_env("CELLD_URL", "optional celld+NATS smoke") else {
            return;
        };
        let Some(nats) = env_support::broker_env("NATS_URL", "optional celld+NATS smoke") else {
            return;
        };

        let nats_addr = nats
            .trim()
            .trim_start_matches("nats://")
            .split('/')
            .next()
            .unwrap_or(nats.trim());
        let (nats_host, nats_port) = nats_addr.split_once(':').unwrap_or((nats_addr, "4222"));
        let _ = tokio::net::TcpStream::connect((nats_host, nats_port.parse::<u16>().unwrap()))
            .await
            .expect("NATS TCP");

        let todo_id = format!(
            "dcs6-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let celld = celld.trim_end_matches('/');

        let pool = sqlx::SqlitePool::connect_lazy("sqlite::memory:").unwrap();
        sqlx::query("CREATE TABLE IF NOT EXISTS todos (id TEXT PRIMARY KEY, title TEXT)")
            .execute(&pool)
            .await
            .ok();
        let schema = Arc::new(schema_service());
        let secret = InternalHttpSecret::new(
            std::env::var(CELL_INTERNAL_SECRET_ENV)
                .expect("live celld profile requires DISTRIBUTED_INTERNAL_SECRET"),
        )
        .expect("valid internal secret");
        let read_secret = secret.clone();
        let host: SharedCommandHost = Arc::new(
            CelldCommandHost::new(celld, Arc::clone(&schema), secret)
                .expect("valid celld host")
                .route(CelldRoute::new(
                    OPTIONAL_TODO_COMMANDS,
                    "todo",
                    optional_todo_shard,
                    optional_todo_payload,
                )),
        );
        let engine = GraphqlEngine::builder(pool)
            .protocol_token_key([0x5a; 32])
            .roles(&["user"])
            .model::<Todos>(ModelPermissions::new().grant("user", read().all_columns()))
            .service(schema.as_ref())
            .build()
            .expect("optional-profile GraphQL engine");

        let mut session = Session::new();
        session.set(USER_ID_KEY, "alice");
        session.set(ROLE_KEY, "user");
        let principal = VerifiedPrincipal::from_trusted_transport("alice");
        let command_id = "0190a000-0000-7000-8000-000000000310";
        let mutation = engine
            .execute(
                &session,
                Request::new(format!(
                    r#"mutation {{ todo_create(commandId: "{command_id}", input: {{ id: "{todo_id}", title: "dcs6" }}) {{ id }} }}"#
                ))
                .data(Arc::clone(&host))
                .data(principal),
            )
            .await;
        assert!(
            mutation.errors.is_empty(),
            "GraphQL wait-path to cell: {mutation:?}"
        );

        let list = engine
            .execute(&session, Request::new("{ todos { id title } }"))
            .await;
        assert!(list.errors.is_empty(), "SQL list query: {list:?}");

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(8))
            .build()
            .unwrap();
        let got: serde_json::Value = client
            .get(format!("{celld}/todo/{todo_id}"))
            .header(CELL_INTERNAL_SECRET_HEADER, read_secret.header_value())
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(got["title"], "dcs6", "sealed GET after wait-path: {got}");
    }
}
