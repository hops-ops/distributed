//! Behavioral suite — GraphQL-only commands + queries, per-user isolation.
//!
//! Set `E2E_BASE_URL` to hit an external process, or leave unset to boot
//! an in-process service (SQLite memory + InMemoryBus + projector loop).

use std::sync::Arc;
use std::time::Duration;

use distributed::bus::{InMemoryBus, RunOptions};
use distributed::microsvc::serve;
use distributed::microsvc::{SqlStores, SqliteStores};
use e2e_service::{
    build_graphql_engine, build_service, distributed_client_surface, read_models, CLIENT_SURFACE,
};
use e2e_suite::{
    assert_http_commands_disabled, cases, graphql, graphql_for_application, graphql_raw,
    new_command_id, offline_oidc_identity, todos_archive, todos_complete, todos_create,
    todos_force_archive, todos_purge, todos_rename, todos_reopen, wait_ready,
};

async fn ensure_target() -> String {
    if let Ok(url) = std::env::var("E2E_BASE_URL") {
        if !url.is_empty() {
            assert!(
                wait_ready(&url, Duration::from_secs(30)).await,
                "E2E_BASE_URL={url} not ready"
            );
            return url;
        }
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let bind = addr.to_string();
    let base = format!("http://{bind}");

    // Always in-memory SQLite for offline suite (ignore compose DATABASE_URL=postgres).
    let stores = SqliteStores::connect("sqlite::memory:")
        .await
        .expect("stores");
    stores
        .bootstrap_read_models(read_models().catalog())
        .await
        .expect("bootstrap");
    let bus = InMemoryBus::new();

    let change_rx = stores.read_model_changes();
    // Offline suite uses a synthetic signed static-JWKS bearer. Durable command
    // tests exercise the same VerifiedPrincipal fence as production.
    let service = build_service(&stores).with_bus(bus.clone());
    let gql = build_graphql_engine(
        stores.graphql_pool(),
        &service,
        offline_oidc_identity(),
        Some(change_rx),
    )
    .expect("gql");
    let service = Arc::new(service.try_with_graphql(gql).expect("bind gql"));

    // Projector consumer (eventual consistency path — no command-side dual-write).
    let consumer_stores = stores.clone();
    let bus_c = bus.clone();
    tokio::spawn(async move {
        loop {
            let service = build_service(&consumer_stores).with_bus(bus_c.clone());
            if let Err(error) = service.run(RunOptions::until_idle()).await {
                eprintln!("offline projector loop: {error}");
                if let Ok(Some((code, detail))) = sqlx::query_as::<_, (String, Vec<u8>)>(
                    "SELECT failure_code, failure_bytes FROM projection_failures LIMIT 1",
                )
                .fetch_optional(consumer_stores.events.pool())
                .await
                {
                    eprintln!(
                        "offline projector failure {code}: {}",
                        String::from_utf8_lossy(&detail)
                    );
                }
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });

    let svc = Arc::clone(&service);
    let bind_c = bind.clone();
    tokio::spawn(async move {
        let _ = serve(svc, &bind_c).await;
    });

    assert!(
        wait_ready(&base, Duration::from_secs(10)).await,
        "in-process todo service not ready at {base}"
    );
    base
}

async fn poll_todo(base: &str, user: &str, todo_id: &str) -> Option<serde_json::Value> {
    for _ in 0..100 {
        if let Ok(v) = graphql(
            base,
            "{ todos { todo_id owner_id title status } }",
            user,
            "user",
        )
        .await
        {
            if let Some(arr) = v["data"]["todos"].as_array() {
                if let Some(row) = arr.iter().find(|r| r["todo_id"] == todo_id) {
                    return Some(row.clone());
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    None
}

fn id(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let s = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{n:x}-{s:x}")
}

#[tokio::test]
async fn t0_http_command_routes_disabled() {
    let base = ensure_target().await;
    assert_http_commands_disabled(&base)
        .await
        .unwrap_or_else(|e| panic!("{}: {e}", cases::HTTP_OFF));
    eprintln!("{} ok", cases::HTTP_OFF);
}

#[tokio::test]
async fn t1a_application_surface_returns_actual_todo_upsert_and_causal_obligation() {
    let base = ensure_target().await;
    let tid = id("tapp");
    let command_id = new_command_id();
    let mutation = format!(
        r#"mutation {{
          todos_create(commandId: "{command_id}", input: {{
            todo_id: "{tid}", title: "Application projection"
          }}) {{ todo_id owner_id title status }}
        }}"#
    );
    let manifest = distributed_client_surface().manifest().unwrap();
    // e2e-ui eligible roles are admin+user (canonical sorted); schema privilege
    // remains the portable user contract.
    let response = graphql_for_application(
        &base,
        &mutation,
        "alice",
        "user",
        CLIENT_SURFACE.name,
        CLIENT_SURFACE.eligible_roles,
        CLIENT_SURFACE.schema_roles,
        &manifest.schema_fingerprint,
    )
    .await
    .unwrap();
    assert!(response.get("errors").is_none(), "{response}");
    let command = &response["extensions"]["distributed"]["command"];
    assert!(
        matches!(
            command["state"].as_str(),
            Some("succeeded" | "succeeded_pending_projection")
        ),
        "{response}"
    );
    assert_eq!(command["consistency"], "eventual", "{response}");
    assert!(
        command["projection"]["delta"]["operations"]
            .as_array()
            .is_some_and(|operations| operations.iter().any(|operation| {
                operation["mutation"]["op"] == "upsert"
                    && operation["mutation"]["scope"]["model"] == "Todos"
            })),
        "application surface must preserve the actual Todo upsert: {response}"
    );
    assert_eq!(
        command["projection"]["obligations"]
            .as_array()
            .map(Vec::len),
        Some(1),
        "application surface must derive one finite projection obligation: {response}"
    );
    assert_eq!(
        command["expects"].as_array().map(Vec::len),
        Some(1),
        "client confirmation must expose the same finite obligation: {response}"
    );
}

/// GraphQL create input has no owner_id; session principal is always the owner.
#[tokio::test]
async fn t1c_create_owner_from_session_not_input() {
    let base = ensure_target().await;
    let tid = id("towner");
    let command_id = new_command_id();
    // Extra owner_id field must not be accepted as spoof — schema rejects or ignores.
    let spoof = format!(
        r#"mutation {{
          todos_create(commandId: "{command_id}", input: {{ todo_id: "{tid}", title: "owned", owner_id: "evil" }}) {{
            todo_id owner_id
          }}
        }}"#
    );
    let spoof_res = graphql(&base, &spoof, "alice", "user").await;
    match spoof_res {
        Ok(v) => {
            // If engine ignores unknown input fields, owner must still be session.
            if let Some(owner) = v["data"]["todos_create"]["owner_id"].as_str() {
                assert_eq!(
                    owner,
                    "alice",
                    "{}: owner spoofed: {v}",
                    cases::CREATE_OWNER_SESSION
                );
            } else {
                // GraphQL errors (unknown field) also acceptable
                let errs = v.get("errors").and_then(|e| e.as_array());
                assert!(
                    errs.map(|a| !a.is_empty()).unwrap_or(false),
                    "{}: expected errors or alice owner: {v}",
                    cases::CREATE_OWNER_SESSION
                );
            }
        }
        Err(e) => {
            assert!(
                e.to_lowercase().contains("error")
                    || e.contains("400")
                    || e.contains("field")
                    || e.contains("owner"),
                "{}: unexpected: {e}",
                cases::CREATE_OWNER_SESSION
            );
        }
    }

    let tid2 = id("towner2");
    let payload = todos_create(&base, &tid2, "session owner", "alice", "user")
        .await
        .expect(cases::CREATE_OWNER_SESSION);
    assert_eq!(payload["owner_id"], "alice");
    eprintln!("{} ok", cases::CREATE_OWNER_SESSION);
}

#[tokio::test]
async fn t2_owner_isolation_graphql() {
    let base = ensure_target().await;
    let alice_id = id("ta");
    let bob_id = id("tb");

    todos_create(&base, &alice_id, "Alice only", "alice", "user")
        .await
        .expect(cases::OWNER_ISOLATION);
    todos_create(&base, &bob_id, "Bob only", "bob", "user")
        .await
        .expect(cases::OWNER_ISOLATION);

    assert!(poll_todo(&base, "alice", &alice_id).await.is_some());
    assert!(poll_todo(&base, "bob", &bob_id).await.is_some());

    let v = graphql(&base, "{ todos { todo_id owner_id } }", "alice", "user")
        .await
        .expect(cases::OWNER_ISOLATION);
    let arr = v["data"]["todos"].as_array().expect("todos array");
    assert!(
        arr.iter().all(|r| r["owner_id"] == "alice"),
        "{}: alice saw foreign todos: {v}",
        cases::OWNER_ISOLATION
    );
    assert!(
        arr.iter().any(|r| r["todo_id"] == alice_id),
        "{}: missing alice todo",
        cases::OWNER_ISOLATION
    );
    assert!(
        !arr.iter().any(|r| r["todo_id"] == bob_id),
        "{}: alice saw bob's todo",
        cases::OWNER_ISOLATION
    );
    eprintln!("{} ok", cases::OWNER_ISOLATION);
}

/// Admin role: same `todos` query has no owner filter — sees alice + bob rows.
#[tokio::test]
async fn t2b_admin_sees_all_owners() {
    let base = ensure_target().await;
    let alice_id = id("taa");
    let bob_id = id("tbb");

    todos_create(&base, &alice_id, "Alice note", "alice", "user")
        .await
        .expect(cases::ADMIN_SEES_ALL);
    todos_create(&base, &bob_id, "Bob note", "bob", "user")
        .await
        .expect(cases::ADMIN_SEES_ALL);

    assert!(poll_todo(&base, "alice", &alice_id).await.is_some());
    assert!(poll_todo(&base, "bob", &bob_id).await.is_some());

    let v = graphql(
        &base,
        "{ todos { todo_id owner_id title } }",
        "admin-user",
        "admin",
    )
    .await
    .expect(cases::ADMIN_SEES_ALL);
    let arr = v["data"]["todos"].as_array().expect("todos array");
    let owners: Vec<&str> = arr.iter().filter_map(|r| r["owner_id"].as_str()).collect();
    assert!(
        arr.iter().any(|r| r["todo_id"] == alice_id),
        "{}: admin missing alice todo: {v}",
        cases::ADMIN_SEES_ALL
    );
    assert!(
        arr.iter().any(|r| r["todo_id"] == bob_id),
        "{}: admin missing bob todo: {v}",
        cases::ADMIN_SEES_ALL
    );
    assert!(
        owners.contains(&"alice") && owners.contains(&"bob"),
        "{}: admin should see multiple owners, got {owners:?}",
        cases::ADMIN_SEES_ALL
    );
    eprintln!("{} ok alice={alice_id} bob={bob_id}", cases::ADMIN_SEES_ALL);
}

/// Admin-only mutation: force-archive another user's todo; user role cannot call it.
#[tokio::test]
async fn t2c_admin_force_archive() {
    let base = ensure_target().await;
    let tid = id("tfa");

    todos_create(&base, &tid, "Alice will be forced", "alice", "user")
        .await
        .expect(cases::ADMIN_FORCE_ARCHIVE);
    assert!(poll_todo(&base, "alice", &tid).await.is_some());

    // User role must not force-archive (field absent / rejected).
    let user_attempt = todos_force_archive(&base, &tid, "alice", "user").await;
    assert!(
        user_attempt.is_err(),
        "{}: user role must not force-archive (got ok: {user_attempt:?})",
        cases::ADMIN_FORCE_ARCHIVE
    );
    // Denied call must not archive the row.
    let after_deny = poll_todo(&base, "alice", &tid)
        .await
        .expect("todo exists after denied force-archive");
    assert_ne!(
        after_deny["status"],
        "archived",
        "{}: user force-archive must not change status",
        cases::ADMIN_FORCE_ARCHIVE
    );

    let payload = todos_force_archive(&base, &tid, "admin-user", "admin")
        .await
        .unwrap_or_else(|e| panic!("{}: {e}", cases::ADMIN_FORCE_ARCHIVE));
    assert_eq!(payload["todo_id"], tid);
    assert_eq!(payload["owner_id"], "alice");
    assert_eq!(payload["status"], "archived");
    assert_eq!(payload["archived_by"], "admin-user");

    let mut ok = false;
    for _ in 0..100 {
        if let Some(row) = poll_todo(&base, "alice", &tid).await {
            if row["status"] == "archived" {
                ok = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert!(
        ok,
        "{}: projected status not archived after admin force",
        cases::ADMIN_FORCE_ARCHIVE
    );
    eprintln!("{} ok {tid}", cases::ADMIN_FORCE_ARCHIVE);
}

/// Role SDL: force-archive only on admin schema (engine source of truth).
#[tokio::test]
async fn t2d_sdl_role_split_force_archive() {
    let stores = SqliteStores::connect("sqlite::memory:")
        .await
        .expect("stores");
    stores
        .bootstrap_read_models(read_models().catalog())
        .await
        .expect("bootstrap");
    let service = build_service(&stores);
    let engine = build_graphql_engine(
        stores.graphql_pool(),
        &service,
        offline_oidc_identity(),
        None,
    )
    .expect("gql");
    let user_sdl = engine.sdl_for_role("user").expect("user sdl");
    let admin_sdl = engine.sdl_for_role("admin").expect("admin sdl");
    assert!(
        !user_sdl.contains("todos_force_archive"),
        "{}: user SDL must not expose force-archive",
        cases::SDL_ROLE_SPLIT
    );
    assert!(
        admin_sdl.contains("todos_force_archive"),
        "{}: admin SDL must expose force-archive",
        cases::SDL_ROLE_SPLIT
    );
    assert!(
        admin_sdl.contains("todo.force_archived")
            || admin_sdl.contains("TodoForceArchive")
            || admin_sdl.contains("todos_force_archive"),
        "{}: admin SDL mutation surface incomplete",
        cases::SDL_ROLE_SPLIT
    );
    eprintln!("{} ok", cases::SDL_ROLE_SPLIT);
}

/// Admin todos query supports limit (bounded list).
#[tokio::test]
async fn t2e_admin_todos_respects_limit() {
    let base = ensure_target().await;
    for i in 0..5 {
        let tid = id(&format!("lim{i}"));
        todos_create(&base, &tid, &format!("n{i}"), "alice", "user")
            .await
            .expect(cases::ADMIN_QUERY_LIMIT);
        assert!(poll_todo(&base, "alice", &tid).await.is_some());
    }
    let v = graphql(
        &base,
        "{ todos(limit: 2, order_by: [{ todo_id: asc }]) { todo_id } }",
        "admin-user",
        "admin",
    )
    .await
    .expect(cases::ADMIN_QUERY_LIMIT);
    let arr = v["data"]["todos"].as_array().expect("todos");
    assert!(
        arr.len() <= 2,
        "{}: expected at most 2 rows, got {}: {v}",
        cases::ADMIN_QUERY_LIMIT,
        arr.len()
    );
    eprintln!("{} ok n={}", cases::ADMIN_QUERY_LIMIT, arr.len());
}

#[tokio::test]
async fn t4_not_owner_rejected() {
    let base = ensure_target().await;
    let tid = id("to");
    todos_create(&base, &tid, "Alice task", "alice", "user")
        .await
        .expect(cases::NOT_OWNER);

    let err = todos_complete(&base, &tid, "bob", "user")
        .await
        .expect_err(cases::NOT_OWNER);
    assert!(
        err.to_lowercase().contains("reject")
            || err.to_lowercase().contains("owner")
            || err.to_lowercase().contains("forbidden")
            || err.contains("422")
            || err.contains("UNPROCESSABLE"),
        "{}: unexpected error: {err}",
        cases::NOT_OWNER
    );
    // Rename/archive/reopen IDOR: bob must not succeed (same domain owner gate as complete).
    assert!(
        todos_rename(&base, &tid, "Hijacked", "bob", "user")
            .await
            .is_err(),
        "{}: rename should fail for bob",
        cases::NOT_OWNER_MUTATES
    );
    assert!(
        todos_archive(&base, &tid, "bob", "user").await.is_err(),
        "{}: archive should fail for bob",
        cases::NOT_OWNER_MUTATES
    );
    // GraphQL todos_reopen path — real mutation helper, non-owner rejected.
    assert!(
        todos_reopen(&base, &tid, "bob", "user").await.is_err(),
        "{}: reopen should fail for bob",
        cases::NOT_OWNER_MUTATES
    );
    let row = poll_todo(&base, "alice", &tid).await.expect("row");
    assert_eq!(row["title"], "Alice task");
    assert_eq!(row["status"], "open");
    eprintln!("{} + {} ok", cases::NOT_OWNER, cases::NOT_OWNER_MUTATES);
}

#[tokio::test]
async fn t5_unauthenticated_rejected() {
    let base = ensure_target().await;
    let tid = id("tu");
    let command_id = new_command_id();
    let doc = format!(
        r#"mutation {{
          todos_create(commandId: "{command_id}", input: {{ todo_id: "{tid}", title: "no user" }}) {{
            todo_id
          }}
        }}"#
    );
    // DevHeaders with no identity → command guard rejects.
    let (status, body) = graphql_raw(&base, &doc).await.expect(cases::UNAUTH);
    // GuardRejected is client-facing (often GraphQL errors / 400); Unauthorized
    // remains 401 when identity is checked elsewhere.
    let has_err = body
        .get("errors")
        .and_then(|e| e.as_array())
        .map(|a| !a.is_empty())
        .unwrap_or(false);
    assert!(
        status == 401 || status == 400 || has_err,
        "{}: expected 401/400 or GraphQL errors, got HTTP {status} {body}",
        cases::UNAUTH
    );
    eprintln!("{} ok", cases::UNAUTH);
}

#[tokio::test]
async fn t7_modeled_no_ops_succeed_without_projection_obligations() {
    let base = ensure_target().await;
    let tid = id("noop");
    todos_create(&base, &tid, "Stable title", "alice", "user")
        .await
        .unwrap();
    assert!(poll_todo(&base, "alice", &tid).await.is_some());

    let rename_id = new_command_id();
    let rename = format!(
        r#"mutation {{
          todos_rename(commandId: "{rename_id}", input: {{
            todo_id: "{tid}", title: "Stable title"
          }}) {{ todo_id title status }}
        }}"#
    );
    let response = graphql(&base, &rename, "alice", "user").await.unwrap();
    assert!(response.get("errors").is_none(), "{response}");
    let receipt = &response["extensions"]["distributed"]["command"];
    assert_eq!(receipt["state"], "succeeded", "{response}");
    assert_eq!(receipt["consistency"], "eventual", "{response}");
    assert_eq!(receipt["expects"], serde_json::json!([]), "{response}");
    let first_payload = response["data"]["todos_rename"].clone();
    let first_receipt = receipt.clone();
    let row_before_retry = poll_todo(&base, "alice", &tid).await.unwrap();

    let retry = graphql(&base, &rename, "alice", "user").await.unwrap();
    assert!(retry.get("errors").is_none(), "{retry}");
    assert_eq!(
        retry["data"]["todos_rename"], first_payload,
        "same commandId must replay the exact zero-occurrence payload"
    );
    assert_eq!(
        retry["extensions"]["distributed"]["command"], first_receipt,
        "same commandId must replay stable zero-obligation receipt bytes"
    );
    let row_after_retry = poll_todo(&base, "alice", &tid).await.unwrap();
    assert_eq!(
        row_after_retry, row_before_retry,
        "zero-occurrence command replay must not change projected status"
    );
    let status = graphql(
        &base,
        &format!(r#"{{ commandStatus(commandId: "{rename_id}") {{ state }} }}"#),
        "alice",
        "user",
    )
    .await
    .unwrap();
    assert!(status.get("errors").is_none(), "{status}");
    assert_eq!(
        status["data"]["commandStatus"]["state"], "succeeded",
        "zero-occurrence replay must remain durably recoverable as succeeded"
    );

    todos_archive(&base, &tid, "alice", "user").await.unwrap();
    let mut archived = None;
    for _ in 0..100 {
        if let Some(row) = poll_todo(&base, "alice", &tid).await {
            if row["status"] == "archived" {
                archived = Some(row);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    let archived = archived.expect("first archive must project before testing its no-op replay");

    let archive_id = new_command_id();
    let archive = format!(
        r#"mutation {{
          todos_archive(commandId: "{archive_id}", input: {{ todo_id: "{tid}" }}) {{
            todo_id status
          }}
        }}"#
    );
    let response = graphql(&base, &archive, "alice", "user").await.unwrap();
    assert!(response.get("errors").is_none(), "{response}");
    let receipt = &response["extensions"]["distributed"]["command"];
    assert_eq!(receipt["state"], "succeeded", "{response}");
    assert_eq!(receipt["expects"], serde_json::json!([]), "{response}");
    let replayed = poll_todo(&base, "alice", &tid).await.unwrap();
    assert_eq!(replayed, archived, "no-op replay changed the projected row");
}

#[tokio::test]
async fn t8_explicit_purge_domain_event_deletes_the_todo_row() {
    let base = ensure_target().await;
    let tid = id("purge");
    todos_create(&base, &tid, "Disposable", "alice", "user")
        .await
        .unwrap();
    assert!(poll_todo(&base, "alice", &tid).await.is_some());

    let payload = todos_purge(&base, &tid, "alice", "user").await.unwrap();
    assert_eq!(payload["todo_id"], tid);
    assert_eq!(payload["purged"], true);

    for _ in 0..100 {
        let response = graphql(
            &base,
            "{ todos { todo_id owner_id title status } }",
            "alice",
            "user",
        )
        .await
        .unwrap();
        let present = response["data"]["todos"]
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["todo_id"] == tid));
        if !present {
            return;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    panic!("purged todo `{tid}` remained query-visible");
}

#[tokio::test]
async fn t9_chat_room_partition_projects_posted_message() {
    use std::time::{SystemTime, UNIX_EPOCH};

    let base = ensure_target().await;
    let message_id = id("chat");
    let room_id = id("room");
    let now_ms = || {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
    };
    // `created_at` is a `now()` default: whatever the caller sends (here a
    // value from 1970) only previews; the server stamps its own clock.
    let created_at = "1";
    let before = now_ms();
    let command_id = new_command_id();
    let mutation = format!(
        r#"mutation {{
          chat_messages_post(commandId: "{command_id}", input: {{
            message_id: "{message_id}",
            room_id: "{room_id}",
            body: "partitioned hello",
            created_at: "{created_at}"
          }}) {{ message_id room_id author_id body created_at }}
        }}"#
    );
    let response = graphql(&base, &mutation, "alice", "user").await.unwrap();
    assert!(response.get("errors").is_none(), "{response}");
    assert_eq!(response["data"]["chat_messages_post"]["room_id"], room_id);

    let query = format!(
        r#"{{
          chat_messages(where: {{ room_id: {{ _eq: "{room_id}" }} }}) {{
            message_id room_id author_id body created_at
          }}
        }}"#
    );
    let mut projected = None;
    for _ in 0..100 {
        let response = graphql(&base, &query, "alice", "user").await.unwrap();
        assert!(response.get("errors").is_none(), "{response}");
        if let Some(row) = response["data"]["chat_messages"]
            .as_array()
            .and_then(|rows| rows.iter().find(|row| row["message_id"] == message_id))
        {
            projected = Some(row.clone());
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    let projected = projected.expect("room-partitioned Chat occurrence must project");
    assert_eq!(projected["room_id"], room_id);
    assert_eq!(projected["author_id"], "alice");
    assert_eq!(projected["body"], "partitioned hello");
    let stored: u128 = projected["created_at"]
        .as_str()
        .and_then(|value| value.parse().ok())
        .expect("created_at is canonical unix milliseconds");
    assert!(
        (before.saturating_sub(5_000)..=now_ms() + 5_000).contains(&stored),
        "server must stamp created_at, got {stored}"
    );
    assert_eq!(
        response["data"]["chat_messages_post"]["created_at"], projected["created_at"],
        "the command payload carries the stamped value"
    );
}

#[tokio::test]
async fn t10_blob_direct_projection_is_immediate_and_owner_filtered() {
    let base = ensure_target().await;
    let game_id = id("blob");
    let command_id = new_command_id();
    let mutation = format!(
        r#"mutation {{
          blob_games_start(commandId: "{command_id}", input: {{ game_id: "{game_id}" }}) {{
            game_id owner_id score status
          }}
        }}"#
    );
    let response = graphql(&base, &mutation, "alice", "user").await.unwrap();
    assert!(response.get("errors").is_none(), "{response}");
    assert_eq!(response["data"]["blob_games_start"]["game_id"], game_id);
    assert_eq!(response["data"]["blob_games_start"]["owner_id"], "alice");

    let query = format!(
        r#"{{
          blob_games(where: {{ game_id: {{ _eq: "{game_id}" }} }}) {{
            game_id owner_id score status
          }}
        }}"#
    );
    let alice = graphql(&base, &query, "alice", "user").await.unwrap();
    assert!(alice.get("errors").is_none(), "{alice}");
    assert_eq!(alice["data"]["blob_games"][0]["game_id"], game_id);

    let bob = graphql(&base, &query, "bob", "user").await.unwrap();
    assert!(bob.get("errors").is_none(), "{bob}");
    assert_eq!(bob["data"]["blob_games"], serde_json::json!([]));
}
