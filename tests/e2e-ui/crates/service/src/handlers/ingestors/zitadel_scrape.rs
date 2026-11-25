//! Command: `zitadel.scrape.v1` — reconcile from the Management API.
//!
//! Lists every user and publishes the profiles that differ from their
//! `auth_users` row, in one commit with the command's receipt; the
//! `auth_users` projection writes them. No aggregate. Requires
//! `ZITADEL_SERVICE_USER_TOKEN` + `ZITADEL_API_URL` / `OIDC_ISSUER` in env.

use std::future::Future;
use std::pin::Pin;

use axum::http::HeaderMap;
use distributed::command::{typed_command, PreparedCommand, Succeeded, TypedCommand};
use distributed::microsvc::{
    CausalCommandContext, CausalDispatchError, HandlerError, HttpCommandDispatch,
    PreparedCommandHandler, Service,
};
use distributed::{
    ExternalEventSource, ReadModelWorkspaceExt, ReadModelWritePlanStore,
    RelationalReadModelQueryStore, RowKey, RowValue,
};
use e2e_readmodels::AuthUsers;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::zitadel::scrape::{list_mapped_users, ZitadelScrapeConfig, API_URL_ENV, TOKEN_ENV};
use super::zitadel::MappedDelivery;
use super::zitadel::{ingestor_principal, ingestor_session, verify_authenticity, INGESTOR_ROLE};

pub const COMMAND: &str = "zitadel.scrape.v1";

#[derive(Debug, Deserialize, distributed::CommandInput)]
pub struct ZitadelScrapeInput {
    /// What asked for this scrape: `http`, `start` or `interval`.
    pub trigger: String,
}

#[derive(Debug, Serialize, distributed::CommandOutput)]
pub struct ZitadelScrapePayload {
    /// Users the Management API listed.
    pub listed: u32,
    /// Profiles published: one per user whose profile changed.
    pub published: u32,
    /// Listed users not published: unchanged, or with no usable id.
    pub skipped: u32,
}

pub fn command() -> TypedCommand<ZitadelScrapeInput, Succeeded<ZitadelScrapePayload>> {
    typed_command(COMMAND).roles([INGESTOR_ROLE])
}

/// The `zitadel.scrape.v1` handler. It holds the read-model store because it
/// reads `auth_users` to tell which listed profiles changed.
pub struct Scrape<S>(pub S);

type Prepared = Result<PreparedCommand<Succeeded<ZitadelScrapePayload>>, HandlerError>;

impl<'a, S> PreparedCommandHandler<'a, (), ZitadelScrapeInput, Succeeded<ZitadelScrapePayload>>
    for Scrape<S>
where
    S: ReadModelWritePlanStore + RelationalReadModelQueryStore + Clone + Send + Sync + 'static,
{
    type Future = Pin<Box<dyn Future<Output = Prepared> + Send + 'a>>;

    fn call(
        &self,
        ctx: &'a CausalCommandContext<'a, ()>,
        _input: ZitadelScrapeInput,
    ) -> Self::Future {
        let read_models = self.0.clone();
        Box::pin(async move {
            let cfg = ZitadelScrapeConfig::from_env().ok_or_else(|| {
                HandlerError::Rejected(format!(
                    "scrape not configured: set {TOKEN_ENV} and {API_URL_ENV} (or OIDC_ISSUER)"
                ))
            })?;
            // Reading the provider is repeatable; a rerun of this attempt
            // lists again.
            let (listed, users) = list_mapped_users(&cfg)
                .await
                .map_err(|error| HandlerError::Other(error.into()))?;
            reconcile(ctx, &read_models, listed, &users).await
        })
    }
}

/// Publish the `users` (of `listed`) whose profile differs from their
/// `auth_users` row. A row equal to the listed profile means it was already
/// published and projected; a profile published but not yet projected is
/// published again, and the projection writes the same row twice.
async fn reconcile<S>(
    ctx: &CausalCommandContext<'_, ()>,
    read_models: &S,
    listed: usize,
    users: &[MappedDelivery],
) -> Prepared
where
    S: ReadModelWritePlanStore + RelationalReadModelQueryStore,
{
    // Each profile is identified by this command and its place in the
    // listing, so a scrape never publishes a fact an earlier one did.
    let command_id = ctx.message_id().unwrap_or_default();
    let mut changed = Vec::new();
    for user in users {
        let key = RowKey::new([("user_id", RowValue::String(user.profile.user_id.clone()))]);
        let stored = read_models
            .workspace()
            .load::<AuthUsers>(key)
            .one()
            .await
            .map_err(|error| HandlerError::Other(Box::new(error)))?;
        if user
            .profile
            .differs_from(stored.as_ref().map(|stored| &stored.data))
        {
            changed.push(user.profile.clone());
        }
    }
    let payload = ZitadelScrapePayload {
        listed: listed as u32,
        published: changed.len() as u32,
        skipped: listed.saturating_sub(changed.len()) as u32,
    };
    let source =
        |position: usize, profile: &e2e_projections::ZitadelUserProfile| ExternalEventSource {
            producer: COMMAND.into(),
            stream: command_id.into(),
            position: position as u64 + 1,
            key: profile.user_id.clone(),
        };
    let mut changed = changed.into_iter().enumerate();
    let Some((position, first)) = changed.next() else {
        return PreparedCommand::prepare(payload)
            .map_err(|error| HandlerError::Other(Box::new(error)));
    };
    changed
        .fold(
            ctx.publish_external(source(position, &first), first),
            |unit, (position, profile)| unit.publish_external(source(position, &profile), profile),
        )
        .commit()?
        .succeeded(payload)
}

/// Dispatch one scrape as the ingestor. Two scrapes are never the same
/// command (each reads the provider as it is then), so each takes a fresh id.
pub async fn scrape(service: &Service, trigger: &str) -> Result<Value, CausalDispatchError> {
    service
        .dispatch_causal(
            COMMAND,
            &uuid::Uuid::now_v7().to_string(),
            json!({ "trigger": trigger }),
            ingestor_session(),
            ingestor_principal(),
        )
        .await
}

/// HTTP edge of [`COMMAND`] (`Service::with_http_command`): the shared
/// secret, then a scrape as the ingestor with a fresh id, like [`scrape`].
/// The body is ignored and no request header reaches the command.
pub fn http_edge(
    headers: &HeaderMap,
    _body: Value,
) -> Result<HttpCommandDispatch, CausalDispatchError> {
    verify_authenticity(headers, false).map_err(CausalDispatchError::Handler)?;
    Ok(HttpCommandDispatch {
        command_id: uuid::Uuid::now_v7().to_string(),
        input: json!({ "trigger": "http" }),
        session: ingestor_session(),
        principal: ingestor_principal(),
    })
}

#[cfg(test)]
mod tests {
    use super::super::zitadel::{user_profile, HUMAN_UPDATED};
    use super::*;
    use distributed::microsvc::Routes;
    use distributed::read_model::ReadModelWritePlanBuilder;
    use distributed::{InMemoryRepository, OutboxStore, RelationalReadModel};
    use e2e_projections::{ZitadelUserProfile, AUTH_USERS};

    fn listed_user(subject: &str, display_name: &str) -> MappedDelivery {
        MappedDelivery {
            message_name: HUMAN_UPDATED.into(),
            delivery_id: format!("zitadel-scrape:{subject}"),
            profile: user_profile(
                HUMAN_UPDATED,
                subject,
                "human".into(),
                format!("{subject}@example.com"),
                Some(display_name.into()),
                "approved".into(),
            ),
        }
    }

    /// [`Scrape`] with the listing given, not fetched from Zitadel.
    struct Listed(InMemoryRepository, Vec<MappedDelivery>);

    impl<'a> PreparedCommandHandler<'a, (), ZitadelScrapeInput, Succeeded<ZitadelScrapePayload>>
        for Listed
    {
        type Future = Pin<Box<dyn Future<Output = Prepared> + Send + 'a>>;

        fn call(
            &self,
            ctx: &'a CausalCommandContext<'a, ()>,
            _input: ZitadelScrapeInput,
        ) -> Self::Future {
            let (read_models, users) = (self.0.clone(), self.1.clone());
            Box::pin(async move { reconcile(ctx, &read_models, users.len() + 1, &users).await })
        }
    }

    #[tokio::test]
    async fn a_scrape_publishes_only_the_profiles_that_differ_from_their_rows() {
        let repo = InMemoryRepository::new();
        let service_listing = |users: Vec<MappedDelivery>| {
            Service::new().named("identity").routes(
                Routes::for_stores(repo.clone(), repo.clone())
                    .typed_command(command())
                    .handle(Listed(repo.clone(), users)),
            )
        };
        let published = || async {
            repo.outbox_store()
                .pending(usize::MAX)
                .await
                .unwrap()
                .iter()
                .map(|message| {
                    let occurrence = message.domain_event_occurrence().unwrap();
                    assert_eq!(message.id(), occurrence.id());
                    occurrence.decode_body::<ZitadelUserProfile>().unwrap()
                })
                .map(|profile| (profile.user_id, profile.display_name))
                .collect::<Vec<_>>()
        };
        // Run the `auth_users` projection over everything published, writing
        // each projected row as an upsert.
        let project = || async {
            for message in repo.outbox_store().pending(usize::MAX).await.unwrap() {
                let occurrence = message.domain_event_occurrence().unwrap();
                let lowered = AUTH_USERS
                    .server_executor()
                    .unwrap()
                    .plan(&occurrence)
                    .unwrap();
                let mut rows = ReadModelWritePlanBuilder::new();
                for (_, record) in lowered.records() {
                    rows.upsert(&AuthUsers::from_row(record.values().clone()).unwrap())
                        .unwrap();
                }
                repo.commit_write_plan(rows.into_write_plan().unwrap())
                    .await
                    .unwrap();
            }
        };
        let users = vec![listed_user("ada", "Ada"), listed_user("grace", "Grace")];

        // The listing also held one user with no usable id.
        let first = scrape(&service_listing(users.clone()), "start")
            .await
            .unwrap();
        assert_eq!(first, json!({ "listed": 3, "published": 2, "skipped": 1 }));
        assert_eq!(
            published().await,
            [
                ("ada".into(), "Ada".into()),
                ("grace".into(), "Grace".into())
            ]
        );
        project().await;

        let unchanged = scrape(&service_listing(users), "interval").await.unwrap();
        assert_eq!(
            unchanged,
            json!({ "listed": 3, "published": 0, "skipped": 3 })
        );
        assert_eq!(published().await.len(), 2);

        // One profile changed: that one alone is published.
        let changed = scrape(
            &service_listing(vec![
                listed_user("ada", "Ada"),
                listed_user("grace", "Grace H."),
            ]),
            "interval",
        )
        .await
        .unwrap();
        assert_eq!(
            changed,
            json!({ "listed": 3, "published": 1, "skipped": 2 })
        );
        assert_eq!(published().await[2], ("grace".into(), "Grace H.".into()));
    }
}
