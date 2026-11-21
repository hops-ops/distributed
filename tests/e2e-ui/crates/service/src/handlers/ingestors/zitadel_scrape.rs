//! Command: `zitadel.scrape.v1` — reconcile from the Management API.
//!
//! Lists every user and publishes the ones whose `auth_users` row differs
//! from the listed profile: one commit holds a provider message and the row
//! per changed user, with the command's receipt. No aggregate. Requires
//! `ZITADEL_SERVICE_USER_TOKEN` + `ZITADEL_API_URL` / `OIDC_ISSUER` in env.

use distributed::command::{typed_command, PreparedCommand, Succeeded, TypedCommand};
use std::future::Future;
use std::pin::Pin;

use distributed::microsvc::{
    CausalCommandContext, CausalDispatchError, HandlerError, PreparedCommandHandler, Service,
    Session,
};
use distributed::read_model::ReadModelWritePlanBuilder;
use distributed::{
    ReadModelWorkspaceExt, ReadModelWritePlanStore, RelationalReadModelQueryStore, RowKey, RowValue,
};
use e2e_readmodels::AuthUsers;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::zitadel::scrape::{
    auth_user_row, list_mapped_users, ZitadelScrapeConfig, API_URL_ENV, TOKEN_ENV,
};
use super::zitadel::MappedDelivery;
use super::zitadel::{
    ingestor_principal, ingestor_session, provider_message, verify_authenticity, INGESTOR_ROLE,
};

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
    /// Provider messages published: one per user whose profile changed.
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

/// Whether a listed profile must be published: its row is missing or differs.
///
/// A scrape commits a user's row with that user's message, so an equal row
/// means the profile was already published. `updated_at` is when a profile was
/// ingested, not part of it.
pub fn profile_changed(listed: &AuthUsers, stored: Option<&AuthUsers>) -> bool {
    stored.is_none_or(|stored| {
        AuthUsers {
            updated_at: listed.updated_at.clone(),
            ..stored.clone()
        } != *listed
    })
}

/// Publish the `users` (of `listed`) whose profile changed, each with its
/// `auth_users` row. Directory joins (chat author, blob owner) do not wait for
/// the bus: the row commits here, with the message.
async fn reconcile<S>(
    ctx: &CausalCommandContext<'_, ()>,
    read_models: &S,
    listed: usize,
    users: &[MappedDelivery],
) -> Prepared
where
    S: ReadModelWritePlanStore + RelationalReadModelQueryStore,
{
    // Message ids come from this command's id, so a scrape never meets an id
    // an earlier one committed.
    let command_id = ctx.message_id().unwrap_or_default();
    let mut messages = Vec::new();
    let mut rows = ReadModelWritePlanBuilder::new();
    for user in users {
        let row = auth_user_row(user);
        let key = RowKey::new([("user_id", RowValue::String(row.user_id.clone()))]);
        let stored = read_models
            .workspace()
            .load::<AuthUsers>(key)
            .one()
            .await
            .map_err(|error| HandlerError::Other(Box::new(error)))?;
        if !profile_changed(&row, stored.as_ref().map(|stored| &stored.data)) {
            continue;
        }
        let id = format!("{COMMAND}:{command_id}:{}", messages.len());
        messages.push(provider_message(id, user)?);
        rows.upsert(&row)
            .map_err(|error| HandlerError::Other(Box::new(error)))?;
    }
    let payload = ZitadelScrapePayload {
        listed: listed as u32,
        published: messages.len() as u32,
        skipped: listed.saturating_sub(messages.len()) as u32,
    };
    let mut messages = messages.into_iter();
    let Some(first) = messages.next() else {
        return PreparedCommand::prepare(payload)
            .map_err(|error| HandlerError::Other(Box::new(error)));
    };
    messages
        .fold(ctx.outbox(first), |unit, message| unit.outbox(message))
        .read_models(rows)
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

/// HTTP edge of [`COMMAND`]: the shared secret, then [`scrape`]. `caller` is
/// the request's headers; none of them reaches the command.
pub async fn receive(service: &Service, caller: &Session) -> Result<Value, CausalDispatchError> {
    verify_authenticity(caller, false).map_err(CausalDispatchError::Handler)?;
    scrape(service, "http").await
}

#[cfg(test)]
mod tests {
    use super::super::zitadel::{HUMAN_UPDATED, MACHINE_CREATED};
    use super::*;
    use distributed::microsvc::Routes;
    use distributed::{InMemoryRepository, OutboxStore};
    use e2e_projections::{ZitadelEmail, ZitadelUserPayload};

    fn listed_user(subject: &str, display_name: &str, ingested_at: &str) -> MappedDelivery {
        MappedDelivery {
            message_name: HUMAN_UPDATED.into(),
            delivery_id: format!("zitadel-scrape:{subject}"),
            payload: ZitadelUserPayload {
                schema_version: 1,
                source: "zitadel-scrape".into(),
                delivery_id: format!("zitadel-scrape:{subject}"),
                provider: "zitadel".into(),
                provider_subject: subject.into(),
                user_kind: "human".into(),
                emails: vec![ZitadelEmail {
                    address: format!("{subject}@example.com"),
                    primary: true,
                    verified: true,
                }],
                display_name: Some(display_name.into()),
                approval_status: "approved".into(),
                ingested_at: ingested_at.into(),
            },
        }
    }

    #[test]
    fn a_profile_is_published_when_its_row_is_missing_or_differs() {
        let listed = auth_user_row(&listed_user("ada", "Ada", "2"));
        assert!(profile_changed(&listed, None));

        // Only the ingestion time differs: the same profile.
        let stored = auth_user_row(&listed_user("ada", "Ada", "1"));
        assert!(!profile_changed(&listed, Some(&stored)));

        let renamed = auth_user_row(&listed_user("ada", "Ada L.", "1"));
        assert!(profile_changed(&listed, Some(&renamed)));
        let deactivated = AuthUsers {
            status: "deactivated".into(),
            ..stored.clone()
        };
        assert!(profile_changed(&listed, Some(&deactivated)));
        let pending = AuthUsers {
            approval_status: "pending".into(),
            ..stored
        };
        assert!(profile_changed(&listed, Some(&pending)));
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
    async fn a_second_scrape_of_an_unchanged_listing_stages_nothing() {
        let repo = InMemoryRepository::new();
        let service_listing = |users: Vec<MappedDelivery>| {
            Service::new().named("identity").routes(
                Routes::for_stores(repo.clone(), repo.clone())
                    .typed_command(command())
                    .handle(Listed(repo.clone(), users)),
            )
        };
        let staged = || async {
            repo.outbox_store()
                .pending(usize::MAX)
                .await
                .unwrap()
                .into_iter()
                .map(|message| message.event_type)
                .collect::<Vec<_>>()
        };
        let users = vec![
            listed_user("ada", "Ada", "1"),
            listed_user("grace", "Grace", "1"),
        ];

        // The listing also held one user with no usable id.
        let first = scrape(&service_listing(users.clone()), "start")
            .await
            .unwrap();
        assert_eq!(first, json!({ "listed": 3, "published": 2, "skipped": 1 }));
        assert_eq!(staged().await, [HUMAN_UPDATED, HUMAN_UPDATED]);

        let unchanged = scrape(&service_listing(users), "interval").await.unwrap();
        assert_eq!(
            unchanged,
            json!({ "listed": 3, "published": 0, "skipped": 3 })
        );
        assert_eq!(staged().await.len(), 2);

        // One profile changed: that one alone is published.
        let mut renamed = listed_user("grace", "Grace H.", "2");
        renamed.message_name = MACHINE_CREATED.into();
        let changed = scrape(
            &service_listing(vec![listed_user("ada", "Ada", "2"), renamed]),
            "interval",
        )
        .await
        .unwrap();
        assert_eq!(
            changed,
            json!({ "listed": 3, "published": 1, "skipped": 2 })
        );
        assert_eq!(
            staged().await,
            [HUMAN_UPDATED, HUMAN_UPDATED, MACHINE_CREATED]
        );
    }
}
