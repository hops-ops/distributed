//! Authenticity for Zitadel Action → HTTP deliveries.
//!
//! Paths:
//! 1. **Shared secret** header `x-zitadel-ingestor-secret` or `Authorization: Bearer`
//! 2. **Actions v2 event body** when `ZITADEL_INGESTOR_ALLOW_ACTION_EVENTS=1` (local only)

use std::env;

use distributed::command_dispatch::VerifiedPrincipal;
use distributed::microsvc::{HandlerError, Session, ROLE_KEY, USER_ID_KEY};

/// Env var for the shared secret (required for fixture/curl path).
pub const SECRET_ENV: &str = "ZITADEL_INGESTOR_SECRET";

/// Preferred Action/HTTP header (lowercase session keys).
pub const SECRET_HEADER: &str = "x-zitadel-ingestor-secret";

/// When `1`/`true`, accept native Actions v2 event envelopes without shared secret.
pub const ALLOW_ACTION_EVENTS_ENV: &str = "ZITADEL_INGESTOR_ALLOW_ACTION_EVENTS";

/// Subject the ingestor's commands run as. The shared secret (or the
/// process's own scrape loop) is what vouches for it; no caller chooses it.
pub const INGESTOR_USER_ID: &str = "system:zitadel-ingestor";

/// Role the ingestor's commands declare. It is not the framework's transport
/// role: declaring that one would let anything that can publish to the bus
/// run them without the secret.
pub const INGESTOR_ROLE: &str = "zitadel-ingestor";

/// Session of an ingestor command: the ingestor identity and nothing the
/// caller sent (the secret stays at the HTTP edge).
pub fn ingestor_session() -> Session {
    let mut session = Session::new();
    session.set(USER_ID_KEY, INGESTOR_USER_ID);
    session.set(ROLE_KEY, INGESTOR_ROLE);
    session
}

pub fn ingestor_principal() -> VerifiedPrincipal {
    VerifiedPrincipal::internal(INGESTOR_USER_ID).expect("the ingestor subject is not empty")
}

pub fn configured_secret() -> Option<String> {
    env::var(SECRET_ENV).ok().filter(|s| !s.trim().is_empty())
}

pub fn allow_action_events() -> bool {
    matches!(
        env::var(ALLOW_ACTION_EVENTS_ENV)
            .ok()
            .as_deref()
            .map(str::trim),
        Some("1") | Some("true") | Some("TRUE") | Some("yes")
    )
}

pub fn presented_secret(session: &Session) -> Option<String> {
    if let Some(v) = session.get(SECRET_HEADER).filter(|s| !s.is_empty()) {
        return Some(v.to_string());
    }
    if let Some(auth) = session.get("authorization") {
        if let Some(token) = auth
            .strip_prefix("Bearer ")
            .or_else(|| auth.strip_prefix("bearer "))
        {
            let token = token.trim();
            if !token.is_empty() {
                return Some(token.to_string());
            }
        }
    }
    None
}

pub fn verify_authenticity(session: &Session, is_action_event: bool) -> Result<(), HandlerError> {
    if let Some(presented) = presented_secret(session) {
        let expected = configured_secret().ok_or_else(|| {
            HandlerError::Unauthorized(format!(
                "{SECRET_ENV} is not configured; refusing Zitadel ingress"
            ))
        })?;
        if presented != expected {
            return Err(HandlerError::Unauthorized(
                "invalid Zitadel ingestor secret".into(),
            ));
        }
        return Ok(());
    }

    if is_action_event && allow_action_events() {
        return Ok(());
    }

    if is_action_event {
        return Err(HandlerError::Unauthorized(format!(
            "Action event rejected: set {ALLOW_ACTION_EVENTS_ENV}=1 (local) or send {SECRET_HEADER}"
        )));
    }

    let _expected = configured_secret().ok_or_else(|| {
        HandlerError::Unauthorized(format!(
            "{SECRET_ENV} is not configured; refusing Zitadel ingress"
        ))
    })?;
    Err(HandlerError::Unauthorized(format!(
        "missing Zitadel authenticity ({SECRET_HEADER} or Authorization: Bearer)"
    )))
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Mutex, MutexGuard};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// The ingestor's environment for one test; restored on drop. Tests that
    /// read it hold one of these, so they do not see each other's settings.
    pub(in super::super) struct IngestorEnv {
        previous: [(&'static str, Option<String>); 2],
        _lock: MutexGuard<'static, ()>,
    }

    impl IngestorEnv {
        pub(in super::super) fn set(secret: Option<&str>, allow_actions: bool) -> Self {
            let lock = ENV_LOCK
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let previous =
                [SECRET_ENV, ALLOW_ACTION_EVENTS_ENV].map(|name| (name, env::var(name).ok()));
            match secret {
                Some(secret) => env::set_var(SECRET_ENV, secret),
                None => env::remove_var(SECRET_ENV),
            }
            if allow_actions {
                env::set_var(ALLOW_ACTION_EVENTS_ENV, "1");
            } else {
                env::remove_var(ALLOW_ACTION_EVENTS_ENV);
            }
            Self {
                previous,
                _lock: lock,
            }
        }
    }

    impl Drop for IngestorEnv {
        fn drop(&mut self) {
            for (name, value) in &self.previous {
                match value {
                    Some(value) => env::set_var(name, value),
                    None => env::remove_var(name),
                }
            }
        }
    }

    fn with_env(secret: Option<&str>, allow_actions: bool, f: impl FnOnce()) {
        let _env = IngestorEnv::set(secret, allow_actions);
        f();
    }

    fn session(pairs: &[(&str, &str)]) -> Session {
        let mut m = HashMap::new();
        for (k, v) in pairs {
            m.insert((*k).to_string(), (*v).to_string());
        }
        Session::from_map(m)
    }

    #[test]
    fn rejects_when_secret_not_configured() {
        with_env(None, false, || {
            let err = verify_authenticity(&session(&[(SECRET_HEADER, "x")]), false).unwrap_err();
            assert!(matches!(err, HandlerError::Unauthorized(_)));
        });
    }

    #[test]
    fn accepts_matching_header() {
        with_env(Some("s3cret"), false, || {
            verify_authenticity(&session(&[(SECRET_HEADER, "s3cret")]), false).unwrap();
        });
    }

    #[test]
    fn accepts_action_event_when_allowed() {
        with_env(None, true, || {
            verify_authenticity(&Session::new(), true).unwrap();
        });
    }
}
