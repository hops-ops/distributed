//! Shared handler helpers.
//!
//! **Admission vs domain**
//! - [`session_has_user`] / [`session_is_admin`] / [`causal_has_user`] /
//!   [`causal_is_admin`] — command **guards** (session admission only).
//! - Handler bodies bind the principal and call the domain; they do not re-check
//!   “am I logged in?” when a guard already did.
//! - Domain owns entity invariants (empty title, ownership, board rules).

use distributed::microsvc::{CausalCommandContext, HandlerError, Session};
use distributed::Aggregate;

pub fn rejected(err: impl std::fmt::Display) -> HandlerError {
    HandlerError::Rejected(err.to_string())
}

/// Authenticated user from session (`x-user-id` via DevHeaders or OIDC claim map).
pub fn require_user(session: &Session) -> Result<String, HandlerError> {
    session
        .user_id()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .ok_or_else(|| HandlerError::Unauthorized("missing x-user-id".into()))
}

/// Session has a non-empty user id (for `guard` — bool, not Result).
pub fn session_has_user(session: &Session) -> bool {
    session.user_id().is_some_and(|s| !s.is_empty())
}

/// Engine role set contains `admin` (`x-roles` / OIDC claim map). For `guard`.
pub fn session_is_admin(session: &Session) -> bool {
    session.has_role("admin")
}

/// Typed causal guard: non-empty session user id.
pub fn causal_has_user<A>(ctx: &CausalCommandContext<'_, A>) -> bool
where
    A: Aggregate + Send + Sync + 'static,
{
    session_has_user(ctx.session())
}

/// Typed causal guard: session user present and carries `admin`.
pub fn causal_is_admin<A>(ctx: &CausalCommandContext<'_, A>) -> bool
where
    A: Aggregate + Send + Sync + 'static,
{
    session_has_user(ctx.session()) && session_is_admin(ctx.session())
}

/// Principal after a user-session guard (for domain `owner_id` / author args).
pub fn principal<A>(ctx: &CausalCommandContext<'_, A>) -> Result<String, HandlerError>
where
    A: Aggregate + Send + Sync + 'static,
{
    ctx.user_id().map(str::to_string)
}

/// Require engine role `admin` (handler-path Result form).
pub fn require_admin(session: &Session) -> Result<(), HandlerError> {
    if session.has_role("admin") {
        return Ok(());
    }
    let roles = session.roles();
    if roles.is_empty() {
        Err(HandlerError::Unauthorized("missing x-roles".into()))
    } else {
        Err(HandlerError::Rejected(format!(
            "admin role required, got `{}`",
            roles.join(",")
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use distributed::microsvc::{Session, ROLE_KEY, USER_ID_KEY};

    #[test]
    fn session_has_user_requires_nonempty_id() {
        let mut s = Session::new();
        assert!(!session_has_user(&s));
        s.set(USER_ID_KEY, "");
        assert!(!session_has_user(&s));
        s.set(USER_ID_KEY, "alice");
        assert!(session_has_user(&s));
    }

    #[test]
    fn session_is_admin_exact_role() {
        let mut s = Session::new();
        assert!(!session_is_admin(&s));
        s.set(ROLE_KEY, "user");
        assert!(!session_is_admin(&s));
        s.set(ROLE_KEY, "admin");
        assert!(session_is_admin(&s));
    }

    #[test]
    fn require_admin_errors() {
        let mut s = Session::new();
        assert!(require_admin(&s).is_err());
        s.set(ROLE_KEY, "user");
        assert!(require_admin(&s).is_err());
        s.set(ROLE_KEY, "admin");
        assert!(require_admin(&s).is_ok());
    }

    #[test]
    fn require_user_errors_and_returns_id() {
        let mut s = Session::new();
        assert!(require_user(&s).is_err());
        s.set(USER_ID_KEY, "bob");
        assert_eq!(require_user(&s).unwrap(), "bob");
    }

    #[test]
    fn session_is_admin_requires_user_for_causal_admin_guard_semantics() {
        // Admin role without a user id is not a usable principal for force_archive.
        let mut s = Session::new();
        s.set(ROLE_KEY, "admin");
        assert!(session_is_admin(&s));
        assert!(!session_has_user(&s));
    }
}
