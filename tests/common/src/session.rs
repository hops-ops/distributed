//! Session builders.

use distributed::microsvc::{Session, ROLE_KEY, USER_ID_KEY};

/// A session for `user` acting in `role`.
pub fn session(role: &str, user: &str) -> Session {
    let mut session = Session::new();
    session.set(ROLE_KEY, role);
    session.set(USER_ID_KEY, user);
    session
}

/// A session for `user` with no role.
pub fn user(user: &str) -> Session {
    let mut session = Session::new();
    session.set(USER_ID_KEY, user);
    session
}
