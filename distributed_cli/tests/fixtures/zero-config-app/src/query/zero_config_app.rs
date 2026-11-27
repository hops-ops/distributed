//! Permissions for `ZeroConfigAppView`.

use distributed::graphql::{read, ModelPermissions};

use crate::read_models::ZeroConfigAppView;

pub type Model = ZeroConfigAppView;

pub fn permissions() -> ModelPermissions<ZeroConfigAppView> {
    ModelPermissions::new()
        // Deny-by-default until roles are granted. grant_all(USER) in mod.rs
        // covers the default; tighten .columns(...) / .rows(...) for prod.
        .grant(super::roles::USER, read().all_columns().aggregations())
}
