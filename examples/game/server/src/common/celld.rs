//! Where a demo's celld is: the base URL and the internal secret shared
//! with the cells worker (`examples/game/cells`).

use distributed::cell_host::InternalHttpSecret;

/// Env var of the celld base URL.
pub const CELLD_URL_ENV: &str = "CELLD_URL";

/// Where the cells are: the celld base URL and the internal secret shared
/// with the cells worker (`DISTRIBUTED_INTERNAL_SECRET`).
#[derive(Clone)]
pub struct CelldOptions {
    /// `http(s)://host:port` of celld serving `examples/game/cells`.
    pub url: String,
    /// Internal-path secret (also authenticates the Queue relay route).
    pub secret: InternalHttpSecret,
}

impl CelldOptions {
    /// Options for `url` and `secret`.
    pub fn new(url: impl Into<String>, secret: impl Into<String>) -> Result<Self, String> {
        let url = url.into().trim_end_matches('/').to_string();
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(format!("celld URL must be http(s): {url}"));
        }
        Ok(Self {
            url,
            secret: InternalHttpSecret::new(secret.into())?,
        })
    }

    /// `CELLD_URL` and `DISTRIBUTED_INTERNAL_SECRET` from the environment.
    pub fn from_env() -> Result<Self, String> {
        let var = |name: &str| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| format!("{name} is required (the demos' cells run on celld)"))
        };
        Self::new(
            var(CELLD_URL_ENV)?,
            var(distributed::cell_host::CELL_INTERNAL_SECRET_ENV)?,
        )
    }
}
