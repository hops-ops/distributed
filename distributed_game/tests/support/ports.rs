//! Loopback ports for a child process that takes its listener port on the
//! command line (`celld dev --port`, `celld --listen`): celld has no
//! `--port 0`, so the harness picks the port, and a port is only known to
//! be free until someone else binds it. Shared by every celld launcher:
//! `distributed_game/tests/support/{celld,fleet}.rs` and
//! `examples/game/server/src/devcelld.rs` (by `#[path]`).
//!
//! - [`free_port`] picks outside the operating systems' ephemeral ranges,
//!   so nothing the kernel hands out (a `bind(:0)`, the local end of an
//!   outgoing connection, celld's own `--internal-listen 127.0.0.1:0`) can
//!   take it between the pick and the child's bind, nor while the child is
//!   down for a restart on the same port.
//! - What is left is another explicit pick of the same port (another
//!   launcher, an unrelated program): [`on_free_port`] launches again on a
//!   fresh port when the child reports exactly that bind conflict, and fails
//!   at once on any other error.

#![allow(dead_code)]

use std::hash::{BuildHasher, Hasher};
use std::net::TcpListener;
use std::ops::Range;

/// Below every default ephemeral range (Linux 32768–60999; macOS, the BSDs
/// and Windows 49152–65535) and below the Kubernetes NodePort range
/// (30000–32767), above the common fixed dev ports.
pub const PORTS: Range<u16> = 20_000..30_000;

/// Launches tried by [`on_free_port`].
pub const ATTEMPTS: usize = 5;

/// A port in [`PORTS`] that nothing listens on now (probed by binding it).
pub fn free_port() -> u16 {
    for attempt in 0..1000u32 {
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        hasher.write_u32(attempt);
        let span = u64::from(PORTS.end - PORTS.start);
        let port = PORTS.start + (hasher.finish() % span) as u16;
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
    panic!("no free loopback port in {PORTS:?}");
}

/// Why one launch failed.
#[derive(Debug)]
pub enum LaunchError {
    /// The child could not bind the port it was given (another process
    /// holds it): worth a launch on another port.
    PortTaken(String),
    /// Anything else: retrying would only hide it.
    Failed(String),
}

impl std::fmt::Display for LaunchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PortTaken(why) | Self::Failed(why) => f.write_str(why),
        }
    }
}

/// `launch` on [`free_port`]s until it does not report
/// [`LaunchError::PortTaken`], at most [`ATTEMPTS`] times.
pub fn on_free_port<T>(launch: impl FnMut(u16) -> Result<T, LaunchError>) -> Result<T, String> {
    on_ports(std::iter::repeat_with(free_port), launch)
}

/// [`on_free_port`] over the given ports (tests inject a taken one).
pub fn on_ports<T>(
    ports: impl IntoIterator<Item = u16>,
    mut launch: impl FnMut(u16) -> Result<T, LaunchError>,
) -> Result<T, String> {
    let mut taken = Vec::new();
    for port in ports.into_iter().take(ATTEMPTS) {
        match launch(port) {
            Ok(value) => return Ok(value),
            Err(LaunchError::PortTaken(why)) => taken.push(format!("{port}: {why}")),
            Err(LaunchError::Failed(why)) => return Err(why),
        }
    }
    Err(format!(
        "every port was taken ({}): {}",
        taken.len(),
        taken.join("; ")
    ))
}

/// celld's output says its Worker listener could not bind `port` because
/// another socket holds it (`Error: bind --listen 127.0.0.1:{port}` caused
/// by `Address already in use`, os error 48 on macOS, 98 on Linux).
pub fn listener_taken(log: &str, port: u16) -> bool {
    let bind = format!("bind --listen 127.0.0.1:{port}");
    log.lines().any(|line| line.trim_end().ends_with(&bind))
        && log.contains("Address already in use")
}
