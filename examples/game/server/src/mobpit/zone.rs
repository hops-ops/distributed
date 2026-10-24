//! The Mob Pit room zone lives in `mobpit_rooms` (portable: the cells
//! worker hosts each room as a celld Durable Object). Re-exported here for
//! the client surface (`run.sh gen`), the native tests and the bots.

pub use mobpit_rooms::*;
