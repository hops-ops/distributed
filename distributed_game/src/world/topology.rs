//! Zone hierarchy and tier-up event propagation.
//!
//! Zones can nest (province > city > borough > neighborhood). A
//! [`ZoneTopology`] names each zone's parent. A zone emits a tier-up event
//! with [`Ctx::emit_upward`](crate::Ctx::emit_upward); the driver
//! (`net::GameServer`) routes it to the parent — activating the parent on
//! demand, on another host through the directory if needed — and the
//! parent's host delivers it to [`Zone::on_child_event`](crate::Zone::on_child_event)
//! on a later tick. Nothing blocks: the child's tick only enqueues.
//!
//! After delivering, the parent's host forwards the event further up while
//! `levels` remain (`levels = 1` reaches only the parent; the default
//! reaches the root).

/// Parent links between zones.
pub trait ZoneTopology: Send + Sync + 'static {
    /// Parent zone id, None for a root.
    fn parent(&self, zone_id: &str) -> Option<String>;
}

/// `"eldoria.port.docks"` → parent `"eldoria.port"` → `"eldoria"` → root.
#[derive(Debug, Clone, Copy, Default)]
pub struct DottedTopology;

impl ZoneTopology for DottedTopology {
    fn parent(&self, zone_id: &str) -> Option<String> {
        zone_id
            .rsplit_once('.')
            .map(|(parent, _)| parent.to_string())
            .filter(|p| !p.is_empty())
    }
}

/// No hierarchy.
#[derive(Debug, Clone, Copy, Default)]
pub struct FlatTopology;

impl ZoneTopology for FlatTopology {
    fn parent(&self, _zone_id: &str) -> Option<String> {
        None
    }
}

impl<F> ZoneTopology for F
where
    F: Fn(&str) -> Option<String> + Send + Sync + 'static,
{
    fn parent(&self, zone_id: &str) -> Option<String> {
        self(zone_id)
    }
}

/// A tier-up event travelling from a child zone towards the root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildEvent {
    /// Zone that emitted it originally.
    pub origin_zone: String,
    /// Zone it was forwarded from (the direct child of the receiver).
    pub from_zone: String,
    /// Game-defined kind, e.g. `"boss_defeated"`.
    pub kind: String,
    /// Game-defined payload.
    pub payload: Vec<u8>,
    /// Origin tick.
    pub origin_tick: u64,
    /// Content version of the origin zone when emitted.
    pub content_version: String,
    /// Remaining levels to travel (>= 1 while in flight).
    pub levels: u8,
}

impl ChildEvent {
    /// Levels value that reaches the root.
    pub const TO_ROOT: u8 = u8::MAX;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dotted_parents() {
        let t = DottedTopology;
        assert_eq!(
            t.parent("eldoria.port.docks").as_deref(),
            Some("eldoria.port")
        );
        assert_eq!(t.parent("eldoria.port").as_deref(), Some("eldoria"));
        assert_eq!(t.parent("eldoria"), None);
        let f = |z: &str| (z == "a").then(|| "b".to_string());
        assert_eq!(f.parent("a").as_deref(), Some("b"));
    }
}
