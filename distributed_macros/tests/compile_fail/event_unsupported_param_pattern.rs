use distributed::{sourced, Entity};

struct Point {
    entity: Entity,
    x: u8,
    y: u8,
}

#[sourced(entity, aggregate_type = "point")]
impl Point {
    // A tuple pattern has no single name to record in the event payload, so
    // the parameter cannot round-trip through replay. Parameters must be
    // plain identifiers.
    #[event("moved")]
    pub fn moved(&mut self, (x, y): (u8, u8)) {
        self.x = x;
        self.y = y;
    }
}

fn main() {}
