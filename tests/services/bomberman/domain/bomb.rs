use distributed::{sourced, Entity};

#[derive(Default, Clone)]
pub struct Bomb {
    pub entity: Entity,
    pub owner_id: String,
    pub x: i32,
    pub y: i32,
    pub ticks_remaining: u8,
    pub blast_radius: u8,
    pub exploded: bool,
}

#[sourced(entity)]
impl Bomb {
    #[event("initialized")]
    pub fn create(&mut self, id: String, owner_id: String, x: i32, y: i32, blast_radius: u8) {
        self.entity.set_id(&id);
        self.owner_id = owner_id;
        self.x = x;
        self.y = y;
        self.ticks_remaining = 3;
        self.blast_radius = blast_radius;
        self.exploded = false;
    }

    #[event("ticked", when = !self.exploded && self.ticks_remaining > 0)]
    pub fn tick(&mut self) {
        self.ticks_remaining -= 1;
    }

    #[event("exploded", when = !self.exploded)]
    pub fn explode(&mut self) {
        self.exploded = true;
    }

    pub fn is_ready_to_explode(&self) -> bool {
        !self.exploded && self.ticks_remaining == 0
    }
}
