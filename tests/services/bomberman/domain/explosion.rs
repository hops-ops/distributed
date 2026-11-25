use distributed::{sourced, Entity};

#[derive(Default, Clone)]
pub struct Explosion {
    pub entity: Entity,
    pub bomb_id: String,
    pub owner: String,
    pub center: (i32, i32),
    pub blast_radius: u8,
    pub rings: Vec<Vec<(i32, i32)>>,
    pub current_ring: usize,
    pub active: bool,
}

#[sourced(entity)]
impl Explosion {
    #[event("started")]
    pub fn start(
        &mut self,
        id: String,
        bomb_id: String,
        owner: String,
        center: (i32, i32),
        blast_radius: u8,
        rings: Vec<Vec<(i32, i32)>>,
    ) {
        self.entity.set_id(&id);
        self.bomb_id = bomb_id;
        self.owner = owner;
        self.center = center;
        self.blast_radius = blast_radius;
        self.rings = rings;
        self.current_ring = 0;
        self.active = true;
    }

    #[event("expanded", when = self.active && !self.is_fully_expanded())]
    pub fn expand(&mut self) {
        self.current_ring += 1;
    }

    #[event("dissipated", when = self.active)]
    pub fn dissipate(&mut self) {
        self.active = false;
    }

    pub fn is_fully_expanded(&self) -> bool {
        self.rings.is_empty() || self.current_ring >= self.rings.len() - 1
    }

    pub fn newly_reached_cells(&self) -> &[(i32, i32)] {
        if self.current_ring < self.rings.len() {
            &self.rings[self.current_ring]
        } else {
            &[]
        }
    }

    pub fn all_active_cells(&self) -> Vec<(i32, i32)> {
        let end = (self.current_ring + 1).min(self.rings.len());
        self.rings[..end]
            .iter()
            .flat_map(|r| r.iter().copied())
            .collect()
    }
}
