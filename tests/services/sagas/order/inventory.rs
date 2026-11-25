use distributed::{sourced, Entity};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Inventory aggregate - tracks stock levels and reservations
#[derive(Default)]
pub struct Inventory {
    pub entity: Entity,
    sku: String,
    available: u32,
    reserved: u32,
    reservations: HashMap<String, u32>, // order_id -> quantity
}

#[allow(dead_code)]
#[sourced(entity)]
impl Inventory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn sku(&self) -> &str {
        &self.sku
    }

    pub fn available(&self) -> u32 {
        self.available
    }

    pub fn reserved(&self) -> u32 {
        self.reserved
    }

    pub fn can_reserve(&self, quantity: u32) -> bool {
        self.available >= quantity
    }

    pub fn reservation_for_order(&self, order_id: &str) -> Option<u32> {
        self.reservations.get(order_id).copied()
    }

    #[event("initialized")]
    pub fn initialize(&mut self, sku: String, initial_stock: u32) {
        self.entity.set_id(&sku);
        self.sku = sku;
        self.available = initial_stock;
    }

    #[event("stock_reserved", when = self.can_reserve(quantity))]
    pub fn reserve(&mut self, order_id: String, quantity: u32) {
        self.available -= quantity;
        self.reserved += quantity;
        self.reservations.insert(order_id, quantity);
    }

    #[event("reservation_released", when = self.reservations.contains_key(&order_id))]
    pub fn release_reservation(&mut self, order_id: String) {
        if let Some(quantity) = self.reservations.remove(&order_id) {
            self.available += quantity;
            self.reserved -= quantity;
        }
    }

    #[event("reservation_committed", when = self.reservations.contains_key(&order_id))]
    pub fn commit_reservation(&mut self, order_id: String) {
        if let Some(quantity) = self.reservations.remove(&order_id) {
            self.reserved -= quantity;
            // Stock is now sold, no longer available or reserved
        }
    }

    pub fn snapshot(&self) -> InventorySnapshot {
        InventorySnapshot {
            sku: self.sku.clone(),
            available: self.available,
            reserved: self.reserved,
            reservations: self.reservations.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InventorySnapshot {
    pub sku: String,
    pub available: u32,
    pub reserved: u32,
    pub reservations: HashMap<String, u32>,
}
