//! Read-only query service. It owns no aggregate repository; it reads the
//! projected relational tables through primary-key loads plus explicit
//! relationship includes.

use distributed::{InMemoryReadModelStore, ReadModelWorkspaceExt, TableStoreError};

use crate::distributed_read_model::read_models::{checkout_key, seat_key, CheckoutView, SeatView};

#[derive(Clone)]
pub struct CheckoutQueryService {
    store: InMemoryReadModelStore,
}

impl CheckoutQueryService {
    pub fn new(store: InMemoryReadModelStore) -> Self {
        Self { store }
    }

    /// Load the checkout screen with its audit steps and current seat row.
    pub async fn checkout_screen(
        &self,
        checkout_id: &str,
    ) -> Result<Option<CheckoutView>, TableStoreError> {
        let mut session = self.store.workspace();
        Ok(session
            .load::<CheckoutView>(checkout_key(checkout_id))
            .include("steps")
            .include("seat")
            .one()
            .await?
            .map(|view| view.data))
    }

    pub async fn seat(&self, seat_id: &str) -> Result<Option<SeatView>, TableStoreError> {
        let mut session = self.store.workspace();
        Ok(session
            .load::<SeatView>(seat_key(seat_id))
            .one()
            .await?
            .map(|view| view.data))
    }
}
