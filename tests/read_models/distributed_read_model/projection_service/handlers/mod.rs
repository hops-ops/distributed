//! Projection handlers. The projection service dispatches each domain event by
//! event type to the handler that owns the matching read-model rows.

pub mod checkout;
pub mod seat;

use distributed::microsvc::HandlerError;
use distributed::TableStoreError;

pub fn read_model_error(err: TableStoreError) -> HandlerError {
    HandlerError::Repository(err.into())
}
