use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use ethereum_types::Address;

use tap_core;

pub struct CollateralAdapter {
    gateway_collateral_storage: Arc<RwLock<HashMap<Address, u128>>>,
}

use thiserror::Error;
#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("something went wrong: {error}")]
    AdapterError { error: String },
}

impl tap_core::adapters::collateral_adapter::CollateralAdapter for CollateralAdapter {
    type AdapterError = AdapterError;

    fn get_available_collateral(&self, gateway_id: Address) -> Result<u128, Self::AdapterError> {
        todo!("Implement retrieval of available collateral from local storage");
    }

    fn subtract_collateral(
        &mut self,
        gateway_id: Address,
        value: u128,
    ) -> Result<(), Self::AdapterError> {
        todo!("Implement subtraction of collateral from local storage");
    }
}
