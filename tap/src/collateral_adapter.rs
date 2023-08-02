/// TODO: Implement the collateral adapter. This is only a basic mock implementation.
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use ethereum_types::Address;
use log::warn;

pub struct CollateralAdapter {
    _gateway_collateral_storage: Arc<RwLock<HashMap<Address, u128>>>,
}

use thiserror::Error;
#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("something went wrong: {error}")]
    AdapterError { error: String },
}

#[async_trait]
impl tap_core::adapters::collateral_adapter::CollateralAdapter for CollateralAdapter {
    type AdapterError = AdapterError;

    async fn get_available_collateral(
        &self,
        _gateway_id: Address,
    ) -> Result<u128, Self::AdapterError> {
        // TODO: Implement retrieval of available collateral from local storage
        warn!("The TAP collateral adapter is not implemented yet. Do not use this in production!");
        Ok(u128::MAX)
    }

    async fn subtract_collateral(
        &self,
        _gateway_id: Address,
        _value: u128,
    ) -> Result<(), Self::AdapterError> {
        // TODO: Implement subtraction of collateral from local storage
        warn!("The TAP collateral adapter is not implemented yet. Do not use this in production!");
        Ok(())
    }
}
