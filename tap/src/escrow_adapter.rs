/// TODO: Implement the escrow adapter. This is only a basic mock implementation.
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use ethereum_types::Address;
use log::warn;
use tap_core::adapters::escrow_adapter::EscrowAdapter as EscrowAdapterTrait;

pub struct EscrowAdapter {
    _gateway_escrow_storage: Arc<RwLock<HashMap<Address, u128>>>,
}

use thiserror::Error;
#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("something went wrong: {error}")]
    AdapterError { error: String },
}

#[async_trait]
impl EscrowAdapterTrait for EscrowAdapter {
    type AdapterError = AdapterError;

    async fn get_available_escrow(&self, _gateway_id: Address) -> Result<u128, Self::AdapterError> {
        // TODO: Implement retrieval of available escrow from local storage
        warn!("The TAP escrow adapter is not implemented yet. Do not use this in production!");
        Ok(u128::MAX)
    }

    async fn subtract_escrow(
        &self,
        _gateway_id: Address,
        _value: u128,
    ) -> Result<(), Self::AdapterError> {
        // TODO: Implement subtraction of escrow from local storage
        warn!("The TAP escrow adapter is not implemented yet. Do not use this in production!");
        Ok(())
    }
}
