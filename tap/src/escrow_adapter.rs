/// TODO: Implement the escrow adapter. This is only a basic mock implementation.
use std::{collections::HashMap, sync::Arc};

use alloy_primitives::Address;
use async_trait::async_trait;
use thiserror::Error;

use tap_core::adapters::escrow_adapter::EscrowAdapter as EscrowAdapterTrait;
use tokio::sync::RwLock;

/// This is Arc internally, so it can be cloned and shared between threads.
#[cfg_attr(test, faux::create)]
#[derive(Clone, Debug)]
pub struct EscrowAdapter {
    gateway_escrow_balance: Arc<RwLock<HashMap<Address, u128>>>,
    gateway_pending_fees: Arc<RwLock<HashMap<Address, u128>>>,
}

#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("something went wrong: {error}")]
    AdapterError { error: String },
}

// TODO: Implement escrow subgraph polling.
#[cfg_attr(test, faux::methods)]
impl EscrowAdapter {
    pub fn new() -> Self {
        Self {
            gateway_escrow_balance: Arc::new(RwLock::new(HashMap::new())),
            gateway_pending_fees: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn is_valid_gateway_id(&self, gateway_id: Address) -> bool {
        self.gateway_escrow_balance
            .read()
            .await
            .contains_key(&gateway_id)
    }
}

#[cfg_attr(test, faux::methods)]
impl Default for EscrowAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg_attr(test, faux::methods)]
#[async_trait]
impl EscrowAdapterTrait for EscrowAdapter {
    type AdapterError = AdapterError;

    async fn get_available_escrow(&self, gateway_id: Address) -> Result<u128, AdapterError> {
        let balance = self
            .gateway_escrow_balance
            .read()
            .await
            .get(&gateway_id)
            .copied()
            .ok_or(AdapterError::AdapterError {
                error: format!(
                    "Gateway {} not found in escrow balances map, could not get available escrow.",
                    gateway_id
                )
                .to_string(),
            })?;
        let fees = self
            .gateway_pending_fees
            .read()
            .await
            .get(&gateway_id)
            .copied()
            .ok_or(AdapterError::AdapterError {
                error: format!(
                    "Gateway {} not found in pending fees map, could not get available escrow.",
                    gateway_id
                )
                .to_string(),
            })?;

        Ok(balance - fees)
    }

    async fn subtract_escrow(&self, gateway_id: Address, value: u128) -> Result<(), AdapterError> {
        let current_available_escrow = self.get_available_escrow(gateway_id).await?;

        let mut fees_write = self.gateway_pending_fees.write().await;

        let fees = fees_write
            .get_mut(&gateway_id)
            .ok_or(AdapterError::AdapterError {
                error: format!(
                "Gateway {} not found in pending fees map, could not subtract available escrow.",
                gateway_id
            )
                .to_string(),
            })?;

        if current_available_escrow < value {
            return Err(AdapterError::AdapterError {
                error: format!(
                    "Gateway {} does not have enough escrow to subtract {} from {}.",
                    gateway_id, value, *fees
                )
                .to_string(),
            });
        }

        *fees += value;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use super::*;

    #[tokio::test]
    async fn test_subtract_escrow() {
        let gateway_id = Address::from_str("0xdeadbeefcafebabedeadbeefcafebabadeadbeef").unwrap();
        let gateway_escrow_balance = Arc::new(RwLock::new(HashMap::new()));
        let gateway_pending_fees = Arc::new(RwLock::new(HashMap::new()));

        let adapter = _FauxOriginal_EscrowAdapter {
            gateway_escrow_balance: gateway_escrow_balance.clone(),
            gateway_pending_fees: gateway_pending_fees.clone(),
        };

        gateway_escrow_balance
            .write()
            .await
            .insert(gateway_id, 1000);
        gateway_pending_fees.write().await.insert(gateway_id, 500);

        adapter
            .subtract_escrow(gateway_id, 500)
            .await
            .expect("Subtract escrow.");

        let available_escrow = adapter
            .get_available_escrow(gateway_id)
            .await
            .expect("Get available escrow.");
        assert_eq!(available_escrow, 0);
    }

    #[tokio::test]
    async fn test_subtract_escrow_overflow() {
        let gateway_id = Address::from_str("0xdeadbeefcafebabedeadbeefcafebabadeadbeef").unwrap();
        let gateway_escrow_balance = Arc::new(RwLock::new(HashMap::new()));
        let gateway_pending_fees = Arc::new(RwLock::new(HashMap::new()));

        let adapter = _FauxOriginal_EscrowAdapter {
            gateway_escrow_balance: gateway_escrow_balance.clone(),
            gateway_pending_fees: gateway_pending_fees.clone(),
        };

        gateway_escrow_balance
            .write()
            .await
            .insert(gateway_id, 1000);
        gateway_pending_fees.write().await.insert(gateway_id, 500);

        adapter
            .subtract_escrow(gateway_id, 250)
            .await
            .expect("Subtract escrow.");

        assert!(adapter.subtract_escrow(gateway_id, 251).await.is_err());

        let available_escrow = adapter
            .get_available_escrow(gateway_id)
            .await
            .expect("Get available escrow.");
        assert_eq!(available_escrow, 250);
    }
}
