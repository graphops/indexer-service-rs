// Copyright 2023-, Semiotic AI, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use alloy_primitives::Address;
use async_trait::async_trait;
use sqlx::PgPool;
use tap_core::adapters::receipt_checks_adapter::ReceiptChecksAdapter as ReceiptChecksAdapterTrait;
use tap_core::{eip_712_signed_message::EIP712SignedMessage, tap_receipt::Receipt};
use thiserror::Error;
use tokio::sync::RwLock;

use crate::escrow_adapter::EscrowAdapter;

#[derive(Debug)]
pub struct ReceiptChecksAdapter {
    pgpool: PgPool,
    query_appraisals: Option<Arc<RwLock<HashMap<u64, u128>>>>,
    allocation_ids: Arc<RwLock<HashSet<Address>>>,
    escrow_adapter: EscrowAdapter,
}

impl ReceiptChecksAdapter {
    pub fn new(
        pgpool: PgPool,
        query_appraisals: Option<Arc<RwLock<HashMap<u64, u128>>>>,
        allocation_ids: Arc<RwLock<HashSet<Address>>>,
        escrow_adapter: EscrowAdapter,
    ) -> Self {
        Self {
            pgpool,
            query_appraisals,
            allocation_ids,
            escrow_adapter,
        }
    }
}

#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("something went wrong: {error}")]
    AdapterError { error: String },
}

#[async_trait]
impl ReceiptChecksAdapterTrait for ReceiptChecksAdapter {
    type AdapterError = AdapterError;

    async fn is_unique(
        &self,
        receipt: &EIP712SignedMessage<Receipt>,
        receipt_id: u64,
    ) -> Result<bool, Self::AdapterError> {
        let record = sqlx::query!(
            r#"
                SELECT id
                FROM scalar_tap_receipts
                WHERE id != $1 and signature = $2
                LIMIT 1
            "#,
            TryInto::<i64>::try_into(receipt_id).map_err(|e| AdapterError::AdapterError {
                error: e.to_string(),
            })?,
            receipt.signature.to_string()
        )
        .fetch_optional(&self.pgpool)
        .await
        .map_err(|e| AdapterError::AdapterError {
            error: e.to_string(),
        })?;

        Ok(record.is_none())
    }

    async fn is_valid_allocation_id(
        &self,
        allocation_id: Address,
    ) -> Result<bool, Self::AdapterError> {
        let allocation_ids = self.allocation_ids.read().await;
        Ok(allocation_ids.contains(&allocation_id))
    }

    async fn is_valid_value(&self, value: u128, query_id: u64) -> Result<bool, Self::AdapterError> {
        let query_appraisals = self.query_appraisals.as_ref().expect(
            "Query appraisals should be initialized. The opposite should never happen when receipts value checking is enabled."
        );
        let query_appraisals_read = query_appraisals.read().await;
        let appraised_value =
            query_appraisals_read
                .get(&query_id)
                .ok_or_else(|| AdapterError::AdapterError {
                    error: "No appraised value found for query".to_string(),
                })?;

        if value != *appraised_value {
            return Ok(false);
        }
        Ok(true)
    }

    async fn is_valid_gateway_id(&self, gateway_id: Address) -> Result<bool, Self::AdapterError> {
        Ok(self.escrow_adapter.is_valid_gateway_id(gateway_id).await)
    }
}

#[cfg(test)]
mod test {
    use std::collections::{HashMap, HashSet};
    use std::str::FromStr;

    use faux::when;
    use tap_core::adapters::receipt_storage_adapter::ReceiptStorageAdapter as ReceiptStorageAdapterTrait;

    use crate::escrow_adapter;
    use crate::receipt_storage_adapter::ReceiptStorageAdapter;
    use crate::test_utils::{create_received_receipt, keys};

    use super::*;

    #[sqlx::test]
    async fn is_unique(pgpool: PgPool) {
        let allocation_id =
            Address::from_str("0xabababababababababababababababababababab").unwrap();
        let allocation_ids = Arc::new(RwLock::new(HashSet::new()));
        allocation_ids.write().await.insert(allocation_id);
        let (_, address) = keys();

        let query_appraisals: Arc<RwLock<HashMap<u64, u128>>> =
            Arc::new(RwLock::new(HashMap::new()));

        let gateway_escrow_balance = Arc::new(RwLock::new(HashMap::new()));
        gateway_escrow_balance.write().await.insert(address, 10000);
        let mut escrow_adapter = escrow_adapter::EscrowAdapter::faux();
        when!(escrow_adapter.is_valid_gateway_id(address)).then_return(true);

        let rav_storage_adapter = ReceiptStorageAdapter::new(pgpool.clone(), allocation_id);
        let receipt_checks_adapter = ReceiptChecksAdapter::new(
            pgpool.clone(),
            Some(query_appraisals),
            allocation_ids,
            escrow_adapter,
        );

        // Insert 3 unique receipts
        for i in 0..3 {
            let received_receipt = create_received_receipt(allocation_id, i, i, i as u128, i).await;
            let receipt_id = rav_storage_adapter
                .store_receipt(received_receipt.clone())
                .await
                .unwrap();

            assert!(receipt_checks_adapter
                .is_unique(&received_receipt.signed_receipt(), receipt_id)
                .await
                .unwrap());
        }

        // Insert a duplicate receipt
        let received_receipt = create_received_receipt(allocation_id, 1, 1, 1, 3).await;
        let receipt_id = rav_storage_adapter
            .store_receipt(received_receipt.clone())
            .await
            .unwrap();
        assert!(
            !(receipt_checks_adapter
                .is_unique(&received_receipt.signed_receipt(), receipt_id)
                .await
                .unwrap())
        );
    }
}
