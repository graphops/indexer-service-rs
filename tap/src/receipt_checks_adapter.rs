// Copyright 2023-, Semiotic AI, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use ethereum_types::Address;

use sqlx::PgPool;
use tap_core::adapters::receipt_checks_adapter::ReceiptChecksAdapter as ReceiptChecksAdapterTrait;
use tap_core::{eip_712_signed_message::EIP712SignedMessage, tap_receipt::Receipt};

pub struct ReceiptChecksAdapter {
    pgpool: PgPool,
    query_appraisals: Arc<RwLock<HashMap<u64, u128>>>,
    allocation_ids: Arc<RwLock<HashSet<Address>>>,
    gateway_ids: Arc<RwLock<HashSet<Address>>>,
}

impl ReceiptChecksAdapter {
    pub fn new(
        pgpool: PgPool,
        query_appraisals: Arc<RwLock<HashMap<u64, u128>>>,
        allocation_ids: Arc<RwLock<HashSet<Address>>>,
        gateway_ids: Arc<RwLock<HashSet<Address>>>,
    ) -> Self {
        Self {
            pgpool,
            query_appraisals,
            allocation_ids,
            gateway_ids,
        }
    }
}

#[async_trait]
impl ReceiptChecksAdapterTrait for ReceiptChecksAdapter {
    async fn is_unique(&self, receipt: &EIP712SignedMessage<Receipt>, receipt_id: u64) -> bool {
        // TODO: Proper error handling - requires changes in TAP Core
        let record = sqlx::query!(
            r#"
                SELECT id
                FROM scalar_tap_receipts
                WHERE id != $1 and signature = $2
                LIMIT 1
            "#,
            TryInto::<i64>::try_into(receipt_id).unwrap(),
            receipt.signature.to_string()
        )
        .fetch_optional(&self.pgpool)
        .await
        .unwrap();

        record.is_none()
    }

    async fn is_valid_allocation_id(&self, allocation_id: Address) -> bool {
        // TODO: Proper error handling - requires changes in TAP Core
        let allocation_ids = self.allocation_ids.read().unwrap();
        allocation_ids.contains(&allocation_id)
    }

    async fn is_valid_value(&self, value: u128, query_id: u64) -> bool {
        // TODO: Proper error handling - requires changes in TAP Core
        let query_appraisals = self.query_appraisals.read().unwrap();
        let appraised_value = query_appraisals.get(&query_id).unwrap();

        if value != *appraised_value {
            return false;
        }
        true
    }

    async fn is_valid_gateway_id(&self, gateway_id: Address) -> bool {
        // TODO: Proper error handling - requires changes in TAP Core
        let gateway_ids = self.gateway_ids.read().unwrap();
        gateway_ids.contains(&gateway_id)
    }
}

#[cfg(test)]
mod test {
    use std::collections::{HashMap, HashSet};
    use std::str::FromStr;

    use ethereum_types::Address;
    use ethers_signers::Signer;
    use tap_core::adapters::receipt_storage_adapter::ReceiptStorageAdapter as ReceiptStorageAdapterTrait;

    use crate::receipt_storage_adapter::ReceiptStorageAdapter;
    use crate::test_utils::{create_received_receipt, keys};

    use super::*;

    #[sqlx::test]
    async fn is_unique(pgpool: PgPool) {
        let allocation_id =
            Address::from_str("0xabababababababababababababababababababab").unwrap();
        let allocation_ids = Arc::new(RwLock::new(HashSet::new()));
        allocation_ids.write().unwrap().insert(allocation_id);
        let (wallet, _) = keys();

        let query_appraisals: Arc<RwLock<HashMap<u64, u128>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let gateway_ids = Arc::new(RwLock::new(HashSet::new()));
        gateway_ids.write().unwrap().insert(wallet.address());

        let rav_storage_adapter = ReceiptStorageAdapter::new(pgpool.clone(), allocation_id);
        let receipt_checks_adapter = ReceiptChecksAdapter::new(
            pgpool.clone(),
            query_appraisals,
            allocation_ids,
            gateway_ids,
        );

        // Insert 3 unique receipts
        for i in 0..3 {
            let received_receipt = create_received_receipt(allocation_id, i, i, i as u128, i).await;
            let receipt_id = rav_storage_adapter
                .store_receipt(received_receipt.clone())
                .await
                .unwrap();

            assert!(
                receipt_checks_adapter
                    .is_unique(&received_receipt.signed_receipt(), receipt_id)
                    .await
            );
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
                .await)
        );
    }
}
