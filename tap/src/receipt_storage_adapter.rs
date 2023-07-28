// Copyright 2023-, Semiotic AI, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    ops::RangeBounds,
    sync::{Arc, RwLock}, num::TryFromIntError, str::Bytes, vec,
};

use ethereum_types::Address;
use sqlx::PgPool;
use tap_core::tap_receipt::ReceivedReceipt;
use sqlx::types::BigDecimal;


pub struct ReceiptStorageAdapter {
    pgpool: PgPool,
    unique_id: u64,
    allocation_id: Address,
}

use thiserror::Error;
#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("something went wrong: {error}")]
    AdapterError { error: String },
}

impl tap_core::adapters::receipt_storage_adapter::ReceiptStorageAdapter for ReceiptStorageAdapter {
    type AdapterError = AdapterError;
    fn store_receipt(&mut self, receipt: ReceivedReceipt) -> Result<u64, Self::AdapterError> {
        let signed_receipt = receipt.signed_receipt();

        let fut = sqlx::query!(
            r#"
                INSERT INTO scalar_tap_receipts (signature, allocation_id, timestamp_ns, signed_receipt)
                VALUES ($1, $2, $3, $4)
                RETURNING id
            "#,
            signed_receipt.signature.to_string(),
            self.allocation_id.to_string(),
            BigDecimal::from(signed_receipt.message.timestamp_ns),
            serde_json::to_value(signed_receipt).map_err(|e| AdapterError::AdapterError {
                error: e.to_string(),
            })?
        ).fetch_one(&self.pgpool);

        // TODO: Make tap_core async
        let id = tokio::runtime::Runtime::new()
            .map_err(|e| AdapterError::AdapterError {
                error: e.to_string(),
            })?
            .block_on(fut)
            .map_err(|e| AdapterError::AdapterError {
                error: e.to_string(),
            })?
            .id;

        // id is BIGSERIAL, so it should be safe to cast to u64.
        let id: u64 = id.try_into().map_err(|e: TryFromIntError| AdapterError::AdapterError {
            error: e.to_string(),
        })?;
        Ok(id)
    }

    fn retrieve_receipts_in_timestamp_range<R: RangeBounds<u64>>(
        &self,
        timestamp_range_ns: R,
    ) -> Result<Vec<(u64, ReceivedReceipt)>, Self::AdapterError> {
        let receipt_storage =
            self.receipt_storage
                .read()
                .map_err(|e| Self::AdapterError::AdapterError {
                    error: e.to_string(),
                })?;
        Ok(receipt_storage
            .iter()
            .filter(|(_, rx_receipt)| {
                timestamp_range_ns.contains(&rx_receipt.signed_receipt.message.timestamp_ns)
            })
            .map(|(&id, rx_receipt)| (id, rx_receipt.clone()))
            .collect())
    }
    fn update_receipt_by_id(
        &mut self,
        receipt_id: u64,
        receipt: ReceivedReceipt,
    ) -> Result<(), Self::AdapterError> {
        let mut receipt_storage =
            self.receipt_storage
                .write()
                .map_err(|e| Self::AdapterError::AdapterError {
                    error: e.to_string(),
                })?;

        if !receipt_storage.contains_key(&receipt_id) {
            return Err(AdapterErrorMock::AdapterError {
                error: "Invalid receipt_id".to_owned(),
            });
        };

        receipt_storage.insert(receipt_id, receipt);
        self.unique_id += 1;
        Ok(())
    }
    fn remove_receipts_in_timestamp_range<R: RangeBounds<u64>>(
        &mut self,
        timestamp_ns: R,
    ) -> Result<(), Self::AdapterError> {
        let mut receipt_storage =
            self.receipt_storage
                .write()
                .map_err(|e| Self::AdapterError::AdapterError {
                    error: e.to_string(),
                })?;
        receipt_storage.retain(|_, rx_receipt| {
            !timestamp_ns.contains(&rx_receipt.signed_receipt.message.timestamp_ns)
        });
        Ok(())
    }
}
