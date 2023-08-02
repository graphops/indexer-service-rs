// Copyright 2023-, Semiotic AI, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    num::TryFromIntError,
    ops::{Bound, RangeBounds},
};

use async_trait::async_trait;
use ethereum_types::Address;
use sqlx::{postgres::types::PgRange, types::BigDecimal, PgPool};
use tap_core::adapters::receipt_storage_adapter::ReceiptStorageAdapter as ReceiptStorageAdapterTrait;
use tap_core::tap_receipt::ReceivedReceipt;
use thiserror::Error;

pub struct ReceiptStorageAdapter {
    pgpool: PgPool,
    allocation_id: Address,
}

#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("something went wrong: {error}")]
    AdapterError { error: String },
}

impl From<TryFromIntError> for AdapterError {
    fn from(error: TryFromIntError) -> Self {
        AdapterError::AdapterError {
            error: error.to_string(),
        }
    }
}
impl From<sqlx::Error> for AdapterError {
    fn from(error: sqlx::Error) -> Self {
        AdapterError::AdapterError {
            error: error.to_string(),
        }
    }
}
impl From<serde_json::Error> for AdapterError {
    fn from(error: serde_json::Error) -> Self {
        AdapterError::AdapterError {
            error: error.to_string(),
        }
    }
}

// convert Bound<u64> to Bound<BigDecimal>
fn u64_bound_to_bigdecimal_bound(bound: Bound<&u64>) -> Bound<BigDecimal> {
    match bound {
        Bound::Included(start) => Bound::Included(BigDecimal::from(*start)),
        Bound::Excluded(start) => Bound::Excluded(BigDecimal::from(*start)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

// convert RangeBounds<u64> to PgRange<BigDecimal>
fn rangebounds_to_pgrange<R: RangeBounds<u64>>(range: R) -> PgRange<BigDecimal> {
    PgRange::<BigDecimal>::from((
        u64_bound_to_bigdecimal_bound(range.start_bound()),
        u64_bound_to_bigdecimal_bound(range.end_bound()),
    ))
}

#[async_trait]
impl ReceiptStorageAdapterTrait for ReceiptStorageAdapter {
    type AdapterError = AdapterError;

    async fn store_receipt(&self, receipt: ReceivedReceipt) -> Result<u64, Self::AdapterError> {
        let signed_receipt = receipt.signed_receipt();

        let record = sqlx::query!(
            r#"
                INSERT INTO scalar_tap_receipts (signature, allocation_id, timestamp_ns, received_receipt)
                VALUES ($1, $2, $3, $4)
                RETURNING id
            "#,
            signed_receipt.signature.to_string(),
            self.allocation_id.to_string(),
            BigDecimal::from(signed_receipt.message.timestamp_ns),
            serde_json::to_value(receipt)?
        ).fetch_one(&self.pgpool).await?;

        // id is BIGSERIAL, so it should be safe to cast to u64.
        let id: u64 = record.id.try_into()?;
        Ok(id)
    }

    async fn retrieve_receipts_in_timestamp_range<R: RangeBounds<u64> + Send>(
        &self,
        timestamp_range_ns: R,
    ) -> Result<Vec<(u64, ReceivedReceipt)>, Self::AdapterError> {
        let records = sqlx::query!(
            r#"
                SELECT id, received_receipt
                FROM scalar_tap_receipts
                WHERE allocation_id = $1 AND $2::numrange @> timestamp_ns
            "#,
            self.allocation_id.to_string(),
            rangebounds_to_pgrange(timestamp_range_ns),
        )
        .fetch_all(&self.pgpool)
        .await?;

        records
            .into_iter()
            .map(|record| {
                let id: u64 = record.id.try_into()?;
                let signed_receipt: ReceivedReceipt =
                    serde_json::from_value(record.received_receipt)?;
                Ok((id, signed_receipt))
            })
            .collect()
    }

    async fn update_receipt_by_id(
        &self,
        receipt_id: u64,
        receipt: ReceivedReceipt,
    ) -> Result<(), Self::AdapterError> {
        let _signed_receipt = receipt.signed_receipt();

        let _record = sqlx::query!(
            r#"
                UPDATE scalar_tap_receipts
                SET received_receipt = $1
                WHERE id = $2
            "#,
            serde_json::to_value(receipt)?,
            TryInto::<i64>::try_into(receipt_id)?
        )
        .fetch_one(&self.pgpool)
        .await?;

        Ok(())
    }

    async fn remove_receipts_in_timestamp_range<R: RangeBounds<u64> + Send>(
        &self,
        timestamp_ns: R,
    ) -> Result<(), Self::AdapterError> {
        sqlx::query!(
            r#"
                DELETE FROM scalar_tap_receipts
                WHERE $1::numrange @> timestamp_ns
            "#,
            rangebounds_to_pgrange(timestamp_ns),
        )
        .execute(&self.pgpool)
        .await?;
        Ok(())
    }
}

impl ReceiptStorageAdapter {
    pub fn new(pgpool: PgPool, allocation_id: Address) -> Self {
        Self {
            pgpool,
            allocation_id,
        }
    }
}

#[cfg(test)]
mod test {}
