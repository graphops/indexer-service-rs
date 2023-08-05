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
        Bound::Included(val) => Bound::Included(BigDecimal::from(*val)),
        Bound::Excluded(val) => Bound::Excluded(BigDecimal::from(*val)),
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
                RETURNING id
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
mod test {
    use std::str::FromStr;

    use anyhow::Result;
    use sqlx::PgPool;

    use crate::test_utils::create_received_receipt;

    use super::*;

    #[sqlx::test]
    async fn store_receipt(pgpool: PgPool) {
        let allocation_id =
            Address::from_str("0xabababababababababababababababababababab").unwrap();
        let received_receipt = create_received_receipt(allocation_id, 0, 42, 124, 0).await;

        let storage_adapter = ReceiptStorageAdapter::new(pgpool, allocation_id);

        let receipt_id = storage_adapter
            .store_receipt(received_receipt.clone())
            .await
            .unwrap();

        let recovered_received_receipt_vec = storage_adapter
            .retrieve_receipts_in_timestamp_range(..)
            .await
            .unwrap();
        assert_eq!(recovered_received_receipt_vec.len(), 1);

        let (recovered_receipt_id, recovered_received_receipt) = &recovered_received_receipt_vec[0];
        assert_eq!(*recovered_receipt_id, receipt_id);
        // Check that the recovered receipt is the same as the original receipt using serde_json.
        assert_eq!(
            serde_json::to_value(recovered_received_receipt).unwrap(),
            serde_json::to_value(received_receipt).unwrap()
        );
    }

    /// This function compares a local receipts vector filter by timestamp range (we assume that the stdlib
    /// implementation is correct) with the receipts vector retrieved from the database using
    /// retrieve_receipts_in_timestamp_range.
    async fn retrieve_range_and_check<R: RangeBounds<u64> + Send>(
        storage_adapter: &ReceiptStorageAdapter,
        received_receipt_vec: &[(u64, ReceivedReceipt)],
        range: R,
    ) -> Result<()> {
        // Filtering the received receipts by timestamp range
        let received_receipt_vec: Vec<(u64, ReceivedReceipt)> = received_receipt_vec
            .iter()
            .filter(|(_, received_receipt)| {
                range.contains(&received_receipt.signed_receipt().message.timestamp_ns)
            })
            .cloned()
            .collect();

        // Retrieving receipts in timestamp range from the database
        let mut recovered_received_receipt_vec = storage_adapter
            .retrieve_receipts_in_timestamp_range(range)
            .await?;

        // Sorting the recovered receipts by id
        recovered_received_receipt_vec.sort_by(|(id1, _), (id2, _)| id1.cmp(id2));

        // Checking
        for (received_receipt, recovered_received_receipt) in received_receipt_vec
            .iter()
            .zip(recovered_received_receipt_vec.iter())
        {
            if serde_json::to_value(recovered_received_receipt)?
                != serde_json::to_value(received_receipt)?
            {
                return Err(anyhow::anyhow!("Receipts do not match"));
            }
        }

        Ok(())
    }

    async fn remove_range_and_check<R: RangeBounds<u64> + Send>(
        storage_adapter: &ReceiptStorageAdapter,
        received_receipt_vec: &[ReceivedReceipt],
        range: R,
    ) -> Result<()> {
        // Storing the receipts
        let mut received_receipt_id_vec = Vec::new();
        for received_receipt in received_receipt_vec.iter() {
            received_receipt_id_vec.push(
                storage_adapter
                    .store_receipt(received_receipt.clone())
                    .await
                    .unwrap(),
            );
        }

        // zip the 2 vectors together
        let received_receipt_vec = received_receipt_id_vec
            .into_iter()
            .zip(received_receipt_vec.iter())
            .collect::<Vec<_>>();

        // Remove the received receipts by timestamp range
        let received_receipt_vec: Vec<(u64, &ReceivedReceipt)> = received_receipt_vec
            .iter()
            .filter(|(_, received_receipt)| {
                !range.contains(&received_receipt.signed_receipt().message.timestamp_ns)
            })
            .cloned()
            .collect();

        // Removing the received receipts in timestamp range from the database
        storage_adapter
            .remove_receipts_in_timestamp_range(range)
            .await?;

        // Retrieving all receipts
        let mut recovered_received_receipt_vec = storage_adapter
            .retrieve_receipts_in_timestamp_range(..)
            .await?;

        // Sorting the recovered receipts by id
        recovered_received_receipt_vec.sort_by(|(id1, _), (id2, _)| id1.cmp(id2));

        // Checking
        for (received_receipt, recovered_received_receipt) in received_receipt_vec
            .iter()
            .zip(recovered_received_receipt_vec.iter())
        {
            if serde_json::to_value(recovered_received_receipt)?
                != serde_json::to_value(received_receipt)?
            {
                return Err(anyhow::anyhow!("Receipts do not match"));
            }
        }

        // Removing the rest of the receipts
        storage_adapter
            .remove_receipts_in_timestamp_range(..)
            .await?;

        // Checking that there are no receipts left
        let recovered_received_receipt_vec = storage_adapter
            .retrieve_receipts_in_timestamp_range(..)
            .await?;
        assert_eq!(recovered_received_receipt_vec.len(), 0);

        Ok(())
    }

    #[sqlx::test]
    async fn retrieve_receipts_in_timestamp_range(pgpool: PgPool) {
        let allocation_id =
            Address::from_str("0xabababababababababababababababababababab").unwrap();
        let storage_adapter = ReceiptStorageAdapter::new(pgpool, allocation_id);

        // Creating 10 receipts with timestamps 42 to 51
        let mut received_receipt_vec = Vec::new();
        for i in 0..10 {
            received_receipt_vec.push(
                create_received_receipt(allocation_id, i + 684, i + 42, (i + 124).into(), i).await,
            );
        }

        // Storing the receipts
        let mut received_receipt_id_vec = Vec::new();
        for received_receipt in received_receipt_vec.iter() {
            received_receipt_id_vec.push(
                storage_adapter
                    .store_receipt(received_receipt.clone())
                    .await
                    .unwrap(),
            );
        }

        // zip the 2 vectors together
        let received_receipt_vec = received_receipt_id_vec
            .into_iter()
            .zip(received_receipt_vec.into_iter())
            .collect::<Vec<_>>();

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, ..)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, ..45)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, ..51)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, ..75)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, ..=45)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, ..=51)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, ..=75)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, 21..=45)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, 45..=45)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, 21..=51)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, 45..=51)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, 21..=75)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, 45..=75)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, 51..=75)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, 70..=75)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, 21..45)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, 45..45)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, 21..51)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, 45..51)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, 21..75)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, 45..75)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, 51..75)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, 70..75)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, 21..)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, 45..)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, 21..)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, 45..)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, 21..)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, 45..)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, 51..)
                .await
                .is_ok()
        );

        assert!(
            retrieve_range_and_check(&storage_adapter, &received_receipt_vec, 70..)
                .await
                .is_ok()
        );
    }

    #[sqlx::test]
    async fn remove_receipts_in_timestamp_range(pgpool: PgPool) {
        let allocation_id =
            Address::from_str("0xabababababababababababababababababababab").unwrap();
        let storage_adapter = ReceiptStorageAdapter::new(pgpool, allocation_id);

        // Creating 10 receipts with timestamps 42 to 51
        let mut received_receipt_vec = Vec::new();
        for i in 0..10 {
            received_receipt_vec.push(
                create_received_receipt(allocation_id, i + 684, i + 42, (i + 124).into(), i).await,
            );
        }

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, ..)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, ..45)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, ..51)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, ..75)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, ..=45)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, ..=51)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, ..=75)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, 21..=45)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, 45..=45)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, 21..=51)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, 45..=51)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, 21..=75)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, 45..=75)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, 51..=75)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, 70..=75)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, 21..45)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, 45..45)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, 21..51)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, 45..51)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, 21..75)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, 45..75)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, 51..75)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, 70..75)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, 21..)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, 45..)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, 21..)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, 45..)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, 21..)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, 45..)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, 51..)
                .await
                .is_ok()
        );

        assert!(
            remove_range_and_check(&storage_adapter, &received_receipt_vec, 70..)
                .await
                .is_ok()
        );
    }

    #[sqlx::test]
    async fn update_receipt_by_id(pgpool: PgPool) {
        let allocation_id =
            Address::from_str("0xabababababababababababababababababababab").unwrap();
        let storage_adapter = ReceiptStorageAdapter::new(pgpool, allocation_id);

        // Creating 10 receipts with timestamps 42 to 51
        let mut received_receipt_vec = Vec::new();
        for i in 0..10 {
            received_receipt_vec.push(
                create_received_receipt(allocation_id, i + 684, i + 42, (i + 124).into(), i).await,
            );
        }

        // Storing the receipts
        let mut received_receipt_id_vec = Vec::new();
        for received_receipt in received_receipt_vec.iter() {
            received_receipt_id_vec.push(
                storage_adapter
                    .store_receipt(received_receipt.clone())
                    .await
                    .unwrap(),
            );
        }

        // zip the 2 vectors together
        let mut received_receipt_vec = received_receipt_id_vec
            .into_iter()
            .zip(received_receipt_vec.into_iter())
            .collect::<Vec<_>>();

        // updating a receipt using an non-existing id
        assert!(storage_adapter
            .update_receipt_by_id(123456, received_receipt_vec[0].1.clone())
            .await
            .is_err());

        // updating a receipt using an existing id
        storage_adapter
            .update_receipt_by_id(received_receipt_vec[4].0, received_receipt_vec[1].1.clone())
            .await
            .unwrap();
        // doing the same in received_receipt_vec
        received_receipt_vec[4].1 = received_receipt_vec[1].1.clone();

        // compare the local vector with the one in the database
        let mut recovered_received_receipt_vec = storage_adapter
            .retrieve_receipts_in_timestamp_range(..)
            .await
            .unwrap();
        // Sorting the recovered receipts by id
        recovered_received_receipt_vec.sort_by(|(id1, _), (id2, _)| id1.cmp(id2));
        // Check
        for (received_receipt, recovered_received_receipt) in received_receipt_vec
            .iter()
            .zip(recovered_received_receipt_vec.iter())
        {
            assert_eq!(
                serde_json::to_value(recovered_received_receipt).unwrap(),
                serde_json::to_value(received_receipt).unwrap()
            )
        }
    }
}
