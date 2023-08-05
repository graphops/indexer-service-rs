use async_trait::async_trait;
use log::debug;
use std::sync::Arc;
use std::sync::RwLock;

use anyhow::Result;
use ethereum_types::Address;
use sqlx::postgres::PgListener;
use sqlx::PgPool;

use tap_core::adapters::rav_storage_adapter::RAVStorageAdapter as RAVStorageAdapterTrait;
use tap_core::tap_manager::SignedRAV;
use thiserror::Error;

pub struct RAVStorageAdapter {
    pgpool: PgPool,
    local_rav_storage: Arc<RwLock<Option<SignedRAV>>>,
    allocation_id: Address,
    #[allow(dead_code)] // Silence "field is never read"
    rav_notifications_watcher_handle: tokio::task::JoinHandle<Result<()>>,
}

#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("something went wrong: {error}")]
    AdapterError { error: String },
}

#[async_trait]
impl RAVStorageAdapterTrait for RAVStorageAdapter {
    type AdapterError = AdapterError;

    async fn update_last_rav(&self, rav: SignedRAV) -> Result<(), Self::AdapterError> {
        let _fut = sqlx::query!(
            r#"
                INSERT INTO scalar_tap_latest_rav (allocation_id, latest_rav)
                VALUES ($1, $2)
                ON CONFLICT (allocation_id)
                DO UPDATE SET latest_rav = $2
            "#,
            self.allocation_id.to_string(),
            serde_json::to_value(rav).map_err(|e| AdapterError::AdapterError {
                error: e.to_string()
            })?
        )
        .execute(&self.pgpool)
        .await
        .map_err(|e| AdapterError::AdapterError {
            error: e.to_string(),
        })?;

        Ok(())
    }
    async fn last_rav(&self) -> Result<Option<SignedRAV>, Self::AdapterError> {
        Ok(self.local_rav_storage.read().unwrap().clone())
    }
}

impl RAVStorageAdapter {
    /// Static version of `retrieve_last_rav` that can be used by a `tokio::spawn`ed task.
    async fn retrieve_last_rav_static(
        pgpool: PgPool,
        allocation_id: Address,
        local_rav_storage: Arc<RwLock<Option<SignedRAV>>>,
    ) -> Result<()> {
        let latest_rav = sqlx::query!(
            r#"
                SELECT latest_rav
                FROM scalar_tap_latest_rav
                WHERE allocation_id = $1
            "#,
            allocation_id.to_string()
        )
        .fetch_optional(&pgpool)
        .await
        .map(|r| r.map(|r| r.latest_rav))?;

        if let Some(latest_rav) = latest_rav {
            let latest_rav: SignedRAV = serde_json::from_value(latest_rav)?;
            local_rav_storage.write().unwrap().replace(latest_rav);
        }

        Ok(())
    }

    pub async fn retrieve_last_rav(&self) -> Result<()> {
        RAVStorageAdapter::retrieve_last_rav_static(
            self.pgpool.clone(),
            self.allocation_id,
            self.local_rav_storage.clone(),
        )
        .await
    }

    /// This function is meant to be spawned as a task that listens for new RAV notifications from the database.
    async fn rav_notifications_watcher(
        pgpool: PgPool,
        allocation_id: Address,
        local_rav_storage: Arc<RwLock<Option<SignedRAV>>>,
    ) -> Result<()> {
        // TODO: make this async thread more robust with a retry mechanism and a backoff
        let mut listener = PgListener::connect_with(&pgpool).await?;
        listener.listen("scalar_tap_rav_notification").await?;
        loop {
            let notification = listener.recv().await?;
            debug!("Received notification: {:?}", notification);
            RAVStorageAdapter::retrieve_last_rav_static(
                pgpool.clone(),
                allocation_id,
                local_rav_storage.clone(),
            )
            .await?;
        }
    }

    pub async fn new(pgpool: PgPool, allocation_id: Address) -> Result<Self> {
        let local_rav_storage: Arc<RwLock<Option<SignedRAV>>> = Arc::new(RwLock::new(None));

        let rav_storage_adapter = RAVStorageAdapter {
            pgpool: pgpool.clone(),
            local_rav_storage: local_rav_storage.clone(),
            allocation_id,
            rav_notifications_watcher_handle: tokio::spawn(
                RAVStorageAdapter::rav_notifications_watcher(
                    pgpool.clone(),
                    allocation_id,
                    local_rav_storage.clone(),
                ),
            ),
        };

        rav_storage_adapter.retrieve_last_rav().await.unwrap();

        Ok(rav_storage_adapter)
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use tap_core::adapters::rav_storage_adapter::RAVStorageAdapter as RAVStorageAdapterTrait;

    use crate::test_utils::create_rav;

    use super::*;

    #[sqlx::test]
    async fn update_and_retrieve_rav(pool: PgPool) {
        let allocation_id =
            Address::from_str("0xabababababababababababababababababababab").unwrap();
        let timestamp_ns = u64::MAX - 10;
        let value_aggregate = u128::MAX;
        let rav_storage_adapter = RAVStorageAdapter::new(pool.clone(), allocation_id)
            .await
            .unwrap();

        // Insert a rav
        let mut new_rav = create_rav(allocation_id, timestamp_ns, value_aggregate).await;
        rav_storage_adapter
            .update_last_rav(new_rav.clone())
            .await
            .unwrap();

        // Wait for the Postgres RAV notification to be processed
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Should trigger a retrieve_last_rav
        // So eventually the last rav should be the one we inserted
        let last_rav = rav_storage_adapter.last_rav().await.unwrap();
        assert_eq!(new_rav, last_rav.unwrap());

        // Update the RAV 3 times in quick succession
        for i in 0..3 {
            new_rav = create_rav(
                allocation_id,
                timestamp_ns + i,
                value_aggregate - (i as u128),
            )
            .await;
            rav_storage_adapter
                .update_last_rav(new_rav.clone())
                .await
                .unwrap();
        }

        // Check that the last rav is the last one we inserted
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        let last_rav = rav_storage_adapter.last_rav().await.unwrap();
        assert_eq!(new_rav, last_rav.unwrap());
    }
}
