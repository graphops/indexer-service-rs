use log::debug;
use std::sync::Arc;
use std::sync::RwLock;

use anyhow::Result;
use ethereum_types::Address;
use sqlx::postgres::PgListener;
use sqlx::PgPool;
use tap_core;
use tap_core::tap_manager::SignedRAV;
use thiserror::Error;
use tokio;

pub struct RAVStorageAdapter {
    pgpool: PgPool,
    local_rav_storage: Arc<RwLock<Option<SignedRAV>>>,
    allocation_id: Address,
    rav_notifications_watcher: tokio::task::JoinHandle<Result<()>>,
}

#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("something went wrong: {error}")]
    AdapterError { error: String },
}

impl tap_core::adapters::rav_storage_adapter::RAVStorageAdapter for RAVStorageAdapter {
    type AdapterError = AdapterError;

    fn update_last_rav(&mut self, rav: SignedRAV) -> Result<(), Self::AdapterError> {
        let fut = sqlx::query!(
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
        .execute(&self.pgpool);

        // TODO: Make tap_core async
        tokio::runtime::Runtime::new()
            .map_err(|e| AdapterError::AdapterError {
                error: e.to_string(),
            })?
            .block_on(fut)
            .map_err(|e| AdapterError::AdapterError {
                error: e.to_string(),
            })?;

        Ok(())
    }
    fn last_rav(&self) -> Result<Option<SignedRAV>, Self::AdapterError> {
        Ok(self.local_rav_storage.read().unwrap().clone())
    }
}

impl RAVStorageAdapter {
    async fn retrieve_last_rav(&self) -> Result<()> {
        let latest_rav = sqlx::query!(
            r#"
                SELECT latest_rav
                FROM scalar_tap_latest_rav
                WHERE allocation_id = $1
            "#,
            self.allocation_id.to_string()
        )
        .fetch_optional(&self.pgpool)
        .await
        .map(|r| r.map(|r| r.latest_rav))?;

        if let Some(latest_rav) = latest_rav {
            let latest_rav: SignedRAV = serde_json::from_value(latest_rav)?;
            self.local_rav_storage.write().unwrap().replace(latest_rav);
        }

        Ok(())
    }

    pub async fn new(pgpool: PgPool, allocation_id: Address) -> Result<Self> {
        let local_rav_storage: Arc<RwLock<Option<SignedRAV>>> = Arc::new(RwLock::new(None));

        // Start rav notifications watcher
        let pgpool_clone = pgpool.clone(); // PgPool is ARC
        let rav_notifications_watcher = tokio::spawn(async move {
            let mut listener = PgListener::connect_with(&pgpool_clone).await?;
            listener.listen("scalar_tap_rav_notification").await?;
            loop {
                let notification = listener.recv().await?;
                debug!("Received notification: {:?}", notification);
            }
        });

        let rav_storage_adapter = RAVStorageAdapter {
            pgpool,
            local_rav_storage,
            allocation_id,
            rav_notifications_watcher,
        };

        // Try to retrieve latest rav from database
        Self::retrieve_last_rav(&rav_storage_adapter);

        Ok(rav_storage_adapter)
    }
}
