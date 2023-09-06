// Copyright 2023-, GraphOps and Semiotic Labs.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use alloy_primitives::Address;
use alloy_sol_types::Eip712Domain;
use log::error;
use sqlx::PgPool;
use tap::{
    core::tap_receipt::ReceiptCheck, escrow_adapter::EscrowAdapter,
    rav_storage_adapter::RAVStorageAdapter, receipt_checks_adapter::ReceiptChecksAdapter,
    receipt_storage_adapter::ReceiptStorageAdapter,
};
use tokio::sync::RwLock;

use crate::allocation_monitor;

type Manager = tap::core::tap_manager::Manager<
    EscrowAdapter,
    ReceiptChecksAdapter,
    ReceiptStorageAdapter,
    RAVStorageAdapter,
>;

// TODO: Have this implement the allocation_ids storage and updates. This should also
//       maintain a hashmap of Monitor instances, keyed by allocation_id.
#[derive(Clone, Debug)]
pub struct TapManager {
    inner: Arc<TapManagerInner>,
    _update_loop_handle: Arc<tokio::task::JoinHandle<()>>,
}

#[derive(Clone)]
struct TapManagerInner {
    allocation_monitor: allocation_monitor::AllocationMonitor,
    pgpool: PgPool,
    managers: Arc<RwLock<HashMap<Address, Manager>>>,
    eligible_allocations: Arc<RwLock<HashSet<alloy_primitives::Address>>>,
    escrow_adapter: EscrowAdapter,
    domain_separator: Eip712Domain,
}

// impl custom Debug that ignores `Manager`
impl std::fmt::Debug for TapManagerInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TapManagerInner")
            .field("allocation_monitor", &self.allocation_monitor)
            .field("pgpool", &self.pgpool)
            .field("eligible_allocations", &self.eligible_allocations)
            .field("escrow_adapter", &self.escrow_adapter)
            .field("domain_separator", &self.domain_separator)
            .finish_non_exhaustive()
    }
}

impl TapManager {
    pub fn new(
        pgpool: PgPool,
        allocation_monitor: allocation_monitor::AllocationMonitor,
        domain_separator: Eip712Domain,
        _required_checks: Vec<ReceiptCheck>,
        _starting_min_timestamp_ns: u64,
    ) -> Self {
        let eligible_allocations = Arc::new(RwLock::new(HashSet::new()));
        let escrow_adapter = EscrowAdapter::new();

        let inner = Arc::new(TapManagerInner {
            allocation_monitor,
            pgpool,
            managers: Arc::new(RwLock::new(HashMap::new())),
            eligible_allocations,
            escrow_adapter,
            domain_separator,
        });

        let update_loop_handle = tokio::spawn(Self::update_loop(inner.clone()));

        Self {
            inner,
            _update_loop_handle: Arc::new(update_loop_handle),
        }
    }

    async fn update_eligible_allocations(inner: &Arc<TapManagerInner>) -> anyhow::Result<()> {
        let allocations_monitor_read = inner.allocation_monitor.get_eligible_allocations().await;
        let mut eligible_allocations_new: HashSet<alloy_primitives::Address> =
            HashSet::with_capacity(allocations_monitor_read.len());
        for allocation in allocations_monitor_read.iter() {
            if !eligible_allocations_new.insert(allocation.id) {
                return Err(anyhow::anyhow!(
                    "Duplicate allocation id: {}",
                    allocation.id
                ));
            }
        }

        // Remove allocations that are no longer eligible from managers
        let mut managers_write = inner.managers.write().await;
        let mut managers_remove = Vec::new();
        for allocation_id in managers_write.keys() {
            if !eligible_allocations_new.contains(allocation_id) {
                managers_remove.push(*allocation_id);
            }
        }
        for allocation_id in managers_remove {
            managers_write.remove(&allocation_id);
        }

        // Add eligible allocations that are not already in managers
        for allocation_id in eligible_allocations_new.iter() {
            if !managers_write.contains_key(allocation_id) {
                // One manager per allocation
                let manager = Manager::new(
                    inner.domain_separator.clone(),
                    inner.escrow_adapter.clone(),
                    ReceiptChecksAdapter::new(
                        inner.pgpool.clone(),
                        None,
                        inner.eligible_allocations.clone(),
                        inner.escrow_adapter.clone(),
                    ),
                    RAVStorageAdapter::new(inner.pgpool.clone(), *allocation_id).await?,
                    ReceiptStorageAdapter::new(inner.pgpool.clone(), *allocation_id),
                    vec![],
                    42,
                );
                managers_write.insert(*allocation_id, manager);
            }
        }

        *inner.eligible_allocations.write().await = eligible_allocations_new;
        Ok(())
    }

    async fn update_loop(inner: Arc<TapManagerInner>) {
        let mut watch_receiver = inner.allocation_monitor.subscribe();

        loop {
            match watch_receiver.changed().await {
                Ok(_) => {
                    Self::update_eligible_allocations(&inner)
                        .await
                        .unwrap_or_else(|e| {
                            error!("Error updating eligible allocations: {}", e);
                        });
                }
                Err(e) => {
                    error!(
                        "Error receiving allocation monitor subscription update: {}",
                        e
                    );
                }
            }
        }
    }
}
