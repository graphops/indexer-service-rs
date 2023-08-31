// Copyright 2023-, GraphOps and Semiotic Labs.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use alloy_sol_types::Eip712Domain;
use ethereum_types::Address;
use log::error;
use sqlx::{any, PgPool};
use tap::{
    core::{adapters::escrow_adapter, tap_receipt::ReceiptCheck},
    escrow_adapter::EscrowAdapter,
    rav_storage_adapter::RAVStorageAdapter,
    receipt_checks_adapter::ReceiptChecksAdapter,
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
#[derive(Clone)]
struct TapManager {
    inner: Arc<RwLock<TapManagerInner>>,
    _update_loop_handle: Arc<tokio::task::JoinHandle<()>>,
}

struct TapManagerInner {
    managers: Arc<RwLock<HashMap<Address, Manager>>>,
    eligible_allocations: Arc<RwLock<HashSet<Address>>>,
    escrow_adapter: EscrowAdapter,
}

impl TapManager {
    pub fn new(
        pgpool: PgPool,
        allocation_monitor: allocation_monitor::AllocationMonitor,
        domain_separator: Eip712Domain,
        required_checks: Vec<ReceiptCheck>,
        starting_min_timestamp_ns: u64,
    ) -> Self {
        let eligible_allocations = Arc::new(RwLock::new(HashSet::new()));
        let update_loop_handle = tokio::spawn(Self::update_loop(
            allocation_monitor.clone(),
            eligible_allocations.clone(),
        ));

        let escrow_adapter = EscrowAdapter::new();

        Self {
            managers: Arc::new(RwLock::new(HashMap::new())),
            eligible_allocations,
            escrow_adapter,
            _update_loop_handle: Arc::new(update_loop_handle),
        }
    }

    async fn update_eligible_allocations(
        inner: &TapManagerInner,
    ) -> anyhow::Result<()> {
        let allocations_monitor_read = inner.allocation_monitor.get_eligible_allocations().await;
        let mut eligible_allocations_new = HashSet::with_capacity(allocations_monitor_read.len());
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
                managers_remove.push(allocation_id.clone());
            }
        }
        for allocation_id in managers_remove {
            managers_write.remove(&allocation_id);
        }

        // Add eligible allocations that are not already in managers
        for allocation_id in eligible_allocations_new.iter() {
            if !managers_write.contains_key(allocation_id) {
                let manager = Manager::new(
                    allocation_id.clone(),
                    inner.escrow_adapter.clone(),
                    ReceiptChecksAdapter::new(
                        inner.pgpool.clone(),
                        inner.allocation_id.clone(),
                        inner.domain_separator.clone(),
                        inner.required_checks.clone(),
                    ),
                    ReceiptStorageAdapter::new(inner.pgpool.clone(), allocation_id.clone()),
                    RAVStorageAdapter::new(inner.pgpool.clone(), allocation_id.clone()),
                );
                managers_write.insert(allocation_id.clone(), manager);
            }
        }

        *eligible_allocations.write().await = eligible_allocations_new;
        Ok(())
    }

    async fn update_loop(
        allocation_monitor: allocation_monitor::AllocationMonitor,
        eligible_allocations: Arc<RwLock<HashSet<Address>>>,
    ) {
        let mut watch_receiver = allocation_monitor.subscribe();

        loop {
            match watch_receiver.changed().await {
                Ok(_) => {
                    Self::update_eligible_allocations(&allocation_monitor, &eligible_allocations)
                        .await;
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
