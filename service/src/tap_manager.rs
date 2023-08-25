// Copyright 2023-, GraphOps and Semiotic Labs.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use alloy_sol_types::Eip712Domain;
use sqlx::PgPool;
use tap::{
    core::{tap_manager::Manager, tap_receipt::ReceiptCheck},
    escrow_adapter::EscrowAdapter,
    rav_storage_adapter::RAVStorageAdapter,
    receipt_checks_adapter::ReceiptChecksAdapter,
    receipt_storage_adapter::ReceiptStorageAdapter,
};
use tokio::sync::RwLock;

#[derive(Clone)]
struct TapManager {
    manager:
        Arc<Manager<EscrowAdapter, ReceiptChecksAdapter, ReceiptStorageAdapter, RAVStorageAdapter>>,
}

impl TapManager {
    pub fn new(
        pgpool: PgPool,
        domain_separator: Eip712Domain,
        required_checks: Vec<ReceiptCheck>,
        starting_min_timestamp_ns: u64,
    ) -> Self {
        let escrow_adapter = EscrowAdapter {
            _gateway_escrow_storage: Arc::new(RwLock::new(HashMap::new())),
        };
        let receipt_checks_adapter = ReceiptChecksAdapter::new(
            pgpool.clone(),
            Arc::new(RwLock::new(HashMap::new())),
            Arc::new(RwLock::new(HashSet::new())),
            Arc::new(RwLock::new(HashSet::new())),
        );
        let rav_storage_adapter = RAVStorageAdapter::new(pgpool.clone(), Address::default())
            .await
            .unwrap();
        let receipt_storage_adapter = ReceiptStorageAdapter::new();

        Self {
            manager: Arc::new(Manager::new(
                domain_separator,
                escrow_adapter,
                receipt_checks_adapter,
                rav_storage_adapter,
                receipt_storage_adapter,
                required_checks,
                starting_min_timestamp_ns,
            )),
        }
    }
}
