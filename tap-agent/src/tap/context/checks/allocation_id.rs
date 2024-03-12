// Copyright 2023-, GraphOps and Semiotic Labs.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use alloy_primitives::Address;
use anyhow::anyhow;
use eventuals::{Eventual, EventualExt};
use indexer_common::subgraph_client::{Query, SubgraphClient};
use tap_core::receipt::{
    checks::{Check, CheckResult},
    Checking, ReceiptWithState,
};
use tokio::time::sleep;
use tracing::error;

use crate::config;

pub struct AllocationId {
    tap_allocation_redeemed: Eventual<bool>,
    allocation_id: Address,
}

impl AllocationId {
    pub fn new(
        sender_id: Address,
        allocation_id: Address,
        escrow_subgraph: &'static SubgraphClient,
        config: &'static config::Cli,
    ) -> Self {
        let tap_allocation_redeemed = tap_allocation_redeemed_eventual(
            allocation_id,
            sender_id,
            config.ethereum.indexer_address,
            escrow_subgraph,
            config.escrow_subgraph.escrow_syncing_interval_ms,
        );

        Self {
            tap_allocation_redeemed,
            allocation_id,
        }
    }
}

#[async_trait::async_trait]
impl Check for AllocationId {
    async fn check(&self, receipt: &ReceiptWithState<Checking>) -> CheckResult {
        let allocation_id = receipt.signed_receipt().message.allocation_id;
        // TODO: Remove the if block below? Each TAP Monitor is specific to an allocation
        // ID. So the receipts that are received here should already have been filtered by
        // allocation ID.
        if allocation_id != self.allocation_id {
            return Err(anyhow!("Receipt allocation_id different from expected: allocation_id: {}, expected_allocation_id: {}", allocation_id, self.allocation_id));
        };

        // Check that the allocation ID is not redeemed yet for this consumer
        match self.tap_allocation_redeemed.value().await {
            Ok(false) => Ok(()),
            Ok(true) => Err(anyhow!("Allocation {} already redeemed", allocation_id)),
            Err(e) => Err(anyhow!(
                "Could not get allocation escrow redemption status from eventual: {:?}",
                e
            )),
        }
    }
}

fn tap_allocation_redeemed_eventual(
    allocation_id: Address,
    sender_address: Address,
    indexer_address: Address,
    escrow_subgraph: &'static SubgraphClient,
    escrow_subgraph_polling_interval_ms: u64,
) -> Eventual<bool> {
    #[derive(serde::Deserialize)]
    struct AllocationResponse {
        #[allow(dead_code)]
        id: String,
    }

    #[derive(serde::Deserialize)]
    struct TransactionsResponse {
        transactions: Vec<AllocationResponse>,
    }

    eventuals::timer(Duration::from_millis(escrow_subgraph_polling_interval_ms)).map_with_retry(
        move |_| async move {
            let response = escrow_subgraph
                .query::<TransactionsResponse>(Query::new_with_variables(
                    r#"
                            query (
                                $sender_id: ID!,
                                $receiver_id: ID!,
                                $allocation_id: String!
                            ) {
                                transactions(
                                    where: {
                                        and: [
                                            { type: "redeem" }
                                            { sender_: { id: $sender_id } }
                                            { receiver_: { id: $receiver_id } }
                                            { allocationID: $allocation_id }
                                        ]
                                    }
                                ) {
                                    allocationID
                                    sender {
                                        id
                                    }
                                }
                            }
                        "#,
                    [
                        ("sender_id", sender_address.to_string().into()),
                        ("receiver_id", indexer_address.to_string().into()),
                        ("allocation_id", allocation_id.to_string().into()),
                    ],
                ))
                .await
                .map_err(|e| e.to_string())?;

            response
                .map_err(|e| e.to_string())
                .map(|data| !data.transactions.is_empty())
        },
        move |error: String| {
            error!(
                "Failed to check the escrow redeem status for allocation {} and sender {}: {}",
                allocation_id, sender_address, error
            );
            sleep(Duration::from_millis(escrow_subgraph_polling_interval_ms).div_f32(2.))
        },
    )
}
