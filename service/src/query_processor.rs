// Copyright 2023-, GraphOps and Semiotic Labs.
// SPDX-License-Identifier: Apache-2.0

use ethers_core::types::{Signature, U256};
use log::error;
use serde::{Deserialize, Serialize};
use tap_core::tap_manager::SignedReceipt;
use toolshed::thegraph::DeploymentId;

use indexer_common::prelude::{AttestationSigner, AttestationSigners};

use crate::graph_node::GraphNodeInstance;
use crate::tap_manager::TapManager;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    #[serde(rename = "graphQLResponse")]
    pub graphql_response: String,
    pub attestation: Option<Signature>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnattestedQueryResult {
    #[serde(rename = "graphQLResponse")]
    pub graphql_response: String,
    pub attestable: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Response<T> {
    pub result: T,
    pub status: i64,
}

/// Free query do not need signature, receipt, signers
/// Also ignore metrics for now
/// Later add along with PaidQuery
#[derive(Debug)]
pub struct FreeQuery {
    pub subgraph_deployment_id: DeploymentId,
    pub query: String,
}

/// Paid query needs subgraph_deployment_id, query, receipt
pub struct PaidQuery {
    pub subgraph_deployment_id: DeploymentId,
    pub query: String,
    pub receipt: String,
}

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error(transparent)]
    Transport(#[from] reqwest::Error),
    #[error("The subgraph is in a failed state")]
    IndexingError,
    #[error("Bad or invalid entity data found in the subgraph: {}", .0.to_string())]
    BadData(anyhow::Error),
    #[error("Unknown error: {0}")]
    Other(anyhow::Error),
}

#[derive(Clone, Debug)]
pub struct QueryProcessor {
    graph_node: GraphNodeInstance,
    attestation_signers: AttestationSigners,
    tap_manager: TapManager,
}

impl QueryProcessor {
    pub fn new(
        graph_node: GraphNodeInstance,
        attestation_signers: AttestationSigners,
        tap_manager: TapManager,
    ) -> QueryProcessor {
        QueryProcessor {
            graph_node,
            attestation_signers,
            tap_manager,
        }
    }

    pub async fn execute_free_query(
        &self,
        query: FreeQuery,
    ) -> Result<Response<UnattestedQueryResult>, QueryError> {
        let response = self
            .graph_node
            .subgraph_query_raw(&query.subgraph_deployment_id, query.query)
            .await?;

        Ok(Response {
            result: response,
            status: 200,
        })
    }

    pub async fn execute_paid_query(
        &self,
        query: PaidQuery,
    ) -> Result<Response<QueryResult>, QueryError> {
        let PaidQuery {
            subgraph_deployment_id,
            query,
            receipt,
        } = query;

        // TODO: Emit IndexerErrorCode::IE031 on error
        let parsed_receipt: SignedReceipt = serde_json::from_str(&receipt)
            .map_err(|e| QueryError::Other(anyhow::Error::from(e)))?;

        let allocation_id = parsed_receipt.message.allocation_id;

        self.tap_manager
            .verify_and_store_receipt(parsed_receipt)
            .await?;

        let signers = self.attestation_signers.read().await;
        let signer = signers.get(&allocation_id).ok_or_else(|| {
            QueryError::Other(anyhow::anyhow!(
                "No signer found for allocation id {}",
                allocation_id
            ))
        })?;

        let response = self
            .graph_node
            .subgraph_query_raw(&subgraph_deployment_id, query.clone())
            .await?;

        let attestation_signature = response
            .attestable
            .then(|| Self::create_attestation(signer, query, &response));

        Ok(Response {
            result: QueryResult {
                graphql_response: response.graphql_response,
                attestation: attestation_signature,
            },
            status: 200,
        })
    }

    fn create_attestation(
        signer: &AttestationSigner,
        query: String,
        response: &UnattestedQueryResult,
    ) -> Signature {
        let attestation = signer.create_attestation(&query, &response.graphql_response);
        Signature {
            r: U256::from_big_endian(&attestation.r),
            s: U256::from_big_endian(&attestation.s),
            v: attestation.v as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use alloy_primitives::Address;
    use hex_literal::hex;

    use indexer_common::prelude::{
        attestation_signer_for_allocation, create_attestation_signer, Allocation, AllocationStatus,
        SubgraphDeployment,
    };
    use lazy_static::lazy_static;

    use super::*;

    const INDEXER_OPERATOR_MNEMONIC: &str = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
    const INDEXER_ADDRESS: &str = "0x1234567890123456789012345678901234567890";

    lazy_static! {
        static ref DEPLOYMENT_ID: DeploymentId = DeploymentId(
            "0xc064c354bc21dd958b1d41b67b8ef161b75d2246b425f68ed4c74964ae705cbd"
                .parse()
                .unwrap(),
        );
    }

    #[test]
    fn paid_query_attestation() {
        let subgraph_deployment = SubgraphDeployment {
            id: *DEPLOYMENT_ID,
            denied_at: None,
            staked_tokens: U256::from(0),
            signalled_tokens: U256::from(0),
            query_fees_amount: U256::from(0),
        };

        let allocation = &Allocation {
            id: Address::from_str("0x4CAF2827961262ADEF3D0Ad15C341e40c21389a4").unwrap(),
            status: AllocationStatus::Null,
            subgraph_deployment,
            indexer: Address::from_str(INDEXER_ADDRESS).unwrap(),
            allocated_tokens: U256::from(100),
            created_at_epoch: 940,
            created_at_block_hash: String::from(""),
            closed_at_epoch: None,
            closed_at_epoch_start_block_hash: None,
            previous_epoch_start_block_hash: None,
            poi: None,
            query_fee_rebates: None,
            query_fees_collected: None,
        };

        let allocation_key =
            attestation_signer_for_allocation(INDEXER_OPERATOR_MNEMONIC, allocation).unwrap();
        let attestation_signer = create_attestation_signer(
            U256::from(1),
            Address::from_str("0xdeadbeefcafebabedeadbeefcafebabedeadbeef").unwrap(),
            allocation_key,
            *DEPLOYMENT_ID,
        )
        .unwrap();

        let attestation = QueryProcessor::create_attestation(
            &attestation_signer,
            "test input".to_string(),
            &UnattestedQueryResult {
                graphql_response: "test output".to_string(),
                attestable: true,
            },
        );

        // Values generated using https://github.com/graphprotocol/indexer/blob/f8786c979a8ed0fae93202e499f5ce25773af473/packages/indexer-native/lib/index.d.ts#L44
        let expected_signature = Signature {
            v: 27,
            r: hex!("a0c83c0785e2223ac1ea1eb9e4ffd4ca867275469a7b73dab24f39ddcdec5466").into(),
            s: hex!("4d0457efea889f2ec7ffcc7ff9b408428d0691356f34b01f419f7674d0eb4ddf").into(),
        };

        assert_eq!(attestation, expected_signature);
    }
}
