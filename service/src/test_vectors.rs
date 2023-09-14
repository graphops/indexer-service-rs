// Copyright 2023-, GraphOps and Semiotic Labs.
// SPDX-License-Identifier: Apache-2.0

use std::str::FromStr;

use alloy_primitives::Address;
use ethers_core::types::U256;

use crate::common::{
    allocation::{Allocation, AllocationStatus, SubgraphDeployment},
    types::SubgraphDeploymentID,
};

pub const INDEXER_OPERATOR_MNEMONIC: &str =
    "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
pub const INDEXER_ADDRESS: &str = "0x1234567890123456789012345678901234567890";
pub const NETWORK_SUBGRAPH_ID: &str = "QmU7zqJyHSyUP3yFii8sBtHT8FaJn2WmUnRvwjAUTjwMBP";
pub const DISPUTE_MANAGER_ADDRESS: &str = "0xdeadbeefcafebabedeadbeefcafebabedeadbeef";

/// The allocation IDs below are generated using the mnemonic
/// "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about"
/// and the following epoch and index:
///
/// - (createdAtEpoch, 0)
/// - (createdAtEpoch-1, 0)
/// - (createdAtEpoch, 2)
/// - (createdAtEpoch-1, 1)
///
/// Using https://github.com/graphprotocol/indexer/blob/f8786c979a8ed0fae93202e499f5ce25773af473/packages/indexer-common/src/allocations/keys.ts#L41-L71
pub const ALLOCATIONS_QUERY_RESPONSE: &str = r#"
    {
        "data": {
            "indexer": {
                "activeAllocations": [
                    {
                        "id": "0xfa44c72b753a66591f241c7dc04e8178c30e13af",
                        "indexer": {
                            "id": "0xd75c4dbcb215a6cf9097cfbcc70aab2596b96a9c"
                        },
                        "allocatedTokens": "5081382841000000014901161",
                        "createdAtBlockHash": "0x99d3fbdc0105f7ccc0cd5bb287b82657fe92db4ea8fb58242dafb90b1c6e2adf",
                        "createdAtEpoch": 953,
                        "closedAtEpoch": null,
                        "subgraphDeployment": {
                            "id": "0xbbde25a2c85f55b53b7698b9476610c3d1202d88870e66502ab0076b7218f98a",
                            "deniedAt": 0,
                            "stakedTokens": "96183284152000000014901161",
                            "signalledTokens": "182832939554154667498047",
                            "queryFeesAmount": "19861336072168874330350"
                        }
                    },
                    {
                        "id": "0xdd975e30aafebb143e54d215db8a3e8fd916a701",
                        "indexer": {
                            "id": "0xd75c4dbcb215a6cf9097cfbcc70aab2596b96a9c"
                        },
                        "allocatedTokens": "601726452999999979510903",
                        "createdAtBlockHash": "0x99d3fbdc0105f7ccc0cd5bb287b82657fe92db4ea8fb58242dafb90b1c6e2adf",
                        "createdAtEpoch": 953,
                        "closedAtEpoch": null,
                        "subgraphDeployment": {
                            "id": "0xcda7fa0405d6fd10721ed13d18823d24b535060d8ff661f862b26c23334f13bf",
                            "deniedAt": 0,
                            "stakedTokens": "53885041676589999979510903",
                            "signalledTokens": "104257136417832003117925",
                            "queryFeesAmount": "2229358609434396563687"
                        }
                    }
                ],
                "recentlyClosedAllocations": [
                    {
                        "id": "0xa171cd12c3dde7eb8fe7717a0bcd06f3ffa65658",
                        "indexer": {
                            "id": "0xd75c4dbcb215a6cf9097cfbcc70aab2596b96a9c"
                        },
                        "allocatedTokens": "5247998688000000081956387",
                        "createdAtBlockHash": "0x6e7b7100c37f659236a029f87ce18914643995120f55ab5d01631f11f40fd887",
                        "createdAtEpoch": 940,
                        "closedAtEpoch": 953,
                        "subgraphDeployment": {
                            "id": "0xbbde25a2c85f55b53b7698b9476610c3d1202d88870e66502ab0076b7218f98a",
                            "deniedAt": 0,
                            "stakedTokens": "96183284152000000014901161",
                            "signalledTokens": "182832939554154667498047",
                            "queryFeesAmount": "19861336072168874330350"
                        }
                    },
                    {
                        "id": "0x69f961358846fdb64b04e1fd7b2701237c13cd9a",
                        "indexer": {
                            "id": "0xd75c4dbcb215a6cf9097cfbcc70aab2596b96a9c"
                        },
                        "allocatedTokens": "2502334654999999795109034",
                        "createdAtBlockHash": "0x6e7b7100c37f659236a029f87ce18914643995120f55ab5d01631f11f40fd887",
                        "createdAtEpoch": 940,
                        "closedAtEpoch": 953,
                        "subgraphDeployment": {
                            "id": "0xc064c354bc21dd958b1d41b67b8ef161b75d2246b425f68ed4c74964ae705cbd",
                            "deniedAt": 0,
                            "stakedTokens": "85450761241000000055879354",
                            "signalledTokens": "154944508746646550301048",
                            "queryFeesAmount": "4293718622418791971020"
                        }
                    }
                ]
            }
        }
    }
"#;

/// These are the expected json-serialized contents of the value returned by
/// AllocationMonitor::current_eligible_allocations with the values above at epoch threshold 940.
pub fn expected_eligible_allocations() -> Vec<Allocation> {
    vec![
        Allocation {
            id: Address::from_str("0xfa44c72b753a66591f241c7dc04e8178c30e13af").unwrap(),
            indexer: Address::from_str("0xd75c4dbcb215a6cf9097cfbcc70aab2596b96a9c").unwrap(),
            allocated_tokens: U256::from_str("5081382841000000014901161").unwrap(),
            created_at_block_hash:
                "0x99d3fbdc0105f7ccc0cd5bb287b82657fe92db4ea8fb58242dafb90b1c6e2adf".to_string(),
            created_at_epoch: 953,
            closed_at_epoch: None,
            subgraph_deployment: SubgraphDeployment {
                id: SubgraphDeploymentID::new(
                    "0xbbde25a2c85f55b53b7698b9476610c3d1202d88870e66502ab0076b7218f98a",
                )
                .unwrap(),
                denied_at: Some(0),
                staked_tokens: U256::from_str("96183284152000000014901161").unwrap(),
                signalled_tokens: U256::from_str("182832939554154667498047").unwrap(),
                query_fees_amount: U256::from_str("19861336072168874330350").unwrap(),
            },
            status: AllocationStatus::Null,
            closed_at_epoch_start_block_hash: None,
            previous_epoch_start_block_hash: None,
            poi: None,
            query_fee_rebates: None,
            query_fees_collected: None,
        },
        Allocation {
            id: Address::from_str("0xdd975e30aafebb143e54d215db8a3e8fd916a701").unwrap(),
            indexer: Address::from_str("0xd75c4dbcb215a6cf9097cfbcc70aab2596b96a9c").unwrap(),
            allocated_tokens: U256::from_str("601726452999999979510903").unwrap(),
            created_at_block_hash:
                "0x99d3fbdc0105f7ccc0cd5bb287b82657fe92db4ea8fb58242dafb90b1c6e2adf".to_string(),
            created_at_epoch: 953,
            closed_at_epoch: None,
            subgraph_deployment: SubgraphDeployment {
                id: SubgraphDeploymentID::new(
                    "0xcda7fa0405d6fd10721ed13d18823d24b535060d8ff661f862b26c23334f13bf",
                )
                .unwrap(),
                denied_at: Some(0),
                staked_tokens: U256::from_str("53885041676589999979510903").unwrap(),
                signalled_tokens: U256::from_str("104257136417832003117925").unwrap(),
                query_fees_amount: U256::from_str("2229358609434396563687").unwrap(),
            },
            status: AllocationStatus::Null,
            closed_at_epoch_start_block_hash: None,
            previous_epoch_start_block_hash: None,
            poi: None,
            query_fee_rebates: None,
            query_fees_collected: None,
        },
        Allocation {
            id: Address::from_str("0xa171cd12c3dde7eb8fe7717a0bcd06f3ffa65658").unwrap(),
            indexer: Address::from_str("0xd75c4dbcb215a6cf9097cfbcc70aab2596b96a9c").unwrap(),
            allocated_tokens: U256::from_str("5247998688000000081956387").unwrap(),
            created_at_block_hash:
                "0x6e7b7100c37f659236a029f87ce18914643995120f55ab5d01631f11f40fd887".to_string(),
            created_at_epoch: 940,
            closed_at_epoch: Some(953),
            subgraph_deployment: SubgraphDeployment {
                id: SubgraphDeploymentID::new(
                    "0xbbde25a2c85f55b53b7698b9476610c3d1202d88870e66502ab0076b7218f98a",
                )
                .unwrap(),
                denied_at: Some(0),
                staked_tokens: U256::from_str("96183284152000000014901161").unwrap(),
                signalled_tokens: U256::from_str("182832939554154667498047").unwrap(),
                query_fees_amount: U256::from_str("19861336072168874330350").unwrap(),
            },
            status: AllocationStatus::Null,
            closed_at_epoch_start_block_hash: None,
            previous_epoch_start_block_hash: None,
            poi: None,
            query_fee_rebates: None,
            query_fees_collected: None,
        },
        Allocation {
            id: Address::from_str("0x69f961358846fdb64b04e1fd7b2701237c13cd9a").unwrap(),
            indexer: Address::from_str("0xd75c4dbcb215a6cf9097cfbcc70aab2596b96a9c").unwrap(),
            allocated_tokens: U256::from_str("2502334654999999795109034").unwrap(),
            created_at_block_hash:
                "0x6e7b7100c37f659236a029f87ce18914643995120f55ab5d01631f11f40fd887".to_string(),
            created_at_epoch: 940,
            closed_at_epoch: Some(953),
            subgraph_deployment: SubgraphDeployment {
                id: SubgraphDeploymentID::new(
                    "0xc064c354bc21dd958b1d41b67b8ef161b75d2246b425f68ed4c74964ae705cbd",
                )
                .unwrap(),
                denied_at: Some(0),
                staked_tokens: U256::from_str("85450761241000000055879354").unwrap(),
                signalled_tokens: U256::from_str("154944508746646550301048").unwrap(),
                query_fees_amount: U256::from_str("4293718622418791971020").unwrap(),
            },
            status: AllocationStatus::Null,
            closed_at_epoch_start_block_hash: None,
            previous_epoch_start_block_hash: None,
            poi: None,
            query_fee_rebates: None,
            query_fees_collected: None,
        },
    ]
}
