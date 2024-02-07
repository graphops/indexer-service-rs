// Copyright 2023-, GraphOps and Semiotic Labs.
// SPDX-License-Identifier: Apache-2.0

pub mod address;
pub mod allocations;
pub mod attestations;
pub mod escrow_accounts;
pub mod graphql;
pub mod indexer_errors;
pub mod indexer_service;
pub mod metrics;
pub mod signature_verification;
pub mod subgraph_client;
pub mod tap_manager;

#[cfg(test)]
mod test_vectors;

pub mod prelude {
    pub use super::allocations::{
        monitor::indexer_allocations, Allocation, AllocationStatus, SubgraphDeployment,
    };
    pub use super::attestations::{
        dispute_manager::dispute_manager, signer::AttestationSigner, signers::attestation_signers,
    };
    pub use super::escrow_accounts::escrow_accounts;
    pub use super::indexer_errors;
    pub use super::subgraph_client::{DeploymentDetails, Query, QueryVariables, SubgraphClient};
    pub use super::tap_manager::TapManager;
}
