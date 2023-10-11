// Copyright 2023-, GraphOps and Semiotic Labs.
// SPDX-License-Identifier: Apache-2.0

use alloy_primitives::Address;
use eip_712_derive::{
    sign_typed, Bytes32, DomainSeparator, Eip712Domain, MemberVisitor, StructType,
};
use ethers::utils::hex;
use ethers_core::k256::ecdsa::SigningKey;
use ethers_core::types::U256;
use keccak_hash::keccak;
use secp256k1::SecretKey;
use std::convert::TryInto;
use toolshed::thegraph::DeploymentId;

/// An attestation signer tied to a specific allocation via its signer key
#[derive(Debug, Clone)]
pub struct AttestationSigner {
    subgraph_deployment_id: DeploymentId,
    domain_separator: DomainSeparator,
    signer: SecretKey,
}

impl AttestationSigner {
    pub fn new(
        chain_id: eip_712_derive::U256,
        dispute_manager: Address,
        signer: SecretKey,
        deployment_id: DeploymentId,
    ) -> Self {
        let bytes = hex::decode("a070ffb1cd7409649bf77822cce74495468e06dbfaef09556838bf188679b9c2")
            .unwrap();

        let salt: [u8; 32] = bytes.try_into().unwrap();

        let domain = Eip712Domain {
            name: "Graph Protocol".to_owned(),
            version: "0".to_owned(),
            chain_id,
            verifying_contract: eip_712_derive::Address(dispute_manager.into()),
            salt,
        };
        let domain_separator = DomainSeparator::new(&domain);

        Self {
            domain_separator,
            signer,
            subgraph_deployment_id: deployment_id,
        }
    }

    pub fn create_attestation(&self, request: &str, response: &str) -> Attestation {
        let request_cid = keccak(request).to_fixed_bytes();
        let response_cid = keccak(response).to_fixed_bytes();

        let receipt = Receipt {
            request_cid,
            response_cid,
            subgraph_deployment_id: *self.subgraph_deployment_id.0,
        };

        // Unwrap: This can only fail if the SecretKey is invalid.
        // Since it is of type SecretKey it has already been validated.
        let (rs, v) = sign_typed(&self.domain_separator, &receipt, self.signer.as_ref()).unwrap();

        let r = rs[0..32].try_into().unwrap();
        let s = rs[32..64].try_into().unwrap();

        Attestation {
            v,
            r,
            s,
            subgraph_deployment_id: *self.subgraph_deployment_id.0,
            request_cid,
            response_cid,
        }
    }
}

pub struct Receipt {
    request_cid: Bytes32,
    response_cid: Bytes32,
    subgraph_deployment_id: Bytes32,
}

impl StructType for Receipt {
    const TYPE_NAME: &'static str = "Receipt";
    fn visit_members<T: MemberVisitor>(&self, visitor: &mut T) {
        visitor.visit("requestCID", &self.request_cid);
        visitor.visit("responseCID", &self.response_cid);
        visitor.visit("subgraphDeploymentID", &self.subgraph_deployment_id);
    }
}

#[derive(Debug)]
pub struct Attestation {
    pub request_cid: Bytes32,
    pub response_cid: Bytes32,
    pub subgraph_deployment_id: Bytes32,
    pub v: u8,
    pub r: Bytes32,
    pub s: Bytes32,
}

/// Helper for creating an AttestationSigner
pub fn create_attestation_signer(
    chain_id: U256,
    dispute_manager_address: Address,
    signer: SigningKey,
    deployment_id: DeploymentId,
) -> anyhow::Result<AttestationSigner> {
    // Tedious conversions to the "indexer_native" types
    let mut chain_id_bytes = [0u8; 32];
    chain_id.to_big_endian(&mut chain_id_bytes);
    let signer = AttestationSigner::new(
        eip_712_derive::U256(chain_id_bytes),
        dispute_manager_address,
        secp256k1::SecretKey::from_slice(&signer.to_bytes())?,
        deployment_id,
    );
    Ok(signer)
}
