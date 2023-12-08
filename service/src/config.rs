// Copyright 2023-, GraphOps and Semiotic Labs.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use alloy_primitives::Address;
use figment::{
    providers::{Format, Toml},
    Figment,
};
use indexer_common::indexer_service::http::IndexerServiceConfig;
use serde::{Deserialize, Serialize};
use thegraph::types::DeploymentId;

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    // pub ethereum: Ethereum,
    // pub receipts: Receipts,
    // pub indexer_infrastructure: IndexerInfrastructure,
    // pub postgres: Postgres,
    // pub network_subgraph: NetworkSubgraph,
    // pub escrow_subgraph: EscrowSubgraph,
    pub common: IndexerServiceConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct Ethereum {
    // #[clap(
    //     long,
    //     value_name = "ethereum-node-provider",
    //     env = "ETH_NODE",
    //     help = "Ethereum node or provider URL"
    // )]
    pub ethereum: String,
    // #[clap(
    //     long,
    //     value_name = "ethereum-polling-interval",
    //     env = "ETHEREUM_POLLING_INTERVAL",
    //     default_value_t = 4000,
    //     help = "Polling interval for the Ethereum provider (ms)"
    // )]
    pub ethereum_polling_interval: usize,
    // #[clap(
    //     long,
    //     value_name = "mnemonic",
    //     env = "MNEMONIC",
    //     help = "Mnemonic for the operator wallet"
    // )]
    pub mnemonic: String,
    // #[clap(
    //     long,
    //     value_name = "indexer-address",
    //     env = "INDEXER_ADDRESS",
    //     help = "Ethereum address of the indexer"
    // )]
    pub indexer_address: Address,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct Receipts {
    // #[clap(
    //     long,
    //     value_name = "receipts-verifier-chain-id",
    //     env = "RECEIPTS_VERIFIER_CHAIN_ID",
    //     help = "Scalar TAP verifier chain ID"
    // )]
    pub receipts_verifier_chain_id: u64,
    // #[clap(
    //     long,
    //     value_name = "receipts-verifier-address",
    //     env = "RECEIPTS_VERIFIER_ADDRESS",
    //     help = "Scalar TAP verifier contract address"
    // )]
    pub receipts_verifier_address: Address,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct IndexerInfrastructure {
    // #[clap(
    //     long,
    //     value_name = "port",
    //     env = "PORT",
    //     default_value_t = 7600,
    //     help = "Port to serve queries at"
    // )]
    pub port: u32,
    // #[clap(
    //     long,
    //     value_name = "metrics-port",
    //     env = "METRICS_PORT",
    //     default_value_t = 7300,
    //     help = "Port to serve Prometheus metrics at"
    // )]
    pub metrics_port: u16,
    // #[clap(
    //     long,
    //     value_name = "graph-node-query-endpoint",
    //     env = "GRAPH_NODE_QUERY_ENDPOINT",
    //     default_value_t = String::from("http://0.0.0.0:8000"),
    //     help = "Graph node GraphQL HTTP service endpoint"
    // )]
    pub graph_node_query_endpoint: String,
    // #[clap(
    //     long,
    //     value_name = "graph-node-status-endpoint",
    //     env = "GRAPH_NODE_STATUS_ENDPOINT",
    //     default_value_t = String::from("http://0.0.0.0:8030"),
    //     help = "Graph node endpoint for the index node server"
    // )]
    pub graph_node_status_endpoint: String,
    // #[clap(
    //     long,
    //     value_name = "log-level",
    //     env = "LOG_LEVEL",
    //     value_enum,
    //     help = "Log level in RUST_LOG format"
    // )]
    pub log_level: Option<String>,
    // #[clap(
    //     long,
    //     value_name = "gcloud-profiling",
    //     env = "GCLOUD_PROFILING",
    //     default_value_t = false,
    //     help = "Whether to enable Google Cloud profiling"
    // )]
    pub gcloud_profiling: bool,
    // #[clap(
    //     long,
    //     value_name = "free-query-auth-token",
    //     env = "FREE_QUERY_AUTH_TOKEN",
    //     help = "Auth token that clients can use to query for free"
    // )]
    pub free_query_auth_token: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct Postgres {
    // #[clap(
    //     long,
    //     value_name = "postgres-host",
    //     env = "POSTGRES_HOST",
    //     default_value_t = String::from("http://0.0.0.0/"),
    //     help = "Postgres host"
    // )]
    pub postgres_host: String,
    // #[clap(
    //     long,
    //     value_name = "postgres-port",
    //     env = "POSTGRES_PORT",
    //     default_value_t = 5432,
    //     help = "Postgres port"
    // )]
    pub postgres_port: usize,
    // #[clap(
    //     long,
    //     value_name = "postgres-database",
    //     env = "POSTGRES_DATABASE",
    //     help = "Postgres database name"
    // )]
    pub postgres_database: String,
    // #[clap(
    //     long,
    //     value_name = "postgres-username",
    //     env = "POSTGRES_USERNAME",
    //     default_value_t = String::from("postgres"),
    //     help = "Postgres username"
    // )]
    pub postgres_username: String,
    // #[clap(
    //     long,
    //     value_name = "postgres-password",
    //     env = "POSTGRES_PASSWORD",
    //     default_value_t = String::from(""),
    //     help = "Postgres password"
    // )]
    pub postgres_password: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct NetworkSubgraph {
    // #[clap(
    //     long,
    //     value_name = "network-subgraph-deployment",
    //     env = "NETWORK_SUBGRAPH_DEPLOYMENT",
    //     help = "Network subgraph deployment"
    // )]
    pub network_subgraph_deployment: Option<DeploymentId>,
    // #[clap(
    //     long,
    //     value_name = "network-subgraph-endpoint",
    //     env = "NETWORK_SUBGRAPH_ENDPOINT",
    //     default_value_t = String::from("https://api.thegraph.com/subgraphs/name/graphprotocol/graph-network-goerli"),
    //     help = "Endpoint to query the network subgraph from"
    // )]
    pub network_subgraph_endpoint: String,
    // #[clap(
    //     long,
    //     value_name = "network-subgraph-auth-token",
    //     env = "NETWORK_SUBGRAPH_AUTH_TOKEN",
    //     help = "Bearer token to require for /network queries"
    // )]
    pub network_subgraph_auth_token: Option<String>,
    // #[clap(
    //     long,
    //     value_name = "serve-network-subgraph",
    //     env = "SERVE_NETWORK_SUBGRAPH",
    //     default_value_t = false,
    //     help = "Whether to serve the network subgraph at /network"
    // )]
    pub serve_network_subgraph: bool,
    // #[clap(
    //     long,
    //     value_name = "allocation-syncing-interval",
    //     env = "ALLOCATION_SYNCING_INTERVAL",
    //     default_value_t = 120_000,
    //     help = "Interval (in ms) for syncing indexer allocations from the network"
    // )]
    pub allocation_syncing_interval: u64,
    // #[clap(
    //     long,
    //     value_name = "client-signer-address",
    //     env = "CLIENT_SIGNER_ADDRESS",
    //     help = "Address that signs query fee receipts from a known client"
    // )]
    pub client_signer_address: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct EscrowSubgraph {
    // #[clap(
    //     long,
    //     value_name = "escrow-subgraph-deployment",
    //     env = "ESCROW_SUBGRAPH_DEPLOYMENT",
    //     help = "Escrow subgraph deployment"
    // )]
    pub escrow_subgraph_deployment: Option<DeploymentId>,
    // #[clap(
    //     long,
    //     value_name = "escrow-subgraph-endpoint",
    //     env = "ESCROW_SUBGRAPH_ENDPOINT",
    //     help = "Endpoint to query the network subgraph from"
    // )]
    pub escrow_subgraph_endpoint: String,
    // #[clap(
    //     long,
    //     value_name = "escrow-subgraph-auth-token",
    //     env = "ESCROW_SUBGRAPH_AUTH_TOKEN",
    //     help = "Bearer token to require for /network queries"
    // )]
    // pub escrow_subgraph_auth_token: Option<String>,
    // #[clap(
    //     long,
    //     value_name = "serve-escrow-subgraph",
    //     env = "SERVE_ESCROW_SUBGRAPH",
    //     default_value_t = false,
    //     help = "Whether to serve the escrow subgraph at /escrow"
    // )]
    // pub serve_escrow_subgraph: bool,
    // #[clap(
    //     long,
    //     value_name = "escrow-syncing-interval",
    //     env = "ESCROW_SYNCING_INTERVAL",
    //     default_value_t = 120_000,
    //     help = "Interval (in ms) for syncing indexer escrow accounts from the escrow subgraph"
    // )]
    pub escrow_syncing_interval: u64,
}

impl Config {
    pub fn load(filename: &PathBuf) -> Result<Self, figment::Error> {
        Figment::new().merge(Toml::file(filename)).extract()
    }
}
