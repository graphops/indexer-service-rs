// Copyright 2023-, Edge & Node, GraphOps, and Semiotic Labs.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use super::routes;
use anyhow::anyhow;
use async_graphql::{EmptySubscription, Schema};
use async_graphql_axum::GraphQL;
use axum::{
    routing::{post, post_service},
    Router,
};
use indexer_config::{Config, DipsConfig};
use reqwest::Url;
use sqlx::PgPool;
use thegraph_core::attestation::eip712_domain;

use crate::{
    cli::Cli,
    database::{
        self,
        dips::{AgreementStore, InMemoryAgreementStore},
    },
    routes::dips::Price,
    service::indexer_service::{IndexerServiceOptions, IndexerServiceRelease},
};
use clap::Parser;
use tracing::error;

mod indexer_service;
mod tap_receipt_header;

pub use tap_receipt_header::TapReceipt;

#[derive(Clone)]
pub struct SubgraphServiceState {
    pub config: &'static Config,
    pub database: PgPool,
    pub cost_schema: routes::cost::CostSchema,
    pub graph_node_client: reqwest::Client,
    pub graph_node_status_url: &'static Url,
    pub graph_node_query_base_url: &'static Url,
}

/// Run the subgraph indexer service
pub async fn run() -> anyhow::Result<()> {
    // Parse command line and environment arguments
    let cli = Cli::parse();

    // Load the json-rpc service configuration, which is a combination of the
    // general configuration options for any indexer service and specific
    // options added for JSON-RPC
    let config = Box::leak(Box::new(
        Config::parse(indexer_config::ConfigPrefix::Service, cli.config.as_ref()).map_err(|e| {
            error!(
                "Invalid configuration file `{}`: {}, if a value is missing you can also use \
                --config to fill the rest of the values",
                cli.config.unwrap_or_default().display(),
                e
            );
            anyhow!(e)
        })?,
    ));

    // Parse basic configurations
    build_info::build_info!(fn build_info);
    let release = IndexerServiceRelease::from(build_info());

    // Some of the subgraph service configuration goes into the so-called
    // "state", which will be passed to any request handler, middleware etc.
    // that is involved in serving requests
    let state = SubgraphServiceState {
        config,
        database: database::connect(config.database.clone().get_formated_postgres_url().as_ref())
            .await,
        cost_schema: routes::cost::build_schema().await,
        graph_node_client: reqwest::ClientBuilder::new()
            .tcp_nodelay(true)
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to init HTTP client for Graph Node"),
        graph_node_status_url: &config.graph_node.status_url,
        graph_node_query_base_url: &config.graph_node.query_url,
    };

    let agreement_store: Arc<dyn AgreementStore> = Arc::new(InMemoryAgreementStore::default());
    let prices: Vec<Price> = vec![];

    let mut router = Router::new()
        .route("/cost", post(routes::cost::cost))
        .route("/status", post(routes::status))
        .with_state(state.clone());

    if let Some(DipsConfig {
        allowed_payers,
        cancellation_time_tolerance,
    }) = config.dips.as_ref()
    {
        let schema = Schema::build(
            routes::dips::AgreementQuery {},
            routes::dips::AgreementMutation {
                expected_payee: config.indexer.indexer_address,
                allowed_payers: allowed_payers.clone(),
                domain: eip712_domain(
                    // 42161, // arbitrum
                    config.blockchain.chain_id as u64,
                    config.blockchain.receipts_verifier_address,
                ),
                cancel_voucher_time_tolerance: cancellation_time_tolerance
                    .unwrap_or(Duration::from_secs(5)),
            },
            EmptySubscription,
        )
        .data(agreement_store)
        .data(prices)
        .finish();

        router = router.route("/dips", post_service(GraphQL::new(schema)));
    }

    indexer_service::run(IndexerServiceOptions {
        release,
        config,
        url_namespace: "subgraphs",
        subgraph_state: state,
        extra_routes: router,
    })
    .await
}
