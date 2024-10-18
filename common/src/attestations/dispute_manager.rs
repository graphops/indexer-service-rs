// Copyright 2023-, Edge & Node, GraphOps, and Semiotic Labs.
// SPDX-License-Identifier: Apache-2.0

use crate::subgraph_client::SubgraphClient;
use alloy::primitives::Address;
use anyhow::Error;
use graphql_client::GraphQLQuery;
use std::time::Duration;
use tokio::sync::watch::{self, Receiver};
use tokio::time::{self, sleep};
use tracing::warn;

type Bytes = Address;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "../graphql/network.schema.graphql",
    query_path = "../graphql/dispute.query.graphql",
    response_derives = "Debug",
    variables_derives = "Clone"
)]
struct DisputeManager;

pub fn dispute_manager(
    network_subgraph: &'static SubgraphClient,
    interval: Duration,
) -> Receiver<Option<Address>> {
    let (tx, rx) = watch::channel(None);
    tokio::spawn(async move {
        let mut time_interval = time::interval(interval);
        time_interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        loop {
            time_interval.tick().await;

            let result = async {
                let response = network_subgraph
                    .query::<DisputeManager, _>(dispute_manager::Variables {})
                    .await?;
                response?
                    .graph_network
                    .map(|network| network.dispute_manager)
                    .ok_or_else(|| Error::msg("Network 1 not found in network subgraph"))
            }
            .await;

            match result {
                Ok(address) => {
                    if tx.send(Some(address)).is_err() {
                        // stopping
                        break;
                    }
                }
                Err(err) => {
                    warn!("Failed to query dispute manager for network: {}", err);
                    // Sleep for a bit before we retry
                    sleep(interval.div_f32(2.0)).await;
                }
            }
        }
    });
    rx
}

#[cfg(test)]
mod test {
    use serde_json::json;
    use wiremock::{
        matchers::{method, path},
        Mock, MockServer, ResponseTemplate,
    };

    use crate::{
        prelude::SubgraphClient,
        subgraph_client::DeploymentDetails,
        test_vectors::{self, DISPUTE_MANAGER_ADDRESS},
    };

    use super::*;

    async fn setup_mock_network_subgraph() -> (&'static SubgraphClient, MockServer) {
        // Set up a mock network subgraph
        let mock_server = MockServer::start().await;
        let network_subgraph = SubgraphClient::new(
            reqwest::Client::new(),
            None,
            DeploymentDetails::for_query_url(&format!(
                "{}/subgraphs/id/{}",
                &mock_server.uri(),
                *test_vectors::NETWORK_SUBGRAPH_DEPLOYMENT
            ))
            .unwrap(),
        );

        // Mock result for current epoch requests
        mock_server
            .register(
                Mock::given(method("POST"))
                    .and(path(format!(
                        "/subgraphs/id/{}",
                        *test_vectors::NETWORK_SUBGRAPH_DEPLOYMENT
                    )))
                    .respond_with(ResponseTemplate::new(200).set_body_json(
                        json!({ "data": { "graphNetwork": { "disputeManager": *DISPUTE_MANAGER_ADDRESS }}}),
                    )),
            )
            .await;

        (Box::leak(Box::new(network_subgraph)), mock_server)
    }

    #[test_log::test(tokio::test)]
    async fn test_parses_dispute_manager_from_network_subgraph_correctly() {
        let (network_subgraph, _mock_server) = setup_mock_network_subgraph().await;

        let dispute_manager = dispute_manager(network_subgraph, Duration::from_secs(60));
        sleep(Duration::from_millis(50)).await;
        let result = *dispute_manager.borrow();
        assert_eq!(result.unwrap(), *DISPUTE_MANAGER_ADDRESS);
    }
}
