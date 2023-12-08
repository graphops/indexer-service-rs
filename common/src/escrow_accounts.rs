// Copyright 2023-, GraphOps and Semiotic Labs.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, time::Duration};

use alloy_primitives::Address;
use anyhow::Result;
use ethers_core::types::U256;
use eventuals::{timer, Eventual, EventualExt};
use serde::Deserialize;
use tokio::time::sleep;
use tracing::{error, warn};

use crate::prelude::{Query, SubgraphClient};

pub fn escrow_accounts(
    escrow_subgraph: &'static SubgraphClient,
    indexer_address: Address,
    interval: Duration,
) -> Eventual<HashMap<Address, U256>> {
    // Types for deserializing the network subgraph response
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct EscrowAccountsResponse {
        escrow_accounts: Vec<EscrowAccount>,
    }
    // These 2 structs are used to deserialize the response from the escrow subgraph.
    // Note that U256's serde implementation is based on serializing the internal bytes, not the string decimal
    // representation. This is why we deserialize them as strings below.
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct EscrowAccount {
        balance: String,
        total_amount_thawing: String,
        sender: Sender,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Sender {
        id: Address,
    }

    timer(interval).map_with_retry(
        move |_| async move {
            let response = escrow_subgraph
                .query::<EscrowAccountsResponse>(Query::new_with_variables(
                    r#"
                        query ($indexer: ID!) {
                            escrowAccounts(where: {receiver_: {id: $indexer}}) {
                                balance
                                totalAmountThawing
                                sender {
                                    id
                                }
                            }
                        }
                    "#,
                    [("indexer", indexer_address.to_string().into())],
                ))
                .await
                .map_err(|e| e.to_string())?;

            response.map_err(|e| e.to_string()).and_then(|data| {
                data.escrow_accounts
                    .iter()
                    .map(|account| {
                        let balance = U256::checked_sub(
                            U256::from_dec_str(&account.balance)?,
                            U256::from_dec_str(&account.total_amount_thawing)?,
                        )
                        .unwrap_or_else(|| {
                            warn!(
                                "Balance minus total amount thawing underflowed for account {}. \
                                 Setting balance to 0, no queries will be served for this sender.",
                                account.sender.id
                            );
                            U256::from(0)
                        });

                        Ok((account.sender.id, balance))
                    })
                    .collect::<Result<HashMap<_, _>, anyhow::Error>>()
                    .map_err(|e| format!("{}", e))
            })
        },
        move |err: String| {
            error!(
                "Failed to fetch escrow accounts for indexer {:?}: {}",
                indexer_address, err
            );

            sleep(interval.div_f32(2.0))
        },
    )
}

#[cfg(test)]
mod tests {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::prelude::DeploymentDetails;
    use crate::test_vectors;

    use super::*;

    #[tokio::test]
    async fn test_current_accounts() {
        // Set up a mock escrow subgraph
        let mock_server = MockServer::start().await;
        let escrow_subgraph = Box::leak(Box::new(SubgraphClient::new(
            reqwest::Client::new(),
            None,
            DeploymentDetails::for_query_url(&format!(
                "{}/subgraphs/id/{}",
                &mock_server.uri(),
                *test_vectors::ESCROW_SUBGRAPH_DEPLOYMENT
            ))
            .unwrap(),
        )));

        let mock = Mock::given(method("POST"))
            .and(path(format!(
                "/subgraphs/id/{}",
                *test_vectors::ESCROW_SUBGRAPH_DEPLOYMENT
            )))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_raw(test_vectors::ESCROW_QUERY_RESPONSE, "application/json"),
            );
        mock_server.register(mock).await;

        let accounts = escrow_accounts(
            escrow_subgraph,
            *test_vectors::INDEXER_ADDRESS,
            Duration::from_secs(60),
        );

        assert_eq!(
            accounts.value().await.unwrap(),
            *test_vectors::ESCROW_ACCOUNTS
        );
    }
}
