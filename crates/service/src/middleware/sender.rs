// Copyright 2023-, Edge & Node, GraphOps, and Semiotic Labs.
// SPDX-License-Identifier: Apache-2.0

use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use indexer_allocation::NetworkAddress;
use indexer_monitor::EscrowAccounts;
use thegraph_core::alloy::sol_types::Eip712Domain;
use tokio::sync::watch;

use crate::{error::IndexerServiceError, tap::TapReceipt};

/// Stated used by sender middleware
#[derive(Clone)]
pub struct SenderState {
    /// Used to recover the signer address
    pub domain_separator: Eip712Domain,
    /// Used to get the sender address given the signer address
    pub escrow_accounts: watch::Receiver<EscrowAccounts>,
}

/// The current query Sender address
pub type Sender = NetworkAddress;

/// Injects the sender found from the signer in the receipt
///
/// A request won't always have a receipt because they might be
/// free queries.
/// That's why we don't fail with 400.
///
/// Requires Receipt extension
pub async fn sender_middleware(
    State(state): State<SenderState>,
    mut request: Request,
    next: Next,
) -> Result<Response, IndexerServiceError> {
    if let Some(receipt) = request.extensions().get::<TapReceipt>() {
        let signer = receipt.recover_signer(&state.domain_separator)?;
        let signer = match receipt {
            TapReceipt::V1(_) => NetworkAddress::Legacy(signer),
            TapReceipt::V2(_) => NetworkAddress::Horizon(signer),
        };
        let sender = state
            .escrow_accounts
            .borrow()
            .get_sender_for_signer(&signer)?;
        request.extensions_mut().insert(sender);
    }

    Ok(next.run(request).await)
}

#[cfg(test)]
mod tests {
    use axum::{
        body::Body,
        http::{Extensions, Request},
        middleware::from_fn_with_state,
        routing::get,
        Router,
    };
    use indexer_monitor::EscrowAccounts;
    use reqwest::StatusCode;
    use test_assets::{
        create_signed_receipt, SignedReceiptRequest, ESCROW_ACCOUNTS_BALANCES,
        ESCROW_ACCOUNTS_SENDERS_TO_SIGNERS,
    };
    use tokio::sync::watch;
    use tower::ServiceExt;

    use super::{sender_middleware, Sender};
    use crate::{middleware::sender::SenderState, tap::TapReceipt};

    #[tokio::test]
    async fn test_sender_middleware() {
        let escrow_accounts = watch::channel(EscrowAccounts::new(
            ESCROW_ACCOUNTS_BALANCES.to_owned(),
            ESCROW_ACCOUNTS_SENDERS_TO_SIGNERS.to_owned(),
        ))
        .1;
        let state = SenderState {
            domain_separator: test_assets::TAP_EIP712_DOMAIN.clone(),
            escrow_accounts,
        };

        let middleware = from_fn_with_state(state, sender_middleware);

        async fn handle(extensions: Extensions) -> Body {
            let sender = extensions.get::<Sender>().expect("Should contain sender");
            assert_eq!(sender.address(), test_assets::TAP_SENDER.1);
            Body::empty()
        }

        let app = Router::new().route("/", get(handle)).layer(middleware);

        let receipt = create_signed_receipt(SignedReceiptRequest::builder().build()).await;

        let res = app
            .oneshot(
                Request::builder()
                    .uri("/")
                    .extension(TapReceipt::V1(receipt))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }
}
