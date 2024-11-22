// Copyright 2023-, Edge & Node, GraphOps, and Semiotic Labs.
// SPDX-License-Identifier: Apache-2.0

//! Validates Tap receipts
//!
//! This looks for a Context in the extensions of the request to inject
//! as part of the checks.
//!
//! This also uses MetricLabels injected in the receipts to provide
//! metrics related to receipt check failure

use std::{future::Future, sync::Arc};

use axum::{
    body::Body,
    http::{Request, Response},
    response::IntoResponse,
};
use tap_core::{
    manager::{adapters::ReceiptStore, Manager},
    receipt::{Context, SignedReceipt},
};
use tower_http::auth::AsyncAuthorizeRequest;

use crate::{error::IndexerServiceError, middleware::prometheus_metrics::MetricLabels};

/// Middleware to verify and store TAP receipts
///
/// It also optionally updates a failed receipt metric if Labels are provided
///
/// Requires SignedReceipt, MetricLabels and Arc<Context> extensions
pub fn tap_receipt_authorize<T, B>(
    tap_manager: &'static Manager<T>,
    failed_receipt_metric: &'static prometheus::CounterVec,
) -> impl AsyncAuthorizeRequest<
    B,
    RequestBody = B,
    ResponseBody = Body,
    Future = impl Future<Output = Result<Request<B>, Response<Body>>> + Send,
> + Clone
       + Send
where
    T: ReceiptStore + Sync + Send,
    B: Send,
{
    |request: Request<B>| {
        let receipt = request.extensions().get::<SignedReceipt>().cloned();
        // load labels from previous middlewares
        let labels = request.extensions().get::<MetricLabels>().cloned();
        // load context from previous middlewares
        let ctx = request.extensions().get::<Arc<Context>>().cloned();

        async {
            let execute = || async {
                let receipt = receipt.ok_or(IndexerServiceError::ReceiptNotFound)?;
                // Verify the receipt and store it in the database
                tap_manager
                    .verify_and_store_receipt(&ctx.unwrap_or_default(), receipt)
                    .await
                    .inspect_err(|_| {
                        if let Some(labels) = labels {
                            failed_receipt_metric
                                .with_label_values(&labels.get_labels())
                                .inc()
                        }
                    })?;
                Ok::<_, IndexerServiceError>(request)
            };
            execute().await.map_err(|error| error.into_response())
        }
    }
}

#[cfg(test)]
mod tests {

    use core::panic;
    use rstest::*;
    use std::{sync::Arc, time::Duration};
    use tokio::time::sleep;
    use tower::{Service, ServiceBuilder, ServiceExt};

    use alloy::primitives::{address, Address};
    use axum::{
        body::Body,
        http::{Request, Response},
    };
    use prometheus::core::Collector;
    use reqwest::StatusCode;
    use sqlx::PgPool;
    use tap_core::{
        manager::Manager,
        receipt::{
            checks::{Check, CheckError, CheckList, CheckResult},
            state::Checking,
            ReceiptWithState,
        },
    };
    use test_assets::{create_signed_receipt, TAP_EIP712_DOMAIN};
    use tower_http::auth::AsyncRequireAuthorizationLayer;

    use crate::{
        middleware::{
            auth::tap_receipt_authorize,
            prometheus_metrics::{MetricLabelProvider, MetricLabels},
        },
        tap::IndexerTapContext,
    };

    const ALLOCATION_ID: Address = address!("deadbeefcafebabedeadbeefcafebabedeadbeef");

    #[fixture]
    fn metric() -> &'static prometheus::CounterVec {
        let registry = prometheus::Registry::new();
        let metric = Box::leak(Box::new(
            prometheus::register_counter_vec_with_registry!(
                "tap_middleware_test",
                "Failed queries to handler",
                &["deployment"],
                registry,
            )
            .unwrap(),
        ));
        metric
    }

    const FAILED_NONCE: u64 = 99;

    async fn service(
        metric: &'static prometheus::CounterVec,
        pgpool: PgPool,
    ) -> impl Service<Request<Body>, Response = Response<Body>, Error = impl std::fmt::Debug> {
        let context = IndexerTapContext::new(pgpool, TAP_EIP712_DOMAIN.clone()).await;

        struct MyCheck;
        #[async_trait::async_trait]
        impl Check for MyCheck {
            async fn check(
                &self,
                _: &tap_core::receipt::Context,
                receipt: &ReceiptWithState<Checking>,
            ) -> CheckResult {
                if receipt.signed_receipt().message.nonce == FAILED_NONCE {
                    Err(CheckError::Failed(anyhow::anyhow!("Failed")))
                } else {
                    Ok(())
                }
            }
        }

        let manager = Box::leak(Box::new(Manager::new(
            TAP_EIP712_DOMAIN.clone(),
            context,
            CheckList::new(vec![Arc::new(MyCheck)]),
        )));
        let tap_auth = tap_receipt_authorize(manager, metric);
        let authorization_middleware = AsyncRequireAuthorizationLayer::new(tap_auth);

        let mut service = ServiceBuilder::new()
            .layer(authorization_middleware)
            .service_fn(|_: Request<Body>| async {
                Ok::<_, anyhow::Error>(Response::new(Body::default()))
            });

        service.ready().await.unwrap();
        service
    }

    #[rstest]
    #[sqlx::test(migrations = "../../migrations")]
    async fn test_tap_valid_receipt(
        metric: &'static prometheus::CounterVec,
        #[ignore] pgpool: PgPool,
    ) {
        let mut service = service(metric, pgpool.clone()).await;

        let receipt = create_signed_receipt(ALLOCATION_ID, 1, 1, 1).await;

        // check with receipt
        let mut req = Request::new(Body::default());
        req.extensions_mut().insert(receipt);
        let res = service.call(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // verify receipts
        if tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let result = sqlx::query!("SELECT * FROM scalar_tap_receipts")
                    .fetch_all(&pgpool)
                    .await
                    .unwrap();

                if result.is_empty() {
                    sleep(Duration::from_millis(50)).await;
                } else {
                    break;
                }
            }
        })
        .await
        .is_err()
        {
            panic!("Timeout assertion");
        }
    }

    #[rstest]
    #[sqlx::test(migrations = "../../migrations")]
    async fn test_invalid_receipt_with_failed_metric(
        metric: &'static prometheus::CounterVec,
        #[ignore] pgpool: PgPool,
    ) {
        let mut service = service(metric, pgpool.clone()).await;
        // if it fails tap receipt, should return failed to process payment + tap message

        assert_eq!(metric.collect().first().unwrap().get_metric().len(), 0);

        struct TestLabel;
        impl MetricLabelProvider for TestLabel {
            fn get_labels(&self) -> Vec<&str> {
                vec!["label1"]
            }
        }

        // default labels, all empty
        let labels: MetricLabels = Arc::new(TestLabel);

        let mut receipt = create_signed_receipt(ALLOCATION_ID, 1, 1, 1).await;
        // change the nonce to make the receipt invalid
        receipt.message.nonce = FAILED_NONCE;
        let mut req = Request::new(Body::default());
        req.extensions_mut().insert(receipt);
        req.extensions_mut().insert(labels);
        let response = service.call(req);

        assert_eq!(response.await.unwrap().status(), StatusCode::BAD_REQUEST);

        assert_eq!(metric.collect().first().unwrap().get_metric().len(), 1);
    }

    #[rstest]
    #[sqlx::test(migrations = "../../migrations")]
    async fn test_tap_missing_signed_receipt(
        metric: &'static prometheus::CounterVec,
        #[ignore] pgpool: PgPool,
    ) {
        let mut service = service(metric, pgpool.clone()).await;
        // if it doesnt contain the signed receipt
        // should return payment required
        let req = Request::new(Body::default());
        let res = service.call(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::PAYMENT_REQUIRED);
    }
}
