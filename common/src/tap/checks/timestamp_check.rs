// Copyright 2023-, GraphOps and Semiotic Labs.
// SPDX-License-Identifier: Apache-2.0
use anyhow::anyhow;
use std::time::{Duration, SystemTime};

pub struct TimestampCheck {
    timestamp_error_tolerance: Duration,
}

use tap_core::receipt::{
    checks::{Check, CheckResult},
    Checking, ReceiptWithState,
};

impl TimestampCheck {
    pub fn new(timestamp_error_tolerance: Duration) -> Self {
        Self {
            timestamp_error_tolerance,
        }
    }
}

#[async_trait::async_trait]
impl Check for TimestampCheck {
    async fn check(&self, receipt: &ReceiptWithState<Checking>) -> CheckResult {
        let timestamp_now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;
        let min_timestamp = timestamp_now - self.timestamp_error_tolerance;
        let max_timestamp = timestamp_now + self.timestamp_error_tolerance;

        let receipt_timestamp = Duration::from_nanos(receipt.signed_receipt().message.timestamp_ns);

        if receipt_timestamp < max_timestamp && receipt_timestamp > min_timestamp {
            Ok(())
        } else {
            Err(anyhow!(
                "Receipt timestamp `{}` is outside of current system time +/- timestamp_error_tolerance",
                receipt_timestamp.as_secs()
            ))
        }
    }
}
#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use alloy_primitives::Address;
    use alloy_sol_types::eip712_domain;
    use alloy_sol_types::Eip712Domain;

    use ethers::signers::coins_bip39::English;
    use ethers::signers::{LocalWallet, MnemonicBuilder};

    use super::*;
    use tap_core::{
        receipt::{checks::Check, Checking, Receipt, ReceiptWithState},
        signed_message::EIP712SignedMessage,
    };

    fn create_signed_receipt_with_custom_timestamp(
        timestamp_ns: u64,
    ) -> ReceiptWithState<Checking> {
        let index: u32 = 0;
        let wallet: LocalWallet = MnemonicBuilder::<English>::default()
            .phrase("abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about")
            .index(index)
            .unwrap()
            .build()
            .unwrap();
        let eip712_domain_separator: Eip712Domain = eip712_domain! {
            name: "TAP",
            version: "1",
            chain_id: 1,
            verifying_contract: Address:: from([0x11u8; 20]),
        };
        let value: u128 = 1234;
        let nonce: u64 = 10;
        let receipt = EIP712SignedMessage::new(
            &eip712_domain_separator,
            Receipt {
                allocation_id: Address::from_str("0xabababababababababababababababababababab")
                    .unwrap(),
                nonce,
                timestamp_ns,
                value,
            },
            &wallet,
        )
        .unwrap();
        ReceiptWithState::<Checking>::new(receipt)
    }

    #[tokio::test]
    async fn test_timestamp_inside_tolerance() {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos()
            + Duration::from_secs(15).as_nanos();
        let timestamp_ns = timestamp as u64;
        let signed_receipt = create_signed_receipt_with_custom_timestamp(timestamp_ns);
        let timestamp_check = TimestampCheck::new(Duration::from_secs(30));
        assert!(timestamp_check.check(&signed_receipt).await.is_ok());
    }

    #[tokio::test]
    async fn test_timestamp_less_than_tolerance() {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos()
            + Duration::from_secs(33).as_nanos();
        let timestamp_ns = timestamp as u64;
        let signed_receipt = create_signed_receipt_with_custom_timestamp(timestamp_ns);
        let timestamp_check = TimestampCheck::new(Duration::from_secs(30));
        assert!(timestamp_check.check(&signed_receipt).await.is_err());
    }

    #[tokio::test]
    async fn test_timestamp_more_than_tolerance() {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos()
            - Duration::from_secs(33).as_nanos();
        let timestamp_ns = timestamp as u64;
        let signed_receipt = create_signed_receipt_with_custom_timestamp(timestamp_ns);
        let timestamp_check = TimestampCheck::new(Duration::from_secs(30));
        assert!(timestamp_check.check(&signed_receipt).await.is_err());
    }
}
