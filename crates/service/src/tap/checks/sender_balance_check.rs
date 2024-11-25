// Copyright 2023-, Edge & Node, GraphOps, and Semiotic Labs.
// SPDX-License-Identifier: Apache-2.0

use alloy::primitives::U256;
use anyhow::anyhow;
use indexer_monitor::EscrowAccounts;
use tap_core::receipt::{
    checks::{Check, CheckError, CheckResult},
    state::Checking,
    ReceiptWithState,
};
use tokio::sync::watch::Receiver;

use crate::middleware::Sender;

pub struct SenderBalanceCheck {
    escrow_accounts: Receiver<EscrowAccounts>,
}

impl SenderBalanceCheck {
    pub fn new(escrow_accounts: Receiver<EscrowAccounts>) -> Self {
        Self { escrow_accounts }
    }
}

#[async_trait::async_trait]
impl Check for SenderBalanceCheck {
    async fn check(
        &self,
        ctx: &tap_core::receipt::Context,
        _: &ReceiptWithState<Checking>,
    ) -> CheckResult {
        let escrow_accounts_snapshot = self.escrow_accounts.borrow();

        let Sender(receipt_sender) = ctx
            .get::<Sender>()
            .ok_or(CheckError::Failed(anyhow::anyhow!("Could not find sender")))?;

        // Check that the sender has a non-zero balance -- more advanced accounting is done in
        // `tap-agent`.
        if !escrow_accounts_snapshot
            .get_balance_for_sender(receipt_sender)
            .map_or(false, |balance| balance > U256::ZERO)
        {
            return Err(CheckError::Failed(anyhow!(
                "Receipt sender `{}` does not have a sufficient balance",
                receipt_sender,
            )));
        }
        Ok(())
    }
}
