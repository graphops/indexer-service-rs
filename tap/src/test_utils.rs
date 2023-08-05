use ethereum_types::Address;
use ethers_signers::{coins_bip39::English, LocalWallet, MnemonicBuilder, Signer};
use tap_core::receipt_aggregate_voucher::ReceiptAggregateVoucher;
use tap_core::tap_manager::SignedRAV;
use tap_core::tap_receipt::ReceivedReceipt;
use tap_core::{eip_712_signed_message::EIP712SignedMessage, tap_receipt::Receipt};

/// Fixture to generate a wallet and address
pub fn keys() -> (LocalWallet, Address) {
    let wallet: LocalWallet = MnemonicBuilder::<English>::default()
        .phrase("abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about")
        .build()
        .unwrap();
    let address = wallet.address();
    (wallet, address)
}

/// Fixture to generate a signed receipt using the wallet from `keys()`
/// and the given `query_id` and `value`
pub async fn create_received_receipt(
    allocation_id: Address,
    nonce: u64,
    timestamp_ns: u64,
    value: u128,
    query_id: u64,
) -> ReceivedReceipt {
    let (wallet, _) = keys();
    let receipt = EIP712SignedMessage::new(
        Receipt {
            allocation_id,
            nonce,
            timestamp_ns,
            value,
        },
        &wallet,
    )
    .await
    .unwrap();

    ReceivedReceipt::new(receipt, query_id, &[])
}

/// Fixture to generate a RAV using the wallet from `keys()`
pub async fn create_rav(
    allocation_id: Address,
    timestamp_ns: u64,
    value_aggregate: u128,
) -> SignedRAV {
    let (wallet, _) = keys();

    EIP712SignedMessage::new(
        ReceiptAggregateVoucher {
            allocation_id,
            timestamp_ns,
            value_aggregate,
        },
        &wallet,
    )
    .await
    .unwrap()
}
