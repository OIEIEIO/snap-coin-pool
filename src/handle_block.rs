// ============================================================================
// File: handle_block.rs
// Location: snap-coin-pool/src/handle_block.rs
// Version: 2.0.3
//
// Description:
// API-only payout distribution. The pool no longer uses a local blockchain
// or node_state. Chain truth (height + tx difficulty) is supplied by caller.
// The completed payout transaction is submitted through the provided async
// submit_tx callback (typically NodeProxy → Request::NewTransaction).
//
// Notes:
//   - Miners retain credit (work is NOT cleared) if payout tx PoW fails
//     or if submit_tx returns an error.
//   - If submit_tx succeeds but share_store update fails, we return an error
//     (duplicate payout risk exists; logged as CRITICAL).
//
// CHANGELOG (v2.0.3):
//   - VarDiff accounting compatibility:
//       * Use ShareStore work-units API (get_work/total_work)
//       * Payout proportions are computed from per-miner work (u128), not shares
//       * MinerPayout records work: u128 (not shares)
// ============================================================================

use snap_coin::{
    core::{
        block::Block,
        blockchain::BlockchainError,
        transaction::{Transaction, TransactionId, TransactionInput, TransactionOutput},
    },
    crypto::keys::{Private, Public},
};
use tokio::task::spawn_blocking;

use crate::share_store::{MinerPayout, PayoutRecord, SharedShareStore};

// ── Constants ───────────────────────────────────────────────────────────────

/// Maximum outputs per transaction. This is the transaction output limit.
/// Reserve 1 slot for the pool operator output.
const MAX_OUTPUTS_PER_TX: usize = 128;

/// Time budget per PoW attempt (seconds).
const POW_TIMEOUT_SECS: f64 = 10.0;

/// Number of PoW retry attempts before giving up.
const MAX_POW_RETRIES: u32 = 3;

// ── Main payout entry point (API-only) ──────────────────────────────────────
//
// NOTE: `submit_tx` is the *only* network submit path. This file does not
// depend on SharedBlockchain/SharedNodeState or accept_transaction.

pub async fn handle_block<F, Fut>(
    chain_height: u64,
    tx_difficulty: [u8; 32],
    block: Block,
    pool_private: Private,
    pool_public: Public,
    pool_dev: Public,
    share_store: &SharedShareStore,
    pool_fee: f64,
    submit_tx: F,
) -> Result<(), BlockchainError>
where
    F: Fn(Transaction) -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<(), BlockchainError>> + Send,
{
    // ── 1. Find the pool's reward output ────────────────────────────────
    let (reward_tx_id, reward_output_index, amount_from_block) =
        find_pool_reward(&block, pool_public)?;

    if amount_from_block == 0 {
        tracing::error!(
            "Block at height {} has zero pool reward — skipping payout",
            chain_height
        );
        return Err(BlockchainError::InvalidRewardTransactionAmount);
    }

    tracing::info!(
        "Block found at height {}: reward={} for pool",
        chain_height,
        amount_from_block
    );

    // ── 2. Calculate fee and distributable amount ───────────────────────
    let operator_fee = (amount_from_block as f64 * pool_fee) as u64;
    let amount_to_share = amount_from_block.saturating_sub(operator_fee);

    // ── 3. Get work snapshot ────────────────────────────────────────────
    let received_work = share_store.get_work().await;
    let total_work: u128 = received_work.values().copied().sum();

    if total_work == 0 {
        tracing::warn!("No work recorded — entire reward goes to operator");
    }

    // ── 4. Calculate per-miner payouts (proportional to work) ───────────
    //
    // We compute payout using f64 to preserve behavior; u128 work is cast.
    // (If you later want exact integer rationals to avoid float rounding,
    //  we can do that in a dedicated rewrite.)
    let mut miner_payouts: Vec<(Public, u128, u64)> = if total_work > 0 {
        received_work
            .iter()
            .filter_map(|(miner, work)| {
                if *work == 0 {
                    return None;
                }

                let payout = (((*work as f64) / (total_work as f64)) * (amount_to_share as f64))
                    as u64;

                if payout > 0 {
                    Some((*miner, *work, payout))
                } else {
                    None
                }
            })
            .collect()
    } else {
        Vec::new()
    };

    // Sort by payout descending — highest earners paid first if we hit the cap
    miner_payouts.sort_by(|(_, _, a), (_, _, b)| b.cmp(a));

    // ── 5. Cap to transaction output limit ──────────────────────────────
    //    Reserve 1 output for the pool operator output.
    let max_payouts = (MAX_OUTPUTS_PER_TX - 1).min(miner_payouts.len());
    let (to_pay_now, to_defer) = miner_payouts.split_at(max_payouts);

    let awarded_total: u64 = to_pay_now.iter().map(|(_, _, amount)| amount).sum();

    // ── 6. Pool operator gets: fee + rounding dust + deferred amounts ───
    let operator_total = amount_from_block.saturating_sub(awarded_total);

    // ── 7. Build transaction outputs ────────────────────────────────────
    let mut tx_outputs: Vec<TransactionOutput> = to_pay_now
        .iter()
        .map(|(miner, _, amount)| TransactionOutput {
            amount: *amount,
            receiver: *miner,
        })
        .collect();

    // Operator output (always present, always last)
    tx_outputs.push(TransactionOutput {
        amount: operator_total,
        receiver: pool_dev,
    });

    // Sanity check: outputs must sum to reward
    let output_sum: u64 = tx_outputs.iter().map(|o| o.amount).sum();
    if output_sum != amount_from_block {
        tracing::error!(
            "Output sum mismatch: {} != {} (reward). Aborting payout.",
            output_sum,
            amount_from_block
        );
        return Err(BlockchainError::InvalidRewardTransactionAmount);
    }

    // ── 8. Build and sign the payout transaction ────────────────────────
    let mut tx = Transaction::new_transaction_now(
        vec![TransactionInput {
            transaction_id: reward_tx_id,
            output_index: reward_output_index,
            output_owner: pool_public,
            signature: None,
        }],
        tx_outputs,
        &mut vec![pool_private],
    )
    .map_err(|e| BlockchainError::BincodeEncode(e.to_string()))?;

    // ── 9. Compute transaction PoW with retries (provided difficulty) ───
    let mut last_err: Option<String> = None;

    for attempt in 1..=MAX_POW_RETRIES {
        let mut tx_clone = tx.clone();
        let diff_clone = tx_difficulty;

        let result = spawn_blocking(move || {
            tx_clone.compute_pow(&diff_clone, Some(POW_TIMEOUT_SECS))?;
            Ok::<Transaction, anyhow::Error>(tx_clone)
        })
        .await
        .map_err(|e| BlockchainError::Io(format!("PoW task panicked: {}", e)))?;

        match result {
            Ok(completed_tx) => {
                tx = completed_tx;
                last_err = None;
                tracing::info!(
                    "Payout tx PoW completed on attempt {}/{}",
                    attempt,
                    MAX_POW_RETRIES
                );
                break;
            }
            Err(e) => {
                tracing::warn!(
                    "Payout tx PoW attempt {}/{} failed: {}",
                    attempt,
                    MAX_POW_RETRIES,
                    e
                );
                last_err = Some(e.to_string());
            }
        }
    }

    if let Some(err) = last_err {
        tracing::error!(
            "All {} PoW attempts failed. Work preserved. Last error: {}",
            MAX_POW_RETRIES,
            err
        );
        return Err(BlockchainError::Io(
            "Failed to compute payout transaction PoW".to_string(),
        ));
    }

    // ── 10. Submit the payout transaction (API callback) ────────────────
    submit_tx(tx).await?;
    tracing::info!("Payout transaction submitted via node API");

    // ── 11. Atomically record payout and clear paid work ────────────────
    let block_hash = block
        .meta
        .hash
        .map(|h| h.dump_buf())
        .unwrap_or([0u8; 32]);

    let payout_record = PayoutRecord {
        block_height: chain_height,
        block_hash,
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        total_reward: amount_from_block,
        pool_fee: operator_fee,
        payouts: to_pay_now
            .iter()
            .map(|(miner, work, amount)| MinerPayout {
                miner: *miner.dump_buf(),
                work: *work,
                amount: *amount,
            })
            .collect(),
        deferred: to_defer
            .iter()
            .map(|(miner, _, _)| *miner.dump_buf())
            .collect(),
    };

    let paid_miners: Vec<Public> = to_pay_now.iter().map(|(m, _, _)| *m).collect();

    share_store
        .record_payout_and_clear(payout_record, &paid_miners)
        .await
        .map_err(|e| {
            tracing::error!(
                "CRITICAL: Payout tx submitted but share store update failed: {}",
                e
            );
            BlockchainError::Io(e.to_string())
        })?;

    // ── 12. Log deferred miners ─────────────────────────────────────────
    if !to_defer.is_empty() {
        tracing::warn!(
            "Deferred payouts for {} miners (carried to next block)",
            to_defer.len()
        );
    }

    tracing::info!(
        "Payout complete: {} miners paid, fee={}, deferred={}",
        paid_miners.len(),
        operator_fee,
        to_defer.len()
    );

    Ok(())
}

// ── Find the pool's output in the coinbase transaction ──────────────────────

fn find_pool_reward(
    block: &Block,
    pool_public: Public,
) -> Result<(TransactionId, usize, u64), BlockchainError> {
    for tx in block.transactions.iter() {
        if !tx.inputs.is_empty() {
            continue;
        }

        let tx_id = tx
            .transaction_id
            .ok_or(BlockchainError::RewardTransactionIdMissing)?;

        for (i, output) in tx.outputs.iter().enumerate() {
            if output.receiver == pool_public {
                return Ok((tx_id, i, output.amount));
            }
        }
    }

    tracing::error!("No pool reward output found in block");
    Err(BlockchainError::InvalidRewardTransaction)
}

// ============================================================================
// File: handle_block.rs
// Location: snap-coin-pool/src/handle_block.rs
// Version: 2.0.3
// Created: 2026-02-08T12:15:00Z
// Updated: 2026-02-08T20:55:00Z
// LOC: 271
// ============================================================================
