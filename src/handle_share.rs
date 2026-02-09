// ============================================================================
// File: handle_share.rs
// Location: snap-coin-pool/src/handle_share.rs
// Version: 1.2.0
//
// Description: Share validation for submitted blocks. Validates block
//              completeness, metadata, reward transaction structure, PoW hash
//              against pool target, duplicate detection, and awards work.
//
// CHANGELOG (v1.2.0):
//   - VarDiff foundation: share accounting is now WORK-UNITS based.
//     * handle_share() now accepts `work_units: u128` (per-miner assigned diff
//       scalar from the connection/session layer).
//     * share_store.award_share(...) now records work_units (not share count).
//   - All validation logic and ordering preserved.
//
// Validation order (fail-fast, cheapest checks first):
//   1. Block completeness (has hash, has transactions)
//   2. Block metadata validity (timestamp, structure)
//   3. Reward transaction structure (dev fee, pool output, amounts)
//   4. Stale-job detection (difficulty mismatch) -> reject (no penalty)
//   5. PoW hash vs pool difficulty target
//   6. Duplicate detection via share store
//   7. Award work
// ============================================================================

use num_bigint::BigUint;
use snap_coin::{
    core::{
        block::Block,
        blockchain::BlockchainError,
        transaction::Transaction,
    },
    crypto::keys::Public,
    economics::{calculate_dev_fee, get_block_reward, DEV_WALLET},
};

use crate::share_store::SharedShareStore;

// ── Constants ───────────────────────────────────────────────────────────────

/// Maximum number of outputs allowed in the reward transaction.
/// Dev fee + pool output = 2. Anything more is suspicious.
const MAX_REWARD_OUTPUTS: usize = 2;

// ── Main validation entry point ─────────────────────────────────────────────

/// API-only share validation:
/// - `chain_height`: current chain height (from synced node API)
/// - `chain_block_diff` / `chain_tx_diff`: current difficulties (from synced node API)
/// - `mempool_opt`: retained for API compatibility but not enforced for pool shares
/// - `pool_difficulty`: pool target for this share (per-connection for VarDiff)
/// - `work_units`: accounting weight credited if accepted (per-connection VarDiff scalar)
pub async fn handle_share(
    chain_height: u64,
    chain_block_diff: [u8; 32],
    chain_tx_diff: [u8; 32],
    _mempool_opt: Option<Vec<Transaction>>,

    block: Block,
    client_address: Public,
    pool_difficulty: &[u8; 32],
    work_units: u128,
    pool_public: Public,
    share_store: &SharedShareStore,
) -> Result<(), BlockchainError> {
    let height_usize = chain_height as usize;

    // ── 1. Block completeness ───────────────────────────────────────────
    block.check_completeness()?;

    // ── 2. Block metadata validity ──────────────────────────────────────
    block.check_meta()?;

    // ── 3. Reward transaction structure ─────────────────────────────────
    validate_reward_tx(&block, pool_public, height_usize)?;

    // ── 4. Stale-job detection: difficulty mismatch ─────────────────────
    //
    // IMPORTANT:
    // Pool shares can be built from a job template that becomes stale if the
    // chain difficulty changes between job issuance and share submission.
    // This is not malicious. Reject the share, but keep it low-noise.
    if block.meta.block_pow_difficulty != chain_block_diff
        || block.meta.tx_pow_difficulty != chain_tx_diff
    {
        tracing::debug!(
            "Share rejected (stale job): miner={} height={} (difficulty mismatch)",
            hex_short(&client_address.dump_buf()),
            chain_height
        );
        return Err(BlockchainError::Block(
            snap_coin::core::block::BlockError::DifficultyMismatch,
        ));
    }

    // ── 5. PoW hash vs pool difficulty ──────────────────────────────────
    // hash <= target means the share meets the target (big-endian compare)
    let block_hash = block.meta.hash.ok_or(BlockchainError::Block(
        snap_coin::core::block::BlockError::BlockPowDifficultyIncorrect,
    ))?;

    let hash_int = BigUint::from_bytes_be(&block_hash.dump_buf());
    let target_int = BigUint::from_bytes_be(pool_difficulty);

    if hash_int > target_int {
        tracing::debug!(
            "Share rejected: miner={} height={} (does not meet pool target)",
            hex_short(&client_address.dump_buf()),
            chain_height
        );
        return Err(BlockchainError::Block(
            snap_coin::core::block::BlockError::BlockPowDifficultyIncorrect,
        ));
    }

    // ── 6. Duplicate detection + 7. Award work ─────────────────────────
    let hash_buf: [u8; 32] = block_hash.dump_buf();

    let awarded = share_store
        .award_share(client_address, chain_height, &hash_buf, work_units)
        .await
        .map_err(|e| {
            tracing::error!("Share store error: {}", e);
            BlockchainError::Io(e.to_string())
        })?;

    if !awarded {
        tracing::debug!(
            "Duplicate share: miner={} height={}",
            hex_short(&client_address.dump_buf()),
            chain_height
        );
        return Err(BlockchainError::Block(
            snap_coin::core::block::BlockError::BlockPowDifficultyIncorrect,
        ));
    }

    tracing::info!(
        "Share accepted: miner={} height={} work_units={}",
        hex_short(&client_address.dump_buf()),
        chain_height,
        work_units
    );

    Ok(())
}

// ── Reward transaction validation ───────────────────────────────────────────

fn validate_reward_tx(
    block: &Block,
    pool_public: Public,
    height: usize,
) -> Result<(), BlockchainError> {
    let expected_reward = get_block_reward(height);
    let expected_dev_fee = calculate_dev_fee(expected_reward);
    let mut found_reward_tx = false;

    for tx in block.transactions.iter() {
        // Reward transactions have no inputs (coinbase)
        if !tx.inputs.is_empty() {
            continue;
        }

        // Only one reward transaction allowed
        if found_reward_tx {
            return Err(BlockchainError::RewardOverspend);
        }
        found_reward_tx = true;

        // Must have a transaction ID
        if tx.transaction_id.is_none() {
            return Err(BlockchainError::RewardTransactionIdMissing);
        }

        // Enforce exactly MAX_REWARD_OUTPUTS outputs (dev fee + pool)
        if tx.outputs.len() != MAX_REWARD_OUTPUTS {
            tracing::warn!(
                "Reward tx has {} outputs, expected {}",
                tx.outputs.len(),
                MAX_REWARD_OUTPUTS
            );
            return Err(BlockchainError::InvalidRewardTransaction);
        }

        // Total output amount must equal block reward
        let total_output: u64 = tx.outputs.iter().map(|o| o.amount).sum();
        if total_output != expected_reward {
            return Err(BlockchainError::InvalidRewardTransactionAmount);
        }

        // Validate each output — must be exactly one dev fee and one pool output
        let mut has_dev_fee = false;
        let mut has_pool_output = false;

        for output in &tx.outputs {
            if output.receiver == DEV_WALLET && output.amount == expected_dev_fee {
                if has_dev_fee {
                    return Err(BlockchainError::InvalidRewardTransaction);
                }
                has_dev_fee = true;
            } else if output.receiver == pool_public
                && output.amount == expected_reward - expected_dev_fee
            {
                if has_pool_output {
                    return Err(BlockchainError::InvalidRewardTransaction);
                }
                has_pool_output = true;
            } else {
                tracing::warn!(
                    "Unexpected reward output: receiver={}, amount={}",
                    hex_short(&output.receiver.dump_buf()),
                    output.amount
                );
                return Err(BlockchainError::InvalidRewardTransaction);
            }
        }

        if !has_dev_fee {
            return Err(BlockchainError::NoDevFee);
        }
        if !has_pool_output {
            return Err(BlockchainError::InvalidRewardTransaction);
        }
    }

    if !found_reward_tx {
        return Err(BlockchainError::NoDevFee);
    }

    Ok(())
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn hex_short(buf: &[u8; 32]) -> String {
    buf[..4].iter().map(|b| format!("{:02x}", b)).collect()
}

// ============================================================================
// File: handle_share.rs
// Location: snap-coin-pool/src/handle_share.rs
// Version: 1.2.0
// Created: 2026-02-08T12:10:00Z
// Updated: 2026-02-08T20:10:00Z
// LOC: 214
// ============================================================================
