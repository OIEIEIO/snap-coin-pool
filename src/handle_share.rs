// ============================================================================
// File: handle_share.rs
// Location: snap-coin-pool/src/handle_share.rs
// Version: 1.1.0-share-diff.1
//
// Changes from v1.0.0:
//   - Compute actual share difficulty (MAX_TARGET / hash) from submitted block hash
//   - Return Ok(share_diff: u64) instead of Ok(()) so caller can emit it
//   - Add share_difficulty_from_hash() helper (same clamped BigUint division
//     used in pool_stats_server.rs for network difficulty)
// ============================================================================

use num_bigint::BigUint;
use snap_coin::{
    core::{
        block::{Block, BlockError},
        blockchain::BlockchainError,
        transaction::TransactionId,
    },
    crypto::{
        address_inclusion_filter::AddressInclusionFilter, keys::Public, merkle_tree::MerkleTree,
    },
    economics::EXPIRATION_TIME,
};

use crate::share_store::SharedShareStore;

/// Compute difficulty from hash bytes: MAX_TARGET / hash_value.
/// Returns 0 if hash is zero (should never happen on a valid share).
pub fn block_difficulty_from_hash(hash_bytes: &[u8]) -> u64 {
    share_difficulty_from_hash(hash_bytes)
}

/// Compute share difficulty from hash bytes: MAX_TARGET / hash_value.
/// Returns 0 if hash is zero (should never happen on a valid share).
fn share_difficulty_from_hash(hash_bytes: &[u8]) -> u64 {
    let hash_val = BigUint::from_bytes_be(hash_bytes);
    if hash_val == BigUint::ZERO {
        return 0;
    }
    let max_target = BigUint::from_bytes_be(&[0xFFu8; 32]);
    let diff = max_target / hash_val;
    // Clamp to u64::MAX
    let bytes = diff.to_bytes_be();
    if bytes.len() <= 8 {
        let mut buf = [0u8; 8];
        buf[8 - bytes.len()..].copy_from_slice(&bytes);
        u64::from_be_bytes(buf)
    } else {
        u64::MAX
    }
}

/// Returns Ok(share_diff) on a valid share, Err on rejection.
pub async fn handle_share(
    current_job: &Block,
    new_block: &Block,
    share_store: &SharedShareStore,
    client_address: Public,
    pool_difficulty: &[u8; 32],
) -> Result<u64, BlockchainError> {
    new_block.check_completeness()?;

    let mut current_job = current_job.clone();

    // Remove expired transactions with 10s margin
    let mut removed_txs = false;
    current_job.transactions.retain(|tx| {
        let expired = tx.timestamp + EXPIRATION_TIME + 10 < chrono::Utc::now().timestamp() as u64;
        if expired {
            removed_txs = true;
        }
        !expired
    });

    // Update merkle tree and filter if transactions were removed
    if removed_txs {
        current_job.meta.merkle_tree_root = MerkleTree::build(
            &current_job
                .transactions
                .iter()
                .map(|tx| tx.transaction_id.unwrap())
                .collect::<Vec<TransactionId>>(),
        )
        .root_hash();
        current_job.meta.address_inclusion_filter =
            AddressInclusionFilter::create_filter(&current_job.transactions)
                .map_err(|e| BlockchainError::from(BlockError::from(e)))?;
    }

    if current_job.transactions != new_block.transactions {
        return Err(BlockchainError::IncompleteBlock);
    }

    let mut job_meta = current_job.meta.clone();
    let mut new_block_meta = new_block.meta.clone();
    job_meta.hash = None;
    new_block_meta.hash = None;

    if job_meta != new_block_meta {
        return Err(BlockchainError::IncompleteBlock);
    }

    let hash_bytes = new_block.meta.hash.unwrap().dump_buf();

    if BigUint::from_bytes_be(&hash_bytes) > BigUint::from_bytes_be(pool_difficulty) {
        return Err(BlockError::BlockPowDifficultyIncorrect.into());
    }

    let share_diff = share_difficulty_from_hash(&hash_bytes);
    share_store.award_share(client_address).await;
    Ok(share_diff)
}

// ============================================================================
// File: handle_share.rs
// Location: snap-coin-pool/src/handle_share.rs
// Version: 1.1.0-share-diff.1
// Created: 2026-03-29T00:00:00Z
// ============================================================================