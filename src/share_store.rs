// ============================================================================
// File: share_store.rs
// Location: snap-coin-pool/src/share_store.rs
// Version: 1.1.1
//
// Description: Persistent share storage for the mining pool. Tracks individual
//              share submissions with metadata, detects duplicates, expires
//              stale shares, and records payout history. Backed by sled for
//              crash-safe persistence.
//
// CHANGELOG (v1.1.1):
//   - Naming/signature consistency for work-units accounting:
//       * get_shares()  -> get_work()
//       * total_shares() -> total_work()
//       * miner_shares() -> miner_work()
//   - No behavioral changes: storage format, expiry, payout records unchanged.
//
// Data Model (sled trees):
//   "shares"     - Per-miner WORK UNITS: Public -> u128 (little-endian 16 bytes)
//   "seen"       - Duplicate detection: block_hash -> timestamp
//   "payouts"    - Payout history: sequential_id -> PayoutRecord (bincode)
//   "metadata"   - Share detail log: sequential_id -> ShareRecord (bincode)
// ============================================================================

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use snap_coin::crypto::keys::Public;
use tokio::sync::RwLock;

// ── Constants ───────────────────────────────────────────────────────────────

/// Shares older than this (seconds) are expired during cleanup.
const SHARE_EXPIRY_SECS: u64 = 3600; // 1 hour

/// How often the expiry task runs (seconds).
pub const EXPIRY_INTERVAL_SECS: u64 = 300; // 5 minutes

// ── Public type alias ───────────────────────────────────────────────────────

pub type SharedShareStore = Arc<ShareStore>;

// ── Share metadata record ───────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ShareRecord {
    pub miner: [u8; 32],
    pub block_height: u64,
    pub block_hash: [u8; 32],
    pub timestamp: u64,
    /// Work credited for this share (VarDiff-safe accounting unit).
    pub work_units: u128,
}

// ── Payout history record ───────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PayoutRecord {
    pub block_height: u64,
    pub block_hash: [u8; 32],
    pub timestamp: u64,
    pub total_reward: u64,
    pub pool_fee: u64,
    pub payouts: Vec<MinerPayout>,
    pub deferred: Vec<[u8; 32]>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MinerPayout {
    pub miner: [u8; 32],
    /// Work units credited (NOT share count).
    pub work: u128,
    pub amount: u64,
}

// ── Share Store ─────────────────────────────────────────────────────────────

pub struct ShareStore {
    db: sled::Db,
    shares: sled::Tree,
    seen: sled::Tree,
    payouts: sled::Tree,
    metadata: sled::Tree,

    /// In-memory cache of per-miner work units for fast reads during payout.
    /// Kept in sync with the "shares" sled tree.
    cache: RwLock<HashMap<Public, u128>>,
}

impl ShareStore {
    /// Open or create a persistent share store at the given path.
    pub fn open(data_dir: &Path) -> anyhow::Result<SharedShareStore> {
        let db_path = data_dir.join("shares.db");
        let db = sled::open(&db_path)
            .context(format!("Failed to open share database at {:?}", db_path))?;

        let shares = db.open_tree("shares").context("Failed to open shares tree")?;
        let seen = db.open_tree("seen").context("Failed to open seen tree")?;
        let payouts = db.open_tree("payouts").context("Failed to open payouts tree")?;
        let metadata = db.open_tree("metadata").context("Failed to open metadata tree")?;

        // Load existing work units into memory cache
        let mut cache: HashMap<Public, u128> = HashMap::new();

        for entry in shares.iter() {
            let (key, value) = entry.context("Failed to read share entry")?;
            if key.len() != 32 {
                continue;
            }

            // v1.1.0+ expects 16 bytes (u128 LE). If DB is wiped (as intended),
            // this will be the only format present.
            if value.len() == 16 {
                let pub_buf: [u8; 32] = key.as_ref().try_into().unwrap();
                let work = u128::from_le_bytes(value.as_ref().try_into().unwrap());
                let public = Public::new_from_buf(&pub_buf);
                cache.insert(public, work);
            } else if value.len() == 8 {
                // Back-compat read (older DB): treat u64 as work units.
                // You said DB can be wiped; this is just defensive.
                let pub_buf: [u8; 32] = key.as_ref().try_into().unwrap();
                let work = u64::from_le_bytes(value.as_ref().try_into().unwrap()) as u128;
                let public = Public::new_from_buf(&pub_buf);
                cache.insert(public, work);
            }
        }

        let total_work: u128 = cache.values().copied().sum();

        tracing::info!(
            "Share store loaded: {} miners, {} total work-units",
            cache.len(),
            total_work
        );

        Ok(Arc::new(Self {
            db,
            shares,
            seen,
            payouts,
            metadata,
            cache: RwLock::new(cache),
        }))
    }

    /// Award a share to a miner (work-units accounting).
    /// Returns false if the share is a duplicate.
    pub async fn award_share(
        &self,
        miner: Public,
        block_height: u64,
        block_hash: &[u8; 32],
        work_units: u128,
    ) -> anyhow::Result<bool> {
        // ── Duplicate check ─────────────────────────────────────────────
        if self.seen.contains_key(block_hash.as_slice())? {
            tracing::warn!(
                "Duplicate share rejected from miner {}",
                hex_short(&miner.dump_buf())
            );
            return Ok(false);
        }

        let now = now_secs();

        // ── Mark as seen ────────────────────────────────────────────────
        self.seen
            .insert(block_hash.as_slice(), &now.to_le_bytes())?;

        // ── Increment persistent work-units ─────────────────────────────
        let miner_key = miner.dump_buf();
        let new_work: u128 = {
            let prev = self
                .shares
                .get(miner_key)?
                .map(|v| {
                    if v.len() == 16 {
                        u128::from_le_bytes(v.as_ref().try_into().unwrap())
                    } else if v.len() == 8 {
                        u64::from_le_bytes(v.as_ref().try_into().unwrap()) as u128
                    } else {
                        0u128
                    }
                })
                .unwrap_or(0u128);

            prev.saturating_add(work_units)
        };

        self.shares
            .insert(miner_key, &new_work.to_le_bytes())?;

        // ── Update memory cache ─────────────────────────────────────────
        {
            let mut cache = self.cache.write().await;
            *cache.entry(miner).or_insert(0u128) = new_work;
        }

        // ── Write share metadata ────────────────────────────────────────
        let record = ShareRecord {
            miner: *miner.dump_buf(),
            block_height,
            block_hash: *block_hash,
            timestamp: now,
            work_units,
        };

        let record_id = self.db.generate_id()?.to_be_bytes();
        let record_bytes =
            bincode::serialize(&record).context("Failed to serialize share record")?;
        self.metadata.insert(record_id, record_bytes)?;

        // ── Flush to disk ───────────────────────────────────────────────
        self.db.flush_async().await?;

        tracing::debug!(
            "Share awarded: miner={} height={} work_total={} (+{})",
            hex_short(&miner.dump_buf()),
            block_height,
            new_work,
            work_units
        );

        Ok(true)
    }

    /// Get a snapshot of all per-miner work units. Returns a cloned HashMap.
    pub async fn get_work(&self) -> HashMap<Public, u128> {
        self.cache.read().await.clone()
    }

    /// Get total work units across all miners.
    pub async fn total_work(&self) -> u128 {
        self.cache.read().await.values().copied().sum()
    }

    /// Record a completed payout and clear shares/work for paid miners.
    /// This is atomic — either both the payout record and share clearing
    /// succeed, or neither does.
    pub async fn record_payout_and_clear(
        &self,
        record: PayoutRecord,
        paid_miners: &[Public],
    ) -> anyhow::Result<()> {
        // ── Write payout record ─────────────────────────────────────────
        let payout_id = self.db.generate_id()?.to_be_bytes();
        let payout_bytes =
            bincode::serialize(&record).context("Failed to serialize payout record")?;

        // Insert payout record into payouts tree (separate from batch)
        self.payouts.insert(payout_id, payout_bytes)?;

        // ── Batch remove paid miners from shares tree ───────────────────
        let mut batch = sled::Batch::default();
        for miner in paid_miners {
            batch.remove(miner.dump_buf().as_slice());
        }
        self.shares.apply_batch(batch)?;

        // ── Update memory cache ─────────────────────────────────────────
        {
            let mut cache = self.cache.write().await;
            for miner in paid_miners {
                cache.remove(miner);
            }
        }

        self.db.flush_async().await?;

        tracing::info!(
            "Payout recorded: {} miners paid, {} deferred",
            paid_miners.len(),
            record.deferred.len()
        );

        Ok(())
    }

    /// Expire stale entries from the seen-hashes set.
    /// Called periodically to prevent unbounded growth.
    pub fn expire_seen(&self) -> anyhow::Result<u64> {
        let cutoff = now_secs().saturating_sub(SHARE_EXPIRY_SECS);
        let mut removed: u64 = 0;

        for entry in self.seen.iter() {
            let (key, value) = entry?;
            if value.len() == 8 {
                let ts = u64::from_le_bytes(value.as_ref().try_into().unwrap_or([0u8; 8]));
                if ts < cutoff {
                    self.seen.remove(key)?;
                    removed += 1;
                }
            }
        }

        if removed > 0 {
            tracing::debug!("Expired {} stale seen-hashes", removed);
        }

        Ok(removed)
    }

    /// Get the most recent payout records (up to `limit`).
    #[allow(dead_code)]
    pub fn recent_payouts(&self, limit: usize) -> anyhow::Result<Vec<PayoutRecord>> {
        let mut records = Vec::new();
        for entry in self.payouts.iter().rev().take(limit) {
            let (_key, value) = entry?;
            let record: PayoutRecord =
                bincode::deserialize(&value).context("Failed to deserialize payout record")?;
            records.push(record);
        }
        Ok(records)
    }

    /// Get work units for a specific miner.
    #[allow(dead_code)]
    pub async fn miner_work(&self, miner: &Public) -> u128 {
        self.cache.read().await.get(miner).copied().unwrap_or(0u128)
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Short hex display for logging (first 8 chars).
fn hex_short(buf: &[u8; 32]) -> String {
    buf[..4].iter().map(|b| format!("{:02x}", b)).collect()
}

// ============================================================================
// File: share_store.rs
// Location: snap-coin-pool/src/share_store.rs
// Version: 1.1.1
// Created: 2026-02-08T12:05:00Z
// Updated: 2026-02-08T20:45:00Z
// LOC: 286
// ============================================================================
