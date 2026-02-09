// ============================================================================
// File: main.rs
// Location: snap-coin-pool/src/main.rs
// Version: 1.3.0
//
// CHANGELOG (v1.3.0):
//   - Add WebSocket stats server on port 5334
//   - Wire pool events to stats dashboard
//   - Spawn both pool server (5333) and stats server (5334)
//   - All existing functionality preserved
// ============================================================================

mod config;
mod handle_block;
mod handle_share;
mod pool_api_server;
mod pool_stats_server;
mod share_store;

use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::signal;
use tokio::time::timeout;

use snap_coin::api::requests::{Request, Response};

use crate::config::Config;
use crate::pool_api_server::PoolServer;
use crate::pool_stats_server::PoolStatsServer;
use crate::share_store::{ShareStore, EXPIRY_INTERVAL_SECS};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const NODE_DIFFICULTY_TIMEOUT: Duration = Duration::from_secs(3);
const STATS_SERVER_PORT: u16 = 5334;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .with_timer(tracing_subscriber::fmt::time::LocalTime::rfc_3339())
        .init();

    tracing::info!("═══════════════════════════════════════════");
    tracing::info!("  Snap Coin Pool v{}", VERSION);
    tracing::info!("═══════════════════════════════════════════");

    // Load config
    let config = Config::from_env()?;

    // ---- Log live network difficulty from node API (tx + block) ----
    {
        let node_api_addr = config.node_addr.to_string();

        match fetch_node_difficulty(&node_api_addr).await {
            Ok((tx_diff, block_diff)) => {
                tracing::info!("─── Difficulty Summary ───────────────────────");
                tracing::info!(
                    "  Pool:        {} ({})",
                    difficulty_full_int_from_target(&config.pool_difficulty),
                    hex_encode(&config.pool_difficulty),
                );
                tracing::info!(
                    "  Network TX:  {} ({})",
                    difficulty_full_int_from_target(&tx_diff),
                    hex_encode(&tx_diff),
                );
                tracing::info!(
                    "  Network BLK: {} ({})",
                    difficulty_full_int_from_target(&block_diff),
                    hex_encode(&block_diff),
                );
                tracing::info!("──────────────────────────────────────────────");
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to fetch live network difficulty from node {}: {}",
                    node_api_addr,
                    e
                );
            }
        }
    }

    // Share store
    let share_store = ShareStore::open(&config.data_dir)?;
    tracing::info!("Share store opened at {:?}", config.data_dir);

    // Share expiry background task
    let expiry_store = share_store.clone();
    let expiry_handle = tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(EXPIRY_INTERVAL_SECS));
        loop {
            interval.tick().await;
            let _ = expiry_store.expire_seen();
        }
    });

    // ── WebSocket stats server ─────────────────────────────────────────
    let (stats_server, event_sender) = PoolStatsServer::new(STATS_SERVER_PORT);
    
    let stats_handle = tokio::spawn(async move {
        if let Err(e) = stats_server.listen().await {
            tracing::error!("Stats server error: {}", e);
        }
    });

    // ── Pool server (API-only node proxy + event broadcasting) ─────────
    let node_api_addr = config.node_addr.to_string();

    let pool_server = Arc::new(PoolServer::new(
        config.port,
        node_api_addr,
        config.pool_difficulty,
        config.pool_private,
        config.pool_public,
        config.pool_dev,
        config.pool_fee,
        share_store.clone(),
        config.max_conn_per_ip,
        event_sender,
    ));

    let server_handle = PoolServer::listen(pool_server).await?;

    tracing::info!("Pool is running. Press Ctrl+C to shut down.");

    let _ = signal::ctrl_c().await;

    tracing::info!("Shutting down...");

    expiry_handle.abort();
    server_handle.abort();
    stats_handle.abort();

    // VarDiff foundation: final summary is work-units, not shares.
    let total_work = share_store.total_work().await;
    let work_map = share_store.get_work().await;

    tracing::info!(
        "Final state: {} miners, {} total unpaid work-units",
        work_map.len(),
        total_work
    );

    tracing::info!("Shutdown complete.");
    Ok(())
}

// ── Node difficulty fetch (live from API) ───────────────────────────────────

async fn fetch_node_difficulty(node_api_addr: &str) -> anyhow::Result<([u8; 32], [u8; 32])> {
    let mut stream = timeout(NODE_DIFFICULTY_TIMEOUT, TcpStream::connect(node_api_addr)).await??;

    let req = Request::Difficulty;
    let buf = req.encode()?;
    timeout(NODE_DIFFICULTY_TIMEOUT, stream.write_all(&buf)).await??;

    let resp = timeout(NODE_DIFFICULTY_TIMEOUT, Response::decode_from_stream(&mut stream)).await??;

    match resp {
        Response::Difficulty {
            transaction_difficulty,
            block_difficulty,
        } => Ok((transaction_difficulty, block_difficulty)),
        _ => Err(anyhow::anyhow!("unexpected node response for Difficulty")),
    }
}

// ── Difficulty formatting helpers ───────────────────────────────────────────

// Full integer difficulty approximation based on leading-zero nibbles of the target.
// difficulty ≈ 16^(leading_zero_nibbles)
fn difficulty_full_int_from_target(target: &[u8; 32]) -> String {
    let lz_nibbles = leading_zero_nibbles_256(target);
    let diff: f64 = 16f64.powi(lz_nibbles as i32);
    format!("{:.0}", diff)
}

fn leading_zero_nibbles_256(target: &[u8; 32]) -> u32 {
    let mut nibbles: u32 = 0;

    for &b in target.iter() {
        if b == 0 {
            nibbles += 2;
            continue;
        }

        // b != 0: check high nibble first
        if (b & 0xF0) == 0 {
            nibbles += 1;
        }
        break;
    }

    nibbles
}

fn hex_encode(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

// ============================================================================
// File: main.rs
// Location: snap-coin-pool/src/main.rs
// Version: 1.3.0
// Created: 2026-02-08T00:00:00Z
// Updated: 2026-02-08T22:50:00Z
// LOC: 228
// ============================================================================