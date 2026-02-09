// ============================================================================
// File: config.rs
// Location: snap-coin-pool/src/config.rs
// Version: 1.0.1
//
// Description: Pool configuration module. Parses .env via dotenvy/envy into
//              a validated, typed Config struct. Handles hex difficulty
//              parsing, key validation, and sensible defaults.
//
// CHANGELOG (v1.0.1):
//   - Pool difficulty display now converts target → FULL INTEGER difficulty
//     (no K/M abbreviation). Startup banner shows e.g. "65536" instead of "65.5K".
//
// Environment Variables:
//   POOL_NODE       - Node address:port (required)
//   POOL_PRIVATE    - Pool private key, base36 (required)
//   POOL_DEV        - Dev/operator wallet, base36 (required)
//   POOL_DIFFICULTY  - Share difficulty, 64-char hex string (required)
//   POOL_FEE        - Pool fee as decimal, e.g. 0.02 (default: 0.02)
//   POOL_PORT       - Stratum listener port (default: 5110)
//   POOL_DATA_DIR   - Persistent data directory (default: ./pool-data)
//   POOL_MAX_CONN   - Max connections per IP (default: 5)
// ============================================================================

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, anyhow};
use serde::Deserialize;
use snap_coin::crypto::keys::{Private, Public};

// ── Defaults ────────────────────────────────────────────────────────────────

const DEFAULT_PORT: u16 = 5333;
const DEFAULT_FEE: f64 = 0.02;
const DEFAULT_DATA_DIR: &str = "./pool-data";
const DEFAULT_MAX_CONN_PER_IP: u32 = 5;
const MAX_FEE: f64 = 0.10;
const MIN_FEE: f64 = 0.0;

// ── Raw env struct (what dotenvy/envy deserializes) ─────────────────────────

#[derive(Deserialize)]
struct RawConfig {
    pool_node: String,
    pool_private: String,
    pool_dev: String,
    pool_difficulty: String,
    #[serde(default = "default_fee")]
    pool_fee: f64,
    #[serde(default = "default_port")]
    pool_port: u16,
    #[serde(default = "default_data_dir")]
    pool_data_dir: String,
    #[serde(default = "default_max_conn")]
    pool_max_conn: u32,
}

fn default_fee() -> f64 { DEFAULT_FEE }
fn default_port() -> u16 { DEFAULT_PORT }
fn default_data_dir() -> String { DEFAULT_DATA_DIR.to_string() }
fn default_max_conn() -> u32 { DEFAULT_MAX_CONN_PER_IP }

// ── Validated config (what the rest of the pool uses) ───────────────────────

pub struct Config {
    pub node_addr: SocketAddr,
    pub pool_private: Private,
    pub pool_public: Public,
    pub pool_dev: Public,
    pub pool_difficulty: [u8; 32],
    pub pool_fee: f64,
    pub port: u16,
    pub data_dir: PathBuf,
    pub max_conn_per_ip: u32,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let _ = dotenvy::dotenv();

        let raw: RawConfig = envy::from_env()
            .context("Failed to parse environment variables. Check .env file.")?;

        let node_addr: SocketAddr = raw.pool_node.parse()
            .context(format!("POOL_NODE '{}' is not a valid address:port", raw.pool_node))?;

        let pool_private = Private::new_from_base36(&raw.pool_private)
            .ok_or_else(|| anyhow!("POOL_PRIVATE is not a valid base36 private key"))?;

        let pool_public = pool_private.to_public();

        let pool_dev = Public::new_from_base36(&raw.pool_dev)
            .ok_or_else(|| anyhow!("POOL_DEV is not a valid base36 public key"))?;

        if pool_dev == pool_public {
            tracing::warn!("POOL_DEV is the same as the pool mining address.");
        }

        let pool_difficulty = parse_hex_difficulty(&raw.pool_difficulty)
            .context("POOL_DIFFICULTY must be a 64-character hex string")?;

        if raw.pool_fee < MIN_FEE || raw.pool_fee > MAX_FEE {
            return Err(anyhow!("POOL_FEE {} is out of range [{}, {}]", raw.pool_fee, MIN_FEE, MAX_FEE));
        }

        if raw.pool_port == 0 {
            return Err(anyhow!("POOL_PORT cannot be 0"));
        }

        let data_dir = PathBuf::from(&raw.pool_data_dir);
        ensure_data_dir(&data_dir)
            .context(format!("POOL_DATA_DIR '{}'", raw.pool_data_dir))?;

        if raw.pool_max_conn == 0 {
            return Err(anyhow!("POOL_MAX_CONN cannot be 0"));
        }

        let config = Config {
            node_addr,
            pool_private,
            pool_public,
            pool_dev,
            pool_difficulty,
            pool_fee: raw.pool_fee,
            port: raw.pool_port,
            data_dir,
            max_conn_per_ip: raw.pool_max_conn,
        };

        config.log_summary();

        Ok(config)
    }

    fn log_summary(&self) {
        tracing::info!("─── Pool Configuration ───────────────────────");
        tracing::info!("  Node:        {}", self.node_addr);
        tracing::info!("  Port:        {}", self.port);
        tracing::info!("  Fee:         {:.1}%", self.pool_fee * 100.0);
        tracing::info!(
            "  Difficulty:  {} ({})",
            human_pool_difficulty_from_target_bytes(&self.pool_difficulty),
            hex_encode(&self.pool_difficulty)
        );
        tracing::info!("  Data dir:    {}", self.data_dir.display());
        tracing::info!("  Max conn/IP: {}", self.max_conn_per_ip);
        tracing::info!("──────────────────────────────────────────────");
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────

fn parse_hex_difficulty(hex: &str) -> anyhow::Result<[u8; 32]> {
    let hex = hex.trim();
    let hex = hex.strip_prefix("0x").unwrap_or(hex);

    if hex.len() != 64 {
        return Err(anyhow!("Expected 64 hex characters, got {}", hex.len()));
    }

    let bytes: Vec<u8> = (0..64).step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16)
            .map_err(|_| anyhow!("Invalid hex at {}", i)))
        .collect::<anyhow::Result<Vec<u8>>>()?;

    bytes.try_into().map_err(|_| anyhow!("Failed to convert to [u8; 32]"))
}

fn hex_encode(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

// Convert target → FULL INTEGER difficulty (no K/M)
fn human_pool_difficulty_from_target_bytes(target: &[u8; 32]) -> String {
    let hex = hex_encode(target);
    let leading_zero_nibbles = hex.chars().take_while(|c| *c == '0').count();
    let diff: f64 = 16f64.powi(leading_zero_nibbles as i32);
    format!("{:.0}", diff)
}

fn ensure_data_dir(path: &Path) -> anyhow::Result<()> {
    if !path.exists() {
        std::fs::create_dir_all(path).context("Failed to create data directory")?;
        tracing::info!("Created data directory: {}", path.display());
    } else if !path.is_dir() {
        return Err(anyhow!("Path exists but is not a directory"));
    }
    Ok(())
}

// ============================================================================
// File: config.rs
// Location: snap-coin-pool/src/config.rs
// Version: 1.0.1
// Created: 2026-02-08T12:00:00Z
// Updated: 2026-02-08T16:55:00Z
// LOC: 233
// ============================================================================
