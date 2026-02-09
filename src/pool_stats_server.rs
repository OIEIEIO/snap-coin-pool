// ============================================================================
// File: pool_stats_server.rs
// Location: snap-coin-pool/src/pool_stats_server.rs
// Version: 1.3.2
//
// Description: WebSocket stats server for real-time pool monitoring dashboard.
//              Broadcasts pool events (miner connections, shares, blocks, payouts,
//              node health, network stats) to connected browser clients via WebSocket.
//
//              Dashboard is served from disk:
//                - GET  /           -> static/pool_dashboard.html
//                - GET  /static/*   -> static assets (css/js/images, etc)
//                - WS   /ws         -> live event feed
//
// Events broadcast:
//   - MinerConnected / MinerDisconnected
//   - ShareAccepted / ShareRejected
//   - BlockFound
//   - PayoutComplete
//   - NodeConnected / NodeHealth
//   - NetworkStats
//
// Architecture:
//   - Axum HTTP server with WebSocket upgrade handler
//   - Tokio broadcast channel for event distribution
//   - Static file serving via tower_http (ServeFile + ServeDir)
//
// CHANGELOG (v1.3.2):
//   - Add avg_block_time_secs field to NetworkStats variant.
//
// CHANGELOG (v1.3.1):
//   - Add timestamp field to NetworkStats variant for consistency with all
//     other PoolEvent variants. Requires matching update in pool_api_server.rs
//     emit site (v1.8.2+).
//
// Notes:
//   - Intentionally avoids `socket.split()` to keep dependencies minimal.
// ============================================================================

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::Response,
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tower_http::{
    cors::{Any, CorsLayer},
    services::{ServeDir, ServeFile},
};

// ── Pool events ─────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PoolEvent {
    MinerConnected {
        miner: String,
        ip: String,
        timestamp: u64,
    },
    MinerDisconnected {
        miner: String,
        ip: String,
        timestamp: u64,
    },
    ShareAccepted {
        miner: String,
        height: u64,
        work_units: u128,
        work_total: u128,
        timestamp: u64,
    },
    ShareRejected {
        miner: String,
        reason: String,
        timestamp: u64,
    },
    BlockFound {
        height: u64,
        hash: String,
        reward: u64,
        timestamp: u64,
    },
    PayoutComplete {
        height: u64,
        miners_paid: usize,
        total_reward: u64,
        pool_fee: u64,
        timestamp: u64,
    },

    // ── Node connectivity + latency ─────────────────────────────────────
    //
    // These are emitted by the pool's NodeProxy (pool_api_server.rs) so the
    // dashboard can show pool↔node RTT and reconnect behavior.

    /// Emitted once on the first successful node response after a connect/reconnect.
    NodeConnected {
        addr: String,
        tcp_connect_ms: u64,
        first_ok_ms: u64,
        timestamp: u64,
    },

    /// Emitted periodically (rate-limited) to report pool↔node RTT health.
    NodeHealth {
        addr: String,
        ok: bool,
        rtt_ms: u64,
        fails: u32,
        last_ok_ts: u64,
        timestamp: u64,
    },

    // ── Network stats snapshot ──────────────────────────────────────────
    //
    // Emitted by NodeProxy (pool_api_server.rs) on the same cadence as NodeHealth.

    /// Network-wide stats snapshot (height, difficulty, hashrate, reward, last block).
    NetworkStats {
        hashrate_hs: u64,
        difficulty: u64,
        height: u64,
        reward: u64,
        last_hash: String,
        last_block_secs_ago: u64,
        avg_block_time_secs: u64,
        timestamp: u64,
    },
}

// ── Event sender (cloneable handle for pool_api_server) ────────────────────

#[derive(Clone)]
pub struct PoolEventSender {
    tx: broadcast::Sender<PoolEvent>,
}

impl PoolEventSender {
    pub fn send(&self, event: PoolEvent) {
        // Ignore errors if no receivers (dashboard not connected)
        let _ = self.tx.send(event);
    }
}

// ── Server state ────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    event_tx: broadcast::Sender<PoolEvent>,
}

// ── Stats server ────────────────────────────────────────────────────────────

pub struct PoolStatsServer {
    port: u16,
    event_tx: broadcast::Sender<PoolEvent>,
}

impl PoolStatsServer {
    /// Create a new stats server.
    /// Returns (server, event_sender) — pass event_sender to PoolServer.
    pub fn new(port: u16) -> (Self, PoolEventSender) {
        // Broadcast channel with 100-event buffer (old events dropped if no receivers)
        let (tx, _rx) = broadcast::channel(100);

        let server = Self {
            port,
            event_tx: tx.clone(),
        };

        let sender = PoolEventSender { tx };

        (server, sender)
    }

    /// Start the stats server (HTTP + WebSocket on specified port).
    pub async fn listen(self) -> anyhow::Result<()> {
        let state = AppState {
            event_tx: self.event_tx,
        };

        let cors = CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any);

        // NOTE:
        //   Create this file:
        //     snap-coin-pool/static/pool_dashboard.html
        //
        //   Any other assets can live under:
        //     snap-coin-pool/static/*
        let app = Router::new()
            .route("/ws", get(websocket_handler))
            .route_service("/", ServeFile::new("static/pool_dashboard.html"))
            .nest_service("/static", ServeDir::new("static"))
            .layer(cors)
            .with_state(state);

        let bind_addr = format!("0.0.0.0:{}", self.port);
        let listener = tokio::net::TcpListener::bind(&bind_addr).await?;

        tracing::info!("Stats server listening on http://{}", bind_addr);
        tracing::info!("Dashboard: http://localhost:{}/", self.port);
        tracing::info!("WS feed: ws://localhost:{}/ws", self.port);

        axum::serve(listener, app).await?;

        Ok(())
    }
}

// ── WebSocket handlers ─────────────────────────────────────────────────────

/// WebSocket upgrade handler.
async fn websocket_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(|socket| websocket_connection(socket, state))
}

/// Handle a single WebSocket connection.
///
/// We intentionally avoid `socket.split()` to avoid adding a `futures-util`
/// direct dependency. We only *send* events; we don't read client messages.
async fn websocket_connection(mut socket: WebSocket, state: AppState) {
    let mut rx = state.event_tx.subscribe();

    // Send initial welcome message
    let welcome = serde_json::json!({
        "type": "connected",
        "message": "Pool stats feed connected"
    });

    if let Ok(msg) = serde_json::to_string(&welcome) {
        let _ = socket.send(Message::Text(msg)).await;
    }

    // Forward broadcast events to this WebSocket client
    while let Ok(event) = rx.recv().await {
        if let Ok(json) = serde_json::to_string(&event) {
            if socket.send(Message::Text(json)).await.is_err() {
                break; // Client disconnected
            }
        }
    }

    tracing::debug!("WebSocket client disconnected");
}

// ============================================================================
// File: pool_stats_server.rs
// Location: snap-coin-pool/src/pool_stats_server.rs
// Version: 1.3.2
// Created: 2026-02-08T22:30:00Z
// Updated: 2026-02-09T01:45:00Z
// ============================================================================