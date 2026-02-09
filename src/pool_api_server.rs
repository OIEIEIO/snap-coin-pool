// ============================================================================
// File: pool_api_server.rs
// Location: snap-coin-pool/src/pool_api_server.rs
// Version: 1.8.4
//
// Description: Binary protocol pool server. Handles miner connections using
//              the snap-coin native Request/Response wire format. Manages
//              handshake, request routing, share validation, block submission
//              to a synced node API, and payout triggering.
//
// CHANGELOG (v1.8.4):
//   - ADD: avg_block_time_secs to NetworkStats. Computed by sampling the
//     last AVG_BLOCK_TIME_SAMPLE (10) blocks and averaging the timestamp
//     deltas. Only 2 extra node calls (BlockHash + Block for the older block).
//
// CHANGELOG (v1.8.3):
//   - FIX: NetworkStats last_hash + last_block_secs_ago now query height-1
//     (last completed block) instead of current height (unmined, doesn't exist).
//   - ADD: timestamp field to NetworkStats emit (matches pool_stats_server v1.3.1).
//
// CHANGELOG (v1.8.2):
//   - FIX: NetworkStats difficulty now computed as MAX_TARGET / block_target
//     instead of raw target-to-scalar (which always overflowed to u64::MAX).
//     hashrate_hs is now derived from the corrected difficulty value.
//   - All other behavior unchanged from v1.8.1.
//
// CHANGELOG (v1.8.1):
//   - Fix last_block_secs_ago: BlockMetadata has no timestamp; use Block.timestamp.
//   - NetworkStats event emit remains, but requires PoolEvent::NetworkStats
//     to be added in pool_stats_server.rs (see instructions below).
//
// CHANGELOG (v1.8.0):
//   - Same as v1.7.0 + emits NetworkStats snapshot over WS from NodeProxy.
//     NetworkStats includes:
//       * hashrate_hs (difficulty / 30)
//       * difficulty (scalar u64, human-readable best-effort)
//       * height
//       * reward
//       * last_hash
//       * last_block_secs_ago
//   - Emission is rate-limited alongside NodeHealth (~1 Hz).
//   - Uses existing NodeProxy connection; no new timers/threads.
// ============================================================================

use std::{
    collections::HashMap,
    sync::Arc,
    time::Duration,
};

use anyhow::anyhow;
use num_bigint::BigUint;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    task::JoinHandle,
    time::{timeout, Instant},
};

use snap_coin::{
    api::requests::{Request, Response},
    core::transaction::Transaction,
    crypto::{
        keys::{Private, Public},
        Hash,
    },
    economics::get_block_reward,
};

use crate::{
    handle_block::handle_block,
    handle_share::handle_share,
    pool_stats_server::{PoolEvent, PoolEventSender},
    share_store::SharedShareStore,
};

// ── Constants ───────────────────────────────────────────────────────────────

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(60);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
const NODE_PROXY_TIMEOUT: Duration = Duration::from_secs(10);

const MAX_MEMPOOL_PAGES: u32 = 512;

const PENALTY_ERROR: i32 = 5;
const PENALTY_RESTRICTED: i32 = 10;
const PENALTY_BAN_EXTRA: i32 = 10;

const BAN_THRESHOLD: i32 = 25;

const SCORE_DECAY_AMOUNT: i32 = 1;
const SCORE_DECAY_INTERVAL: Duration = Duration::from_secs(30);

const NODE_HEALTH_EMIT_MIN_MS: u64 = 1000;

// NetworkStats: hashrate rule-of-thumb window
const NETWORK_HASHRATE_WINDOW_SECS: u64 = 30;

// NetworkStats: number of recent blocks to sample for avg block time
const AVG_BLOCK_TIME_SAMPLE: u64 = 10;

// ── VarDiff config (unchanged) ─────────────────────────────────────────────

const VARDIFF_ENABLED: bool = false;
const VARDIFF_NO_SHARE_SECS: u64 = 30;
const VARDIFF_MIN_ADJUST_SECS: u64 = 15;
const VARDIFF_EASE_MULTIPLIER: u32 = 2;
const MAX_TARGET: [u8; 32] = [0xFFu8; 32];
const WORK_FP_SHIFT: u32 = 64;

// ── IP tracking ─────────────────────────────────────────────────────────────

#[derive(Clone)]
struct IpState {
    score: i32,
    banned: bool,
    connections: u32,
}

type IpStateMap = Arc<Mutex<HashMap<String, IpState>>>;

// ── Miner session ───────────────────────────────────────────────────────────

struct MinerSession {
    target: [u8; 32],
    work_units: u128,
    last_share_at: Instant,
    last_adjust_at: Instant,
}

impl MinerSession {
    fn new(base_target: [u8; 32]) -> Self {
        let now = Instant::now();
        Self {
            target: base_target,
            work_units: compute_work_units_q64_64(&base_target, &base_target),
            last_share_at: now,
            last_adjust_at: now,
        }
    }
}

// ── NodeProxy ───────────────────────────────────────────────────────────────

#[derive(Clone)]
struct NodeProxy {
    addr: String,
    inner: Arc<Mutex<Option<TcpStream>>>,
    metrics: Arc<Mutex<NodeMetrics>>,
    event_sender: PoolEventSender,
}

#[derive(Clone, Debug)]
struct NodeMetrics {
    last_tcp_connect_ms: Option<u64>,
    last_rtt_ms: Option<u64>,
    last_ok_ts: Option<u64>,
    fails: u32,
    ever_ok: bool,
    first_ok_after_connect_emitted: bool,
    first_ok_ms: Option<u64>,
    last_health_emit_ms: u64,
}

impl NodeMetrics {
    fn new() -> Self {
        Self {
            last_tcp_connect_ms: None,
            last_rtt_ms: None,
            last_ok_ts: None,
            fails: 0,
            ever_ok: false,
            first_ok_after_connect_emitted: false,
            first_ok_ms: None,
            last_health_emit_ms: 0,
        }
    }
}

impl NodeProxy {
    pub fn new(addr: impl Into<String>, event_sender: PoolEventSender) -> Self {
        Self {
            addr: addr.into(),
            inner: Arc::new(Mutex::new(None)),
            metrics: Arc::new(Mutex::new(NodeMetrics::new())),
            event_sender,
        }
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    async fn ensure_connected(&self) -> anyhow::Result<()> {
        let mut guard = self.inner.lock().await;
        if guard.is_some() {
            return Ok(());
        }

        {
            let mut m = self.metrics.lock().await;
            m.first_ok_after_connect_emitted = false;
            m.first_ok_ms = None;
        }

        let t0 = Instant::now();
        let stream = TcpStream::connect(&self.addr).await?;
        let connect_ms = t0.elapsed().as_millis() as u64;

        {
            let mut m = self.metrics.lock().await;
            m.last_tcp_connect_ms = Some(connect_ms);
        }

        *guard = Some(stream);
        Ok(())
    }

    async fn reconnect(&self) -> anyhow::Result<()> {
        let mut guard = self.inner.lock().await;
        *guard = None;
        drop(guard);
        self.ensure_connected().await
    }

    /// Emit NodeHealth (rate-limited). Returns true if an emit occurred.
    async fn maybe_emit_node_health(&self, ok: bool, rtt_ms: Option<u64>) -> bool {
        let now_ms = now_millis();
        let mut m = self.metrics.lock().await;

        if let Some(r) = rtt_ms {
            m.last_rtt_ms = Some(r);
        }

        if now_ms.saturating_sub(m.last_health_emit_ms) < NODE_HEALTH_EMIT_MIN_MS {
            return false;
        }
        m.last_health_emit_ms = now_ms;

        self.event_sender.send(PoolEvent::NodeHealth {
            addr: self.addr.clone(),
            ok,
            rtt_ms: rtt_ms.unwrap_or(0),
            fails: m.fails,
            last_ok_ts: m.last_ok_ts.unwrap_or(0),
            timestamp: now_secs(),
        });

        true
    }

    async fn maybe_emit_node_connected_on_first_ok(&self, first_rtt_ms: u64) {
        let mut m = self.metrics.lock().await;
        if m.first_ok_after_connect_emitted {
            return;
        }
        m.first_ok_after_connect_emitted = true;

        let tcp_ms = m.last_tcp_connect_ms.unwrap_or(0);
        let first_ok_ms = tcp_ms.saturating_add(first_rtt_ms);
        m.first_ok_ms = Some(first_ok_ms);

        self.event_sender.send(PoolEvent::NodeConnected {
            addr: self.addr.clone(),
            tcp_connect_ms: tcp_ms,
            first_ok_ms,
            timestamp: now_secs(),
        });
    }

    /// Quiet call used internally to avoid recursion while taking NetworkStats snapshot.
    async fn call_quiet(&self, req: Request) -> anyhow::Result<Response> {
        self.ensure_connected().await?;

        let resp = {
            let mut guard = self.inner.lock().await;
            let stream = guard.as_mut().ok_or_else(|| anyhow!("node proxy not connected"))?;

            timeout(NODE_PROXY_TIMEOUT, async {
                let buf = req.encode()?;
                stream.write_all(&buf).await?;
                let response = Response::decode_from_stream(stream).await?;
                Ok::<Response, anyhow::Error>(response)
            })
            .await
        };

        match resp {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(e)) => {
                let _ = self.reconnect().await;
                Err(e)
            }
            Err(_) => {
                let _ = self.reconnect().await;
                Err(anyhow!("node proxy request timeout"))
            }
        }
    }

    async fn emit_network_stats_snapshot_quiet(&self) {
        let height = match self.call_quiet(Request::Height).await {
            Ok(Response::Height { height }) => height,
            _ => return,
        };

        let (_tx_diff, block_diff_bytes) = match self.call_quiet(Request::Difficulty).await {
            Ok(Response::Difficulty {
                transaction_difficulty,
                block_difficulty,
            }) => (transaction_difficulty, block_difficulty),
            _ => return,
        };

        // ── v1.8.2 FIX ─────────────────────────────────────────────────
        // Difficulty = MAX_TARGET / block_target (not raw target-to-scalar).
        // The raw 256-bit target almost always exceeds u64::MAX, so the old
        // approach clamped to u64::MAX every time. Computing the ratio gives
        // a meaningful human-readable difficulty that fits in u64.
        let max_bi = BigUint::from_bytes_be(&MAX_TARGET);
        let target_bi = BigUint::from_bytes_be(&block_diff_bytes);

        let difficulty_u64 = if biguint_is_zero(&target_bi) {
            // Target of zero means infinite difficulty — clamp to u64::MAX
            u64::MAX
        } else {
            let diff_bi = &max_bi / &target_bi;
            match biguint_to_u128(&diff_bi) {
                Some(v) => {
                    if v > (u64::MAX as u128) {
                        u64::MAX
                    } else {
                        v as u64
                    }
                }
                None => u64::MAX,
            }
        };

        let hashrate_hs = if difficulty_u64 > 0 && difficulty_u64 < u64::MAX {
            difficulty_u64 / NETWORK_HASHRATE_WINDOW_SECS
        } else {
            0
        };
        // ── end v1.8.2 FIX ──────────────────────────────────────────────

        // Last completed block hash + age + avg block time (best-effort).
        // height from the node is the *next* block to mine, so the last
        // completed block is at height - 1.
        let mut last_hash_str = String::from("unknown");
        let mut last_block_secs_ago = 0u64;
        let mut avg_block_time_secs = 0u64;
        let mut last_block_ts: Option<u64> = None;
        let last_height = height.saturating_sub(1);

        if last_height > 0 {
            if let Ok(Response::BlockHash { hash }) = self.call_quiet(Request::BlockHash { height: last_height }).await {
                if let Some(h) = hash {
                    last_hash_str = hex_encode(&h.dump_buf());

                    if let Ok(Response::Block { block }) = self.call_quiet(Request::Block { block_hash: h }).await {
                        if let Some(b) = block {
                            // v1.8.1 FIX: timestamp is on Block, not BlockMetadata.
                            let ts = b.timestamp as u64;
                            last_block_secs_ago = now_secs().saturating_sub(ts);
                            last_block_ts = Some(ts);
                        }
                    }
                }
            }
        }

        // Avg block time: sample last N blocks by comparing timestamps of
        // block at last_height and block at (last_height - N).
        // Only 2 extra node calls (BlockHash + Block for the older block).
        if let Some(recent_ts) = last_block_ts {
            let sample = AVG_BLOCK_TIME_SAMPLE;
            let older_height = last_height.saturating_sub(sample);

            if older_height > 0 && older_height < last_height {
                if let Ok(Response::BlockHash { hash }) = self.call_quiet(Request::BlockHash { height: older_height }).await {
                    if let Some(oh) = hash {
                        if let Ok(Response::Block { block }) = self.call_quiet(Request::Block { block_hash: oh }).await {
                            if let Some(ob) = block {
                                let older_ts = ob.timestamp as u64;
                                let span = last_height.saturating_sub(older_height);
                                if span > 0 && recent_ts > older_ts {
                                    avg_block_time_secs = (recent_ts - older_ts) / span;
                                }
                            }
                        }
                    }
                }
            }
        }

        // Reward for the *next* block (current mining target)
        let reward = get_block_reward(height as usize);

        self.event_sender.send(PoolEvent::NetworkStats {
            hashrate_hs,
            difficulty: difficulty_u64,
            height,
            reward,
            last_hash: last_hash_str,
            last_block_secs_ago,
            avg_block_time_secs,
            timestamp: now_secs(),
        });
    }

    async fn call(&self, req: Request) -> anyhow::Result<Response> {
        self.ensure_connected().await?;

        let t0 = Instant::now();
        let resp = {
            let mut guard = self.inner.lock().await;
            let stream = guard.as_mut().ok_or_else(|| anyhow!("node proxy not connected"))?;

            timeout(NODE_PROXY_TIMEOUT, async {
                let buf = req.encode()?;
                stream.write_all(&buf).await?;
                let response = Response::decode_from_stream(stream).await?;
                Ok::<Response, anyhow::Error>(response)
            })
            .await
        };

        match resp {
            Ok(Ok(r)) => {
                let rtt_ms = t0.elapsed().as_millis() as u64;

                {
                    let mut m = self.metrics.lock().await;
                    m.last_rtt_ms = Some(rtt_ms);
                    m.last_ok_ts = Some(now_secs());
                    m.fails = 0;
                    if !m.ever_ok {
                        m.ever_ok = true;
                    }
                }

                self.maybe_emit_node_connected_on_first_ok(rtt_ms).await;

                // Only snapshot NetworkStats when we actually emit health (same cadence)
                let emitted = self.maybe_emit_node_health(true, Some(rtt_ms)).await;
                if emitted {
                    self.emit_network_stats_snapshot_quiet().await;
                }

                Ok(r)
            }
            Ok(Err(e)) => {
                {
                    let mut m = self.metrics.lock().await;
                    m.fails = m.fails.saturating_add(1);
                }
                let _ = self.maybe_emit_node_health(false, None).await;

                let _ = self.reconnect().await;
                Err(e)
            }
            Err(_) => {
                {
                    let mut m = self.metrics.lock().await;
                    m.fails = m.fails.saturating_add(1);
                }
                let _ = self.maybe_emit_node_health(false, None).await;

                let _ = self.reconnect().await;
                Err(anyhow!("node proxy request timeout"))
            }
        }
    }

    // ── Typed helpers ───────────────────────────────────────────────────

    async fn height(&self) -> anyhow::Result<u64> {
        match self.call(Request::Height).await? {
            Response::Height { height } => Ok(height),
            _ => Err(anyhow!("unexpected node response for Height")),
        }
    }

    async fn block(
        &self,
        block_hash: Hash,
    ) -> anyhow::Result<Option<snap_coin::core::block::Block>> {
        match self.call(Request::Block { block_hash }).await? {
            Response::Block { block } => Ok(block),
            _ => Err(anyhow!("unexpected node response for Block")),
        }
    }

    async fn block_hash(&self, height: u64) -> anyhow::Result<Option<Hash>> {
        match self.call(Request::BlockHash { height }).await? {
            Response::BlockHash { hash } => Ok(hash),
            _ => Err(anyhow!("unexpected node response for BlockHash")),
        }
    }

    async fn block_height(&self, hash: Hash) -> anyhow::Result<Option<usize>> {
        match self.call(Request::BlockHeight { hash }).await? {
            Response::BlockHeight { height } => Ok(height),
            _ => Err(anyhow!("unexpected node response for BlockHeight")),
        }
    }

    async fn difficulty(&self) -> anyhow::Result<([u8; 32], [u8; 32])> {
        match self.call(Request::Difficulty).await? {
            Response::Difficulty {
                transaction_difficulty,
                block_difficulty,
            } => Ok((transaction_difficulty, block_difficulty)),
            _ => Err(anyhow!("unexpected node response for Difficulty")),
        }
    }

    async fn live_transaction_difficulty(&self) -> anyhow::Result<[u8; 32]> {
        match self.call(Request::LiveTransactionDifficulty).await? {
            Response::LiveTransactionDifficulty { live_difficulty } => Ok(live_difficulty),
            _ => Err(anyhow!("unexpected node response for LiveTransactionDifficulty")),
        }
    }

    async fn mempool_page(
        &self,
        page: u32,
    ) -> anyhow::Result<(Vec<snap_coin::core::transaction::Transaction>, Option<u32>)> {
        match self.call(Request::Mempool { page }).await? {
            Response::Mempool { mempool, next_page } => Ok((mempool, next_page)),
            _ => Err(anyhow!("unexpected node response for Mempool")),
        }
    }

    async fn submit_block(&self, block: snap_coin::core::block::Block) -> anyhow::Result<()> {
        match self.call(Request::NewBlock { new_block: block }).await? {
            Response::NewBlock { status } => status.map_err(|e| anyhow!(e.to_string())),
            _ => Err(anyhow!("unexpected node response for NewBlock")),
        }
    }

    async fn submit_tx(&self, tx: Transaction) -> anyhow::Result<()> {
        match self
            .call(Request::NewTransaction {
                new_transaction: tx,
            })
            .await?
        {
            Response::NewTransaction { status } => status.map_err(|e| anyhow!(e.to_string())),
            _ => Err(anyhow!("unexpected node response for NewTransaction")),
        }
    }
}

// ── PoolServer ──────────────────────────────────────────────────────────────

pub struct PoolServer {
    port: u16,
    node: NodeProxy,

    pool_difficulty: [u8; 32],
    pool_private: Private,
    pool_public: Public,
    pool_dev: Public,
    pool_fee: f64,

    share_store: SharedShareStore,

    ip_states: IpStateMap,
    max_conn_per_ip: u32,

    event_sender: PoolEventSender,
}

impl PoolServer {
    pub fn new(
        port: u16,
        node_api_addr: String,
        pool_difficulty: [u8; 32],
        pool_private: Private,
        pool_public: Public,
        pool_dev: Public,
        pool_fee: f64,
        share_store: SharedShareStore,
        max_conn_per_ip: u32,
        event_sender: PoolEventSender,
    ) -> Self {
        PoolServer {
            port,
            node: NodeProxy::new(node_api_addr, event_sender.clone()),
            pool_difficulty,
            pool_private,
            pool_public,
            pool_dev,
            pool_fee,
            share_store,
            ip_states: Arc::new(Mutex::new(HashMap::new())),
            max_conn_per_ip,
            event_sender,
        }
    }

    async fn add_penalty(&self, ip: &str, points: i32) {
        let mut states = self.ip_states.lock().await;
        let entry = states.entry(ip.to_string()).or_insert(IpState {
            score: 0,
            banned: false,
            connections: 0,
        });

        entry.score += points;

        if entry.score >= BAN_THRESHOLD && !entry.banned {
            entry.score += PENALTY_BAN_EXTRA;
            entry.banned = true;
            tracing::warn!("IP {} BANNED (score: {})", ip, entry.score);
        }
    }

    async fn is_banned(&self, ip: &str) -> bool {
        let states = self.ip_states.lock().await;
        states.get(ip).map(|s| s.banned).unwrap_or(false)
    }

    async fn try_acquire_connection(&self, ip: &str) -> bool {
        let mut states = self.ip_states.lock().await;
        let entry = states.entry(ip.to_string()).or_insert(IpState {
            score: 0,
            banned: false,
            connections: 0,
        });

        if entry.connections >= self.max_conn_per_ip {
            return false;
        }

        entry.connections += 1;
        true
    }

    async fn release_connection(&self, ip: &str) {
        let mut states = self.ip_states.lock().await;
        if let Some(entry) = states.get_mut(ip) {
            entry.connections = entry.connections.saturating_sub(1);
        }
    }

    async fn score_decay_task(ip_states: IpStateMap) {
        let mut interval = tokio::time::interval(SCORE_DECAY_INTERVAL);
        loop {
            interval.tick().await;
            let mut states = ip_states.lock().await;

            states.retain(|ip, entry| {
                entry.score = (entry.score - SCORE_DECAY_AMOUNT).max(0);

                if entry.banned && entry.score < BAN_THRESHOLD {
                    entry.banned = false;
                    tracing::info!("IP {} unbanned (score: {})", ip, entry.score);
                }

                entry.score > 0 || entry.banned || entry.connections > 0
            });
        }
    }

    async fn maybe_adjust_vardiff(
        &self,
        stream: &mut TcpStream,
        client_address: &Public,
        session: &mut MinerSession,
    ) -> anyhow::Result<()> {
        if !VARDIFF_ENABLED {
            return Ok(());
        }

        let now = Instant::now();

        let since_share = now.duration_since(session.last_share_at);
        if since_share.as_secs() < VARDIFF_NO_SHARE_SECS {
            return Ok(());
        }

        let since_adjust = now.duration_since(session.last_adjust_at);
        if since_adjust.as_secs() < VARDIFF_MIN_ADJUST_SECS {
            return Ok(());
        }

        let new_target = ease_target(&session.target, VARDIFF_EASE_MULTIPLIER);

        if new_target == session.target {
            session.last_adjust_at = now;
            return Ok(());
        }

        stream.write_all(&new_target).await?;

        let old_target = session.target;
        session.target = new_target;
        session.work_units = compute_work_units_q64_64(&self.pool_difficulty, &session.target);
        session.last_adjust_at = now;

        let old_bits = target_leading_zero_bits(&old_target);
        let new_bits = target_leading_zero_bits(&new_target);
        let ratio = target_difficulty_ratio(&self.pool_difficulty, &new_target);

        tracing::info!(
            "VarDiff update: miner={} {} → {} bits ({:.2}x easier than base) work_units={} (Q64.64) target={} (pushed)",
            hex_short(&client_address.dump_buf()),
            old_bits,
            new_bits,
            ratio,
            session.work_units,
            hex_encode(&session.target)
        );

        Ok(())
    }

    async fn handshake(
        &self,
        stream: &mut TcpStream,
        session: &MinerSession,
    ) -> anyhow::Result<Public> {
        let mut client_public_buf = [0u8; 32];
        stream.read_exact(&mut client_public_buf).await?;
        stream.write_all(&session.target).await?;
        stream.write_all(self.pool_public.dump_buf()).await?;
        Ok(Public::new_from_buf(&client_public_buf))
    }

    async fn handle_request(
        self: &Arc<Self>,
        stream: &mut TcpStream,
        session: &mut MinerSession,
        client_address: Public,
        ip: &str,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let request = Request::decode_from_stream(stream).await?;

        let response = match request {
            Request::Height => {
                let height = self.node.height().await?;
                Response::Height { height }
            }

            Request::Block { block_hash } => {
                let block = self.node.block(block_hash).await?;
                Response::Block { block }
            }

            Request::BlockHash { height } => {
                let hash = self.node.block_hash(height).await?;
                Response::BlockHash { hash }
            }

            Request::BlockHeight { hash } => {
                let height = self.node.block_height(hash).await?;
                Response::BlockHeight { height }
            }

            Request::Reward => {
                let height = self.node.height().await? as usize;
                Response::Reward {
                    reward: get_block_reward(height),
                }
            }

            Request::Difficulty => {
                let (transaction_difficulty, block_difficulty) = self.node.difficulty().await?;
                Response::Difficulty {
                    transaction_difficulty,
                    block_difficulty,
                }
            }

            Request::LiveTransactionDifficulty => {
                let live_difficulty = self.node.live_transaction_difficulty().await?;
                Response::LiveTransactionDifficulty { live_difficulty }
            }

            Request::Mempool { page } => {
                let (mempool, next_page) = self.node.mempool_page(page).await?;
                Response::Mempool { mempool, next_page }
            }

            Request::NewBlock { new_block } => {
                let share_result = self
                    .handle_new_block(new_block, client_address, session.target, session.work_units)
                    .await;

                match &share_result {
                    Ok(()) => {
                        session.last_share_at = Instant::now();
                    }
                    Err(e) => {
                        self.event_sender.send(PoolEvent::ShareRejected {
                            miner: hex_short(&client_address.dump_buf()),
                            reason: format!("{:?}", e),
                            timestamp: now_secs(),
                        });
                    }
                }

                Response::NewBlock { status: share_result }
            }

            Request::SubscribeToChainEvents => {
                self.proxy_chain_events_to_client(stream).await?;
                return Ok(None);
            }

            Request::Transaction { .. }
            | Request::TransactionAndInfo { .. }
            | Request::TransactionsOfAddress { .. }
            | Request::AvailableUTXOs { .. }
            | Request::Balance { .. }
            | Request::Peers
            | Request::NewTransaction { .. } => {
                self.add_penalty(ip, PENALTY_RESTRICTED).await;
                return Err(anyhow!("Restricted API endpoint"));
            }
        };

        let response_buf = response.encode()?;
        Ok(Some(response_buf))
    }

    async fn handle_new_block(
        self: &Arc<Self>,
        new_block: snap_coin::core::block::Block,
        client_address: Public,
        miner_target: [u8; 32],
        miner_work_units: u128,
    ) -> Result<(), snap_coin::core::blockchain::BlockchainError> {
        let chain_height = self
            .node
            .height()
            .await
            .map_err(|e| snap_coin::core::blockchain::BlockchainError::Io(e.to_string()))?;

        let (chain_tx_diff, chain_block_diff) = self
            .node
            .difficulty()
            .await
            .map_err(|e| snap_coin::core::blockchain::BlockchainError::Io(e.to_string()))?;

        let mempool = self
            .fetch_full_mempool_from_node()
            .await
            .map_err(|e| snap_coin::core::blockchain::BlockchainError::Io(e.to_string()))?;

        handle_share(
            chain_height,
            chain_block_diff,
            chain_tx_diff,
            Some(mempool),
            new_block.clone(),
            client_address,
            &miner_target,
            miner_work_units,
            self.pool_public,
            &self.share_store,
        )
        .await?;

        let work_total = self.share_store.total_work().await;

        self.event_sender.send(PoolEvent::ShareAccepted {
            miner: hex_short(&client_address.dump_buf()),
            height: chain_height,
            work_units: miner_work_units,
            work_total,
            timestamp: now_secs(),
        });

        if new_block
            .validate_difficulties(&chain_block_diff, &chain_tx_diff)
            .is_err()
        {
            return Ok(());
        }

        tracing::info!("Block meets network difficulty — submitting to node...");

        let block_hash = new_block
            .meta
            .hash
            .map(|h| hex_encode(&h.dump_buf()))
            .unwrap_or_else(|| "unknown".to_string());

        let block_reward = snap_coin::economics::get_block_reward(chain_height as usize);

        self.event_sender.send(PoolEvent::BlockFound {
            height: chain_height,
            hash: block_hash,
            reward: block_reward,
            timestamp: now_secs(),
        });

        if let Err(e) = self.node.submit_block(new_block.clone()).await {
            tracing::error!("Node rejected block submission: {}", e);
            return Ok(());
        }

        tracing::info!("Node accepted block. Running payout.");

        let node = self.node.clone();
        let submit_tx = move |tx: Transaction| {
            let node = node.clone();
            async move {
                node.submit_tx(tx)
                    .await
                    .map_err(|e| snap_coin::core::blockchain::BlockchainError::Io(e.to_string()))
            }
        };

        if let Err(e) = handle_block(
            chain_height,
            chain_tx_diff,
            new_block,
            self.pool_private,
            self.pool_public,
            self.pool_dev,
            &self.share_store,
            self.pool_fee,
            submit_tx,
        )
        .await
        {
            tracing::error!("Block payout failed: {} (work preserved)", e);
        } else {
            tracing::info!("Block payout completed successfully");

            let pool_fee_amount = (block_reward as f64 * self.pool_fee) as u64;

            self.event_sender.send(PoolEvent::PayoutComplete {
                height: chain_height,
                miners_paid: 0,
                total_reward: block_reward,
                pool_fee: pool_fee_amount,
                timestamp: now_secs(),
            });
        }

        Ok(())
    }

    async fn fetch_full_mempool_from_node(
        &self,
    ) -> anyhow::Result<Vec<snap_coin::core::transaction::Transaction>> {
        let mut out: Vec<snap_coin::core::transaction::Transaction> = Vec::new();
        let mut page: u32 = 0;

        loop {
            if page >= MAX_MEMPOOL_PAGES {
                return Err(anyhow!(
                    "mempool fetch exceeded MAX_MEMPOOL_PAGES={}",
                    MAX_MEMPOOL_PAGES
                ));
            }

            let (mut mempool_page, next_page) = self.node.mempool_page(page).await?;
            out.append(&mut mempool_page);

            match next_page {
                Some(p) => page = p,
                None => break,
            }
        }

        Ok(out)
    }

    async fn proxy_chain_events_to_client(
        &self,
        client_stream: &mut TcpStream,
    ) -> anyhow::Result<()> {
        let mut node_stream = TcpStream::connect(self.node.addr()).await?;

        let req_buf = Request::SubscribeToChainEvents.encode()?;
        node_stream.write_all(&req_buf).await?;

        loop {
            let event_resp = match Response::decode_from_stream(&mut node_stream).await {
                Ok(r) => r,
                Err(_) => break,
            };

            let buf = match event_resp.encode() {
                Ok(b) => b,
                Err(_) => break,
            };

            match timeout(Duration::from_secs(10), client_stream.write_all(&buf)).await {
                Ok(Ok(())) => {}
                _ => break,
            }
        }

        Ok(())
    }

    async fn connection(
        self: Arc<Self>,
        mut stream: TcpStream,
        client_address: Public,
        ip: String,
        mut session: MinerSession,
    ) {
        tracing::info!(
            "Miner connected: {} from {}",
            hex_short(&client_address.dump_buf()),
            ip
        );

        self.event_sender.send(PoolEvent::MinerConnected {
            miner: hex_short(&client_address.dump_buf()),
            ip: ip.clone(),
            timestamp: now_secs(),
        });

        loop {
            let result = timeout(
                REQUEST_TIMEOUT,
                self.handle_request(&mut stream, &mut session, client_address, &ip),
            )
            .await;

            match result {
                Ok(Ok(Some(response_buf))) => {
                    if let Err(e) = stream.write_all(&response_buf).await {
                        tracing::debug!("Write error to {}: {}", ip, e);
                        break;
                    }
                }
                Ok(Ok(None)) => break,
                Ok(Err(e)) => {
                    tracing::debug!("Request error from {}: {}", ip, e);
                    self.add_penalty(&ip, PENALTY_ERROR).await;
                    break;
                }
                Err(_) => {
                    if let Err(e) = self
                        .maybe_adjust_vardiff(&mut stream, &client_address, &mut session)
                        .await
                    {
                        tracing::debug!("VarDiff adjust failed for {}: {}", ip, e);
                    }
                    continue;
                }
            }
        }

        tracing::debug!("Miner disconnected: {}", ip);

        self.event_sender.send(PoolEvent::MinerDisconnected {
            miner: hex_short(&client_address.dump_buf()),
            ip,
            timestamp: now_secs(),
        });
    }

    pub async fn listen(self: Arc<Self>) -> anyhow::Result<JoinHandle<()>> {
        let bind_addr = format!("0.0.0.0:{}", self.port);
        let listener = TcpListener::bind(&bind_addr).await.map_err(|e| {
            anyhow!(
                "Failed to bind to {} — is another process using this port? Error: {}",
                bind_addr,
                e
            )
        })?;

        tracing::info!("Pool server listening on {}", bind_addr);

        let ip_states_clone = self.ip_states.clone();
        tokio::spawn(async move {
            Self::score_decay_task(ip_states_clone).await;
        });

        let server = self.clone();

        Ok(tokio::spawn(async move {
            loop {
                let (mut stream, addr) = match listener.accept().await {
                    Ok(conn) => conn,
                    Err(e) => {
                        tracing::error!("Accept error: {}", e);
                        continue;
                    }
                };

                let ip = addr.ip().to_string();
                let server = server.clone();

                tokio::spawn(async move {
                    if server.is_banned(&ip).await {
                        let _ = stream.shutdown().await;
                        return;
                    }

                    if !server.try_acquire_connection(&ip).await {
                        tracing::debug!(
                            "Connection limit reached for {} (max {})",
                            ip,
                            server.max_conn_per_ip
                        );
                        let _ = stream.shutdown().await;
                        return;
                    }

                    let session = MinerSession::new(server.pool_difficulty);

                    let client_address = match timeout(
                        HANDSHAKE_TIMEOUT,
                        server.handshake(&mut stream, &session),
                    )
                    .await
                    {
                        Ok(Ok(pubkey)) => pubkey,
                        Ok(Err(e)) => {
                            tracing::debug!("Handshake error from {}: {}", ip, e);
                            server.add_penalty(&ip, PENALTY_ERROR).await;
                            server.release_connection(&ip).await;
                            return;
                        }
                        Err(_) => {
                            tracing::debug!("Handshake timeout from {}", ip);
                            server.add_penalty(&ip, PENALTY_ERROR).await;
                            server.release_connection(&ip).await;
                            return;
                        }
                    };

                    server
                        .clone()
                        .connection(stream, client_address, ip.clone(), session)
                        .await;

                    server.release_connection(&ip).await;
                });
            }
        }))
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn hex_short(buf: &[u8; 32]) -> String {
    buf[..4].iter().map(|b| format!("{:02x}", b)).collect()
}

fn hex_encode(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn compute_work_units_q64_64(base_target: &[u8; 32], miner_target: &[u8; 32]) -> u128 {
    let base = BigUint::from_bytes_be(base_target);
    let miner = BigUint::from_bytes_be(miner_target);

    if biguint_is_zero(&miner) {
        return 1;
    }

    let num = base << WORK_FP_SHIFT;
    let q = num / miner;

    match biguint_to_u128(&q) {
        Some(v) => v.max(1),
        None => u128::MAX,
    }
}

fn ease_target(cur: &[u8; 32], mult: u32) -> [u8; 32] {
    let cur_bi = BigUint::from_bytes_be(cur);
    if biguint_is_zero(&cur_bi) {
        return *cur;
    }

    let eased = cur_bi * BigUint::from(mult);
    let max = BigUint::from_bytes_be(&MAX_TARGET);

    let out = if eased > max { max } else { eased };

    let mut buf = [0u8; 32];
    let bytes = out.to_bytes_be();
    if bytes.len() >= 32 {
        buf.copy_from_slice(&bytes[bytes.len() - 32..]);
    } else {
        buf[32 - bytes.len()..].copy_from_slice(&bytes);
    }
    buf
}

fn target_leading_zero_bits(target: &[u8; 32]) -> u32 {
    let mut count = 0;
    for &byte in target.iter() {
        if byte == 0 {
            count += 8;
        } else {
            count += byte.leading_zeros();
            break;
        }
    }
    count
}

fn target_difficulty_ratio(base_target: &[u8; 32], miner_target: &[u8; 32]) -> f64 {
    let base = BigUint::from_bytes_be(base_target);
    let miner = BigUint::from_bytes_be(miner_target);

    if biguint_is_zero(&base) || biguint_is_zero(&miner) {
        return 1.0;
    }

    let ratio = (&miner * BigUint::from(1000000u32)) / &base;

    if let Some(r) = biguint_to_u128(&ratio) {
        r as f64 / 1000000.0
    } else {
        999999.0
    }
}

fn biguint_is_zero(v: &BigUint) -> bool {
    v == &BigUint::from(0u8)
}

fn biguint_to_u128(v: &BigUint) -> Option<u128> {
    let bytes = v.to_bytes_be();
    if bytes.len() > 16 {
        return None;
    }
    let mut buf = [0u8; 16];
    buf[16 - bytes.len()..].copy_from_slice(&bytes);
    Some(u128::from_be_bytes(buf))
}

// ============================================================================
// File: pool_api_server.rs
// Location: snap-coin-pool/src/pool_api_server.rs
// Version: 1.8.4
// Created: 2026-02-08T15:00:00Z
// Updated: 2026-02-09T01:45:00Z
// ============================================================================