// =============================================================================
// File: src/job_handler.rs
// Project: snap-coin-pool
// Version: 1.1.0
// Description: Job handler with reconnect/backoff logic on TCP connection loss.
//              When build_job fails, the dead job_client is dropped and
//              Client::connect() is retried with exponential backoff.
// =============================================================================

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use snap_coin::{
    api::{api_server::ApiError, client::Client},
    blockchain_data_provider::BlockchainDataProviderError,
    build_block,
    core::{
        block::{Block, MAX_TRANSACTIONS_PER_BLOCK},
        transaction::Transaction,
        utils::slice_vec,
    },
    crypto::keys::Private,
    economics::EXPIRATION_TIME,
};
use tokio::sync::{broadcast, Mutex};

async fn get_current_mempool(
    client: &Client,
) -> Result<Vec<Transaction>, BlockchainDataProviderError> {
    let mut mempool = slice_vec(
        &client.get_mempool().await?,
        0,
        MAX_TRANSACTIONS_PER_BLOCK - 1,
    )
    .to_vec();
    mempool.retain(|tx| tx.timestamp + 5 < EXPIRATION_TIME + chrono::Utc::now().timestamp() as u64); // Add a 5s expiration buffer
    Ok(mempool)
}

pub struct JobHandler {
    tx_subscriber: broadcast::Sender<Block>,
}

impl JobHandler {
    pub async fn listen(
        node_api: SocketAddr,
        pool_private: Private,
    ) -> Result<(Self, Block), ApiError> {
        let event_client = Client::connect(node_api).await?;
        let job_client = Arc::new(Mutex::new(Client::connect(node_api).await?));

        let (job_tx, _job_rx) = broadcast::channel::<Block>(24);

        let tx_subscriber = job_tx.clone();
        let job_tx = Arc::new(job_tx);
        let is_building = Arc::new(AtomicBool::new(false));

        let first_job = build_job(&job_client, &is_building, pool_private, &job_tx, node_api)
            .await
            .expect("Could not get first job!"); // Build first job before events

        tokio::spawn(async move {
            event_client
                .convert_to_event_listener(
                    move |_event| {
                        let is_building = is_building.clone();
                        let job_client = job_client.clone();
                        let job_tx = job_tx.clone();
                        tokio::spawn(async move {
                            build_job(&job_client, &is_building, pool_private, &job_tx, node_api).await;
                            is_building.store(false, Ordering::Relaxed);
                        });
                    },
                    None, // snap-coin v13.2.0: optional shutdown receiver
                )
                .await
                .unwrap();
        });

        Ok((JobHandler { tx_subscriber }, first_job))
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Block> {
        self.tx_subscriber.subscribe()
    }
}

async fn build_job(
    job_client: &Arc<Mutex<Client>>,
    is_building: &Arc<AtomicBool>,
    pool_private: Private,
    job_tx: &broadcast::Sender<Block>,
    node_api: SocketAddr,
) -> Option<Block> {
    if is_building.load(Ordering::Relaxed) {
        return None;
    }
    is_building.store(true, Ordering::Relaxed);

    let result = {
        let client = job_client.lock().await;
        async move {
            let block = build_block(
                &*client,
                &get_current_mempool(&*client).await?,
                pool_private.to_public(),
            )
            .await?;

            let _ = job_tx.send(block.clone());

            Ok::<Block, anyhow::Error>(block)
        }
        .await
    };

    match result {
        Ok(block) => Some(block),
        Err(e) => {
            println!("[JOB] Job Handler failed: {} — reconnecting...", e);

            // Reconnect with exponential backoff
            let mut backoff = Duration::from_secs(2);
            loop {
                tokio::time::sleep(backoff).await;
                match Client::connect(node_api).await {
                    Ok(new_client) => {
                        *job_client.lock().await = new_client;
                        println!("[JOB] Reconnected to node.");
                        break;
                    }
                    Err(e) => {
                        println!("[JOB] Reconnect failed: {} — retrying in {}s", e, backoff.as_secs());
                        backoff = (backoff * 2).min(Duration::from_secs(60));
                    }
                }
            }

            None
        }
    }
}

// =============================================================================
// File: src/job_handler.rs
// Project: snap-coin-pool
// Created: 2026-04-07T00:00:00Z
// =============================================================================