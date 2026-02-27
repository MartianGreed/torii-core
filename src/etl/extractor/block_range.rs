//! Block range extractor for fetching events from Starknet full nodes
//!
//! Fetches blocks in batches and extracts all events from transaction receipts.
//! Supports automatic cursor persistence and retry logic for network failures.

use anyhow::{Context, Result};
use async_trait::async_trait;
use starknet::core::types::MaybePreConfirmedBlockWithReceipts;
use starknet::providers::jsonrpc::{HttpTransport, JsonRpcClient};
use starknet::providers::{Provider, ProviderResponseData};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::etl::engine_db::EngineDb;
use crate::etl::extractor::starknet_helpers::{
    block_into_contexts, block_with_receipts_batch_from_block_range,
};

use super::{ExtractionBatch, Extractor, RetryPolicy};

const EXTRACTOR_TYPE: &str = "block_range";
const STATE_KEY: &str = "last_block";

/// Block range extractor configuration
#[derive(Debug, Clone)]
pub struct BlockRangeConfig {
    /// RPC endpoint URL
    pub rpc_url: String,

    /// Starting block number
    pub from_block: u64,

    /// Ending block number (None = follow chain head indefinitely)
    pub to_block: Option<u64>,

    /// Number of blocks to fetch per batch (max efficiency: ~100-1000 depending on RPC)
    pub batch_size: u64,

    /// Retry policy for network failures
    pub retry_policy: RetryPolicy,
}

impl Default for BlockRangeConfig {
    fn default() -> Self {
        Self {
            rpc_url: "http://localhost:5050".to_string(),
            from_block: 0,
            to_block: None,
            batch_size: 100,
            retry_policy: RetryPolicy::default(),
        }
    }
}

#[derive(Debug)]
struct PreparedBatch {
    next_block: u64,
    batch: ExtractionBatch,
}

/// Block range extractor.
///
/// Fetches blocks sequentially in batches, extracts events from transaction receipts,
/// and builds enriched extraction batches with deduplicated block/transaction context.
///
/// # Cursor Management
///
/// The cursor is stored as "block:N" where N is the last successfully processed block.
/// On restart, extraction resumes from N+1.
///
/// # Chain Head Polling
///
/// When `to_block` is None and the extractor reaches the chain head:
/// - Returns empty batch (with `is_finished() = false`)
/// - Caller should wait and retry (the mainloop handles this)
/// - On next call, checks for new blocks
/// Block range extractor using JsonRpcClient with HTTP transport.
#[derive(Debug)]
pub struct BlockRangeExtractor {
    /// Provider to fetch data from.
    provider: Arc<JsonRpcClient<HttpTransport>>,

    /// Configuration.
    config: BlockRangeConfig,

    /// Current block number.
    current_block: u64,

    /// Whether we've reached the configured end block.
    reached_end: bool,
}

impl BlockRangeExtractor {
    /// Creates a new block range extractor with the given provider.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use starknet::providers::jsonrpc::{JsonRpcClient, HttpTransport};
    ///
    /// let provider = JsonRpcClient::new(HttpTransport::new(url));
    /// let extractor = BlockRangeExtractor::new(Arc::new(provider), config);
    /// ```
    pub fn new(provider: Arc<JsonRpcClient<HttpTransport>>, config: BlockRangeConfig) -> Self {
        Self {
            provider,
            config,
            current_block: 0,
            reached_end: false,
        }
    }

    /// Initializes the extractor state from cursor or config.
    async fn initialize(&mut self, cursor: Option<String>, engine_db: &EngineDb) -> Result<()> {
        // Priority: cursor > saved state > config.from_block
        if let Some(cursor_str) = cursor {
            // Parse cursor: "block:N"
            if let Some(block_str) = cursor_str.strip_prefix("block:") {
                self.current_block = block_str
                    .parse::<u64>()
                    .context("Invalid cursor format")?
                    .saturating_add(1); // Resume from next block
                tracing::info!(
                    target: "torii::etl::block_range",
                    "Resuming from cursor: block {}",
                    self.current_block
                );
            } else {
                anyhow::bail!("Invalid cursor format: expected 'block:N', got '{cursor_str}'");
            }
        } else {
            // Try loading from EngineDb
            if let Some(saved_state) = engine_db
                .get_extractor_state(EXTRACTOR_TYPE, STATE_KEY)
                .await?
            {
                self.current_block = saved_state
                    .parse::<u64>()
                    .context("Invalid saved state")?
                    .saturating_add(1); // Resume from next block
                tracing::info!(
                    target: "torii::etl::block_range",
                    "Resuming from saved state: block {}",
                    self.current_block
                );
            } else {
                // Start from config
                self.current_block = self.config.from_block;
                tracing::info!(
                    target: "torii::etl::block_range",
                    "Starting from configured block: {}",
                    self.current_block
                );
            }
        }

        Ok(())
    }

    /// Fetches a batch of blocks with receipts using JSON-RPC batch requests.
    ///
    /// Every block in the range **must** be a mined block on Starknet. Otherwise, the request will fail.
    ///
    /// # Arguments
    ///
    /// * `from_block` - The starting block number
    /// * `to_block` - The ending block number
    ///
    /// # Returns
    ///
    /// A vector of blocks with receipts.
    async fn fetch_blocks_batch_with(
        provider: Arc<JsonRpcClient<HttpTransport>>,
        retry_policy: RetryPolicy,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<MaybePreConfirmedBlockWithReceipts>> {
        let requests = block_with_receipts_batch_from_block_range(from_block, to_block);

        let responses = retry_policy
            .execute(|| {
                let provider = provider.clone();
                let requests_ref = &requests;
                async move {
                    provider
                        .batch_requests(requests_ref)
                        .await
                        .context("Failed to execute batch request for blocks")
                }
            })
            .await?;

        let mut blocks = Vec::with_capacity((to_block - from_block + 1) as usize);
        for (idx, response) in responses.into_iter().enumerate() {
            let block_num = from_block + idx as u64;
            match response {
                ProviderResponseData::GetBlockWithReceipts(block) => {
                    blocks.push(block);
                }
                _ => {
                    anyhow::bail!(
                        "Unexpected response type for block {block_num}: expected GetBlockWithReceipts"
                    );
                }
            }
        }

        Ok(blocks)
    }

    /// Check if we've reached the end of the configured range
    fn should_stop(&self) -> bool {
        if let Some(to_block) = self.config.to_block {
            self.current_block > to_block
        } else {
            false // Never stop if to_block is None
        }
    }

    async fn prepare_batch_for(
        provider: Arc<JsonRpcClient<HttpTransport>>,
        config: BlockRangeConfig,
        current_block: u64,
    ) -> Result<PreparedBatch> {
        let total_start = Instant::now();
        let chain_head = provider.block_number().await?;

        let batch_end = if let Some(to_block) = config.to_block {
            (current_block + config.batch_size - 1).min(to_block)
        } else if current_block > chain_head {
            let batch = ExtractionBatch {
                events: Vec::new(),
                blocks: HashMap::new(),
                transactions: HashMap::new(),
                declared_classes: Vec::new(),
                deployed_contracts: Vec::new(),
                cursor: Some(format!("block:{}", current_block.saturating_sub(1))),
                chain_head: Some(chain_head),
            };
            return Ok(PreparedBatch {
                next_block: current_block,
                batch,
            });
        } else {
            (current_block + config.batch_size - 1).min(chain_head)
        };

        tracing::info!(
            target: "torii::etl::block_range",
            "Fetching blocks {}-{} (batch size: {})",
            current_block,
            batch_end,
            batch_end - current_block + 1
        );

        let fetch_start = Instant::now();
        let blocks = Self::fetch_blocks_batch_with(
            provider.clone(),
            config.retry_policy.clone(),
            current_block,
            batch_end,
        )
        .await?;
        let fetch_ms = fetch_start.elapsed().as_millis();

        let transform_start = Instant::now();
        let mut all_events = Vec::new();
        let mut blocks_map = HashMap::with_capacity(blocks.len());
        let mut transactions_map = HashMap::new();
        let mut all_declared_classes = Vec::new();
        let mut all_deployed_contracts = Vec::new();

        for block in blocks {
            let block_data = block_into_contexts(block)?;

            all_events.reserve(block_data.events.len());
            transactions_map.reserve(block_data.transactions.len());
            all_declared_classes.reserve(block_data.declared_classes.len());
            all_deployed_contracts.reserve(block_data.deployed_contracts.len());

            blocks_map.insert(block_data.block_context.number, block_data.block_context);

            for tx_ctx in block_data.transactions {
                transactions_map.insert(tx_ctx.hash, tx_ctx);
            }

            all_events.extend(block_data.events);
            all_declared_classes.extend(block_data.declared_classes);
            all_deployed_contracts.extend(block_data.deployed_contracts);
        }
        let transform_ms = transform_start.elapsed().as_millis();
        let total_ms = total_start.elapsed().as_millis();

        tracing::info!(
            target: "torii::etl::block_range",
            "Extracted {} events, {} declared classes, {} deployed contracts from {} blocks ({} transactions) [fetch={}ms transform={}ms total={}ms]",
            all_events.len(),
            all_declared_classes.len(),
            all_deployed_contracts.len(),
            blocks_map.len(),
            transactions_map.len(),
            fetch_ms,
            transform_ms,
            total_ms
        );

        let batch = ExtractionBatch {
            events: all_events,
            blocks: blocks_map,
            transactions: transactions_map,
            declared_classes: all_declared_classes,
            deployed_contracts: all_deployed_contracts,
            cursor: Some(format!("block:{batch_end}")),
            chain_head: Some(chain_head),
        };

        Ok(PreparedBatch {
            next_block: batch_end + 1,
            batch,
        })
    }
}

#[async_trait]
impl Extractor for BlockRangeExtractor {
    fn is_finished(&self) -> bool {
        self.reached_end
    }

    async fn commit_cursor(&mut self, cursor: &str, engine_db: &EngineDb) -> Result<()> {
        if let Some(block_str) = cursor.strip_prefix("block:") {
            let block_num: u64 = block_str.parse().context("Invalid cursor format")?;
            engine_db
                .set_extractor_state(EXTRACTOR_TYPE, STATE_KEY, &block_num.to_string())
                .await
                .context("Failed to commit cursor")?;
            tracing::debug!(
                target: "torii::etl::block_range",
                "Committed cursor: block {}",
                block_num
            );
        }
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn extract(
        &mut self,
        cursor: Option<String>,
        engine_db: &EngineDb,
    ) -> Result<ExtractionBatch> {
        // Initialize only on first call - after that, extractor maintains its own state
        // The cursor parameter is for initial resume from checkpoint, not every iteration
        if self.current_block == 0 {
            self.initialize(cursor, engine_db).await?;
        }

        // Check if we've reached the configured end
        if self.reached_end {
            return Ok(ExtractionBatch::empty());
        }

        if self.should_stop() {
            tracing::info!(
                target: "torii::etl::block_range",
                "Reached configured end block"
            );
            self.reached_end = true;
            return Ok(ExtractionBatch::empty());
        }

        let prepared =
            Self::prepare_batch_for(self.provider.clone(), self.config.clone(), self.current_block)
                .await?;
        self.current_block = prepared.next_block;

        tracing::debug!(
            target: "torii::etl::block_range",
            next_block = self.current_block,
            "Using block-range batch"
        );

        Ok(prepared.batch)
    }
}
