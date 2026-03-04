//! Extractor trait for fetching events from various sources

pub mod block_range;
pub mod composite;
pub mod event;
pub mod retry;
pub mod sample;
pub mod starknet_helpers;
pub mod synthetic;
pub mod synthetic_erc20;

use crate::etl::engine_db::EngineDb;
use anyhow::Result;
use async_trait::async_trait;
use starknet::core::types::{EmittedEvent, Felt};
use std::collections::HashMap;

pub use block_range::{BlockRangeConfig, BlockRangeExtractor};
pub use composite::CompositeExtractor;
pub use event::{ContractEventConfig, EventExtractor, EventExtractorConfig};
pub use retry::RetryPolicy;
pub use sample::SampleExtractor;
pub use starknet_helpers::ContractAbi;
pub use synthetic::SyntheticExtractor;
pub use synthetic_erc20::{SyntheticErc20Config, SyntheticErc20Extractor};

/// Block context information
#[derive(Debug, Clone)]
pub struct BlockContext {
    pub number: u64,
    pub hash: Felt,
    pub parent_hash: Felt,
    pub timestamp: u64,
}

/// Transaction context information
#[derive(Debug, Clone)]
pub struct TransactionContext {
    pub hash: Felt,
    pub block_number: u64,
    pub sender_address: Option<Felt>,
    pub calldata: Vec<Felt>,
}

/// Declared class information
#[derive(Debug, Clone)]
pub struct DeclaredClass {
    pub class_hash: Felt,
    pub compiled_class_hash: Option<Felt>, // Only for Cairo 1.0+ (V2+)
    pub transaction_hash: Felt,
}

/// Deployed contract information
#[derive(Debug, Clone)]
pub struct DeployedContract {
    pub contract_address: Felt,
    pub class_hash: Felt,
    pub transaction_hash: Felt,
}

/// Complete block data with all extracted information
#[derive(Debug, Clone)]
pub struct BlockData {
    pub block_context: BlockContext,
    pub transactions: Vec<TransactionContext>,
    pub events: Vec<EmittedEvent>,
    pub declared_classes: Vec<DeclaredClass>,
    pub deployed_contracts: Vec<DeployedContract>,
}

/// Enriched extraction batch with events and context
///
/// This structure optimizes memory by **deduplicating** blocks and transactions:
/// - Multiple events from the same block share a single `BlockContext`.
/// - Multiple events from the same transaction share a single `TransactionContext`.
///
/// # Memory Optimization Example
///
/// For 100 events from 1 block and 10 transactions:
/// - `events`: 100 items (raw events)
/// - `blocks`: 1 item (deduplicated)
/// - `transactions`: 10 items (deduplicated)
///
/// Instead of storing block/transaction data 100 times, we store it 11 times total.
///
/// # Usage in Sinks
///
/// Sinks receive this batch along with decoded envelopes. To access context:
/// ```rust,ignore
/// // Fast O(1) lookups via HashMap
/// let block = &batch.blocks[&block_number];
/// let tx = &batch.transactions[&tx_hash];
///
/// // Slow O(n) - avoid iterating events
/// for event in &batch.events { /* ... */ }
/// ```
#[derive(Debug, Clone)]
pub struct ExtractionBatch {
    /// Events extracted (may contain duplicates from same block/tx)
    pub events: Vec<EmittedEvent>,

    /// Block context (deduplicated by block_number for memory efficiency)
    pub blocks: HashMap<u64, BlockContext>,

    /// Transaction context (deduplicated by tx_hash for memory efficiency)
    pub transactions: HashMap<Felt, TransactionContext>,

    /// Declared classes from Declare transactions
    pub declared_classes: Vec<DeclaredClass>,

    /// Deployed contracts from Deploy and DeployAccount transactions
    pub deployed_contracts: Vec<DeployedContract>,

    /// Opaque cursor for pagination (continuation token or cursor string)
    pub cursor: Option<String>,

    /// Current chain head block number (if known by extractor).
    /// Used by sinks to determine if events should be broadcast to real-time subscribers.
    pub chain_head: Option<u64>,
}

impl ExtractionBatch {
    /// Create an empty batch
    pub fn empty() -> Self {
        Self {
            events: Vec::new(),
            blocks: HashMap::new(),
            transactions: HashMap::new(),
            declared_classes: Vec::new(),
            deployed_contracts: Vec::new(),
            cursor: None,
            chain_head: None,
        }
    }

    /// Check if batch is empty
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Get number of events
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Get the maximum block number in this batch.
    pub fn max_block(&self) -> Option<u64> {
        self.blocks.keys().max().copied()
    }

    /// Check if this batch is "live" (near chain head).
    ///
    /// Returns true if:
    /// - chain_head is known AND
    /// - the max block in this batch is within `threshold` blocks of chain_head
    ///
    /// This is useful for sinks to decide whether to broadcast events to real-time
    /// subscribers. During historical indexing, broadcasting millions of events
    /// would overwhelm clients and slow down the indexer.
    ///
    /// # Example
    /// ```ignore
    /// // Only broadcast if within 100 blocks of chain head
    /// if batch.is_live(100) {
    ///     grpc_service.broadcast_transfer(proto_transfer);
    /// }
    /// ```
    pub fn is_live(&self, threshold: u64) -> bool {
        match (self.chain_head, self.max_block()) {
            (Some(head), Some(max_block)) => head.saturating_sub(max_block) <= threshold,
            // If we don't know chain head, assume not live (safer for historical indexing)
            _ => false,
        }
    }
}

/// Extractor trait for fetching enriched event batches
#[async_trait]
pub trait Extractor: Send + Sync {
    /// Extract events with enriched context (blocks, transactions)
    ///
    /// The cursor parameter is an opaque string that allows resuming from a previous extraction.
    /// - None: Start from the beginning or use extractor's internal state
    /// - Some(cursor): Resume from the given cursor
    ///
    /// Returns an ExtractionBatch with:
    /// - events: The extracted events
    /// - blocks/transactions: Deduplicated context
    /// - cursor: Opaque cursor for next extraction
    ///
    /// # Return Value Semantics
    ///
    /// - Non-empty batch: Process events, call `extract()` again
    /// - Empty batch + `is_finished() = false`: Waiting for new blocks, sleep and retry
    /// - Empty batch + `is_finished() = true`: Extractor reached its end, stop calling
    async fn extract(
        &mut self,
        cursor: Option<String>,
        engine_db: &EngineDb,
    ) -> Result<ExtractionBatch>;

    /// Check if the extractor has finished its configured range
    ///
    /// Returns `true` when the extractor has reached its configured end point
    /// and will not produce more data, even if called again.
    ///
    /// Returns `false` when the extractor can potentially produce more data:
    /// - Still has blocks to fetch in the configured range
    /// - Following chain head indefinitely (no end block configured)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// loop {
    ///     let batch = extractor.extract(cursor, engine_db).await?;
    ///
    ///     if !batch.is_empty() {
    ///         process_batch(batch);
    ///     }
    ///
    ///     if extractor.is_finished() {
    ///         break; // Done, reached configured end
    ///     }
    ///
    ///     if batch.is_empty() {
    ///         // Waiting for new blocks, sleep and retry
    ///         tokio::time::sleep(Duration::from_secs(5)).await;
    ///     }
    ///
    ///     cursor = batch.cursor;
    /// }
    /// ```
    fn is_finished(&self) -> bool;

    /// Commit the cursor after successful processing.
    ///
    /// This method is called AFTER sink processing completes successfully.
    /// By separating cursor persistence from extraction, we ensure no data loss
    /// if the process is killed between extraction and sink processing.
    ///
    /// # Arguments
    ///
    /// * `cursor` - The cursor string to commit (e.g., "block:12345")
    /// * `engine_db` - The engine database for state persistence
    ///
    /// # Default Implementation
    ///
    /// The default implementation does nothing (no-op). Extractors that need
    /// cursor persistence should override this method.
    async fn commit_cursor(&mut self, _cursor: &str, _engine_db: &EngineDb) -> Result<()> {
        Ok(())
    }

    /// Downcast to Any for type checking
    fn as_any(&self) -> &dyn std::any::Any;
}
