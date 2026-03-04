//! Synthetic extractor trait for deterministic event generation
//!
//! This trait defines the interface for synthetic event generators used in
//! profiling and testing the ingestion pipeline without external dependencies.

use crate::etl::extractor::ExtractionBatch;
use anyhow::Result;
use async_trait::async_trait;

/// Trait for synthetic event generation
///
/// Each token type (ERC20, ERC721, ERC1155) implements this to generate
/// deterministic synthetic events for profiling the ingestion pipeline.
///
/// # Design Principles
///
/// - **Deterministic**: Same seed always produces identical events
/// - **Self-contained**: No RPC calls, no external data
/// - **Realistic**: Mimics real event structure and distribution
/// - **Configurable**: Event ratios, token counts, batch sizes
///
/// # Example
///
/// ```rust,ignore
/// use torii_erc20::{SyntheticErc20Config, SyntheticErc20Extractor};
/// use torii::etl::extractor::SyntheticExtractor;
///
/// let config = SyntheticErc20Config {
///     from_block: 1_000_000,
///     block_count: 200,
///     tx_per_block: 1_000,
///     seed: 42,
///     ..Default::default()
/// };
///
/// let mut extractor = SyntheticErc20Extractor::new(config)?;
///
/// while !extractor.is_finished() {
///     let batch = extractor.extract(None).await?;
///     // Process batch...
/// }
/// ```
#[async_trait]
pub trait SyntheticExtractor: Send + Sync {
    /// Configuration type for this extractor
    type Config: Clone + Send + Sync;

    /// Create a new synthetic extractor with the given config
    fn new(config: Self::Config) -> Result<Self>
    where
        Self: Sized;

    /// Extract a batch of synthetic events
    ///
    /// The cursor parameter allows resuming from a previous extraction point.
    /// - None: Start from the beginning
    /// - Some(cursor): Resume from the given cursor
    ///
    /// Returns an ExtractionBatch with synthetic events and context.
    async fn extract(&mut self, cursor: Option<String>) -> Result<ExtractionBatch>;

    /// Check if extraction is complete
    ///
    /// Returns true when the extractor has generated all configured blocks
    /// and will not produce more data.
    fn is_finished(&self) -> bool;

    /// Get the extractor name for logging/cursor tracking
    ///
    /// Should be unique across different token types (e.g., "synthetic_erc20")
    fn extractor_name(&self) -> &'static str;
}
