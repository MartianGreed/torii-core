//! Configuration for the unified token indexer

use anyhow::Result;
use clap::{Parser, ValueEnum};
use starknet::core::types::Felt;

/// Extraction mode for the token indexer.
///
/// - **BlockRange**: Fetches ALL events from each block using a single global cursor.
///   Best for full chain indexing and auto-discovery of contracts.
///
/// - **Event**: Uses `starknet_getEvents` with per-contract cursors.
///   Best for indexing specific contracts - easy to add new ones without re-indexing.
///
#[derive(Clone, Debug, Default, ValueEnum, PartialEq, Eq)]
pub enum ExtractionMode {
    /// Fetch all events from blocks (single global cursor)
    #[default]
    BlockRange,
    /// Fetch events per contract (per-contract cursors)
    Event,
}

/// Metadata fetching behavior.
#[derive(Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum MetadataMode {
    /// Fetch metadata during indexing.
    Inline,
    /// Skip metadata fetching during indexing (faster backfills).
    Deferred,
}

/// Unified Token Indexer for Starknet
///
/// Indexes ERC20, ERC721, and ERC1155 token transfers and events.
///
/// # Extraction Modes
///
/// - **block-range** (default): Fetches ALL events from each block.
///   Single global cursor. Best for full chain indexing.
///
/// - **event**: Uses `starknet_getEvents` with per-contract cursors.
///   Easy to add new contracts without re-indexing existing ones.
///
/// # Examples
///
/// ```bash
/// # Block range mode (default) - index ETH and STRK
/// torii-tokens --include-well-known --from-block 0
///
/// # Event mode - per-contract cursors
/// torii-tokens --mode event --erc20 0x...ETH,0x...STRK --from-block 0
///
/// # Add a new contract in event mode (just restart with updated list)
/// torii-tokens --mode event --erc20 0x...ETH,0x...STRK,0x...USDC --from-block 0
/// # USDC starts from block 0, ETH and STRK resume from their cursors
///
/// ```
#[derive(Parser, Debug)]
#[command(name = "torii-tokens")]
#[command(about = "Index ERC20, ERC721, and ERC1155 tokens on Starknet", long_about = None)]
pub struct Config {
    /// Extraction mode
    ///
    /// - block-range: Fetch all events from blocks (single global cursor)
    /// - event: Fetch events per contract (per-contract cursors, easy to add new contracts)
    #[arg(long, value_enum, default_value = "block-range")]
    pub mode: ExtractionMode,

    /// Starknet RPC URL
    #[arg(
        long,
        env = "STARKNET_RPC_URL",
        default_value = "https://api.cartridge.gg/x/starknet/mainnet"
    )]
    pub rpc_url: String,

    /// Starting block number
    #[arg(long, default_value = "0")]
    pub from_block: u64,

    /// Ending block number (None = follow chain head)
    #[arg(long)]
    pub to_block: Option<u64>,

    /// Directory where all databases will be stored
    ///
    /// Creates: engine.db, erc20.db, erc721.db, erc1155.db
    #[arg(long, default_value = "./torii-data")]
    pub db_dir: String,

    /// Optional engine database URL/path.
    ///
    /// Supports PostgreSQL (`postgres://...`) and SQLite (`sqlite:...` or file path).
    /// When omitted, engine state uses `<db-dir>/engine.db`.
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: Option<String>,

    /// Optional token storage database URL/path.
    ///
    /// Supports PostgreSQL (`postgres://...`) and SQLite (`sqlite:...` or file path).
    /// When omitted, token storages use `--database-url` if set, otherwise `<db-dir>/*.db`.
    #[arg(long, env = "STORAGE_DATABASE_URL")]
    pub storage_database_url: Option<String>,

    /// Port for the HTTP/gRPC API
    #[arg(long, default_value = "3000")]
    pub port: u16,

    /// ERC20 contracts to index (comma-separated hex addresses)
    ///
    /// Example: --erc20 0x049D36570D4e46f48e99674bd3fcc84644DdD6b96F7C741B1562B82f9e004dC7,0x04718f5a0Fc34cC1AF16A1cdee98fFB20C31f5cD61D6Ab07201858f4287c938D
    #[arg(long, value_delimiter = ',')]
    pub erc20: Vec<String>,

    /// ERC721 contracts to index (comma-separated hex addresses)
    ///
    /// Example: --erc721 0x...nft_contract
    #[arg(long, value_delimiter = ',')]
    pub erc721: Vec<String>,

    /// ERC1155 contracts to index (comma-separated hex addresses)
    ///
    /// Example: --erc1155 0x...game_items
    #[arg(long, value_delimiter = ',')]
    pub erc1155: Vec<String>,

    /// Include well-known ERC20 contracts (ETH, STRK)
    #[arg(long)]
    pub include_well_known: bool,

    /// Number of blocks to fetch per batch (block-range mode)
    ///
    /// Higher values improve throughput but use more memory.
    /// Lower values reduce memory usage but may slow down indexing.
    #[arg(long, default_value = "50")]
    pub batch_size: u64,

    /// Events per RPC request (event mode, max 1024 for most providers)
    #[arg(long, default_value = "1000")]
    pub event_chunk_size: u64,

    /// Block range to query per iteration in event mode
    #[arg(long, default_value = "10000")]
    pub event_block_batch_size: u64,

    /// Metadata fetching mode.
    ///
    /// If omitted: defaults to `deferred` in block-range mode and `inline` in event mode.
    #[arg(long, value_enum)]
    pub metadata_mode: Option<MetadataMode>,

    /// Run metadata-only flow and exit.
    ///
    /// Note: currently implemented as a guarded no-op placeholder.
    #[arg(long)]
    pub metadata_backfill_only: bool,
}

impl Config {
    /// Parse a hex address string to Felt
    pub fn parse_address(addr: &str) -> Result<Felt> {
        Felt::from_hex(addr).map_err(|e| anyhow::anyhow!("Invalid address {addr}: {e}"))
    }

    /// Get well-known ERC20 contracts (ETH, STRK)
    pub fn well_known_erc20_contracts() -> Vec<(Felt, &'static str)> {
        vec![
            (
                Felt::from_hex_unchecked(
                    "0x049D36570D4e46f48e99674bd3fcc84644DdD6b96F7C741B1562B82f9e004dC7",
                ),
                "ETH",
            ),
            (
                Felt::from_hex_unchecked(
                    "0x04718f5a0Fc34cC1AF16A1cdee98fFB20C31f5cD61D6Ab07201858f4287c938D",
                ),
                "STRK",
            ),
        ]
    }

    /// Check if any token types are configured
    pub fn has_tokens(&self) -> bool {
        !self.erc20.is_empty()
            || !self.erc721.is_empty()
            || !self.erc1155.is_empty()
            || self.include_well_known
    }
}
