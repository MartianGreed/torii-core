//! Torii Tokens - Unified Starknet Token Indexer
//!
//! A production-ready indexer for ERC20, ERC721, and ERC1155 tokens on Starknet.
//!
//! # Features
//!
//! - Explicit contract mapping with token type specification
//! - ERC20: Transfer and approval tracking
//! - ERC721: NFT ownership tracking with transfer history
//! - ERC1155: Semi-fungible token transfer tracking (single and batch)
//! - SQLite persistence with separate tables per token type
//! - gRPC subscriptions for real-time updates
//! - Historical queries with pagination
//!
//! # Extraction Modes
//!
//! - **block-range** (default): Fetches ALL events from each block.
//!   Single global cursor. Best for full chain indexing.
//!
//! - **event**: Uses `starknet_getEvents` with per-contract cursors.
//!   Easy to add new contracts without re-indexing existing ones.
//!
//! # Usage
//!
//! ```bash
//! # Block range mode (default) - full chain indexing
//! torii-tokens --include-well-known --from-block 0
//!
//! # Event mode - per-contract cursors
//! torii-tokens --mode event --erc20 0x...ETH,0x...STRK --from-block 0
//!
//! # Add a new contract in event mode (just restart with updated list)
//! torii-tokens --mode event --erc20 0x...ETH,0x...STRK,0x...USDC --from-block 0
//! # USDC starts from block 0, ETH and STRK resume from their cursors
//! ```

mod config;

use anyhow::Result;
use clap::Parser;
use config::{Config, ExtractionMode, MetadataMode};
use starknet::core::types::Felt;
use std::path::Path;
use std::sync::Arc;
#[cfg(feature = "profiling")]
use std::time::{SystemTime, UNIX_EPOCH};
use torii::etl::decoder::DecoderId;
use torii::etl::extractor::{
    BlockRangeConfig, BlockRangeExtractor, ContractEventConfig, EventExtractor,
    EventExtractorConfig, Extractor, RetryPolicy,
};
use torii::etl::identification::ContractRegistry;

// Import from ERC20 library crate
use torii_erc20::proto::erc20_server::Erc20Server;
use torii_erc20::{
    Erc20Decoder, Erc20Rule, Erc20Service, Erc20Sink, Erc20Storage,
    FILE_DESCRIPTOR_SET as ERC20_DESCRIPTOR_SET,
};

// Import from ERC721 library crate
use torii_erc721::proto::erc721_server::Erc721Server;
use torii_erc721::{
    Erc721Decoder, Erc721Rule, Erc721Service, Erc721Sink, Erc721Storage,
    FILE_DESCRIPTOR_SET as ERC721_DESCRIPTOR_SET,
};

use torii_common::{MetadataFetcher, TokenUriService};

// Import from ERC1155 library crate
use torii_erc1155::proto::erc1155_server::Erc1155Server;
use torii_erc1155::{
    Erc1155Decoder, Erc1155Rule, Erc1155Service, Erc1155Sink, Erc1155Storage,
    FILE_DESCRIPTOR_SET as ERC1155_DESCRIPTOR_SET,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DbBackend {
    Postgres,
    Sqlite,
}

fn backend_from_path(path: &str) -> DbBackend {
    if path.starts_with("postgres://") || path.starts_with("postgresql://") {
        DbBackend::Postgres
    } else {
        DbBackend::Sqlite
    }
}

fn resolve_storage_url(config: &Config, db_dir: &Path, fallback_file: &str) -> String {
    config
        .storage_database_url
        .clone()
        .or_else(|| config.database_url.clone())
        .unwrap_or_else(|| db_dir.join(fallback_file).to_string_lossy().to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .init();

    let config = Config::parse();
    run_indexer(config).await
}

/// Run the main indexer
async fn run_indexer(config: Config) -> Result<()> {
    #[cfg(feature = "profiling")]
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .unwrap();

    tracing::info!("Starting Torii Unified Token Indexer");
    tracing::info!("Mode: {:?}", config.mode);
    tracing::info!("RPC URL: {}", config.rpc_url);
    tracing::info!("From block: {}", config.from_block);
    if let Some(to_block) = config.to_block {
        tracing::info!("To block: {}", to_block);
    } else {
        tracing::info!("To block: following chain head");
    }
    tracing::info!("Database directory: {}", config.db_dir);
    if let Some(url) = &config.database_url {
        tracing::info!("Engine database URL: {}", url);
    }
    if let Some(url) = &config.storage_database_url {
        tracing::info!("Storage database URL: {}", url);
    }

    if config.mode == ExtractionMode::Event && !config.has_tokens() {
        tracing::error!(
            "Event mode requires explicit contracts. Use --erc20, --erc721, --erc1155, or --include-well-known"
        );
        tracing::error!(
            "Example: torii-tokens --mode event --include-well-known --from-block 100000"
        );
        return Ok(());
    }

    if config.mode == ExtractionMode::BlockRange {
        if config.has_tokens() {
            tracing::info!("Block-range mode with explicit contracts (will also auto-discover)");
        } else {
            tracing::info!("Block-range mode with full auto-discovery enabled");
        }
    }

    let provider = Arc::new(starknet::providers::jsonrpc::JsonRpcClient::new(
        starknet::providers::jsonrpc::HttpTransport::new(
            url::Url::parse(&config.rpc_url).expect("Invalid RPC URL"),
        ),
    ));

    let mut all_erc20_addresses: Vec<Felt> = Vec::new();
    let mut all_erc721_addresses: Vec<Felt> = Vec::new();
    let mut all_erc1155_addresses: Vec<Felt> = Vec::new();

    if config.include_well_known {
        for (address, name) in Config::well_known_erc20_contracts() {
            tracing::info!("Adding well-known ERC20: {} at {:#x}", name, address);
            all_erc20_addresses.push(address);
        }
    }

    for addr_str in &config.erc20 {
        let address = Config::parse_address(addr_str)?;
        tracing::info!("Adding ERC20 contract: {:#x}", address);
        all_erc20_addresses.push(address);
    }

    for addr_str in &config.erc721 {
        let address = Config::parse_address(addr_str)?;
        tracing::info!("Adding ERC721 contract: {:#x}", address);
        all_erc721_addresses.push(address);
    }

    for addr_str in &config.erc1155 {
        let address = Config::parse_address(addr_str)?;
        tracing::info!("Adding ERC1155 contract: {:#x}", address);
        all_erc1155_addresses.push(address);
    }

    let extractor: Box<dyn Extractor> = match config.mode {
        ExtractionMode::BlockRange => {
            tracing::info!("Using Block Range mode (single global cursor)");
            tracing::info!("  Batch size: {} blocks", config.batch_size);

            let extractor_config = BlockRangeConfig {
                rpc_url: config.rpc_url.clone(),
                from_block: config.from_block,
                to_block: config.to_block,
                batch_size: config.batch_size,
                retry_policy: RetryPolicy::default(),
            };
            Box::new(BlockRangeExtractor::new(provider.clone(), extractor_config))
        }
        ExtractionMode::Event => {
            tracing::info!("Using Event mode (per-contract cursors)");
            tracing::info!("  Chunk size: {} events", config.event_chunk_size);
            tracing::info!(
                "  Block batch size: {} blocks",
                config.event_block_batch_size
            );

            let mut event_configs = Vec::new();
            let to_block = config.to_block.unwrap_or(u64::MAX);

            for addr in &all_erc20_addresses {
                event_configs.push(ContractEventConfig {
                    address: *addr,
                    from_block: config.from_block,
                    to_block,
                });
            }

            for addr in &all_erc721_addresses {
                event_configs.push(ContractEventConfig {
                    address: *addr,
                    from_block: config.from_block,
                    to_block,
                });
            }

            for addr in &all_erc1155_addresses {
                event_configs.push(ContractEventConfig {
                    address: *addr,
                    from_block: config.from_block,
                    to_block,
                });
            }

            tracing::info!(
                "  Configured {} contracts for event extraction",
                event_configs.len()
            );

            let extractor_config = EventExtractorConfig {
                contracts: event_configs,
                chunk_size: config.event_chunk_size,
                block_batch_size: config.event_block_batch_size,
                retry_policy: RetryPolicy::default(),
            };
            Box::new(EventExtractor::new(provider.clone(), extractor_config))
        }
    };

    tracing::info!("Extractor configured");

    let db_dir = Path::new(&config.db_dir);
    std::fs::create_dir_all(db_dir)?;

    let effective_metadata_mode =
        config
            .metadata_mode
            .clone()
            .unwrap_or_else(|| match config.mode {
                ExtractionMode::BlockRange => MetadataMode::Deferred,
                ExtractionMode::Event => MetadataMode::Inline,
            });
    tracing::info!("Metadata mode: {:?}", effective_metadata_mode);

    if config.metadata_backfill_only {
        tracing::warn!(
            "--metadata-backfill-only is set. Dedicated metadata backfill flow is not implemented yet; exiting."
        );
        return Ok(());
    }

    let engine_db_path = config
        .database_url
        .clone()
        .unwrap_or_else(|| db_dir.join("engine.db").to_string_lossy().to_string());
    let erc20_db_path = resolve_storage_url(&config, db_dir, "erc20.db");
    let erc721_db_path = resolve_storage_url(&config, db_dir, "erc721.db");
    let erc1155_db_path = resolve_storage_url(&config, db_dir, "erc1155.db");

    let engine_backend = backend_from_path(&engine_db_path);
    let erc20_backend = backend_from_path(&erc20_db_path);
    let erc721_backend = backend_from_path(&erc721_db_path);
    let erc1155_backend = backend_from_path(&erc1155_db_path);

    tracing::info!("Engine backend: {:?} ({})", engine_backend, engine_db_path);
    tracing::info!(
        "ERC20 storage backend: {:?} ({})",
        erc20_backend,
        erc20_db_path
    );
    tracing::info!(
        "ERC721 storage backend: {:?} ({})",
        erc721_backend,
        erc721_db_path
    );
    tracing::info!(
        "ERC1155 storage backend: {:?} ({})",
        erc1155_backend,
        erc1155_db_path
    );

    if config
        .database_url
        .as_deref()
        .is_some_and(|u| backend_from_path(u) == DbBackend::Postgres)
        && (erc20_backend != DbBackend::Postgres
            || erc721_backend != DbBackend::Postgres
            || erc1155_backend != DbBackend::Postgres)
    {
        anyhow::bail!(
            "Engine is configured for Postgres but one or more token storages resolved to SQLite. Set --storage-database-url to the same Postgres URL."
        );
    }

    let engine_db_config = torii::etl::engine_db::EngineDbConfig {
        path: engine_db_path,
    };
    let engine_db = Arc::new(torii::etl::EngineDb::new(engine_db_config).await?);

    let registry = Arc::new(
        ContractRegistry::new(provider.clone(), engine_db.clone())
            .with_rule(Box::new(Erc20Rule::new()))
            .with_rule(Box::new(Erc721Rule::new()))
            .with_rule(Box::new(Erc1155Rule::new())),
    );

    // Load any previously identified contracts from database
    let loaded_count = registry.load_from_db().await?;
    if loaded_count > 0 {
        tracing::info!("Loaded {} contract mappings from database", loaded_count);
    }

    let mut reflection_builder = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(torii::TORII_DESCRIPTOR_SET);

    let mut torii_config = torii::ToriiConfig::builder()
        .port(config.port)
        .database_root(&config.db_dir)
        .engine_database_url(
            config
                .database_url
                .clone()
                .unwrap_or_else(|| db_dir.join("engine.db").to_string_lossy().to_string()),
        )
        .with_extractor(extractor)
        .with_contract_identifier(registry);

    let mut enabled_types: Vec<&str> = Vec::new();
    let mut erc20_grpc_service: Option<Erc20Service> = None;
    let mut erc721_grpc_service: Option<Erc721Service> = None;
    let mut erc1155_grpc_service: Option<Erc1155Service> = None;

    // Block-range mode: create all token infra (auto-discovery). Event mode: only explicit types.
    let is_block_range = config.mode == ExtractionMode::BlockRange;
    let create_erc20 = is_block_range || !all_erc20_addresses.is_empty();
    let create_erc721 = is_block_range || !all_erc721_addresses.is_empty();
    let create_erc1155 = is_block_range || !all_erc1155_addresses.is_empty();

    if create_erc20 {
        enabled_types.push("ERC20");

        let storage = Arc::new(Erc20Storage::new(&erc20_db_path).await?);
        tracing::info!("ERC20 database initialized: {}", erc20_db_path);

        let decoder = Arc::new(Erc20Decoder::new());
        torii_config = torii_config.add_decoder(decoder);

        let grpc_service = Erc20Service::new(storage.clone());
        let sink = Box::new(
            Erc20Sink::new(storage)
                .with_grpc_service(grpc_service.clone())
                .with_balance_tracking(provider.clone()),
        );
        torii_config = torii_config.add_sink_boxed(sink);

        erc20_grpc_service = Some(grpc_service);
        reflection_builder =
            reflection_builder.register_encoded_file_descriptor_set(ERC20_DESCRIPTOR_SET);

        let erc20_decoder_id = DecoderId::new("erc20");
        for address in &all_erc20_addresses {
            torii_config = torii_config.map_contract(*address, vec![erc20_decoder_id]);
        }

        if all_erc20_addresses.is_empty() {
            tracing::info!("ERC20 configured for auto-discovery");
        } else {
            tracing::info!(
                "ERC20 configured with {} explicit contracts (plus auto-discovery in block-range mode)",
                all_erc20_addresses.len()
            );
        }
    }

    if create_erc721 {
        enabled_types.push("ERC721");

        let storage = Arc::new(Erc721Storage::new(&erc721_db_path).await?);
        tracing::info!("ERC721 database initialized: {}", erc721_db_path);

        let decoder = Arc::new(Erc721Decoder::new());
        torii_config = torii_config.add_decoder(decoder);

        let grpc_service = Erc721Service::new(storage.clone());
        let mut sink = Erc721Sink::new(storage).with_grpc_service(grpc_service.clone());
        if effective_metadata_mode == MetadataMode::Inline {
            let image_cache_dir = db_dir.join("image-cache");
            let (token_uri_sender, _token_uri_service) = TokenUriService::spawn_with_image_cache(
                Arc::new(MetadataFetcher::new(provider.clone())),
                sink.storage().clone(),
                1024, // buffer size
                8,    // max concurrent metadata fetches
                Some(image_cache_dir),
                8, // max concurrent image downloads
            );
            sink = sink
                .with_metadata_fetching(provider.clone())
                .with_token_uri_sender(token_uri_sender);
        } else {
            tracing::info!("ERC721 metadata fetching disabled for throughput (deferred mode)");
        }
        let sink = Box::new(sink);
        torii_config = torii_config.add_sink_boxed(sink);

        erc721_grpc_service = Some(grpc_service);
        reflection_builder =
            reflection_builder.register_encoded_file_descriptor_set(ERC721_DESCRIPTOR_SET);

        let erc721_decoder_id = DecoderId::new("erc721");
        for address in &all_erc721_addresses {
            torii_config = torii_config.map_contract(*address, vec![erc721_decoder_id]);
        }

        if all_erc721_addresses.is_empty() {
            tracing::info!("ERC721 configured for auto-discovery");
        } else {
            tracing::info!(
                "ERC721 configured with {} explicit contracts (plus auto-discovery in block-range mode)",
                all_erc721_addresses.len()
            );
        }
    }

    if create_erc1155 {
        enabled_types.push("ERC1155");

        let storage = Arc::new(Erc1155Storage::new(&erc1155_db_path).await?);
        tracing::info!("ERC1155 database initialized: {}", erc1155_db_path);

        let decoder = Arc::new(Erc1155Decoder::new());
        torii_config = torii_config.add_decoder(decoder);

        let grpc_service = Erc1155Service::new(storage.clone());
        let mut sink = Erc1155Sink::new(storage)
            .with_grpc_service(grpc_service.clone())
            .with_balance_tracking(provider.clone());
        if effective_metadata_mode == MetadataMode::Inline {
            let erc1155_fetcher = Arc::new(MetadataFetcher::new(provider.clone()));
            let image_cache_dir = db_dir.join("image-cache");
            let (erc1155_uri_sender, _erc1155_uri_service) =
                TokenUriService::spawn_with_image_cache(
                    erc1155_fetcher,
                    sink.storage().clone(),
                    1024,
                    8,
                    Some(image_cache_dir),
                    8,
                );
            sink = sink.with_token_uri_sender(erc1155_uri_sender);
        } else {
            tracing::info!("ERC1155 metadata fetching disabled for throughput (deferred mode)");
        }
        let sink = Box::new(sink);
        torii_config = torii_config.add_sink_boxed(sink);

        erc1155_grpc_service = Some(grpc_service);
        reflection_builder =
            reflection_builder.register_encoded_file_descriptor_set(ERC1155_DESCRIPTOR_SET);

        let erc1155_decoder_id = DecoderId::new("erc1155");
        for address in &all_erc1155_addresses {
            torii_config = torii_config.map_contract(*address, vec![erc1155_decoder_id]);
        }

        if all_erc1155_addresses.is_empty() {
            tracing::info!("ERC1155 configured for auto-discovery");
        } else {
            tracing::info!(
                "ERC1155 configured with {} explicit contracts (plus auto-discovery in block-range mode)",
                all_erc1155_addresses.len()
            );
        }
    }

    let reflection = reflection_builder
        .build_v1()
        .expect("Failed to build gRPC reflection service");

    let mut grpc_builder = tonic::transport::Server::builder();
    let grpc_router = match (
        erc20_grpc_service,
        erc721_grpc_service,
        erc1155_grpc_service,
    ) {
        (Some(erc20), Some(erc721), Some(erc1155)) => grpc_builder
            .add_service(tonic_web::enable(Erc20Server::new(erc20)))
            .add_service(tonic_web::enable(Erc721Server::new(erc721)))
            .add_service(tonic_web::enable(Erc1155Server::new(erc1155)))
            .add_service(reflection),
        (Some(erc20), Some(erc721), None) => grpc_builder
            .add_service(tonic_web::enable(Erc20Server::new(erc20)))
            .add_service(tonic_web::enable(Erc721Server::new(erc721)))
            .add_service(reflection),
        (Some(erc20), None, Some(erc1155)) => grpc_builder
            .add_service(tonic_web::enable(Erc20Server::new(erc20)))
            .add_service(tonic_web::enable(Erc1155Server::new(erc1155)))
            .add_service(reflection),
        (None, Some(erc721), Some(erc1155)) => grpc_builder
            .add_service(tonic_web::enable(Erc721Server::new(erc721)))
            .add_service(tonic_web::enable(Erc1155Server::new(erc1155)))
            .add_service(reflection),
        (Some(erc20), None, None) => grpc_builder
            .add_service(tonic_web::enable(Erc20Server::new(erc20)))
            .add_service(reflection),
        (None, Some(erc721), None) => grpc_builder
            .add_service(tonic_web::enable(Erc721Server::new(erc721)))
            .add_service(reflection),
        (None, None, Some(erc1155)) => grpc_builder
            .add_service(tonic_web::enable(Erc1155Server::new(erc1155)))
            .add_service(reflection),
        (None, None, None) => {
            // No token services, just reflection
            grpc_builder.add_service(reflection)
        }
    };

    let torii_config = torii_config
        .with_grpc_router(grpc_router)
        .with_custom_reflection(true)
        .build();

    tracing::info!("Torii configured, starting ETL pipeline...");
    tracing::info!("Enabled token types: {}", enabled_types.join(", "));
    tracing::info!("gRPC service available at localhost:{}", config.port);
    tracing::info!("  - torii.Torii (EventBus subscriptions)");

    if create_erc20 {
        tracing::info!("  - torii.sinks.erc20.Erc20 (ERC20 queries and subscriptions)");
    }
    if create_erc721 {
        tracing::info!("  - torii.sinks.erc721.Erc721 (ERC721 queries and subscriptions)");
    }
    if create_erc1155 {
        tracing::info!("  - torii.sinks.erc1155.Erc1155 (ERC1155 queries and subscriptions)");
    }

    torii::run(torii_config)
        .await
        .map_err(|e| anyhow::anyhow!("Torii error: {e}"))?;

    tracing::info!("Torii shutdown complete");

    #[cfg(feature = "profiling")]
    {
        if let Ok(report) = guard.report().build() {
            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let db_backend = if erc20_backend == DbBackend::Postgres {
                "postgres"
            } else {
                "sqlite"
            };
            let mode = match config.mode {
                ExtractionMode::BlockRange => "block-range",
                ExtractionMode::Event => "event",
            };
            let filename = format!("flamegraph-torii-tokens-{mode}-{db_backend}-{ts}.svg");
            let file = std::fs::File::create(&filename).unwrap();
            report.flamegraph(file).unwrap();
            tracing::info!("Flamegraph generated: {}", filename);
        }
    }

    Ok(())
}
