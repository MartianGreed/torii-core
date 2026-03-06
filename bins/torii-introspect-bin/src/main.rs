//! Torii Introspect - Dojo introspect indexer backed by PostgreSQL.

mod config;

use anyhow::Result;
use clap::Parser;
use config::Config;
use std::sync::Arc;
use torii::etl::decoder::DecoderId;
use torii::etl::extractor::{
    ContractEventConfig, EventExtractor, EventExtractorConfig, RetryPolicy,
};
use torii_dojo::decoder::DojoDecoder;
use torii_dojo::manager::DojoTableStore;
use torii_introspect_postgres_sink::IntrospectPostgresSink;

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

async fn run_indexer(config: Config) -> Result<()> {
    tracing::info!("Starting Torii Introspect Indexer");

    let metrics_enabled = if config.observability {
        "true"
    } else {
        "false"
    };
    std::env::set_var("TORII_METRICS_ENABLED", metrics_enabled);

    let storage_database_url = config.storage_database_url()?.to_string();
    let engine_database_url = config.engine_database_url();
    let contracts = config.contract_addresses()?;

    tracing::info!("RPC URL: {}", config.rpc_url);
    tracing::info!("Contracts: {}", contracts.len());
    tracing::info!("From block: {}", config.from_block);
    if let Some(to_block) = config.to_block {
        tracing::info!("To block: {}", to_block);
    } else {
        tracing::info!("To block: following chain head");
    }
    tracing::info!("Engine database URL: {}", engine_database_url);
    tracing::info!("Storage database URL: {}", storage_database_url);
    tracing::info!(
        "Observability: {}",
        if config.observability {
            "enabled"
        } else {
            "disabled"
        }
    );

    let provider = starknet::providers::jsonrpc::JsonRpcClient::new(
        starknet::providers::jsonrpc::HttpTransport::new(
            url::Url::parse(&config.rpc_url).expect("Invalid RPC URL"),
        ),
    );
    let extractor_provider = Arc::new(provider.clone());

    let extractor = Box::new(EventExtractor::new(
        extractor_provider.clone(),
        EventExtractorConfig {
            contracts: contracts
                .iter()
                .copied()
                .map(|address| ContractEventConfig {
                    address,
                    from_block: config.from_block,
                    to_block: config.to_block.unwrap_or(u64::MAX),
                })
                .collect(),
            chunk_size: config.event_chunk_size,
            block_batch_size: config.event_block_batch_size,
            retry_policy: RetryPolicy::default(),
        },
    ));

    let manager = DojoTableStore::new_postgres(&storage_database_url).await?;
    let bootstrap_tables = manager.create_table_messages()?;
    let decoder = Arc::new(DojoDecoder::new_default(manager, provider));

    let mut torii_config = torii::ToriiConfig::builder()
        .port(config.port)
        .engine_database_url(engine_database_url)
        .with_extractor(extractor)
        .add_decoder(decoder)
        .add_sink_boxed(Box::new(
            IntrospectPostgresSink::new(storage_database_url, config.max_db_connections)
                .with_bootstrap_tables(bootstrap_tables),
        ));

    let decoder_id = DecoderId::new("dojo-introspect");
    for contract in contracts {
        tracing::info!("Mapping Dojo contract {:#x} to dojo-introspect", contract);
        torii_config = torii_config.map_contract(contract, vec![decoder_id]);
    }

    tracing::info!("Torii configured, starting ETL pipeline...");
    tracing::info!("gRPC service available on port {}", config.port);
    tracing::info!("  - torii.Torii (core subscriptions and metrics endpoint)");

    torii::run(torii_config.build())
        .await
        .map_err(|e| anyhow::anyhow!("Torii error: {e}"))?;

    tracing::info!("Torii shutdown complete");
    Ok(())
}
