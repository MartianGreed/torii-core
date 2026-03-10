//! Torii Introspect - Dojo introspect indexer backed by PostgreSQL.

mod config;

use anyhow::Result;
use clap::Parser;
use config::Config;
use starknet::core::types::Felt;
use std::sync::Arc;
use tonic::transport::Server;
use torii::etl::decoder::DecoderId;
use torii::etl::extractor::{
    ContractEventConfig, EventExtractor, EventExtractorConfig, RetryPolicy,
};
use torii_dojo::decoder::DojoDecoder;
use torii_dojo::manager::{DojoTableStore, MergedStore, PostgresStore, SchemaBootstrapPoint};
use torii_dojo::store::postgres::initialize_dojo_schema;
use torii_dojo::store::DojoStoreTrait;
use torii_ecs_sink::proto::world::world_server::WorldServer;
use torii_ecs_sink::EcsSink;
use torii_introspect_postgres_sink::IntrospectPostgresSink;
use torii_postgres_common::{connect, table_exists};
use torii_runtime::{configure_observability, init_tracing, starknet_provider};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing()?;

    let config = Config::parse();
    run_indexer(config).await
}

async fn run_indexer(config: Config) -> Result<()> {
    tracing::info!("Starting Torii Introspect Indexer");

    configure_observability(config.observability.observability);

    let storage_database_url = config.storage_database_url()?.to_string();
    let engine_database_url = config.engine_database_url();
    let contracts = config.contract_addresses()?;

    tracing::info!("RPC URL: {}", config.rpc.rpc_url);
    tracing::info!("Contracts: {}", contracts.len());
    tracing::info!("From block: {}", config.blocks.from_block);
    if let Some(to_block) = config.blocks.to_block {
        tracing::info!("To block: {}", to_block);
    } else {
        tracing::info!("To block: following chain head");
    }
    tracing::info!("Engine database URL: {}", engine_database_url);
    tracing::info!("Storage database URL: {}", storage_database_url);
    tracing::info!(
        "Observability: {}",
        if config.observability.observability {
            "enabled"
        } else {
            "disabled"
        }
    );

    let extractor_provider = starknet_provider(&config.rpc.rpc_url)?;
    let provider = (*extractor_provider).clone();

    let extractor = Box::new(EventExtractor::new(
        extractor_provider.clone(),
        EventExtractorConfig {
            contracts: contracts
                .iter()
                .copied()
                .map(|address| ContractEventConfig {
                    address,
                    from_block: config.blocks.from_block,
                    to_block: config.blocks.to_block.unwrap_or(u64::MAX),
                })
                .collect(),
            chunk_size: config.event_extraction.event_chunk_size,
            block_batch_size: config.event_extraction.event_block_batch_size,
            retry_policy: RetryPolicy::default(),
            ignore_saved_state: config.event_extraction.ignore_saved_state,
        },
    ));

    let bootstrap_points = resolve_bootstrap_points(
        &storage_database_url,
        &contracts,
        config.blocks.from_block,
        config.event_extraction.ignore_saved_state,
    )
    .await?;
    for point in &bootstrap_points {
        tracing::info!(
            target: "torii::dojo::metadata",
            owner = %point.owner,
            block_number = point.block_number,
            had_saved_state = point.had_saved_state,
            "Resolved Dojo schema bootstrap point"
        );
    }

    let primary_store = PostgresStore::new(&storage_database_url).await?;
    let historical_bootstrap = primary_store
        .load_historical_bootstrap(&bootstrap_points)
        .await?;
    if !historical_bootstrap.unsafe_owners.is_empty()
        && !config.allow_unsafe_latest_schema_bootstrap
    {
        anyhow::bail!(
            "historical Dojo schema bootstrap is unavailable for contract(s): {}. \
             Re-run with --ignore-saved-state, use a fresh database, or pass \
             --allow-unsafe-latest-schema-bootstrap to keep the previous best-effort behavior",
            historical_bootstrap
                .unsafe_owners
                .iter()
                .map(|owner| format!("{owner:#x}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    let secondary_store = connect(&storage_database_url, Some(5)).await?;
    initialize_dojo_schema(&secondary_store).await?;
    let merged_store = MergedStore::new(primary_store, secondary_store);
    let preloaded_tables = if historical_bootstrap.unsafe_owners.is_empty() {
        tracing::info!(
            target: "torii::dojo::metadata",
            latest_tables = historical_bootstrap.latest_table_count,
            historical_tables = historical_bootstrap.historical_table_count,
            "Bootstrapping decoder from historical Dojo schema snapshot"
        );
        historical_bootstrap.tables
    } else {
        tracing::warn!(
            target: "torii::dojo::metadata",
            owners = historical_bootstrap
                .unsafe_owners
                .iter()
                .map(|owner| format!("{owner:#x}"))
                .collect::<Vec<_>>()
                .join(", "),
            "Historical Dojo schema bootstrap unavailable; falling back to latest persisted schema"
        );
        merged_store.load_tables(&[]).await?
    };

    let metadata_store = DojoTableStore::from_loaded_tables(merged_store, preloaded_tables.clone());
    let bootstrap_tables = metadata_store.create_table_messages()?;
    let decoder: DojoDecoder<DojoTableStore<MergedStore<PostgresStore, _>>, _> =
        DojoDecoder::with_tables(metadata_store, provider, preloaded_tables);
    let decoder: Arc<dyn torii::etl::Decoder> = Arc::new(decoder);
    let ecs_sink = EcsSink::new(&storage_database_url).await?;
    let ecs_grpc = ecs_sink.get_grpc_service_impl();

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(torii::TORII_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(torii_ecs_sink::FILE_DESCRIPTOR_SET)
        .build_v1()?;
    let grpc_router = Server::builder()
        .accept_http1(true)
        .add_service(tonic_web::enable(WorldServer::new((*ecs_grpc).clone())))
        .add_service(tonic_web::enable(reflection));

    let mut torii_config = torii::ToriiConfig::builder()
        .port(config.server.port)
        .engine_database_url(engine_database_url)
        .with_grpc_router(grpc_router)
        .with_custom_reflection(true)
        .with_extractor(extractor)
        .add_decoder(decoder)
        .add_sink_boxed(Box::new(
            IntrospectPostgresSink::new(storage_database_url, config.postgres.max_db_connections)
                .with_bootstrap_tables(bootstrap_tables),
        ))
        .add_sink_boxed(Box::new(ecs_sink));

    let decoder_id = DecoderId::new("dojo-introspect");
    for contract in contracts {
        tracing::info!("Mapping Dojo contract {:#x} to dojo-introspect", contract);
        torii_config = torii_config.map_contract(contract, vec![decoder_id]);
    }

    tracing::info!("Torii configured, starting ETL pipeline...");
    tracing::info!("gRPC service available on port {}", config.server.port);
    tracing::info!("  - torii.Torii (core subscriptions and metrics endpoint)");
    tracing::info!("  - world.World (legacy Dojo ECS queries and subscriptions)");

    torii::run(torii_config.build())
        .await
        .map_err(|e| anyhow::anyhow!("Torii error: {e}"))?;

    tracing::info!("Torii shutdown complete");
    Ok(())
}

async fn resolve_bootstrap_points(
    database_url: &str,
    contracts: &[Felt],
    from_block: u64,
    ignore_saved_state: bool,
) -> Result<Vec<SchemaBootstrapPoint>> {
    if ignore_saved_state {
        return Ok(contracts
            .iter()
            .copied()
            .map(|owner| SchemaBootstrapPoint {
                owner,
                block_number: from_block,
                had_saved_state: false,
            })
            .collect());
    }

    let pool = connect(database_url, Some(1)).await?;

    let engine_table_exists = table_exists(&pool, "engine", "extractor_state")
        .await
        .unwrap_or(false);

    if !engine_table_exists {
        return Ok(contracts
            .iter()
            .copied()
            .map(|owner| SchemaBootstrapPoint {
                owner,
                block_number: from_block,
                had_saved_state: false,
            })
            .collect());
    }

    let mut points = Vec::with_capacity(contracts.len());
    for owner in contracts {
        let state_key = format!("{owner:#x}");
        let saved_state: Option<String> = sqlx::query_scalar(
            r"
            SELECT state_value
            FROM engine.extractor_state
            WHERE extractor_type = 'event' AND state_key = $1
            ",
        )
        .bind(&state_key)
        .fetch_optional(&pool)
        .await?;

        let block_number = saved_state
            .as_deref()
            .and_then(parse_saved_state_block)
            .unwrap_or(from_block);
        points.push(SchemaBootstrapPoint {
            owner: *owner,
            block_number,
            had_saved_state: saved_state.is_some(),
        });
    }

    Ok(points)
}

fn parse_saved_state_block(saved_state: &str) -> Option<u64> {
    saved_state
        .split('|')
        .next()
        .and_then(|part| part.strip_prefix("block:"))
        .and_then(|block| block.parse::<u64>().ok())
}
