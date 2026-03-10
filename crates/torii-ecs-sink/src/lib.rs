pub mod grpc_service;
mod storage;

pub mod proto {
    pub mod types {
        include!("generated/types.rs");
    }

    pub mod world {
        include!("generated/world.rs");
    }
}

pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("generated/world_descriptor.bin");

use anyhow::Context;
use async_trait::async_trait;
use std::sync::Arc;

use torii::etl::{
    envelope::{Envelope, TypeId},
    extractor::ExtractionBatch,
    sink::{EventBus, Sink, TopicInfo},
};

pub use grpc_service::EcsGrpcService;
use grpc_service::GrpcState;
use storage::{bootstrap_contract_state, persist_events_batch, refresh_metadata, SharedMetadata};

pub struct EcsSink {
    pool: Arc<sqlx::PgPool>,
    grpc_service: Arc<EcsGrpcService>,
}

impl EcsSink {
    pub async fn new(database_url: &str) -> anyhow::Result<Self> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .with_context(|| format!("failed to connect torii-ecs-sink to {database_url}"))?;

        storage::initialize_storage(&pool).await?;
        let metadata = SharedMetadata::new(refresh_metadata(&pool).await?);
        let contract_state = bootstrap_contract_state(&pool, &metadata).await?;
        let state = GrpcState::new(Arc::new(pool), metadata, contract_state);

        Ok(Self {
            pool: state.pool.clone(),
            grpc_service: Arc::new(EcsGrpcService::new(state)),
        })
    }

    pub fn get_grpc_service_impl(&self) -> Arc<EcsGrpcService> {
        self.grpc_service.clone()
    }
}

#[async_trait]
impl Sink for EcsSink {
    fn name(&self) -> &str {
        "ecs"
    }

    fn interested_types(&self) -> Vec<TypeId> {
        vec![TypeId::new("introspect")]
    }

    async fn process(&self, envelopes: &[Envelope], batch: &ExtractionBatch) -> anyhow::Result<()> {
        persist_events_batch(&self.pool, batch).await?;
        self.grpc_service.handle_batch(envelopes, batch).await
    }

    fn topics(&self) -> Vec<TopicInfo> {
        Vec::new()
    }

    fn build_routes(&self) -> torii::axum::Router {
        torii::axum::Router::new()
    }

    async fn initialize(
        &mut self,
        _event_bus: Arc<EventBus>,
        _context: &torii::etl::sink::SinkContext,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
