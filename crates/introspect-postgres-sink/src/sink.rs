use crate::processor::PostgresSimpleDb;
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use torii::axum::Router;
use torii::etl::{
    envelope::{Envelope, TypeId},
    extractor::ExtractionBatch,
    sink::{EventBus, Sink, SinkContext, TopicInfo},
};
use torii_introspect::events::{IntrospectBody, IntrospectMsg};
use torii_introspect::CreateTable;

pub const LOGGING_TARGET: &str = "torii::sinks::introspect::postgres";

pub struct IntrospectPostgresSink {
    database_url: String,
    max_connections: Option<u32>,
    bootstrap_tables: Vec<CreateTable>,
    db: Option<Arc<Mutex<PostgresSimpleDb>>>,
}

impl IntrospectPostgresSink {
    pub fn new(database_url: impl Into<String>, max_connections: Option<u32>) -> Self {
        Self {
            database_url: database_url.into(),
            max_connections,
            bootstrap_tables: Vec::new(),
            db: None,
        }
    }

    pub fn with_bootstrap_tables(mut self, bootstrap_tables: Vec<CreateTable>) -> Self {
        self.bootstrap_tables = bootstrap_tables;
        self
    }

    fn db(&self) -> Result<&Arc<Mutex<PostgresSimpleDb>>> {
        self.db
            .as_ref()
            .ok_or_else(|| anyhow!("introspect-postgres sink used before initialize"))
    }
}

#[async_trait]
impl Sink for IntrospectPostgresSink {
    fn name(&self) -> &'static str {
        "introspect-postgres"
    }

    fn interested_types(&self) -> Vec<TypeId> {
        vec![TypeId::new("introspect")]
    }

    async fn process(&self, envelopes: &[Envelope], batch: &ExtractionBatch) -> Result<()> {
        let mut processed = 0usize;
        let mut create_tables = 0usize;
        let mut update_tables = 0usize;
        let mut inserts_fields = 0usize;
        let mut inserted_records = 0usize;
        let mut delete_records = 0usize;
        let db = self.db()?.clone();
        let mut db = db.lock().await;

        for envelope in envelopes {
            if envelope.type_id != TypeId::new("introspect") {
                continue;
            }

            let Some(body) = envelope.downcast_ref::<IntrospectBody>() else {
                continue;
            };

            let context = batch
                .get_event_context(&body.transaction_hash, body.from_address)
                .with_context(|| {
                    format!(
                        "failed to resolve event context for introspect tx {:#x}",
                        body.transaction_hash
                    )
                })?;

            db.process_message(&body.msg, &context).await?;
            processed += 1;
            match &body.msg {
                IntrospectMsg::CreateTable(_) => create_tables += 1,
                IntrospectMsg::UpdateTable(_) => update_tables += 1,
                IntrospectMsg::InsertsFields(event) => {
                    inserts_fields += 1;
                    inserted_records += event.records.len();
                }
                IntrospectMsg::DeleteRecords(event) => {
                    delete_records += event.rows.len();
                }
                _ => {}
            }
        }

        if processed > 0 {
            tracing::info!(
                target: LOGGING_TARGET,
                processed,
                create_tables,
                update_tables,
                inserts_fields,
                inserted_records,
                delete_records,
                "Processed introspect envelopes"
            );
            ::metrics::counter!("torii_introspect_sink_messages_total", "message" => "create_table")
                .increment(create_tables as u64);
            ::metrics::counter!("torii_introspect_sink_messages_total", "message" => "update_table")
                .increment(update_tables as u64);
            ::metrics::counter!("torii_introspect_sink_messages_total", "message" => "inserts_fields")
                .increment(inserts_fields as u64);
            ::metrics::counter!("torii_introspect_sink_records_total", "message" => "inserts_fields")
                .increment(inserted_records as u64);
            ::metrics::counter!("torii_introspect_sink_records_total", "message" => "delete_records")
                .increment(delete_records as u64);
        }

        Ok(())
    }

    fn topics(&self) -> Vec<TopicInfo> {
        Vec::new()
    }

    fn build_routes(&self) -> Router {
        Router::new()
    }

    async fn initialize(
        &mut self,
        _event_bus: Arc<EventBus>,
        _context: &SinkContext,
    ) -> Result<()> {
        let mut db = PostgresSimpleDb::new(&self.database_url, self.max_connections).await?;
        db.initialize().await?;
        if !db.has_tables() && !self.bootstrap_tables.is_empty() {
            db.bootstrap_tables(&self.bootstrap_tables).await?;
            tracing::info!(
                target: LOGGING_TARGET,
                tables = self.bootstrap_tables.len(),
                "Bootstrapped sink schema from persisted manager fallback"
            );
        }

        tracing::info!(
            target: LOGGING_TARGET,
            "Initialized introspect Postgres sink"
        );

        self.db = Some(Arc::new(Mutex::new(db)));
        Ok(())
    }
}
