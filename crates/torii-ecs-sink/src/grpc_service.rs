use async_trait::async_trait;
use serde_json::Map;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use torii::etl::extractor::ExtractionBatch;
use torii_introspect::events::{DeleteRecords, IntrospectBody, IntrospectMsg};
use torii_introspect::schema::TableSchema;

use crate::proto::world::world_server::World;
use crate::proto::world::{
    RetrieveEntitiesRequest, RetrieveEntitiesResponse, RetrieveEventsRequest,
    RetrieveEventsResponse, SubscribeContractsRequest, SubscribeContractsResponse,
    SubscribeEntitiesRequest, SubscribeEntityResponse, SubscribeEventsRequest,
    SubscribeEventsResponse, UpdateEntitiesSubscriptionRequest, WorldsRequest, WorldsResponse,
};
use crate::proto::{self};
use crate::storage::{
    build_worlds_response, column_value_to_ty, contract_to_proto, event_from_emitted,
    felt_from_json, load_events, load_table_rows, model_label, update_contract_state, ManagedTable,
    SharedContracts, SharedMetadata, TableKind,
};
use introspect_types::{Attributes, FeltId};
use starknet::core::types::Felt;
use torii_dojo::DojoTable;

#[derive(Clone)]
pub struct GrpcState {
    pub pool: Arc<sqlx::PgPool>,
    pub metadata: SharedMetadata,
    pub contracts: SharedContracts,
}

impl GrpcState {
    pub fn new(
        pool: Arc<sqlx::PgPool>,
        metadata: SharedMetadata,
        contracts: SharedContracts,
    ) -> Self {
        Self {
            pool,
            metadata,
            contracts,
        }
    }
}

#[derive(Clone)]
struct EntitySubscriber {
    clause: Option<proto::types::Clause>,
    world_addresses: Vec<Felt>,
    tx: mpsc::Sender<Result<SubscribeEntityResponse, Status>>,
}

#[derive(Default)]
struct EntitySubscriptions {
    next_id: u64,
    subscribers: HashMap<u64, EntitySubscriber>,
}

#[derive(Clone, Default)]
struct EntityManager(Arc<RwLock<EntitySubscriptions>>);

impl EntityManager {
    async fn add_subscriber(
        &self,
        clause: Option<proto::types::Clause>,
        world_addresses: Vec<Felt>,
    ) -> mpsc::Receiver<Result<SubscribeEntityResponse, Status>> {
        let (tx, rx) = mpsc::channel(128);
        let subscription_id = {
            let mut state = self.0.write().unwrap();
            state.next_id += 1;
            let subscription_id = state.next_id;
            state.subscribers.insert(
                subscription_id,
                EntitySubscriber {
                    clause,
                    world_addresses,
                    tx: tx.clone(),
                },
            );
            subscription_id
        };
        let _ = tx
            .send(Ok(SubscribeEntityResponse {
                entity: None,
                subscription_id,
            }))
            .await;
        rx
    }

    fn update_subscriber(
        &self,
        id: u64,
        clause: Option<proto::types::Clause>,
        world_addresses: Vec<Felt>,
    ) {
        if let Some(subscriber) = self.0.write().unwrap().subscribers.get_mut(&id) {
            subscriber.clause = clause;
            subscriber.world_addresses = world_addresses;
        }
    }

    async fn broadcast(&self, entity: proto::types::Entity) {
        let hashed_keys = Felt::from_bytes_be_slice(&entity.hashed_keys);
        let keys = entity
            .models
            .first()
            .map(extract_model_keys)
            .unwrap_or_default();
        let mut stale = Vec::new();

        for (id, subscriber) in self.0.read().unwrap().subscribers.iter() {
            if !subscriber.world_addresses.is_empty()
                && !subscriber
                    .world_addresses
                    .contains(&Felt::from_bytes_be_slice(&entity.world_address))
            {
                continue;
            }

            if let Some(clause) = &subscriber.clause {
                if !match_entity_clause(hashed_keys, &keys, &entity.models, clause) {
                    continue;
                }
            }

            if subscriber
                .tx
                .try_send(Ok(SubscribeEntityResponse {
                    entity: Some(entity.clone()),
                    subscription_id: *id,
                }))
                .is_err()
            {
                stale.push(*id);
            }
        }

        if !stale.is_empty() {
            let mut state = self.0.write().unwrap();
            for id in stale {
                state.subscribers.remove(&id);
            }
        }
    }
}

#[derive(Clone)]
struct ContractSubscriber {
    query: proto::types::ContractQuery,
    tx: mpsc::Sender<Result<SubscribeContractsResponse, Status>>,
}

#[derive(Clone, Default)]
struct ContractManager(Arc<RwLock<HashMap<u64, ContractSubscriber>>>);

impl ContractManager {
    async fn add_subscriber(
        &self,
        query: proto::types::ContractQuery,
        initial: Vec<proto::types::Contract>,
    ) -> mpsc::Receiver<Result<SubscribeContractsResponse, Status>> {
        let (tx, rx) = mpsc::channel(128);
        for contract in initial {
            let _ = tx
                .send(Ok(SubscribeContractsResponse {
                    contract: Some(contract),
                }))
                .await;
        }

        let id = rand_id();
        self.0
            .write()
            .unwrap()
            .insert(id, ContractSubscriber { query, tx });
        rx
    }

    async fn broadcast(&self, contract: proto::types::Contract) {
        let contract_address = contract.contract_address.clone();
        let mut stale = Vec::new();
        for (id, subscriber) in self.0.read().unwrap().iter() {
            if !subscriber.query.contract_addresses.is_empty()
                && !subscriber
                    .query
                    .contract_addresses
                    .contains(&contract_address)
            {
                continue;
            }

            if subscriber
                .tx
                .try_send(Ok(SubscribeContractsResponse {
                    contract: Some(contract.clone()),
                }))
                .is_err()
            {
                stale.push(*id);
            }
        }

        if !stale.is_empty() {
            let mut state = self.0.write().unwrap();
            for id in stale {
                state.remove(&id);
            }
        }
    }
}

#[derive(Clone)]
struct EventSubscriber {
    keys: Vec<proto::types::KeysClause>,
    tx: mpsc::Sender<Result<SubscribeEventsResponse, Status>>,
}

#[derive(Clone, Default)]
struct EventManager(Arc<RwLock<HashMap<u64, EventSubscriber>>>);

impl EventManager {
    async fn add_subscriber(
        &self,
        keys: Vec<proto::types::KeysClause>,
    ) -> mpsc::Receiver<Result<SubscribeEventsResponse, Status>> {
        let (tx, rx) = mpsc::channel(128);
        let _ = tx.send(Ok(SubscribeEventsResponse { event: None })).await;
        self.0
            .write()
            .unwrap()
            .insert(rand_id(), EventSubscriber { keys, tx });
        rx
    }

    async fn broadcast(&self, event: proto::types::Event) {
        let keys = event
            .keys
            .iter()
            .map(|key| Felt::from_bytes_be_slice(key))
            .collect::<Vec<_>>();
        let mut stale = Vec::new();
        for (id, subscriber) in self.0.read().unwrap().iter() {
            if !match_keys_clause(&keys, &subscriber.keys) {
                continue;
            }

            if subscriber
                .tx
                .try_send(Ok(SubscribeEventsResponse {
                    event: Some(event.clone()),
                }))
                .is_err()
            {
                stale.push(*id);
            }
        }

        if !stale.is_empty() {
            let mut state = self.0.write().unwrap();
            for id in stale {
                state.remove(&id);
            }
        }
    }
}

#[derive(Clone)]
pub struct EcsGrpcService {
    state: GrpcState,
    entity_manager: EntityManager,
    event_message_manager: EntityManager,
    event_manager: EventManager,
    contract_manager: ContractManager,
}

impl EcsGrpcService {
    pub fn new(state: GrpcState) -> Self {
        Self {
            state,
            entity_manager: EntityManager::default(),
            event_message_manager: EntityManager::default(),
            event_manager: EventManager::default(),
            contract_manager: ContractManager::default(),
        }
    }

    pub async fn handle_batch(
        &self,
        envelopes: &[torii::etl::envelope::Envelope],
        batch: &ExtractionBatch,
    ) -> anyhow::Result<()> {
        let touched_worlds = batch
            .events
            .iter()
            .map(|event| event.from_address)
            .collect::<Vec<_>>();
        update_contract_state(
            &self.state.contracts,
            touched_worlds.clone(),
            batch.max_block(),
            batch
                .max_block()
                .and_then(|block| batch.blocks.get(&block).map(|ctx| ctx.timestamp)),
        );

        for world in touched_worlds {
            let contract = { self.state.contracts.read().unwrap().get(&world).cloned() };
            if let Some(contract) = contract {
                self.contract_manager
                    .broadcast(contract_to_proto(&contract))
                    .await;
            }
        }

        for raw_event in &batch.events {
            self.event_manager
                .broadcast(event_from_emitted(raw_event))
                .await;
        }

        for envelope in envelopes {
            if envelope.type_id != torii::etl::envelope::TypeId::new("introspect") {
                continue;
            }

            let Some(body) = envelope.downcast_ref::<IntrospectBody>() else {
                continue;
            };

            match &body.msg {
                IntrospectMsg::CreateTable(table) => {
                    let schema = TableSchema {
                        id: table.id,
                        name: table.name.clone(),
                        attributes: table.attributes.clone(),
                        primary: table.primary.clone(),
                        columns: table.columns.clone(),
                    };
                    self.state
                        .metadata
                        .upsert_table(body.from_address, DojoTable::from(schema));
                }
                IntrospectMsg::UpdateTable(table) => {
                    let schema = TableSchema {
                        id: table.id,
                        name: table.name.clone(),
                        attributes: table.attributes.clone(),
                        primary: table.primary.clone(),
                        columns: table.columns.clone(),
                    };
                    self.state
                        .metadata
                        .upsert_table(body.from_address, DojoTable::from(schema));
                }
                IntrospectMsg::InsertsFields(insert) => {
                    self.broadcast_table_records(
                        insert.table,
                        insert.records.iter().map(|record| record.id).collect(),
                    )
                    .await?;
                }
                IntrospectMsg::DeleteRecords(DeleteRecords { table, rows }) => {
                    let ids = rows
                        .iter()
                        .map(|row| row.id().to_bytes_be())
                        .collect::<Vec<_>>();
                    self.broadcast_table_records(*table, ids).await?;
                }
                _ => {}
            }
        }

        Ok(())
    }

    async fn broadcast_table_records(
        &self,
        table_id: Felt,
        record_ids: Vec<[u8; 32]>,
    ) -> anyhow::Result<()> {
        let table = {
            let metadata = self.state.metadata.read();
            metadata.tables.get(&table_id).cloned()
        };

        let Some(table) = table else {
            return Ok(());
        };

        for record_id in record_ids {
            if let Some(entity) = self
                .load_entity(
                    table.kind,
                    table.world_address,
                    Felt::from_bytes_be(&record_id),
                )
                .await?
            {
                match table.kind {
                    TableKind::Model => self.entity_manager.broadcast(entity).await,
                    TableKind::EventMessage => self.event_message_manager.broadcast(entity).await,
                }
            }
        }

        Ok(())
    }

    async fn load_entity(
        &self,
        kind: TableKind,
        world_address: Felt,
        hashed_keys: Felt,
    ) -> anyhow::Result<Option<proto::types::Entity>> {
        let query = proto::types::Query {
            clause: Some(proto::types::Clause {
                clause_type: Some(proto::types::clause::ClauseType::HashedKeys(
                    proto::types::HashedKeysClause {
                        hashed_keys: vec![hashed_keys.to_bytes_be().to_vec()],
                    },
                )),
            }),
            no_hashed_keys: false,
            models: Vec::new(),
            pagination: Some(proto::types::Pagination {
                cursor: String::new(),
                limit: 1,
            }),
            historical: false,
            world_addresses: vec![world_address.to_bytes_be().to_vec()],
        };

        Ok(self.fetch_entities(kind, &query).await?.into_iter().next())
    }

    async fn fetch_entities(
        &self,
        kind: TableKind,
        query: &proto::types::Query,
    ) -> anyhow::Result<Vec<proto::types::Entity>> {
        if query.historical {
            return Err(anyhow::anyhow!(
                "historical entity queries are not available in torii-ecs-sink"
            ));
        }

        let requested_worlds = query
            .world_addresses
            .iter()
            .map(|bytes| Felt::from_bytes_be_slice(bytes))
            .collect::<Vec<_>>();
        let tables =
            self.state
                .metadata
                .read()
                .tables_for_kind(kind, &requested_worlds, &query.models);

        let mut grouped = HashMap::<(Felt, Felt), proto::types::Entity>::new();

        for table in tables {
            for row in load_table_rows(&self.state.pool, &table.table.name).await? {
                if let Some((hashed_keys, keys, model)) = table_row_to_model(&table, &row) {
                    let entity = grouped
                        .entry((table.world_address, hashed_keys))
                        .or_insert_with(|| proto::types::Entity {
                            hashed_keys: hashed_keys.to_bytes_be().to_vec(),
                            models: Vec::new(),
                            created_at: 0,
                            updated_at: 0,
                            executed_at: 0,
                            world_address: table.world_address.to_bytes_be().to_vec(),
                        });
                    entity.models.push(model);

                    if let Some(clause) = &query.clause {
                        if !match_entity_clause(hashed_keys, &keys, &entity.models, clause) {
                            continue;
                        }
                    }
                }
            }
        }

        let mut entities = grouped.into_values().collect::<Vec<_>>();
        entities.sort_by(|left, right| left.hashed_keys.cmp(&right.hashed_keys));

        if let Some(limit) = query
            .pagination
            .as_ref()
            .map(|pagination| pagination.limit)
            .filter(|limit| *limit > 0)
        {
            entities.truncate(limit as usize);
        }

        Ok(entities)
    }

    async fn fetch_contracts(
        &self,
        query: &proto::types::ContractQuery,
    ) -> Vec<proto::types::Contract> {
        self.state
            .contracts
            .read()
            .unwrap()
            .values()
            .filter(|contract| {
                query.contract_addresses.is_empty()
                    || query
                        .contract_addresses
                        .contains(&contract.contract_address.to_bytes_be().to_vec())
            })
            .map(contract_to_proto)
            .collect()
    }
}

fn rand_id() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or_default()
}

fn table_row_to_model(
    table: &ManagedTable,
    row: &Map<String, serde_json::Value>,
) -> Option<(Felt, Vec<Felt>, proto::types::Struct)> {
    let primary_value = row.get(&table.table.primary.name)?;
    let hashed_keys = felt_from_json(primary_value)?;

    let mut keys = Vec::new();
    let children = table
        .table
        .selectors()
        .filter_map(|selector| {
            let column = table.table.get_column(selector).ok()?;
            let value = row.get(&column.name)?;
            if column.attributes.has_attribute("key") {
                keys.push(felt_from_json(value)?);
            }
            Some(proto::types::Member {
                name: column.name.clone(),
                ty: Some(column_value_to_ty(&column.type_def, value)?),
                key: column.attributes.has_attribute("key"),
            })
        })
        .collect::<Vec<_>>();

    Some((
        hashed_keys,
        keys,
        proto::types::Struct {
            name: model_label(table),
            children,
        },
    ))
}

fn extract_model_keys(model: &proto::types::Struct) -> Vec<Felt> {
    model
        .children
        .iter()
        .filter(|member| member.key)
        .filter_map(|member| member.ty.as_ref())
        .filter_map(primitive_to_felt)
        .collect()
}

fn primitive_to_felt(ty: &proto::types::Ty) -> Option<Felt> {
    let primitive = match &ty.ty_type {
        Some(proto::types::ty::TyType::Primitive(primitive)) => primitive,
        _ => return None,
    };

    match primitive.primitive_type.as_ref()? {
        proto::types::primitive::PrimitiveType::Felt252(bytes)
        | proto::types::primitive::PrimitiveType::ClassHash(bytes)
        | proto::types::primitive::PrimitiveType::ContractAddress(bytes)
        | proto::types::primitive::PrimitiveType::EthAddress(bytes) => {
            Some(Felt::from_bytes_be_slice(bytes))
        }
        proto::types::primitive::PrimitiveType::U8(value) => Some(Felt::from(*value as u8)),
        proto::types::primitive::PrimitiveType::U16(value) => Some(Felt::from(*value as u16)),
        proto::types::primitive::PrimitiveType::U32(value) => Some(Felt::from(*value)),
        proto::types::primitive::PrimitiveType::U64(value) => Some(Felt::from(*value)),
        proto::types::primitive::PrimitiveType::I8(value) => Some(Felt::from(*value as i8)),
        proto::types::primitive::PrimitiveType::I16(value) => Some(Felt::from(*value as i16)),
        proto::types::primitive::PrimitiveType::I32(value) => Some(Felt::from(*value)),
        proto::types::primitive::PrimitiveType::I64(value) => Some(Felt::from(*value)),
        _ => None,
    }
}

type EntityStream = Pin<Box<dyn Stream<Item = Result<SubscribeEntityResponse, Status>> + Send>>;
type ContractStream =
    Pin<Box<dyn Stream<Item = Result<SubscribeContractsResponse, Status>> + Send>>;
type EventStream = Pin<Box<dyn Stream<Item = Result<SubscribeEventsResponse, Status>> + Send>>;

#[async_trait]
impl World for EcsGrpcService {
    type SubscribeContractsStream = ContractStream;
    type SubscribeEntitiesStream = EntityStream;
    type SubscribeEventMessagesStream = EntityStream;
    type SubscribeEventsStream = EventStream;

    async fn subscribe_contracts(
        &self,
        request: Request<SubscribeContractsRequest>,
    ) -> Result<Response<Self::SubscribeContractsStream>, Status> {
        let query = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("missing contract query"))?;
        let initial = self.fetch_contracts(&query).await;
        let rx = self.contract_manager.add_subscriber(query, initial).await;
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn worlds(
        &self,
        request: Request<WorldsRequest>,
    ) -> Result<Response<WorldsResponse>, Status> {
        let requested_worlds = request
            .into_inner()
            .world_addresses
            .iter()
            .map(|bytes| Felt::from_bytes_be_slice(bytes))
            .collect::<Vec<_>>();
        let worlds = build_worlds_response(&self.state.metadata.read(), &requested_worlds);
        Ok(Response::new(WorldsResponse { worlds }))
    }

    async fn subscribe_entities(
        &self,
        request: Request<SubscribeEntitiesRequest>,
    ) -> Result<Response<Self::SubscribeEntitiesStream>, Status> {
        let request = request.into_inner();
        let world_addresses = request
            .world_addresses
            .iter()
            .map(|bytes| Felt::from_bytes_be_slice(bytes))
            .collect();
        let rx = self
            .entity_manager
            .add_subscriber(request.clause, world_addresses)
            .await;
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn update_entities_subscription(
        &self,
        request: Request<UpdateEntitiesSubscriptionRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        let world_addresses = request
            .world_addresses
            .iter()
            .map(|bytes| Felt::from_bytes_be_slice(bytes))
            .collect();
        self.entity_manager.update_subscriber(
            request.subscription_id,
            request.clause,
            world_addresses,
        );
        Ok(Response::new(()))
    }

    async fn retrieve_entities(
        &self,
        request: Request<RetrieveEntitiesRequest>,
    ) -> Result<Response<RetrieveEntitiesResponse>, Status> {
        let query = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("missing entity query"))?;
        let entities = self
            .fetch_entities(TableKind::Model, &query)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(RetrieveEntitiesResponse {
            next_cursor: String::new(),
            entities,
        }))
    }

    async fn subscribe_event_messages(
        &self,
        request: Request<SubscribeEntitiesRequest>,
    ) -> Result<Response<Self::SubscribeEventMessagesStream>, Status> {
        let request = request.into_inner();
        let world_addresses = request
            .world_addresses
            .iter()
            .map(|bytes| Felt::from_bytes_be_slice(bytes))
            .collect();
        let rx = self
            .event_message_manager
            .add_subscriber(request.clause, world_addresses)
            .await;
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn update_event_messages_subscription(
        &self,
        request: Request<UpdateEntitiesSubscriptionRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        let world_addresses = request
            .world_addresses
            .iter()
            .map(|bytes| Felt::from_bytes_be_slice(bytes))
            .collect();
        self.event_message_manager.update_subscriber(
            request.subscription_id,
            request.clause,
            world_addresses,
        );
        Ok(Response::new(()))
    }

    async fn retrieve_event_messages(
        &self,
        request: Request<RetrieveEntitiesRequest>,
    ) -> Result<Response<RetrieveEntitiesResponse>, Status> {
        let query = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("missing event-message query"))?;
        let entities = self
            .fetch_entities(TableKind::EventMessage, &query)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(RetrieveEntitiesResponse {
            next_cursor: String::new(),
            entities,
        }))
    }

    async fn retrieve_events(
        &self,
        request: Request<RetrieveEventsRequest>,
    ) -> Result<Response<RetrieveEventsResponse>, Status> {
        let limit = request
            .into_inner()
            .query
            .and_then(|query| query.pagination)
            .map(|pagination| pagination.limit)
            .filter(|limit| *limit > 0)
            .unwrap_or(100) as usize;
        let events = load_events(&self.state.pool, limit)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(RetrieveEventsResponse {
            next_cursor: String::new(),
            events,
        }))
    }

    async fn subscribe_events(
        &self,
        request: Request<SubscribeEventsRequest>,
    ) -> Result<Response<Self::SubscribeEventsStream>, Status> {
        let rx = self
            .event_manager
            .add_subscriber(request.into_inner().keys)
            .await;
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

fn match_entity_clause(
    hashed_keys: Felt,
    keys: &[Felt],
    models: &[proto::types::Struct],
    clause: &proto::types::Clause,
) -> bool {
    match clause.clause_type.as_ref() {
        Some(proto::types::clause::ClauseType::HashedKeys(inner)) => {
            inner.hashed_keys.is_empty()
                || inner
                    .hashed_keys
                    .iter()
                    .any(|value| Felt::from_bytes_be_slice(value) == hashed_keys)
        }
        Some(proto::types::clause::ClauseType::Keys(inner)) => {
            if !inner.models.is_empty()
                && !models.iter().any(|model| {
                    inner
                        .models
                        .iter()
                        .any(|requested| requested == &model.name)
                })
            {
                return false;
            }

            if inner.pattern_matching == proto::types::PatternMatching::FixedLen as i32
                && inner.keys.len() != keys.len()
            {
                return false;
            }

            keys.iter().enumerate().all(|(index, key)| {
                inner.keys.get(index).map_or(true, |expected| {
                    expected.is_empty() || Felt::from_bytes_be_slice(expected) == *key
                })
            })
        }
        Some(proto::types::clause::ClauseType::Member(inner)) => match_member_clause(models, inner),
        Some(proto::types::clause::ClauseType::Composite(inner)) => {
            if inner.operator == proto::types::LogicalOperator::And as i32 {
                inner
                    .clauses
                    .iter()
                    .all(|clause| match_entity_clause(hashed_keys, keys, models, clause))
            } else {
                inner
                    .clauses
                    .iter()
                    .any(|clause| match_entity_clause(hashed_keys, keys, models, clause))
            }
        }
        None => true,
    }
}

fn match_member_clause(
    models: &[proto::types::Struct],
    clause: &proto::types::MemberClause,
) -> bool {
    let Some(model) = models.iter().find(|model| model.name == clause.model) else {
        return false;
    };

    let Some(member) = model
        .children
        .iter()
        .find(|member| member.name == clause.member)
    else {
        return false;
    };

    match (member.ty.as_ref(), clause.value.as_ref()) {
        (Some(ty), Some(value)) => compare_member_value(ty, clause.operator, value),
        _ => false,
    }
}

fn compare_member_value(
    ty: &proto::types::Ty,
    operator: i32,
    value: &proto::types::MemberValue,
) -> bool {
    let primitive = match &ty.ty_type {
        Some(proto::types::ty::TyType::Primitive(primitive)) => primitive,
        Some(proto::types::ty::TyType::Bytearray(text)) => {
            return compare_string(text, operator, value);
        }
        _ => return false,
    };

    match primitive.primitive_type.as_ref() {
        Some(proto::types::primitive::PrimitiveType::Bool(boolean)) => {
            compare_string(&boolean.to_string(), operator, value)
        }
        Some(proto::types::primitive::PrimitiveType::U8(number)) => {
            compare_number(*number as i128, operator, value)
        }
        Some(proto::types::primitive::PrimitiveType::U16(number)) => {
            compare_number(*number as i128, operator, value)
        }
        Some(proto::types::primitive::PrimitiveType::U32(number)) => {
            compare_number(*number as i128, operator, value)
        }
        Some(proto::types::primitive::PrimitiveType::U64(number)) => {
            compare_number(*number as i128, operator, value)
        }
        Some(proto::types::primitive::PrimitiveType::I8(number)) => {
            compare_number(*number as i128, operator, value)
        }
        Some(proto::types::primitive::PrimitiveType::I16(number)) => {
            compare_number(*number as i128, operator, value)
        }
        Some(proto::types::primitive::PrimitiveType::I32(number)) => {
            compare_number(*number as i128, operator, value)
        }
        Some(proto::types::primitive::PrimitiveType::I64(number)) => {
            compare_number(*number as i128, operator, value)
        }
        Some(proto::types::primitive::PrimitiveType::Felt252(bytes))
        | Some(proto::types::primitive::PrimitiveType::ClassHash(bytes))
        | Some(proto::types::primitive::PrimitiveType::ContractAddress(bytes))
        | Some(proto::types::primitive::PrimitiveType::EthAddress(bytes)) => compare_string(
            &format!("{:#x}", Felt::from_bytes_be_slice(bytes)),
            operator,
            value,
        ),
        _ => false,
    }
}

fn compare_string(current: &str, operator: i32, value: &proto::types::MemberValue) -> bool {
    match value.value_type.as_ref() {
        Some(proto::types::member_value::ValueType::String(expected)) => match operator {
            x if x == proto::types::ComparisonOperator::Eq as i32 => current == expected,
            x if x == proto::types::ComparisonOperator::Neq as i32 => current != expected,
            x if x == proto::types::ComparisonOperator::Gt as i32 => current > expected.as_str(),
            x if x == proto::types::ComparisonOperator::Gte as i32 => current >= expected.as_str(),
            x if x == proto::types::ComparisonOperator::Lt as i32 => current < expected.as_str(),
            x if x == proto::types::ComparisonOperator::Lte as i32 => current <= expected.as_str(),
            x if x == proto::types::ComparisonOperator::In as i32 => false,
            x if x == proto::types::ComparisonOperator::NotIn as i32 => false,
            _ => false,
        },
        Some(proto::types::member_value::ValueType::List(list)) => match operator {
            x if x == proto::types::ComparisonOperator::In as i32 => list
                .values
                .iter()
                .filter_map(|value| match value.value_type.as_ref() {
                    Some(proto::types::member_value::ValueType::String(item)) => Some(item),
                    _ => None,
                })
                .any(|item| item == current),
            x if x == proto::types::ComparisonOperator::NotIn as i32 => !list
                .values
                .iter()
                .filter_map(|value| match value.value_type.as_ref() {
                    Some(proto::types::member_value::ValueType::String(item)) => Some(item),
                    _ => None,
                })
                .any(|item| item == current),
            _ => false,
        },
        _ => false,
    }
}

fn compare_number(current: i128, operator: i32, value: &proto::types::MemberValue) -> bool {
    let expected = match value.value_type.as_ref() {
        Some(proto::types::member_value::ValueType::Primitive(primitive)) => {
            let Some(expected) = primitive.primitive_type.as_ref() else {
                return false;
            };
            match expected {
                proto::types::primitive::PrimitiveType::U8(value) => *value as i128,
                proto::types::primitive::PrimitiveType::U16(value) => *value as i128,
                proto::types::primitive::PrimitiveType::U32(value) => *value as i128,
                proto::types::primitive::PrimitiveType::U64(value) => *value as i128,
                proto::types::primitive::PrimitiveType::I8(value) => *value as i128,
                proto::types::primitive::PrimitiveType::I16(value) => *value as i128,
                proto::types::primitive::PrimitiveType::I32(value) => *value as i128,
                proto::types::primitive::PrimitiveType::I64(value) => *value as i128,
                _ => return false,
            }
        }
        _ => return false,
    };

    match operator {
        x if x == proto::types::ComparisonOperator::Eq as i32 => current == expected,
        x if x == proto::types::ComparisonOperator::Neq as i32 => current != expected,
        x if x == proto::types::ComparisonOperator::Gt as i32 => current > expected,
        x if x == proto::types::ComparisonOperator::Gte as i32 => current >= expected,
        x if x == proto::types::ComparisonOperator::Lt as i32 => current < expected,
        x if x == proto::types::ComparisonOperator::Lte as i32 => current <= expected,
        _ => false,
    }
}

fn match_keys_clause(keys: &[Felt], clauses: &[proto::types::KeysClause]) -> bool {
    if clauses.is_empty() {
        return true;
    }

    clauses.iter().any(|clause| {
        if clause.pattern_matching == proto::types::PatternMatching::FixedLen as i32
            && clause.keys.len() != keys.len()
        {
            return false;
        }

        keys.iter().enumerate().all(|(index, key)| {
            clause.keys.get(index).map_or(true, |expected| {
                expected.is_empty() || Felt::from_bytes_be_slice(expected) == *key
            })
        })
    })
}
