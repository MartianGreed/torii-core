use crate::manager::DojoTableManager;
use crate::{DojoTable, DojoToriiError, DojoToriiResult};
pub use anyhow::Result as AnyResult;
use async_trait::async_trait;
use dojo_introspect::events::{
    DojoEvent, EventEmitted, EventRegistered, EventUpgraded, ModelRegistered, ModelUpgraded,
    ModelWithSchemaRegistered, StoreDelRecord, StoreSetRecord, StoreUpdateMember,
    StoreUpdateRecord,
};
use dojo_introspect::DojoSchemaFetcher;
use introspect_types::{
    CairoEvent, CairoEventInfo, CairoSerde, FeltIds, IntoFeltSource, PrimaryDef, PrimaryTypeDef,
    ResultInto, SliceFeltSource,
};
use starknet::core::types::EmittedEvent;
use starknet_types_core::felt::Felt;
use std::fmt::Debug;
use torii::etl::event::EmittedEventExt;
use torii::etl::{Decoder, Envelope, EventBody};
use torii_introspect::events::{
    CreateTable, DeleteRecords, InsertsFields, IntrospectMsg, UpdateTable,
};
use torii_introspect::EventId;

pub struct DojoDecoder<M, F> {
    pub manager: M,
    pub fetcher: F,
    pub primary_field: PrimaryDef,
}
#[async_trait]
pub trait DojoEventProcessor<M: Sync, F: Sync>: Sized + CairoEventInfo + Debug {
    type Msg: EventId;
    async fn event_to_msg(self, decoder: &DojoDecoder<M, F>) -> DojoToriiResult<Self::Msg>;
    fn deserialize_data<'a>(keys: &[Felt], data: &'a [Felt]) -> DojoToriiResult<Self>
    where
        Self: CairoEvent<CairoSerde<SliceFeltSource<'a>>>,
    {
        let mut keys = keys.into_source();
        let mut data: CairoSerde<_> = data.into();
        match Self::deserialize_and_verify_event(&mut keys, &mut data) {
            Ok(event) => Ok(event),
            Err(err) => Err(DojoToriiError::EventDeserializationError(Self::NAME, err)),
        }
    }
    async fn data_to_msg<'a>(
        decoder: &DojoDecoder<M, F>,
        keys: &[Felt],
        data: &'a [Felt],
    ) -> DojoToriiResult<Self::Msg>
    where
        Self: CairoEvent<CairoSerde<SliceFeltSource<'a>>>,
        Self::Msg: Into<IntrospectMsg>,
    {
        Self::deserialize_data(keys, data)?
            .event_to_msg(decoder)
            .await
    }
}

#[async_trait]
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventProcessor<M, F>
    for ModelWithSchemaRegistered
{
    type Msg = CreateTable;
    async fn event_to_msg(self, decoder: &DojoDecoder<M, F>) -> DojoToriiResult<Self::Msg> {
        decoder
            .manager
            .register_table(&self.namespace, &self.name, self.schema)
            .map(Into::into)
    }
}

#[async_trait]
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventProcessor<M, F>
    for ModelRegistered
{
    type Msg = CreateTable;
    async fn event_to_msg(self, decoder: &DojoDecoder<M, F>) -> DojoToriiResult<Self::Msg> {
        let schema = decoder.fetcher.schema(self.address).await?;
        decoder
            .manager
            .register_table(&self.namespace, &self.name, schema)
            .map(Into::into)
    }
}

#[async_trait]
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventProcessor<M, F>
    for EventRegistered
{
    type Msg = CreateTable;
    async fn event_to_msg(self, decoder: &DojoDecoder<M, F>) -> DojoToriiResult<Self::Msg> {
        let schema = decoder.fetcher.schema(self.address).await?;
        decoder
            .manager
            .register_table(&self.namespace, &self.name, schema)
            .map(Into::into)
    }
}

#[async_trait]
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventProcessor<M, F>
    for ModelUpgraded
{
    type Msg = UpdateTable;
    async fn event_to_msg(self, decoder: &DojoDecoder<M, F>) -> DojoToriiResult<Self::Msg> {
        let schema = decoder.fetcher.schema(self.address).await?;
        decoder
            .manager
            .update_table(self.selector, schema)
            .map(Into::into)
    }
}

#[async_trait]
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventProcessor<M, F>
    for EventUpgraded
{
    type Msg = UpdateTable;
    async fn event_to_msg(self, decoder: &DojoDecoder<M, F>) -> DojoToriiResult<Self::Msg> {
        let schema = decoder.fetcher.schema(self.address).await?;
        decoder
            .manager
            .update_table(self.selector, schema)
            .map(Into::into)
    }
}

#[async_trait]
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventProcessor<M, F>
    for StoreSetRecord
{
    type Msg = InsertsFields;
    async fn event_to_msg(self, decoder: &DojoDecoder<M, F>) -> DojoToriiResult<Self::Msg> {
        let (columns, data) = decoder.with_table(self.selector, |table| {
            table.parse_record(self.keys, self.values)
        })?;
        Ok(InsertsFields::new_single(
            self.selector,
            columns,
            self.entity_id,
            data,
        ))
    }
}

#[async_trait]
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventProcessor<M, F>
    for StoreUpdateRecord
{
    type Msg = InsertsFields;
    async fn event_to_msg(self, decoder: &DojoDecoder<M, F>) -> DojoToriiResult<Self::Msg> {
        let (columns, data) =
            decoder.with_table(self.selector, |table| table.parse_values(self.values))?;
        Ok(InsertsFields::new_single(
            self.selector,
            columns,
            self.entity_id,
            data,
        ))
    }
}

#[async_trait]
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventProcessor<M, F>
    for EventEmitted
{
    type Msg = InsertsFields;
    async fn event_to_msg(self, decoder: &DojoDecoder<M, F>) -> DojoToriiResult<Self::Msg> {
        let primary = Felt::from_bytes_be(&self.keys.hash().into());
        let (columns, data) = decoder.with_table(self.selector, |table| {
            table.parse_record(self.keys, self.values)
        })?;
        Ok(InsertsFields::new_single(
            self.selector,
            columns,
            primary,
            data,
        ))
    }
}

#[async_trait]
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventProcessor<M, F>
    for StoreUpdateMember
{
    type Msg = InsertsFields;
    async fn event_to_msg(self, decoder: &DojoDecoder<M, F>) -> DojoToriiResult<Self::Msg> {
        let member = self.member_selector;
        let data = decoder.with_table(self.selector, |table| {
            table.parse_field(member, self.values)
        })?;
        Ok(InsertsFields::new_single(
            self.selector,
            vec![member],
            self.entity_id,
            data,
        ))
    }
}

#[async_trait]
impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoEventProcessor<M, F>
    for StoreDelRecord
{
    type Msg = DeleteRecords;
    async fn event_to_msg(self, _decoder: &DojoDecoder<M, F>) -> DojoToriiResult<Self::Msg> {
        Ok(DeleteRecords::new(
            self.selector,
            vec![self.entity_id.into()],
        ))
    }
}

impl<M: DojoTableManager + Sync, F: DojoSchemaFetcher + Sync> DojoDecoder<M, F> {
    pub fn new(manager: M, fetcher: F, primary_field: PrimaryDef) -> Self {
        Self {
            manager,
            fetcher,
            primary_field,
        }
    }

    pub fn new_default(manager: M, fetcher: F) -> Self {
        Self::new(
            manager,
            fetcher,
            PrimaryDef {
                name: "__id".to_string(),
                attributes: vec![],
                type_def: PrimaryTypeDef::Felt252,
            },
        )
    }

    async fn to_msg<'a, E>(
        &self,
        keys: &'a [Felt],
        values: &'a [Felt],
    ) -> DojoToriiResult<IntrospectMsg>
    where
        E: DojoEventProcessor<M, F> + CairoEvent<CairoSerde<SliceFeltSource<'a>>> + Send,
        E::Msg: Into<IntrospectMsg>,
    {
        E::data_to_msg(self, keys, values).await.ok_into()
    }
    fn with_table<Fn, R>(&self, id: Felt, f: Fn) -> DojoToriiResult<R>
    where
        Fn: FnOnce(&DojoTable) -> DojoToriiResult<R>,
    {
        self.manager.with_table(id, f)?
    }

    pub fn to_event<'a, E>(&self, keys: &'a [Felt], values: &'a [Felt]) -> DojoToriiResult<E>
    where
        E: DojoEventProcessor<M, F> + CairoEvent<CairoSerde<SliceFeltSource<'a>>> + Send,
    {
        E::deserialize_data(keys, values)
    }

    pub fn deserialize_dojo_event<'a>(
        &self,
        selector: &Felt,
        keys: &'a [Felt],
        values: &'a [Felt],
    ) -> DojoToriiResult<DojoEvent> {
        let selector_raw = selector.to_raw();
        match selector_raw {
            ModelRegistered::SELECTOR_RAW => {
                self.to_event::<ModelRegistered>(keys, values).ok_into()
            }
            ModelWithSchemaRegistered::SELECTOR_RAW => self
                .to_event::<ModelWithSchemaRegistered>(keys, values)
                .ok_into(),
            ModelUpgraded::SELECTOR_RAW => self.to_event::<ModelUpgraded>(keys, values).ok_into(),
            EventRegistered::SELECTOR_RAW => {
                self.to_event::<EventRegistered>(keys, values).ok_into()
            }
            EventUpgraded::SELECTOR_RAW => self.to_event::<EventUpgraded>(keys, values).ok_into(),
            StoreSetRecord::SELECTOR_RAW => self.to_event::<StoreSetRecord>(keys, values).ok_into(),
            StoreUpdateRecord::SELECTOR_RAW => {
                self.to_event::<StoreUpdateRecord>(keys, values).ok_into()
            }
            StoreUpdateMember::SELECTOR_RAW => {
                self.to_event::<StoreUpdateMember>(keys, values).ok_into()
            }
            StoreDelRecord::SELECTOR_RAW => self.to_event::<StoreDelRecord>(keys, values).ok_into(),
            EventEmitted::SELECTOR_RAW => self.to_event::<EventEmitted>(keys, values).ok_into(),
            _ => Err(DojoToriiError::UnknownDojoEventSelector(*selector)),
        }
    }

    pub async fn decode_event_data(
        &self,
        selector: &Felt,
        keys: &[Felt],
        values: &[Felt],
    ) -> DojoToriiResult<IntrospectMsg> {
        let selector_raw = selector.to_raw();
        match selector_raw {
            ModelRegistered::SELECTOR_RAW => self.to_msg::<ModelRegistered>(keys, values).await,
            ModelWithSchemaRegistered::SELECTOR_RAW => {
                self.to_msg::<ModelWithSchemaRegistered>(keys, values).await
            }
            ModelUpgraded::SELECTOR_RAW => self.to_msg::<ModelUpgraded>(keys, values).await,
            EventRegistered::SELECTOR_RAW => self.to_msg::<EventRegistered>(keys, values).await,
            EventUpgraded::SELECTOR_RAW => self.to_msg::<EventUpgraded>(keys, values).await,
            StoreSetRecord::SELECTOR_RAW => self.to_msg::<StoreSetRecord>(keys, values).await,
            StoreUpdateRecord::SELECTOR_RAW => self.to_msg::<StoreUpdateRecord>(keys, values).await,
            StoreUpdateMember::SELECTOR_RAW => self.to_msg::<StoreUpdateMember>(keys, values).await,
            StoreDelRecord::SELECTOR_RAW => self.to_msg::<StoreDelRecord>(keys, values).await,
            EventEmitted::SELECTOR_RAW => self.to_msg::<EventEmitted>(keys, values).await,
            _ => Err(DojoToriiError::UnknownDojoEventSelector(*selector)),
        }
    }

    pub async fn decode_raw_event(&self, raw: &EmittedEvent) -> DojoToriiResult<IntrospectMsg> {
        let (selector, keys, values) = raw
            .split_content()
            .ok_or(DojoToriiError::MissingEventSelector)?;
        self.decode_event_data(selector, keys, values).await
    }
}

#[async_trait]
impl<M: DojoTableManager + Sync + Send, F: DojoSchemaFetcher + Sync + Send> Decoder
    for DojoDecoder<M, F>
{
    fn decoder_name(&self) -> &'static str {
        "dojo-introspect"
    }

    async fn decode_event(&self, event: &EmittedEvent) -> AnyResult<Vec<Envelope>> {
        let (selector, keys, data) = event
            .split_content()
            .ok_or(DojoToriiError::MissingEventSelector)?;

        match self.decode_event_data(selector, keys, data).await {
            Ok(msg) => Ok(vec![EventBody::new_envelope(msg, event)]),
            Err(DojoToriiError::UnknownDojoEventSelector(_)) => Ok(Vec::new()),
            Err(err) => Err(err.into()),
        }
    }
}
