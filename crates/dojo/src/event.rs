use crate::decoder::{DojoDecoder, DojoRecordEvent, DojoTableEvent};
use crate::store::DojoStoreTrait;
use crate::DojoToriiResult;
pub use anyhow::Result as AnyResult;
use async_trait::async_trait;
use dojo_introspect::events::{
    EventEmitted, EventRegistered, EventUpgraded, ModelRegistered, ModelUpgraded,
    ModelWithSchemaRegistered, StoreDelRecord, StoreSetRecord, StoreUpdateMember,
    StoreUpdateRecord,
};
use dojo_introspect::DojoSchemaFetcher;
use introspect_types::{Attribute, FeltIds};
use starknet_types_core::felt::Felt;
use torii_introspect::events::{CreateTable, DeleteRecords, InsertsFields, UpdateTable};

pub const MODEL_KIND_ATTRIBUTE: &str = "torii_kind_model";
pub const EVENT_MESSAGE_KIND_ATTRIBUTE: &str = "torii_kind_event_message";

fn ensure_kind_attribute(schema: &mut dojo_introspect::DojoSchema, attribute: &str) {
    if !schema.attributes.iter().any(|attr| attr.name == attribute) {
        schema
            .attributes
            .push(Attribute::new_empty(attribute.to_string()));
    }
}

#[async_trait]
impl<Store, F> DojoTableEvent<Store, F> for ModelWithSchemaRegistered
where
    Store: DojoStoreTrait + Sync + Send,
    F: DojoSchemaFetcher + Sync + Send + 'static,
{
    type Msg = CreateTable;
    async fn event_to_msg(
        self,
        owner: &Felt,
        block_number: Option<u64>,
        decoder: &DojoDecoder<Store, F>,
    ) -> DojoToriiResult<Self::Msg> {
        let mut schema = self.schema;
        ensure_kind_attribute(&mut schema, MODEL_KIND_ATTRIBUTE);

        decoder
            .register_table(owner, &self.namespace, &self.name, schema, block_number)
            .await
            .map(Into::into)
    }
}

#[async_trait]
impl<Store, F> DojoTableEvent<Store, F> for ModelRegistered
where
    Store: DojoStoreTrait + Sync + Send,
    F: DojoSchemaFetcher + Sync + Send + 'static,
{
    type Msg = CreateTable;
    async fn event_to_msg(
        self,
        owner: &Felt,
        block_number: Option<u64>,
        decoder: &DojoDecoder<Store, F>,
    ) -> DojoToriiResult<Self::Msg> {
        let mut schema = decoder.fetcher.schema(self.address).await?;
        ensure_kind_attribute(&mut schema, MODEL_KIND_ATTRIBUTE);
        decoder
            .register_table(owner, &self.namespace, &self.name, schema, block_number)
            .await
            .map(Into::into)
    }
}

#[async_trait]
impl<Store, F> DojoTableEvent<Store, F> for EventRegistered
where
    Store: DojoStoreTrait + Sync + Send,
    F: DojoSchemaFetcher + Sync + Send + 'static,
{
    type Msg = CreateTable;
    async fn event_to_msg(
        self,
        owner: &Felt,
        block_number: Option<u64>,
        decoder: &DojoDecoder<Store, F>,
    ) -> DojoToriiResult<Self::Msg> {
        let mut schema = decoder.fetcher.schema(self.address).await?;
        ensure_kind_attribute(&mut schema, EVENT_MESSAGE_KIND_ATTRIBUTE);
        decoder
            .register_table(owner, &self.namespace, &self.name, schema, block_number)
            .await
            .map(Into::into)
    }
}

#[async_trait]
impl<Store, F> DojoTableEvent<Store, F> for ModelUpgraded
where
    Store: DojoStoreTrait + Sync + Send,
    F: DojoSchemaFetcher + Sync + Send + 'static,
{
    type Msg = UpdateTable;
    async fn event_to_msg(
        self,
        owner: &Felt,
        block_number: Option<u64>,
        decoder: &DojoDecoder<Store, F>,
    ) -> DojoToriiResult<Self::Msg> {
        let mut schema = decoder.fetcher.schema(self.address).await?;
        ensure_kind_attribute(&mut schema, MODEL_KIND_ATTRIBUTE);
        decoder
            .update_table(owner, self.selector, schema, block_number)
            .await
            .map(Into::into)
    }
}

#[async_trait]
impl<Store, F> DojoTableEvent<Store, F> for EventUpgraded
where
    Store: DojoStoreTrait + Sync + Send,
    F: DojoSchemaFetcher + Sync + Send + 'static,
{
    type Msg = UpdateTable;
    async fn event_to_msg(
        self,
        owner: &Felt,
        block_number: Option<u64>,
        decoder: &DojoDecoder<Store, F>,
    ) -> DojoToriiResult<Self::Msg> {
        let mut schema = decoder.fetcher.schema(self.address).await?;
        ensure_kind_attribute(&mut schema, EVENT_MESSAGE_KIND_ATTRIBUTE);
        decoder
            .update_table(owner, self.selector, schema, block_number)
            .await
            .map(Into::into)
    }
}

impl<Store, F> DojoRecordEvent<Store, F> for StoreSetRecord {
    type Msg = InsertsFields;
    fn event_to_msg(self, decoder: &DojoDecoder<Store, F>) -> DojoToriiResult<Self::Msg> {
        let (columns, data) = decoder.with_table(&self.selector, |table| {
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

impl<Store, F> DojoRecordEvent<Store, F> for StoreUpdateRecord {
    type Msg = InsertsFields;
    fn event_to_msg(self, decoder: &DojoDecoder<Store, F>) -> DojoToriiResult<Self::Msg> {
        let (columns, data) =
            decoder.with_table(&self.selector, |table| table.parse_values(self.values))?;
        Ok(InsertsFields::new_single(
            self.selector,
            columns,
            self.entity_id,
            data,
        ))
    }
}

impl<Store, F> DojoRecordEvent<Store, F> for EventEmitted {
    type Msg = InsertsFields;
    fn event_to_msg(self, decoder: &DojoDecoder<Store, F>) -> DojoToriiResult<Self::Msg> {
        let primary = Felt::from_bytes_be(&self.keys.hash().into());
        let (columns, data) = decoder.with_table(&self.selector, |table| {
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

impl<Store, F> DojoRecordEvent<Store, F> for StoreUpdateMember {
    type Msg = InsertsFields;
    fn event_to_msg(self, decoder: &DojoDecoder<Store, F>) -> DojoToriiResult<Self::Msg> {
        let data = decoder.with_table(&self.selector, |table| {
            table.parse_field(self.member_selector, self.values)
        })?;
        Ok(InsertsFields::new_single(
            self.selector,
            vec![self.member_selector],
            self.entity_id,
            data,
        ))
    }
}

impl<Store, F> DojoRecordEvent<Store, F> for StoreDelRecord {
    type Msg = DeleteRecords;
    fn event_to_msg(self, _decoder: &DojoDecoder<Store, F>) -> DojoToriiResult<Self::Msg> {
        Ok(DeleteRecords::new(
            self.selector,
            vec![self.entity_id.into()],
        ))
    }
}
