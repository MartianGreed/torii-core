use anyhow::Context;
use chrono::{DateTime, Utc};
use introspect_types::{Attributes, TypeDef};
use itertools::Itertools;
use serde_json::{Map, Value};
use sqlx::Row;
use starknet::core::types::{EmittedEvent, Felt};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};
use torii::etl::extractor::ExtractionBatch;
use torii_dojo::event::{EVENT_MESSAGE_KIND_ATTRIBUTE, MODEL_KIND_ATTRIBUTE};
use torii_dojo::DojoTable;

use crate::proto;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableKind {
    Model,
    EventMessage,
}

#[derive(Clone, Debug)]
pub struct ManagedTable {
    pub world_address: Felt,
    pub table: DojoTable,
    pub kind: TableKind,
}

#[derive(Clone, Debug, Default)]
pub struct MetadataCache {
    pub tables: HashMap<Felt, ManagedTable>,
}

impl MetadataCache {
    pub fn world_addresses(&self) -> Vec<Felt> {
        self.tables
            .values()
            .map(|table| table.world_address)
            .unique()
            .collect()
    }

    pub fn tables_for_kind(
        &self,
        kind: TableKind,
        requested_worlds: &[Felt],
        requested_models: &[String],
    ) -> Vec<ManagedTable> {
        self.tables
            .values()
            .filter(|table| table.kind == kind)
            .filter(|table| {
                requested_worlds.is_empty() || requested_worlds.contains(&table.world_address)
            })
            .filter(|table| {
                requested_models.is_empty()
                    || requested_models
                        .iter()
                        .any(|model| model == &table.table.name || model == &model_label(table))
            })
            .cloned()
            .collect()
    }
}

#[derive(Clone, Debug, Default)]
pub struct SharedMetadata(pub Arc<RwLock<MetadataCache>>);

impl SharedMetadata {
    pub fn new(metadata: MetadataCache) -> Self {
        Self(Arc::new(RwLock::new(metadata)))
    }

    pub fn read(&self) -> std::sync::RwLockReadGuard<'_, MetadataCache> {
        self.0.read().unwrap()
    }

    pub fn upsert_table(&self, world_address: Felt, table: DojoTable) {
        let kind = if table
            .attributes
            .iter()
            .any(|attr| attr == EVENT_MESSAGE_KIND_ATTRIBUTE)
        {
            TableKind::EventMessage
        } else {
            TableKind::Model
        };

        self.0.write().unwrap().tables.insert(
            table.id,
            ManagedTable {
                world_address,
                table,
                kind,
            },
        );
    }
}

#[derive(Clone, Debug)]
pub struct ContractState {
    pub contract_address: Felt,
    pub head: Option<u64>,
    pub last_block_timestamp: Option<u64>,
    pub updated_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

pub type SharedContracts = Arc<RwLock<HashMap<Felt, ContractState>>>;

pub async fn initialize_storage(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    sqlx::query("CREATE SCHEMA IF NOT EXISTS ecs_sink")
        .execute(pool)
        .await?;
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS ecs_sink.events (
            id BIGSERIAL PRIMARY KEY,
            from_address BYTEA NOT NULL,
            transaction_hash BYTEA NOT NULL,
            keys BYTEA[] NOT NULL,
            data BYTEA[] NOT NULL,
            block_number BIGINT,
            block_timestamp BIGINT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn persist_events_batch(
    pool: &sqlx::PgPool,
    batch: &ExtractionBatch,
) -> anyhow::Result<()> {
    for event in &batch.events {
        sqlx::query(
            r#"
            INSERT INTO ecs_sink.events
                (from_address, transaction_hash, keys, data, block_number, block_timestamp)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(event.from_address.to_bytes_be().to_vec())
        .bind(event.transaction_hash.to_bytes_be().to_vec())
        .bind(
            event
                .keys
                .iter()
                .map(|key| key.to_bytes_be().to_vec())
                .collect::<Vec<_>>(),
        )
        .bind(
            event
                .data
                .iter()
                .map(|value| value.to_bytes_be().to_vec())
                .collect::<Vec<_>>(),
        )
        .bind(event.block_number.map(|value| value as i64))
        .bind(
            batch
                .get_event_context(&event.transaction_hash, event.from_address)
                .map(|context| context.block.timestamp as i64),
        )
        .execute(pool)
        .await?;
    }

    Ok(())
}

pub async fn refresh_metadata(pool: &sqlx::PgPool) -> anyhow::Result<MetadataCache> {
    let mut metadata = MetadataCache::default();
    let rows = sqlx::query(
        r#"
        SELECT owner, table_json
        FROM torii_dojo_manager_state
        ORDER BY updated_at ASC
        "#,
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();

    for row in rows {
        let owner: Vec<u8> = row.try_get("owner")?;
        let table_json: String = row.try_get("table_json")?;
        let table: DojoTable = serde_json::from_str(&table_json)?;
        metadata.tables.insert(
            table.id,
            ManagedTable {
                world_address: Felt::from_bytes_be_slice(&owner),
                kind: table_kind(&table),
                table,
            },
        );
    }

    Ok(metadata)
}

pub async fn bootstrap_contract_state(
    pool: &sqlx::PgPool,
    metadata: &SharedMetadata,
) -> anyhow::Result<SharedContracts> {
    let mut head = None;
    if let Ok(Some(value)) = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT block_number
        FROM engine.head
        ORDER BY updated_at DESC
        LIMIT 1
        "#,
    )
    .fetch_optional(pool)
    .await
    {
        head = Some(value as u64);
    }

    let now = Utc::now();
    let contracts = metadata
        .read()
        .world_addresses()
        .into_iter()
        .map(|world_address| {
            (
                world_address,
                ContractState {
                    contract_address: world_address,
                    head,
                    last_block_timestamp: None,
                    updated_at: now,
                    created_at: now,
                },
            )
        })
        .collect();

    Ok(Arc::new(RwLock::new(contracts)))
}

pub fn table_kind(table: &DojoTable) -> TableKind {
    if table
        .attributes
        .iter()
        .any(|attribute| attribute == EVENT_MESSAGE_KIND_ATTRIBUTE)
    {
        TableKind::EventMessage
    } else if table
        .attributes
        .iter()
        .any(|attribute| attribute == MODEL_KIND_ATTRIBUTE)
    {
        TableKind::Model
    } else {
        TableKind::Model
    }
}

pub fn model_label(table: &ManagedTable) -> String {
    let (namespace, name) = split_model_name(&table.table.name);
    format!("{namespace}-{name}")
}

pub fn split_model_name(name: &str) -> (String, String) {
    name.split_once('-')
        .map(|(namespace, name)| (namespace.to_string(), name.to_string()))
        .unwrap_or_else(|| ("".to_string(), name.to_string()))
}

pub fn felt_from_json(value: &Value) -> Option<Felt> {
    match value {
        Value::String(text) if text.starts_with("\\x") => {
            let bytes = hex::decode(&text[2..]).ok()?;
            Some(Felt::from_bytes_be_slice(&bytes))
        }
        Value::String(text) if text.starts_with("0x") => Felt::from_hex(text).ok(),
        Value::Number(number) => Some(Felt::from(number.as_u64()?)),
        Value::Bool(boolean) => Some(Felt::from(*boolean as u8)),
        _ => None,
    }
}

pub fn string_from_json(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(boolean) => Some(boolean.to_string()),
        _ => None,
    }
}

pub fn bytes_from_json(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::String(text) if text.starts_with("\\x") => hex::decode(&text[2..]).ok(),
        Value::String(text) if text.starts_with("0x") => hex::decode(&text[2..]).ok(),
        _ => None,
    }
}

pub fn column_value_to_ty(type_def: &TypeDef, value: &Value) -> Option<proto::types::Ty> {
    use introspect_types::TypeDef as Def;
    let ty_type = match type_def {
        Def::Bool => Some(proto::types::ty::TyType::Primitive(
            proto::types::Primitive {
                primitive_type: Some(proto::types::primitive::PrimitiveType::Bool(
                    value.as_bool()?,
                )),
            },
        )),
        Def::U8 => Some(proto::types::ty::TyType::Primitive(
            proto::types::Primitive {
                primitive_type: Some(proto::types::primitive::PrimitiveType::U8(
                    value.as_u64()? as u32
                )),
            },
        )),
        Def::U16 => Some(proto::types::ty::TyType::Primitive(
            proto::types::Primitive {
                primitive_type: Some(proto::types::primitive::PrimitiveType::U16(
                    value.as_u64()? as u32
                )),
            },
        )),
        Def::U32 => Some(proto::types::ty::TyType::Primitive(
            proto::types::Primitive {
                primitive_type: Some(proto::types::primitive::PrimitiveType::U32(
                    value.as_u64()? as u32
                )),
            },
        )),
        Def::U64 => Some(proto::types::ty::TyType::Primitive(
            proto::types::Primitive {
                primitive_type: Some(proto::types::primitive::PrimitiveType::U64(value.as_u64()?)),
            },
        )),
        Def::I8 => Some(proto::types::ty::TyType::Primitive(
            proto::types::Primitive {
                primitive_type: Some(proto::types::primitive::PrimitiveType::I8(
                    value.as_i64()? as i32
                )),
            },
        )),
        Def::I16 => Some(proto::types::ty::TyType::Primitive(
            proto::types::Primitive {
                primitive_type: Some(proto::types::primitive::PrimitiveType::I16(
                    value.as_i64()? as i32
                )),
            },
        )),
        Def::I32 => Some(proto::types::ty::TyType::Primitive(
            proto::types::Primitive {
                primitive_type: Some(proto::types::primitive::PrimitiveType::I32(
                    value.as_i64()? as i32
                )),
            },
        )),
        Def::I64 => Some(proto::types::ty::TyType::Primitive(
            proto::types::Primitive {
                primitive_type: Some(proto::types::primitive::PrimitiveType::I64(value.as_i64()?)),
            },
        )),
        Def::Felt252
        | Def::ClassHash
        | Def::ContractAddress
        | Def::StorageAddress
        | Def::StorageBaseAddress => Some(proto::types::ty::TyType::Primitive(
            proto::types::Primitive {
                primitive_type: Some(proto::types::primitive::PrimitiveType::Felt252(
                    felt_from_json(value)?.to_bytes_be().to_vec(),
                )),
            },
        )),
        Def::EthAddress => Some(proto::types::ty::TyType::Primitive(
            proto::types::Primitive {
                primitive_type: Some(proto::types::primitive::PrimitiveType::EthAddress(
                    bytes_from_json(value)?,
                )),
            },
        )),
        Def::Utf8String | Def::ShortUtf8 => Some(proto::types::ty::TyType::Bytearray(
            string_from_json(value)?,
        )),
        Def::ByteArray | Def::ByteArrayEncoded(_) | Def::Bytes31 | Def::Bytes31Encoded(_) => Some(
            proto::types::ty::TyType::Bytearray(bytes_from_json(value).map(hex::encode)?),
        ),
        Def::Struct(def) => {
            let object = value.as_object()?;
            let children = def
                .members
                .iter()
                .filter_map(|member| {
                    let member_value = object.get(&member.name)?;
                    Some(proto::types::Member {
                        name: member.name.clone(),
                        ty: Some(column_value_to_ty(&member.type_def, member_value)?),
                        key: member.attributes.has_attribute("key"),
                    })
                })
                .collect();
            Some(proto::types::ty::TyType::Struct(proto::types::Struct {
                name: def.name.clone(),
                children,
            }))
        }
        Def::Tuple(def) => Some(proto::types::ty::TyType::Tuple(proto::types::Array {
            children: tuple_or_array_children(&def.elements, value)?,
        })),
        Def::Array(def) => Some(proto::types::ty::TyType::Array(proto::types::Array {
            children: array_children(&def.type_def, value)?,
        })),
        Def::FixedArray(def) => Some(proto::types::ty::TyType::FixedSizeArray(
            proto::types::FixedSizeArray {
                children: array_children(&def.type_def, value)?,
                size: def.size as u32,
            },
        )),
        Def::Enum(def) => {
            let object = value.as_object()?;
            let selected = string_from_json(object.get("variant")?)?;
            let options = def
                .order
                .iter()
                .filter_map(|selector| def.variants.get(selector))
                .map(|variant| {
                    let option_ty = if let Some(inner) = object.get(&format!("_{}", variant.name)) {
                        column_value_to_ty(&variant.type_def, inner)
                    } else {
                        column_value_to_ty(&variant.type_def, &Value::Null)
                    };
                    proto::types::EnumOption {
                        name: variant.name.clone(),
                        ty: option_ty,
                    }
                })
                .collect::<Vec<_>>();
            let selected_index = def
                .order
                .iter()
                .filter_map(|selector| def.variants.get(selector))
                .position(|variant| variant.name == selected)
                .unwrap_or_default() as u32;
            Some(proto::types::ty::TyType::Enum(proto::types::Enum {
                name: def.name.clone(),
                option: selected_index,
                options,
            }))
        }
        Def::Option(def) => {
            if value.is_null() {
                Some(proto::types::ty::TyType::Bytearray(String::new()))
            } else {
                column_value_to_ty(&def.type_def, value).and_then(|ty| ty.ty_type)
            }
        }
        Def::Nullable(def) => {
            if value.is_null() {
                Some(proto::types::ty::TyType::Bytearray(String::new()))
            } else {
                column_value_to_ty(&def.type_def, value).and_then(|ty| ty.ty_type)
            }
        }
        Def::None
        | Def::U128
        | Def::U256
        | Def::U512
        | Def::I128
        | Def::Custom(_)
        | Def::Felt252Dict(_)
        | Def::Result(_)
        | Def::Ref(_) => Some(proto::types::ty::TyType::Bytearray(
            string_from_json(value).unwrap_or_default(),
        )),
    }?;

    Some(proto::types::Ty {
        ty_type: Some(ty_type),
    })
}

fn array_children(type_def: &TypeDef, value: &Value) -> Option<Vec<proto::types::Ty>> {
    let values = value.as_array()?;
    values
        .iter()
        .map(|value| column_value_to_ty(type_def, value))
        .collect()
}

fn tuple_or_array_children(type_defs: &[TypeDef], value: &Value) -> Option<Vec<proto::types::Ty>> {
    let values = value.as_object()?;
    type_defs
        .iter()
        .enumerate()
        .map(|(index, type_def)| column_value_to_ty(type_def, values.get(&format!("_{index}"))?))
        .collect()
}

pub async fn load_table_rows(
    pool: &sqlx::PgPool,
    table_name: &str,
) -> anyhow::Result<Vec<Map<String, Value>>> {
    let sql = format!(r#"SELECT to_jsonb(t) AS row FROM "{table_name}" t"#);
    let rows = sqlx::query(&sql)
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to query table {table_name}"))?;

    rows.into_iter()
        .map(|row| {
            let value: Value = row.try_get("row")?;
            value
                .as_object()
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("row_to_json returned non-object"))
        })
        .collect()
}

pub async fn load_events(
    pool: &sqlx::PgPool,
    limit: usize,
) -> anyhow::Result<Vec<proto::types::Event>> {
    let rows = sqlx::query(
        r#"
        SELECT keys, data, transaction_hash
        FROM ecs_sink.events
        ORDER BY id DESC
        LIMIT $1
        "#,
    )
    .bind(limit as i64)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| proto::types::Event {
            keys: row.try_get::<Vec<Vec<u8>>, _>("keys").unwrap_or_default(),
            data: row.try_get::<Vec<Vec<u8>>, _>("data").unwrap_or_default(),
            transaction_hash: row
                .try_get::<Vec<u8>, _>("transaction_hash")
                .unwrap_or_default(),
        })
        .collect())
}

pub fn update_contract_state(
    contracts: &SharedContracts,
    touched_worlds: impl IntoIterator<Item = Felt>,
    head: Option<u64>,
    block_timestamp: Option<u64>,
) {
    let now = Utc::now();
    let mut contracts = contracts.write().unwrap();
    for world in touched_worlds {
        let created_at = contracts
            .get(&world)
            .map(|state| state.created_at)
            .unwrap_or(now);
        contracts.insert(
            world,
            ContractState {
                contract_address: world,
                head,
                last_block_timestamp: block_timestamp,
                updated_at: now,
                created_at,
            },
        );
    }
}

pub fn contract_to_proto(contract: &ContractState) -> proto::types::Contract {
    proto::types::Contract {
        contract_address: contract.contract_address.to_bytes_be().to_vec(),
        contract_type: proto::types::ContractType::World as i32,
        head: contract.head,
        tps: None,
        last_block_timestamp: contract.last_block_timestamp,
        last_pending_block_tx: None,
        updated_at: contract.updated_at.timestamp() as u64,
        created_at: contract.created_at.timestamp() as u64,
    }
}

pub fn build_worlds_response(
    metadata: &MetadataCache,
    requested_worlds: &[Felt],
) -> Vec<proto::types::World> {
    let mut grouped = BTreeMap::<String, Vec<proto::types::Model>>::new();

    for table in metadata.tables.values() {
        if requested_worlds.is_empty() || requested_worlds.contains(&table.world_address) {
            let (namespace, name) = split_model_name(&table.table.name);
            grouped
                .entry(format!("{:#x}", table.world_address))
                .or_default()
                .push(proto::types::Model {
                    selector: table.table.id.to_bytes_be().to_vec(),
                    namespace,
                    name,
                    packed_size: 0,
                    unpacked_size: 0,
                    class_hash: Vec::new(),
                    layout: Vec::new(),
                    schema: serde_json::to_vec(&table.table.to_schema()).unwrap_or_default(),
                    contract_address: Vec::new(),
                    use_legacy_store: table.table.legacy,
                    world_address: table.world_address.to_bytes_be().to_vec(),
                });
        }
    }

    grouped
        .into_iter()
        .map(|(world_address, models)| proto::types::World {
            world_address,
            models,
        })
        .collect()
}

pub fn event_from_emitted(event: &EmittedEvent) -> proto::types::Event {
    proto::types::Event {
        keys: event
            .keys
            .iter()
            .map(|key| key.to_bytes_be().to_vec())
            .collect(),
        data: event
            .data
            .iter()
            .map(|value| value.to_bytes_be().to_vec())
            .collect(),
        transaction_hash: event.transaction_hash.to_bytes_be().to_vec(),
    }
}
