use crate::{DojoTable, DojoToriiError, DojoToriiResult};
use dojo_introspect::serde::dojo_primary_def;
use dojo_introspect::DojoSchema;
use introspect_types::schema::PrimaryInfo;
use introspect_types::{Attributes, PrimaryDef, PrimaryTypeDef, TableSchema};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use std::fs;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::RwLock;
use torii_introspect::CreateTable;

pub const DOJO_ID_FIELD_NAME: &str = "entity_id";

// impl From<serde_json::Error> for DojoToriiErrors<JsonStore> {
//     fn from(err: serde_json::Error) -> Self {
//         DojoToriiErrors::StoreError(err)
//     }
// }

pub struct DojoManagerInner<Store>
where
    Store: Send + Sync,
{
    pub tables: HashMap<Felt, RwLock<DojoTable>>,
    pub store: Store,
}

pub struct DojoTableStore<Store>(pub RwLock<DojoManagerInner<Store>>)
where
    Store: Send + Sync;

impl<Store> Deref for DojoTableStore<Store>
where
    Store: Send + Sync,
{
    type Target = RwLock<DojoManagerInner<Store>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DojoTableStore<JsonStore> {
    /// Create a filesystem-backed Dojo table cache.
    ///
    /// This JSON store is intended for local caching and examples. Each discovered
    /// Dojo table schema is serialized as `<table_id>.json` under the provided
    /// directory so subsequent local runs can inspect or reuse the cached schema
    /// state. For the main indexer path we use `new_postgres(...)` instead.
    pub fn new<P: Into<PathBuf>>(path: P) -> DojoToriiResult<Self> {
        let store = JsonStore::new(&path.into());
        Ok(Self(RwLock::new(DojoManagerInner::new(store)?)))
    }

    /// Create a persistent filesystem-backed Dojo table cache.
    ///
    /// Unlike `new(...)`, this preserves any existing JSON cache entries already
    /// present in the directory.
    pub fn new_persistent<P: Into<PathBuf>>(path: P) -> DojoToriiResult<Self> {
        let store = JsonStore::new_persistent(&path.into());
        Ok(Self(RwLock::new(DojoManagerInner::new(store)?)))
    }
}

impl DojoTableStore<PostgresStore> {
    pub async fn new_postgres(database_url: &str) -> DojoToriiResult<Self> {
        let store = PostgresStore::new(database_url).await?;
        Ok(Self(RwLock::new(DojoManagerInner::new(store)?)))
    }
}

pub fn primary_field_def() -> PrimaryDef {
    PrimaryDef {
        name: DOJO_ID_FIELD_NAME.to_string(),
        attributes: vec![],
        type_def: PrimaryTypeDef::Felt252,
    }
}

pub fn primary_field_info() -> PrimaryInfo {
    PrimaryInfo {
        name: DOJO_ID_FIELD_NAME.to_string(),
        attributes: vec![],
    }
}

impl<Store> DojoManagerInner<Store>
where
    Store: StoreTrait<Table = DojoTable> + Send + Sync + Sized + 'static,
{
    pub fn new(store: Store) -> DojoToriiResult<Self> {
        let tables = store
            .load_all()
            .map_err(|e| DojoToriiError::StoreError(e.to_string()))?
            .into_iter()
            .map(|(id, table)| (id, RwLock::new(table)))
            .collect();
        Ok(Self { tables, store })
    }
}

/// Local filesystem cache for Dojo table schemas.
///
/// The store writes one JSON file per table into a directory such as
/// `data/dojo-manager`. This is useful for local development and examples, while
/// the production indexer path persists the same state in Postgres.
pub struct JsonStore {
    pub path: PathBuf,
}

pub struct PostgresStore {
    pool: PgPool,
}

impl JsonStore {
    pub fn new(path: &PathBuf) -> Self {
        Self::new_with_options(path, true)
    }

    pub fn new_persistent(path: &PathBuf) -> Self {
        Self::new_with_options(path, false)
    }

    fn new_with_options(path: &PathBuf, clean_on_start: bool) -> Self {
        if clean_on_start && path.exists() {
            std::fs::remove_dir_all(path).expect("Unable to clean directory");
        }

        std::fs::create_dir_all(path).expect("Unable to create directory");

        Self {
            path: path.to_path_buf(),
        }
    }
}

impl PostgresStore {
    pub async fn new(database_url: &str) -> DojoToriiResult<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .map_err(|e| DojoToriiError::StoreError(e.to_string()))?;

        let store = Self { pool };
        store
            .initialize()
            .await
            .map_err(|e| DojoToriiError::StoreError(e.to_string()))?;

        Ok(store)
    }

    async fn initialize(&self) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS torii_dojo_manager_state (
                table_id BYTEA PRIMARY KEY,
                table_json TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    fn block_on<F, T>(&self, future: F) -> Result<T, sqlx::Error>
    where
        F: std::future::Future<Output = Result<T, sqlx::Error>>,
    {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
            Err(_) => {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(sqlx::Error::Io)?;
                runtime.block_on(future)
            }
        }
    }
}
pub trait StoreTrait
where
    Self: Send + Sync + 'static + Sized,
{
    type Table;
    type Error: std::error::Error;

    fn dump(&self, table_id: Felt, data: &Self::Table) -> Result<(), Self::Error>;
    fn load(&self, table_id: Felt) -> Result<Self::Table, Self::Error>;
    fn load_all(&self) -> Result<Vec<(Felt, Self::Table)>, Self::Error>;
}

fn felt_to_fixed_hex_string(felt: &Felt) -> String {
    format!("0x{:0>32x}", felt)
}
fn felt_to_json_file_name(felt: &Felt) -> String {
    format!("{}.json", felt_to_fixed_hex_string(felt))
}

fn json_file_name_to_felt(file_name: &str) -> Option<Felt> {
    let hex_str = file_name.strip_suffix(".json")?;
    Felt::from_hex(hex_str).ok()
}

impl StoreTrait for JsonStore {
    type Table = DojoTable;
    type Error = serde_json::Error;
    fn dump(&self, table_id: Felt, data: &Self::Table) -> Result<(), Self::Error> {
        let file_path = self.path.join(felt_to_json_file_name(&table_id));
        std::fs::write(file_path, serde_json::to_string_pretty(data).unwrap())
            .expect("Unable to write file");
        Ok(())
    }

    fn load(&self, table_id: Felt) -> Result<Self::Table, Self::Error> {
        let file_path = self.path.join(felt_to_json_file_name(&table_id));
        let data = std::fs::read_to_string(file_path).expect("Unable to read file");
        Ok(serde_json::from_str(&data)?)
    }

    fn load_all(&self) -> Result<Vec<(Felt, Self::Table)>, Self::Error> {
        let mut tables: Vec<(Felt, Self::Table)> = Vec::new();
        let paths = fs::read_dir(&self.path).unwrap();
        for path in paths {
            let path = path.unwrap().path();
            let table_id = path
                .file_name()
                .and_then(|p| json_file_name_to_felt(p.to_str()?));
            let data: Option<DojoTable> =
                serde_json::from_str(&fs::read_to_string(&path).unwrap()).ok();
            match (table_id, data) {
                (Some(id), Some(table)) => {
                    tables.push((id, table));
                }
                _ => {}
            }
        }
        Ok(tables)
    }
}

impl StoreTrait for PostgresStore {
    type Table = DojoTable;
    type Error = sqlx::Error;

    fn dump(&self, table_id: Felt, data: &Self::Table) -> Result<(), Self::Error> {
        let json = serde_json::to_string(data).map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        self.block_on(async {
            sqlx::query(
                r#"
                INSERT INTO torii_dojo_manager_state (table_id, table_json, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (table_id)
                DO UPDATE SET
                    table_json = EXCLUDED.table_json,
                    updated_at = NOW()
                "#,
            )
            .bind(table_id.to_bytes_be().to_vec())
            .bind(json)
            .execute(&self.pool)
            .await?;
            Ok(())
        })
    }

    fn load(&self, table_id: Felt) -> Result<Self::Table, Self::Error> {
        self.block_on(async {
            let row = sqlx::query(
                r#"
                SELECT table_json
                FROM torii_dojo_manager_state
                WHERE table_id = $1
                "#,
            )
            .bind(table_id.to_bytes_be().to_vec())
            .fetch_one(&self.pool)
            .await?;

            let json: String = row.try_get("table_json")?;
            serde_json::from_str(&json).map_err(|e| sqlx::Error::Protocol(e.to_string()))
        })
    }

    fn load_all(&self) -> Result<Vec<(Felt, Self::Table)>, Self::Error> {
        self.block_on(async {
            let rows = sqlx::query(
                r#"
                SELECT table_id, table_json
                FROM torii_dojo_manager_state
                ORDER BY updated_at ASC
                "#,
            )
            .fetch_all(&self.pool)
            .await?;

            rows.into_iter()
                .map(|row| {
                    let table_id: Vec<u8> = row.try_get("table_id")?;
                    let table_json: String = row.try_get("table_json")?;
                    let felt = Felt::from_bytes_be_slice(&table_id);
                    let table = serde_json::from_str(&table_json)
                        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
                    Ok((felt, table))
                })
                .collect()
        })
    }
}

pub trait DojoTableManager {
    fn register_table(
        &self,
        namespace: &str,
        name: &str,
        schema: DojoSchema,
    ) -> DojoToriiResult<TableSchema>;
    fn update_table(&self, id: Felt, schema: DojoSchema) -> DojoToriiResult<TableSchema>;
    fn with_table<F, R>(&self, id: Felt, f: F) -> DojoToriiResult<R>
    where
        F: FnOnce(&DojoTable) -> R;
}

impl<Store> DojoTableManager for DojoTableStore<Store>
where
    Store: StoreTrait<Table = DojoTable> + Send + Sync,
{
    fn register_table(
        &self,
        namespace: &str,
        name: &str,
        schema: DojoSchema,
    ) -> DojoToriiResult<TableSchema> {
        let table = DojoTable::from_schema(schema, namespace, name, dojo_primary_def());
        if self
            .read()
            .map_err(|e| DojoToriiError::LockError(e.to_string()))?
            .tables
            .contains_key(&table.id)
        {
            return Err(DojoToriiError::TableAlreadyExists(table.id));
        }
        let mut manager = self
            .write()
            .map_err(|e| DojoToriiError::LockError(e.to_string()))?;
        manager
            .store
            .dump(table.id, &table)
            .map_err(|e| DojoToriiError::StoreError(e.to_string()))?;
        manager.tables.insert(table.id, RwLock::new(table.clone()));
        Ok(table.into())
    }

    fn update_table(&self, id: Felt, schema: DojoSchema) -> DojoToriiResult<TableSchema> {
        let manager = self
            .read()
            .map_err(|e| DojoToriiError::LockError(e.to_string()))?;
        let mut table = match manager.tables.get(&id) {
            Some(t) => t
                .write()
                .map_err(|e| DojoToriiError::LockError(e.to_string())),
            None => return Err(DojoToriiError::TableNotFoundById(id)),
        }?;
        let mut key_fields = Vec::new();
        let mut value_fields = Vec::new();
        for column in schema.columns {
            match column.has_attribute("key") {
                true => key_fields.push(column.id),
                false => value_fields.push(column.id),
            }
            table.columns.insert(column.id, column);
        }
        table.key_fields = key_fields;
        table.value_fields = value_fields;
        self.read()
            .map_err(|e| DojoToriiError::LockError(e.to_string()))?
            .store
            .dump(id, &table)
            .map_err(|e| DojoToriiError::StoreError(e.to_string()))?;
        Ok(table.to_schema())
    }

    fn with_table<F, R>(&self, id: Felt, f: F) -> DojoToriiResult<R>
    where
        F: FnOnce(&DojoTable) -> R,
    {
        let manager = self
            .read()
            .map_err(|e| DojoToriiError::LockError(e.to_string()))?;
        let table = manager
            .tables
            .get(&id)
            .ok_or_else(|| DojoToriiError::TableNotFoundById(id))?;
        let table_guard = table
            .read()
            .map_err(|e| DojoToriiError::LockError(e.to_string()))?;
        Ok(f(&*table_guard))
    }
}

impl<Store> DojoTableStore<Store>
where
    Store: StoreTrait<Table = DojoTable> + Send + Sync,
{
    pub fn create_table_messages(&self) -> DojoToriiResult<Vec<CreateTable>> {
        let manager = self
            .read()
            .map_err(|e| DojoToriiError::LockError(e.to_string()))?;

        manager
            .tables
            .values()
            .map(|table| {
                let table = table
                    .read()
                    .map_err(|e| DojoToriiError::LockError(e.to_string()))?;
                let schema = table.to_schema();
                Ok(CreateTable::from(schema))
            })
            .collect()
    }
}
