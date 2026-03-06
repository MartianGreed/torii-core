use crate::sql::{
    add_column_query, add_enum_variant_query, add_member_query, alter_table_query,
    create_enum_type_query, create_struct_type_query, create_table_query, create_tuple_type_query,
    modify_column_query, modify_member_query,
};
use crate::types::variant_member_name;
use crate::{HasherExt, PgRustEnum, PgStructDef, PostgresField, PostgresType};
use introspect_types::type_def::TypeName;
use introspect_types::{
    Attribute, ColumnDef, EnumDef, FixedArrayDef, MemberDef, PrimaryDef, PrimaryTypeDef,
    StructDef, TupleDef, TypeDef, VariantDef,
};
use sqlx::{Postgres, Transaction};
use starknet_types_core::felt::Felt;
use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};
use std::vec;
use thiserror::Error;
use xxhash_rust::xxh3::Xxh3;
pub struct SchemaManager(pub HashMap<String, RwLock<HashMap<String, PostgresType>>>);

#[derive(Debug, Error)]
pub enum TableError {
    #[error("Cannot Alter column {0} from {1} to {2}")]
    TypeUpgradeError(String, String, String),
    #[error("Database error: {0}")]
    DatabaseError(String),
    #[error("Type {0} not found")]
    TypeNotFound(String),
    #[error("Struct {0} not found")]
    StructNotFound(String),
    #[error("Not a struct type: {0}")]
    NotStructType(String),
    #[error("Failed to acquire write lock")]
    LockError,
    #[error("Unsupported type for {0}")]
    UnsupportedType(String),
}

pub trait PgMutTableManager {
    fn update_table(
        &self,
        id: Felt,
        name: &str,
        attributes: &[Attribute],
        primary: &PrimaryDef,
        columns: &[ColumnDef],
        tx: &mut Transaction<'_, Postgres>,
    ) -> ManagerResult<()>;
    fn update_column(
        &self,
        table: Felt,
        id: &Felt,
        name: &str,
        attributes: &[Attribute],
        type_def: &TypeDef,
        tx: &mut Transaction<'_, Postgres>,
    ) -> ManagerResult<()>;
}

type ManagerResult<T> = Result<T>;

impl<T> From<PoisonError<T>> for TableError {
    fn from(_: PoisonError<T>) -> Self {
        TableError::LockError
    }
}

pub type Result<T> = std::result::Result<T, TableError>;

#[derive(Default)]
pub struct PgTableSchema {
    columns: HashMap<String, PostgresType>,
    structs: RwLock<HashMap<String, Arc<RwLock<PgStructDef>>>>,
    tuples: RwLock<HashMap<String, Arc<RwLock<Vec<PostgresType>>>>>,
    enums: RwLock<HashMap<String, Arc<RwLock<PgRustEnum>>>>,
    order: Vec<String>,
}

impl PgStructDef {
    pub fn new(fields: Vec<PostgresField>) -> Self {
        Self {
            order: fields.iter().map(|f| f.name.clone()).collect(),
            fields: fields.into_iter().map(|f| (f.name, f.pg_type)).collect(),
        }
    }
    pub fn compare_member(
        &mut self,
        branch: &Xxh3,
        struct_name: &str,
        new: &MemberDef,
        schema: &PgTableSchema,
        queries: &mut Vec<String>,
    ) -> Result<()> {
        match self.fields.get(&new.name) {
            Some(old_type) => {
                match schema.compare_type(
                    &branch.branch(&new.name),
                    &new.name,
                    old_type,
                    &new.type_def,
                    queries,
                )? {
                    Some(t) => {
                        queries.push(modify_member_query(struct_name, &new.name, &t));
                        self.fields.insert(new.name.clone(), t);
                    }
                    None => {}
                }
            }
            None => {
                let type_def = new.extract_type(&branch, schema, queries)?;
                queries.push(add_member_query(struct_name, &new.name, &type_def));
                self.fields.insert(new.name.clone(), type_def);
            }
        };
        Ok(())
    }
}

impl PgRustEnum {
    pub fn compare_variant(
        &mut self,
        branch: &Xxh3,
        enum_name: &str,
        new: &VariantDef,
        schema: &PgTableSchema,
        queries: &mut Vec<String>,
    ) -> Result<()> {
        match self.variants.get(&new.name) {
            Some(old_type) => {
                match schema.compare_type(
                    &branch.branch(&new.name),
                    &new.name,
                    old_type,
                    &new.type_def,
                    queries,
                )? {
                    Some(t) => {
                        queries.push(modify_member_query(
                            enum_name,
                            &variant_member_name(&new.name),
                            &t,
                        ));
                        self.variants.insert(new.name.clone(), t);
                    }
                    None => {}
                }
            }
            None => {
                let type_def = new.extract_type(&branch, schema, queries)?;
                queries.push(add_enum_variant_query(&self.variants_type_name, &new.name));
                if type_def != PostgresType::None {
                    queries.push(add_member_query(
                        enum_name,
                        &variant_member_name(&new.name),
                        &type_def,
                    ));
                }
                self.variants.insert(new.name.clone(), type_def);
                self.order.push(new.name.clone());
            }
        };
        Ok(())
    }
}

impl PgTableSchema {
    pub fn new(
        name: &str,
        primary_key: &PrimaryDef,
        columns: &[ColumnDef],
    ) -> Result<(Self, Vec<String>)> {
        let mut queries = vec![];
        let mut table = PgTableSchema::default();
        let branch = Xxh3::new_based(name);
        let type_def = primary_key
            .type_def
            .extract_type(&branch, &table, &mut queries)?;
        let mut column_queries = vec![format!(
            r#""{}" {} PRIMARY KEY"#,
            primary_key.name,
            type_def.to_string()
        )];
        table.columns.insert(primary_key.name.clone(), type_def);

        for col in columns.iter() {
            let type_def = col.extract_type(&branch, &table, &mut queries)?;
            column_queries.push(format!(r#""{}" {}"#, col.name, type_def.to_string()));
            table.columns.insert(col.name.clone(), type_def);
            table.order.push(col.name.clone());
        }
        queries.push(create_table_query(name, &column_queries));
        Ok((table, queries))
    }

    pub fn upgrade_schema(&mut self, name: &str, new: &[ColumnDef]) -> Result<Vec<String>> {
        let mut queries = vec![];
        let mut table_queries = vec![];
        let branch = Xxh3::new_based(name);
        for column in new {
            let col_branch = branch.branch(&column.name);
            match self.columns.get(&column.name) {
                Some(old_def) => {
                    match self.compare_type(
                        &col_branch,
                        &column.name,
                        old_def,
                        &column.type_def,
                        &mut queries,
                    )? {
                        Some(t) => {
                            table_queries.push(modify_column_query(&column.name, &t));
                            self.columns.insert(column.name.clone(), t);
                        }
                        None => {}
                    }
                }
                None => {
                    let new_type = column.extract_type(&branch, self, &mut queries)?;
                    table_queries.push(add_column_query(&column.name, &new_type));
                    self.order.push(column.name.clone());
                    self.columns.insert(column.name.clone(), new_type);
                }
            }
        }
        if !table_queries.is_empty() {
            queries.push(alter_table_query(name, &table_queries));
        }
        Ok(queries)
    }

    fn add_struct(
        &self,
        branch: &Xxh3,
        name: &str,
        fields: Vec<PostgresField>,
        queries: &mut Vec<String>,
    ) -> Result<String> {
        let struct_name = branch.type_name(name);
        queries.push(create_struct_type_query(&struct_name, &fields));
        let mut structs = self.structs.write()?;
        structs.insert(
            struct_name.clone(),
            Arc::new(RwLock::new(PgStructDef::new(fields))),
        );
        Ok(struct_name)
    }

    fn add_rust_enum(
        &self,
        branch: &Xxh3,
        name: &str,
        variants: Vec<PostgresField>,
        queries: &mut Vec<String>,
    ) -> Result<String> {
        let (struct_name, enum_def) = PgRustEnum::new(branch, name, variants);
        queries.push(create_enum_type_query(
            &enum_def.variants_type_name,
            &enum_def.order,
        ));
        queries.push(create_struct_type_query(
            &struct_name,
            &enum_def.all_fields(),
        ));
        let mut enums = self.enums.write()?;
        enums.insert(struct_name.clone(), Arc::new(RwLock::new(enum_def)));
        Ok(struct_name)
    }

    fn add_tuple(
        &self,
        branch: &Xxh3,
        elements: Vec<PostgresType>,
        queries: &mut Vec<String>,
    ) -> Result<String> {
        let name = branch.type_name("tuple");

        queries.push(create_tuple_type_query(&name, &elements));
        let mut tuples = self.tuples.write()?;
        tuples.insert(name.clone(), Arc::new(RwLock::new(elements)));
        Ok(name)
    }

    fn compare_type(
        &self,
        branch: &Xxh3,
        name: &str,
        old: &PostgresType,
        new: &TypeDef,
        queries: &mut Vec<String>,
    ) -> Result<Option<PostgresType>> {
        use PostgresType::{
            Array as PgArray, BigInt, Boolean, Bytea, Bytes31 as PgBytes31, Char31,
            EthAddress as PgEthAddress, Felt252 as PgFelt252, Int, Int128, None as PgNone,
            RustEnum as PgRustEnum, SmallInt, StarknetHash, Struct as PgStruct, Text,
            Tuple as PgTuple, Uint128, Uint16, Uint32, Uint64, Uint8,
        };
        use TypeDef::{
            Array, Bool, ByteArray, ByteArrayE, Bytes31, Bytes31E, ClassHash, ContractAddress,
            Enum, EthAddress, Felt252, FixedArray, None as TDNone, ShortUtf8, StorageAddress,
            StorageBaseAddress, Struct, Tuple, Utf8String, I128, I16, I32, I64, I8, U128, U16, U32,
            U64, U8,
        };
        match (old, new) {
            (PgNone, TDNone)
            | (Bytea, ByteArray(_) | ByteArrayE(_))
            | (Text, Utf8String(_))
            | (PgBytes31, Bytes31 | Bytes31E(_))
            | (Char31, ShortUtf8)
            | (Boolean, Bool)
            | (SmallInt, I8 | I16)
            | (Int, I32)
            | (BigInt, I64)
            | (Uint8, U8)
            | (Uint16, U16)
            | (Uint32, U32)
            | (Uint64, U64)
            | (Uint128, U128)
            | (Int128, I128)
            | (PgFelt252, Felt252)
            | (StarknetHash, ClassHash | ContractAddress | StorageAddress | StorageBaseAddress)
            | (PgEthAddress, EthAddress) => Ok(None),
            (Boolean | Uint8, I8 | I16) => Ok(Some(SmallInt)),
            (Boolean | Uint8 | Uint16 | SmallInt, I32) => Ok(Some(Int)),
            (Boolean | Uint8 | Uint16 | SmallInt | Uint32 | Int, I64) => Ok(Some(BigInt)),
            (Boolean | Uint8 | Uint16 | SmallInt | Uint32 | Int | Uint64 | BigInt, I128) => {
                Ok(Some(Int128))
            }
            (Boolean, U8) => Ok(Some(Uint8)),
            (Boolean | Uint8, U16) => Ok(Some(Uint16)),
            (Boolean | Uint8 | Uint16, U32) => Ok(Some(Uint32)),
            (Boolean | Uint8 | Uint16 | Uint32, U64) => Ok(Some(Uint64)),
            (Boolean | Uint8 | Uint16 | Uint32 | Uint64, U128) => Ok(Some(Uint128)),
            (
                Boolean | Uint8 | Uint16 | Uint32 | Uint64 | Uint128 | StarknetHash | PgEthAddress,
                Felt252,
            ) => Ok(Some(PgFelt252)),
            (
                Boolean | Uint8 | Uint16 | Uint32 | Uint64 | Uint128 | PgFelt252,
                ClassHash | ContractAddress,
            ) => Ok(Some(StarknetHash)),
            (PgStruct(struct_name), Struct(new_def)) => {
                self.upgrade_struct(branch, struct_name, new_def, queries)?;
                Ok(None)
            }
            (PgRustEnum(enum_name), Enum(new_def)) => {
                self.upgrade_enum(branch, enum_name, new_def, queries)?;
                Ok(None)
            }
            (PgTuple(tuple_name), Tuple(new_def)) => {
                self.upgrade_tuple(branch, tuple_name, new_def, queries)?;
                Ok(None)
            }
            (PgArray(elem_type, _), Array(new_elem_type)) => self
                .compare_type(branch, name, elem_type, new_elem_type, queries)
                .map(|s| s.map(|t| PgArray(Box::new(t), None))),
            (PgArray(elem_type, Some(size)), FixedArray(new_def)) => {
                if new_def.size >= *size {
                    self.compare_type(branch, name, elem_type, &new_def.type_def, queries)
                        .map(|s| s.map(|t| PgArray(Box::new(t), Some(*size))))
                } else {
                    new_upgrade_error(name, old, new)
                }
            }
            _ => new_upgrade_error(name, old, new),
        }
    }

    fn upgrade_struct(
        &self,
        branch: &Xxh3,
        name: &str,
        new: &StructDef,
        queries: &mut Vec<String>,
    ) -> Result<()> {
        // First, get a clone of the current fields to work with
        let struct_lock = {
            let structs = self.structs.read()?;
            structs
                .get(name)
                .ok_or(TableError::StructNotFound(name.to_string()))?
                .clone()
        };
        let mut struct_def = struct_lock.write()?;
        for new_field in new.members.iter() {
            struct_def.compare_member(&branch, name, new_field, self, queries)?;
        }
        Ok(())
    }

    fn upgrade_enum(
        &self,
        branch: &Xxh3,
        name: &str,
        new: &EnumDef,
        queries: &mut Vec<String>,
    ) -> Result<()> {
        let enum_lock = {
            let enums = self.enums.read()?;
            enums
                .get(name)
                .ok_or(TableError::StructNotFound(name.to_string()))?
                .clone()
        };
        let mut enum_def = enum_lock.write()?;
        for selector in new.order.iter() {
            let variant = &new.variants[selector];
            enum_def.compare_variant(branch, name, variant, self, queries)?;
        }
        Ok(())
    }

    fn upgrade_tuple(
        &self,
        branch: &Xxh3,
        name: &str,
        new: &[TypeDef],
        queries: &mut Vec<String>,
    ) -> Result<()> {
        let tuple_lock = {
            let tuples = self.tuples.read()?;
            tuples
                .get(name)
                .ok_or(TableError::StructNotFound(name.to_string()))?
                .clone()
        };
        let mut tuple_def = tuple_lock.write()?;
        for (i, new_type) in new.iter().enumerate() {
            let part_name = format!("_{i}");
            if i >= tuple_def.len() {
                let type_def = new_type.extract_type(branch, self, queries)?;
                queries.push(add_member_query(name, &part_name, &type_def));
                tuple_def.push(type_def);
            } else {
                let old_type = &tuple_def[i];
                match self.compare_type(branch, &part_name, old_type, new_type, queries)? {
                    Some(t) => {
                        queries.push(modify_member_query(name, &part_name, &t));
                        tuple_def[i] = t;
                    }
                    None => {}
                }
            }
        }
        Ok(())
    }
}

pub trait PostgresTypeExtractor {
    fn extract_type(
        &self,
        branch: &Xxh3,
        schema: &PgTableSchema,
        queries: &mut Vec<String>,
    ) -> Result<PostgresType>;
    fn extract_type_string(
        &self,
        branch: &Xxh3,
        schema: &PgTableSchema,
        queries: &mut Vec<String>,
    ) -> Result<String> {
        self.extract_type(branch, schema, queries)
            .map(|ty| ty.to_string())
    }
}

pub trait PostgresFieldExtractor {
    fn name(&self) -> &str;
    fn type_def(&self) -> &TypeDef;
    fn extract_type(
        &self,
        branch: &Xxh3,
        schema: &PgTableSchema,
        queries: &mut Vec<String>,
    ) -> Result<PostgresType> {
        self.type_def()
            .extract_type(&branch.branch(self.name()), schema, queries)
    }
    fn extract_field(
        &self,
        branch: &Xxh3,
        schema: &PgTableSchema,
        queries: &mut Vec<String>,
    ) -> Result<PostgresField> {
        Ok(PostgresField::new(
            self.name().to_string(),
            self.extract_type(branch, schema, queries)?,
        ))
    }
}

impl PostgresFieldExtractor for ColumnDef {
    fn name(&self) -> &str {
        &self.name
    }

    fn type_def(&self) -> &TypeDef {
        &self.type_def
    }
}

impl PostgresFieldExtractor for MemberDef {
    fn name(&self) -> &str {
        &self.name
    }

    fn type_def(&self) -> &TypeDef {
        &self.type_def
    }
}

impl PostgresFieldExtractor for VariantDef {
    fn name(&self) -> &str {
        &self.name
    }

    fn type_def(&self) -> &TypeDef {
        &self.type_def
    }
}

// impl PostgresFieldExtractor for PrimaryDef {
//     fn name(&self) -> &str {
//         &self.name
//     }

//     fn type_def(&self) -> &TypeDef {
//         &self.type_def
//     }
// }

impl PostgresTypeExtractor for TypeDef {
    fn extract_type(
        &self,
        branch: &Xxh3,
        schema: &PgTableSchema,
        queries: &mut Vec<String>,
    ) -> Result<PostgresType> {
        match self {
            TypeDef::None => Ok(PostgresType::None),
            TypeDef::Bool => Ok(PostgresType::Boolean),
            TypeDef::I8 | TypeDef::I16 => Ok(PostgresType::SmallInt),
            TypeDef::I32 => Ok(PostgresType::Int),
            TypeDef::I64 => Ok(PostgresType::BigInt),
            TypeDef::U8 => Ok(PostgresType::Uint8),
            TypeDef::U16 => Ok(PostgresType::Uint16),
            TypeDef::U32 => Ok(PostgresType::Uint32),
            TypeDef::U64 => Ok(PostgresType::Uint64),
            TypeDef::U128 => Ok(PostgresType::Uint128),
            TypeDef::I128 => Ok(PostgresType::Int128),
            TypeDef::U256 => Ok(PostgresType::Uint256),
            TypeDef::U512 => Ok(PostgresType::Uint512),
            TypeDef::Felt252 => Ok(PostgresType::Felt252),
            TypeDef::ContractAddress
            | TypeDef::ClassHash
            | TypeDef::StorageAddress
            | TypeDef::StorageBaseAddress => Ok(PostgresType::StarknetHash),
            TypeDef::EthAddress => Ok(PostgresType::EthAddress),
            TypeDef::Utf8String(_) => Ok(PostgresType::Text),
            TypeDef::ShortUtf8 => Ok(PostgresType::Char31),
            TypeDef::ByteArray(_) | TypeDef::ByteArrayE(_) => Ok(PostgresType::Bytea),
            TypeDef::Bytes31 | TypeDef::Bytes31E(_) => Ok(PostgresType::Bytes31),
            TypeDef::Tuple(type_defs) => type_defs.extract_type(branch, schema, queries),
            TypeDef::Enum(enum_def) => enum_def.extract_type(branch, schema, queries),
            TypeDef::Array(array_def) => Ok(PostgresType::Array(
                Box::new(array_def.extract_type(branch, schema, queries)?),
                None,
            )),
            TypeDef::FixedArray(fixed_array_def) => {
                fixed_array_def.extract_type(branch, schema, queries)
            }
            TypeDef::Struct(struct_def) => struct_def.extract_type(branch, schema, queries),
            _ => Err(TableError::UnsupportedType(format!("{self:?}"))),
        }
    }
}

impl PostgresTypeExtractor for PrimaryTypeDef {
    fn extract_type(
        &self,
        _branch: &Xxh3,
        _schema: &PgTableSchema,
        _queries: &mut Vec<String>,
    ) -> Result<PostgresType> {
        match self {
            PrimaryTypeDef::Bool => Ok(PostgresType::Boolean),
            PrimaryTypeDef::I8 | PrimaryTypeDef::I16 => Ok(PostgresType::SmallInt),
            PrimaryTypeDef::I32 => Ok(PostgresType::Int),
            PrimaryTypeDef::I64 => Ok(PostgresType::BigInt),
            PrimaryTypeDef::U8 => Ok(PostgresType::Uint8),
            PrimaryTypeDef::U16 => Ok(PostgresType::Uint16),
            PrimaryTypeDef::U32 => Ok(PostgresType::Uint32),
            PrimaryTypeDef::U64 => Ok(PostgresType::Uint64),
            PrimaryTypeDef::U128 => Ok(PostgresType::Uint128),
            PrimaryTypeDef::I128 => Ok(PostgresType::Int128),
            PrimaryTypeDef::Felt252 => Ok(PostgresType::Felt252),
            PrimaryTypeDef::ContractAddress
            | PrimaryTypeDef::ClassHash
            | PrimaryTypeDef::StorageAddress
            | PrimaryTypeDef::StorageBaseAddress => Ok(PostgresType::StarknetHash),
            PrimaryTypeDef::EthAddress => Ok(PostgresType::EthAddress),
            PrimaryTypeDef::ShortUtf8 => Ok(PostgresType::Char31),
            PrimaryTypeDef::Bytes31 | PrimaryTypeDef::Bytes31E(_) => Ok(PostgresType::Bytes31),
        }
    }
}

impl PostgresTypeExtractor for FixedArrayDef {
    fn extract_type(
        &self,
        branch: &Xxh3,
        schema: &PgTableSchema,
        queries: &mut Vec<String>,
    ) -> Result<PostgresType> {
        Ok(PostgresType::Array(
            Box::new(self.type_def.extract_type(branch, schema, queries)?),
            Some(self.size as u32),
        ))
    }
}

impl PostgresTypeExtractor for StructDef {
    fn extract_type(
        &self,
        branch: &Xxh3,
        schema: &PgTableSchema,
        queries: &mut Vec<String>,
    ) -> Result<PostgresType> {
        let members = self
            .members
            .iter()
            .map(|f| f.extract_field(branch, schema, queries))
            .collect::<Result<Vec<_>>>()?;
        schema
            .add_struct(branch, &self.name, members, queries)
            .map(PostgresType::Struct)
    }
}

// impl PostgresTypeExtractor for MemberDef {
//     fn extract_type(
//         &self,
//         branch: &Xxh3,
//         schema: &TableSchema,
//         queries: &mut Vec<String>,
//     ) -> Result<PostgresType> {
//         self.type_def
//             .extract_type(&branch.branch(&self.name), schema, queries)
//     }
// }

impl PostgresTypeExtractor for EnumDef {
    fn extract_type(
        &self,
        branch: &Xxh3,
        schema: &PgTableSchema,
        queries: &mut Vec<String>,
    ) -> Result<PostgresType> {
        let variants = self
            .order
            .iter()
            .map(|selector| {
                let variant = &self.variants[selector];
                variant.extract_field(branch, schema, queries)
            })
            .collect::<Result<Vec<_>>>()?;
        schema
            .add_rust_enum(branch, &self.name, variants, queries)
            .map(PostgresType::RustEnum)
    }
}

impl PostgresTypeExtractor for TupleDef {
    fn extract_type(
        &self,
        branch: &Xxh3,
        schema: &PgTableSchema,
        queries: &mut Vec<String>,
    ) -> Result<PostgresType> {
        let variants = self
            .iter()
            .enumerate()
            .map(|(i, type_def)| {
                type_def.extract_type(&branch.branch(&format!("_{i}")), schema, queries)
            })
            .collect::<Result<Vec<_>>>()?;
        schema
            .add_tuple(branch, variants, queries)
            .map(PostgresType::Tuple)
    }
}

pub fn new_upgrade_error<T>(field: &str, old: &PostgresType, new: &TypeDef) -> Result<T> {
    Err(TableError::TypeUpgradeError(
        field.to_string(),
        old.to_string(),
        new.type_name(),
    ))
}
