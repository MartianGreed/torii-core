//! SQLite storage for ERC1155 token transfers and balances
//!
//! Balance tracking uses a "fetch-on-inconsistency" approach:
//! - Tracks balances computed from transfer events
//! - When a balance would go negative (indicating missed history like genesis allocations),
//!   fetches the actual balance from the chain and adjusts
//! - Records all adjustments in an audit table for debugging

use anyhow::Result;
use rusqlite::{params, Connection};
use starknet::core::types::{Felt, U256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio_postgres::{types::ToSql as PgToSql, Client, NoTls};
use torii_common::{
    blob_to_felt, blob_to_u256, felt_to_blob, u256_to_blob, TokenUriResult, TokenUriStore,
};

use crate::balance_fetcher::Erc1155BalanceFetchRequest;

/// Storage for ERC1155 token data
pub struct Erc1155Storage {
    backend: StorageBackend,
    conn: Arc<Mutex<Connection>>,
    pg_conn: Option<Arc<tokio::sync::Mutex<Client>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageBackend {
    Sqlite,
    Postgres,
}

/// Token transfer data for batch insertion
pub struct TokenTransferData {
    pub id: Option<i64>,
    pub token: Felt,
    pub operator: Felt,
    pub from: Felt,
    pub to: Felt,
    pub token_id: U256,
    pub amount: U256,
    pub is_batch: bool,
    pub batch_index: u32,
    pub block_number: u64,
    pub tx_hash: Felt,
    pub timestamp: Option<i64>,
}

/// Operator approval data
pub struct OperatorApprovalData {
    pub id: Option<i64>,
    pub token: Felt,
    pub owner: Felt,
    pub operator: Felt,
    pub approved: bool,
    pub block_number: u64,
    pub tx_hash: Felt,
    pub timestamp: Option<i64>,
}

/// Token URI data
pub struct TokenUriData {
    pub token: Felt,
    pub token_id: U256,
    pub uri: String,
    pub block_number: u64,
    pub tx_hash: Felt,
    pub timestamp: Option<i64>,
}

/// Cursor for paginated transfer queries
#[derive(Debug, Clone, Copy)]
pub struct TransferCursor {
    pub block_number: u64,
    pub id: i64,
}

/// Aggregated facet count for one key/value pair.
pub struct AttributeFacetCount {
    pub key: String,
    pub value: String,
    pub count: u64,
}

/// Result of token ID search by attributes.
pub struct TokenAttributeQueryResult {
    pub token_ids: Vec<U256>,
    pub next_cursor_token_id: Option<U256>,
    pub total_hits: u64,
    pub facets: Vec<AttributeFacetCount>,
}

/// Balance data for a (contract, wallet, token_id) tuple
#[derive(Debug, Clone)]
pub struct Erc1155BalanceData {
    pub contract: Felt,
    pub wallet: Felt,
    pub token_id: U256,
    pub balance: U256,
    pub last_block: u64,
}

/// Balance adjustment record for audit trail
#[derive(Debug, Clone)]
pub struct Erc1155BalanceAdjustment {
    pub contract: Felt,
    pub wallet: Felt,
    pub token_id: U256,
    /// What we computed from transfers
    pub computed_balance: U256,
    /// What the RPC returned as actual balance
    pub actual_balance: U256,
    /// Block at which adjustment was made
    pub adjusted_at_block: u64,
    pub tx_hash: Felt,
}

impl Erc1155Storage {
    /// Create or open the database
    pub async fn new(db_path: &str) -> Result<Self> {
        if db_path.starts_with("postgres://") || db_path.starts_with("postgresql://") {
            let (client, connection) = tokio_postgres::connect(db_path, NoTls).await?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::error!(target: "torii_erc1155::storage", error = %e, "PostgreSQL connection task failed");
                }
            });

            client.batch_execute(
                r"
                CREATE SCHEMA IF NOT EXISTS erc1155;

                CREATE TABLE IF NOT EXISTS erc1155.token_transfers (
                    id BIGSERIAL PRIMARY KEY,
                    token BYTEA NOT NULL,
                    operator BYTEA NOT NULL,
                    from_addr BYTEA NOT NULL,
                    to_addr BYTEA NOT NULL,
                    token_id BYTEA NOT NULL,
                    amount BYTEA NOT NULL,
                    is_batch BIGINT NOT NULL DEFAULT 0,
                    batch_index BIGINT NOT NULL DEFAULT 0,
                    block_number BIGINT NOT NULL,
                    tx_hash BYTEA NOT NULL,
                    timestamp BIGINT,
                    UNIQUE(token, tx_hash, token_id, from_addr, to_addr, batch_index)
                );
                CREATE INDEX IF NOT EXISTS idx_token_transfers_token ON erc1155.token_transfers(token);
                CREATE INDEX IF NOT EXISTS idx_token_transfers_from ON erc1155.token_transfers(from_addr);
                CREATE INDEX IF NOT EXISTS idx_token_transfers_to ON erc1155.token_transfers(to_addr);
                CREATE INDEX IF NOT EXISTS idx_token_transfers_block ON erc1155.token_transfers(block_number DESC);
                CREATE INDEX IF NOT EXISTS idx_token_transfers_token_id ON erc1155.token_transfers(token, token_id);

                CREATE TABLE IF NOT EXISTS erc1155.token_wallet_activity (
                    id BIGSERIAL PRIMARY KEY,
                    wallet_address BYTEA NOT NULL,
                    token BYTEA NOT NULL,
                    transfer_id BIGINT NOT NULL REFERENCES erc1155.token_transfers(id),
                    direction TEXT NOT NULL CHECK(direction IN ('sent', 'received', 'both')),
                    block_number BIGINT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_token_wallet_activity_wallet_block ON erc1155.token_wallet_activity(wallet_address, block_number DESC);
                CREATE INDEX IF NOT EXISTS idx_token_wallet_activity_wallet_token ON erc1155.token_wallet_activity(wallet_address, token, block_number DESC);

                CREATE TABLE IF NOT EXISTS erc1155.token_operators (
                    id BIGSERIAL PRIMARY KEY,
                    token BYTEA NOT NULL,
                    owner BYTEA NOT NULL,
                    operator BYTEA NOT NULL,
                    approved BIGINT NOT NULL,
                    block_number BIGINT NOT NULL,
                    tx_hash BYTEA NOT NULL,
                    timestamp BIGINT,
                    UNIQUE(token, owner, operator)
                );

                CREATE TABLE IF NOT EXISTS erc1155.token_uris (
                    token BYTEA NOT NULL,
                    token_id BYTEA NOT NULL,
                    uri TEXT,
                    metadata_json TEXT,
                    updated_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT),
                    PRIMARY KEY (token, token_id)
                );

                CREATE TABLE IF NOT EXISTS erc1155.token_attributes (
                    token BYTEA NOT NULL,
                    token_id BYTEA NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT NOT NULL,
                    PRIMARY KEY (token, token_id, key)
                );
                CREATE INDEX IF NOT EXISTS idx_token_attributes_token ON erc1155.token_attributes(token);
                CREATE INDEX IF NOT EXISTS idx_token_attributes_key ON erc1155.token_attributes(key);
                CREATE INDEX IF NOT EXISTS idx_token_attributes_key_value ON erc1155.token_attributes(key, value);
                CREATE INDEX IF NOT EXISTS idx_token_attributes_token_key_value ON erc1155.token_attributes(token, key, value);
                CREATE INDEX IF NOT EXISTS idx_token_attributes_token_token_id ON erc1155.token_attributes(token, token_id);

                CREATE TABLE IF NOT EXISTS erc1155.erc1155_balances (
                    id BIGSERIAL PRIMARY KEY,
                    contract BYTEA NOT NULL,
                    wallet BYTEA NOT NULL,
                    token_id BYTEA NOT NULL,
                    balance BYTEA NOT NULL,
                    last_block BIGINT NOT NULL,
                    updated_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT),
                    UNIQUE(contract, wallet, token_id)
                );
                CREATE INDEX IF NOT EXISTS idx_erc1155_balances_contract ON erc1155.erc1155_balances(contract);
                CREATE INDEX IF NOT EXISTS idx_erc1155_balances_wallet ON erc1155.erc1155_balances(wallet);
                CREATE INDEX IF NOT EXISTS idx_erc1155_balances_contract_wallet ON erc1155.erc1155_balances(contract, wallet);

                CREATE TABLE IF NOT EXISTS erc1155.erc1155_balance_adjustments (
                    id BIGSERIAL PRIMARY KEY,
                    contract BYTEA NOT NULL,
                    wallet BYTEA NOT NULL,
                    token_id BYTEA NOT NULL,
                    computed_balance BYTEA NOT NULL,
                    actual_balance BYTEA NOT NULL,
                    adjusted_at_block BIGINT NOT NULL,
                    tx_hash BYTEA NOT NULL,
                    created_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT)
                );
                CREATE INDEX IF NOT EXISTS idx_erc1155_adjustments_wallet ON erc1155.erc1155_balance_adjustments(wallet);

                CREATE TABLE IF NOT EXISTS erc1155.token_metadata (
                    token BYTEA PRIMARY KEY,
                    name TEXT,
                    symbol TEXT,
                    total_supply BYTEA
                );
                ",
            ).await?;

            tracing::info!(target: "torii_erc1155::storage", "PostgreSQL storage initialized");
            return Ok(Self {
                backend: StorageBackend::Postgres,
                conn: Arc::new(Mutex::new(Connection::open_in_memory()?)),
                pg_conn: Some(Arc::new(tokio::sync::Mutex::new(client))),
            });
        }

        let conn = Connection::open(db_path)?;

        // Enable WAL mode + Performance PRAGMAs
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA foreign_keys=ON;
             PRAGMA cache_size=-64000;
             PRAGMA temp_store=MEMORY;
             PRAGMA mmap_size=268435456;
             PRAGMA wal_autocheckpoint=10000;
             PRAGMA page_size=4096;
             PRAGMA busy_timeout=5000;",
        )?;

        tracing::info!(target: "torii_erc1155::storage", "SQLite configured: WAL mode, 64MB cache, 256MB mmap, NORMAL sync");

        // Token transfers table (both single and batch transfers)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS token_transfers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                operator BLOB NOT NULL,
                from_addr BLOB NOT NULL,
                to_addr BLOB NOT NULL,
                token_id BLOB NOT NULL,
                amount BLOB NOT NULL,
                is_batch INTEGER NOT NULL DEFAULT 0,
                batch_index INTEGER NOT NULL DEFAULT 0,
                block_number INTEGER NOT NULL,
                tx_hash BLOB NOT NULL,
                timestamp INTEGER,
                UNIQUE(token, tx_hash, token_id, from_addr, to_addr, batch_index)
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_token_transfers_token ON token_transfers(token)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_token_transfers_from ON token_transfers(from_addr)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_token_transfers_to ON token_transfers(to_addr)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_token_transfers_block ON token_transfers(block_number DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_token_transfers_token_id ON token_transfers(token, token_id)",
            [],
        )?;

        // Wallet activity table for efficient OR queries
        conn.execute(
            "CREATE TABLE IF NOT EXISTS token_wallet_activity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_address BLOB NOT NULL,
                token BLOB NOT NULL,
                transfer_id INTEGER NOT NULL,
                direction TEXT NOT NULL CHECK(direction IN ('sent', 'received', 'both')),
                block_number INTEGER NOT NULL,
                FOREIGN KEY (transfer_id) REFERENCES token_transfers(id)
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_token_wallet_activity_wallet_block
             ON token_wallet_activity(wallet_address, block_number DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_token_wallet_activity_wallet_token
             ON token_wallet_activity(wallet_address, token, block_number DESC)",
            [],
        )?;

        // Operator approvals
        conn.execute(
            "CREATE TABLE IF NOT EXISTS token_operators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                owner BLOB NOT NULL,
                operator BLOB NOT NULL,
                approved INTEGER NOT NULL,
                block_number INTEGER NOT NULL,
                tx_hash BLOB NOT NULL,
                timestamp INTEGER,
                UNIQUE(token, owner, operator)
            )",
            [],
        )?;

        // URI metadata
        conn.execute(
            "CREATE TABLE IF NOT EXISTS token_uris (
                token BLOB NOT NULL,
                token_id BLOB NOT NULL,
                uri TEXT,
                metadata_json TEXT,
                updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                PRIMARY KEY (token, token_id)
            )",
            [],
        )?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS token_attributes (
                token BLOB NOT NULL,
                token_id BLOB NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (token, token_id, key),
                FOREIGN KEY (token, token_id) REFERENCES token_uris(token, token_id)
            );
            CREATE INDEX IF NOT EXISTS idx_token_attributes_token ON token_attributes(token);
            CREATE INDEX IF NOT EXISTS idx_token_attributes_key ON token_attributes(key);
            CREATE INDEX IF NOT EXISTS idx_token_attributes_key_value ON token_attributes(key, value);
            CREATE INDEX IF NOT EXISTS idx_token_attributes_token_key_value ON token_attributes(token, key, value);
            CREATE INDEX IF NOT EXISTS idx_token_attributes_token_token_id ON token_attributes(token, token_id);",
        )?;

        // Balance tracking tables
        // Tracks current balance per (contract, wallet, token_id) tuple
        conn.execute(
            "CREATE TABLE IF NOT EXISTS erc1155_balances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                contract BLOB NOT NULL,
                wallet BLOB NOT NULL,
                token_id BLOB NOT NULL,
                balance BLOB NOT NULL,
                last_block INTEGER NOT NULL,
                updated_at INTEGER DEFAULT (strftime('%s', 'now')),
                UNIQUE(contract, wallet, token_id)
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_erc1155_balances_contract ON erc1155_balances(contract)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_erc1155_balances_wallet ON erc1155_balances(wallet)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_erc1155_balances_contract_wallet
             ON erc1155_balances(contract, wallet)",
            [],
        )?;

        // Balance adjustments table for audit trail
        conn.execute(
            "CREATE TABLE IF NOT EXISTS erc1155_balance_adjustments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                contract BLOB NOT NULL,
                wallet BLOB NOT NULL,
                token_id BLOB NOT NULL,
                computed_balance BLOB NOT NULL,
                actual_balance BLOB NOT NULL,
                adjusted_at_block INTEGER NOT NULL,
                tx_hash BLOB NOT NULL,
                created_at INTEGER DEFAULT (strftime('%s', 'now'))
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_erc1155_adjustments_wallet
             ON erc1155_balance_adjustments(wallet)",
            [],
        )?;

        // Token metadata table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS token_metadata (
                token BLOB PRIMARY KEY,
                name TEXT,
                symbol TEXT,
                total_supply BLOB
            )",
            [],
        )?;

        tracing::info!(target: "torii_erc1155::storage", db_path = %db_path, "ERC1155 database initialized");

        Ok(Self {
            backend: StorageBackend::Sqlite,
            conn: Arc::new(Mutex::new(conn)),
            pg_conn: None,
        })
    }

    /// Insert multiple transfers in a single transaction
    pub async fn insert_transfers_batch(&self, transfers: &[TokenTransferData]) -> Result<usize> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_insert_transfers_batch(transfers).await;
        }
        if transfers.is_empty() {
            return Ok(0);
        }

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let mut inserted = 0;

        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO token_transfers (token, operator, from_addr, to_addr, token_id, amount, is_batch, batch_index, block_number, tx_hash, timestamp)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, COALESCE(?11, strftime('%s', 'now')))",
            )?;
            let mut wallet_both_stmt = tx.prepare_cached(
                "INSERT INTO token_wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                 VALUES (?1, ?2, ?3, 'both', ?4)",
            )?;
            let mut wallet_sent_stmt = tx.prepare_cached(
                "INSERT INTO token_wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                 VALUES (?1, ?2, ?3, 'sent', ?4)",
            )?;
            let mut wallet_received_stmt = tx.prepare_cached(
                "INSERT INTO token_wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                 VALUES (?1, ?2, ?3, 'received', ?4)",
            )?;

            for transfer in transfers {
                let token_blob = felt_to_blob(transfer.token);
                let operator_blob = felt_to_blob(transfer.operator);
                let from_blob = felt_to_blob(transfer.from);
                let to_blob = felt_to_blob(transfer.to);
                let token_id_blob = u256_to_blob(transfer.token_id);
                let amount_blob = u256_to_blob(transfer.amount);
                let tx_hash_blob = felt_to_blob(transfer.tx_hash);

                let rows = stmt.execute(params![
                    &token_blob,
                    &operator_blob,
                    &from_blob,
                    &to_blob,
                    &token_id_blob,
                    &amount_blob,
                    transfer.is_batch as i32,
                    transfer.batch_index,
                    transfer.block_number,
                    &tx_hash_blob,
                    transfer.timestamp,
                ])?;

                if rows > 0 {
                    inserted += 1;
                    let transfer_id = tx.last_insert_rowid();

                    // Insert wallet activity records
                    if transfer.from != Felt::ZERO
                        && transfer.to != Felt::ZERO
                        && transfer.from == transfer.to
                    {
                        wallet_both_stmt.execute(params![
                            &from_blob,
                            &token_blob,
                            transfer_id,
                            transfer.block_number
                        ])?;
                    } else {
                        if transfer.from != Felt::ZERO {
                            wallet_sent_stmt.execute(params![
                                &from_blob,
                                &token_blob,
                                transfer_id,
                                transfer.block_number
                            ])?;
                        }
                        if transfer.to != Felt::ZERO {
                            wallet_received_stmt.execute(params![
                                &to_blob,
                                &token_blob,
                                transfer_id,
                                transfer.block_number
                            ])?;
                        }
                    }
                }
            }
        }

        tx.commit()?;
        Ok(inserted)
    }

    /// Insert operator approvals in a single transaction
    pub async fn insert_operator_approvals_batch(
        &self,
        approvals: &[OperatorApprovalData],
    ) -> Result<usize> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_insert_operator_approvals_batch(approvals).await;
        }
        if approvals.is_empty() {
            return Ok(0);
        }

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let mut inserted = 0;

        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR REPLACE INTO token_operators (token, owner, operator, approved, block_number, tx_hash, timestamp)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, COALESCE(?7, strftime('%s', 'now')))",
            )?;

            for approval in approvals {
                let token_blob = felt_to_blob(approval.token);
                let owner_blob = felt_to_blob(approval.owner);
                let operator_blob = felt_to_blob(approval.operator);
                let tx_hash_blob = felt_to_blob(approval.tx_hash);

                stmt.execute(params![
                    &token_blob,
                    &owner_blob,
                    &operator_blob,
                    approval.approved as i32,
                    approval.block_number,
                    &tx_hash_blob,
                    approval.timestamp,
                ])?;

                inserted += 1;
            }
        }

        tx.commit()?;
        Ok(inserted)
    }

    /// Insert or update token URIs in a single transaction
    pub async fn upsert_token_uris_batch(&self, uris: &[TokenUriData]) -> Result<usize> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_upsert_token_uris_batch(uris).await;
        }
        if uris.is_empty() {
            return Ok(0);
        }

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let mut updated = 0;

        let mut stmt = tx.prepare_cached(
            "INSERT INTO token_uris (token, token_id, uri, updated_at)
             VALUES (?1, ?2, ?3, strftime('%s', 'now'))
             ON CONFLICT(token, token_id) DO UPDATE SET
               uri = excluded.uri,
               updated_at = excluded.updated_at",
        )?;

        for entry in uris {
            let token_blob = felt_to_blob(entry.token);
            let token_id_blob = u256_to_blob(entry.token_id);
            let rows = stmt.execute(params![&token_blob, &token_id_blob, &entry.uri,])?;

            if rows > 0 {
                updated += 1;
            }
        }
        drop(stmt);

        tx.commit()?;
        Ok(updated)
    }

    /// Get filtered transfers with cursor-based pagination
    pub async fn get_transfers_filtered(
        &self,
        wallet: Option<Felt>,
        from: Option<Felt>,
        to: Option<Felt>,
        operator: Option<Felt>,
        tokens: &[Felt],
        token_ids: &[U256],
        block_from: Option<u64>,
        block_to: Option<u64>,
        cursor: Option<TransferCursor>,
        limit: u32,
    ) -> Result<(Vec<TokenTransferData>, Option<TransferCursor>)> {
        if self.backend == StorageBackend::Postgres {
            return self
                .pg_get_transfers_filtered(
                    wallet, from, to, operator, tokens, token_ids, block_from, block_to, cursor,
                    limit,
                )
                .await;
        }
        let conn = self.conn.lock().unwrap();

        let mut query = String::new();
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(wallet_addr) = wallet {
            query.push_str(
                "SELECT DISTINCT t.id, t.token, t.operator, t.from_addr, t.to_addr, t.token_id, t.amount, t.is_batch, t.batch_index, t.block_number, t.tx_hash, t.timestamp
                 FROM token_wallet_activity wa
                 JOIN token_transfers t ON wa.transfer_id = t.id
                 WHERE wa.wallet_address = ?",
            );
            params_vec.push(Box::new(felt_to_blob(wallet_addr)));

            if !tokens.is_empty() {
                let placeholders: Vec<&str> = tokens.iter().map(|_| "?").collect();
                query.push_str(&format!(" AND wa.token IN ({})", placeholders.join(",")));
                for token in tokens {
                    params_vec.push(Box::new(felt_to_blob(*token)));
                }
            }
        } else {
            query.push_str(
                "SELECT t.id, t.token, t.operator, t.from_addr, t.to_addr, t.token_id, t.amount, t.is_batch, t.batch_index, t.block_number, t.tx_hash, t.timestamp
                 FROM token_transfers t
                 WHERE 1=1",
            );

            if let Some(from_addr) = from {
                query.push_str(" AND t.from_addr = ?");
                params_vec.push(Box::new(felt_to_blob(from_addr)));
            }

            if let Some(to_addr) = to {
                query.push_str(" AND t.to_addr = ?");
                params_vec.push(Box::new(felt_to_blob(to_addr)));
            }

            if let Some(op_addr) = operator {
                query.push_str(" AND t.operator = ?");
                params_vec.push(Box::new(felt_to_blob(op_addr)));
            }

            if !tokens.is_empty() {
                let placeholders: Vec<&str> = tokens.iter().map(|_| "?").collect();
                query.push_str(&format!(" AND t.token IN ({})", placeholders.join(",")));
                for token in tokens {
                    params_vec.push(Box::new(felt_to_blob(*token)));
                }
            }
        }

        if !token_ids.is_empty() {
            let placeholders: Vec<&str> = token_ids.iter().map(|_| "?").collect();
            query.push_str(&format!(" AND t.token_id IN ({})", placeholders.join(",")));
            for tid in token_ids {
                params_vec.push(Box::new(u256_to_blob(*tid)));
            }
        }

        if let Some(block_min) = block_from {
            query.push_str(" AND t.block_number >= ?");
            params_vec.push(Box::new(block_min as i64));
        }

        if let Some(block_max) = block_to {
            query.push_str(" AND t.block_number <= ?");
            params_vec.push(Box::new(block_max as i64));
        }

        if let Some(c) = cursor {
            query.push_str(" AND (t.block_number < ? OR (t.block_number = ? AND t.id < ?))");
            params_vec.push(Box::new(c.block_number as i64));
            params_vec.push(Box::new(c.block_number as i64));
            params_vec.push(Box::new(c.id));
        }

        query.push_str(" ORDER BY t.block_number DESC, t.id DESC LIMIT ?");
        params_vec.push(Box::new(limit as i64));

        let mut stmt = conn.prepare(&query)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(std::convert::AsRef::as_ref).collect();

        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            let id: i64 = row.get(0)?;
            let token_bytes: Vec<u8> = row.get(1)?;
            let operator_bytes: Vec<u8> = row.get(2)?;
            let from_bytes: Vec<u8> = row.get(3)?;
            let to_bytes: Vec<u8> = row.get(4)?;
            let token_id_bytes: Vec<u8> = row.get(5)?;
            let amount_bytes: Vec<u8> = row.get(6)?;
            let is_batch: i32 = row.get(7)?;
            let batch_index: i32 = row.get(8)?;
            let block_number: i64 = row.get(9)?;
            let tx_hash_bytes: Vec<u8> = row.get(10)?;
            let timestamp: Option<i64> = row.get(11)?;

            Ok(TokenTransferData {
                id: Some(id),
                token: blob_to_felt(&token_bytes),
                operator: blob_to_felt(&operator_bytes),
                from: blob_to_felt(&from_bytes),
                to: blob_to_felt(&to_bytes),
                token_id: blob_to_u256(&token_id_bytes),
                amount: blob_to_u256(&amount_bytes),
                is_batch: is_batch != 0,
                batch_index: batch_index as u32,
                block_number: block_number as u64,
                tx_hash: blob_to_felt(&tx_hash_bytes),
                timestamp,
            })
        })?;

        let transfers: Vec<TokenTransferData> = rows.collect::<Result<_, _>>()?;

        let next_cursor = if transfers.len() == limit as usize {
            transfers.last().map(|t| TransferCursor {
                block_number: t.block_number,
                id: t.id.unwrap(),
            })
        } else {
            None
        };

        Ok((transfers, next_cursor))
    }

    /// Query token IDs by flattened metadata attributes.
    ///
    /// Filter semantics:
    /// - OR within a single key (`values`)
    /// - AND across keys (`filters`)
    pub async fn query_token_ids_by_attributes(
        &self,
        token: Felt,
        filters: &[(String, Vec<String>)],
        cursor_token_id: Option<U256>,
        limit: u32,
        include_facets: bool,
        facet_limit: u32,
    ) -> Result<TokenAttributeQueryResult> {
        let filters = Self::normalize_attribute_filters(filters);
        let page_limit = limit.clamp(1, 1000);
        let page_fetch = page_limit as i64 + 1;
        let facet_limit = facet_limit.clamp(1, 1000) as i64;

        if self.backend == StorageBackend::Postgres {
            return self
                .pg_query_token_ids_by_attributes(
                    token,
                    &filters,
                    cursor_token_id,
                    page_fetch,
                    include_facets,
                    facet_limit,
                )
                .await;
        }

        let conn = self.conn.lock().unwrap();

        let mut query = String::from("SELECT u.token_id FROM token_uris u WHERE u.token = ?");
        let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(felt_to_blob(token))];
        if let Some(cursor) = cursor_token_id {
            query.push_str(" AND u.token_id > ?");
            params.push(Box::new(u256_to_blob(cursor)));
        }
        Self::sqlite_append_attribute_exists_filters(&mut query, &mut params, &filters, "u");
        query.push_str(" ORDER BY u.token_id ASC LIMIT ?");
        params.push(Box::new(page_fetch));

        let mut stmt = conn.prepare(&query)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params.iter().map(std::convert::AsRef::as_ref).collect();
        let rows = stmt.query_map(params_refs.as_slice(), |row| row.get::<_, Vec<u8>>(0))?;
        let mut token_ids: Vec<U256> = rows
            .collect::<Result<Vec<Vec<u8>>, _>>()?
            .into_iter()
            .map(|b| blob_to_u256(&b))
            .collect();

        let next_cursor_token_id = if token_ids.len() > page_limit as usize {
            let next = token_ids[page_limit as usize];
            token_ids.truncate(page_limit as usize);
            Some(next)
        } else {
            None
        };

        let mut count_query = String::from("SELECT COUNT(*) FROM token_uris u WHERE u.token = ?");
        let mut count_params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(felt_to_blob(token))];
        Self::sqlite_append_attribute_exists_filters(
            &mut count_query,
            &mut count_params,
            &filters,
            "u",
        );
        let count_refs: Vec<&dyn rusqlite::ToSql> = count_params
            .iter()
            .map(std::convert::AsRef::as_ref)
            .collect();
        let total_hits: i64 =
            conn.query_row(&count_query, count_refs.as_slice(), |row| row.get(0))?;

        let facets = if include_facets {
            let mut facet_query = String::from(
                "SELECT a.key, a.value, COUNT(*) AS cnt
                 FROM token_attributes a
                 JOIN token_uris u ON u.token = a.token AND u.token_id = a.token_id
                 WHERE u.token = ?",
            );
            let mut facet_params: Vec<Box<dyn rusqlite::ToSql>> =
                vec![Box::new(felt_to_blob(token))];
            Self::sqlite_append_attribute_exists_filters(
                &mut facet_query,
                &mut facet_params,
                &filters,
                "u",
            );
            facet_query.push_str(
                " GROUP BY a.key, a.value
                  ORDER BY cnt DESC, a.key ASC, a.value ASC
                  LIMIT ?",
            );
            facet_params.push(Box::new(facet_limit));

            let facet_refs: Vec<&dyn rusqlite::ToSql> = facet_params
                .iter()
                .map(std::convert::AsRef::as_ref)
                .collect();
            let mut stmt = conn.prepare(&facet_query)?;
            let rows = stmt.query_map(facet_refs.as_slice(), |row| {
                Ok(AttributeFacetCount {
                    key: row.get(0)?,
                    value: row.get(1)?,
                    count: row.get::<_, i64>(2)? as u64,
                })
            })?;
            rows.collect::<Result<Vec<AttributeFacetCount>, _>>()?
        } else {
            Vec::new()
        };

        Ok(TokenAttributeQueryResult {
            token_ids,
            next_cursor_token_id,
            total_hits: total_hits as u64,
            facets,
        })
    }

    /// Get transfer count
    pub async fn get_transfer_count(&self) -> Result<u64> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_transfer_count().await;
        }
        let conn = self.conn.lock().unwrap();
        let count: i64 =
            conn.query_row("SELECT COUNT(*) FROM token_transfers", [], |row| row.get(0))?;
        Ok(count as u64)
    }

    /// Get unique token contract count
    pub async fn get_token_count(&self) -> Result<u64> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_token_count().await;
        }
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(DISTINCT token) FROM token_transfers",
            [],
            |row| row.get(0),
        )?;
        Ok(count as u64)
    }

    /// Get unique token ID count
    pub async fn get_token_id_count(&self) -> Result<u64> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_token_id_count().await;
        }
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(DISTINCT token || token_id) FROM token_transfers",
            [],
            |row| row.get(0),
        )?;
        Ok(count as u64)
    }

    /// Get latest block number indexed
    pub async fn get_latest_block(&self) -> Result<Option<u64>> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_latest_block().await;
        }
        let conn = self.conn.lock().unwrap();
        let block: Option<i64> = conn
            .query_row("SELECT MAX(block_number) FROM token_transfers", [], |row| {
                row.get(0)
            })
            .ok();
        Ok(block.map(|b| b as u64))
    }

    // ===== Balance Tracking Methods =====

    /// Get current balance for a (contract, wallet, token_id) tuple
    pub async fn get_balance(
        &self,
        contract: Felt,
        wallet: Felt,
        token_id: U256,
    ) -> Result<Option<U256>> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_balance(contract, wallet, token_id).await;
        }
        let conn = self.conn.lock().unwrap();
        let contract_blob = felt_to_blob(contract);
        let wallet_blob = felt_to_blob(wallet);
        let token_id_blob = u256_to_blob(token_id);

        let result: Option<Vec<u8>> = conn
            .query_row(
                "SELECT balance FROM erc1155_balances
                 WHERE contract = ? AND wallet = ? AND token_id = ?",
                params![&contract_blob, &wallet_blob, &token_id_blob],
                |row| row.get(0),
            )
            .ok();

        Ok(result.map(|bytes| blob_to_u256(&bytes)))
    }

    /// Get balance with last block info for a (contract, wallet, token_id) tuple
    pub async fn get_balance_with_block(
        &self,
        contract: Felt,
        wallet: Felt,
        token_id: U256,
    ) -> Result<Option<(U256, u64)>> {
        if self.backend == StorageBackend::Postgres {
            return self
                .pg_get_balance_with_block(contract, wallet, token_id)
                .await;
        }
        let conn = self.conn.lock().unwrap();
        let contract_blob = felt_to_blob(contract);
        let wallet_blob = felt_to_blob(wallet);
        let token_id_blob = u256_to_blob(token_id);

        let result: Option<(Vec<u8>, i64)> = conn
            .query_row(
                "SELECT balance, last_block FROM erc1155_balances
                 WHERE contract = ? AND wallet = ? AND token_id = ?",
                params![&contract_blob, &wallet_blob, &token_id_blob],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .ok();

        Ok(result.map(|(bytes, block)| (blob_to_u256(&bytes), block as u64)))
    }

    /// Get balances for multiple (contract, wallet, token_id) tuples in a single query
    pub async fn get_balances_batch(
        &self,
        tuples: &[(Felt, Felt, U256)],
    ) -> Result<HashMap<(Felt, Felt, U256), U256>> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_balances_batch(tuples).await;
        }
        if tuples.is_empty() {
            return Ok(HashMap::new());
        }

        let conn = self.conn.lock().unwrap();
        let mut result = HashMap::new();

        let mut stmt = conn.prepare_cached(
            "SELECT balance FROM erc1155_balances
             WHERE contract = ? AND wallet = ? AND token_id = ?",
        )?;

        for (contract, wallet, token_id) in tuples {
            let contract_blob = felt_to_blob(*contract);
            let wallet_blob = felt_to_blob(*wallet);
            let token_id_blob = u256_to_blob(*token_id);

            if let Ok(balance_bytes) = stmt.query_row(
                params![&contract_blob, &wallet_blob, &token_id_blob],
                |row| row.get::<_, Vec<u8>>(0),
            ) {
                result.insert(
                    (*contract, *wallet, *token_id),
                    blob_to_u256(&balance_bytes),
                );
            }
        }

        Ok(result)
    }

    /// Check which transfers need balance adjustments
    ///
    /// For each transfer, checks if the sender's current balance would go negative.
    /// Returns Erc1155BalanceFetchRequests for wallets that need adjustment.
    pub async fn check_balances_batch(
        &self,
        transfers: &[TokenTransferData],
    ) -> Result<Vec<Erc1155BalanceFetchRequest>> {
        if transfers.is_empty() {
            return Ok(Vec::new());
        }

        // Collect unique sender tuples (contract, wallet, token_id) that need checking
        // Skip zero addresses (mints)
        let sender_tuples: Vec<(Felt, Felt, U256)> = transfers
            .iter()
            .filter(|t| t.from != Felt::ZERO)
            .map(|t| (t.token, t.from, t.token_id))
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        // Get current balances for all senders
        let current_balances = self.get_balances_batch(&sender_tuples).await?;

        // Track running balance changes within this batch
        let mut pending_debits: HashMap<(Felt, Felt, U256), U256> = HashMap::new();

        let mut adjustment_requests = Vec::new();

        for transfer in transfers {
            if transfer.from == Felt::ZERO {
                continue; // Skip mints
            }

            let key = (transfer.token, transfer.from, transfer.token_id);

            // Get current stored balance (default to 0)
            let stored_balance = current_balances
                .get(&key)
                .copied()
                .unwrap_or(U256::from(0u64));

            // Add any pending debits from earlier in this batch
            let total_pending = pending_debits
                .get(&key)
                .copied()
                .unwrap_or(U256::from(0u64));

            // Check if balance would go negative
            let total_needed = total_pending + transfer.amount;

            if stored_balance >= total_needed {
                // Balance is sufficient, track the debit
                pending_debits.insert(key, total_needed);
            } else {
                // Balance would go negative - need to fetch actual balance
                let block_before = transfer.block_number.saturating_sub(1);
                let already_requested =
                    adjustment_requests
                        .iter()
                        .any(|r: &Erc1155BalanceFetchRequest| {
                            r.contract == transfer.token
                                && r.wallet == transfer.from
                                && r.token_id == transfer.token_id
                                && r.block_number == block_before
                        });

                if !already_requested {
                    adjustment_requests.push(Erc1155BalanceFetchRequest {
                        contract: transfer.token,
                        wallet: transfer.from,
                        token_id: transfer.token_id,
                        block_number: block_before,
                    });
                }
            }
        }

        if !adjustment_requests.is_empty() {
            tracing::info!(
                target: "torii_erc1155::storage",
                count = adjustment_requests.len(),
                "Detected balance inconsistencies, will fetch from RPC"
            );
        }

        Ok(adjustment_requests)
    }

    /// Apply transfers with adjustments and update balances
    ///
    /// # Arguments
    /// * `transfers` - The transfers to apply
    /// * `adjustments` - Map of (contract, wallet, token_id) -> actual_balance fetched from RPC
    pub async fn apply_transfers_with_adjustments(
        &self,
        transfers: &[TokenTransferData],
        adjustments: &HashMap<(Felt, Felt, U256), U256>,
    ) -> Result<()> {
        if self.backend == StorageBackend::Postgres {
            return self
                .pg_apply_transfers_with_adjustments(transfers, adjustments)
                .await;
        }
        if transfers.is_empty() {
            return Ok(());
        }

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        // Track balance changes in memory first
        let mut balance_cache: HashMap<(Felt, Felt, U256), U256> = HashMap::new();

        // Load existing balances for all affected wallets
        {
            let mut stmt = tx.prepare_cached(
                "SELECT balance FROM erc1155_balances
                 WHERE contract = ? AND wallet = ? AND token_id = ?",
            )?;

            for transfer in transfers {
                // Load sender balance (if not zero address)
                if transfer.from != Felt::ZERO {
                    let key = (transfer.token, transfer.from, transfer.token_id);
                    if let std::collections::hash_map::Entry::Vacant(e) = balance_cache.entry(key) {
                        let contract_blob = felt_to_blob(transfer.token);
                        let wallet_blob = felt_to_blob(transfer.from);
                        let token_id_blob = u256_to_blob(transfer.token_id);
                        let balance: U256 = stmt
                            .query_row(
                                params![&contract_blob, &wallet_blob, &token_id_blob],
                                |row| {
                                    let bytes: Vec<u8> = row.get(0)?;
                                    Ok(blob_to_u256(&bytes))
                                },
                            )
                            .unwrap_or(U256::from(0u64));
                        e.insert(balance);
                    }
                }

                // Load receiver balance (if not zero address)
                if transfer.to != Felt::ZERO {
                    let key = (transfer.token, transfer.to, transfer.token_id);
                    if let std::collections::hash_map::Entry::Vacant(e) = balance_cache.entry(key) {
                        let contract_blob = felt_to_blob(transfer.token);
                        let wallet_blob = felt_to_blob(transfer.to);
                        let token_id_blob = u256_to_blob(transfer.token_id);
                        let balance: U256 = stmt
                            .query_row(
                                params![&contract_blob, &wallet_blob, &token_id_blob],
                                |row| {
                                    let bytes: Vec<u8> = row.get(0)?;
                                    Ok(blob_to_u256(&bytes))
                                },
                            )
                            .unwrap_or(U256::from(0u64));
                        e.insert(balance);
                    }
                }
            }
        }

        // Apply adjustments - these are the "corrected" starting balances
        let mut adjustments_to_record: Vec<Erc1155BalanceAdjustment> = Vec::new();

        for ((contract, wallet, token_id), actual_balance) in adjustments {
            let key = (*contract, *wallet, *token_id);
            let computed = balance_cache.get(&key).copied().unwrap_or(U256::from(0u64));

            if computed != *actual_balance {
                let triggering_transfer = transfers
                    .iter()
                    .find(|t| t.token == *contract && t.from == *wallet && t.token_id == *token_id);

                if let Some(transfer) = triggering_transfer {
                    adjustments_to_record.push(Erc1155BalanceAdjustment {
                        contract: *contract,
                        wallet: *wallet,
                        token_id: *token_id,
                        computed_balance: computed,
                        actual_balance: *actual_balance,
                        adjusted_at_block: transfer.block_number,
                        tx_hash: transfer.tx_hash,
                    });
                }
            }

            balance_cache.insert(key, *actual_balance);
        }

        // Apply transfers to balances
        let mut last_block_per_key: HashMap<(Felt, Felt, U256), u64> = HashMap::new();

        for transfer in transfers {
            // Debit sender (if not mint)
            if transfer.from != Felt::ZERO {
                let key = (transfer.token, transfer.from, transfer.token_id);
                let current = balance_cache.get(&key).copied().unwrap_or(U256::from(0u64));
                // Saturating subtract: return 0 if would underflow
                let new_balance = if current >= transfer.amount {
                    current - transfer.amount
                } else {
                    U256::from(0u64)
                };
                balance_cache.insert(key, new_balance);
                last_block_per_key.insert(key, transfer.block_number);
            }

            // Credit receiver (if not burn)
            if transfer.to != Felt::ZERO {
                let key = (transfer.token, transfer.to, transfer.token_id);
                let current = balance_cache.get(&key).copied().unwrap_or(U256::from(0u64));
                // For addition, we just add (overflow is extremely unlikely for token balances)
                let new_balance = current + transfer.amount;
                balance_cache.insert(key, new_balance);
                last_block_per_key.insert(key, transfer.block_number);
            }
        }

        // Write balances to database
        {
            let mut upsert_stmt = tx.prepare_cached(
                "INSERT INTO erc1155_balances (contract, wallet, token_id, balance, last_block)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(contract, wallet, token_id) DO UPDATE SET
                     balance = excluded.balance,
                     last_block = excluded.last_block,
                     updated_at = strftime('%s', 'now')",
            )?;

            for ((contract, wallet, token_id), balance) in &balance_cache {
                let last_block = last_block_per_key
                    .get(&(*contract, *wallet, *token_id))
                    .copied()
                    .unwrap_or(0);

                let contract_blob = felt_to_blob(*contract);
                let wallet_blob = felt_to_blob(*wallet);
                let token_id_blob = u256_to_blob(*token_id);
                let balance_blob = u256_to_blob(*balance);

                upsert_stmt.execute(params![
                    &contract_blob,
                    &wallet_blob,
                    &token_id_blob,
                    &balance_blob,
                    last_block as i64,
                ])?;
            }
        }

        // Record adjustments for audit
        if !adjustments_to_record.is_empty() {
            let mut adj_stmt = tx.prepare_cached(
                "INSERT INTO erc1155_balance_adjustments
                 (contract, wallet, token_id, computed_balance, actual_balance, adjusted_at_block, tx_hash)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?;

            for adj in &adjustments_to_record {
                let contract_blob = felt_to_blob(adj.contract);
                let wallet_blob = felt_to_blob(adj.wallet);
                let token_id_blob = u256_to_blob(adj.token_id);
                let computed_blob = u256_to_blob(adj.computed_balance);
                let actual_blob = u256_to_blob(adj.actual_balance);
                let tx_hash_blob = felt_to_blob(adj.tx_hash);

                adj_stmt.execute(params![
                    &contract_blob,
                    &wallet_blob,
                    &token_id_blob,
                    &computed_blob,
                    &actual_blob,
                    adj.adjusted_at_block as i64,
                    &tx_hash_blob,
                ])?;
            }

            tracing::info!(
                target: "torii_erc1155::storage",
                count = adjustments_to_record.len(),
                "Applied balance adjustments (genesis/airdrop detection)"
            );
        }

        tx.commit()?;

        Ok(())
    }

    /// Get adjustment count (for statistics)
    pub async fn get_adjustment_count(&self) -> Result<u64> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_adjustment_count().await;
        }
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM erc1155_balance_adjustments",
            [],
            |row| row.get(0),
        )?;
        Ok(count as u64)
    }

    /// Get unique wallet count with balances
    pub async fn get_wallet_count(&self) -> Result<u64> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_wallet_count().await;
        }
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(DISTINCT wallet) FROM erc1155_balances",
            [],
            |row| row.get(0),
        )?;
        Ok(count as u64)
    }

    // ===== Token Metadata Methods =====

    /// Check if metadata exists for a token
    pub async fn has_token_metadata(&self, token: Felt) -> Result<bool> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_has_token_metadata(token).await;
        }
        let conn = self.conn.lock().unwrap();
        let token_blob = felt_to_blob(token);
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM token_metadata WHERE token = ?",
            params![&token_blob],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Insert or update token metadata
    pub async fn upsert_token_metadata(
        &self,
        token: Felt,
        name: Option<&str>,
        symbol: Option<&str>,
        total_supply: Option<U256>,
    ) -> Result<()> {
        if self.backend == StorageBackend::Postgres {
            return self
                .pg_upsert_token_metadata(token, name, symbol, total_supply)
                .await;
        }
        let conn = self.conn.lock().unwrap();
        let token_blob = felt_to_blob(token);
        let supply_blob = total_supply.map(u256_to_blob);
        conn.execute(
            "INSERT INTO token_metadata (token, name, symbol, total_supply)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(token) DO UPDATE SET
                 name = COALESCE(excluded.name, token_metadata.name),
                 symbol = COALESCE(excluded.symbol, token_metadata.symbol),
                 total_supply = COALESCE(excluded.total_supply, token_metadata.total_supply)",
            params![&token_blob, name, symbol, supply_blob],
        )?;
        Ok(())
    }

    /// Get token metadata
    pub async fn get_token_metadata(
        &self,
        token: Felt,
    ) -> Result<Option<(Option<String>, Option<String>, Option<U256>)>> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_token_metadata(token).await;
        }
        let conn = self.conn.lock().unwrap();
        let token_blob = felt_to_blob(token);
        let result = conn
            .query_row(
                "SELECT name, symbol, total_supply FROM token_metadata WHERE token = ?",
                params![&token_blob],
                |row| {
                    let name: Option<String> = row.get(0)?;
                    let symbol: Option<String> = row.get(1)?;
                    let supply_bytes: Option<Vec<u8>> = row.get(2)?;
                    Ok((name, symbol, supply_bytes.map(|b| blob_to_u256(&b))))
                },
            )
            .ok();
        Ok(result)
    }

    /// Get all token metadata
    pub async fn get_all_token_metadata(
        &self,
    ) -> Result<Vec<(Felt, Option<String>, Option<String>, Option<U256>)>> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_all_token_metadata().await;
        }
        let conn = self.conn.lock().unwrap();
        let mut stmt =
            conn.prepare("SELECT token, name, symbol, total_supply FROM token_metadata")?;
        let rows = stmt.query_map([], |row| {
            let token_bytes: Vec<u8> = row.get(0)?;
            let name: Option<String> = row.get(1)?;
            let symbol: Option<String> = row.get(2)?;
            let supply_bytes: Option<Vec<u8>> = row.get(3)?;
            Ok((
                blob_to_felt(&token_bytes),
                name,
                symbol,
                supply_bytes.map(|b| blob_to_u256(&b)),
            ))
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// Get token metadata with cursor-based pagination.
    ///
    /// Returns at most `limit` rows and an optional next cursor token.
    pub async fn get_token_metadata_paginated(
        &self,
        cursor: Option<Felt>,
        limit: u32,
    ) -> Result<(
        Vec<(Felt, Option<String>, Option<String>, Option<U256>)>,
        Option<Felt>,
    )> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_token_metadata_paginated(cursor, limit).await;
        }
        let conn = self.conn.lock().unwrap();
        let fetch_limit = limit.clamp(1, 1000) as usize + 1;

        let mut out = if let Some(cursor_token) = cursor {
            let cursor_blob = felt_to_blob(cursor_token);
            let mut stmt = conn.prepare(
                "SELECT token, name, symbol, total_supply
                 FROM token_metadata
                 WHERE token > ?1
                 ORDER BY token ASC
                 LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![&cursor_blob, fetch_limit as i64], |row| {
                let token_bytes: Vec<u8> = row.get(0)?;
                let name: Option<String> = row.get(1)?;
                let symbol: Option<String> = row.get(2)?;
                let supply_bytes: Option<Vec<u8>> = row.get(3)?;
                Ok((
                    blob_to_felt(&token_bytes),
                    name,
                    symbol,
                    supply_bytes.map(|b| blob_to_u256(&b)),
                ))
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        } else {
            let mut stmt = conn.prepare(
                "SELECT token, name, symbol, total_supply
                 FROM token_metadata
                 ORDER BY token ASC
                 LIMIT ?1",
            )?;
            let rows = stmt.query_map(params![fetch_limit as i64], |row| {
                let token_bytes: Vec<u8> = row.get(0)?;
                let name: Option<String> = row.get(1)?;
                let symbol: Option<String> = row.get(2)?;
                let supply_bytes: Option<Vec<u8>> = row.get(3)?;
                Ok((
                    blob_to_felt(&token_bytes),
                    name,
                    symbol,
                    supply_bytes.map(|b| blob_to_u256(&b)),
                ))
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };

        let capped = limit.clamp(1, 1000) as usize;
        let next_cursor = if out.len() > capped {
            let next = out[capped].0;
            out.truncate(capped);
            Some(next)
        } else {
            None
        };

        Ok((out, next_cursor))
    }

    /// Returns true if a token URI row exists for `(token, token_id)`.
    pub async fn has_token_uri(&self, token: Felt, token_id: U256) -> Result<bool> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_has_token_uri(token, token_id).await;
        }
        let conn = self.conn.lock().unwrap();
        let token_blob = felt_to_blob(token);
        let token_id_blob = u256_to_blob(token_id);
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM token_uris WHERE token = ?1 AND token_id = ?2",
            params![&token_blob, &token_id_blob],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Returns all token URI rows for a given contract.
    pub async fn get_token_uris_by_contract(
        &self,
        token: Felt,
    ) -> Result<Vec<(U256, Option<String>, Option<String>)>> {
        if self.backend == StorageBackend::Postgres {
            return self.pg_get_token_uris_by_contract(token).await;
        }
        let conn = self.conn.lock().unwrap();
        let token_blob = felt_to_blob(token);
        let mut stmt = conn.prepare(
            "SELECT token_id, uri, metadata_json
             FROM token_uris
             WHERE token = ?1
             ORDER BY token_id ASC",
        )?;
        let rows = stmt.query_map(params![&token_blob], |row| {
            let token_id_bytes: Vec<u8> = row.get(0)?;
            let uri: Option<String> = row.get(1)?;
            let metadata_json: Option<String> = row.get(2)?;
            Ok((blob_to_u256(&token_id_bytes), uri, metadata_json))
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    async fn pg_client(&self) -> Result<tokio::sync::MutexGuard<'_, Client>> {
        let conn = self
            .pg_conn
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("PostgreSQL connection not initialized"))?;
        Ok(conn.lock().await)
    }

    fn pg_next_param(
        params: &mut Vec<Box<dyn PgToSql + Sync + Send>>,
        value: impl PgToSql + Sync + Send + 'static,
    ) -> String {
        params.push(Box::new(value));
        format!("${}", params.len())
    }

    fn normalize_attribute_filters(
        filters: &[(String, Vec<String>)],
    ) -> Vec<(String, Vec<String>)> {
        filters
            .iter()
            .filter_map(|(key, values)| {
                let key = key.trim();
                if key.is_empty() {
                    return None;
                }

                let mut out_values: Vec<String> = Vec::new();
                for value in values {
                    let value = value.trim();
                    if value.is_empty() || out_values.iter().any(|v| v == value) {
                        continue;
                    }
                    out_values.push(value.to_owned());
                }

                if out_values.is_empty() {
                    None
                } else {
                    Some((key.to_owned(), out_values))
                }
            })
            .collect()
    }

    fn sqlite_append_attribute_exists_filters(
        query: &mut String,
        params: &mut Vec<Box<dyn rusqlite::ToSql>>,
        filters: &[(String, Vec<String>)],
        parent_alias: &str,
    ) {
        for (key, values) in filters {
            query.push_str(&format!(
                " AND EXISTS (
                     SELECT 1 FROM token_attributes a
                     WHERE a.token = {parent_alias}.token
                       AND a.token_id = {parent_alias}.token_id
                       AND a.key = ? AND a.value IN ("
            ));
            params.push(Box::new(key.clone()));
            let placeholders = values.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            query.push_str(&placeholders);
            query.push_str("))");
            for value in values {
                params.push(Box::new(value.clone()));
            }
        }
    }

    fn pg_append_attribute_exists_filters(
        query: &mut String,
        params: &mut Vec<Box<dyn PgToSql + Sync + Send>>,
        filters: &[(String, Vec<String>)],
        parent_alias: &str,
    ) {
        for (key, values) in filters {
            query.push_str(&format!(
                " AND EXISTS (
                     SELECT 1 FROM erc1155.token_attributes a
                     WHERE a.token = {parent_alias}.token
                       AND a.token_id = {parent_alias}.token_id
                       AND a.key = "
            ));
            query.push_str(&Self::pg_next_param(params, key.clone()));
            query.push_str(" AND a.value IN (");
            let value_placeholders = values
                .iter()
                .map(|value| Self::pg_next_param(params, value.clone()))
                .collect::<Vec<_>>()
                .join(",");
            query.push_str(&value_placeholders);
            query.push_str("))");
        }
    }

    async fn pg_insert_transfers_batch(&self, transfers: &[TokenTransferData]) -> Result<usize> {
        if transfers.is_empty() {
            return Ok(0);
        }

        let mut client = self.pg_client().await?;
        let tx = client.transaction().await?;
        let mut inserted = 0usize;

        for transfer in transfers {
            let token_blob = felt_to_blob(transfer.token);
            let operator_blob = felt_to_blob(transfer.operator);
            let from_blob = felt_to_blob(transfer.from);
            let to_blob = felt_to_blob(transfer.to);
            let token_id_blob = u256_to_blob(transfer.token_id);
            let amount_blob = u256_to_blob(transfer.amount);
            let tx_hash_blob = felt_to_blob(transfer.tx_hash);
            let ts = transfer
                .timestamp
                .unwrap_or_else(|| chrono::Utc::now().timestamp());

            let row = tx.query_opt(
                "INSERT INTO erc1155.token_transfers
                 (token, operator, from_addr, to_addr, token_id, amount, is_batch, batch_index, block_number, tx_hash, timestamp)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                 ON CONFLICT (token, tx_hash, token_id, from_addr, to_addr, batch_index) DO NOTHING
                 RETURNING id",
                &[
                    &token_blob,
                    &operator_blob,
                    &from_blob,
                    &to_blob,
                    &token_id_blob,
                    &amount_blob,
                    &((transfer.is_batch as i32) as i64),
                    &(transfer.batch_index as i64),
                    &(transfer.block_number as i64),
                    &tx_hash_blob,
                    &ts,
                ],
            ).await?;

            if let Some(row) = row {
                inserted += 1;
                let transfer_id: i64 = row.get(0);

                if transfer.from != Felt::ZERO
                    && transfer.to != Felt::ZERO
                    && transfer.from == transfer.to
                {
                    tx.execute(
                        "INSERT INTO erc1155.token_wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                         VALUES ($1, $2, $3, 'both', $4)",
                        &[
                            &from_blob,
                            &token_blob,
                            &transfer_id,
                            &(transfer.block_number as i64),
                        ],
                    ).await?;
                } else {
                    if transfer.from != Felt::ZERO {
                        tx.execute(
                            "INSERT INTO erc1155.token_wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                             VALUES ($1, $2, $3, 'sent', $4)",
                            &[
                                &from_blob,
                                &token_blob,
                                &transfer_id,
                                &(transfer.block_number as i64),
                            ],
                        ).await?;
                    }
                    if transfer.to != Felt::ZERO {
                        tx.execute(
                            "INSERT INTO erc1155.token_wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                             VALUES ($1, $2, $3, 'received', $4)",
                            &[
                                &to_blob,
                                &token_blob,
                                &transfer_id,
                                &(transfer.block_number as i64),
                            ],
                        ).await?;
                    }
                }
            }
        }

        tx.commit().await?;
        Ok(inserted)
    }

    async fn pg_insert_operator_approvals_batch(
        &self,
        approvals: &[OperatorApprovalData],
    ) -> Result<usize> {
        if approvals.is_empty() {
            return Ok(0);
        }
        let mut client = self.pg_client().await?;
        let tx = client.transaction().await?;
        let mut inserted = 0usize;

        for approval in approvals {
            tx.execute(
                "INSERT INTO erc1155.token_operators (token, owner, operator, approved, block_number, tx_hash, timestamp)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (token, owner, operator) DO UPDATE SET
                    approved = EXCLUDED.approved,
                    block_number = EXCLUDED.block_number,
                    tx_hash = EXCLUDED.tx_hash,
                    timestamp = EXCLUDED.timestamp",
                &[
                    &felt_to_blob(approval.token),
                    &felt_to_blob(approval.owner),
                    &felt_to_blob(approval.operator),
                    &((approval.approved as i32) as i64),
                    &(approval.block_number as i64),
                    &felt_to_blob(approval.tx_hash),
                    &approval
                        .timestamp
                        .unwrap_or_else(|| chrono::Utc::now().timestamp()),
                ],
            ).await?;
            inserted += 1;
        }

        tx.commit().await?;
        Ok(inserted)
    }

    async fn pg_upsert_token_uris_batch(&self, uris: &[TokenUriData]) -> Result<usize> {
        if uris.is_empty() {
            return Ok(0);
        }
        let mut client = self.pg_client().await?;
        let tx = client.transaction().await?;
        let mut updated = 0usize;

        for entry in uris {
            let rows = tx
                .execute(
                    "INSERT INTO erc1155.token_uris (token, token_id, uri, updated_at)
                 VALUES ($1, $2, $3, EXTRACT(EPOCH FROM NOW())::BIGINT)
                 ON CONFLICT(token, token_id) DO UPDATE SET
                    uri = EXCLUDED.uri,
                    updated_at = EXCLUDED.updated_at",
                    &[
                        &felt_to_blob(entry.token),
                        &u256_to_blob(entry.token_id),
                        &entry.uri,
                    ],
                )
                .await?;
            if rows > 0 {
                updated += 1;
            }
        }

        tx.commit().await?;
        Ok(updated)
    }

    #[allow(clippy::too_many_arguments)]
    async fn pg_get_transfers_filtered(
        &self,
        wallet: Option<Felt>,
        from: Option<Felt>,
        to: Option<Felt>,
        operator: Option<Felt>,
        tokens: &[Felt],
        token_ids: &[U256],
        block_from: Option<u64>,
        block_to: Option<u64>,
        cursor: Option<TransferCursor>,
        limit: u32,
    ) -> Result<(Vec<TokenTransferData>, Option<TransferCursor>)> {
        let client = self.pg_client().await?;
        let mut query = String::new();
        let mut params: Vec<Box<dyn PgToSql + Sync + Send>> = Vec::new();

        if let Some(wallet_addr) = wallet {
            query.push_str(
                "SELECT DISTINCT t.id, t.token, t.operator, t.from_addr, t.to_addr, t.token_id, t.amount, t.is_batch, t.batch_index, t.block_number, t.tx_hash, t.timestamp
                 FROM erc1155.token_wallet_activity wa
                 JOIN erc1155.token_transfers t ON wa.transfer_id = t.id
                 WHERE wa.wallet_address = ",
            );
            query.push_str(&Self::pg_next_param(&mut params, felt_to_blob(wallet_addr)));
            if !tokens.is_empty() {
                let list = tokens
                    .iter()
                    .map(|token| Self::pg_next_param(&mut params, felt_to_blob(*token)))
                    .collect::<Vec<_>>()
                    .join(",");
                query.push_str(&format!(" AND wa.token IN ({list})"));
            }
        } else {
            query.push_str(
                "SELECT t.id, t.token, t.operator, t.from_addr, t.to_addr, t.token_id, t.amount, t.is_batch, t.batch_index, t.block_number, t.tx_hash, t.timestamp
                 FROM erc1155.token_transfers t
                 WHERE 1=1",
            );
            if let Some(from_addr) = from {
                query.push_str(" AND t.from_addr = ");
                query.push_str(&Self::pg_next_param(&mut params, felt_to_blob(from_addr)));
            }
            if let Some(to_addr) = to {
                query.push_str(" AND t.to_addr = ");
                query.push_str(&Self::pg_next_param(&mut params, felt_to_blob(to_addr)));
            }
            if let Some(op_addr) = operator {
                query.push_str(" AND t.operator = ");
                query.push_str(&Self::pg_next_param(&mut params, felt_to_blob(op_addr)));
            }
            if !tokens.is_empty() {
                let list = tokens
                    .iter()
                    .map(|token| Self::pg_next_param(&mut params, felt_to_blob(*token)))
                    .collect::<Vec<_>>()
                    .join(",");
                query.push_str(&format!(" AND t.token IN ({list})"));
            }
        }

        if !token_ids.is_empty() {
            let list = token_ids
                .iter()
                .map(|tid| Self::pg_next_param(&mut params, u256_to_blob(*tid)))
                .collect::<Vec<_>>()
                .join(",");
            query.push_str(&format!(" AND t.token_id IN ({list})"));
        }
        if let Some(block_min) = block_from {
            query.push_str(" AND t.block_number >= ");
            query.push_str(&Self::pg_next_param(&mut params, block_min as i64));
        }
        if let Some(block_max) = block_to {
            query.push_str(" AND t.block_number <= ");
            query.push_str(&Self::pg_next_param(&mut params, block_max as i64));
        }
        if let Some(c) = cursor {
            let p1 = Self::pg_next_param(&mut params, c.block_number as i64);
            let p2 = Self::pg_next_param(&mut params, c.block_number as i64);
            let p3 = Self::pg_next_param(&mut params, c.id);
            query.push_str(&format!(
                " AND (t.block_number < {p1} OR (t.block_number = {p2} AND t.id < {p3}))"
            ));
        }
        query.push_str(" ORDER BY t.block_number DESC, t.id DESC LIMIT ");
        query.push_str(&Self::pg_next_param(&mut params, limit as i64));

        let refs: Vec<&(dyn PgToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn PgToSql + Sync))
            .collect();
        let rows = client.query(&query, &refs).await?;
        let transfers: Vec<TokenTransferData> = rows
            .into_iter()
            .map(|row| TokenTransferData {
                id: Some(row.get::<usize, i64>(0)),
                token: blob_to_felt(&row.get::<usize, Vec<u8>>(1)),
                operator: blob_to_felt(&row.get::<usize, Vec<u8>>(2)),
                from: blob_to_felt(&row.get::<usize, Vec<u8>>(3)),
                to: blob_to_felt(&row.get::<usize, Vec<u8>>(4)),
                token_id: blob_to_u256(&row.get::<usize, Vec<u8>>(5)),
                amount: blob_to_u256(&row.get::<usize, Vec<u8>>(6)),
                is_batch: row.get::<usize, i64>(7) != 0,
                batch_index: row.get::<usize, i64>(8) as u32,
                block_number: row.get::<usize, i64>(9) as u64,
                tx_hash: blob_to_felt(&row.get::<usize, Vec<u8>>(10)),
                timestamp: Some(row.get::<usize, i64>(11)),
            })
            .collect();

        let next_cursor = if transfers.len() == limit as usize {
            transfers.last().map(|t| TransferCursor {
                block_number: t.block_number,
                id: t.id.unwrap_or_default(),
            })
        } else {
            None
        };

        Ok((transfers, next_cursor))
    }

    async fn pg_query_token_ids_by_attributes(
        &self,
        token: Felt,
        filters: &[(String, Vec<String>)],
        cursor_token_id: Option<U256>,
        page_fetch: i64,
        include_facets: bool,
        facet_limit: i64,
    ) -> Result<TokenAttributeQueryResult> {
        let client = self.pg_client().await?;

        let mut query =
            String::from("SELECT u.token_id FROM erc1155.token_uris u WHERE u.token = ");
        let mut params: Vec<Box<dyn PgToSql + Sync + Send>> = Vec::new();
        query.push_str(&Self::pg_next_param(&mut params, felt_to_blob(token)));
        if let Some(cursor) = cursor_token_id {
            query.push_str(" AND u.token_id > ");
            query.push_str(&Self::pg_next_param(&mut params, u256_to_blob(cursor)));
        }
        Self::pg_append_attribute_exists_filters(&mut query, &mut params, filters, "u");
        query.push_str(" ORDER BY u.token_id ASC LIMIT ");
        query.push_str(&Self::pg_next_param(&mut params, page_fetch));

        let refs: Vec<&(dyn PgToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn PgToSql + Sync))
            .collect();
        let rows = client.query(&query, &refs).await?;
        let mut token_ids: Vec<U256> = rows
            .into_iter()
            .map(|row| blob_to_u256(&row.get::<usize, Vec<u8>>(0)))
            .collect();

        let page_limit = (page_fetch - 1) as usize;
        let next_cursor_token_id = if token_ids.len() > page_limit {
            let next = token_ids[page_limit];
            token_ids.truncate(page_limit);
            Some(next)
        } else {
            None
        };

        let mut count_query =
            String::from("SELECT COUNT(*) FROM erc1155.token_uris u WHERE u.token = ");
        let mut count_params: Vec<Box<dyn PgToSql + Sync + Send>> = Vec::new();
        count_query.push_str(&Self::pg_next_param(&mut count_params, felt_to_blob(token)));
        Self::pg_append_attribute_exists_filters(&mut count_query, &mut count_params, filters, "u");
        let count_refs: Vec<&(dyn PgToSql + Sync)> = count_params
            .iter()
            .map(|p| p.as_ref() as &(dyn PgToSql + Sync))
            .collect();
        let total_row = client.query_one(&count_query, &count_refs).await?;
        let total_hits = total_row.get::<usize, i64>(0) as u64;

        let facets = if include_facets {
            let mut facet_query = String::from(
                "SELECT a.key, a.value, COUNT(*) AS cnt
                 FROM erc1155.token_attributes a
                 JOIN erc1155.token_uris u ON u.token = a.token AND u.token_id = a.token_id
                 WHERE u.token = ",
            );
            let mut facet_params: Vec<Box<dyn PgToSql + Sync + Send>> = Vec::new();
            facet_query.push_str(&Self::pg_next_param(&mut facet_params, felt_to_blob(token)));
            Self::pg_append_attribute_exists_filters(
                &mut facet_query,
                &mut facet_params,
                filters,
                "u",
            );
            facet_query.push_str(
                " GROUP BY a.key, a.value
                  ORDER BY cnt DESC, a.key ASC, a.value ASC
                  LIMIT ",
            );
            facet_query.push_str(&Self::pg_next_param(&mut facet_params, facet_limit));
            let facet_refs: Vec<&(dyn PgToSql + Sync)> = facet_params
                .iter()
                .map(|p| p.as_ref() as &(dyn PgToSql + Sync))
                .collect();
            let rows = client.query(&facet_query, &facet_refs).await?;
            rows.into_iter()
                .map(|row| AttributeFacetCount {
                    key: row.get(0),
                    value: row.get(1),
                    count: row.get::<usize, i64>(2) as u64,
                })
                .collect()
        } else {
            Vec::new()
        };

        Ok(TokenAttributeQueryResult {
            token_ids,
            next_cursor_token_id,
            total_hits,
            facets,
        })
    }

    async fn pg_get_transfer_count(&self) -> Result<u64> {
        let client = self.pg_client().await?;
        let row = client
            .query_one("SELECT COUNT(*) FROM erc1155.token_transfers", &[])
            .await?;
        Ok(row.get::<usize, i64>(0) as u64)
    }

    async fn pg_get_token_count(&self) -> Result<u64> {
        let client = self.pg_client().await?;
        let row = client
            .query_one(
                "SELECT COUNT(DISTINCT token) FROM erc1155.token_transfers",
                &[],
            )
            .await?;
        Ok(row.get::<usize, i64>(0) as u64)
    }

    async fn pg_get_token_id_count(&self) -> Result<u64> {
        let client = self.pg_client().await?;
        let row = client.query_one(
            "SELECT COUNT(*) FROM (SELECT DISTINCT token, token_id FROM erc1155.token_transfers) t",
            &[],
        ).await?;
        Ok(row.get::<usize, i64>(0) as u64)
    }

    async fn pg_get_latest_block(&self) -> Result<Option<u64>> {
        let client = self.pg_client().await?;
        let row = client
            .query_one("SELECT MAX(block_number) FROM erc1155.token_transfers", &[])
            .await?;
        let v: Option<i64> = row.get(0);
        Ok(v.map(|x| x as u64))
    }

    async fn pg_get_balance(
        &self,
        contract: Felt,
        wallet: Felt,
        token_id: U256,
    ) -> Result<Option<U256>> {
        let client = self.pg_client().await?;
        let row = client
            .query_opt(
                "SELECT balance FROM erc1155.erc1155_balances
             WHERE contract = $1 AND wallet = $2 AND token_id = $3",
                &[
                    &felt_to_blob(contract),
                    &felt_to_blob(wallet),
                    &u256_to_blob(token_id),
                ],
            )
            .await?;
        Ok(row.map(|r| blob_to_u256(&r.get::<usize, Vec<u8>>(0))))
    }

    async fn pg_get_balance_with_block(
        &self,
        contract: Felt,
        wallet: Felt,
        token_id: U256,
    ) -> Result<Option<(U256, u64)>> {
        let client = self.pg_client().await?;
        let row = client
            .query_opt(
                "SELECT balance, last_block FROM erc1155.erc1155_balances
             WHERE contract = $1 AND wallet = $2 AND token_id = $3",
                &[
                    &felt_to_blob(contract),
                    &felt_to_blob(wallet),
                    &u256_to_blob(token_id),
                ],
            )
            .await?;
        Ok(row.map(|r| {
            (
                blob_to_u256(&r.get::<usize, Vec<u8>>(0)),
                r.get::<usize, i64>(1) as u64,
            )
        }))
    }

    async fn pg_get_balances_batch(
        &self,
        tuples: &[(Felt, Felt, U256)],
    ) -> Result<HashMap<(Felt, Felt, U256), U256>> {
        if tuples.is_empty() {
            return Ok(HashMap::new());
        }

        let client = self.pg_client().await?;
        let mut result = HashMap::new();

        for (contract, wallet, token_id) in tuples {
            let row = client
                .query_opt(
                    "SELECT balance FROM erc1155.erc1155_balances
                 WHERE contract = $1 AND wallet = $2 AND token_id = $3",
                    &[
                        &felt_to_blob(*contract),
                        &felt_to_blob(*wallet),
                        &u256_to_blob(*token_id),
                    ],
                )
                .await?;

            if let Some(row) = row {
                result.insert(
                    (*contract, *wallet, *token_id),
                    blob_to_u256(&row.get::<usize, Vec<u8>>(0)),
                );
            }
        }

        Ok(result)
    }

    async fn pg_apply_transfers_with_adjustments(
        &self,
        transfers: &[TokenTransferData],
        adjustments: &HashMap<(Felt, Felt, U256), U256>,
    ) -> Result<()> {
        if transfers.is_empty() {
            return Ok(());
        }

        let mut client = self.pg_client().await?;
        let tx = client.transaction().await?;
        let mut balance_cache: HashMap<(Felt, Felt, U256), U256> = HashMap::new();

        for transfer in transfers {
            if transfer.from != Felt::ZERO {
                let key = (transfer.token, transfer.from, transfer.token_id);
                if let std::collections::hash_map::Entry::Vacant(e) = balance_cache.entry(key) {
                    let row = tx
                        .query_opt(
                            "SELECT balance FROM erc1155.erc1155_balances
                         WHERE contract = $1 AND wallet = $2 AND token_id = $3",
                            &[
                                &felt_to_blob(transfer.token),
                                &felt_to_blob(transfer.from),
                                &u256_to_blob(transfer.token_id),
                            ],
                        )
                        .await?;
                    let balance = row
                        .map(|r| blob_to_u256(&r.get::<usize, Vec<u8>>(0)))
                        .unwrap_or(U256::from(0u64));
                    e.insert(balance);
                }
            }

            if transfer.to != Felt::ZERO {
                let key = (transfer.token, transfer.to, transfer.token_id);
                if let std::collections::hash_map::Entry::Vacant(e) = balance_cache.entry(key) {
                    let row = tx
                        .query_opt(
                            "SELECT balance FROM erc1155.erc1155_balances
                         WHERE contract = $1 AND wallet = $2 AND token_id = $3",
                            &[
                                &felt_to_blob(transfer.token),
                                &felt_to_blob(transfer.to),
                                &u256_to_blob(transfer.token_id),
                            ],
                        )
                        .await?;
                    let balance = row
                        .map(|r| blob_to_u256(&r.get::<usize, Vec<u8>>(0)))
                        .unwrap_or(U256::from(0u64));
                    e.insert(balance);
                }
            }
        }

        let mut adjustments_to_record: Vec<Erc1155BalanceAdjustment> = Vec::new();
        for ((contract, wallet, token_id), actual_balance) in adjustments {
            let key = (*contract, *wallet, *token_id);
            let computed = balance_cache.get(&key).copied().unwrap_or(U256::from(0u64));

            if computed != *actual_balance {
                let triggering_transfer = transfers
                    .iter()
                    .find(|t| t.token == *contract && t.from == *wallet && t.token_id == *token_id);

                if let Some(transfer) = triggering_transfer {
                    adjustments_to_record.push(Erc1155BalanceAdjustment {
                        contract: *contract,
                        wallet: *wallet,
                        token_id: *token_id,
                        computed_balance: computed,
                        actual_balance: *actual_balance,
                        adjusted_at_block: transfer.block_number,
                        tx_hash: transfer.tx_hash,
                    });
                }
            }

            balance_cache.insert(key, *actual_balance);
        }

        let mut last_block_per_key: HashMap<(Felt, Felt, U256), u64> = HashMap::new();
        for transfer in transfers {
            if transfer.from != Felt::ZERO {
                let key = (transfer.token, transfer.from, transfer.token_id);
                let current = balance_cache.get(&key).copied().unwrap_or(U256::from(0u64));
                let new_balance = if current >= transfer.amount {
                    current - transfer.amount
                } else {
                    U256::from(0u64)
                };
                balance_cache.insert(key, new_balance);
                last_block_per_key.insert(key, transfer.block_number);
            }

            if transfer.to != Felt::ZERO {
                let key = (transfer.token, transfer.to, transfer.token_id);
                let current = balance_cache.get(&key).copied().unwrap_or(U256::from(0u64));
                let new_balance = current + transfer.amount;
                balance_cache.insert(key, new_balance);
                last_block_per_key.insert(key, transfer.block_number);
            }
        }

        for ((contract, wallet, token_id), balance) in &balance_cache {
            let last_block = last_block_per_key
                .get(&(*contract, *wallet, *token_id))
                .copied()
                .unwrap_or(0);

            tx.execute(
                "INSERT INTO erc1155.erc1155_balances (contract, wallet, token_id, balance, last_block)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT(contract, wallet, token_id) DO UPDATE SET
                    balance = EXCLUDED.balance,
                    last_block = EXCLUDED.last_block,
                    updated_at = EXTRACT(EPOCH FROM NOW())::BIGINT",
                &[
                    &felt_to_blob(*contract),
                    &felt_to_blob(*wallet),
                    &u256_to_blob(*token_id),
                    &u256_to_blob(*balance),
                    &(last_block as i64),
                ],
            ).await?;
        }

        for adj in &adjustments_to_record {
            tx.execute(
                "INSERT INTO erc1155.erc1155_balance_adjustments
                 (contract, wallet, token_id, computed_balance, actual_balance, adjusted_at_block, tx_hash)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
                &[
                    &felt_to_blob(adj.contract),
                    &felt_to_blob(adj.wallet),
                    &u256_to_blob(adj.token_id),
                    &u256_to_blob(adj.computed_balance),
                    &u256_to_blob(adj.actual_balance),
                    &(adj.adjusted_at_block as i64),
                    &felt_to_blob(adj.tx_hash),
                ],
            ).await?;
        }

        if !adjustments_to_record.is_empty() {
            tracing::info!(
                target: "torii_erc1155::storage",
                count = adjustments_to_record.len(),
                "Applied balance adjustments (genesis/airdrop detection)"
            );
        }

        tx.commit().await?;
        Ok(())
    }

    async fn pg_get_adjustment_count(&self) -> Result<u64> {
        let client = self.pg_client().await?;
        let row = client
            .query_one(
                "SELECT COUNT(*) FROM erc1155.erc1155_balance_adjustments",
                &[],
            )
            .await?;
        Ok(row.get::<usize, i64>(0) as u64)
    }

    async fn pg_get_wallet_count(&self) -> Result<u64> {
        let client = self.pg_client().await?;
        let row = client
            .query_one(
                "SELECT COUNT(DISTINCT wallet) FROM erc1155.erc1155_balances",
                &[],
            )
            .await?;
        Ok(row.get::<usize, i64>(0) as u64)
    }

    async fn pg_has_token_metadata(&self, token: Felt) -> Result<bool> {
        let client = self.pg_client().await?;
        let row = client
            .query_one(
                "SELECT COUNT(*) FROM erc1155.token_metadata WHERE token = $1",
                &[&felt_to_blob(token)],
            )
            .await?;
        Ok(row.get::<usize, i64>(0) > 0)
    }

    async fn pg_upsert_token_metadata(
        &self,
        token: Felt,
        name: Option<&str>,
        symbol: Option<&str>,
        total_supply: Option<U256>,
    ) -> Result<()> {
        let client = self.pg_client().await?;
        client.execute(
            "INSERT INTO erc1155.token_metadata (token, name, symbol, total_supply)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (token) DO UPDATE SET
                name = COALESCE(EXCLUDED.name, erc1155.token_metadata.name),
                symbol = COALESCE(EXCLUDED.symbol, erc1155.token_metadata.symbol),
                total_supply = COALESCE(EXCLUDED.total_supply, erc1155.token_metadata.total_supply)",
            &[&felt_to_blob(token), &name, &symbol, &total_supply.map(u256_to_blob)],
        ).await?;
        Ok(())
    }

    async fn pg_get_token_metadata(
        &self,
        token: Felt,
    ) -> Result<Option<(Option<String>, Option<String>, Option<U256>)>> {
        let client = self.pg_client().await?;
        let row = client
            .query_opt(
                "SELECT name, symbol, total_supply FROM erc1155.token_metadata WHERE token = $1",
                &[&felt_to_blob(token)],
            )
            .await?;
        Ok(row.map(|r| {
            let supply: Option<Vec<u8>> = r.get(2);
            (r.get(0), r.get(1), supply.map(|b| blob_to_u256(&b)))
        }))
    }

    async fn pg_get_all_token_metadata(
        &self,
    ) -> Result<Vec<(Felt, Option<String>, Option<String>, Option<U256>)>> {
        let client = self.pg_client().await?;
        let rows = client
            .query(
                "SELECT token, name, symbol, total_supply
             FROM erc1155.token_metadata
             ORDER BY token ASC",
                &[],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let supply: Option<Vec<u8>> = row.get(3);
                (
                    blob_to_felt(&row.get::<usize, Vec<u8>>(0)),
                    row.get(1),
                    row.get(2),
                    supply.map(|b| blob_to_u256(&b)),
                )
            })
            .collect())
    }

    async fn pg_get_token_metadata_paginated(
        &self,
        cursor: Option<Felt>,
        limit: u32,
    ) -> Result<(
        Vec<(Felt, Option<String>, Option<String>, Option<U256>)>,
        Option<Felt>,
    )> {
        let client = self.pg_client().await?;
        let fetch_limit = limit.clamp(1, 1000) as i64 + 1;
        let rows = if let Some(cursor_token) = cursor {
            client
                .query(
                    "SELECT token, name, symbol, total_supply
                 FROM erc1155.token_metadata
                 WHERE token > $1
                 ORDER BY token ASC
                 LIMIT $2",
                    &[&felt_to_blob(cursor_token), &fetch_limit],
                )
                .await?
        } else {
            client
                .query(
                    "SELECT token, name, symbol, total_supply
                 FROM erc1155.token_metadata
                 ORDER BY token ASC
                 LIMIT $1",
                    &[&fetch_limit],
                )
                .await?
        };

        let mut out: Vec<(Felt, Option<String>, Option<String>, Option<U256>)> = rows
            .into_iter()
            .map(|row| {
                let supply: Option<Vec<u8>> = row.get(3);
                (
                    blob_to_felt(&row.get::<usize, Vec<u8>>(0)),
                    row.get(1),
                    row.get(2),
                    supply.map(|b| blob_to_u256(&b)),
                )
            })
            .collect();

        let capped = limit.clamp(1, 1000) as usize;
        let next_cursor = if out.len() > capped {
            let next = out[capped].0;
            out.truncate(capped);
            Some(next)
        } else {
            None
        };
        Ok((out, next_cursor))
    }

    async fn pg_has_token_uri(&self, token: Felt, token_id: U256) -> Result<bool> {
        let client = self.pg_client().await?;
        let row = client
            .query_one(
                "SELECT COUNT(*) FROM erc1155.token_uris WHERE token = $1 AND token_id = $2",
                &[&felt_to_blob(token), &u256_to_blob(token_id)],
            )
            .await?;
        Ok(row.get::<usize, i64>(0) > 0)
    }

    async fn pg_get_token_uris_by_contract(
        &self,
        token: Felt,
    ) -> Result<Vec<(U256, Option<String>, Option<String>)>> {
        let client = self.pg_client().await?;
        let rows = client
            .query(
                "SELECT token_id, uri, metadata_json
                 FROM erc1155.token_uris
                 WHERE token = $1
                 ORDER BY token_id ASC",
                &[&felt_to_blob(token)],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    blob_to_u256(&row.get::<usize, Vec<u8>>(0)),
                    row.get(1),
                    row.get(2),
                )
            })
            .collect())
    }
}

#[async_trait::async_trait]
impl TokenUriStore for Erc1155Storage {
    async fn store_token_uri(&self, result: &TokenUriResult) -> Result<()> {
        if self.backend == StorageBackend::Postgres {
            let mut client = self.pg_client().await?;
            let tx = client.transaction().await?;

            let token_blob = felt_to_blob(result.contract);
            let token_id_blob = u256_to_blob(result.token_id);

            tx.execute(
                "INSERT INTO erc1155.token_uris (token, token_id, uri, metadata_json, updated_at)
                 VALUES ($1, $2, $3, $4, EXTRACT(EPOCH FROM NOW())::BIGINT)
                 ON CONFLICT(token, token_id) DO UPDATE SET
                    uri = EXCLUDED.uri,
                    metadata_json = EXCLUDED.metadata_json,
                    updated_at = EXCLUDED.updated_at",
                &[
                    &token_blob,
                    &token_id_blob,
                    &result.uri.as_deref(),
                    &result.metadata_json.as_deref(),
                ],
            )
            .await?;

            tx.execute(
                "DELETE FROM erc1155.token_attributes WHERE token = $1 AND token_id = $2",
                &[&token_blob, &token_id_blob],
            )
            .await?;

            if let Some(metadata_json) = &result.metadata_json {
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(metadata_json) {
                    if let Some(attrs) = value.get("attributes").and_then(|a| a.as_array()) {
                        for attr in attrs {
                            let key = attr
                                .get("trait_type")
                                .or_else(|| attr.get("key"))
                                .and_then(|v| v.as_str());
                            let value = attr.get("value").and_then(|v| {
                                v.as_str().map(ToOwned::to_owned).or_else(|| {
                                    if v.is_null() {
                                        None
                                    } else {
                                        Some(v.to_string())
                                    }
                                })
                            });

                            let key = key.and_then(sanitize_metadata_text);
                            let value = value.and_then(|v| sanitize_metadata_text(&v));

                            if let (Some(key), Some(value)) = (key, value) {
                                tx.execute(
                                    "INSERT INTO erc1155.token_attributes (token, token_id, key, value)
                                     VALUES ($1, $2, $3, $4)
                                     ON CONFLICT (token, token_id, key) DO UPDATE SET value = EXCLUDED.value",
                                    &[&token_blob, &token_id_blob, &key, &value],
                                ).await?;
                            }
                        }
                    }
                }
            }

            tx.commit().await?;
            return Ok(());
        }

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let token_blob = felt_to_blob(result.contract);
        let token_id_blob = u256_to_blob(result.token_id);

        tx.execute(
            "INSERT INTO token_uris (token, token_id, uri, metadata_json, updated_at)
             VALUES (?1, ?2, ?3, ?4, strftime('%s', 'now'))
             ON CONFLICT(token, token_id) DO UPDATE SET
                uri = excluded.uri,
                metadata_json = excluded.metadata_json,
                updated_at = excluded.updated_at",
            params![
                &token_blob,
                &token_id_blob,
                result.uri.as_deref(),
                result.metadata_json.as_deref()
            ],
        )?;

        tx.execute(
            "DELETE FROM token_attributes WHERE token = ?1 AND token_id = ?2",
            params![&token_blob, &token_id_blob],
        )?;

        if let Some(metadata_json) = &result.metadata_json {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(metadata_json) {
                if let Some(attrs) = value.get("attributes").and_then(|a| a.as_array()) {
                    let mut attr_stmt = tx.prepare_cached(
                        "INSERT OR REPLACE INTO token_attributes (token, token_id, key, value)
                         VALUES (?1, ?2, ?3, ?4)",
                    )?;

                    for attr in attrs {
                        let key = attr
                            .get("trait_type")
                            .or_else(|| attr.get("key"))
                            .and_then(|v| v.as_str());
                        let value = attr.get("value").and_then(|v| {
                            v.as_str().map(ToOwned::to_owned).or_else(|| {
                                if v.is_null() {
                                    None
                                } else {
                                    Some(v.to_string())
                                }
                            })
                        });

                        let key = key.and_then(sanitize_metadata_text);
                        let value = value.and_then(|v| sanitize_metadata_text(&v));

                        if let (Some(key), Some(value)) = (key, value) {
                            attr_stmt.execute(params![&token_blob, &token_id_blob, key, value])?;
                        }
                    }
                }
            }
        }

        tx.commit()?;
        Ok(())
    }
}

fn sanitize_metadata_text(input: &str) -> Option<String> {
    let sanitized = input.replace('\0', "").trim().to_owned();
    if sanitized.is_empty() {
        None
    } else {
        Some(sanitized)
    }
}
