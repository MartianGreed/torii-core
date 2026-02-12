//! SQLite storage for ERC721 NFT transfers, approvals, and ownership
//!
//! Uses binary (BLOB) storage for efficiency.
//! Tracks current NFT ownership state (who owns each token).

use anyhow::Result;
use rusqlite::{params, Connection};
use starknet::core::types::{Felt, U256};
use std::sync::{Arc, Mutex};
use torii_common::{
    blob_to_felt, blob_to_u256, felt_to_blob, u256_to_blob, TokenUriResult, TokenUriStore,
};

/// Storage for ERC721 NFT data
pub struct Erc721Storage {
    conn: Arc<Mutex<Connection>>,
}

/// NFT transfer data for batch insertion
pub struct NftTransferData {
    pub id: Option<i64>,
    pub token: Felt,
    pub token_id: U256,
    pub from: Felt,
    pub to: Felt,
    pub block_number: u64,
    pub tx_hash: Felt,
    pub timestamp: Option<i64>,
}

/// NFT ownership data
pub struct NftOwnershipData {
    pub id: Option<i64>,
    pub token: Felt,
    pub token_id: U256,
    pub owner: Felt,
    pub block_number: u64,
}

/// NFT approval data
pub struct NftApprovalData {
    pub id: Option<i64>,
    pub token: Felt,
    pub token_id: U256,
    pub owner: Felt,
    pub approved: Felt,
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

/// Cursor for paginated transfer queries
#[derive(Debug, Clone, Copy)]
pub struct TransferCursor {
    pub block_number: u64,
    pub id: i64,
}

/// Cursor for paginated ownership queries
#[derive(Debug, Clone, Copy)]
pub struct OwnershipCursor {
    pub block_number: u64,
    pub id: i64,
}

impl Erc721Storage {
    /// Create or open the database
    pub fn new(db_path: &str) -> Result<Self> {
        let conn = Connection::open(db_path)?;

        // Enable WAL mode + Performance PRAGMAs
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA foreign_keys=ON;
             PRAGMA cache_size=-64000;
             PRAGMA temp_store=MEMORY;
             PRAGMA mmap_size=268435456;
             PRAGMA page_size=4096;
             PRAGMA busy_timeout=5000;",
        )?;

        tracing::info!(target: "torii_erc721::storage", "SQLite configured: WAL mode, 64MB cache, 256MB mmap, NORMAL sync");

        // NFT ownership (current state) - one owner per NFT
        conn.execute(
            "CREATE TABLE IF NOT EXISTS nft_ownership (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                token_id BLOB NOT NULL,
                owner BLOB NOT NULL,
                block_number INTEGER NOT NULL,
                tx_hash BLOB NOT NULL,
                timestamp INTEGER,
                UNIQUE(token, token_id)
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nft_ownership_owner ON nft_ownership(owner)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nft_ownership_token ON nft_ownership(token)",
            [],
        )?;

        // Transfer history
        conn.execute(
            "CREATE TABLE IF NOT EXISTS nft_transfers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                token_id BLOB NOT NULL,
                from_addr BLOB NOT NULL,
                to_addr BLOB NOT NULL,
                block_number INTEGER NOT NULL,
                tx_hash BLOB NOT NULL,
                timestamp INTEGER,
                UNIQUE(token, tx_hash, token_id, from_addr, to_addr)
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nft_transfers_token ON nft_transfers(token)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nft_transfers_from ON nft_transfers(from_addr)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nft_transfers_to ON nft_transfers(to_addr)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nft_transfers_block ON nft_transfers(block_number DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nft_transfers_token_id ON nft_transfers(token, token_id)",
            [],
        )?;

        // Wallet activity table for efficient OR queries
        conn.execute(
            "CREATE TABLE IF NOT EXISTS nft_wallet_activity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_address BLOB NOT NULL,
                token BLOB NOT NULL,
                transfer_id INTEGER NOT NULL,
                direction TEXT NOT NULL CHECK(direction IN ('sent', 'received', 'both')),
                block_number INTEGER NOT NULL,
                FOREIGN KEY (transfer_id) REFERENCES nft_transfers(id)
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nft_wallet_activity_wallet_block
             ON nft_wallet_activity(wallet_address, block_number DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nft_wallet_activity_wallet_token
             ON nft_wallet_activity(wallet_address, token, block_number DESC)",
            [],
        )?;

        // Approvals (single token)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS nft_approvals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                token_id BLOB NOT NULL,
                owner BLOB NOT NULL,
                approved BLOB NOT NULL,
                block_number INTEGER NOT NULL,
                tx_hash BLOB NOT NULL,
                timestamp INTEGER
            )",
            [],
        )?;

        // Operator approvals (all tokens)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS nft_operators (
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

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS token_uris (
                token BLOB NOT NULL,
                token_id BLOB NOT NULL,
                uri TEXT,
                metadata_json TEXT,
                updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                PRIMARY KEY (token, token_id)
            );
            CREATE INDEX IF NOT EXISTS idx_token_uris_token ON token_uris(token);

            CREATE TABLE IF NOT EXISTS token_attributes (
                token BLOB NOT NULL,
                token_id BLOB NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (token, token_id, key),
                FOREIGN KEY (token, token_id) REFERENCES token_uris(token, token_id)
            );
            CREATE INDEX IF NOT EXISTS idx_token_attributes_token ON token_attributes(token);
            CREATE INDEX IF NOT EXISTS idx_token_attributes_key ON token_attributes(key);
            CREATE INDEX IF NOT EXISTS idx_token_attributes_key_value ON token_attributes(key, value);",
        )?;

        tracing::info!(target: "torii_erc721::storage", db_path = %db_path, "ERC721 database initialized");

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Insert multiple transfers and update ownership in a single transaction
    pub fn insert_transfers_batch(&self, transfers: &[NftTransferData]) -> Result<usize> {
        if transfers.is_empty() {
            return Ok(0);
        }

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let mut inserted = 0;

        {
            let mut transfer_stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO nft_transfers (token, token_id, from_addr, to_addr, block_number, tx_hash, timestamp)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, COALESCE(?7, strftime('%s', 'now')))",
            )?;

            let mut ownership_stmt = tx.prepare_cached(
                "INSERT OR REPLACE INTO nft_ownership (token, token_id, owner, block_number, tx_hash, timestamp)
                 VALUES (?1, ?2, ?3, ?4, ?5, COALESCE(?6, strftime('%s', 'now')))",
            )?;

            for transfer in transfers {
                let token_blob = felt_to_blob(transfer.token);
                let token_id_blob = u256_to_blob(transfer.token_id);
                let from_blob = felt_to_blob(transfer.from);
                let to_blob = felt_to_blob(transfer.to);
                let tx_hash_blob = felt_to_blob(transfer.tx_hash);

                // Insert transfer
                let rows = transfer_stmt.execute(params![
                    &token_blob,
                    &token_id_blob,
                    &from_blob,
                    &to_blob,
                    transfer.block_number,
                    &tx_hash_blob,
                    transfer.timestamp,
                ])?;

                if rows > 0 {
                    inserted += 1;
                    let transfer_id = tx.last_insert_rowid();

                    // Update ownership (only if to is not zero address)
                    if transfer.to != Felt::ZERO {
                        ownership_stmt.execute(params![
                            &token_blob,
                            &token_id_blob,
                            &to_blob,
                            transfer.block_number,
                            &tx_hash_blob,
                            transfer.timestamp,
                        ])?;
                    }

                    // Insert wallet activity records
                    if transfer.from != Felt::ZERO
                        && transfer.to != Felt::ZERO
                        && transfer.from == transfer.to
                    {
                        tx.execute(
                            "INSERT INTO nft_wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                             VALUES (?, ?, ?, 'both', ?)",
                            params![&from_blob, &token_blob, transfer_id, transfer.block_number],
                        )?;
                    } else {
                        if transfer.from != Felt::ZERO {
                            tx.execute(
                                "INSERT INTO nft_wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                                 VALUES (?, ?, ?, 'sent', ?)",
                                params![&from_blob, &token_blob, transfer_id, transfer.block_number],
                            )?;
                        }
                        if transfer.to != Felt::ZERO {
                            tx.execute(
                                "INSERT INTO nft_wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                                 VALUES (?, ?, ?, 'received', ?)",
                                params![&to_blob, &token_blob, transfer_id, transfer.block_number],
                            )?;
                        }
                    }
                }
            }
        }

        tx.commit()?;
        Ok(inserted)
    }

    /// Insert operator approvals in a single transaction
    pub fn insert_operator_approvals_batch(
        &self,
        approvals: &[OperatorApprovalData],
    ) -> Result<usize> {
        if approvals.is_empty() {
            return Ok(0);
        }

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let mut inserted = 0;

        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR REPLACE INTO nft_operators (token, owner, operator, approved, block_number, tx_hash, timestamp)
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

    /// Get filtered transfers with cursor-based pagination
    pub fn get_transfers_filtered(
        &self,
        wallet: Option<Felt>,
        from: Option<Felt>,
        to: Option<Felt>,
        tokens: &[Felt],
        token_ids: &[U256],
        block_from: Option<u64>,
        block_to: Option<u64>,
        cursor: Option<TransferCursor>,
        limit: u32,
    ) -> Result<(Vec<NftTransferData>, Option<TransferCursor>)> {
        let conn = self.conn.lock().unwrap();

        let mut query = String::new();
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(wallet_addr) = wallet {
            query.push_str(
                "SELECT DISTINCT t.id, t.token, t.token_id, t.from_addr, t.to_addr, t.block_number, t.tx_hash, t.timestamp
                 FROM nft_wallet_activity wa
                 JOIN nft_transfers t ON wa.transfer_id = t.id
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
                "SELECT t.id, t.token, t.token_id, t.from_addr, t.to_addr, t.block_number, t.tx_hash, t.timestamp
                 FROM nft_transfers t
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
            let token_id_bytes: Vec<u8> = row.get(2)?;
            let from_bytes: Vec<u8> = row.get(3)?;
            let to_bytes: Vec<u8> = row.get(4)?;
            let block_number: i64 = row.get(5)?;
            let tx_hash_bytes: Vec<u8> = row.get(6)?;
            let timestamp: Option<i64> = row.get(7)?;

            Ok(NftTransferData {
                id: Some(id),
                token: blob_to_felt(&token_bytes),
                token_id: blob_to_u256(&token_id_bytes),
                from: blob_to_felt(&from_bytes),
                to: blob_to_felt(&to_bytes),
                block_number: block_number as u64,
                tx_hash: blob_to_felt(&tx_hash_bytes),
                timestamp,
            })
        })?;

        let transfers: Vec<NftTransferData> = rows.collect::<Result<_, _>>()?;

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

    /// Get current owner of a specific NFT
    pub fn get_owner(&self, token: Felt, token_id: U256) -> Result<Option<Felt>> {
        let conn = self.conn.lock().unwrap();

        let result: Result<Vec<u8>, _> = conn.query_row(
            "SELECT owner FROM nft_ownership WHERE token = ? AND token_id = ?",
            params![felt_to_blob(token), u256_to_blob(token_id)],
            |row| row.get(0),
        );

        match result {
            Ok(owner_bytes) => Ok(Some(blob_to_felt(&owner_bytes))),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Get ownership records filtered by owner
    pub fn get_ownership_by_owner(
        &self,
        owner: Felt,
        tokens: &[Felt],
        cursor: Option<OwnershipCursor>,
        limit: u32,
    ) -> Result<(Vec<NftOwnershipData>, Option<OwnershipCursor>)> {
        let conn = self.conn.lock().unwrap();

        let mut query = String::from(
            "SELECT id, token, token_id, owner, block_number FROM nft_ownership WHERE owner = ?",
        );
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(felt_to_blob(owner))];

        if !tokens.is_empty() {
            let placeholders: Vec<&str> = tokens.iter().map(|_| "?").collect();
            query.push_str(&format!(" AND token IN ({})", placeholders.join(",")));
            for token in tokens {
                params_vec.push(Box::new(felt_to_blob(*token)));
            }
        }

        if let Some(c) = cursor {
            query.push_str(" AND (block_number < ? OR (block_number = ? AND id < ?))");
            params_vec.push(Box::new(c.block_number as i64));
            params_vec.push(Box::new(c.block_number as i64));
            params_vec.push(Box::new(c.id));
        }

        query.push_str(" ORDER BY block_number DESC, id DESC LIMIT ?");
        params_vec.push(Box::new(limit as i64));

        let mut stmt = conn.prepare(&query)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(std::convert::AsRef::as_ref).collect();

        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            let id: i64 = row.get(0)?;
            let token_bytes: Vec<u8> = row.get(1)?;
            let token_id_bytes: Vec<u8> = row.get(2)?;
            let owner_bytes: Vec<u8> = row.get(3)?;
            let block_number: i64 = row.get(4)?;

            Ok(NftOwnershipData {
                id: Some(id),
                token: blob_to_felt(&token_bytes),
                token_id: blob_to_u256(&token_id_bytes),
                owner: blob_to_felt(&owner_bytes),
                block_number: block_number as u64,
            })
        })?;

        let ownership: Vec<NftOwnershipData> = rows.collect::<Result<_, _>>()?;

        let next_cursor = if ownership.len() == limit as usize {
            ownership.last().map(|o| OwnershipCursor {
                block_number: o.block_number,
                id: o.id.unwrap(),
            })
        } else {
            None
        };

        Ok((ownership, next_cursor))
    }

    /// Get transfer count
    pub fn get_transfer_count(&self) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 =
            conn.query_row("SELECT COUNT(*) FROM nft_transfers", [], |row| row.get(0))?;
        Ok(count as u64)
    }

    /// Get unique token contract count
    pub fn get_token_count(&self) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(DISTINCT token) FROM nft_transfers",
            [],
            |row| row.get(0),
        )?;
        Ok(count as u64)
    }

    /// Get unique NFT count
    pub fn get_nft_count(&self) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 =
            conn.query_row("SELECT COUNT(*) FROM nft_ownership", [], |row| row.get(0))?;
        Ok(count as u64)
    }

    /// Get latest block number indexed
    pub fn get_latest_block(&self) -> Result<Option<u64>> {
        let conn = self.conn.lock().unwrap();
        let block: Option<i64> = conn
            .query_row("SELECT MAX(block_number) FROM nft_transfers", [], |row| {
                row.get(0)
            })
            .ok();
        Ok(block.map(|b| b as u64))
    }

    // ===== Token Metadata Methods =====

    /// Check if metadata exists for a token
    pub fn has_token_metadata(&self, token: Felt) -> Result<bool> {
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
    pub fn upsert_token_metadata(
        &self,
        token: Felt,
        name: Option<&str>,
        symbol: Option<&str>,
        total_supply: Option<U256>,
    ) -> Result<()> {
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
    pub fn get_token_metadata(
        &self,
        token: Felt,
    ) -> Result<Option<(Option<String>, Option<String>, Option<U256>)>> {
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
    pub fn get_all_token_metadata(
        &self,
    ) -> Result<Vec<(Felt, Option<String>, Option<String>, Option<U256>)>> {
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
    pub fn get_token_metadata_paginated(
        &self,
        cursor: Option<Felt>,
        limit: u32,
    ) -> Result<(
        Vec<(Felt, Option<String>, Option<String>, Option<U256>)>,
        Option<Felt>,
    )> {
        let conn = self.conn.lock().unwrap();
        let fetch_limit = limit.max(1).min(1000) as usize + 1;

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

        let capped = limit.max(1).min(1000) as usize;
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
    pub fn has_token_uri(&self, token: Felt, token_id: U256) -> Result<bool> {
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
    pub fn get_token_uris_by_contract(
        &self,
        token: Felt,
    ) -> Result<Vec<(U256, Option<String>, Option<String>)>> {
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
}

#[async_trait::async_trait]
impl TokenUriStore for Erc721Storage {
    async fn store_token_uri(&self, result: &TokenUriResult) -> Result<()> {
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
