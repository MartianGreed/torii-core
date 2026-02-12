//! SQLite storage for ERC20 transfers, approvals, and balances
//!
//! Uses binary (BLOB) storage for efficiency (~51% size reduction vs hex strings).
//! Uses U256 for amounts (proper 256-bit arithmetic for ERC20 token amounts).
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
use torii_common::{blob_to_felt, blob_to_u256, felt_to_blob, u256_to_blob};

use crate::balance_fetcher::BalanceFetchRequest;

/// Direction filter for transfer queries
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransferDirection {
    /// All transfers (sent and received)
    #[default]
    All,
    /// Only sent transfers (wallet is sender)
    Sent,
    /// Only received transfers (wallet is receiver)
    Received,
}

/// Storage for ERC20 transfers and approvals
pub struct Erc20Storage {
    conn: Arc<Mutex<Connection>>,
}

/// Transfer data for batch insertion
pub struct TransferData {
    pub id: Option<i64>,
    pub token: Felt,
    pub from: Felt,
    pub to: Felt,
    /// Amount as U256 (256-bit) for proper ERC20 token amounts
    pub amount: U256,
    pub block_number: u64,
    pub tx_hash: Felt,
    pub timestamp: Option<i64>,
}

/// Approval data for batch insertion
pub struct ApprovalData {
    pub id: Option<i64>,
    pub token: Felt,
    pub owner: Felt,
    pub spender: Felt,
    /// Amount as U256 (256-bit) for proper ERC20 token amounts
    pub amount: U256,
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

/// Cursor for paginated approval queries
#[derive(Debug, Clone, Copy)]
pub struct ApprovalCursor {
    pub block_number: u64,
    pub id: i64,
}

/// Balance data for a token/wallet pair
#[derive(Debug, Clone)]
pub struct BalanceData {
    pub token: Felt,
    pub wallet: Felt,
    pub balance: U256,
    pub last_block: u64,
    pub last_tx_hash: Felt,
}

/// Balance adjustment record for audit trail
#[derive(Debug, Clone)]
pub struct BalanceAdjustment {
    pub token: Felt,
    pub wallet: Felt,
    /// What we computed from transfers
    pub computed_balance: U256,
    /// What the RPC returned as actual balance
    pub actual_balance: U256,
    /// Block at which adjustment was made
    pub adjusted_at_block: u64,
    pub tx_hash: Felt,
}

impl Erc20Storage {
    /// Create or open the database
    pub fn new(db_path: &str) -> Result<Self> {
        let conn = Connection::open(db_path)?;

        // Enable WAL mode + Performance PRAGMAs (critical for production scale)
        // - WAL mode: Readers don't block writers (~2-3x write throughput)
        // - synchronous=NORMAL: Relaxed fsync (safe with WAL, ~10x faster writes)
        // - cache_size=-64000: 64MB cache (negative value = KB)
        // - mmap_size: 256MB memory-mapped I/O for zero-copy reads
        // - busy_timeout: 5s wait for lock (handles concurrent access)
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

        tracing::info!(target: "torii_erc20::storage", "SQLite configured: WAL mode, 64MB cache, 256MB mmap, NORMAL sync");

        // Create transfers table with BLOB columns for efficient storage
        conn.execute(
            "CREATE TABLE IF NOT EXISTS transfers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                from_addr BLOB NOT NULL,
                to_addr BLOB NOT NULL,
                amount BLOB NOT NULL,
                block_number INTEGER NOT NULL,
                tx_hash BLOB NOT NULL,
                timestamp INTEGER DEFAULT (strftime('%s', 'now')),
                UNIQUE(token, tx_hash, from_addr, to_addr)
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_transfers_token ON transfers(token)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_transfers_from ON transfers(from_addr)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_transfers_to ON transfers(to_addr)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_transfers_block ON transfers(block_number)",
            [],
        )?;

        // Composite indexes for efficient queries
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_transfers_token_block ON transfers(token, block_number DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_transfers_from_block ON transfers(from_addr, block_number DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_transfers_to_block ON transfers(to_addr, block_number DESC)",
            [],
        )?;

        // Create approvals table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS approvals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                owner BLOB NOT NULL,
                spender BLOB NOT NULL,
                amount BLOB NOT NULL,
                block_number INTEGER NOT NULL,
                tx_hash BLOB NOT NULL,
                timestamp INTEGER DEFAULT (strftime('%s', 'now')),
                UNIQUE(token, tx_hash, owner, spender)
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_approvals_owner ON approvals(owner, block_number DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_approvals_spender ON approvals(spender, block_number DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_approvals_token ON approvals(token, block_number DESC)",
            [],
        )?;

        // Metadata table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )",
            [],
        )?;

        // Wallet activity table - denormalized for efficient wallet queries
        // Solves the OR query problem: "get all transfers where wallet is sender OR receiver"
        // Instead of: WHERE from_addr = ? OR to_addr = ? (can only use one index)
        // We use: JOIN wallet_activity WHERE wallet_address = ? (uses this table's index)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS wallet_activity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_address BLOB NOT NULL,
                token BLOB NOT NULL,
                transfer_id INTEGER NOT NULL,
                direction TEXT NOT NULL CHECK(direction IN ('sent', 'received', 'both')),
                block_number INTEGER NOT NULL,
                FOREIGN KEY (transfer_id) REFERENCES transfers(id)
            )",
            [],
        )?;

        // Compound index: wallet -> block (optimal for wallet activity queries)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_wallet_activity_wallet_block
             ON wallet_activity(wallet_address, block_number DESC)",
            [],
        )?;

        // Compound index: wallet -> token -> block (token-specific activity)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_wallet_activity_wallet_token
             ON wallet_activity(wallet_address, token, block_number DESC)",
            [],
        )?;

        // Index for reverse lookup (transfer -> wallets)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_wallet_activity_transfer
             ON wallet_activity(transfer_id)",
            [],
        )?;

        // Approval activity table - similar pattern for approvals
        conn.execute(
            "CREATE TABLE IF NOT EXISTS approval_activity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_address BLOB NOT NULL,
                token BLOB NOT NULL,
                approval_id INTEGER NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('owner', 'spender', 'both')),
                block_number INTEGER NOT NULL,
                FOREIGN KEY (approval_id) REFERENCES approvals(id)
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_approval_activity_account_block
             ON approval_activity(account_address, block_number DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_approval_activity_account_token
             ON approval_activity(account_address, token, block_number DESC)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_approval_activity_approval
             ON approval_activity(approval_id)",
            [],
        )?;

        // Balance tracking tables
        // Tracks current balance per (token, wallet) pair
        conn.execute(
            "CREATE TABLE IF NOT EXISTS balances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                wallet BLOB NOT NULL,
                balance BLOB NOT NULL,
                last_block INTEGER NOT NULL,
                last_tx_hash BLOB NOT NULL,
                updated_at INTEGER DEFAULT (strftime('%s', 'now')),
                UNIQUE(token, wallet)
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_balances_token ON balances(token)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_balances_wallet ON balances(wallet)",
            [],
        )?;

        // Balance adjustments table for audit trail
        // Records when we had to fetch balance from RPC due to inconsistency
        conn.execute(
            "CREATE TABLE IF NOT EXISTS balance_adjustments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token BLOB NOT NULL,
                wallet BLOB NOT NULL,
                computed_balance BLOB NOT NULL,
                actual_balance BLOB NOT NULL,
                adjusted_at_block INTEGER NOT NULL,
                tx_hash BLOB NOT NULL,
                created_at INTEGER DEFAULT (strftime('%s', 'now'))
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_adjustments_wallet ON balance_adjustments(wallet)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_adjustments_token ON balance_adjustments(token)",
            [],
        )?;

        // Token metadata table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS token_metadata (
                token BLOB PRIMARY KEY,
                name TEXT,
                symbol TEXT,
                decimals INTEGER
            )",
            [],
        )?;

        tracing::info!(target: "torii_erc20::storage", db_path = %db_path, "Database initialized");

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Insert multiple transfers in a single transaction
    ///
    /// This is significantly faster than inserting one by one because:
    /// - Single lock acquisition
    /// - Single transaction (all-or-nothing commit)
    /// - Prepared statement reuse
    pub fn insert_transfers_batch(&self, transfers: &[TransferData]) -> Result<usize> {
        if transfers.is_empty() {
            return Ok(0);
        }

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let mut inserted = 0;

        {
            let mut transfer_stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO transfers (token, from_addr, to_addr, amount, block_number, tx_hash, timestamp)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, COALESCE(?7, strftime('%s', 'now')))",
            )?;
            let mut wallet_both_stmt = tx.prepare_cached(
                "INSERT INTO wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                 VALUES (?1, ?2, ?3, 'both', ?4)",
            )?;
            let mut wallet_sent_stmt = tx.prepare_cached(
                "INSERT INTO wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                 VALUES (?1, ?2, ?3, 'sent', ?4)",
            )?;
            let mut wallet_received_stmt = tx.prepare_cached(
                "INSERT INTO wallet_activity (wallet_address, token, transfer_id, direction, block_number)
                 VALUES (?1, ?2, ?3, 'received', ?4)",
            )?;

            for transfer in transfers {
                let token_blob = felt_to_blob(transfer.token);
                let from_blob = felt_to_blob(transfer.from);
                let to_blob = felt_to_blob(transfer.to);
                let amount_blob = u256_to_blob(transfer.amount);
                let tx_hash_blob = felt_to_blob(transfer.tx_hash);

                // Insert transfer
                let rows = transfer_stmt.execute(params![
                    &token_blob,
                    &from_blob,
                    &to_blob,
                    &amount_blob,
                    transfer.block_number,
                    &tx_hash_blob,
                    transfer.timestamp,
                ])?;

                if rows > 0 {
                    inserted += 1;
                    let transfer_id = tx.last_insert_rowid();

                    // Insert wallet activity records
                    // This enables O(log n) wallet queries instead of O(n) OR scans
                    if transfer.from != Felt::ZERO
                        && transfer.to != Felt::ZERO
                        && transfer.from == transfer.to
                    {
                        // Self-transfer (one record with 'both' direction)
                        wallet_both_stmt.execute(params![
                            &from_blob,
                            &token_blob,
                            transfer_id,
                            transfer.block_number
                        ])?;
                    } else {
                        // Sender record
                        if transfer.from != Felt::ZERO {
                            wallet_sent_stmt.execute(params![
                                &from_blob,
                                &token_blob,
                                transfer_id,
                                transfer.block_number
                            ])?;
                        }
                        // Receiver record
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

    /// Insert multiple approvals in a single transaction
    pub fn insert_approvals_batch(&self, approvals: &[ApprovalData]) -> Result<usize> {
        if approvals.is_empty() {
            return Ok(0);
        }

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let mut inserted = 0;

        {
            let mut approval_stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO approvals (token, owner, spender, amount, block_number, tx_hash, timestamp)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, COALESCE(?7, strftime('%s', 'now')))",
            )?;
            let mut approval_both_stmt = tx.prepare_cached(
                "INSERT INTO approval_activity (account_address, token, approval_id, role, block_number)
                 VALUES (?1, ?2, ?3, 'both', ?4)",
            )?;
            let mut approval_owner_stmt = tx.prepare_cached(
                "INSERT INTO approval_activity (account_address, token, approval_id, role, block_number)
                 VALUES (?1, ?2, ?3, 'owner', ?4)",
            )?;
            let mut approval_spender_stmt = tx.prepare_cached(
                "INSERT INTO approval_activity (account_address, token, approval_id, role, block_number)
                 VALUES (?1, ?2, ?3, 'spender', ?4)",
            )?;

            for approval in approvals {
                let token_blob = felt_to_blob(approval.token);
                let owner_blob = felt_to_blob(approval.owner);
                let spender_blob = felt_to_blob(approval.spender);
                let amount_blob = u256_to_blob(approval.amount);
                let tx_hash_blob = felt_to_blob(approval.tx_hash);

                // Insert approval
                let rows = approval_stmt.execute(params![
                    &token_blob,
                    &owner_blob,
                    &spender_blob,
                    &amount_blob,
                    approval.block_number,
                    &tx_hash_blob,
                    approval.timestamp,
                ])?;

                if rows > 0 {
                    inserted += 1;
                    let approval_id = tx.last_insert_rowid();

                    // Insert approval activity records
                    if approval.owner != Felt::ZERO
                        && approval.spender != Felt::ZERO
                        && approval.owner == approval.spender
                    {
                        // Self-approval (one record with 'both' role)
                        approval_both_stmt.execute(params![
                            &owner_blob,
                            &token_blob,
                            approval_id,
                            approval.block_number
                        ])?;
                    } else {
                        // Owner record
                        if approval.owner != Felt::ZERO {
                            approval_owner_stmt.execute(params![
                                &owner_blob,
                                &token_blob,
                                approval_id,
                                approval.block_number
                            ])?;
                        }
                        // Spender record
                        if approval.spender != Felt::ZERO {
                            approval_spender_stmt.execute(params![
                                &spender_blob,
                                &token_blob,
                                approval_id,
                                approval.block_number
                            ])?;
                        }
                    }
                }
            }
        }

        tx.commit()?;

        Ok(inserted)
    }

    /// Get filtered transfers with cursor-based pagination
    ///
    /// Supports:
    /// - `wallet`: Matches from OR to (uses wallet_activity table for efficient OR queries)
    /// - `from`: Exact from address match
    /// - `to`: Exact to address match
    /// - `tokens`: Token whitelist (empty = all tokens)
    /// - `direction`: Filter by sent/received/all (only applies with wallet)
    /// - `block_from`/`block_to`: Block range filter
    /// - `cursor`: Cursor from previous page
    /// - `limit`: Maximum results
    pub fn get_transfers_filtered(
        &self,
        wallet: Option<Felt>,
        from: Option<Felt>,
        to: Option<Felt>,
        tokens: &[Felt],
        direction: TransferDirection,
        block_from: Option<u64>,
        block_to: Option<u64>,
        cursor: Option<TransferCursor>,
        limit: u32,
    ) -> Result<(Vec<TransferData>, Option<TransferCursor>)> {
        let conn = self.conn.lock().unwrap();

        // Build dynamic query based on filters
        let mut query = String::new();
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(wallet_addr) = wallet {
            // Use wallet_activity table for efficient OR queries
            query.push_str(
                "SELECT DISTINCT t.id, t.token, t.from_addr, t.to_addr, t.amount, t.block_number, t.tx_hash, t.timestamp
                 FROM wallet_activity wa
                 JOIN transfers t ON wa.transfer_id = t.id
                 WHERE wa.wallet_address = ?",
            );
            params_vec.push(Box::new(felt_to_blob(wallet_addr)));

            // Apply direction filter
            match direction {
                TransferDirection::All => {}
                TransferDirection::Sent => {
                    query.push_str(" AND wa.direction IN ('sent', 'both')");
                }
                TransferDirection::Received => {
                    query.push_str(" AND wa.direction IN ('received', 'both')");
                }
            }

            // Token filter on wallet_activity for efficiency
            if !tokens.is_empty() {
                let placeholders: Vec<&str> = tokens.iter().map(|_| "?").collect();
                query.push_str(&format!(" AND wa.token IN ({})", placeholders.join(",")));
                for token in tokens {
                    params_vec.push(Box::new(felt_to_blob(*token)));
                }
            }
        } else {
            // Standard query without wallet optimization
            query.push_str(
                "SELECT t.id, t.token, t.from_addr, t.to_addr, t.amount, t.block_number, t.tx_hash, t.timestamp
                 FROM transfers t
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

        // Block range filters
        if let Some(block_min) = block_from {
            query.push_str(" AND t.block_number >= ?");
            params_vec.push(Box::new(block_min as i64));
        }

        if let Some(block_max) = block_to {
            query.push_str(" AND t.block_number <= ?");
            params_vec.push(Box::new(block_max as i64));
        }

        // Cursor-based pagination
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
            let from_bytes: Vec<u8> = row.get(2)?;
            let to_bytes: Vec<u8> = row.get(3)?;
            let amount_bytes: Vec<u8> = row.get(4)?;
            let block_number: i64 = row.get(5)?;
            let tx_hash_bytes: Vec<u8> = row.get(6)?;
            let timestamp: Option<i64> = row.get(7)?;

            Ok(TransferData {
                id: Some(id),
                token: blob_to_felt(&token_bytes),
                from: blob_to_felt(&from_bytes),
                to: blob_to_felt(&to_bytes),
                amount: blob_to_u256(&amount_bytes),
                block_number: block_number as u64,
                tx_hash: blob_to_felt(&tx_hash_bytes),
                timestamp,
            })
        })?;

        let transfers: Vec<TransferData> = rows.collect::<Result<_, _>>()?;

        // Compute next cursor if there might be more pages
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

    /// Get filtered approvals with cursor-based pagination
    ///
    /// Supports:
    /// - `account`: Matches owner OR spender (uses approval_activity table)
    /// - `owner`: Exact owner address match
    /// - `spender`: Exact spender address match
    /// - `tokens`: Token whitelist (empty = all tokens)
    /// - `block_from`/`block_to`: Block range filter
    /// - `cursor`: Cursor from previous page
    /// - `limit`: Maximum results
    pub fn get_approvals_filtered(
        &self,
        account: Option<Felt>,
        owner: Option<Felt>,
        spender: Option<Felt>,
        tokens: &[Felt],
        block_from: Option<u64>,
        block_to: Option<u64>,
        cursor: Option<ApprovalCursor>,
        limit: u32,
    ) -> Result<(Vec<ApprovalData>, Option<ApprovalCursor>)> {
        let conn = self.conn.lock().unwrap();

        // Build dynamic query based on filters
        let mut query = String::new();
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(account_addr) = account {
            // Use approval_activity table for efficient OR queries
            query.push_str(
                "SELECT DISTINCT a.id, a.token, a.owner, a.spender, a.amount, a.block_number, a.tx_hash, a.timestamp
                 FROM approval_activity aa
                 JOIN approvals a ON aa.approval_id = a.id
                 WHERE aa.account_address = ?",
            );
            params_vec.push(Box::new(felt_to_blob(account_addr)));

            // Token filter on approval_activity for efficiency
            if !tokens.is_empty() {
                let placeholders: Vec<&str> = tokens.iter().map(|_| "?").collect();
                query.push_str(&format!(" AND aa.token IN ({})", placeholders.join(",")));
                for token in tokens {
                    params_vec.push(Box::new(felt_to_blob(*token)));
                }
            }
        } else {
            // Standard query without account optimization
            query.push_str(
                "SELECT a.id, a.token, a.owner, a.spender, a.amount, a.block_number, a.tx_hash, a.timestamp
                 FROM approvals a
                 WHERE 1=1",
            );

            if let Some(owner_addr) = owner {
                query.push_str(" AND a.owner = ?");
                params_vec.push(Box::new(felt_to_blob(owner_addr)));
            }

            if let Some(spender_addr) = spender {
                query.push_str(" AND a.spender = ?");
                params_vec.push(Box::new(felt_to_blob(spender_addr)));
            }

            if !tokens.is_empty() {
                let placeholders: Vec<&str> = tokens.iter().map(|_| "?").collect();
                query.push_str(&format!(" AND a.token IN ({})", placeholders.join(",")));
                for token in tokens {
                    params_vec.push(Box::new(felt_to_blob(*token)));
                }
            }
        }

        // Block range filters
        if let Some(block_min) = block_from {
            query.push_str(" AND a.block_number >= ?");
            params_vec.push(Box::new(block_min as i64));
        }

        if let Some(block_max) = block_to {
            query.push_str(" AND a.block_number <= ?");
            params_vec.push(Box::new(block_max as i64));
        }

        // Cursor-based pagination
        if let Some(c) = cursor {
            query.push_str(" AND (a.block_number < ? OR (a.block_number = ? AND a.id < ?))");
            params_vec.push(Box::new(c.block_number as i64));
            params_vec.push(Box::new(c.block_number as i64));
            params_vec.push(Box::new(c.id));
        }

        query.push_str(" ORDER BY a.block_number DESC, a.id DESC LIMIT ?");
        params_vec.push(Box::new(limit as i64));

        let mut stmt = conn.prepare(&query)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(std::convert::AsRef::as_ref).collect();

        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            let id: i64 = row.get(0)?;
            let token_bytes: Vec<u8> = row.get(1)?;
            let owner_bytes: Vec<u8> = row.get(2)?;
            let spender_bytes: Vec<u8> = row.get(3)?;
            let amount_bytes: Vec<u8> = row.get(4)?;
            let block_number: i64 = row.get(5)?;
            let tx_hash_bytes: Vec<u8> = row.get(6)?;
            let timestamp: Option<i64> = row.get(7)?;

            Ok(ApprovalData {
                id: Some(id),
                token: blob_to_felt(&token_bytes),
                owner: blob_to_felt(&owner_bytes),
                spender: blob_to_felt(&spender_bytes),
                amount: blob_to_u256(&amount_bytes),
                block_number: block_number as u64,
                tx_hash: blob_to_felt(&tx_hash_bytes),
                timestamp,
            })
        })?;

        let approvals: Vec<ApprovalData> = rows.collect::<Result<_, _>>()?;

        // Compute next cursor if there might be more pages
        let next_cursor = if approvals.len() == limit as usize {
            approvals.last().map(|a| ApprovalCursor {
                block_number: a.block_number,
                id: a.id.unwrap(),
            })
        } else {
            None
        };

        Ok((approvals, next_cursor))
    }

    /// Get transfer count
    pub fn get_transfer_count(&self) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM transfers", [], |row| row.get(0))?;
        Ok(count as u64)
    }

    /// Get approval count
    pub fn get_approval_count(&self) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM approvals", [], |row| row.get(0))?;
        Ok(count as u64)
    }

    /// Get indexed token count
    pub fn get_token_count(&self) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 =
            conn.query_row("SELECT COUNT(DISTINCT token) FROM transfers", [], |row| {
                row.get(0)
            })?;
        Ok(count as u64)
    }

    /// Get latest block number indexed
    pub fn get_latest_block(&self) -> Result<Option<u64>> {
        let conn = self.conn.lock().unwrap();
        let block: Option<i64> = conn
            .query_row("SELECT MAX(block_number) FROM transfers", [], |row| {
                row.get(0)
            })
            .ok();
        Ok(block.map(|b| b as u64))
    }

    // ===== Balance Tracking Methods =====

    /// Get current balance for a wallet/token pair
    pub fn get_balance(&self, token: Felt, wallet: Felt) -> Result<Option<U256>> {
        let conn = self.conn.lock().unwrap();
        let token_blob = felt_to_blob(token);
        let wallet_blob = felt_to_blob(wallet);

        let result: Option<Vec<u8>> = conn
            .query_row(
                "SELECT balance FROM balances WHERE token = ? AND wallet = ?",
                params![&token_blob, &wallet_blob],
                |row| row.get(0),
            )
            .ok();

        Ok(result.map(|bytes| blob_to_u256(&bytes)))
    }

    /// Get balance with last block info for a wallet/token pair
    pub fn get_balance_with_block(&self, token: Felt, wallet: Felt) -> Result<Option<(U256, u64)>> {
        let conn = self.conn.lock().unwrap();
        let token_blob = felt_to_blob(token);
        let wallet_blob = felt_to_blob(wallet);

        let result: Option<(Vec<u8>, i64)> = conn
            .query_row(
                "SELECT balance, last_block FROM balances WHERE token = ? AND wallet = ?",
                params![&token_blob, &wallet_blob],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .ok();

        Ok(result.map(|(bytes, block)| (blob_to_u256(&bytes), block as u64)))
    }

    /// Get balances with optional token/wallet filters and cursor pagination.
    ///
    /// Pagination is cursor-based on the `balances.id` primary key in ascending order.
    /// Returns `(rows, next_cursor)`.
    pub fn get_balances_filtered(
        &self,
        token: Option<Felt>,
        wallet: Option<Felt>,
        cursor: Option<i64>,
        limit: u32,
    ) -> Result<(Vec<BalanceData>, Option<i64>)> {
        let conn = self.conn.lock().unwrap();

        let mut query = String::from(
            "SELECT id, token, wallet, balance, last_block, last_tx_hash
             FROM balances
             WHERE 1=1",
        );
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(token_addr) = token {
            query.push_str(" AND token = ?");
            params_vec.push(Box::new(felt_to_blob(token_addr)));
        }

        if let Some(wallet_addr) = wallet {
            query.push_str(" AND wallet = ?");
            params_vec.push(Box::new(felt_to_blob(wallet_addr)));
        }

        if let Some(c) = cursor {
            query.push_str(" AND id > ?");
            params_vec.push(Box::new(c));
        }

        query.push_str(" ORDER BY id ASC LIMIT ?");
        params_vec.push(Box::new(limit as i64));

        let mut stmt = conn.prepare(&query)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(std::convert::AsRef::as_ref).collect();

        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            let id: i64 = row.get(0)?;
            let token_bytes: Vec<u8> = row.get(1)?;
            let wallet_bytes: Vec<u8> = row.get(2)?;
            let balance_bytes: Vec<u8> = row.get(3)?;
            let last_block: i64 = row.get(4)?;
            let last_tx_hash_bytes: Vec<u8> = row.get(5)?;

            Ok((
                id,
                BalanceData {
                    token: blob_to_felt(&token_bytes),
                    wallet: blob_to_felt(&wallet_bytes),
                    balance: blob_to_u256(&balance_bytes),
                    last_block: last_block as u64,
                    last_tx_hash: blob_to_felt(&last_tx_hash_bytes),
                },
            ))
        })?;

        let mut out: Vec<BalanceData> = Vec::new();
        let mut last_id: Option<i64> = None;

        for row in rows {
            let (id, data) = row?;
            last_id = Some(id);
            out.push(data);
        }

        let next_cursor = if out.len() == limit as usize {
            last_id
        } else {
            None
        };

        Ok((out, next_cursor))
    }

    /// Get balances for multiple wallet/token pairs in a single query
    pub fn get_balances_batch(
        &self,
        pairs: &[(Felt, Felt)],
    ) -> Result<HashMap<(Felt, Felt), U256>> {
        if pairs.is_empty() {
            return Ok(HashMap::new());
        }

        let conn = self.conn.lock().unwrap();
        let mut result = HashMap::new();

        // SQLite doesn't support batch IN with tuples well, so we query one by one
        // but we keep the connection lock to avoid repeated locking
        let mut stmt =
            conn.prepare_cached("SELECT balance FROM balances WHERE token = ? AND wallet = ?")?;

        for (token, wallet) in pairs {
            let token_blob = felt_to_blob(*token);
            let wallet_blob = felt_to_blob(*wallet);

            if let Ok(balance_bytes) = stmt.query_row(params![&token_blob, &wallet_blob], |row| {
                row.get::<_, Vec<u8>>(0)
            }) {
                result.insert((*token, *wallet), blob_to_u256(&balance_bytes));
            }
        }

        Ok(result)
    }

    /// Check which transfers need balance adjustments
    ///
    /// For each transfer, checks if the sender's current balance would go negative.
    /// Returns BalanceFetchRequests for wallets that need adjustment.
    ///
    /// The returned requests have block_number set to transfer.block_number - 1
    /// (the block RIGHT BEFORE the transfer occurred).
    pub fn check_balances_batch(
        &self,
        transfers: &[TransferData],
    ) -> Result<Vec<BalanceFetchRequest>> {
        if transfers.is_empty() {
            return Ok(Vec::new());
        }

        // Collect unique sender pairs (token, wallet) that need checking
        // Skip zero addresses (mints)
        let sender_pairs: Vec<(Felt, Felt)> = transfers
            .iter()
            .filter(|t| t.from != Felt::ZERO)
            .map(|t| (t.token, t.from))
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        // Get current balances for all senders
        let current_balances = self.get_balances_batch(&sender_pairs)?;

        // Track running balance changes within this batch
        // (multiple transfers from same wallet in same batch)
        let mut pending_debits: HashMap<(Felt, Felt), U256> = HashMap::new();

        let mut adjustment_requests = Vec::new();

        for transfer in transfers {
            if transfer.from == Felt::ZERO {
                continue; // Skip mints
            }

            let key = (transfer.token, transfer.from);

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
            // Since U256 doesn't have checked_add, we add and check for overflow via comparison
            let total_needed = total_pending + transfer.amount;

            if stored_balance >= total_needed {
                // Balance is sufficient, track the debit
                pending_debits.insert(key, total_needed);
            } else {
                // Balance would go negative - need to fetch actual balance
                // Only add one request per unique (token, wallet, block)
                let block_before = transfer.block_number.saturating_sub(1);
                // Check if we already have a request for this (token, from_wallet, block)
                #[allow(clippy::suspicious_operation_groupings)]
                let already_requested =
                    adjustment_requests.iter().any(|r: &BalanceFetchRequest| {
                        r.token == transfer.token
                            && r.wallet == transfer.from
                            && r.block_number == block_before
                    });

                if !already_requested {
                    adjustment_requests.push(BalanceFetchRequest {
                        token: transfer.token,
                        wallet: transfer.from,
                        block_number: block_before,
                    });
                }
            }
        }

        if !adjustment_requests.is_empty() {
            tracing::info!(
                target: "torii_erc20::storage",
                count = adjustment_requests.len(),
                "Detected balance inconsistencies, will fetch from RPC"
            );
        }

        Ok(adjustment_requests)
    }

    /// Apply transfers with adjustments and update balances
    ///
    /// This is the main balance tracking method. It:
    /// 1. Uses adjustments for wallets where we fetched actual balances
    /// 2. Applies transfer debits/credits to update balances
    /// 3. Records adjustments in the audit table
    ///
    /// # Arguments
    /// * `transfers` - The transfers to apply
    /// * `adjustments` - Map of (token, wallet) -> actual_balance fetched from RPC
    pub fn apply_transfers_with_adjustments(
        &self,
        transfers: &[TransferData],
        adjustments: &HashMap<(Felt, Felt), U256>,
    ) -> Result<()> {
        if transfers.is_empty() {
            return Ok(());
        }

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        // Track balance changes in memory first
        // Key: (token, wallet), Value: current balance
        let mut balance_cache: HashMap<(Felt, Felt), U256> = HashMap::new();

        // Load existing balances for all affected wallets
        {
            let mut stmt =
                tx.prepare_cached("SELECT balance FROM balances WHERE token = ? AND wallet = ?")?;

            for transfer in transfers {
                // Load sender balance (if not zero address)
                if transfer.from != Felt::ZERO {
                    let key = (transfer.token, transfer.from);
                    if let std::collections::hash_map::Entry::Vacant(e) = balance_cache.entry(key) {
                        let token_blob = felt_to_blob(transfer.token);
                        let wallet_blob = felt_to_blob(transfer.from);
                        let balance: U256 = stmt
                            .query_row(params![&token_blob, &wallet_blob], |row| {
                                let bytes: Vec<u8> = row.get(0)?;
                                Ok(blob_to_u256(&bytes))
                            })
                            .unwrap_or(U256::from(0u64));
                        e.insert(balance);
                    }
                }

                // Load receiver balance (if not zero address)
                if transfer.to != Felt::ZERO {
                    let key = (transfer.token, transfer.to);
                    if let std::collections::hash_map::Entry::Vacant(e) = balance_cache.entry(key) {
                        let token_blob = felt_to_blob(transfer.token);
                        let wallet_blob = felt_to_blob(transfer.to);
                        let balance: U256 = stmt
                            .query_row(params![&token_blob, &wallet_blob], |row| {
                                let bytes: Vec<u8> = row.get(0)?;
                                Ok(blob_to_u256(&bytes))
                            })
                            .unwrap_or(U256::from(0u64));
                        e.insert(balance);
                    }
                }
            }
        }

        // Apply adjustments - these are the "corrected" starting balances
        let mut adjustments_to_record: Vec<BalanceAdjustment> = Vec::new();

        for ((token, wallet), actual_balance) in adjustments {
            let key = (*token, *wallet);
            let computed = balance_cache.get(&key).copied().unwrap_or(U256::from(0u64));

            // Only record if there's actually a difference
            if computed != *actual_balance {
                // Find the transfer that triggered this adjustment
                let triggering_transfer = transfers
                    .iter()
                    .find(|t| t.token == *token && t.from == *wallet);

                if let Some(transfer) = triggering_transfer {
                    adjustments_to_record.push(BalanceAdjustment {
                        token: *token,
                        wallet: *wallet,
                        computed_balance: computed,
                        actual_balance: *actual_balance,
                        adjusted_at_block: transfer.block_number,
                        tx_hash: transfer.tx_hash,
                    });
                }
            }

            // Update cache with actual balance
            balance_cache.insert(key, *actual_balance);
        }

        // Apply transfers to balances
        let mut last_block_per_wallet: HashMap<(Felt, Felt), (u64, Felt)> = HashMap::new();

        for transfer in transfers {
            // Debit sender (if not mint)
            if transfer.from != Felt::ZERO {
                let key = (transfer.token, transfer.from);
                let current = balance_cache.get(&key).copied().unwrap_or(U256::from(0u64));
                // Saturating subtract: return 0 if would underflow
                let new_balance = if current >= transfer.amount {
                    current - transfer.amount
                } else {
                    U256::from(0u64)
                };
                balance_cache.insert(key, new_balance);
                last_block_per_wallet.insert(key, (transfer.block_number, transfer.tx_hash));
            }

            // Credit receiver (if not burn)
            if transfer.to != Felt::ZERO {
                let key = (transfer.token, transfer.to);
                let current = balance_cache.get(&key).copied().unwrap_or(U256::from(0u64));
                // For addition, we just add (overflow is extremely unlikely for token balances)
                let new_balance = current + transfer.amount;
                balance_cache.insert(key, new_balance);
                last_block_per_wallet.insert(key, (transfer.block_number, transfer.tx_hash));
            }
        }

        // Write balances to database
        {
            let mut upsert_stmt = tx.prepare_cached(
                "INSERT INTO balances (token, wallet, balance, last_block, last_tx_hash)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(token, wallet) DO UPDATE SET
                     balance = excluded.balance,
                     last_block = excluded.last_block,
                     last_tx_hash = excluded.last_tx_hash,
                     updated_at = strftime('%s', 'now')",
            )?;

            for ((token, wallet), balance) in &balance_cache {
                let (last_block, last_tx_hash) = last_block_per_wallet
                    .get(&(*token, *wallet))
                    .copied()
                    .unwrap_or((0, Felt::ZERO));

                let token_blob = felt_to_blob(*token);
                let wallet_blob = felt_to_blob(*wallet);
                let balance_blob = u256_to_blob(*balance);
                let tx_hash_blob = felt_to_blob(last_tx_hash);

                upsert_stmt.execute(params![
                    &token_blob,
                    &wallet_blob,
                    &balance_blob,
                    last_block as i64,
                    &tx_hash_blob,
                ])?;
            }
        }

        // Record adjustments for audit
        if !adjustments_to_record.is_empty() {
            let mut adj_stmt = tx.prepare_cached(
                "INSERT INTO balance_adjustments
                 (token, wallet, computed_balance, actual_balance, adjusted_at_block, tx_hash)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )?;

            for adj in &adjustments_to_record {
                let token_blob = felt_to_blob(adj.token);
                let wallet_blob = felt_to_blob(adj.wallet);
                let computed_blob = u256_to_blob(adj.computed_balance);
                let actual_blob = u256_to_blob(adj.actual_balance);
                let tx_hash_blob = felt_to_blob(adj.tx_hash);

                adj_stmt.execute(params![
                    &token_blob,
                    &wallet_blob,
                    &computed_blob,
                    &actual_blob,
                    adj.adjusted_at_block as i64,
                    &tx_hash_blob,
                ])?;
            }

            tracing::info!(
                target: "torii_erc20::storage",
                count = adjustments_to_record.len(),
                "Applied balance adjustments (genesis/airdrop detection)"
            );
        }

        tx.commit()?;

        Ok(())
    }

    /// Get adjustment count (for statistics)
    pub fn get_adjustment_count(&self) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM balance_adjustments", [], |row| {
            row.get(0)
        })?;
        Ok(count as u64)
    }

    /// Get unique wallet count with balances
    pub fn get_wallet_count(&self) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 =
            conn.query_row("SELECT COUNT(DISTINCT wallet) FROM balances", [], |row| {
                row.get(0)
            })?;
        Ok(count as u64)
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
        decimals: Option<u8>,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let token_blob = felt_to_blob(token);
        conn.execute(
            "INSERT INTO token_metadata (token, name, symbol, decimals)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(token) DO UPDATE SET
                 name = COALESCE(excluded.name, token_metadata.name),
                 symbol = COALESCE(excluded.symbol, token_metadata.symbol),
                 decimals = COALESCE(excluded.decimals, token_metadata.decimals)",
            params![&token_blob, name, symbol, decimals.map(|d| d as i64)],
        )?;
        Ok(())
    }

    /// Get token metadata
    pub fn get_token_metadata(
        &self,
        token: Felt,
    ) -> Result<Option<(Option<String>, Option<String>, Option<u8>)>> {
        let conn = self.conn.lock().unwrap();
        let token_blob = felt_to_blob(token);
        let result = conn
            .query_row(
                "SELECT name, symbol, decimals FROM token_metadata WHERE token = ?",
                params![&token_blob],
                |row| {
                    let name: Option<String> = row.get(0)?;
                    let symbol: Option<String> = row.get(1)?;
                    let decimals: Option<i64> = row.get(2)?;
                    Ok((name, symbol, decimals.map(|d| d as u8)))
                },
            )
            .ok();
        Ok(result)
    }

    /// Get all token metadata
    pub fn get_all_token_metadata(
        &self,
    ) -> Result<Vec<(Felt, Option<String>, Option<String>, Option<u8>)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT token, name, symbol, decimals FROM token_metadata")?;
        let rows = stmt.query_map([], |row| {
            let token_bytes: Vec<u8> = row.get(0)?;
            let name: Option<String> = row.get(1)?;
            let symbol: Option<String> = row.get(2)?;
            let decimals: Option<i64> = row.get(3)?;
            Ok((
                blob_to_felt(&token_bytes),
                name,
                symbol,
                decimals.map(|d| d as u8),
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
    ) -> Result<(Vec<(Felt, Option<String>, Option<String>, Option<u8>)>, Option<Felt>)> {
        let conn = self.conn.lock().unwrap();
        let fetch_limit = limit.max(1).min(1000) as usize + 1;

        let mut out = if let Some(cursor_token) = cursor {
            let cursor_blob = felt_to_blob(cursor_token);
            let mut stmt = conn.prepare(
                "SELECT token, name, symbol, decimals
                 FROM token_metadata
                 WHERE token > ?1
                 ORDER BY token ASC
                 LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![&cursor_blob, fetch_limit as i64], |row| {
                let token_bytes: Vec<u8> = row.get(0)?;
                let name: Option<String> = row.get(1)?;
                let symbol: Option<String> = row.get(2)?;
                let decimals: Option<i64> = row.get(3)?;
                Ok((
                    blob_to_felt(&token_bytes),
                    name,
                    symbol,
                    decimals.map(|d| d as u8),
                ))
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        } else {
            let mut stmt = conn.prepare(
                "SELECT token, name, symbol, decimals
                 FROM token_metadata
                 ORDER BY token ASC
                 LIMIT ?1",
            )?;
            let rows = stmt.query_map(params![fetch_limit as i64], |row| {
                let token_bytes: Vec<u8> = row.get(0)?;
                let name: Option<String> = row.get(1)?;
                let symbol: Option<String> = row.get(2)?;
                let decimals: Option<i64> = row.get(3)?;
                Ok((
                    blob_to_felt(&token_bytes),
                    name,
                    symbol,
                    decimals.map(|d| d as u8),
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
}
