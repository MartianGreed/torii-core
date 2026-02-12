//! gRPC service implementation for ERC1155 queries and subscriptions

use crate::proto::{
    erc1155_server::Erc1155 as Erc1155Trait, Cursor, GetBalanceRequest, GetBalanceResponse,
    GetStatsRequest, GetStatsResponse, GetTokenMetadataRequest, GetTokenMetadataResponse,
    GetTransfersRequest, GetTransfersResponse, SubscribeTransfersRequest, TokenMetadataEntry,
    TokenTransfer, TransferFilter, TransferUpdate,
};
use crate::storage::{Erc1155Storage, TokenTransferData, TransferCursor};
use async_trait::async_trait;
use futures::stream::Stream;
use starknet::core::types::Felt;
use starknet::core::types::U256;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast;
use tonic::{Request, Response, Status};
use torii_common::{bytes_to_felt, bytes_to_u256, u256_to_bytes};

/// gRPC service implementation for ERC1155
#[derive(Clone)]
pub struct Erc1155Service {
    storage: Arc<Erc1155Storage>,
    /// Broadcast channel for real-time transfer updates
    pub transfer_tx: broadcast::Sender<TransferUpdate>,
}

impl Erc1155Service {
    /// Creates a new Erc1155Service
    pub fn new(storage: Arc<Erc1155Storage>) -> Self {
        let (transfer_tx, _) = broadcast::channel(1000);

        Self {
            storage,
            transfer_tx,
        }
    }

    /// Broadcasts a transfer to all subscribers
    pub fn broadcast_transfer(&self, transfer: TokenTransfer) {
        let update = TransferUpdate {
            transfer: Some(transfer),
            timestamp: chrono::Utc::now().timestamp(),
        };
        let _ = self.transfer_tx.send(update);
    }

    /// Convert storage TokenTransferData to proto TokenTransfer
    fn transfer_data_to_proto(data: &TokenTransferData) -> TokenTransfer {
        TokenTransfer {
            token: data.token.to_bytes_be().to_vec(),
            operator: data.operator.to_bytes_be().to_vec(),
            from: data.from.to_bytes_be().to_vec(),
            to: data.to.to_bytes_be().to_vec(),
            token_id: u256_to_bytes(data.token_id),
            amount: u256_to_bytes(data.amount),
            block_number: data.block_number,
            tx_hash: data.tx_hash.to_bytes_be().to_vec(),
            timestamp: data.timestamp.unwrap_or(0),
            is_batch: data.is_batch,
            batch_index: data.batch_index,
        }
    }

    /// Check if a transfer matches a filter (for subscriptions)
    fn matches_transfer_filter(transfer: &TokenTransfer, filter: &TransferFilter) -> bool {
        // Wallet filter (OR logic: matches from OR to)
        if let Some(ref wallet) = filter.wallet {
            let matches_from = transfer.from == *wallet;
            let matches_to = transfer.to == *wallet;
            if !matches_from && !matches_to {
                return false;
            }
        }

        // Exact from filter
        if let Some(ref from) = filter.from {
            if transfer.from != *from {
                return false;
            }
        }

        // Exact to filter
        if let Some(ref to) = filter.to {
            if transfer.to != *to {
                return false;
            }
        }

        // Exact operator filter
        if let Some(ref operator) = filter.operator {
            if transfer.operator != *operator {
                return false;
            }
        }

        // Token whitelist
        if !filter.tokens.is_empty() && !filter.tokens.contains(&transfer.token) {
            return false;
        }

        // Token ID whitelist
        if !filter.token_ids.is_empty() && !filter.token_ids.contains(&transfer.token_id) {
            return false;
        }

        // Block range filters
        if let Some(block_from) = filter.block_from {
            if transfer.block_number < block_from {
                return false;
            }
        }

        if let Some(block_to) = filter.block_to {
            if transfer.block_number > block_to {
                return false;
            }
        }

        true
    }
}

#[async_trait]
impl Erc1155Trait for Erc1155Service {
    /// Query historical transfers with filtering and pagination
    async fn get_transfers(
        &self,
        request: Request<GetTransfersRequest>,
    ) -> Result<Response<GetTransfersResponse>, Status> {
        let req = request.into_inner();
        let filter = req.filter.unwrap_or_default();

        let wallet = filter.wallet.as_ref().and_then(|b| bytes_to_felt(b));
        let from = filter.from.as_ref().and_then(|b| bytes_to_felt(b));
        let to = filter.to.as_ref().and_then(|b| bytes_to_felt(b));
        let operator = filter.operator.as_ref().and_then(|b| bytes_to_felt(b));
        let tokens: Vec<Felt> = filter
            .tokens
            .iter()
            .filter_map(|b| bytes_to_felt(b))
            .collect();
        let token_ids: Vec<U256> = filter.token_ids.iter().map(|b| bytes_to_u256(b)).collect();

        let cursor = req.cursor.map(|c| TransferCursor {
            block_number: c.block_number,
            id: c.id,
        });

        let limit = if req.limit == 0 {
            100
        } else {
            req.limit.min(1000)
        };

        let (transfers, next_cursor) = self
            .storage
            .get_transfers_filtered(
                wallet,
                from,
                to,
                operator,
                &tokens,
                &token_ids,
                filter.block_from,
                filter.block_to,
                cursor,
                limit,
            )
            .map_err(|e| Status::internal(format!("Query failed: {e}")))?;

        let proto_transfers: Vec<TokenTransfer> =
            transfers.iter().map(Self::transfer_data_to_proto).collect();

        let proto_cursor = next_cursor.map(|c| Cursor {
            block_number: c.block_number,
            id: c.id,
        });

        Ok(Response::new(GetTransfersResponse {
            transfers: proto_transfers,
            next_cursor: proto_cursor,
        }))
    }

    /// Get balance for a specific contract, wallet, and token ID
    async fn get_balance(
        &self,
        request: Request<GetBalanceRequest>,
    ) -> Result<Response<GetBalanceResponse>, Status> {
        let req = request.into_inner();

        let contract = bytes_to_felt(&req.contract)
            .ok_or_else(|| Status::invalid_argument("Invalid contract address"))?;
        let wallet = bytes_to_felt(&req.wallet)
            .ok_or_else(|| Status::invalid_argument("Invalid wallet address"))?;
        let token_id = bytes_to_u256(&req.token_id);

        tracing::debug!(
            target: "torii_erc1155::grpc",
            "GetBalance: contract={:#x}, wallet={:#x}, token_id={}",
            contract,
            wallet,
            token_id
        );

        let (balance, last_block) = self
            .storage
            .get_balance_with_block(contract, wallet, token_id)
            .map_err(|e| Status::internal(format!("Query failed: {e}")))?
            .unwrap_or((U256::from(0u64), 0));

        Ok(Response::new(GetBalanceResponse {
            balance: u256_to_bytes(balance),
            last_block,
        }))
    }

    /// Get token metadata (name, symbol)
    async fn get_token_metadata(
        &self,
        request: Request<GetTokenMetadataRequest>,
    ) -> Result<Response<GetTokenMetadataResponse>, Status> {
        let req = request.into_inner();

        if let Some(token_bytes) = req.token {
            let token = bytes_to_felt(&token_bytes)
                .ok_or_else(|| Status::invalid_argument("Invalid token address"))?;

            let entries = match self.storage.get_token_metadata(token) {
                Ok(Some((name, symbol, total_supply))) => vec![TokenMetadataEntry {
                    token: token.to_bytes_be().to_vec(),
                    name,
                    symbol,
                    total_supply: total_supply.map(u256_to_bytes),
                }],
                Ok(None) => vec![],
                Err(e) => return Err(Status::internal(format!("Query failed: {e}"))),
            };

            return Ok(Response::new(GetTokenMetadataResponse {
                tokens: entries,
                next_cursor: None,
            }));
        }

        let cursor = req.cursor.as_ref().and_then(|b| bytes_to_felt(b));
        let limit = if req.limit == 0 {
            100
        } else {
            req.limit.min(1000)
        };

        let (all, next_cursor) = self
            .storage
            .get_token_metadata_paginated(cursor, limit)
            .map_err(|e| Status::internal(format!("Query failed: {e}")))?;

        let entries = all
            .into_iter()
            .map(|(token, name, symbol, total_supply)| TokenMetadataEntry {
                token: token.to_bytes_be().to_vec(),
                name,
                symbol,
                total_supply: total_supply.map(u256_to_bytes),
            })
            .collect();

        Ok(Response::new(GetTokenMetadataResponse {
            tokens: entries,
            next_cursor: next_cursor.map(|c| c.to_bytes_be().to_vec()),
        }))
    }

    /// Subscribe to real-time transfer events
    type SubscribeTransfersStream =
        Pin<Box<dyn Stream<Item = Result<TransferUpdate, Status>> + Send>>;

    async fn subscribe_transfers(
        &self,
        request: Request<SubscribeTransfersRequest>,
    ) -> Result<Response<Self::SubscribeTransfersStream>, Status> {
        let req = request.into_inner();
        let filter = req.filter.unwrap_or_default();

        tracing::info!(
            target: "torii_erc1155::grpc",
            "New transfer subscription from client: {}",
            req.client_id
        );

        let mut rx = self.transfer_tx.subscribe();

        let stream = async_stream::try_stream! {
            loop {
                match rx.recv().await {
                    Ok(update) => {
                        if let Some(ref transfer) = update.transfer {
                            if !Self::matches_transfer_filter(transfer, &filter) {
                                continue;
                            }
                        }
                        yield update;
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            target: "torii_erc1155::grpc",
                            "Client {} lagged, skipped {} updates",
                            req.client_id,
                            skipped
                        );
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }

    /// Get indexer statistics
    async fn get_stats(
        &self,
        _request: Request<GetStatsRequest>,
    ) -> Result<Response<GetStatsResponse>, Status> {
        let total_transfers = self
            .storage
            .get_transfer_count()
            .map_err(|e| Status::internal(format!("Failed to get transfer count: {e}")))?;

        let unique_tokens = self
            .storage
            .get_token_count()
            .map_err(|e| Status::internal(format!("Failed to get token count: {e}")))?;

        let unique_token_ids = self
            .storage
            .get_token_id_count()
            .map_err(|e| Status::internal(format!("Failed to get token ID count: {e}")))?;

        let latest_block = self
            .storage
            .get_latest_block()
            .map_err(|e| Status::internal(format!("Failed to get latest block: {e}")))?
            .unwrap_or(0);

        Ok(Response::new(GetStatsResponse {
            total_transfers,
            unique_tokens,
            unique_token_ids,
            latest_block,
        }))
    }
}
