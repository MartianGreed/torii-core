//! gRPC service implementation for ERC721 queries and subscriptions

use crate::proto::{
    erc721_server::Erc721 as Erc721Trait, Cursor, GetOwnerRequest, GetOwnerResponse,
    GetOwnershipRequest, GetOwnershipResponse, GetStatsRequest, GetStatsResponse,
    GetTokenMetadataRequest, GetTokenMetadataResponse, GetTransfersRequest, GetTransfersResponse,
    NftTransfer, Ownership, SubscribeTransfersRequest, TokenMetadataEntry, TransferFilter,
    TransferUpdate,
};
use crate::storage::{Erc721Storage, NftTransferData, TransferCursor};
use async_trait::async_trait;
use futures::stream::Stream;
use starknet::core::types::Felt;
use starknet::core::types::U256;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast;
use tonic::{Request, Response, Status};
use torii_common::{bytes_to_felt, bytes_to_u256, u256_to_bytes};

/// gRPC service implementation for ERC721
#[derive(Clone)]
pub struct Erc721Service {
    storage: Arc<Erc721Storage>,
    /// Broadcast channel for real-time transfer updates
    pub transfer_tx: broadcast::Sender<TransferUpdate>,
}

impl Erc721Service {
    /// Creates a new Erc721Service
    pub fn new(storage: Arc<Erc721Storage>) -> Self {
        let (transfer_tx, _) = broadcast::channel(1000);

        Self {
            storage,
            transfer_tx,
        }
    }

    /// Broadcasts a transfer to all subscribers
    pub fn broadcast_transfer(&self, transfer: NftTransfer) {
        let update = TransferUpdate {
            transfer: Some(transfer),
            timestamp: chrono::Utc::now().timestamp(),
        };
        let _ = self.transfer_tx.send(update);
    }

    /// Convert storage NftTransferData to proto NftTransfer
    fn transfer_data_to_proto(data: &NftTransferData) -> NftTransfer {
        NftTransfer {
            token: data.token.to_bytes_be().to_vec(),
            token_id: u256_to_bytes(data.token_id),
            from: data.from.to_bytes_be().to_vec(),
            to: data.to.to_bytes_be().to_vec(),
            block_number: data.block_number,
            tx_hash: data.tx_hash.to_bytes_be().to_vec(),
            timestamp: data.timestamp.unwrap_or(0),
        }
    }

    /// Check if a transfer matches a filter (for subscriptions)
    fn matches_transfer_filter(transfer: &NftTransfer, filter: &TransferFilter) -> bool {
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
impl Erc721Trait for Erc721Service {
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
                &tokens,
                &token_ids,
                filter.block_from,
                filter.block_to,
                cursor,
                limit,
            )
            .map_err(|e| Status::internal(format!("Query failed: {e}")))?;

        let proto_transfers: Vec<NftTransfer> =
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

    /// Query current NFT ownership
    async fn get_ownership(
        &self,
        request: Request<GetOwnershipRequest>,
    ) -> Result<Response<GetOwnershipResponse>, Status> {
        let req = request.into_inner();
        let filter = req.filter.unwrap_or_default();

        let owner = filter.owner.as_ref().and_then(|b| bytes_to_felt(b));
        let tokens: Vec<Felt> = filter
            .tokens
            .iter()
            .filter_map(|b| bytes_to_felt(b))
            .collect();

        let cursor = req.cursor.map(|c| crate::storage::OwnershipCursor {
            block_number: c.block_number,
            id: c.id,
        });

        let limit = if req.limit == 0 {
            100
        } else {
            req.limit.min(1000)
        };

        let owner = owner.ok_or_else(|| Status::invalid_argument("owner filter is required"))?;

        let (ownership, next_cursor) = self
            .storage
            .get_ownership_by_owner(owner, &tokens, cursor, limit)
            .map_err(|e| Status::internal(format!("Query failed: {e}")))?;

        let proto_ownership: Vec<Ownership> = ownership
            .iter()
            .map(|o| Ownership {
                token: o.token.to_bytes_be().to_vec(),
                token_id: u256_to_bytes(o.token_id),
                owner: o.owner.to_bytes_be().to_vec(),
                block_number: o.block_number,
            })
            .collect();

        let proto_cursor = next_cursor.map(|c| Cursor {
            block_number: c.block_number,
            id: c.id,
        });

        Ok(Response::new(GetOwnershipResponse {
            ownership: proto_ownership,
            next_cursor: proto_cursor,
        }))
    }

    /// Get the current owner of a specific NFT
    async fn get_owner(
        &self,
        request: Request<GetOwnerRequest>,
    ) -> Result<Response<GetOwnerResponse>, Status> {
        let req = request.into_inner();

        let token = bytes_to_felt(&req.token)
            .ok_or_else(|| Status::invalid_argument("invalid token address"))?;
        let token_id = bytes_to_u256(&req.token_id);

        let owner = self
            .storage
            .get_owner(token, token_id)
            .map_err(|e| Status::internal(format!("Query failed: {e}")))?;

        Ok(Response::new(GetOwnerResponse {
            owner: owner.map(|o| o.to_bytes_be().to_vec()),
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
            target: "torii_erc721::grpc",
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
                            target: "torii_erc721::grpc",
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

        let unique_nfts = self
            .storage
            .get_nft_count()
            .map_err(|e| Status::internal(format!("Failed to get NFT count: {e}")))?;

        let latest_block = self
            .storage
            .get_latest_block()
            .map_err(|e| Status::internal(format!("Failed to get latest block: {e}")))?
            .unwrap_or(0);

        Ok(Response::new(GetStatsResponse {
            total_transfers,
            unique_tokens,
            unique_nfts,
            latest_block,
        }))
    }
}
