//! ERC1155 sink for processing token transfers and operator approvals
//!
//! This sink:
//! - Stores transfer and approval records in the database
//! - Tracks balances with automatic inconsistency detection
//! - Publishes events via EventBus for real-time subscriptions
//! - Broadcasts events via gRPC service for rich subscriptions
//!
//! Balance tracking uses a "fetch-on-inconsistency" approach:
//! - Computes balances from transfer events
//! - When a balance would go negative (genesis allocation, airdrop, etc.),
//!   fetches the actual balance from the chain and adjusts

use crate::balance_fetcher::Erc1155BalanceFetcher;
use crate::decoder::{
    OperatorApproval as DecodedOperatorApproval, TransferBatch as DecodedTransferBatch,
    TransferSingle as DecodedTransferSingle, UriUpdate as DecodedUriUpdate,
};
use crate::grpc_service::Erc1155Service;
use crate::proto;
use crate::storage::{Erc1155Storage, OperatorApprovalData, TokenTransferData, TokenUriData};
use anyhow::Result;
use async_trait::async_trait;
use axum::Router;
use prost::Message;
use prost_types::Any;
use starknet::core::types::{Felt, U256};
use starknet::providers::jsonrpc::{HttpTransport, JsonRpcClient};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use torii::etl::sink::{EventBus, TopicInfo};
use torii::etl::{Envelope, ExtractionBatch, Sink, TypeId};
use torii::grpc::UpdateType;
use torii_common::{
    u256_to_bytes, MetadataFetcher, TokenStandard, TokenUriRequest, TokenUriSender,
};

/// Default threshold for "live" detection: 100 blocks from chain head.
/// Events from blocks older than this won't be broadcast to real-time subscribers.
const LIVE_THRESHOLD_BLOCKS: u64 = 100;

/// ERC1155 token sink
///
/// Processes ERC1155 TransferSingle, TransferBatch, and ApprovalForAll events:
/// - Stores transfer records in the database
/// - Tracks balances with automatic inconsistency detection
/// - Publishes events via EventBus for real-time subscriptions (only when live)
/// - Broadcasts events via gRPC service for rich subscriptions (only when live)
///
/// During historical indexing (more than 100 blocks from chain head), events are
/// stored but not broadcast to avoid overwhelming real-time subscribers.
pub struct Erc1155Sink {
    storage: Arc<Erc1155Storage>,
    event_bus: Option<Arc<EventBus>>,
    grpc_service: Option<Erc1155Service>,
    /// Balance fetcher for RPC calls (None = balance tracking disabled)
    balance_fetcher: Option<Arc<Erc1155BalanceFetcher>>,
    /// Metadata fetcher for contract name/symbol
    metadata_fetcher: Option<Arc<MetadataFetcher>>,
    /// Token URI service sender for async URI fetching
    token_uri_sender: Option<TokenUriSender>,
}

impl Erc1155Sink {
    pub fn new(storage: Arc<Erc1155Storage>) -> Self {
        Self {
            storage,
            event_bus: None,
            grpc_service: None,
            balance_fetcher: None,
            metadata_fetcher: None,
            token_uri_sender: None,
        }
    }

    /// Enable async token URI fetching
    pub fn with_token_uri_sender(mut self, sender: TokenUriSender) -> Self {
        self.token_uri_sender = Some(sender);
        self
    }

    /// Set the gRPC service for dual publishing
    pub fn with_grpc_service(mut self, service: Erc1155Service) -> Self {
        self.grpc_service = Some(service);
        self
    }

    /// Enable balance tracking with a provider for RPC calls
    ///
    /// When enabled, the sink will:
    /// - Track balances computed from transfer events
    /// - Detect when a balance would go negative (indicating missed history)
    /// - Fetch actual balance from the chain and adjust
    /// - Record adjustments in an audit table
    pub fn with_balance_tracking(mut self, provider: Arc<JsonRpcClient<HttpTransport>>) -> Self {
        self.balance_fetcher = Some(Arc::new(Erc1155BalanceFetcher::new(provider.clone())));
        self.metadata_fetcher = Some(Arc::new(MetadataFetcher::new(provider)));
        self
    }

    /// Get a reference to the storage
    pub fn storage(&self) -> &Arc<Erc1155Storage> {
        &self.storage
    }

    /// Filter function for ERC1155 transfer events
    fn matches_transfer_filters(
        transfer: &proto::TokenTransfer,
        filters: &HashMap<String, String>,
    ) -> bool {
        if filters.is_empty() {
            return true;
        }

        // Wallet filter with OR logic (matches from OR to)
        if let Some(wallet_filter) = filters.get("wallet") {
            let from_hex = format!("0x{}", hex::encode(&transfer.from));
            let to_hex = format!("0x{}", hex::encode(&transfer.to));
            if !from_hex.eq_ignore_ascii_case(wallet_filter)
                && !to_hex.eq_ignore_ascii_case(wallet_filter)
            {
                return false;
            }
        }

        // Exact token filter
        if let Some(token_filter) = filters.get("token") {
            let token_hex = format!("0x{}", hex::encode(&transfer.token));
            if !token_hex.eq_ignore_ascii_case(token_filter) {
                return false;
            }
        }

        // Exact from filter
        if let Some(from_filter) = filters.get("from") {
            let from_hex = format!("0x{}", hex::encode(&transfer.from));
            if !from_hex.eq_ignore_ascii_case(from_filter) {
                return false;
            }
        }

        // Exact to filter
        if let Some(to_filter) = filters.get("to") {
            let to_hex = format!("0x{}", hex::encode(&transfer.to));
            if !to_hex.eq_ignore_ascii_case(to_filter) {
                return false;
            }
        }

        true
    }

    /// Filter function for ERC1155 token metadata updates.
    ///
    /// Supports filters:
    /// - "token": Filter by token contract address (hex string)
    fn matches_metadata_filters(
        metadata: &proto::TokenMetadataEntry,
        filters: &HashMap<String, String>,
    ) -> bool {
        if filters.is_empty() {
            return true;
        }

        if let Some(token_filter) = filters.get("token") {
            let token_hex = format!("0x{}", hex::encode(&metadata.token));
            if !token_hex.eq_ignore_ascii_case(token_filter) {
                return false;
            }
        }

        true
    }

    /// Filter function for ERC1155 URI updates.
    ///
    /// Supports filters:
    /// - "token": Filter by token contract address (hex string)
    /// - "token_id": Filter by token id (hex string)
    fn matches_uri_filters(uri: &proto::TokenUri, filters: &HashMap<String, String>) -> bool {
        if filters.is_empty() {
            return true;
        }

        if let Some(token_filter) = filters.get("token") {
            let token_hex = format!("0x{}", hex::encode(&uri.token));
            if !token_hex.eq_ignore_ascii_case(token_filter) {
                return false;
            }
        }

        if let Some(token_id_filter) = filters.get("token_id") {
            let token_id_hex = format!("0x{}", hex::encode(&uri.token_id));
            if !token_id_hex.eq_ignore_ascii_case(token_id_filter) {
                return false;
            }
        }

        true
    }
}

#[async_trait]
impl Sink for Erc1155Sink {
    fn name(&self) -> &'static str {
        "erc1155"
    }

    fn interested_types(&self) -> Vec<TypeId> {
        vec![
            TypeId::new("erc1155.transfer_single"),
            TypeId::new("erc1155.transfer_batch"),
            TypeId::new("erc1155.approval_for_all"),
            TypeId::new("erc1155.uri"),
        ]
    }

    async fn initialize(
        &mut self,
        event_bus: Arc<EventBus>,
        _context: &torii::etl::sink::SinkContext,
    ) -> Result<()> {
        self.event_bus = Some(event_bus);
        tracing::info!(target: "torii_erc1155::sink", "ERC1155 sink initialized");
        Ok(())
    }

    async fn process(&self, envelopes: &[Envelope], batch: &ExtractionBatch) -> Result<()> {
        let mut transfers: Vec<TokenTransferData> = Vec::new();
        let mut operator_approvals: Vec<OperatorApprovalData> = Vec::new();
        let mut uri_updates: Vec<TokenUriData> = Vec::new();

        // Get block timestamps from batch
        let block_timestamps: HashMap<u64, i64> = batch
            .blocks
            .iter()
            .map(|(num, block)| (*num, block.timestamp as i64))
            .collect();

        for envelope in envelopes {
            // Handle single transfers
            if envelope.type_id == TypeId::new("erc1155.transfer_single") {
                if let Some(transfer) = envelope
                    .body
                    .as_any()
                    .downcast_ref::<DecodedTransferSingle>()
                {
                    let timestamp = block_timestamps.get(&transfer.block_number).copied();
                    transfers.push(TokenTransferData {
                        id: None,
                        token: transfer.token,
                        operator: transfer.operator,
                        from: transfer.from,
                        to: transfer.to,
                        token_id: transfer.id,
                        amount: transfer.value,
                        is_batch: false,
                        batch_index: 0,
                        block_number: transfer.block_number,
                        tx_hash: transfer.transaction_hash,
                        timestamp,
                    });
                }
            }
            // Handle batch transfers
            else if envelope.type_id == TypeId::new("erc1155.transfer_batch") {
                if let Some(transfer) = envelope
                    .body
                    .as_any()
                    .downcast_ref::<DecodedTransferBatch>()
                {
                    let timestamp = block_timestamps.get(&transfer.block_number).copied();
                    transfers.push(TokenTransferData {
                        id: None,
                        token: transfer.token,
                        operator: transfer.operator,
                        from: transfer.from,
                        to: transfer.to,
                        token_id: transfer.id,
                        amount: transfer.value,
                        is_batch: true,
                        batch_index: transfer.batch_index,
                        block_number: transfer.block_number,
                        tx_hash: transfer.transaction_hash,
                        timestamp,
                    });
                }
            }
            // Handle approval for all
            else if envelope.type_id == TypeId::new("erc1155.approval_for_all") {
                if let Some(approval) = envelope
                    .body
                    .as_any()
                    .downcast_ref::<DecodedOperatorApproval>()
                {
                    let timestamp = block_timestamps.get(&approval.block_number).copied();
                    operator_approvals.push(OperatorApprovalData {
                        id: None,
                        token: approval.token,
                        owner: approval.owner,
                        operator: approval.operator,
                        approved: approval.approved,
                        block_number: approval.block_number,
                        tx_hash: approval.transaction_hash,
                        timestamp,
                    });
                }
            }
            // Handle URI updates
            else if envelope.type_id == TypeId::new("erc1155.uri") {
                if let Some(uri) = envelope.body.as_any().downcast_ref::<DecodedUriUpdate>() {
                    let timestamp = block_timestamps.get(&uri.block_number).copied();
                    uri_updates.push(TokenUriData {
                        token: uri.token,
                        token_id: uri.token_id,
                        uri: uri.uri.clone(),
                        block_number: uri.block_number,
                        tx_hash: uri.transaction_hash,
                        timestamp,
                    });
                }
            }
        }

        // Fetch metadata for any new token contracts
        if let Some(ref fetcher) = self.metadata_fetcher {
            let new_tokens: HashSet<Felt> = transfers.iter().map(|t| t.token).collect();
            for token in new_tokens {
                match self.storage.has_token_metadata(token) {
                    Ok(exists) => {
                        if exists {
                            continue;
                        }

                        let meta = fetcher.fetch_erc1155_metadata(token).await;
                        tracing::info!(
                            target: "torii_erc1155::sink",
                            token = %format!("{:#x}", token),
                            name = ?meta.name,
                            symbol = ?meta.symbol,
                            "Fetched token metadata"
                        );
                        if let Err(e) = self.storage.upsert_token_metadata(
                            token,
                            meta.name.as_deref(),
                            meta.symbol.as_deref(),
                            meta.total_supply,
                        ) {
                            tracing::warn!(
                                target: "torii_erc1155::sink",
                                error = %e,
                                "Failed to store token metadata"
                            );
                        } else if let Some(event_bus) = &self.event_bus {
                            let meta_entry = proto::TokenMetadataEntry {
                                token: token.to_bytes_be().to_vec(),
                                name: meta.name,
                                symbol: meta.symbol,
                                total_supply: meta.total_supply.map(u256_to_bytes),
                            };

                            let mut buf = Vec::new();
                            meta_entry.encode(&mut buf)?;
                            let any = Any {
                                type_url:
                                    "type.googleapis.com/torii.sinks.erc1155.TokenMetadataEntry"
                                        .to_string(),
                                value: buf,
                            };

                            event_bus.publish_protobuf(
                                "erc1155.metadata",
                                "erc1155.metadata",
                                &any,
                                &meta_entry,
                                UpdateType::Created,
                                Self::matches_metadata_filters,
                            );
                        }
                    }
                    Err(e) => {
                        tracing::warn!(target: "torii_erc1155::sink", error = %e, "Failed to check token metadata");
                    }
                }
            }
        }

        // Request token URI fetches for new token IDs
        if let Some(ref sender) = self.token_uri_sender {
            for transfer in &transfers {
                match self
                    .storage
                    .has_token_uri(transfer.token, transfer.token_id)
                {
                    Ok(false) => {
                        sender
                            .request_update(TokenUriRequest {
                                contract: transfer.token,
                                token_id: transfer.token_id,
                                standard: TokenStandard::Erc1155,
                            })
                            .await;
                    }
                    Ok(true) => {}
                    Err(e) => {
                        tracing::warn!(
                            target: "torii_erc1155::sink",
                            error = %e,
                            "Failed to check token URI existence"
                        );
                    }
                }
            }
        }

        // Batch insert transfers
        if !transfers.is_empty() {
            let transfer_count = match self.storage.insert_transfers_batch(&transfers) {
                Ok(count) => count,
                Err(e) => {
                    tracing::error!(
                        target: "torii_erc1155::sink",
                        count = transfers.len(),
                        error = %e,
                        "Failed to batch insert transfers"
                    );
                    return Err(e);
                }
            };

            if transfer_count > 0 {
                tracing::info!(
                    target: "torii_erc1155::sink",
                    count = transfer_count,
                    "Batch inserted token transfers"
                );

                // Update balances if balance tracking is enabled
                if let Some(ref fetcher) = self.balance_fetcher {
                    // Step 1: Check which balances need adjustment (would go negative)
                    let adjustment_requests = match self.storage.check_balances_batch(&transfers) {
                        Ok(requests) => requests,
                        Err(e) => {
                            tracing::warn!(
                                target: "torii_erc1155::sink",
                                error = %e,
                                "Failed to check balance inconsistencies, skipping balance tracking"
                            );
                            Vec::new()
                        }
                    };

                    // Step 2: Batch fetch actual balances from RPC for inconsistent wallets
                    let mut adjustments: HashMap<(Felt, Felt, U256), U256> = HashMap::new();
                    if !adjustment_requests.is_empty() {
                        tracing::info!(
                            target: "torii_erc1155::sink",
                            count = adjustment_requests.len(),
                            "Fetching balance adjustments from RPC"
                        );

                        match fetcher.fetch_balances_batch(&adjustment_requests).await {
                            Ok(fetched) => {
                                for (contract, wallet, token_id, balance) in fetched {
                                    adjustments.insert((contract, wallet, token_id), balance);
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    target: "torii_erc1155::sink",
                                    error = %e,
                                    "Failed to fetch balances from RPC, using 0 for adjustments"
                                );
                                // On failure, use 0 for all requested adjustments
                                for req in &adjustment_requests {
                                    adjustments.insert(
                                        (req.contract, req.wallet, req.token_id),
                                        U256::from(0u64),
                                    );
                                }
                            }
                        }
                    }

                    // Step 3: Apply transfers with adjustments to update balances
                    if let Err(e) = self
                        .storage
                        .apply_transfers_with_adjustments(&transfers, &adjustments)
                    {
                        tracing::error!(
                            target: "torii_erc1155::sink",
                            error = %e,
                            "Failed to apply balance updates"
                        );
                        // Don't fail the whole batch - transfers are already inserted
                    }
                }

                // Only broadcast to real-time subscribers when near chain head
                let is_live = batch.is_live(LIVE_THRESHOLD_BLOCKS);
                if is_live {
                    // Publish transfer events
                    for transfer in &transfers {
                        let proto_transfer = proto::TokenTransfer {
                            token: transfer.token.to_bytes_be().to_vec(),
                            operator: transfer.operator.to_bytes_be().to_vec(),
                            from: transfer.from.to_bytes_be().to_vec(),
                            to: transfer.to.to_bytes_be().to_vec(),
                            token_id: u256_to_bytes(transfer.token_id),
                            amount: u256_to_bytes(transfer.amount),
                            block_number: transfer.block_number,
                            tx_hash: transfer.tx_hash.to_bytes_be().to_vec(),
                            timestamp: transfer.timestamp.unwrap_or(0),
                            is_batch: transfer.is_batch,
                            batch_index: transfer.batch_index,
                        };

                        // Publish to EventBus
                        if let Some(event_bus) = &self.event_bus {
                            let mut buf = Vec::new();
                            proto_transfer.encode(&mut buf)?;
                            let any = Any {
                                type_url: "type.googleapis.com/torii.sinks.erc1155.TokenTransfer"
                                    .to_string(),
                                value: buf,
                            };

                            event_bus.publish_protobuf(
                                "erc1155.transfer",
                                "erc1155.transfer",
                                &any,
                                &proto_transfer,
                                UpdateType::Created,
                                Self::matches_transfer_filters,
                            );
                        }

                        // Broadcast to gRPC service
                        if let Some(grpc_service) = &self.grpc_service {
                            grpc_service.broadcast_transfer(proto_transfer);
                        }
                    }
                }
            }
        }

        // Batch insert operator approvals
        if !operator_approvals.is_empty() {
            match self
                .storage
                .insert_operator_approvals_batch(&operator_approvals)
            {
                Ok(count) => {
                    tracing::info!(
                        target: "torii_erc1155::sink",
                        count = count,
                        "Batch inserted operator approvals"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        target: "torii_erc1155::sink",
                        "Failed to batch insert {} operator approvals: {}",
                        operator_approvals.len(),
                        e
                    );
                    return Err(e);
                }
            }
        }

        // Batch upsert token URI updates
        if !uri_updates.is_empty() {
            match self.storage.upsert_token_uris_batch(&uri_updates) {
                Ok(count) => {
                    tracing::info!(
                        target: "torii_erc1155::sink",
                        count = count,
                        "Batch upserted token URI updates"
                    );

                    // Publish URI updates to topic subscribers
                    if let Some(event_bus) = &self.event_bus {
                        for uri in &uri_updates {
                            let proto_uri = proto::TokenUri {
                                token: uri.token.to_bytes_be().to_vec(),
                                token_id: u256_to_bytes(uri.token_id),
                                uri: uri.uri.clone(),
                                block_number: uri.block_number,
                            };

                            let mut buf = Vec::new();
                            proto_uri.encode(&mut buf)?;
                            let any = Any {
                                type_url: "type.googleapis.com/torii.sinks.erc1155.TokenUri"
                                    .to_string(),
                                value: buf,
                            };

                            event_bus.publish_protobuf(
                                "erc1155.uri",
                                "erc1155.uri",
                                &any,
                                &proto_uri,
                                UpdateType::Updated,
                                Self::matches_uri_filters,
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(
                        target: "torii_erc1155::sink",
                        count = uri_updates.len(),
                        error = %e,
                        "Failed to batch upsert token URI updates"
                    );
                    return Err(e);
                }
            }
        }

        // Log combined statistics
        if !transfers.is_empty() || !operator_approvals.is_empty() {
            if let Ok(total_transfers) = self.storage.get_transfer_count() {
                if let Ok(token_id_count) = self.storage.get_token_id_count() {
                    if let Ok(token_count) = self.storage.get_token_count() {
                        tracing::info!(
                            target: "torii_erc1155::sink",
                            transfers = total_transfers,
                            token_ids = token_id_count,
                            contracts = token_count,
                            blocks = batch.blocks.len(),
                            "Total statistics"
                        );
                    }
                }
            }
        }

        Ok(())
    }

    fn topics(&self) -> Vec<TopicInfo> {
        vec![
            TopicInfo::new(
                "erc1155.transfer",
                vec![
                    "token".to_string(),
                    "from".to_string(),
                    "to".to_string(),
                    "wallet".to_string(),
                ],
                "ERC1155 token transfers. Use 'wallet' filter for from OR to matching.",
            ),
            TopicInfo::new(
                "erc1155.metadata",
                vec!["token".to_string()],
                "ERC1155 token metadata updates (registered/updated token attributes).",
            ),
            TopicInfo::new(
                "erc1155.uri",
                vec!["token".to_string(), "token_id".to_string()],
                "ERC1155 token URI updates (registered/updated token attributes).",
            ),
        ]
    }

    fn build_routes(&self) -> Router {
        Router::new()
    }
}
