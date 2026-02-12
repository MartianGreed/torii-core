//! gRPC service implementation for topic-based subscriptions.
//!
//! Provides the core Torii gRPC service that manages client subscriptions
//! and broadcasts updates from sinks to subscribed clients.

use futures_util::StreamExt as FuturesStreamExt;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

pub mod proto {
    tonic::include_proto!("torii");
}

// Re-export commonly used types
pub use proto::{TopicUpdate, UpdateType};

use proto::{
    torii_server::{Torii, ToriiServer},
    GetVersionRequest, GetVersionResponse, ListTopicsRequest, ListTopicsResponse,
    SubscriptionRequest, TopicSubscription,
};

/// Client subscription information
#[derive(Clone, Debug)]
pub struct ClientSubscription {
    /// A mapping of topic names to a mapping of filter names to filter values
    pub topics: HashMap<String, HashMap<String, String>>,
    /// A channel to send topic updates to the client
    pub tx: mpsc::Sender<TopicUpdate>,
}

/// Centralized subscription manager
///
/// Manages client subscriptions and broadcasts updates from sinks to subscribed clients.
#[derive(Clone)]
pub struct SubscriptionManager {
    /// Mapping of client IDs to their subscriptions
    clients: Arc<RwLock<HashMap<String, ClientSubscription>>>,
}

impl SubscriptionManager {
    /// Creates a new subscription manager
    pub fn new() -> Self {
        SubscriptionManager {
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Returns a reference to the mapping of client IDs to their subscriptions
    pub fn clients(&self) -> &Arc<RwLock<HashMap<String, ClientSubscription>>> {
        &self.clients
    }

    /// Registers a new client with the subscription manager
    pub fn register_client(&self, client_id: String, tx: mpsc::Sender<TopicUpdate>) {
        let mut clients = self.clients.write().unwrap();
        clients.insert(
            client_id.clone(),
            ClientSubscription {
                topics: HashMap::new(),
                tx,
            },
        );
        tracing::info!(target: "torii::grpc", "Client {} registered", client_id);
    }

    /// Unregisters a client from the subscription manager
    pub fn unregister_client(&self, client_id: &str) {
        let mut clients = self.clients.write().unwrap();
        clients.remove(client_id);
        tracing::info!(target: "torii::grpc", "Client {} unregistered", client_id);
    }

    /// Updates the subscriptions for a client
    pub fn update_subscriptions(
        &self,
        client_id: &str,
        topics: Vec<TopicSubscription>,
        unsubscribe_topics: Vec<String>,
    ) {
        let mut clients = self.clients.write().unwrap();
        if let Some(client) = clients.get_mut(client_id) {
            for topic in unsubscribe_topics {
                if client.topics.remove(&topic).is_some() {
                    tracing::info!(
                        target: "torii::grpc",
                        "Client {} unsubscribed from topic '{}'",
                        client_id,
                        topic
                    );
                }
            }

            for topic_sub in topics {
                client
                    .topics
                    .insert(topic_sub.topic.clone(), topic_sub.filters.clone());
                tracing::debug!(
                    target: "torii::grpc",
                    "Client {} subscribed to topic '{}' with {} filters",
                    client_id,
                    topic_sub.topic,
                    topic_sub.filters.len()
                );
            }

            tracing::info!(
                target: "torii::grpc",
                "Client {} updated subscriptions: {} topics",
                client_id,
                client.topics.len()
            );
        }
    }
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// gRPC service state
///
/// Contains the subscription manager for handling client subscriptions.
/// This is kept minimal - sinks can maintain their own state separately.
#[derive(Clone)]
pub struct GrpcState {
    subscription_manager: Arc<SubscriptionManager>,
    topics: Vec<crate::etl::sink::TopicInfo>,
}

impl GrpcState {
    pub fn new(
        subscription_manager: Arc<SubscriptionManager>,
        topics: Vec<crate::etl::sink::TopicInfo>,
    ) -> Self {
        GrpcState {
            subscription_manager,
            topics,
        }
    }

    pub fn subscription_manager(&self) -> &Arc<SubscriptionManager> {
        &self.subscription_manager
    }
}

// gRPC service implementation
pub struct ToriiService {
    state: GrpcState,
}

impl ToriiService {
    pub fn new(state: GrpcState) -> Self {
        ToriiService { state }
    }
}

#[tonic::async_trait]
impl Torii for ToriiService {
    async fn get_version(
        &self,
        _request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        Ok(Response::new(GetVersionResponse {
            version: env!("CARGO_PKG_VERSION").to_string(),
            build_time: chrono::Utc::now().format("%Y-%m-%d").to_string(),
        }))
    }

    async fn list_topics(
        &self,
        request: Request<ListTopicsRequest>,
    ) -> Result<Response<ListTopicsResponse>, Status> {
        // Log request metadata to debug gRPC-web
        let metadata = request.metadata();
        tracing::debug!(
            target: "torii::grpc",
            "ListTopics request received, metadata: {:?}",
            metadata
        );

        let topics = self
            .state
            .topics
            .iter()
            .map(|topic_info| proto::TopicInfo {
                name: topic_info.name.clone(),
                sink_name: String::new(), // Can be populated by sinks if needed
                available_filters: topic_info.available_filters.clone(),
                description: topic_info.description.clone(),
            })
            .collect();

        tracing::info!(
            target: "torii::grpc",
            "ListTopics returning {} topics",
            self.state.topics.len()
        );

        Ok(Response::new(ListTopicsResponse { topics }))
    }

    type SubscribeToTopicsStreamStream =
        Pin<Box<dyn Stream<Item = Result<TopicUpdate, Status>> + Send>>;

    async fn subscribe_to_topics_stream(
        &self,
        request: Request<SubscriptionRequest>,
    ) -> Result<Response<Self::SubscribeToTopicsStreamStream>, Status> {
        // Log request metadata to debug gRPC-web
        let metadata = request.metadata();
        tracing::debug!(
            target: "torii::grpc",
            "SubscribeToTopicsStream request metadata: {:?}",
            metadata
        );

        let sub_req = request.into_inner();
        let (tx, rx) = mpsc::channel(100);
        let subscription_manager = self.state.subscription_manager().clone();
        let client_id = sub_req.client_id.clone();

        // Register client and set up subscriptions
        subscription_manager.register_client(client_id.clone(), tx.clone());
        subscription_manager.update_subscriptions(
            &client_id,
            sub_req.topics,
            sub_req.unsubscribe_topics,
        );

        tracing::info!(
            target: "torii::grpc",
            "Client {} connected via SubscribeToTopicsStream",
            client_id
        );

        // Clean up as soon as the stream receiver is dropped (client disconnects).
        // This avoids stale client entries accumulating in SubscriptionManager.
        let cleanup_manager = subscription_manager;
        let cleanup_id = client_id;
        tokio::spawn(async move {
            tx.closed().await;
            cleanup_manager.unregister_client(&cleanup_id);
        });

        // Convert mpsc receiver to stream
        let output_stream = ReceiverStream::new(rx).map(Ok).boxed();

        Ok(Response::new(output_stream))
    }

    type SubscribeToTopicsStream = Pin<Box<dyn Stream<Item = Result<TopicUpdate, Status>> + Send>>;

    async fn subscribe_to_topics(
        &self,
        request: Request<Streaming<SubscriptionRequest>>,
    ) -> Result<Response<Self::SubscribeToTopicsStream>, Status> {
        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(100);
        let subscription_manager = self.state.subscription_manager().clone();

        // Spawn task to handle incoming subscription requests
        tokio::spawn(async move {
            let mut client_id: Option<String> = None;

            while let Some(result) = stream.next().await {
                match result {
                    Ok(sub_req) => {
                        // First request establishes client ID
                        if client_id.is_none() {
                            client_id = Some(sub_req.client_id.clone());
                            subscription_manager
                                .register_client(sub_req.client_id.clone(), tx.clone());
                        }

                        // Update subscriptions
                        subscription_manager.update_subscriptions(
                            &sub_req.client_id,
                            sub_req.topics,
                            sub_req.unsubscribe_topics,
                        );
                    }
                    Err(e) => {
                        tracing::error!(target: "torii::grpc", "Error receiving subscription request: {}", e);
                        break;
                    }
                }
            }

            // Clean up on disconnect
            if let Some(id) = client_id {
                subscription_manager.unregister_client(&id);
            }
        });

        // Convert mpsc receiver to stream
        let output_stream = ReceiverStream::new(rx).map(Ok).boxed();

        Ok(Response::new(output_stream))
    }
}

pub fn create_grpc_service(state: GrpcState) -> ToriiServer<ToriiService> {
    ToriiServer::new(ToriiService::new(state))
}
