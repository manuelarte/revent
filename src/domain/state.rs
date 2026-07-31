use crate::config::Config;
use crate::domain::client::ClientInfo;
use crate::domain::messages::client::{
    ClientMessage, QueryRequestMessage, QueryResponseMessage, RegisterClientMessage,
    RegisterSourceEventMessage,
};
use crate::domain::messages::server::ServerMessage::Heartbeat;
use crate::domain::messages::server::{QueryRespondedMessage, ServerMessage};
use crate::domain::messages::{QueryId, RequestId};
use crate::domain::query_handlers::{OngoingQueryRequest, QueryHandler, QueryRequestHandlerError};
use crate::domain::source_events::NewSourceEvent;
use crate::domain::tx::Tx;
use crate::domain::{ClientId, PageRequest, PageResponse, Repository};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{error, info, trace, warn};

/// `State` is the state of the domain node.
#[derive(Clone)]
pub struct State<R: Repository> {
    /// `repository` repository for the source event.
    repository: R,
    /// `connected_clients` maps client IDs to their corresponding `ClientInfo`.
    connected_clients: Arc<RwLock<HashMap<ClientId, ClientInfo>>>,
    /// `query_handlers` struct to handle clients' queries.
    query_handlers: QueryHandler,
}

// Constructors and getters
impl<R: Repository + 'static> State<R> {
    pub fn new(cfg: &Config, repository: R) -> Self {
        let state = Self {
            repository,
            connected_clients: Arc::new(RwLock::new(HashMap::default())),
            query_handlers: QueryHandler::new(),
        };
        state.start(
            *cfg.server_interval_heartbeat_message(),
            *cfg.server_expiration_client(),
            *cfg.server_query_response_timeout(),
        );

        state
    }

    pub async fn get_connected_clients(
        &self,
        page_request: PageRequest,
    ) -> PageResponse<HashMap<ClientId, ClientInfo>> {
        let connected_clients = self.connected_clients.read().await;
        let total_count = connected_clients.len();

        if page_request.offset() == 0 || total_count == 0 {
            return PageResponse::new(Vec::new(), total_count);
        }

        let start = page_request.page();
        if start >= total_count {
            return PageResponse::new(Vec::new(), total_count);
        }

        // Sort by client id so pages are stable regardless of HashMap iteration order.
        let mut sorted_clients: Vec<(ClientId, ClientInfo)> = connected_clients
            .iter()
            .map(|(client_id, client_info)| (client_id.clone(), client_info.clone()))
            .collect();
        sorted_clients.sort_by(|(a, _), (b, _)| a.cmp(b));

        let end = (start + page_request.offset()).min(total_count);
        let items = sorted_clients[start..end]
            .iter()
            .map(|(client_id, client_info)| {
                HashMap::from([(client_id.clone(), client_info.clone())])
            })
            .collect();

        PageResponse::new(items, total_count)
    }

    pub async fn get_client_by_id(&self, client_id: impl Into<&ClientId>) -> Option<ClientInfo> {
        self.connected_clients
            .read()
            .await
            .get(client_id.into())
            .cloned()
    }

    /// `get_query_handlers_for_client_id` get the queries that can be responded by the `client_id`.
    pub async fn get_query_handlers_for_client_id(&self, client_id: &ClientId) -> Vec<QueryId> {
        self.query_handlers
            .get_query_handlers_for_client_id(client_id)
            .await
    }

    fn start(
        &self,
        server_interval_heartbeat_message: Duration,
        server_expiration_client: Duration,
        query_response_timeout: Duration,
    ) {
        async fn old_clients<R: Repository>(
            state: &State<R>,
            server_expiration_client: Duration,
        ) -> Vec<(ClientId, Arc<dyn Tx>)> {
            state
                .connected_clients
                .read()
                .await
                .iter()
                .filter(|(_client_id, client_info)| {
                    client_info.last_seen().elapsed() > server_expiration_client
                })
                .map(|(client_id, handle)| (client_id.clone(), handle.tx()))
                .collect::<Vec<_>>()
        }

        info!("Domain node started.");
        let state_clone = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(server_interval_heartbeat_message);
            loop {
                interval.tick().await;

                let txs = old_clients(&state_clone, server_expiration_client).await;

                for (client_id, tx) in txs {
                    let tx_msg = Heartbeat;
                    let msg = if matches!(tx.send(tx_msg).await, Ok(())) {
                        &ClientMessage::Heartbeat { client_id }
                    } else {
                        &ClientMessage::DisconnectClient { client_id }
                    };
                    let () = state_clone.handle_message(msg).await;
                }

                let expired_requests = state_clone
                    .query_handlers
                    .get_expired_requests(query_response_timeout)
                    .await;
                for expired_request in expired_requests {
                    let requester_info = state_clone
                        .get_client_by_id(expired_request.requester())
                        .await;
                    if let Some(requester_info) = requester_info {
                        let tx = requester_info.tx();
                        let tx_msg = ServerMessage::query_timed_out(
                            expired_request.request_id().clone(),
                            expired_request.query_id().clone(),
                        );
                        tx.send(tx_msg).await.ok();
                    }
                    state_clone
                        .handle_query_timed_out(expired_request.request_id())
                        .await;
                }
            }
        });
    }
}

// handle messages methods
impl<R: Repository + 'static> State<R> {
    /// `handle_message` handles incoming messages from the clients.
    pub async fn handle_message(&self, msg: &ClientMessage) {
        if let Some(source) = msg.get_source() {
            let mut connected_clients = self.connected_clients.write().await;
            if let Some(client) = connected_clients.get_mut(source) {
                client.update_last_seen(Instant::now());
            }
        }
        match msg {
            ClientMessage::RegisterClient(msg) => self.handle_register_client_message(msg).await,
            ClientMessage::QueryRequest(msg) => self.handle_query_request_message(msg).await,
            ClientMessage::QueryResponse(msg) => self.handle_query_response_message(msg).await,
            ClientMessage::RegisterSourceEvent(msg) => {
                self.handle_register_source_event_message(msg).await;
            }
            ClientMessage::Heartbeat { client_id } => {
                trace!(client_id=%client_id, "Heartbeat acknowledged");
            }
            ClientMessage::DisconnectClient { client_id } => {
                self.handle_disconnected_client_message(client_id).await;
            }
        }
    }

    async fn handle_register_client_message(&self, msg: &RegisterClientMessage) {
        let client_id = msg.client_id().clone();
        if self.get_client_by_id(&client_id).await.is_some() {
            warn!(client_id=%client_id, "Attempted to register an already connected client");
            let _ = msg
                .tx()
                .send(ServerMessage::client_id_duplicated(client_id.clone()))
                .await;
            return;
        }

        if let Err(err) = msg
            .tx()
            .send(ServerMessage::ClientRegistered {
                client_id: msg.client_id().clone(),
            })
            .await
        {
            error!(err=%err, client_id=%msg.client_id(), "Can't send response for RegisterClientMessage");
            return;
        }

        let client_id = msg.client_id();
        {
            let mut lock = self.connected_clients.write().await;
            lock.insert(client_id.clone(), msg.into());
        }
        self.query_handlers
            .add_client_query_handler(client_id.clone(), msg.query_handlers())
            .await;
    }

    #[allow(clippy::too_many_lines)]
    async fn handle_query_request_message(&self, msg: &QueryRequestMessage) {
        let requester_client_info_option = self.get_client_by_id(msg.requester()).await;
        // Check for duplicate request ID
        if let Some(current_ongoing_query_request) = self
            .query_handlers
            .get_ongoing_request(msg.request_id())
            .await
        {
            if current_ongoing_query_request.is_same(msg) {
                return;
            }
            // Try to notify requester of duplicate request ID
            if let Some(requester_client_info) = requester_client_info_option {
                let _ = requester_client_info
                    .tx()
                    .send(ServerMessage::query_request_id_duplicated(msg))
                    .await;
            } else {
                warn!(
                    requester = %msg.requester(),
                    request_id = %msg.request_id(),
                    "Requester not found for duplicate request ID"
                );
            }
            info!(
                request_id = %msg.request_id(),
                query_id = %msg.query_id(),
                "Query rejected because request id is duplicated"
            );
            return;
        }

        // Get the selected responder for this query ID
        let selected_responder_result = self
            .query_handlers
            .get_client_id_for_query_id(msg.query_id())
            .await;

        match selected_responder_result {
            Ok(selected_responder) => {
                // Try to get the responder's client info
                let selected_handler = self
                    .connected_clients
                    .read()
                    .await
                    .get(&selected_responder)
                    .cloned()
                    .map(|responder_info| (selected_responder.clone(), responder_info));

                if let Some((responder, responder_client_info)) = selected_handler {
                    // Send query request to responder
                    if let Err(err) = responder_client_info
                        .tx()
                        .send(ServerMessage::QueryRequested(msg.clone().into()))
                        .await
                    {
                        error!(%err, responder=%responder, "Can't send QueryRequestMessage to responder");
                        // Notify requester that we failed to reach responder
                        if let Some(requester_client_info) = requester_client_info_option {
                            let _ = requester_client_info
                                .tx()
                                .send(ServerMessage::query_handler_not_found(msg))
                                .await;
                        }
                        return;
                    }

                    // Record the ongoing query request
                    let ongoing_query_request = OngoingQueryRequest::from(responder, msg);
                    self.query_handlers
                        .handle_query_requested_event(ongoing_query_request.clone())
                        .await;
                    info!(client_id = %msg.requester().to_string(), responder_client_id = ongoing_query_request.responder().to_string(),
                        request_id = %msg.request_id(), query_id = %msg.query_id(),
                        "Query requested"
                    );
                } else {
                    // Responder was selected but is no longer connected
                    error!(
                        request_id = %msg.request_id(),
                        responder = %selected_responder,
                        "Selected responder not found in connected clients"
                    );
                    // TODO: start the process with a new client, maybe
                }
            }
            Err(err) => match err {
                QueryRequestHandlerError::QueryHandlerNotFound => {
                    // No handler registered for this query ID
                    if let Some(requester_client_info) = requester_client_info_option {
                        let _ = requester_client_info
                            .tx()
                            .send(ServerMessage::query_handler_not_found(msg))
                            .await;
                    } else {
                        warn!(
                            requester = %msg.requester(),
                            request_id = %msg.request_id(),
                            "Requester not found when sending QueryHandlerNotFound"
                        );
                    }
                    info!(
                        request_id = %msg.request_id(),
                        query_id = %msg.query_id(),
                        "Query handler not found"
                    );
                }
                QueryRequestHandlerError::ResponderNotFound => {
                    error!(
                        request_id = %msg.request_id(), requester=%msg.requester(),
                        "Query responder not found"
                    );
                }
            },
        }
    }

    async fn handle_query_response_message(&self, msg: &QueryResponseMessage) {
        let Some(ongoing_query_request) = self
            .query_handlers
            .get_ongoing_request(msg.request_id())
            .await
        else {
            warn!(
                request_id = %msg.request_id(),
                "Query response received but request id not found"
            );
            return;
        };

        let requester = ongoing_query_request.requester();
        let Some(requester_client_info) = self.get_client_by_id(requester).await else {
            warn!(
                request_id = %msg.request_id(), requester=%requester,
                "Query response received but requester not found"
            );
            return;
        };

        if let Err(err) = requester_client_info
            .tx()
            .send(ServerMessage::QueryResponded(QueryRespondedMessage::new(
                msg.request_id().clone(),
                msg.result(),
            )))
            .await
        {
            error!(%err, request_id=%msg.request_id(), requester=%requester, "Can't send response for QueryResponseMessage to requester");
            return;
        }

        self.query_handlers
            .remove_ongoing_request(msg.request_id())
            .await;
        info!(responder_client_id = msg.responder().to_string(),
            request_id = %msg.request_id(),
            "Query responded"
        );
    }

    async fn handle_register_source_event_message(&self, msg: &RegisterSourceEventMessage) {
        // TODO(manuelarte): check if event exists, and if exists, check whether is the same
        // or not, if it's not, then send back an error.
        let new_source_event: NewSourceEvent = msg.into();
        let Ok(source_event) = self.repository.save_source_event(new_source_event).await else {
            error!(client_id = %msg.source(), event_id=%msg.event_id(), "Failed to save source event");
            // TODO: inform the source
            return;
        };
        // Inform the client that the event could be saved, not broadcasted yet, but saved.
        let Some(source_info) = self.get_client_by_id(msg.source()).await else {
            error!(client_id = %msg.source(), event_id=%msg.event_id(), "Failed to find the source info to inform that the event was saved");
            return;
        };
        let _ = source_info
            .tx()
            .send(ServerMessage::SourceEventRegistered {
                event_id: source_event.id().clone(),
                monotonic_clock: source_event.monotonic_clock(),
            })
            .await;
        // Trigger an asynchronous job to send the new source_event to every client
        let state_clone = self.clone();
        tokio::spawn(async move {
            let connected_clients = state_clone.connected_clients.read().await;
            for (client_id, client_info) in connected_clients.iter() {
                let tx = client_info.tx();
                let msg = ServerMessage::SourceEvent(source_event.clone());
                if let Err(err) = tx.send(msg).await {
                    warn!(client_id = %client_id, event_id = %source_event.id(), err = %err, "Failed to send source event to client");
                }
            }
        });
    }

    async fn handle_query_timed_out(&self, request_id: &RequestId) {
        let removed = self.query_handlers.remove_ongoing_request(request_id).await;
        if let Some(removed) = removed {
            info!(
                request_id = %removed.request_id(),
                query_id = %removed.query_id(),
                "Query timed out"
            );
        }
    }

    /// `handle_disconnected_client_message` removes a client.
    /// Removes it from connected clients and also removes itself from the query handlers.
    async fn handle_disconnected_client_message(&self, client_id: &ClientId) {
        self.connected_clients.write().await.remove(client_id);
        self.query_handlers.disconnect_client(client_id).await;
        info!(client_id = %client_id, "Client disconnected");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::domain::messages::server::QueryRequestedErrorReason;
    use crate::domain::query_handlers::{QueryHandler, QueryRequestHandlerError};
    use crate::domain::source_events::{
        NewSourceEvent, SourceEvent, SourceEventRepository, SourceEventRepositoryError,
    };
    use crate::domain::tx::TxError;
    use crate::domain::{ClientSelectionStrategy, RoundRobinStrategy};
    use std::sync::Mutex;

    use chrono::Utc;
    use tonic::async_trait;
    use uuid::Uuid;

    #[derive(Clone)]
    struct NoopSourceEventRepository;

    #[async_trait]
    impl SourceEventRepository for NoopSourceEventRepository {
        async fn save_source_event(
            &self,
            _new_event: NewSourceEvent,
        ) -> Result<SourceEvent, SourceEventRepositoryError> {
            Ok(SourceEvent::new(
                Uuid::new_v4().into(),
                Utc::now(),
                1,
                "test".try_into().expect("Failed to create AggregateId"),
                "TestAggregate".to_string(),
                Vec::new(),
            ))
        }
    }

    struct InspectableRoundRobinStrategy {
        inner: Arc<Mutex<RoundRobinStrategy>>,
    }

    #[derive(Debug)]
    struct NoopTx;

    #[derive(Debug)]
    struct InspectableTx {
        sent_messages: Arc<Mutex<Vec<ServerMessage>>>,
    }

    #[async_trait]
    impl Tx for NoopTx {
        async fn send(&self, _msg: ServerMessage) -> Result<(), TxError> {
            Ok(())
        }
    }

    #[async_trait]
    impl Tx for InspectableTx {
        async fn send(&self, msg: ServerMessage) -> Result<(), TxError> {
            self.sent_messages
                .lock()
                .expect("inspectable tx mutex should not be poisoned")
                .push(msg);
            Ok(())
        }
    }

    fn test_state(
        client_selection_strategy: Box<dyn ClientSelectionStrategy>,
    ) -> State<NoopSourceEventRepository> {
        let repository = NoopSourceEventRepository;
        State {
            connected_clients: Arc::new(RwLock::new(HashMap::default())),
            query_handlers: QueryHandler::new()
                .with_client_selection_strategy(client_selection_strategy),
            repository,
        }
    }

    fn round_robin_strategy() -> Box<dyn ClientSelectionStrategy> {
        Box::new(RoundRobinStrategy::new())
    }

    impl ClientSelectionStrategy for InspectableRoundRobinStrategy {
        fn next(&mut self, candidates: &[ClientId]) -> Option<ClientId> {
            self.inner
                .lock()
                .expect("round robin mutex should not be poisoned")
                .next(candidates)
        }
    }

    fn inspectable_round_robin_strategy() -> (
        Box<dyn ClientSelectionStrategy>,
        Arc<Mutex<RoundRobinStrategy>>,
    ) {
        let inner = Arc::new(Mutex::new(RoundRobinStrategy::new()));
        (
            Box::new(InspectableRoundRobinStrategy {
                inner: Arc::clone(&inner),
            }),
            inner,
        )
    }

    #[tokio::test]
    async fn get_client_can_handle_clients_dont_change() {
        let (strategy, _) = inspectable_round_robin_strategy();
        let state = test_state(strategy);
        let tx: Arc<dyn Tx> = Arc::new(NoopTx);
        let query_id = "query.users.list".try_into().expect("valid query id");

        state
            .handle_register_client_message(&RegisterClientMessage::new(
                "client-1".try_into().expect("valid client id"),
                std::slice::from_ref(&query_id),
                tx.clone(),
            ))
            .await;

        state
            .handle_register_client_message(&RegisterClientMessage::new(
                "client-2".try_into().expect("valid client id"),
                std::slice::from_ref(&query_id),
                tx,
            ))
            .await;

        let before = state
            .query_handlers
            .get_client_ids_for_query_id(&query_id)
            .await;
        assert_eq!(
            before,
            vec![
                "client-1".try_into().expect("valid client id"),
                "client-2".try_into().expect("valid client id")
            ]
        );

        let selected = state
            .query_handlers
            .get_client_id_for_query_id(&query_id)
            .await;
        assert!(selected.is_ok(), "a handler should be selected");

        let after = state
            .query_handlers
            .get_client_ids_for_query_id(&query_id)
            .await;
        assert_eq!(
            after,
            vec![
                "client-1".try_into().expect("valid client id"),
                "client-2".try_into().expect("valid client id")
            ]
        );

        let selected_second = state
            .query_handlers
            .get_client_id_for_query_id(&query_id)
            .await;
        assert!(
            selected_second.is_ok(),
            "a second handler should be selected"
        );
    }

    #[tokio::test]
    async fn get_client_can_handle_updates_round_robin_last_selected() {
        let (strategy, inspector) = inspectable_round_robin_strategy();
        let state = test_state(strategy);
        let tx: Arc<dyn Tx> = Arc::new(NoopTx);
        let query_id = "query.users.list".try_into().expect("valid query id");

        state
            .handle_register_client_message(&RegisterClientMessage::new(
                "client-1".try_into().expect("valid client id"),
                std::slice::from_ref(&query_id),
                tx.clone(),
            ))
            .await;

        state
            .handle_register_client_message(&RegisterClientMessage::new(
                "client-2".try_into().expect("valid client id"),
                std::slice::from_ref(&query_id),
                tx,
            ))
            .await;

        let selected = state
            .query_handlers
            .get_client_id_for_query_id(&query_id)
            .await;
        assert!(selected.is_ok(), "a handler should be selected");
        assert_eq!(
            inspector
                .lock()
                .expect("round robin mutex should not be poisoned")
                .last_selected,
            Some("client-1".try_into().expect("valid client id"))
        );

        let selected_second = state
            .query_handlers
            .get_client_id_for_query_id(&query_id)
            .await;
        assert!(
            selected_second.is_ok(),
            "a second handler should be selected"
        );
        assert_eq!(
            inspector
                .lock()
                .expect("round robin mutex should not be poisoned")
                .last_selected,
            Some("client-2".try_into().expect("valid client id"))
        );
    }

    #[tokio::test]
    async fn get_client_can_handle_single_handler_keeps_same_position() {
        let (strategy, inspector) = inspectable_round_robin_strategy();
        let state = test_state(strategy);
        let tx: Arc<dyn Tx> = Arc::new(NoopTx);
        let query_id = "query.users.detail".try_into().expect("valid query id");

        state
            .handle_register_client_message(&RegisterClientMessage::new(
                "client-1".try_into().expect("valid client id"),
                std::slice::from_ref(&query_id),
                tx,
            ))
            .await;

        let selected = state
            .query_handlers
            .get_client_id_for_query_id(&query_id)
            .await;
        assert!(selected.is_ok(), "single handler should be selected");
        assert_eq!(
            inspector
                .lock()
                .expect("round robin mutex should not be poisoned")
                .last_selected,
            Some("client-1".try_into().expect("valid client id"))
        );

        let selected_second = state
            .query_handlers
            .get_client_id_for_query_id(&query_id)
            .await;
        assert!(
            selected_second.is_ok(),
            "a second handler should be selected"
        );
        assert_eq!(
            inspector
                .lock()
                .expect("round robin mutex should not be poisoned")
                .last_selected,
            Some("client-1".try_into().expect("valid client id"))
        );
    }

    #[tokio::test]
    async fn get_client_can_handle_returns_error_when_no_handlers_registered_for_query_id() {
        let state = test_state(round_robin_strategy());
        let tx: Arc<dyn Tx> = Arc::new(NoopTx);
        let query_id: QueryId = "query.unknown".try_into().expect("valid query id");

        state
            .handle_register_client_message(&RegisterClientMessage::new(
                "client-1".try_into().expect("valid client id"),
                std::slice::from_ref(&"another".try_into().expect("valid query id")),
                tx,
            ))
            .await;

        let result = state
            .query_handlers
            .get_client_id_for_query_id(&query_id)
            .await;

        assert!(matches!(
            result,
            Err(QueryRequestHandlerError::QueryHandlerNotFound)
        ));
    }

    #[tokio::test]
    async fn get_client_can_handle_returns_error_when_no_handlers_registered() {
        let state = test_state(round_robin_strategy());
        let query_id: QueryId = "query.unknown".try_into().expect("valid query id");

        let result = state
            .query_handlers
            .get_client_id_for_query_id(&query_id)
            .await;

        assert!(matches!(
            result,
            Err(QueryRequestHandlerError::QueryHandlerNotFound)
        ));
    }

    #[tokio::test]
    async fn handle_message_query_request_without_handlers_sends_query_rejected_to_requester() {
        let state = test_state(round_robin_strategy());
        let sent_messages = Arc::new(Mutex::new(Vec::new()));
        let tx: Arc<dyn Tx> = Arc::new(InspectableTx {
            sent_messages: sent_messages.clone(),
        });
        let client_id = ClientId::new("client-1").expect("valid client id");

        state
            .handle_register_client_message(&RegisterClientMessage::new(
                client_id.clone(),
                std::slice::from_ref(&"query.users.list".try_into().expect("valid query id")),
                tx,
            ))
            .await;

        let request_id: RequestId = Uuid::new_v4().into();
        let query_id: QueryId = "query.unknown".try_into().expect("valid query id");
        state
            .handle_message(&ClientMessage::QueryRequest(QueryRequestMessage::new(
                request_id.clone(),
                client_id,
                query_id.clone(),
                HashMap::new(),
            )))
            .await;

        let sent_messages = sent_messages
            .lock()
            .expect("inspectable tx mutex should not be poisoned");
        assert_eq!(sent_messages.len(), 2);

        let ServerMessage::QueryRequestedError {
            request_id: msg_request_id,
            query_id: msg_query_id,
            reason: QueryRequestedErrorReason::QueryHandlerNotFound,
        } = &sent_messages[1]
        else {
            panic!("expected QueryRequestedError message to be sent to requester")
        };
        assert_eq!(msg_request_id.clone(), request_id);
        assert_eq!(msg_query_id.clone(), query_id);
        drop(sent_messages);
    }

    #[tokio::test]
    async fn get_connected_clients_returns_first_page_sorted_by_client_id() {
        let state = test_state(round_robin_strategy());
        let tx: Arc<dyn Tx> = Arc::new(NoopTx);

        state
            .handle_register_client_message(&RegisterClientMessage::new(
                "client-3".try_into().expect("valid client id"),
                &[],
                tx.clone(),
            ))
            .await;
        state
            .handle_register_client_message(&RegisterClientMessage::new(
                "client-1".try_into().expect("valid client id"),
                &[],
                tx.clone(),
            ))
            .await;
        state
            .handle_register_client_message(&RegisterClientMessage::new(
                "client-2".try_into().expect("valid client id"),
                &[],
                tx,
            ))
            .await;

        let page = state.get_connected_clients(PageRequest::new(0, 2)).await;

        assert_eq!(page.total_count(), 3);
        assert_eq!(page.items().len(), 2);

        let ids: Vec<ClientId> = page
            .items()
            .iter()
            .flat_map(|item| item.keys().cloned())
            .collect();
        assert_eq!(
            ids,
            vec![
                "client-1".try_into().expect("valid client id"),
                "client-2".try_into().expect("valid client id")
            ]
        );
    }

    #[tokio::test]
    async fn get_connected_clients_returns_second_page() {
        let state = test_state(round_robin_strategy());
        let tx: Arc<dyn Tx> = Arc::new(NoopTx);

        state
            .handle_register_client_message(&RegisterClientMessage::new(
                "client-1".try_into().expect("valid client id"),
                &[],
                tx.clone(),
            ))
            .await;
        state
            .handle_register_client_message(&RegisterClientMessage::new(
                "client-2".try_into().expect("valid client id"),
                &[],
                tx.clone(),
            ))
            .await;
        state
            .handle_register_client_message(&RegisterClientMessage::new(
                "client-3".try_into().expect("valid client id"),
                &[],
                tx,
            ))
            .await;

        let page = state.get_connected_clients(PageRequest::new(2, 2)).await;

        assert_eq!(page.total_count(), 3);
        assert_eq!(page.items().len(), 1);

        let ids: Vec<ClientId> = page
            .items()
            .iter()
            .flat_map(|item| item.keys().cloned())
            .collect();
        assert_eq!(ids, vec!["client-3".try_into().expect("valid client id")]);
    }

    #[tokio::test]
    async fn get_connected_clients_with_zero_offset_returns_empty_page() {
        let state = test_state(round_robin_strategy());
        let tx: Arc<dyn Tx> = Arc::new(NoopTx);

        state
            .handle_register_client_message(&RegisterClientMessage::new(
                "client-1".try_into().expect("valid client id"),
                &[],
                tx,
            ))
            .await;

        let page = state.get_connected_clients(PageRequest::new(0, 0)).await;

        assert_eq!(page.total_count(), 1);
        assert!(page.items().is_empty());
    }
}
