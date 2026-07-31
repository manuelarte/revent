use crate::domain::messages::client::QueryRequestMessage;
use crate::domain::messages::{QueryId, RequestId};
use crate::domain::{ClientId, ClientSelectionStrategy, RoundRobinStrategy};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct QueryHandler {
    /// `query_handlers` maps query types to the list of clients that can handle them.
    query_handlers: Arc<RwLock<HashMap<QueryId, Vec<ClientId>>>>,
    /// `ongoing_query_requests` contains the ongoing query requests.
    ongoing_query_requests: Arc<RwLock<HashMap<RequestId, OngoingQueryRequest>>>,
    /// `client_selection_strategy` client selection strategy
    client_selection_strategy: Arc<RwLock<Box<dyn ClientSelectionStrategy>>>,
}

pub enum QueryRequestHandlerError {
    /// `QueryHandlerNotFound` no client can handle this `QueryId`.
    QueryHandlerNotFound,
    /// `ClientIdNotFound` In theory impossible error.
    ResponderNotFound,
}

impl QueryHandler {
    pub(crate) fn new() -> Self {
        Self {
            query_handlers: Arc::new(RwLock::new(HashMap::default())),
            ongoing_query_requests: Arc::new(RwLock::new(HashMap::default())),
            client_selection_strategy: Arc::new(RwLock::new(Box::new(RoundRobinStrategy::new()))),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_client_selection_strategy(
        mut self,
        client_selection_strategy: Box<dyn ClientSelectionStrategy>,
    ) -> Self {
        self.client_selection_strategy = Arc::new(RwLock::new(client_selection_strategy));
        self
    }

    pub(crate) async fn add_client_query_handler(&self, client_id: ClientId, queries: &[QueryId]) {
        let mut handlers_lock = self.query_handlers.write().await;
        for query_id in &queries.to_vec() {
            handlers_lock
                .entry(query_id.clone())
                .or_default()
                .push(client_id.clone());
        }
    }

    pub(crate) async fn disconnect_client(&self, client_id: &ClientId) {
        let mut handlers_lock = self.query_handlers.write().await;
        handlers_lock.retain(|_, clients| {
            clients.retain(|current| current != client_id);
            !clients.is_empty()
        });
    }

    /// `get_client_id_for_query_id` get the `client_id` that can respond to the `query_id`.
    pub(crate) async fn get_client_id_for_query_id(
        &self,
        query_id: &QueryId,
    ) -> Result<ClientId, QueryRequestHandlerError> {
        let mut query_handlers = self.query_handlers.write().await;
        let Some(active_handlers) = query_handlers
            .get_mut(query_id)
            .filter(|handlers| !handlers.is_empty())
        else {
            return Err(QueryRequestHandlerError::QueryHandlerNotFound);
        };

        self.client_selection_strategy
            .write()
            .await
            .next(active_handlers)
            .ok_or(QueryRequestHandlerError::ResponderNotFound)
    }

    pub(crate) async fn get_query_handlers_for_client_id(
        &self,
        client_id: &ClientId,
    ) -> Vec<QueryId> {
        let query_handlers_map = self.query_handlers.read().await;
        query_handlers_map
            .iter()
            .filter(|(_query_id, clients)| clients.contains(client_id))
            .map(|(query_id, _clients)| query_id.clone())
            .collect()
    }
    pub(crate) async fn get_ongoing_request(
        &self,
        request_id: &RequestId,
    ) -> Option<OngoingQueryRequest> {
        self.ongoing_query_requests
            .read()
            .await
            .get(request_id)
            .cloned()
    }

    pub(crate) async fn get_expired_requests(
        &self,
        query_response_timeout: Duration,
    ) -> Vec<OngoingQueryRequest> {
        let lock = self.ongoing_query_requests.read().await;
        lock.iter()
            .filter(|(_request_id, ongoing_query_request)| {
                ongoing_query_request.created_at().elapsed() > query_response_timeout
            })
            .map(|(_request_id, ongoing_query_request)| ongoing_query_request.clone())
            .collect()
    }

    pub(crate) async fn handle_query_requested_event(
        &self,
        ongoing_query_request: OngoingQueryRequest,
    ) {
        self.ongoing_query_requests.write().await.insert(
            ongoing_query_request.clone().request_id,
            ongoing_query_request,
        );
    }

    pub(crate) async fn remove_ongoing_request(
        &self,
        request_id: &RequestId,
    ) -> Option<OngoingQueryRequest> {
        self.ongoing_query_requests.write().await.remove(request_id)
    }

    #[cfg(test)]
    pub(crate) async fn get_client_ids_for_query_id(&self, query_id: &QueryId) -> Vec<ClientId> {
        let query_handlers_map = self.query_handlers.read().await;
        query_handlers_map
            .get(query_id)
            .cloned()
            .unwrap_or_default()
    }
}

#[derive(Clone, Debug)]
pub struct OngoingQueryRequest {
    /// `request_id` unique identifier of the request.
    request_id: RequestId,
    /// `query_id` unique identifier of the query.
    query_id: QueryId,
    /// `parameters` query parameters, if applicable.
    parameters: HashMap<String, String>,
    /// `requester` the origin of the query request.
    requester: ClientId,
    /// `responder` the client id target of the query request.
    responder: ClientId,
    /// `created_at` the instant the query request was created.
    created_at: Instant,
}

impl OngoingQueryRequest {
    pub(crate) fn from(responder: impl Into<ClientId>, msg: &QueryRequestMessage) -> Self {
        Self {
            request_id: msg.request_id().clone(),
            query_id: msg.query_id().clone(),
            parameters: msg.parameters(),
            requester: msg.requester().clone(),
            responder: responder.into(),
            created_at: Instant::now(),
        }
    }

    /// `is_same` check that the `QueryRequestMessage` represents the same query request
    /// already being processed
    pub(crate) fn is_same(&self, msg: &QueryRequestMessage) -> bool {
        self.request_id == msg.request_id().clone()
            && self.query_id == msg.query_id().clone()
            && self.requester == msg.requester().clone()
    }

    pub(crate) const fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    pub(crate) const fn query_id(&self) -> &QueryId {
        &self.query_id
    }

    pub(crate) fn parameters(&self) -> HashMap<String, String> {
        self.parameters.clone()
    }

    pub(crate) const fn requester(&self) -> &ClientId {
        &self.requester
    }

    pub(crate) const fn responder(&self) -> &ClientId {
        &self.responder
    }

    pub(crate) const fn created_at(&self) -> &Instant {
        &self.created_at
    }
}
