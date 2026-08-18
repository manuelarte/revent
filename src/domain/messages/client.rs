use crate::domain::ClientId;
use crate::domain::messages::server::QueryRequestedMessage;
use crate::domain::messages::{QueryId, RequestId};
use crate::domain::source_events::{AggregateId, EventId, EventPayload, NewSourceEvent};
use crate::domain::tx::Tx;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::sync::Arc;

/// `ClientMessage` that can be received from the `DomainNode`s.
#[derive(Debug, Clone)]
pub enum ClientMessage {
    /// `RegisterClient` is sent when want to register a client.
    RegisterClient(RegisterClientMessage),
    /// `QueryRequest` is sent when a client wants to query.
    QueryRequest(QueryRequestMessage),
    /// `QueryResponse` is sent when a client replies to a query request.
    QueryResponse(QueryResponseMessage),
    /// `QueryHandlingError` is sent when a client (responder) wants to indicate that there was an error
    /// processing the query request.
    QueryHandlingError(QueryHandlingErrorMessage),
    /// `PublishSourceEvent` is sent when an event happened.
    RegisterSourceEvent(RegisterSourceEventMessage),
    /// `Heartbeat` sent to check that the client is connected.
    Heartbeat { client_id: ClientId },
    /// `DisconnectClient` message of the client being disconnected
    DisconnectClient { client_id: ClientId },
}

impl ClientMessage {
    pub(crate) const fn get_source(&self) -> Option<&ClientId> {
        match self {
            Self::RegisterClient(_) => None,
            Self::QueryRequest(msg) => Some(msg.requester()),
            Self::QueryResponse(msg) => Some(msg.responder()),
            Self::RegisterSourceEvent(msg) => Some(msg.source()),
            Self::Heartbeat { client_id } | Self::DisconnectClient { client_id } => Some(client_id),
            Self::QueryHandlingError(msg) => Some(msg.responder()),
        }
    }
}

#[derive(Clone)]
pub struct RegisterClientMessage {
    client_id: ClientId,
    query_handlers: Vec<QueryId>,
    tx: Arc<dyn Tx>,
}

impl RegisterClientMessage {
    pub(crate) fn new(client_id: ClientId, query_handlers: &[QueryId], tx: Arc<dyn Tx>) -> Self {
        Self {
            client_id,
            query_handlers: query_handlers.to_vec(),
            tx,
        }
    }

    #[must_use]
    pub(crate) const fn client_id(&self) -> &ClientId {
        &self.client_id
    }

    #[must_use]
    pub(crate) const fn query_handlers(&self) -> &[QueryId] {
        self.query_handlers.as_slice()
    }

    #[must_use]
    pub(crate) fn tx(&self) -> Arc<dyn Tx> {
        self.tx.clone()
    }
}

impl fmt::Debug for RegisterClientMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "RegisterClientMessage {{ client_id: {} }}",
            self.client_id
        )
    }
}

#[derive(Debug, Clone)]
pub struct QueryRequestMessage {
    /// `request_id` unique identifier of the query request process.
    request_id: RequestId,
    /// `requester` client id that initiated the query request.
    requester: ClientId,
    /// `query_id` unique identifier of the query that needs to be called.
    query_id: QueryId,
    /// `parameters` query parameters, if applicable.
    parameters: HashMap<String, String>,
}

impl QueryRequestMessage {
    #[must_use]
    pub(crate) fn new(
        request_id: impl Into<RequestId>,
        requester: impl Into<ClientId>,
        query_id: impl Into<QueryId>,
        parameters: impl Into<HashMap<String, String>>,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            requester: requester.into(),
            query_id: query_id.into(),
            parameters: parameters.into(),
        }
    }

    #[must_use]
    pub(crate) const fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    #[must_use]
    pub(crate) const fn requester(&self) -> &ClientId {
        &self.requester
    }

    #[must_use]
    pub(crate) const fn query_id(&self) -> &QueryId {
        &self.query_id
    }

    #[must_use]
    pub(crate) fn parameters(&self) -> HashMap<String, String> {
        self.parameters.clone()
    }
}

impl From<QueryRequestMessage> for QueryRequestedMessage {
    fn from(msg: QueryRequestMessage) -> Self {
        Self::new(
            msg.request_id().clone(),
            msg.query_id().clone(),
            msg.parameters(),
        )
    }
}

#[derive(Debug, Clone)]
pub struct QueryResponseMessage {
    /// `request_id` unique identifier of the query request process.
    request_id: RequestId,
    /// `responder` the client that responded to the query request.
    responder: ClientId,
    /// `result` query result.
    result: Vec<u8>,
}

impl QueryResponseMessage {
    #[must_use]
    pub(crate) fn new(
        request_id: impl Into<RequestId>,
        responder: impl Into<ClientId>,
        result: &[u8],
    ) -> Self {
        Self {
            request_id: request_id.into(),
            responder: responder.into(),
            result: result.to_vec(),
        }
    }

    #[must_use]
    pub(crate) const fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    #[must_use]
    pub(crate) const fn responder(&self) -> &ClientId {
        &self.responder
    }

    #[must_use]
    pub(crate) const fn result(&self) -> &[u8] {
        self.result.as_slice()
    }
}

#[derive(Debug, Clone)]
pub struct QueryHandlingErrorMessage {
    request_id: RequestId,
    responder: ClientId,
    reason: QueryHandlingErrorReason,
}

impl QueryHandlingErrorMessage {
    #[must_use]
    pub(crate) fn new(
        request_id: impl Into<RequestId>,
        responder: impl Into<ClientId>,
        reason: QueryHandlingErrorReason,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            responder: responder.into(),
            reason,
        }
    }

    #[must_use]
    pub(crate) const fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    #[must_use]
    pub(crate) const fn responder(&self) -> &ClientId {
        &self.responder
    }

    #[must_use]
    pub(crate) const fn reason(&self) -> &QueryHandlingErrorReason {
        &self.reason
    }
}

#[derive(Debug, Clone)]
pub struct RegisterSourceEventMessage {
    /// `source` client id that published the event.
    source: ClientId,
    /// `event_id` unique identifier of the query request process.
    event_id: EventId,
    /// `aggregate_id` unique identifier of the aggregate.
    aggregate_id: AggregateId,
    /// `aggregate_type`
    aggregate_type: String,
    /// `payload`
    payload: EventPayload,
}

impl RegisterSourceEventMessage {
    pub(crate) fn new(
        source: ClientId,
        event_id: impl Into<EventId>,
        aggregate_id: impl Into<AggregateId>,
        aggregate_type: String,
        payload: &[u8],
    ) -> Self {
        Self {
            source,
            event_id: event_id.into(),
            aggregate_id: aggregate_id.into(),
            aggregate_type,
            payload: payload.to_vec(),
        }
    }

    #[must_use]
    pub(crate) const fn source(&self) -> &ClientId {
        &self.source
    }

    #[must_use]
    pub(crate) const fn event_id(&self) -> &EventId {
        &self.event_id
    }

    #[must_use]
    pub(crate) const fn aggregate_id(&self) -> &AggregateId {
        &self.aggregate_id
    }

    #[must_use]
    pub(crate) const fn aggregate_type(&self) -> &str {
        self.aggregate_type.as_str()
    }

    #[must_use]
    pub(crate) const fn payload(&self) -> &EventPayload {
        &self.payload
    }
}

impl From<&RegisterSourceEventMessage> for NewSourceEvent {
    fn from(val: &RegisterSourceEventMessage) -> Self {
        Self::new(
            val.event_id().clone(),
            val.aggregate_id().clone(),
            val.aggregate_type().to_string(),
            val.payload().clone(),
        )
    }
}

#[derive(Debug, Clone)]
pub enum QueryHandlingErrorReason {
    // ErrorHandling is an error that occurred while the client was handling the query request.
    ErrorHandling,
    // Unknown is an error that occurred while the client was handling the query request, but the reason is unknown.
    Unknown { details: Option<String> },
}

impl Display for QueryHandlingErrorReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ErrorHandling => write!(f, "Error handling the query request"),
            Self::Unknown { details } => {
                if let Some(details) = details {
                    write!(f, "Unknown error handling the query request: {details}")
                } else {
                    write!(f, "Unknown error handling the query request")
                }
            }
        }
    }
}
