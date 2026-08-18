use crate::domain::ClientId;
use crate::domain::messages::client::{QueryHandlingErrorReason, QueryRequestMessage};
use crate::domain::messages::{QueryId, RequestId};
use crate::domain::source_events::{EventId, SourceEvent};
use std::collections::HashMap;

/// `ServerMessage` that can be sent to the client.
#[derive(Debug, Clone)]
pub enum ServerMessage {
    /// `ClientRegistered` is sent when a client is registered.
    ClientRegistered { client_id: ClientId },
    /// `ClientRegistrationError` is sent to the client to indicate that the client registration failed.
    ClientRegistrationError {
        client_id: ClientId,
        reason: ClientRegistrationErrorReason,
    },
    /// `Heartbeat` is sent to check if the client is connected.
    Heartbeat,
    /// `QueryRequested` is sent when a client did a query request.
    QueryRequested(QueryRequestedMessage),
    /// `QueryRequestedError` is sent to the client (requester) to indicate that there was an error
    /// processing the query request.
    QueryRequestedError {
        request_id: RequestId,
        query_id: QueryId,
        reason: QueryRequestedErrorReason,
    },
    /// `QueryResponded` is sent when a client responded to a query request.
    QueryResponded(QueryRespondedMessage),
    /// `SourceEventRegistered` is sent to the source that created an event to indicate that it was saved.
    SourceEventRegistered {
        event_id: EventId,
        monotonic_clock: u64,
    },
    /// `SourceEvent` is sent to all clients to notify them of a new event.
    SourceEvent(SourceEvent),
}

#[derive(Debug, Clone)]
pub enum ClientRegistrationErrorReason {
    ClientIdDuplicated,
}

impl ClientRegistrationErrorReason {
    pub(crate) const fn as_str(&self) -> &str {
        match self {
            Self::ClientIdDuplicated => "ClientIdDuplicated",
        }
    }
}

#[derive(Debug, Clone)]
pub enum QueryRequestedErrorReason {
    RequestIdDuplicated,
    QueryHandlerNotFound,
    QueryTimedOut,
    HandlingError(QueryHandlingErrorReason)
}

impl QueryRequestedErrorReason {
    pub(crate) const fn as_str(&self) -> &str {
        match self {
            Self::RequestIdDuplicated => "RequestIdDuplicated",
            Self::QueryHandlerNotFound => "QueryHandlerNotFound",
            Self::QueryTimedOut => "QueryTimedOut",
            Self::HandlingError(reason) => match reason {
                QueryHandlingErrorReason::ErrorHandling => "ErrorHandling",
                QueryHandlingErrorReason::Unknown {details: _ } => "Unknown",
            },
        }
    }

    pub(crate) fn details(&self) -> Option<String> {
        match self {
            Self::RequestIdDuplicated => None,
            Self::QueryHandlerNotFound => None,
            Self::QueryTimedOut => None,
            Self::HandlingError(reason) => match reason {
                QueryHandlingErrorReason::ErrorHandling => None,
                QueryHandlingErrorReason::Unknown { details } => details.clone(),
            },
        }
    }
}

impl ServerMessage {
    pub(in crate::domain) const fn client_id_duplicated(client_id: ClientId) -> Self {
        Self::ClientRegistrationError {
            client_id,
            reason: ClientRegistrationErrorReason::ClientIdDuplicated,
        }
    }

    pub(in crate::domain) fn query_request_id_duplicated(msg: &QueryRequestMessage) -> Self {
        Self::QueryRequestedError {
            request_id: msg.request_id().clone(),
            query_id: msg.query_id().clone(),
            reason: QueryRequestedErrorReason::RequestIdDuplicated,
        }
    }

    pub(in crate::domain) fn query_handler_not_found(msg: &QueryRequestMessage) -> Self {
        Self::QueryRequestedError {
            request_id: msg.request_id().clone(),
            query_id: msg.query_id().clone(),
            reason: QueryRequestedErrorReason::QueryHandlerNotFound,
        }
    }

    pub(in crate::domain) const fn query_timed_out(
        request_id: RequestId,
        query_id: QueryId,
    ) -> Self {
        Self::QueryRequestedError {
            request_id,
            query_id,
            reason: QueryRequestedErrorReason::QueryTimedOut,
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueryRequestedMessage {
    /// `request_id` unique identifier of the query request process.
    request_id: RequestId,
    /// `query_id` unique identifier of the query that needs to be called.
    query_id: QueryId,
    /// `parameters` query parameters, if applicable
    parameters: HashMap<String, String>,
}

impl QueryRequestedMessage {
    pub(crate) fn new(
        request_id: RequestId,
        query_id: QueryId,
        parameters: impl Into<HashMap<String, String>>,
    ) -> Self {
        Self {
            request_id,
            query_id,
            parameters: parameters.into(),
        }
    }

    #[must_use]
    pub(crate) const fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    #[must_use]
    pub(crate) const fn query_id(&self) -> &QueryId {
        &self.query_id
    }

    #[must_use]
    pub(crate) const fn parameters(&self) -> &HashMap<String, String> {
        &self.parameters
    }
}

#[derive(Debug, Clone)]
pub struct QueryRespondedMessage {
    /// `request_id` unique identifier of the query request process.
    request_id: RequestId,
    /// `result` query result.
    result: Vec<u8>,
}

impl QueryRespondedMessage {
    #[must_use]
    pub(crate) fn new(request_id: RequestId, result: &[u8]) -> Self {
        Self {
            request_id,
            result: result.to_vec(),
        }
    }

    #[must_use]
    pub(crate) const fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    #[must_use]
    pub(crate) const fn result(&self) -> &[u8] {
        self.result.as_slice()
    }
}
