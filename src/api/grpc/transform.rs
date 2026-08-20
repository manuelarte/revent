use crate::api::grpc::GrpcTx;
use crate::api::grpc::control::protocontrol::client_to_server_message::Payload;
use crate::api::grpc::control::protocontrol::{
    QueryRequest as QueryRequestProto, QueryRequested, QueryResponded, RegisterSourceEvent,
    ServerToClientMessage,
};
use crate::domain::messages::client::ClientMessage::{
    Heartbeat, QueryHandlingError, QueryRequest, QueryResponse, RegisterClient,
};
use crate::domain::messages::client::{
    ClientMessage, QueryHandlingErrorMessage, QueryHandlingErrorReason, QueryRequestMessage,
    QueryResponseMessage, RegisterClientMessage, RegisterSourceEventMessage,
};
use crate::domain::messages::server::{QueryRequestedMessage, QueryRespondedMessage};
use crate::domain::messages::{InvalidQueryId, QueryId, RequestId};
use crate::domain::source_events::{AggregateId, EventId, InvalidAggregateId};
use crate::domain::tx::Tx;
use crate::domain::{ClientId, ClientIdNotValid};
use std::fmt::Display;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tonic::Status;

#[derive(Debug)]
pub(super) enum ClientMessageError {
    ClientNotRegistered,
    ValidationError { field: String, reason: String },
}

impl From<ClientIdNotValid> for ClientMessageError {
    fn from(value: ClientIdNotValid) -> Self {
        Self::ValidationError {
            field: "client_id".to_string(),
            reason: value.0,
        }
    }
}

impl From<InvalidAggregateId> for ClientMessageError {
    fn from(value: InvalidAggregateId) -> Self {
        let reason = match value {
            InvalidAggregateId::Empty => "value is empty".to_string(),
            InvalidAggregateId::TooShort { min, .. } => format!("value is too short (min={min})"),
            InvalidAggregateId::TooLong { max, .. } => format!("value is too long (max={max})"),
        };
        Self::ValidationError {
            field: "aggregate_id".to_string(),
            reason,
        }
    }
}

impl From<InvalidQueryId> for ClientMessageError {
    fn from(value: InvalidQueryId) -> Self {
        let reason = match value {
            InvalidQueryId::Empty => "value is empty".to_string(),
            InvalidQueryId::TooShort { min, .. } => format!("value is too short (min={min})"),
            InvalidQueryId::TooLong { max, .. } => format!("value is too long (max={max})"),
        };
        Self::ValidationError {
            field: "query_id".to_string(),
            reason,
        }
    }
}

impl Display for ClientMessageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ClientNotRegistered => write!(f, "Client not registered"),
            Self::ValidationError { field, reason } => {
                write!(f, "Validation error for field {field}: {reason}")
            }
        }
    }
}

impl ClientMessage {
    /// `from` converts a gRPC `Request` into a domain `ClientMessage`.
    /// It also takes a `Sender` to send responses back to the gRPC client.
    ///
    /// # Panics
    ///
    /// It panics if the `request_id` is not a UUID.
    pub(super) fn from(
        req: Payload,
        client_id_option: Option<ClientId>,
        tx: Sender<Result<ServerToClientMessage, Status>>,
    ) -> Result<Self, ClientMessageError> {
        let grpc_tx = GrpcTx::new(tx);
        let client_tx: Arc<dyn Tx> = Arc::new(grpc_tx);
        match req {
            Payload::RegisterClient(r) => {
                let query_handlers: Vec<QueryId> = r
                    .query_handlers
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<QueryId>, _>>()?;
                let client_id =
                    ClientId::new(r.client_id.as_str()).map_err(ClientMessageError::from)?;
                Ok(RegisterClient(RegisterClientMessage::new(
                    client_id,
                    &query_handlers,
                    client_tx,
                )))
            }
            Payload::QueryRequest(qr) => {
                let Some(client_id) = client_id_option else {
                    return Err(ClientMessageError::ClientNotRegistered);
                };
                match QueryRequestMessage::from(qr, client_id) {
                    Ok(msg) => Ok(QueryRequest(msg)),
                    Err(err) => Err(err),
                }
            }
            Payload::QueryResponse(qr) => {
                let Some(client_id) = client_id_option else {
                    return Err(ClientMessageError::ClientNotRegistered);
                };
                Ok(QueryResponse(QueryResponseMessage::new(
                    qr.request_id
                        .parse::<RequestId>()
                        .expect("request id should be an uuid"),
                    client_id,
                    qr.result.as_slice(),
                )))
            }
            Payload::QueryHandlingError(msg) => {
                let Some(client_id) = client_id_option else {
                    return Err(ClientMessageError::ClientNotRegistered);
                };
                // Convert prost's raw i32 enum value to the generated enum type.
                let proto_reason =
                    crate::api::grpc::control::protocontrol::QueryHandlingErrorReason::try_from(
                        msg.reason,
                    )
                    .unwrap_or(
                        crate::api::grpc::control::protocontrol::QueryHandlingErrorReason::Unknown,
                    );
                let reason = match proto_reason {
                    crate::api::grpc::control::protocontrol::QueryHandlingErrorReason::Unknown => {
                        QueryHandlingErrorReason::Unknown {
                            details: if msg.details.is_empty() {
                                None
                            } else {
                                Some(msg.details)
                            },
                        }
                    }
                    crate::api::grpc::control::protocontrol::QueryHandlingErrorReason::ErrorHandling => {
                        QueryHandlingErrorReason::ErrorHandling
                    }
                };
                Ok(QueryHandlingError(QueryHandlingErrorMessage::new(
                    msg.request_id
                        .parse::<RequestId>()
                        .expect("request id should be an uuid"),
                    client_id,
                    reason,
                )))
            }
            Payload::RegisterSourceEvent(msg) => {
                let Some(client_id) = client_id_option else {
                    return Err(ClientMessageError::ClientNotRegistered);
                };
                match RegisterSourceEventMessage::from(client_id, msg) {
                    Ok(client_message) => Ok(Self::RegisterSourceEvent(client_message)),
                    Err(err) => Err(err),
                }
            }
            Payload::Heartbeat(_) => {
                let Some(client_id) = client_id_option else {
                    return Err(ClientMessageError::ClientNotRegistered);
                };
                Ok(Heartbeat { client_id })
            }
        }
    }
}

impl QueryRequestMessage {
    fn from(req: QueryRequestProto, client_id: ClientId) -> Result<Self, ClientMessageError> {
        let query_id: QueryId = req.query_id.try_into().map_err(ClientMessageError::from)?;
        Ok(Self::new(
            req.request_id.parse::<RequestId>().unwrap(),
            client_id,
            query_id,
            req.parameters,
        ))
    }
}

impl From<QueryRequestedMessage> for QueryRequested {
    fn from(val: QueryRequestedMessage) -> Self {
        Self {
            request_id: val.request_id().to_string(),
            query_id: val.query_id().clone().into(),
            parameters: val.parameters().clone(),
        }
    }
}

impl From<QueryRespondedMessage> for QueryResponded {
    fn from(val: QueryRespondedMessage) -> Self {
        Self {
            request_id: val.request_id().to_string(),
            result: val.result().to_vec(),
        }
    }
}

impl RegisterSourceEventMessage {
    fn from(client_id: ClientId, val: RegisterSourceEvent) -> Result<Self, ClientMessageError> {
        let aggregate_id: AggregateId = val
            .aggregate_id
            .try_into()
            .map_err(ClientMessageError::from)?;
        Ok(Self::new(
            client_id,
            val.event_id.parse::<EventId>().unwrap(),
            aggregate_id,
            val.aggregate_type,
            val.payload.as_slice(),
        ))
    }
}
