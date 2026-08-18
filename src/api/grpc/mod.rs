use crate::api::grpc::control::protocontrol::{
    ClientRegistered, ClientRegistrationError, Heartbeat, QueryRequestedError,
    ServerToClientMessage, SourceEvent, SourceEventRegistered,
};
use crate::domain::messages::server::ServerMessage;
use crate::domain::source_events::SourceEvent as DomainSourceEvent;
use crate::domain::tx::{Tx, TxError};
use prost_types::value::Kind;
use prost_types::{ListValue, NullValue, Struct, Value};
use serde_json::Value as JsonValue;
use tokio::sync::mpsc::Sender;
use tonic::Status;
use tonic::async_trait;

pub(super) mod control;
mod transform;

#[derive(Debug)]
pub struct GrpcTx {
    tx: Sender<Result<ServerToClientMessage, Status>>,
}

impl GrpcTx {
    pub(crate) const fn new(tx: Sender<Result<ServerToClientMessage, Status>>) -> Self {
        Self { tx }
    }
}

#[async_trait]
impl Tx for GrpcTx {
    async fn send(&self, msg: ServerMessage) -> Result<(), TxError> {
        let response = match msg {
            ServerMessage::ClientRegistered { client_id } => {
                control::protocontrol::server_to_client_message::Payload::ClientRegistered(
                    ClientRegistered {
                        client_id: client_id.to_string(),
                    },
                )
            }
            ServerMessage::ClientRegistrationError { client_id, reason } => {
                control::protocontrol::server_to_client_message::Payload::ClientRegistrationError(
                    ClientRegistrationError {
                        client_id: client_id.as_str().to_string(),
                        reason: reason.as_str().to_string(),
                    },
                )
            }
            ServerMessage::QueryRequested(msg) => {
                control::protocontrol::server_to_client_message::Payload::QueryRequested(msg.into())
            }
            ServerMessage::QueryResponded(msg) => {
                control::protocontrol::server_to_client_message::Payload::QueryResponded(msg.into())
            }
            ServerMessage::QueryRequestedError {
                request_id,
                query_id,
                reason,
            } => control::protocontrol::server_to_client_message::Payload::QueryRequestedError(
                QueryRequestedError {
                    request_id: request_id.to_string(),
                    query_id: query_id.to_string(),
                    reason: reason.as_str().to_string(),
                    details: reason.details().unwrap_or_default(),
                },
            ),
            ServerMessage::SourceEventRegistered {
                event_id,
                monotonic_clock,
            } => control::protocontrol::server_to_client_message::Payload::SourceEventRegistered(
                SourceEventRegistered {
                    event_id: event_id.to_string(),
                    monotonic_clock,
                },
            ),
            ServerMessage::SourceEvent(source_event) => {
                control::protocontrol::server_to_client_message::Payload::SourceEvent(
                    source_event.into(),
                )
            }
            ServerMessage::Heartbeat => {
                control::protocontrol::server_to_client_message::Payload::Heartbeat(Heartbeat {})
            }
        };
        let resp = ServerToClientMessage {
            payload: Some(response),
        };
        self.tx
            .send(Ok(resp))
            .await
            .map_err(|e| TxError::SendError(e.to_string()))
    }
}

impl From<DomainSourceEvent> for SourceEvent {
    fn from(event: DomainSourceEvent) -> Self {
        let payload: JsonValue = serde_json::from_slice(event.payload()).unwrap_or(JsonValue::Null);
        Self {
            id: event.id().to_string(),
            created_at: Some(prost_types::Timestamp {
                seconds: event.created_at().timestamp(),
                nanos: event.created_at().timestamp_subsec_nanos().cast_signed(),
            }),
            monotonic_clock: event.monotonic_clock(),
            aggregate_type: event.aggregate_type().to_string(),
            aggregate_id: event.aggregate_id().as_str().to_string(),
            payload: Some(json_to_prost_struct(payload)),
        }
    }
}

fn json_to_prost_struct(json: JsonValue) -> Struct {
    if let JsonValue::Object(map) = json {
        Struct {
            fields: map
                .into_iter()
                .map(|(k, v)| (k, json_to_prost_value(v)))
                .collect(),
        }
    } else {
        Struct::default()
    }
}

fn json_to_prost_value(json: JsonValue) -> Value {
    let kind = match json {
        JsonValue::Null => Kind::NullValue(NullValue::NullValue as i32),
        JsonValue::Bool(b) => Kind::BoolValue(b),
        JsonValue::Number(n) => Kind::NumberValue(n.as_f64().unwrap_or(0.0)),
        JsonValue::String(s) => Kind::StringValue(s),
        JsonValue::Array(v) => Kind::ListValue(ListValue {
            values: v.into_iter().map(json_to_prost_value).collect(),
        }),
        JsonValue::Object(m) => Kind::StructValue(json_to_prost_struct(JsonValue::Object(m))),
    };
    Value { kind: Some(kind) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::source_events::{AggregateId, EventId};
    use chrono::Utc;
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn test_json_to_prost_struct() {
        let json = json!({
            "string": "value",
            "number": 123.45,
            "boolean": true,
            "null": null,
            "object": {
                "nested": "item"
            },
            "array": [1, "two", false]
        });

        let prost_struct = json_to_prost_struct(json);

        let fields = &prost_struct.fields;
        assert_eq!(
            fields.get("string").unwrap().kind,
            Some(Kind::StringValue("value".to_string()))
        );
        assert_eq!(
            fields.get("number").unwrap().kind,
            Some(Kind::NumberValue(123.45))
        );
        assert_eq!(
            fields.get("boolean").unwrap().kind,
            Some(Kind::BoolValue(true))
        );
        assert_eq!(
            fields.get("null").unwrap().kind,
            Some(Kind::NullValue(NullValue::NullValue as i32))
        );

        if let Some(Kind::StructValue(s)) = &fields.get("object").unwrap().kind {
            assert_eq!(
                s.fields.get("nested").unwrap().kind,
                Some(Kind::StringValue("item".to_string()))
            );
        } else {
            panic!("Expected StructValue for 'object'");
        }

        if let Some(Kind::ListValue(l)) = &fields.get("array").unwrap().kind {
            assert_eq!(l.values.len(), 3);
            assert_eq!(l.values[0].kind, Some(Kind::NumberValue(1.0)));
            assert_eq!(l.values[1].kind, Some(Kind::StringValue("two".to_string())));
            assert_eq!(l.values[2].kind, Some(Kind::BoolValue(false)));
        } else {
            panic!("Expected ListValue for 'array'");
        }
    }

    #[test]
    fn test_json_to_prost_struct_empty() {
        let json = json!({});
        let prost_struct = json_to_prost_struct(json);
        assert!(prost_struct.fields.is_empty());
    }

    #[test]
    fn test_json_to_prost_struct_not_object() {
        let json = json!(["not", "an", "object"]);
        let prost_struct = json_to_prost_struct(json);
        assert!(prost_struct.fields.is_empty());
    }

    #[test]
    fn test_from_domain_source_event() {
        let event_id = EventId::from(Uuid::new_v4());
        let aggregate_id = AggregateId::new("test-aggregate").unwrap();
        let payload = json!({"foo": "bar"}).to_string().into_bytes();

        let domain_event = DomainSourceEvent::new(
            event_id.clone(),
            Utc::now(),
            100,
            aggregate_id,
            "test-type".to_string(),
            payload,
        );

        let grpc_event: SourceEvent = domain_event.into();

        assert_eq!(grpc_event.id, event_id.to_string());
        assert_eq!(grpc_event.monotonic_clock, 100);
        assert_eq!(grpc_event.aggregate_id, "test-aggregate");
        assert_eq!(grpc_event.aggregate_type, "test-type");

        let payload_struct = grpc_event.payload.unwrap();
        assert_eq!(
            payload_struct.fields.get("foo").unwrap().kind,
            Some(Kind::StringValue("bar".to_string()))
        );
    }

    #[test]
    fn test_from_domain_source_event_invalid_payload() {
        let event_id = EventId::from(Uuid::new_v4());
        let aggregate_id = AggregateId::new("test-aggregate").unwrap();
        let payload = b"not a json".to_vec();

        let domain_event = DomainSourceEvent::new(
            event_id,
            Utc::now(),
            100,
            aggregate_id,
            "test-type".to_string(),
            payload,
        );

        let grpc_event: SourceEvent = domain_event.into();
        assert!(grpc_event.payload.unwrap().fields.is_empty());
    }
}
