//! Client Source Events contains the struct to handle events sent by the clients.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::fmt;
use std::fmt::Formatter;
use std::str::FromStr;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Ord, PartialOrd, Eq, Hash)]
pub struct EventId(Uuid);

impl From<Uuid> for EventId {
    fn from(val: Uuid) -> Self {
        Self(val)
    }
}

impl FromStr for EventId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<Uuid>().map(EventId)
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// `AggregateId` type to represent the id of an aggregate.
#[derive(Debug, Clone, PartialEq, Ord, PartialOrd, Eq, Hash)]
pub struct AggregateId(String);

#[derive(Debug)]
pub enum InvalidAggregateId {
    Empty,
    TooShort { len: usize, min: usize },
    TooLong { len: usize, max: usize },
}

impl AggregateId {
    const MAX: usize = 255;
    const MIN: usize = 3;
    pub fn new(aggregate_id: &str) -> Result<Self, InvalidAggregateId> {
        if aggregate_id.trim().is_empty() {
            return Err(InvalidAggregateId::Empty);
        }
        if aggregate_id.chars().count() > Self::MAX {
            return Err(InvalidAggregateId::TooLong {
                len: aggregate_id.chars().count(),
                max: Self::MAX,
            });
        }
        if aggregate_id.chars().count() < Self::MIN {
            return Err(InvalidAggregateId::TooShort {
                len: aggregate_id.chars().count(),
                min: Self::MIN,
            });
        }
        Ok(Self(aggregate_id.to_string()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for AggregateId {
    type Error = InvalidAggregateId;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(&value)
    }
}

impl TryFrom<&str> for AggregateId {
    type Error = InvalidAggregateId;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.to_string().try_into()
    }
}

/// `EventPayload` type to represent the serialized event.
pub type EventPayload = Vec<u8>;

#[derive(Clone, Debug)]
pub struct SourceEvent {
    /// `id` unique identifier of the event.
    id: EventId,
    /// `created_at` time when the event was stored in the server.
    created_at: DateTime<Utc>,
    /// `monotonic_clock` represents the monotonic clock of the event.
    monotonic_clock: u64,
    /// `aggregate_id` unique identifier of the aggregate instance.
    aggregate_id: AggregateId,
    /// `aggregate_type` unique identifier of the aggregate type.
    aggregate_type: String,
    /// `payload` the serialized event.
    payload: EventPayload,
}

impl SourceEvent {
    #[must_use]
    pub(crate) const fn new(
        id: EventId,
        created_at: DateTime<Utc>,
        monotonic_clock: u64,
        aggregate_id: AggregateId,
        aggregate_type: String,
        payload: EventPayload,
    ) -> Self {
        Self {
            id,
            created_at,
            monotonic_clock,
            aggregate_id,
            aggregate_type,
            payload,
        }
    }

    #[must_use]
    pub const fn id(&self) -> &EventId {
        &self.id
    }

    #[must_use]
    pub const fn created_at(&self) -> &DateTime<Utc> {
        &self.created_at
    }

    #[must_use]
    pub const fn monotonic_clock(&self) -> u64 {
        self.monotonic_clock
    }

    #[must_use]
    pub const fn aggregate_id(&self) -> &AggregateId {
        &self.aggregate_id
    }

    #[must_use]
    pub fn aggregate_type(&self) -> &str {
        &self.aggregate_type
    }

    #[must_use]
    pub const fn payload(&self) -> &EventPayload {
        &self.payload
    }
}

pub struct NewSourceEvent {
    /// `event_id` unique identifier of the event.
    event_id: EventId,
    /// `aggregate_id` unique identifier of the aggregate instance.
    aggregate_id: AggregateId,
    /// `aggregate_type` unique identifier of the aggregate type.
    aggregate_type: String,
    /// `payload` the serialized event.
    payload: EventPayload,
}

impl NewSourceEvent {
    pub fn new(
        event_id: EventId,
        aggregate_id: AggregateId,
        aggregate_type: impl Into<String>,
        payload: impl Into<EventPayload>,
    ) -> Self {
        Self {
            event_id,
            aggregate_id,
            aggregate_type: aggregate_type.into(),
            payload: payload.into(),
        }
    }

    #[must_use]
    pub const fn event_id(&self) -> &EventId {
        &self.event_id
    }

    #[must_use]
    pub const fn aggregate_id(&self) -> &AggregateId {
        &self.aggregate_id
    }

    #[must_use]
    pub fn aggregate_type(&self) -> &str {
        &self.aggregate_type
    }

    #[must_use]
    pub const fn payload(&self) -> &EventPayload {
        &self.payload
    }
}

#[derive(Debug)]
pub struct SourceEventRepositoryError {
    message: String,
}

impl SourceEventRepositoryError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

#[async_trait]
pub trait SourceEventRepository: Clone + Send + Sync {
    /// `save` save the new client source event.
    async fn save_source_event(
        &self,
        new_event: NewSourceEvent,
    ) -> Result<SourceEvent, SourceEventRepositoryError>;
}
