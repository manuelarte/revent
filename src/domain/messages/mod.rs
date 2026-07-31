//! Messages that can be sent from the client to the server and from the server to the client.

use std::fmt;
use std::fmt::Formatter;
use std::str::FromStr;
use uuid::Uuid;

/// Client messages
pub mod client;
/// Server messages
pub mod server;

#[derive(Debug, Clone, PartialEq, Ord, PartialOrd, Eq, Hash)]
pub struct QueryId(String);

#[derive(Debug)]
pub enum InvalidQueryId {
    Empty,
    TooShort { len: usize, min: usize },
    TooLong { len: usize, max: usize },
}

impl QueryId {
    const MAX: usize = 255;
    const MIN: usize = 3;

    pub fn new(query_id: &str) -> Result<Self, InvalidQueryId> {
        if query_id.trim().is_empty() {
            return Err(InvalidQueryId::Empty);
        }
        if query_id.chars().count() > Self::MAX {
            return Err(InvalidQueryId::TooLong {
                len: query_id.chars().count(),
                max: Self::MAX,
            });
        }
        if query_id.chars().count() < Self::MIN {
            return Err(InvalidQueryId::TooShort {
                len: query_id.chars().count(),
                min: Self::MIN,
            });
        }
        Ok(Self(query_id.to_string()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for QueryId {
    type Error = InvalidQueryId;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value.as_str())
    }
}

impl TryFrom<&str> for QueryId {
    type Error = InvalidQueryId;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.to_string().try_into()
    }
}

impl From<QueryId> for String {
    fn from(val: QueryId) -> Self {
        val.0
    }
}

impl fmt::Display for QueryId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Ord, PartialOrd, Eq, Hash)]
pub struct RequestId(Uuid);

impl From<Uuid> for RequestId {
    fn from(val: Uuid) -> Self {
        Self(val)
    }
}

impl FromStr for RequestId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<Uuid>().map(RequestId)
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
