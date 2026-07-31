use crate::domain::messages::server::ServerMessage;
use std::fmt;
use std::fmt::Debug;
use tonic::async_trait;

#[derive(Debug)]
pub enum TxError {
    SendError(String),
}

impl fmt::Display for TxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SendError(msg) => write!(f, "send error: {msg}"),
        }
    }
}

impl std::error::Error for TxError {}

/// `Tx` trait that allows to send a message to a node
#[async_trait]
pub trait Tx: Send + Sync + Debug {
    async fn send(&self, msg: ServerMessage) -> Result<(), TxError>;
}
