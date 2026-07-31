use crate::domain::messages::client::RegisterClientMessage;
use crate::domain::tx::Tx;
use std::sync::Arc;
use std::time::{Instant, SystemTime};

#[derive(Clone, Debug)]
pub struct ClientInfo {
    tx: Arc<dyn Tx>,
    /// `since` is the time when the client was registered.
    since: SystemTime,
    /// `last_seen` is the time when the client last sent a message.
    last_seen: Instant,
}

impl From<&RegisterClientMessage> for ClientInfo {
    fn from(val: &RegisterClientMessage) -> Self {
        Self {
            tx: val.tx(),
            since: SystemTime::now(),
            last_seen: Instant::now(),
        }
    }
}

impl ClientInfo {
    #[must_use]
    pub(crate) fn tx(&self) -> Arc<dyn Tx> {
        self.tx.clone()
    }

    #[must_use]
    pub(crate) const fn since(&self) -> &SystemTime {
        &self.since
    }
    #[must_use]
    pub(crate) const fn last_seen(&self) -> &Instant {
        &self.last_seen
    }

    pub(crate) const fn update_last_seen(&mut self, last_seen: Instant) {
        self.last_seen = last_seen;
    }
}
