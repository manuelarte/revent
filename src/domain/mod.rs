pub(crate) mod client;
pub(crate) mod messages;
mod query_handlers;
pub(crate) mod source_events;
pub mod state;
pub(crate) mod tx;

pub use crate::domain::source_events::SourceEventRepository;

/// `Repository` is a trait that implements `SourceEventRepository`.
pub trait Repository: SourceEventRepository {}

impl<T: SourceEventRepository> Repository for T {}

#[derive(Debug, Clone, PartialEq, Ord, PartialOrd, Eq, Hash)]
pub struct ClientId(String);

#[derive(Debug)]
pub struct ClientIdNotValid(pub String);

impl ClientId {
    /// `new` creates a new `ClientId` from a `&str`.
    ///
    /// # Errors
    ///
    /// Returns a `ClientIdNotValid` error if the client id is not valid.
    pub fn new(client_id: &str) -> Result<Self, ClientIdNotValid> {
        if client_id.is_empty() {
            return Err(ClientIdNotValid("ClientId is empty".to_string()));
        }
        if client_id.chars().count() > 64 {
            return Err(ClientIdNotValid("ClientId is too long".to_string()));
        }
        if client_id.chars().count() < 3 {
            return Err(ClientIdNotValid("ClientId is too short".to_string()));
        }
        Ok(Self(client_id.to_string()))
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ClientId {
    type Error = ClientIdNotValid;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(&value)
    }
}

impl TryFrom<&str> for ClientId {
    type Error = ClientIdNotValid;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl std::fmt::Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// `ClientSelectionStrategy` strategy to select a new client to handle a request.
pub(crate) trait ClientSelectionStrategy: Send + Sync {
    fn next(&mut self, candidates: &[ClientId]) -> Option<ClientId>;
}

pub(crate) struct RoundRobinStrategy {
    last_selected: Option<ClientId>,
}

impl RoundRobinStrategy {
    pub(crate) const fn new() -> Self {
        Self {
            last_selected: None,
        }
    }
}

impl ClientSelectionStrategy for RoundRobinStrategy {
    fn next(&mut self, candidates: &[ClientId]) -> Option<ClientId> {
        if candidates.is_empty() {
            self.last_selected = None;
            return None;
        }

        // sort candidates
        let mut sorted_candidates = candidates.to_vec();
        sorted_candidates.sort();

        let next = self
            .last_selected
            .as_ref()
            .and_then(|last| sorted_candidates.iter().position(|id| id == last))
            .map_or_else(
                || sorted_candidates[0].clone(),
                |pos| sorted_candidates[(pos + 1) % sorted_candidates.len()].clone(),
            );

        self.last_selected = Some(next.clone());
        Some(next)
    }
}

pub struct PageRequest {
    page: usize,
    offset: usize,
}

impl PageRequest {
    #[must_use]
    pub const fn new(page: usize, offset: usize) -> Self {
        Self { page, offset }
    }

    #[must_use]
    pub const fn page(&self) -> usize {
        self.page
    }

    #[must_use]
    pub const fn offset(&self) -> usize {
        self.offset
    }
}

pub struct PageResponse<T> {
    items: Vec<T>,
    total_count: usize,
}

impl<T> PageResponse<T> {
    #[must_use]
    pub const fn new(items: Vec<T>, total_count: usize) -> Self {
        Self { items, total_count }
    }

    #[must_use]
    pub fn items(&self) -> &[T] {
        &self.items
    }

    #[must_use]
    pub const fn total_count(&self) -> usize {
        self.total_count
    }
}

#[cfg(test)]
mod tests {
    use super::{ClientId, ClientSelectionStrategy, RoundRobinStrategy};

    fn ids(values: &[&str]) -> Vec<ClientId> {
        values.iter().filter_map(|v| (*v).try_into().ok()).collect()
    }

    #[test]
    fn round_robin_rotates_through_all_candidates() {
        let mut strategy = RoundRobinStrategy::new();
        let candidates = ids(&["client-1", "client-2", "client-3"]);

        assert_eq!(
            strategy.next(&candidates),
            Some("client-1".try_into().expect("valid client id"))
        );
        assert_eq!(
            strategy.next(&candidates),
            Some("client-2".try_into().expect("valid client id"))
        );
        assert_eq!(
            strategy.next(&candidates),
            Some("client-3".try_into().expect("valid client id"))
        );
        assert_eq!(
            strategy.next(&candidates),
            Some("client-1".try_into().expect("valid client id"))
        );
    }

    #[test]
    fn round_robin_recovers_when_last_selected_is_removed() {
        let mut strategy = RoundRobinStrategy::new();

        let initial = ids(&["client-1", "client-2", "client-3"]);
        assert_eq!(
            strategy.next(&initial),
            Some("client-1".try_into().expect("valid client id"))
        );

        let without_first = ids(&["client-2", "client-3"]);
        assert_eq!(
            strategy.next(&without_first),
            Some("client-2".try_into().expect("valid client id"))
        );
        assert_eq!(
            strategy.next(&without_first),
            Some("client-3".try_into().expect("valid client id"))
        );
    }

    #[test]
    fn round_robin_returns_none_for_empty_candidates() {
        let mut strategy = RoundRobinStrategy::new();
        let empty: Vec<ClientId> = vec![];

        assert_eq!(strategy.next(&empty), None);
    }

    #[test]
    fn round_robin_resets_after_empty_candidate_set() {
        let mut strategy = RoundRobinStrategy::new();
        let candidates = ids(&["client-1", "client-2"]);
        let empty: Vec<ClientId> = vec![];

        assert_eq!(
            strategy.next(&candidates),
            Some("client-1".try_into().expect("valid client id"))
        );
        assert_eq!(strategy.next(&empty), None);
        assert_eq!(
            strategy.next(&candidates),
            Some("client-1".try_into().expect("valid client id"))
        );
    }
}
