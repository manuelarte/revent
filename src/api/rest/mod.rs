use serde::Deserialize;

pub(super) mod actuator_handler;
pub(super) mod clients_handler;
pub(super) mod errors;
pub(super) mod openapi_handler;
pub(super) mod source_events_handler;

#[derive(Deserialize)]
pub struct Pagination {
    #[serde(default = "default_offset")]
    offset: usize,
    #[serde(default = "default_limit")]
    limit: usize,
}

const fn default_offset() -> usize {
    0
}

const fn default_limit() -> usize {
    20
}

#[derive(serde::Serialize)]
pub struct Page<T: Clone> {
    content: Vec<T>,
    offset: usize,
    limit: usize,
    size: usize,
    total_elements: usize,
    total_pages: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    next: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prev: Option<String>,
}

impl<T: Clone> Page<T> {
    pub(crate) fn new(
        content: &[T],
        offset: usize,
        limit: usize,
        total_elements: usize,
        total_pages: usize,
    ) -> Self {
        Self {
            content: content.to_vec(),
            offset,
            limit,
            size: content.len(),
            total_elements,
            total_pages,
            next: None,
            prev: None,
        }
    }

    pub(crate) fn with_pagination_links(mut self, base_path: &str) -> Self {
        // Generate prev link if not at the first page
        if self.offset > 0 {
            let prev_offset = self.offset.saturating_sub(self.limit);
            self.prev = Some(format!(
                "{}?offset={}&limit={}",
                base_path, prev_offset, self.limit
            ));
        }

        // Generate next link if there are more pages
        let next_offset = self.offset + self.limit;
        if next_offset < self.total_elements {
            self.next = Some(format!(
                "{}?offset={}&limit={}",
                base_path, next_offset, self.limit
            ));
        }

        self
    }
}
