use crate::api::AppState;
use crate::api::rest::errors::{ErrorDetail, ErrorResponse};
use crate::domain::Repository;
use axum::Json;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Query, State};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use uuid::Uuid;

pub async fn get_source_events<R: Repository + 'static>(
    cursor_pagination_result: Result<Query<CursorPagination>, QueryRejection>,
    State(_app_state): State<AppState<R>>,
) -> Result<Json<CursorPage<SourceEventDto>>, ErrorResponse> {
    const MAX_LIMIT: usize = 50;

    let cursor_pagination = match cursor_pagination_result {
        Ok(cursor_pagination) => cursor_pagination,
        Err(err) => {
            return Err(ErrorResponse::validation_error(
                "Invalid parameters",
                &[ErrorDetail::new(
                    "invalidParameters".to_string(),
                    err.to_string(),
                    "query".to_string(),
                )],
            ));
        }
    };

    let mut errors: Vec<ErrorDetail> = vec![];
    if cursor_pagination.limit == 0 || cursor_pagination.limit > MAX_LIMIT {
        errors.push(ErrorDetail::new(
            "invalidParameters".to_string(),
            format!("Failed to deserialize query string: limit must be between 1 and {MAX_LIMIT}"),
            "query".to_string(),
        ));
    }

    if !errors.is_empty() {
        return Err(ErrorResponse::validation_error(
            "Invalid parameters",
            errors.as_slice(),
        ));
    }

    Ok(Json(CursorPage::<SourceEventDto> {
        content: vec![SourceEventDto {
            id: Uuid::default(),
            created_at: DateTime::default(),
            monotonic_clock: 1,
            aggregate_type: "my_type".to_string(),
            aggregate_id: "my_id".to_string(),
            payload: r#"{"name": "John", "surname": "Doe"}"#.to_string(),
        }],
        cursor: cursor_pagination.cursor,
        limit: cursor_pagination.limit,
        size: 0,
        total_elements: 0,
        next: None,
        prev: None,
    }))
}

#[derive(Deserialize)]
pub struct CursorPagination {
    #[serde(default = "default_cursor")]
    cursor: u64,
    #[serde(default = "default_limit")]
    limit: usize,
}

#[derive(serde::Serialize)]
pub struct SourceEventDto {
    id: Uuid,
    created_at: DateTime<Utc>,
    monotonic_clock: u64,
    aggregate_type: String,
    aggregate_id: String,
    payload: String,
}

#[derive(serde::Serialize)]
pub struct CursorPage<T> {
    content: Vec<T>,
    cursor: u64,
    limit: usize,
    size: usize,
    total_elements: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    next: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prev: Option<String>,
}

const fn default_cursor() -> u64 {
    0
}

const fn default_limit() -> usize {
    20
}
