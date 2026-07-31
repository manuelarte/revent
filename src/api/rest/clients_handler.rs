use crate::api::AppState;
use crate::api::rest::errors::{ErrorDetail, ErrorResponse};
use crate::api::rest::{Page, Pagination};
use crate::domain::{ClientId, PageRequest};
use crate::domain::{ClientIdNotValid, Repository};
use axum::Json;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, State};
use chrono::{DateTime, Utc};
use serde::Serializer;
use std::time::Duration;

pub async fn get_clients_page<R: Repository + 'static>(
    pagination_result: Result<Query<Pagination>, QueryRejection>,
    State(app_state): State<AppState<R>>,
) -> Result<Json<Page<ClientInPageDto>>, ErrorResponse> {
    const MAX_OFFSET: usize = 100_000;
    const MAX_LIMIT: usize = 100;

    let pagination = match pagination_result {
        Ok(pagination) => pagination,
        Err(err) => {
            let errors = vec![ErrorDetail::new(
                "invalidParameters".to_string(),
                err.to_string(),
                "query".to_string(),
            )];
            return Err(ErrorResponse::validation_error(
                "Invalid parameters",
                errors.as_slice(),
            ));
        }
    };

    let mut errors: Vec<ErrorDetail> = vec![];
    if pagination.offset > MAX_OFFSET {
        errors.push(ErrorDetail::new(
            "invalidParameters".to_string(),
            format!("Failed to deserialize query string: offset must be <= {MAX_OFFSET}"),
            "query".to_string(),
        ));
    }

    if pagination.limit == 0 || pagination.limit > MAX_LIMIT {
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

    let clients_page = app_state
        .state
        .get_connected_clients(PageRequest::new(pagination.offset, pagination.limit))
        .await;

    let clients: Vec<ClientInPageDto> = clients_page
        .items()
        .iter()
        .flat_map(|item| item.iter())
        .map(|(id, info)| ClientInPageDto {
            id: id.as_str().to_string(),
            since: (*info.since()).into(),
            link: format!("http://localhost:10001/api/clients/{id}"),
        })
        .collect();

    let total_elements = clients_page.total_count();

    let total_pages = if total_elements == 0 {
        0
    } else {
        total_elements.div_ceil(pagination.limit)
    };

    Ok(Json(
        Page::new(
            &clients,
            pagination.offset,
            pagination.limit,
            total_elements,
            total_pages,
        )
        .with_pagination_links("http://localhost:10001/api/clients"),
    ))
}

pub async fn get_client_by_id<R: Repository + 'static>(
    Path(client_id): Path<String>,
    State(app_state): State<AppState<R>>,
) -> Result<Json<ClientDto>, ErrorResponse> {
    let client_id_result: Result<ClientId, ClientIdNotValid> = client_id.clone().try_into();
    if let Err(err) = client_id_result {
        return Err(ErrorResponse::validation_error(
            "Invalid client id",
            &[ErrorDetail::new(
                "invalid parameter".to_string(),
                err.0,
                "path".to_string(),
            )],
        ));
    }
    let client_id = client_id_result.unwrap();
    let client = app_state
        .state
        .get_client_by_id(&client_id)
        .await
        .ok_or_else(|| {
            ErrorResponse::not_found(
                format!("Client with id {client_id} not found"),
                &[ErrorDetail::new(
                    "notFound".to_string(),
                    format!("Client with id {client_id} not found"),
                    "path".to_string(),
                )],
            )
        })?;

    let query_handlers = app_state
        .state
        .get_query_handlers_for_client_id(&client_id)
        .await;

    Ok(Json(ClientDto {
        id: client_id.to_string(),
        since: (*client.since()).into(),
        last_seen: client.last_seen().elapsed(),
        query_handlers: query_handlers.into_iter().map(Into::into).collect(),
    }))
}

#[derive(Clone, serde::Serialize)]
pub struct ClientInPageDto {
    id: String,
    since: DateTime<Utc>,
    link: String,
}

#[derive(Clone, serde::Serialize)]
pub struct ClientDto {
    id: String,
    since: DateTime<Utc>,
    #[serde(serialize_with = "serialize_duration_as_seconds_f64")]
    last_seen: Duration,
    query_handlers: Vec<String>,
}

fn serialize_duration_as_seconds_f64<S>(dur: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let seconds = dur.as_secs_f64();
    serializer.serialize_f64(seconds)
}

#[cfg(test)]
mod tests {
    use crate::api::rest::Page;

    #[test]
    fn test_pagination_link_generation() {
        let page: Page<String> =
            Page::new(&[], 0, 20, 100, 5).with_pagination_links("/api/clients");

        // First page, should have next but no prev
        assert!(page.next.is_some(), "first page should have next link");
        assert!(page.prev.is_none(), "first page should not have prev link");
        assert_eq!(page.next.unwrap(), "/api/clients?offset=20&limit=20");
    }

    #[test]
    fn test_pagination_link_middle_page() {
        let page: Page<String> =
            Page::new(&[], 20, 20, 100, 5).with_pagination_links("/api/clients");

        // Middle page, should have both prev and next
        assert!(page.next.is_some(), "middle page should have next link");
        assert!(page.prev.is_some(), "middle page should have prev link");
        assert_eq!(page.prev.unwrap(), "/api/clients?offset=0&limit=20");
        assert_eq!(page.next.unwrap(), "/api/clients?offset=40&limit=20");
    }

    #[test]
    fn test_pagination_link_last_page() {
        let page: Page<String> =
            Page::new(&[], 80, 20, 100, 5).with_pagination_links("/api/clients");

        // Last page, should have prev but no next
        assert!(page.next.is_none(), "last page should not have next link");
        assert!(page.prev.is_some(), "last page should have prev link");
        assert_eq!(page.prev.unwrap(), "/api/clients?offset=60&limit=20");
    }
}
