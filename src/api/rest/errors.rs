use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

/// A custom error response that can be returned from handlers
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    /// HTTP status code
    code: u16,
    /// Error message
    message: String,
    /// errors list of errors
    errors: Vec<ErrorDetail>,
}

impl ErrorResponse {
    pub fn new(code: StatusCode, message: impl Into<String>, errors: &[ErrorDetail]) -> Self {
        Self {
            code: code.as_u16(),
            message: message.into(),
            errors: errors.to_vec(),
        }
    }

    /// Validation error (400 Bad Request)
    pub fn not_found(message: impl Into<String>, errors: &[ErrorDetail]) -> Self {
        Self::new(StatusCode::NOT_FOUND, message, errors)
    }

    /// Validation error (400 Bad Request)
    pub fn validation_error(message: impl Into<String>, errors: &[ErrorDetail]) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message, errors)
    }
}

impl IntoResponse for ErrorResponse {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let body = Json(self);

        (status, body).into_response()
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ErrorDetail {
    reason: String,
    message: String,
    location_type: String,
}

impl ErrorDetail {
    pub fn new(
        reason: impl Into<String>,
        message: impl Into<String>,
        location_type: impl Into<String>,
    ) -> Self {
        Self {
            reason: reason.into(),
            message: message.into(),
            location_type: location_type.into(),
        }
    }
}
