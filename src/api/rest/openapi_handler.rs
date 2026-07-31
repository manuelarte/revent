use axum::extract::Path;
use axum::http::header;
use axum::response::{IntoResponse, Response};
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "./static/swagger"]
struct SwaggerAssets;

pub async fn openapi_schema() -> Response {
    let schema = include_str!("../../../openapi.yml");
    // Serve as text so browsers display the YAML instead of forcing a download.
    (
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (
                header::CONTENT_DISPOSITION,
                "inline; filename=\"openapi.yml\"",
            ),
        ],
        schema,
    )
        .into_response()
}

pub async fn openapi_ui() -> Response {
    match SwaggerAssets::get("index.html") {
        Some(content) => {
            let body = String::from_utf8_lossy(&content.data);
            (
                [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
                body.into_owned(),
            )
                .into_response()
        }
        None => (axum::http::StatusCode::NOT_FOUND, "Not Found").into_response(),
    }
}

pub async fn swagger_asset(Path(path): Path<String>) -> Response {
    // Normalize the captured path: remove any leading slash
    let key = path.trim_start_matches('/');
    // If the wildcard is empty (e.g. request to `/swagger/`), serve index.html
    let key = if key.is_empty() { "index.html" } else { key };

    match SwaggerAssets::get(key) {
        Some(content) => {
            let mime_type = mime_guess::from_path(key)
                .first_raw()
                .unwrap_or("application/octet-stream");
            // Add sensible caching headers: index.html should not be heavily cached,
            // static assets can be cached long-term and marked immutable.
            let cache_header = if key == "index.html" {
                "no-cache"
            } else {
                "public, max-age=31536000, immutable"
            };

            (
                [
                    (header::CONTENT_TYPE, mime_type),
                    (header::CACHE_CONTROL, cache_header),
                ],
                content.data.into_response(),
            )
                .into_response()
        }
        None => (axum::http::StatusCode::NOT_FOUND, "Not Found").into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::extract::Path;

    #[tokio::test]
    async fn openapi_ui_returns_index_html() {
        let resp = openapi_ui().await;
        let (_parts, body) = resp.into_parts();
        let bytes = to_bytes(body, usize::MAX).await.expect("body bytes");
        let s = std::str::from_utf8(&bytes).expect("utf8");
        assert!(
            s.contains("<div id=\"swagger-ui\">"),
            "index.html should contain swagger container"
        );
    }

    #[tokio::test]
    async fn swagger_asset_serves_index_when_empty_path() {
        // Call with empty path (simulates `/swagger/` wildcard)
        let resp = swagger_asset(Path(String::new())).await;
        let (_parts, body) = resp.into_parts();
        let bytes = to_bytes(body, usize::MAX).await.expect("body bytes");
        let s = std::str::from_utf8(&bytes).expect("utf8");
        assert!(
            s.contains("<div id=\"swagger-ui\">"),
            "index.html should be served for empty wildcard"
        );
    }

    #[tokio::test]
    async fn openapi_schema_is_displayed_inline() {
        let resp = openapi_schema().await;
        let (parts, body) = resp.into_parts();
        let ct = parts
            .headers
            .get(axum::http::header::CONTENT_TYPE)
            .expect("content-type header");
        assert_eq!(ct, "text/plain; charset=utf-8");
        let cd = parts
            .headers
            .get(axum::http::header::CONTENT_DISPOSITION)
            .expect("content-disposition header");
        assert_eq!(cd, "inline; filename=\"openapi.yml\"");
        let bytes = to_bytes(body, usize::MAX).await.expect("body bytes");
        let s = std::str::from_utf8(&bytes).expect("utf8");
        // Basic sanity check that it's YAML containing the openapi root key
        assert!(
            s.contains("openapi:"),
            "openapi.yml should contain 'openapi:'"
        );
    }
}
