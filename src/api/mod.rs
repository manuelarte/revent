use crate::api::grpc::control::ControlService;
use crate::api::grpc::control::protocontrol::control_server::ControlServer;
use crate::api::rest::{actuator_handler, clients_handler, openapi_handler, source_events_handler};
use crate::config::Config;
use crate::domain::Repository;
use crate::domain::state::State;
use axum::Router;
use axum::response::Redirect;
use axum::routing::get;
use rune_axum_sanitize::SanitizeLayer;
use std::fmt::{Display, Formatter};
use std::future::Future;
use tokio::sync::watch;
use tonic::transport::Server;

mod grpc;
mod rest;

#[derive(Debug)]
pub struct ErrorStartingServer {
    pub reason: String,
}

impl Display for ErrorStartingServer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Error starting server: {}", self.reason)
    }
}

/// `start_rest_server` start the rest server
///
/// # Errors
///
/// Return an error if it can't start the rest server or can't find the commit hash.
pub async fn start_rest_server<R: Repository + 'static>(
    cfg: Config,
    state: State<R>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), ErrorStartingServer> {
    let addr = cfg.get_rest_address();
    let version_info = rustc_tools_util::get_version_info!();
    let app = Router::new()
        .route("/actuator/info", get(actuator_handler::info))
        .route("/api/clients", get(clients_handler::get_clients_page))
        .route(
            "/api/clients/{client_id}",
            get(clients_handler::get_client_by_id),
        )
        .route(
            "/api/source-events",
            get(source_events_handler::get_source_events),
        )
        .route("/openapi.yml", get(openapi_handler::openapi_schema))
        .route(
            "/swagger",
            get(|| async { Redirect::permanent("/swagger/index.html") }),
        )
        .route(
            "/swagger/",
            get(|| async { Redirect::permanent("/swagger/index.html") }),
        )
        .route("/swagger/index.html", get(openapi_handler::openapi_ui))
        .route("/swagger/{*wildcard}", get(openapi_handler::swagger_asset))
        .layer(SanitizeLayer::default())
        .with_state(AppState {
            cfg,
            git_info: GitInfo {
                branch: env!("GIT_BRANCH").to_string(),
                commit_id: version_info
                    .commit_hash
                    .unwrap_or_else(|| "Commit hash not found".to_string()),
            },
            state,
        });
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| ErrorStartingServer {
            reason: format!("Error creating TcpListener: {e}"),
        })?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(|e| ErrorStartingServer {
            reason: format!("Error starting HTTP server: {e}"),
        })?;
    Ok(())
}

/// Spawns the gRPC server in a background task and returns a handle to it
/// # Errors
///
/// Returns an error if the gRPC server can't be started.
pub async fn start_grpc_server<R: Repository + 'static>(
    cfg: Config,
    state: State<R>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), ErrorStartingServer> {
    let addr = cfg.get_grpc_address();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let control_service = ControlServer::new(ControlService::new(state, shutdown_rx.clone()));

    tokio::spawn(async move {
        shutdown.await;
        let _ = shutdown_tx.send(true);
    });

    let mut server_shutdown = shutdown_rx;

    Server::builder()
        .add_service(control_service)
        .serve_with_shutdown(addr, async move {
            if !*server_shutdown.borrow() {
                let _ = server_shutdown.changed().await;
            }
        })
        .await
        .map_err(|e| ErrorStartingServer {
            reason: format!("Error starting gRPC server: {e}"),
        })?;

    Ok(())
}

#[derive(Clone)]
pub(crate) struct AppState<R: Repository + 'static> {
    cfg: Config,
    git_info: GitInfo,
    state: State<R>,
}

#[derive(Clone, Debug)]
pub(crate) struct GitInfo {
    branch: String,
    commit_id: String,
}
