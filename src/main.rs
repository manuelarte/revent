use revent::api::{start_grpc_server, start_rest_server};
use revent::config::Config;
use revent::db::RustLiteSourceEventRepository;
use revent::domain::state::State;
use revent::telemetry;
use std::process::ExitCode;
use tokio::sync::watch;
use tracing::{error, info};

#[tokio::main]
async fn main() -> ExitCode {
    if let Err(e) = dotenvy::dotenv() {
        eprintln!("Error loading .env file: {e}");
        return ExitCode::FAILURE;
    }

    let cfg = match load_and_validate_config() {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Error: {e}");
            return ExitCode::FAILURE;
        }
    };

    let telemetry = match telemetry::init(&cfg) {
        Ok(tel) => tel,
        Err(e) => {
            eprintln!("Error initializing telemetry: {e}");
            return ExitCode::FAILURE;
        }
    };

    let db = match RustLiteSourceEventRepository::new().await {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Error initializing database: {e}");
            return ExitCode::FAILURE;
        }
    };

    if let Err(e) = db.migrate().await {
        eprintln!("Error applying database migrations: {e}");
        return ExitCode::FAILURE;
    }
    info!("Database migrations applied successfully");

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    tokio::spawn(async move {
        if let Err(err) = wait_for_shutdown_signal().await {
            error!(%err, "Failed to listen for shutdown signal");
        } else {
            info!("Shutdown signal received (Ctrl+C or SIGTERM), stopping servers");
        }
        let _ = shutdown_tx.send(true);
    });

    let mut grpc_rx = shutdown_rx.clone();
    let grpc_shutdown = async move {
        if !*grpc_rx.borrow() {
            let _ = grpc_rx.changed().await;
        }
    };

    let mut rest_rx = shutdown_rx;
    let rest_shutdown = async move {
        if !*rest_rx.borrow() {
            let _ = rest_rx.changed().await;
        }
    };

    let state = State::new(&cfg, db);
    info!(gRPC = %cfg.get_grpc_address(), rest=%cfg.get_rest_address(), "Starting server");
    if let Err(e) = tokio::try_join!(
        start_grpc_server(cfg.clone(), state.clone(), grpc_shutdown),
        start_rest_server(cfg, state.clone(), rest_shutdown),
    ) {
        eprintln!("Error starting servers: {e}");
        return ExitCode::FAILURE;
    }

    telemetry.shutdown();

    ExitCode::SUCCESS
}

fn load_and_validate_config() -> Result<Config, String> {
    let cfg = envy::from_env::<Config>().map_err(|e| format!("Failed to parse config: {e}"))?;

    if let Err(errors) = cfg.validate() {
        let error_messages: Vec<String> = errors.into_iter().collect();
        return Err(format!(
            "Config validation failed with the following errors: {}",
            error_messages.join(", ")
        ));
    }
    Ok(cfg)
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> Result<(), std::io::Error> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate())?;

    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> Result<(), std::io::Error> {
    tokio::signal::ctrl_c().await
}
