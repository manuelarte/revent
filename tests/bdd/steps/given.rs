use crate::ReventWorld;
use cucumber::gherkin::Step as GherkinStep;
use cucumber::given;
use revent::api::{start_grpc_server, start_rest_server};
use revent::config::Config;
use revent::db::RustLiteSourceEventRepository;
use revent::domain::state::State;
use std::collections::HashMap;

#[given("a running gRPC server")]
#[given("a running gRPC server with the following env variables:")]
fn given_running_grpc_server(world: &mut ReventWorld, #[step] step: &GherkinStep) {
    let mut env_vars: HashMap<String, String> = step
        .table
        .as_ref()
        .map(|table| {
            table
                .rows
                .iter()
                .filter_map(|row| match row.as_slice() {
                    [key, value, ..] => Some((key.trim().to_string(), value.trim().to_string())),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();
    let grpc_port = reserve_free_port();
    let rest_port = reserve_free_port();
    env_vars
        .entry("SERVER_GRPC_PORT".to_string())
        .or_insert_with(|| format!("{grpc_port}"));
    env_vars
        .entry("SERVER_REST_PORT".to_string())
        .or_insert_with(|| format!("{rest_port}"));

    start_server(world, env_vars, grpc_port, rest_port);
}

fn start_server(
    world: &mut ReventWorld,
    env_vars: HashMap<String, String>,
    grpc_port: u16,
    rest_port: u16,
) {
    let cfg = envy::from_iter::<_, Config>(env_vars).expect("failed to build config");

    let server_task = tokio::spawn(async move {
        let repository = RustLiteSourceEventRepository::new()
            .await
            .expect("failed to initialize database");
        repository
            .migrate()
            .await
            .expect("failed to apply database migrations");
        let state = State::new(&cfg, repository);
        let (_grpc_shutdown_tx, grpc_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let (_rest_shutdown_tx, rest_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let _ = tokio::try_join!(
            start_grpc_server(cfg.clone(), state.clone(), async {
                let _ = grpc_shutdown_rx.await;
            }),
            start_rest_server(cfg, state, async {
                let _ = rest_shutdown_rx.await;
            }),
        );
    });

    let grpc_endpoint = format!("http://127.0.0.1:{grpc_port}");
    let rest_endpoint = format!("http://127.0.0.1:{rest_port}");
    world.endpoint = Some(grpc_endpoint);
    world.rest_endpoint = Some(rest_endpoint);
    world.server_task = Some(server_task);
}

fn reserve_free_port() -> u16 {
    std::net::TcpListener::bind("[::1]:0")
        .expect("failed to reserve free port")
        .local_addr()
        .expect("failed to read local addr")
        .port()
}
