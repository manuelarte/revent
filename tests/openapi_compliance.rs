use revent::config::Config;
use revent::db::RustLiteSourceEventRepository;
use std::process::Command;

/// Integration test that validates the REST API against the `OpenAPI` schema
/// Requires: Docker to be installed and `schemathesis/schemathesis` image available.
///
/// This test:
/// 1. Starts the REST server on a random port
/// 2. Runs schemathesis (in Docker) to validate the API against the `OpenAPI` schema
/// 3. Stops the server and checks if validation passed
#[tokio::test]
async fn test_rest_api_openapi_compliance() {
    let port = reserve_free_port();

    println!("Starting REST server on port {port}");

    // Start the REST server in a background task
    let server_handle = tokio::spawn(async move {
        let cfg =
            envy::from_iter::<_, Config>([("SERVER_REST_PORT".to_string(), format!("{port}"))])
                .expect("failed to build config");
        let repository = RustLiteSourceEventRepository::new()
            .await
            .expect("failed to initialize database");
        repository
            .migrate()
            .await
            .expect("failed to apply database migrations");
        let state = revent::domain::state::State::new(&cfg, repository);

        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        if let Err(e) = revent::api::start_rest_server(cfg, state, async {
            let _ = shutdown_rx.await;
        })
        .await
        {
            eprintln!("REST server error: {e}");
        }
    });

    println!("Running schemathesis validation in Docker...");

    // Run in a blocking thread so we don't block the tokio runtime.
    let output = tokio::task::spawn_blocking(move || {
        let schema_url = if cfg!(target_os = "linux") {
            format!("http://localhost:{port}/openapi.yml")
        } else {
            // On macOS / Windows, Docker containers reach host services via host.docker.internal.
            format!("http://host.docker.internal:{port}/openapi.yml")
        };

        let mut cmd = Command::new("docker");
        cmd.arg("run").arg("--rm");
        if cfg!(target_os = "linux") {
            cmd.arg("--network").arg("host");
        }
        cmd.arg("schemathesis/schemathesis")
            .arg("run")
            .arg("--exclude-checks")
            .arg("negative_data_rejection")
            .arg("--wait-for-schema")
            .arg("10")
            .arg(schema_url)
            .output()
            .expect("Failed to run schemathesis Docker container - ensure Docker is installed and running")
    })
    .await
    .expect("schemathesis thread panicked");

    // Stop the server
    server_handle.abort();
    let _ = server_handle.await;

    println!("\n=== Schemathesis Output ===");
    println!("stdout:\n{}", String::from_utf8_lossy(&output.stdout));
    if !output.stderr.is_empty() {
        eprintln!("stderr:\n{}", String::from_utf8_lossy(&output.stderr));
    }

    assert!(
        output.status.success(),
        "schemathesis validation failed. Exit code: {:?}",
        output.status.code()
    );
}

fn reserve_free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("failed to reserve free port")
        .local_addr()
        .expect("failed to read local addr")
        .port()
}
