use serde::Deserialize;
use std::net::SocketAddr;
use std::time::Duration;

#[derive(Deserialize, Clone, Debug)]
pub struct Config {
    /// `app_name` app name. By default $`CARGO_PKG_NAME` is used.
    #[serde(default = "default_app_name")]
    app_name: String,

    /// `app_version` version of the application. By default $`CARGO_PKG_VERSION` is used.
    #[serde(default = "default_app_version")]
    app_version: String,

    /// `environment` environment that the server is running.
    #[serde(default = "default_environment")]
    environment: String,

    /// `server_rest_port` port where the Rest server is going to run.
    #[serde(default = "default_server_html_port")]
    server_rest_port: u16,

    /// `server_grpc_port` port where the gRPC server is going to run.
    #[serde(default = "default_server_grpc_port")]
    server_grpc_port: u16,

    /// `server_interval_heartbeat_message` interval that the server is sending `Heartbeat` messages
    /// to check if a client is still connected.
    #[serde(
        with = "humantime_serde",
        default = "default_server_interval_heartbeat_message"
    )]
    server_interval_heartbeat_message: Duration,

    /// `server_expiration_client` expiration duration with respect to the `last_seen` field in which
    /// the server sends a `HeartBeat` message to the client to check whether is online.
    #[serde(with = "humantime_serde", default = "default_server_expiration_client")]
    server_expiration_client: Duration,

    /// `server_query_response_timeout` timeout when if the server receives no response for a query
    /// it triggers `QueryTimedOut` event.
    #[serde(
        with = "humantime_serde",
        default = "default_server_query_response_timeout"
    )]
    server_query_response_timeout: Duration,

    /// `otel_collector_endpoint` OTLP/gRPC endpoint exposed by the collector.
    #[serde(default = "default_otel_collector_endpoint")]
    otel_collector_endpoint: String,
}

impl Config {
    /// `validate` validates the configuration and returns a list of errors if any.
    ///
    /// # Errors
    ///
    /// Returns the list of configuration errors if applicable.
    pub fn validate(&self) -> Result<(), Vec<ConfigError>> {
        let mut cfg_errors: Vec<ConfigError> = vec![];
        // Heartbeat interval should be lower than the expiration duration.
        let interval_heartbeat_message = self.server_interval_heartbeat_message();
        let expiration_client = self.server_expiration_client();
        if interval_heartbeat_message > expiration_client {
            cfg_errors.push(format!(
                    "server_interval_heartbeat_message ({interval_heartbeat_message:?}) should be lower than server_expiration_client ({expiration_client:?})"
                ));
        }

        // server_query_response_timeout should not be low
        if self.server_query_response_timeout() < &Duration::from_millis(500) {
            let server_query_response_timeout = self.server_query_response_timeout();
            cfg_errors.push(format!(
                "server_query_response_timeout ({server_query_response_timeout:?}) it is very low"
            ));
        }

        if !cfg_errors.is_empty() {
            return Err(cfg_errors);
        }
        Ok(())
    }

    /// `app_name`
    #[must_use]
    pub fn app_name(&self) -> &str {
        &self.app_name
    }

    /// `app_version`
    #[must_use]
    pub fn app_version(&self) -> &str {
        &self.app_version
    }

    /// `environment` environment the server is running.
    #[must_use]
    pub fn environment(&self) -> &str {
        &self.environment
    }

    /// `server_html_port` port where the HTML server is running.
    #[must_use]
    pub const fn server_rest_port(&self) -> u16 {
        self.server_rest_port
    }

    /// `server_grpc_port` port where the gRPC server is running.
    #[must_use]
    pub const fn server_grpc_port(&self) -> u16 {
        self.server_grpc_port
    }

    /// `server_interval_heartbeat_message` interval that the server is sending `Heartbeat` messages
    #[must_use]
    pub const fn server_interval_heartbeat_message(&self) -> &Duration {
        &self.server_interval_heartbeat_message
    }

    /// `server_query_response_timeout` timeout when if the server receives no response for a query
    /// it triggers `QueryTimedOut` event.
    #[must_use]
    pub const fn server_query_response_timeout(&self) -> &Duration {
        &self.server_query_response_timeout
    }

    /// `server_expiration_client` expiration duration with respect to the `last_seen` field in which
    /// the server sends a `HeartBeat` message to the client to check whether is online.
    #[must_use]
    pub const fn server_expiration_client(&self) -> &Duration {
        &self.server_expiration_client
    }

    /// `otel_collector_endpoint` OTLP/gRPC endpoint for traces and logs.
    #[must_use]
    pub fn otel_collector_endpoint(&self) -> &str {
        &self.otel_collector_endpoint
    }

    /// `get_rest_address` get the socket address for the REST server based on the configuration.
    ///
    /// # Panics
    ///
    /// It panics if it can't create the `SocketAddr`.
    #[must_use]
    pub fn get_rest_address(&self) -> SocketAddr {
        let port = self.server_rest_port();
        format!("0.0.0.0:{port}")
            .parse()
            .expect("Failed to parse REST address")
    }

    /// `get_address` get the socket address for the gRPC server based on the configuration.
    ///
    /// # Panics
    ///
    /// It panics if it can't create the `SocketAddr`.
    #[must_use]
    pub fn get_grpc_address(&self) -> SocketAddr {
        let port = self.server_grpc_port();
        format!("[::1]:{port}")
            .parse()
            .expect("Failed to parse address")
    }
}

pub type ConfigError = String;

fn default_app_name() -> String {
    env!("CARGO_PKG_NAME").to_string()
}

fn default_app_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

fn default_environment() -> String {
    "local".to_string()
}

const fn default_server_html_port() -> u16 {
    10001
}

const fn default_server_grpc_port() -> u16 {
    10000
}

const fn default_server_interval_heartbeat_message() -> Duration {
    Duration::from_secs(1)
}

const fn default_server_query_response_timeout() -> Duration {
    Duration::from_secs(3)
}

const fn default_server_expiration_client() -> Duration {
    Duration::from_millis(2800)
}

fn default_otel_collector_endpoint() -> String {
    "http://localhost:4317".to_string()
}
