use crate::config::Config;
use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub struct Telemetry {
    tracer_provider: SdkTracerProvider,
    logger_provider: SdkLoggerProvider,
}

impl Telemetry {
    pub fn shutdown(self) {
        let _ = self.tracer_provider.shutdown();
        let _ = self.logger_provider.shutdown();
    }
}

/// `init` initializes OpenTelemetry tracing and logging with OTLP exporters.
///
/// It returns a `Telemetry` struct that holds the tracer and logger providers,
/// which can be used to shut down the telemetry system gracefully when needed.
///
/// # Errors
///
/// It returns `TelemetryError` in case Telemetry can't be configured.
pub fn init(cfg: &Config) -> Result<Telemetry, TelemetryError> {
    let is_local_environment = cfg.environment().eq_ignore_ascii_case("local");

    let mut resource_attributes = vec![
        KeyValue::new("service.name", cfg.app_name().to_string()),
        KeyValue::new("service.version", cfg.app_version().to_string()),
        KeyValue::new("deployment.environment", cfg.environment().to_string()),
        KeyValue::new("os.type", std::env::consts::OS.to_string()),
        KeyValue::new(
            "os.description",
            format!("{} {}", std::env::consts::OS, std::env::consts::ARCH),
        ),
    ];

    // Process Resource Detector
    resource_attributes.push(KeyValue::new("process.pid", std::process::id().to_string()));
    if let Ok(exe) = std::env::current_exe()
        && let Some(exe_name) = exe.file_name().and_then(|n| n.to_str())
    {
        resource_attributes.push(KeyValue::new(
            "process.executable.name",
            exe_name.to_string(),
        ));
    }

    // Host Resource Detector
    if let Ok(hostname) = hostname::get()
        && let Ok(hostname_str) = hostname.into_string()
    {
        resource_attributes.push(KeyValue::new("host.name", hostname_str));
    }

    let resource = Resource::builder_empty()
        .with_attributes(resource_attributes)
        .build();

    let tracer_provider = if is_local_environment {
        SdkTracerProvider::builder()
            .with_batch_exporter(opentelemetry_stdout::SpanExporter::default())
            .with_resource(resource.clone())
            .build()
    } else {
        let trace_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(cfg.otel_collector_endpoint())
            .build()
            .map_err(TelemetryError::TraceExporter)?;

        SdkTracerProvider::builder()
            .with_batch_exporter(trace_exporter)
            .with_resource(resource.clone())
            .build()
    };

    let tracer = tracer_provider.tracer(cfg.app_name().to_string());

    let logger_provider = if is_local_environment {
        SdkLoggerProvider::builder()
            .with_batch_exporter(opentelemetry_stdout::LogExporter::default())
            .with_resource(resource)
            .build()
    } else {
        let log_exporter = opentelemetry_otlp::LogExporter::builder()
            .with_tonic()
            .with_endpoint(cfg.otel_collector_endpoint())
            .build()
            .map_err(TelemetryError::LogExporter)?;

        SdkLoggerProvider::builder()
            .with_batch_exporter(log_exporter)
            .with_resource(resource)
            .build()
    };

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .with(OpenTelemetryTracingBridge::new(&logger_provider))
        .init();

    if is_local_environment {
        tracing::info!(
            environment = %cfg.environment(),
            telemetry_exporter = "console",
            "OpenTelemetry initialized"
        );
    } else {
        tracing::info!(
            environment = %cfg.environment(),
            telemetry_exporter = "otlp",
            otel_collector_endpoint = %cfg.otel_collector_endpoint(),
            "OpenTelemetry initialized"
        );
    }

    Ok(Telemetry {
        tracer_provider,
        logger_provider,
    })
}

#[derive(Debug)]
pub enum TelemetryError {
    TraceExporter(opentelemetry_otlp::ExporterBuildError),
    LogExporter(opentelemetry_otlp::ExporterBuildError),
}

impl std::fmt::Display for TelemetryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TraceExporter(err) => write!(f, "failed to create OTLP trace exporter: {err}"),
            Self::LogExporter(err) => write!(f, "failed to create OTLP log exporter: {err}"),
        }
    }
}

impl std::error::Error for TelemetryError {}
