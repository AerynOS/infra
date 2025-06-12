//! Tracing support
use std::{env, sync::OnceLock};

use http::Uri;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{propagation::TraceContextPropagator, trace::SdkTracerProvider};
use serde::Deserialize;
use thiserror::Error;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

pub use opentelemetry::{Context as OpenTelemetryContext, context::FutureExt};
pub use tracing_opentelemetry::OpenTelemetrySpanExt;

static OTEL_TRACER_PROVIDER: OnceLock<SdkTracerProvider> = OnceLock::new();

/// Output format
#[derive(Debug, Clone, Copy, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum Format {
    /// Compact
    #[default]
    Compact,
    /// JSON
    Json,
}

/// Tracing configuration
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Level filter, such as `my_crate=info,my_crate::my_mod=debug,[my_span]=trace`
    #[serde(default = "default_level_filter")]
    pub level_filter: String,
    /// Output format
    #[serde(default)]
    pub format: Format,
    /// Otlp configuration
    ///
    /// If specified, export tracing to
    /// provided OTLP collector
    pub otlp: Option<OtlpConfig>,
}

/// Otlp configuration
#[derive(Debug, Clone, Deserialize)]
pub struct OtlpConfig {
    /// Endpoint to the OTLP collector
    #[serde(default, with = "http_serde::uri")]
    pub endpoint: Uri,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            level_filter: default_level_filter(),
            format: Format::default(),
            otlp: None,
        }
    }
}

fn default_level_filter() -> String {
    "info".into()
}

/// Initialize tracing using the provided [`Config`]
///
/// `RUST_LOG` env var can be set at runtime to override the [`Config::level_filter`]
pub fn init(config: &Config, service_description: impl ToString) -> Result<(), Error> {
    let level_filter = if let Ok(level) = env::var("RUST_LOG") {
        level
    } else {
        config.level_filter.to_string()
    };

    let subscriber = tracing_subscriber::registry()
        .with(EnvFilter::builder().parse_lossy(level_filter))
        .with(
            config
                .otlp
                .as_ref()
                .map(|config| otlp_exporter(config, &service_description))
                .transpose()?,
        );

    match config.format {
        Format::Compact => {
            subscriber
                .with(tracing_subscriber::fmt::layer().compact().with_target(false))
                .init();
        }
        Format::Json => {
            subscriber
                .with(
                    tracing_subscriber::fmt::layer()
                        .json()
                        .with_target(false)
                        .flatten_event(true),
                )
                .init();
        }
    }

    Ok(())
}

fn otlp_exporter<S>(
    config: &OtlpConfig,
    description: &impl ToString,
) -> Result<impl tracing_subscriber::Layer<S>, Error>
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let otlp_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(config.endpoint.to_string())
        .with_protocol(opentelemetry_otlp::Protocol::Grpc)
        .build()?;

    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name(description.to_string())
                .build(),
        )
        .with_batch_exporter(otlp_exporter)
        .build();
    let tracer = tracer_provider.tracer(description.to_string());

    OTEL_TRACER_PROVIDER
        .set(tracer_provider)
        .expect("tracing initialized once");

    Ok(tracing_opentelemetry::layer().with_tracer(tracer))
}

/// Flush & gracefully shutdown tracing providers
pub async fn shutdown() {
    tokio::task::spawn_blocking(|| {
        if let Some(provider) = OTEL_TRACER_PROVIDER.get() {
            provider.shutdown().expect("shutdown tracing");
        }
    })
    .await
    .expect("join handle");
}

/// Tracing initialization error
#[derive(Debug, Error)]
pub enum Error {
    /// Error setting up OLTP exporter
    #[error(transparent)]
    OltpExporter(#[from] opentelemetry_otlp::ExporterBuildError),
    /// Error setting global tracing subscriber
    #[error(transparent)]
    SetGlobalSubscriber(#[from] tracing::subscriber::SetGlobalDefaultError),
}
