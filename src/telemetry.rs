use opentelemetry::{
    global, propagation::TextMapCompositePropagator, trace::TracerProvider as _, KeyValue,
};
use opentelemetry_otlp::{SpanExporter, WithTonicConfig};
use opentelemetry_sdk::{
    propagation::{BaggagePropagator, TraceContextPropagator},
    trace::{BatchConfigBuilder, BatchSpanProcessor, TracerProvider as SdkTracerProvider},
    Resource,
};
use tonic::{metadata::MetadataMap, transport::Channel};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

/// Initialise tracing with an OTLP exporter pointed at OpenObserve (or any
/// OTLP-compatible backend).  Returns the provider so callers can drive a
/// clean `shutdown()` on exit.
pub fn init(
    service_name: &'static str,
    otlp_endpoint: &str,
    otlp_auth: Option<&str>,
    otlp_organization: Option<&str>,
) -> SdkTracerProvider {
    let mut metadata = MetadataMap::new();
    if let Some(auth) = otlp_auth {
        metadata.insert(
            "authorization",
            auth.parse().expect("invalid auth header value"),
        );
    }
    if let Some(org) = otlp_organization {
        metadata.insert(
            "organization",
            org.parse().expect("invalid organization header value"),
        );
    }

    let channel = Channel::from_shared(otlp_endpoint.to_string())
        .expect("invalid OTLP endpoint URI")
        .connect_lazy();

    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_channel(channel)
        .with_metadata(metadata)
        .build()
        .expect("Failed to build OTLP span exporter");

    let resource = Resource::new([KeyValue::new("service.name", service_name)]);

    let processor = BatchSpanProcessor::builder(exporter, opentelemetry_sdk::runtime::Tokio)
        .with_batch_config(
            BatchConfigBuilder::default()
                .with_scheduled_delay(std::time::Duration::from_secs(1))
                .build(),
        )
        .build();

    let provider = SdkTracerProvider::builder()
        .with_span_processor(processor)
        .with_resource(resource)
        .build();

    let tracer = provider.tracer(service_name);

    global::set_text_map_propagator(TextMapCompositePropagator::new(vec![
        Box::new(TraceContextPropagator::new()),
        Box::new(BaggagePropagator::new()),
    ]));

    // Stdout: info and above only (no debug spam).
    let stdout_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("info,h2=off,hyper=off,tower=off,symphonia_bundle_mp3=error,symphonia_format_ogg=error,symphonia_format_isomp4=error,mesastream::audio=info")
    });
    let stdout_layer = tracing_subscriber::fmt::layer().with_filter(stdout_filter);

    // OTLP (OpenObserve): debug and above — debug! logs go here only.
    let otlp_filter = EnvFilter::new(
        "debug,h2=off,hyper=off,tower=off,symphonia_bundle_mp3=error,symphonia_format_ogg=error",
    );
    let otlp_layer = OpenTelemetryLayer::new(tracer).with_filter(otlp_filter);

    tracing_subscriber::registry()
        .with(stdout_layer)
        .with(otlp_layer)
        .init();

    provider
}
