mod api;
mod audio;
mod auth;
mod cache;
mod config;
mod dave;
mod discord;
mod errors;
mod models;
mod player;
mod source;
mod telemetry;
mod voice;
mod ws;

use actix_web::{web, App, HttpServer};
use actix_web_httpauth::middleware::HttpAuthentication;
use bm_lib::model::mesastream::MesastreamEvent;
use source::SourceRegistry;
use tracing::info;
use tracing_actix_web::TracingLogger;

use crate::{
    cache::RedisCache,
    config::Settings,
    errors::{AppError, AppResult},
    player::PlayerManager,
};

const SERVICE_NAME: &str = concat!(env!("CARGO_PKG_NAME"), " v", env!("CARGO_PKG_VERSION"));

#[tokio::main]
async fn main() -> AppResult<()> {
    let settings = Settings::from_env()?;

    let tracer = telemetry::init(
        SERVICE_NAME,
        &settings.otlp_endpoint,
        settings.otlp_auth.as_deref(),
        settings.otlp_organization.as_deref(),
    );

    let cache = RedisCache::connect(&settings.redis_uri, &settings.redis_prefix).await?;

    let sources = SourceRegistry::new(
        settings.yt_dlp_path.clone(),
        settings.yt_dlp_cookies.clone(),
    );

    // Broadcast channel for real-time player events → WebSocket clients.
    let (event_tx, _) = tokio::sync::broadcast::channel::<MesastreamEvent>(64);

    let manager = PlayerManager::new(
        settings.clone().into(),
        cache.clone(),
        sources,
        event_tx.clone(),
    );

    // Start background task to clean up orphaned cache files (metadata expired from Redis)
    manager.start_cache_cleanup_task();

    let token = settings.auth_token.clone();
    let bind_addr = format!("0.0.0.0:{}", settings.port);
    let event_tx_for_server = event_tx.clone();

    let server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(manager.clone()))
            .app_data(web::Data::new(cache.clone()))
            .app_data(web::Data::new(token.clone()))
            .app_data(web::Data::new(event_tx_for_server.clone()))
            .wrap(TracingLogger::default())
            // WS endpoint - outside bearer-auth scope so clients can connect
            // without an Authorization header (runs on a private network).
            .route("/ws", web::get().to(ws::ws_handler))
            // All other routes require Bearer authentication.
            .service(
                web::scope("")
                    .wrap(HttpAuthentication::bearer(auth::validator))
                    .service(api::routes()),
            )
    })
    .bind(&bind_addr)
    .map_err(|error| AppError::Internal(format!("failed to bind server: {error}")))?
    .run();

    info!(address = %bind_addr, "mesastream started");

    let handle = server.handle();
    let shutdown_tx = event_tx.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        // Notify connected WS clients that we're shutting down.
        let _ = shutdown_tx.send(MesastreamEvent::Goodbye);
        handle.stop(true).await;
    });

    server
        .await
        .map_err(|error| AppError::Internal(format!("server  error: {error}")))?;

    if let Err(e) = tracer.shutdown() {
        tracing::warn!("OpenTelemetry provider shutdown error: {}", e);
    }

    Ok(())
}
