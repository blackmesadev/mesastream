use std::env;

use crate::errors::{AppError, AppResult};

/// Runtime settings loaded from environment variables.
#[derive(Clone, Debug)]
pub struct Settings {
    pub auth_token: String,
    pub redis_uri: String,
    pub redis_prefix: String,
    pub otlp_endpoint: String,
    pub otlp_auth: Option<String>,
    pub otlp_organization: Option<String>,
    pub port: u16,
    pub yt_dlp_path: String,
    pub encoding_bitrate: u32,
    /// Directory where transcoded opus frames are cached on disk.
    pub audio_cache_path: String,
    /// TTL for cached audio metadata in Redis (in seconds). When metadata expires,
    /// the corresponding .msopus file becomes orphaned and will be cleaned up.
    pub audio_cache_ttl_secs: u64,
    /// Path to cookies file for yt-dlp (optional, for authentication/bypassing restrictions).
    pub yt_dlp_cookies: Option<String>,
    /// Capacity of the async byte channel between HTTP stream fetch and decoder.
    ///
    /// Higher values absorb network jitter; lower values reduce memory/latency.
    pub source_stream_channel_capacity: usize,
    /// Buffer size (bytes) for cache file writes.
    pub cache_writer_buffer_bytes: usize,
    /// Buffer size (bytes) for cache file reads.
    pub cache_reader_buffer_bytes: usize,
}

impl Settings {
    /// Loads service settings from `.env` and process environment.
    pub fn from_env() -> AppResult<Self> {
        dotenvy::dotenv().ok();

        let auth_token = required("AUTH_TOKEN")?;
        let redis_uri = required("REDIS_URI")?;
        let redis_prefix = env::var("REDIS_PREFIX").unwrap_or_else(|_| "mesastream".to_string());
        let otlp_endpoint = required("OTLP_ENDPOINT")?;
        let otlp_auth = env::var("OTLP_AUTH").ok();
        let otlp_organization = env::var("OTLP_ORGANIZATION").ok();

        let port = env::var("PORT")
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(8080);

        let encoding_bitrate = env::var("ENCODING_BITRATE")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(128);

        let source_stream_channel_capacity = env::var("SOURCE_STREAM_CHANNEL_CAPACITY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(16);

        let cache_writer_buffer_bytes = env::var("CACHE_WRITER_BUFFER_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(192 * 1024);

        let cache_reader_buffer_bytes = env::var("CACHE_READER_BUFFER_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(64 * 1024);

        Ok(Self {
            auth_token,
            redis_uri,
            redis_prefix,
            otlp_endpoint,
            otlp_auth,
            otlp_organization,
            port,
            yt_dlp_path: env::var("YT_DLP_PATH").unwrap_or_else(|_| "yt-dlp".to_string()),
            encoding_bitrate,
            audio_cache_path: env::var("AUDIO_CACHE_PATH")
                .unwrap_or_else(|_| "/var/cache/mesastream/audio".to_string()),
            audio_cache_ttl_secs: env::var("AUDIO_CACHE_TTL_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(604800), // 7 days default
            yt_dlp_cookies: env::var("YT_DLP_COOKIES").ok(),
            source_stream_channel_capacity,
            cache_writer_buffer_bytes,
            cache_reader_buffer_bytes,
        })
    }
}

fn required(key: &str) -> AppResult<String> {
    env::var(key)
        .map_err(|_| AppError::Internal(format!("Missing required environment variable: {key}")))
}
