use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::Stream;
use tracing::{debug, instrument, Instrument};

use crate::{
    errors::{AppError, AppResult},
    models::SourceKind,
};

pub mod soundcloud;
pub mod youtube;

/// An async byte stream of audio data from a source.
///
/// Each item is a chunk of raw audio bytes (container format depends on source).
/// The stream ends naturally (returns `None`) when the source is exhausted.
/// Dropping the stream cancels any in-flight downloads.
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, String>> + Send>>;

/// Metadata-only resolution result.
#[derive(Debug, Clone)]
pub struct ResolvedMetadata {
    pub source: SourceKind,
    pub artist: String,
    pub title: String,
    pub duration_ms: u64,
}

/// Source abstraction for pluggable audio providers.
///
/// Each source "owns" how its audio is downloaded and exposed as an async
/// byte stream. `get_stream` returns data immediately (progressive download),
/// never waiting for a full download to complete.
#[async_trait]
pub trait AudioSource: Send + Sync {
    /// Resolve metadata (title, artist, duration) without starting a download.
    /// Called at enqueue time so the API response has full metadata immediately.
    async fn resolve_metadata(&self, url: &str) -> AppResult<ResolvedMetadata>;

    /// Start streaming audio from the beginning.
    ///
    /// Returns an async byte stream of raw audio data that begins delivering
    /// chunks immediately as they are downloaded. The encoder consumes this
    /// stream through an async→sync bridge.
    async fn get_stream(&self, url: &str) -> AppResult<ByteStream>;

    fn supports(&self, url: &str) -> bool;
}

/// Source registry that dispatches URL resolution to matching providers.
#[derive(Clone)]
pub struct SourceRegistry {
    youtube: youtube::YouTubeSource,
    soundcloud: soundcloud::SoundcloudSource,
}

impl SourceRegistry {
    pub fn new(yt_dlp_path: impl Into<String>, cookies_path: Option<String>) -> Self {
        let yt_dlp_path = yt_dlp_path.into();
        let cookies = cookies_path.clone();
        Self {
            youtube: youtube::YouTubeSource::new(yt_dlp_path.clone(), cookies.clone()),
            soundcloud: soundcloud::SoundcloudSource::new(),
        }
    }

    /// Resolve metadata only - called at enqueue time.
    #[instrument(skip(self), fields(url = url))]
    pub async fn resolve_metadata(&self, url: &str) -> AppResult<ResolvedMetadata> {
        let result = match self.route_source(url) {
            Some(SourceKind::Youtube) => self.youtube.resolve_metadata(url).await,
            Some(SourceKind::Soundcloud) => self.soundcloud.resolve_metadata(url).await,
            None => Err(AppError::SourceUnavailable(
                "no source supports this URL".to_string(),
            )),
        };

        if let Err(e) = &result {
            tracing::error!(error = %e, "metadata resolution failed");
        }

        result
    }

    /// Start streaming - called at play time.
    /// Returns an async byte stream that delivers audio data as it downloads.
    #[instrument(skip(self), fields(url = url))]
    pub async fn get_stream(&self, url: &str) -> AppResult<ByteStream> {
        let start = std::time::Instant::now();

        let result = match self.route_source(url) {
            Some(SourceKind::Youtube) => self.youtube.get_stream(url).in_current_span().await,
            Some(SourceKind::Soundcloud) => self.soundcloud.get_stream(url).in_current_span().await,
            None => Err(AppError::SourceUnavailable(
                "no source supports this URL".to_string(),
            )),
        };

        match &result {
            Ok(_) => debug!(elapsed_ms = start.elapsed().as_millis(), "stream started"),
            Err(e) => {
                tracing::error!(elapsed_ms = start.elapsed().as_millis(), error = %e, "stream resolution failed")
            }
        }

        result
    }

    pub fn detect_source(&self, url: &str) -> Option<SourceKind> {
        self.route_source(url)
    }

    fn route_source(&self, url: &str) -> Option<SourceKind> {
        if self.youtube.supports(url) {
            Some(SourceKind::Youtube)
        } else if self.soundcloud.supports(url) {
            Some(SourceKind::Soundcloud)
        } else {
            None
        }
    }
}
