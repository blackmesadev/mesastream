//! YouTube source: yt-dlp backed audio streaming and metadata resolution.

pub mod ytdlp;

use async_trait::async_trait;
use serde::Deserialize;
use url::Url;

use crate::{
    errors::{AppError, AppResult},
    models::SourceKind,
};

use super::{AudioSource, ByteStream, ResolvedMetadata};

/// yt-dlp backed YouTube source implementation.
#[derive(Clone)]
pub struct YouTubeSource {
    yt_dlp_path: String,
    cookies_path: Option<String>,
}

impl YouTubeSource {
    pub fn new(yt_dlp_path: impl Into<String>, cookies_path: Option<String>) -> Self {
        Self {
            yt_dlp_path: yt_dlp_path.into(),
            cookies_path,
        }
    }
}

#[derive(Debug, Deserialize)]
struct YtDlpMetadata {
    title: Option<String>,
    uploader: Option<String>,
    duration: Option<u64>,
}

#[async_trait]
impl AudioSource for YouTubeSource {
    async fn resolve_metadata(&self, url: &str) -> AppResult<ResolvedMetadata> {
        if url.is_empty() || url == "false" || url == "true" {
            return Err(AppError::SourceUnavailable(format!(
                "invalid YouTube URL: {url}"
            )));
        }

        let extra_args = [
            "--age-limit",
            "99",
            "--format",
            "bestaudio/best",
            "--extractor-args",
            "youtube:player_client=web,android",
        ];

        let metadata: YtDlpMetadata = ytdlp::fetch_and_parse_metadata(
            &self.yt_dlp_path,
            url,
            "youtube",
            &extra_args,
            self.cookies_path.as_deref(),
        )
        .await?;

        Ok(ResolvedMetadata {
            source: SourceKind::Youtube,
            artist: metadata.uploader.unwrap_or_else(|| "Unknown".to_string()),
            title: metadata.title.unwrap_or_else(|| "Unknown".to_string()),
            duration_ms: metadata.duration.unwrap_or(0) * 1000,
        })
    }

    async fn get_stream(&self, url: &str) -> AppResult<ByteStream> {
        if url.is_empty() || url == "false" || url == "true" {
            return Err(AppError::SourceUnavailable(format!(
                "invalid YouTube URL: {url}"
            )));
        }

        // Spawn yt-dlp — bytes start flowing to the stream immediately.
        ytdlp::spawn_audio_stream(&self.yt_dlp_path, url, self.cookies_path.as_deref()).await
    }

    fn supports(&self, input: &str) -> bool {
        let Ok(url) = Url::parse(input) else {
            return false;
        };

        matches!(
            url.domain(),
            Some("youtube.com")
                | Some("www.youtube.com")
                | Some("youtu.be")
                | Some("music.youtube.com")
        )
    }
}
