pub mod model;
mod rest;

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{unfold, StreamExt};
use tokio::sync::{mpsc, RwLock};
use url::Url;

use crate::{
    errors::{AppError, AppResult},
    models::SourceKind,
};

use super::{AudioSource, ByteStream, ResolvedMetadata};

#[derive(Debug, Clone)]
pub struct SoundcloudClient {
    http_client: reqwest::Client,
    /// Cached SoundCloud client_id, fetched on first use and refreshed on auth errors.
    client_id: Arc<RwLock<Option<String>>>,
}

/// SoundCloud source implementation with native resolution.
/// Handles both progressive downloads (direct HTTP) and HLS (m3u8 playlists).
#[derive(Clone)]
pub struct SoundcloudSource {
    client: SoundcloudClient,
}

impl SoundcloudSource {
    pub fn new() -> Self {
        Self {
            client: SoundcloudClient::new(),
        }
    }

    /// Stream a progressive (direct) download URL.
    async fn stream_progressive(&self, url: &str) -> AppResult<ByteStream> {
        let response = self.client.http_client.get(url).send().await.map_err(|e| {
            AppError::SourceUnavailable(format!("progressive download failed: {e}"))
        })?;

        if !response.status().is_success() {
            return Err(AppError::SourceUnavailable(format!(
                "progressive download HTTP {}",
                response.status()
            )));
        }

        let stream = response
            .bytes_stream()
            .map(|result| result.map_err(|e| e.to_string()));

        Ok(Box::pin(stream))
    }

    /// Stream an HLS (m3u8) playlist by fetching and concatenating segments.
    async fn stream_hls(&self, m3u8_url: &str) -> AppResult<ByteStream> {
        let response = self
            .client
            .http_client
            .get(m3u8_url)
            .send()
            .await
            .map_err(|e| AppError::SourceUnavailable(format!("m3u8 fetch failed: {e}")))?;

        let m3u8_body = response
            .text()
            .await
            .map_err(|e| AppError::SourceUnavailable(format!("m3u8 read failed: {e}")))?;

        let segment_urls = parse_m3u8_segments(&m3u8_body, m3u8_url);
        if segment_urls.is_empty() {
            return Err(AppError::SourceUnavailable(
                "m3u8 playlist contained no segments".to_string(),
            ));
        }

        tracing::debug!(segments = segment_urls.len(), "parsed HLS playlist");

        let (tx, rx) = mpsc::channel::<Result<Bytes, String>>(16);
        let client = self.client.http_client.clone();

        tokio::spawn(async move {
            for segment_url in segment_urls {
                let resp = match client.get(&segment_url).send().await {
                    Ok(r) if r.status().is_success() => r,
                    Ok(r) => {
                        let _ = tx.send(Err(format!("segment HTTP {}", r.status()))).await;
                        return;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(format!("segment fetch failed: {e}"))).await;
                        return;
                    }
                };

                let mut byte_stream = resp.bytes_stream();
                while let Some(chunk) = byte_stream.next().await {
                    match chunk {
                        Ok(bytes) => {
                            if tx.send(Ok(bytes)).await.is_err() {
                                return; // receiver dropped
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(e.to_string())).await;
                            return;
                        }
                    }
                }
            }
        });

        let stream = unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        });

        Ok(Box::pin(stream))
    }
}

#[async_trait]
impl AudioSource for SoundcloudSource {
    async fn resolve_metadata(&self, url: &str) -> AppResult<ResolvedMetadata> {
        if url.is_empty() || url == "false" || url == "true" {
            return Err(AppError::SourceUnavailable(format!(
                "invalid SoundCloud URL: {url}"
            )));
        }

        let track =
            self.client.track_from_url(url).await.map_err(|e| {
                AppError::SourceUnavailable(format!("soundcloud resolve failed: {e}"))
            })?;

        Ok(ResolvedMetadata {
            source: SourceKind::Soundcloud,
            artist: track.user.username,
            title: track.title,
            duration_ms: track.duration.unwrap_or(0),
        })
    }

    async fn get_stream(&self, url: &str) -> AppResult<ByteStream> {
        if url.is_empty() || url == "false" || url == "true" {
            return Err(AppError::SourceUnavailable(format!(
                "invalid SoundCloud URL: {url}"
            )));
        }

        let track =
            self.client.track_from_url(url).await.map_err(|e| {
                AppError::SourceUnavailable(format!("soundcloud resolve failed: {e}"))
            })?;

        let (stream_url, transcoding) = self
            .client
            .resolve_stream_for_track(&track)
            .await
            .map_err(|e| {
                AppError::SourceUnavailable(format!("soundcloud stream resolve failed: {e}"))
            })?;

        match transcoding.format.protocol.as_str() {
            "progressive" => self.stream_progressive(&stream_url).await,
            "hls" => self.stream_hls(&stream_url).await,
            other => Err(AppError::SourceUnavailable(format!(
                "unsupported SoundCloud protocol: {other}"
            ))),
        }
    }

    fn supports(&self, input: &str) -> bool {
        let Ok(url) = Url::parse(input) else {
            return false;
        };

        matches!(
            url.domain(),
            Some("soundcloud.com") | Some("www.soundcloud.com") | Some("on.soundcloud.com")
        )
    }
}

/// Parse segment URLs from an m3u8 playlist body.
///
/// Handles both absolute URLs and relative paths (resolved against the
/// playlist's base URL).
fn parse_m3u8_segments(body: &str, playlist_url: &str) -> Vec<String> {
    let base_url = if let Some(pos) = playlist_url.rfind('/') {
        &playlist_url[..=pos]
    } else {
        playlist_url
    };

    body.lines()
        .filter(|line| {
            let trimmed = line.trim();
            !trimmed.is_empty() && !trimmed.starts_with('#')
        })
        .map(|line| {
            let trimmed = line.trim();
            if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
                trimmed.to_string()
            } else {
                format!("{}{}", base_url, trimmed)
            }
        })
        .collect()
}
