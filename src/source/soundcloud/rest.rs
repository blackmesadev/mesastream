use std::sync::Arc;

use reqwest::{Client, Response, StatusCode};
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::instrument;

use crate::errors::{AppError, AppResult};

use super::model::{AudioResponse, Track, Transcoding};
use super::SoundcloudClient;

const MAX_RETRIES: u32 = 3;
const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(500);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(5);

impl SoundcloudClient {
    /// Creates a new SoundCloud client instance
    pub fn new() -> Self {
        Self {
            http_client: Client::new(),
            client_id: Arc::new(RwLock::new(None)),
        }
    }

    /// Returns a cached client_id, or fetches a fresh one if none is cached.
    #[instrument(skip(self))]
    pub async fn get_client_id(&self) -> AppResult<String> {
        // Fast path: read lock
        {
            let guard = self.client_id.read().await;
            if let Some(id) = guard.as_ref() {
                return Ok(id.clone());
            }
        }

        // Slow path: write lock + fetch
        self.refresh_client_id().await
    }

    /// Forces a fresh client_id extraction and stores it.
    #[instrument(skip(self))]
    pub async fn refresh_client_id(&self) -> AppResult<String> {
        let id = self.extract_client_id().await?;
        let mut guard = self.client_id.write().await;
        *guard = Some(id.clone());
        tracing::info!("SoundCloud client_id refreshed");
        Ok(id)
    }

    /// Invalidates the cached client_id so the next call re-fetches it.
    pub async fn invalidate_client_id(&self) {
        let mut guard = self.client_id.write().await;
        *guard = None;
        tracing::info!("SoundCloud client_id invalidated");
    }

    #[instrument(skip(self, req))]
    async fn make_request(&self, req: reqwest::RequestBuilder) -> AppResult<Response> {
        let mut retries = 0;
        let mut delay = INITIAL_RETRY_DELAY;

        loop {
            let cloned_req = req.try_clone().ok_or_else(|| {
                AppError::Internal("request body cannot be cloned for retry".to_string())
            })?;

            match cloned_req.send().await {
                Ok(resp) => match resp.status() {
                    StatusCode::TOO_MANY_REQUESTS => {
                        if retries >= MAX_RETRIES {
                            return Err(AppError::ServiceUnavailable(
                                "soundcloud rate limited".to_string(),
                            ));
                        }

                        tracing::warn!("Rate limited, waiting {:?} before retry", delay);
                        sleep(delay).await;

                        delay = std::cmp::min(delay * 2, MAX_RETRY_DELAY);
                        retries += 1;
                        continue;
                    }
                    _ => return Ok(resp),
                },
                Err(e) => {
                    return Err(AppError::ServiceUnavailable(format!(
                        "soundcloud request failed: {e}"
                    )));
                }
            }
        }
    }

    #[instrument(skip(self), fields(url = %url))]
    pub async fn track_from_url(&self, url: &str) -> AppResult<Track> {
        let resp = self
            .make_request(self.http_client.get(url))
            .await?
            .text()
            .await
            .map_err(|error| {
                AppError::SourceUnavailable(format!("soundcloud page read failed: {error}"))
            })?;

        let hydration_data = resp
            .split("window.__sc_hydration = ")
            .nth(1)
            .and_then(|s| s.split(";</script>").next())
            .ok_or_else(|| AppError::MetadataParse("could not find hydration data".to_string()))?;

        let hydration: serde_json::Value =
            serde_json::from_str(hydration_data).map_err(|error| {
                AppError::MetadataParse(format!("invalid soundcloud hydration json: {error}"))
            })?;

        if let Some(track_data) = hydration
            .as_array()
            .and_then(|arr| arr.iter().find(|item| item["hydratable"] == "sound"))
            .and_then(|item| item.get("data"))
        {
            serde_json::from_value(track_data.clone()).map_err(|error| {
                AppError::MetadataParse(format!("invalid soundcloud track payload: {error}"))
            })
        } else {
            Err(AppError::MetadataParse(
                "could not find track data".to_string(),
            ))
        }
    }

    fn pick_best_transcoding<'t>(&self, track: &'t Track) -> Option<&'t Transcoding> {
        let transcoding = track
            .media
            .transcodings
            .iter()
            .find(|t| t.format.protocol == "progressive" && t.quality == "hq")
            .or_else(|| {
                track
                    .media
                    .transcodings
                    .iter()
                    .find(|t| t.format.protocol == "hls" && t.quality == "hq")
            })
            .or_else(|| {
                track
                    .media
                    .transcodings
                    .iter()
                    .find(|t| t.format.protocol == "progressive" && t.quality == "sq")
            })
            .or_else(|| {
                track
                    .media
                    .transcodings
                    .iter()
                    .find(|t| t.format.protocol == "hls" && t.quality == "sq")
            });

        transcoding
    }

    #[instrument(skip(self))]
    async fn extract_client_id(&self) -> AppResult<String> {
        let resp = self
            .make_request(self.http_client.get("https://soundcloud.com"))
            .await?
            .text()
            .await
            .map_err(|error| {
                AppError::SourceUnavailable(format!("soundcloud homepage fetch failed: {error}"))
            })?;

        // Find script URLs from the page
        for line in resp.lines() {
            if line.contains("<script") && line.contains("src=") {
                if let Some(start) = line.find("src=\"") {
                    if let Some(end) = line[start + 5..].find('"') {
                        let script_url = &line[start + 5..start + 5 + end];

                        // Only fetch crossorigin scripts (where client_id is typically found)
                        if !line.contains("crossorigin") {
                            continue;
                        }

                        // Make the URL absolute if it's relative
                        let full_url = if script_url.starts_with("http") {
                            script_url.to_string()
                        } else if script_url.starts_with("//") {
                            format!("https:{}", script_url)
                        } else {
                            format!("https://soundcloud.com{}", script_url)
                        };

                        // Fetch the script and look for client_id
                        if let Ok(script_resp) = self.http_client.get(&full_url).send().await {
                            if let Ok(script) = script_resp.text().await {
                                // Look for client_id patterns with alphanumeric validation
                                for pattern in
                                    &["client_id:\"", "client_id=\"", "clientId:\"", "clientId=\""]
                                {
                                    if let Some(pos) = script.find(pattern) {
                                        let rest = &script[pos + pattern.len()..];
                                        if let Some(end) = rest.find('"') {
                                            let candidate = &rest[..end];
                                            // Validate: alphanumeric, 20-40 chars (typical SoundCloud client_id length)
                                            if candidate.len() >= 20
                                                && candidate.len() <= 40
                                                && candidate.chars().all(|c| c.is_alphanumeric())
                                            {
                                                tracing::debug!(
                                                    "Extracted SoundCloud client_id: {}",
                                                    candidate
                                                );
                                                return Ok(candidate.to_string());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Err(AppError::SourceUnavailable(
            "could not extract soundcloud client_id".to_string(),
        ))
    }

    /// Attempts to fetch the stream URL using the given `client_id`.
    async fn try_resolve_stream(
        &self,
        transcoding: &Transcoding,
        client_id: &str,
    ) -> Result<String, AppError> {
        let url = if transcoding.url.contains('?') {
            format!("{}&client_id={}", transcoding.url, client_id)
        } else {
            format!("{}?client_id={}", transcoding.url, client_id)
        };

        let resp = self.make_request(self.http_client.get(&url)).await?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp
                .text()
                .await
                .unwrap_or_else(|_| "<could not read body>".to_string());

            // Surface auth errors distinctly so the caller can retry with a fresh client_id
            if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
                tracing::warn!(
                    status = %status,
                    "SoundCloud auth error - client_id may be stale"
                );
                return Err(AppError::SourceUnavailable(format!(
                    "soundcloud auth error ({}): {}",
                    status, body
                )));
            }

            tracing::error!(
                "SoundCloud stream API error: status={} body={}",
                status,
                body
            );
            return Err(AppError::SourceUnavailable(format!(
                "soundcloud stream API returned {}: {}",
                status, body
            )));
        }

        let body_text = resp.text().await.map_err(|error| {
            AppError::SourceUnavailable(format!("could not read stream response body: {error}"))
        })?;

        tracing::debug!("SoundCloud stream response: {}", body_text);

        let audio_resp: AudioResponse = serde_json::from_str(&body_text).map_err(|error| {
            tracing::error!(
                "Failed to parse SoundCloud stream response: {} - body: {}",
                error,
                body_text
            );
            AppError::SourceUnavailable(format!("invalid soundcloud stream response: {error}"))
        })?;

        Ok(audio_resp.url)
    }

    /// Resolve the best stream URL and its transcoding metadata for a track.
    ///
    /// Returns `(stream_url, transcoding)` so the caller knows whether
    /// this is a progressive download or an HLS playlist.
    #[instrument(skip(self, track), fields(track_id = track.id))]
    pub async fn resolve_stream_for_track(
        &self,
        track: &Track,
    ) -> AppResult<(String, Transcoding)> {
        let transcoding = self.pick_best_transcoding(track).cloned().ok_or_else(|| {
            AppError::SourceUnavailable("no suitable soundcloud transcoding found".to_string())
        })?;

        // Use cached client_id (fetches on first call)
        let client_id = self.get_client_id().await?;

        match self.try_resolve_stream(&transcoding, &client_id).await {
            Ok(url) => Ok((url, transcoding)),
            Err(_) => {
                // Auth/request failure - invalidate cached id, fetch a fresh one, retry once
                tracing::info!("Retrying SoundCloud stream with refreshed client_id");
                self.invalidate_client_id().await;
                let new_client_id = self.refresh_client_id().await?;
                let url = self
                    .try_resolve_stream(&transcoding, &new_client_id)
                    .await?;
                Ok((url, transcoding))
            }
        }
    }
}
