//! yt-dlp process management: metadata fetching and async audio streaming.
//!
//! All process I/O is fully async via `tokio::process`. The `spawn_audio_stream`
//! function returns a `ByteStream` that delivers raw audio data as yt-dlp
//! outputs it - no intermediate files, no sync blocking.

use std::{collections::HashMap, process::Stdio};

use bytes::Bytes;
use futures_util::stream::unfold;
use serde::de::DeserializeOwned;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::Command,
    sync::mpsc,
};
use tracing::{debug, error, warn};

use crate::errors::{AppError, AppResult};
use crate::source::ByteStream;

const YT_EXTRACTOR_ARGS_ANDROID: &str = "youtube:player_client=android";

/// Size of each read from yt-dlp stdout (32 KiB - matches typical TCP window).
const STDOUT_READ_BUF: usize = 32_768;

/// Channel capacity for stdout → stream bridge.
const STREAM_CHANNEL_CAP: usize = 16;

fn trim_for_log(input: &str, max_chars: usize) -> &str {
    if input.len() <= max_chars {
        input
    } else {
        &input[..max_chars]
    }
}

#[tracing::instrument(skip(yt_dlp_path, url, cookies_path))]
pub async fn fetch_and_parse_metadata<T: DeserializeOwned>(
    yt_dlp_path: &str,
    url: &str,
    source: &'static str,
    extra_args: &[&str],
    cookies_path: Option<&str>,
) -> AppResult<T> {
    // Defensive URL validation: reject clearly invalid inputs
    if url.is_empty() || url == "false" || url == "true" || !url.starts_with("http") {
        return Err(AppError::SourceUnavailable(format!(
            "rejected invalid URL before yt-dlp: {}",
            url
        )));
    }

    let fetch_start = std::time::Instant::now();

    let mut command = Command::new(yt_dlp_path);
    command
        .arg("--ignore-config")
        .arg("--quiet")
        .arg("--no-warnings")
        .arg("--no-playlist")
        .arg("--skip-download")
        .arg("--socket-timeout")
        .arg("15")
        .args(extra_args)
        .arg("--print-json")
        .arg(url)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if let Some(cookies) = cookies_path {
        command.arg("--cookies").arg(cookies);
    }

    let output = command.output().await.map_err(|error| {
        AppError::SourceUnavailable(format!("yt-dlp invocation failed: {error}"))
    })?;

    let fetch_ms = fetch_start.elapsed().as_millis();
    debug!(
        source,
        fetch_ms,
        status_code = output.status.code(),
        stdout_bytes = output.stdout.len(),
        stderr_bytes = output.stderr.len(),
        "yt-dlp metadata fetch complete"
    );

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stderr_str = stderr.trim();

        // Detect HTTP 4xx errors and return early
        if stderr_str.contains("HTTP Error 4") {
            let status = if stderr_str.contains("403") {
                "403 Forbidden"
            } else if stderr_str.contains("404") {
                "404 Not Found"
            } else if stderr_str.contains("400") {
                "400 Bad Request"
            } else {
                "4xx Client Error"
            };
            error!(
                source,
                status_code = output.status.code(),
                http_error = status,
                stderr = %trim_for_log(stderr_str, 500),
                "yt-dlp received HTTP 4xx error - content unavailable or rate-limited"
            );
            return Err(AppError::SourceUnavailable(format!(
                "{source} returned HTTP {status} (likely rate-limited or blocked)"
            )));
        }

        if stderr_str.contains("Requested format is not available")
            || stderr_str.contains("Only images are available for download")
        {
            return Err(AppError::SourceUnavailable(format!(
                "{source} did not expose a playable audio format"
            )));
        }

        error!(
            source,
            status_code = output.status.code(),
            stderr = %trim_for_log(stderr_str, 800),
            "yt-dlp metadata fetch failed"
        );
        return Err(AppError::SourceUnavailable(format!(
            "{source} media is unavailable"
        )));
    }

    let parse_start = std::time::Instant::now();
    let metadata: T = serde_json::from_slice(&output.stdout)
        .map_err(|error| AppError::MetadataParse(format!("invalid yt-dlp json: {error}")))?;

    debug!(
        source,
        parse_ms = parse_start.elapsed().as_millis(),
        json_bytes = output.stdout.len(),
        "parsed yt-dlp JSON"
    );

    Ok(metadata)
}

/// Spawn yt-dlp for audio streaming. Returns an async `ByteStream` that
/// delivers raw audio data as yt-dlp outputs it.
///
/// The child process is managed internally - it is killed when the stream
/// is dropped (sender side closes, the stdout task cleans up).
#[tracing::instrument(skip(yt_dlp_path, source_url, cookies_path))]
pub async fn spawn_audio_stream(
    yt_dlp_path: &str,
    source_url: &str,
    cookies_path: Option<&str>,
) -> AppResult<ByteStream> {
    if source_url.is_empty()
        || source_url == "false"
        || source_url == "true"
        || !source_url.starts_with("http")
    {
        return Err(AppError::SourceUnavailable(format!(
            "rejected invalid URL before yt-dlp: {}",
            source_url
        )));
    }

    debug!(
        yt_dlp_path,
        source_url, "spawning async yt-dlp audio stream"
    );

    let mut command = Command::new(yt_dlp_path);
    command
        .arg("--ignore-config")
        .arg("--no-playlist")
        .arg("--extractor-args")
        .arg(YT_EXTRACTOR_ARGS_ANDROID)
        .arg("--format")
        .arg("bestaudio[acodec=opus]/bestaudio[ext=webm]/bestaudio/best")
        .arg("--socket-timeout")
        .arg("15")
        .arg("-o")
        .arg("-")
        .arg(source_url)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("PYTHONUNBUFFERED", "1");

    if let Some(cookies) = cookies_path {
        command.arg("--cookies").arg(cookies);
    }

    let mut child = command.spawn().map_err(|error| {
        error!(
            yt_dlp_path,
            source_url,
            error = %error,
            error_kind = ?error.kind(),
            "yt-dlp process spawn failed"
        );
        AppError::SourceUnavailable(format!("yt-dlp spawn failed: {error}"))
    })?;

    let pid = child.id();
    debug!(pid, "yt-dlp process spawned");

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| AppError::SourceUnavailable("yt-dlp stdout not available".to_string()))?;

    // Drain stderr in a background async task (noise-reduced logging).
    if let Some(stderr) = child.stderr.take() {
        tokio::spawn(drain_stderr(stderr));
    }

    // Bridge: read stdout chunks → mpsc channel → ByteStream.
    // The task owns `child` so the process is killed when the stream ends.
    let (tx, rx) = mpsc::channel::<Result<Bytes, String>>(STREAM_CHANNEL_CAP);

    tokio::spawn(async move {
        let mut stdout = stdout;
        let mut buf = vec![0u8; STDOUT_READ_BUF];
        loop {
            match stdout.read(&mut buf).await {
                Ok(0) => break, // EOF
                Ok(n) => {
                    if tx
                        .send(Ok(Bytes::copy_from_slice(&buf[..n])))
                        .await
                        .is_err()
                    {
                        break; // receiver dropped
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e.to_string())).await;
                    break;
                }
            }
        }
        // Ensure child is cleaned up when the stream ends
        let _ = child.kill().await;
        let _ = child.wait().await;
    });

    // Wrap mpsc receiver as a futures Stream
    let stream = unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });

    Ok(Box::pin(stream))
}

/// Drain yt-dlp stderr asynchronously with noise reduction.
async fn drain_stderr(stderr: tokio::process::ChildStderr) {
    let reader = BufReader::new(stderr);
    let mut lines = reader.lines();
    let mut noisy_counts: HashMap<&'static str, u64> = HashMap::new();
    let mut http_4xx_count = 0u64;
    let mut line_count = 0u64;

    while let Ok(Some(line)) = lines.next_line().await {
        line_count += 1;
        let trimmed = line.trim().to_string();
        if trimmed.is_empty() {
            continue;
        }

        if line_count <= 5 {
            debug!(source = "yt-dlp", line_num = line_count, "{trimmed}");
        }

        if trimmed.contains("HTTP Error 4") {
            http_4xx_count += 1;
            if http_4xx_count <= 2 {
                warn!(source = "yt-dlp", "{trimmed}");
            }
            continue;
        }

        if trimmed.to_uppercase().starts_with("ERROR") {
            error!(source = "yt-dlp", "{trimmed}");
            continue;
        }

        let noisy_key = if trimmed.starts_with("[hls @") {
            Some("hls_open")
        } else if trimmed.starts_with("[download]") {
            Some("download_progress")
        } else if trimmed.starts_with("Input #")
            || trimmed.starts_with("Output #")
            || trimmed.starts_with("Stream mapping")
            || trimmed.starts_with("Press [q]")
            || trimmed.starts_with("av_interleaved_write_frame")
            || trimmed.starts_with("Error writing trailer")
            || trimmed.starts_with("Error closing file")
            || trimmed.starts_with("WARNING: [youtube]")
            || (trimmed.starts_with("[youtube]")
                && (trimmed.contains("Downloading") || trimmed.contains("Extracting")))
        {
            Some("ffmpeg_noise")
        } else {
            None
        };

        if let Some(key) = noisy_key {
            *noisy_counts.entry(key).or_insert(0) += 1;
            continue;
        }

        warn!(source = "yt-dlp", "{trimmed}");
    }

    if http_4xx_count > 2 {
        warn!(
            source = "yt-dlp",
            count = http_4xx_count,
            "suppressed additional HTTP 4xx errors"
        );
    }

    for (key, count) in noisy_counts {
        if count > 0 {
            warn!(
                source = "yt-dlp",
                category = key,
                count,
                "suppressed repetitive stderr lines"
            );
        }
    }

    debug!(
        source = "yt-dlp",
        total_stderr_lines = line_count,
        "stderr processing complete"
    );
}
