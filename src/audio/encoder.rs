//! Encoder task: source → decode → resample → Opus encode → cache file.
//!
//! Runs inside `spawn_blocking` at full speed (bounded only by source I/O +
//! CPU). Progress is published via `EncoderProgress` so the cache reader can
//! follow the write head in real time.

use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

use tracing::{debug, error};

use super::{
    cache::CacheWriter,
    codec::{probe_source, transcode_loop, OpusCodec},
    EncoderProgress, ENCODER_FLUSH_INTERVAL, OPUS_FRAME_MS,
};

/// Spawn a blocking encoder task.
///
/// Writes opus frames to `cache_path` in the `[u16 LE size][data]` format.
/// Returns `(progress, duration_rx)`:
///   - `progress` — shared counters the cache reader polls
///   - `duration_rx` — fires with `duration_ms` when encoding completes
pub fn spawn(
    reader: Box<dyn Read + Send + Sync + 'static>,
    cache_path: PathBuf,
    bitrate_kbps: u32,
    cache_writer_buffer_bytes: usize,
) -> (Arc<EncoderProgress>, tokio::sync::oneshot::Receiver<u64>) {
    let progress = Arc::new(EncoderProgress::new());
    let progress_ref = progress.clone();
    let (duration_tx, duration_rx) = tokio::sync::oneshot::channel::<u64>();

    tokio::task::spawn_blocking(move || {
        run(
            reader,
            &cache_path,
            bitrate_kbps,
            cache_writer_buffer_bytes,
            &progress_ref,
            duration_tx,
        );
    });

    (progress, duration_rx)
}

/// Blocking encoder body — runs inside `spawn_blocking`.
fn run(
    reader: Box<dyn Read + Send + Sync + 'static>,
    cache_path: &PathBuf,
    bitrate_kbps: u32,
    cache_writer_buffer_bytes: usize,
    progress: &EncoderProgress,
    duration_tx: tokio::sync::oneshot::Sender<u64>,
) {
    if let Some(parent) = cache_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    let mut writer = match CacheWriter::create(cache_path, cache_writer_buffer_bytes) {
        Ok(w) => w,
        Err(e) => {
            error!(error = %e, "cache file create failed");
            progress.mark_done();
            return;
        }
    };

    let (mut format, track) = match probe_source(reader) {
        Ok(r) => r,
        Err(e) => {
            error!(error = %e, "probe failed");
            let _ = std::fs::remove_file(cache_path);
            let _ = duration_tx.send(0);
            progress.mark_done();
            return;
        }
    };

    let mut decoder = match symphonia::default::get_codecs().make(
        &track.codec_params,
        &symphonia::core::codecs::DecoderOptions::default(),
    ) {
        Ok(d) => d,
        Err(e) => {
            error!(error = ?e, "decoder init failed");
            let _ = std::fs::remove_file(cache_path);
            let _ = duration_tx.send(0);
            progress.mark_done();
            return;
        }
    };

    let mut codec = match OpusCodec::new(bitrate_kbps) {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "opus init failed");
            let _ = std::fs::remove_file(cache_path);
            let _ = duration_tx.send(0);
            progress.mark_done();
            return;
        }
    };

    let cancel = &progress.cancelled;

    let (total_frames, _total_bytes) = transcode_loop(
        &mut format,
        track.id,
        &mut decoder,
        &mut codec,
        cancel,
        |_| {}, // no PCM processing during encoding
        |frame| {
            let ok = writer.write_frame(frame);
            if ok {
                let count = writer.frames_written();
                if count == 1 || count % ENCODER_FLUSH_INTERVAL == 0 {
                    writer.flush_to_disk();
                    progress.publish_frames(count);
                }
            }
            ok
        },
    );

    let duration_ms = total_frames * OPUS_FRAME_MS;

    // Finalise: flush on success, remove on failure.
    if !cancel.load(std::sync::atomic::Ordering::Relaxed) && writer.is_ok() {
        writer.flush_to_disk();
        progress.publish_frames(total_frames);
        debug!(duration_ms, total_frames, "cache finalized");
    } else {
        writer.flush_to_disk();
        let _ = std::fs::remove_file(cache_path);
    }

    progress.mark_done();
    let _ = duration_tx.send(duration_ms);
}
