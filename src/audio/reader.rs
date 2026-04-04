use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use tokio::time::{sleep, Duration};

use super::{
    cache::CacheReader, effects::EffectsProcessor, EncoderProgress, PlaybackEffects, OPUS_FRAME_MS,
};

/// Async-friendly cache reader for a single track.
///
/// Wraps `CacheReader` (sync file I/O - sub-microsecond for buffered local
/// reads) and `EncoderProgress` (for uncached tracks where the encoder is
/// still writing).
pub struct TrackReader {
    reader: CacheReader,
    progress: Arc<EncoderProgress>,
    effects: Option<EffectsProcessor>,
    shared_effects: Arc<PlaybackEffects>,
    bitrate_kbps: u32,
}

impl TrackReader {
    /// Wait for the cache file to appear on disk, then open it.
    ///
    /// Uses async sleep instead of blocking thread::sleep.
    /// Returns `None` if `cancel` fires before the file appears.
    pub async fn open(
        cache_path: &Path,
        progress: Arc<EncoderProgress>,
        shared_effects: Arc<PlaybackEffects>,
        cancel: &AtomicBool,
        bitrate_kbps: u32,
        cache_reader_buffer_bytes: usize,
    ) -> Option<Self> {
        // Wait for the file - polls every 20ms (one frame period) using async sleep.
        let reader = loop {
            if let Some(r) = CacheReader::open(cache_path, cache_reader_buffer_bytes) {
                break r;
            }
            if cancel.load(Ordering::Relaxed) {
                return None;
            }
            sleep(Duration::from_millis(20)).await;
        };

        Some(Self {
            reader,
            progress,
            effects: None,
            shared_effects,
            bitrate_kbps,
        })
    }

    /// Seek to a position in milliseconds.
    pub fn seek_to_ms(&mut self, ms: u64) {
        let target_frame = ms / OPUS_FRAME_MS;
        self.reader.seek_to_frame(target_frame);
        if let Some(ref mut p) = self.effects {
            p.reset_filters();
        }
    }

    /// Try to read the next opus frame, applying effects if volume/EQ is active.
    ///
    /// Returns:
    /// - `Some(frame)` - processed frame ready to send
    /// - `None` - no frame available right now (caller should wait on progress.notify)
    ///
    /// The caller must check `progress.encoding_done` to distinguish
    /// "no frame yet" from "track finished".
    pub fn next_frame(&mut self) -> Option<Vec<u8>> {
        let available = self.progress.frames_written.load(Ordering::Acquire);
        if self.reader.frames_read() >= available {
            return None; // caught up - caller decides if EOF or wait
        }

        let raw = self.reader.read_frame()?;

        // Hot path: when volume is 1.0 and EQ is flat, pass raw opus through.
        let volume = self.shared_effects.volume();
        let eq = self.shared_effects.eq();
        let needs_effects = (volume - 1.0).abs() >= 0.01 || !eq.is_flat();

        if needs_effects {
            let proc = self.effects.get_or_insert_with(|| {
                EffectsProcessor::new(self.bitrate_kbps).expect("EffectsProcessor init failed")
            });
            Some(proc.process_frame(&raw, volume, &eq))
        } else {
            Some(raw)
        }
    }

    /// Returns `true` when the encoder has finished and all frames are consumed.
    pub fn is_eof(&self) -> bool {
        self.progress.encoding_done.load(Ordering::Acquire)
            && self.reader.frames_read() >= self.progress.frames_written.load(Ordering::Acquire)
    }
}
