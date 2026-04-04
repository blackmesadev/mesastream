pub mod cache;
pub mod codec;
pub mod effects;
pub mod encoder;
pub mod reader;

use std::{
    io::Read,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
        Arc,
    },
};

use futures_util::StreamExt;
use tokio::sync::mpsc;

use crate::{config::Settings, models::EqualizerSettings, source::ByteStream};

use codec::ChannelReader;

/// Cache file extension for opus frames.
const CACHE_EXT: &str = "opus";

/// Maximum opus frame size (20 ms at max 510 kbps ≈ 1,275 B; 1,500 gives headroom).
const MAX_OPUS_FRAME: usize = 1500;

/// Interleaved stereo samples per 20 ms frame at 48 kHz.
const PCM_FRAME_SAMPLES: usize = 1920;

/// Duration of one opus frame in milliseconds.
pub const OPUS_FRAME_MS: u64 = 20;

/// Encoder flushes to disk every N frames (first frame flushes immediately).
const ENCODER_FLUSH_INTERVAL: u64 = 25;

/// Shared counters between the encoder and cache reader.
///
/// The encoder increments `frames_written` after flushing to disk (Release),
/// and the reader loads it (Acquire) to know how many frames are safe to read.
pub struct EncoderProgress {
    pub frames_written: AtomicU64,
    pub encoding_done: AtomicBool,
    pub cancelled: AtomicBool,
    /// Wakes async readers when new frames are flushed or encoding completes.
    pub notify: tokio::sync::Notify,
}

impl EncoderProgress {
    pub fn new() -> Self {
        Self {
            frames_written: AtomicU64::new(0),
            encoding_done: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
        }
    }

    /// Pre-completed progress for a fully cached track.
    pub fn completed(total_frames: u64) -> Self {
        let p = Self::new();
        p.frames_written.store(total_frames, Ordering::Relaxed);
        p.encoding_done.store(true, Ordering::Relaxed);
        p
    }

    /// Mark encoding as done (success or failure) and wake any reader.
    pub fn mark_done(&self) {
        self.encoding_done.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// Publish new frame count and wake any async reader waiting for data.
    pub fn publish_frames(&self, count: u64) {
        self.frames_written.store(count, Ordering::Release);
        self.notify.notify_waiters();
    }
}

/// Atomic volume and EQ parameters readable from sync `spawn_blocking` context.
///
/// Updated by `Player::set_volume` / `set_equalizer` (async), consumed by
/// the cache reader (blocking). Uses `AtomicU32` bit-casts to avoid locks.
pub struct PlaybackEffects {
    volume: AtomicU32,
    bass: AtomicU32,
    mid: AtomicU32,
    treble: AtomicU32,
}

impl PlaybackEffects {
    pub fn new(volume: f32, eq: &EqualizerSettings) -> Self {
        Self {
            volume: AtomicU32::new(volume.to_bits()),
            bass: AtomicU32::new(eq.bass.to_bits()),
            mid: AtomicU32::new(eq.mid.to_bits()),
            treble: AtomicU32::new(eq.treble.to_bits()),
        }
    }

    pub fn set_volume(&self, v: f32) {
        self.volume.store(v.to_bits(), Ordering::Relaxed);
    }

    pub fn set_eq(&self, eq: &EqualizerSettings) {
        self.bass.store(eq.bass.to_bits(), Ordering::Relaxed);
        self.mid.store(eq.mid.to_bits(), Ordering::Relaxed);
        self.treble.store(eq.treble.to_bits(), Ordering::Relaxed);
    }

    pub fn volume(&self) -> f32 {
        f32::from_bits(self.volume.load(Ordering::Relaxed))
    }

    pub fn eq(&self) -> EqualizerSettings {
        EqualizerSettings {
            bass: f32::from_bits(self.bass.load(Ordering::Relaxed)),
            mid: f32::from_bits(self.mid.load(Ordering::Relaxed)),
            treble: f32::from_bits(self.treble.load(Ordering::Relaxed)),
        }
    }
}

/// Top-level audio pipeline orchestrator.
///
/// Provides the interface consumed by `PlayerManager`:
/// - `open_stream()` - bridge an async `ByteStream` into a sync reader
/// - `spawn_encoder()` - launch encoder task → cache file
/// - `cache_path()` - build cache file path for a cache key
/// - `bitrate_kbps()` - configured opus bitrate
#[derive(Clone)]
pub struct AudioPipeline {
    settings: Arc<Settings>,
}

impl AudioPipeline {
    pub fn new(settings: Arc<Settings>) -> Self {
        Self { settings }
    }

    /// Bridge an async `ByteStream` from a source into a synchronous reader
    /// for the encoder.
    ///
    /// Spawns a task that pulls chunks from the stream and feeds them through
    /// an mpsc channel to a `ChannelReader`. The encoder consumes the reader
    /// inside `spawn_blocking`.
    pub fn open_stream(&self, stream: ByteStream) -> Box<dyn Read + Send + Sync + 'static> {
        let (tx, rx) = mpsc::channel::<Result<bytes::Bytes, String>>(
            self.settings.source_stream_channel_capacity,
        );

        tokio::spawn(async move {
            let mut stream = stream;
            while let Some(result) = stream.next().await {
                match result {
                    Ok(chunk) => {
                        if tx.send(Ok(chunk)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        break;
                    }
                }
            }
        });

        Box::new(ChannelReader::new(rx))
    }

    /// Spawn an encoder that writes opus frames to a cache file.
    ///
    /// Returns `(progress, duration_rx)`.
    pub fn spawn_encoder(
        &self,
        reader: Box<dyn Read + Send + Sync + 'static>,
        cache_key: &str,
    ) -> (Arc<EncoderProgress>, tokio::sync::oneshot::Receiver<u64>) {
        let cache_path =
            PathBuf::from(&self.settings.audio_cache_path).join(format!("{cache_key}.{CACHE_EXT}"));
        let bitrate = self.settings.encoding_bitrate;
        encoder::spawn(
            reader,
            cache_path,
            bitrate,
            self.settings.cache_writer_buffer_bytes,
        )
    }

    /// The configured Opus bitrate (kbps) for effects processing.
    pub fn bitrate_kbps(&self) -> u32 {
        self.settings.encoding_bitrate
    }
}
