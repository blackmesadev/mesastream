//! Cache file I/O for the `[u16 LE size][opus data]` frame format.
//!
//! `CacheWriter` wraps a `BufWriter` and tracks flushed frame count via
//! `EncoderProgress` so the reader can safely follow the writer in real time.
//!
//! `CacheReader` wraps a `BufReader` and provides seeking by frame index
//! plus batched reads for the effects pipeline.

use std::io::{BufReader, BufWriter, Read, Seek, Write};
use std::path::Path;

use super::MAX_OPUS_FRAME;

/// Buffered writer for the cache frame format.
///
/// Each frame is stored as `[u16 LE length][opus bytes]`. The writer tracks
/// how many frames have been flushed to disk so `CacheReader` can follow
/// safely via `EncoderProgress`.
pub struct CacheWriter {
    writer: BufWriter<std::fs::File>,
    frames_written: u64,
    ok: bool,
}

impl CacheWriter {
    /// Create a new cache file at `path`, truncating if it exists.
    pub fn create(path: &Path, buffer_bytes: usize) -> std::io::Result<Self> {
        let file = std::fs::File::create(path)?;
        Ok(Self {
            writer: BufWriter::with_capacity(buffer_bytes.max(MAX_OPUS_FRAME * 8), file),
            frames_written: 0,
            ok: true,
        })
    }

    /// Write a single opus frame. Returns `false` on I/O error.
    pub fn write_frame(&mut self, data: &[u8]) -> bool {
        if !self.ok {
            return false;
        }
        let size = (data.len() as u16).to_le_bytes();
        self.ok = self.writer.write_all(&size).is_ok() && self.writer.write_all(data).is_ok();
        if self.ok {
            self.frames_written += 1;
        }
        self.ok
    }

    /// Flush buffered data to the OS.
    pub fn flush_to_disk(&mut self) {
        let _ = self.writer.flush();
    }

    /// Total frames written (whether or not they have been flushed).
    pub fn frames_written(&self) -> u64 {
        self.frames_written
    }

    /// Whether all writes so far succeeded.
    pub fn is_ok(&self) -> bool {
        self.ok
    }
}

/// Buffered reader for the cache frame format.
///
/// Tracks how many frames have been consumed so the playback loop knows its
/// position relative to the encoder's write head.
pub struct CacheReader {
    reader: BufReader<std::fs::File>,
    frames_read: u64,
}

impl CacheReader {
    /// Open an existing cache file. Returns `None` if the file doesn't exist.
    pub fn open(path: &Path, buffer_bytes: usize) -> Option<Self> {
        let file = std::fs::File::open(path).ok()?;
        Some(Self {
            reader: BufReader::with_capacity(buffer_bytes.max(MAX_OPUS_FRAME * 4), file),
            frames_read: 0,
        })
    }

    /// Seek to a specific frame index by scanning forward from the start.
    pub fn seek_to_frame(&mut self, target_frame: u64) {
        if self.reader.seek(std::io::SeekFrom::Start(0)).is_err() {
            return;
        }
        for _ in 0..target_frame {
            let mut len_buf = [0u8; 2];
            if self.reader.read_exact(&mut len_buf).is_err() {
                return;
            }
            let skip = u16::from_le_bytes(len_buf) as i64;
            if self.reader.seek(std::io::SeekFrom::Current(skip)).is_err() {
                return;
            }
        }
        self.frames_read = target_frame;
    }

    /// Read a single opus frame. Returns `None` on EOF or corrupt data.
    pub fn read_frame(&mut self) -> Option<Vec<u8>> {
        let frame = read_frame_from(&mut self.reader)?;
        self.frames_read += 1;
        Some(frame)
    }

    /// How many frames have been consumed so far.
    pub fn frames_read(&self) -> u64 {
        self.frames_read
    }
}

/// Read a single `[u16 LE size][data]` frame from any reader.
fn read_frame_from(reader: &mut impl Read) -> Option<Vec<u8>> {
    let mut len_buf = [0u8; 2];
    if reader.read_exact(&mut len_buf).is_err() {
        return None;
    }
    let len = u16::from_le_bytes(len_buf) as usize;
    if len == 0 || len > MAX_OPUS_FRAME {
        return None;
    }
    let mut frame = vec![0u8; len];
    if reader.read_exact(&mut frame).is_err() {
        return None;
    }
    Some(frame)
}
