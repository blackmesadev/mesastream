use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};

use symphonia::core::{
    audio::SampleBuffer,
    codecs::CODEC_TYPE_NULL,
    errors::Error as SymphoniaError,
    formats::FormatOptions,
    io::{MediaSource, MediaSourceStream},
    meta::MetadataOptions,
    probe::Hint,
};
use symphonia::default::get_probe;
use tokio::sync::mpsc;
use tracing::{debug, error};

use crate::errors::{AppError, AppResult};

use super::{MAX_OPUS_FRAME, PCM_FRAME_SAMPLES};

/// Reusable Opus encoder/decoder pair.
///
/// The decoder is optional - only allocated when effects processing is needed.
/// Entire pipeline uses f32; Opus `encode_float` / `decode_float` handle
/// quantisation internally.
pub struct OpusCodec {
    decoder: Option<opus::Decoder>,
    encoder: opus::Encoder,
    /// Single-frame PCM scratch (960 stereo samples = 20 ms).
    pcm: Vec<f32>,
    /// Encoded output scratch.
    output: Vec<u8>,
}

impl OpusCodec {
    /// Construct an encoder+decoder pair at the given bitrate.
    pub fn new(bitrate_kbps: u32) -> AppResult<Self> {
        let decoder = opus::Decoder::new(48_000, opus::Channels::Stereo)
            .map_err(|e| AppError::Internal(format!("opus decoder init: {e}")))?;
        let mut encoder =
            opus::Encoder::new(48_000, opus::Channels::Stereo, opus::Application::Audio)
                .map_err(|e| AppError::Internal(format!("opus encoder init: {e}")))?;
        encoder
            .set_bitrate(opus::Bitrate::Bits((bitrate_kbps * 1000) as i32))
            .map_err(|e| AppError::Internal(format!("opus bitrate: {e}")))?;

        Ok(Self {
            decoder: Some(decoder),
            encoder,
            pcm: vec![0.0f32; PCM_FRAME_SAMPLES],
            output: vec![0u8; MAX_OPUS_FRAME],
        })
    }

    /// Encode f32 PCM samples → Opus frame. Returns `(len, data)`.
    pub fn encode(&mut self, pcm: &[f32]) -> Result<(usize, &[u8]), opus::Error> {
        let len = self.encoder.encode_float(pcm, &mut self.output)?;
        Ok((len, &self.output[..len]))
    }

    /// Decode into the internal scratch buffer and return a slice.
    /// Avoids a double-borrow by accessing decoder and pcm in separate borrows.
    pub fn decode_to_scratch(&mut self, opus_data: &[u8]) -> Result<&[f32], opus::Error> {
        let dec = self
            .decoder
            .as_mut()
            .expect("OpusCodec::decode_to_scratch called without a decoder");
        let samples = dec.decode_float(opus_data, &mut self.pcm, false)?;
        Ok(&self.pcm[..samples * 2])
    }
}

/// Probe a media source → format reader + track metadata.
pub fn probe_source(
    reader: Box<dyn Read + Send + Sync + 'static>,
) -> Result<
    (
        Box<dyn symphonia::core::formats::FormatReader>,
        symphonia::core::formats::Track,
    ),
    String,
> {
    let mss = MediaSourceStream::new(Box::new(PipeMediaSource::new(reader)), Default::default());
    let probed = get_probe()
        .format(
            &Hint::new(),
            mss,
            &FormatOptions::default(),
            &MetadataOptions::default(),
        )
        .map_err(|e| format!("{e:?}"))?;

    let format = probed.format;
    let track = format
        .tracks()
        .iter()
        .find(|t| t.codec_params.codec != CODEC_TYPE_NULL)
        .cloned()
        .ok_or_else(|| "no audio track".to_string())?;

    debug!(codec = ?track.codec_params.codec, "probed source");
    Ok((format, track))
}

/// Packet → decode → resample(f32) → per-frame callback → Opus encode → emit.
///
/// The entire pipeline stays f32. For 48 kHz stereo sources (YouTube) frames
/// are encoded directly from the decoder's output slice - zero extra
/// allocation. Non-48 kHz sources (e.g. SoundCloud 44.1 kHz) are linearly
/// resampled into a scratch buffer first.
///
/// Returns `(total_frames, total_bytes)`.
pub fn transcode_loop(
    format: &mut Box<dyn symphonia::core::formats::FormatReader>,
    track_id: u32,
    decoder: &mut Box<dyn symphonia::core::codecs::Decoder>,
    codec: &mut OpusCodec,
    cancel: &AtomicBool,
    mut process_pcm: impl FnMut(&mut [f32]),
    mut emit: impl FnMut(&[u8]) -> bool,
) -> (u64, u64) {
    let mut remainder: Vec<f32> = Vec::with_capacity(PCM_FRAME_SAMPLES);
    let mut frame_scratch = vec![0.0f32; PCM_FRAME_SAMPLES];
    let mut resample_buf: Vec<f32> = Vec::new();
    let mut sample_buf: Option<SampleBuffer<f32>> = None;
    let mut packet_errors = 0u32;
    let (mut frames, mut bytes) = (0u64, 0u64);
    // Cached once from the first successfully decoded packet.
    // `is_native` = true means 48 kHz stereo -  no resample needed.
    let mut stream_fmt: Option<(u32, usize, bool)> = None; // (rate, channels, is_native)

    loop {
        if cancel.load(Ordering::Relaxed) {
            debug!("encode cancelled");
            return (frames, bytes);
        }

        let packet = match format.next_packet() {
            Ok(p) => {
                packet_errors = 0;
                p
            }
            Err(SymphoniaError::ResetRequired) => continue,
            Err(SymphoniaError::IoError(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                debug!(frames, bytes, "decode complete");
                break;
            }
            Err(SymphoniaError::IoError(e)) if e.kind() == std::io::ErrorKind::WouldBlock => {
                continue;
            }
            Err(e) => {
                packet_errors += 1;
                if packet_errors % 100 == 0 {
                    debug!(error = ?e, packet_errors, "packet errors");
                }
                continue;
            }
        };

        if packet.track_id() != track_id {
            continue;
        }

        let decoded = match decoder.decode(&packet) {
            Ok(buf) => buf,
            Err(_) => continue,
        };

        // Cache stream format on first decoded packet; log it once.
        let &mut (rate, channels, is_native) = stream_fmt.get_or_insert_with(|| {
            let rate = decoded.spec().rate;
            let channels = decoded.spec().channels.count();
            debug!(sample_rate = rate, channels, "decoded format");
            (rate, channels, rate == 48_000 && channels == 2)
        });

        // Grow the sample buffer if a larger packet arrives (rare).
        let buf = sample_buf.get_or_insert_with(|| {
            SampleBuffer::<f32>::new(decoded.capacity() as u64, *decoded.spec())
        });
        if (buf.capacity() as usize) < decoded.capacity() {
            *buf = SampleBuffer::<f32>::new(decoded.capacity() as u64, *decoded.spec());
        }
        buf.copy_interleaved_ref(decoded);

        let src: &[f32] = if is_native {
            buf.samples()
        } else {
            resample_buf.clear();
            resample_to_48k_stereo(buf.samples(), channels, rate, &mut resample_buf);
            &resample_buf
        };

        let mut pos = 0usize;

        // Complete partial frame from previous packet.
        if !remainder.is_empty() {
            let need = PCM_FRAME_SAMPLES - remainder.len();
            if src.len() < need {
                remainder.extend_from_slice(src);
                continue;
            }
            remainder.extend_from_slice(&src[..need]);
            pos = need;
            process_pcm(&mut remainder);
            match codec.encode(&remainder) {
                Ok((len, data)) => {
                    frames += 1;
                    bytes += len as u64;
                    if !emit(data) {
                        return (frames, bytes);
                    }
                }
                Err(e) => {
                    error!(error = %e, "opus encode failed");
                    return (frames, bytes);
                }
            }
            remainder.clear();
        }

        // Encode full frames.
        while src.len() - pos >= PCM_FRAME_SAMPLES {
            frame_scratch.copy_from_slice(&src[pos..pos + PCM_FRAME_SAMPLES]);
            process_pcm(&mut frame_scratch);
            match codec.encode(&frame_scratch) {
                Ok((len, data)) => {
                    frames += 1;
                    bytes += len as u64;
                    pos += PCM_FRAME_SAMPLES;
                    if !emit(data) {
                        return (frames, bytes);
                    }
                }
                Err(e) => {
                    error!(error = %e, "opus encode failed");
                    return (frames, bytes);
                }
            }
        }

        // Stash leftover samples.
        remainder.extend_from_slice(&src[pos..]);
    }

    (frames, bytes)
}

/// Resample arbitrary-rate, arbitrary-channel PCM to 48 kHz interleaved stereo.
///
/// Fast paths exist for 48 kHz stereo (copy) and 48 kHz mono (duplicate).
/// Everything else uses linear interpolation.
pub fn resample_to_48k_stereo(input: &[f32], in_channels: usize, in_rate: u32, out: &mut Vec<f32>) {
    if input.is_empty() {
        return;
    }
    let channels = in_channels.max(1);
    let in_frames = input.len() / channels;
    if in_frames == 0 {
        return;
    }
    if in_rate == 48_000 && channels == 2 {
        out.extend_from_slice(input);
        return;
    }
    if in_rate == 48_000 && channels == 1 {
        out.reserve(in_frames * 2);
        for &s in input {
            out.push(s);
            out.push(s);
        }
        return;
    }

    let out_frames = ((in_frames as u64) * 48_000u64 / (in_rate.max(1) as u64)).max(1) as usize;
    let src_ratio = (in_rate.max(1) as f64) / 48_000.0;

    out.reserve(out_frames * 2);
    for out_idx in 0..out_frames {
        let src_pos = (out_idx as f64) * src_ratio;
        let i0 = src_pos.floor() as usize;
        let i1 = (i0 + 1).min(in_frames - 1);
        let frac = (src_pos - (i0 as f64)) as f32;

        let l0 = sample_channel(input, channels, i0, 0);
        let l1 = sample_channel(input, channels, i1, 0);
        let r0 = sample_channel(input, channels, i0, 1);
        let r1 = sample_channel(input, channels, i1, 1);

        out.push(l0 + (l1 - l0) * frac);
        out.push(r0 + (r1 - r0) * frac);
    }
}

fn sample_channel(input: &[f32], channels: usize, frame_idx: usize, channel: usize) -> f32 {
    match channels {
        1 => input[frame_idx],
        c if channel < c => input[frame_idx * channels + channel],
        _ => input[frame_idx * channels],
    }
}

/// Synchronous reader that pulls bytes from a tokio mpsc channel.
///
/// Uses `blocking_recv()` inside `spawn_blocking` so it doesn't starve the
/// Tokio runtime. The async source stream task feeds chunks into the channel.
pub struct ChannelReader {
    rx: mpsc::Receiver<Result<bytes::Bytes, String>>,
    buffer: bytes::Bytes,
}

impl ChannelReader {
    pub fn new(rx: mpsc::Receiver<Result<bytes::Bytes, String>>) -> Self {
        Self {
            rx,
            buffer: bytes::Bytes::new(),
        }
    }
}

impl Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if !self.buffer.is_empty() {
                let n = buf.len().min(self.buffer.len());
                buf[..n].copy_from_slice(&self.buffer[..n]);
                self.buffer = self.buffer.slice(n..);
                return Ok(n);
            }
            match self.rx.blocking_recv() {
                Some(Ok(chunk)) => self.buffer = chunk,
                Some(Err(e)) => {
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, e));
                }
                None => return Ok(0),
            }
        }
    }
}

/// MediaSource wrapper for non-seekable streams (wraps any `Read`).
///
/// Provides the `symphonia::core::io::MediaSource` trait by adding a
/// non-functional `Seek` implementation. Used to feed the Symphonia
/// decoder from pipe/channel sources.
struct PipeMediaSource<R: Read + Send + Sync + 'static> {
    inner: std::io::BufReader<R>,
}

impl<R: Read + Send + Sync + 'static> PipeMediaSource<R> {
    fn new(reader: R) -> Self {
        Self {
            inner: std::io::BufReader::new(reader),
        }
    }
}

impl<R: Read + Send + Sync + 'static> Read for PipeMediaSource<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

impl<R: Read + Send + Sync + 'static> std::io::Seek for PipeMediaSource<R> {
    fn seek(&mut self, _pos: std::io::SeekFrom) -> std::io::Result<u64> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "stream is not seekable",
        ))
    }
}

impl<R: Read + Send + Sync + 'static> MediaSource for PipeMediaSource<R> {
    fn is_seekable(&self) -> bool {
        false
    }

    fn byte_len(&self) -> Option<u64> {
        None
    }
}
