//! Audio effects: volume scaling and 3-band parametric EQ.
//!
//! `EffectsProcessor` owns an `OpusCodec` and an `EqFilterBank` and provides
//! the batch decode→effects→re-encode pipeline. Biquad delay-line state is
//! persistent across batches so the EQ doesn't click at frame boundaries.

use crate::{errors::AppResult, models::EqualizerSettings};

use super::codec::OpusCodec;

/// Second-order IIR (biquad) filter with persistent delay-line state.
#[derive(Debug, Clone, Copy)]
struct BiquadFilter {
    b0: f32,
    b1: f32,
    b2: f32,
    a1: f32,
    a2: f32,
    // delay-line state
    x1: f32,
    x2: f32,
    y1: f32,
    y2: f32,
}

impl BiquadFilter {
    const SAMPLE_RATE: f32 = 48_000.0;

    fn low_shelf(freq: f32, q: f32, gain: f32) -> Self {
        let w0 = 2.0 * std::f32::consts::PI * freq / Self::SAMPLE_RATE;
        let alpha = w0.sin() / (2.0 * q);
        let a = 10_f32.powf(gain.log10() / 2.0);
        let cos_w0 = w0.cos();
        let sqrt_a = a.sqrt();

        let b0 = a * ((a + 1.0) - (a - 1.0) * cos_w0 + 2.0 * sqrt_a * alpha);
        let b1 = 2.0 * a * ((a - 1.0) - (a + 1.0) * cos_w0);
        let b2 = a * ((a + 1.0) - (a - 1.0) * cos_w0 - 2.0 * sqrt_a * alpha);
        let a0 = (a + 1.0) + (a - 1.0) * cos_w0 + 2.0 * sqrt_a * alpha;
        let a1 = -2.0 * ((a - 1.0) + (a + 1.0) * cos_w0);
        let a2 = (a + 1.0) + (a - 1.0) * cos_w0 - 2.0 * sqrt_a * alpha;

        Self::normalised(b0, b1, b2, a0, a1, a2)
    }

    fn peaking(freq: f32, q: f32, gain: f32) -> Self {
        let w0 = 2.0 * std::f32::consts::PI * freq / Self::SAMPLE_RATE;
        let alpha = w0.sin() / (2.0 * q);
        let a = 10_f32.powf(gain.log10() / 2.0);
        let cos_w0 = w0.cos();

        let b0 = 1.0 + alpha * a;
        let b1 = -2.0 * cos_w0;
        let b2 = 1.0 - alpha * a;
        let a0 = 1.0 + alpha / a;
        let a1 = -2.0 * cos_w0;
        let a2 = 1.0 - alpha / a;

        Self::normalised(b0, b1, b2, a0, a1, a2)
    }

    fn high_shelf(freq: f32, q: f32, gain: f32) -> Self {
        let w0 = 2.0 * std::f32::consts::PI * freq / Self::SAMPLE_RATE;
        let alpha = w0.sin() / (2.0 * q);
        let a = 10_f32.powf(gain.log10() / 2.0);
        let cos_w0 = w0.cos();
        let sqrt_a = a.sqrt();

        let b0 = a * ((a + 1.0) + (a - 1.0) * cos_w0 + 2.0 * sqrt_a * alpha);
        let b1 = -2.0 * a * ((a - 1.0) + (a + 1.0) * cos_w0);
        let b2 = a * ((a + 1.0) + (a - 1.0) * cos_w0 - 2.0 * sqrt_a * alpha);
        let a0 = (a + 1.0) - (a - 1.0) * cos_w0 + 2.0 * sqrt_a * alpha;
        let a1 = 2.0 * ((a - 1.0) - (a + 1.0) * cos_w0);
        let a2 = (a + 1.0) - (a - 1.0) * cos_w0 - 2.0 * sqrt_a * alpha;

        Self::normalised(b0, b1, b2, a0, a1, a2)
    }

    /// Normalise coefficients by a0 and zero the delay line.
    fn normalised(b0: f32, b1: f32, b2: f32, a0: f32, a1: f32, a2: f32) -> Self {
        Self {
            b0: b0 / a0,
            b1: b1 / a0,
            b2: b2 / a0,
            a1: a1 / a0,
            a2: a2 / a0,
            x1: 0.0,
            x2: 0.0,
            y1: 0.0,
            y2: 0.0,
        }
    }

    fn process(&mut self, input: f32) -> f32 {
        let output = self.b0 * input + self.b1 * self.x1 + self.b2 * self.x2
            - self.a1 * self.y1
            - self.a2 * self.y2;
        self.x2 = self.x1;
        self.x1 = input;
        self.y2 = self.y1;
        self.y1 = output;
        output
    }
}

/// Persistent stereo 3-band EQ (low-shelf / peaking / high-shelf).
///
/// Delay-line state carries across frames and batches so transitions are
/// click-free. Coefficients are recomputed only when `EqualizerSettings`
/// change.
struct EqFilterBank {
    bass_l: BiquadFilter,
    bass_r: BiquadFilter,
    mid_l: BiquadFilter,
    mid_r: BiquadFilter,
    treble_l: BiquadFilter,
    treble_r: BiquadFilter,
}

impl EqFilterBank {
    fn new(eq: &EqualizerSettings) -> Self {
        let bass = BiquadFilter::low_shelf(250.0, 0.7, eq.bass);
        let mid = BiquadFilter::peaking(1500.0, 1.0, eq.mid);
        let treble = BiquadFilter::high_shelf(4000.0, 0.7, eq.treble);
        Self {
            bass_l: bass,
            bass_r: bass,
            mid_l: mid,
            mid_r: mid,
            treble_l: treble,
            treble_r: treble,
        }
    }

    /// Apply 3-band EQ in-place to interleaved stereo PCM.
    fn apply(&mut self, pcm: &mut [f32]) {
        for chunk in pcm.chunks_exact_mut(2) {
            chunk[0] = self
                .treble_l
                .process(self.mid_l.process(self.bass_l.process(chunk[0])));
            chunk[1] = self
                .treble_r
                .process(self.mid_r.process(self.bass_r.process(chunk[1])));
        }
    }
}

/// Batch decode → volume/EQ → re-encode pipeline.
///
/// Owns an `OpusCodec` and `EqFilterBank`. Processing a batch of N frames
/// decodes them all into a contiguous PCM buffer, applies effects once across
/// the whole buffer, then re-encodes per-frame.
///
/// This amortises filter-coefficient checks and volume comparisons over the
/// batch, improves CPU cache utilisation, and — critically — preserves biquad
/// delay-line continuity across frame boundaries.
pub struct EffectsProcessor {
    codec: OpusCodec,
    eq_bank: EqFilterBank,
    last_eq: EqualizerSettings,
    pcm_buf: Vec<f32>,
}

impl EffectsProcessor {
    pub fn new(bitrate_kbps: u32) -> AppResult<Self> {
        let default_eq = EqualizerSettings::default();
        Ok(Self {
            codec: OpusCodec::new(bitrate_kbps)?,
            eq_bank: EqFilterBank::new(&default_eq),
            last_eq: default_eq,
            pcm_buf: Vec::new(),
        })
    }

    /// Reset biquad delay-line state (call after seek to prevent filter bleed).
    pub fn reset_filters(&mut self) {
        self.eq_bank = EqFilterBank::new(&self.last_eq);
    }

    /// Process a single opus frame: decode → volume/EQ → re-encode.
    ///
    /// Returns the original frame unchanged when volume is 1.0 and EQ is flat.
    /// Falls back to pass-through on decode failure.
    pub fn process_frame(&mut self, frame: &[u8], volume: f32, eq: &EqualizerSettings) -> Vec<u8> {
        let has_vol = (volume - 1.0).abs() >= 0.01;
        let has_eq = !eq.is_flat();
        if !has_vol && !has_eq {
            return frame.to_vec();
        }

        // Recompute coefficients only when EQ actually changes.
        if has_eq && *eq != self.last_eq {
            self.eq_bank = EqFilterBank::new(eq);
            self.last_eq = eq.clone();
        }

        // Decode
        let pcm = match self.codec.decode_to_scratch(frame) {
            Ok(pcm) => pcm,
            _ => return frame.to_vec(),
        };
        let n = pcm.len();

        // Copy to bulk buffer for in-place mutation
        self.pcm_buf.clear();
        self.pcm_buf.extend_from_slice(pcm);

        // Apply effects
        if has_eq {
            self.eq_bank.apply(&mut self.pcm_buf);
        }
        if has_vol {
            for s in self.pcm_buf.iter_mut() {
                *s *= volume;
            }
        }

        // Re-encode
        match self.codec.encode(&self.pcm_buf[..n]) {
            Ok((len, data)) => data[..len].to_vec(),
            Err(_) => frame.to_vec(),
        }
    }
}
