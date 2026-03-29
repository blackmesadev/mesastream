use bm_lib::discord::{Id, VoiceConnectionDetails};
use bm_lib::model::mesastream::VoiceBridgePayload;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// External source type for a queued track.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    Youtube,
    Soundcloud,
}

/// Queue metadata for a track.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackMetadata {
    pub artist: String,
    pub title: String,
    pub duration_ms: u64,
    pub url: String,
}

/// Internal track model persisted in player state and cache snapshots.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Track {
    pub id: Uuid,
    pub source: SourceKind,
    pub source_url: String,
    pub stream_url: String,
    pub metadata: TrackMetadata,
    /// Error message if track failed to play (set during playback).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Track {
    /// Convert to the shared lib `Track` type used by WS events / the API model.
    pub fn to_lib_track(&self) -> bm_lib::model::mesastream::Track {
        bm_lib::model::mesastream::Track {
            id: self.id.to_string(),
            source: match self.source {
                SourceKind::Youtube => bm_lib::model::mesastream::SourceKind::Youtube,
                SourceKind::Soundcloud => bm_lib::model::mesastream::SourceKind::Soundcloud,
            },
            source_url: self.source_url.clone(),
            stream_url: self.stream_url.clone(),
            metadata: bm_lib::model::mesastream::TrackMetadata {
                artist: self.metadata.artist.clone(),
                title: self.metadata.title.clone(),
                duration_ms: self.metadata.duration_ms,
                url: self.metadata.url.clone(),
            },
            error: self.error.clone(),
        }
    }
}

/// Runtime status for a player.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlayerPlaybackStatus {
    Idle,
    Playing,
    Paused,
    Stopped,
}

/// 3-band equalizer settings for audio processing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EqualizerSettings {
    /// Bass gain multiplier (0.0 - 2.0, default 1.0). Affects frequencies < 250 Hz.
    pub bass: f32,
    /// Mid gain multiplier (0.0 - 2.0, default 1.0). Affects frequencies 250 Hz - 4000 Hz.
    pub mid: f32,
    /// Treble gain multiplier (0.0 - 2.0, default 1.0). Affects frequencies > 4000 Hz.
    pub treble: f32,
}

impl Default for EqualizerSettings {
    fn default() -> Self {
        Self {
            bass: 1.0,
            mid: 1.0,
            treble: 1.0,
        }
    }
}

impl EqualizerSettings {
    /// Returns true if EQ is effectively bypassed (all bands at 1.0).
    pub fn is_flat(&self) -> bool {
        (self.bass - 1.0).abs() < 0.01
            && (self.mid - 1.0).abs() < 0.01
            && (self.treble - 1.0).abs() < 0.01
    }
}

/// HTTP DTO carrying all Discord voice session info needed by Mesastream to connect.
/// Black Mesa builds this via `VoiceConnectionDetails::into_bridge_payload()`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoiceBridgePayloadDto {
    pub guild_id: Id,
    /// Identifies the player; equals channel_id by convention.
    pub player_id: Id,
    pub channel_id: Id,
    pub user_id: Id,
    pub session_id: String,
    pub token: String,
    pub endpoint: String,
    /// Full WSS URL for the Discord Voice Gateway, e.g. `wss://endpoint/?v=4&encoding=json`.
    pub gateway_url: String,
}

impl From<VoiceBridgePayload> for VoiceBridgePayloadDto {
    fn from(value: VoiceBridgePayload) -> Self {
        Self {
            guild_id: value.guild_id,
            player_id: value.player_id,
            channel_id: value.channel_id,
            user_id: value.user_id,
            session_id: value.session_id,
            token: value.token,
            endpoint: value.endpoint,
            gateway_url: value.gateway_url,
        }
    }
}

impl From<VoiceBridgePayloadDto> for VoiceConnectionDetails {
    fn from(value: VoiceBridgePayloadDto) -> Self {
        Self {
            guild_id: value.guild_id,
            player_id: value.player_id,
            channel_id: value.channel_id,
            user_id: value.user_id,
            session_id: value.session_id,
            token: value.token,
            endpoint: value.endpoint,
        }
    }
}

/// HTTP payload carrying Discord voice details from Black Mesa.
/// Kept for internal state serialisation; not exposed in inbound API requests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoiceConnectionDetailsDto {
    pub guild_id: Id,
    pub channel_id: Id,
    pub user_id: Id,
    pub session_id: String,
    pub token: String,
    pub endpoint: String,
}

impl From<VoiceConnectionDetails> for VoiceConnectionDetailsDto {
    fn from(value: VoiceConnectionDetails) -> Self {
        Self {
            guild_id: value.guild_id,
            channel_id: value.channel_id,
            user_id: value.user_id,
            session_id: value.session_id,
            token: value.token,
            endpoint: value.endpoint,
        }
    }
}

impl From<VoiceConnectionDetailsDto> for VoiceConnectionDetails {
    fn from(value: VoiceConnectionDetailsDto) -> Self {
        Self {
            guild_id: value.guild_id,
            player_id: value.channel_id,
            channel_id: value.channel_id,
            user_id: value.user_id,
            session_id: value.session_id,
            token: value.token,
            endpoint: value.endpoint,
        }
    }
}

/// Snapshot returned by API and stored in Redis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerStateSnapshot {
    pub player_id: Id,
    pub connection: VoiceBridgePayloadDto,
    pub status: PlayerPlaybackStatus,
    pub queue: Vec<Track>,
    pub current_track: Option<Track>,
    pub position_ms: u64,
    pub volume: f32,
    pub equalizer: EqualizerSettings,
    /// Whether the voice transport is currently connected to Discord.
    pub voice_connected: bool,
    /// Current playback session ID for telemetry correlation (present when a track is actively playing).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub playback_session_id: Option<Uuid>,
}

/// Current track info returned by the `/current` endpoint.
/// Includes playback position and duration with both raw ms and formatted strings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrentTrackResponse {
    pub track: Track,
    /// Current playback position in milliseconds.
    pub position_ms: u64,
    /// Total track duration in milliseconds.
    pub duration_ms: u64,
    /// Current position as "MM:SS".
    pub position: String,
    /// Total duration as "MM:SS".
    pub duration: String,
    /// Remaining time as "MM:SS".
    pub remaining: String,
}

/// Format a millisecond duration as `"MM:SS"`.
pub fn format_duration_ms(ms: u64) -> String {
    let total_secs = ms / 1000;
    let mins = total_secs / 60;
    let secs = total_secs % 60;
    format!("{mins}:{secs:02}")
}

/// Redis-stored playlist representation for a player.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlaylistSnapshot {
    pub player_id: Id,
    pub name: String,
    pub tracks: Vec<Track>,
}
