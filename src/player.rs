use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

use arc_swap::ArcSwap;
use bm_lib::discord::Id;
use sha2::{Digest, Sha256};
use tokio::{
    sync::{Mutex, Notify, RwLock},
    task::{self, JoinHandle},
    time::{sleep, Duration},
};
use tracing::{debug, error, info, instrument, warn, Instrument};
use uuid::Uuid;

use crate::{
    audio::{reader::TrackReader, AudioPipeline, EncoderProgress, PlaybackEffects},
    cache::RedisCache,
    config::Settings,
    discord::DiscordTransport,
    errors::{AppError, AppResult},
    models::{
        format_duration_ms, CurrentTrackResponse, EqualizerSettings, PlayerPlaybackStatus,
        PlayerStateSnapshot, PlaylistSnapshot, Track, TrackMetadata, VoiceBridgePayloadDto,
    },
    source::SourceRegistry,
    ws::EventSender,
};

// Playback timing constants
const OPUS_FRAME_DURATION_MS: u64 = 20;
const PLAYBACK_TICK_INTERVAL_MS: Duration = Duration::from_millis(OPUS_FRAME_DURATION_MS);

// Cache cleanup
const CACHE_CLEANUP_INTERVAL_SECS: u64 = 3600; // 1 hour

/// Mutable state persisted per player instance.
#[derive(Debug, Clone)]
struct PlayerState {
    connection: VoiceBridgePayloadDto,
    status: PlayerPlaybackStatus,
    queue: VecDeque<Track>,
    current_track: Option<Track>,
    volume: f32,
    equalizer: EqualizerSettings,
}

impl PlayerState {
    fn snapshot(
        &self,
        player_id: Id,
        position_ms: u64,
        playback_session_id: Option<Uuid>,
        voice_connected: bool,
    ) -> PlayerStateSnapshot {
        PlayerStateSnapshot {
            player_id,
            connection: self.connection.clone(),
            status: self.status.clone(),
            queue: self.queue.iter().cloned().collect(),
            current_track: self.current_track.clone(),
            position_ms,
            volume: self.volume,
            equalizer: self.equalizer.clone(),
            voice_connected,
            playback_session_id,
        }
    }
}

/// Persistent player object with queue and playback runtime controls.
pub struct Player {
    pub id: Id,
    state: RwLock<PlayerState>,
    transport: ArcSwap<DiscordTransport>,
    playback_task: Mutex<Option<JoinHandle<()>>>,
    paused: AtomicBool,
    stop_requested: AtomicBool,
    skip_requested: AtomicBool,
    seek_requested_ms: Mutex<Option<u64>>,
    /// Frame counter incremented every 20ms by the discord sender.
    /// Position is derived on-demand: `current_frame * OPUS_FRAME_DURATION_MS`.
    current_frame: AtomicU64,
    /// Current playback session ID for telemetry correlation.
    playback_session_id: RwLock<Option<Uuid>>,
    /// Cache key for current track
    current_cache_key: Mutex<String>,
    /// Track URL for re-fetching stream when seeking
    current_track_url: Mutex<String>,
    /// Shared effect parameters (volume/EQ) readable from sync context.
    /// Updated by set_volume/set_equalizer, consumed by the cache reader.
    effects: Arc<PlaybackEffects>,
    /// Wakes the playback loop instantly when a control action happens
    /// (seek, skip, stop, pause, resume, enqueue, update_connection).
    /// Replaces fixed-interval sleep polling.
    notify: Arc<Notify>,
}

impl Player {
    fn new(id: Id, connection: VoiceBridgePayloadDto) -> Self {
        Self {
            id,
            current_frame: AtomicU64::new(0),
            state: RwLock::new(PlayerState {
                connection,
                status: PlayerPlaybackStatus::Idle,
                queue: VecDeque::new(),
                current_track: None,
                volume: 1.0,
                equalizer: EqualizerSettings::default(),
            }),
            transport: ArcSwap::from_pointee(DiscordTransport::default()),
            playback_task: Mutex::new(None),
            paused: AtomicBool::new(false),
            stop_requested: AtomicBool::new(false),
            skip_requested: AtomicBool::new(false),
            seek_requested_ms: Mutex::new(None),
            playback_session_id: RwLock::new(None),
            current_cache_key: Mutex::new(String::new()),
            current_track_url: Mutex::new(String::new()),
            effects: Arc::new(PlaybackEffects::new(1.0, &EqualizerSettings::default())),
            notify: Arc::new(Notify::new()),
        }
    }

    async fn snapshot(&self) -> PlayerStateSnapshot {
        let playback_session_id = *self.playback_session_id.read().await;
        let voice_connected = self.transport.load().is_connected();
        let snapshot = self.state.read().await.snapshot(
            self.id,
            self.get_position_ms(),
            playback_session_id,
            voice_connected,
        );
        snapshot
    }

    async fn pop_next_track(&self) -> Option<Track> {
        let mut state = self.state.write().await;
        let next = state.queue.pop_front();
        state.current_track = next.clone();
        if next.is_none() {
            state.status = PlayerPlaybackStatus::Idle;
        }
        next
    }

    /// Get current playback position in milliseconds, derived from the frame counter.
    fn get_position_ms(&self) -> u64 {
        self.current_frame.load(Ordering::Relaxed) * OPUS_FRAME_DURATION_MS
    }

    /// Set playback position - converts ms to a frame offset and stores it.
    fn set_position_ms(&self, ms: u64) {
        self.current_frame
            .store(ms / OPUS_FRAME_DURATION_MS, Ordering::Relaxed);
    }

    /// Get the duration of the current track in milliseconds, or 0 if unknown.
    async fn get_duration_ms(&self) -> u64 {
        self.state
            .read()
            .await
            .current_track
            .as_ref()
            .map(|t| t.metadata.duration_ms)
            .unwrap_or(0)
    }
}

/// Player manager service for CRUD and playback commands.
#[derive(Clone)]
pub struct PlayerManager {
    players: Arc<RwLock<HashMap<Id, Arc<Player>>>>,
    settings: Arc<Settings>,
    cache: RedisCache,
    sources: SourceRegistry,
    audio_pipeline: AudioPipeline,
    event_tx: EventSender,
}

/// Request payload for creating a player.
/// Black Mesa builds this via `VoiceConnectionDetails::into_bridge_payload()`.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct CreatePlayerRequest {
    #[serde(flatten)]
    pub payload: VoiceBridgePayloadDto,
}

/// Request payload for updating voice connection.
/// Sent when Black Mesa detects `VoiceSessionChange::EndpointUpdated`.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct UpdateConnectionRequest {
    #[serde(flatten)]
    pub payload: VoiceBridgePayloadDto,
}

/// Request payload to enqueue a source URL.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct EnqueueRequest {
    pub url: String,
}

/// Request payload for seek command.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct SeekRequest {
    pub position_ms: u64,
}

/// Request payload for volume command.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct VolumeRequest {
    pub volume: f32,
}

/// Request payload for equalizer command.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct EqualizerRequest {
    #[serde(flatten)]
    pub equalizer: EqualizerSettings,
}

impl PlayerManager {
    pub fn new(
        settings: Arc<Settings>,
        cache: RedisCache,
        sources: SourceRegistry,
        event_tx: EventSender,
    ) -> Self {
        let audio_pipeline = AudioPipeline::new(settings.clone());
        Self {
            players: Arc::new(RwLock::new(HashMap::new())),
            settings,
            cache,
            sources,
            audio_pipeline,
            event_tx,
        }
    }

    /// Best-effort broadcast of a player event to connected WS clients.
    fn emit(&self, event: bm_lib::model::mesastream::MesastreamEvent) {
        // Ignore send errors - just means no one is listening right now.
        let _ = self.event_tx.send(event);
    }

    /// Helper to persist player state and return snapshot
    #[instrument(skip(self, player))]
    async fn persist_and_snapshot(&self, player: &Arc<Player>) -> PlayerStateSnapshot {
        let snapshot = player.snapshot().await;
        self.cache.set_player_snapshot(&snapshot).await;
        snapshot
    }

    /// Spawns a background task that periodically scans the audio cache directory
    /// and removes .msopus files whose metadata has expired from Redis.
    pub fn start_cache_cleanup_task(&self) {
        let cache = self.cache.clone();
        let cache_path = self.settings.audio_cache_path.clone();

        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(CACHE_CLEANUP_INTERVAL_SECS));
            loop {
                interval.tick().await;

                if let Err(e) = cleanup_orphaned_cache_files(&cache, &cache_path).await {
                    error!(error = %e, "cache cleanup task failed");
                }
            }
        });
    }

    #[instrument(skip(self, request))]
    pub async fn create_player(
        &self,
        request: CreatePlayerRequest,
    ) -> AppResult<PlayerStateSnapshot> {
        let player_id = request.payload.player_id;
        let guild_id = request.payload.guild_id;

        // Ensure only one active player per guild and prevent replacing an existing
        // player entry without stopping its playback task.
        self.destroy_conflicting_players(player_id, guild_id).await;

        let player = Arc::new(Player::new(player_id, request.payload.clone()));

        let transport = DiscordTransport::connect(&request.payload).await?;
        player.transport.store(Arc::new(transport));

        // Restore any queue that survived a previous destroy_player call.
        let saved_queue = self.cache.get_queue_snapshot(guild_id).await;
        let restored_queue_len = saved_queue.len();
        if !saved_queue.is_empty() {
            let mut state = player.state.write().await;
            state.queue = saved_queue.into_iter().collect();
            debug!(player_id = %player_id, queue_len = state.queue.len(), "restored queue from previous session");
        }

        // Restore playback position if the player was destroyed mid-playback
        let restored_position_ms = self.cache.get_guild_position(guild_id).await.unwrap_or(0);
        if restored_position_ms > 0 {
            player.set_position_ms(restored_position_ms);
            debug!(player_id = %player_id, position_ms = restored_position_ms, "restored playback position from previous session");
        }

        self.players.write().await.insert(player_id, player.clone());

        // Spawn the playback loop task
        {
            let mut task_lock = player.playback_task.lock().await;
            let manager = self.clone();
            let player_clone = player.clone();
            *task_lock = Some(tokio::spawn(async move {
                manager.playback_loop(player_clone).await;
            }));
        }

        self.emit(bm_lib::model::mesastream::MesastreamEvent::PlayerCreated {
            guild_id,
            player_id,
            restored_queue_len,
            restored_position_ms,
        });

        Ok(self.persist_and_snapshot(&player).await)
    }

    #[instrument(skip(self))]
    pub async fn list_players(&self) -> Vec<PlayerStateSnapshot> {
        let player_refs: Vec<Arc<Player>> = self.players.read().await.values().cloned().collect();
        let mut snapshots = Vec::with_capacity(player_refs.len());
        for player in player_refs {
            snapshots.push(player.snapshot().await);
        }
        snapshots
    }

    #[instrument(skip(self))]
    pub async fn get_player(&self, player_id: Id) -> AppResult<PlayerStateSnapshot> {
        let player = self.get_player_ref(player_id).await?;
        Ok(player.snapshot().await)
    }

    #[instrument(skip(self, request))]
    pub async fn update_connection(
        &self,
        player_id: Id,
        request: UpdateConnectionRequest,
    ) -> AppResult<PlayerStateSnapshot> {
        // Called by Black Mesa when Discord voice session changes are detected:
        // - VoiceSessionChange::EndpointUpdated: Discord rotated voice server (maintenance/load balancing)
        // Black Mesa calls this with a new VoiceBridgePayloadDto built from the updated endpoint.
        // - Session reconnection: after network issues, Black Mesa rejoins and sends a fresh payload.
        //
        // Mesastream re-runs the voice gateway handshake and replaces the UDP transport
        // atomically so playback is uninterrupted.
        let player = self.get_player_ref(player_id).await?;

        let transport = DiscordTransport::connect(&request.payload).await?;

        {
            let mut state = player.state.write().await;
            state.connection = request.payload;
        }

        player.transport.store(Arc::new(transport));

        // Wake the playback loop's voice-recovery wait immediately.
        player.notify.notify_waiters();

        Ok(self.persist_and_snapshot(&player).await)
    }

    #[instrument(skip(self, request), fields(player_id = %player_id, url = %request.url, track_id))]
    pub async fn enqueue(
        &self,
        player_id: Id,
        request: EnqueueRequest,
    ) -> AppResult<PlayerStateSnapshot> {
        let track_id = Uuid::new_v4();
        tracing::Span::current().record("track_id", &tracing::field::display(&track_id));

        // Validate URL before proceeding
        if request.url.is_empty()
            || request.url == "false"
            || request.url == "true"
            || !request.url.starts_with("http")
        {
            return Err(AppError::SourceUnavailable(format!(
                "invalid URL: {}",
                request.url
            )));
        }

        let _source = self.sources.detect_source(&request.url).ok_or_else(|| {
            AppError::SourceUnavailable("no source supports this URL".to_string())
        })?;
        let player = self.get_player_ref(player_id).await?;

        let metadata_url = request.url;

        // Resolve metadata before returning so the response has title/artist/duration.
        // Also seeds the cache so start_playback never needs a separate metadata call.
        let cache_key = hex::encode(Sha256::digest(metadata_url.as_bytes()));
        let resolved = self.sources.resolve_metadata(&metadata_url).await?;

        info!(
            source = ?resolved.source,
            artist = %resolved.artist,
            title = %resolved.title,
            duration_ms = resolved.duration_ms,
            "track_enqueued"
        );

        let track = Track {
            id: track_id,
            source: resolved.source,
            source_url: metadata_url.clone(),
            stream_url: metadata_url.clone(),
            metadata: TrackMetadata {
                artist: resolved.artist,
                title: resolved.title,
                duration_ms: resolved.duration_ms,
                url: metadata_url,
            },
            error: None,
        };

        // Persist metadata to cache so start_playback can skip resolution.
        self.cache
            .set_cache_metadata(&cache_key, &track, self.settings.audio_cache_ttl_secs)
            .await;

        {
            let mut state = player.state.write().await;
            state.queue.push_back(track.clone());
        }

        let snapshot = self.persist_and_snapshot(&player).await;
        self.cache
            .set_queue_snapshot(snapshot.connection.guild_id, &snapshot.queue)
            .await;

        // Wake the playback loop if it's idle-waiting for the first enqueue.
        player.notify.notify_waiters();

        Ok(snapshot)
    }

    #[instrument(skip(self))]
    pub async fn clear_queue(&self, player_id: Id) -> AppResult<PlayerStateSnapshot> {
        let player = self.get_player_ref(player_id).await?;
        {
            let mut state = player.state.write().await;
            state.queue.clear();
        }

        let snapshot = self.persist_and_snapshot(&player).await;
        self.cache
            .set_queue_snapshot(snapshot.connection.guild_id, &snapshot.queue)
            .await;
        Ok(snapshot)
    }

    #[instrument(skip(self))]
    pub async fn play(&self, player_id: Id) -> AppResult<PlayerStateSnapshot> {
        let player = self.get_player_ref(player_id).await?;

        player.stop_requested.store(false, Ordering::Relaxed);
        player.paused.store(false, Ordering::Relaxed);

        // If the transport is not connected (e.g. player was restored from Redis
        // cache after a restart and create_player was never called again), reconnect
        // using the stored connection info before starting playback.
        if !player.transport.load().is_connected() {
            let connection = player.state.read().await.connection.clone();
            info!(player_id = %player_id, "transport disconnected - reconnecting before play");
            match DiscordTransport::connect(&connection).await {
                Ok(transport) => {
                    player.transport.store(Arc::new(transport));
                }
                Err(e) => {
                    error!(player_id = %player_id, error = %e, "voice reconnect failed");
                    return Err(e);
                }
            }
        }

        // Restart the playback loop if it was killed by stop() (task aborted)
        // or if it exited naturally.  Without this, play-after-stop would set
        // the status to Playing but never actually process the queue.
        {
            let mut task_lock = player.playback_task.lock().await;
            let needs_restart = match task_lock.as_ref() {
                None => true,
                Some(handle) => handle.is_finished(),
            };
            if needs_restart {
                let manager = self.clone();
                let player_clone = player.clone();
                *task_lock = Some(tokio::spawn(async move {
                    manager.playback_loop(player_clone).await;
                }));
                debug!(player_id = %player_id, "restarted playback loop");
            }
        }

        player.notify.notify_waiters();

        {
            let mut state = player.state.write().await;
            state.status = PlayerPlaybackStatus::Playing;
        }

        Ok(self.persist_and_snapshot(&player).await)
    }

    #[instrument(skip(self))]
    pub async fn pause(&self, player_id: Id) -> AppResult<PlayerStateSnapshot> {
        let player = self.get_player_ref(player_id).await?;
        player.paused.store(true, Ordering::Relaxed);
        player.notify.notify_waiters();

        {
            let mut state = player.state.write().await;
            state.status = PlayerPlaybackStatus::Paused;
        }

        Ok(self.persist_and_snapshot(&player).await)
    }

    #[instrument(skip(self))]
    pub async fn resume(&self, player_id: Id) -> AppResult<PlayerStateSnapshot> {
        let player = self.get_player_ref(player_id).await?;
        player.paused.store(false, Ordering::Relaxed);
        player.notify.notify_waiters();

        {
            let mut state = player.state.write().await;
            state.status = PlayerPlaybackStatus::Playing;
        }

        Ok(self.persist_and_snapshot(&player).await)
    }

    #[instrument(skip(self))]
    pub async fn skip(&self, player_id: Id) -> AppResult<PlayerStateSnapshot> {
        let player = self.get_player_ref(player_id).await?;
        player.skip_requested.store(true, Ordering::Relaxed);
        player.notify.notify_waiters();
        Ok(self.persist_and_snapshot(&player).await)
    }

    #[instrument(skip(self))]
    pub async fn stop(&self, player_id: Id) -> AppResult<PlayerStateSnapshot> {
        let player = self.get_player_ref(player_id).await?;
        player.stop_requested.store(true, Ordering::Relaxed);
        player.notify.notify_waiters();
        *player.seek_requested_ms.lock().await = None;

        {
            let mut state = player.state.write().await;
            state.status = PlayerPlaybackStatus::Stopped;
            state.current_track = None;
        }
        player.set_position_ms(0);

        if let Some(task) = player.playback_task.lock().await.take() {
            task.abort();
        }

        Ok(self.persist_and_snapshot(&player).await)
    }

    #[instrument(skip(self, request))]
    pub async fn seek(
        &self,
        player_id: Id,
        request: SeekRequest,
    ) -> AppResult<PlayerStateSnapshot> {
        let player = self.get_player_ref(player_id).await?;

        // Validate seek position against track duration (skip if duration not yet resolved)
        let duration_ms = player.get_duration_ms().await;
        if duration_ms > 0 && request.position_ms > duration_ms {
            return Err(AppError::BadRequest(format!(
                "seek position {} exceeds track duration {}",
                format_duration_ms(request.position_ms),
                format_duration_ms(duration_ms),
            )));
        }

        *player.seek_requested_ms.lock().await = Some(request.position_ms);
        player.notify.notify_waiters();
        Ok(self.persist_and_snapshot(&player).await)
    }

    #[instrument(skip(self, request))]
    pub async fn set_volume(
        &self,
        player_id: Id,
        request: VolumeRequest,
    ) -> AppResult<PlayerStateSnapshot> {
        if !(0.0..=2.0).contains(&request.volume) {
            return Err(AppError::BadRequest(
                "volume must be between 0.0 and 2.0".to_string(),
            ));
        }

        let player = self.get_player_ref(player_id).await?;
        {
            let mut state = player.state.write().await;
            state.volume = request.volume;
        }
        player.effects.set_volume(request.volume);

        Ok(self.persist_and_snapshot(&player).await)
    }

    #[instrument(skip(self, request))]
    pub async fn set_equalizer(
        &self,
        player_id: Id,
        request: EqualizerRequest,
    ) -> AppResult<PlayerStateSnapshot> {
        // Validate EQ band values
        if !(0.0..=2.0).contains(&request.equalizer.bass)
            || !(0.0..=2.0).contains(&request.equalizer.mid)
            || !(0.0..=2.0).contains(&request.equalizer.treble)
        {
            return Err(AppError::BadRequest(
                "equalizer bands must be between 0.0 and 2.0".to_string(),
            ));
        }

        let player = self.get_player_ref(player_id).await?;
        {
            let mut state = player.state.write().await;
            state.equalizer = request.equalizer.clone();
        }
        player.effects.set_eq(&request.equalizer);

        Ok(self.persist_and_snapshot(&player).await)
    }

    #[instrument(skip(self))]
    pub async fn current_track(&self, player_id: Id) -> AppResult<Option<CurrentTrackResponse>> {
        let player = self.get_player_ref(player_id).await?;
        let state = player.state.read().await;
        match &state.current_track {
            Some(track) => {
                let position_ms = player.get_position_ms();
                let duration_ms = track.metadata.duration_ms;
                let remaining_ms = duration_ms.saturating_sub(position_ms);
                Ok(Some(CurrentTrackResponse {
                    track: track.clone(),
                    position_ms,
                    duration_ms,
                    position: format_duration_ms(position_ms),
                    duration: format_duration_ms(duration_ms),
                    remaining: format_duration_ms(remaining_ms),
                }))
            }
            None => Ok(None),
        }
    }

    #[instrument(skip(self))]
    pub async fn destroy_player(&self, player_id: Id) -> AppResult<()> {
        let player = self
            .players
            .write()
            .await
            .remove(&player_id)
            .ok_or_else(|| AppError::NotFound(format!("player {player_id} not found")))?;

        // Stop playback first so the queue is no longer being mutated.
        player.stop_requested.store(true, Ordering::Relaxed);
        if let Some(task) = player.playback_task.lock().await.take() {
            task.abort();
        }

        // Persist current playback position (derived from frame counter)
        let current_position = player.get_position_ms();
        let guild_id = player.state.read().await.connection.guild_id;
        if current_position > 0 {
            self.cache
                .set_guild_position(guild_id, current_position)
                .await;
            debug!(player_id = %player_id, position_ms = current_position, "saved playback position on destroy");
        }

        // Persist the current queue so create_player can restore it.
        // Write a clean Idle snapshot (no current_track) with the queue intact.
        {
            let mut state = player.state.write().await;
            state.status = PlayerPlaybackStatus::Idle;
            state.current_track = None;
        }
        let snapshot = player.snapshot().await;
        self.cache.set_player_snapshot(&snapshot).await;
        self.cache
            .set_queue_snapshot(guild_id, &snapshot.queue)
            .await;
        info!(player_id = %player_id, queue_len = snapshot.queue.len(), "player destroyed - queue persisted");

        self.emit(
            bm_lib::model::mesastream::MesastreamEvent::PlayerDestroyed {
                guild_id,
                player_id,
                was_stopped: true,
            },
        );

        Ok(())
    }

    pub async fn save_playlist(&self, player_id: Id, name: String) -> AppResult<PlaylistSnapshot> {
        let playlist_name = name.clone();

        let (playlist, guild_id) = async {
            let player = self.get_player_ref(player_id).await?;
            let state = player.state.read().await;
            let playlist = PlaylistSnapshot {
                player_id,
                name,
                tracks: state.queue.iter().cloned().collect(),
            };
            let guild_id = player.state.read().await.connection.guild_id;
            Ok::<_, AppError>((playlist, guild_id))
        }
        .instrument(
            tracing::info_span!("save_playlist", player_id = %player_id, playlist = %playlist_name),
        )
        .await?;

        self.cache.set_playlist(guild_id, &playlist).await;
        Ok(playlist)
    }

    pub async fn load_playlist(
        &self,
        player_id: Id,
        name: String,
    ) -> AppResult<PlayerStateSnapshot> {
        let snapshot = async {
            let player = self.get_player_ref(player_id).await?;
            let guild_id = player.state.read().await.connection.guild_id;
            let playlist = self
                .cache
                .get_playlist(guild_id, &name)
                .await
                .ok_or_else(|| AppError::NotFound(format!("playlist {name} not found")))?;

            {
                let mut state = player.state.write().await;
                for track in playlist.tracks {
                    state.queue.push_back(track);
                }
            }

            Ok(self.persist_and_snapshot(&player).await)
        }
        .instrument(tracing::info_span!("load_playlist", player_id = %player_id, playlist = %name))
        .await?;

        self.cache
            .set_queue_snapshot(snapshot.connection.guild_id, &snapshot.queue)
            .await;
        Ok(snapshot)
    }

    async fn get_player_ref(&self, player_id: Id) -> AppResult<Arc<Player>> {
        self.players
            .read()
            .await
            .get(&player_id)
            .cloned()
            .ok_or_else(|| AppError::NotFound(format!("player {player_id} not found")))
    }

    /// Destroy any existing players that conflict with the given player_id or guild_id.
    /// This ensures only one player exists per guild and prevents duplicate player_id entries.
    async fn destroy_conflicting_players(&self, player_id: Id, guild_id: Id) {
        let existing_players: Vec<Arc<Player>> =
            self.players.read().await.values().cloned().collect();
        let mut players_to_destroy = Vec::new();

        for existing in existing_players {
            let existing_guild_id = existing.state.read().await.connection.guild_id;
            if existing.id == player_id || existing_guild_id == guild_id {
                players_to_destroy.push(existing.id);
            }
        }

        for existing_id in players_to_destroy {
            if let Err(error) = self.destroy_player(existing_id).await {
                match error {
                    AppError::NotFound(_) => {}
                    other => {
                        warn!(
                            guild_id = %guild_id,
                            old_player_id = %existing_id,
                            error = %other,
                            "failed to destroy conflicting player before create"
                        );
                    }
                }
            }
        }
    }

    /// Starts or resumes playback: encoder → cache file → cache reader → discord_tx.
    ///
    /// Cached path: reads directly from the completed `.opus` file.
    /// Uncached path: spawns encoder writing `.opus` at full speed; reader follows
    /// via `EncoderProgress` to know how many frames are safe to read.
    ///
    /// Returns `(cache_file_path, encoder_progress, cancel_flag)`.
    /// The caller (unified playback loop) reads frames from the cache at 20ms
    /// pacing and sends them directly to Discord.
    #[instrument(skip(self, player), fields(player_id = %player.id))]
    async fn setup_source(
        &self,
        player: &Arc<Player>,
    ) -> Result<(std::path::PathBuf, Arc<EncoderProgress>, Arc<AtomicBool>), AppError> {
        let cache_key = player.current_cache_key.lock().await.clone();
        let track_url = player.current_track_url.lock().await.clone();
        let cache_dir = std::path::PathBuf::from(&self.settings.audio_cache_path);
        let final_path = cache_dir.join(format!("{cache_key}.opus"));

        let reader_cancel = Arc::new(AtomicBool::new(false));

        // Try to use an existing completed cache file.
        // Validate file size against expected bytes to catch truncated files.
        let cached_duration_ms = if final_path.exists() {
            match self.cache.get_cache_metadata(&cache_key).await {
                Some(cached_track) => {
                    let duration_ms = cached_track.metadata.duration_ms;
                    // If we have a known duration, check file isn't truncated
                    if duration_ms > 0 {
                        let file_size = tokio::fs::metadata(&final_path)
                            .await
                            .map(|m| m.len())
                            .unwrap_or(0);
                        let min_expected_bytes = (duration_ms / 1000) * 50 * 20;
                        if file_size < min_expected_bytes {
                            warn!(
                                cache_key = %cache_key,
                                file_size,
                                min_expected_bytes,
                                duration_ms,
                                "cache file appears truncated - deleting and re-encoding"
                            );
                            let _ = tokio::fs::remove_file(&final_path).await;
                            None
                        } else {
                            Some(duration_ms)
                        }
                    } else {
                        Some(0)
                    }
                }
                None => None, // metadata expired - treat as uncached
            }
        } else {
            None
        };

        if let Some(duration_ms) = cached_duration_ms {
            // Fully cached - progress pre-completed so reader streams immediately
            let total_frames = duration_ms / OPUS_FRAME_DURATION_MS;
            let progress = Arc::new(EncoderProgress::completed(total_frames));
            Ok((final_path, progress, reader_cancel))
        } else {
            // Not cached - get stream, encode to .opus, reader follows encoder
            let _ = tokio::fs::remove_file(&final_path).await;

            let byte_stream = self.sources.get_stream(&track_url).await.map_err(|_| {
                AppError::SourceUnavailable("stream resolution timed out after 30s".to_string())
            })?;

            let reader = self.audio_pipeline.open_stream(byte_stream);
            let (progress, duration_rx) = self.audio_pipeline.spawn_encoder(reader, &cache_key);

            // When encoding completes, backfill duration_ms into player state and cache
            let player_clone = player.clone();
            let cache_clone = self.cache.clone();
            let cache_key_clone = cache_key.clone();
            let ttl_secs = self.settings.audio_cache_ttl_secs;
            tokio::spawn(
                async move {
                    let Ok(duration_ms) = duration_rx.await else {
                        return;
                    };
                    let mut state = player_clone.state.write().await;
                    let Some(current) = state.current_track.as_mut() else {
                        return;
                    };
                    if current.metadata.duration_ms == 0 {
                        current.metadata.duration_ms = duration_ms;
                        debug!(duration_ms, "track duration computed from encoder");
                    }
                    let mut updated = current.clone();
                    updated.metadata.duration_ms = duration_ms;
                    cache_clone
                        .set_cache_metadata(&cache_key_clone, &updated, ttl_secs)
                        .await;
                }
                .in_current_span(),
            );

            Ok((final_path, progress, reader_cancel))
        }
    }

    /// Initializes track playback: sets up cache keys, starts encoder if needed.
    /// Returns `(cache_path, progress, cancel)` for the unified playback loop.
    #[instrument(skip(self, player), fields(track_id = %track.id, player_id = %player.id))]
    async fn start_playback(
        &self,
        player: &Arc<Player>,
        track: &Track,
    ) -> Result<
        (
            std::path::PathBuf,
            Arc<EncoderProgress>,
            Arc<AtomicBool>,
            Option<u64>,
        ),
        AppError,
    > {
        // Setup track state
        let cache_key = hex::encode(Sha256::digest(track.source_url.as_bytes()));
        *player.current_cache_key.lock().await = cache_key.clone();
        *player.current_track_url.lock().await = track.metadata.url.clone();
        let playback_session_id = Uuid::new_v4();
        *player.playback_session_id.write().await = Some(playback_session_id);
        player.skip_requested.store(false, Ordering::Relaxed);

        // Sync effects from player state so reader uses current settings
        {
            let state = player.state.read().await;
            player.effects.set_volume(state.volume);
            player.effects.set_eq(&state.equalizer);
        }

        // Restore position if reconnecting mid-track (captured before reset)
        let start_frames = player.current_frame.swap(0, Ordering::Relaxed);
        let init_seek = if start_frames > 0 {
            Some(start_frames * OPUS_FRAME_DURATION_MS)
        } else {
            None
        };

        // Setup source → encoder → cache file (reader is handled by the caller)
        let (cache_path, progress, cancel) = self.setup_source(player).await.map_err(|e| {
            error!(error = %e, "source_init_failed");
            e
        })?;

        {
            let mut state = player.state.write().await;
            state.status = PlayerPlaybackStatus::Playing;
        }

        // Emit TrackStarted event
        let guild_id = player.state.read().await.connection.guild_id;
        self.emit(bm_lib::model::mesastream::MesastreamEvent::TrackStarted {
            guild_id,
            player_id: player.id,
            track: track.to_lib_track(),
            position_ms: init_seek.unwrap_or(0),
        });

        Ok((cache_path, progress, cancel, init_seek))
    }

    /// Unified per-track playback loop.
    ///
    /// Combines the former reader, discord_sender, and await_track into a
    /// single async loop that ticks every 20ms:
    ///
    /// tick → read opus frame → check seek/pause/skip/stop →
    /// apply volume/EQ if enabled → send to Discord (DAVE + UDP)
    ///
    /// No `spawn_blocking`, no mpsc channel, no spin-waits.
    async fn play_track(
        &self,
        player: &Arc<Player>,
        cache_path: std::path::PathBuf,
        progress: Arc<EncoderProgress>,
        cancel: Arc<AtomicBool>,
        init_seek: Option<u64>,
    ) {
        let bitrate = self.audio_pipeline.bitrate_kbps();

        // Wait for cache file and open the reader
        let Some(mut track_reader) = TrackReader::open(
            &cache_path,
            progress.clone(),
            player.effects.clone(),
            &cancel,
            bitrate,
            self.settings.cache_reader_buffer_bytes,
        )
        .await
        else {
            // Cancelled before file appeared
            return;
        };

        // Seek to initial position if resuming mid-track
        if let Some(ms) = init_seek {
            track_reader.seek_to_ms(ms);
        }

        let mut transport = player.transport.load_full();
        let mut speaking = false;
        // Use Interval for precise 20ms pacing with Skip policy to avoid
        // burst-catching-up after pause/seek stalls.
        let mut ticker = tokio::time::interval(PLAYBACK_TICK_INTERVAL_MS);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Consume the instant first tick so we pace from now
        ticker.tick().await;

        // Health check: time since last successful transport health verification
        let mut last_health_check = tokio::time::Instant::now();
        let health_check_interval = Duration::from_secs(5);

        loop {
            // Stop or skip requested - break to outer loop to handle cleanup or next track setup
            if player.stop_requested.load(Ordering::Relaxed) {
                cancel.store(true, Ordering::Relaxed);
                progress.cancelled.store(true, Ordering::Relaxed);
                break;
            }

            if player.skip_requested.swap(false, Ordering::Relaxed) {
                cancel.store(true, Ordering::Relaxed);
                player.set_position_ms(0);
                break;
            }

            // Pause requested - async wait until unpaused.  Zero CPU while paused.
            if player.paused.load(Ordering::Relaxed) {
                if speaking {
                    transport.set_speaking(false);
                    speaking = false;
                }
                // Async wait - zero CPU while paused
                player.notify.notified().await;
                ticker.reset();
                continue;
            }

            // Seek requested - update reader position and reset ticker to avoid burst of frames after long seek
            if let Some(seek_ms) = player.seek_requested_ms.lock().await.take() {
                debug!(position_ms = seek_ms, "seek_started");
                track_reader.seek_to_ms(seek_ms);
                player.set_position_ms(seek_ms);
                ticker.reset();
                info!(position_ms = seek_ms, "seek_completed");
            }

            // Periodic health check of the transport connection.  If disconnected, emit event and wait for either update_connection or timeout.
            if last_health_check.elapsed() >= health_check_interval {
                last_health_check = tokio::time::Instant::now();
                let connected = player.transport.load().is_connected();
                if !connected {
                    if speaking {
                        transport.set_speaking(false);
                        speaking = false;
                    }
                    let guild_id = player.state.read().await.connection.guild_id;
                    warn!(player_id = %player.id, %guild_id, "voice transport disconnected - emitting VoiceDisconnected");
                    self.emit(
                        bm_lib::model::mesastream::MesastreamEvent::VoiceDisconnected {
                            guild_id,
                            player_id: player.id,
                            reason: "voice gateway closed".to_string(),
                        },
                    );

                    // Wait for update_connection (woken by notify) or timeout
                    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
                    loop {
                        tokio::select! {
                            _ = player.notify.notified() => {}
                            _ = sleep(Duration::from_secs(5)) => {}
                        }
                        if player.stop_requested.load(Ordering::Relaxed) {
                            cancel.store(true, Ordering::Relaxed);
                            progress.cancelled.store(true, Ordering::Relaxed);
                            if speaking {
                                transport.set_speaking(false);
                            }
                            return;
                        }
                        if player.transport.load().is_connected() {
                            transport = player.transport.load_full();
                            info!(player_id = %player.id, "voice transport recovered via update_connection");
                            ticker.reset();
                            break;
                        }
                        if tokio::time::Instant::now() >= deadline {
                            error!(player_id = %player.id, "voice transport recovery timed out after 60s - stopping playback");
                            player.stop_requested.store(true, Ordering::Relaxed);
                            if speaking {
                                transport.set_speaking(false);
                            }
                            return;
                        }
                    }
                }
            }

            // Read opus frame
            let frame = match track_reader.next_frame() {
                Some(f) => f,
                None => {
                    if track_reader.is_eof() {
                        break;
                    }
                    progress.notify.notified().await;
                    continue;
                }
            };

            if !speaking {
                transport.set_speaking(true);
                speaking = true;
            }

            ticker.tick().await;

            // Keep local handle in sync with update_connection() without lock churn.
            transport = player.transport.load_full();

            // Send to Discord (DAVE encrypt + RTP + UDP inside transport)
            if transport.is_connected() {
                let _ = transport.send_opus_frame(&frame).await;
            }

            player.current_frame.fetch_add(1, Ordering::Relaxed);
        }

        if speaking {
            transport.set_speaking(false);
        }
        player.set_position_ms(0);
    }

    async fn playback_loop(&self, player: Arc<Player>) {
        let mut tracks_played: u64 = 0;

        loop {
            task::yield_now().await;

            if player.stop_requested.load(Ordering::Relaxed) {
                break;
            }

            let Some(track) = player.pop_next_track().await else {
                if tracks_played > 0 {
                    // Queue drained after playing at least one track - auto-destroy
                    // the player to free resources.  The Discord voice connection
                    // stays open (managed by Black Mesa).  Queue & position are
                    // persisted in Redis so the next `create_player` restores them.
                    info!(player_id = %player.id, tracks_played, "queue empty after playback - auto-destroying player");
                    break;
                }
                // No track has been played yet (player just created) - wait
                // for enqueue (woken by notify) rather than polling.
                player.set_position_ms(0);
                player.notify.notified().await;
                continue;
            };

            // Per-track span: covers init, then closes before the playback
            // loop so it flushes to the telemetry backend immediately.
            let track_init_span = tracing::info_span!(
                "track_playback",
                track_id = %track.id,
                url = %track.metadata.url,
                player_id = %player.id,
            );

            let playback_result = async { self.start_playback(&player, &track).await }
                .instrument(track_init_span)
                .await;

            let (cache_path, progress, cancel, init_seek) = match playback_result {
                Ok(result) => result,
                Err(_) => continue,
            };

            // Runs outside the init span - won't hold it open for the track's duration
            self.play_track(&player, cache_path, progress, cancel, init_seek)
                .await;

            tracks_played += 1;

            // Emit TrackEnded if the track finished naturally (not stopped)
            if !player.stop_requested.load(Ordering::Relaxed) {
                let guild_id = player.state.read().await.connection.guild_id;
                self.emit(bm_lib::model::mesastream::MesastreamEvent::TrackEnded {
                    guild_id,
                    player_id: player.id,
                    track_id: track.id.to_string(),
                });

                // Persist updated queue (track was just popped) but skip full snapshot
                // to avoid a Redis write per track - snapshot is written on destroy.
                let queue: Vec<Track> = player.state.read().await.queue.iter().cloned().collect();
                self.cache.set_queue_snapshot(guild_id, &queue).await;
            }

            if player.stop_requested.load(Ordering::Relaxed) {
                break;
            }
        }

        let was_stop_requested = player.stop_requested.load(Ordering::Relaxed);

        {
            let mut state = player.state.write().await;
            state.status = PlayerPlaybackStatus::Stopped;
            state.current_track = None;
        }
        player.set_position_ms(0);
        *player.playback_session_id.write().await = None;

        let snapshot = player.snapshot().await;
        self.cache.set_player_snapshot(&snapshot).await;

        // If the loop exited because the queue emptied (not an explicit stop),
        // auto-remove the player from the map.  Queue & position were already
        // persisted after the last track and in destroy_player, so the next
        // create_player call will restore them.
        if !was_stop_requested {
            let player_id = player.id;
            let guild_id = snapshot.connection.guild_id;
            self.players.write().await.remove(&player_id);
            // Persist queue so create_player can restore it later.
            self.cache
                .set_queue_snapshot(guild_id, &snapshot.queue)
                .await;
            info!(player_id = %player_id, "player auto-destroyed after queue emptied");

            self.emit(
                bm_lib::model::mesastream::MesastreamEvent::PlayerDestroyed {
                    guild_id,
                    player_id,
                    was_stopped: false,
                },
            );
        }
    }
}

/// Scans the audio cache directory and removes .opus files whose metadata
/// has expired from Redis.
#[instrument(skip(cache))]
async fn cleanup_orphaned_cache_files(cache: &RedisCache, cache_path: &str) -> AppResult<()> {
    let mut entries = tokio::fs::read_dir(cache_path)
        .await
        .map_err(|e| AppError::Internal(format!("failed to read cache directory: {e}")))?;

    let mut removed = 0;
    let mut checked = 0;

    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| AppError::Internal(format!("failed to read cache entry: {e}")))?
    {
        let path = entry.path();

        // Only process .opus cache files
        if path.extension().and_then(|s| s.to_str()) != Some("opus") {
            continue;
        }

        // Extract cache_key from filename (without .opus extension)
        let Some(cache_key) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };

        checked += 1;

        // If metadata expired in Redis, remove the orphaned cache file
        if cache.get_cache_metadata(cache_key).await.is_none() {
            // Metadata expired - remove the orphaned file
            if let Err(e) = tokio::fs::remove_file(&path).await {
                error!(error = %e, cache_key = %cache_key, "failed to remove orphaned cache file");
            } else {
                removed += 1;
                info!(cache_key = %cache_key, "removed orphaned cache file");
            }
        }
    }

    info!(
        checked = checked,
        removed = removed,
        "cache cleanup completed"
    );
    Ok(())
}
