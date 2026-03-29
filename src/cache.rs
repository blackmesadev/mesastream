use bm_lib::cache::{Cache as BmCache, RedisCache as BmRedisCache};
use bm_lib::discord::Id;
use std::sync::Arc;
use tracing::{error, instrument};

use crate::{
    errors::{AppError, AppResult},
    models::{PlayerStateSnapshot, PlaylistSnapshot, Track},
};

/// Async Redis cache wrapper for player state and queue metadata.
#[derive(Clone)]
pub struct RedisCache {
    cache: Arc<BmCache<BmRedisCache>>,
}

impl RedisCache {
    /// Establishes Redis connection manager and verifies startup connectivity.
    pub async fn connect(redis_uri: &str, prefix: &str) -> AppResult<Self> {
        let backend = BmRedisCache::new(redis_uri.to_string(), prefix.to_string())
            .await
            .map_err(|error| {
                AppError::ServiceUnavailable(format!("redis connection error: {error}"))
            })?;

        let cache = Arc::new(BmCache::new(backend));

        cache
            .ping()
            .await
            .map_err(|error| AppError::ServiceUnavailable(format!("redis ping failed: {error}")))?;

        Ok(Self { cache })
    }

    /// Stores serialized player snapshot in Redis.
    pub async fn set_player_snapshot(&self, snapshot: &PlayerStateSnapshot) {
        let key = format!("player:{}:state", snapshot.player_id);
        if let Err(error) = self.cache.set(key, snapshot, None).await {
            error!(error = %error, "failed to cache player snapshot");
        }
    }

    /// Stores queue metadata list scoped to a guild.
    pub async fn set_queue_snapshot(&self, guild_id: Id, tracks: &[Track]) {
        let key = format!("guild:{}:queue", guild_id);
        if let Err(error) = self.cache.set(key, &tracks, None).await {
            error!(error = %error, "failed to cache queue snapshot");
        }
    }

    /// Stores track metadata keyed by cache_key (SHA256) with TTL.
    /// When the metadata expires, the corresponding .msopus file should be cleaned up.
    #[instrument(skip(self, track), fields(cache_key = %cache_key, track_id = %track.id, ttl_secs, artist = %track.metadata.artist, title = %track.metadata.title))]
    pub async fn set_cache_metadata(&self, cache_key: &str, track: &Track, ttl_secs: u64) {
        let key = format!("cache:{}", cache_key);
        let ttl = std::time::Duration::from_secs(ttl_secs);
        if let Err(error) = self.cache.set(key, track, Some(ttl)).await {
            error!(error = %error, "failed to cache track metadata");
        }
    }

    /// Retrieves track metadata by cache_key.
    #[instrument(skip(self), fields(cache_key = %cache_key))]
    pub async fn get_cache_metadata(&self, cache_key: &str) -> Option<Track> {
        let key = format!("cache:{}", cache_key);
        let result = self.cache.get::<_, Track>(&key).await.ok().flatten();
        match result {
            Some(ref track) => {
                tracing::trace!(track_id = %track.id, "cache metadata hit");
            }
            None => {
                tracing::trace!("cache metadata miss");
            }
        }
        result
    }

    /// Stores playlist snapshot scoped to a guild.
    #[instrument(skip(self, playlist), fields(guild_id = %guild_id, playlist = %playlist.name))]
    pub async fn set_playlist(&self, guild_id: Id, playlist: &PlaylistSnapshot) {
        let key = format!("guild:{}:playlist:{}", guild_id, playlist.name);
        if let Err(error) = self.cache.set(key, playlist, None).await {
            error!(error = %error, "failed to cache playlist snapshot");
        }
    }

    /// Loads playlist snapshot scoped to a guild.
    pub async fn get_playlist(&self, guild_id: Id, name: &str) -> Option<PlaylistSnapshot> {
        let key = format!("guild:{}:playlist:{}", guild_id, name);
        self.cache
            .get::<_, PlaylistSnapshot>(&key)
            .await
            .ok()
            .flatten()
    }

    /// Fetch the persisted queue for a guild.
    /// Returns an empty vec when no queue is stored.
    #[instrument(skip(self), fields(guild_id = %guild_id))]
    pub async fn get_queue_snapshot(&self, guild_id: Id) -> Vec<Track> {
        let key = format!("guild:{guild_id}:queue");
        let result = self
            .cache
            .get::<_, Vec<Track>>(&key)
            .await
            .ok()
            .flatten()
            .unwrap_or_default();
        if !result.is_empty() {
            tracing::debug!(track_count = result.len(), "restored queue from cache");
        }
        result
    }

    /// Save the playback position for a guild.
    pub async fn set_guild_position(&self, guild_id: Id, position_ms: u64) {
        let key = format!("guild:{guild_id}:position");
        let ttl = std::time::Duration::from_secs(3600); // 1 hour TTL
        if let Err(e) = self.cache.set(&key, &position_ms, Some(ttl)).await {
            tracing::error!(error = %e, "failed to save guild position");
        }
    }

    /// Fetch the saved playback position for a guild.
    #[instrument(skip(self), fields(guild_id = %guild_id))]
    pub async fn get_guild_position(&self, guild_id: Id) -> Option<u64> {
        let key = format!("guild:{guild_id}:position");
        let result = self.cache.get::<_, u64>(&key).await.ok().flatten();
        if let Some(pos) = result {
            tracing::debug!(position_ms = pos, "restored position from cache");
        }
        result
    }

    pub async fn ping(&self) -> bool {
        self.cache.ping().await.is_ok()
    }
}
