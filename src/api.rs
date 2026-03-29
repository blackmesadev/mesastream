use actix_web::{web, HttpResponse, Scope};
use bm_lib::discord::Id;
use tracing::instrument;

use crate::{
    auth,
    cache::RedisCache,
    errors::AppResult,
    player::{
        CreatePlayerRequest, EnqueueRequest, EqualizerRequest, PlayerManager, SeekRequest,
        UpdateConnectionRequest, VolumeRequest,
    },
};

#[instrument(skip(manager, payload))]
async fn create_player(
    manager: web::Data<PlayerManager>,
    payload: web::Json<CreatePlayerRequest>,
) -> AppResult<HttpResponse> {
    let snapshot = manager.create_player(payload.into_inner()).await?;
    Ok(HttpResponse::Created().json(snapshot))
}

#[instrument(skip(manager))]
async fn list_players(manager: web::Data<PlayerManager>) -> AppResult<HttpResponse> {
    let snapshots = manager.list_players().await;
    Ok(HttpResponse::Ok().json(snapshots))
}

#[instrument(skip(manager))]
async fn get_player(
    manager: web::Data<PlayerManager>,
    path: web::Path<Id>,
) -> AppResult<HttpResponse> {
    let snapshot = manager.get_player(path.into_inner()).await?;
    Ok(HttpResponse::Ok().json(snapshot))
}

#[instrument(skip(manager, payload))]
async fn update_connection(
    manager: web::Data<PlayerManager>,
    path: web::Path<Id>,
    payload: web::Json<UpdateConnectionRequest>,
) -> AppResult<HttpResponse> {
    let snapshot = manager
        .update_connection(path.into_inner(), payload.into_inner())
        .await?;
    Ok(HttpResponse::Ok().json(snapshot))
}

#[instrument(skip(manager, payload))]
async fn enqueue(
    manager: web::Data<PlayerManager>,
    path: web::Path<Id>,
    payload: web::Json<EnqueueRequest>,
) -> AppResult<HttpResponse> {
    let snapshot = manager
        .enqueue(path.into_inner(), payload.into_inner())
        .await?;
    Ok(HttpResponse::Ok().json(snapshot))
}

#[instrument(skip(manager))]
async fn clear_queue(
    manager: web::Data<PlayerManager>,
    path: web::Path<Id>,
) -> AppResult<HttpResponse> {
    let snapshot = manager.clear_queue(path.into_inner()).await?;
    Ok(HttpResponse::Ok().json(snapshot))
}

#[instrument(skip(manager))]
async fn get_queue(
    manager: web::Data<PlayerManager>,
    path: web::Path<Id>,
) -> AppResult<HttpResponse> {
    let snapshot = manager.get_player(path.into_inner()).await?;
    Ok(HttpResponse::Ok().json(snapshot.queue))
}

#[instrument(skip(manager))]
async fn play(manager: web::Data<PlayerManager>, path: web::Path<Id>) -> AppResult<HttpResponse> {
    let snapshot = manager.play(path.into_inner()).await?;
    Ok(HttpResponse::Ok().json(snapshot))
}

#[instrument(skip(manager))]
async fn pause(manager: web::Data<PlayerManager>, path: web::Path<Id>) -> AppResult<HttpResponse> {
    let snapshot = manager.pause(path.into_inner()).await?;
    Ok(HttpResponse::Ok().json(snapshot))
}

#[instrument(skip(manager))]
async fn resume(manager: web::Data<PlayerManager>, path: web::Path<Id>) -> AppResult<HttpResponse> {
    let snapshot = manager.resume(path.into_inner()).await?;
    Ok(HttpResponse::Ok().json(snapshot))
}

#[instrument(skip(manager))]
async fn skip(manager: web::Data<PlayerManager>, path: web::Path<Id>) -> AppResult<HttpResponse> {
    let snapshot = manager.skip(path.into_inner()).await?;
    Ok(HttpResponse::Ok().json(snapshot))
}

#[instrument(skip(manager))]
async fn stop(manager: web::Data<PlayerManager>, path: web::Path<Id>) -> AppResult<HttpResponse> {
    let snapshot = manager.stop(path.into_inner()).await?;
    Ok(HttpResponse::Ok().json(snapshot))
}

#[instrument(skip(manager, payload))]
async fn seek(
    manager: web::Data<PlayerManager>,
    path: web::Path<Id>,
    payload: web::Json<SeekRequest>,
) -> AppResult<HttpResponse> {
    let snapshot = manager
        .seek(path.into_inner(), payload.into_inner())
        .await?;
    Ok(HttpResponse::Ok().json(snapshot))
}

#[instrument(skip(manager, payload))]
async fn volume(
    manager: web::Data<PlayerManager>,
    path: web::Path<Id>,
    payload: web::Json<VolumeRequest>,
) -> AppResult<HttpResponse> {
    let snapshot = manager
        .set_volume(path.into_inner(), payload.into_inner())
        .await?;
    Ok(HttpResponse::Ok().json(snapshot))
}

#[instrument(skip(manager, payload))]
async fn equalizer(
    manager: web::Data<PlayerManager>,
    path: web::Path<Id>,
    payload: web::Json<EqualizerRequest>,
) -> AppResult<HttpResponse> {
    let snapshot = manager
        .set_equalizer(path.into_inner(), payload.into_inner())
        .await?;
    Ok(HttpResponse::Ok().json(snapshot))
}

#[instrument(skip(manager))]
async fn current_track(
    manager: web::Data<PlayerManager>,
    path: web::Path<Id>,
) -> AppResult<HttpResponse> {
    let info = manager.current_track(path.into_inner()).await?;
    Ok(HttpResponse::Ok().json(info))
}

#[instrument(skip(manager))]
async fn status(manager: web::Data<PlayerManager>, path: web::Path<Id>) -> AppResult<HttpResponse> {
    let snapshot = manager.get_player(path.into_inner()).await?;
    Ok(HttpResponse::Ok().json(snapshot))
}

#[instrument(skip(manager))]
async fn destroy(
    manager: web::Data<PlayerManager>,
    path: web::Path<Id>,
) -> AppResult<HttpResponse> {
    manager.destroy_player(path.into_inner()).await?;
    Ok(HttpResponse::NoContent().finish())
}

#[instrument(skip(manager))]
async fn save_playlist(
    manager: web::Data<PlayerManager>,
    path: web::Path<(Id, String)>,
) -> AppResult<HttpResponse> {
    let (player_id, name) = path.into_inner();
    let playlist = manager.save_playlist(player_id, name).await?;
    Ok(HttpResponse::Ok().json(playlist))
}

#[instrument(skip(manager))]
async fn load_playlist(
    manager: web::Data<PlayerManager>,
    path: web::Path<(Id, String)>,
) -> AppResult<HttpResponse> {
    let (player_id, name) = path.into_inner();
    let snapshot = manager.load_playlist(player_id, name).await?;
    Ok(HttpResponse::Ok().json(snapshot))
}

async fn health() -> HttpResponse {
    auth::ok()
}

async fn ready(cache: web::Data<RedisCache>) -> HttpResponse {
    if cache.ping().await {
        HttpResponse::Ok().json(serde_json::json!({ "status": "ready" }))
    } else {
        HttpResponse::ServiceUnavailable().json(serde_json::json!({ "status": "degraded" }))
    }
}

pub fn routes() -> Scope {
    web::scope("")
        .route("/health", web::get().to(health))
        .route("/ready", web::get().to(ready))
        .service(
            web::scope("/players")
                .route("", web::post().to(create_player))
                .route("", web::get().to(list_players))
                .route("/{id}", web::get().to(get_player))
                .route("/{id}", web::delete().to(destroy))
                .route("/{id}/connection", web::put().to(update_connection))
                .route("/{id}/queue", web::post().to(enqueue))
                .route("/{id}/queue", web::get().to(get_queue))
                .route("/{id}/queue", web::delete().to(clear_queue))
                .route("/{id}/playlists/{name}", web::post().to(save_playlist))
                .route(
                    "/{id}/playlists/{name}/enqueue",
                    web::post().to(load_playlist),
                )
                .route("/{id}/play", web::post().to(play))
                .route("/{id}/pause", web::post().to(pause))
                .route("/{id}/resume", web::post().to(resume))
                .route("/{id}/skip", web::post().to(skip))
                .route("/{id}/stop", web::post().to(stop))
                .route("/{id}/seek", web::post().to(seek))
                .route("/{id}/volume", web::post().to(volume))
                .route("/{id}/equalizer", web::post().to(equalizer))
                .route("/{id}/current", web::get().to(current_track))
                .route("/{id}/status", web::get().to(status)),
        )
}
