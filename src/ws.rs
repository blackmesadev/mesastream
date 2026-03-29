//! WebSocket endpoint for broadcasting real-time player events to connected
//! clients (e.g. Black Mesa).
//!
//! Route: `GET /ws`
//!
//! The server sends [`MesastreamEvent`] JSON frames and periodic Ping frames
//! to detect dead clients.  Clients are expected to reply with Pong.
//! Client → server text messages are logged but currently unused.

use std::time::Duration;

use actix_web::{web, HttpRequest, HttpResponse};
use actix_ws::Message;
use bm_lib::model::mesastream::MesastreamEvent;
use futures_util::StreamExt;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

/// Interval at which the server sends Ping frames to clients.
const SERVER_PING_INTERVAL: Duration = Duration::from_secs(30);

/// Shared state injected into the actix-web app via `web::Data`.
pub type EventSender = broadcast::Sender<MesastreamEvent>;

/// WebSocket upgrade handler.
///
/// Subscribes to the global event broadcast channel and forwards every
/// [`MesastreamEvent`] to the client as a JSON text frame.  Sends periodic
/// server-side Ping frames for liveness detection.
pub async fn ws_handler(
    req: HttpRequest,
    body: web::Payload,
    sender: web::Data<EventSender>,
) -> Result<HttpResponse, actix_web::Error> {
    let (response, mut session, mut msg_stream) = actix_ws::handle(&req, body)?;

    let mut rx = sender.subscribe();
    let peer = req
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|| "unknown".into());
    info!(peer = %peer, "ws client connected");

    actix_web::rt::spawn(async move {
        let mut ping_interval = tokio::time::interval(SERVER_PING_INTERVAL);
        ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Skip the immediate first tick
        ping_interval.tick().await;

        loop {
            tokio::select! {
                // Forward broadcast events → client
                result = rx.recv() => {
                    match result {
                        Ok(event) => {
                            let json = match serde_json::to_string(&event) {
                                Ok(j) => j,
                                Err(e) => {
                                    warn!(error = %e, "failed to serialize ws event");
                                    continue;
                                }
                            };
                            if session.text(json).await.is_err() {
                                break; // client disconnected
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!(missed = n, "ws client lagged — dropped events");
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
                // Handle client → server messages
                msg = msg_stream.next() => {
                    match msg {
                        Some(Ok(Message::Ping(bytes))) => {
                            if session.pong(&bytes).await.is_err() {
                                break;
                            }
                        }
                        Some(Ok(Message::Text(text))) => {
                            debug!(peer = %peer, msg = %text, "ws client text message");
                        }
                        Some(Ok(Message::Close(_))) | None => break,
                        _ => {} // binary, pong — ignore
                    }
                }
                // Server-side heartbeat ping
                _ = ping_interval.tick() => {
                    if session.ping(b"").await.is_err() {
                        break; // client disconnected
                    }
                }
            }
        }

        info!(peer = %peer, "ws client disconnected");
        let _ = session.close(None).await;
    });

    Ok(response)
}
