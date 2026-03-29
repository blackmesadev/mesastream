use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicBool, AtomicI64, Ordering},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::UdpSocket;
use tokio::time::{interval, Duration, Instant};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, instrument, warn};

/// Returns the current time as milliseconds since the Unix epoch.
/// Used as the `t` nonce in v8 heartbeat payloads.
#[inline]
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Build a v8 heartbeat payload, including `seq_ack` when a sequenced message
/// has been received.  If `last_seq` is negative (i.e. no seq received yet),
/// the field is omitted per the spec.
#[inline]
fn heartbeat_payload(last_seq: i64) -> Value {
    if last_seq >= 0 {
        json!({ "op": OP_HEARTBEAT, "d": { "t": now_ms(), "seq_ack": last_seq } })
    } else {
        json!({ "op": OP_HEARTBEAT, "d": { "t": now_ms() } })
    }
}

use crate::{
    dave::{
        transition_id, transition_ready, DaveEncryptor, DaveHandshake, DAVE_PROTOCOL_VERSION,
        OP_DAVE_EXECUTE_TRANSITION, OP_DAVE_MLS_ANNOUNCE_COMMIT, OP_DAVE_MLS_COMMIT_WELCOME,
        OP_DAVE_MLS_EXTERNAL_SENDER, OP_DAVE_MLS_INVALID_COMMIT_WELCOME, OP_DAVE_MLS_KEY_PACKAGE,
        OP_DAVE_MLS_PROPOSALS, OP_DAVE_MLS_WELCOME, OP_DAVE_PREPARE_EPOCH,
        OP_DAVE_PREPARE_TRANSITION, OP_DAVE_READY_FOR_TRANSITION,
    },
    errors::{AppError, AppResult},
    models::VoiceBridgePayloadDto,
};

// Discord Voice Gateway opcodes
const OP_IDENTIFY: u8 = 0;
const OP_SELECT_PROTOCOL: u8 = 1;
const OP_READY: u8 = 2;
const OP_HEARTBEAT: u8 = 3;
const OP_SESSION_DESCRIPTION: u8 = 4;
const OP_SPEAKING: u8 = 5;
const OP_HEARTBEAT_ACK: u8 = 6;
const OP_HELLO: u8 = 8;
const OP_RESUMED: u8 = 9;
const OP_CLIENTS_CONNECT: u8 = 11;
const OP_CLIENT_DISCONNECT: u8 = 13;

/// Encryption modes supported by Mesastream, in preference order.
/// Discord no longer accepts the legacy xsalsa20_poly1305 family; these
/// are the only modes it currently advertises in the Ready payload.
const PREFERRED_MODES: &[&str] = &["aead_xchacha20_poly1305_rtpsize", "aead_aes256_gcm_rtpsize"];

/// Outcome of a successful voice gateway handshake.
pub struct VoiceGatewaySession {
    pub ssrc: u32,
    pub server_addr: SocketAddr,
    pub secret_key: [u8; 32],
    /// Negotiated RTP encryption mode, e.g. `"aead_xchacha20_poly1305_rtpsize"`.
    pub encryption_mode: String,
    /// Per-frame DAVE encryptor, present when the channel has E2EE enabled.
    pub dave: Option<DaveEncryptor>,
    /// Becomes `false` when the voice-gateway WebSocket keepalive task exits
    /// (Discord closed the session, network error, etc.).  The UDP transport is
    /// dead once this is false — Discord silently drops any RTP we send.
    pub ws_alive: Arc<AtomicBool>,
    /// Sender for Speaking state updates. Send `true` to signal speaking, `false` for silent.
    pub speaking_tx: tokio::sync::mpsc::UnboundedSender<bool>,
}

/// Timeout applied to each individual voice-gateway handshake step.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Grace period after sending DAVE `ready_for_transition` before allowing
/// handshake completion even if `execute_transition` is not observed.
const DAVE_EXECUTE_TRANSITION_GRACE: Duration = Duration::from_millis(300);
/// After `SessionDescription` is received with DAVE active, allow this long for
/// the MLS handshake to produce material before falling back without E2EE.
/// This prevents the full 30s outer timeout when Discord's MLS path stalls.
const DAVE_MLS_TIMEOUT: Duration = Duration::from_secs(5);

/// Connect to the Discord Voice Gateway, perform the full handshake, and return
/// the negotiated session details needed to build a `VoiceUdpConnection`.
///
/// Steps per the Discord Voice Gateway v8 + DAVE spec v1.1.4:
///  1. WS connect → receive Hello (op 8) → start heartbeat (v8 format with seq_ack)
///  2. Send Identify (op 0) with `max_dave_protocol_version = 1`
///  3. Receive Ready (op 2)
///  4. IP Discovery via UDP
///  5. Send Select Protocol (op 1) with `dave_protocol_version`
///  6. Receive Session Description (op 4); `d.dave_protocol_version` signals DAVE active.
///     If DAVE: send binary KeyPackage (op 26), await binary Welcome (op 30).
///  6b. If DAVE: process Welcome → send ready_for_transition (op 23) → DaveEncryptor
#[instrument(skip(payload), fields(guild_id = %payload.guild_id, user_id = %payload.user_id))]
pub async fn connect(payload: &VoiceBridgePayloadDto) -> AppResult<VoiceGatewaySession> {
    info!(gateway_url = %payload.gateway_url, "connecting to Discord Voice Gateway");

    let ws_connect_started = Instant::now();
    let (mut ws, _) = tokio::time::timeout(HANDSHAKE_TIMEOUT, connect_async(&payload.gateway_url))
        .await
        .map_err(|_| AppError::ServiceUnavailable("voice gateway connect timed out".into()))?
        .map_err(|e| AppError::ServiceUnavailable(format!("voice gateway connect failed: {e}")))?;
    debug!(
        elapsed_ms = ws_connect_started.elapsed().as_millis(),
        "voice gateway websocket connected"
    );

    // Hello
    let hello_started = Instant::now();
    let heartbeat_interval_ms = tokio::time::timeout(HANDSHAKE_TIMEOUT, recv_hello(&mut ws))
        .await
        .map_err(|_| AppError::ServiceUnavailable("voice gateway Hello timed out".into()))??;
    let heartbeat_interval = Duration::from_millis(heartbeat_interval_ms);
    debug!(
        heartbeat_interval_ms,
        elapsed_ms = hello_started.elapsed().as_millis(),
        "received Hello from voice gateway"
    );

    // Identify
    let identify = json!({
        "op": OP_IDENTIFY,
        "d": {
            "server_id":  payload.guild_id.to_string(),
            "user_id":    payload.user_id.to_string(),
            "session_id": payload.session_id,
            "token":      payload.token,
            "max_dave_protocol_version": DAVE_PROTOCOL_VERSION,
        }
    });
    ws.send(Message::Text(identify.to_string().into()))
        .await
        .map_err(|e| {
            AppError::ServiceUnavailable(format!("voice gateway identify send failed: {e}"))
        })?;

    debug!("sent Identify to voice gateway");

    // Ready
    let ready_started = Instant::now();
    let (ssrc, udp_ip, udp_port, server_modes) =
        tokio::time::timeout(HANDSHAKE_TIMEOUT, async {
            let mut last_seq: i64 = -1; // v8 seq_ack tracking
            let mut hb_ticker = interval(heartbeat_interval);
            hb_ticker.tick().await; // consume instant first tick
            loop {
                tokio::select! {
                    _ = hb_ticker.tick() => {
                        let hb = heartbeat_payload(last_seq);
                        let _ = ws.send(Message::Text(hb.to_string().into())).await;
                    }
                    msg = ws.next() => {
                        let msg = msg
                            .ok_or_else(|| AppError::ServiceUnavailable("voice gateway closed before Ready".into()))?
                            .map_err(|e| AppError::ServiceUnavailable(format!("voice gateway ws error: {e}")))?;

                        match msg {
                            Message::Close(frame) => {
                                let reason = close_reason(frame.as_ref());
                                return Err(AppError::ServiceUnavailable(
                                    format!("voice gateway closed before Ready: {reason}"),
                                ));
                            }
                            Message::Text(text) => {
                                let v: Value = serde_json::from_str(&text)
                                    .map_err(|e| AppError::Internal(format!("voice gateway parse error: {e}")))?;

                                if let Some(seq) = v["seq"].as_i64() {
                                    last_seq = last_seq.max(seq);
                                }

                                match v["op"].as_u64().unwrap_or(255) as u8 {
                                    OP_READY => {
                                        let d = &v["d"];
                                        let ssrc = d["ssrc"].as_u64()
                                            .ok_or_else(|| AppError::Internal("Ready missing ssrc".into()))? as u32;
                                        let ip = d["ip"].as_str()
                                            .ok_or_else(|| AppError::Internal("Ready missing ip".into()))?.to_owned();
                                        let port = d["port"].as_u64()
                                            .ok_or_else(|| AppError::Internal("Ready missing port".into()))? as u16;
                                        let modes: Vec<String> = d["modes"]
                                            .as_array()
                                            .unwrap_or(&vec![])
                                            .iter()
                                            .filter_map(|m| m.as_str().map(str::to_owned))
                                            .collect();
                                        debug!(ssrc, ip = %ip, port, ?modes, "received Ready from voice gateway");
                                        break Ok((ssrc, ip, port, modes));
                                    }
                                    OP_RESUMED => debug!("voice gateway resumed"),
                                    op => debug!(op, "JSON op received before Ready (ignoring)"),
                                }
                            }
                            Message::Binary(data) if data.len() >= 3 => {
                                debug!(dave_op = data[2], len = data.len(), "binary op received before Ready");
                            }
                            _ => {}
                        }
                    }
                }
            }
        })
        .await
        .map_err(|_| AppError::ServiceUnavailable("voice gateway Ready timed out".into()))??;
    debug!(
        elapsed_ms = ready_started.elapsed().as_millis(),
        ssrc, "voice gateway Ready phase complete"
    );

    let server_addr: SocketAddr = format!("{udp_ip}:{udp_port}")
        .parse()
        .map_err(|e| AppError::Internal(format!("invalid voice server addr: {e}")))?;

    // IP Discovery
    let ip_discovery_started = Instant::now();
    let (external_ip, external_port) = ip_discovery(server_addr, ssrc).await?;
    debug!(external_ip = %external_ip, external_port, elapsed_ms = ip_discovery_started.elapsed().as_millis(), "IP discovery complete");

    // Select Protocol
    //
    // Negotiate the best encryption mode from what Discord advertised in Ready.
    // Discord has deprecated all xsalsa20_poly1305 variants; current modes are
    // aead_xchacha20_poly1305_rtpsize and aead_aes256_gcm_rtpsize.
    let encryption_mode = PREFERRED_MODES
        .iter()
        .find(|&&m| server_modes.iter().any(|s| s == m))
        .copied()
        .ok_or_else(|| {
            AppError::Internal(format!(
                "no supported encryption mode in server list: {:?}",
                server_modes
            ))
        })?;
    debug!(encryption_mode, "negotiated encryption mode");

    // In v8 on DAVE-required channels, SelectProtocol must include
    // `dave_protocol_version` at the top level of `d` (not inside `data`).
    // Omitting it causes Discord to close with 4006 immediately.
    // Note: on v4 this field caused 4020, so it must only be sent on v8+.
    let select = json!({
        "op": OP_SELECT_PROTOCOL,
        "d": {
            "protocol": "udp",
            "data": {
                "address": external_ip,
                "port":    external_port,
                "mode":    encryption_mode,
            },
            "dave_protocol_version": DAVE_PROTOCOL_VERSION,
        }
    });
    debug!(payload = %select, "sending Select Protocol");
    let select_started = Instant::now();
    ws.send(Message::Text(select.to_string().into()))
        .await
        .map_err(|e| {
            AppError::ServiceUnavailable(format!("voice gateway select_protocol send failed: {e}"))
        })?;

    debug!(
        elapsed_ms = select_started.elapsed().as_millis(),
        "sent Select Protocol to voice gateway"
    );

    // SessionDescription + DAVE handshake
    //
    // Two DAVE paths after SessionDescription (op 4):
    //
    //  A) Sole/first member — server sends op 25 (ExternalSender) + op 27
    //     (Proposals) expecting the client to commit.  Client creates a fresh
    //     MLS group, self-update-commits, and sends op 28 (CommitWelcome).
    //     No op 30 Welcome arrives; the encryptor is derived from the new epoch.
    //
    //  B) Joining an existing group — an existing member commits and Discord
    //     delivers op 30 (Welcome) directly to the new joiner.  Client calls
    //     MlsGroup::new_from_welcome to join and derive the encryptor.
    //
    // Binary server→client framing: [seq_hi][seq_lo][opcode][body...].
    // For op 30 Welcome body:        [tid_hi][tid_lo][Welcome TLS bytes...].
    let loop2_timeout = HANDSHAKE_TIMEOUT * 3;
    let session_description_started = Instant::now();
    let (secret_key, dave_handshake_out, dave_welcome_out, dave_encryptor_initial) =
        tokio::time::timeout(loop2_timeout, async {
            let mut last_seq: i64 = -1; // v8 seq_ack tracking
            let mut hb_ticker = interval(heartbeat_interval);
            let mut handshake_progress_ticker = interval(Duration::from_millis(100));
            hb_ticker.tick().await;
            handshake_progress_ticker.tick().await;
            let mut session_key: Option<[u8; 32]> = None;
            let mut session_key_received_at: Option<Instant> = None;
            let mut dave_version: u8 = 0;
            let mut dave_handshake: Option<DaveHandshake> = None;
            let mut dave_welcome: Option<(Vec<u8>, u16)> = None; // (bytes, transition_id)
            let mut dave_encryptor_inner: Option<DaveEncryptor> = None;
            let mut dave_prepare_transition_id: Option<u16> = None;
            let mut dave_execute_transition_id: Option<u16> = None;
            let mut dave_ready_sent = false;
            let mut dave_ready_tid: Option<u16> = None;
            let mut dave_ready_sent_at: Option<Instant> = None;
            let mut dave_transition_timeout_warned = false;

            loop {
                let dave_material_ready = dave_welcome.is_some() || dave_encryptor_inner.is_some();
                if dave_version > 0 && dave_material_ready && !dave_ready_sent {
                    let ready_tid = dave_prepare_transition_id
                        .or_else(|| dave_welcome.as_ref().map(|(_, tid)| *tid))
                        .or_else(|| dave_encryptor_inner.as_ref().map(|_| 0u16));

                    if let Some(tid) = ready_tid {
                        let ack = json!({"op": OP_DAVE_READY_FOR_TRANSITION, "d": {"transition_id": tid}});
                        match ws.send(Message::Text(ack.to_string().into())).await {
                            Ok(_) => {
                                dave_ready_sent = true;
                                dave_ready_tid = Some(tid);
                                dave_ready_sent_at = Some(Instant::now());
                                debug!(tid, "DAVE ready_for_transition sent (op 23)");
                            }
                            Err(e) => {
                                return Err(AppError::ServiceUnavailable(
                                    format!("voice gateway ready_for_transition send failed: {e}"),
                                ));
                            }
                        }
                    }
                }

                let dave_transition_ready = transition_ready(dave_ready_tid, dave_execute_transition_id);
                let dave_transition_grace_elapsed = dave_ready_sent_at
                    .map(|at| at.elapsed() >= DAVE_EXECUTE_TRANSITION_GRACE)
                    .unwrap_or(false);

                if dave_version > 0
                    && dave_material_ready
                    && dave_ready_sent
                    && !dave_transition_ready
                    && dave_transition_grace_elapsed
                    && !dave_transition_timeout_warned
                {
                    dave_transition_timeout_warned = true;
                    warn!(
                        ready_tid = ?dave_ready_tid,
                        execute_tid = ?dave_execute_transition_id,
                        wait_ms = dave_ready_sent_at.map(|at| at.elapsed().as_millis()).unwrap_or(0),
                        "DAVE execute_transition not observed within grace period; proceeding with established encryptor"
                    );
                }

                // Safety net: if SessionDescription was received but MLS material
                // never arrived, fall back gracefully rather than waiting the full
                // 30s outer timeout (e.g. path A where proposals produce no commit,
                // or Discord stalls the MLS exchange on endpoint updates).
                let dave_mls_stalled = session_key_received_at
                    .map(|at| dave_version > 0 && !dave_material_ready && at.elapsed() >= DAVE_MLS_TIMEOUT)
                    .unwrap_or(false);
                if dave_mls_stalled {
                    warn!(
                        elapsed_ms = session_key_received_at.map(|at| at.elapsed().as_millis()).unwrap_or(0),
                        "DAVE MLS material not received within timeout; proceeding without E2EE"
                    );
                }

                let dave_handshake_complete = dave_version == 0
                    || dave_mls_stalled
                    || (dave_material_ready
                        && dave_ready_sent
                        && (dave_transition_ready || dave_transition_grace_elapsed));

                if session_key.is_some() && dave_handshake_complete {
                    break Ok::<_, AppError>((
                        session_key.take().unwrap(),
                        dave_handshake,
                        dave_welcome,
                        dave_encryptor_inner,
                    ));
                }

                tokio::select! {
                    _ = handshake_progress_ticker.tick() => {
                        // Periodic wakeup so DAVE grace checks do not wait for
                        // long heartbeat intervals when no inbound messages arrive.
                    }
                    _ = hb_ticker.tick() => {
                        let hb = heartbeat_payload(last_seq);
                        let _ = ws.send(Message::Text(hb.to_string().into())).await;
                    }
                    msg = ws.next() => {
                        let msg = msg
                            .ok_or_else(|| AppError::ServiceUnavailable("voice gateway closed before SessionDescription".into()))?
                            .map_err(|e| AppError::ServiceUnavailable(format!("voice gateway ws error: {e}")))?;

                        match msg {
                            Message::Close(frame) => {
                                let reason = close_reason(frame.as_ref());
                                return Err(AppError::ServiceUnavailable(
                                    format!("voice gateway closed before SessionDescription: {reason}"),
                                ));
                            }

                            Message::Text(text) => {
                                let v: Value = serde_json::from_str(&text)
                                    .map_err(|e| AppError::Internal(format!("voice gateway parse error: {e}")))?;

                                if let Some(seq) = v["seq"].as_i64() {
                                    last_seq = last_seq.max(seq);
                                }

                                match v["op"].as_u64().unwrap_or(255) as u8 {
                                    OP_SESSION_DESCRIPTION => {
                                        let d = &v["d"];

                                        // Extract transport-layer secret key (always present).
                                        let raw = d["secret_key"]
                                            .as_array()
                                            .ok_or_else(|| AppError::Internal("SessionDescription missing secret_key".into()))?;
                                        let mut key = [0u8; 32];
                                        for (i, b) in key.iter_mut().take(32).enumerate() {
                                            *b = raw.get(i).and_then(|v| v.as_u64()).unwrap_or(0) as u8;
                                        }
                                        session_key = Some(key);
                                        session_key_received_at = Some(Instant::now());

                                        // DAVE spec: dave_protocol_version in the select_protocol_ack.
                                        if let Some(ver) = d["dave_protocol_version"].as_u64() {
                                            dave_version = ver as u8;
                                        }

                                        if dave_version > 0 {
                                            match DaveHandshake::new(payload.user_id.get(), payload.channel_id.get()) {
                                                Ok((hs, kp_bytes)) => {
                                                    // Binary format: [op_byte = 26][TLS-MLSMessage bytes]
                                                    let mut bin = Vec::with_capacity(1 + kp_bytes.len());
                                                    bin.push(OP_DAVE_MLS_KEY_PACKAGE);
                                                    bin.extend_from_slice(&kp_bytes);
                                                    match ws.send(Message::Binary(bin.into())).await {
                                                        Ok(_) => {
                                                            dave_handshake = Some(hs);
                                                        }
                                                        Err(e) => {
                                                            warn!("DAVE KeyPackage send failed: {e} — E2EE unavailable");
                                                            dave_version = 0; // fall back
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    warn!("DAVE KeyPackage generation failed: {e} — E2EE unavailable");
                                                    dave_version = 0;
                                                }
                                            }
                                        } else {
                                            debug!("received Session Description (no DAVE)");
                                        }
                                    }

                                    // JSON control opcodes from DAVE spec §"Voice Gateway Opcodes"
                                    OP_DAVE_PREPARE_TRANSITION => {
                                        let _tid = v["d"]["transition_id"].as_u64().unwrap_or(99999);
                                        dave_prepare_transition_id = transition_id(&v["d"]["transition_id"]);
                                        // The previous code for tid == 0 has been removed as it is now handled in the loop.
                                    }
                                    OP_DAVE_EXECUTE_TRANSITION => {
                                        let _tid = v["d"]["transition_id"].as_u64().unwrap_or(0);
                                        dave_execute_transition_id = transition_id(&v["d"]["transition_id"]);
                                    }
                                    OP_DAVE_PREPARE_EPOCH => {
                                        let epoch = v["d"]["epoch"].as_u64().unwrap_or(0);
                                        let _ver   = v["d"]["protocol_version"].as_u64().unwrap_or(0);
                                        if epoch == 1 && dave_version > 0 {
                                            warn!("DAVE prepare_epoch epoch=1 (group re-creation) — not yet handled");
                                        }
                                    }
                                    OP_DAVE_MLS_INVALID_COMMIT_WELCOME => {
                                        let tid = v["d"]["transition_id"].as_u64().unwrap_or(0);
                                        warn!(tid, "DAVE invalid_commit_welcome — local MLS reset needed");
                                    }

                                    // Routine voice gateway status ops — safe to ignore.
                                    OP_SPEAKING | OP_CLIENTS_CONNECT | OP_CLIENT_DISCONNECT => {}

                                    // Op 6 = HeartbeatAck (server echoes heartbeat nonce).
                                    OP_HEARTBEAT_ACK => {}

                                    // Op 18 = voice_backend_version (undocumented); op 20 = unknown platform info.
                                    // Op 15 = undocumented Discord-internal message.
                                    15 | 18 => debug!(op = ?v["op"], "undocumented Discord voice op"),
                                    20 => debug!(platform = ?v["d"]["platform"], "platform_info (op 20)"),

                                    op => {
                                        warn!(op, msg = %text.chars().take(200).collect::<String>(), "unhandled JSON op");
                                    }
                                }
                            }

                            Message::Binary(data) => {
                                // Binary DAVE messages: [seq_hi][seq_lo][opcode][body...]
                                if data.len() < 3 {
                                    debug!(len = data.len(), "short binary message");
                                    continue;
                                }
                                let dave_op = data[2];
                                let body    = &data[3..];

                                match dave_op {
                                    OP_DAVE_MLS_WELCOME => {
                                        // body = [tid_hi][tid_lo][Welcome TLS bytes]
                                        if body.len() >= 2 {
                                            let tid = u16::from_be_bytes([body[0], body[1]]);
                                            let welcome_bytes = body[2..].to_vec();
                                            if let Some(hs) = dave_handshake.as_ref() {
                                                match hs.process_welcome(&welcome_bytes) {
                                                    Ok(()) => {
                                                        dave_welcome = Some((welcome_bytes, tid));
                                                        if let Ok(true) = hs.is_ready() {
                                                            if let Ok(enc) = hs.encryptor() {
                                                                dave_encryptor_inner = Some(enc);
                                                            }
                                                        }
                                                    }
                                                    Err(e) => {
                                                        warn!("DAVE process_welcome failed: {e}");
                                                    }
                                                }
                                            } else {
                                                dave_welcome = Some((welcome_bytes, tid));
                                            }
                                        } else {
                                            warn!("DAVE Welcome body too short ({} bytes)", body.len());
                                        }
                                    }
                                    OP_DAVE_MLS_ANNOUNCE_COMMIT => {
                                        // body = [tid_hi][tid_lo][MLSMessage commit...]
                                        if body.len() >= 2 {
                                            if let Some(hs) = dave_handshake.as_ref() {
                                                if let Err(e) = hs.process_commit(&body[2..]) {
                                                    warn!("DAVE process_commit failed: {e}");
                                                } else if let Ok(true) = hs.is_ready() {
                                                    if let Ok(enc) = hs.encryptor() {
                                                        dave_encryptor_inner = Some(enc);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    OP_DAVE_MLS_EXTERNAL_SENDER => {
                                        if let Some(hs) = dave_handshake.as_ref() {
                                            if let Err(e) = hs.set_external_sender(body) {
                                                warn!("DAVE set_external_sender failed: {e}");
                                            }
                                        }
                                    }
                                    OP_DAVE_MLS_PROPOSALS => {
                                        if body.is_empty() {
                                            warn!("DAVE proposals body too short");
                                        } else if let Some(hs) = dave_handshake.as_ref() {
                                            let op_type = body[0];
                                            let proposals_payload = &body[1..];
                                            match hs.process_proposals_payload(op_type, proposals_payload) {
                                                Ok(Some(commit_welcome_bytes)) => {
                                                    let mut bin = Vec::with_capacity(1 + commit_welcome_bytes.len());
                                                    bin.push(OP_DAVE_MLS_COMMIT_WELCOME);
                                                    bin.extend_from_slice(&commit_welcome_bytes);
                                                    match ws.send(Message::Binary(bin.into())).await {
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            warn!("DAVE op 28 send failed: {e} — E2EE unavailable");
                                                        }
                                                    }
                                                    // Path A (sole member): after sending CommitWelcome the
                                                    // MLS session is complete — derive the encryptor now.
                                                    // Without this, `dave_material_ready` stays false and
                                                    // the loop spins until the 30s outer timeout fires.
                                                    if dave_encryptor_inner.is_none() {
                                                        if let Ok(true) = hs.is_ready() {
                                                            match hs.encryptor() {
                                                                Ok(enc) => {
                                                                    debug!("DAVE encryptor derived from CommitWelcome (path A)");
                                                                    dave_encryptor_inner = Some(enc);
                                                                }
                                                                Err(e) => warn!("DAVE encryptor unavailable after CommitWelcome: {e}"),
                                                            }
                                                        }
                                                    }
                                                }
                                                Ok(None) => {
                                                    debug!("DAVE proposals produced no commit (nothing to send)");
                                                }
                                                Err(e) => {
                                                    warn!("DAVE process_proposals failed: {e} — E2EE unavailable");
                                                }
                                            }
                                        }
                                    }
                                    op => {
                                        let hex: String = body.iter().take(16)
                                            .map(|b| format!("{b:02x}"))
                                            .collect::<Vec<_>>().join(" ");
                                        warn!(op, total_len = data.len(), first_bytes = %hex, "unhandled binary DAVE op");
                                    }
                                }
                            }

                            _ => {}
                        }
                    }
                }
            }
        })
        .await
        .map_err(|_| AppError::ServiceUnavailable("voice gateway SessionDescription timed out".into()))??;
    debug!(
        elapsed_ms = session_description_started.elapsed().as_millis(),
        "voice gateway SessionDescription/DAVE phase complete"
    );

    // Complete DAVE handshake
    //
    // DAVE session events are processed during step 6.  At this point we only
    // need to carry forward an already-ready encryptor (if any).
    //
    // `ready_for_transition` (op 23) is sent during step 6 as soon as MLS
    // material is ready. Step 6 may proceed after `execute_transition` (op 22)
    // or after a short grace period when op 22 is not emitted by Discord.
    let dave_encryptor = if let Some(enc) = dave_encryptor_initial {
        Some(enc)
    } else {
        if dave_handshake_out.is_some() || dave_welcome_out.is_some() {
            warn!("DAVE handshake did not reach ready state; continuing without DAVE encryptor");
        }
        None
    };

    // Create channel for Speaking state updates.
    let (speaking_tx, mut speaking_rx) = tokio::sync::mpsc::unbounded_channel::<bool>();

    // Keep the WS alive in a background task and handle Speaking updates.
    let last_seq_for_task = Arc::new(AtomicI64::new(-1));
    let ws_alive = Arc::new(AtomicBool::new(true));
    let ws_alive_task = Arc::clone(&ws_alive);
    tokio::spawn(async move {
        let mut ticker = interval(heartbeat_interval);
        // Split the WS so we can read and write independently.
        let (mut ws_tx, mut ws_rx) = ws.split();

        loop {
            tokio::select! {
                biased;

                // Inbound WS messages — process seq_ack updates and close frames.
                msg = ws_rx.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Ok(v) = serde_json::from_str::<Value>(&text) {
                                if let Some(seq) = v["seq"].as_i64() {
                                    last_seq_for_task.fetch_max(seq, Ordering::Relaxed);
                                }
                            }
                        }
                        Some(Ok(Message::Close(frame))) => {
                            let code = frame.as_ref().map(|f| u16::from(f.code)).unwrap_or(0);
                            let reason = close_reason(frame.as_ref());
                            match code {
                                4006 | 4009 | 4014 | 4015 => debug!(reason = %reason, "voice gateway closed by server"),
                                _ => warn!(reason = %reason, "voice gateway closed by server"),
                            }
                            ws_alive_task.store(false, Ordering::Relaxed);
                            return;
                        }
                        Some(Err(e)) => {
                            warn!(error = %e, "voice gateway WS error in keepalive");
                            ws_alive_task.store(false, Ordering::Relaxed);
                            return;
                        }
                        None => {
                            debug!("voice gateway WS stream ended");
                            ws_alive_task.store(false, Ordering::Relaxed);
                            return;
                        }
                        _ => {}
                    }
                }

                // Heartbeat timer
                _ = ticker.tick() => {
                    let hb = heartbeat_payload(last_seq_for_task.load(Ordering::Relaxed));
                    if ws_tx.send(Message::Text(hb.to_string().into())).await.is_err() {
                        break;
                    }
                }

                // Speaking state updates from the player
                Some(speaking) = speaking_rx.recv() => {
                    let speaking_value = if speaking { 1 } else { 0 };
                    debug!("sending speaking state to Discord: speaking={} ssrc={}", speaking, ssrc);
                    let speaking_msg = json!({
                        "op": OP_SPEAKING,
                        "d": { "speaking": speaking_value, "delay": 0, "ssrc": ssrc }
                    });
                    if ws_tx.send(Message::Text(speaking_msg.to_string().into())).await.is_err() {
                        warn!("failed to send speaking state via websocket");
                        break;
                    }
                }
            }
        }
        warn!("voice gateway keepalive stopped (heartbeat send failed) — transport is dead");
        ws_alive_task.store(false, Ordering::Relaxed);
    });

    Ok(VoiceGatewaySession {
        ssrc,
        server_addr,
        secret_key,
        encryption_mode: encryption_mode.to_owned(),
        dave: dave_encryptor,
        ws_alive,
        speaking_tx,
    })
}

/// Formats a WebSocket close frame into a human-readable string including the
/// Discord-specific close code where known.
fn close_reason(frame: Option<&tokio_tungstenite::tungstenite::protocol::CloseFrame>) -> String {
    let Some(f) = frame else {
        return "no close frame".to_string();
    };
    let code = u16::from(f.code);
    let description = match code {
        4001 => "unknown opcode",
        4002 => "failed to decode payload",
        4003 => "not authenticated",
        4004 => "authentication failed",
        4005 => "already authenticated",
        4006 => "session no longer valid (stale voice token)",
        4009 => "session timed out",
        4011 => "server not found",
        4012 => "unknown protocol",
        4014 => "disconnected (channel deleted or bot kicked)",
        4015 => "voice server crashed",
        4016 => "unknown encryption mode",
        4017 => "E2EE/DAVE protocol required",
        4020 => "Bad Request (malformed payload)",
        _ => "unknown",
    };
    format!("code {code} ({description}): {}", f.reason)
}

/// Read the first Hello frame (op 8) and return `heartbeat_interval` in ms.
async fn recv_hello(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> AppResult<u64> {
    loop {
        let msg = ws
            .next()
            .await
            .ok_or_else(|| {
                AppError::ServiceUnavailable("voice gateway closed before Hello".into())
            })?
            .map_err(|e| AppError::ServiceUnavailable(format!("voice gateway ws error: {e}")))?;

        if let Message::Text(text) = msg {
            let v: Value = serde_json::from_str(&text)
                .map_err(|e| AppError::Internal(format!("voice gateway parse error: {e}")))?;

            if v["op"].as_u64().unwrap_or(255) as u8 == OP_HELLO {
                // Discord sends heartbeat_interval as a float (e.g. 41.25ms),
                // so as_u64() silently returns None. Use as_f64() and truncate.
                let interval = v["d"]["heartbeat_interval"]
                    .as_f64()
                    .ok_or_else(|| AppError::Internal("Hello missing heartbeat_interval".into()))?
                    as u64;
                return Ok(interval);
            }
        }
    }
}

/// Perform UDP IP Discovery per the Discord Voice protocol.
///
/// Sends a 74-byte discovery packet to the voice server and reads the response
/// containing our external IP and port.
async fn ip_discovery(server_addr: SocketAddr, ssrc: u32) -> AppResult<(String, u16)> {
    let socket = UdpSocket::bind("0.0.0.0:0")
        .await
        .map_err(|e| AppError::ServiceUnavailable(format!("ip discovery bind failed: {e}")))?;
    socket
        .connect(server_addr)
        .await
        .map_err(|e| AppError::ServiceUnavailable(format!("ip discovery connect failed: {e}")))?;

    // Build 74-byte discovery request.
    let mut packet = [0u8; 74];
    packet[0] = 0x00;
    packet[1] = 0x01; // type: request
    packet[2] = 0x00;
    packet[3] = 0x46; // length: 70
    packet[4..8].copy_from_slice(&ssrc.to_be_bytes());
    // bytes 8..72 remain 0 (null-padded address)
    // bytes 72..74 remain 0 (port)

    socket
        .send(&packet)
        .await
        .map_err(|e| AppError::ServiceUnavailable(format!("ip discovery send failed: {e}")))?;

    let mut buf = [0u8; 74];
    tokio::time::timeout(Duration::from_secs(5), socket.recv(&mut buf))
        .await
        .map_err(|_| AppError::ServiceUnavailable("ip discovery timed out".into()))?
        .map_err(|e| AppError::ServiceUnavailable(format!("ip discovery recv failed: {e}")))?;

    // Response: bytes 8..72 = null-terminated external IP, bytes 72..74 = port.
    let ip_bytes = &buf[8..72];
    let null_pos = ip_bytes
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(ip_bytes.len());
    let ip = std::str::from_utf8(&ip_bytes[..null_pos])
        .map_err(|e| AppError::Internal(format!("ip discovery invalid ip encoding: {e}")))?
        .to_owned();
    let port = u16::from_be_bytes([buf[72], buf[73]]);

    Ok((ip, port))
}
