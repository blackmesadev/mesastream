use std::sync::{
    atomic::{AtomicBool, AtomicU16, AtomicU32, Ordering},
    Arc,
};

use aes_gcm::{aead::Aead, Aes256Gcm, KeyInit, Nonce as GcmNonce};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use tokio::net::UdpSocket;
use tracing::instrument;

use crate::{
    dave::DaveEncryptor,
    errors::{AppError, AppResult},
    models::VoiceBridgePayloadDto,
    voice_gateway,
};

enum Cipher {
    XChaCha20Poly1305(XChaCha20Poly1305),
    Aes256Gcm(Aes256Gcm),
}

impl Cipher {
    fn from_session(mode: &str, key: &[u8; 32]) -> AppResult<Self> {
        match mode {
            "aead_xchacha20_poly1305_rtpsize" => {
                let k = chacha20poly1305::Key::from_slice(key);
                Ok(Cipher::XChaCha20Poly1305(XChaCha20Poly1305::new(k)))
            }
            "aead_aes256_gcm_rtpsize" => {
                let k = aes_gcm::Key::<Aes256Gcm>::from_slice(key);
                Ok(Cipher::Aes256Gcm(Aes256Gcm::new(k)))
            }
            other => Err(AppError::Internal(format!(
                "unsupported encryption mode: {other}"
            ))),
        }
    }

    /// Encrypt `plaintext` with `aad`, returning `ciphertext || 16-byte tag`.
    fn encrypt(&self, nonce_counter: u32, aad: &[u8], plaintext: &[u8]) -> AppResult<Vec<u8>> {
        let tag = nonce_counter.to_be_bytes();
        match self {
            Cipher::XChaCha20Poly1305(c) => {
                // XChaCha20-Poly1305 nonce: 24 bytes = 4-byte counter || 20 zero bytes
                let mut nonce_bytes = [0u8; 24];
                nonce_bytes[..4].copy_from_slice(&tag);
                let nonce = XNonce::from_slice(&nonce_bytes);
                c.encrypt(
                    nonce,
                    chacha20poly1305::aead::Payload {
                        msg: plaintext,
                        aad,
                    },
                )
                .map_err(|e| AppError::Internal(format!("xchacha20 encrypt failed: {e}")))
            }
            Cipher::Aes256Gcm(c) => {
                // AES-256-GCM nonce: 12 bytes = 4-byte counter || 8 zero bytes
                let mut nonce_bytes = [0u8; 12];
                nonce_bytes[..4].copy_from_slice(&tag);
                let nonce = GcmNonce::from_slice(&nonce_bytes);
                c.encrypt(
                    nonce,
                    aes_gcm::aead::Payload {
                        msg: plaintext,
                        aad,
                    },
                )
                .map_err(|e| AppError::Internal(format!("aes256gcm encrypt failed: {e}")))
            }
        }
    }
}

/// Owns the UDP socket and all per-packet state for one Discord voice session.
///
/// All mutable state uses atomics so multiple async tasks can call
/// `send_packet` concurrently without holding a mutex.
struct EncryptedUdpSender {
    socket: UdpSocket,
    ssrc: u32,
    sequence: AtomicU16,
    timestamp: AtomicU32,
    nonce_counter: AtomicU32,
    cipher: Cipher,
    ws_alive: Arc<AtomicBool>,
}

impl EncryptedUdpSender {
    #[instrument(skip(secret_key, ws_alive), fields(%server_addr, ssrc, encryption_mode))]
    async fn connect(
        server_addr: std::net::SocketAddr,
        ssrc: u32,
        secret_key: &[u8; 32],
        encryption_mode: &str,
        ws_alive: Arc<AtomicBool>,
    ) -> AppResult<Self> {
        let socket = UdpSocket::bind("0.0.0.0:0")
            .await
            .map_err(|e| AppError::ServiceUnavailable(format!("UDP bind failed: {e}")))?;

        socket
            .connect(server_addr)
            .await
            .map_err(|e| AppError::ServiceUnavailable(format!("UDP connect failed: {e}")))?;

        let cipher = Cipher::from_session(encryption_mode, secret_key)?;

        Ok(Self {
            socket,
            ssrc,
            sequence: AtomicU16::new(0),
            timestamp: AtomicU32::new(0),
            nonce_counter: AtomicU32::new(0),
            cipher,
            ws_alive,
        })
    }

    /// Build an encrypted RTP packet and send it over UDP.
    ///
    /// Packet layout (rtpsize modes):
    /// ```text
    ///   RTP header (12) | ciphertext+tag | 4-byte nonce counter (BE)
    /// ```
    /// The RTP header doubles as the AEAD additional authenticated data (AAD).
    ///
    /// Reuses the AEAD-returned Vec to build the final packet in-place,
    /// avoiding a second allocation per frame.
    async fn send_packet(&self, opus: &[u8]) -> AppResult<()> {
        let seq = self.sequence.fetch_add(1, Ordering::Relaxed);
        let ts = self.timestamp.fetch_add(960, Ordering::Relaxed);
        let nonce_val = self.nonce_counter.fetch_add(1, Ordering::Relaxed);

        // 12 byte RTP header: version=2, payload_type=0x78
        let mut header = [0u8; 12];
        header[0] = 0x80;
        header[1] = 0x78;
        header[2..4].copy_from_slice(&seq.to_be_bytes());
        header[4..8].copy_from_slice(&ts.to_be_bytes());
        header[8..12].copy_from_slice(&self.ssrc.to_be_bytes());

        // Encrypt opus frame with RTP header as AAD → ciphertext || tag.
        let ciphertext = self.cipher.encrypt(nonce_val, &header, opus)?;

        // Assemble final packet in one pass by prepending header and appending
        // nonce to the Vec the AEAD crate already allocated.
        let ct_len = ciphertext.len();
        let mut packet = ciphertext;
        packet.reserve(12 + 4); // header + nonce
        packet.resize(12 + ct_len + 4, 0);
        // Shift ciphertext right by 12 to make room for the header.
        packet.copy_within(..ct_len, 12);
        packet[..12].copy_from_slice(&header);
        packet[12 + ct_len..].copy_from_slice(&nonce_val.to_be_bytes());

        self.socket
            .send(&packet)
            .await
            .map_err(|e| AppError::ServiceUnavailable(format!("UDP send failed: {e}")))?;

        Ok(())
    }
}

/// Discord UDP transport for one voice session.
///
/// Mesastream connects to the Discord Voice Gateway itself using the
/// `VoiceBridgePayload` sent by Black Mesa.  The gateway handshake yields
/// the SSRC, server UDP address, negotiated encryption mode, and session key
/// that are used to build this transport.
///
/// When Black Mesa detects a session change (`VoiceSessionChange::EndpointUpdated`)
/// it calls `PUT /players/{id}/connection` with an updated `VoiceBridgePayloadDto`.
/// Mesastream re-runs the voice gateway handshake and replaces this transport
/// atomically, so playback is uninterrupted.
///
/// `Clone` is cheap — the inner sender is reference-counted.
#[derive(Clone, Default)]
pub struct DiscordTransport {
    sender: Option<Arc<EncryptedUdpSender>>,
    dave: Option<DaveEncryptor>,
    speaking_tx: Option<tokio::sync::mpsc::UnboundedSender<bool>>,
    speaking_state: Arc<AtomicBool>,
}

impl DiscordTransport {
    /// Runs the Discord Voice Gateway handshake and establishes the UDP transport.
    #[instrument(skip(payload), fields(guild_id = %payload.guild_id, player_id = %payload.player_id))]
    pub async fn connect(payload: &VoiceBridgePayloadDto) -> AppResult<Self> {
        let session = voice_gateway::connect(payload).await?;

        let sender = EncryptedUdpSender::connect(
            session.server_addr,
            session.ssrc,
            &session.secret_key,
            &session.encryption_mode,
            session.ws_alive,
        )
        .await?;

        Ok(Self {
            sender: Some(Arc::new(sender)),
            dave: session.dave,
            speaking_tx: Some(session.speaking_tx),
            speaking_state: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Sends one Opus frame to Discord via the UDP transport.
    ///
    /// When a `DaveEncryptor` is present (E2EE channel) the frame is
    /// DAVE-encrypted before RTP encapsulation and transport-layer encryption.
    pub async fn send_opus_frame(&self, frame: &[u8]) -> AppResult<()> {
        let Some(sender) = &self.sender else {
            return Err(AppError::ServiceUnavailable(
                "voice UDP transport is not configured".to_string(),
            ));
        };

        // Keep the per-frame path minimal: no tracing span and no Cow indirection.
        if let Some(enc) = &self.dave {
            let encrypted = enc.encrypt_opus(frame)?;
            sender.send_packet(&encrypted).await
        } else {
            sender.send_packet(frame).await
        }
    }

    /// Returns `true` when the UDP transport is connected and the voice-gateway
    /// WebSocket keepalive is still alive.  Once Discord closes the WS session
    /// (server crash, rotation, timeout) this returns `false` and the player
    /// should stop sending frames or trigger reconnect via `update_connection`.
    #[inline]
    pub fn is_connected(&self) -> bool {
        self.sender
            .as_ref()
            .map_or(false, |s| s.ws_alive.load(Ordering::Relaxed))
    }

    /// Sets the Speaking state for this voice session.
    /// Send `true` when starting to send audio frames, `false` when stopping.
    #[inline]
    pub fn set_speaking(&self, speaking: bool) {
        let previous = self.speaking_state.swap(speaking, Ordering::Relaxed);
        if previous == speaking {
            return;
        }

        if let Some(tx) = &self.speaking_tx {
            let _ = tx.send(speaking);
        }
    }
}
