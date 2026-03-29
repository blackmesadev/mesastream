//! DAVE (Discord Audio Video Encryption) integration backed by `davey`.

use std::num::NonZeroU16;
use std::sync::{Arc, Mutex};

use davey::{DaveSession, ProposalsOperationType};
use serde_json::Value;
use tracing::instrument;

use crate::errors::{AppError, AppResult};

// DAVE protocol version and gateway opcodes (https://daveprotocol.com/)
pub const DAVE_PROTOCOL_VERSION: u8 = 1;

pub const OP_DAVE_PREPARE_TRANSITION: u8 = 21;
pub const OP_DAVE_EXECUTE_TRANSITION: u8 = 22;
pub const OP_DAVE_READY_FOR_TRANSITION: u8 = 23;
pub const OP_DAVE_PREPARE_EPOCH: u8 = 24;
pub const OP_DAVE_MLS_EXTERNAL_SENDER: u8 = 25;
pub const OP_DAVE_MLS_KEY_PACKAGE: u8 = 26;
pub const OP_DAVE_MLS_PROPOSALS: u8 = 27;
pub const OP_DAVE_MLS_COMMIT_WELCOME: u8 = 28;
pub const OP_DAVE_MLS_ANNOUNCE_COMMIT: u8 = 29;
pub const OP_DAVE_MLS_WELCOME: u8 = 30;
pub const OP_DAVE_MLS_INVALID_COMMIT_WELCOME: u8 = 31;

#[inline]
pub fn transition_id(value: &Value) -> Option<u16> {
    value.as_u64().and_then(|tid| u16::try_from(tid).ok())
}

#[inline]
pub fn transition_ready(ready_tid: Option<u16>, execute_tid: Option<u16>) -> bool {
    matches!(ready_tid, Some(tid) if execute_tid == Some(tid))
}

#[derive(Clone)]
pub struct DaveEncryptor {
    session: Arc<Mutex<DaveSession>>,
}

impl DaveEncryptor {
    pub fn encrypt_opus(&self, opus_data: &[u8]) -> AppResult<Vec<u8>> {
        let mut session = self
            .session
            .lock()
            .map_err(|_| AppError::Internal("DAVE session lock poisoned".into()))?;

        if !session.is_ready() {
            return Err(AppError::ServiceUnavailable(
                "DAVE session is not ready to encrypt".into(),
            ));
        }

        let encrypted = session
            .encrypt_opus(opus_data)
            .map_err(|e| AppError::Internal(format!("DAVE encrypt_opus failed: {e:?}")))?;

        Ok(encrypted.into_owned())
    }
}

pub struct DaveHandshake {
    session: Arc<Mutex<DaveSession>>,
}

impl DaveHandshake {
    #[instrument(fields(user_id, channel_id))]
    pub fn new(user_id: u64, channel_id: u64) -> AppResult<(Self, Vec<u8>)> {
        let protocol_version = NonZeroU16::new(1)
            .ok_or_else(|| AppError::Internal("invalid DAVE protocol version".into()))?;

        let mut session = DaveSession::new(protocol_version, user_id, channel_id, None)
            .map_err(|e| AppError::Internal(format!("DAVE session init failed: {e:?}")))?;

        let key_package = session.create_key_package().map_err(|e| {
            AppError::Internal(format!("DAVE key package generation failed: {e:?}"))
        })?;

        Ok((
            Self {
                session: Arc::new(Mutex::new(session)),
            },
            key_package,
        ))
    }

    #[instrument(skip(self, external_sender), fields(external_sender_bytes = external_sender.len()))]
    pub fn set_external_sender(&self, external_sender: &[u8]) -> AppResult<()> {
        let mut session = self
            .session
            .lock()
            .map_err(|_| AppError::Internal("DAVE session lock poisoned".into()))?;

        session
            .set_external_sender(external_sender)
            .map_err(|e| AppError::Internal(format!("DAVE set_external_sender failed: {e:?}")))
    }

    #[instrument(skip(self, proposals_payload), fields(op_type, proposals_bytes = proposals_payload.len()))]
    pub fn process_proposals_payload(
        &self,
        op_type: u8,
        proposals_payload: &[u8],
    ) -> AppResult<Option<Vec<u8>>> {
        let operation_type = match op_type {
            0 => ProposalsOperationType::APPEND,
            1 => ProposalsOperationType::REVOKE,
            _ => {
                return Err(AppError::Internal(format!(
                    "invalid DAVE proposals operation type: {op_type}"
                )));
            }
        };

        let mut session = self
            .session
            .lock()
            .map_err(|_| AppError::Internal("DAVE session lock poisoned".into()))?;

        let commit_welcome = session
            .process_proposals(operation_type, proposals_payload, None)
            .map_err(|e| AppError::Internal(format!("DAVE process_proposals failed: {e:?}")))?;

        Ok(commit_welcome.map(|cw| {
            let mut out = cw.commit;
            if let Some(welcome) = cw.welcome {
                out.extend_from_slice(&welcome);
            }
            out
        }))
    }

    #[instrument(skip(self, commit_bytes), fields(commit_bytes_len = commit_bytes.len()))]
    pub fn process_commit(&self, commit_bytes: &[u8]) -> AppResult<()> {
        let mut session = self
            .session
            .lock()
            .map_err(|_| AppError::Internal("DAVE session lock poisoned".into()))?;

        session
            .process_commit(commit_bytes)
            .map_err(|e| AppError::Internal(format!("DAVE process_commit failed: {e:?}")))
    }

    #[instrument(skip(self, welcome_bytes), fields(welcome_bytes_len = welcome_bytes.len()))]
    pub fn process_welcome(&self, welcome_bytes: &[u8]) -> AppResult<()> {
        let mut session = self
            .session
            .lock()
            .map_err(|_| AppError::Internal("DAVE session lock poisoned".into()))?;

        session
            .process_welcome(welcome_bytes)
            .map_err(|e| AppError::Internal(format!("DAVE process_welcome failed: {e:?}")))
    }

    #[instrument(skip(self))]
    pub fn is_ready(&self) -> AppResult<bool> {
        let session = self
            .session
            .lock()
            .map_err(|_| AppError::Internal("DAVE session lock poisoned".into()))?;
        Ok(session.is_ready())
    }

    #[instrument(skip(self))]
    pub fn encryptor(&self) -> AppResult<DaveEncryptor> {
        if !self.is_ready()? {
            return Err(AppError::ServiceUnavailable(
                "DAVE session is not ready after handshake".into(),
            ));
        }
        Ok(DaveEncryptor {
            session: Arc::clone(&self.session),
        })
    }
}
