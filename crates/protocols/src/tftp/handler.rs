// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! TftpdHandler and util functions used in tftp server.

use super::config::{TftpAccessMode, TftpConfig};
use super::path::resolve_tftp_path;
use super::reader::{ReadHandler, open_reader};
use super::writer::WriteHandler;
use crate::common::client::s3::StorageBackend;
use crate::common::gateway::{AuthorizationError, S3Action, authorize_operation};
use crate::common::session::{Protocol, ProtocolPrincipal, SessionContext, is_temporary_credential};
use async_tftp::packet;
use async_tftp::server::Handler;
use std::fmt::Debug;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::sync::OwnedSemaphorePermit;
use tracing::{debug, error, warn};

const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
const LOG_SUBSYSTEM_TFTP_SERVER: &str = "tftp_server";
const EVENT_TFTP_RRQ_STATE: &str = "tftp_rrq_state";
const EVENT_TFTP_WRQ_STATE: &str = "tftp_wrq_state";
const EVENT_TFTP_SESSION_STATE: &str = "tftp_session_state";

/// Implements async_tftp::server::Handler, translating RRQ/WRQ into
/// S3 GetObject / PutObject calls.
pub struct TftpdHandler<S: StorageBackend + Send + Sync + 'static> {
    storage: Arc<S>,
    default_bucket: Option<String>,
    mode: TftpAccessMode,
    access_key: String,
    part_size: u64,
    concurrency_limits: Arc<Semaphore>,
}

impl<S: StorageBackend + Send + Sync + 'static> TftpdHandler<S> {
    /// Create a new handler from configuration and a storage backend.
    pub fn new(config: &TftpConfig, storage: Arc<S>) -> Self {
        TftpdHandler {
            storage,
            default_bucket: config.default_bucket.clone(),
            mode: config.mode,
            access_key: config.access_key.clone(),
            part_size: config.part_size,
            concurrency_limits: Arc::new(Semaphore::new(config.concurrency_limits)),
        }
    }

    fn acquire_request_permit(&self) -> Result<OwnedSemaphorePermit, packet::Error> {
        self.concurrency_limits
            .clone()
            .try_acquire_owned()
            .map_err(|_| packet::Error::Msg("TFTP concurrency limit reached".to_string()))
    }

    /// Look up the configured access key via IAM and build a per-request
    /// [`SessionContext`] using the client's source IP address.
    ///
    /// Credential validation (secret-key check) already happened in
    /// [`TftpConfig::validate`], so this method only builds the struct.
    async fn get_session_context(&self, client_ip: IpAddr) -> Result<Arc<SessionContext>, AuthorizationError> {
        use rustfs_iam::get;

        let iam_sys = get().map_err(|e| {
            error!(
                event = EVENT_TFTP_SESSION_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
                result = "iam_unavailable",
                error = %e,
                "TFTP session init: IAM unavailable"
            );
            AuthorizationError::IamUnavailable
        })?;

        let (user_identity, is_valid) = iam_sys.check_key(&self.access_key).await.map_err(|e| {
            error!(
                event = EVENT_TFTP_SESSION_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
                result = "check_key_failed",
                error = %e,
                "TFTP session init: key check failed"
            );
            AuthorizationError::IamUnavailable
        })?;

        if !is_valid {
            error!(
                event = EVENT_TFTP_SESSION_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
                result = "invalid_access_key",
                "TFTP session init: access key rejected"
            );
            return Err(AuthorizationError::AccessDenied);
        }

        let identity = user_identity.ok_or_else(|| {
            error!(
                event = EVENT_TFTP_SESSION_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
                result = "identity_missing",
                "TFTP session init: identity missing"
            );
            AuthorizationError::AccessDenied
        })?;

        if is_temporary_credential(&identity.credentials) {
            warn!(
                event = EVENT_TFTP_SESSION_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
                result = "temporary_credential",
                "TFTP session init: temporary credentials are not supported"
            );
            return Err(AuthorizationError::AccessDenied);
        }

        let principal = ProtocolPrincipal::new(Arc::new(identity));
        Ok(Arc::new(SessionContext::new(principal, Protocol::Tftp, client_ip)))
    }
}

impl<S: StorageBackend + Send + Sync + 'static + Debug> Handler for TftpdHandler<S> {
    type Reader = ReadHandler;
    type Writer = WriteHandler<S>;

    async fn read_req_open(&mut self, _client: &SocketAddr, path: &Path) -> Result<(Self::Reader, Option<u64>), packet::Error> {
        if self.mode == TftpAccessMode::WriteOnly {
            return Err(packet::Error::Msg("TFTP server is write-only".to_string()));
        }

        let _permit = self.acquire_request_permit()?;

        let (bucket, key) = resolve_tftp_path(self.default_bucket.as_deref(), path).map_err(packet::Error::Msg)?;

        debug!(
            event = EVENT_TFTP_RRQ_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
            bucket = %bucket, key = %key,
            "TFTP RRQ"
        );
        let session_ctx = self.get_session_context(_client.ip()).await.map_err(|e| match e {
            AuthorizationError::IamUnavailable => packet::Error::Msg("Internal authentication service unavailable".to_string()),
            AuthorizationError::AccessDenied => packet::Error::PermissionDenied,
        })?;

        authorize_operation(&session_ctx, &S3Action::GetObject, &bucket, Some(&key))
            .await
            .map_err(|_| packet::Error::PermissionDenied)?;
        open_reader(self.storage.as_ref(), &bucket, &key, &session_ctx, _permit).await
    }

    async fn write_req_open(
        &mut self,
        _client: &SocketAddr,
        path: &Path,
        _size: Option<u64>,
    ) -> Result<Self::Writer, packet::Error> {
        if self.mode == TftpAccessMode::ReadOnly {
            return Err(packet::Error::Msg("TFTP server is read-only".to_string()));
        }

        let _permit = self.acquire_request_permit()?;

        let (bucket, key) = resolve_tftp_path(self.default_bucket.as_deref(), path).map_err(packet::Error::Msg)?;

        debug!(
            event = EVENT_TFTP_WRQ_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
            bucket = %bucket, key = %key,
            "TFTP WRQ"
        );

        let session_ctx = self.get_session_context(_client.ip()).await.map_err(|e| match e {
            AuthorizationError::IamUnavailable => packet::Error::Msg("Internal authentication service unavailable".to_string()),
            AuthorizationError::AccessDenied => packet::Error::PermissionDenied,
        })?;

        authorize_operation(&session_ctx, &S3Action::PutObject, &bucket, Some(&key))
            .await
            .map_err(|_| packet::Error::PermissionDenied)?;

        Ok(WriteHandler::new(
            Arc::clone(&self.storage),
            bucket,
            key,
            self.access_key.clone(),
            Arc::clone(&session_ctx),
            self.part_size,
            _permit,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(limit: usize) -> TftpConfig {
        TftpConfig {
            bind_addr: "127.0.0.1:0".parse().expect("loopback parses"),
            default_bucket: Some("bucket".to_string()),
            access_key: "test-access-key".to_string(),
            mode: TftpAccessMode::ReadWrite,
            part_size: 1024,
            concurrency_limits: limit,
        }
    }

    #[test]
    fn try_acquire_owned_reports_limit_exhaustion() {
        let handler = TftpdHandler::new(&test_config(1), Arc::new(crate::common::dummy_storage::DummyBackend::new()));

        let first = handler.acquire_request_permit().expect("first request should acquire");
        assert!(
            handler.acquire_request_permit().is_err(),
            "second request should fail once the limit is exhausted"
        );
        drop(first);
        assert!(handler.acquire_request_permit().is_ok(), "permit should be available again after release");
    }
}
