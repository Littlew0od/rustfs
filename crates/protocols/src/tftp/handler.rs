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
use super::write::WriteHandler;
use crate::common::client::s3::StorageBackend;
use crate::common::gateway::{AuthorizationError, S3Action, authorize_operation};
use crate::common::session::{Protocol, ProtocolPrincipal, SessionContext};
use crate::constants::network::DEFAULT_SOURCE_IP;
use async_tftp::packet;
use async_tftp::server::Handler;
use futures_lite::{AsyncWrite, StreamExt, io::Cursor};
use futures_util::stream;
use s3s::dto::{PutObjectInput, StreamingBlob};
use std::fmt::Debug;
use std::io;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::OnceCell;
use tracing::{debug, error, info};

const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
const LOG_SUBSYSTEM_TFTP_SERVER: &str = "tftp_server";
const EVENT_TFTP_RRQ_STATE: &str = "tftp_rrq_state";
const EVENT_TFTP_WRQ_STATE: &str = "tftp_wrq_state";
const EVENT_TFTP_SESSION_STATE: &str = "tftp_session_state";

// ---------------------------------------------------------------------------
// TftpdHandler — async-tftp Handler backed by StorageBackend
// ---------------------------------------------------------------------------

/// Implements async_tftp::server::Handler, translating RRQ/WRQ into
/// S3 GetObject / PutObject calls.
pub struct TftpdHandler<S: StorageBackend + Send + Sync + 'static> {
    storage: Arc<S>,
    default_bucket: Option<String>,
    mode: TftpAccessMode,
    access_key: String,
    /// Lazily-initialised session context built from the configured
    /// credentials via IAM. TFTP has no per-connection authentication,
    /// so the same context is reused for every request.
    session_context: OnceCell<Arc<SessionContext>>,
    part_size: u64,
    concurrency_limits: u64,
}

impl<S: StorageBackend + Send + Sync + 'static> TftpdHandler<S> {
    /// Create a new handler from configuration and a storage backend.
    pub fn new(config: &TftpConfig, storage: Arc<S>) -> Self {
        TftpdHandler {
            storage,
            default_bucket: config.default_bucket.clone(),
            mode: config.mode,
            access_key: config.access_key.clone(),
            session_context: OnceCell::new(),
            part_size: config.part_size,
            concurrency_limits: config.concurrency_limits,
        }
    }

    /// Lazily initialise and return the per-server [`SessionContext`].
    ///
    /// On first call this looks up the configured access key via IAM
    /// and caches the result. Credential validation (secret-key check)
    /// already happened in [`TftpConfig::validate`], so this method
    /// only builds the struct.
    /// Subsequent calls return the cached context without IAM round-trips.
    async fn get_session_context(&self) -> Result<Arc<SessionContext>, AuthorizationError> {
        self.session_context
            .get_or_try_init(|| async {
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

                let source_ip: IpAddr = DEFAULT_SOURCE_IP.parse().unwrap();
                let principal = ProtocolPrincipal::new(Arc::new(identity));
                Ok(Arc::new(SessionContext::new(principal, Protocol::Tftp, source_ip)))
            })
            .await
            .map(Arc::clone)
    }
}

impl<S: StorageBackend + Send + Sync + 'static + Debug> Handler for TftpdHandler<S> {
    type Reader = Cursor<Vec<u8>>;
    type Writer = WriteHandler<S>;

    async fn read_req_open(&mut self, _client: &SocketAddr, path: &Path) -> Result<(Self::Reader, Option<u64>), packet::Error> {
        if self.mode == TftpAccessMode::WriteOnly {
            return Err(packet::Error::Msg("TFTP server is write-only".to_string()));
        }

        let (bucket, key) = resolve_tftp_path(self.default_bucket.as_deref(), path).map_err(packet::Error::Msg)?;

        debug!(
            event = EVENT_TFTP_RRQ_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
            bucket = %bucket, key = %key,
            "TFTP RRQ"
        );
        let session_ctx = self.get_session_context().await.map_err(|e| match e {
            AuthorizationError::IamUnavailable => packet::Error::Msg("Internal authentication service unavailable".to_string()),
            AuthorizationError::AccessDenied => packet::Error::PermissionDenied,
        })?;

        authorize_operation(&session_ctx, &S3Action::GetObject, &bucket, Some(&key))
            .await
            .map_err(|_| packet::Error::PermissionDenied)?;

        let output = self
            .storage
            .get_object(&bucket, &key, &self.access_key, "", None)
            .await
            .map_err(|e| {
                error!(
                    event = EVENT_TFTP_RRQ_STATE,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
                    bucket = %bucket, key = %key, error = %e,
                    "S3 get_object failed for TFTP RRQ"
                );
                packet::Error::FileNotFound
            })?;

        let content_length = output.content_length.unwrap_or(0).max(0) as u64;

        // 应该使用类似于包裹其它的流的方式来处理 S3 的响应体，而不是一次性将其读入内存。这样可以避免在处理大文件时占用过多内存。
        // Drain the S3 body stream into an in-memory buffer.
        let mut buf = Vec::with_capacity(content_length as usize);
        if let Some(mut body) = output.body {
            while let Some(chunk_result) = body.next().await {
                let chunk = chunk_result.map_err(|_| packet::Error::Msg("Failed to read object body".into()))?;
                buf.extend_from_slice(&chunk);
            }
        }

        info!(
            event = EVENT_TFTP_RRQ_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
            bucket = %bucket, key = %key, size = buf.len(),
            "TFTP RRQ: loaded from S3"
        );

        Ok((Cursor::new(buf), Some(content_length)))
    }

    async fn write_req_open(
        &mut self,
        _client: &SocketAddr,
        path: &Path,
        _size: Option<u64>,
    ) -> Result<Self::Writer, packet::Error> {
        println!("size of file: {:?}", _size);
        if self.mode == TftpAccessMode::ReadOnly {
            return Err(packet::Error::Msg("TFTP server is read-only".to_string()));
        }

        let (bucket, key) = resolve_tftp_path(self.default_bucket.as_deref(), path).map_err(packet::Error::Msg)?;

        debug!(
            event = EVENT_TFTP_WRQ_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
            bucket = %bucket, key = %key,
            "TFTP WRQ"
        );

        let session_ctx = self.get_session_context().await.map_err(|e| match e {
            AuthorizationError::IamUnavailable => packet::Error::Msg("Internal authentication service unavailable".to_string()),
            AuthorizationError::AccessDenied => packet::Error::PermissionDenied,
        })?;

        authorize_operation(&session_ctx, &S3Action::PutObject, &bucket, Some(&key))
            .await
            .map_err(|_| packet::Error::PermissionDenied)?;

        info!(
            event = EVENT_TFTP_WRQ_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
            bucket = %bucket, key = %key,
            "TFTP WRQ: ready to receive"
        );

        // 每个请求是独立的，应该是不需要考虑他们之间的问题
        Ok(WriteHandler::new(
            Arc::clone(&self.storage),
            bucket,
            key,
            self.access_key.clone(),
            Arc::clone(&session_ctx),
            self.part_size,
            self.concurrency_limits,
        ))
    }
}
