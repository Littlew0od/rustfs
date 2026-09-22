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

//! Write-side state-machine helpers and [`VecWriter`] for the TFTP
//! write path.
//!
//! The async-tftp server drives [`VecWriter`] through its
//! [`AsyncWrite`] impl. S3 uploads (single PutObject or multipart)
//! are spawned from [`Drop`] so async work does not block the
//! synchronous poll methods.

use super::state::{CompletedPart, WriteState};
use crate::common::client::s3::StorageBackend;
use crate::common::gateway::{S3Action, authorize_operation};
use crate::common::session::SessionContext;
use async_tftp::packet;
use bytes::Bytes;
use futures_lite::AsyncWrite;
use futures_util::stream;
use rustfs_utils::MaskedAccessKey;
use s3s::dto::{
    AbortMultipartUploadInput, CompleteMultipartUploadInput, CompletedMultipartUpload, CompletedPart as S3CompletedPart,
    CreateMultipartUploadInput, PutObjectInput, StreamingBlob, UploadPartInput,
};
use std::cell::RefCell;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};
// use tokio::sync::RwLock;
use tokio::sync::RwLock;
use tracing::{error, info, warn, debug};
use tokio::sync::OwnedSemaphorePermit;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
const LOG_SUBSYSTEM_TFTP_WRITE: &str = "tftp_write";

const EVENT_TFTP_WRITE_STATE: &str = "tftp_write_state";
const EVENT_TFTP_MULTIPART_STATE: &str = "tftp_multipart_state";
const EVENT_TFTP_PUTOBJECT_STATE: &str = "tftp_putobject_state";
const EVENT_TFTP_ABORT_STATE: &str = "tftp_abort_state";

// ---------------------------------------------------------------------------
// Retry policy for commit/abort S3 calls spawned from Drop
// ---------------------------------------------------------------------------

const COMMIT_WRITE_MAX_RETRIES: usize = 3;
const COMMIT_WRITE_BACKOFF_MS: [u64; 3] = [100, 500, 1500];
const EVENT_TFTP_FLUSH_COMPLETE: &str = "tftp_flush_complete";

// VecWriter — in-memory AsyncWrite with S3 upload on drop
// ---------------------------------------------------------------------------

/// Accumulates TFTP write bytes into an in-memory buffer backed by a
/// [`WriteState`] state machine.
///
/// **Small files** (below `part_size`): the entire payload stays in
/// [`WriteState::Buffering`] and a single PutObject is issued from
/// [`Drop`] when `poll_flush` was called.
///
/// **Large files**: when the buffer reaches `part_size`, the Drop
/// task issues CreateMultipartUpload and UploadPart for each
/// part_size chunk, then CompleteMultipartUpload. If the writer is
/// dropped without `poll_flush` (abnormal termination), any
/// in-progress multipart upload is aborted.
pub struct VecWriter<S: StorageBackend + Send + Sync + 'static> {
    storage: Arc<S>,
    bucket: String,
    key: String,
    access_key: String,
    session_ctx: Arc<SessionContext>,
    part_size: u64,
    /// Shared upload_id set by the first CreateMultipartUpload task.
    /// Subsequent UploadPart tasks read from here; flush task reads
    /// from here for CompleteMultipartUpload.
    shared_upload_id: Arc<RwLock<Option<String>>>,
}

impl<S: StorageBackend + Send + Sync + 'static> VecWriter<S> {
    pub fn new(
        storage: Arc<S>,
        bucket: String,
        key: String,
        access_key: String,
        session_ctx: Arc<SessionContext>,
        part_size: u64,
    ) -> Self {
        VecWriter {
            storage,
            bucket,
            key,
            access_key,
            session_ctx,
            part_size,
            shared_upload_id: Arc::new(RwLock::new(None)),
        }
    }

    // ===== S3 operation helpers (standalone, usable from spawned tasks) =====

    /// Resolve the upload_id for a multipart upload.
    ///
    /// When `part_number == 1`, this creates the multipart upload and
    /// stores the resulting upload_id in `shared_upload_id`. All other
    /// callers poll `shared_upload_id` until the first task finishes.
    pub(super) async fn resolve_upload_id(self: &Arc<Self>, part_number: i32) -> Result<String, packet::Error> {
        debug!(
            event = EVENT_TFTP_MULTIPART_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
            bucket = %self.bucket, key = %self.key,
            part_number = part_number,
            "TFTP resolve_upload_id: part_number={part_number}"
        );
        if part_number == 1 {
            let uid = self.create_multipart_upload().await?;
            *self.shared_upload_id.write().await = Some(uid.clone());
            Ok(uid)
        } else {
            loop {
                if let Some(ref uid) = *self.shared_upload_id.read().await {
                    break Ok(uid.clone());
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }

    /// Upload buffered bytes with a single PutObject, retrying on
    /// transient backend errors.
    pub(super) async fn put_object(self: &Arc<Self>, buffer: Vec<u8>) -> Result<(), String> {
        let size = buffer.len() as i64;
        let body_bytes = Bytes::from(buffer);
        for attempt in 0..=COMMIT_WRITE_MAX_RETRIES {
            if attempt > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(COMMIT_WRITE_BACKOFF_MS[attempt - 1])).await;
                info!(
                    event = EVENT_TFTP_PUTOBJECT_STATE,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
                    bucket = %self.bucket,
                    key = %self.key,
                    attempt = attempt,
                    "TFTP put_object retry scheduled"
                );
            }

            if let Err(e) = authorize_operation(&self.session_ctx, &S3Action::PutObject, &self.bucket, Some(&self.key)).await {
                warn!(
                    event = EVENT_TFTP_PUTOBJECT_STATE,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
                    bucket = %self.bucket, key = %self.key,
                    err = ?e,
                    "TFTP put_object auth failed"
                );
                return Err(format!("put_object auth failed: {e:?}"));
            }

            let body = body_bytes.clone();
            let body_stream = stream::once(async move { Ok::<Bytes, std::io::Error>(body) });
            let streaming = StreamingBlob::wrap(body_stream);
            let input = match PutObjectInput::builder()
                .bucket(self.bucket.clone())
                .key(self.key.clone())
                .content_length(Some(size))
                .body(Some(streaming))
                .build()
            {
                Ok(input) => input,
                Err(e) => return Err(format!("build put_object input: {e}")),
            };

            match self.storage.put_object(input, self.session_ctx.credentials()).await {
                Ok(_) => {
                    info!(
                        event = EVENT_TFTP_PUTOBJECT_STATE,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
                        bucket = %self.bucket, key = %self.key, size = size,
                        "TFTP put_object completed"
                    );
                    return Ok(());
                }
                Err(e) => {
                    let msg = e.to_string();
                    if attempt < COMMIT_WRITE_MAX_RETRIES && rustfs_utils::retry::is_s3code_in_message_retryable(&msg) {
                        continue;
                    }
                    error!(
                        event = EVENT_TFTP_PUTOBJECT_STATE,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
                        bucket = %self.bucket, key = %self.key,
                        err = %e,
                        "TFTP put_object failed"
                    );
                    return Err(format!("put_object: {e}"));
                }
            }
        }

        error!(
            event = EVENT_TFTP_PUTOBJECT_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
            bucket = %self.bucket, key = %self.key,
            "TFTP put_object retry loop fell through"
        );
        Err("put_object retry loop fell through".to_string())
    }

    /// Issue CreateMultipartUpload. Returns the upload_id.
    pub(super) async fn create_multipart_upload(self: &Arc<Self>) -> Result<String, packet::Error> {
        authorize_operation(&self.session_ctx, &S3Action::CreateMultipartUpload, &self.bucket, Some(&self.key))
            .await
            .map_err(|_| packet::Error::PermissionDenied)?;

        debug!(
            event = EVENT_TFTP_MULTIPART_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
            bucket = %self.bucket, key = %self.key,
            "TFTP CreateMultipartUpload"
        );

        let input = CreateMultipartUploadInput::builder()
            .bucket(self.bucket.clone())
            .key(self.key.clone())
            .build()
            .map_err(|e| packet::Error::Msg(format!("build create_multipart_upload: {e}")))?;

        let out = self
            .storage
            .create_multipart_upload(input, self.session_ctx.credentials())
            .await
            .map_err(|e| packet::Error::Msg(format!("CreateMultipartUpload: {e}")))?;

        let upload_id = out.upload_id.ok_or_else(|| {
            error!(
                event = EVENT_TFTP_MULTIPART_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
                bucket = %self.bucket, key = %self.key,
                "TFTP CreateMultipartUpload missing upload_id"
            );
            packet::Error::Msg("CreateMultipartUpload: missing upload_id".to_string())
        })?;

        Ok(upload_id)
    }

    pub(super) async fn complete_multipart_upload(
        self: &Arc<Self>,
        upload_id: &str,
        pending_handles: Vec<tokio::task::JoinHandle<Result<CompletedPart, packet::Error>>>,
    ) -> Result<(), packet::Error> {
        debug!(
            event = EVENT_TFTP_MULTIPART_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
            bucket = %self.bucket, key = %self.key,
            upload_id = %upload_id,
            "TFTP flush: CompleteMultipartUpload with {} pending handles", pending_handles.len()
        );
        let mut uploaded_parts: Vec<CompletedPart> = Vec::with_capacity(pending_handles.len());

        for handle in pending_handles {
            match handle.await {
                Ok(Ok(part)) => uploaded_parts.push(part),
                Ok(Err(e)) => {
                    error!(
                        event = EVENT_TFTP_MULTIPART_STATE,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
                        bucket = %self.bucket, key = %self.key,
                        err = ?e,
                        "TFTP flush: UploadPart task failed"
                    );
                    self.abort_multipart_upload(upload_id).await;
                    return Err(packet::Error::Msg(format!("UploadPart task failed: {e:?}")));
                }
                Err(join_err) => {
                    error!(
                        event = EVENT_TFTP_MULTIPART_STATE,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
                        bucket = %self.bucket, key = %self.key,
                        err = ?join_err,
                        "TFTP flush: UploadPart task panicked"
                    );
                    self.abort_multipart_upload(upload_id).await;
                    return Err(packet::Error::Msg(format!("UploadPart task panicked: {join_err:?}")));
                }
            }
        }

        // All UploadPart tasks succeeded. S3 requires parts in ascending
        // numeric order, even when async uploads finish out of order.
        uploaded_parts.sort_by_key(|p| p.part_number);

        authorize_operation(&self.session_ctx, &S3Action::CompleteMultipartUpload, &self.bucket, Some(&self.key))
            .await
            .map_err(|_| packet::Error::PermissionDenied)?;

        let parts: Vec<S3CompletedPart> = uploaded_parts
            .into_iter()
            .map(|p| S3CompletedPart {
                part_number: Some(p.part_number),
                e_tag: Some(p.e_tag),
                ..Default::default()
            })
            .collect();


        debug!(
            event = EVENT_TFTP_MULTIPART_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
            bucket = %self.bucket, key = %self.key,
            upload_id = %upload_id,
            parts_count = parts.len(),
            "TFTP flush: CompleteMultipartUpload with {} parts", parts.len()
        );
        let input = CompleteMultipartUploadInput::builder()
            .bucket(self.bucket.clone())
            .key(self.key.clone())
            .upload_id(upload_id.to_string())
            .multipart_upload(Some(CompletedMultipartUpload { parts: Some(parts) }))
            .build()
            .map_err(|e| packet::Error::Msg(format!("build complete_multipart_upload: {e}")))?;

        self.storage
            .complete_multipart_upload(input, self.session_ctx.credentials())
            .await
            .map_err(|e| packet::Error::Msg(format!("CompleteMultipartUpload: {e}")))?;

        info!(
            event = EVENT_TFTP_MULTIPART_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
            bucket = %self.bucket,
            key = %self.key,
            upload_id = %upload_id,
            "TFTP flush: CompleteMultipartUpload succeeded"
        );

        Ok(())
    }

    /// Upload one part. Returns a CompletedPart on success.
    pub(super) async fn upload_part(
        self: &Arc<Self>,
        upload_id: &str,
        part_number: i32,
        part_bytes: Vec<u8>,
    ) -> Result<CompletedPart, packet::Error> {
        authorize_operation(&self.session_ctx, &S3Action::UploadPart, &self.bucket, Some(&self.key))
            .await
            .map_err(|_| packet::Error::PermissionDenied)?;

        let part_len = part_bytes.len() as i64;
        let body_bytes = Bytes::from(part_bytes);
        let body_stream = stream::once(async move { Ok::<Bytes, std::io::Error>(body_bytes) });
        let streaming = StreamingBlob::wrap(body_stream);

        let input = UploadPartInput::builder()
            .bucket(self.bucket.clone())
            .key(self.key.clone())
            .upload_id(upload_id.to_string())
            .part_number(part_number)
            .content_length(Some(part_len))
            .body(Some(streaming))
            .build()
            .map_err(|e| packet::Error::Msg(format!("build upload_part: {e}")))?;

        let out = self
            .storage
            .upload_part(input, self.session_ctx.credentials())
            .await
            .map_err(|e| packet::Error::Msg(format!("UploadPart: {e}")))?;

        let e_tag = out.e_tag.ok_or_else(|| {
            warn!(
                event = EVENT_TFTP_MULTIPART_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
                upload_id = %upload_id,
                part_number = part_number,
                "TFTP UploadPart missing etag"
            );
            packet::Error::Msg("UploadPart: missing etag".to_string())
        })?;

        Ok(CompletedPart { part_number, e_tag })
    }

    /// Abort a multipart upload.
    pub(super) async fn abort_multipart_upload(self: &Arc<Self>, upload_id: &str) {
        if let Err(e) =
            authorize_operation(&self.session_ctx, &S3Action::AbortMultipartUpload, &self.bucket, Some(&self.key)).await
        {
            warn!(
                event = EVENT_TFTP_ABORT_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
                bucket = %self.bucket, key = %self.key,
                upload_id = %upload_id,
                access_key = %MaskedAccessKey(&self.access_key),
                err = ?e,
                "TFTP AbortMultipartUpload skipped: auth denied"
            );
            return;
        }

        let input = match AbortMultipartUploadInput::builder()
            .bucket(self.bucket.clone())
            .key(self.key.clone())
            .upload_id(upload_id.to_string())
            .build()
        {
            Ok(input) => input,
            Err(e) => {
                error!(
                    event = EVENT_TFTP_ABORT_STATE,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
                    bucket = %self.bucket, key = %self.key,
                    upload_id = %upload_id,
                    err = %e,
                    "TFTP build AbortMultipartUpload failed"
                );
                return;
            }
        };

        if let Err(e) = self.storage.abort_multipart_upload(input, self.session_ctx.credentials()).await {
            error!(
                event = EVENT_TFTP_ABORT_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
                bucket = %self.bucket, key = %self.key,
                upload_id = %upload_id,
                err = %e,
                "TFTP AbortMultipartUpload failed"
            );
        } else {
            info!(
                event = EVENT_TFTP_ABORT_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
                bucket = %self.bucket, key = %self.key,
                upload_id = %upload_id,
                "TFTP AbortMultipartUpload completed"
            );
        }
    }
}

pub struct WriteHandler<S: StorageBackend + Send + Sync + 'static> {
    writer: Arc<VecWriter<S>>,
    buffer: WriteState,
    /// True when poll_flush has been called (normal completion).
    flushed: AtomicBool,
    /// Handles for in-flight UploadPart tasks spawned from do_write.
    pending_handles: Vec<tokio::task::JoinHandle<Result<CompletedPart, packet::Error>>>,
    /// oneshot receiver for the flush completion result. Set by
    /// do_flush on the first poll_flush call; subsequent polls check this.
    flush_rx: RefCell<Option<tokio::sync::oneshot::Receiver<Result<(), String>>>>,
    // /// Shared upload_id set by the first CreateMultipartUpload task.
    // /// Subsequent UploadPart tasks read from here; flush task reads
    // /// from here for CompleteMultipartUpload.
    // upload_id: Option<String>,
    _permit: OwnedSemaphorePermit,
}

impl<S: StorageBackend + Send + Sync + 'static> WriteHandler<S> {
    pub fn new(
        storage: Arc<S>,
        bucket: String,
        key: String,
        access_key: String,
        session_ctx: Arc<SessionContext>,
        part_size: u64,
        _permit: OwnedSemaphorePermit,
    ) -> Self {
        WriteHandler {
            writer: Arc::new(VecWriter::new(
                storage,
                bucket,
                key,
                access_key,
                session_ctx,
                part_size,

            )),
            buffer: WriteState::Buffering { part_buffer: Vec::new() },
            flushed: AtomicBool::new(false),
            pending_handles: Vec::new(),
            flush_rx: RefCell::new(None),
            // upload_id: None,
            _permit,
        }
    }

    /// Append data to the internal buffer. Returns the number of
    /// bytes accepted.
    fn do_write(&mut self, data: &[u8]) -> Result<u64, io::Error> {
        let accepted = data.len() as u64;
        self.buffer
            .write_append_bytes(data)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // Drain loop: when a full part is ready, spawn an async
        // UploadPart task via the do_upload_part helper.
        while self.buffer.write_has_full_part(self.writer.part_size) {
            if matches!(self.buffer, WriteState::Buffering { .. }) {
                self.buffer
                    .write_begin_streaming(
                        String::new(), // placeholder
                        false,
                    )
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            }

            let (part_bytes, part_number) = match &mut self.buffer {
                WriteState::Streaming {
                    part_buffer,
                    next_part_number,
                    ..
                } => {
                    let drain_len = (self.writer.part_size as usize).min(part_buffer.len());
                    if drain_len == 0 {
                        break;
                    }
                    let bytes: Vec<u8> = part_buffer.drain(..drain_len).collect();
                    let pn = *next_part_number;
                    *next_part_number += 1;
                    (bytes, pn)
                }
                _ => {
                    error!(
                        event = EVENT_TFTP_MULTIPART_STATE,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
                        result = "drain_loop_not_streaming",
                        "TFTP drain loop: not in Streaming state"
                    );
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "drain loop: not in Streaming state",
                    ));
                },
            };

            let writer = self.writer.clone();

            let handle: tokio::task::JoinHandle<Result<CompletedPart, packet::Error>> = tokio::spawn(async move {
                let upload_id = writer.resolve_upload_id(part_number).await?;

                writer.upload_part(&upload_id, part_number, part_bytes).await
            });

            self.pending_handles.push(handle);
        }
        Ok(accepted)
    }

    /// Spawn the async flush completion task. The spawned task:
    ///   1. Waits for all pending UploadPart JoinHandles.
    ///   2. Handles PutObject (Buffering) or CompleteMultipartUpload
    ///      (Streaming) via the existing helper methods.
    ///   3. Calls `waker.wake_by_ref()` when done.
    fn do_flush(&mut self, waker: Waker) {
        let (tx, rx) = tokio::sync::oneshot::channel();

        debug!(
            event = EVENT_TFTP_FLUSH_COMPLETE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
            "TFTP flush: spawning flush completion task"
        );

        // Take ownership of state and pending handles so the spawned
        // task can use them without borrowing self.
        let state = std::mem::replace(
            &mut self.buffer,
            WriteState::Finished {
                upload_id: String::new(),
                part_number: 0,
            },
        );

        let writer = Arc::clone(&self.writer);
        match state {
            WriteState::Buffering { part_buffer } => {
                tokio::spawn(async move {
                    let result = writer.put_object(part_buffer).await;
                    let _ = tx.send(result.map_err(|e| e));
                    waker.wake_by_ref();
                });
            }
            WriteState::Streaming {
                part_buffer,
                next_part_number,
                ..
            } => {
                debug!(
                    event = EVENT_TFTP_FLUSH_COMPLETE,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
                    "TFTP flush: spawning flush completion task for Streaming state"
                );
                // let mut uploaded_parts: Vec<CompletedPart> = Vec::with_capacity(pending_handles.len());
                let mut pending_handles: Vec<_> = std::mem::take(&mut self.pending_handles);
                let writer_clone = writer.clone();
                let handle = tokio::spawn(async move {
                    let upload_id = writer_clone.resolve_upload_id(next_part_number).await.unwrap();
                    writer_clone
                        .upload_part(&upload_id.clone(), next_part_number, part_buffer)
                        .await
                });
                pending_handles.push(handle);
                tokio::spawn(async move {
                    let upload_id = writer.resolve_upload_id(next_part_number).await.unwrap();

                    let result = writer
                        .complete_multipart_upload(&upload_id, pending_handles)
                        .await
                        .map_err(|e| format!("{e:?}"));
                    let _ = tx.send(result);
                    debug!(
                        event = EVENT_TFTP_FLUSH_COMPLETE,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
                        "TFTP flush: flush completion task finished"
                    );
                    waker.wake_by_ref();
                });
            }
            WriteState::Finished { upload_id, .. } => {
                tokio::spawn(async move {
                    writer.abort_multipart_upload(&upload_id).await;
                    let _ = tx.send(Err("write handle was finished".to_string()));
                    waker.wake_by_ref();
                });
                // Err("write handle was poisoned (Failed)".to_string())
            }
        }

        self.flush_rx = RefCell::new(Some(rx));
        self.flushed.store(true, Ordering::SeqCst);
    }
}

impl<S: StorageBackend + Send + Sync + 'static> AsyncWrite for WriteHandler<S> {
    fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.do_write(buf) {
            Ok(n) => Poll::Ready(Ok(n as usize)),
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.as_mut().get_mut();

        debug!(
            event = EVENT_TFTP_FLUSH_COMPLETE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
            "TFTP flush: poll_flush called"
        );

        // First call: spawn the flush completion task.
        if this.flush_rx.borrow().is_none() {
            this.do_flush(cx.waker().clone());
            debug!(
                event = EVENT_TFTP_FLUSH_COMPLETE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
                "TFTP flush: first poll_flush call, flush task spawned, return Pending"
            );
            return Poll::Pending;
        }

        // Subsequent calls: check the oneshot receiver.
        if let Some(rx) = this.flush_rx.borrow_mut().as_mut() {
            debug!(
                event = EVENT_TFTP_FLUSH_COMPLETE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
                "TFTP flush: subsequent poll_flush call, checking flush task result"
            );
            let temp = rx.try_recv();
            debug!(
                event = EVENT_TFTP_FLUSH_COMPLETE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
                "TFTP flush: flush task result: {:?}",
                temp
            );
            match temp {
                Ok(Ok(())) => Poll::Ready(Ok(())),
                Ok(Err(e)) => Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e))),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => Poll::Pending,
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, "flush task panicked")))
                }
            }
        } else {
            // do_flush always stores a receiver; this branch is unreachable.
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
