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
use tracing::{error, info, warn};

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
    cocurrency_limits: u64,
    /// Shared upload_id set by the first CreateMultipartUpload task.
    /// Subsequent UploadPart tasks read from here; flush task reads
    /// from here for CompleteMultipartUpload.
    /// 这个也是会阻塞的
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
        cocurrency_limits: u64,
    ) -> Self {
        VecWriter {
            storage,
            bucket,
            key,
            access_key,
            session_ctx,
            part_size,
            cocurrency_limits,
            shared_upload_id: Arc::new(RwLock::new(None)),
        }
    }

    // ===== S3 operation helpers (standalone, usable from spawned tasks) =====

    /// Upload buffered bytes with a single PutObject, retrying on
    /// transient backend errors.
    pub(super) async fn do_put_object(self: &Arc<Self>, buffer: Vec<u8>) -> Result<(), String> {
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

            match self.storage.put_object(input, &self.access_key, "").await {
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
    pub(super) async fn do_create_multipart_upload(self: &Arc<Self>) -> Result<String, packet::Error> {
        authorize_operation(&self.session_ctx, &S3Action::CreateMultipartUpload, &self.bucket, Some(&self.key))
            .await
            .map_err(|_| packet::Error::PermissionDenied)?;

        let input = CreateMultipartUploadInput::builder()
            .bucket(self.bucket.clone())
            .key(self.key.clone())
            .build()
            .map_err(|e| packet::Error::Msg(format!("build create_multipart_upload: {e}")))?;

        let out = self
            .storage
            .create_multipart_upload(input, &self.access_key, "")
            .await
            .map_err(|e| packet::Error::Msg(format!("CreateMultipartUpload: {e}")))?;

        let upload_id = out.upload_id.ok_or_else(|| {
            warn!(
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

    /// Resolve the upload_id for a multipart upload.
    ///
    /// When `part_number == 1`, this creates the multipart upload and
    /// stores the resulting upload_id in `shared_upload_id`. All other
    /// callers poll `shared_upload_id` until the first task finishes.
    pub(super) async fn do_resolve_upload_id(self: &Arc<Self>, part_number: i32) -> Result<String, packet::Error> {
        if part_number == 1 {
            let uid = self.do_create_multipart_upload().await?;
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

    pub(super) async fn do_multipart_upload(
        self: &Arc<Self>,
        // 传入等待的队列
        // 传入剩余的buffer
        upload_id: &str,
        part_number: i32,
        pending_handles: Vec<tokio::task::JoinHandle<Result<CompletedPart, packet::Error>>>,
    ) -> Result<CompletedPart, packet::Error> {
        let mut uploaded_parts: Vec<CompletedPart> = Vec::with_capacity(pending_handles.len());
        let writer = self.clone();
        // pending_handles.push(tokio::spawn(async move {
        //     writer.clone().do_upload_part(upload_id, part_number, buffer);
        // }));

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
                    self.do_abort_multipart_upload(upload_id).await;
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
                    // let uid_snapshot = {
                    //     let guard = shared_upload_id.lock().unwrap();
                    //     guard.clone()
                    // };
                    // if let Some(ref uid) = uid_snapshot {
                    self.do_abort_multipart_upload(upload_id).await;
                    // }
                    return Err(packet::Error::Msg(format!("UploadPart task panicked: {join_err:?}")));
                }
            }
        }

        Err(packet::Error::Msg("flush: no UploadPart tasks completed successfully".to_string()))
    }

    /// Upload one part. Returns a CompletedPart on success.
    pub(super) async fn do_upload_part(
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
            .upload_part(input, &self.access_key, "")
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

    /// Complete a multipart upload.
    pub(super) async fn do_complete_multipart_upload(
        self: &Arc<Self>,
        upload_id: &str,
        uploaded_parts: Vec<CompletedPart>,
    ) -> Result<(), packet::Error> {
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

        let input = CompleteMultipartUploadInput::builder()
            .bucket(self.bucket.clone())
            .key(self.key.clone())
            .upload_id(upload_id.to_string())
            .multipart_upload(Some(CompletedMultipartUpload { parts: Some(parts) }))
            .build()
            .map_err(|e| packet::Error::Msg(format!("build complete_multipart_upload: {e}")))?;

        self.storage
            .complete_multipart_upload(input, &self.access_key, "")
            .await
            .map_err(|e| packet::Error::Msg(format!("CompleteMultipartUpload: {e}")))?;

        Ok(())
    }

    /// Abort a multipart upload.
    pub(super) async fn do_abort_multipart_upload(self: &Arc<Self>, upload_id: &str) {
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

        if let Err(e) = self.storage.abort_multipart_upload(input, &self.access_key, "").await {
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

    // ---------------------------------------------------------------------------
    // State-machine helpers (sync, pure)
    // ---------------------------------------------------------------------------

    /// Returns Some(upload_id) when the phase carries a live multipart
    /// upload AND the cached abort_authorized flag permits abort.
    pub(super) fn should_abort_on_drop(state: &WriteState) -> Option<&str> {
        match state {
            WriteState::Streaming {
                upload_id,
                abort_authorized: true,
                ..
            } => Some(upload_id.as_str()),
            WriteState::Failed { upload_id, .. } => Some(upload_id.as_str()),
            _ => None,
        }
    }
}

// impl<S: StorageBackend + Send + Sync + 'static> AsyncWrite for VecWriter<S> {
//     fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
//         match self.do_write(buf) {
//             Ok(n) => Poll::Ready(Ok(n as usize)),
//             Err(e) => Poll::Ready(Err(e)),
//         }
//     }

//     fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
//         let this = self.as_mut().get_mut();

//         // First call: spawn the flush completion task.
//         if this.flush_rx.borrow().is_none() {
//             this.do_flush(cx.waker().clone());
//             return Poll::Pending;
//         }

//         // Subsequent calls: check the oneshot receiver.
//         if let Some(rx) = this.flush_rx.borrow_mut().as_mut() {
//             match rx.try_recv() {
//                 Ok(Ok(())) => Poll::Ready(Ok(())),
//                 Ok(Err(e)) => Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e))),
//                 Err(tokio::sync::oneshot::error::TryRecvError::Empty) => Poll::Pending,
//                 Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
//                     Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, "flush task panicked")))
//                 }
//             }
//         } else {
//             // 这里是因为put_object的情况吗？
//             Poll::Ready(Ok(()))
//         }
//     }

//     fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
//         Poll::Ready(Ok(()))
//     }
// }

// 这个Drop应该没作用了
// impl<S: StorageBackend + Send + Sync + 'static> Drop for VecWriter<S> {
//     fn drop(&mut self) {
//         // If poll_flush was called, the flush task is already
//         // spawned and will complete asynchronously — nothing to do here.
//         if self.flush_rx.borrow().is_some() {
//             return;
//         }

//         // Not flushed: abort any in-progress multipart upload.
//         let upload_id = Self::should_abort_on_drop(&self.state).map(|s| s.to_string());
//         if let Some(uid) = upload_id {
//             tokio::spawn(async move {
//                 self.do_abort_multipart_upload(&uid).await;
//             });
//         }
//     }
// }

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
}

impl<S: StorageBackend + Send + Sync + 'static> WriteHandler<S> {
    pub fn new(
        storage: Arc<S>,
        bucket: String,
        key: String,
        access_key: String,
        session_ctx: Arc<SessionContext>,
        part_size: u64,
        cocurrency_limits: u64,
    ) -> Self {
        WriteHandler {
            writer: Arc::new(VecWriter::new(
                storage,
                bucket,
                key,
                access_key,
                session_ctx,
                part_size,
                cocurrency_limits,
            )),
            buffer: WriteState::Buffering { part_buffer: Vec::new() },
            flushed: AtomicBool::new(false),
            pending_handles: Vec::new(),
            flush_rx: RefCell::new(None),
            // upload_id: None,
        }
    }

    /// Append data to the internal buffer. Returns the number of
    /// bytes accepted.
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
                _ => break,
            };

            let writer = self.writer.clone();

            let handle: tokio::task::JoinHandle<Result<CompletedPart, packet::Error>> = tokio::spawn(async move {
                let upload_id = writer.do_resolve_upload_id(part_number).await?;

                writer.do_upload_part(&upload_id, part_number, part_bytes).await
            });

            self.pending_handles.push(handle);
        }
        Ok(accepted)
    }

    /// Spawn the async flush completion task. The spawned task:
    ///   1. Waits for all pending UploadPart JoinHandles.
    ///   2. Handles PutObject (Buffering) or CompleteMultipartUpload
    ///      (Streaming) via the existing helper methods.
    ///   3. Calls `waker.wake()` when done.
    ///
    /// 这个真的需要Arc吗？
    fn do_flush(&mut self, waker: Waker) {
        let (tx, rx) = tokio::sync::oneshot::channel();

        // Take ownership of state and pending handles so the spawned
        // task can use them without borrowing self.
        // 我靠这也行，ai说的对啊
        let state = std::mem::replace(
            &mut self.buffer,
            WriteState::Failed {
                upload_id: String::new(),
                part_number: 0,
            },
        );

        // let storage = Arc::clone(&self.storage);
        // let session_ctx = self.session_ctx.clone();
        // let bucket = self.bucket.clone();
        // let key = self.key.clone();
        // let access_key = self.access_key.clone();
        // let part_size = self.part_size;
        // let shared_upload_id = Arc::clone(&self.shared_upload_id);
        let writer = Arc::clone(&self.writer);
        match state {
            WriteState::Buffering { part_buffer } => {
                tokio::spawn(async move {
                    // 这能通过编译？
                    let result = writer.do_put_object(part_buffer).await;
                    let _ = tx.send(result.map_err(|e| e));
                    waker.wake();
                });
            }
            WriteState::Streaming {
                part_buffer,
                next_part_number,
                ..
            } => {
                // let mut uploaded_parts: Vec<CompletedPart> = Vec::with_capacity(pending_handles.len());
                // 这一行代码什么意思啊
                let mut pending_handles: Vec<_> = std::mem::take(&mut self.pending_handles);
                let writer_clone = writer.clone();
                let handle = tokio::spawn(async move {
                    let upload_id = writer_clone.do_resolve_upload_id(next_part_number).await.unwrap();
                    writer_clone
                        .do_upload_part(&upload_id.clone(), next_part_number, part_buffer)
                        .await
                });
                pending_handles.push(handle);
                tokio::spawn(async move {
                    // 没有进行错误处理
                    let upload_id = writer.do_resolve_upload_id(next_part_number).await.unwrap();
                    // 进行了clone

                    let result = writer
                        .do_multipart_upload(&upload_id, next_part_number, pending_handles)
                        .await
                        .map(|_| ())
                        .map_err(|e| format!("{e:?}"));
                    let _ = tx.send(result);
                    waker.wake();
                });
            }
            WriteState::Failed { upload_id, .. } => {
                tokio::spawn(async move {
                    writer.do_abort_multipart_upload(&upload_id).await;
                    let _ = tx.send(Err("write handle was poisoned (Failed)".to_string()));
                    waker.wake();
                });
                // Err("write handle was poisoned (Failed)".to_string())
            }
        }

        self.flush_rx = RefCell::new(Some(rx));
        self.flushed.store(true, Ordering::SeqCst);

        // tokio::spawn(async move {
        // match state {
        //             WriteState::Buffering { part_buffer } => self.do_put_object(part_buffer).await,
        //             WriteState::Streaming { part_buffer, .. } => {
        //                 let mut uploaded_parts: Vec<CompletedPart> = Vec::with_capacity(pending_handles.len());
        //                 let upload_id = shared_upload_id
        //                     .lock()
        //                     .unwrap()
        //                     .clone()
        //                     .ok_or_else(|| "flush: upload_id not set".to_string())?;
        //                 let max_pn = uploaded_parts.iter().map(|p| p.part_number).max().unwrap_or(0);
        //                 self.flush_streaming(
        //                     &storage,
        //                     &session_ctx,
        //                     &bucket,
        //                     &key,
        //                     &access_key,
        //                     &upload_id,
        //                     part_buffer,
        //                     uploaded_parts,
        //                     max_pn + 1,
        //                     part_size,
        //                 )
        //                 .await
        //                 .map_err(|e| format!("{e:?}"))
        //             }
        //             WriteState::Failed { upload_id, .. } => {
        //                 self.do_abort_multipart_upload(&storage, &session_ctx, &bucket, &key, &access_key, &upload_id)
        //                     .await;
        //                 Err("write handle was poisoned (Failed)".to_string())
        //             }
        //         }
        // })
    }
    //
    // async fn do_mulitpart_upload() {}

    // tokio::spawn(async move {
    //     let outcome = self
    //         .do_flush_inner(
    //             state,
    //             pending_handles,
    //             storage,
    //             session_ctx,
    //             bucket,
    //             key,
    //             access_key,
    //             part_size,
    //             shared_upload_id,
    //         )
    //         .await;
    //     let _ = tx.send(outcome);
    //     waker.wake();
    // });
    // 不确定这个逻辑对不对
    //     self.flush_rx = RwLock::new(Some(rx));
    //     self.flushed.store(true, Ordering::SeqCst);
    // }

    // Inner async logic for do_flush, extracted so tokio::spawn gets
    // a clean future type.
    // async fn do_flush_inner(
    //     self: Arc<Self>,
    //     state: WriteState,
    //     pending_handles: Vec<tokio::task::JoinHandle<Result<CompletedPart, packet::Error>>>,
    //     storage: Arc<S>,
    //     session_ctx: Arc<SessionContext>,
    //     bucket: String,
    //     key: String,
    //     access_key: String,
    //     part_size: u64,
    //     shared_upload_id: Arc<std::sync::Mutex<Option<String>>>,
    // ) -> Result<(), String> {

    // }

    // 这个函数没用吧
    // / Flush the trailing partial part of a Streaming upload, then call
    // / CompleteMultipartUpload.
    // pub(super) async fn flush_streaming(
    //     self: Arc<Self>,
    //     upload_id: &str,
    //     part_buffer: Vec<u8>,
    //     mut uploaded_parts: Vec<CompletedPart>,
    //     next_part_number: i32,
    //     part_size: u64,
    // ) -> Result<(), packet::Error> {
    //     let _ = part_size;

    //     if !part_buffer.is_empty() {
    //         let completed = self.do_upload_part(upload_id, next_part_number, part_buffer).await?;
    //         uploaded_parts.push(completed);
    //     }

    //     self.do_complete_multipart_upload(upload_id, uploaded_parts).await
    // }
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

        // First call: spawn the flush completion task.
        if this.flush_rx.borrow().is_none() {
            this.do_flush(cx.waker().clone());
            return Poll::Pending;
        }

        // Subsequent calls: check the oneshot receiver.
        if let Some(rx) = this.flush_rx.borrow_mut().as_mut() {
            match rx.try_recv() {
                Ok(Ok(())) => Poll::Ready(Ok(())),
                Ok(Err(e)) => Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e))),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => Poll::Pending,
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, "flush task panicked")))
                }
            }
        } else {
            // 这里是因为put_object的情况吗？
            // do_flush always stores a receiver; this branch is unreachable.
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
