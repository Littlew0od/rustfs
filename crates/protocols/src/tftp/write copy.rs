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
use std::task::{Context, Poll, Waker};
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
#[derive(Send, Sync)]
pub struct VecWriter<S: StorageBackend + Send + Sync + 'static> {
    state: WriteState,
    storage: Arc<S>,
    bucket: String,
    key: String,
    access_key: String,
    session_ctx: Arc<SessionContext>,
    part_size: u64,
    cocurrency_limits: u64,
    /// True when poll_flush has been called (normal completion).
    flushed: RefCell<bool>,
    pending_future: Future,
    Waker: Waker,
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
            state: WriteState::Buffering { part_buffer: Vec::new() },
            storage,
            bucket,
            key,
            access_key,
            session_ctx,
            part_size,
            cocurrency_limits,
            flushed: RefCell::new(false),
        }
    }

    /// Append data to the internal buffer. Returns the number of
    /// bytes accepted.
    fn do_write(&mut self, data: &[u8]) -> Result<u64, io::Error> {
        let accepted = data.len() as u64;
        self.write_append_bytes(data)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // Drain loop: transition Buffering→Streaming when the first
        // full part is ready.  CreateMultipartUpload is async and
        // deferred to Drop, so we carry a placeholder upload_id.
        // Full-part UploadPart calls likewise occur in the Drop task.
        while self.write_has_full_part(self.part_size) {
            if matches!(self.state, WriteState::Buffering { .. }) {
                self.write_begin_streaming(&mut self.state, String::new(), false)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            }
            // Full-part uploads are deferred to spawn_flush_task on Drop.
            // tokio::spawn(self.write_flush_on_part(self.part_size));
            // futures::executor::block_on(async move {
            //     RUNTIME
            //         .spawn(async move {
            //             let mut res = vec![];
            //             for i in 0..10 {
            //                 res.push(i);
            //                 tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            //             }
            //             res
            //         })
            //         .await
            //         .unwrap()
            // });
            // .await
            //     .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("flush_on_part: {e:?}")))?;
            break;
        }
        Ok(accepted)
    }

    fn do_flush(&mut self) {
        // No-op; the Drop task will handle the flush.
    }

    // ===== S3 operation helpers (associated functions for spawn paths) =====

    /// Upload buffered bytes with a single PutObject, retrying on
    /// transient backend errors.
    pub(super) async fn do_put_object(&self, buffer: Vec<u8>) -> Result<(), String> {
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

            // Re-authorise each attempt; policy may have changed.
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
            let stream = stream::once(async move { Ok::<Bytes, std::io::Error>(body) });
            let streaming = StreamingBlob::wrap(stream);
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

        // Defensive fallback; the loop above is exhaustive.
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
    pub(super) async fn do_create_multipart_upload(&self) -> Result<String, packet::Error> {
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

    /// Upload one part. Returns a CompletedPart on success.
    pub(super) async fn do_upload_part(
        &self,
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

        // let out = self
        //     .storage
        //     .upload_part(input, &self.access_key, "")
        //     .await
        //     .map_err(|e| packet::Error::Msg(format!("UploadPart: {e}")))?;

        let out = self
            .storage
            .upload_part(input, &self.access_key, "");
            // .await
            // .map_err(|e| packet::Error::Msg(format!("UploadPart: {e}")))?;

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
        &self,
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
    pub(super) async fn do_abort_multipart_upload(&self, upload_id: &str) {
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

    /// Flush the trailing partial part of a Streaming upload, then call
    /// CompleteMultipartUpload.
    pub(super) async fn flush_streaming(
        &self,
        upload_id: &str,
        part_buffer: Vec<u8>,
        mut uploaded_parts: Vec<CompletedPart>,
        next_part_number: i32,
        part_size: u64,
    ) -> Result<(), packet::Error> {
        let _ = part_size; // reserved for future part-size validation

        // Upload trailing partial part if non-empty.
        if !part_buffer.is_empty() {
            let completed = self.do_upload_part(upload_id, next_part_number, part_buffer).await?;
            uploaded_parts.push(completed);
        }

        self.do_complete_multipart_upload(upload_id, uploaded_parts).await
    }

    /// Extract one full part from the Streaming buffer and upload it.
    /// On success the [`CompletedPart`] is appended to
    /// `uploaded_parts` and `next_part_number` is incremented.
    ///
    /// Returns `Ok(())` when the buffer has no full part to drain
    /// (caller should break the drain loop).
    pub(super) async fn write_flush_on_part(&mut self, part_size: u64) -> Result<(), packet::Error> {
        // Extract values under a short mutable borrow, then release before .await.
        let (upload_id, part_bytes, part_number) = match &mut self.state {
            WriteState::Streaming {
                upload_id,
                part_buffer,
                next_part_number,
                ..
            } => {
                let drain_len = (part_size as usize).min(part_buffer.len());
                if drain_len == 0 {
                    return Ok(());
                }
                let part_bytes: Vec<u8> = part_buffer.drain(..drain_len).collect();
                (upload_id.clone(), part_bytes, *next_part_number)
            }
            _ => return Ok(()),
        };

        match self.do_upload_part(&upload_id, part_number, part_bytes).await {
            Ok(completetd) => match &mut self.state {
                WriteState::Streaming {
                    uploaded_parts,
                    next_part_number,
                    ..
                } => {
                    uploaded_parts.push(completetd);
                    *next_part_number += 1;
                    Ok(())
                }
                _ => {
                    error!(
                        event = EVENT_TFTP_MULTIPART_STATE,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
                        upload_id = %upload_id,
                        part_number = part_number,
                        "TFTP write_flush_on_part: state changed unexpectedly"
                    );
                    Err(packet::Error::Msg("state changed unexpectedly".to_string()))
                }
            },
            Err(e) => {
                // Poison the upload on failure.
                let failed_state = WriteState::Failed { upload_id, part_number };
                self.state = failed_state;
                Err(e)
            }
        }
    }

    /// Spawn the async completion task: PutObject for small files,
    /// CompleteMultipartUpload for large files. Called from Drop
    /// when `flushed` is true.
    // fn spawn_flush_task(&self) {
    //     let storage = Arc::clone(&self.storage);
    //     let session_ctx = self.session_ctx.clone();
    //     let bucket = self.bucket.clone();
    //     let key = self.key.clone();
    //     let access_key = self.access_key.clone();
    //     let part_size = self.part_size;
    //     // Take what we need from the current state by value.
    //     // We must not move out of &self, so clone the fields.
    //     match &self.state {
    //         WriteState::Buffering { part_buffer } => {
    //             let buffer = part_buffer.clone();
    //             // tokio::spawn(async move {
    //             //     if let Err(e) = Self::do_put_object(&storage, &session_ctx, &bucket, &key, &access_key, buffer).await {
    //             //         error!(
    //             //             event = EVENT_TFTP_PUTOBJECT_STATE,
    //             //             component = LOG_COMPONENT_PROTOCOLS,
    //             //             subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
    //             //             bucket = %bucket, key = %key,
    //             //             err = %e,
    //             //             "TFTP flush PutObject failed"
    //             //         );
    //             //     }
    //             // });
    //         }
    //         WriteState::Streaming {
    //             upload_id: _,
    //             abort_authorized: _,
    //             part_buffer,
    //             uploaded_parts,
    //             next_part_number,
    //         } => {
    //             let part_buffer_clone = part_buffer.clone();
    //             let uploaded_parts_clone = uploaded_parts.clone();
    //             let next_part_number_val = *next_part_number;
    //             tokio::spawn(async move {
    //                 // 1. CreateMultipartUpload — replaces the placeholder
    //                 //    upload_id set by the sync drain loop.
    //                 let upload_id = match Self::do_create_multipart_upload(
    //                     &storage, &session_ctx, &bucket, &key, &access_key,
    //                 )
    //                 .await
    //                 {
    //                     Ok(id) => id,
    //                     Err(e) => {
    //                         error!(
    //                             event = EVENT_TFTP_MULTIPART_STATE,
    //                             component = LOG_COMPONENT_PROTOCOLS,
    //                             subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
    //                             bucket = %bucket, key = %key,
    //                             err = ?e,
    //                             "TFTP flush CreateMultipartUpload failed"
    //                         );
    //                         return;
    //                     }
    //                 };
    //                 // 2. Build a local state machine and drain full parts
    //                 //    from the buffer.
    //                 let mut local_state = WriteState::Streaming {
    //                     upload_id: upload_id.clone(),
    //                     abort_authorized: false, // not needed for flush
    //                     part_buffer: part_buffer_clone,
    //                     uploaded_parts: uploaded_parts_clone,
    //                     next_part_number: next_part_number_val,
    //                 };
    //                 while write_has_full_part(&local_state, part_size) {
    //                     if let Err(e) = write_flush_one_part(
    //                         &mut local_state,
    //                         &storage,
    //                         &session_ctx,
    //                         &bucket,
    //                         &key,
    //                         &access_key,
    //                         part_size,
    //                     )
    //                     .await
    //                     {
    //                         error!(
    //                             event = EVENT_TFTP_MULTIPART_STATE,
    //                             component = LOG_COMPONENT_PROTOCOLS,
    //                             subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
    //                             bucket = %bucket, key = %key,
    //                             upload_id = %upload_id,
    //                             err = ?e,
    //                             "TFTP flush UploadPart failed"
    //                         );
    //                         Self::do_abort_multipart_upload(
    //                             &storage, &session_ctx, &bucket, &key, &access_key, &upload_id,
    //                         )
    //                         .await;
    //                         return;
    //                     }
    //                 }
    //                 // 3. Decompose local_state for flush_streaming.
    //                 let (trailing_buffer, uploaded, next_pn) = match local_state {
    //                     WriteState::Streaming {
    //                         part_buffer,
    //                         uploaded_parts,
    //                         next_part_number,
    //                         ..
    //                     } => (part_buffer, uploaded_parts, next_part_number),
    //                     _ => unreachable!(),
    //                 };
    //                 if let Err(e) = Self::flush_streaming(
    //                     &storage,
    //                     &session_ctx,
    //                     &bucket,
    //                     &key,
    //                     &access_key,
    //                     &upload_id,
    //                     trailing_buffer,
    //                     uploaded,
    //                     next_pn,
    //                     part_size,
    //                 )
    //                 .await
    //                 {
    //                     error!(
    //                         event = EVENT_TFTP_MULTIPART_STATE,
    //                         component = LOG_COMPONENT_PROTOCOLS,
    //                         subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
    //                         bucket = %bucket, key = %key,
    //                         upload_id = %upload_id,
    //                         err = ?e,
    //                         "TFTP flush CompleteMultipartUpload failed"
    //                     );
    //                     // Best-effort abort after Complete failure.
    //                     Self::do_abort_multipart_upload(
    //                         &storage, &session_ctx, &bucket, &key, &access_key, &upload_id,
    //                     )
    //                     .await;
    //                 }
    //             });
    //         }
    //         WriteState::Failed { .. } => {
    //             // Upload already poisoned; nothing to flush.
    //         }
    //     }
    // }

    /// Spawn the async abort task for an in-progress multipart
    /// upload. Called from Drop when `flushed` is false.
    // fn spawn_abort_task(&self) {
    //     let upload_id = match should_abort_on_drop(&self.state) {
    //         Some(id) => id.to_string(),
    //         None => return,
    //     };
    //     let storage = Arc::clone(&self.storage);
    //     let session_ctx = self.session_ctx.clone();
    //     let bucket = self.bucket.clone();
    //     let key = self.key.clone();
    //     let access_key = self.access_key.clone();
    //     tokio::spawn(async move {
    //         Self::do_abort_multipart_upload(&storage, &session_ctx, &bucket, &key, &access_key, &upload_id).await;
    //     });
    // }

    // ---------------------------------------------------------------------------
    // State-machine helpers (sync, pure)
    // ---------------------------------------------------------------------------

    /// Append incoming bytes to whichever buffer the current phase carries.
    ///
    /// Failed rejects the write: a prior UploadPart failure poisoned the
    /// upload. Any further bytes would violate the sequential-offset
    /// invariant.
    fn write_append_bytes(&mut self, data: &[u8]) -> Result<(), &'static str> {
        match &mut self.state {
            WriteState::Buffering { part_buffer } | WriteState::Streaming { part_buffer, .. } => {
                part_buffer.extend_from_slice(data);
                Ok(())
            }
            WriteState::Failed { .. } => {
                warn!(
                    event = EVENT_TFTP_WRITE_STATE,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
                    result = "append_on_failed_handle",
                    "TFTP write append rejected on failed handle"
                );
                Err("write rejected: handle is poisoned (Failed)")
            }
        }
    }

    /// Drain-loop predicate. Returns true when the current phase's
    /// part_buffer holds at least `part_size` bytes.
    ///
    /// Failed returns false so a drain loop exits cleanly.
    // Called by the handler's write-dispatch drain loop (pending integration).
    fn write_has_full_part(&self, part_size: u64) -> bool {
        match &self.state {
            WriteState::Buffering { part_buffer } | WriteState::Streaming { part_buffer, .. } => {
                (part_buffer.len() as u64) >= part_size
            }
            WriteState::Failed { .. } => false,
        }
    }

    /// Transition from [`WriteState::Buffering`] to
    /// [`WriteState::Streaming`].
    ///
    /// The caller must have already issued CreateMultipartUpload and pass
    /// the resulting `upload_id` and cached `abort_authorized` flag. The
    /// existing `part_buffer` is carried forward so no bytes are lost.
    ///
    /// Returns an error when the state is not Buffering.
    // Called by the handler when drain-loop transitions Buffering→Streaming.

    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // TODO: integrate do_create_multipart_upload() call; async, deferred to Drop for now.
    fn write_begin_streaming(
        &mut self,
        state: &mut WriteState,
        upload_id: String,
        abort_authorized: bool,
    ) -> Result<(), &'static str> {
        let part_buffer = match &mut self.state {
            WriteState::Buffering { part_buffer } => std::mem::take(part_buffer),
            _ => {
                error!(
                    event = EVENT_TFTP_MULTIPART_STATE,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_TFTP_WRITE,
                    result = "begin_streaming_not_buffering",
                    "TFTP begin_streaming called on non-Buffering state"
                );
                return Err("begin_streaming: state is not Buffering");
            }
        };
        *state = WriteState::Streaming {
            upload_id,
            abort_authorized,
            part_buffer,
            uploaded_parts: Vec::new(),
            next_part_number: 1,
        };
        Ok(())
    }

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

// 对于do_write来说，如果需要异步上传，返回Joinandle，其他过程正常处理。
// 对于do_flush来说，生成一个单独的tokio::spawn任务，等待所有JoinHandle执行结束后再返回。在这个任务中需要将waker传入，任务执行结束后调用waker.wake()唤醒当前任务，以及完成complete_multipart_upload或put_object的操作。
impl<S: StorageBackend + Send + Sync + 'static> AsyncWrite for VecWriter<S> {
    // 对于put_object来说，绝对不应该在poll_write中调用
    fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        // let temp = tokio::spawn(async move {
        //     tokio::time::sleep(std::time::Duration::from_millis(100));
        //     _cx.waker().wake_by_ref();
        // });
        // 我是不是可以收集这些JoinHandle，等到flush时等待其执行结束？
        match self.do_write(buf) {
            Ok(n) => Poll::Ready(Ok(n as usize)),
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // self.do_flush();
        // 可以在这其中检测任务是否执行结束，
        // 将waker传入任务中，任务执行结束后调用waker.wake()唤醒当前任务，这样是不是就可以在poll_flush中等待任务执行结束了？
        self.flushed.replace(true);
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl<S: StorageBackend + Send + Sync + 'static> Drop for VecWriter<S> {
    fn drop(&mut self) {
        if *self.flushed.borrow() {
            self.spawn_flush_task();
        } else {
            self.spawn_abort_task();
        }
    }
}

// impl<S: StorageBackend + Send + Sync + 'static> Drop for VecWriter<S> {
//     fn drop(&mut self) {
//         let data = std::mem::take(&mut self.buf);
//         let storage = Arc::clone(&self.storage);
//         let bucket = self.bucket.clone();
//         let key = self.key.clone();
//         let access_key = self.access_key.clone();

//         tokio::spawn(async move {
//             let size = data.len();
//             let mut put_builder = PutObjectInput::builder();
//             put_builder.set_bucket(bucket.clone());
//             put_builder.set_key(key.clone());
//             put_builder.set_content_length(Some(size as i64));

//             // Create StreamingBlob with known size
//             let data_bytes = bytes::Bytes::from(data);
//             let stream = stream::once(async move { Ok::<bytes::Bytes, std::io::Error>(data_bytes) });
//             let streaming_blob = StreamingBlob::wrap(stream);
//             put_builder.set_body(Some(streaming_blob));
//             let input = match put_builder.build() {
//                 Ok(input) => input,
//                 Err(e) => {
//                     error!(
//                         event = EVENT_TFTP_WRQ_STATE,
//                         component = LOG_COMPONENT_PROTOCOLS,
//                         subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
//                         bucket = %bucket, key = %key, error = %e,
//                         "Failed to build PutObjectInput for TFTP WRQ"
//                     );
//                     return;
//                 }
//             };

//             match storage.put_object(input, &access_key, "").await {
//                 Ok(_) => {
//                     info!(
//                         event = EVENT_TFTP_WRQ_STATE,
//                         component = LOG_COMPONENT_PROTOCOLS,
//                         subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
//                         bucket = %bucket, key = %key, size = size,
//                         "TFTP upload to S3 completed"
//                     );
//                 }
//                 Err(e) => {
//                     error!(
//                         event = EVENT_TFTP_WRQ_STATE,
//                         component = LOG_COMPONENT_PROTOCOLS,
//                         subsystem = LOG_SUBSYSTEM_TFTP_SERVER,
//                         bucket = %bucket, key = %key, error = %e,
//                         "Failed to upload TFTP data to S3"
//                     );
//                 }
//             }
//         });
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;

    // --- write_append_bytes ---

    #[test]
    fn append_bytes_buffering_extends_buffer() {
        let mut state = WriteState::Buffering {
            part_buffer: vec![1, 2, 3],
        };
        write_append_bytes(&mut state, &[9, 9, 9]).unwrap();
        match &state {
            WriteState::Buffering { part_buffer } => {
                assert_eq!(part_buffer.as_slice(), &[1, 2, 3, 9, 9, 9]);
            }
            _ => panic!("state must remain Buffering"),
        }
    }

    #[test]
    fn append_bytes_streaming_extends_buffer() {
        let mut state = WriteState::Streaming {
            upload_id: "UP-1".to_string(),
            abort_authorized: true,
            part_buffer: vec![5, 6],
            uploaded_parts: Vec::new(),
            next_part_number: 1,
        };
        write_append_bytes(&mut state, &[7, 8]).unwrap();
        match &state {
            WriteState::Streaming { part_buffer, .. } => {
                assert_eq!(part_buffer.as_slice(), &[5, 6, 7, 8]);
            }
            _ => panic!("state must remain Streaming"),
        }
    }

    #[test]
    fn append_bytes_failed_returns_err() {
        let mut state = WriteState::Failed {
            upload_id: "UP-F".to_string(),
            part_number: 3,
        };
        assert!(write_append_bytes(&mut state, &[1, 2, 3]).is_err());
        assert!(matches!(state, WriteState::Failed { .. }));
    }

    // --- write_has_full_part ---

    #[test]
    fn has_full_part_exact_boundary() {
        let part_size: u64 = 1024;
        let at = WriteState::Buffering {
            part_buffer: vec![0u8; 1024],
        };
        let below = WriteState::Buffering {
            part_buffer: vec![0u8; 1023],
        };
        assert!(write_has_full_part(&at, part_size));
        assert!(!write_has_full_part(&below, part_size));
    }

    #[test]
    fn has_full_part_failed_returns_false() {
        let state = WriteState::Failed {
            upload_id: "UP-F".to_string(),
            part_number: 1,
        };
        assert!(!write_has_full_part(&state, 0));
        assert!(!write_has_full_part(&state, u64::MAX));
    }

    #[test]
    fn has_full_part_streaming_checks_buffer() {
        let part_size: u64 = 100;
        let above = WriteState::Streaming {
            upload_id: "UP".to_string(),
            abort_authorized: true,
            part_buffer: vec![0u8; 100],
            uploaded_parts: Vec::new(),
            next_part_number: 1,
        };
        let below = WriteState::Streaming {
            upload_id: "UP".to_string(),
            abort_authorized: true,
            part_buffer: vec![0u8; 99],
            uploaded_parts: Vec::new(),
            next_part_number: 1,
        };
        assert!(write_has_full_part(&above, part_size));
        assert!(!write_has_full_part(&below, part_size));
    }

    // --- write_begin_streaming ---

    #[test]
    fn begin_streaming_transitions_and_preserves_buffer() {
        let mut state = WriteState::Buffering {
            part_buffer: vec![1, 2, 3, 4],
        };
        write_begin_streaming(&mut state, "UP-BEG".to_string(), true).unwrap();
        match &state {
            WriteState::Streaming {
                upload_id,
                abort_authorized,
                part_buffer,
                uploaded_parts,
                next_part_number,
            } => {
                assert_eq!(upload_id, "UP-BEG");
                assert!(*abort_authorized);
                assert_eq!(part_buffer.as_slice(), &[1, 2, 3, 4]);
                assert!(uploaded_parts.is_empty());
                assert_eq!(*next_part_number, 1);
            }
            _ => panic!("state must be Streaming after begin_streaming"),
        }
    }

    #[test]
    fn begin_streaming_with_abort_denied_caches_false() {
        let mut state = WriteState::Buffering { part_buffer: Vec::new() };
        write_begin_streaming(&mut state, "UP-DENY".to_string(), false).unwrap();
        match &state {
            WriteState::Streaming { abort_authorized, .. } => {
                assert!(!abort_authorized);
            }
            _ => panic!("state must be Streaming"),
        }
    }

    #[test]
    fn begin_streaming_on_streaming_returns_err() {
        let mut state = WriteState::Streaming {
            upload_id: "UP-EXIST".to_string(),
            abort_authorized: true,
            part_buffer: Vec::new(),
            uploaded_parts: Vec::new(),
            next_part_number: 1,
        };
        let upload_id_before = match &state {
            WriteState::Streaming { upload_id, .. } => upload_id.clone(),
            _ => unreachable!(),
        };
        assert!(write_begin_streaming(&mut state, "UP-NEW".to_string(), true).is_err());
        match &state {
            WriteState::Streaming { upload_id, .. } => {
                assert_eq!(upload_id, &upload_id_before);
            }
            _ => panic!("state must remain Streaming"),
        }
    }

    #[test]
    fn begin_streaming_on_failed_returns_err() {
        let mut state = WriteState::Failed {
            upload_id: "UP-F".to_string(),
            part_number: 2,
        };
        assert!(write_begin_streaming(&mut state, "UP-NEW".to_string(), true).is_err());
        assert!(matches!(state, WriteState::Failed { .. }));
    }

    // --- should_abort_on_drop ---

    #[test]
    fn should_abort_on_drop_buffering_is_none() {
        let state = WriteState::Buffering { part_buffer: Vec::new() };
        assert!(should_abort_on_drop(&state).is_none());
    }

    #[test]
    fn should_abort_on_drop_streaming_authorized_returns_upload_id() {
        let state = WriteState::Streaming {
            upload_id: "UP-7".to_string(),
            abort_authorized: true,
            part_buffer: Vec::new(),
            uploaded_parts: Vec::new(),
            next_part_number: 1,
        };
        assert_eq!(should_abort_on_drop(&state), Some("UP-7"));
    }

    #[test]
    fn should_abort_on_drop_streaming_denied_is_none() {
        let state = WriteState::Streaming {
            upload_id: "UP-8".to_string(),
            abort_authorized: false,
            part_buffer: Vec::new(),
            uploaded_parts: Vec::new(),
            next_part_number: 1,
        };
        assert!(should_abort_on_drop(&state).is_none());
    }

    #[test]
    fn should_abort_on_drop_failed_returns_upload_id() {
        let state = WriteState::Failed {
            upload_id: "UP-9".to_string(),
            part_number: 1,
        };
        assert_eq!(should_abort_on_drop(&state), Some("UP-9"));
    }
}
