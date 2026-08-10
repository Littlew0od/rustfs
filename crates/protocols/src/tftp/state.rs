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

use s3s::dto::ETag;
use tracing::{error, info, warn};

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

/// Write-side state machine for a single open write handle.
///
/// Transitions are strictly forward. A handle begins in Buffering. Once the
/// first full part is ready, the driver issues CreateMultipartUpload and
/// transitions to Streaming. On any UploadPart failure the phase moves to
/// Failed, which rejects further writes and releases the upload_id via
/// AbortMultipartUpload at CLOSE. There is no recovery from Failed.
///
///
///     OPEN
///      |
///      v
///   Buffering --CLOSE--> PutObject (small file) ---------> DONE
///      |
///      |  buffer >= part_size
///      |  CreateMultipartUpload ok
///      v
///   Streaming --CLOSE--> UploadPart (tail) then
///      | ^               CompleteMultipartUpload --------> DONE
///      | |               (large file)
///      | |
///      | |  buffer >= part_size
///      | |  UploadPart ok (loop)
///      | |
///      | UploadPart fails
///      v
///    Failed  --CLOSE--> AbortMultipartUpload ---> (handle gone, no object)
///
///   Retry: CreateMultipartUpload fails -> stay in Buffering,
///          retry on next flush.
///
pub(super) enum WriteState {
    /// Bytes received via WRITE not yet flushed to S3. Bounded by part_size: the
    /// while-loop in write() drains it below part_size before returning.
    Buffering {
        part_buffer: Vec<u8>,
    },
    /// CreateMultipartUpload has been issued. Full parts flush at the
    /// completion of each part, and the last partial part flush issues
    /// CompleteMultipartUpload.
    Streaming {
        /// upload_id returned by CreateMultipartUpload. Required by every
        /// subsequent UploadPart, CompleteMultipartUpload, and
        /// AbortMultipartUpload call.
        /// 这个好像荒废了
        upload_id: String,
        /// Cached result of authorize_operation for AbortMultipartUpload,
        /// evaluated at CreateMultipartUpload time. Drop consults this
        /// to decide whether to issue AbortMultipartUpload without
        /// running an async auth call (Drop is synchronous). close()
        /// consults it too for consistency: same policy decision, same
        /// observable outcome. False means the principal's IAM policy
        /// denies AbortMultipartUpload, so cleanup is deferred to the
        /// bucket's AbortIncompleteMultipartUpload lifecycle rule. The
        /// flag is cached for one upload's lifetime: a policy edit
        /// between the cache and the abort attempt is not honoured in
        /// this session.
        abort_authorized: bool,
        /// Bytes received via WRITE not yet flushed to S3.
        part_buffer: Vec<u8>,
        /// Parts already uploaded. Passed to CompleteMultipartUpload in
        /// order. Each entry carries the part number and the ETag returned
        /// by UploadPart.
        uploaded_parts: Vec<CompletedPart>,
        /// Part number to use for the next UploadPart call. S3 part numbers
        /// begin at 1 and increase monotonically.
        next_part_number: i32,
    },
    Failed {
        upload_id: String,
        part_number: i32,
    },
}

impl WriteState {
    //// Append incoming bytes to whichever buffer the current phase carries.
    ///
    /// Failed rejects the write: a prior UploadPart failure poisoned the
    /// upload. Any further bytes would violate the sequential-offset
    /// invariant.
    pub fn write_append_bytes(&mut self, data: &[u8]) -> Result<(), &'static str> {
        match self {
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
    pub fn write_has_full_part(&self, part_size: u64) -> bool {
        match self {
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
    pub fn write_begin_streaming(&mut self, upload_id: String, abort_authorized: bool) -> Result<(), &'static str> {
        let part_buffer = match self {
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
        *self = WriteState::Streaming {
            upload_id,
            abort_authorized,
            part_buffer,
            uploaded_parts: Vec::new(),
            next_part_number: 1,
        };
        Ok(())
    }
}

/// Record of one successfully uploaded part. Carries the part number and
/// ETag needed by CompleteMultipartUpload to assemble the final object.
#[derive(Clone)]
pub(super) struct CompletedPart {
    pub(super) part_number: i32,
    pub(super) e_tag: ETag,
}
